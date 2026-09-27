use crate::models::EpgConfig;
use anyhow::{Context, Result};
use chrono::{DateTime, FixedOffset, Local, TimeZone, Utc};
use ferrous_opencc::{OpenCC, config::BuiltinConfig};
use flate2::{Compression, write::GzEncoder};
use futures::{StreamExt, stream};
use regex::Regex;
use reqwest::Proxy;
use roxmltree::Document;
use std::{
    collections::{HashMap, HashSet},
    fs::{self, File},
    io::Write,
    path::Path,
    sync::{Arc, OnceLock, RwLock},
    time::{Duration, SystemTime},
};

#[derive(Debug, Clone)]
pub struct EpgProgramme {
    pub start: DateTime<FixedOffset>,
    pub stop: DateTime<FixedOffset>,
    pub title: String,
}

pub type EpgResult = HashMap<String, Vec<EpgProgramme>>;

#[derive(Debug, Clone, serde::Serialize)]
pub struct EpgStatus {
    pub state: EpgRunState,
    pub last_started_at: Option<DateTime<Utc>>,
    pub last_finished_at: Option<DateTime<Utc>>,
    pub last_duration_ms: Option<u128>,
    pub last_error: Option<String>,
    pub channels: usize,
    pub programmes: usize,
    pub xml_path: String,
    pub gz_path: String,
}

impl Default for EpgStatus {
    fn default() -> Self {
        Self {
            state: EpgRunState::Idle,
            last_started_at: None,
            last_finished_at: None,
            last_duration_ms: None,
            last_error: None,
            channels: 0,
            programmes: 0,
            xml_path: String::new(),
            gz_path: String::new(),
        }
    }
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EpgRunState {
    Idle,
    Running,
    Success,
    Skipped,
    Error,
}

impl EpgStatus {
    pub fn start(&mut self, config: &EpgConfig) {
        self.state = EpgRunState::Running;
        self.last_started_at = Some(Utc::now());
        self.last_finished_at = None;
        self.last_duration_ms = None;
        self.last_error = None;
        self.xml_path = config.output_xml_path.clone();
        self.gz_path = config.output_gz_path.clone();
    }

    pub fn finish_success(&mut self, started: std::time::Instant, programmes: &EpgResult) {
        self.state = EpgRunState::Success;
        self.last_finished_at = Some(Utc::now());
        self.last_duration_ms = Some(started.elapsed().as_millis());
        self.last_error = None;
        self.channels = programmes.len();
        self.programmes = programmes.values().map(Vec::len).sum();
    }

    pub fn finish_skipped(&mut self, started: std::time::Instant) {
        self.state = EpgRunState::Skipped;
        self.last_finished_at = Some(Utc::now());
        self.last_duration_ms = Some(started.elapsed().as_millis());
        self.last_error = None;
        self.channels = 0;
        self.programmes = 0;
    }

    pub fn finish_error(&mut self, started: std::time::Instant, error: String) {
        self.state = EpgRunState::Error;
        self.last_finished_at = Some(Utc::now());
        self.last_duration_ms = Some(started.elapsed().as_millis());
        self.last_error = Some(error);
    }
}

fn http_proxy_from_config(value: &str) -> Option<Proxy> {
    let value = value.trim();
    if value.is_empty() {
        return None;
    }
    Proxy::all(value).ok()
}

pub async fn run(config: &EpgConfig, names: Option<&HashSet<String>>) -> Result<EpgResult> {
    let urls = read_epg_urls(&config.sources_path, &config.cdn_url)?;
    let aliases = aliases_for_path(&config.alias_path);
    if urls.is_empty() {
        tracing::info!(path = %config.sources_path, "No EPG sources configured");
        return Ok(HashMap::new());
    }

    let mut client_builder = reqwest::Client::builder()
        .use_rustls_tls()
        .timeout(Duration::from_millis(config.timeout_ms))
        .user_agent("iptvapi-rs/0.0.2");
    if let Some(proxy) = http_proxy_from_config(&config.http_proxy) {
        client_builder = client_builder.proxy(proxy);
    }
    let client = client_builder.build()?;
    let name_filter = names
        .map(|set| aliases.normalize_filter_set(set))
        .map(Arc::new);
    let concurrency = config.concurrency.max(1);

    let results = stream::iter(urls.into_iter().map(|url| {
        let client = client.clone();
        let name_filter = name_filter.clone();
        let aliases = aliases.clone();
        async move { fetch_parse_one(&client, url, name_filter.as_deref(), &aliases).await }
    }))
    .buffer_unordered(concurrency)
    .collect::<Vec<_>>()
    .await;

    let mut merged = HashMap::new();
    let mut seen = HashSet::new();
    for result in results {
        match result {
            Ok(source_result) => merge_source_result(&mut merged, &mut seen, source_result),
            Err(error) => {
                metrics::counter!("iptvapi_epg_source_errors_total").increment(1);
                tracing::warn!(error = %error, "Failed to fetch/parse EPG source");
            }
        }
    }

    metrics::gauge!("iptvapi_epg_channels").set(merged.len() as f64);
    metrics::gauge!("iptvapi_epg_programmes")
        .set(merged.values().map(Vec::len).sum::<usize>() as f64);
    Ok(merged)
}

pub fn write_outputs(programmes: &EpgResult, xml_path: &str, gz_path: &str) -> Result<()> {
    write_xml(programmes, xml_path)?;
    compress_to_gz(xml_path, gz_path)?;
    Ok(())
}

fn read_epg_urls(path: &str, cdn_url: &str) -> Result<Vec<String>> {
    let content = match fs::read_to_string(path) {
        Ok(content) => content,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error).with_context(|| format!("read EPG source list: {path}")),
    };
    let url_re = Regex::new(r"(?i)https?://\S+")?;
    let mut seen = HashSet::new();
    let mut urls = Vec::new();
    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some(found) = url_re.find(line) {
            let mut url = found.as_str().trim_end_matches(',').to_owned();
            if !cdn_url.trim().is_empty() && url.contains("raw.githubusercontent.com") {
                url = join_url(cdn_url, &url);
            }
            if seen.insert(url.clone()) {
                urls.push(url);
            }
        }
    }
    Ok(urls)
}

fn join_url(prefix: &str, url: &str) -> String {
    format!(
        "{}/{}",
        prefix.trim_end_matches('/'),
        url.trim_start_matches('/')
    )
}

async fn fetch_parse_one(
    client: &reqwest::Client,
    url: String,
    names: Option<&HashSet<String>>,
    aliases: &ChannelAliases,
) -> Result<ParsedEpg> {
    tracing::info!(url = %url, "Fetching EPG source");
    let content = client
        .get(&url)
        .send()
        .await
        .with_context(|| format!("request EPG source {url}"))?
        .error_for_status()
        .with_context(|| format!("bad EPG status {url}"))?
        .text()
        .await
        .with_context(|| format!("read EPG source body {url}"))?;
    parse_epg(&content, names, aliases).with_context(|| format!("parse EPG source {url}"))
}

#[derive(Debug, Default)]
struct ParsedEpg {
    channels: HashMap<String, String>,
    programmes: HashMap<String, Vec<EpgProgramme>>,
}

fn parse_epg(
    content: &str,
    names: Option<&HashSet<String>>,
    aliases: &ChannelAliases,
) -> Result<ParsedEpg> {
    let doc = Document::parse(content)?;
    let mut channels = HashMap::new();
    let mut programmes: HashMap<String, Vec<EpgProgramme>> = HashMap::new();
    let cutoff = Local::now() - chrono::Duration::days(7);

    for channel in doc
        .descendants()
        .filter(|node| node.has_tag_name("channel"))
    {
        if let Some(id) = channel.attribute("id")
            && let Some(display_name) = channel
                .children()
                .find(|node| node.has_tag_name("display-name"))
                .and_then(|node| node.text())
        {
            channels.insert(id.to_owned(), aliases.primary_name(display_name));
        }
    }

    for programme in doc
        .descendants()
        .filter(|node| node.has_tag_name("programme"))
    {
        let Some(channel_id) = programme.attribute("channel") else {
            continue;
        };
        let Some(display_name) = channels.get(channel_id) else {
            continue;
        };
        if names.is_some_and(|set| !set.contains(display_name)) {
            continue;
        }
        let Some(start) = programme.attribute("start").and_then(parse_xmltv_datetime) else {
            continue;
        };
        if start < cutoff.with_timezone(start.offset()) {
            continue;
        }
        let Some(stop) = programme.attribute("stop").and_then(parse_xmltv_datetime) else {
            continue;
        };
        let Some(title) = programme
            .children()
            .find(|node| node.has_tag_name("title"))
            .and_then(|node| node.text())
            .map(str::trim)
            .filter(|value| !value.is_empty())
        else {
            continue;
        };
        programmes
            .entry(channel_id.to_owned())
            .or_default()
            .push(EpgProgramme {
                start,
                stop,
                title: convert_t2s(title),
            });
    }

    Ok(ParsedEpg {
        channels,
        programmes,
    })
}

fn parse_xmltv_datetime(value: &str) -> Option<DateTime<FixedOffset>> {
    let compact: String = value.chars().filter(|c| !c.is_whitespace()).collect();
    if let Ok(parsed) = DateTime::parse_from_str(&compact, "%Y%m%d%H%M%S%z") {
        return Some(parsed);
    }
    if compact.len() >= 14 {
        let naive = chrono::NaiveDateTime::parse_from_str(&compact[..14], "%Y%m%d%H%M%S").ok()?;
        let offset = FixedOffset::east_opt(8 * 3600)?;
        return offset.from_local_datetime(&naive).single();
    }
    None
}

fn merge_source_result(merged: &mut EpgResult, seen: &mut HashSet<String>, source: ParsedEpg) {
    for (channel_id, display_name) in source.channels {
        if seen.contains(&channel_id) || seen.contains(&display_name) {
            continue;
        }
        if let Some(programmes) = source.programmes.get(&channel_id) {
            seen.insert(channel_id);
            seen.insert(display_name.clone());
            merged.insert(display_name, programmes.clone());
        }
    }
}

fn write_xml(programmes: &EpgResult, path: &str) -> Result<()> {
    if let Some(parent) = Path::new(path)
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent)?;
    }
    let mut keys = programmes.keys().collect::<Vec<_>>();
    keys.sort();

    let mut file = File::create(path).with_context(|| format!("create EPG XML: {path}"))?;
    writeln!(file, "<?xml version=\"1.0\" encoding=\"UTF-8\"?>")?;
    writeln!(
        file,
        "<tv date=\"{}\">",
        format_epg_time(&Local::now().fixed_offset())
    )?;
    for channel in &keys {
        let escaped_attr = escape_xml_attr(channel);
        let escaped_text = escape_xml_text(channel);
        writeln!(file, "\t<channel id=\"{escaped_attr}\">")?;
        writeln!(
            file,
            "\t\t<display-name lang=\"zh\">{escaped_text}</display-name>"
        )?;
        writeln!(file, "\t</channel>")?;
        if let Some(items) = programmes.get(*channel) {
            for item in items {
                writeln!(
                    file,
                    "\t<programme channel=\"{}\" start=\"{}\" stop=\"{}\">",
                    escaped_attr,
                    format_epg_time(&item.start),
                    format_epg_time(&item.stop)
                )?;
                writeln!(
                    file,
                    "\t\t<title lang=\"zh\">{}</title>",
                    escape_xml_text(&item.title)
                )?;
                writeln!(file, "\t</programme>")?;
            }
        }
    }
    writeln!(file, "</tv>")?;
    Ok(())
}

fn format_epg_time(value: &DateTime<FixedOffset>) -> String {
    let offset = FixedOffset::east_opt(8 * 3600).expect("+0800 offset is valid");
    value
        .with_timezone(&offset)
        .format("%Y%m%d%H%M%S %z")
        .to_string()
}

fn compress_to_gz(input_path: &str, output_path: &str) -> Result<()> {
    if let Some(parent) = Path::new(output_path)
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent)?;
    }
    let input = fs::read(input_path).with_context(|| format!("read EPG XML: {input_path}"))?;
    let output =
        File::create(output_path).with_context(|| format!("create EPG gzip: {output_path}"))?;
    let mut encoder = GzEncoder::new(output, Compression::default());
    encoder.write_all(&input)?;
    encoder.finish()?;
    Ok(())
}

fn escape_xml_text(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

fn escape_xml_attr(value: &str) -> String {
    escape_xml_text(value).replace('"', "&quot;")
}

static ALIAS_CACHE: OnceLock<RwLock<AliasCache>> = OnceLock::new();

#[derive(Debug, Default)]
struct AliasCache {
    path: String,
    modified: Option<SystemTime>,
    aliases: Arc<ChannelAliases>,
}

pub(crate) fn aliases_for_path(path: &str) -> Arc<ChannelAliases> {
    let modified = fs::metadata(path)
        .and_then(|metadata| metadata.modified())
        .ok();
    let cache = ALIAS_CACHE.get_or_init(|| RwLock::new(AliasCache::default()));

    {
        let cached = cache.read().expect("alias cache read lock poisoned");
        if cached.path == path && cached.modified == modified {
            return cached.aliases.clone();
        }
    }

    let mut cached = cache.write().expect("alias cache write lock poisoned");
    if cached.path == path && cached.modified == modified {
        return cached.aliases.clone();
    }

    let aliases = Arc::new(ChannelAliases::load(path));
    metrics::gauge!("iptvapi_epg_aliases").set(aliases.alias_to_primary.len() as f64);
    metrics::gauge!("iptvapi_epg_alias_patterns").set(aliases.pattern_to_primary.len() as f64);
    cached.path = path.to_owned();
    cached.modified = modified;
    cached.aliases = aliases.clone();
    aliases
}

#[derive(Debug, Default)]
pub(crate) struct ChannelAliases {
    alias_to_primary: HashMap<String, String>,
    pattern_to_primary: Vec<(fancy_regex::Regex, String)>,
}

impl ChannelAliases {
    pub(crate) fn load(path: &str) -> Self {
        let content = match fs::read_to_string(path) {
            Ok(content) => content,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Self::default(),
            Err(error) => {
                tracing::warn!(path = %path, error = %error, "Failed to read alias file");
                return Self::default();
            }
        };

        let mut aliases = Self::default();
        for line in content.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') || !line.contains(',') {
                continue;
            }
            let parts = line
                .split(',')
                .map(str::trim)
                .filter(|part| !part.is_empty())
                .collect::<Vec<_>>();
            let Some(primary) = parts.first() else {
                continue;
            };
            aliases
                .alias_to_primary
                .insert((*primary).to_owned(), (*primary).to_owned());
            aliases
                .alias_to_primary
                .insert(format_channel_name(primary), (*primary).to_owned());

            for alias in parts.iter().skip(1) {
                if let Some(pattern) = alias.strip_prefix("re:") {
                    match fancy_regex::Regex::new(pattern) {
                        Ok(pattern) => aliases
                            .pattern_to_primary
                            .push((pattern, (*primary).to_owned())),
                        Err(error) => {
                            tracing::warn!(alias = %alias, error = %error, "Invalid channel alias regex")
                        }
                    }
                    continue;
                }
                aliases
                    .alias_to_primary
                    .insert((*alias).to_owned(), (*primary).to_owned());
                aliases
                    .alias_to_primary
                    .insert(format_channel_name(alias), (*primary).to_owned());
            }
        }
        aliases
    }

    pub(crate) fn primary_name(&self, name: &str) -> String {
        let trimmed = name.trim();
        if let Some(primary) = self.alias_to_primary.get(trimmed) {
            return primary.clone();
        }
        for (pattern, primary) in &self.pattern_to_primary {
            if pattern.is_match(trimmed).unwrap_or(false) {
                return primary.clone();
            }
        }
        let formatted = format_channel_name(trimmed);
        self.alias_to_primary
            .get(&formatted)
            .cloned()
            .unwrap_or_else(|| trimmed.to_owned())
    }

    pub(crate) fn normalize_filter_set(&self, names: &HashSet<String>) -> HashSet<String> {
        names.iter().map(|name| self.primary_name(name)).collect()
    }
}

static T2S_CONVERTER: OnceLock<Option<OpenCC>> = OnceLock::new();

fn convert_t2s(value: &str) -> String {
    let converter = T2S_CONVERTER.get_or_init(|| match OpenCC::from_config(BuiltinConfig::T2s) {
        Ok(converter) => Some(converter),
        Err(error) => {
            tracing::warn!(error = %error, "Failed to initialize OpenCC T2S converter");
            None
        }
    });
    converter
        .as_ref()
        .map(|converter| converter.convert(value))
        .unwrap_or_else(|| value.to_owned())
}

fn format_channel_name(name: &str) -> String {
    let converted = convert_t2s(name);
    let remove_re = Regex::new(r"-|_|\((.*?)\)|（(.*?)）|\[(.*?)]|「(.*?)」| |｜|频道|普清|标清|高清|HD|hd|超清|超高|超高清|4K|4k|中央|央视|电视台|台|电信|联通|移动")
        .expect("channel normalization regex is valid");
    let mut formatted = remove_re.replace_all(converted.trim(), "").to_string();
    for (old, new) in [("plus", "+"), ("PLUS", "+"), ("＋", "+")] {
        formatted = formatted.replace(old, new);
    }
    formatted.to_lowercase()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn epg_config_defaults_to_enabled_for_python_open_epg() {
        assert!(EpgConfig::default().enabled);
    }

    #[test]
    fn parses_xmltv_with_timezone_and_filters_names() {
        let xml = r#"<tv>
            <channel id="cctv1"><display-name>CCTV-1</display-name></channel>
            <programme channel="cctv1" start="20990101000000 +0800" stop="20990101003000 +0800">
                <title lang="zh">新聞與天氣</title>
            </programme>
        </tv>"#;
        let names = Arc::new(HashSet::from(["CCTV-1".to_owned()]));
        let parsed = parse_epg(xml, Some(names.as_ref()), &ChannelAliases::default()).unwrap();
        assert_eq!(parsed.channels["cctv1"], "CCTV-1");
        assert_eq!(parsed.programmes["cctv1"][0].title, "新闻与天气");
    }

    #[test]
    fn aliases_map_epg_display_name_to_primary_name() {
        let dir = tempfile::tempdir().unwrap();
        let alias_path = dir.path().join("alias.txt");
        fs::write(
            &alias_path,
            "CCTV-1,re:(?i)^\\s*CCTV[-\\s_]*0?1(?![0-9Kk+])[\\s\\S]*$,CCTV1,CCTV-01高清
",
        )
        .unwrap();
        let aliases = ChannelAliases::load(alias_path.to_str().unwrap());
        assert_eq!(aliases.primary_name("CCTV-01高清"), "CCTV-1");
        assert_eq!(aliases.primary_name("cctv 1 HD"), "CCTV-1");
        let filter = HashSet::from(["CCTV-1".to_owned()]);
        let xml = r#"<tv>
            <channel id="one"><display-name>CCTV-01高清</display-name></channel>
            <programme channel="one" start="20990101000000 +0800" stop="20990101003000 +0800">
                <title lang="zh">新闻</title>
            </programme>
        </tv>"#;
        let parsed = parse_epg(xml, Some(&filter), &aliases).unwrap();
        assert_eq!(parsed.channels["one"], "CCTV-1");
        assert_eq!(parsed.programmes["one"][0].title, "新闻");
    }

    #[test]
    fn writes_xml_and_gzip() {
        let dir = tempfile::tempdir().unwrap();
        let xml_path = dir.path().join("epg.xml");
        let gz_path = dir.path().join("epg.gz");
        let mut epg = HashMap::new();
        epg.insert(
            "CCTV-1".to_owned(),
            vec![EpgProgramme {
                start: parse_xmltv_datetime("20990101000000 +0800").unwrap(),
                stop: parse_xmltv_datetime("20990101003000 +0800").unwrap(),
                title: "新闻 & 天气".to_owned(),
            }],
        );
        write_outputs(&epg, xml_path.to_str().unwrap(), gz_path.to_str().unwrap()).unwrap();
        let output = fs::read_to_string(xml_path).unwrap();
        assert!(output.contains("新闻 &amp; 天气"));
        assert!(gz_path.exists());
    }
}
