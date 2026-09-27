use crate::{engine::fetcher::Fetcher, epg::ChannelAliases, models::SubscribeConfig};
use anyhow::{Context, Result};
use futures::{StreamExt, stream};
use regex::Regex;
use reqwest::Proxy;
use std::{
    collections::{HashMap, HashSet},
    fs,
    path::Path,
    sync::Arc,
    time::Duration,
};

#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct SubscribeStatus {
    pub configured_urls: usize,
    pub whitelist_urls: usize,
    pub fetched_urls: usize,
    pub failed_urls: usize,
    pub channels: usize,
    pub whitelist_channels: usize,
    pub header_channels: usize,
    pub extra_info_channels: usize,
    pub nomatch_channels: usize,
}

fn http_proxy_from_config(value: &str) -> Option<Proxy> {
    let value = value.trim();
    if value.is_empty() {
        return None;
    }
    Proxy::all(value).ok()
}

pub async fn fetch_channels(
    config: &SubscribeConfig,
    fetcher: &Fetcher,
    names: Option<&HashSet<String>>,
) -> Result<(Vec<crate::models::Channel>, SubscribeStatus)> {
    let urls = read_subscribe_urls(&config.sources_path, &config.cdn_url)?;
    if urls.is_empty() {
        return Ok((Vec::new(), SubscribeStatus::default()));
    }

    let mut client_builder = reqwest::Client::builder()
        .use_rustls_tls()
        .timeout(Duration::from_millis(config.timeout_ms))
        .user_agent("iptvapi-rs/0.0.2");
    if let Some(proxy) = http_proxy_from_config(&config.http_proxy) {
        client_builder = client_builder.proxy(proxy);
    }
    let client = client_builder.build()?;
    let concurrency = config.concurrency.max(1);
    let configured_urls = urls.len();
    let whitelist_urls = urls.iter().filter(|source| source.is_whitelist).count();
    let aliases = crate::epg::aliases_for_path(&config.alias_path);
    let name_filter = names
        .filter(|names| !names.is_empty())
        .map(|names| aliases.normalize_filter_set(names))
        .map(Arc::new);

    let results =
        stream::iter(
            urls.into_iter().map(|source| {
                let client = client.clone();
                let aliases = aliases.clone();
                let name_filter = name_filter.clone();
                async move {
                    fetch_one(&client, fetcher, source, name_filter.as_deref(), &aliases).await
                }
            }),
        )
        .buffer_unordered(concurrency)
        .collect::<Vec<_>>()
        .await;

    let mut channels = Vec::new();
    let mut fetched_urls = 0;
    let mut failed_urls = 0;
    let mut whitelist_channels = 0;
    let mut header_channels = 0;
    let mut extra_info_channels = 0;
    let mut nomatch_channels = 0;
    let mut nomatch_entries = Vec::new();
    for result in results {
        match result {
            Ok((mut source_channels, is_whitelist, source_stats)) => {
                fetched_urls += 1;
                header_channels += source_stats.header_channels;
                extra_info_channels += source_stats.extra_info_channels;
                nomatch_channels += source_stats.nomatch_channels;
                nomatch_entries.extend(source_stats.nomatch_entries);
                if is_whitelist {
                    whitelist_channels += source_channels.len();
                }
                channels.append(&mut source_channels);
            }
            Err(error) => {
                failed_urls += 1;
                metrics::counter!("iptvapi_subscribe_source_errors_total").increment(1);
                tracing::warn!(error = %error, "Failed to fetch subscribe source");
                crate::error_log::push("subscribe", error.to_string()).await;
            }
        }
    }

    write_nomatch_log(&config.nomatch_log_path, &nomatch_entries)?;

    let status = SubscribeStatus {
        configured_urls,
        whitelist_urls,
        fetched_urls,
        failed_urls,
        channels: channels.len(),
        whitelist_channels,
        header_channels,
        extra_info_channels,
        nomatch_channels,
    };
    metrics::gauge!("iptvapi_subscribe_sources").set(configured_urls as f64);
    metrics::gauge!("iptvapi_subscribe_channels").set(channels.len() as f64);
    metrics::gauge!("iptvapi_subscribe_header_channels").set(header_channels as f64);
    metrics::gauge!("iptvapi_subscribe_extra_info_channels").set(extra_info_channels as f64);
    metrics::gauge!("iptvapi_subscribe_nomatch_channels").set(nomatch_channels as f64);
    Ok((channels, status))
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SubscribeSource {
    url: String,
    is_whitelist: bool,
}

fn read_subscribe_urls(path: &str, cdn_url: &str) -> Result<Vec<SubscribeSource>> {
    let content = match fs::read_to_string(path) {
        Ok(content) => content,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => {
            return Err(error).with_context(|| format!("read subscribe source list: {path}"));
        }
    };
    let url_re = Regex::new(r"(?i)https?://\S+")?;
    let mut seen = HashSet::new();
    let mut urls = Vec::new();
    let mut in_whitelist = false;
    for line in content.lines() {
        let line = line.trim();
        if line.eq_ignore_ascii_case("[WHITELIST]") {
            in_whitelist = true;
            continue;
        }
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some(found) = url_re.find(line) {
            let mut url = found.as_str().trim_end_matches(',').to_owned();
            if !cdn_url.trim().is_empty() && url.contains("raw.githubusercontent.com") {
                url = join_url(cdn_url, &url);
            }
            if seen.insert(url.clone()) {
                urls.push(SubscribeSource {
                    url,
                    is_whitelist: in_whitelist,
                });
            }
        }
    }
    urls.sort_by_key(|source| !source.is_whitelist);
    Ok(urls)
}

fn join_url(prefix: &str, url: &str) -> String {
    format!(
        "{}/{}",
        prefix.trim_end_matches('/'),
        url.trim_start_matches('/')
    )
}

#[derive(Debug, Clone, Default)]
struct SubscribeSourceStats {
    header_channels: usize,
    extra_info_channels: usize,
    nomatch_channels: usize,
    nomatch_entries: Vec<String>,
}

async fn fetch_one(
    client: &reqwest::Client,
    fetcher: &Fetcher,
    source: SubscribeSource,
    name_filter: Option<&HashSet<String>>,
    aliases: &ChannelAliases,
) -> Result<(Vec<crate::models::Channel>, bool, SubscribeSourceStats)> {
    let url = source.url;
    tracing::info!(url = %url, whitelist = source.is_whitelist, "Fetching subscribe source");
    let content = client
        .get(&url)
        .send()
        .await
        .with_context(|| format!("request subscribe source {url}"))?
        .error_for_status()
        .with_context(|| format!("bad subscribe source status {url}"))?
        .text()
        .await
        .with_context(|| format!("read subscribe source body {url}"))?;
    let (mut channels, stats) = if content.contains("#EXTM3U") {
        parse_m3u_with_metadata(fetcher, &content, name_filter, aliases)?
    } else {
        parse_txt_with_extra_info(fetcher, &content, name_filter, aliases)?
    };
    for channel in &mut channels {
        channel.origin = if source.is_whitelist {
            crate::models::ChannelOrigin::Whitelist
        } else {
            crate::models::ChannelOrigin::Subscribe
        };
    }
    if source.is_whitelist {
        for channel in &mut channels {
            channel.is_online = true;
            channel.latency = Some(0);
        }
    }
    Ok((channels, source.is_whitelist, stats))
}

fn parse_m3u_with_metadata(
    fetcher: &Fetcher,
    content: &str,
    name_filter: Option<&HashSet<String>>,
    aliases: &ChannelAliases,
) -> Result<(Vec<crate::models::Channel>, SubscribeSourceStats)> {
    let mut channels = fetcher.parse_m3u(content)?;
    let mut stats = SubscribeSourceStats::default();
    let mut pending_headers: Option<HashMap<String, String>> = None;
    let mut pending_catchup: Option<HashMap<String, String>> = None;
    let mut channel_index = 0;

    for line in content.lines().map(str::trim) {
        if line.starts_with("#EXTINF") {
            merge_headers(&mut pending_headers, extract_headers(line));
            merge_headers(&mut pending_catchup, extract_catchup(line));
            continue;
        }
        if line.starts_with("#EXTVLCOPT") {
            merge_headers(&mut pending_headers, extract_headers(line));
            continue;
        }
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some(channel) = channels.get_mut(channel_index) {
            let (url, extra_info) = split_extra_info(&channel.url);
            if extra_info.is_some() {
                stats.extra_info_channels += 1;
            }
            if let Some(headers) = pending_headers.take()
                && !headers.is_empty()
            {
                stats.header_channels += 1;
                channel.headers = Some(headers);
            }
            if let Some(catchup) = pending_catchup.take()
                && !catchup.is_empty()
            {
                channel.catchup = Some(catchup);
            }
            channel.extra_info = extra_info.map(str::to_owned);
            channel.url = rebuild_url(url, extra_info);
            channel_index += 1;
            pending_headers = None;
            pending_catchup = None;
        }
    }
    let channels = filter_and_alias_channels(channels, name_filter, aliases, &mut stats);
    Ok((channels, stats))
}

fn parse_txt_with_extra_info(
    fetcher: &Fetcher,
    content: &str,
    name_filter: Option<&HashSet<String>>,
    aliases: &ChannelAliases,
) -> Result<(Vec<crate::models::Channel>, SubscribeSourceStats)> {
    let mut channels = fetcher.parse_txt(content)?;
    let mut stats = SubscribeSourceStats::default();
    for channel in &mut channels {
        let (url, extra_info) = split_extra_info(&channel.url);
        if extra_info.is_some() {
            stats.extra_info_channels += 1;
        }
        channel.extra_info = extra_info.map(str::to_owned);
        channel.url = rebuild_url(url, extra_info);
    }
    let channels = filter_and_alias_channels(channels, name_filter, aliases, &mut stats);
    Ok((channels, stats))
}

fn filter_and_alias_channels(
    channels: Vec<crate::models::Channel>,
    name_filter: Option<&HashSet<String>>,
    aliases: &ChannelAliases,
    stats: &mut SubscribeSourceStats,
) -> Vec<crate::models::Channel> {
    channels
        .into_iter()
        .filter_map(|mut channel| {
            let original_name = channel.name.trim().to_owned();
            let primary_name = aliases.primary_name(&original_name);
            if let Some(names) = name_filter
                && !names.contains(&primary_name)
            {
                stats.nomatch_channels += 1;
                stats
                    .nomatch_entries
                    .push(format!("{},{}", original_name, channel.url));
                return None;
            }
            channel.name = primary_name;
            Some(channel)
        })
        .collect()
}

fn write_nomatch_log(path: &str, entries: &[String]) -> Result<()> {
    if let Some(parent) = Path::new(path)
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent)
            .with_context(|| format!("create subscribe nomatch log dir: {}", parent.display()))?;
    }
    let content = if entries.is_empty() {
        String::new()
    } else {
        format!("{}\n", entries.join("\n"))
    };
    fs::write(path, content).with_context(|| format!("write subscribe nomatch log: {path}"))
}

fn merge_headers(
    target: &mut Option<HashMap<String, String>>,
    headers: Option<HashMap<String, String>>,
) {
    let Some(headers) = headers else {
        return;
    };
    target.get_or_insert_with(HashMap::new).extend(headers);
}

fn extract_headers(line: &str) -> Option<HashMap<String, String>> {
    let mut headers = HashMap::new();
    for captures in Regex::new(
        r#"(?i)(?:http-)?(?P<key>user-agent|useragent|referer|referrer|origin)=(?P<value>\S+)"#,
    )
    .expect("header regex is valid")
    .captures_iter(line)
    {
        let key = captures
            .name("key")
            .map(|value| value.as_str().to_ascii_lowercase().replace('-', ""))?;
        let value = captures
            .name("value")
            .map(|value| value.as_str().trim_matches('"').to_owned())?;
        let key = match key.as_str() {
            "useragent" => "User-Agent",
            "referer" | "referrer" => "Referer",
            "origin" => "Origin",
            _ => continue,
        };
        if !value.is_empty() {
            headers.insert(key.to_owned(), value);
        }
    }
    (!headers.is_empty()).then_some(headers)
}

fn extract_catchup(line: &str) -> Option<HashMap<String, String>> {
    let mut catchup = HashMap::new();
    for captures in
        Regex::new(r#"(?i)(?P<key>catchup|catchup-source|catchupsource)=(?P<value>"[^"]*"|\S+)"#)
            .expect("catchup regex is valid")
            .captures_iter(line)
    {
        let key = captures.name("key")?.as_str().to_ascii_lowercase();
        let value = captures
            .name("value")
            .map(|value| value.as_str().trim_matches('"').to_owned())?;
        let key = match key.as_str() {
            "catchup" => "catchup",
            "catchup-source" | "catchupsource" => "catchup-source",
            _ => continue,
        };
        if !value.is_empty() {
            catchup.insert(key.to_owned(), value);
        }
    }
    (!catchup.is_empty()).then_some(catchup)
}

fn split_extra_info(url: &str) -> (&str, Option<&str>) {
    let Some((base, extra)) = url.split_once('$') else {
        return (url, None);
    };
    (base, (!extra.is_empty()).then_some(extra))
}

fn rebuild_url(base: &str, extra_info: Option<&str>) -> String {
    match extra_info {
        Some(extra_info) => format!("{base}${extra_info}"),
        None => base.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subscribe_config_defaults_to_enabled_for_python_open_subscribe() {
        assert!(SubscribeConfig::default().enabled);
    }

    #[test]
    fn reads_subscribe_urls_with_whitelist_first() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("subscribe.txt");
        fs::write(
            &path,
            "# comment\nhttp://example.com/a.m3u\n[WHITELIST]\nhttp://example.com/w.m3u\nhttp://example.com/a.m3u\n",
        )
        .unwrap();
        let urls = read_subscribe_urls(path.to_str().unwrap(), "").unwrap();
        assert_eq!(urls.len(), 2);
        assert_eq!(urls[0].url, "http://example.com/w.m3u");
        assert!(urls[0].is_whitelist);
        assert_eq!(urls[1].url, "http://example.com/a.m3u");
        assert!(!urls[1].is_whitelist);
    }

    #[test]
    fn parses_m3u_headers_and_extra_info_counts() {
        let fetcher = Fetcher::new(10, "");
        let content = r#"#EXTM3U
#EXTINF:-1 http-user-agent=UA catchup="default" catchup-source="https://replay.example/{utc}" group-title="Test",Name
#EXTVLCOPT:http-referrer=https://example.com/
http://stream.example/live.m3u8$source
"#;
        let (channels, stats) =
            parse_m3u_with_metadata(&fetcher, content, None, &ChannelAliases::default()).unwrap();
        assert_eq!(channels.len(), 1);
        assert_eq!(channels[0].url, "http://stream.example/live.m3u8$source");
        assert_eq!(channels[0].headers.as_ref().unwrap()["User-Agent"], "UA");
        assert_eq!(
            channels[0].headers.as_ref().unwrap()["Referer"],
            "https://example.com/"
        );
        assert_eq!(channels[0].extra_info.as_deref(), Some("source"));
        assert_eq!(channels[0].catchup.as_ref().unwrap()["catchup"], "default");
        assert_eq!(
            channels[0].catchup.as_ref().unwrap()["catchup-source"],
            "https://replay.example/{utc}"
        );
        assert_eq!(stats.header_channels, 1);
        assert_eq!(stats.extra_info_channels, 1);
    }

    #[test]
    fn aliases_and_filters_subscribe_channels() {
        let dir = tempfile::tempdir().unwrap();
        let alias_path = dir.path().join("alias.txt");
        fs::write(
            &alias_path,
            "CCTV-1,CCTV1,CCTV-01高清
",
        )
        .unwrap();
        let aliases = ChannelAliases::load(alias_path.to_str().unwrap());
        let names = HashSet::from(["CCTV-1".to_owned()]);
        let fetcher = Fetcher::new(10, "");
        let content = r#"#EXTM3U
#EXTINF:-1 group-title="Test",CCTV-01高清
http://stream.example/cctv1.m3u8
#EXTINF:-1 group-title="Test",Other Channel
http://stream.example/other.m3u8
"#;

        let (channels, stats) =
            parse_m3u_with_metadata(&fetcher, content, Some(&names), &aliases).unwrap();

        assert_eq!(channels.len(), 1);
        assert_eq!(channels[0].name, "CCTV-1");
        assert_eq!(channels[0].url, "http://stream.example/cctv1.m3u8");
        assert_eq!(stats.nomatch_channels, 1);
        assert_eq!(
            stats.nomatch_entries,
            vec!["Other Channel,http://stream.example/other.m3u8".to_owned()]
        );
    }

    #[test]
    fn writes_nomatch_log_with_parent_directory() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("log/nomatch.log");
        write_nomatch_log(
            path.to_str().unwrap(),
            &["Other,http://stream.example/other.m3u8".to_owned()],
        )
        .unwrap();
        assert_eq!(
            fs::read_to_string(&path).unwrap(),
            "Other,http://stream.example/other.m3u8
"
        );

        write_nomatch_log(path.to_str().unwrap(), &[]).unwrap();
        assert_eq!(fs::read_to_string(path).unwrap(), "");
    }
}
