pub mod checker;
pub mod fetcher;
pub mod filter;

use crate::engine::checker::Checker;
use crate::engine::fetcher::Fetcher;
use crate::engine::filter::{Blacklist, Whitelist};
use crate::epg::EpgStatus;
use crate::models::{AppConfig, Channel, ChannelOrigin, EngineConfig, FilterConfig, OutputConfig};
use anyhow::Result;
use chrono::{DateTime, Local, TimeZone, Timelike, Utc};
use dashmap::DashMap;
use futures::future::join_all;
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    env, fs,
    hash::{Hash, Hasher},
    io,
    net::{SocketAddr, TcpStream, ToSocketAddrs},
    path::Path,
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};

#[derive(Debug, Clone, serde::Serialize)]
pub struct EngineStatus {
    pub state: EngineRunState,
    pub last_started_at: Option<DateTime<Utc>>,
    pub last_finished_at: Option<DateTime<Utc>>,
    pub last_duration_ms: Option<u128>,
    pub last_error: Option<String>,
    pub fetched_channels: usize,
    pub filtered_channels: usize,
    pub online_groups: usize,
    pub online_channels: usize,
    pub subscribe_sources: usize,
    pub subscribe_failed_sources: usize,
    pub subscribe_channels: usize,
    pub subscribe_whitelist_sources: usize,
    pub subscribe_whitelist_channels: usize,
    pub subscribe_header_channels: usize,
    pub subscribe_extra_info_channels: usize,
    pub subscribe_nomatch_channels: usize,
}

#[derive(Debug, Clone, Copy)]
struct EngineSuccessSnapshot {
    started: Instant,
    fetched_channels: usize,
    filtered_channels: usize,
    online_groups: usize,
    online_channels: usize,
    subscribe_sources: usize,
    subscribe_failed_sources: usize,
    subscribe_channels: usize,
    subscribe_whitelist_sources: usize,
    subscribe_whitelist_channels: usize,
    subscribe_header_channels: usize,
    subscribe_extra_info_channels: usize,
    subscribe_nomatch_channels: usize,
}

impl Default for EngineStatus {
    fn default() -> Self {
        Self {
            state: EngineRunState::Idle,
            last_started_at: None,
            last_finished_at: None,
            last_duration_ms: None,
            last_error: None,
            fetched_channels: 0,
            filtered_channels: 0,
            online_groups: 0,
            online_channels: 0,
            subscribe_sources: 0,
            subscribe_failed_sources: 0,
            subscribe_channels: 0,
            subscribe_whitelist_sources: 0,
            subscribe_whitelist_channels: 0,
            subscribe_header_channels: 0,
            subscribe_extra_info_channels: 0,
            subscribe_nomatch_channels: 0,
        }
    }
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EngineRunState {
    Idle,
    Running,
    Success,
    Error,
}

impl EngineStatus {
    fn start(&mut self) {
        self.state = EngineRunState::Running;
        self.last_started_at = Some(Utc::now());
        self.last_finished_at = None;
        self.last_duration_ms = None;
        self.last_error = None;
    }

    fn finish_success(&mut self, snapshot: EngineSuccessSnapshot) {
        self.state = EngineRunState::Success;
        self.last_finished_at = Some(Utc::now());
        self.last_duration_ms = Some(snapshot.started.elapsed().as_millis());
        self.last_error = None;
        self.fetched_channels = snapshot.fetched_channels;
        self.filtered_channels = snapshot.filtered_channels;
        self.online_groups = snapshot.online_groups;
        self.online_channels = snapshot.online_channels;
        self.subscribe_sources = snapshot.subscribe_sources;
        self.subscribe_failed_sources = snapshot.subscribe_failed_sources;
        self.subscribe_channels = snapshot.subscribe_channels;
        self.subscribe_whitelist_sources = snapshot.subscribe_whitelist_sources;
        self.subscribe_whitelist_channels = snapshot.subscribe_whitelist_channels;
        self.subscribe_header_channels = snapshot.subscribe_header_channels;
        self.subscribe_extra_info_channels = snapshot.subscribe_extra_info_channels;
        self.subscribe_nomatch_channels = snapshot.subscribe_nomatch_channels;
    }

    fn finish_error(&mut self, started: Instant, error: String) {
        self.state = EngineRunState::Error;
        self.last_finished_at = Some(Utc::now());
        self.last_duration_ms = Some(started.elapsed().as_millis());
        self.last_error = Some(error);
    }
}

pub struct Engine {
    config: AppConfig,
    channels: Arc<DashMap<String, Vec<Channel>>>,
    fetcher: Fetcher,
    epg_status: Arc<RwLock<EpgStatus>>,
    engine_status: Arc<RwLock<EngineStatus>>,
}

fn write_playlist_outputs(
    output: &OutputConfig,
    hls_enabled: bool,
    channels: &DashMap<String, Vec<Channel>>,
    ip_filter: Option<IpFilter>,
    no_result_names: &[String],
) -> io::Result<()> {
    let txt = playlist_txt(output, channels, ip_filter, no_result_names);
    atomic_write(&output.result_txt_path, &txt)?;
    if output.m3u_result_enabled {
        let m3u = playlist_m3u(output, channels, ip_filter, None, no_result_names);
        atomic_write(&output.result_m3u_path, &m3u)?;
    }

    let ipv4_txt = playlist_txt(output, channels, Some(IpFilter::Ipv4), no_result_names);
    atomic_write(&output.ipv4_result_txt_path, &ipv4_txt)?;
    if output.m3u_result_enabled {
        let ipv4_m3u = playlist_m3u(
            output,
            channels,
            Some(IpFilter::Ipv4),
            None,
            no_result_names,
        );
        atomic_write(&output.ipv4_result_m3u_path, &ipv4_m3u)?;
    }

    let ipv6_txt = playlist_txt(output, channels, Some(IpFilter::Ipv6), no_result_names);
    atomic_write(&output.ipv6_result_txt_path, &ipv6_txt)?;
    if output.m3u_result_enabled {
        let ipv6_m3u = playlist_m3u(
            output,
            channels,
            Some(IpFilter::Ipv6),
            None,
            no_result_names,
        );
        atomic_write(&output.ipv6_result_m3u_path, &ipv6_m3u)?;
    }

    if hls_enabled {
        let hls_base_url = format!("{}/hls", output_public_base(output).trim_end_matches('/'));
        write_hls_playlist_outputs(output, channels, no_result_names, &hls_base_url)?;
    }
    Ok(())
}

fn write_hls_playlist_outputs(
    config: &OutputConfig,
    channels: &DashMap<String, Vec<Channel>>,
    no_result_names: &[String],
    hls_base_url: &str,
) -> io::Result<()> {
    let txt = hls_playlist_txt(config, channels, None, no_result_names, hls_base_url);
    atomic_write(&config.hls_result_txt_path, &txt)?;
    if config.m3u_result_enabled {
        let m3u = hls_playlist_m3u(config, channels, None, no_result_names, hls_base_url);
        atomic_write(&config.hls_result_m3u_path, &m3u)?;
    }

    let ipv4_txt = hls_playlist_txt(
        config,
        channels,
        Some(IpFilter::Ipv4),
        no_result_names,
        hls_base_url,
    );
    atomic_write(&config.hls_ipv4_result_txt_path, &ipv4_txt)?;
    if config.m3u_result_enabled {
        let ipv4_m3u = hls_playlist_m3u(
            config,
            channels,
            Some(IpFilter::Ipv4),
            no_result_names,
            hls_base_url,
        );
        atomic_write(&config.hls_ipv4_result_m3u_path, &ipv4_m3u)?;
    }

    let ipv6_txt = hls_playlist_txt(
        config,
        channels,
        Some(IpFilter::Ipv6),
        no_result_names,
        hls_base_url,
    );
    atomic_write(&config.hls_ipv6_result_txt_path, &ipv6_txt)?;
    if config.m3u_result_enabled {
        let ipv6_m3u = hls_playlist_m3u(
            config,
            channels,
            Some(IpFilter::Ipv6),
            no_result_names,
            hls_base_url,
        );
        atomic_write(&config.hls_ipv6_result_m3u_path, &ipv6_m3u)?;
    }

    write_rtmp_result_data(config, channels)?;
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IpFilter {
    Ipv4,
    Ipv6,
}

fn playlist_txt(
    config: &OutputConfig,
    channels: &DashMap<String, Vec<Channel>>,
    ip_filter: Option<IpFilter>,
    no_result_names: &[String],
) -> String {
    let mut groups = sorted_channel_groups(channels, ip_filter);
    let mut txt = String::new();
    for (group, channels) in groups.drain(..) {
        txt.push_str(&format!("{group},#genre#\n"));
        for channel in select_output_channels(config, channels) {
            txt.push_str(&format!(
                "{},{}\n",
                channel.name,
                channel_url_with_extra_info(config, &channel)
            ));
        }
        txt.push('\n');
    }
    append_no_result_txt(config, &mut txt, no_result_names);
    with_update_time_txt(config, channels, ip_filter, txt)
}

fn playlist_m3u(
    config: &OutputConfig,
    channels: &DashMap<String, Vec<Channel>>,
    ip_filter: Option<IpFilter>,
    request_base_url: Option<&str>,
    no_result_names: &[String],
) -> String {
    let mut m3u = String::from("#EXTM3U\n");
    let mut name_ids = HashMap::new();
    let mut next_id = 1usize;
    if config.update_time_enabled
        && update_time_is_top(config)
        && let Some((name, url)) = first_channel_for_update_time(config, channels, ip_filter)
    {
        m3u.push_str(&format!(
            "#EXTINF:-1 group-title=\"更新时间\",{}
{}
",
            update_time_label(config, &name),
            url
        ));
    }
    for (group, channels) in sorted_channel_groups(channels, ip_filter) {
        for channel in select_output_channels(config, channels) {
            let logo = channel_logo(config, &channel, request_base_url);
            let tvg_name = crate::playlist::m3u_tvg_name(config, &channel.name);
            let tvg_id = crate::playlist::m3u_tvg_id(&mut name_ids, &mut next_id, &tvg_name);
            m3u.push_str(&m3u_extinf_line(
                tvg_id,
                &tvg_name,
                &logo,
                &group,
                &channel.name,
                channel.catchup.as_ref(),
            ));
            if config.open_headers
                && let Some(headers) = &channel.headers
            {
                let mut headers = headers.iter().collect::<Vec<_>>();
                headers.sort_by(|left, right| left.0.cmp(right.0));
                for (key, value) in headers {
                    m3u.push_str(&format!(
                        "#EXTVLCOPT:http-{}={}\n",
                        key.to_ascii_lowercase(),
                        value
                    ));
                }
            }
            m3u.push_str(&format!(
                "{}\n",
                channel_url_with_extra_info(config, &channel)
            ));
        }
    }
    append_no_result_m3u(config, &mut m3u, no_result_names);
    if config.update_time_enabled
        && !update_time_is_top(config)
        && let Some((name, url)) = first_channel_for_update_time(config, channels, ip_filter)
    {
        m3u.push_str(&format!(
            "
#EXTINF:-1 group-title=\"更新时间\",{}
{}
",
            update_time_label(config, &name),
            url
        ));
    }
    m3u
}

fn hls_playlist_txt(
    config: &OutputConfig,
    channels: &DashMap<String, Vec<Channel>>,
    ip_filter: Option<IpFilter>,
    no_result_names: &[String],
    hls_base_url: &str,
) -> String {
    let mut groups = sorted_channel_groups(channels, ip_filter);
    let mut txt = String::new();
    for (group, channels) in groups.drain(..) {
        txt.push_str(&format!("{group},#genre#\n"));
        for channel in select_output_channels(config, channels) {
            txt.push_str(&format!(
                "{},{}\n",
                channel.name,
                hls_proxy_url(hls_base_url, &channel)
            ));
        }
        txt.push('\n');
    }
    append_no_result_txt(config, &mut txt, no_result_names);
    with_hls_update_time_txt(config, channels, ip_filter, txt, hls_base_url)
}

fn hls_playlist_m3u(
    config: &OutputConfig,
    channels: &DashMap<String, Vec<Channel>>,
    ip_filter: Option<IpFilter>,
    no_result_names: &[String],
    hls_base_url: &str,
) -> String {
    let mut m3u = String::from("#EXTM3U\n");
    let mut name_ids = HashMap::new();
    let mut next_id = 1usize;
    if config.update_time_enabled
        && update_time_is_top(config)
        && let Some((name, url)) =
            first_hls_channel_for_update_time(config, channels, ip_filter, hls_base_url)
    {
        m3u.push_str(&format!(
            "#EXTINF:-1 group-title=\"更新时间\",{}
{}
",
            update_time_label(config, &name),
            url
        ));
    }
    for (group, channels) in sorted_channel_groups(channels, ip_filter) {
        for channel in select_output_channels(config, channels) {
            let logo = channel_logo(config, &channel, Some(output_public_base(config).as_str()));
            let tvg_name = crate::playlist::m3u_tvg_name(config, &channel.name);
            let tvg_id = crate::playlist::m3u_tvg_id(&mut name_ids, &mut next_id, &tvg_name);
            m3u.push_str(&m3u_extinf_line(
                tvg_id,
                &tvg_name,
                &logo,
                &group,
                &channel.name,
                channel.catchup.as_ref(),
            ));
            if config.open_headers
                && let Some(headers) = &channel.headers
            {
                let mut headers = headers.iter().collect::<Vec<_>>();
                headers.sort_by(|left, right| left.0.cmp(right.0));
                for (key, value) in headers {
                    m3u.push_str(&format!(
                        "#EXTVLCOPT:http-{}={}\n",
                        key.to_ascii_lowercase(),
                        value
                    ));
                }
            }
            m3u.push_str(&format!("{}\n", hls_proxy_url(hls_base_url, &channel)));
        }
    }
    append_no_result_m3u(config, &mut m3u, no_result_names);
    if config.update_time_enabled
        && !update_time_is_top(config)
        && let Some((name, url)) =
            first_hls_channel_for_update_time(config, channels, ip_filter, hls_base_url)
    {
        m3u.push_str(&format!(
            "
#EXTINF:-1 group-title=\"更新时间\",{}
{}
",
            update_time_label(config, &name),
            url
        ));
    }
    m3u
}

fn m3u_extinf_line(
    tvg_id: usize,
    tvg_name: &str,
    logo: &str,
    group: &str,
    channel_name: &str,
    catchup: Option<&HashMap<String, String>>,
) -> String {
    let mut line = format!(
        "#EXTINF:-1 tvg-id=\"{tvg_id}\" tvg-name=\"{tvg_name}\" tvg-logo=\"{logo}\" group-title=\"{group}\""
    );
    if let Some(catchup) = catchup {
        let mut items = catchup.iter().collect::<Vec<_>>();
        items.sort_by(|left, right| left.0.cmp(right.0));
        for (key, value) in items {
            if !value.is_empty() {
                line.push_str(&format!(" {key}=\"{value}\""));
            }
        }
    }
    line.push_str(&format!(",{channel_name}\n"));
    line
}

fn append_no_result_txt(config: &OutputConfig, txt: &mut String, no_result_names: &[String]) {
    if !config.open_empty_category || no_result_names.is_empty() {
        return;
    }
    if !txt.is_empty() && !txt.ends_with("\n\n") {
        txt.push('\n');
    }
    txt.push_str("🈳无结果频道,#genre#\n");
    for name in no_result_names {
        txt.push_str(&format!("{name},url\n"));
    }
}

fn append_no_result_m3u(config: &OutputConfig, m3u: &mut String, no_result_names: &[String]) {
    if !config.open_empty_category || no_result_names.is_empty() {
        return;
    }
    for name in no_result_names {
        m3u.push_str(&format!(
            "#EXTINF:-1 group-title=\"🈳无结果频道\",{name}\nurl\n"
        ));
    }
}

fn no_result_names(
    requested_names: &[String],
    channels: &DashMap<String, Vec<Channel>>,
) -> Vec<String> {
    let online_names = channels
        .iter()
        .flat_map(|entry| {
            entry
                .value()
                .iter()
                .map(|channel| channel.name.clone())
                .collect::<Vec<_>>()
        })
        .collect::<HashSet<_>>();
    requested_names
        .iter()
        .filter(|name| !online_names.contains(*name))
        .cloned()
        .collect()
}

fn requested_channel_names(channels: &[Channel]) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut names = Vec::new();
    for channel in channels {
        let name = channel.name.trim();
        if !name.is_empty() && seen.insert(name.to_owned()) {
            names.push(name.to_owned());
        }
    }
    names
}

fn with_update_time_txt(
    config: &OutputConfig,
    channels: &DashMap<String, Vec<Channel>>,
    ip_filter: Option<IpFilter>,
    content: String,
) -> String {
    if !config.update_time_enabled {
        return content;
    }
    let Some((name, url)) = first_channel_for_update_time(config, channels, ip_filter) else {
        return content;
    };
    let entry = format!(
        "更新时间,#genre#
{},{}

",
        update_time_label(config, &name),
        url
    );
    if update_time_is_top(config) {
        format!("{entry}{content}")
    } else {
        format!(
            "{content}
{entry}"
        )
    }
}

fn with_hls_update_time_txt(
    config: &OutputConfig,
    channels: &DashMap<String, Vec<Channel>>,
    ip_filter: Option<IpFilter>,
    content: String,
    hls_base_url: &str,
) -> String {
    if !config.update_time_enabled {
        return content;
    }
    let Some((name, url)) =
        first_hls_channel_for_update_time(config, channels, ip_filter, hls_base_url)
    else {
        return content;
    };
    let entry = format!(
        "更新时间,#genre#
{},{}

",
        update_time_label(config, &name),
        url
    );
    if update_time_is_top(config) {
        format!("{entry}{content}")
    } else {
        format!(
            "{content}
{entry}"
        )
    }
}

fn update_time_is_top(config: &OutputConfig) -> bool {
    !config.update_time_position.eq_ignore_ascii_case("bottom")
}

fn output_urls_limit(config: &OutputConfig) -> usize {
    config.urls_limit.max(1)
}

fn select_output_channels(config: &OutputConfig, mut channels: Vec<Channel>) -> Vec<Channel> {
    crate::playlist::sort_channels_by_preferences(config, &mut channels);
    let mut eligible = Vec::new();
    let mut local_count = 0;
    let mut subscribe_count = 0;
    for channel in channels {
        if crate::playlist::origin_is_within_limit(
            config,
            channel.origin,
            &mut local_count,
            &mut subscribe_count,
        ) {
            eligible.push(channel);
        }
    }
    prefer_recent_channels(eligible, config)
}

fn prefer_recent_channels(channels: Vec<Channel>, config: &OutputConfig) -> Vec<Channel> {
    let limit = output_urls_limit(config);
    if channels.len() <= limit {
        return channels;
    }
    let recent_days = if config.recent_days > 0 {
        config.recent_days
    } else {
        30
    };
    let start_date = Local::now().date_naive() - chrono::Duration::days(recent_days);
    let mut recent = Vec::new();
    let mut older = Vec::new();
    for channel in channels {
        if channel_date(&channel).is_some_and(|date| date >= start_date) {
            recent.push(channel);
        } else {
            older.push(channel);
        }
    }
    if recent.is_empty() {
        older.truncate(limit);
        return older;
    }
    if recent.len() < limit {
        recent.extend(older.into_iter().take(limit - recent.len()));
    }
    recent.truncate(limit);
    recent
}

fn channel_date(channel: &Channel) -> Option<chrono::NaiveDate> {
    channel
        .date
        .as_deref()
        .and_then(|value| chrono::NaiveDate::parse_from_str(value, "%m-%d-%Y").ok())
}

fn channel_logo(
    config: &OutputConfig,
    channel: &Channel,
    request_base_url: Option<&str>,
) -> String {
    crate::playlist::channel_logo(
        config,
        &channel.name,
        channel.logo.as_deref(),
        request_base_url,
    )
}

fn update_time_label(config: &OutputConfig, first_channel_name: &str) -> String {
    format!(
        "{} {}",
        zoned_now(&config.time_zone).format("%Y-%m-%d %H:%M:%S"),
        first_channel_name
    )
}

fn zoned_now(time_zone: &str) -> DateTime<chrono_tz::Tz> {
    let tz = parse_time_zone(time_zone);
    Utc::now().with_timezone(&tz)
}

fn parse_time_zone(time_zone: &str) -> chrono_tz::Tz {
    time_zone
        .trim()
        .parse::<chrono_tz::Tz>()
        .unwrap_or(chrono_tz::Asia::Shanghai)
}

fn first_channel_for_update_time(
    config: &OutputConfig,
    channels: &DashMap<String, Vec<Channel>>,
    ip_filter: Option<IpFilter>,
) -> Option<(String, String)> {
    sorted_channel_groups(channels, ip_filter)
        .into_iter()
        .flat_map(|(_, channels)| channels)
        .next()
        .map(|channel| {
            (
                channel.name.clone(),
                channel_url_with_extra_info(config, &channel),
            )
        })
}

fn first_hls_channel_for_update_time(
    config: &OutputConfig,
    channels: &DashMap<String, Vec<Channel>>,
    ip_filter: Option<IpFilter>,
    hls_base_url: &str,
) -> Option<(String, String)> {
    sorted_channel_groups(channels, ip_filter)
        .into_iter()
        .flat_map(|(_, channels)| select_output_channels(config, channels))
        .next()
        .map(|channel| (channel.name.clone(), hls_proxy_url(hls_base_url, &channel)))
}

fn sorted_channel_groups(
    channels: &DashMap<String, Vec<Channel>>,
    ip_filter: Option<IpFilter>,
) -> Vec<(String, Vec<Channel>)> {
    let mut groups = channels
        .iter()
        .filter_map(|entry| {
            let mut values = entry
                .value()
                .iter()
                .filter(|channel| channel_matches_ip_filter(&channel.url, ip_filter))
                .cloned()
                .collect::<Vec<_>>();
            if values.is_empty() {
                return None;
            }
            values.sort_by_key(|channel| channel.latency.unwrap_or(u64::MAX));
            Some((entry.key().clone(), values))
        })
        .collect::<Vec<_>>();
    groups.sort_by(|left, right| left.0.cmp(&right.0));
    groups
}

fn configured_ip_filter(ipv_type: &str) -> Option<IpFilter> {
    match ipv_type.trim().to_ascii_lowercase().as_str() {
        "ipv4" => Some(IpFilter::Ipv4),
        "ipv6" => Some(IpFilter::Ipv6),
        _ => None,
    }
}

fn channel_matches_configured_ip_type(url: &str, ipv_type: &str) -> bool {
    channel_matches_ip_filter(url, configured_ip_filter(ipv_type))
}

fn channel_matches_location_isp_filter(channel: &Channel, filter: &FilterConfig) -> bool {
    if matches!(
        channel.origin,
        ChannelOrigin::Whitelist | ChannelOrigin::Hls
    ) {
        return true;
    }
    metadata_matches_filter(channel.location.as_deref(), &filter.location)
        && metadata_matches_filter(channel.isp.as_deref(), &filter.isp)
}

struct IpdbLookup {
    reader: Option<ipdb::Reader>,
}

impl IpdbLookup {
    fn load(path: &str) -> Self {
        Self {
            reader: ipdb::Reader::open_file(path).ok(),
        }
    }

    fn find_map(&self, ip: &str) -> Option<(Option<String>, Option<String>)> {
        let reader = self.reader.as_ref()?;
        let result = reader.find_map(ip, "CN").ok()?;
        location_isp_from_ipdb_map(&result)
    }
}

fn enrich_location_isp_from_ipdb(channel: &mut Channel, ipdb: &IpdbLookup) {
    if is_retained_origin(channel.origin) || (channel.location.is_some() && channel.isp.is_some()) {
        return;
    }
    let Some(ip) = channel_lookup_ip(&channel.url) else {
        return;
    };
    let Some((location, isp)) = ipdb.find_map(&ip) else {
        return;
    };
    if channel.location.is_none() {
        channel.location = location;
    }
    if channel.isp.is_none() {
        channel.isp = isp;
    }
}

fn location_isp_from_ipdb_map(
    map: &BTreeMap<&str, &str>,
) -> Option<(Option<String>, Option<String>)> {
    let parts = ["country_name", "region_name", "city_name"]
        .into_iter()
        .filter_map(|key| non_empty_string(map.get(key).copied()))
        .collect::<Vec<_>>();
    let location = (!parts.is_empty()).then(|| parts.join("-"));
    let isp = map
        .get("isp_domain")
        .and_then(|value| non_empty_string(Some(*value)));
    (location.is_some() || isp.is_some()).then_some((location, isp))
}

fn non_empty_string(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}

fn channel_lookup_ip(url: &str) -> Option<String> {
    let host = crate::playlist::url_host(url)?;
    let host = host.trim_start_matches('[').trim_end_matches(']');
    if host.parse::<std::net::IpAddr>().is_ok() {
        return Some(host.to_owned());
    }
    let addrs = (host, 0).to_socket_addrs().ok()?.collect::<Vec<_>>();
    addrs
        .iter()
        .find(|addr| addr.is_ipv6())
        .or_else(|| addrs.iter().find(|addr| addr.is_ipv4()))
        .map(|addr| addr.ip().to_string())
}

fn metadata_matches_filter(value: Option<&str>, filters: &[String]) -> bool {
    filters.is_empty()
        || value
            .map(|value| filters.iter().any(|filter| value.contains(filter)))
            .unwrap_or(true)
}

fn mark_channel_online_without_speed_test(mut channel: Channel) -> Channel {
    channel.is_online = true;
    if channel.latency.is_none() {
        channel.latency = Some(0);
    }
    if channel.last_checked.is_none() {
        channel.last_checked = Some(Utc::now());
    }
    channel
}

fn channel_probe_host(channel: &Channel) -> Option<String> {
    crate::playlist::url_host(&channel.url).filter(|host| !host.trim().is_empty())
}

fn apply_host_probe_result(mut channel: Channel, checked: &Channel) -> Channel {
    channel.is_online = checked.is_online;
    channel.latency = checked.latency;
    channel.last_checked = checked.last_checked;
    channel.speed = checked.speed;
    channel.resolution = checked.resolution.clone();
    channel
}

fn channel_matches_speed_test_filters(channel: &Channel, config: &EngineConfig) -> bool {
    if is_retained_origin(channel.origin) {
        return true;
    }
    if config.open_supply {
        return true;
    }
    if config.open_filter_speed
        && let Some(speed) = channel.speed
    {
        let min_speed = channel
            .resolution
            .as_deref()
            .and_then(|resolution| config.resolution_speed_map.get(resolution))
            .copied()
            .unwrap_or(config.min_speed);
        if speed < min_speed {
            return false;
        }
    }
    if config.open_filter_resolution
        && let Some(resolution) = channel
            .resolution
            .as_deref()
            .map(str::trim)
            .filter(|resolution| !resolution.is_empty())
    {
        let value = resolution_value(resolution);
        let min = resolution_value(&config.min_resolution);
        let max = resolution_value(&config.max_resolution);
        if value < min || value > max {
            return false;
        }
    }
    true
}

fn channel_needs_frozen(channel: &Channel, config: &EngineConfig) -> bool {
    if is_retained_origin(channel.origin) {
        return false;
    }
    if !channel.is_online || channel.latency.is_none() {
        return true;
    }
    if channel.speed.unwrap_or(0.0) == 0.0 {
        return true;
    }
    if let Some(resolution) = channel.resolution.as_deref()
        && resolution_value(resolution) < resolution_value(&config.min_resolution)
    {
        return true;
    }
    false
}

fn is_retained_origin(origin: ChannelOrigin) -> bool {
    matches!(origin, ChannelOrigin::Whitelist | ChannelOrigin::Hls)
}

fn resolution_value(resolution: &str) -> u64 {
    let mut parts = resolution
        .split(['x', 'X', '*'])
        .filter_map(|part| part.trim().parse::<u64>().ok());
    match (parts.next(), parts.next()) {
        (Some(width), Some(height)) => width.saturating_mul(height),
        _ => 0,
    }
}

fn channel_matches_ip_filter(url: &str, ip_filter: Option<IpFilter>) -> bool {
    match ip_filter {
        None => true,
        Some(IpFilter::Ipv4) => crate::playlist::ip_family(url) == crate::playlist::IpFamily::Ipv4,
        Some(IpFilter::Ipv6) => crate::playlist::ip_family(url) == crate::playlist::IpFamily::Ipv6,
    }
}

fn channel_url_with_extra_info(config: &OutputConfig, channel: &Channel) -> String {
    if !config.open_url_info {
        return channel
            .url
            .split_once('$')
            .map(|(url, _)| url)
            .unwrap_or(&channel.url)
            .to_owned();
    }
    match channel
        .extra_info
        .as_deref()
        .filter(|value| !value.is_empty())
    {
        Some(extra_info) if !channel.url.contains('$') => format!("{}${}", channel.url, extra_info),
        _ => channel.url.clone(),
    }
}

fn hls_proxy_url(hls_base_url: &str, channel: &Channel) -> String {
    format!(
        "{}/{}.m3u8",
        hls_base_url.trim_end_matches('/'),
        channel_rtmp_id(channel)
    )
}

fn channel_rtmp_id(channel: &Channel) -> String {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    channel.url.hash(&mut hasher);
    hasher.finish().to_string()
}

fn output_public_base(config: &OutputConfig) -> String {
    if !config.public_base_url.trim().is_empty() {
        return config.public_base_url.trim_end_matches('/').to_owned();
    }
    let scheme = if config.public_scheme.trim().is_empty() {
        "http"
    } else {
        config.public_scheme.trim()
    };
    let domain = if config.public_domain.trim().is_empty() {
        "127.0.0.1"
    } else {
        config.public_domain.trim()
    };
    let default_port = if scheme.eq_ignore_ascii_case("https") {
        443
    } else {
        80
    };
    match config.public_port {
        Some(port) if port != default_port => format!("{scheme}://{domain}:{port}"),
        _ => format!("{scheme}://{domain}"),
    }
}

fn write_rtmp_result_data(
    config: &OutputConfig,
    channels: &DashMap<String, Vec<Channel>>,
) -> io::Result<()> {
    if let Some(parent) = Path::new(&config.rtmp_data_path)
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent)?;
    }
    let mut connection = rusqlite::Connection::open(&config.rtmp_data_path)
        .map_err(|error| io::Error::other(error.to_string()))?;
    connection
        .execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA synchronous=NORMAL;
             PRAGMA busy_timeout = 30000;
             CREATE TABLE IF NOT EXISTS result_data (id TEXT PRIMARY KEY, url TEXT, headers TEXT);",
        )
        .map_err(|error| io::Error::other(error.to_string()))?;
    let transaction = connection
        .transaction()
        .map_err(|error| io::Error::other(error.to_string()))?;
    {
        let mut statement = transaction
            .prepare("INSERT OR REPLACE INTO result_data (id, url, headers) VALUES (?1, ?2, ?3)")
            .map_err(|error| io::Error::other(error.to_string()))?;
        let mut seen = HashSet::new();
        for (_, channels) in sorted_channel_groups(channels, None) {
            for channel in select_output_channels(config, channels) {
                if !seen.insert(channel.url.clone()) {
                    continue;
                }
                let id = channel_rtmp_id(&channel);
                let headers = serde_json::to_string(&channel.headers)
                    .map_err(|error| io::Error::other(error.to_string()))?;
                statement
                    .execute(rusqlite::params![id, channel.url, headers])
                    .map_err(|error| io::Error::other(error.to_string()))?;
            }
        }
    }
    transaction
        .commit()
        .map_err(|error| io::Error::other(error.to_string()))?;
    Ok(())
}

fn atomic_write(path: &str, content: &str) -> io::Result<()> {
    let path = Path::new(path);
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent)?;
    }
    let tmp_path = path.with_extension(format!(
        "{}.tmp",
        path.extension()
            .and_then(|value| value.to_str())
            .unwrap_or("out")
    ));
    fs::write(&tmp_path, content)?;
    fs::rename(tmp_path, path)
}

fn merge_local_channels(
    channels: &mut Vec<Channel>,
    local_channels: Vec<Channel>,
    match_aliases: bool,
    alias_path: &str,
) {
    if !match_aliases || channels.is_empty() {
        append_unique_channels(channels, local_channels);
        return;
    }

    let aliases = crate::epg::aliases_for_path(alias_path);
    let mut primary_groups = HashMap::new();
    for channel in channels.iter() {
        primary_groups
            .entry(aliases.primary_name(&channel.name))
            .or_insert_with(|| channel.group.clone());
    }

    let mut matched = Vec::new();
    for mut channel in local_channels {
        let primary = aliases.primary_name(&channel.name);
        let Some(group) = primary_groups.get(&primary) else {
            continue;
        };
        channel.name = primary;
        channel.group = group.clone();
        matched.push(channel);
    }
    append_unique_channels(channels, matched);
}

fn append_unique_channels(channels: &mut Vec<Channel>, new_channels: Vec<Channel>) {
    let mut seen = channels
        .iter()
        .map(|channel| {
            (
                channel.group.clone(),
                channel.name.clone(),
                channel.url.clone(),
            )
        })
        .collect::<HashSet<_>>();
    for channel in new_channels {
        let key = (
            channel.group.clone(),
            channel.name.clone(),
            channel.url.clone(),
        );
        if seen.insert(key) {
            channels.push(channel);
        }
    }
}

impl Engine {
    pub fn new(
        config: AppConfig,
        channels: Arc<DashMap<String, Vec<Channel>>>,
        epg_status: Arc<RwLock<EpgStatus>>,
        engine_status: Arc<RwLock<EngineStatus>>,
    ) -> Self {
        let fetcher = Fetcher::new(config.engine.request_timeout, &config.engine.http_proxy);
        Self {
            config,
            channels,
            fetcher,
            epg_status,
            engine_status,
        }
    }

    pub async fn run_once(&self) -> Result<()> {
        let update_started = Instant::now();
        self.engine_status
            .write()
            .expect("engine status write lock poisoned")
            .start();
        tracing::info!("Starting update cycle...");
        let checker = Checker::with_options(
            self.config.engine.check_concurrency,
            self.config.engine.check_timeout,
            self.config.engine.speed_test_allow_invalid_certs,
            self.config.engine.speed_test_max_download_bytes,
            self.config.engine.speed_test_segment_concurrency,
        );
        let ipv6_supported =
            self.config.engine.ipv6_support || check_external_ipv6_support(Duration::from_secs(10));
        let ipv6_proxy_enabled =
            speed_test_ipv6_proxy_url(&self.config.engine.ipv_type, ipv6_supported).is_some();

        // Load filters
        let whitelist = Whitelist::load(&self.config.filter.whitelist_path);
        let blacklist = Blacklist::load(&self.config.filter.blacklist_path);

        // 1. Fetch
        let mut all_channels = Vec::new();
        let mut frozen_store = crate::history::load_frozen(&self.config.output);
        let history_cache = crate::history::load_cache(&self.config.output);
        let mut subscribe_status = crate::subscribe::SubscribeStatus::default();
        for source in &self.config.sources {
            match self.fetcher.fetch_source(source).await {
                Ok(channels) => all_channels.extend(channels),
                Err(e) => tracing::error!("Failed to fetch source {}: {}", source.name, e),
            }
        }
        if self.config.local.enabled {
            match self.fetcher.fetch_local_sources(&self.config.local).await {
                Ok(channels) => {
                    merge_local_channels(
                        &mut all_channels,
                        channels,
                        self.config.local.match_aliases,
                        &self.config.subscribe.alias_path,
                    );
                }
                Err(e) => tracing::error!("Failed to fetch local sources: {}", e),
            }
        }
        let source_names = all_channels
            .iter()
            .map(|channel| channel.name.clone())
            .collect::<HashSet<_>>();
        if self.config.subscribe.enabled {
            match crate::subscribe::fetch_channels(
                &self.config.subscribe,
                &self.fetcher,
                Some(&source_names),
            )
            .await
            {
                Ok((mut channels, status)) => {
                    subscribe_status = status;
                    all_channels.append(&mut channels);
                }
                Err(error) => {
                    metrics::counter!("iptvapi_subscribe_source_errors_total").increment(1);
                    crate::error_log::push("subscribe", error.to_string()).await;
                    tracing::warn!(error = %error, "Subscribe pipeline failed");
                }
            }
        } else {
            tracing::info!("Subscribe pipeline disabled by open_subscribe=false");
        }

        crate::history::merge_cached_channels(&mut all_channels, &history_cache, &mut frozen_store);

        let requested_names = requested_channel_names(&all_channels);
        let fetched_channels = all_channels.len();
        metrics::gauge!("iptvapi_fetched_channels").set(fetched_channels as f64);
        let ipdb = IpdbLookup::load(&self.config.filter.ipdb_path);

        // 2. Filter (Blacklist and detect Whitelist)
        let mut filtered_channels = Vec::new();
        for mut channel in all_channels {
            if channel.url.trim().is_empty() {
                continue;
            }
            if blacklist.is_blacklisted(&channel.url) {
                continue;
            }
            if !self.config.output.open_headers {
                channel.headers = None;
            }
            if !channel_matches_configured_ip_type(&channel.url, &self.config.engine.ipv_type) {
                continue;
            }
            if whitelist.is_whitelisted(&channel.url, &channel.name) {
                channel.origin = ChannelOrigin::Whitelist;
                channel.is_online = true; // Whitelisted are always online for now
                channel.latency = Some(0); // Top priority
            }
            enrich_location_isp_from_ipdb(&mut channel, &ipdb);
            if !channel_matches_location_isp_filter(&channel, &self.config.filter) {
                continue;
            }
            filtered_channels.push(channel);
        }

        let filtered_channels_count = filtered_channels.len();
        metrics::gauge!("iptvapi_filtered_channels").set(filtered_channels_count as f64);

        // 3. Check (Concurrent) - skip those already marked online by whitelist if desired,
        // but here we check everyone for latency unless it's whitelist.
        let results = if self.config.engine.open_speed_test {
            tracing::info!("Checking {} channels...", filtered_channels.len());
            check_filtered_channels(
                filtered_channels,
                &checker,
                self.config.engine.speed_test_filter_host,
                self.config.engine.open_full_speed_test,
                self.config.output.urls_limit,
                ipv6_proxy_enabled,
            )
            .await
        } else {
            tracing::info!("Speed test disabled by open_speed_test=false");
            filtered_channels
                .into_iter()
                .map(mark_channel_online_without_speed_test)
                .collect()
        };

        if self.config.output.open_history {
            for channel in results
                .iter()
                .filter(|channel| !is_retained_origin(channel.origin))
            {
                if self.config.engine.open_speed_test
                    && channel_needs_frozen(channel, &self.config.engine)
                {
                    frozen_store.mark_url_bad(&channel.url, false);
                } else if channel.is_online {
                    frozen_store.mark_url_good(&channel.url);
                } else if self.config.engine.open_speed_test {
                    frozen_store.mark_url_bad(&channel.url, false);
                }
            }
            if let Err(error) = crate::history::save_frozen(&self.config.output, &frozen_store) {
                tracing::warn!(error = %error, "Failed to save frozen URL history");
            }
        }

        // 3. Update State (Group by Genre)
        self.channels.clear();
        for channel in results {
            if channel.is_online
                && channel_matches_speed_test_filters(&channel, &self.config.engine)
            {
                self.channels
                    .entry(channel.group.clone())
                    .or_default()
                    .push(channel);
            }
        }

        let online_channels: usize = self.channels.iter().map(|entry| entry.value().len()).sum();
        metrics::gauge!("iptvapi_online_groups").set(self.channels.len() as f64);
        metrics::gauge!("iptvapi_online_channels").set(online_channels as f64);

        // 4. Sort by latency within each group
        for mut entry in self.channels.iter_mut() {
            entry.sort_by_key(|c| c.latency.unwrap_or(u64::MAX));
        }

        let no_result_names = no_result_names(&requested_names, &self.channels);
        if self.config.output.open_history {
            let online_snapshot = self
                .channels
                .iter()
                .flat_map(|entry| entry.value().clone())
                .collect::<Vec<_>>();
            if let Err(error) = crate::history::save_cache(&self.config.output, &online_snapshot) {
                tracing::warn!(error = %error, "Failed to save channel history cache");
            }
        }

        if let Err(error) = write_playlist_outputs(
            &self.config.output,
            self.config.local.hls_enabled,
            &self.channels,
            configured_ip_filter(&self.config.engine.ipv_type),
            &no_result_names,
        ) {
            metrics::counter!("iptvapi_update_errors_total").increment(1);
            crate::error_log::push("output", error.to_string()).await;
            tracing::warn!(error = %error, "Failed to write playlist output files");
        }

        self.update_epg().await;

        self.engine_status
            .write()
            .expect("engine status write lock poisoned")
            .finish_success(EngineSuccessSnapshot {
                started: update_started,
                fetched_channels,
                filtered_channels: filtered_channels_count,
                online_groups: self.channels.len(),
                online_channels,
                subscribe_sources: subscribe_status.configured_urls,
                subscribe_failed_sources: subscribe_status.failed_urls,
                subscribe_channels: subscribe_status.channels,
                subscribe_whitelist_sources: subscribe_status.whitelist_urls,
                subscribe_whitelist_channels: subscribe_status.whitelist_channels,
                subscribe_header_channels: subscribe_status.header_channels,
                subscribe_extra_info_channels: subscribe_status.extra_info_channels,
                subscribe_nomatch_channels: subscribe_status.nomatch_channels,
            });
        metrics::counter!("iptvapi_update_cycles_total").increment(1);
        metrics::histogram!("iptvapi_update_duration_seconds")
            .record(update_started.elapsed().as_secs_f64());
        tracing::info!(
            groups = self.channels.len(),
            channels = online_channels,
            elapsed_ms = update_started.elapsed().as_millis(),
            "Update cycle finished"
        );
        Ok(())
    }

    async fn update_epg(&self) {
        if !self.config.epg.enabled {
            tracing::info!("EPG pipeline disabled by open_epg=false");
            return;
        }

        let started = Instant::now();
        self.epg_status
            .write()
            .expect("EPG status write lock poisoned")
            .start(&self.config.epg);
        let names = self
            .channels
            .iter()
            .flat_map(|entry| {
                entry
                    .value()
                    .iter()
                    .map(|channel| channel.name.clone())
                    .collect::<Vec<_>>()
            })
            .collect::<HashSet<_>>();

        match crate::epg::run(&self.config.epg, Some(&names)).await {
            Ok(programmes) if programmes.is_empty() => {
                self.epg_status
                    .write()
                    .expect("EPG status write lock poisoned")
                    .finish_skipped(started);
                tracing::info!("EPG pipeline skipped: no programmes found");
            }
            Ok(programmes) => {
                if let Err(error) = crate::epg::write_outputs(
                    &programmes,
                    &self.config.epg.output_xml_path,
                    &self.config.epg.output_gz_path,
                ) {
                    metrics::counter!("iptvapi_epg_errors_total").increment(1);
                    self.epg_status
                        .write()
                        .expect("EPG status write lock poisoned")
                        .finish_error(started, error.to_string());
                    crate::error_log::push("epg", error.to_string()).await;
                    tracing::warn!(error = %error, "Failed to write EPG outputs");
                    return;
                }
                self.epg_status
                    .write()
                    .expect("EPG status write lock poisoned")
                    .finish_success(started, &programmes);
                metrics::counter!("iptvapi_epg_runs_total").increment(1);
                metrics::histogram!("iptvapi_epg_duration_seconds")
                    .record(started.elapsed().as_secs_f64());
                tracing::info!(
                    channels = programmes.len(),
                    elapsed_ms = started.elapsed().as_millis(),
                    "EPG pipeline finished"
                );
            }
            Err(error) => {
                metrics::counter!("iptvapi_epg_errors_total").increment(1);
                self.epg_status
                    .write()
                    .expect("EPG status write lock poisoned")
                    .finish_error(started, error.to_string());
                crate::error_log::push("epg", error.to_string()).await;
                tracing::warn!(error = %error, "EPG pipeline failed");
            }
        }
    }

    pub async fn schedule(self: Arc<Self>) {
        if !self.config.engine.open_update {
            tracing::info!("IPTV update cycle disabled by open_update=false");
            return;
        }

        if self.config.engine.update_startup {
            let initial_delay = initial_update_delay();
            if !initial_delay.is_zero() {
                tracing::info!(
                    "Delaying first IPTV update by {}s to keep app startup responsive",
                    initial_delay.as_secs()
                );
                tokio::time::sleep(initial_delay).await;
            }
            self.run_scheduled_update().await;
        }

        while let Some(delay) = next_update_delay(
            zoned_now(&self.config.engine.time_zone),
            &self.config.engine,
        ) {
            tracing::info!(
                delay_seconds = delay.as_secs(),
                update_mode = %self.config.engine.update_mode,
                "Scheduled next IPTV update"
            );
            tokio::time::sleep(delay).await;
            self.run_scheduled_update().await;
        }
    }

    async fn run_scheduled_update(&self) {
        if let Err(e) = self.run_once().await {
            metrics::counter!("iptvapi_update_errors_total").increment(1);
            self.engine_status
                .write()
                .expect("engine status write lock poisoned")
                .finish_error(Instant::now(), e.to_string());
            crate::error_log::push("engine", e.to_string()).await;
            tracing::error!("Engine update error: {}", e);
        }
    }
}

async fn check_filtered_channels(
    channels: Vec<Channel>,
    checker: &Checker,
    filter_host: bool,
    open_full_speed_test: bool,
    urls_limit: usize,
    ipv6_proxy_enabled: bool,
) -> Vec<Channel> {
    let channels = limit_speed_test_candidates(channels, open_full_speed_test, urls_limit);
    if !filter_host {
        let mut check_tasks = Vec::new();
        for channel in channels {
            let is_whitelist = channel.is_online;
            check_tasks.push(async move {
                if is_whitelist {
                    channel
                } else if ipv6_proxy_enabled
                    && crate::playlist::ip_family(&channel.url) == crate::playlist::IpFamily::Ipv6
                {
                    mark_unsupported_ipv6_default_result(channel)
                } else {
                    checker.check_channel(channel).await
                }
            });
        }
        return join_all(check_tasks).await;
    }

    let mut results = Vec::new();
    let mut probe_inputs = Vec::new();
    let mut first_by_host = HashMap::new();
    let mut duplicates_by_first: HashMap<usize, Vec<Channel>> = HashMap::new();

    for channel in channels {
        if channel.is_online || is_retained_origin(channel.origin) {
            results.push(channel);
            continue;
        }
        if ipv6_proxy_enabled
            && crate::playlist::ip_family(&channel.url) == crate::playlist::IpFamily::Ipv6
        {
            results.push(mark_unsupported_ipv6_default_result(channel));
            continue;
        }
        let Some(host) = channel_probe_host(&channel) else {
            probe_inputs.push(channel);
            continue;
        };
        if let Some(first_index) = first_by_host.get(&host).copied() {
            duplicates_by_first
                .entry(first_index)
                .or_default()
                .push(channel);
        } else {
            first_by_host.insert(host, probe_inputs.len());
            probe_inputs.push(channel);
        }
    }

    let probe_results = join_all(
        probe_inputs
            .into_iter()
            .enumerate()
            .map(|(index, channel)| async move { (index, checker.check_channel(channel).await) }),
    )
    .await;

    for (index, checked) in probe_results {
        if let Some(duplicates) = duplicates_by_first.remove(&index) {
            results.extend(
                duplicates
                    .into_iter()
                    .map(|channel| apply_host_probe_result(channel, &checked)),
            );
        }
        results.push(checked);
    }

    results
}

fn limit_speed_test_candidates(
    channels: Vec<Channel>,
    open_full_speed_test: bool,
    urls_limit: usize,
) -> Vec<Channel> {
    if open_full_speed_test {
        return channels;
    }
    let limit = urls_limit.max(1);
    let mut counts: HashMap<(String, String), usize> = HashMap::new();
    let mut limited = Vec::new();
    for channel in channels {
        if channel.is_online || is_retained_origin(channel.origin) {
            limited.push(channel);
            continue;
        }
        let key = (channel.group.clone(), channel.name.clone());
        let count = counts.entry(key).or_default();
        if *count < limit {
            *count += 1;
            limited.push(channel);
        }
    }
    limited
}

const IPV6_PROXY_URL: &str = "http://www.ipv6proxy.net/go.php?u=";
const DEFAULT_IPV6_DELAY_MS: u64 = 100;
const DEFAULT_IPV6_RESOLUTION: &str = "1920x1080";

fn speed_test_ipv6_proxy_url(ipv_type: &str, ipv6_supported: bool) -> Option<&'static str> {
    let ipv_type = ipv_type.trim().to_ascii_lowercase();
    let open_ipv6 = ipv_type.contains("ipv6") || ipv_type.contains("all");
    (open_ipv6 && !ipv6_supported).then_some(IPV6_PROXY_URL)
}

fn check_external_ipv6_support(timeout: Duration) -> bool {
    if env::var_os("GITHUB_ACTIONS").is_some() {
        return false;
    }
    check_external_ipv6_support_addr("[2606:4700:4700::1111]:53", timeout)
}

fn check_external_ipv6_support_addr(remote: &str, timeout: Duration) -> bool {
    let Ok(addr) = remote.parse::<SocketAddr>() else {
        return false;
    };
    TcpStream::connect_timeout(&addr, timeout).is_ok()
}

fn mark_unsupported_ipv6_default_result(mut channel: Channel) -> Channel {
    channel.is_online = true;
    channel.latency = Some(DEFAULT_IPV6_DELAY_MS);
    channel.speed = Some(f64::INFINITY);
    channel.resolution = Some(DEFAULT_IPV6_RESOLUTION.to_owned());
    channel.last_checked = Some(Utc::now());
    channel
}

fn next_update_delay<Tz: TimeZone>(now: DateTime<Tz>, config: &EngineConfig) -> Option<Duration> {
    let update_times = parse_update_times(&config.update_times);
    if config.update_mode.eq_ignore_ascii_case("time") && !update_times.is_empty() {
        return next_fixed_time_delay(now, &update_times);
    }

    if config.update_interval == 0 {
        None
    } else {
        Some(Duration::from_secs(config.update_interval))
    }
}

fn parse_update_times(value: &str) -> Vec<(u32, u32)> {
    value
        .split(',')
        .filter_map(|raw| {
            let (hour, minute) = raw.trim().split_once(':')?;
            let hour = hour.trim().parse::<u32>().ok()?;
            let minute = minute.trim().parse::<u32>().ok()?;
            (hour < 24 && minute < 60).then_some((hour, minute))
        })
        .collect()
}

fn next_fixed_time_delay<Tz: TimeZone>(
    now: DateTime<Tz>,
    update_times: &[(u32, u32)],
) -> Option<Duration> {
    let now_seconds = now.hour() * 3600 + now.minute() * 60 + now.second();
    update_times
        .iter()
        .map(|(hour, minute)| hour * 3600 + minute * 60)
        .map(|target_seconds| {
            if target_seconds > now_seconds {
                target_seconds - now_seconds
            } else {
                24 * 3600 - now_seconds + target_seconds
            }
        })
        .min()
        .map(|seconds| Duration::from_secs(seconds as u64))
}

fn initial_update_delay() -> Duration {
    env::var("TV_IPTV_INITIAL_UPDATE_DELAY_SECS")
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .map(Duration::from_secs)
        .unwrap_or_else(|| Duration::from_secs(15))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn engine_config_for_schedule() -> EngineConfig {
        EngineConfig {
            open_update: true,
            open_speed_test: true,
            update_interval: 12 * 60 * 60,
            update_mode: "interval".to_owned(),
            update_times: String::new(),
            time_zone: "Asia/Shanghai".to_owned(),
            update_startup: true,
            request_timeout: 10,
            http_proxy: String::new(),
            check_concurrency: 5,
            check_timeout: 10_000,
            speed_test_timeout: None,
            speed_test_allow_invalid_certs: true,
            speed_test_max_download_bytes: 8 * 1024 * 1024,
            speed_test_segment_concurrency: 2,
            speed_test_filter_host: false,
            open_full_speed_test: false,
            open_filter_speed: true,
            open_supply: false,
            min_speed: 0.5,
            open_filter_resolution: true,
            min_resolution: "1920x1080".to_owned(),
            max_resolution: "1920x1080".to_owned(),
            resolution_speed_map: HashMap::new(),
            ipv6_support: false,
            ipv_type: "all".to_owned(),
        }
    }

    #[test]
    fn open_update_defaults_to_enabled_for_python_global_switch() {
        let config = engine_config_for_schedule();
        assert!(config.open_update);
    }

    #[test]
    fn open_speed_test_defaults_to_enabled_for_python_speed_switch() {
        let config: EngineConfig = toml::from_str(
            r#"
update_interval = 12
time_zone = "Asia/Shanghai"
request_timeout = 10
http_proxy = ""
speed_test_limit = 5
speed_test_timeout = 10
ipv6_support = false
"#,
        )
        .unwrap();
        assert_eq!(config.update_interval, 12 * 60 * 60);
        assert!(config.open_speed_test);
        assert!(!config.speed_test_filter_host);
        assert!(!config.open_full_speed_test);
        assert!(config.open_filter_speed);
        assert!(!config.open_supply);
        assert!(config.open_filter_resolution);
    }

    #[test]
    fn engine_config_parses_fractional_update_interval_hours_like_python() {
        let config: EngineConfig = toml::from_str(
            r#"
update_interval = 0.5
time_zone = "Asia/Shanghai"
request_timeout = 10
http_proxy = ""
speed_test_limit = 5
speed_test_timeout = 10
ipv6_support = false
"#,
        )
        .unwrap();
        assert_eq!(config.update_interval, 30 * 60);
    }

    #[test]
    fn disabled_speed_test_keeps_filtered_candidates_online_without_probe() {
        let channel = mark_channel_online_without_speed_test(channel(
            "CCTV-1",
            "央视频道",
            "http://127.0.0.1/live.m3u8",
        ));
        assert!(channel.is_online);
        assert_eq!(channel.latency, Some(0));
        assert!(channel.last_checked.is_some());
    }

    #[test]
    fn host_filter_cache_key_uses_url_host_and_replays_probe_result() {
        let mut checked = channel("A", "央视频道", "http://cache.example/a.m3u8");
        checked.is_online = true;
        checked.latency = Some(42);
        checked.last_checked = Some(Utc.with_ymd_and_hms(2026, 5, 27, 1, 2, 3).unwrap());

        let duplicate = channel("B", "央视频道", "http://cache.example/b.m3u8");
        assert_eq!(
            channel_probe_host(&duplicate).as_deref(),
            Some("cache.example")
        );

        let replayed = apply_host_probe_result(duplicate, &checked);
        assert!(replayed.is_online);
        assert_eq!(replayed.latency, Some(42));
        assert_eq!(replayed.last_checked, checked.last_checked);
    }

    #[test]
    fn speed_test_filters_match_python_thresholds_for_existing_metadata() {
        let mut config = engine_config_for_schedule();
        config.min_speed = 0.5;
        config.min_resolution = "1280x720".to_owned();
        config.max_resolution = "1920x1080".to_owned();
        config
            .resolution_speed_map
            .insert("3840x2160".to_owned(), 1.0);

        let mut channel = channel("CCTV-1", "央视频道", "http://127.0.0.1/live.m3u8");
        channel.speed = Some(0.6);
        channel.resolution = Some("1920x1080".to_owned());
        assert!(channel_matches_speed_test_filters(&channel, &config));

        channel.speed = Some(0.4);
        assert!(!channel_matches_speed_test_filters(&channel, &config));

        channel.speed = Some(2.0);
        channel.resolution = Some("3840x2160".to_owned());
        assert!(!channel_matches_speed_test_filters(&channel, &config));

        config.open_supply = true;
        assert!(channel_matches_speed_test_filters(&channel, &config));
    }

    #[test]
    fn frozen_decision_matches_python_speed_test_failure_rules() {
        let mut config = engine_config_for_schedule();
        config.min_resolution = "1280x720".to_owned();

        let offline = channel("Offline", "Test", "http://offline/live.m3u8");
        assert!(channel_needs_frozen(&offline, &config));

        let mut zero_speed = channel("Zero", "Test", "http://zero/live.m3u8");
        zero_speed.is_online = true;
        zero_speed.latency = Some(10);
        zero_speed.speed = Some(0.0);
        assert!(channel_needs_frozen(&zero_speed, &config));

        let mut low_resolution = zero_speed.clone();
        low_resolution.speed = Some(1.0);
        low_resolution.resolution = Some("640x360".to_owned());
        assert!(channel_needs_frozen(&low_resolution, &config));

        let mut good = low_resolution.clone();
        good.resolution = Some("1920x1080".to_owned());
        assert!(!channel_needs_frozen(&good, &config));

        good.origin = ChannelOrigin::Whitelist;
        good.speed = Some(0.0);
        assert!(!channel_needs_frozen(&good, &config));
    }

    #[test]
    fn resolution_value_matches_python_width_times_height_parse() {
        assert_eq!(resolution_value("1280x720"), 921_600);
        assert_eq!(resolution_value("1920X1080"), 2_073_600);
        assert_eq!(resolution_value("bad"), 0);
    }

    #[test]
    fn full_speed_test_switch_controls_per_channel_probe_limit() {
        let channels = vec![
            channel("CCTV-1", "央视频道", "http://one.example/1.m3u8"),
            channel("CCTV-1", "央视频道", "http://two.example/2.m3u8"),
            channel("CCTV-1", "央视频道", "http://three.example/3.m3u8"),
        ];

        let limited = limit_speed_test_candidates(channels.clone(), false, 2);
        assert_eq!(
            limited
                .iter()
                .map(|channel| channel.url.as_str())
                .collect::<Vec<_>>(),
            vec!["http://one.example/1.m3u8", "http://two.example/2.m3u8"]
        );

        let full = limit_speed_test_candidates(channels, true, 2);
        assert_eq!(full.len(), 3);
    }

    #[test]
    fn full_speed_test_limit_keeps_retained_origins_uncapped() {
        let channels = vec![
            channel("CCTV-1", "央视频道", "http://one.example/1.m3u8"),
            channel("CCTV-1", "央视频道", "http://two.example/2.m3u8"),
            channel_with_origin(
                "CCTV-1",
                "央视频道",
                "http://white.example/live.m3u8",
                ChannelOrigin::Whitelist,
            ),
        ];

        let limited = limit_speed_test_candidates(channels, false, 1);
        assert_eq!(
            limited
                .iter()
                .map(|channel| channel.url.as_str())
                .collect::<Vec<_>>(),
            vec![
                "http://one.example/1.m3u8",
                "http://white.example/live.m3u8"
            ]
        );
    }

    #[test]
    fn invalid_external_ipv6_probe_returns_false() {
        assert!(!check_external_ipv6_support_addr(
            "not a socket address",
            Duration::from_millis(1)
        ));
    }

    #[tokio::test]
    async fn unsupported_ipv6_gets_python_default_speed_result_without_probe() {
        let checker = Checker::with_options(1, 1, true, 8 * 1024 * 1024, 2);
        let results = check_filtered_channels(
            vec![channel("IPv6", "Test", "http://[::1]/live.m3u8")],
            &checker,
            false,
            true,
            10,
            speed_test_ipv6_proxy_url("all", false).is_some(),
        )
        .await;

        assert_eq!(results.len(), 1);
        assert!(results[0].is_online);
        assert_eq!(results[0].latency, Some(DEFAULT_IPV6_DELAY_MS));
        assert_eq!(results[0].speed, Some(f64::INFINITY));
        assert_eq!(
            results[0].resolution.as_deref(),
            Some(DEFAULT_IPV6_RESOLUTION)
        );
    }

    #[test]
    fn ipv6_proxy_flag_matches_python_open_ipv6_and_runtime_support() {
        assert_eq!(
            speed_test_ipv6_proxy_url("all", false),
            Some(IPV6_PROXY_URL)
        );
        assert_eq!(
            speed_test_ipv6_proxy_url("ipv6", false),
            Some(IPV6_PROXY_URL)
        );
        assert_eq!(speed_test_ipv6_proxy_url("ipv4", false), None);
        assert_eq!(speed_test_ipv6_proxy_url("all", true), None);
    }

    #[tokio::test]
    async fn supported_ipv6_is_probed_instead_of_default_proxy_result() {
        let checker = Checker::with_options(1, 1, true, 8 * 1024 * 1024, 2);
        let results = check_filtered_channels(
            vec![channel("IPv6", "Test", "http://[::1]/live.m3u8")],
            &checker,
            false,
            true,
            10,
            speed_test_ipv6_proxy_url("all", true).is_some(),
        )
        .await;

        assert_eq!(results.len(), 1);
        assert!(!results[0].is_online);
        assert_ne!(results[0].speed, Some(f64::INFINITY));
    }

    #[tokio::test]
    async fn filter_host_unsupported_ipv6_defaults_without_poisoning_host_replay() {
        let checker = Checker::with_options(1, 1, true, 8 * 1024 * 1024, 2);
        let results = check_filtered_channels(
            vec![
                channel("IPv6 A", "Test", "http://[::1]/a.m3u8"),
                channel("IPv6 B", "Test", "http://[::1]/b.m3u8"),
            ],
            &checker,
            true,
            true,
            10,
            speed_test_ipv6_proxy_url("all", false).is_some(),
        )
        .await;

        assert_eq!(results.len(), 2);
        assert!(results.iter().all(|channel| channel.is_online));
        assert!(
            results
                .iter()
                .all(|channel| channel.speed == Some(f64::INFINITY))
        );
    }

    #[test]
    fn parse_update_times_ignores_invalid_entries() {
        assert_eq!(
            parse_update_times("01:30, bad, 24:00, 23:45, 12:60"),
            vec![(1, 30), (23, 45)]
        );
    }

    #[test]
    fn fixed_time_schedule_uses_configured_python_time_zone() {
        let now = chrono_tz::UTC
            .with_ymd_and_hms(2026, 5, 27, 23, 50, 0)
            .single()
            .unwrap();
        let shanghai_now = now.with_timezone(&parse_time_zone("Asia/Shanghai"));

        assert_eq!(
            next_fixed_time_delay(shanghai_now, &[(8, 5)]),
            Some(Duration::from_secs(15 * 60))
        );
    }

    #[test]
    fn fixed_time_schedule_picks_next_same_day_or_tomorrow() {
        let now = Local
            .with_ymd_and_hms(2026, 5, 27, 10, 15, 0)
            .single()
            .unwrap();
        assert_eq!(
            next_fixed_time_delay(now, &[(1, 30), (23, 45)]),
            Some(Duration::from_secs(13 * 3600 + 30 * 60))
        );

        let late = Local
            .with_ymd_and_hms(2026, 5, 27, 23, 50, 0)
            .single()
            .unwrap();
        assert_eq!(
            next_fixed_time_delay(late, &[(1, 30), (23, 45)]),
            Some(Duration::from_secs(3600 + 40 * 60))
        );
    }

    #[test]
    fn next_update_delay_handles_python_hour_interval_and_one_shot() {
        let now = Local
            .with_ymd_and_hms(2026, 5, 27, 10, 15, 0)
            .single()
            .unwrap();
        let mut config = engine_config_for_schedule();
        config.update_interval = 5 * 60 * 60;
        assert_eq!(
            next_update_delay(now, &config),
            Some(Duration::from_secs(5 * 60 * 60))
        );

        config.update_interval = 0;
        assert_eq!(next_update_delay(now, &config), None);
    }

    #[test]
    fn next_update_delay_uses_fixed_times_when_configured() {
        let now = Local
            .with_ymd_and_hms(2026, 5, 27, 10, 15, 0)
            .single()
            .unwrap();
        let mut config = engine_config_for_schedule();
        config.update_mode = "time".to_owned();
        config.update_times = "01:30,23:45".to_owned();
        assert_eq!(
            next_update_delay(now, &config),
            Some(Duration::from_secs(13 * 3600 + 30 * 60))
        );
    }

    fn channel(name: &str, group: &str, url: &str) -> Channel {
        channel_with_origin(name, group, url, ChannelOrigin::Local)
    }

    fn channel_with_origin(name: &str, group: &str, url: &str, origin: ChannelOrigin) -> Channel {
        Channel {
            origin,
            name: name.to_owned(),
            group: group.to_owned(),
            url: url.to_owned(),
            logo: None,
            headers: None,
            extra_info: None,
            catchup: None,
            location: None,
            isp: None,
            speed: None,
            resolution: None,
            date: None,
            latency: None,
            last_checked: None,
            is_online: false,
        }
    }

    #[test]
    fn output_selection_respects_origin_and_ip_preferences_before_limits() {
        let output = OutputConfig {
            urls_limit: 2,
            local_num: 2,
            subscribe_num: 2,
            origin_type_prefer: vec![ChannelOrigin::Subscribe, ChannelOrigin::Local],
            ipv_type_prefer: vec!["ipv6".to_owned(), "ipv4".to_owned()],
            ..Default::default()
        };
        let selected = select_output_channels(
            &output,
            vec![
                channel_with_origin(
                    "Local v4",
                    "Test",
                    "http://127.0.0.1/live.m3u8",
                    ChannelOrigin::Local,
                ),
                channel_with_origin(
                    "Sub v4",
                    "Test",
                    "http://127.0.0.1/sub.m3u8",
                    ChannelOrigin::Subscribe,
                ),
                channel_with_origin(
                    "Sub v6",
                    "Test",
                    "http://[::1]/sub.m3u8",
                    ChannelOrigin::Subscribe,
                ),
            ],
        );

        assert_eq!(
            selected
                .iter()
                .map(|channel| channel.name.as_str())
                .collect::<Vec<_>>(),
            vec!["Sub v6", "Sub v4"]
        );
    }

    #[test]
    fn location_isp_filter_matches_python_metadata_contains_semantics() {
        let filter = FilterConfig {
            location: vec!["广东".to_owned()],
            isp: vec!["电信".to_owned()],
            ..Default::default()
        };
        let mut channel = channel("广东电信", "Test", "http://127.0.0.1/live.m3u8");
        channel.location = Some("中国广东广州".to_owned());
        channel.isp = Some("中国电信".to_owned());
        assert!(channel_matches_location_isp_filter(&channel, &filter));

        channel.isp = Some("中国联通".to_owned());
        assert!(!channel_matches_location_isp_filter(&channel, &filter));

        channel.origin = ChannelOrigin::Whitelist;
        assert!(channel_matches_location_isp_filter(&channel, &filter));
    }

    #[test]
    fn location_isp_filter_keeps_unknown_metadata_like_python_before_ipdb_lookup() {
        let filter = FilterConfig {
            location: vec!["广东".to_owned()],
            isp: vec!["电信".to_owned()],
            ..Default::default()
        };
        let channel = channel("Unknown", "Test", "http://127.0.0.1/live.m3u8");
        assert!(channel_matches_location_isp_filter(&channel, &filter));
    }

    #[test]
    fn ipdb_map_to_location_isp_matches_python_find_map_shape() {
        let map = BTreeMap::from([
            ("country_name", "中国"),
            ("region_name", "广东"),
            ("city_name", "广州"),
            ("isp_domain", "电信"),
        ]);

        let (location, isp) = location_isp_from_ipdb_map(&map).unwrap();

        assert_eq!(location.as_deref(), Some("中国-广东-广州"));
        assert_eq!(isp.as_deref(), Some("电信"));
    }

    #[test]
    fn literal_url_host_can_be_used_for_ipdb_lookup() {
        assert_eq!(
            channel_lookup_ip("http://127.0.0.1/live.m3u8").as_deref(),
            Some("127.0.0.1")
        );
        assert_eq!(
            channel_lookup_ip("http://[::1]/live.m3u8").as_deref(),
            Some("::1")
        );
    }

    #[test]
    fn output_selection_respects_local_and_subscribe_limits() {
        let output = OutputConfig {
            urls_limit: 10,
            local_num: 1,
            subscribe_num: 1,
            ..Default::default()
        };
        let selected = select_output_channels(
            &output,
            vec![
                channel_with_origin(
                    "Local 1",
                    "Test",
                    "http://local/1.m3u8",
                    ChannelOrigin::Local,
                ),
                channel_with_origin(
                    "Local 2",
                    "Test",
                    "http://local/2.m3u8",
                    ChannelOrigin::Local,
                ),
                channel_with_origin(
                    "Sub 1",
                    "Test",
                    "http://sub/1.m3u8",
                    ChannelOrigin::Subscribe,
                ),
                channel_with_origin(
                    "Sub 2",
                    "Test",
                    "http://sub/2.m3u8",
                    ChannelOrigin::Subscribe,
                ),
                channel_with_origin(
                    "White",
                    "Test",
                    "http://white/1.m3u8",
                    ChannelOrigin::Whitelist,
                ),
            ],
        );

        assert_eq!(
            selected
                .iter()
                .map(|channel| channel.name.as_str())
                .collect::<Vec<_>>(),
            vec!["Local 1", "Sub 1", "White"]
        );
    }

    #[test]
    fn output_selection_prefers_recent_dates_when_over_limit() {
        let output = OutputConfig {
            urls_limit: 2,
            recent_days: 30,
            ..Default::default()
        };
        let recent_date = Local::now().format("%m-%d-%Y").to_string();
        let mut old = channel("Old", "Test", "http://old/1.m3u8");
        old.date = Some("01-01-1970".to_owned());
        let mut recent = channel("Recent", "Test", "http://recent/1.m3u8");
        recent.date = Some(recent_date);
        let missing = channel("Missing", "Test", "http://missing/1.m3u8");

        let selected = select_output_channels(&output, vec![old, recent, missing]);

        assert_eq!(
            selected
                .iter()
                .map(|channel| channel.name.as_str())
                .collect::<Vec<_>>(),
            vec!["Recent", "Old"]
        );
    }

    #[test]
    fn output_selection_falls_back_when_no_recent_dates() {
        let output = OutputConfig {
            urls_limit: 2,
            recent_days: 30,
            ..Default::default()
        };
        let mut old = channel("Old", "Test", "http://old/1.m3u8");
        old.date = Some("01-01-1970".to_owned());
        let missing = channel("Missing", "Test", "http://missing/1.m3u8");
        let mut invalid = channel("Invalid", "Test", "http://invalid/1.m3u8");
        invalid.date = Some("bad-date".to_owned());

        let selected = select_output_channels(&output, vec![old, missing, invalid]);

        assert_eq!(
            selected
                .iter()
                .map(|channel| channel.name.as_str())
                .collect::<Vec<_>>(),
            vec!["Old", "Missing"]
        );
    }

    #[test]
    fn configured_ip_filter_matches_python_ipv_type_setting() {
        assert_eq!(configured_ip_filter("all"), None);
        assert_eq!(configured_ip_filter("ipv4"), Some(IpFilter::Ipv4));
        assert_eq!(configured_ip_filter("IPv6"), Some(IpFilter::Ipv6));
        assert!(channel_matches_configured_ip_type(
            "http://127.0.0.1/live.m3u8",
            "ipv4"
        ));
        assert!(!channel_matches_configured_ip_type(
            "http://[::1]/live.m3u8",
            "ipv4"
        ));
        assert!(channel_matches_configured_ip_type(
            "http://[::1]/live.m3u8",
            "ipv6"
        ));
        assert!(!channel_matches_configured_ip_type(
            "http://127.0.0.1/live.m3u8",
            "ipv6"
        ));
    }

    #[test]
    fn playlist_output_can_include_python_no_result_section() {
        let channels = DashMap::new();
        let output = OutputConfig {
            update_time_enabled: false,
            open_empty_category: true,
            ..Default::default()
        };

        let txt = playlist_txt(&output, &channels, None, &["Missing".to_owned()]);
        let m3u = playlist_m3u(&output, &channels, None, None, &["Missing".to_owned()]);

        assert!(txt.contains("🈳无结果频道,#genre#\nMissing,url"));
        assert!(m3u.contains("group-title=\"🈳无结果频道\",Missing\nurl"));
    }

    #[test]
    fn no_result_names_preserve_requested_order_and_skip_online() {
        let channels = DashMap::new();
        channels.insert(
            "Test".to_owned(),
            vec![channel("Online", "Test", "http://127.0.0.1/live.m3u8")],
        );
        let requested = vec![
            "Missing A".to_owned(),
            "Online".to_owned(),
            "Missing B".to_owned(),
        ];

        assert_eq!(
            no_result_names(&requested, &channels),
            vec!["Missing A", "Missing B"]
        );
    }

    #[test]
    fn playlist_output_respects_open_m3u_result_switch_for_artifacts() {
        let dir = tempfile::tempdir().unwrap();
        let channels = DashMap::new();
        channels.insert(
            "Test".to_owned(),
            vec![channel("One", "Test", "http://127.0.0.1/live.m3u8")],
        );
        let output = OutputConfig {
            update_time_enabled: false,
            m3u_result_enabled: false,
            result_txt_path: dir.path().join("result.txt").to_string_lossy().into_owned(),
            result_m3u_path: dir.path().join("result.m3u").to_string_lossy().into_owned(),
            ipv4_result_txt_path: dir
                .path()
                .join("ipv4/result.txt")
                .to_string_lossy()
                .into_owned(),
            ipv4_result_m3u_path: dir
                .path()
                .join("ipv4/result.m3u")
                .to_string_lossy()
                .into_owned(),
            ipv6_result_txt_path: dir
                .path()
                .join("ipv6/result.txt")
                .to_string_lossy()
                .into_owned(),
            ipv6_result_m3u_path: dir
                .path()
                .join("ipv6/result.m3u")
                .to_string_lossy()
                .into_owned(),
            ..Default::default()
        };

        write_playlist_outputs(&output, false, &channels, None, &[]).unwrap();

        assert!(dir.path().join("result.txt").exists());
        assert!(dir.path().join("ipv4/result.txt").exists());
        assert!(dir.path().join("ipv6/result.txt").exists());
        assert!(!dir.path().join("result.m3u").exists());
        assert!(!dir.path().join("ipv4/result.m3u").exists());
        assert!(!dir.path().join("ipv6/result.m3u").exists());
    }

    #[test]
    fn hls_outputs_use_proxy_urls_and_persist_rtmp_lookup_like_python() {
        let dir = tempfile::tempdir().unwrap();
        let mut channel = channel("One", "Test", "http://127.0.0.1/live.m3u8");
        channel.is_online = true;
        channel.headers = Some(HashMap::from([(
            "Referer".to_owned(),
            "https://example.com/".to_owned(),
        )]));
        let channel_id = channel_rtmp_id(&channel);
        let channels = DashMap::new();
        channels.insert("Test".to_owned(), vec![channel]);
        let output = OutputConfig {
            update_time_enabled: false,
            public_base_url: "http://example.test:8080".to_owned(),
            result_txt_path: dir.path().join("result.txt").to_string_lossy().into_owned(),
            result_m3u_path: dir.path().join("result.m3u").to_string_lossy().into_owned(),
            ipv4_result_txt_path: dir
                .path()
                .join("ipv4/result.txt")
                .to_string_lossy()
                .into_owned(),
            ipv4_result_m3u_path: dir
                .path()
                .join("ipv4/result.m3u")
                .to_string_lossy()
                .into_owned(),
            ipv6_result_txt_path: dir
                .path()
                .join("ipv6/result.txt")
                .to_string_lossy()
                .into_owned(),
            ipv6_result_m3u_path: dir
                .path()
                .join("ipv6/result.m3u")
                .to_string_lossy()
                .into_owned(),
            hls_result_txt_path: dir.path().join("hls.txt").to_string_lossy().into_owned(),
            hls_result_m3u_path: dir.path().join("hls.m3u").to_string_lossy().into_owned(),
            hls_ipv4_result_txt_path: dir
                .path()
                .join("ipv4/hls.txt")
                .to_string_lossy()
                .into_owned(),
            hls_ipv4_result_m3u_path: dir
                .path()
                .join("ipv4/hls.m3u")
                .to_string_lossy()
                .into_owned(),
            hls_ipv6_result_txt_path: dir
                .path()
                .join("ipv6/hls.txt")
                .to_string_lossy()
                .into_owned(),
            hls_ipv6_result_m3u_path: dir
                .path()
                .join("ipv6/hls.m3u")
                .to_string_lossy()
                .into_owned(),
            rtmp_data_path: dir
                .path()
                .join("data/rtmp.db")
                .to_string_lossy()
                .into_owned(),
            ..Default::default()
        };

        write_playlist_outputs(&output, true, &channels, None, &[]).unwrap();

        let hls_txt = fs::read_to_string(dir.path().join("hls.txt")).unwrap();
        assert!(hls_txt.contains(&format!(
            "One,http://example.test:8080/hls/{channel_id}.m3u8"
        )));
        assert!(!hls_txt.contains("One,http://127.0.0.1/live.m3u8"));
        let normal_txt = fs::read_to_string(dir.path().join("result.txt")).unwrap();
        assert!(normal_txt.contains("One,http://127.0.0.1/live.m3u8"));

        let connection = rusqlite::Connection::open(dir.path().join("data/rtmp.db")).unwrap();
        let (url, headers): (String, String) = connection
            .query_row(
                "SELECT url, headers FROM result_data WHERE id=?1",
                rusqlite::params![channel_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(url, "http://127.0.0.1/live.m3u8");
        assert!(headers.contains("Referer"));
    }

    #[test]
    fn playlist_m3u_respects_open_headers_switch() {
        let mut with_headers = channel("WithHeaders", "Test", "http://one.example/live.m3u8");
        with_headers.headers = Some(HashMap::from([(
            "Referer".to_owned(),
            "https://example.com/".to_owned(),
        )]));
        let channels = DashMap::new();
        channels.insert("Test".to_owned(), vec![with_headers]);

        let output = OutputConfig {
            update_time_enabled: false,
            open_headers: false,
            ..Default::default()
        };
        let m3u = playlist_m3u(&output, &channels, None, None, &[]);
        assert!(!m3u.contains("#EXTVLCOPT"));

        let output = OutputConfig {
            open_headers: true,
            ..output
        };
        let m3u = playlist_m3u(&output, &channels, None, None, &[]);
        assert!(m3u.contains("#EXTVLCOPT:http-referer=https://example.com/"));
    }

    #[test]
    fn playlist_output_respects_open_url_info_switch() {
        let mut with_info = channel("WithInfo", "Test", "http://one.example/live.m3u8");
        with_info.extra_info = Some("source".to_owned());
        let already_tagged = channel("Tagged", "Test", "http://two.example/live.m3u8$old");
        let channels = DashMap::new();
        channels.insert("Test".to_owned(), vec![with_info, already_tagged]);

        let output = OutputConfig {
            update_time_enabled: false,
            open_url_info: true,
            urls_limit: 10,
            ..Default::default()
        };
        let txt = playlist_txt(&output, &channels, None, &[]);
        assert!(txt.contains("WithInfo,http://one.example/live.m3u8$source"));
        assert!(txt.contains("Tagged,http://two.example/live.m3u8$old"));

        let output = OutputConfig {
            open_url_info: false,
            ..output
        };
        let txt = playlist_txt(&output, &channels, None, &[]);
        let m3u = playlist_m3u(&output, &channels, None, None, &[]);
        assert!(txt.contains("WithInfo,http://one.example/live.m3u8"));
        assert!(txt.contains("Tagged,http://two.example/live.m3u8"));
        assert!(!txt.contains("$source"));
        assert!(!txt.contains("$old"));
        assert!(!m3u.contains("$source"));
        assert!(!m3u.contains("$old"));
    }

    #[test]
    fn generated_m3u_preserves_python_catchup_attributes() {
        let mut with_catchup = channel("Replay", "Test", "http://one.example/live.m3u8");
        with_catchup.catchup = Some(HashMap::from([
            ("catchup".to_owned(), "default".to_owned()),
            (
                "catchup-source".to_owned(),
                "https://replay.example/{utc}".to_owned(),
            ),
        ]));
        let channels = DashMap::new();
        channels.insert("Test".to_owned(), vec![with_catchup]);
        let output = OutputConfig {
            update_time_enabled: false,
            urls_limit: 10,
            ..Default::default()
        };

        let m3u = playlist_m3u(&output, &channels, None, None, &[]);
        let hls_m3u = hls_playlist_m3u(&output, &channels, None, &[], "http://example.test/hls");

        assert!(m3u.contains(
            " catchup=\"default\" catchup-source=\"https://replay.example/{utc}\",Replay"
        ));
        assert!(hls_m3u.contains(
            " catchup=\"default\" catchup-source=\"https://replay.example/{utc}\",Replay"
        ));
    }

    #[test]
    fn playlist_output_respects_configured_ip_filter() {
        let channels = DashMap::new();
        channels.insert(
            "Test".to_owned(),
            vec![
                channel("IPv4", "Test", "http://127.0.0.1/live.m3u8"),
                channel("IPv6", "Test", "http://[::1]/live.m3u8"),
            ],
        );
        let output = OutputConfig {
            update_time_enabled: false,
            ..Default::default()
        };

        let txt = playlist_txt(&output, &channels, configured_ip_filter("ipv6"), &[]);

        assert!(txt.contains("IPv6,http://[::1]/live.m3u8"));
        assert!(!txt.contains("IPv4,http://127.0.0.1/live.m3u8"));
    }

    #[test]
    fn generated_m3u_uses_python_tvg_id_name_and_fanmingming_logo_names() {
        let channels = DashMap::new();
        channels.insert(
            "央视频道".to_owned(),
            vec![
                channel("CCTV-1", "央视频道", "http://one.example/live.m3u8"),
                channel("CCTV-1", "央视频道", "http://two.example/live.m3u8"),
                channel("CETV-4+", "央视频道", "http://cetv.example/live.m3u8"),
            ],
        );
        let mut output = OutputConfig {
            update_time_enabled: false,
            logo_url: "https://raw.githubusercontent.com/fanmingming/live/main/tv".to_owned(),
            ..Default::default()
        };
        output.urls_limit = 10;

        let m3u = playlist_m3u(&output, &channels, None, None, &[]);

        assert!(m3u.contains(
            "tvg-id=\"1\" tvg-name=\"CCTV1\" tvg-logo=\"https://raw.githubusercontent.com/fanmingming/live/main/tv/CCTV1.png\" group-title=\"央视频道\",CCTV-1"
        ));
        assert_eq!(m3u.matches("tvg-id=\"1\" tvg-name=\"CCTV1\"").count(), 2);
        assert!(m3u.contains("tvg-id=\"2\" tvg-name=\"CETV4+\""));
    }

    #[test]
    fn local_merge_uses_aliases_and_template_group() {
        let dir = tempfile::tempdir().unwrap();
        let alias_path = dir.path().join("alias.txt");
        fs::write(&alias_path, "CCTV-1,CCTV-01高清\n").unwrap();
        let mut base = vec![channel(
            "CCTV-1",
            "央视频道",
            "http://base.example/live.m3u8",
        )];
        let local = vec![
            channel("CCTV-01高清", "本地源", "http://local.example/live.m3u8"),
            channel("Other", "本地源", "http://local.example/other.m3u8"),
        ];

        merge_local_channels(&mut base, local, true, alias_path.to_str().unwrap());

        assert_eq!(base.len(), 2);
        assert!(base.iter().any(|item| item.name == "CCTV-1"
            && item.group == "央视频道"
            && item.url == "http://local.example/live.m3u8"));
        assert!(!base.iter().any(|item| item.name == "Other"));
    }

    #[test]
    fn local_merge_deduplicates_exact_urls() {
        let mut base = vec![channel(
            "CCTV-1",
            "央视频道",
            "http://same.example/live.m3u8",
        )];
        let local = vec![channel("CCTV-1", "本地源", "http://same.example/live.m3u8")];

        merge_local_channels(&mut base, local, true, "missing-alias-file.txt");

        assert_eq!(base.len(), 1);
    }
}
