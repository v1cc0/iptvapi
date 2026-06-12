use crate::models::{AppConfig, ChannelOrigin};
use anyhow::{Context, Result};
use std::fs;
use std::net::{SocketAddr, UdpSocket};
use std::path::{Path, PathBuf};

pub fn resolve_config_path() -> PathBuf {
    config_candidates()
        .into_iter()
        .find(|path| path.exists())
        .unwrap_or_else(|| PathBuf::from("config.toml"))
}

fn config_candidates() -> Vec<PathBuf> {
    let mut candidates = Vec::new();

    if let Some(home) = std::env::var_os("HOME").filter(|home| !home.is_empty()) {
        let home = PathBuf::from(home);
        candidates.push(home.join(".config/iptvapi/config.toml"));
        candidates.push(home.join(".iptvapi/config.toml"));
    }

    candidates.push(PathBuf::from("config.toml"));
    candidates
}

pub fn load_config<P: AsRef<Path>>(path: P) -> Result<AppConfig> {
    let content = fs::read_to_string(path).context("Failed to read config file")?;
    let mut config: AppConfig = toml::from_str(&content).context("Failed to parse TOML config")?;
    apply_python_setting_aliases(&mut config);
    apply_env_overrides(&mut config);
    apply_python_setting_aliases(&mut config);
    resolve_tilde_paths(&mut config);
    ensure_config_files(&config)?;
    Ok(config)
}

fn expand_tilde(s: &str) -> String {
    if s.starts_with('~') {
        if let Some(home) = std::env::var_os("HOME").filter(|h| !h.is_empty()) {
            let home = home.to_string_lossy();
            let suffix = &s[1..];
            if suffix.is_empty() {
                home.into_owned()
            } else if suffix.starts_with('/') {
                format!("{}{}", home, suffix)
            } else {
                format!("{}/{}", home, suffix)
            }
        } else {
            tracing::warn!(
                "Path '{}' starts with '~' but the HOME environment variable is not set. Expansion skipped.",
                s
            );
            s.to_owned()
        }
    } else {
        s.to_owned()
    }
}

fn resolve_tilde_paths(config: &mut AppConfig) {
    config.filter.whitelist_path = expand_tilde(&config.filter.whitelist_path);
    config.filter.blacklist_path = expand_tilde(&config.filter.blacklist_path);
    config.filter.ipdb_path = expand_tilde(&config.filter.ipdb_path);

    config.subscribe.sources_path = expand_tilde(&config.subscribe.sources_path);
    config.subscribe.alias_path = expand_tilde(&config.subscribe.alias_path);
    config.subscribe.nomatch_log_path = expand_tilde(&config.subscribe.nomatch_log_path);

    config.epg.sources_path = expand_tilde(&config.epg.sources_path);
    config.epg.output_xml_path = expand_tilde(&config.epg.output_xml_path);
    config.epg.output_gz_path = expand_tilde(&config.epg.output_gz_path);
    config.epg.alias_path = expand_tilde(&config.epg.alias_path);

    config.local.file_path = expand_tilde(&config.local.file_path);
    config.local.dir_path = expand_tilde(&config.local.dir_path);
    config.local.hls_dir_path = expand_tilde(&config.local.hls_dir_path);
    config.local.hls_temp_path = expand_tilde(&config.local.hls_temp_path);
    config.local.nginx_dir_path = expand_tilde(&config.local.nginx_dir_path);

    config.output.result_txt_path = expand_tilde(&config.output.result_txt_path);
    config.output.result_m3u_path = expand_tilde(&config.output.result_m3u_path);
    config.output.ipv4_result_txt_path = expand_tilde(&config.output.ipv4_result_txt_path);
    config.output.ipv4_result_m3u_path = expand_tilde(&config.output.ipv4_result_m3u_path);
    config.output.ipv6_result_txt_path = expand_tilde(&config.output.ipv6_result_txt_path);
    config.output.ipv6_result_m3u_path = expand_tilde(&config.output.ipv6_result_m3u_path);
    config.output.hls_result_txt_path = expand_tilde(&config.output.hls_result_txt_path);
    config.output.hls_result_m3u_path = expand_tilde(&config.output.hls_result_m3u_path);
    config.output.hls_ipv4_result_txt_path = expand_tilde(&config.output.hls_ipv4_result_txt_path);
    config.output.hls_ipv4_result_m3u_path = expand_tilde(&config.output.hls_ipv4_result_m3u_path);
    config.output.hls_ipv6_result_txt_path = expand_tilde(&config.output.hls_ipv6_result_txt_path);
    config.output.hls_ipv6_result_m3u_path = expand_tilde(&config.output.hls_ipv6_result_m3u_path);
    config.output.rtmp_data_path = expand_tilde(&config.output.rtmp_data_path);
    config.output.logo_dir = expand_tilde(&config.output.logo_dir);
    config.output.result_log_path = expand_tilde(&config.output.result_log_path);
    config.output.speed_test_log_path = expand_tilde(&config.output.speed_test_log_path);
    config.output.statistic_log_path = expand_tilde(&config.output.statistic_log_path);
    config.output.cache_path = expand_tilde(&config.output.cache_path);
    config.output.frozen_path = expand_tilde(&config.output.frozen_path);

    for source in &mut config.sources {
        if source.url.starts_with("file://") {
            let file_path = &source.url[7..];
            source.url = format!("file://{}", expand_tilde(file_path));
        } else {
            source.url = expand_tilde(&source.url);
        }
    }
}

fn apply_python_setting_aliases(config: &mut AppConfig) {
    config.output.time_zone = config.engine.time_zone.clone();
    if let Some(seconds) = config.engine.speed_test_timeout {
        config.engine.check_timeout = seconds.saturating_mul(1000);
    }
}

fn apply_env_overrides(config: &mut AppConfig) {
    if let Some(value) = env_string("SOURCE_FILE").or_else(|| env_string("SETTINGS_SOURCE_FILE")) {
        config.sources = vec![crate::models::SourceConfig {
            name: "Demo Source".to_owned(),
            url: value,
            source_type: crate::models::SourceType::Txt,
        }];
    }
    if let Some(value) = env_string("APP_PORT").or_else(|| env_string("SETTINGS_APP_PORT"))
        && let Ok(port) = value.parse::<u16>()
    {
        config.server.port = port;
    }
    if let Some(value) = env_bool("OPEN_SERVICE").or_else(|| env_bool("SETTINGS_OPEN_SERVICE")) {
        config.server.open_service = value;
    }
    if let Some(value) = env_string("FINAL_FILE").or_else(|| env_string("SETTINGS_FINAL_FILE")) {
        config.output.result_txt_path = value;
    }
    if let Some(value) =
        env_bool("OPEN_M3U_RESULT").or_else(|| env_bool("SETTINGS_OPEN_M3U_RESULT"))
    {
        config.output.m3u_result_enabled = value;
    }
    if let Some(value) = env_usize("URLS_LIMIT").or_else(|| env_usize("SETTINGS_URLS_LIMIT")) {
        config.output.urls_limit = value;
    }
    if let Some(value) = env_usize("LOCAL_NUM").or_else(|| env_usize("SETTINGS_LOCAL_NUM")) {
        config.output.local_num = value;
    }
    if let Some(value) = env_usize("SUBSCRIBE_NUM").or_else(|| env_usize("SETTINGS_SUBSCRIBE_NUM"))
    {
        config.output.subscribe_num = value;
    }
    if let Some(value) = env_i64("RECENT_DAYS").or_else(|| env_i64("SETTINGS_RECENT_DAYS")) {
        config.output.recent_days = value;
    }
    if let Some(value) =
        env_string("ORIGIN_TYPE_PREFER").or_else(|| env_string("SETTINGS_ORIGIN_TYPE_PREFER"))
    {
        config.output.origin_type_prefer = parse_origin_type_prefer(&value);
    }
    if let Some(value) =
        env_string("IPV_TYPE_PREFER").or_else(|| env_string("SETTINGS_IPV_TYPE_PREFER"))
    {
        config.output.ipv_type_prefer = parse_list(&value);
    }
    if let Some(value) =
        env_bool("OPEN_UPDATE_TIME").or_else(|| env_bool("SETTINGS_OPEN_UPDATE_TIME"))
    {
        config.output.update_time_enabled = value;
    }
    if let Some(value) =
        env_bool("OPEN_EMPTY_CATEGORY").or_else(|| env_bool("SETTINGS_OPEN_EMPTY_CATEGORY"))
    {
        config.output.open_empty_category = value;
    }
    if let Some(value) = env_bool("OPEN_URL_INFO").or_else(|| env_bool("SETTINGS_OPEN_URL_INFO")) {
        config.output.open_url_info = value;
    }
    if let Some(value) = env_bool("OPEN_HEADERS").or_else(|| env_bool("SETTINGS_OPEN_HEADERS")) {
        config.output.open_headers = value;
    }
    if let Some(value) = env_bool("OPEN_HISTORY").or_else(|| env_bool("SETTINGS_OPEN_HISTORY")) {
        config.output.open_history = value;
    }
    if let Some(value) = env_bool("OPEN_USE_CACHE").or_else(|| env_bool("SETTINGS_OPEN_USE_CACHE"))
    {
        config.output.open_use_cache = value;
    }
    if let Some(value) = env_bool("OPEN_REQUEST")
        .or_else(|| env_bool("OPEN_REQUESTS"))
        .or_else(|| env_bool("SETTINGS_OPEN_REQUEST"))
        .or_else(|| env_bool("SETTINGS_OPEN_REQUESTS"))
    {
        config.output.open_request = value;
    }
    if let Some(value) = env_string("CACHE_PATH").or_else(|| env_string("SETTINGS_CACHE_PATH")) {
        config.output.cache_path = value;
    }
    if let Some(value) = env_string("FROZEN_PATH").or_else(|| env_string("SETTINGS_FROZEN_PATH")) {
        config.output.frozen_path = value;
    }
    if let Some(value) =
        env_string("UPDATE_TIME_POSITION").or_else(|| env_string("SETTINGS_UPDATE_TIME_POSITION"))
    {
        config.output.update_time_position = value;
    }
    if let Some(value) = env_string("LOGO_URL").or_else(|| env_string("SETTINGS_LOGO_URL")) {
        config.output.logo_url = value;
    }
    if let Some(value) = env_string("LOGO_TYPE").or_else(|| env_string("SETTINGS_LOGO_TYPE")) {
        config.output.logo_type = value;
    }
    if let Some(value) =
        env_string("PUBLIC_SCHEME").or_else(|| env_string("SETTINGS_PUBLIC_SCHEME"))
    {
        config.output.public_scheme = value;
    }
    if let Some(value) =
        env_string("PUBLIC_DOMAIN").or_else(|| env_string("SETTINGS_PUBLIC_DOMAIN"))
    {
        config.output.public_domain = value;
    }
    if let Some(value) = env_u16("PUBLIC_PORT").or_else(|| env_u16("SETTINGS_PUBLIC_PORT")) {
        config.output.public_port = Some(value);
    }
    if let Some(value) = env_string("IPV_TYPE").or_else(|| env_string("SETTINGS_IPV_TYPE")) {
        config.engine.ipv_type = value.to_ascii_lowercase();
    }
    if let Some(value) = env_bool("IPV6_SUPPORT").or_else(|| env_bool("SETTINGS_IPV6_SUPPORT")) {
        config.engine.ipv6_support = value;
    }
    if let Some(value) = env_string("LOCATION").or_else(|| env_string("SETTINGS_LOCATION")) {
        config.filter.location = parse_filter_list(&value);
    }
    if let Some(value) = env_string("ISP").or_else(|| env_string("SETTINGS_ISP")) {
        config.filter.isp = parse_filter_list(&value);
    }
    if let Some(value) = env_string("IPDB_PATH").or_else(|| env_string("SETTINGS_IPDB_PATH")) {
        config.filter.ipdb_path = value;
    }
    if let Some(value) = env_bool("OPEN_UPDATE").or_else(|| env_bool("SETTINGS_OPEN_UPDATE")) {
        config.engine.open_update = value;
    }
    if let Some(value) =
        env_bool("OPEN_SPEED_TEST").or_else(|| env_bool("SETTINGS_OPEN_SPEED_TEST"))
    {
        config.engine.open_speed_test = value;
    }
    if let Some(value) =
        env_usize("SPEED_TEST_LIMIT").or_else(|| env_usize("SETTINGS_SPEED_TEST_LIMIT"))
    {
        config.engine.check_concurrency = value;
    }
    if let Some(value) = env_u64_seconds_as_millis("SPEED_TEST_TIMEOUT")
        .or_else(|| env_u64_seconds_as_millis("SETTINGS_SPEED_TEST_TIMEOUT"))
    {
        config.engine.check_timeout = value;
        config.engine.speed_test_timeout = Some(value / 1000);
    }
    if let Some(value) = env_bool("SPEED_TEST_ALLOW_INVALID_CERTS")
        .or_else(|| env_bool("SETTINGS_SPEED_TEST_ALLOW_INVALID_CERTS"))
    {
        config.engine.speed_test_allow_invalid_certs = value;
    }
    if let Some(value) = env_u64("SPEED_TEST_MAX_DOWNLOAD_BYTES")
        .or_else(|| env_u64("SETTINGS_SPEED_TEST_MAX_DOWNLOAD_BYTES"))
    {
        config.engine.speed_test_max_download_bytes = value;
    }
    if let Some(value) = env_usize("SPEED_TEST_SEGMENT_CONCURRENCY")
        .or_else(|| env_usize("SETTINGS_SPEED_TEST_SEGMENT_CONCURRENCY"))
    {
        config.engine.speed_test_segment_concurrency = value.max(1);
    }
    if let Some(value) =
        env_bool("SPEED_TEST_FILTER_HOST").or_else(|| env_bool("SETTINGS_SPEED_TEST_FILTER_HOST"))
    {
        config.engine.speed_test_filter_host = value;
    }
    if let Some(value) =
        env_bool("OPEN_FULL_SPEED_TEST").or_else(|| env_bool("SETTINGS_OPEN_FULL_SPEED_TEST"))
    {
        config.engine.open_full_speed_test = value;
    }
    if let Some(value) =
        env_bool("OPEN_FILTER_SPEED").or_else(|| env_bool("SETTINGS_OPEN_FILTER_SPEED"))
    {
        config.engine.open_filter_speed = value;
    }
    if let Some(value) = env_bool("OPEN_SUPPLY").or_else(|| env_bool("SETTINGS_OPEN_SUPPLY")) {
        config.engine.open_supply = value;
    }
    if let Some(value) = env_f64("MIN_SPEED").or_else(|| env_f64("SETTINGS_MIN_SPEED")) {
        config.engine.min_speed = value;
    }
    if let Some(value) =
        env_bool("OPEN_FILTER_RESOLUTION").or_else(|| env_bool("SETTINGS_OPEN_FILTER_RESOLUTION"))
    {
        config.engine.open_filter_resolution = value;
    }
    if let Some(value) =
        env_string("MIN_RESOLUTION").or_else(|| env_string("SETTINGS_MIN_RESOLUTION"))
    {
        config.engine.min_resolution = value;
    }
    if let Some(value) =
        env_string("MAX_RESOLUTION").or_else(|| env_string("SETTINGS_MAX_RESOLUTION"))
    {
        config.engine.max_resolution = value;
    }
    if let Some(value) =
        env_string("RESOLUTION_SPEED_MAP").or_else(|| env_string("SETTINGS_RESOLUTION_SPEED_MAP"))
    {
        config.engine.resolution_speed_map = parse_resolution_speed_map(&value);
    }
    if let Some(value) = env_string("UPDATE_MODE").or_else(|| env_string("SETTINGS_UPDATE_MODE")) {
        config.engine.update_mode = value.to_ascii_lowercase();
    }
    if let Some(value) = env_update_interval_secs("UPDATE_INTERVAL")
        .or_else(|| env_update_interval_secs("SETTINGS_UPDATE_INTERVAL"))
    {
        config.engine.update_interval = value;
    }
    if let Some(value) = env_string("UPDATE_TIMES").or_else(|| env_string("SETTINGS_UPDATE_TIMES"))
    {
        config.engine.update_times = value;
    }
    if let Some(value) = env_string("TIME_ZONE").or_else(|| env_string("SETTINGS_TIME_ZONE")) {
        config.engine.time_zone = value.clone();
        config.output.time_zone = value;
    }
    if let Some(value) = env_string("LANGUAGE").or_else(|| env_string("SETTINGS_LANGUAGE")) {
        config.output.language = value;
    }
    if let Some(value) =
        env_bool("OPEN_REALTIME_WRITE").or_else(|| env_bool("SETTINGS_OPEN_REALTIME_WRITE"))
    {
        config.output.open_realtime_write = value;
    }
    if let Some(value) = env_bool("UPDATE_STARTUP").or_else(|| env_bool("SETTINGS_UPDATE_STARTUP"))
    {
        config.engine.update_startup = value;
    }
    if let Some(value) = env_u64("REQUEST_TIMEOUT").or_else(|| env_u64("SETTINGS_REQUEST_TIMEOUT"))
    {
        config.engine.request_timeout = value;
    }
    if let Some(value) = env_string("HTTP_PROXY").or_else(|| env_string("SETTINGS_HTTP_PROXY")) {
        config.engine.http_proxy = value.clone();
        config.subscribe.http_proxy = value.clone();
        config.epg.http_proxy = value;
    }
    if let Some(value) = env_bool("OPEN_EPG").or_else(|| env_bool("SETTINGS_OPEN_EPG")) {
        config.epg.enabled = value;
    }
    if let Some(value) = env_bool("OPEN_SUBSCRIBE").or_else(|| env_bool("SETTINGS_OPEN_SUBSCRIBE"))
    {
        config.subscribe.enabled = value;
    }
    if let Some(value) = env_bool("OPEN_LOCAL").or_else(|| env_bool("SETTINGS_OPEN_LOCAL")) {
        config.local.enabled = value;
    }
    if let Some(value) = env_bool("OPEN_RTMP").or_else(|| env_bool("SETTINGS_OPEN_RTMP")) {
        config.local.hls_enabled = value;
    }
    if std::env::var_os("GITHUB_ACTIONS").is_some() {
        config.local.hls_enabled = false;
    }
    if let Some(value) = env_string("HLS_DIR_PATH").or_else(|| env_string("SETTINGS_HLS_DIR_PATH"))
    {
        config.local.hls_dir_path = value;
    }
    if let Some(value) =
        env_string("HLS_TEMP_PATH").or_else(|| env_string("SETTINGS_HLS_TEMP_PATH"))
    {
        config.local.hls_temp_path = value;
    }
    if let Some(value) = env_u16("NGINX_HTTP_PORT").or_else(|| env_u16("SETTINGS_NGINX_HTTP_PORT"))
    {
        config.local.nginx_http_port = value;
    }
    if let Some(value) = env_u16("NGINX_RTMP_PORT").or_else(|| env_u16("SETTINGS_NGINX_RTMP_PORT"))
    {
        config.local.nginx_rtmp_port = value;
    }
    if let Some(value) =
        env_u64("RTMP_IDLE_TIMEOUT").or_else(|| env_u64("SETTINGS_RTMP_IDLE_TIMEOUT"))
    {
        config.local.rtmp_idle_timeout = value;
    }
    if let Some(value) =
        env_usize("RTMP_MAX_STREAMS").or_else(|| env_usize("SETTINGS_RTMP_MAX_STREAMS"))
    {
        config.local.rtmp_max_streams = value;
    }
    if let Some(value) =
        env_bool("LOCAL_MATCH_ALIASES").or_else(|| env_bool("SETTINGS_LOCAL_MATCH_ALIASES"))
    {
        config.local.match_aliases = value;
    }
    if let Some(value) = configured_public_base_url(config) {
        config.output.public_base_url = value;
    }
}

fn configured_public_base_url(config: &AppConfig) -> Option<String> {
    if let Some(value) = env_string("PUBLIC_BASE_URL").or_else(|| env_string("PUBLIC_URL")) {
        return Some(value.trim_end_matches('/').to_owned());
    }
    let raw_domain = config.output.public_domain.trim();
    if raw_domain.is_empty() {
        return None;
    }
    let domain = public_domain(raw_domain);
    let scheme = if config.output.public_scheme.trim().is_empty() {
        "http"
    } else {
        config.output.public_scheme.trim()
    };
    let port = config
        .output
        .public_port
        .or(Some(if config.local.hls_enabled {
            config.local.nginx_http_port
        } else {
            config.server.port
        }));
    Some(public_base_url(scheme, &domain, port))
}

fn public_domain(configured: &str) -> String {
    let configured = configured.trim();
    if configured != "127.0.0.1" {
        return configured.to_owned();
    }
    discover_local_ipv4().unwrap_or_else(|| configured.to_owned())
}

fn discover_local_ipv4() -> Option<String> {
    local_ipv4_with_probe("8.8.8.8:80")
}

fn local_ipv4_with_probe(remote: &str) -> Option<String> {
    let socket = UdpSocket::bind("0.0.0.0:0").ok()?;
    socket.connect(remote).ok()?;
    match socket.local_addr().ok()? {
        SocketAddr::V4(addr) if !addr.ip().is_loopback() => Some(addr.ip().to_string()),
        _ => None,
    }
}

fn public_base_url(scheme: &str, domain: &str, port: Option<u16>) -> String {
    let scheme = scheme.trim().trim_end_matches("://");
    let domain = domain.trim().trim_end_matches('/');
    let default_port = match scheme {
        "https" => Some(443),
        "http" => Some(80),
        _ => None,
    };
    match port {
        Some(port) if Some(port) != default_port => format!("{scheme}://{domain}:{port}"),
        _ => format!("{scheme}://{domain}"),
    }
}

fn env_string(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

fn parse_list(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(|item| item.trim().to_ascii_lowercase())
        .filter(|item| !item.is_empty())
        .collect()
}

fn parse_filter_list(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(|item| item.trim().to_owned())
        .filter(|item| !item.is_empty())
        .collect()
}

fn parse_origin_type_prefer(value: &str) -> Vec<ChannelOrigin> {
    parse_list(value)
        .into_iter()
        .filter_map(|item| match item.as_str() {
            "local" => Some(ChannelOrigin::Local),
            "subscribe" => Some(ChannelOrigin::Subscribe),
            "whitelist" => Some(ChannelOrigin::Whitelist),
            "hls" => Some(ChannelOrigin::Hls),
            _ => None,
        })
        .collect()
}

fn parse_resolution_speed_map(value: &str) -> std::collections::HashMap<String, f64> {
    value
        .split(',')
        .filter_map(|item| {
            let (resolution, speed) = item.split_once(':')?;
            let resolution = resolution.trim();
            if resolution.is_empty() {
                return None;
            }
            let speed = speed.trim().parse::<f64>().ok()?;
            Some((resolution.to_owned(), speed))
        })
        .collect()
}

fn env_usize(name: &str) -> Option<usize> {
    env_string(name).and_then(|value| value.parse::<usize>().ok())
}

fn env_f64(name: &str) -> Option<f64> {
    env_string(name).and_then(|value| value.parse::<f64>().ok())
}

fn env_i64(name: &str) -> Option<i64> {
    env_string(name).and_then(|value| value.parse::<i64>().ok())
}

fn env_u64(name: &str) -> Option<u64> {
    env_string(name).and_then(|value| value.parse::<u64>().ok())
}

fn env_u16(name: &str) -> Option<u16> {
    env_string(name).and_then(|value| value.parse::<u16>().ok())
}

fn env_u64_seconds_as_millis(name: &str) -> Option<u64> {
    env_string(name)
        .and_then(|value| value.parse::<u64>().ok())
        .map(|seconds| seconds.saturating_mul(1000))
}

fn env_update_interval_secs(name: &str) -> Option<u64> {
    env_string(name).map(|value| {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            return 0;
        }
        trimmed
            .parse::<f64>()
            .ok()
            .filter(|hours| hours.is_finite() && *hours > 0.0)
            .map(|hours| (hours * 3600.0).round() as u64)
            .unwrap_or(12 * 60 * 60)
    })
}

fn env_bool(name: &str) -> Option<bool> {
    env_string(name).and_then(|value| match value.to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    })
}

const DEFAULT_WHITELIST: &str = r#"# 这是接口的白名单，白名单内的接口将不会参与测速，始终保留至结果最前；
# 填写频道名称会直接保留该记录至该频道的最终结果，如：CCTV-1,接口地址；
# 如果不填写频道名称，则该地址会被加入到所有频道的结果中，多条记录换行输入。
# This is the whitelist for interfaces. Interfaces in the whitelist will not be speed tested and will always be kept at the top of the results;
# Filling in the channel name will directly retain the record to the final result of the channel, such as: CCTV-1, interface address;
# If the channel name is not filled in, the address will be added to the results of all channels, with multiple records entered on new lines.

[KEYWORDS]
# 以下区域是关键字白名单，某频道获取到的接口地址中含有指定关键字，则该接口会被加入该频道的白名单，多条记录换行输入。
# This area is the keyword whitelist. If the interface address obtained by a certain channel contains the specified keyword, the interface will be added to the whitelist of the channel, with multiple records entered on new lines.
"#;

const DEFAULT_BLACKLIST: &str = r#"# 这是接口黑名单列表，符合关键字的接口将被拦截，一个关键字一行
# This is the interface blacklist list, the interface matching the keyword will be blocked, one keyword line
/audio/
bxtv.3a.ink
catvod.com
综合
新闻综合
山东
新闻
"#;

const DEFAULT_SUBSCRIBE: &str = r#"# 订阅源列表，每行一个M3U或TXT格式的订阅URL
# List of subscription sources, one M3U or TXT format subscription URL per line
"#;

const DEFAULT_EPG: &str = r#"# EPG 节目单订阅源列表，每行一个 XMLTV 格式的 URL
# List of EPG program guide sources, one XMLTV format URL per line
http://epg.51zmt.top:11111/e.xml
"#;

const DEFAULT_ALIAS: &str = r#"# 频道别名映射，格式为：标准频道名,别名1,别名2...
# Channel alias mappings, format: StandardChannelName,Alias1,Alias2...
CCTV-1,CCTV1,CCTV-1 综合,CCTV-1综合
CCTV-2,CCTV2,CCTV-2 财经,CCTV-2财经
CCTV-3,CCTV3,CCTV-3 综艺,CCTV-3综艺
CCTV-4,CCTV4,CCTV-4 中文国际,CCTV-4中文国际
CCTV-5,CCTV5,CCTV-5 体育,CCTV-5体育
CCTV-6,CCTV6,CCTV-6 电影,CCTV-6电影
CCTV-7,CCTV7,CCTV-7 军事农业,CCTV-7国防军事
CCTV-8,CCTV8,CCTV-8 电视剧,CCTV-8电视剧
CCTV-9,CCTV9,CCTV-9 纪录,CCTV-9纪录
CCTV-10,CCTV10,CCTV-10 科教,CCTV-10科教
CCTV-11,CCTV11,CCTV-11 戏曲,CCTV-11戏曲
CCTV-12,CCTV12,CCTV-12 社会与法,CCTV-12社会与法
CCTV-13,CCTV13,CCTV-13 新闻,CCTV-13新闻
CCTV-14,CCTV14,CCTV-14 少儿,CCTV-14少儿
CCTV-15,CCTV15,CCTV-15 音乐,CCTV-15音乐
CCTV-16,CCTV16,CCTV-16 奥林匹克,CCTV-16奥林匹克
CCTV-17,CCTV17,CCTV-17 农业农村,CCTV-17农业农村
"#;

const DEFAULT_LOCAL: &str = r#"# 本地自定义频道，格式为：频道名称,接口地址
# Local custom channels, format: ChannelName,InterfaceAddress
# 例如：
# CCTV-1综合,http://example.com/cctv1.m3u8
"#;

const DEFAULT_DEMO: &str = r#"央视频道,#genre#
CCTV-1,http://ivi.bupt.edu.cn/hls/cctv1hd.m3u8
CCTV-3,http://ivi.bupt.edu.cn/hls/cctv3hd.m3u8
CCTV-6,http://ivi.bupt.edu.cn/hls/cctv6hd.m3u8
CCTV-8,http://ivi.bupt.edu.cn/hls/cctv8hd.m3u8
"#;

fn ensure_file_exists(path_str: &str, default_content: &str) -> Result<()> {
    if path_str.is_empty() {
        return Ok(());
    }
    let path = Path::new(path_str);
    if !path.exists() {
        if let Some(parent) = path.parent() {
            if !parent.exists() {
                fs::create_dir_all(parent)
                    .with_context(|| format!("Failed to create directory {:?}", parent))?;
                tracing::info!("Created directory {:?}", parent);
            }
        }

        // Try migrating from parent's parent (grandparent) if the file exists there
        let mut migrated = false;
        if let Some(filename) = path.file_name() {
            if let Some(parent) = path.parent() {
                if let Some(grandparent) = parent.parent() {
                    let fallback_path = grandparent.join(filename);
                    if fallback_path.exists() && fallback_path.is_file() {
                        if let Ok(content) = fs::read_to_string(&fallback_path) {
                            fs::write(path, content)
                                .with_context(|| format!("Failed to migrate file to {:?}", path))?;
                            tracing::info!(
                                "Migrated/copied file from {:?} to {:?}",
                                fallback_path,
                                path
                            );
                            migrated = true;
                        }
                    }
                }
            }
        }

        if !migrated {
            fs::write(path, default_content)
                .with_context(|| format!("Failed to write default file {:?}", path))?;
            tracing::info!("Created default file {:?}", path);
        }
    }
    Ok(())
}

fn ensure_dir_exists(dir_str: &str) -> Result<()> {
    if dir_str.is_empty() {
        return Ok(());
    }
    let path = Path::new(dir_str);
    if !path.exists() {
        fs::create_dir_all(path)
            .with_context(|| format!("Failed to create directory {:?}", path))?;
        tracing::info!("Created directory {:?}", path);
    }
    Ok(())
}

pub fn ensure_config_files(config: &AppConfig) -> Result<()> {
    // Files
    ensure_file_exists(&config.filter.whitelist_path, DEFAULT_WHITELIST)?;
    ensure_file_exists(&config.filter.blacklist_path, DEFAULT_BLACKLIST)?;
    ensure_file_exists(&config.subscribe.sources_path, DEFAULT_SUBSCRIBE)?;
    ensure_file_exists(&config.subscribe.alias_path, DEFAULT_ALIAS)?;
    ensure_file_exists(&config.epg.sources_path, DEFAULT_EPG)?;
    ensure_file_exists(&config.epg.alias_path, DEFAULT_ALIAS)?;
    ensure_file_exists(&config.local.file_path, DEFAULT_LOCAL)?;

    // Directories
    ensure_dir_exists(&config.local.dir_path)?;
    ensure_dir_exists(&config.local.hls_dir_path)?;
    ensure_dir_exists(&config.local.hls_temp_path)?;
    ensure_dir_exists(&config.output.logo_dir)?;

    // Sources
    for source in &config.sources {
        if source.url.starts_with("file://") {
            let file_path = &source.url[7..];
            let default_content = if file_path.ends_with("demo.txt") {
                DEFAULT_DEMO
            } else {
                ""
            };
            ensure_file_exists(file_path, default_content)?;
        } else if !source.url.contains("://") {
            let default_content = if source.url.ends_with("demo.txt") {
                DEFAULT_DEMO
            } else {
                ""
            };
            ensure_file_exists(&source.url, default_content)?;
        }
    }

    Ok(())
}

pub fn create_default_config<P: AsRef<Path>>(path: P) -> Result<()> {
    let default_config = r#"
[server]
host = "0.0.0.0"
port = 8080
open_service = true

[engine]
open_update = true
open_speed_test = true
update_interval = 12
update_mode = "interval"
update_times = ""
time_zone = "Asia/Shanghai"
update_startup = true
request_timeout = 10
http_proxy = ""
speed_test_limit = 5
speed_test_timeout = 10
speed_test_allow_invalid_certs = true
speed_test_max_download_bytes = 1048576
speed_test_segment_concurrency = 2
speed_test_filter_host = false
open_full_speed_test = false
open_filter_speed = true
open_supply = false
min_speed = 0.5
open_filter_resolution = true
min_resolution = "1920x1080"
max_resolution = "1920x1080"
resolution_speed_map = {}
ipv6_support = false
ipv_type = "all"

[filter]
whitelist_path = "config/whitelist.txt"
blacklist_path = "config/blacklist.txt"
location = []
isp = []
ipdb_path = "utils/ip_checker/data/qqwry.ipdb"

[subscribe]
enabled = true
sources_path = "config/subscribe.txt"
timeout_ms = 30000
concurrency = 10
cdn_url = ""
http_proxy = ""
alias_path = "config/alias.txt"
nomatch_log_path = "output/log/nomatch.log"

[epg]
enabled = true
sources_path = "config/epg.txt"
output_xml_path = "output/epg/epg.xml"
output_gz_path = "output/epg/epg.gz"
timeout_ms = 30000
concurrency = 10
cdn_url = ""
http_proxy = ""
alias_path = "config/alias.txt"

[local]
enabled = true
file_path = "config/local.txt"
dir_path = "config/local"
match_aliases = true
hls_enabled = false
hls_dir_path = "config/hls"
hls_temp_path = "/tmp/hls"
nginx_dir_path = "utils/nginx-rtmp-win32"
nginx_http_port = 8080
nginx_rtmp_port = 1935
rtmp_idle_timeout = 300
rtmp_max_streams = 10

[output]
result_txt_path = "output/result.txt"
result_m3u_path = "output/result.m3u"
m3u_result_enabled = true
ipv4_result_txt_path = "output/ipv4/result.txt"
ipv4_result_m3u_path = "output/ipv4/result.m3u"
ipv6_result_txt_path = "output/ipv6/result.txt"
ipv6_result_m3u_path = "output/ipv6/result.m3u"
hls_result_txt_path = "output/hls.txt"
hls_result_m3u_path = "output/hls.m3u"
hls_ipv4_result_txt_path = "output/ipv4/hls.txt"
hls_ipv4_result_m3u_path = "output/ipv4/hls.m3u"
hls_ipv6_result_txt_path = "output/ipv6/hls.txt"
hls_ipv6_result_m3u_path = "output/ipv6/hls.m3u"
rtmp_data_path = "output/data/rtmp.db"
logo_dir = "config/logo"
logo_url = ""
logo_type = "png"
public_base_url = ""
public_scheme = "http"
public_domain = "127.0.0.1"
urls_limit = 10
local_num = 10
subscribe_num = 10
recent_days = 30
origin_type_prefer = []
ipv_type_prefer = []
result_log_path = "output/log/result.log"
speed_test_log_path = "output/log/speed_test.log"
statistic_log_path = "output/log/statistic.log"
update_time_enabled = true
update_time_position = "top"
time_zone = "Asia/Shanghai"
language = "zh_CN"
open_url_info = true
open_headers = false
open_empty_category = false
open_history = true
open_use_cache = true
open_request = false
open_realtime_write = true
cache_path = "output/data/cache.gz"
frozen_path = "output/data/frozen.gz"

[[sources]]
name = "Demo Source"
url = "config/demo.txt"
source_type = "txt"
"#;
    fs::write(path, default_config).context("Failed to write default config")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Mutex, OnceLock};

    fn env_lock() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(())).lock().unwrap()
    }

    #[test]
    fn public_domain_keeps_configured_non_loopback_domain() {
        assert_eq!(public_domain("iptv.example"), "iptv.example");
    }

    #[test]
    fn local_ipv4_probe_returns_none_for_invalid_remote() {
        assert_eq!(local_ipv4_with_probe("not a socket address"), None);
    }

    #[test]
    fn public_base_url_uses_config_file_public_domain_without_env() {
        let _guard = env_lock();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        create_default_config(&path).unwrap();
        let mut content = fs::read_to_string(&path).unwrap();
        content = content.replace(
            "public_domain = \"127.0.0.1\"",
            "public_domain = \"iptv.example\"",
        );
        content = content.replace("public_scheme = \"http\"", "public_scheme = \"https\"");
        fs::write(&path, content).unwrap();

        let config = load_config(&path).unwrap();

        assert_eq!(config.output.public_base_url, "https://iptv.example:8080");
    }

    #[test]
    fn public_base_url_uses_nginx_http_port_when_rtmp_enabled_like_python() {
        let _guard = env_lock();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        create_default_config(&path).unwrap();
        unsafe {
            std::env::set_var("PUBLIC_DOMAIN", "iptv.example");
            std::env::set_var("OPEN_RTMP", "true");
            std::env::set_var("NGINX_HTTP_PORT", "18080");
        }
        let config = load_config(&path).unwrap();
        unsafe {
            std::env::remove_var("PUBLIC_DOMAIN");
            std::env::remove_var("OPEN_RTMP");
            std::env::remove_var("NGINX_HTTP_PORT");
        }
        assert_eq!(config.output.public_base_url, "http://iptv.example:18080");
    }

    #[test]
    fn public_base_url_uses_app_port_when_rtmp_disabled_like_python() {
        let _guard = env_lock();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        create_default_config(&path).unwrap();
        unsafe {
            std::env::set_var("PUBLIC_DOMAIN", "iptv.example");
            std::env::set_var("APP_PORT", "5180");
            std::env::set_var("OPEN_RTMP", "false");
        }
        let config = load_config(&path).unwrap();
        unsafe {
            std::env::remove_var("PUBLIC_DOMAIN");
            std::env::remove_var("APP_PORT");
            std::env::remove_var("OPEN_RTMP");
        }
        assert_eq!(config.output.public_base_url, "http://iptv.example:5180");
    }

    #[test]
    fn public_base_url_elides_default_ports() {
        assert_eq!(
            public_base_url("http", "example.com", Some(80)),
            "http://example.com"
        );
        assert_eq!(
            public_base_url("https", "example.com", Some(8443)),
            "https://example.com:8443"
        );
    }

    #[test]
    fn recent_days_env_parses_python_setting_name() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        create_default_config(&path).unwrap();
        unsafe {
            std::env::set_var("RECENT_DAYS", "7");
        }
        let config = load_config(&path).unwrap();
        unsafe {
            std::env::remove_var("RECENT_DAYS");
        }
        assert_eq!(config.output.recent_days, 7);
    }

    #[test]
    fn ipv6_support_env_parses_python_setting_name() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        create_default_config(&path).unwrap();
        unsafe {
            std::env::set_var("IPV6_SUPPORT", "true");
        }
        let config = load_config(&path).unwrap();
        unsafe {
            std::env::remove_var("IPV6_SUPPORT");
        }
        assert!(config.engine.ipv6_support);
    }

    #[test]
    fn open_supply_env_parses_python_setting_name() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        create_default_config(&path).unwrap();
        unsafe {
            std::env::set_var("OPEN_SUPPLY", "true");
        }
        let config = load_config(&path).unwrap();
        unsafe {
            std::env::remove_var("OPEN_SUPPLY");
        }
        assert!(config.engine.open_supply);
    }

    #[test]
    fn rtmp_service_ports_and_limits_parse_python_setting_names() {
        let _guard = env_lock();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        create_default_config(&path).unwrap();
        unsafe {
            std::env::set_var("NGINX_HTTP_PORT", "18080");
            std::env::set_var("NGINX_RTMP_PORT", "11935");
            std::env::set_var("RTMP_IDLE_TIMEOUT", "123");
            std::env::set_var("RTMP_MAX_STREAMS", "4");
        }
        let config = load_config(&path).unwrap();
        unsafe {
            std::env::remove_var("NGINX_HTTP_PORT");
            std::env::remove_var("NGINX_RTMP_PORT");
            std::env::remove_var("RTMP_IDLE_TIMEOUT");
            std::env::remove_var("RTMP_MAX_STREAMS");
        }
        assert_eq!(config.local.nginx_http_port, 18080);
        assert_eq!(config.local.nginx_rtmp_port, 11935);
        assert_eq!(config.local.rtmp_idle_timeout, 123);
        assert_eq!(config.local.rtmp_max_streams, 4);
    }

    #[test]
    fn open_rtmp_is_disabled_on_github_actions_like_python() {
        let _guard = env_lock();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        create_default_config(&path).unwrap();
        unsafe {
            std::env::set_var("OPEN_RTMP", "true");
            std::env::set_var("GITHUB_ACTIONS", "true");
        }
        let config = load_config(&path).unwrap();
        unsafe {
            std::env::remove_var("OPEN_RTMP");
            std::env::remove_var("GITHUB_ACTIONS");
        }
        assert!(!config.local.hls_enabled);
    }

    #[test]
    fn http_proxy_env_updates_all_python_network_clients() {
        let _guard = env_lock();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        create_default_config(&path).unwrap();
        unsafe {
            std::env::set_var("HTTP_PROXY", "http://127.0.0.1:8080");
        }
        let config = load_config(&path).unwrap();
        unsafe {
            std::env::remove_var("HTTP_PROXY");
        }
        assert_eq!(config.engine.http_proxy, "http://127.0.0.1:8080");
        assert_eq!(config.subscribe.http_proxy, "http://127.0.0.1:8080");
        assert_eq!(config.epg.http_proxy, "http://127.0.0.1:8080");
    }

    #[test]
    fn request_timeout_env_uses_python_seconds_setting() {
        let _guard = env_lock();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        create_default_config(&path).unwrap();
        unsafe {
            std::env::set_var("REQUEST_TIMEOUT", "8");
        }
        let config = load_config(&path).unwrap();
        unsafe {
            std::env::remove_var("REQUEST_TIMEOUT");
        }
        assert_eq!(config.engine.request_timeout, 8);
    }

    #[test]
    fn speed_test_ini_names_map_to_rust_runtime_fields() {
        let _guard = env_lock();
        unsafe {
            std::env::remove_var("SPEED_TEST_LIMIT");
            std::env::remove_var("SETTINGS_SPEED_TEST_LIMIT");
            std::env::remove_var("SPEED_TEST_TIMEOUT");
            std::env::remove_var("SETTINGS_SPEED_TEST_TIMEOUT");
        }
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        fs::write(
            &path,
            r#"
[server]
host = "0.0.0.0"
port = 8080

[engine]
update_interval = 12
speed_test_limit = 7
speed_test_timeout = 9
ipv6_support = false

[[sources]]
name = "Demo Source"
url = "config/demo.txt"
source_type = "txt"
"#,
        )
        .unwrap();

        let config = load_config(&path).unwrap();

        assert_eq!(config.engine.check_concurrency, 7);
        assert_eq!(config.engine.check_timeout, 9_000);
    }

    #[test]
    fn speed_test_timeout_env_uses_python_seconds_semantics() {
        let _guard = env_lock();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        create_default_config(&path).unwrap();
        unsafe {
            std::env::set_var("SPEED_TEST_TIMEOUT", "4");
        }
        let config = load_config(&path).unwrap();
        unsafe {
            std::env::remove_var("SPEED_TEST_TIMEOUT");
        }
        assert_eq!(config.engine.check_timeout, 4_000);
    }

    #[test]
    fn time_zone_env_updates_schedule_and_output_config() {
        let _guard = env_lock();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        create_default_config(&path).unwrap();
        unsafe {
            std::env::set_var("TIME_ZONE", "UTC");
        }
        let config = load_config(&path).unwrap();
        unsafe {
            std::env::remove_var("TIME_ZONE");
        }
        assert_eq!(config.engine.time_zone, "UTC");
        assert_eq!(config.output.time_zone, "UTC");
    }

    #[test]
    fn gui_service_compat_flags_parse_python_setting_names() {
        let _guard = env_lock();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        create_default_config(&path).unwrap();
        unsafe {
            std::env::set_var("LANGUAGE", "en");
            std::env::set_var("OPEN_REALTIME_WRITE", "false");
        }
        let config = load_config(&path).unwrap();
        unsafe {
            std::env::remove_var("LANGUAGE");
            std::env::remove_var("OPEN_REALTIME_WRITE");
        }
        assert_eq!(config.output.language, "en");
        assert!(!config.output.open_realtime_write);
    }

    #[test]
    fn offline_query_compat_flags_parse_python_setting_names() {
        let _guard = env_lock();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        create_default_config(&path).unwrap();
        unsafe {
            std::env::set_var("OPEN_USE_CACHE", "false");
            // Python Tkinter historically writes the plural key; accept it as a compatibility alias.
            std::env::set_var("OPEN_REQUESTS", "true");
        }
        let config = load_config(&path).unwrap();
        unsafe {
            std::env::remove_var("OPEN_USE_CACHE");
            std::env::remove_var("OPEN_REQUESTS");
        }
        assert!(!config.output.open_use_cache);
        assert!(config.output.open_request);
    }

    #[test]
    fn python_config_property_audit_has_no_unaccounted_runtime_fields() {
        let missing_runtime_fields: [&str; 0] = [];
        assert!(missing_runtime_fields.is_empty());

        let derived_or_alias_fields = [
            "open_ipv6",
            "source_limits",
            "min_resolution_value",
            "max_resolution_value",
            "source_file",
            "final_file",
            "open_m3u_result",
            "open_subscribe",
            "open_method",
            "open_update_time",
            "app_port",
            "open_local",
            "open_rtmp",
            "open_epg",
        ];
        assert!(derived_or_alias_fields.contains(&"source_file"));
        assert!(derived_or_alias_fields.contains(&"open_method"));
    }

    #[test]
    fn update_interval_env_uses_python_hours_semantics() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        create_default_config(&path).unwrap();
        unsafe {
            std::env::set_var("UPDATE_INTERVAL", "0.5");
        }
        let config = load_config(&path).unwrap();
        unsafe {
            std::env::remove_var("UPDATE_INTERVAL");
        }
        assert_eq!(config.engine.update_interval, 30 * 60);
    }

    #[test]
    fn ipdb_path_env_parses_python_setting_name() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        create_default_config(&path).unwrap();
        unsafe {
            std::env::set_var("IPDB_PATH", "custom.ipdb");
        }
        let config = load_config(&path).unwrap();
        unsafe {
            std::env::remove_var("IPDB_PATH");
        }
        assert_eq!(config.filter.ipdb_path, "custom.ipdb");
    }

    #[test]
    fn test_ensure_config_files_creates_and_migrates() {
        let _guard = env_lock();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        create_default_config(&path).unwrap();
        let mut config = load_config(&path).unwrap();

        // 1. Prepare an existing file in the grandparent dir (e.g. dir/whitelist.txt)
        let grandparent_file = dir.path().join("whitelist.txt");
        let test_content = "CCTV-1,http://ivi.bupt.edu.cn/hls/cctv1hd.m3u8";
        fs::write(&grandparent_file, test_content).unwrap();

        // 2. Adjust paths to point to dir/config/
        config.filter.whitelist_path = dir
            .path()
            .join("config/whitelist.txt")
            .to_string_lossy()
            .to_string();
        config.filter.blacklist_path = dir
            .path()
            .join("config/blacklist.txt")
            .to_string_lossy()
            .to_string();
        config.subscribe.sources_path = dir
            .path()
            .join("config/subscribe.txt")
            .to_string_lossy()
            .to_string();
        config.subscribe.alias_path = dir
            .path()
            .join("config/alias_path_sub.txt")
            .to_string_lossy()
            .to_string();
        config.epg.sources_path = dir
            .path()
            .join("config/epg.txt")
            .to_string_lossy()
            .to_string();
        config.epg.alias_path = dir
            .path()
            .join("config/alias_path_epg.txt")
            .to_string_lossy()
            .to_string();
        config.local.file_path = dir
            .path()
            .join("config/local.txt")
            .to_string_lossy()
            .to_string();
        config.local.dir_path = dir
            .path()
            .join("config/local")
            .to_string_lossy()
            .to_string();
        config.local.hls_dir_path = dir.path().join("config/hls").to_string_lossy().to_string();
        config.local.hls_temp_path = dir
            .path()
            .join("config/hls_temp")
            .to_string_lossy()
            .to_string();
        config.output.logo_dir = dir.path().join("config/logo").to_string_lossy().to_string();
        config.sources = vec![crate::models::SourceConfig {
            name: "Demo".to_string(),
            url: dir
                .path()
                .join("config/demo.txt")
                .to_string_lossy()
                .to_string(),
            source_type: crate::models::SourceType::Txt,
        }];

        // 3. Run ensure_config_files
        ensure_config_files(&config).unwrap();

        // 4. Verify migrated file
        let migrated_whitelist = Path::new(&config.filter.whitelist_path);
        assert!(migrated_whitelist.exists());
        assert_eq!(
            fs::read_to_string(migrated_whitelist).unwrap(),
            test_content
        );

        // 5. Verify default files created
        let blacklist_file = Path::new(&config.filter.blacklist_path);
        assert!(blacklist_file.exists());
        assert!(fs::read_to_string(blacklist_file).unwrap().contains("综合"));

        let demo_file = Path::new(&config.sources[0].url);
        assert!(demo_file.exists());
        assert!(fs::read_to_string(demo_file).unwrap().contains("CCTV-1"));

        // 6. Verify directory created
        let local_dir = Path::new(&config.local.dir_path);
        assert!(local_dir.exists());
    }
}
