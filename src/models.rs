use std::collections::HashMap;

use serde::{Deserialize, Deserializer, Serialize};

const SECONDS_PER_HOUR: f64 = 3600.0;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppConfig {
    pub server: ServerConfig,
    pub engine: EngineConfig,
    #[serde(default)]
    pub sources: Vec<SourceConfig>,
    #[serde(default)]
    pub filter: FilterConfig,
    #[serde(default)]
    pub epg: EpgConfig,
    #[serde(default)]
    pub subscribe: SubscribeConfig,
    #[serde(default)]
    pub output: OutputConfig,
    #[serde(default)]
    pub local: LocalConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalConfig {
    #[serde(default = "default_local_enabled")]
    pub enabled: bool,
    #[serde(default = "default_local_file_path")]
    pub file_path: String,
    #[serde(default = "default_local_dir_path")]
    pub dir_path: String,
    #[serde(default = "default_local_match_aliases")]
    pub match_aliases: bool,
    #[serde(default)]
    pub hls_enabled: bool,
    #[serde(default = "default_hls_dir_path")]
    pub hls_dir_path: String,
    #[serde(default = "default_hls_temp_path")]
    pub hls_temp_path: String,
    #[serde(default = "default_nginx_dir_path")]
    pub nginx_dir_path: String,
    #[serde(default = "default_nginx_http_port")]
    pub nginx_http_port: u16,
    #[serde(default = "default_nginx_rtmp_port")]
    pub nginx_rtmp_port: u16,
    #[serde(default = "default_rtmp_idle_timeout")]
    pub rtmp_idle_timeout: u64,
    #[serde(default = "default_rtmp_max_streams")]
    pub rtmp_max_streams: usize,
}

impl Default for LocalConfig {
    fn default() -> Self {
        Self {
            enabled: default_local_enabled(),
            file_path: default_local_file_path(),
            dir_path: default_local_dir_path(),
            match_aliases: default_local_match_aliases(),
            hls_enabled: false,
            hls_dir_path: default_hls_dir_path(),
            hls_temp_path: default_hls_temp_path(),
            nginx_dir_path: default_nginx_dir_path(),
            nginx_http_port: default_nginx_http_port(),
            nginx_rtmp_port: default_nginx_rtmp_port(),
            rtmp_idle_timeout: default_rtmp_idle_timeout(),
            rtmp_max_streams: default_rtmp_max_streams(),
        }
    }
}

fn default_local_enabled() -> bool {
    true
}

fn default_local_file_path() -> String {
    "config/local.txt".to_string()
}

fn default_local_dir_path() -> String {
    "config/local".to_string()
}

fn default_local_match_aliases() -> bool {
    true
}

fn default_hls_dir_path() -> String {
    "config/hls".to_string()
}

fn default_hls_temp_path() -> String {
    "/tmp/hls".to_string()
}

fn default_nginx_dir_path() -> String {
    "utils/nginx-rtmp-win32".to_string()
}

fn default_nginx_http_port() -> u16 {
    8080
}

fn default_nginx_rtmp_port() -> u16 {
    1935
}

fn default_rtmp_idle_timeout() -> u64 {
    300
}

fn default_rtmp_max_streams() -> usize {
    10
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutputConfig {
    #[serde(default = "default_result_txt_path")]
    pub result_txt_path: String,
    #[serde(default = "default_result_m3u_path")]
    pub result_m3u_path: String,
    #[serde(default = "default_m3u_result_enabled")]
    pub m3u_result_enabled: bool,
    #[serde(default = "default_ipv4_result_txt_path")]
    pub ipv4_result_txt_path: String,
    #[serde(default = "default_ipv4_result_m3u_path")]
    pub ipv4_result_m3u_path: String,
    #[serde(default = "default_ipv6_result_txt_path")]
    pub ipv6_result_txt_path: String,
    #[serde(default = "default_ipv6_result_m3u_path")]
    pub ipv6_result_m3u_path: String,
    #[serde(default = "default_hls_result_txt_path")]
    pub hls_result_txt_path: String,
    #[serde(default = "default_hls_result_m3u_path")]
    pub hls_result_m3u_path: String,
    #[serde(default = "default_hls_ipv4_result_txt_path")]
    pub hls_ipv4_result_txt_path: String,
    #[serde(default = "default_hls_ipv4_result_m3u_path")]
    pub hls_ipv4_result_m3u_path: String,
    #[serde(default = "default_hls_ipv6_result_txt_path")]
    pub hls_ipv6_result_txt_path: String,
    #[serde(default = "default_hls_ipv6_result_m3u_path")]
    pub hls_ipv6_result_m3u_path: String,
    #[serde(default = "default_rtmp_data_path")]
    pub rtmp_data_path: String,
    #[serde(default = "default_logo_dir")]
    pub logo_dir: String,
    #[serde(default)]
    pub logo_url: String,
    #[serde(default = "default_logo_type")]
    pub logo_type: String,
    #[serde(default)]
    pub public_base_url: String,
    #[serde(default = "default_public_scheme")]
    pub public_scheme: String,
    #[serde(default = "default_public_domain")]
    pub public_domain: String,
    #[serde(default)]
    pub public_port: Option<u16>,
    #[serde(default = "default_urls_limit")]
    pub urls_limit: usize,
    #[serde(default = "default_local_num")]
    pub local_num: usize,
    #[serde(default = "default_subscribe_num")]
    pub subscribe_num: usize,
    #[serde(default = "default_recent_days")]
    pub recent_days: i64,
    #[serde(default)]
    pub origin_type_prefer: Vec<ChannelOrigin>,
    #[serde(default)]
    pub ipv_type_prefer: Vec<String>,
    #[serde(default = "default_result_log_path")]
    pub result_log_path: String,
    #[serde(default = "default_speed_test_log_path")]
    pub speed_test_log_path: String,
    #[serde(default = "default_statistic_log_path")]
    pub statistic_log_path: String,
    #[serde(default = "default_update_time_enabled")]
    pub update_time_enabled: bool,
    #[serde(default = "default_update_time_position")]
    pub update_time_position: String,
    #[serde(default = "default_time_zone")]
    pub time_zone: String,
    #[serde(default = "default_language")]
    pub language: String,
    #[serde(default = "default_open_url_info")]
    pub open_url_info: bool,
    #[serde(default)]
    pub open_headers: bool,
    #[serde(default)]
    pub open_empty_category: bool,
    #[serde(default = "default_open_history")]
    pub open_history: bool,
    #[serde(default = "default_open_use_cache")]
    pub open_use_cache: bool,
    #[serde(default)]
    pub open_request: bool,
    #[serde(default = "default_open_realtime_write")]
    pub open_realtime_write: bool,
    #[serde(default = "default_cache_path")]
    pub cache_path: String,
    #[serde(default = "default_frozen_path")]
    pub frozen_path: String,
}

impl Default for OutputConfig {
    fn default() -> Self {
        Self {
            result_txt_path: default_result_txt_path(),
            result_m3u_path: default_result_m3u_path(),
            m3u_result_enabled: default_m3u_result_enabled(),
            ipv4_result_txt_path: default_ipv4_result_txt_path(),
            ipv4_result_m3u_path: default_ipv4_result_m3u_path(),
            ipv6_result_txt_path: default_ipv6_result_txt_path(),
            ipv6_result_m3u_path: default_ipv6_result_m3u_path(),
            hls_result_txt_path: default_hls_result_txt_path(),
            hls_result_m3u_path: default_hls_result_m3u_path(),
            hls_ipv4_result_txt_path: default_hls_ipv4_result_txt_path(),
            hls_ipv4_result_m3u_path: default_hls_ipv4_result_m3u_path(),
            hls_ipv6_result_txt_path: default_hls_ipv6_result_txt_path(),
            hls_ipv6_result_m3u_path: default_hls_ipv6_result_m3u_path(),
            rtmp_data_path: default_rtmp_data_path(),
            logo_dir: default_logo_dir(),
            logo_url: String::new(),
            logo_type: default_logo_type(),
            public_base_url: String::new(),
            public_scheme: default_public_scheme(),
            public_domain: default_public_domain(),
            public_port: None,
            urls_limit: default_urls_limit(),
            local_num: default_local_num(),
            subscribe_num: default_subscribe_num(),
            recent_days: default_recent_days(),
            origin_type_prefer: Vec::new(),
            ipv_type_prefer: Vec::new(),
            result_log_path: default_result_log_path(),
            speed_test_log_path: default_speed_test_log_path(),
            statistic_log_path: default_statistic_log_path(),
            update_time_enabled: default_update_time_enabled(),
            update_time_position: default_update_time_position(),
            time_zone: default_time_zone(),
            language: default_language(),
            open_url_info: default_open_url_info(),
            open_headers: false,
            open_empty_category: false,
            open_history: default_open_history(),
            open_use_cache: default_open_use_cache(),
            open_request: false,
            open_realtime_write: default_open_realtime_write(),
            cache_path: default_cache_path(),
            frozen_path: default_frozen_path(),
        }
    }
}

fn default_result_txt_path() -> String {
    "output/result.txt".to_string()
}

fn default_result_m3u_path() -> String {
    "output/result.m3u".to_string()
}

fn default_m3u_result_enabled() -> bool {
    true
}

fn default_ipv4_result_txt_path() -> String {
    "output/ipv4/result.txt".to_string()
}

fn default_ipv4_result_m3u_path() -> String {
    "output/ipv4/result.m3u".to_string()
}

fn default_ipv6_result_txt_path() -> String {
    "output/ipv6/result.txt".to_string()
}

fn default_ipv6_result_m3u_path() -> String {
    "output/ipv6/result.m3u".to_string()
}

fn default_hls_result_txt_path() -> String {
    "output/hls.txt".to_string()
}

fn default_hls_result_m3u_path() -> String {
    "output/hls.m3u".to_string()
}

fn default_hls_ipv4_result_txt_path() -> String {
    "output/ipv4/hls.txt".to_string()
}

fn default_hls_ipv4_result_m3u_path() -> String {
    "output/ipv4/hls.m3u".to_string()
}

fn default_hls_ipv6_result_txt_path() -> String {
    "output/ipv6/hls.txt".to_string()
}

fn default_hls_ipv6_result_m3u_path() -> String {
    "output/ipv6/hls.m3u".to_string()
}

fn default_rtmp_data_path() -> String {
    "output/data/rtmp.db".to_string()
}

fn default_logo_dir() -> String {
    "config/logo".to_string()
}

fn default_logo_type() -> String {
    "png".to_string()
}

fn default_public_scheme() -> String {
    "http".to_string()
}

fn default_public_domain() -> String {
    "127.0.0.1".to_string()
}

fn default_urls_limit() -> usize {
    10
}

fn default_local_num() -> usize {
    10
}

fn default_subscribe_num() -> usize {
    10
}

fn default_recent_days() -> i64 {
    30
}

fn default_result_log_path() -> String {
    "output/log/result.log".to_string()
}

fn default_speed_test_log_path() -> String {
    "output/log/speed_test.log".to_string()
}

fn default_statistic_log_path() -> String {
    "output/log/statistic.log".to_string()
}

fn default_update_time_enabled() -> bool {
    true
}

fn default_update_time_position() -> String {
    "top".to_string()
}

fn default_time_zone() -> String {
    "Asia/Shanghai".to_string()
}

fn default_language() -> String {
    "zh_CN".to_string()
}

fn default_open_url_info() -> bool {
    true
}

fn default_open_history() -> bool {
    true
}

fn default_open_use_cache() -> bool {
    true
}

fn default_open_realtime_write() -> bool {
    true
}

fn default_cache_path() -> String {
    "output/data/cache.gz".to_string()
}

fn default_frozen_path() -> String {
    "output/data/frozen.gz".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscribeConfig {
    #[serde(default = "default_subscribe_enabled")]
    pub enabled: bool,
    #[serde(default = "default_subscribe_sources_path")]
    pub sources_path: String,
    #[serde(default = "default_subscribe_timeout_ms")]
    pub timeout_ms: u64,
    #[serde(default = "default_subscribe_concurrency")]
    pub concurrency: usize,
    #[serde(default)]
    pub cdn_url: String,
    #[serde(default)]
    pub http_proxy: String,
    #[serde(default = "default_alias_path")]
    pub alias_path: String,
    #[serde(default = "default_subscribe_nomatch_log_path")]
    pub nomatch_log_path: String,
}

impl Default for SubscribeConfig {
    fn default() -> Self {
        Self {
            enabled: default_subscribe_enabled(),
            sources_path: default_subscribe_sources_path(),
            timeout_ms: default_subscribe_timeout_ms(),
            concurrency: default_subscribe_concurrency(),
            cdn_url: String::new(),
            http_proxy: String::new(),
            alias_path: default_alias_path(),
            nomatch_log_path: default_subscribe_nomatch_log_path(),
        }
    }
}

fn default_subscribe_enabled() -> bool {
    true
}

fn default_subscribe_sources_path() -> String {
    "config/subscribe.txt".to_string()
}

fn default_subscribe_timeout_ms() -> u64 {
    30_000
}

fn default_subscribe_concurrency() -> usize {
    10
}

fn default_subscribe_nomatch_log_path() -> String {
    "output/log/nomatch.log".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EpgConfig {
    #[serde(default = "default_epg_enabled")]
    pub enabled: bool,
    #[serde(default = "default_epg_sources_path")]
    pub sources_path: String,
    #[serde(default = "default_epg_output_xml_path")]
    pub output_xml_path: String,
    #[serde(default = "default_epg_output_gz_path")]
    pub output_gz_path: String,
    #[serde(default = "default_epg_timeout_ms")]
    pub timeout_ms: u64,
    #[serde(default = "default_epg_concurrency")]
    pub concurrency: usize,
    #[serde(default)]
    pub cdn_url: String,
    #[serde(default)]
    pub http_proxy: String,
    #[serde(default = "default_alias_path")]
    pub alias_path: String,
}

impl Default for EpgConfig {
    fn default() -> Self {
        Self {
            enabled: default_epg_enabled(),
            sources_path: default_epg_sources_path(),
            output_xml_path: default_epg_output_xml_path(),
            output_gz_path: default_epg_output_gz_path(),
            timeout_ms: default_epg_timeout_ms(),
            concurrency: default_epg_concurrency(),
            cdn_url: String::new(),
            http_proxy: String::new(),
            alias_path: default_alias_path(),
        }
    }
}

fn default_epg_enabled() -> bool {
    true
}

fn default_epg_sources_path() -> String {
    "config/epg.txt".to_string()
}

fn default_epg_output_xml_path() -> String {
    "output/epg/epg.xml".to_string()
}

fn default_epg_output_gz_path() -> String {
    "output/epg/epg.gz".to_string()
}

fn default_epg_timeout_ms() -> u64 {
    30_000
}

fn default_epg_concurrency() -> usize {
    10
}

fn default_alias_path() -> String {
    "config/alias.txt".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilterConfig {
    #[serde(default = "default_whitelist_path")]
    pub whitelist_path: String,
    #[serde(default = "default_blacklist_path")]
    pub blacklist_path: String,
    #[serde(default)]
    pub location: Vec<String>,
    #[serde(default)]
    pub isp: Vec<String>,
    #[serde(default = "default_ipdb_path")]
    pub ipdb_path: String,
}

impl Default for FilterConfig {
    fn default() -> Self {
        Self {
            whitelist_path: default_whitelist_path(),
            blacklist_path: default_blacklist_path(),
            location: Vec::new(),
            isp: Vec::new(),
            ipdb_path: default_ipdb_path(),
        }
    }
}

fn default_whitelist_path() -> String {
    "config/whitelist.txt".to_string()
}

fn default_blacklist_path() -> String {
    "config/blacklist.txt".to_string()
}

fn default_ipdb_path() -> String {
    "utils/ip_checker/data/qqwry.ipdb".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    pub port: u16,
    pub host: String,
    #[serde(default = "default_open_service")]
    pub open_service: bool,
}

fn default_open_service() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineConfig {
    #[serde(default = "default_open_update")]
    pub open_update: bool,
    #[serde(default = "default_open_speed_test")]
    pub open_speed_test: bool,
    #[serde(
        default = "default_update_interval_secs",
        deserialize_with = "deserialize_update_interval_secs"
    )]
    pub update_interval: u64, // 秒；Python 配置值单位为小时
    #[serde(default = "default_update_mode")]
    pub update_mode: String,
    #[serde(default)]
    pub update_times: String,
    #[serde(default = "default_time_zone")]
    pub time_zone: String,
    #[serde(default = "default_update_startup")]
    pub update_startup: bool,
    #[serde(default = "default_request_timeout_secs")]
    pub request_timeout: u64,
    #[serde(default)]
    pub http_proxy: String,
    #[serde(default = "default_speed_test_limit", alias = "speed_test_limit")]
    pub check_concurrency: usize,
    #[serde(default = "default_speed_test_timeout_ms")]
    pub check_timeout: u64, // 毫秒；Python speed_test_timeout 单位为秒
    #[serde(default)]
    pub speed_test_timeout: Option<u64>, // 秒；兼容 Python INI 设置名
    #[serde(default = "default_speed_test_allow_invalid_certs")]
    pub speed_test_allow_invalid_certs: bool,
    #[serde(default = "default_speed_test_max_download_bytes")]
    pub speed_test_max_download_bytes: u64,
    #[serde(default = "default_speed_test_segment_concurrency")]
    pub speed_test_segment_concurrency: usize,
    #[serde(default)]
    pub speed_test_filter_host: bool,
    #[serde(default)]
    pub open_full_speed_test: bool,
    #[serde(default = "default_open_filter_speed")]
    pub open_filter_speed: bool,
    #[serde(default)]
    pub open_supply: bool,
    #[serde(default = "default_min_speed")]
    pub min_speed: f64,
    #[serde(default = "default_open_filter_resolution")]
    pub open_filter_resolution: bool,
    #[serde(default = "default_min_resolution")]
    pub min_resolution: String,
    #[serde(default = "default_max_resolution")]
    pub max_resolution: String,
    #[serde(default)]
    pub resolution_speed_map: HashMap<String, f64>,
    pub ipv6_support: bool,
    #[serde(default = "default_ipv_type")]
    pub ipv_type: String,
}

fn default_open_update() -> bool {
    true
}

fn default_open_speed_test() -> bool {
    true
}

fn default_update_interval_secs() -> u64 {
    12 * 60 * 60
}

fn deserialize_update_interval_secs<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Option::<toml::Value>::deserialize(deserializer)?;
    Ok(match value {
        None => default_update_interval_secs(),
        Some(toml::Value::Integer(value)) => hours_to_seconds(value as f64),
        Some(toml::Value::Float(value)) => hours_to_seconds(value),
        Some(toml::Value::String(value)) => parse_update_interval_hours(&value),
        _ => default_update_interval_secs(),
    })
}

fn parse_update_interval_hours(value: &str) -> u64 {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return 0;
    }
    trimmed
        .parse::<f64>()
        .map(hours_to_seconds)
        .unwrap_or_else(|_| default_update_interval_secs())
}

fn hours_to_seconds(hours: f64) -> u64 {
    if !hours.is_finite() || hours <= 0.0 {
        0
    } else {
        (hours * SECONDS_PER_HOUR).round() as u64
    }
}

fn default_request_timeout_secs() -> u64 {
    10
}

fn default_speed_test_limit() -> usize {
    5
}

fn default_speed_test_timeout_ms() -> u64 {
    10_000
}

fn default_speed_test_allow_invalid_certs() -> bool {
    true
}

fn default_speed_test_max_download_bytes() -> u64 {
    8 * 1024 * 1024
}

fn default_speed_test_segment_concurrency() -> usize {
    2
}

fn default_open_filter_speed() -> bool {
    true
}

fn default_min_speed() -> f64 {
    0.5
}

fn default_open_filter_resolution() -> bool {
    true
}

fn default_min_resolution() -> String {
    "1920x1080".to_string()
}

fn default_max_resolution() -> String {
    "1920x1080".to_string()
}

fn default_update_mode() -> String {
    "interval".to_string()
}

fn default_update_startup() -> bool {
    true
}

fn default_ipv_type() -> String {
    "all".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceConfig {
    pub name: String,
    pub url: String,
    pub source_type: SourceType,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum SourceType {
    M3u,
    Txt,
    Dynamic, // 新增：动态解析源
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Channel {
    #[serde(default)]
    pub origin: ChannelOrigin,
    pub name: String,
    pub group: String,
    pub url: String,
    pub logo: Option<String>,
    #[serde(default)]
    pub headers: Option<HashMap<String, String>>,
    #[serde(default)]
    pub extra_info: Option<String>,
    #[serde(default)]
    pub catchup: Option<HashMap<String, String>>,
    #[serde(default)]
    pub location: Option<String>,
    #[serde(default)]
    pub isp: Option<String>,
    #[serde(default)]
    pub speed: Option<f64>,
    #[serde(default)]
    pub resolution: Option<String>,
    #[serde(default)]
    pub date: Option<String>,
    pub latency: Option<u64>, // 延迟，单位毫秒
    pub last_checked: Option<chrono::DateTime<chrono::Utc>>,
    pub is_online: bool,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "lowercase")]
pub enum ChannelOrigin {
    #[default]
    Local,
    Subscribe,
    Whitelist,
    Hls,
}
