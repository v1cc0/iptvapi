use std::{
    collections::BTreeMap,
    fs,
    io::{Read, Write},
    path::Path,
};

use anyhow::{Context, Result};
use chrono::Utc;
use flate2::{Compression, read::GzDecoder, write::GzEncoder};
use serde::{Deserialize, Serialize};
use serde_pickle::{
    DeOptions, HashableValue, SerOptions, Value as PickleValue, value_from_slice, value_to_vec,
};

use crate::models::{Channel, ChannelOrigin, OutputConfig};

const BASE_BACKOFF_SECS: i64 = 60;
const MAX_BACKOFF_SECS: i64 = 24 * 3600;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub(crate) struct HistoryCache {
    pub channels: BTreeMap<String, BTreeMap<String, Vec<Channel>>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub(crate) struct FrozenStore {
    pub urls: BTreeMap<String, FrozenUrl>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub(crate) struct FrozenUrl {
    pub bad_count: u32,
    pub last_bad: i64,
    pub last_good: i64,
    pub frozen_until: Option<i64>,
}

pub(crate) fn load_cache(config: &OutputConfig) -> HistoryCache {
    if !config.open_history {
        return HistoryCache::default();
    }
    load_gzip_pickle_value(&config.cache_path)
        .ok()
        .and_then(history_cache_from_python_pickle)
        .or_else(|| load_gzip_json(&config.cache_path).ok())
        .unwrap_or_default()
}

pub(crate) fn save_cache(config: &OutputConfig, channels: &[Channel]) -> Result<()> {
    if !config.open_history {
        return Ok(());
    }
    let mut cache = HistoryCache::default();
    for channel in channels {
        if channel.url.trim().is_empty() {
            continue;
        }
        cache
            .channels
            .entry(channel.group.clone())
            .or_default()
            .entry(channel.name.clone())
            .or_default()
            .push(channel.clone());
    }
    save_gzip_pickle_value(&config.cache_path, &history_cache_to_python_pickle(&cache))
}

pub(crate) fn load_frozen(config: &OutputConfig) -> FrozenStore {
    if !config.open_history {
        return FrozenStore::default();
    }
    load_gzip_pickle_value(&config.frozen_path)
        .ok()
        .and_then(frozen_store_from_python_pickle)
        .or_else(|| load_gzip_json(&config.frozen_path).ok())
        .unwrap_or_default()
}

pub(crate) fn save_frozen(config: &OutputConfig, frozen: &FrozenStore) -> Result<()> {
    if !config.open_history {
        return Ok(());
    }
    save_gzip_pickle_value(&config.frozen_path, &frozen_store_to_python_pickle(frozen))
}

pub(crate) fn merge_cached_channels(
    fresh: &mut Vec<Channel>,
    cache: &HistoryCache,
    frozen: &mut FrozenStore,
) {
    let mut seen = fresh
        .iter()
        .filter(|channel| !channel.url.trim().is_empty())
        .map(|channel| {
            (
                channel.group.clone(),
                channel.name.clone(),
                channel.url.clone(),
            )
        })
        .collect::<std::collections::HashSet<_>>();

    for (group, names) in &cache.channels {
        for (name, channels) in names {
            for channel in channels {
                if channel.url.trim().is_empty() || is_retained_origin(channel.origin) {
                    continue;
                }
                if frozen.is_url_frozen(&channel.url) {
                    continue;
                }
                let key = (group.clone(), name.clone(), channel.url.clone());
                if seen.insert(key) {
                    let mut cached = channel.clone();
                    cached.group = group.clone();
                    cached.name = name.clone();
                    fresh.push(cached);
                }
            }
        }
    }
}

fn is_retained_origin(origin: ChannelOrigin) -> bool {
    matches!(origin, ChannelOrigin::Whitelist | ChannelOrigin::Hls)
}

impl FrozenStore {
    pub(crate) fn mark_url_bad(&mut self, url: &str, initial: bool) {
        if url.trim().is_empty() {
            return;
        }
        let now = now_ts();
        let meta = self.urls.entry(url.to_owned()).or_default();
        if initial {
            meta.bad_count = meta.bad_count.max(3);
        }
        meta.bad_count = meta.bad_count.saturating_add(1);
        meta.last_bad = now;
        let exp = 2_i64.saturating_pow(meta.bad_count.min(30)) * BASE_BACKOFF_SECS;
        meta.frozen_until = Some(now + exp.min(MAX_BACKOFF_SECS));
    }

    pub(crate) fn mark_url_good(&mut self, url: &str) {
        let Some(meta) = self.urls.get_mut(url) else {
            return;
        };
        meta.last_good = now_ts();
        meta.bad_count = meta.bad_count.saturating_sub(1);
        meta.frozen_until = None;
        if meta.bad_count == 0 {
            self.urls.remove(url);
        }
    }

    pub(crate) fn is_url_frozen(&mut self, url: &str) -> bool {
        self.is_url_frozen_at(url, now_ts())
    }

    fn is_url_frozen_at(&mut self, url: &str, now: i64) -> bool {
        let Some(meta) = self.urls.get_mut(url) else {
            return false;
        };
        let Some(frozen_until) = meta.frozen_until else {
            return false;
        };
        if frozen_until > now {
            return true;
        }
        meta.frozen_until = None;
        meta.bad_count = meta.bad_count.saturating_sub(1);
        if meta.bad_count == 0 {
            self.urls.remove(url);
        }
        false
    }
}

fn load_gzip_json<T>(path: &str) -> Result<T>
where
    T: for<'de> Deserialize<'de>,
{
    let file = fs::File::open(path).with_context(|| format!("open gzip json: {path}"))?;
    let mut decoder = GzDecoder::new(file);
    let mut content = String::new();
    decoder
        .read_to_string(&mut content)
        .with_context(|| format!("read gzip json: {path}"))?;
    serde_json::from_str(&content).with_context(|| format!("parse gzip json: {path}"))
}

fn load_gzip_pickle_value(path: &str) -> Result<PickleValue> {
    let file = fs::File::open(path).with_context(|| format!("open gzip pickle: {path}"))?;
    let mut decoder = GzDecoder::new(file);
    let mut content = Vec::new();
    decoder
        .read_to_end(&mut content)
        .with_context(|| format!("read gzip pickle: {path}"))?;
    value_from_slice(&content, DeOptions::new().decode_strings())
        .with_context(|| format!("parse gzip pickle: {path}"))
}

fn save_gzip_pickle_value(path: &str, value: &PickleValue) -> Result<()> {
    let path_ref = Path::new(path);
    if let Some(parent) = path_ref
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent)
            .with_context(|| format!("create history dir: {}", parent.display()))?;
    }
    let file = fs::File::create(path).with_context(|| format!("create gzip pickle: {path}"))?;
    let mut encoder = GzEncoder::new(file, Compression::default());
    let content =
        value_to_vec(value, SerOptions::new()).context("serialize python-compatible pickle")?;
    encoder
        .write_all(&content)
        .with_context(|| format!("write gzip pickle: {path}"))?;
    encoder
        .finish()
        .with_context(|| format!("finish gzip pickle: {path}"))?;
    Ok(())
}

fn history_cache_to_python_pickle(cache: &HistoryCache) -> PickleValue {
    PickleValue::Dict(
        cache
            .channels
            .iter()
            .map(|(group, names)| {
                (
                    py_key(group),
                    PickleValue::Dict(
                        names
                            .iter()
                            .map(|(name, channels)| {
                                (
                                    py_key(name),
                                    PickleValue::List(
                                        channels.iter().map(channel_to_python_pickle).collect(),
                                    ),
                                )
                            })
                            .collect(),
                    ),
                )
            })
            .collect(),
    )
}

fn channel_to_python_pickle(channel: &Channel) -> PickleValue {
    let mut fields = BTreeMap::new();
    fields.insert(
        py_key("origin"),
        pickle_string(channel_origin_name(channel.origin)),
    );
    fields.insert(py_key("name"), pickle_string(&channel.name));
    fields.insert(py_key("group"), pickle_string(&channel.group));
    fields.insert(py_key("url"), pickle_string(&channel.url));
    insert_optional_string(&mut fields, "logo", channel.logo.as_deref());
    insert_optional_string(&mut fields, "extra_info", channel.extra_info.as_deref());
    insert_optional_string(&mut fields, "location", channel.location.as_deref());
    insert_optional_string(&mut fields, "isp", channel.isp.as_deref());
    insert_optional_string(&mut fields, "resolution", channel.resolution.as_deref());
    insert_optional_string(&mut fields, "date", channel.date.as_deref());
    fields.insert(
        py_key("headers"),
        channel
            .headers
            .as_ref()
            .map(headers_to_python_pickle)
            .unwrap_or(PickleValue::None),
    );
    fields.insert(
        py_key("catchup"),
        channel
            .catchup
            .as_ref()
            .map(headers_to_python_pickle)
            .unwrap_or(PickleValue::None),
    );
    fields.insert(
        py_key("speed"),
        channel
            .speed
            .map(PickleValue::F64)
            .unwrap_or(PickleValue::None),
    );
    fields.insert(
        py_key("delay"),
        channel
            .latency
            .map(|value| PickleValue::I64(value as i64))
            .unwrap_or(PickleValue::None),
    );
    fields.insert(py_key("is_online"), PickleValue::Bool(channel.is_online));
    PickleValue::Dict(fields)
}

fn headers_to_python_pickle(headers: &std::collections::HashMap<String, String>) -> PickleValue {
    PickleValue::Dict(
        headers
            .iter()
            .map(|(key, value)| (py_key(key), pickle_string(value)))
            .collect(),
    )
}

fn history_cache_from_python_pickle(value: PickleValue) -> Option<HistoryCache> {
    let mut cache = HistoryCache::default();
    for (group, names) in py_dict(value)? {
        let group = py_hashable_string(&group)?;
        for (name, channels) in py_dict(names)? {
            let name = py_hashable_string(&name)?;
            let channels = match channels {
                PickleValue::List(items) | PickleValue::Tuple(items) => items,
                _ => continue,
            };
            let parsed = channels
                .into_iter()
                .filter_map(|value| channel_from_python_pickle(&group, &name, value))
                .collect::<Vec<_>>();
            if !parsed.is_empty() {
                cache
                    .channels
                    .entry(group.clone())
                    .or_default()
                    .entry(name)
                    .or_default()
                    .extend(parsed);
            }
        }
    }
    Some(cache)
}

fn channel_from_python_pickle(group: &str, name: &str, value: PickleValue) -> Option<Channel> {
    let fields = py_string_dict(value)?;
    let url = py_field_string(&fields, "url")?;
    Some(Channel {
        origin: py_field_string(&fields, "origin")
            .as_deref()
            .and_then(channel_origin_from_name)
            .unwrap_or(ChannelOrigin::Local),
        name: py_field_string(&fields, "name").unwrap_or_else(|| name.to_owned()),
        group: py_field_string(&fields, "group").unwrap_or_else(|| group.to_owned()),
        url,
        logo: py_field_string(&fields, "logo"),
        headers: fields.get("headers").and_then(headers_from_python_pickle),
        extra_info: py_field_string(&fields, "extra_info"),
        catchup: fields.get("catchup").and_then(headers_from_python_pickle),
        location: py_field_string(&fields, "location"),
        isp: py_field_string(&fields, "isp"),
        speed: fields.get("speed").and_then(py_f64),
        resolution: py_field_string(&fields, "resolution"),
        date: py_field_string(&fields, "date"),
        latency: fields
            .get("delay")
            .and_then(py_i64)
            .and_then(|value| (value >= 0).then_some(value as u64)),
        last_checked: None,
        is_online: fields
            .get("is_online")
            .and_then(py_bool)
            .unwrap_or_else(|| {
                fields
                    .get("delay")
                    .and_then(py_i64)
                    .is_some_and(|delay| delay >= 0)
            }),
    })
}

fn frozen_store_to_python_pickle(store: &FrozenStore) -> PickleValue {
    PickleValue::Dict(
        store
            .urls
            .iter()
            .map(|(url, frozen)| {
                let mut fields = BTreeMap::new();
                fields.insert(
                    py_key("bad_count"),
                    PickleValue::I64(frozen.bad_count as i64),
                );
                fields.insert(py_key("last_bad"), PickleValue::I64(frozen.last_bad));
                fields.insert(py_key("last_good"), PickleValue::I64(frozen.last_good));
                fields.insert(
                    py_key("frozen_until"),
                    frozen
                        .frozen_until
                        .map(PickleValue::I64)
                        .unwrap_or(PickleValue::None),
                );
                (py_key(url), PickleValue::Dict(fields))
            })
            .collect(),
    )
}

fn frozen_store_from_python_pickle(value: PickleValue) -> Option<FrozenStore> {
    let mut store = FrozenStore::default();
    for (url, meta) in py_dict(value)? {
        let url = py_hashable_string(&url)?;
        let meta = py_string_dict(meta)?;
        store.urls.insert(
            url,
            FrozenUrl {
                bad_count: meta.get("bad_count").and_then(py_i64).unwrap_or_default() as u32,
                last_bad: meta.get("last_bad").and_then(py_i64).unwrap_or_default(),
                last_good: meta.get("last_good").and_then(py_i64).unwrap_or_default(),
                frozen_until: meta.get("frozen_until").and_then(py_i64),
            },
        );
    }
    Some(store)
}

fn headers_from_python_pickle(
    value: &PickleValue,
) -> Option<std::collections::HashMap<String, String>> {
    let fields = py_string_dict(value.clone())?;
    Some(
        fields
            .into_iter()
            .filter_map(|(key, value)| pickle_value_string(value).map(|value| (key, value)))
            .collect(),
    )
}

fn py_dict(value: PickleValue) -> Option<BTreeMap<HashableValue, PickleValue>> {
    match value {
        PickleValue::Dict(values) => Some(values),
        _ => None,
    }
}

fn py_string_dict(value: PickleValue) -> Option<BTreeMap<String, PickleValue>> {
    Some(
        py_dict(value)?
            .into_iter()
            .filter_map(|(key, value)| py_hashable_string(&key).map(|key| (key, value)))
            .collect(),
    )
}

fn py_field_string(fields: &BTreeMap<String, PickleValue>, key: &str) -> Option<String> {
    fields.get(key).cloned().and_then(pickle_value_string)
}

fn insert_optional_string(
    fields: &mut BTreeMap<HashableValue, PickleValue>,
    key: &str,
    value: Option<&str>,
) {
    fields.insert(
        py_key(key),
        value.map(pickle_string).unwrap_or(PickleValue::None),
    );
}

fn py_key(value: &str) -> HashableValue {
    HashableValue::String(value.to_owned())
}

fn pickle_string(value: &str) -> PickleValue {
    PickleValue::String(value.to_owned())
}

fn py_hashable_string(value: &HashableValue) -> Option<String> {
    match value {
        HashableValue::String(value) => Some(value.clone()),
        HashableValue::Bytes(value) => String::from_utf8(value.clone()).ok(),
        _ => None,
    }
}

fn pickle_value_string(value: PickleValue) -> Option<String> {
    match value {
        PickleValue::String(value) => Some(value),
        PickleValue::Bytes(value) => String::from_utf8(value).ok(),
        _ => None,
    }
}

fn py_i64(value: &PickleValue) -> Option<i64> {
    match value {
        PickleValue::I64(value) => Some(*value),
        PickleValue::Int(value) => value.to_string().parse::<i64>().ok(),
        PickleValue::F64(value) => Some(*value as i64),
        _ => None,
    }
}

fn py_f64(value: &PickleValue) -> Option<f64> {
    match value {
        PickleValue::F64(value) => Some(*value),
        PickleValue::I64(value) => Some(*value as f64),
        PickleValue::Int(value) => value.to_string().parse::<f64>().ok(),
        _ => None,
    }
}

fn py_bool(value: &PickleValue) -> Option<bool> {
    match value {
        PickleValue::Bool(value) => Some(*value),
        _ => None,
    }
}

fn channel_origin_name(origin: ChannelOrigin) -> &'static str {
    match origin {
        ChannelOrigin::Local => "local",
        ChannelOrigin::Subscribe => "subscribe",
        ChannelOrigin::Whitelist => "whitelist",
        ChannelOrigin::Hls => "hls",
    }
}

fn channel_origin_from_name(value: &str) -> Option<ChannelOrigin> {
    match value {
        "local" => Some(ChannelOrigin::Local),
        "subscribe" => Some(ChannelOrigin::Subscribe),
        "whitelist" => Some(ChannelOrigin::Whitelist),
        "hls" => Some(ChannelOrigin::Hls),
        _ => None,
    }
}

fn now_ts() -> i64 {
    Utc::now().timestamp()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn channel(name: &str, group: &str, url: &str, origin: ChannelOrigin) -> Channel {
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
            latency: Some(1),
            last_checked: None,
            is_online: true,
        }
    }

    #[test]
    fn cache_round_trips_as_gzip_json() {
        let dir = tempfile::tempdir().unwrap();
        let cache_path = dir.path().join("cache.gz");
        let config = OutputConfig {
            cache_path: cache_path.to_string_lossy().into_owned(),
            ..Default::default()
        };
        save_cache(
            &config,
            &[channel(
                "CCTV-1",
                "央视频道",
                "http://one",
                ChannelOrigin::Local,
            )],
        )
        .unwrap();

        let loaded = load_cache(&config);

        assert_eq!(loaded.channels["央视频道"]["CCTV-1"][0].url, "http://one");
    }

    #[test]
    fn frozen_store_backoff_and_good_decay() {
        let mut store = FrozenStore::default();
        store.mark_url_bad("http://bad", true);
        assert!(store.is_url_frozen("http://bad"));
        store.mark_url_good("http://bad");
        assert!(!store.is_url_frozen("http://bad"));
    }

    #[test]
    fn merge_cache_skips_retained_and_frozen_urls() {
        let mut fresh = vec![channel(
            "CCTV-1",
            "央视频道",
            "http://fresh",
            ChannelOrigin::Local,
        )];
        let mut cache = HistoryCache::default();
        cache
            .channels
            .entry("央视频道".to_owned())
            .or_default()
            .insert(
                "CCTV-1".to_owned(),
                vec![
                    channel("CCTV-1", "央视频道", "http://old", ChannelOrigin::Local),
                    channel(
                        "CCTV-1",
                        "央视频道",
                        "http://white",
                        ChannelOrigin::Whitelist,
                    ),
                ],
            );
        let mut frozen = FrozenStore::default();
        frozen.mark_url_bad("http://old", true);

        merge_cached_channels(&mut fresh, &cache, &mut frozen);

        assert_eq!(fresh.len(), 1);
        assert_eq!(fresh[0].url, "http://fresh");
    }

    #[test]
    fn merge_cache_preserves_non_retained_origin_like_python_history() {
        let mut fresh = Vec::new();
        let mut cache = HistoryCache::default();
        cache
            .channels
            .entry("央视频道".to_owned())
            .or_default()
            .insert(
                "CCTV-1".to_owned(),
                vec![channel(
                    "CCTV-1",
                    "央视频道",
                    "http://subscribe",
                    ChannelOrigin::Subscribe,
                )],
            );
        let mut frozen = FrozenStore::default();

        merge_cached_channels(&mut fresh, &cache, &mut frozen);

        assert_eq!(fresh.len(), 1);
        assert_eq!(fresh[0].origin, ChannelOrigin::Subscribe);
    }

    #[test]
    fn loads_python_pickle_cache_gzip() {
        let dir = tempfile::tempdir().unwrap();
        let cache_path = dir.path().join("cache.gz");
        fs::write(&cache_path, hex_bytes(PYTHON_PICKLE_CACHE_GZ)).unwrap();
        let config = OutputConfig {
            cache_path: cache_path.to_string_lossy().into_owned(),
            ..Default::default()
        };

        let loaded = load_cache(&config);
        let channel = &loaded.channels["央视频道"]["CCTV-1"][0];

        assert_eq!(channel.origin, ChannelOrigin::Subscribe);
        assert_eq!(channel.url, "http://one");
        assert_eq!(channel.speed, Some(1.25));
        assert_eq!(channel.resolution.as_deref(), Some("1920x1080"));
        assert_eq!(channel.headers.as_ref().unwrap()["User-Agent"], "UA");
    }

    #[test]
    fn loads_python_pickle_frozen_gzip() {
        let dir = tempfile::tempdir().unwrap();
        let frozen_path = dir.path().join("frozen.gz");
        fs::write(&frozen_path, hex_bytes(PYTHON_PICKLE_FROZEN_GZ)).unwrap();
        let config = OutputConfig {
            frozen_path: frozen_path.to_string_lossy().into_owned(),
            ..Default::default()
        };

        let loaded = load_frozen(&config);
        let frozen = &loaded.urls["http://bad"];

        assert_eq!(frozen.bad_count, 4);
        assert_eq!(frozen.last_bad, 11);
        assert_eq!(frozen.last_good, 7);
        assert_eq!(frozen.frozen_until, Some(99));
    }

    #[test]
    fn save_cache_writes_python_pickle_shape() {
        let dir = tempfile::tempdir().unwrap();
        let cache_path = dir.path().join("cache.gz");
        let config = OutputConfig {
            cache_path: cache_path.to_string_lossy().into_owned(),
            ..Default::default()
        };
        let mut channel = channel("CCTV-1", "央视频道", "http://one", ChannelOrigin::Subscribe);
        channel.speed = Some(1.0);
        save_cache(&config, &[channel]).unwrap();

        let value = load_gzip_pickle_value(cache_path.to_str().unwrap()).unwrap();
        assert!(matches!(value, PickleValue::Dict(_)));
        assert!(load_gzip_json::<HistoryCache>(cache_path.to_str().unwrap()).is_err());
    }

    #[test]
    fn save_frozen_writes_python_pickle_shape() {
        let dir = tempfile::tempdir().unwrap();
        let frozen_path = dir.path().join("frozen.gz");
        let config = OutputConfig {
            frozen_path: frozen_path.to_string_lossy().into_owned(),
            ..Default::default()
        };
        let mut frozen = FrozenStore::default();
        frozen.mark_url_bad("http://bad", true);
        save_frozen(&config, &frozen).unwrap();

        let value = load_gzip_pickle_value(frozen_path.to_str().unwrap()).unwrap();
        assert!(matches!(value, PickleValue::Dict(_)));
        assert!(load_gzip_json::<FrozenStore>(frozen_path.to_str().unwrap()).is_err());
    }

    fn hex_bytes(value: &str) -> Vec<u8> {
        value
            .as_bytes()
            .chunks_exact(2)
            .map(|pair| {
                let pair = std::str::from_utf8(pair).unwrap();
                u8::from_str_radix(pair, 16).unwrap()
            })
            .collect()
    }

    const PYTHON_PICKLE_CACHE_GZ: &str = "1f8b08000000000002ff6b60992ac3c80006b5537a789e2e59f76279dbcb45135f364e9e02146073760e09d3359c120be468f4b0e51765a667e64de9e12c2e4d2a4e2eca4c4a9dd2c39297989b3a2583b98735bd28bfb4604a06630f736951ce941eae8c9292022b7dfdfc3ca02ad6e282d4d49429eef65f2096f57015a516e7e7949664e683cc33b43432a83034b03000aa4c49cd49ac9ce2add5c392925802d4ca6560aa6b64ae6b64606436a58723b3a02cbea4b200643190690214c9c94f4e8418c3f364c7daa7b3f73eddb9ffc98e39537a98338b0b805e783e65eb93fd0b81e6a456941425c667e6a5e5036d0c46f8803d23353125b5a818e461aed0e2d4225dc7f4d4bc92293d4ca18e538a4b138b8bf50078fdc75c27010000";
    const PYTHON_PICKLE_FROZEN_GZ: &str = "1f8b08000000000002ff6b6099eacf0001b5537ab8324a4a0aacf4f5931253a6d44ed1e8e10432e293f34bf34aa678b3f470e4241697c483e4bcb97b38c19cf4fc7c208fbd8727ad28bf2a352f1ea83233678a777269b11e00dfea1c835a000000";
}
