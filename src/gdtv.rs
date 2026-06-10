use crate::{
    gdtv_signer,
    models::{Channel, ChannelOrigin},
};
use anyhow::Context;
use futures::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, env, sync::OnceLock, time::Instant};
use tokio::{
    net::TcpStream,
    sync::Mutex,
    time::{Duration, timeout},
};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async,
    tungstenite::{Message, client::IntoClientRequest, http::HeaderValue},
};
use url::Url;
pub const GDTV_HOST: &str = "127.0.0.1";
pub const GDTV_PORT: u16 = 12345;
pub const GDTV_PLAYLIST_PATH: &str = "/gdtv.m3u";
pub const GDTV_STATUS_PATH: &str = "/gdtv/status";
pub const GDTV_PLAY_PATH_PREFIX: &str = "/gdtv/play";
const GDTV_GROUP: &str = "GDTV";
const OFFICIAL_CHANNEL_LIST_URL: &str = "https://gdtv-api.gdtv.cn/api/tv/v2/tvChannel?category=0";
const TCDN_PARAM_URL: &str = "https://tcdn-api.itouchtv.cn/getParam";
const TCDN_WS_URL: &str = "wss://tcdn-ws.itouchtv.cn:3800/connect";
const OFFICIAL_REFERER: &str = "https://www.gdtv.cn/";
const OFFICIAL_ORIGIN: &str = "https://www.gdtv.cn";
const OFFICIAL_CLIENT: &str = "WEB_PC";
const OFFICIAL_DEVICE_ID: &str = "WEB_gdtv_playlist";

const OFFICIAL_HTTP_USER_AGENT: &str = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36";
const TCDN_TIMEOUT_SECS: u64 = 8;
const DEFAULT_OFFICIAL_PLAY_URL_CACHE_TTL_SECS: u64 = 45;
const OFFICIAL_PLAY_URL_CACHE_TTL_ENV: &str = "TV_GDTV_PLAY_URL_CACHE_TTL_SECS";

#[derive(Clone, Debug)]
struct CachedOfficialPlayUrl {
    fetched_at: Instant,
    play_url: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct GdtvStatus {
    pub playlist_path: String,
    pub play_path_prefix: String,
    pub pinned_channels: usize,
    pub cache_ttl_secs: u64,
    pub cached_play_urls: usize,
    pub cached_pks: Vec<u64>,
}

static OFFICIAL_PLAY_URL_CACHE: OnceLock<Mutex<HashMap<u64, CachedOfficialPlayUrl>>> =
    OnceLock::new();

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct OfficialChannel {
    pk: u64,
    name: String,
    avatar_url: Option<String>,
    play_url: String,
}

#[derive(Clone, Debug)]
struct OfficialChannelMeta {
    pk: u64,
    name: String,
    logo: Option<String>,
    list_play_url: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
struct TcdnParamResponse {
    node: String,
}

#[derive(Clone, Debug, Deserialize)]
struct TcdnWsResponse {
    status: Option<u16>,
    wsnode: Option<String>,
}

struct TcdnWsSession {
    wsnode: String,
    _socket: WebSocketStream<MaybeTlsStream<TcpStream>>,
}

#[derive(Clone, Debug, Deserialize)]
struct OfficialPlayUrl {
    hd: Option<String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct OfficialGdtvChannel {
    pub tvg_id: String,
    pub name: String,
    pub logo: Option<String>,
    pub play_url: String,
}

#[derive(Clone, Copy, Debug)]
pub struct GdtvChannel {
    pub tvg_id: &'static str,
    pub name: &'static str,
    pub logo: &'static str,
    pub play_url: Option<&'static str>,
}

pub const GDTV_CHANNELS: &[GdtvChannel] = &[
    GdtvChannel {
        tvg_id: "GuangdongSatelliteTV.cn",
        name: "广东卫视",
        logo: "https://parco-zh.github.io/demo/guangdong.jpg",
        play_url: None,
    },
    GdtvChannel {
        tvg_id: "GuangdongPearlRiverChannel.cn",
        name: "广东珠江",
        logo: "",
        play_url: None,
    },
    GdtvChannel {
        tvg_id: "GuangdongNewsChannel.cn",
        name: "广东新闻",
        logo: "",
        play_url: None,
    },
    GdtvChannel {
        tvg_id: "GuangdongPublic.cn",
        name: "广东民生",
        logo: "",
        play_url: None,
    },
    GdtvChannel {
        tvg_id: "GBASatelliteTV.cn",
        name: "大湾区卫视",
        logo: "",
        play_url: None,
    },
    GdtvChannel {
        tvg_id: "GuangdongEconomyScienceandEducationChannel.cn",
        name: "广东经济科教",
        logo: "",
        play_url: None,
    },
    GdtvChannel {
        tvg_id: "GuangdongSports.cn",
        name: "广东体育",
        logo: "",
        play_url: None,
    },
    GdtvChannel {
        tvg_id: "GuangdongDramaMovieChannel.cn",
        name: "广东影视",
        logo: "",
        play_url: None,
    },
    GdtvChannel {
        tvg_id: "GuangdongVarietyChannel.cn",
        name: "广东综艺",
        logo: "",
        play_url: None,
    },
    GdtvChannel {
        tvg_id: "GuangdongChildrensChannel.cn",
        name: "广东少儿",
        logo: "",
        play_url: None,
    },
    GdtvChannel {
        tvg_id: "JiaJiaCartoon.cn",
        name: "嘉佳卡通",
        logo: "",
        play_url: None,
    },
    GdtvChannel {
        tvg_id: "GuangdongWorldChannel.cn",
        name: "广东国际",
        logo: "",
        play_url: None,
    },
    GdtvChannel {
        tvg_id: "LingnanOperaChannel.cn",
        name: "岭南戏曲",
        logo: "",
        play_url: None,
    },
    GdtvChannel {
        tvg_id: "ModernEducationChannel.cn",
        name: "现代教育",
        logo: "",
        play_url: None,
    },
    GdtvChannel {
        tvg_id: "RealEstateChannel.cn",
        name: "广东房产",
        logo: "",
        play_url: None,
    },
    GdtvChannel {
        tvg_id: "GuangdongMobileChannel.cn",
        name: "广东移动",
        logo: "",
        play_url: None,
    },
    GdtvChannel {
        tvg_id: "SouthernShoppingChannel.cn",
        name: "南方购物",
        logo: "",
        play_url: None,
    },
    GdtvChannel {
        tvg_id: "GRTNCulturalChannel.cn",
        name: "GRTN文化",
        logo: "",
        play_url: None,
    },
    GdtvChannel {
        tvg_id: "GRTNHealthChannel.cn",
        name: "GRTN健康",
        logo: "",
        play_url: None,
    },
    GdtvChannel {
        tvg_id: "TVS1EconomicScience.cn",
        name: "TVS1",
        logo: "",
        play_url: None,
    },
];

pub fn playlist_url() -> String {
    format!("http://{}{}", default_host_port(), GDTV_PLAYLIST_PATH)
}

pub fn status_url() -> String {
    format!("http://{}{}", default_host_port(), GDTV_STATUS_PATH)
}

pub fn default_host_port() -> String {
    format!("{GDTV_HOST}:{GDTV_PORT}")
}

pub async fn official_playlist_m3u(base_url: &str) -> String {
    playlist_m3u_from_official_meta(&pinned_official_channel_meta(), base_url)
}

pub async fn resolve_official_play_url(pk: u64) -> anyhow::Result<String> {
    let client = official_http_client()?;
    cached_resolve_official_play_url_with_client(&client, pk).await
}

pub async fn official_hls_playlist(pk: u64) -> anyhow::Result<String> {
    let client = official_http_client()?;
    let play_url = cached_resolve_official_play_url_with_client(&client, pk).await?;
    match fetch_normalized_hls_playlist(&client, &play_url, 0).await {
        Ok(playlist) => Ok(playlist),
        Err(first_error) => {
            evict_official_play_url(pk).await;
            let play_url = cached_resolve_official_play_url_with_client(&client, pk).await?;
            fetch_normalized_hls_playlist(&client, &play_url, 0)
                .await
                .with_context(|| {
                    format!(
                        "refreshing GDTV token after HLS fetch failure; first error: {first_error:#}"
                    )
                })
        }
    }
}

fn official_http_client() -> anyhow::Result<reqwest::Client> {
    Ok(reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .user_agent(OFFICIAL_HTTP_USER_AGENT)
        .cookie_store(true)
        .build()?)
}

async fn cached_resolve_official_play_url_with_client(
    client: &reqwest::Client,
    pk: u64,
) -> anyhow::Result<String> {
    let mut cache = official_play_url_cache().lock().await;
    if let Some(cached) = cache.get(&pk)
        && cached.fetched_at.elapsed() < official_play_url_cache_ttl()
    {
        return Ok(cached.play_url.clone());
    }

    let play_url = resolve_official_play_url_with_client(client, pk).await?;
    cache.insert(
        pk,
        CachedOfficialPlayUrl {
            fetched_at: Instant::now(),
            play_url: play_url.clone(),
        },
    );
    Ok(play_url)
}

fn official_play_url_cache_ttl() -> Duration {
    Duration::from_secs(parse_cache_ttl_secs(
        env::var(OFFICIAL_PLAY_URL_CACHE_TTL_ENV).ok(),
    ))
}

fn parse_cache_ttl_secs(value: Option<String>) -> u64 {
    value
        .as_deref()
        .map(str::trim)
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_OFFICIAL_PLAY_URL_CACHE_TTL_SECS)
}

fn official_play_url_cache() -> &'static Mutex<HashMap<u64, CachedOfficialPlayUrl>> {
    OFFICIAL_PLAY_URL_CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

async fn evict_official_play_url(pk: u64) {
    official_play_url_cache().lock().await.remove(&pk);
}

async fn resolve_official_play_url_with_client(
    client: &reqwest::Client,
    pk: u64,
) -> anyhow::Result<String> {
    let _ = official_channel_meta_by_pk(pk)
        .ok_or_else(|| anyhow::anyhow!("unknown official GDTV channel pk: {pk}"))?;
    let node = fetch_tcdn_node(client).await?;
    let node = resolve_tcdn_wsnode(&node).await.unwrap_or(node);
    let node = base64_encode(&node);
    fetch_official_detail_play_url(client, pk, &node)
        .await?
        .ok_or_else(|| anyhow::anyhow!("official GDTV detail returned no play URL for pk: {pk}"))
}

async fn fetch_normalized_hls_playlist(
    client: &reqwest::Client,
    url: &str,
    depth: usize,
) -> anyhow::Result<String> {
    if depth > 3 {
        anyhow::bail!("official GDTV HLS playlist nesting is too deep: {url}");
    }
    let text = client
        .get(url)
        .header("Referer", OFFICIAL_REFERER)
        .header(
            "Accept",
            "application/vnd.apple.mpegurl, application/x-mpegURL, */*",
        )
        .header("Accept-Encoding", "identity;q=1, *;q=0")
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;
    let Some(next_playlist) = first_nested_hls_playlist(url, &text) else {
        return normalize_hls_media_playlist(url, &text);
    };
    Box::pin(fetch_normalized_hls_playlist(
        client,
        &next_playlist,
        depth + 1,
    ))
    .await
}

fn first_nested_hls_playlist(base_url: &str, playlist: &str) -> Option<String> {
    playlist
        .lines()
        .map(str::trim)
        .find(|line| !line.is_empty() && !line.starts_with('#') && looks_like_hls_uri(line))
        .and_then(|line| resolve_hls_uri(base_url, line))
}

fn normalize_hls_media_playlist(base_url: &str, playlist: &str) -> anyhow::Result<String> {
    if !playlist.trim_start().starts_with("#EXTM3U") {
        anyhow::bail!("official GDTV response is not an HLS playlist: {base_url}");
    }
    let mut normalized = String::with_capacity(playlist.len() + 512);
    for line in playlist.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            normalized.push('\n');
            continue;
        }
        if trimmed.starts_with('#') {
            normalized.push_str(&rewrite_hls_tag_uris(base_url, trimmed));
        } else {
            normalized.push_str(
                &resolve_hls_uri(base_url, trimmed).unwrap_or_else(|| trimmed.to_owned()),
            );
        }
        normalized.push('\n');
    }
    Ok(normalized)
}

fn rewrite_hls_tag_uris(base_url: &str, line: &str) -> String {
    let Some(start) = line.find("URI=\"") else {
        return line.to_owned();
    };
    let value_start = start + 5;
    let Some(relative_end) = line[value_start..].find('\"') else {
        return line.to_owned();
    };
    let value_end = value_start + relative_end;
    let value = &line[value_start..value_end];
    let Some(resolved) = resolve_hls_uri(base_url, value) else {
        return line.to_owned();
    };
    format!("{}{}{}", &line[..value_start], resolved, &line[value_end..])
}

fn looks_like_hls_uri(value: &str) -> bool {
    value.to_ascii_lowercase().contains(".m3u8")
}

fn resolve_hls_uri(base_url: &str, value: &str) -> Option<String> {
    let value = value.trim();
    if value.starts_with("//") {
        let base = Url::parse(base_url).ok()?;
        return Some(format!("{}:{value}", base.scheme()));
    }
    if value.starts_with("http://") || value.starts_with("https://") {
        return Some(value.to_owned());
    }
    Url::parse(base_url)
        .ok()
        .and_then(|base| base.join(value).ok())
        .map(Into::into)
}

pub async fn official_exposable_channel_count() -> usize {
    pinned_official_channel_meta().len()
}

pub async fn official_status() -> GdtvStatus {
    let cache = official_play_url_cache().lock().await;
    let mut cached_pks = cache
        .iter()
        .filter_map(|(pk, cached)| {
            (cached.fetched_at.elapsed() < official_play_url_cache_ttl()).then_some(*pk)
        })
        .collect::<Vec<_>>();
    cached_pks.sort_unstable();

    GdtvStatus {
        playlist_path: GDTV_PLAYLIST_PATH.to_owned(),
        play_path_prefix: GDTV_PLAY_PATH_PREFIX.to_owned(),
        pinned_channels: pinned_official_channel_meta().len(),
        cache_ttl_secs: official_play_url_cache_ttl().as_secs(),
        cached_play_urls: cached_pks.len(),
        cached_pks,
    }
}

pub async fn fetch_official_channels() -> anyhow::Result<Vec<OfficialGdtvChannel>> {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .user_agent(OFFICIAL_HTTP_USER_AGENT)
        .build()?;

    let channel_meta = match signed_get(&client, OFFICIAL_CHANNEL_LIST_URL)?
        .send()
        .await
        .and_then(reqwest::Response::error_for_status)
    {
        Ok(response) => parse_official_channel_meta(response.json::<Vec<OfficialChannel>>().await?),
        Err(error) => {
            tracing::warn!(
                "Official GDTV list API failed; falling back to pinned channel ids: {error:#}"
            );
            pinned_official_channel_meta()
        }
    };
    if let Ok(channels) = fetch_official_detail_channels(&client, &channel_meta).await
        && !channels.is_empty()
    {
        return Ok(channels);
    }

    tracing::warn!(
        "Official GDTV detail resolver yielded no playable channels; falling back to list playUrl filtering"
    );
    let channels = parse_official_channels_from_list(channel_meta);
    filter_playable_official_channels(&client, channels).await
}

async fn fetch_official_detail_channels(
    client: &reqwest::Client,
    channel_meta: &[OfficialChannelMeta],
) -> anyhow::Result<Vec<OfficialGdtvChannel>> {
    let node = fetch_tcdn_node(client).await?;
    let node = resolve_tcdn_wsnode(&node).await.unwrap_or(node);
    let node = base64_encode(&node);
    let mut channels = Vec::new();

    for meta in channel_meta {
        let Some(play_url) = fetch_official_detail_play_url(client, meta.pk, &node).await? else {
            continue;
        };
        if is_blocked_source_url(&play_url) {
            tracing::warn!(
                "Skipping non-official GDTV detail URL for {}: {}",
                meta.name,
                play_url
            );
            continue;
        }
        match is_playable_official_url(client, &play_url).await {
            Ok(true) => channels.push(OfficialGdtvChannel {
                tvg_id: format!("gdtv-{}", meta.pk),
                name: meta.name.clone(),
                logo: meta.logo.clone(),
                play_url,
            }),
            Ok(false) => tracing::warn!(
                "Skipping non-playable official GDTV detail URL for {}: {}",
                meta.name,
                play_url
            ),
            Err(error) => tracing::warn!(
                "Skipping unverified official GDTV detail URL for {}: {:#}",
                meta.name,
                error
            ),
        }
    }

    Ok(channels)
}

async fn fetch_tcdn_node(client: &reqwest::Client) -> anyhow::Result<String> {
    let response = signed_get(client, TCDN_PARAM_URL)?
        .send()
        .await?
        .error_for_status()?
        .json::<TcdnParamResponse>()
        .await?;

    Ok(response.node)
}

fn signed_get(client: &reqwest::Client, url: &str) -> anyhow::Result<reqwest::RequestBuilder> {
    signed_get_with_device(client, url, OFFICIAL_DEVICE_ID)
}

fn signed_get_with_device(
    client: &reqwest::Client,
    url: &str,
    device_id: &str,
) -> anyhow::Result<reqwest::RequestBuilder> {
    let headers = gdtv_signer::sign_get(url, device_id, OFFICIAL_CLIENT)?;
    Ok(client
        .get(url)
        .header("Origin", OFFICIAL_ORIGIN)
        .header("Referer", OFFICIAL_REFERER)
        .header("Accept", "application/json, text/plain, */*")
        .header("Content-Type", "application/json")
        .header("X-ITOUCHTV-CLIENT", headers.client)
        .header("X-ITOUCHTV-DEVICE-ID", headers.device_id)
        .header("X-ITOUCHTV-Ca-Timestamp", headers.timestamp)
        .header("X-ITOUCHTV-Ca-Key", headers.key)
        .header("X-ITOUCHTV-Ca-Signature", headers.signature))
}

async fn resolve_tcdn_wsnode(node: &str) -> anyhow::Result<String> {
    Ok(open_tcdn_ws_session(node).await?.wsnode)
}

async fn open_tcdn_ws_session(node: &str) -> anyhow::Result<TcdnWsSession> {
    let mut request = TCDN_WS_URL.into_client_request()?;
    let headers = request.headers_mut();
    headers.insert("Origin", HeaderValue::from_static(OFFICIAL_ORIGIN));
    headers.insert(
        "User-Agent",
        HeaderValue::from_static(OFFICIAL_HTTP_USER_AGENT),
    );
    let (mut socket, _) = timeout(
        Duration::from_secs(TCDN_TIMEOUT_SECS),
        connect_async(request),
    )
    .await??;
    let request = serde_json::json!({ "route": "getwsparam", "message": node }).to_string();
    timeout(
        Duration::from_secs(TCDN_TIMEOUT_SECS),
        socket.send(Message::Text(request.into())),
    )
    .await??;

    while let Some(message) = timeout(Duration::from_secs(TCDN_TIMEOUT_SECS), socket.next()).await?
    {
        let message = message?;
        if !message.is_text() {
            continue;
        }
        let response = serde_json::from_str::<TcdnWsResponse>(message.to_text()?)?;
        if response.status == Some(201)
            && let Some(wsnode) = response.wsnode.filter(|node| !node.trim().is_empty())
        {
            return Ok(TcdnWsSession {
                wsnode,
                _socket: socket,
            });
        }
    }

    anyhow::bail!("TCDN websocket closed without wsnode")
}

async fn fetch_official_detail_play_url(
    client: &reqwest::Client,
    pk: u64,
    node: &str,
) -> anyhow::Result<Option<String>> {
    let detail_url = official_detail_url(pk, node);
    preflight_official_detail(client, &detail_url).await?;
    let response = signed_get(client, &detail_url)?
        .send()
        .await?
        .error_for_status()?
        .json::<OfficialChannel>()
        .await?;

    parse_official_play_url(&response.play_url)
}

fn official_detail_url(pk: u64, node: &str) -> String {
    // Match the official web app's GET query construction. Do not form-urlencode
    // the base64 node here; the server distinguishes this route subtly, and the
    // browser sends the value as the raw `window.btoa(...)` output.
    format!("https://gdtv-api.gdtv.cn/api/tv/v2/tvChannel/{pk}?tvChannelPk={pk}&node={node}")
}

async fn preflight_official_detail(
    client: &reqwest::Client,
    detail_url: &str,
) -> anyhow::Result<()> {
    client
        .request(reqwest::Method::OPTIONS, detail_url)
        .header("Origin", OFFICIAL_ORIGIN)
        .header("Access-Control-Request-Method", "GET")
        .header(
            "Access-Control-Request-Headers",
            "content-type,x-itouchtv-ca-key,x-itouchtv-ca-signature,x-itouchtv-ca-timestamp,x-itouchtv-client,x-itouchtv-device-id",
        )
        .header("Sec-Fetch-Mode", "cors")
        .send()
        .await?
        .error_for_status()?;
    Ok(())
}

fn official_channel_meta_by_pk(pk: u64) -> Option<OfficialChannelMeta> {
    pinned_official_channel_meta()
        .into_iter()
        .find(|channel| channel.pk == pk)
}

fn pinned_official_channel_meta() -> Vec<OfficialChannelMeta> {
    [
        (
            43,
            "广东卫视",
            "https://parco-zh.github.io/demo/guangdong.jpg",
        ),
        (44, "广东珠江", ""),
        (45, "广东新闻", ""),
        (48, "广东民生", ""),
        (47, "广东体育", ""),
        (51, "大湾区卫视", ""),
        (46, "大湾区卫视（海外版）", ""),
        (53, "广东影视", ""),
        (54, "广东少儿", ""),
        (66, "嘉佳卡通", ""),
        (15, "岭南戏曲", ""),
        (74, "广东移动", ""),
        (42, "南方购物", ""),
        (16, "4K超高清", ""),
        (99, "健康", ""),
        (100, "广东台经典剧", ""),
        (102, "GRTN生活频道", ""),
    ]
    .into_iter()
    .map(|(pk, name, logo)| OfficialChannelMeta {
        pk,
        name: name.to_owned(),
        logo: (!logo.is_empty()).then(|| logo.to_owned()),
        list_play_url: None,
    })
    .collect()
}

fn parse_official_channels_from_list(
    channels: Vec<OfficialChannelMeta>,
) -> Vec<OfficialGdtvChannel> {
    channels
        .into_iter()
        .filter_map(|channel| {
            let play_url = channel.list_play_url?;
            if is_blocked_source_url(&play_url) {
                tracing::warn!(
                    "Skipping non-official GDTV URL for {}: {}",
                    channel.name,
                    play_url
                );
                return None;
            }
            Some(OfficialGdtvChannel {
                tvg_id: format!("gdtv-{}", channel.pk),
                name: channel.name,
                logo: channel.logo,
                play_url,
            })
        })
        .collect()
}

fn base64_encode(value: &str) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let bytes = value.as_bytes();
    let mut encoded = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let b0 = chunk[0];
        let b1 = *chunk.get(1).unwrap_or(&0);
        let b2 = *chunk.get(2).unwrap_or(&0);
        encoded.push(TABLE[(b0 >> 2) as usize] as char);
        encoded.push(TABLE[(((b0 & 0b0000_0011) << 4) | (b1 >> 4)) as usize] as char);
        if chunk.len() > 1 {
            encoded.push(TABLE[(((b1 & 0b0000_1111) << 2) | (b2 >> 6)) as usize] as char);
        } else {
            encoded.push('=');
        }
        if chunk.len() > 2 {
            encoded.push(TABLE[(b2 & 0b0011_1111) as usize] as char);
        } else {
            encoded.push('=');
        }
    }
    encoded
}

async fn filter_playable_official_channels(
    client: &reqwest::Client,
    channels: Vec<OfficialGdtvChannel>,
) -> anyhow::Result<Vec<OfficialGdtvChannel>> {
    let mut playable_channels = Vec::new();
    for channel in channels {
        match is_playable_official_url(client, &channel.play_url).await {
            Ok(true) => playable_channels.push(channel),
            Ok(false) => tracing::warn!(
                "Skipping non-playable official GDTV URL for {}: {}",
                channel.name,
                channel.play_url
            ),
            Err(error) => tracing::warn!(
                "Skipping unverified official GDTV URL for {}: {:#}",
                channel.name,
                error
            ),
        }
    }
    Ok(playable_channels)
}

async fn is_playable_official_url(client: &reqwest::Client, url: &str) -> anyhow::Result<bool> {
    if is_blocked_source_url(url) {
        return Ok(false);
    }

    let response = client
        .get(url)
        .header("Referer", OFFICIAL_REFERER)
        .header(
            "Accept",
            "application/vnd.apple.mpegurl, application/x-mpegURL, */*",
        )
        .header("Range", "bytes=0-")
        .header("Accept-Encoding", "identity;q=1, *;q=0")
        .send()
        .await?;

    if !response.status().is_success() {
        return Ok(false);
    }

    let content_type = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_ascii_lowercase();
    let body = response.text().await?;

    Ok(body.trim_start().starts_with("#EXTM3U")
        || content_type.contains("mpegurl")
        || content_type.contains("vnd.apple"))
}

fn parse_official_channel_meta(channels: Vec<OfficialChannel>) -> Vec<OfficialChannelMeta> {
    channels
        .into_iter()
        .map(|channel| OfficialChannelMeta {
            pk: channel.pk,
            name: channel.name,
            logo: channel.avatar_url,
            list_play_url: parse_official_play_url(&channel.play_url).ok().flatten(),
        })
        .collect()
}

fn parse_official_play_url(raw: &str) -> anyhow::Result<Option<String>> {
    let play_url = serde_json::from_str::<OfficialPlayUrl>(raw)
        .with_context(|| format!("invalid official playUrl JSON: {raw}"))?;
    Ok(play_url.hd.filter(|url| !url.trim().is_empty()))
}

fn playlist_m3u_from_official_meta(channels: &[OfficialChannelMeta], base_url: &str) -> String {
    let mut m3u = String::from("#EXTM3U\n");
    let base_url = base_url.trim_end_matches('/');
    for channel in channels {
        let play_url = format!(
            "{}/{}/{}",
            base_url,
            GDTV_PLAY_PATH_PREFIX.trim_start_matches('/'),
            channel.pk
        );
        m3u.push_str(&format!(
            "#EXTINF:-1 tvg-id=\"gdtv-{}\" tvg-name=\"{}\" tvg-logo=\"{}\" group-title=\"{}\",{}\n{}\n",
            channel.pk,
            channel.name,
            channel.logo.as_deref().unwrap_or(""),
            GDTV_GROUP,
            channel.name,
            play_url
        ));
    }
    m3u
}

pub fn playlist_m3u_from_official_channels(channels: &[OfficialGdtvChannel]) -> String {
    let mut m3u = String::from("#EXTM3U\n");
    for channel in channels {
        if is_blocked_source_url(&channel.play_url) {
            continue;
        }
        m3u.push_str(&format!(
            "#EXTINF:-1 tvg-id=\"{}\" tvg-name=\"{}\" tvg-logo=\"{}\" group-title=\"{}\",{}\n{}\n",
            channel.tvg_id,
            channel.name,
            channel.logo.as_deref().unwrap_or(""),
            GDTV_GROUP,
            channel.name,
            channel.play_url
        ));
    }
    m3u
}

pub fn playlist_m3u() -> String {
    let mut m3u = String::from("#EXTM3U\n");
    for channel in GDTV_CHANNELS
        .iter()
        .filter(|channel| channel.is_exposable())
    {
        let Some(play_url) = channel.play_url else {
            continue;
        };
        m3u.push_str(&format!(
            "#EXTINF:-1 tvg-id=\"{}\" tvg-name=\"{}\" tvg-logo=\"{}\" group-title=\"{}\",{}\n{}\n",
            channel.tvg_id, channel.name, channel.logo, GDTV_GROUP, channel.name, play_url
        ));
    }
    m3u
}

pub fn channels() -> Vec<Channel> {
    GDTV_CHANNELS
        .iter()
        .filter(|channel| channel.is_exposable())
        .filter_map(|channel| {
            let play_url = channel.play_url?;
            Some(Channel {
                origin: ChannelOrigin::Local,
                name: channel.name.to_owned(),
                group: GDTV_GROUP.to_owned(),
                url: play_url.to_owned(),
                logo: (!channel.logo.is_empty()).then(|| channel.logo.to_owned()),
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
                is_online: true,
            })
        })
        .collect()
}

pub fn exposable_channel_count() -> usize {
    GDTV_CHANNELS
        .iter()
        .filter(|channel| channel.is_exposable())
        .count()
}

impl GdtvChannel {
    pub fn is_exposable(&self) -> bool {
        self.play_url
            .map(|url| !is_blocked_source_url(url))
            .unwrap_or(false)
    }
}

pub fn is_blocked_source_url(url: &str) -> bool {
    !is_official_source_url(url)
}

pub fn is_official_source_url(url: &str) -> bool {
    let lower = url.to_ascii_lowercase();
    [
        "gdtv.cn",
        "itouchtv.cn",
        "gdtv.com.cn",
        "grtn.cn",
        "gdtvcdn.cn",
    ]
    .iter()
    .any(|domain| lower.contains(domain))
}

pub fn extinf_count(m3u: &str) -> usize {
    m3u.lines()
        .filter(|line| line.starts_with("#EXTINF"))
        .count()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[ignore = "hits official GDTV HTTP APIs"]
    async fn official_detail_fetches_play_url_without_browser() {
        let play_url = resolve_official_play_url(43)
            .await
            .expect("resolve official play URL");
        assert!(play_url.starts_with("https://tcdn.itouchtv.cn/live/"));
        assert!(play_url.contains("t_token="));
        let client = official_http_client().expect("client");
        assert!(
            is_playable_official_url(&client, &play_url)
                .await
                .expect("playable check")
        );
    }

    #[test]
    fn parses_cache_ttl_from_env_value() {
        assert_eq!(parse_cache_ttl_secs(Some("12".to_owned())), 12);
        assert_eq!(parse_cache_ttl_secs(Some(" 7 ".to_owned())), 7);
        assert_eq!(
            parse_cache_ttl_secs(Some("0".to_owned())),
            DEFAULT_OFFICIAL_PLAY_URL_CACHE_TTL_SECS
        );
        assert_eq!(
            parse_cache_ttl_secs(Some("nope".to_owned())),
            DEFAULT_OFFICIAL_PLAY_URL_CACHE_TTL_SECS
        );
        assert_eq!(
            parse_cache_ttl_secs(None),
            DEFAULT_OFFICIAL_PLAY_URL_CACHE_TTL_SECS
        );
    }

    #[test]
    fn pinned_official_channel_meta_matches_known_official_ids() {
        let channels = pinned_official_channel_meta();
        let by_pk = |pk| {
            channels
                .iter()
                .find(|channel| channel.pk == pk)
                .map(|channel| channel.name.as_str())
        };

        assert_eq!(by_pk(43), Some("广东卫视"));
        assert_eq!(by_pk(44), Some("广东珠江"));
        assert_eq!(by_pk(45), Some("广东新闻"));
        assert_eq!(by_pk(47), Some("广东体育"));
        assert_eq!(by_pk(48), Some("广东民生"));
        assert_eq!(by_pk(51), Some("大湾区卫视"));
        assert_eq!(by_pk(46), Some("大湾区卫视（海外版）"));
        assert_eq!(by_pk(53), Some("广东影视"));
        assert_eq!(by_pk(54), Some("广东少儿"));
        assert_eq!(by_pk(66), Some("嘉佳卡通"));
        assert_eq!(by_pk(15), Some("岭南戏曲"));
        assert_eq!(by_pk(74), Some("广东移动"));
        assert_eq!(by_pk(42), Some("南方购物"));
        assert_eq!(by_pk(16), Some("4K超高清"));
        assert_eq!(by_pk(99), Some("健康"));
        assert_eq!(by_pk(100), Some("广东台经典剧"));
        assert_eq!(by_pk(102), Some("GRTN生活频道"));

        assert_eq!(by_pk(49), None);
        assert_eq!(by_pk(52), None);
    }

    #[test]
    fn static_gdtv_playlist_is_empty_without_official_fetch() {
        let m3u = playlist_m3u();
        assert!(m3u.starts_with("#EXTM3U\n"));
        assert_eq!(extinf_count(&m3u), exposable_channel_count());
        assert_eq!(extinf_count(&m3u), 0);
        assert!(!m3u.to_ascii_lowercase().contains("catvod.com"));
        assert!(
            GDTV_CHANNELS
                .iter()
                .all(|channel| channel.play_url.is_none())
        );
    }

    #[test]
    fn non_official_urls_are_blocked() {
        assert!(is_blocked_source_url(
            "https://iptv.catvod.com/live.php?id=x"
        ));
        assert!(is_blocked_source_url("https://migu.188766.xyz/?id=gd"));
        assert!(!is_blocked_source_url(
            "https://live.gdtv.cn/example/index.m3u8"
        ));
        assert!(
            GDTV_CHANNELS
                .iter()
                .all(|channel| { channel.play_url.map(is_official_source_url).unwrap_or(true) })
        );
    }

    #[test]
    fn official_playlist_uses_only_official_urls() {
        let channels = vec![OfficialGdtvChannel {
            tvg_id: "gdtv-43".to_owned(),
            name: "广东卫视".to_owned(),
            logo: Some("https://img.gdtv.cn/logo.png".to_owned()),
            play_url: "https://tcdn.itouchtv.cn/live/gdws.m3u8?t_token=test".to_owned(),
        }];
        let m3u = playlist_m3u_from_official_channels(&channels);
        assert_eq!(extinf_count(&m3u), 1);
        assert!(m3u.contains("广东卫视"));
        assert!(m3u.contains("https://tcdn.itouchtv.cn/live/gdws.m3u8"));
        assert!(!m3u.to_ascii_lowercase().contains("catvod.com"));
    }

    #[test]
    fn local_official_playlist_uses_request_base_url() {
        let channels = vec![OfficialChannelMeta {
            pk: 43,
            name: "广东卫视".to_owned(),
            logo: None,
            list_play_url: None,
        }];
        let m3u = playlist_m3u_from_official_meta(&channels, "http://127.0.0.1:12315/");

        assert_eq!(extinf_count(&m3u), 1);
        assert!(m3u.contains("http://127.0.0.1:12315/gdtv/play/43"));
        assert!(!m3u.contains("127.0.0.1:12345/gdtv/play/43"));
    }

    #[test]
    fn parses_official_play_url_json() {
        let play_url = parse_official_play_url(
            r#"{"hd":"https://tcdn.itouchtv.cn/live/gdws.m3u8?t_token=test"}"#,
        )
        .expect("parse playUrl")
        .expect("hd url");
        assert_eq!(
            play_url,
            "https://tcdn.itouchtv.cn/live/gdws.m3u8?t_token=test"
        );
    }

    #[test]
    fn gdtv_channels_are_unique() {
        let mut names = GDTV_CHANNELS
            .iter()
            .map(|channel| channel.name)
            .collect::<Vec<_>>();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), GDTV_CHANNELS.len());
    }
}
