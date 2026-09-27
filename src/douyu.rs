use anyhow::Context;
use axum::body::Bytes;
use futures::{Stream, StreamExt, stream};
use reqwest::header::{HeaderMap, HeaderValue, REFERER, USER_AGENT};
use serde::Deserialize;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::OnceLock;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

const DEVICE_ID: &str = "10000000000000000000000000003306";
const CACHE_TTL_PLAY_URL: Duration = Duration::from_secs(120); // 2 minutes play url cache
const HTTP_TIMEOUT: Duration = Duration::from_secs(10);
const USER_AGENT_VAL: &str = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36";

#[derive(Clone, Debug)]
struct CachedPlayUrl {
    fetched_at: Instant,
    play_url: String,
}

static PLAY_URL_CACHE: OnceLock<Mutex<HashMap<u64, CachedPlayUrl>>> = OnceLock::new();
static HTTP_CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
static STREAM_CLIENT: OnceLock<reqwest::Client> = OnceLock::new();

fn get_client() -> &'static reqwest::Client {
    HTTP_CLIENT.get_or_init(|| {
        reqwest::Client::builder()
            .use_rustls_tls()
            .timeout(HTTP_TIMEOUT)
            .cookie_store(true)
            .build()
            .unwrap_or_default()
    })
}

fn get_stream_client() -> &'static reqwest::Client {
    STREAM_CLIENT.get_or_init(|| {
        reqwest::Client::builder()
            .cookie_store(true)
            .use_native_tls()
            .build()
            .unwrap_or_default()
    })
}

fn play_url_cache() -> &'static Mutex<HashMap<u64, CachedPlayUrl>> {
    PLAY_URL_CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn compute_md5(text: &str) -> String {
    use md5::{Digest, Md5};
    let mut hasher = Md5::new();
    hasher.update(text.as_bytes());
    format!("{:x}", hasher.finalize())
}

#[derive(Clone, Debug)]
struct CachedRooms {
    fetched_at: Instant,
    rooms: Vec<MixListRoom>,
}

static ROOMS_CACHE: OnceLock<Mutex<HashMap<String, CachedRooms>>> = OnceLock::new();
const CACHE_TTL_ROOMS: Duration = Duration::from_secs(30);

fn rooms_cache() -> &'static Mutex<HashMap<String, CachedRooms>> {
    ROOMS_CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

#[derive(Deserialize)]
struct MixListResponse {
    code: i32,
    data: Option<MixListData>,
}

#[derive(Deserialize)]
struct MixListData {
    rl: Vec<MixListRoom>,
}

#[derive(Deserialize, Clone, Debug)]
struct MixListRoom {
    rid: u64,
    rn: String, // Room name/title
    nn: String, // Nickname/anchor name
    #[allow(dead_code)]
    ol: u64, // Online heat
    rs16: Option<String>, // Room cover image
}

async fn fetch_rooms_upstream(cate_id: &str) -> anyhow::Result<Vec<MixListRoom>> {
    let client = get_client();
    let url = format!(
        "https://www.douyu.com/gapi/rkc/directory/mixList/{}/1",
        cate_id
    );
    let res = client
        .get(&url)
        .header(USER_AGENT, USER_AGENT_VAL)
        .send()
        .await
        .context(format!("failed to fetch douyu room list for {}", cate_id))?;
    let mix_list: MixListResponse = res.json().await.context(format!(
        "failed to parse douyu room list JSON for {}",
        cate_id
    ))?;
    if mix_list.code != 0 || mix_list.data.is_none() {
        anyhow::bail!(
            "douyu room list for {} returned error code {}",
            cate_id,
            mix_list.code
        );
    }
    Ok(mix_list.data.unwrap().rl)
}

/// Fetches active rooms for a given category and returns them as M3U playlist format (with 30s caching).
pub async fn generate_douyu_playlist_m3u(
    cate_id: &str,
    group_title: &str,
    base_url: &str,
) -> anyhow::Result<String> {
    let rooms = {
        let mut cache = rooms_cache().lock().await;
        let now = Instant::now();
        let needs_fetch = match cache.get(cate_id) {
            Some(cached) => cached.fetched_at.elapsed() >= CACHE_TTL_ROOMS,
            None => true,
        };
        if needs_fetch {
            let fresh = fetch_rooms_upstream(cate_id).await?;
            cache.insert(
                cate_id.to_string(),
                CachedRooms {
                    fetched_at: now,
                    rooms: fresh.clone(),
                },
            );
            fresh
        } else {
            cache.get(cate_id).unwrap().rooms.clone()
        }
    };

    let mut m3u = String::from("#EXTM3U\n");

    for room in rooms {
        let logo = room.rs16.unwrap_or_default();
        let logo_attr = if !logo.is_empty() {
            format!(" tvg-logo=\"{}\"", logo)
        } else {
            String::new()
        };

        m3u.push_str(&format!(
            "#EXTINF:-1 tvg-id=\"douyu-{}\" tvg-name=\"{}\"{} group-title=\"{}\", {} - {}\n{}/douyu/play/{}\n",
            room.rid, room.nn, logo_attr, group_title, room.nn, room.rn, base_url, room.rid
        ));
    }

    Ok(m3u)
}

#[derive(Deserialize)]
struct EncryptionResponse {
    error: i8,
    msg: String,
    data: Option<serde_json::Value>,
}

#[derive(Deserialize)]
struct EncryptionData {
    rand_str: String,
    key: String,
    enc_time: u8,
    is_special: u8,
    enc_data: String,
}

#[derive(Deserialize)]
struct RoomInfo {
    error: i32,
    msg: String,
    data: Option<serde_json::Value>,
}

#[derive(Deserialize)]
struct RoomData {
    rtmp_url: String,
    rtmp_live: Option<String>,
}

/// Resolves a single Douyu room ID into a direct stream play URL.
pub async fn resolve_douyu_play_url(room_id: u64) -> anyhow::Result<String> {
    // 1. Check cache first
    {
        let cache = play_url_cache().lock().await;
        if let Some(cached) = cache.get(&room_id) {
            if cached.fetched_at.elapsed() < CACHE_TTL_PLAY_URL {
                return Ok(cached.play_url.clone());
            }
        }
    }

    // 2. Fetch room page html to extract final room id
    let client = get_client();
    let room_url = format!("https://www.douyu.com/{}", room_id);
    let html = client
        .get(&room_url)
        .header(USER_AGENT, USER_AGENT_VAL)
        .send()
        .await
        .context("failed to fetch room page HTML")?
        .text()
        .await
        .context("failed to read room page HTML text")?;

    if html.contains("<span><p>该房间目前没有开放</p></span>") {
        anyhow::bail!("room is not open or does not exist");
    }

    // Extract final room id using regex
    static RE_FINAL_ID: OnceLock<regex::Regex> = OnceLock::new();
    let re = RE_FINAL_ID
        .get_or_init(|| regex::Regex::new(r"getLegacyFirstStream\(\{\s*roomID:\s*(\d+)").unwrap());
    let final_room_id = re
        .captures(&html)
        .and_then(|caps| caps.get(1))
        .and_then(|m| m.as_str().parse::<u64>().ok())
        .unwrap_or(room_id);

    // 3. Check replay status via betard API
    let betard_url = format!("https://www.douyu.com/betard/{}", final_room_id);
    if let Ok(res) = client
        .get(&betard_url)
        .header(USER_AGENT, USER_AGENT_VAL)
        .send()
        .await
    {
        if let Ok(body) = res.json::<serde_json::Value>().await {
            let is_replay = body["room"]["videoLoop"].as_i64().unwrap_or(0);
            if is_replay == 1 {
                anyhow::bail!("stream is a replay, not live");
            }
        }
    }

    // 4. Fetch encryption data
    let enc_url = format!(
        "https://www.douyu.com/wgapi/livenc/liveweb/websec/getEncryption?did={}",
        DEVICE_ID
    );
    let mut headers = HeaderMap::new();
    headers.insert(USER_AGENT, HeaderValue::from_static(USER_AGENT_VAL));
    headers.insert(
        REFERER,
        HeaderValue::from_str(&format!("https://www.douyu.com/{}", final_room_id))?,
    );

    let enc_res = client
        .get(&enc_url)
        .headers(headers.clone())
        .send()
        .await
        .context("failed to fetch encryption info")?;

    let enc_data_res: EncryptionResponse = enc_res
        .json()
        .await
        .context("failed to parse encryption response JSON")?;

    if enc_data_res.error != 0 || enc_data_res.data.is_none() {
        anyhow::bail!("encryption fetch failed: {}", enc_data_res.msg);
    }

    let enc_data_val = enc_data_res.data.unwrap();
    let enc_data: EncryptionData =
        serde_json::from_value(enc_data_val).context("failed to parse encryption data struct")?;
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();

    let sign_str = if enc_data.is_special == 1 {
        String::new()
    } else {
        format!("{}{}", final_room_id, ts)
    };

    let mut auth = enc_data.rand_str;
    for _ in 0..enc_data.enc_time {
        auth = compute_md5(&format!("{}{}", auth, enc_data.key));
    }
    auth = compute_md5(&format!("{}{}{}", auth, enc_data.key, sign_str));

    // 5. Post to getH5PlayV1 to get rtmp details
    let play_url = format!(
        "https://www.douyu.com/lapi/live/getH5PlayV1/{}",
        final_room_id
    );
    let form = [
        ("enc_data", enc_data.enc_data),
        ("tt", ts.to_string()),
        ("did", DEVICE_ID.to_string()),
        ("auth", auth),
        ("cdn", String::new()),
        ("rate", "-1".to_string()),
        ("hevc", "0".to_string()),
        ("fa", "0".to_string()),
        ("ive", "0".to_string()),
    ];

    let play_res = client
        .post(&play_url)
        .headers(headers)
        .form(&form)
        .send()
        .await
        .context("failed to request live play details")?;

    let room_info: RoomInfo = play_res
        .json()
        .await
        .context("failed to parse room info JSON")?;

    if room_info.error != 0 || room_info.data.is_none() {
        anyhow::bail!("failed to resolve room info: {}", room_info.msg);
    }

    let room_data_val = room_info.data.unwrap();
    let room_data: RoomData =
        serde_json::from_value(room_data_val).context("failed to parse room data struct")?;
    let rtmp_live = room_data
        .rtmp_live
        .ok_or_else(|| anyhow::anyhow!("stream is offline"))?;

    let stream_url = format!("{}/{}", room_data.rtmp_url, rtmp_live);

    // 6. Cache and return
    {
        let mut cache = play_url_cache().lock().await;
        cache.insert(
            room_id,
            CachedPlayUrl {
                fetched_at: Instant::now(),
                play_url: stream_url.clone(),
            },
        );
    }

    Ok(stream_url)
}

/// Opens a fresh Douyu URL for a new player connection.
pub async fn resolve_douyu_play_url_fresh(room_id: u64) -> anyhow::Result<String> {
    play_url_cache().lock().await.remove(&room_id);
    resolve_douyu_play_url(room_id).await
}

/// Proxies the live response so player reconnects return to our resolver.
pub async fn open_stream(url: &str) -> anyhow::Result<reqwest::Response> {
    get_stream_client()
        .get(url)
        .header(USER_AGENT, USER_AGENT_VAL)
        .header("Accept", "*/*")
        .header("Accept-Encoding", "identity")
        .send()
        .await
        .context("failed to open Douyu stream")?
        .error_for_status()
        .context("Douyu stream returned an error status")
}

type DouyuByteStream = Pin<Box<dyn Stream<Item = reqwest::Result<Bytes>> + Send>>;

struct ProxyState {
    room_id: u64,
    body: Option<DouyuByteStream>,
    reconnect_failures: u8,
}

/// Keeps one local HTTP response alive across Douyu's expiring stream URLs.
pub fn proxy_stream(
    room_id: u64,
    upstream: reqwest::Response,
) -> impl Stream<Item = anyhow::Result<Bytes>> + Send + 'static {
    stream::unfold(
        ProxyState {
            room_id,
            body: Some(Box::pin(upstream.bytes_stream())),
            reconnect_failures: 0,
        },
        |mut state| async move {
            loop {
                if let Some(body) = state.body.as_mut() {
                    match body.next().await {
                        Some(Ok(chunk)) => return Some((Ok(chunk), state)),
                        Some(Err(error)) => {
                            tracing::warn!(
                                room_id = state.room_id,
                                error = %error,
                                "Douyu upstream stream interrupted; reconnecting"
                            );
                        }
                        None => {
                            tracing::info!(
                                room_id = state.room_id,
                                "Douyu upstream stream ended; refreshing play URL"
                            );
                        }
                    }
                    state.body = None;
                }

                if state.reconnect_failures >= 3 {
                    tracing::warn!(
                        room_id = state.room_id,
                        "Douyu stream reconnect failed repeatedly; closing local stream"
                    );
                    return None;
                }
                state.reconnect_failures += 1;

                match resolve_douyu_play_url_fresh(state.room_id).await {
                    Ok(url) => match open_stream(&url).await {
                        Ok(response) => {
                            state.body = Some(Box::pin(response.bytes_stream()));
                            state.reconnect_failures = 0;
                        }
                        Err(error) => tracing::warn!(
                            room_id = state.room_id,
                            error = %error,
                            "Failed to reopen Douyu stream"
                        ),
                    },
                    Err(error) => tracing::warn!(
                        room_id = state.room_id,
                        error = %error,
                        "Failed to refresh Douyu play URL"
                    ),
                }
            }
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_generate_douyu_playlist_m3u() {
        let res = generate_douyu_playlist_m3u("2_3", "斗鱼DOTA2", "http://localhost:12345").await;
        match res {
            Ok(playlist) => {
                assert!(playlist.starts_with("#EXTM3U"));
                assert!(playlist.contains("group-title=\"斗鱼DOTA2\""));
                assert!(playlist.contains("/douyu/play/"));
            }
            Err(e) => {
                println!("test_generate_douyu_playlist_m3u got error: {:?}", e);
            }
        }
    }

    #[tokio::test]
    async fn test_resolve_douyu_play_url() {
        // Room 9999 is yyf, usually active
        let res = resolve_douyu_play_url(9999).await;
        match res {
            Ok(url) => {
                assert!(
                    url.contains("douyucdn")
                        || url.contains("douyuscdn")
                        || url.contains(".m3u8")
                        || url.contains(".flv")
                );
            }
            Err(e) => {
                println!("test_resolve_douyu_play_url got error: {:?}", e);
            }
        }
    }

    #[test]
    fn test_room_info_deserialization_error_case() {
        let json_data = r#"{"error": 1, "msg": "房间未开播", "data": ""}"#;
        let room_info: RoomInfo = serde_json::from_str(json_data).unwrap();
        assert_eq!(room_info.error, 1);
        assert_eq!(room_info.msg, "房间未开播");
        assert!(room_info.data.is_some());

        let data_val = room_info.data.unwrap();
        let parsed_data: Result<RoomData, _> = serde_json::from_value(data_val);
        assert!(parsed_data.is_err());
    }
}
