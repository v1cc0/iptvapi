use anyhow::Context;
use bytes::{Bytes, BytesMut};
use futures::{Stream, StreamExt, stream};
use reqwest::header::{HeaderMap, HeaderValue, REFERER, USER_AGENT};
use serde::Deserialize;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::OnceLock;
use std::time::{Duration, Instant};
use tokio::{sync::Mutex, task::JoinSet};

const DEVICE_ID: &str = "10000000000000000000000000003306";
const PREFETCH_MAX_AGE: Duration = Duration::from_secs(120);
const INITIAL_PLAY_URL_PREFETCH: Duration = Duration::from_secs(240);
const RECONNECT_PLAY_URL_PREFETCH: Duration = Duration::from_secs(180);
const HTTP_TIMEOUT: Duration = Duration::from_secs(10);
const USER_AGENT_VAL: &str = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36";

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
            .connect_timeout(HTTP_TIMEOUT)
            .read_timeout(HTTP_TIMEOUT)
            .build()
            .unwrap_or_default()
    })
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
    // Fetch the room page to find the canonical room ID.
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

    // Check for replays via the betard API.
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

    // Fetch encryption data for the playback request.
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

    // Request the signed playback URL.
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

    Ok(format!("{}/{}", room_data.rtmp_url, rtmp_live))
}

type DouyuByteStream = Pin<Box<dyn Stream<Item = reqwest::Result<Bytes>> + Send>>;

type PlayUrlPrefetch = JoinSet<anyhow::Result<(Instant, String)>>;

pub struct FlvInput {
    body: DouyuByteStream,
    buffer: BytesMut,
}

impl FlvInput {
    async fn fill(&mut self, len: usize) -> anyhow::Result<bool> {
        while self.buffer.len() < len {
            let Some(chunk) = self.body.next().await else {
                return Ok(false);
            };
            self.buffer
                .extend_from_slice(&chunk.map_err(reqwest::Error::without_url)?);
        }
        Ok(true)
    }

    async fn read_header(&mut self) -> anyhow::Result<Bytes> {
        anyhow::ensure!(self.fill(9).await?, "truncated FLV header");
        let offset = u32::from_be_bytes(self.buffer[5..9].try_into()?) as usize;
        // shortcut: Douyu emits the standard 9-byte header, raise the extension cap if it changes.
        anyhow::ensure!(
            self.buffer.starts_with(b"FLV\x01") && (9..=4096).contains(&offset),
            "invalid FLV header"
        );
        anyhow::ensure!(self.fill(offset + 4).await?, "truncated FLV header");
        anyhow::ensure!(
            self.buffer[offset..offset + 4] == [0; 4],
            "invalid FLV header trailer"
        );
        Ok(self.buffer.split_to(offset + 4).freeze())
    }

    async fn next_tag(&mut self) -> anyhow::Result<Option<BytesMut>> {
        if !self.fill(11).await? {
            anyhow::ensure!(self.buffer.is_empty(), "truncated FLV tag header");
            return Ok(None);
        }
        let payload_size = u32::from_be_bytes([0, self.buffer[1], self.buffer[2], self.buffer[3]]);
        let tag_size = payload_size as usize + 11;
        anyhow::ensure!(self.fill(tag_size + 4).await?, "truncated FLV tag");
        let previous_tag_size = u32::from_be_bytes(self.buffer[tag_size..tag_size + 4].try_into()?);
        anyhow::ensure!(
            previous_tag_size as usize == tag_size,
            "invalid FLV tag trailer"
        );
        Ok(Some(self.buffer.split_to(tag_size + 4)))
    }
}

pub async fn open_stream(url: &str) -> anyhow::Result<(Bytes, FlvInput)> {
    let response = get_stream_client()
        .get(url)
        .header(USER_AGENT, USER_AGENT_VAL)
        .header("Accept", "*/*")
        .header("Accept-Encoding", "identity")
        .send()
        .await
        .map_err(reqwest::Error::without_url)
        .context("failed to open Douyu stream")?
        .error_for_status()
        .map_err(reqwest::Error::without_url)
        .context("Douyu stream returned an error status")?;
    let mut input = FlvInput {
        body: Box::pin(response.bytes_stream()),
        buffer: BytesMut::new(),
    };
    let header = input.read_header().await?;
    Ok((header, input))
}

fn schedule_play_url_prefetch(room_id: u64, delay: Duration) -> PlayUrlPrefetch {
    let mut prefetch = JoinSet::new();
    prefetch.spawn(async move {
        tokio::time::sleep(delay).await;
        let url = resolve_douyu_play_url(room_id).await?;
        Ok((Instant::now(), url))
    });
    prefetch
}

#[derive(Default)]
struct FlvTimeline {
    last_media: Option<u32>,
    has_video: bool,
    awaiting_resume: bool,
    offset_ms: i64,
}

impl FlvTimeline {
    fn reconnect(&mut self) {
        self.awaiting_resume = self.last_media.is_some();
    }

    fn output_timestamp(&self, upstream: u32) -> u32 {
        (i64::from(upstream) + self.offset_ms).clamp(0, i64::from(u32::MAX)) as u32
    }

    /// Returns whether a forwarded tag contains media, or None if it overlaps the old stream.
    fn rewrite_tag(&mut self, tag: &mut [u8]) -> Option<bool> {
        let payload = &tag[11..tag.len() - 4];
        let kind = tag[0] & 0x1f;
        let media = match (kind, payload) {
            (8, [format, rest @ ..]) if format >> 4 == 10 => rest.first() == Some(&1),
            (9, [format, rest @ ..]) if format & 0x0f == 7 => rest.first() == Some(&1),
            (8 | 9, [_, ..]) => true,
            _ => false,
        };
        let original = u32::from_be_bytes([tag[7], tag[4], tag[5], tag[6]]);
        let mut timestamp = original;
        if let Some(last) = self.last_media {
            if self.awaiting_resume && media {
                if self.has_video && !(kind == 9 && payload[0] >> 4 == 1) {
                    return None;
                }
                // shortcut: infer a shared broadcast clock from a short overlap or gap.
                if (last.saturating_sub(15_000)..=last.saturating_add(5_000)).contains(&original) {
                    if original <= last {
                        return None;
                    }
                } else {
                    self.offset_ms = i64::from(self.output_timestamp(last).saturating_add(1))
                        - i64::from(original);
                    self.last_media = None;
                }
                self.awaiting_resume = false;
            } else if !media && (self.awaiting_resume || self.offset_ms != 0) {
                // Config tags can carry timestamps from long before the first media packet.
                timestamp = last;
            }
        }
        let timestamp = self.output_timestamp(timestamp);
        if timestamp != original {
            let [ext, hi, mid, lo] = timestamp.to_be_bytes();
            tag[4..8].copy_from_slice(&[hi, mid, lo, ext]);
        }
        if media {
            self.has_video |= kind == 9;
            self.last_media = Some(self.last_media.map_or(original, |last| last.max(original)));
        }
        Some(media)
    }
}

struct ProxyState {
    room_id: u64,
    upstream: Option<FlvInput>,
    prefetch: PlayUrlPrefetch,
    timeline: FlvTimeline,
    reconnect_failures: u8,
}

/// Emits one FLV header and complete tags across Douyu's expiring stream URLs.
pub fn proxy_stream(
    room_id: u64,
    (header, upstream): (Bytes, FlvInput),
) -> impl Stream<Item = anyhow::Result<Bytes>> + Send + 'static {
    let tags = stream::unfold(
        ProxyState {
            room_id,
            upstream: Some(upstream),
            prefetch: schedule_play_url_prefetch(room_id, INITIAL_PLAY_URL_PREFETCH),
            timeline: FlvTimeline::default(),
            reconnect_failures: 0,
        },
        |mut state| async move {
            loop {
                if let Some(upstream) = state.upstream.as_mut() {
                    match upstream.next_tag().await {
                        Ok(Some(mut tag)) => {
                            let Some(media) = state.timeline.rewrite_tag(&mut tag) else {
                                continue;
                            };
                            if media {
                                state.reconnect_failures = 0;
                            }
                            return Some((Ok(tag.freeze()), state));
                        }
                        Ok(None) => tracing::info!(
                            room_id = state.room_id,
                            "Douyu upstream stream ended; reconnecting"
                        ),
                        Err(error) => tracing::warn!(
                            room_id = state.room_id,
                            %error,
                            "Douyu upstream stream interrupted; reconnecting"
                        ),
                    }
                    state.upstream = None;
                }

                if state.reconnect_failures >= 3 {
                    tracing::warn!(
                        room_id = state.room_id,
                        "Douyu stream reconnect failed repeatedly; closing local stream"
                    );
                    return None;
                }
                state.reconnect_failures += 1;

                let prefetched = state.prefetch.try_join_next();
                state.prefetch.abort_all();
                let url = match prefetched {
                    Some(Ok(Ok((fetched_at, url)))) if fetched_at.elapsed() < PREFETCH_MAX_AGE => {
                        Ok(url)
                    }
                    _ => resolve_douyu_play_url(state.room_id).await,
                };
                let reopened = match url {
                    Ok(url) => open_stream(&url).await,
                    Err(error) => Err(error),
                };
                match reopened {
                    Ok((_, upstream)) => {
                        state.timeline.reconnect();
                        state.upstream = Some(upstream);
                        state.prefetch =
                            schedule_play_url_prefetch(state.room_id, RECONNECT_PLAY_URL_PREFETCH);
                    }
                    Err(error) => tracing::warn!(
                        room_id = state.room_id,
                        %error,
                        "Failed to reopen Douyu stream"
                    ),
                }
            }
        },
    );
    stream::iter([Ok(header)]).chain(tags)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn flv_tag(kind: u8, timestamp: u32, payload: &[u8]) -> BytesMut {
        let [_, size_hi, size_mid, size_lo] = (payload.len() as u32).to_be_bytes();
        let [ext, hi, mid, lo] = timestamp.to_be_bytes();
        let mut tag =
            BytesMut::from(&[kind, size_hi, size_mid, size_lo, hi, mid, lo, ext, 0, 0, 0][..]);
        tag.extend_from_slice(payload);
        tag.extend_from_slice(&(tag.len() as u32).to_be_bytes());
        tag
    }

    fn flv_input(data: &[u8]) -> FlvInput {
        let chunks: Vec<_> = data
            .chunks(7)
            .map(|chunk| Ok(Bytes::copy_from_slice(chunk)))
            .collect();
        FlvInput {
            body: Box::pin(stream::iter(chunks)),
            buffer: BytesMut::new(),
        }
    }

    #[tokio::test]
    async fn flv_rollover_keeps_complete_tags_on_one_timeline() {
        const HEADER: &[u8] = b"FLV\x01\x05\0\0\0\x09\0\0\0\0";
        let last = flv_tag(9, 0x01ff_ffff, &[0x17, 1]);
        let config = flv_tag(9, 0, &[0x17, 0]);
        let video = flv_tag(9, 100, &[0x17, 1]);
        let audio = flv_tag(8, 123, &[0xaf, 1]);
        let mut first = flv_input(&[HEADER, &last, &audio[..12]].concat());
        let mut second = flv_input(&[HEADER, &config, &video, &audio].concat());
        let mut output = first.read_header().await.unwrap().to_vec();
        let mut timeline = FlvTimeline::default();
        let mut tag = first.next_tag().await.unwrap().unwrap();
        assert_eq!(timeline.rewrite_tag(&mut tag), Some(true));
        output.extend_from_slice(&tag);
        assert!(first.next_tag().await.is_err());

        second.read_header().await.unwrap();
        timeline.reconnect();
        for media in [false, true, true] {
            let mut tag = second.next_tag().await.unwrap().unwrap();
            assert_eq!(timeline.rewrite_tag(&mut tag), Some(media));
            output.extend_from_slice(&tag);
        }
        assert!(second.next_tag().await.unwrap().is_none());
        assert_eq!(
            output,
            [
                HEADER,
                &last,
                &flv_tag(9, 0x01ff_ffff, &[0x17, 0]),
                &flv_tag(9, 0x0200_0000, &[0x17, 1]),
                &flv_tag(8, 0x0200_0017, &[0xaf, 1])
            ]
            .concat()
        );
    }

    #[test]
    fn flv_rollover_skips_overlap_until_a_new_keyframe() {
        let mut timeline = FlvTimeline::default();
        timeline.rewrite_tag(&mut flv_tag(9, 301_000, &[0x27, 1]));
        timeline.reconnect();

        assert_eq!(
            timeline.rewrite_tag(&mut flv_tag(9, 293_000, &[0x17, 1])),
            None
        );
        assert_eq!(
            timeline.rewrite_tag(&mut flv_tag(8, 301_020, &[0xaf, 1])),
            None
        );
        assert_eq!(
            timeline.rewrite_tag(&mut flv_tag(9, 301_033, &[0x27, 1])),
            None
        );
        let keyframe = flv_tag(9, 301_066, &[0x17, 1]);
        let mut resumed = keyframe.clone();
        assert_eq!(timeline.rewrite_tag(&mut resumed), Some(true));
        assert_eq!(resumed, keyframe);
        assert_eq!(
            timeline.rewrite_tag(&mut flv_tag(8, 301_085, &[0xaf, 1])),
            Some(true)
        );
    }

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
