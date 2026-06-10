use crate::models::Channel;
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use chacha20poly1305::{ChaCha20Poly1305, KeyInit, aead::Aead};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::sync::OnceLock;
use std::time::{SystemTime, UNIX_EPOCH};
use url::Url;

pub const PPV_PLAYLIST_PATH: &str = "/ppv.m3u";
pub const PPV_STATUS_PATH: &str = "/ppv/status";
pub const PPV_PLAY_PATH_PREFIX: &str = "/ppv/play";

const PPV_STREAMS: &str = "https://api.ppv.to/api/streams";
const BROWSER_UA: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36";
const ROOM_TTL: u64 = 600;
const SOURCE_TTL: u64 = 300;
const TOKEN_MARGIN: u64 = 90;

fn poo_domain() -> String {
    std::env::var("TV_POO_DOMAIN")
        .ok()
        .map(|s| s.trim().to_owned())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "pooembed.top".to_owned())
}

fn poo_fetch_url() -> String {
    format!("https://{}/fetch", poo_domain())
}

fn poo_origin() -> String {
    format!("https://{}", poo_domain())
}

struct CachedRoom {
    slug: String,
    expires_at: u64,
}

struct CachedSource {
    final_url: String,
    expires_at: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct PpvStatus {
    pub playlist_path: String,
    pub play_path_prefix: String,
    pub cache_ttl_secs: u64,
    pub cached_rooms: usize,
    pub cached_sources: usize,
}

#[derive(Deserialize, Clone, Debug)]
struct PpvStreamsResponse {
    streams: Option<Vec<PpvCategory>>,
}

#[derive(Deserialize, Clone, Debug)]
struct PpvCategory {
    name: Option<String>,
    streams: Option<Vec<PpvStreamItem>>,
}

#[derive(Deserialize, Clone, Debug)]
struct PpvStreamItem {
    id: serde_json::Value,
    name: Option<String>,
    uri_name: Option<String>,
    logo: Option<String>,
}

#[derive(Debug, Clone)]
struct M3u8Ref {
    url: String,
    rank: i32,
    score: u64,
}

static ROOM_CACHE: OnceLock<DashMap<String, CachedRoom>> = OnceLock::new();
static SOURCE_CACHE: OnceLock<DashMap<String, CachedSource>> = OnceLock::new();

fn room_cache() -> &'static DashMap<String, CachedRoom> {
    ROOM_CACHE.get_or_init(DashMap::new)
}

fn source_cache() -> &'static DashMap<String, CachedSource> {
    SOURCE_CACHE.get_or_init(DashMap::new)
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(std::time::Duration::ZERO)
        .as_secs()
}

pub fn get_client() -> reqwest::Client {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    CLIENT
        .get_or_init(|| {
            reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(15))
                .cookie_store(true)
                .build()
                .unwrap_or_default()
        })
        .clone()
}

fn urlencode(s: &str) -> String {
    let mut encoded = String::new();
    for b in s.bytes() {
        match b {
            b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                encoded.push(b as char);
            }
            _ => {
                encoded.push_str(&format!("%{:02X}", b));
            }
        }
    }
    encoded
}

fn enc_varint(mut n: u64) -> Vec<u8> {
    let mut s = Vec::new();
    loop {
        let b = (n & 0x7f) as u8;
        n >>= 7;
        if n == 0 {
            s.push(b);
            return s;
        }
        s.push(b | 0x80);
    }
}

fn get_varint(s: &[u8], i: &mut usize) -> anyhow::Result<u64> {
    let mut n = 0u64;
    let mut shift = 0;
    let len = s.len();
    while *i < len {
        let b = s[*i];
        *i += 1;
        n |= ((b & 0x7f) as u64) << shift;
        if b < 0x80 {
            return Ok(n);
        }
        shift += 7;
        if shift > 63 {
            anyhow::bail!("bad varint");
        }
    }
    anyhow::bail!("short varint");
}

fn pb_put(field: u32, value: &str) -> Vec<u8> {
    let mut body = Vec::new();
    let tag = (field << 3) | 2;
    body.extend(enc_varint(tag as u64));
    body.extend(enc_varint(value.len() as u64));
    body.extend_from_slice(value.as_bytes());
    body
}

fn pb_read_field1(s: &[u8]) -> anyhow::Result<Vec<u8>> {
    let mut i = 0;
    let len = s.len();
    while i < len {
        let tag = get_varint(s, &mut i)?;
        let field = tag >> 3;
        let wire = tag & 7;
        if wire != 2 {
            anyhow::bail!("bad wire");
        }
        let n = get_varint(s, &mut i)? as usize;
        if i + n > len {
            anyhow::bail!("short field");
        }
        let val = &s[i..i + n];
        i += n;
        if field == 1 {
            return Ok(val.to_vec());
        }
    }
    anyhow::bail!("field 1 not found");
}

fn shift_payload(s: &str) -> String {
    let s = s.trim();
    let mut o = String::with_capacity(s.len());
    for c in s.chars() {
        let cp = c as u32;
        if cp >= 33 && cp <= 126 {
            let new_cp = ((cp - 33 + 71) % 94) + 33;
            o.push(std::char::from_u32(new_cp).unwrap_or(c));
        } else {
            o.push(c);
        }
    }
    o
}

fn open_payload(payload: &str, island: &str) -> anyhow::Result<Vec<u8>> {
    let shifted = shift_payload(payload);
    let packed = BASE64_STANDARD
        .decode(shifted.as_bytes())
        .map_err(|e| anyhow::anyhow!("base64 decode failed: {:?}", e))?;

    if packed.len() < 28 {
        anyhow::bail!("bad payload length");
    }

    let nonce = &packed[0..12];
    let ciphertext_with_tag = &packed[12..];

    let mut key_bytes = [0u8; 32];
    let island_bytes = island.as_bytes();
    let copy_len = island_bytes.len().min(32);
    key_bytes[..copy_len].copy_from_slice(&island_bytes[..copy_len]);

    let cipher = ChaCha20Poly1305::new(&key_bytes.into());
    let decrypted = cipher
        .decrypt(nonce.into(), ciphertext_with_tag)
        .map_err(|e| anyhow::anyhow!("chacha20poly1305 decryption failed: {:?}", e))?;

    Ok(decrypted)
}

fn secure_until(url_str: &str) -> Option<u64> {
    let parsed = Url::parse(url_str).ok()?;
    let path = parsed.path();
    let parts: Vec<&str> = path.split('/').collect();
    for i in 0..parts.len() {
        if parts[i] == "secure" && i + 3 < parts.len() {
            if let Ok(ts) = parts[i + 3].parse::<u64>() {
                return Some(ts.saturating_sub(TOKEN_MARGIN));
            }
        }
    }
    None
}

fn source_until(source: &str, final_url: &str) -> u64 {
    let now = now_secs();
    let mut until = now + SOURCE_TTL;
    for url in &[source, final_url] {
        if let Some(end) = secure_until(url) {
            until = until.min(end);
        }
    }
    until.max(now + 20)
}

fn url_join(base: &str, ref_url: &str) -> anyhow::Result<String> {
    let ref_url = ref_url.trim();
    if ref_url.is_empty() {
        return Ok(ref_url.to_owned());
    }
    if Url::parse(ref_url).is_ok() {
        return Ok(ref_url.to_owned());
    }
    if ref_url.starts_with("//") {
        let base_parsed = Url::parse(base)?;
        return Ok(format!("{}:{}", base_parsed.scheme(), ref_url));
    }
    let base_parsed = Url::parse(base)?;
    let joined = base_parsed.join(ref_url)?;
    Ok(joined.to_string())
}

fn stream_score(line: &str) -> u64 {
    let mut score = 0;
    if let Some(pos) = line.find("BANDWIDTH=") {
        let s = &line[pos + 10..];
        let end = s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len());
        if let Ok(bw) = s[..end].parse::<u64>() {
            score += bw;
        }
    }
    if let Some(pos) = line.find("RESOLUTION=") {
        let s = &line[pos + 11..];
        let end = s
            .find(|c: char| !c.is_ascii_digit() && c != 'x' && c != 'X')
            .unwrap_or(s.len());
        let res_str = &s[..end];
        let parts: Vec<&str> = res_str.split(|c| c == 'x' || c == 'X').collect();
        if parts.len() == 2 {
            if let (Ok(w), Ok(h)) = (parts[0].parse::<u64>(), parts[1].parse::<u64>()) {
                score += w * h * 10;
            }
        }
    }
    score
}

fn maybe_m3u8(uri: &str) -> bool {
    uri.to_ascii_lowercase().contains(".m3u8")
}

fn resolve_hls_uri(base_url: &str, value: &str) -> Option<String> {
    url_join(base_url, value).ok()
}

fn m3u8_refs(text: &str, base: &str) -> Vec<M3u8Ref> {
    let mut refs = Vec::new();
    let mut pending_score = 0;

    for line in text.lines() {
        let trim = line.trim();
        if trim.is_empty() {
            continue;
        }
        if trim.starts_with('#') {
            if trim.starts_with("#EXT-X-STREAM-INF") {
                pending_score = stream_score(trim);
            }
            if trim.contains("URI=\"") {
                let mut start_pos = 0;
                while let Some(pos) = trim[start_pos..].find("URI=\"") {
                    let val_start = start_pos + pos + 5;
                    if let Some(val_end) = trim[val_start..].find('\"') {
                        let val_absolute_end = val_start + val_end;
                        let uri = &trim[val_start..val_absolute_end];
                        if maybe_m3u8(uri) {
                            if let Some(url) = resolve_hls_uri(base, uri) {
                                let rank = if trim.contains("I-FRAME") { 0 } else { 1 };
                                refs.push(M3u8Ref {
                                    url,
                                    rank,
                                    score: pending_score,
                                });
                            }
                        }
                        start_pos = val_absolute_end + 1;
                    } else {
                        break;
                    }
                }
            }
            continue;
        }
        if maybe_m3u8(trim) {
            if let Some(url) = resolve_hls_uri(base, trim) {
                let rank = if pending_score > 0 { 3 } else { 2 };
                refs.push(M3u8Ref {
                    url,
                    rank,
                    score: pending_score,
                });
            }
        }
        pending_score = 0;
    }

    refs.sort_by(|a, b| match b.rank.cmp(&a.rank) {
        std::cmp::Ordering::Equal => b.score.cmp(&a.score),
        other => other,
    });

    refs
}

async fn hls_get(client: &reqwest::Client, url: &str, slug: &str) -> anyhow::Result<String> {
    let origin = poo_origin();
    let referer = format!("{}/embed/{}", origin, urlencode(slug));
    let text = client
        .get(url)
        .header("Origin", origin)
        .header("Referer", referer)
        .header("Accept", "*/*")
        .header("User-Agent", BROWSER_UA)
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;
    if !text.trim_start().starts_with("#EXTM3U") {
        anyhow::bail!("not m3u8");
    }
    Ok(text)
}

fn quote_uris(line: &str, base: &str) -> String {
    let mut out = String::with_capacity(line.len());
    let mut start_pos = 0;
    while let Some(pos) = line[start_pos..].find("URI=\"") {
        let absolute_pos = start_pos + pos;
        out.push_str(&line[start_pos..absolute_pos]);
        let val_start = absolute_pos + 5;
        if let Some(val_end) = line[val_start..].find('\"') {
            let val_absolute_end = val_start + val_end;
            let uri = &line[val_start..val_absolute_end];
            let abs_uri = url_join(base, uri).unwrap_or_else(|_| uri.to_owned());
            out.push_str("URI=\"");
            out.push_str(&abs_uri);
            out.push_str("\"");
            start_pos = val_absolute_end + 1;
        } else {
            out.push_str("URI=\"");
            start_pos = val_start;
        }
    }
    out.push_str(&line[start_pos..]);
    out
}

fn abs_m3u8(text: &str, base: &str) -> String {
    let mut out = Vec::new();
    for line in text.lines() {
        let trim = line.trim();
        if trim.is_empty() {
            out.push(line.to_owned());
        } else if trim.starts_with('#') {
            if trim.contains("URI=\"") {
                out.push(quote_uris(line, base));
            } else {
                out.push(line.to_owned());
            }
        } else {
            out.push(url_join(base, trim).unwrap_or_else(|_| trim.to_owned()));
        }
    }
    out.join("\n") + "\n"
}

async fn fetch_ppv_streams_json(client: &reqwest::Client) -> anyhow::Result<PpvStreamsResponse> {
    let resp = client
        .get(PPV_STREAMS)
        .header("User-Agent", BROWSER_UA)
        .send()
        .await?
        .error_for_status()?
        .json::<PpvStreamsResponse>()
        .await?;
    Ok(resp)
}

pub async fn resolve_room_slug(client: &reqwest::Client, id: &str) -> anyhow::Result<String> {
    let id_trimmed = id.trim();
    if let Some(cached) = room_cache().get(id_trimmed) {
        if cached.expires_at > now_secs() {
            return Ok(cached.slug.clone());
        }
    }

    let streams_resp = fetch_ppv_streams_json(client).await?;
    let want = id_trimmed.strip_prefix("ppv-").unwrap_or(id_trimmed);

    if let Some(categories) = streams_resp.streams {
        for cat in categories {
            if let Some(streams) = cat.streams {
                for item in streams {
                    let item_id_str = match &item.id {
                        serde_json::Value::Number(n) => n.to_string(),
                        serde_json::Value::String(s) => s.clone(),
                        _ => String::new(),
                    };
                    let uri_name = item.uri_name.as_deref().unwrap_or("").trim();
                    if item_id_str == want
                        || format!("ppv-{}", item_id_str) == id_trimmed
                        || uri_name == id_trimmed
                    {
                        if !uri_name.is_empty() {
                            let slug = uri_name.to_owned();
                            room_cache().insert(
                                id_trimmed.to_owned(),
                                CachedRoom {
                                    slug: slug.clone(),
                                    expires_at: now_secs() + ROOM_TTL,
                                },
                            );
                            return Ok(slug);
                        }
                    }
                }
            }
        }
    }

    anyhow::bail!("room not found")
}

async fn fetch_fresh_url(client: &reqwest::Client, slug: &str) -> anyhow::Result<String> {
    let body = pb_put(1, slug);
    let origin = poo_origin();
    let referer = format!("{}/embed/{}", origin, urlencode(slug));

    let resp = client
        .post(poo_fetch_url())
        .header("Content-Type", "application/octet-stream")
        .header("Origin", origin)
        .header("Referer", referer)
        .header("Accept", "*/*")
        .header("User-Agent", BROWSER_UA)
        .body(body)
        .send()
        .await?
        .error_for_status()?;

    let island = resp
        .headers()
        .get("island")
        .and_then(|val| val.to_str().ok())
        .map(|s| s.to_owned())
        .ok_or_else(|| anyhow::anyhow!("no island header in fetch response"))?;

    let bin = resp.bytes().await?;
    let payload = pb_read_field1(&bin)?;

    let payload_str = String::from_utf8(payload)?;
    let decrypted_bytes = open_payload(&payload_str, &island)?;
    let decrypted_url = String::from_utf8(decrypted_bytes)?;
    Ok(decrypted_url)
}

async fn final_m3u8(
    client: &reqwest::Client,
    mut url: String,
    slug: &str,
) -> anyhow::Result<(String, String)> {
    let mut seen = std::collections::HashSet::new();
    for _ in 0..8 {
        if seen.contains(&url) {
            anyhow::bail!("m3u8 loop");
        }
        seen.insert(url.clone());
        let text = hls_get(client, &url, slug).await?;
        let refs = m3u8_refs(&text, &url);
        if refs.is_empty() {
            return Ok((text, url));
        }
        url = refs[0].url.clone();
    }
    anyhow::bail!("m3u8 too deep");
}

pub async fn resolve_live_m3u8(
    client: &reqwest::Client,
    slug: &str,
) -> anyhow::Result<(String, String, &'static str)> {
    if let Some(cached) = source_cache().get(slug) {
        if cached.expires_at > now_secs() {
            match hls_get(client, &cached.final_url, slug).await {
                Ok(text) => {
                    if m3u8_refs(&text, &cached.final_url).is_empty() {
                        return Ok((text, cached.final_url.clone(), "HIT"));
                    }
                }
                Err(_) => {
                    source_cache().remove(slug);
                }
            }
        }
    }

    let source = fetch_fresh_url(client, slug).await?;
    let (text, final_url) = final_m3u8(client, source.clone(), slug).await?;
    let expires = source_until(&source, &final_url);
    source_cache().insert(
        slug.to_owned(),
        CachedSource {
            final_url: final_url.clone(),
            expires_at: expires,
        },
    );
    Ok((text, final_url, "MISS"))
}

pub async fn generate_ppv_playlist_m3u(
    client: &reqwest::Client,
    base_url: &str,
) -> anyhow::Result<String> {
    let streams_resp = fetch_ppv_streams_json(client).await?;
    let mut m3u = String::from("#EXTM3U\n");
    let base_url = base_url.trim_end_matches('/');

    if let Some(categories) = streams_resp.streams {
        for cat in categories {
            let cat_name = cat.name.as_deref().unwrap_or("PPV");
            if let Some(streams) = cat.streams {
                for item in streams {
                    let item_id_str = match &item.id {
                        serde_json::Value::Number(n) => n.to_string(),
                        serde_json::Value::String(s) => s.clone(),
                        _ => continue,
                    };
                    let uri_name = item.uri_name.as_deref().unwrap_or("").trim();
                    if uri_name.is_empty() {
                        continue;
                    }
                    let name = item.name.as_deref().unwrap_or(&item_id_str);
                    let logo = item.logo.as_deref().unwrap_or("");
                    let play_url = format!("{}/ppv/play/{}", base_url, uri_name);

                    m3u.push_str(&format!(
                        "#EXTINF:-1 tvg-id=\"ppv-{}\" tvg-name=\"{}\" tvg-logo=\"{}\" group-title=\"{}\",{}\n{}\n",
                        item_id_str, name, logo, cat_name, name, play_url
                    ));
                }
            }
        }
    }

    Ok(m3u)
}

pub async fn ppv_status() -> PpvStatus {
    PpvStatus {
        playlist_path: PPV_PLAYLIST_PATH.to_owned(),
        play_path_prefix: PPV_PLAY_PATH_PREFIX.to_owned(),
        cache_ttl_secs: SOURCE_TTL,
        cached_rooms: room_cache().len(),
        cached_sources: source_cache().len(),
    }
}

pub async fn ppv_hls_playlist(id: &str) -> anyhow::Result<(String, &'static str)> {
    let client = get_client();
    let slug = resolve_room_slug(&client, id).await?;
    let (m3u8, base, cache_status) = resolve_live_m3u8(&client, &slug).await?;
    let absolute_m3u8 = abs_m3u8(&m3u8, &base);
    Ok((absolute_m3u8, cache_status))
}

#[allow(dead_code)]
pub fn channels() -> Vec<Channel> {
    Vec::new()
}

#[allow(dead_code)]
pub fn exposable_channel_count() -> usize {
    room_cache().len()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_urlencode() {
        assert_eq!(urlencode("abc"), "abc");
        assert_eq!(urlencode("rally-tv"), "rally-tv");
        assert_eq!(urlencode("a b/c"), "a%20b%2Fc");
        assert_eq!(
            urlencode("cfl/2026-06-11/ham-wpg"),
            "cfl%2F2026-06-11%2Fham-wpg"
        );
    }

    #[test]
    fn test_shift_payload() {
        let input = "hello-world-123";
        let shifted = shift_payload(input);

        // Let's verify shift of 71 + shift of 23 = 94 (identity)
        // Since shift_payload shifts by 71, we can implement shift by 23 to reverse it:
        let mut unshifted = String::new();
        for c in shifted.chars() {
            let cp = c as u32;
            if cp >= 33 && cp <= 126 {
                let new_cp = ((cp - 33 + 23) % 94) + 33;
                unshifted.push(std::char::from_u32(new_cp).unwrap());
            } else {
                unshifted.push(c);
            }
        }
        assert_eq!(unshifted, input);
    }

    #[test]
    fn test_protobuf_roundtrip() {
        let slug = "rally-tv-test-slug-value";
        let encoded = pb_put(1, slug);
        let decoded = pb_read_field1(&encoded).unwrap();
        assert_eq!(String::from_utf8(decoded).unwrap(), slug);
    }

    #[test]
    fn test_url_join() {
        assert_eq!(
            url_join("https://pooembed.eu/embed/rally-tv", "/stream.m3u8").unwrap(),
            "https://pooembed.eu/stream.m3u8"
        );
        assert_eq!(
            url_join("https://pooembed.eu/embed/rally-tv", "stream.m3u8").unwrap(),
            "https://pooembed.eu/embed/stream.m3u8"
        );
        assert_eq!(
            url_join(
                "https://pooembed.eu/embed/rally-tv",
                "//external.com/stream.m3u8"
            )
            .unwrap(),
            "https://external.com/stream.m3u8"
        );
        assert_eq!(
            url_join(
                "https://pooembed.eu/embed/rally-tv",
                "https://external.com/stream.m3u8"
            )
            .unwrap(),
            "https://external.com/stream.m3u8"
        );
    }

    #[test]
    fn test_secure_until() {
        assert_eq!(
            secure_until(
                "https://example.com/live/secure/token/123/1779641799/playlist.m3u8"
            ),
            Some(1779641799 - 90)
        );
        assert_eq!(
            secure_until("https://example.com/live/token/123/1779641799/playlist.m3u8"),
            None
        );
    }
}
