use crate::models::Channel;
use futures::StreamExt;
use reqwest::{Client, RequestBuilder, StatusCode, Url, header};
use std::{
    collections::HashMap,
    env,
    path::Path,
    process::Stdio,
    sync::{Arc, OnceLock},
    time::{Duration, Instant},
};
use tokio::{process::Command, sync::Semaphore, time::timeout};

const MAX_HLS_PROBE_DEPTH: usize = 3;
const FFPROBE_CMD: &str = "ffprobe";
const FFMPEG_CMD: &str = "ffmpeg";
const FFPROBE_USER_AGENT: &str = "Mozilla/5.0";
static FFPROBE_AVAILABLE: OnceLock<bool> = OnceLock::new();
static FFMPEG_AVAILABLE: OnceLock<bool> = OnceLock::new();

pub struct Checker {
    client: Client,
    semaphore: Arc<Semaphore>,
    segment_semaphore: Arc<Semaphore>,
    timeout: Duration,
    max_download_bytes: u64,
}

impl Checker {
    pub fn with_options(
        concurrency: usize,
        timeout_ms: u64,
        allow_invalid_certs: bool,
        max_download_bytes: u64,
        segment_concurrency: usize,
    ) -> Self {
        let client = Client::builder()
            .timeout(Duration::from_millis(timeout_ms))
            .danger_accept_invalid_certs(allow_invalid_certs)
            .build()
            .unwrap_or_default();

        Self {
            client,
            semaphore: Arc::new(Semaphore::new(concurrency.max(1))),
            segment_semaphore: Arc::new(Semaphore::new(segment_concurrency.max(1))),
            timeout: Duration::from_millis(timeout_ms),
            max_download_bytes,
        }
    }

    pub async fn check_channel(&self, mut channel: Channel) -> Channel {
        let _permit = self.semaphore.acquire().await.unwrap();

        let start = Instant::now();
        let probe = self
            .probe_playable(&channel.url, channel.headers.as_ref())
            .await;

        if probe.is_playable {
            channel.latency = Some(start.elapsed().as_millis() as u64);
            channel.is_online = true;
            if channel.resolution.is_none() {
                channel.resolution = probe.resolution;
            }
            if channel.speed.is_none() {
                channel.speed = probe.speed;
            }
        } else {
            channel.is_online = false;
            channel.latency = None;
        }
        channel.last_checked = Some(chrono::Utc::now());
        channel
    }

    async fn probe_playable(
        &self,
        url: &str,
        headers: Option<&HashMap<String, String>>,
    ) -> ProbeResult {
        Box::pin(self.probe_playable_inner(url.to_owned(), 0, headers)).await
    }

    async fn probe_playable_inner(
        &self,
        url: String,
        depth: usize,
        headers: Option<&HashMap<String, String>>,
    ) -> ProbeResult {
        if depth > MAX_HLS_PROBE_DEPTH || is_known_bad_stream_url(&url) {
            return ProbeResult::offline();
        }

        if looks_like_realtime_url(&url) {
            return self
                .ffprobe_probe(&url, headers)
                .await
                .map(realtime_probe_result)
                .unwrap_or_else(ProbeResult::offline);
        }

        if looks_like_hls_url(&url) {
            return self.probe_hls_playlist(&url, depth, headers).await;
        }

        if let Some(result) = self
            .ffprobe_probe(&url, headers)
            .await
            .filter(|result| result.is_playable)
        {
            return result;
        }

        self.measure_plain_download(&url, headers).await
    }

    async fn probe_hls_playlist(
        &self,
        url: &str,
        depth: usize,
        headers: Option<&HashMap<String, String>>,
    ) -> ProbeResult {
        let Some(text) = self.fetch_text(url, headers).await else {
            return ProbeResult::offline();
        };
        if !looks_like_hls_text(&text) {
            if let Some(result) = self
                .ffprobe_probe(url, headers)
                .await
                .filter(|result| result.is_playable)
            {
                return result;
            }
            return self.measure_plain_download(url, headers).await;
        }
        let Some(next) = first_hls_media_candidate(url, &text) else {
            return ProbeResult::offline();
        };
        if is_known_bad_stream_url(&next) {
            return ProbeResult::offline();
        }
        if looks_like_hls_url(&next) {
            return Box::pin(self.probe_playable_inner(next, depth + 1, headers)).await;
        }
        let speed = self.measure_hls_segments(url, &text, headers).await;
        let result = ProbeResult::online_with_speed(None, speed);
        self.ffmpeg_fallback_when_speedless(url, headers, result)
            .await
    }

    async fn ffprobe_probe(
        &self,
        url: &str,
        headers: Option<&HashMap<String, String>>,
    ) -> Option<ProbeResult> {
        if !*FFPROBE_AVAILABLE.get_or_init(|| command_available(FFPROBE_CMD)) {
            return None;
        }
        let timeout_window = self.timeout.saturating_add(Duration::from_secs(2));
        let output = timeout(timeout_window, {
            let mut command = Command::new(FFPROBE_CMD);
            command
                .kill_on_drop(true)
                .stdin(Stdio::null())
                .stdout(Stdio::piped())
                .stderr(Stdio::null())
                .arg("-v")
                .arg("error");
            if let Some(header_text) = ffprobe_headers(headers) {
                command.arg("-headers").arg(header_text);
            }
            command
                .arg("-user_agent")
                .arg(ffprobe_user_agent(headers))
                .arg("-rw_timeout")
                .arg(self.timeout.as_micros().to_string())
                .arg("-analyzeduration")
                .arg("3000000")
                .arg("-probesize")
                .arg("1048576")
                .arg("-show_entries")
                .arg("stream=codec_type,width,height")
                .arg("-of")
                .arg("csv=p=0")
                .arg(url)
                .output()
        })
        .await;
        match output {
            Ok(Ok(output)) => Some(parse_ffprobe_probe_output(
                output.status.success(),
                &output.stdout,
            )),
            Ok(Err(_)) | Err(_) => Some(ProbeResult::offline()),
        }
    }

    async fn fetch_text(
        &self,
        url: &str,
        headers: Option<&HashMap<String, String>>,
    ) -> Option<String> {
        let response = self.get_with_headers(url, headers).send().await.ok()?;
        if !is_playable_status(response.status()) {
            return None;
        }
        response.text().await.ok()
    }

    async fn measure_hls_segments(
        &self,
        playlist_url: &str,
        playlist: &str,
        headers: Option<&HashMap<String, String>>,
    ) -> Option<f64> {
        let segments = hls_media_candidates(playlist_url, playlist);
        if segments.is_empty() {
            return None;
        }
        let results = join_segment_measurements(
            segments
                .into_iter()
                .take(5)
                .map(|segment| self.measure_download_segment(segment, headers)),
        )
        .await;
        measured_speed_mibps(&results)
    }

    async fn measure_download_segment(
        &self,
        url: String,
        headers: Option<&HashMap<String, String>>,
    ) -> Option<DownloadMeasurement> {
        let _permit = self.segment_semaphore.acquire().await.ok()?;
        self.measure_download(url, headers).await
    }

    async fn measure_download(
        &self,
        url: String,
        headers: Option<&HashMap<String, String>>,
    ) -> Option<DownloadMeasurement> {
        let started = Instant::now();
        let response = self.get_with_headers(&url, headers).send().await.ok()?;
        if !is_playable_status(response.status()) {
            return None;
        }
        let max_bytes = self.max_download_bytes;
        let mut bytes = 0u64;
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.ok()?;
            bytes = bytes.saturating_add(chunk.len() as u64);
            if max_bytes > 0 && bytes >= max_bytes {
                break;
            }
        }
        let seconds = started.elapsed().as_secs_f64();
        (bytes > 0 && seconds > 0.0).then_some(DownloadMeasurement { bytes, seconds })
    }

    async fn measure_plain_download(
        &self,
        url: &str,
        headers: Option<&HashMap<String, String>>,
    ) -> ProbeResult {
        let result = match self.measure_download(url.to_owned(), headers).await {
            Some(measurement) => {
                ProbeResult::online_with_speed(None, measured_speed_mibps(&[Some(measurement)]))
            }
            None => ProbeResult::offline(),
        };
        self.ffmpeg_fallback_when_speedless(url, headers, result)
            .await
    }

    async fn ffmpeg_fallback_when_speedless(
        &self,
        url: &str,
        headers: Option<&HashMap<String, String>>,
        mut result: ProbeResult,
    ) -> ProbeResult {
        if !result.is_playable || !speed_is_effectively_zero(result.speed) {
            return result;
        }
        let Some(output) = self.ffmpeg_output(url, headers).await else {
            return result;
        };
        if let Some(speed) = ffmpeg_output_speed(&output).filter(|speed| *speed > 0.0) {
            result.speed = Some(speed);
        }
        if result.resolution.is_none() {
            result.resolution = ffmpeg_output_resolution(&output);
        }
        result
    }

    async fn ffmpeg_output(
        &self,
        url: &str,
        headers: Option<&HashMap<String, String>>,
    ) -> Option<String> {
        if !*FFMPEG_AVAILABLE.get_or_init(|| command_available(FFMPEG_CMD)) {
            return None;
        }
        let timeout_secs = self.timeout.as_secs().max(1).to_string();
        let timeout_window = self.timeout.saturating_add(Duration::from_secs(2));
        let output = timeout(timeout_window, {
            let mut command = Command::new(FFMPEG_CMD);
            command
                .kill_on_drop(true)
                .stdin(Stdio::null())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .arg("-t")
                .arg(timeout_secs);
            if let Some(header_text) = ffmpeg_headers(headers) {
                command.arg("-headers").arg(header_text);
            }
            command
                .arg("-http_persistent")
                .arg("0")
                .arg("-stats")
                .arg("-i")
                .arg(url)
                .arg("-f")
                .arg("null")
                .arg("-")
                .output()
        })
        .await
        .ok()?
        .ok()?;
        let mut combined = output.stderr;
        combined.extend_from_slice(&output.stdout);
        String::from_utf8(combined).ok()
    }

    fn get_with_headers(
        &self,
        url: &str,
        headers: Option<&HashMap<String, String>>,
    ) -> RequestBuilder {
        let mut request = self.client.get(url);
        if let Some(headers) = headers {
            for (name, value) in headers {
                let Ok(header_name) = header::HeaderName::from_bytes(name.as_bytes()) else {
                    continue;
                };
                let Ok(header_value) = header::HeaderValue::from_str(value) else {
                    continue;
                };
                request = request.header(header_name, header_value);
            }
        }
        request
    }
}

fn measured_speed_mibps(results: &[Option<DownloadMeasurement>]) -> Option<f64> {
    let total_bytes = results
        .iter()
        .filter_map(|result| result.as_ref())
        .map(|result| result.bytes)
        .sum::<u64>();
    let total_seconds = results
        .iter()
        .filter_map(|result| result.as_ref())
        .map(|result| result.seconds)
        .sum::<f64>();
    (total_bytes > 0 && total_seconds > 0.0)
        .then(|| total_bytes as f64 / total_seconds / 1024.0 / 1024.0)
}

#[derive(Debug, Clone, PartialEq)]
struct ProbeResult {
    is_playable: bool,
    resolution: Option<String>,
    speed: Option<f64>,
}

impl ProbeResult {
    fn online(resolution: Option<String>) -> Self {
        Self::online_with_speed(resolution, None)
    }

    fn online_with_speed(resolution: Option<String>, speed: Option<f64>) -> Self {
        Self {
            is_playable: true,
            resolution,
            speed,
        }
    }

    fn offline() -> Self {
        Self {
            is_playable: false,
            resolution: None,
            speed: None,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct DownloadMeasurement {
    bytes: u64,
    seconds: f64,
}

fn ffprobe_user_agent(headers: Option<&HashMap<String, String>>) -> &str {
    headers
        .and_then(|headers| find_header(headers, "User-Agent"))
        .map(String::as_str)
        .unwrap_or(FFPROBE_USER_AGENT)
}

fn ffprobe_headers(headers: Option<&HashMap<String, String>>) -> Option<String> {
    let headers = headers?;
    let mut lines = headers
        .iter()
        .filter(|(name, value)| !name.eq_ignore_ascii_case("User-Agent") && !value.is_empty())
        .map(|(name, value)| format!("{}: {}", name, value))
        .collect::<Vec<_>>();
    if lines.is_empty() {
        return None;
    }
    lines.sort_unstable();
    Some(format!("{}\r\n", lines.join("\r\n")))
}

fn ffmpeg_headers(headers: Option<&HashMap<String, String>>) -> Option<String> {
    let headers = headers?;
    let mut lines = headers
        .iter()
        .filter(|(_, value)| !value.is_empty())
        .map(|(name, value)| format!("{name}: {value}"))
        .collect::<Vec<_>>();
    if lines.is_empty() {
        return None;
    }
    lines.sort_unstable();
    Some(format!("{}\r\n", lines.join("\r\n")))
}

fn find_header<'a>(headers: &'a HashMap<String, String>, wanted: &str) -> Option<&'a String> {
    headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case(wanted))
        .map(|(_, value)| value)
}

fn ffprobe_output_has_media(stdout: &[u8]) -> bool {
    let output = String::from_utf8_lossy(stdout);
    output.lines().map(str::trim).any(|line| {
        let codec = line.split(',').next().unwrap_or(line).trim();
        matches!(codec, "video" | "audio")
    })
}

fn parse_ffprobe_probe_output(success: bool, stdout: &[u8]) -> ProbeResult {
    if !success || !ffprobe_output_has_media(stdout) {
        return ProbeResult::offline();
    }
    ProbeResult::online(ffprobe_output_resolution(stdout))
}

fn ffprobe_output_resolution(stdout: &[u8]) -> Option<String> {
    String::from_utf8_lossy(stdout).lines().find_map(|line| {
        let parts = line.split(',').map(str::trim).collect::<Vec<_>>();
        if parts.first().copied() != Some("video") {
            return None;
        }
        let width = parts.get(1)?.parse::<u32>().ok()?;
        let height = parts.get(2)?.parse::<u32>().ok()?;
        (width > 0 && height > 0).then(|| format!("{width}x{height}"))
    })
}

fn speed_is_effectively_zero(speed: Option<f64>) -> bool {
    speed
        .map(|speed| (speed * 100.0).round() / 100.0 == 0.0)
        .unwrap_or(true)
}

fn parse_ffmpeg_time_seconds(value: &str) -> Option<f64> {
    let mut total = 0.0;
    let mut saw_part = false;
    for (index, part) in value
        .split(':')
        .filter(|part| !part.trim().is_empty())
        .rev()
        .enumerate()
    {
        let value = part.trim().parse::<f64>().ok()?;
        total += value * 60_f64.powi(index as i32);
        saw_part = true;
    }
    (saw_part && total > 0.0).then_some(total)
}

fn parse_ffmpeg_size_bytes(value: &str, unit: Option<&str>) -> Option<f64> {
    let value = value.parse::<f64>().ok()?;
    let multiplier = match unit.unwrap_or_default().to_ascii_lowercase().as_str() {
        "b" | "bytes" | "" => 1.0,
        "kib" | "k" => 1024.0,
        "kb" => 1000.0,
        "mib" | "mb" => 1024.0 * 1024.0,
        _ => 1.0,
    };
    Some(value * multiplier)
}

fn ffmpeg_output_speed(output: &str) -> Option<f64> {
    ffmpeg_video_audio_speed(output)
        .or_else(|| ffmpeg_size_time_speed(output))
        .or_else(|| ffmpeg_bitrate_speed(output))
}

fn ffmpeg_video_audio_speed(output: &str) -> Option<f64> {
    let mut total_bytes = 0.0;
    for label in ["video", "audio"] {
        let pattern = format!(r"(?i){label}:\s*([0-9]+(?:\.[0-9]+)?)\s*(KiB|MiB|kB|B|kb|KB)?");
        if let Some(captures) = regex::Regex::new(&pattern).ok()?.captures(output) {
            total_bytes += parse_ffmpeg_size_bytes(
                captures.get(1)?.as_str(),
                captures.get(2).map(|value| value.as_str()),
            )?;
        }
    }
    if total_bytes <= 0.0 {
        return None;
    }
    let seconds = ffmpeg_output_time(output)?;
    Some(total_bytes / seconds / 1024.0 / 1024.0)
}

fn ffmpeg_size_time_speed(output: &str) -> Option<f64> {
    let lsize_re =
        regex::Regex::new(r"(?i)Lsize=\s*([0-9]+(?:\.[0-9]+)?)\s*(KiB|kB|MiB|B|kb|KB)?").ok()?;
    let size_re =
        regex::Regex::new(r"(?i)size=\s*([0-9]+(?:\.[0-9]+)?)\s*(KiB|kB|MiB|B|kb|KB)?").ok()?;
    let captures = lsize_re
        .captures(output)
        .or_else(|| size_re.captures(output))?;
    let bytes = parse_ffmpeg_size_bytes(
        captures.get(1)?.as_str(),
        captures.get(2).map(|value| value.as_str()),
    )?;
    let seconds = ffmpeg_output_time(output)?;
    (bytes > 0.0).then_some(bytes / seconds / 1024.0 / 1024.0)
}

fn ffmpeg_bitrate_speed(output: &str) -> Option<f64> {
    let captures = regex::Regex::new(r"bitrate=\s*([0-9\.]+)\s*k?bits/s")
        .ok()?
        .captures(output)?;
    let kbps = captures.get(1)?.as_str().parse::<f64>().ok()?;
    Some(kbps / 8.0 / 1024.0)
}

fn ffmpeg_output_time(output: &str) -> Option<f64> {
    let captures = regex::Regex::new(r"time=\s*([0-9:\.]+)")
        .ok()?
        .captures(output)?;
    parse_ffmpeg_time_seconds(captures.get(1)?.as_str())
}

fn ffmpeg_output_resolution(output: &str) -> Option<String> {
    regex::Regex::new(r"(\d{3,4}x\d{3,4})")
        .ok()?
        .captures(output)?
        .get(1)
        .map(|value| value.as_str().to_owned())
}

fn command_available(program: &str) -> bool {
    if program.contains('/') {
        return Path::new(program).is_file();
    }
    env::var_os("PATH")
        .map(|paths| env::split_paths(&paths).any(|dir| dir.join(program).is_file()))
        .unwrap_or(false)
}

fn is_known_bad_stream_url(url: &str) -> bool {
    let lower = url.to_ascii_lowercase();
    [
        "no_signal",
        "nosignal",
        "playad",
        "testvideo",
        "/media/video/no_signal",
        "/media/video/nosignal",
    ]
    .iter()
    .any(|needle| lower.contains(needle))
}

fn is_playable_status(status: StatusCode) -> bool {
    status.is_success() || status == StatusCode::PARTIAL_CONTENT
}

fn looks_like_hls_url(url: &str) -> bool {
    let lower = url.to_ascii_lowercase();
    lower.contains(".m3u8") || lower.ends_with(".m3u")
}

fn looks_like_realtime_url(url: &str) -> bool {
    let lower = url.to_ascii_lowercase();
    lower.starts_with("rtmp://") || lower.starts_with("rtsp://")
}

fn realtime_probe_result(mut result: ProbeResult) -> ProbeResult {
    if result.is_playable && result.resolution.is_some() && result.speed.is_none() {
        result.speed = Some(f64::INFINITY);
    }
    result
}

fn looks_like_hls_text(text: &str) -> bool {
    text.trim_start().starts_with("#EXTM3U")
}

fn first_hls_media_candidate(base_url: &str, playlist: &str) -> Option<String> {
    playlist
        .lines()
        .map(str::trim)
        .find(|line| !line.is_empty() && !line.starts_with('#'))
        .and_then(|line| resolve_playlist_url(base_url, line))
}

fn hls_media_candidates(base_url: &str, playlist: &str) -> Vec<String> {
    playlist
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .filter(|line| !looks_like_hls_url(line))
        .filter_map(|line| resolve_playlist_url(base_url, line))
        .collect()
}

async fn join_segment_measurements<F>(
    futures: impl IntoIterator<Item = F>,
) -> Vec<Option<DownloadMeasurement>>
where
    F: std::future::Future<Output = Option<DownloadMeasurement>>,
{
    futures::future::join_all(futures).await
}

fn resolve_playlist_url(base_url: &str, value: &str) -> Option<String> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    #[test]
    fn playable_status_accepts_success_and_partial_content() {
        assert!(is_playable_status(StatusCode::OK));
        assert!(is_playable_status(StatusCode::PARTIAL_CONTENT));
        assert!(!is_playable_status(StatusCode::NOT_FOUND));
    }

    #[test]
    fn hls_candidate_resolves_absolute_variant() {
        let playlist = "#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1\nhttp://cdn.example/live.m3u8\n";
        assert_eq!(
            first_hls_media_candidate("http://origin.example/root/index.m3u8", playlist),
            Some("http://cdn.example/live.m3u8".to_owned())
        );
    }

    #[test]
    fn hls_candidate_resolves_relative_segment() {
        let playlist = "#EXTM3U\n#EXTINF:7,\nseg/0001.ts\n";
        assert_eq!(
            first_hls_media_candidate("https://cdn.example/live/main.m3u8", playlist),
            Some("https://cdn.example/live/seg/0001.ts".to_owned())
        );
    }

    #[test]
    fn hls_candidate_resolves_protocol_relative_segment() {
        let playlist = "#EXTM3U\n#EXTINF:7,\n//cdn.example/live/0001.ts\n";
        assert_eq!(
            first_hls_media_candidate("https://origin.example/live/main.m3u8", playlist),
            Some("https://cdn.example/live/0001.ts".to_owned())
        );
    }

    #[test]
    fn hls_media_candidates_skip_variant_playlists_and_resolve_segments() {
        let playlist = "#EXTM3U
#EXT-X-STREAM-INF:BANDWIDTH=1
variant.m3u8
#EXTINF:7,
seg/0001.ts
#EXTINF:7,
//cdn.example/live/0002.ts
";
        assert_eq!(
            hls_media_candidates("https://origin.example/live/main.m3u8", playlist),
            vec![
                "https://origin.example/live/seg/0001.ts".to_owned(),
                "https://cdn.example/live/0002.ts".to_owned(),
            ]
        );
    }

    #[tokio::test]
    async fn plain_http_download_measures_speed_like_python_fallback() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let body = vec![b'x'; 64 * 1024];
        let server = tokio::spawn(async move {
            for _ in 0..3 {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                let body = body.clone();
                tokio::spawn(async move {
                    let mut request = [0u8; 1024];
                    let _ = socket.read(&mut request).await.unwrap();
                    socket
                        .write_all(
                            format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n", body.len())
                                .as_bytes(),
                        )
                        .await
                        .unwrap();
                    socket.write_all(&body).await.unwrap();
                });
            }
        });
        let checker = Checker::with_options(1, 3_000, true, 8 * 1024 * 1024, 2);

        let result = checker
            .probe_playable(&format!("http://{addr}/live.bin"), None)
            .await;

        server.abort();
        assert!(result.is_playable);
        assert!(result.speed.is_some_and(|speed| speed > 0.0));
    }

    #[test]
    fn known_bad_stream_url_rejects_no_signal_and_ads() {
        assert!(is_known_bad_stream_url(
            "http://files4.3y1.xyz/media/video/no_signal_epg/no_signal0.ts"
        ));
        assert!(is_known_bad_stream_url(
            "https://cdn.jsdelivr.net/gh/example/testvideo/playad10.ts"
        ));
        assert!(!is_known_bad_stream_url(
            "https://cdn.example/live/segment.ts"
        ));
    }

    #[test]
    fn realtime_urls_match_python_rt_url_pattern() {
        assert!(looks_like_realtime_url("rtmp://example/live"));
        assert!(looks_like_realtime_url("rtsp://example/live"));
        assert!(!looks_like_realtime_url("http://example/live"));
    }

    #[test]
    fn realtime_probe_with_resolution_gets_python_infinite_speed() {
        let result = realtime_probe_result(ProbeResult::online(Some("1920x1080".to_owned())));
        assert_eq!(result.speed, Some(f64::INFINITY));

        let without_resolution = realtime_probe_result(ProbeResult::online(None));
        assert_eq!(without_resolution.speed, None);
    }

    #[test]
    fn ffprobe_headers_split_user_agent_from_extra_headers() {
        let headers = HashMap::from([
            ("User-Agent".to_owned(), "UA".to_owned()),
            ("Referer".to_owned(), "https://example.com/".to_owned()),
            ("Origin".to_owned(), "https://origin.example".to_owned()),
        ]);

        assert_eq!(ffprobe_user_agent(Some(&headers)), "UA");
        let header_text = ffprobe_headers(Some(&headers)).unwrap();
        assert!(header_text.contains("Referer: https://example.com/\r\n"));
        assert!(header_text.contains("Origin: https://origin.example\r\n"));
        assert!(!header_text.contains("User-Agent"));
    }

    #[test]
    fn ffprobe_user_agent_defaults_when_absent() {
        let headers = HashMap::from([("Referer".to_owned(), "https://example.com/".to_owned())]);
        assert_eq!(ffprobe_user_agent(Some(&headers)), FFPROBE_USER_AGENT);
        assert_eq!(ffprobe_user_agent(None), FFPROBE_USER_AGENT);
    }

    #[test]
    fn ffprobe_output_requires_audio_or_video_stream() {
        assert!(ffprobe_output_has_media(b"video\naudio\n"));
        assert!(ffprobe_output_has_media(b"video,1920,1080\n"));
        assert!(ffprobe_output_has_media(b"audio\n"));
        assert!(!ffprobe_output_has_media(b"subtitle\n"));
        assert!(!ffprobe_output_has_media(b""));
    }

    #[test]
    fn ffprobe_output_extracts_video_resolution() {
        assert_eq!(
            ffprobe_output_resolution(b"audio,N/A,N/A\nvideo,1920,1080\n"),
            Some("1920x1080".to_owned())
        );
        assert_eq!(ffprobe_output_resolution(b"audio,N/A,N/A\n"), None);
    }

    #[test]
    fn ffprobe_probe_output_combines_playable_and_resolution() {
        assert_eq!(
            parse_ffprobe_probe_output(true, b"video,1280,720\n"),
            ProbeResult::online(Some("1280x720".to_owned()))
        );
        assert_eq!(
            parse_ffprobe_probe_output(true, b"subtitle,1280,720\n"),
            ProbeResult::offline()
        );
        assert_eq!(
            parse_ffprobe_probe_output(false, b"video,1280,720\n"),
            ProbeResult::offline()
        );
    }

    #[test]
    fn measured_speed_matches_python_total_bytes_over_total_time_mib() {
        let speed = measured_speed_mibps(&[
            Some(DownloadMeasurement {
                bytes: 1024 * 1024,
                seconds: 1.0,
            }),
            Some(DownloadMeasurement {
                bytes: 1024 * 1024,
                seconds: 3.0,
            }),
            None,
        ])
        .unwrap();
        assert!((speed - 0.5).abs() < f64::EPSILON);
        assert!(measured_speed_mibps(&[None]).is_none());
    }

    #[test]
    fn ffmpeg_output_speed_matches_python_video_audio_summary() {
        let output = "frame=30 fps=0.0 size=0kB time=00:00:02.00 bitrate=0.0kbits/s\nvideo:1024KiB audio:1024KiB subtitle:0kB";
        let speed = ffmpeg_output_speed(output).unwrap();
        assert!((speed - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn ffmpeg_output_speed_matches_python_lsize_and_bitrate_fallbacks() {
        let lsize_speed = ffmpeg_output_speed("Lsize=2048KiB time=00:00:04.00").unwrap();
        assert!((lsize_speed - 0.5).abs() < f64::EPSILON);

        let bitrate_speed = ffmpeg_output_speed("bitrate=8192.0kbits/s").unwrap();
        assert!((bitrate_speed - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn ffmpeg_output_resolution_matches_python_video_info_regex() {
        assert_eq!(
            ffmpeg_output_resolution("Stream #0:0: Video: h264, yuv420p, 1280x720"),
            Some("1280x720".to_owned())
        );
        assert_eq!(ffmpeg_output_resolution("audio only"), None);
    }

    #[test]
    fn ffmpeg_headers_keep_user_agent_like_python() {
        let headers = HashMap::from([
            ("User-Agent".to_owned(), "UA".to_owned()),
            ("Referer".to_owned(), "https://example.com/".to_owned()),
        ]);
        let header_text = ffmpeg_headers(Some(&headers)).unwrap();
        assert!(header_text.contains("User-Agent: UA\r\n"));
        assert!(header_text.contains("Referer: https://example.com/\r\n"));
    }
}
