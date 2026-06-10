mod config;
mod engine;
mod epg;
mod error_log;
#[allow(dead_code)]
mod gdtv;
mod gdtv_signer;
mod history;
mod models;
mod playlist;
mod ppv;
mod subscribe;
mod telemetry;

use crate::engine::{Engine, EngineStatus};
use crate::models::{Channel, LocalConfig, OutputConfig};
use axum::{
    Json, Router,
    body::Body,
    extract::{Form, Path, State},
    http::{HeaderMap, header},
    response::IntoResponse,
    routing::{get, post},
};
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use metrics_exporter_prometheus::PrometheusHandle;
use reqwest::StatusCode;
use std::{
    collections::HashMap,
    path::{Path as FsPath, PathBuf},
    process::Stdio,
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};
use tokio::{process::Command, sync::Mutex};
use tracing_subscriber::EnvFilter;

struct AppState {
    channels: Arc<DashMap<String, Vec<Channel>>>,
    metrics: PrometheusHandle,
    epg_xml_path: String,
    epg_gz_path: String,
    epg_status: Arc<RwLock<epg::EpgStatus>>,
    engine_status: Arc<RwLock<EngineStatus>>,
    subscribe_nomatch_log_path: String,
    output: OutputConfig,
    local: LocalConfig,
    hls_streams: Arc<Mutex<HashMap<String, HlsStream>>>,
    rtmp_service: Arc<Mutex<Option<tokio::process::Child>>>,
    started_at: DateTime<Utc>,
}

struct HlsStream {
    child: tokio::process::Child,
    last_access: Instant,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 1. Logging
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive(tracing::Level::INFO.into()))
        .init();

    tracing::info!("Starting iptvapi-rs...");
    let metrics_handle = telemetry::init()?;
    telemetry::spawn_process_metrics_task();

    // 2. Config
    let config_path = config::resolve_config_path();
    if !config_path.exists() {
        tracing::info!("Creating default config.toml");
        config::create_default_config(&config_path)?;
    }
    tracing::info!("Using config file: {}", config_path.display());
    let app_config = config::load_config(&config_path)?;
    let epg_xml_path = app_config.epg.output_xml_path.clone();
    let epg_gz_path = app_config.epg.output_gz_path.clone();
    let epg_status = Arc::new(RwLock::new(epg::EpgStatus {
        xml_path: epg_xml_path.clone(),
        gz_path: epg_gz_path.clone(),
        ..Default::default()
    }));
    let engine_status = Arc::new(RwLock::new(EngineStatus::default()));

    // 3. Shared State
    let state = Arc::new(AppState {
        channels: Arc::new(DashMap::new()),
        metrics: metrics_handle,
        epg_xml_path,
        epg_gz_path,
        epg_status: epg_status.clone(),
        engine_status: engine_status.clone(),
        subscribe_nomatch_log_path: app_config.subscribe.nomatch_log_path.clone(),
        output: app_config.output.clone(),
        local: app_config.local.clone(),
        hls_streams: Arc::new(Mutex::new(HashMap::new())),
        rtmp_service: Arc::new(Mutex::new(None)),
        started_at: Utc::now(),
    });

    // 4. Background Engine
    let engine = Arc::new(Engine::new(
        app_config.clone(),
        state.channels.clone(),
        epg_status,
        engine_status,
    ));
    if !app_config.server.open_service {
        tracing::info!("HTTP service disabled by open_service=false");
        if app_config.engine.open_update && app_config.engine.update_startup {
            engine.run_once().await?;
        }
        return Ok(());
    }
    let engine_task = engine.clone();
    tokio::spawn(async move {
        engine_task.schedule().await;
    });
    if app_config.local.hls_enabled {
        match start_rtmp_service(&app_config.local, app_config.server.port) {
            Ok(child) => {
                *state.rtmp_service.lock().await = child;
            }
            Err(error) => {
                crate::error_log::push("hls_proxy", format!("rtmp service start failed: {error}"))
                    .await;
                tracing::warn!(error = %error, "Failed to start RTMP nginx service");
            }
        }
        spawn_hls_idle_monitor(state.clone());
    }

    // 5. Web API
    let app = Router::new()
        .route("/", get(get_playlist_default))
        .route("/m3u", get(get_playlist_m3u))
        .route("/txt", get(get_playlist_txt))
        .route("/content", get(get_playlist_m3u_content))
        .route("/favicon.ico", get(get_favicon))
        .route("/ipv4", get(get_ipv4_playlist_default_file))
        .route("/ipv4/m3u", get(get_ipv4_playlist_m3u_file))
        .route("/ipv4/txt", get(get_ipv4_playlist_txt_file))
        .route("/ipv6", get(get_ipv6_playlist_default_file))
        .route("/ipv6/m3u", get(get_ipv6_playlist_m3u_file))
        .route("/ipv6/txt", get(get_ipv6_playlist_txt_file))
        .route("/hls", get(get_hls_playlist_default_file))
        .route("/hls/m3u", get(get_hls_playlist_m3u_file))
        .route("/hls/txt", get(get_hls_playlist_txt_file))
        .route("/hls/ipv4", get(get_hls_ipv4_playlist_default_file))
        .route("/hls/ipv4/m3u", get(get_hls_ipv4_playlist_m3u_file))
        .route("/hls/ipv4/txt", get(get_hls_ipv4_playlist_txt_file))
        .route("/hls/ipv6", get(get_hls_ipv6_playlist_default_file))
        .route("/hls/ipv6/m3u", get(get_hls_ipv6_playlist_m3u_file))
        .route("/hls/ipv6/txt", get(get_hls_ipv6_playlist_txt_file))
        .route("/logo/{filename}", get(get_logo_file))
        .route("/playlist.m3u", get(get_playlist_m3u))
        .route("/playlist.txt", get(get_playlist_txt))
        .route("/epg/epg.xml", get(get_epg_xml))
        .route("/epg/epg.gz", get(get_epg_gz))
        .route("/epg/status", get(get_epg_status))
        .route(gdtv::GDTV_PLAYLIST_PATH, get(get_gdtv_playlist_m3u))
        .route(gdtv::GDTV_STATUS_PATH, get(get_gdtv_status))
        .route("/gdtv/status.json", get(get_gdtv_status_json))
        .route("/gdtv/play/{pk}", get(get_gdtv_play_hls))
        .route(ppv::PPV_PLAYLIST_PATH, get(get_ppv_playlist_m3u))
        .route(ppv::PPV_STATUS_PATH, get(get_ppv_status))
        .route("/ppv/status.json", get(get_ppv_status_json))
        .route("/ppv/play/{*id}", get(get_ppv_play_hls))
        .route("/status", get(get_status))
        .route("/engine/status", get(get_engine_status))
        .route("/metrics", get(get_metrics))
        .route("/dashboard", get(get_dashboard))
        .route("/log/result", get(get_result_log))
        .route("/log/speed-test", get(get_speed_test_log))
        .route("/log/statistic", get(get_statistic_log))
        .route("/log/nomatch", get(get_nomatch_log))
        .route("/errors/recent", get(get_recent_errors))
        .route("/hls_proxy/{channel_id}", get(get_hls_proxy))
        .route("/on_done", post(post_on_done))
        .layer(axum::middleware::from_fn(telemetry::track_http))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(format!(
        "{}:{}",
        app_config.server.host, app_config.server.port
    ))
    .await?;
    tracing::info!("Listening on http://{}", listener.local_addr()?);

    axum::serve(listener, app).await?;

    Ok(())
}

async fn get_hls_playlist_default_file(
    State(state): State<Arc<AppState>>,
) -> axum::response::Response {
    if state.output.m3u_result_enabled {
        serve_file_attachment(
            &state.output.hls_result_m3u_path,
            "text/plain; charset=utf-8",
        )
        .await
    } else {
        serve_file(
            &state.output.hls_result_txt_path,
            "text/plain; charset=utf-8",
        )
        .await
    }
}

async fn get_hls_playlist_m3u_file(State(state): State<Arc<AppState>>) -> axum::response::Response {
    serve_file_attachment(
        &state.output.hls_result_m3u_path,
        "text/plain; charset=utf-8",
    )
    .await
}

async fn get_hls_playlist_txt_file(State(state): State<Arc<AppState>>) -> axum::response::Response {
    serve_file(
        &state.output.hls_result_txt_path,
        "text/plain; charset=utf-8",
    )
    .await
}

async fn get_hls_ipv4_playlist_default_file(
    State(state): State<Arc<AppState>>,
) -> axum::response::Response {
    if state.output.m3u_result_enabled {
        serve_file_attachment(
            &state.output.hls_ipv4_result_m3u_path,
            "text/plain; charset=utf-8",
        )
        .await
    } else {
        serve_file(
            &state.output.hls_ipv4_result_txt_path,
            "text/plain; charset=utf-8",
        )
        .await
    }
}

async fn get_hls_ipv4_playlist_m3u_file(
    State(state): State<Arc<AppState>>,
) -> axum::response::Response {
    serve_file_attachment(
        &state.output.hls_ipv4_result_m3u_path,
        "text/plain; charset=utf-8",
    )
    .await
}

async fn get_hls_ipv4_playlist_txt_file(
    State(state): State<Arc<AppState>>,
) -> axum::response::Response {
    serve_file(
        &state.output.hls_ipv4_result_txt_path,
        "text/plain; charset=utf-8",
    )
    .await
}

async fn get_hls_ipv6_playlist_default_file(
    State(state): State<Arc<AppState>>,
) -> axum::response::Response {
    if state.output.m3u_result_enabled {
        serve_file_attachment(
            &state.output.hls_ipv6_result_m3u_path,
            "text/plain; charset=utf-8",
        )
        .await
    } else {
        serve_file(
            &state.output.hls_ipv6_result_txt_path,
            "text/plain; charset=utf-8",
        )
        .await
    }
}

async fn get_hls_ipv6_playlist_m3u_file(
    State(state): State<Arc<AppState>>,
) -> axum::response::Response {
    serve_file_attachment(
        &state.output.hls_ipv6_result_m3u_path,
        "text/plain; charset=utf-8",
    )
    .await
}

async fn get_hls_ipv6_playlist_txt_file(
    State(state): State<Arc<AppState>>,
) -> axum::response::Response {
    serve_file(
        &state.output.hls_ipv6_result_txt_path,
        "text/plain; charset=utf-8",
    )
    .await
}

async fn get_ipv4_playlist_default_file(
    State(state): State<Arc<AppState>>,
) -> axum::response::Response {
    if state.output.m3u_result_enabled {
        serve_file_attachment(
            &state.output.ipv4_result_m3u_path,
            "text/plain; charset=utf-8",
        )
        .await
    } else {
        serve_file(
            &state.output.ipv4_result_txt_path,
            "text/plain; charset=utf-8",
        )
        .await
    }
}

async fn get_ipv4_playlist_m3u_file(
    State(state): State<Arc<AppState>>,
) -> axum::response::Response {
    serve_file_attachment(
        &state.output.ipv4_result_m3u_path,
        "text/plain; charset=utf-8",
    )
    .await
}

async fn get_ipv4_playlist_txt_file(
    State(state): State<Arc<AppState>>,
) -> axum::response::Response {
    serve_file(
        &state.output.ipv4_result_txt_path,
        "text/plain; charset=utf-8",
    )
    .await
}

async fn get_ipv6_playlist_default_file(
    State(state): State<Arc<AppState>>,
) -> axum::response::Response {
    if state.output.m3u_result_enabled {
        serve_file_attachment(
            &state.output.ipv6_result_m3u_path,
            "text/plain; charset=utf-8",
        )
        .await
    } else {
        serve_file(
            &state.output.ipv6_result_txt_path,
            "text/plain; charset=utf-8",
        )
        .await
    }
}

async fn get_ipv6_playlist_m3u_file(
    State(state): State<Arc<AppState>>,
) -> axum::response::Response {
    serve_file_attachment(
        &state.output.ipv6_result_m3u_path,
        "text/plain; charset=utf-8",
    )
    .await
}

async fn get_ipv6_playlist_txt_file(
    State(state): State<Arc<AppState>>,
) -> axum::response::Response {
    serve_file(
        &state.output.ipv6_result_txt_path,
        "text/plain; charset=utf-8",
    )
    .await
}

async fn get_favicon() -> axum::response::Response {
    serve_static_file("favicon.ico", "image/vnd.microsoft.icon").await
}

async fn serve_static_file(path: &str, content_type: &'static str) -> axum::response::Response {
    match tokio::fs::read(path).await {
        Ok(content) => file_response(path, content_type, false, content),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            StatusCode::NOT_FOUND.into_response()
        }
        Err(error) => {
            let message = format!("failed to read static file {path}: {error}");
            crate::error_log::push("file", message.clone()).await;
            tracing::warn!(path = %path, error = %error, "Failed to read static file");
            (StatusCode::INTERNAL_SERVER_ERROR, "failed to read file").into_response()
        }
    }
}

async fn get_logo_file(
    State(state): State<Arc<AppState>>,
    Path(filename): Path<String>,
) -> axum::response::Response {
    let Some(path) = safe_child_path(&state.output.logo_dir, &filename) else {
        return (StatusCode::BAD_REQUEST, "invalid logo filename").into_response();
    };
    let extension = path
        .extension()
        .and_then(|value| value.to_str())
        .unwrap_or_default()
        .to_ascii_lowercase();
    let content_type = match extension.as_str() {
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "webp" => "image/webp",
        "svg" => "image/svg+xml",
        "ico" => "image/x-icon",
        _ => "application/octet-stream",
    };
    serve_file(path.to_string_lossy().as_ref(), content_type).await
}

fn safe_child_path(base: &str, filename: &str) -> Option<PathBuf> {
    let path = FsPath::new(filename);
    if path.components().count() != 1 {
        return None;
    }
    let name = path.file_name()?.to_str()?;
    if name.is_empty() || name.starts_with('.') {
        return None;
    }
    Some(FsPath::new(base).join(name))
}

async fn get_epg_xml(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    serve_file_attachment(&state.epg_xml_path, "text/plain; charset=utf-8").await
}

async fn get_epg_gz(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    serve_file_attachment(&state.epg_gz_path, "text/plain; charset=utf-8").await
}

async fn get_epg_status(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    Json(
        state
            .epg_status
            .read()
            .expect("EPG status read lock poisoned")
            .clone(),
    )
}

async fn get_engine_status(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    Json(
        state
            .engine_status
            .read()
            .expect("engine status read lock poisoned")
            .clone(),
    )
}

async fn serve_file(path: &str, content_type: &'static str) -> axum::response::Response {
    serve_file_inner(path, content_type, false).await
}

async fn serve_file_attachment(path: &str, content_type: &'static str) -> axum::response::Response {
    serve_file_inner(path, content_type, true).await
}

async fn serve_file_inner(
    path: &str,
    content_type: &'static str,
    attachment: bool,
) -> axum::response::Response {
    match tokio::fs::read(path).await {
        Ok(content) => file_response(path, content_type, attachment, content),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => waiting_tip_response(),
        Err(error) => {
            let message = format!("failed to read {path}: {error}");
            crate::error_log::push("file", message.clone()).await;
            tracing::warn!(path = %path, error = %error, "Failed to read output file");
            (StatusCode::INTERNAL_SERVER_ERROR, "failed to read file").into_response()
        }
    }
}

fn file_response(
    path: &str,
    content_type: &'static str,
    attachment: bool,
    content: Vec<u8>,
) -> axum::response::Response {
    let mut response = axum::response::Response::new(Body::from(content));
    response.headers_mut().insert(
        axum::http::header::CONTENT_TYPE,
        axum::http::HeaderValue::from_static(content_type),
    );
    if attachment
        && let Ok(value) = axum::http::HeaderValue::from_str(&attachment_disposition(path))
    {
        response
            .headers_mut()
            .insert(axum::http::header::CONTENT_DISPOSITION, value);
    }
    response
}

fn waiting_tip_response() -> axum::response::Response {
    file_response(
        "",
        "text/plain; charset=utf-8",
        false,
        "📄 请等待有效结果生成".as_bytes().to_vec(),
    )
}

async fn get_hls_proxy(
    State(state): State<Arc<AppState>>,
    Path(channel_id): Path<String>,
) -> axum::response::Response {
    if channel_id.trim().is_empty() {
        return (StatusCode::BAD_REQUEST, "channel_id required").into_response();
    }
    let filename = format!("{}.m3u8", channel_id.trim());
    let Some(path) = safe_child_path(&state.local.hls_temp_path, &filename) else {
        return (StatusCode::BAD_REQUEST, "invalid channel_id").into_response();
    };
    if let Err(error) = ensure_hls_stream_started(&state, channel_id.trim()).await {
        let message = format!(
            "failed to start hls proxy stream {}: {}",
            channel_id.trim(),
            error
        );
        crate::error_log::push("hls_proxy", message.clone()).await;
        tracing::warn!(channel_id = %channel_id, error = %error, "Failed to start HLS proxy stream");
    }
    let path_string = path.to_string_lossy().into_owned();
    match read_ready_hls_playlist(&path_string, hls_proxy_wait_timeout()).await {
        Ok(content) => {
            touch_hls_stream(&state, channel_id.trim()).await;
            (
                [
                    (
                        axum::http::header::CONTENT_TYPE,
                        "application/vnd.apple.mpegurl",
                    ),
                    (axum::http::header::CACHE_CONTROL, "no-store"),
                ],
                content,
            )
                .into_response()
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            (StatusCode::SERVICE_UNAVAILABLE, "m3u8 hls not ready").into_response()
        }
        Err(error) => {
            let message = format!("failed to read hls proxy file {path_string}: {error}");
            crate::error_log::push("hls_proxy", message.clone()).await;
            tracing::warn!(path = %path_string, error = %error, "Failed to read HLS proxy file");
            (StatusCode::INTERNAL_SERVER_ERROR, "failed to read m3u8").into_response()
        }
    }
}

async fn ensure_hls_stream_started(state: &AppState, channel_id: &str) -> std::io::Result<()> {
    let mut streams = state.hls_streams.lock().await;
    let now = Instant::now();
    if let Some(stream) = streams.get_mut(channel_id)
        && stream.child.try_wait()?.is_none()
    {
        stream.last_access = now;
        return Ok(());
    }
    streams.remove(channel_id);

    let Some(source) = lookup_rtmp_channel(&state.output.rtmp_data_path, channel_id)? else {
        return Ok(());
    };
    cleanup_hls_streams(&mut streams, state.local.rtmp_max_streams.max(1))?;

    let target = format!(
        "rtmp://127.0.0.1:{}/hls/{}",
        state.local.nginx_rtmp_port, channel_id
    );
    let args = ffmpeg_hls_args(&source.url, source.headers.as_ref(), &target);
    let child = Command::new("ffmpeg")
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()?;
    streams.insert(
        channel_id.to_owned(),
        HlsStream {
            child,
            last_access: now,
        },
    );
    Ok(())
}

fn cleanup_hls_streams(
    streams: &mut HashMap<String, HlsStream>,
    max_streams: usize,
) -> std::io::Result<()> {
    let mut dead = Vec::new();
    for (channel_id, stream) in streams.iter_mut() {
        if stream.child.try_wait()?.is_some() {
            dead.push(channel_id.clone());
        }
    }
    for channel_id in dead {
        streams.remove(&channel_id);
    }
    while streams.len() >= max_streams {
        let Some(channel_id) = streams.keys().next().cloned() else {
            break;
        };
        if let Some(mut stream) = streams.remove(&channel_id) {
            let _ = stream.child.start_kill();
        }
    }
    Ok(())
}

async fn touch_hls_stream(state: &AppState, channel_id: &str) {
    if let Some(stream) = state.hls_streams.lock().await.get_mut(channel_id) {
        stream.last_access = Instant::now();
    }
}

fn spawn_hls_idle_monitor(state: Arc<AppState>) {
    tokio::spawn(async move {
        let idle_timeout = Duration::from_secs(state.local.rtmp_idle_timeout.max(1));
        let mut interval = tokio::time::interval(Duration::from_secs(5));
        loop {
            interval.tick().await;
            if let Err(error) = stop_idle_hls_streams(&state, idle_timeout).await {
                crate::error_log::push("hls_proxy", format!("hls idle cleanup failed: {error}"))
                    .await;
                tracing::warn!(error = %error, "Failed to clean idle HLS streams");
            }
        }
    });
}

async fn stop_idle_hls_streams(state: &AppState, idle_timeout: Duration) -> std::io::Result<()> {
    let now = Instant::now();
    let mut streams = state.hls_streams.lock().await;
    stop_idle_hls_streams_inner(&mut streams, now, idle_timeout)
}

fn stop_idle_hls_streams_inner(
    streams: &mut HashMap<String, HlsStream>,
    now: Instant,
    idle_timeout: Duration,
) -> std::io::Result<()> {
    let mut to_stop = Vec::new();
    for (channel_id, stream) in streams.iter_mut() {
        if stream.child.try_wait()?.is_some() {
            to_stop.push(channel_id.clone());
            continue;
        }
        if now.duration_since(stream.last_access) > idle_timeout {
            to_stop.push(channel_id.clone());
        }
    }
    for channel_id in to_stop {
        if let Some(mut stream) = streams.remove(&channel_id) {
            let _ = stream.child.start_kill();
        }
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RtmpChannelSource {
    url: String,
    headers: Option<HashMap<String, String>>,
}

fn lookup_rtmp_channel(
    db_path: &str,
    channel_id: &str,
) -> std::io::Result<Option<RtmpChannelSource>> {
    let connection = rusqlite::Connection::open(db_path)
        .map_err(|error| std::io::Error::other(error.to_string()))?;
    match connection.query_row(
        "SELECT url, headers FROM result_data WHERE id=?1",
        rusqlite::params![channel_id],
        |row| {
            let url: String = row.get(0)?;
            let headers: Option<String> = row.get(1)?;
            Ok((url, headers))
        },
    ) {
        Ok((url, headers)) => Ok(Some(RtmpChannelSource {
            url,
            headers: parse_rtmp_headers(headers.as_deref()),
        })),
        Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
        Err(error) => Err(std::io::Error::other(error.to_string())),
    }
}

fn parse_rtmp_headers(value: Option<&str>) -> Option<HashMap<String, String>> {
    let value = value?.trim();
    if value.is_empty() || value.eq_ignore_ascii_case("null") {
        return None;
    }
    serde_json::from_str::<Option<HashMap<String, String>>>(value)
        .ok()
        .flatten()
        .or_else(|| serde_json::from_str::<HashMap<String, String>>(value).ok())
        .filter(|headers| !headers.is_empty())
}

fn ffmpeg_hls_args(
    url: &str,
    headers: Option<&HashMap<String, String>>,
    target: &str,
) -> Vec<String> {
    let mut args = vec!["-loglevel".to_owned(), "error".to_owned(), "-re".to_owned()];
    if let Some(header_text) = ffmpeg_hls_headers(headers) {
        args.push("-headers".to_owned());
        args.push(header_text);
    }
    args.extend(
        [
            "-i",
            url_without_extra_info(url),
            "-c:v",
            "libx264",
            "-preset",
            "veryfast",
            "-tune",
            "zerolatency",
            "-vf",
            "scale=trunc(iw/2)*2:trunc(ih/2)*2",
            "-c:a",
            "aac",
            "-b:a",
            "128k",
            "-f",
            "flv",
            "-flvflags",
            "no_duration_filesize",
            target,
        ]
        .into_iter()
        .map(str::to_owned),
    );
    args
}

fn ffmpeg_hls_headers(headers: Option<&HashMap<String, String>>) -> Option<String> {
    let headers = headers?;
    let mut lines = headers
        .iter()
        .filter(|(_, value)| !value.is_empty())
        .map(|(key, value)| format!("{key}: {value}\r\n"))
        .collect::<Vec<_>>();
    if lines.is_empty() {
        return None;
    }
    lines.sort_unstable();
    Some(lines.concat())
}

fn url_without_extra_info(url: &str) -> &str {
    url.split_once('$').map(|(url, _)| url).unwrap_or(url)
}

fn render_nginx_conf_template(template: &str, app_port: u16, local: &LocalConfig) -> String {
    template
        .replace("${APP_PORT}", &app_port.to_string())
        .replace("${NGINX_HTTP_PORT}", &local.nginx_http_port.to_string())
        .replace("${NGINX_RTMP_PORT}", &local.nginx_rtmp_port.to_string())
}

fn render_nginx_conf(local: &LocalConfig, app_port: u16) -> std::io::Result<PathBuf> {
    let nginx_dir = FsPath::new(&local.nginx_dir_path);
    let template_path = nginx_dir.join("conf/nginx.conf.template");
    let conf_path = nginx_dir.join("conf/nginx.conf");
    let template = std::fs::read_to_string(&template_path)?;
    let rendered = render_nginx_conf_template(&template, app_port, local);
    if let Some(parent) = conf_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(&conf_path, rendered)?;
    Ok(conf_path)
}

fn start_rtmp_service(
    local: &LocalConfig,
    app_port: u16,
) -> std::io::Result<Option<tokio::process::Child>> {
    render_nginx_conf(local, app_port)?;
    let nginx_dir = FsPath::new(&local.nginx_dir_path);
    let nginx_path = nginx_dir.join(if cfg!(windows) { "nginx.exe" } else { "nginx" });
    if !nginx_path.exists() {
        tracing::warn!(
            path = %nginx_path.display(),
            "RTMP nginx binary not found; rendered config only"
        );
        return Ok(None);
    }
    let child = Command::new(&nginx_path)
        .current_dir(nginx_dir)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?;
    Ok(Some(child))
}

#[allow(dead_code)]
async fn stop_rtmp_service(
    rtmp_service: &Arc<Mutex<Option<tokio::process::Child>>>,
) -> std::io::Result<()> {
    if let Some(mut child) = rtmp_service.lock().await.take() {
        let _ = child.start_kill();
    }
    Ok(())
}

const HLS_PROXY_WAIT_INTERVAL: Duration = Duration::from_millis(500);
const HLS_PROXY_MIN_SEGMENTS: usize = 3;

fn hls_proxy_wait_timeout() -> Duration {
    std::env::var("TV_IPTV_HLS_WAIT_TIMEOUT_SECS")
        .ok()
        .and_then(|value| value.trim().parse::<f64>().ok())
        .filter(|value| value.is_finite() && *value >= 0.0)
        .map(Duration::from_secs_f64)
        .unwrap_or_else(|| Duration::from_secs(30))
}

async fn read_ready_hls_playlist(path: &str, wait_timeout: Duration) -> std::io::Result<Vec<u8>> {
    let started = tokio::time::Instant::now();
    loop {
        match tokio::fs::read(path).await {
            Ok(content) if hls_playlist_ready(&content) => return Ok(content),
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        if started.elapsed() >= wait_timeout {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "m3u8 hls not ready",
            ));
        }
        let remaining = wait_timeout.saturating_sub(started.elapsed());
        tokio::time::sleep(HLS_PROXY_WAIT_INTERVAL.min(remaining)).await;
    }
}

fn hls_playlist_ready(content: &[u8]) -> bool {
    let text = String::from_utf8_lossy(content);
    text.matches("#EXTINF").count() >= HLS_PROXY_MIN_SEGMENTS
        && !text.trim_end().ends_with("#EXT-X-DISCONTINUITY")
}

#[derive(Debug, serde::Deserialize)]
struct RtmpOnDoneForm {
    #[serde(default)]
    name: String,
}

async fn post_on_done(Form(form): Form<RtmpOnDoneForm>) -> axum::response::Response {
    tracing::info!(channel_id = %form.name, "RTMP publishing ended");
    axum::response::Response::new(Body::empty())
}

async fn get_gdtv_playlist_m3u(headers: HeaderMap) -> impl IntoResponse {
    gdtv::official_playlist_m3u(&request_base_url(&headers)).await
}

fn request_base_url(headers: &HeaderMap) -> String {
    let host = headers
        .get(header::HOST)
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.trim().is_empty())
        .map(str::to_owned)
        .unwrap_or_else(gdtv::default_host_port);
    format!("http://{host}")
}

async fn get_gdtv_status() -> impl IntoResponse {
    format!(
        "OK - GDTV playlist: {}, Channels: {}",
        gdtv::GDTV_PLAYLIST_PATH,
        gdtv::official_exposable_channel_count().await
    )
}

async fn get_gdtv_status_json() -> impl IntoResponse {
    Json(gdtv::official_status().await)
}

async fn get_gdtv_play_hls(Path(pk): Path<u64>) -> impl IntoResponse {
    match gdtv::official_hls_playlist(pk).await {
        Ok(playlist) => (
            [
                (
                    axum::http::header::CONTENT_TYPE,
                    "application/vnd.apple.mpegurl",
                ),
                (axum::http::header::CACHE_CONTROL, "no-store"),
            ],
            playlist,
        )
            .into_response(),
        Err(error) => {
            crate::error_log::push("gdtv", format!("failed to resolve channel {pk}: {error:#}"))
                .await;
            tracing::warn!("Failed to resolve GDTV channel {pk}: {error:#}");
            (StatusCode::NOT_FOUND, "GDTV channel unavailable").into_response()
        }
    }
}

async fn get_ppv_playlist_m3u(headers: HeaderMap) -> impl IntoResponse {
    let client = ppv::get_client();
    match ppv::generate_ppv_playlist_m3u(&client, &request_base_url(&headers)).await {
        Ok(playlist) => (
            [
                (
                    axum::http::header::CONTENT_TYPE,
                    "application/vnd.apple.mpegurl",
                ),
                (axum::http::header::CACHE_CONTROL, "no-store"),
            ],
            playlist,
        )
            .into_response(),
        Err(error) => {
            crate::error_log::push("ppv", format!("failed to generate ppv playlist: {error:#}"))
                .await;
            tracing::warn!("Failed to generate PPV playlist: {error:#}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "PPV playlist generation failed",
            )
                .into_response()
        }
    }
}

async fn get_ppv_status() -> impl IntoResponse {
    let status = ppv::ppv_status().await;
    format!(
        "OK - PPV playlist: {}, Cached rooms: {}, Cached sources: {}",
        status.playlist_path, status.cached_rooms, status.cached_sources
    )
}

async fn get_ppv_status_json() -> impl IntoResponse {
    Json(ppv::ppv_status().await)
}

async fn get_ppv_play_hls(Path(id): Path<String>) -> impl IntoResponse {
    match ppv::ppv_hls_playlist(&id).await {
        Ok((playlist, cache_status)) => (
            [
                (
                    axum::http::header::CONTENT_TYPE,
                    "application/vnd.apple.mpegurl",
                ),
                (axum::http::header::CACHE_CONTROL, "no-store"),
                (
                    axum::http::header::HeaderName::from_static("x-ppv-cache"),
                    cache_status,
                ),
            ],
            playlist,
        )
            .into_response(),
        Err(error) => {
            crate::error_log::push(
                "ppv",
                format!("failed to resolve ppv channel {id}: {error:#}"),
            )
            .await;
            tracing::warn!("Failed to resolve PPV channel {id}: {error:#}");
            (StatusCode::NOT_FOUND, "PPV channel unavailable").into_response()
        }
    }
}

async fn get_playlist_default(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> axum::response::Response {
    if state.output.m3u_result_enabled {
        playlist_m3u_response(&state, &headers)
    } else {
        render_playlist_txt(&state).into_response()
    }
}

async fn get_playlist_m3u(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    playlist_m3u_response(&state, &headers)
}

fn playlist_m3u_response(state: &AppState, headers: &HeaderMap) -> axum::response::Response {
    (
        [
            (
                axum::http::header::CONTENT_TYPE,
                "text/plain; charset=utf-8".to_owned(),
            ),
            (
                axum::http::header::CONTENT_DISPOSITION,
                attachment_disposition(&state.output.result_m3u_path),
            ),
        ],
        render_playlist_m3u(state, headers),
    )
        .into_response()
}

async fn get_playlist_m3u_content(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> axum::response::Response {
    playlist_content_response(&state, &headers)
}

fn playlist_content_response(state: &AppState, headers: &HeaderMap) -> axum::response::Response {
    if state.output.m3u_result_enabled {
        render_playlist_m3u(state, headers).into_response()
    } else {
        render_playlist_txt(state).into_response()
    }
}

fn render_playlist_txt(state: &AppState) -> String {
    let mut txt = String::new();
    for entry in state.channels.iter() {
        txt.push_str(&format!("{},#genre#\n", entry.key()));
        let mut channels = entry.value().iter().collect::<Vec<_>>();
        playlist::sort_channel_refs_by_preferences(&state.output, &mut channels);
        let mut emitted = 0usize;
        let mut local_count = 0usize;
        let mut subscribe_count = 0usize;
        for channel in channels {
            if emitted >= state.output.urls_limit.max(1) {
                break;
            }
            if !playlist::origin_is_within_limit(
                &state.output,
                channel.origin,
                &mut local_count,
                &mut subscribe_count,
            ) {
                continue;
            }
            emitted += 1;
            txt.push_str(&format!(
                "{},{}\n",
                channel.name,
                channel_url_with_extra_info(&state.output, channel)
            ));
        }
        txt.push('\n');
    }
    txt
}

fn render_playlist_m3u(state: &AppState, headers: &HeaderMap) -> String {
    let mut m3u = String::from("#EXTM3U\n");
    let mut name_ids = HashMap::new();
    let mut next_id = 1usize;
    let base_url = request_base_url(headers);
    for entry in state.channels.iter() {
        let group = entry.key();
        let mut channels = entry.value().iter().collect::<Vec<_>>();
        playlist::sort_channel_refs_by_preferences(&state.output, &mut channels);
        let mut emitted = 0usize;
        let mut local_count = 0usize;
        let mut subscribe_count = 0usize;
        for channel in channels {
            if emitted >= state.output.urls_limit.max(1) {
                break;
            }
            if !playlist::origin_is_within_limit(
                &state.output,
                channel.origin,
                &mut local_count,
                &mut subscribe_count,
            ) {
                continue;
            }
            emitted += 1;
            let logo = playlist_logo(&state.output, channel, &base_url);
            let tvg_name = playlist::m3u_tvg_name(&state.output, &channel.name);
            let tvg_id = playlist::m3u_tvg_id(&mut name_ids, &mut next_id, &tvg_name);
            m3u.push_str(&format!(
                "#EXTINF:-1 tvg-id=\"{}\" tvg-name=\"{}\" tvg-logo=\"{}\" group-title=\"{}\",{}\n",
                tvg_id, tvg_name, logo, group, channel.name
            ));
            if state.output.open_headers
                && let Some(headers) = &channel.headers
            {
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
                channel_url_with_extra_info(&state.output, channel)
            ));
        }
    }
    m3u
}

fn attachment_disposition(path: &str) -> String {
    let filename = FsPath::new(path)
        .file_name()
        .and_then(|value| value.to_str())
        .filter(|value| !value.trim().is_empty())
        .unwrap_or("playlist.m3u");
    format!("attachment; filename=\"{}\"", filename.replace('\"', ""))
}

async fn get_playlist_txt(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    render_playlist_txt(&state)
}

fn playlist_logo(config: &OutputConfig, channel: &Channel, request_base_url: &str) -> String {
    playlist::channel_logo(
        config,
        &channel.name,
        channel.logo.as_deref(),
        Some(request_base_url),
    )
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

async fn get_dashboard(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let mut total_channels = 0;
    for entry in state.channels.iter() {
        total_channels += entry.value().len();
    }
    let engine = state
        .engine_status
        .read()
        .expect("engine status read lock poisoned")
        .clone();
    let epg = state
        .epg_status
        .read()
        .expect("EPG status read lock poisoned")
        .clone();

    let epg_duration = epg
        .last_duration_ms
        .map(|value| format!("{value} ms"))
        .unwrap_or_else(|| "n/a".to_owned());
    let epg_started = epg
        .last_started_at
        .map(|value| value.to_rfc3339())
        .unwrap_or_else(|| "n/a".to_owned());
    let epg_finished = epg
        .last_finished_at
        .map(|value| value.to_rfc3339())
        .unwrap_or_else(|| "n/a".to_owned());
    let epg_error = epg
        .last_error
        .as_deref()
        .map(|error| {
            format!(
                r#"<p class="err"><span class="k">error:</span> {}</p>"#,
                html_escape(error)
            )
        })
        .unwrap_or_default();

    let gdtv_status = gdtv::official_status().await;
    let ppv_status = ppv::ppv_status().await;

    let recent_errors = crate::error_log::snapshot().await;
    let recent_errors_html = if recent_errors.is_empty() {
        "<p>No recent errors.</p>".to_owned()
    } else {
        let items = recent_errors
            .iter()
            .take(10)
            .map(|error| {
                format!(
                    "<li><span class=\"k\">{}</span> <strong>{}</strong>: {}</li>",
                    html_escape(&error.at.to_rfc3339()),
                    html_escape(&error.scope),
                    html_escape(&error.message)
                )
            })
            .collect::<String>();
        format!("<ul>{items}</ul>")
    };

    let html = format!(
        r#"<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>iptvapi-rs dashboard</title>
<style>
body{{font-family:system-ui,-apple-system,Segoe UI,sans-serif;max-width:960px;margin:2rem auto;padding:0 1rem;line-height:1.5;color:#18212f}}
.card{{border:1px solid #d8dee9;border-radius:12px;padding:1rem;margin:1rem 0;background:#fff}}
.grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:1rem}}
.k{{color:#667085;font-size:.9rem}}
.v{{font-size:1.4rem;font-weight:700}}
a{{color:#175cd3}}
.err{{color:#b42318}}
</style>
</head>
<body>
<h1>iptvapi-rs dashboard</h1>
<div class="grid">
  <div class="card"><div class="k">uptime start</div><div>{started}</div></div>
  <div class="card"><div class="k">groups</div><div class="v">{groups}</div></div>
  <div class="card"><div class="k">online channels</div><div class="v">{channels}</div></div>
</div>
<div class="card">
  <h2>Engine</h2>
  <div class="grid">
    <div><div class="k">state</div><div class="v">{engine_state}</div></div>
    <div><div class="k">fetched</div><div class="v">{engine_fetched}</div></div>
    <div><div class="k">filtered</div><div class="v">{engine_filtered}</div></div>
    <div><div class="k">duration</div><div>{engine_duration}</div></div>
    <div><div class="k">subscribe channels</div><div class="v">{subscribe_channels}</div></div>
    <div><div class="k">subscribe failures</div><div class="v">{subscribe_failures}</div></div>
    <div><div class="k">subscribe whitelist</div><div>{subscribe_whitelist_channels} channels from {subscribe_whitelist_sources} sources</div></div>
    <div><div class="k">subscribe metadata</div><div>{subscribe_header_channels} header channels · {subscribe_extra_info_channels} extra-info channels · {subscribe_nomatch_channels} nomatch channels</div></div>
  </div>
  <p><span class="k">started:</span> {engine_started}<br>
     <span class="k">finished:</span> {engine_finished}</p>
  <p><a href="/engine/status">/engine/status</a></p>
</div>
<div class="card">
  <h2>GDTV</h2>
  <div class="grid">
    <div><div class="k">pinned channels</div><div class="v">{gdtv_channels}</div></div>
    <div><div class="k">cached play URLs</div><div class="v">{gdtv_cached}</div></div>
    <div><div class="k">cache TTL</div><div>{gdtv_ttl} s</div></div>
  </div>
  <p><a href="/gdtv/status">/gdtv/status</a> · <a href="/gdtv/status.json">/gdtv/status.json</a> · <a href="/gdtv.m3u">/gdtv.m3u</a></p>
</div>
<div class="card">
  <h2>PPV</h2>
  <div class="grid">
    <div><div class="k">cached rooms</div><div class="v">{ppv_rooms}</div></div>
    <div><div class="k">cached play URLs</div><div class="v">{ppv_sources}</div></div>
    <div><div class="k">cache TTL</div><div>{ppv_ttl} s</div></div>
  </div>
  <p><a href="/ppv/status">/ppv/status</a> · <a href="/ppv/status.json">/ppv/status.json</a> · <a href="/ppv.m3u">/ppv.m3u</a></p>
</div>
<div class="card">
  <h2>EPG</h2>
  <div class="grid">
    <div><div class="k">state</div><div class="v">{epg_state}</div></div>
    <div><div class="k">channels</div><div class="v">{epg_channels}</div></div>
    <div><div class="k">programmes</div><div class="v">{epg_programmes}</div></div>
    <div><div class="k">duration</div><div>{epg_duration}</div></div>
  </div>
  <p><span class="k">started:</span> {epg_started}<br>
     <span class="k">finished:</span> {epg_finished}</p>
  {epg_error}
  <p><a href="/epg/status">/epg/status</a> · <a href="/epg/epg.xml">/epg/epg.xml</a> · <a href="/epg/epg.gz">/epg/epg.gz</a></p>
</div>
<div class="card">
  <h2>Recent errors</h2>
  {recent_errors_html}
  <p><a href="/errors/recent">/errors/recent</a></p>
</div>
<div class="card">
  <h2>Endpoints</h2>
  <p><a href="/playlist.m3u">playlist.m3u</a> · <a href="/playlist.txt">playlist.txt</a> · <a href="/status">status</a> · <a href="/engine/status">engine status</a> · <a href="/metrics">metrics</a> · <a href="/gdtv/status">gdtv status</a> · <a href="/gdtv/status.json">gdtv json</a> · <a href="/gdtv.m3u">gdtv.m3u</a> · <a href="/ppv/status">ppv status</a> · <a href="/ppv/status.json">ppv json</a> · <a href="/ppv.m3u">ppv.m3u</a></p>
</div>
</body>
</html>
"#,
        started = state.started_at.to_rfc3339(),
        groups = state.channels.len(),
        channels = total_channels,
        engine_state = html_escape(&format!("{:?}", engine.state).to_lowercase()),
        engine_fetched = engine.fetched_channels,
        engine_filtered = engine.filtered_channels,
        subscribe_channels = engine.subscribe_channels,
        subscribe_failures = engine.subscribe_failed_sources,
        subscribe_whitelist_sources = engine.subscribe_whitelist_sources,
        subscribe_whitelist_channels = engine.subscribe_whitelist_channels,
        subscribe_header_channels = engine.subscribe_header_channels,
        subscribe_extra_info_channels = engine.subscribe_extra_info_channels,
        subscribe_nomatch_channels = engine.subscribe_nomatch_channels,
        engine_duration = html_escape(
            &engine
                .last_duration_ms
                .map(|value| format!("{value} ms"))
                .unwrap_or_else(|| "n/a".to_owned()),
        ),
        engine_started = html_escape(
            &engine
                .last_started_at
                .map(|value| value.to_rfc3339())
                .unwrap_or_else(|| "n/a".to_owned()),
        ),
        engine_finished = html_escape(
            &engine
                .last_finished_at
                .map(|value| value.to_rfc3339())
                .unwrap_or_else(|| "n/a".to_owned()),
        ),
        gdtv_channels = gdtv_status.pinned_channels,
        gdtv_cached = gdtv_status.cached_play_urls,
        gdtv_ttl = gdtv_status.cache_ttl_secs,
        ppv_rooms = ppv_status.cached_rooms,
        ppv_sources = ppv_status.cached_sources,
        ppv_ttl = ppv_status.cache_ttl_secs,
        epg_state = html_escape(&format!("{:?}", epg.state).to_lowercase()),
        epg_channels = epg.channels,
        epg_programmes = epg.programmes,
        epg_duration = html_escape(&epg_duration),
        epg_started = html_escape(&epg_started),
        epg_finished = html_escape(&epg_finished),
        epg_error = epg_error,
        recent_errors_html = recent_errors_html,
    );

    (
        [(axum::http::header::CONTENT_TYPE, "text/html; charset=utf-8")],
        html,
    )
}

fn html_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

async fn get_result_log(State(state): State<Arc<AppState>>) -> axum::response::Response {
    serve_file(&state.output.result_log_path, "text/plain; charset=utf-8").await
}

async fn get_speed_test_log(State(state): State<Arc<AppState>>) -> axum::response::Response {
    serve_file(
        &state.output.speed_test_log_path,
        "text/plain; charset=utf-8",
    )
    .await
}

async fn get_statistic_log(State(state): State<Arc<AppState>>) -> axum::response::Response {
    serve_file(
        &state.output.statistic_log_path,
        "text/plain; charset=utf-8",
    )
    .await
}

async fn get_nomatch_log(State(state): State<Arc<AppState>>) -> axum::response::Response {
    serve_file(
        &state.subscribe_nomatch_log_path,
        "text/plain; charset=utf-8",
    )
    .await
}

async fn get_recent_errors() -> impl IntoResponse {
    Json(crate::error_log::snapshot().await)
}

async fn get_metrics(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4",
        )],
        state.metrics.render(),
    )
}

async fn get_status(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let mut total_channels = 0;
    for entry in state.channels.iter() {
        total_channels += entry.value().len();
    }
    format!(
        "OK - Groups: {}, Total Channels: {}",
        state.channels.len(),
        total_channels
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::HttpBody;

    #[test]
    fn server_config_defaults_open_service_enabled() {
        let server: crate::models::ServerConfig =
            toml::from_str("host = \"127.0.0.1\"\nport = 12315\n").unwrap();
        assert!(server.open_service);
    }

    #[test]
    fn safe_child_path_rejects_hls_proxy_traversal() {
        assert!(safe_child_path("/tmp/hls", "channel.m3u8").is_some());
        assert!(safe_child_path("/tmp/hls", "../channel.m3u8").is_none());
        assert!(safe_child_path("/tmp/hls", ".hidden.m3u8").is_none());
    }

    #[test]
    fn hls_proxy_readiness_matches_python_segment_wait_gate() {
        assert!(!hls_playlist_ready(
            b"#EXTM3U\n#EXTINF:1,\na.ts\n#EXTINF:1,\nb.ts\n"
        ));
        assert!(!hls_playlist_ready(
            b"#EXTM3U\n#EXTINF:1,\na.ts\n#EXTINF:1,\nb.ts\n#EXTINF:1,\nc.ts\n#EXT-X-DISCONTINUITY\n"
        ));
        assert!(hls_playlist_ready(
            b"#EXTM3U\n#EXTINF:1,\na.ts\n#EXTINF:1,\nb.ts\n#EXTINF:1,\nc.ts\n"
        ));
    }

    #[tokio::test]
    async fn hls_proxy_waits_for_ready_playlist_like_python() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("channel.m3u8");
        tokio::fs::write(&path, "#EXTM3U\n#EXTINF:1,\na.ts\n")
            .await
            .unwrap();
        let writer_path = path.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            tokio::fs::write(
                writer_path,
                "#EXTM3U\n#EXTINF:1,\na.ts\n#EXTINF:1,\nb.ts\n#EXTINF:1,\nc.ts\n",
            )
            .await
            .unwrap();
        });

        let content = read_ready_hls_playlist(path.to_str().unwrap(), Duration::from_millis(1_500))
            .await
            .unwrap();

        assert!(hls_playlist_ready(&content));
    }

    #[test]
    fn hls_proxy_ffmpeg_args_match_python_start_shape() {
        let args = ffmpeg_hls_args(
            "http://example.test/live.m3u8$extra",
            Some(&HashMap::from([(
                "Referer".to_owned(),
                "https://example.test/".to_owned(),
            )])),
            "rtmp://127.0.0.1:1935/hls/abc",
        );

        assert_eq!(args[0..3], ["-loglevel", "error", "-re"]);
        assert!(
            args.windows(2)
                .any(|pair| pair == ["-headers", "Referer: https://example.test/\r\n"])
        );
        assert!(
            args.windows(2)
                .any(|pair| pair == ["-i", "http://example.test/live.m3u8"])
        );
        assert!(args.windows(2).any(|pair| pair == ["-f", "flv"]));
        assert_eq!(args.last().unwrap(), "rtmp://127.0.0.1:1935/hls/abc");
    }

    #[test]
    fn nginx_conf_render_matches_python_placeholders() {
        let local = crate::models::LocalConfig {
            nginx_http_port: 18080,
            nginx_rtmp_port: 11935,
            ..Default::default()
        };
        let rendered = render_nginx_conf_template(
            "listen ${NGINX_HTTP_PORT}; rtmp ${NGINX_RTMP_PORT}; proxy ${APP_PORT};",
            15180,
            &local,
        );

        assert_eq!(rendered, "listen 18080; rtmp 11935; proxy 15180;");
    }

    #[test]
    fn start_rtmp_service_renders_config_even_without_nginx_binary() {
        let dir = tempfile::tempdir().unwrap();
        let conf_dir = dir.path().join("conf");
        std::fs::create_dir(&conf_dir).unwrap();
        std::fs::write(
            conf_dir.join("nginx.conf.template"),
            "listen ${NGINX_HTTP_PORT}; rtmp ${NGINX_RTMP_PORT}; proxy ${APP_PORT};",
        )
        .unwrap();
        let local = crate::models::LocalConfig {
            nginx_dir_path: dir.path().to_string_lossy().into_owned(),
            nginx_http_port: 18080,
            nginx_rtmp_port: 11935,
            ..Default::default()
        };

        let child = start_rtmp_service(&local, 15180).unwrap();

        assert!(child.is_none());
        assert_eq!(
            std::fs::read_to_string(conf_dir.join("nginx.conf")).unwrap(),
            "listen 18080; rtmp 11935; proxy 15180;"
        );
    }

    #[test]
    fn hls_proxy_reads_python_rtmp_lookup_db() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("rtmp.db");
        let connection = rusqlite::Connection::open(&db_path).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE result_data (id TEXT PRIMARY KEY, url TEXT, headers TEXT);",
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO result_data (id, url, headers) VALUES (?1, ?2, ?3)",
                rusqlite::params![
                    "abc",
                    "http://example.test/live.m3u8",
                    r#"{"User-Agent":"UA"}"#
                ],
            )
            .unwrap();
        drop(connection);

        let source = lookup_rtmp_channel(db_path.to_str().unwrap(), "abc")
            .unwrap()
            .unwrap();

        assert_eq!(source.url, "http://example.test/live.m3u8");
        assert_eq!(source.headers.unwrap()["User-Agent"], "UA");
    }

    #[tokio::test]
    async fn hls_idle_cleanup_stops_streams_after_timeout_like_python() {
        let mut streams = HashMap::new();
        let child = Command::new("sleep")
            .arg("60")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let now = Instant::now();
        streams.insert(
            "old".to_owned(),
            HlsStream {
                child,
                last_access: now - Duration::from_secs(10),
            },
        );

        stop_idle_hls_streams_inner(&mut streams, now, Duration::from_secs(5)).unwrap();

        assert!(!streams.contains_key("old"));
    }

    #[test]
    fn content_route_respects_open_m3u_result_like_python() {
        let output = crate::models::OutputConfig {
            m3u_result_enabled: false,
            ..Default::default()
        };
        let state = AppState {
            channels: Arc::new(DashMap::new()),
            metrics: metrics_exporter_prometheus::PrometheusBuilder::new()
                .build_recorder()
                .handle(),
            epg_xml_path: String::new(),
            epg_gz_path: String::new(),
            epg_status: Arc::new(RwLock::new(crate::epg::EpgStatus::default())),
            engine_status: Arc::new(RwLock::new(crate::engine::EngineStatus::default())),
            subscribe_nomatch_log_path: String::new(),
            output,
            local: crate::models::LocalConfig::default(),
            hls_streams: Arc::new(Mutex::new(HashMap::new())),
            rtmp_service: Arc::new(Mutex::new(None)),
            started_at: Utc::now(),
        };

        let response = playlist_content_response(&state, &HeaderMap::new());

        assert_eq!(response.status(), StatusCode::OK);
        assert!(
            response
                .headers()
                .get(axum::http::header::CONTENT_DISPOSITION)
                .is_none()
        );
    }

    #[test]
    fn missing_file_response_matches_python_waiting_tip_status() {
        let response = waiting_tip_response();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get(axum::http::header::CONTENT_TYPE)
                .unwrap(),
            "text/plain; charset=utf-8"
        );
    }

    #[tokio::test]
    async fn favicon_response_matches_python_static_icon_type() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("favicon.ico");
        tokio::fs::write(&path, b"ico").await.unwrap();

        let response = serve_static_file(path.to_str().unwrap(), "image/vnd.microsoft.icon").await;

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get(axum::http::header::CONTENT_TYPE)
                .unwrap(),
            "image/vnd.microsoft.icon"
        );
    }

    #[tokio::test]
    async fn missing_static_file_returns_404_not_waiting_tip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("favicon.ico");

        let response = serve_static_file(path.to_str().unwrap(), "image/vnd.microsoft.icon").await;

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert_ne!(
            response
                .headers()
                .get(axum::http::header::CONTENT_TYPE)
                .and_then(|value| value.to_str().ok()),
            Some("text/plain; charset=utf-8")
        );
    }

    #[tokio::test]
    async fn on_done_matches_python_empty_success_response() {
        let response = post_on_done(Form(RtmpOnDoneForm {
            name: "abc".to_owned(),
        }))
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.body().size_hint().lower(), 0);
    }

    #[test]
    fn epg_file_response_matches_python_attachment_style() {
        let response = file_response(
            "output/epg/epg.xml",
            "text/plain; charset=utf-8",
            true,
            b"xml".to_vec(),
        );
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get(axum::http::header::CONTENT_TYPE)
                .unwrap(),
            "text/plain; charset=utf-8"
        );
        assert_eq!(
            response
                .headers()
                .get(axum::http::header::CONTENT_DISPOSITION)
                .unwrap(),
            "attachment; filename=\"epg.xml\""
        );
    }

    #[test]
    fn attachment_disposition_uses_safe_file_name() {
        assert_eq!(
            attachment_disposition("output/result.m3u"),
            "attachment; filename=\"result.m3u\""
        );
        assert_eq!(
            attachment_disposition("output/bad\"name.m3u"),
            "attachment; filename=\"badname.m3u\""
        );
    }
}
