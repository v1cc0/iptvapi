use axum::{body::Body, extract::MatchedPath, http::Request, middleware::Next, response::Response};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use std::{fs, sync::OnceLock, time::Instant};
use tokio::time::{Duration, interval};

static PROCESS_STARTED: OnceLock<Instant> = OnceLock::new();
const PROCESS_METRICS_INTERVAL: Duration = Duration::from_secs(15);

pub fn init() -> anyhow::Result<PrometheusHandle> {
    let _ = PROCESS_STARTED.set(Instant::now());
    let handle = PrometheusBuilder::new().install_recorder()?;
    metrics::describe_counter!(
        "iptvapi_update_cycles_total",
        "Completed IPTV update cycles."
    );
    metrics::describe_counter!("iptvapi_update_errors_total", "Failed IPTV update cycles.");
    metrics::describe_histogram!(
        "iptvapi_update_duration_seconds",
        "IPTV update cycle duration in seconds."
    );
    metrics::describe_gauge!(
        "iptvapi_online_groups",
        "Number of channel groups with at least one online channel."
    );
    metrics::describe_gauge!(
        "iptvapi_online_channels",
        "Number of online channels currently exposed."
    );
    metrics::describe_gauge!(
        "iptvapi_fetched_channels",
        "Number of channels fetched before filtering/checking in the last update."
    );
    metrics::describe_gauge!(
        "iptvapi_filtered_channels",
        "Number of channels kept after blacklist/whitelist filtering in the last update."
    );
    metrics::describe_counter!(
        "iptvapi_http_requests_total",
        "HTTP requests served by iptvapi-rs."
    );
    metrics::describe_histogram!(
        "iptvapi_http_request_duration_seconds",
        "HTTP request duration in seconds."
    );
    metrics::describe_counter!("iptvapi_epg_runs_total", "Completed EPG pipeline runs.");
    metrics::describe_counter!("iptvapi_epg_errors_total", "Failed EPG pipeline runs.");
    metrics::describe_counter!("iptvapi_epg_source_errors_total", "Failed EPG sources.");
    metrics::describe_histogram!(
        "iptvapi_epg_duration_seconds",
        "EPG pipeline duration in seconds."
    );
    metrics::describe_gauge!(
        "iptvapi_epg_channels",
        "Channels written into generated EPG output."
    );
    metrics::describe_gauge!(
        "iptvapi_epg_programmes",
        "Programmes written into generated EPG output."
    );
    metrics::describe_gauge!(
        "iptvapi_epg_aliases",
        "Loaded EPG direct/normalized alias entries."
    );
    metrics::describe_gauge!(
        "iptvapi_epg_alias_patterns",
        "Loaded EPG regex alias entries."
    );
    metrics::describe_counter!(
        "iptvapi_subscribe_source_errors_total",
        "Failed subscribe sources."
    );
    metrics::describe_gauge!(
        "iptvapi_subscribe_sources",
        "Configured subscribe sources read in the last update."
    );
    metrics::describe_gauge!(
        "iptvapi_subscribe_channels",
        "Channels fetched from subscribe sources in the last update."
    );
    metrics::describe_gauge!(
        "iptvapi_subscribe_header_channels",
        "Subscribe channels carrying parsed request headers."
    );
    metrics::describe_gauge!(
        "iptvapi_subscribe_extra_info_channels",
        "Subscribe channels carrying URL extra-info suffixes."
    );
    metrics::describe_gauge!(
        "iptvapi_subscribe_nomatch_channels",
        "Subscribe channels filtered out because no configured source channel matched."
    );
    metrics::describe_gauge!(
        "iptvapi_process_uptime_seconds",
        "Process uptime in seconds."
    );
    metrics::describe_gauge!(
        "iptvapi_process_resident_memory_bytes",
        "Resident set size reported by /proc/self/status."
    );
    metrics::describe_gauge!(
        "iptvapi_process_virtual_memory_bytes",
        "Virtual memory size reported by /proc/self/status."
    );
    metrics::describe_gauge!(
        "iptvapi_process_threads",
        "Thread count reported by /proc/self/status."
    );
    metrics::describe_gauge!(
        "iptvapi_process_open_fds",
        "Open file descriptor count from /proc/self/fd."
    );
    Ok(handle)
}

pub fn spawn_process_metrics_task() {
    record_process_metrics();
    tokio::spawn(async {
        let mut ticker = interval(PROCESS_METRICS_INTERVAL);
        loop {
            ticker.tick().await;
            record_process_metrics();
        }
    });
}

fn record_process_metrics() {
    if let Some(started) = PROCESS_STARTED.get() {
        metrics::gauge!("iptvapi_process_uptime_seconds").set(started.elapsed().as_secs_f64());
    }
    match read_process_metrics() {
        Ok(snapshot) => snapshot.record(),
        Err(error) => tracing::debug!(error = %error, "failed to read process metrics"),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ProcessMetricsSnapshot {
    resident_memory_bytes: Option<u64>,
    virtual_memory_bytes: Option<u64>,
    threads: Option<u64>,
    open_fds: Option<u64>,
}

impl ProcessMetricsSnapshot {
    fn record(&self) {
        if let Some(value) = self.resident_memory_bytes {
            metrics::gauge!("iptvapi_process_resident_memory_bytes").set(value as f64);
        }
        if let Some(value) = self.virtual_memory_bytes {
            metrics::gauge!("iptvapi_process_virtual_memory_bytes").set(value as f64);
        }
        if let Some(value) = self.threads {
            metrics::gauge!("iptvapi_process_threads").set(value as f64);
        }
        if let Some(value) = self.open_fds {
            metrics::gauge!("iptvapi_process_open_fds").set(value as f64);
        }
    }
}

fn read_process_metrics() -> std::io::Result<ProcessMetricsSnapshot> {
    let status = fs::read_to_string("/proc/self/status")?;
    Ok(ProcessMetricsSnapshot {
        resident_memory_bytes: parse_status_kb_field(&status, "VmRSS").map(kib_to_bytes),
        virtual_memory_bytes: parse_status_kb_field(&status, "VmSize").map(kib_to_bytes),
        threads: parse_status_u64_field(&status, "Threads"),
        open_fds: count_open_fds(),
    })
}

fn parse_status_kb_field(status: &str, key: &str) -> Option<u64> {
    parse_status_u64_field(status, key)
}

fn parse_status_u64_field(status: &str, key: &str) -> Option<u64> {
    status.lines().find_map(|line| {
        let (field, rest) = line.split_once(':')?;
        (field == key)
            .then(|| rest.split_whitespace().next()?.parse::<u64>().ok())
            .flatten()
    })
}

fn kib_to_bytes(kib: u64) -> u64 {
    kib.saturating_mul(1024)
}

fn count_open_fds() -> Option<u64> {
    fs::read_dir("/proc/self/fd")
        .ok()
        .map(|entries| entries.filter_map(Result::ok).count() as u64)
}

pub async fn track_http(request: Request<Body>, next: Next) -> Response {
    let method = request.method().clone();
    let path = request
        .extensions()
        .get::<MatchedPath>()
        .map(MatchedPath::as_str)
        .unwrap_or_else(|| request.uri().path())
        .to_owned();
    let started = Instant::now();
    let response = next.run(request).await;
    let latency = started.elapsed();
    let status = response.status().as_u16().to_string();

    metrics::counter!(
        "iptvapi_http_requests_total",
        "method" => method.to_string(),
        "path" => path.clone(),
        "status" => status.clone(),
    )
    .increment(1);
    metrics::histogram!(
        "iptvapi_http_request_duration_seconds",
        "method" => method.to_string(),
        "path" => path.clone(),
        "status" => status.clone(),
    )
    .record(latency.as_secs_f64());
    tracing::info!(
        method = %method,
        path = %path,
        status = %status,
        latency_ms = latency.as_millis(),
        "request complete"
    );

    response
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_linux_status_metrics() {
        let status = "Name:\tiptvapi\nVmSize:\t  1234 kB\nVmRSS:\t  567 kB\nThreads:\t8\n";
        assert_eq!(parse_status_kb_field(status, "VmSize"), Some(1234));
        assert_eq!(parse_status_kb_field(status, "VmRSS"), Some(567));
        assert_eq!(parse_status_u64_field(status, "Threads"), Some(8));
        assert_eq!(parse_status_u64_field(status, "Missing"), None);
    }

    #[test]
    fn converts_kib_to_bytes_safely() {
        assert_eq!(kib_to_bytes(2), 2048);
        assert_eq!(kib_to_bytes(u64::MAX), u64::MAX);
    }
}
