use crate::{
    gdtv,
    models::{Channel, ChannelOrigin, LocalConfig, SourceConfig, SourceType},
    ppv,
};
use anyhow::Result;
use reqwest::{Client, Proxy};
use std::{fs, path::Path, time::Duration};

pub struct Fetcher {
    client: Client,
    #[cfg(test)]
    timeout: Duration,
}

fn http_proxy_from_config(value: &str) -> Option<Proxy> {
    let value = value.trim();
    if value.is_empty() {
        return None;
    }
    Proxy::all(value).ok()
}

impl Fetcher {
    pub fn new(timeout_secs: u64, http_proxy: &str) -> Self {
        let timeout = Duration::from_secs(timeout_secs.max(1));
        let mut builder = Client::builder()
            .timeout(timeout)
            .user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36");
        if let Some(proxy) = http_proxy_from_config(http_proxy) {
            builder = builder.proxy(proxy);
        }
        let client = builder.build().unwrap_or_default();
        Self {
            client,
            #[cfg(test)]
            timeout,
        }
    }

    #[cfg(test)]
    fn timeout(&self) -> Duration {
        self.timeout
    }

    pub async fn fetch_local_sources(&self, config: &LocalConfig) -> Result<Vec<Channel>> {
        let mut channels = Vec::new();
        channels.extend(self.fetch_hls_dir_sources(config)?);
        for path in local_source_paths(config) {
            if !path.exists() || !path.is_file() {
                continue;
            }
            let content = fs::read_to_string(&path)?;
            channels.extend(self.parse_local_file(&path, &content)?);
        }
        Ok(channels)
    }

    fn fetch_hls_dir_sources(&self, config: &LocalConfig) -> Result<Vec<Channel>> {
        if !config.hls_enabled {
            return Ok(Vec::new());
        }
        let dir = Path::new(&config.hls_dir_path);
        if !dir.is_dir() {
            return Ok(Vec::new());
        }
        let mut files = fs::read_dir(dir)?
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .filter(|path| path.is_file())
            .collect::<Vec<_>>();
        files.sort();

        Ok(files
            .into_iter()
            .filter_map(|path| {
                let name = path.file_stem()?.to_str()?.trim().to_owned();
                if name.is_empty() {
                    return None;
                }
                Some(Channel {
                    origin: ChannelOrigin::Hls,
                    name,
                    group: "HLS".to_owned(),
                    url: path.to_string_lossy().into_owned(),
                    logo: None,
                    headers: None,
                    extra_info: None,
                    catchup: None,
                    location: None,
                    isp: None,
                    speed: None,
                    resolution: None,
                    date: None,
                    latency: Some(0),
                    last_checked: None,
                    is_online: true,
                })
            })
            .collect())
    }

    fn parse_local_file(&self, path: &Path, content: &str) -> Result<Vec<Channel>> {
        match path
            .extension()
            .and_then(|value| value.to_str())
            .map(|value| value.to_ascii_lowercase())
            .as_deref()
        {
            Some("m3u" | "m3u8") => self.parse_m3u(content),
            _ => self.parse_txt(content),
        }
    }

    pub async fn fetch_source(&self, source: &SourceConfig) -> Result<Vec<Channel>> {
        tracing::info!("Fetching source: {} ({})", source.name, source.url);

        match source.source_type {
            SourceType::M3u => {
                let content = self.fetch_text_source(&source.url).await?;
                self.parse_m3u(&content)
            }
            SourceType::Txt => {
                let content = self.fetch_text_source(&source.url).await?;
                self.parse_txt(&content)
            }
            SourceType::Dynamic => {
                if source.url.starts_with("gdtv:") {
                    let id = source.url.trim_start_matches("gdtv:");
                    self.fetch_gdtv(id, &source.name).await
                } else if source.url.starts_with("ppv:") {
                    let id = source.url.trim_start_matches("ppv:");
                    self.fetch_ppv(id, &source.name).await
                } else {
                    anyhow::bail!("Unsupported dynamic source: {}", source.url)
                }
            }
        }
    }

    async fn fetch_text_source(&self, url: &str) -> Result<String> {
        if url.starts_with("http://") || url.starts_with("https://") {
            return Ok(self.client.get(url).send().await?.text().await?);
        }

        let path = url.strip_prefix("file://").unwrap_or(url);
        Ok(fs::read_to_string(Path::new(path))?)
    }

    async fn fetch_gdtv(&self, id: &str, name: &str) -> Result<Vec<Channel>> {
        let pk = id.trim();
        if pk.is_empty() || !pk.chars().all(|ch| ch.is_ascii_digit()) {
            anyhow::bail!("Invalid GDTV channel pk: {id}");
        }

        Ok(vec![Channel {
            origin: ChannelOrigin::Local,
            name: name.to_string(),
            group: "GDTV".to_string(),
            url: format!(
                "http://{}/{}/{}",
                gdtv::default_host_port(),
                gdtv::GDTV_PLAY_PATH_PREFIX.trim_start_matches('/'),
                pk
            ),
            logo: None,
            headers: None,
            extra_info: None,
            catchup: None,
            location: None,
            isp: None,
            speed: None,
            resolution: None,
            date: None,
            latency: Some(0),
            last_checked: None,
            is_online: true,
        }])
    }

    async fn fetch_ppv(&self, id: &str, name: &str) -> Result<Vec<Channel>> {
        let room_id = id.trim();
        if room_id.is_empty() {
            anyhow::bail!("Invalid PPV channel id: {id}");
        }

        Ok(vec![Channel {
            origin: ChannelOrigin::Local,
            name: name.to_string(),
            group: "PPV".to_string(),
            url: format!(
                "http://127.0.0.1:12345/{}/{}",
                ppv::PPV_PLAY_PATH_PREFIX.trim_start_matches('/'),
                room_id
            ),
            logo: None,
            headers: None,
            extra_info: None,
            catchup: None,
            location: None,
            isp: None,
            speed: None,
            resolution: None,
            date: None,
            latency: Some(0),
            last_checked: None,
            is_online: true,
        }])
    }

    pub fn parse_m3u(&self, content: &str) -> Result<Vec<Channel>> {
        let mut channels = Vec::new();
        let mut current_group = "Other".to_string();

        // 简单的 M3U 解析，利用正则表达式或逐行扫描
        // 也可以使用 m3u8-rs，但对于 IPTV 列表，通常包含 #EXTINF:-1 group-title="xxx",Name 这种非标准扩展
        for line in content.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }

            if line.starts_with("#EXTINF") {
                // 提取 group-title
                if let Some(group) = self.extract_attribute(line, "group-title") {
                    current_group = group;
                }
                // 提取名称，兼容 Python 旧逻辑里的英文/中文逗号分隔
                if let Some((_, name)) = split_name_value(line) {
                    channels.push(Channel {
                        origin: ChannelOrigin::Local,
                        name: name.trim().to_string(),
                        group: current_group.clone(),
                        url: String::new(), // 下一步填充
                        logo: self.extract_attribute(line, "tvg-logo"),
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
                        is_online: false,
                    });
                }
            } else if !line.starts_with('#') {
                // 这是 URL 行
                if let Some(last_channel) = channels.last_mut()
                    && last_channel.url.is_empty()
                {
                    last_channel.url = line.to_string();
                }
            }
        }
        Ok(channels.into_iter().filter(|c| !c.url.is_empty()).collect())
    }

    pub fn parse_txt(&self, content: &str) -> Result<Vec<Channel>> {
        let mut channels = Vec::new();
        let mut current_group = "Other".to_string();

        for line in content.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }

            if line.contains("#genre#") {
                current_group = split_name_value(line)
                    .map(|(name, _)| name.trim())
                    .unwrap_or_else(|| line.split("#genre#").next().unwrap_or("Other").trim())
                    .to_string();
            } else if let Some((name, url)) = split_name_value(line) {
                let name = name.trim();
                let url = url.trim();
                if !name.is_empty() {
                    channels.push(Channel {
                        origin: ChannelOrigin::Local,
                        name: name.to_string(),
                        group: current_group.clone(),
                        url: url.to_string(),
                        logo: None,
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
                        is_online: false,
                    });
                }
            }
        }
        Ok(channels)
    }

    fn extract_attribute(&self, line: &str, attr: &str) -> Option<String> {
        let pattern = format!("{}=\"([^\"]*)\"", attr);
        let re = regex::Regex::new(&pattern).ok()?;
        re.captures(line).map(|cap| cap[1].to_string())
    }
}

fn local_source_paths(config: &LocalConfig) -> Vec<std::path::PathBuf> {
    let mut paths = Vec::new();
    if !config.file_path.trim().is_empty() {
        paths.push(Path::new(&config.file_path).to_path_buf());
    }
    let dir = Path::new(&config.dir_path);
    if dir.is_dir()
        && let Ok(entries) = fs::read_dir(dir)
    {
        let mut files = entries
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .filter(|path| path.is_file())
            .filter(|path| {
                path.extension()
                    .and_then(|value| value.to_str())
                    .map(|ext| matches!(ext.to_ascii_lowercase().as_str(), "txt" | "m3u" | "m3u8"))
                    .unwrap_or(false)
            })
            .collect::<Vec<_>>();
        files.sort();
        paths.extend(files);
    }
    paths
}

fn split_name_value(line: &str) -> Option<(&str, &str)> {
    let (index, separator_len) = line
        .char_indices()
        .find_map(|(index, ch)| matches!(ch, ',' | '，').then_some((index, ch.len_utf8())))?;
    Some((&line[..index], &line[index + separator_len..]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fetcher_uses_python_request_timeout_seconds() {
        let fetcher = Fetcher::new(7, "");
        assert_eq!(fetcher.timeout(), Duration::from_secs(7));
    }

    #[test]
    fn http_proxy_config_accepts_python_proxy_url() {
        assert!(http_proxy_from_config("http://127.0.0.1:8080").is_some());
        assert!(http_proxy_from_config(" ").is_none());
    }

    #[tokio::test]
    async fn dynamic_gdtv_source_uses_official_play_endpoint() {
        let fetcher = Fetcher::new(10, "");
        let channels = fetcher.fetch_gdtv(" 43 ", "广东卫视").await.unwrap();

        assert_eq!(channels.len(), 1);
        assert_eq!(channels[0].group, "GDTV");
        assert_eq!(channels[0].url, "http://127.0.0.1:12345/gdtv/play/43");
        assert!(!channels[0].url.contains("/proxy/gdtv/"));
    }

    #[tokio::test]
    async fn dynamic_gdtv_source_rejects_non_pk_ids() {
        let fetcher = Fetcher::new(10, "");
        let error = fetcher.fetch_gdtv("abc", "bad").await.unwrap_err();

        assert!(error.to_string().contains("Invalid GDTV channel pk"));
    }

    #[tokio::test]
    async fn dynamic_ppv_source_uses_play_endpoint() {
        let fetcher = Fetcher::new(10, "");
        let channels = fetcher.fetch_ppv("rally-tv", "Rally TV").await.unwrap();

        assert_eq!(channels.len(), 1);
        assert_eq!(channels[0].group, "PPV");
        assert_eq!(channels[0].url, "http://127.0.0.1:12345/ppv/play/rally-tv");
    }

    #[tokio::test]
    async fn dynamic_ppv_source_rejects_empty_ids() {
        let fetcher = Fetcher::new(10, "");
        let error = fetcher.fetch_ppv("", "bad").await.unwrap_err();

        assert!(error.to_string().contains("Invalid PPV channel id"));
    }

    #[tokio::test]
    async fn fetch_local_sources_reads_local_file_and_directory() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("local.txt");
        let dir_path = dir.path().join("local");
        fs::create_dir(&dir_path).unwrap();
        fs::write(
            &file_path,
            "File,#genre#
File 1,http://stream.example/file.m3u8
",
        )
        .unwrap();
        fs::write(
            dir_path.join("extra.txt"),
            "Dir,#genre#
Dir 1,http://stream.example/dir.m3u8
",
        )
        .unwrap();
        fs::write(
            dir_path.join("extra.m3u"),
            "#EXTM3U
#EXTINF:-1 group-title=\"M3U Dir\",Dir M3U
http://stream.example/dir-m3u.m3u8
",
        )
        .unwrap();
        let fetcher = Fetcher::new(10, "");
        let channels = fetcher
            .fetch_local_sources(&LocalConfig {
                enabled: true,
                file_path: file_path.to_string_lossy().into_owned(),
                dir_path: dir_path.to_string_lossy().into_owned(),
                match_aliases: true,
                hls_enabled: false,
                hls_dir_path: String::new(),
                hls_temp_path: String::new(),
                nginx_dir_path: String::new(),
                nginx_http_port: 8080,
                nginx_rtmp_port: 1935,
                rtmp_idle_timeout: 300,
                rtmp_max_streams: 10,
            })
            .await
            .unwrap();

        assert_eq!(channels.len(), 3);
        assert!(channels.iter().any(|channel| channel.name == "File 1"));
        assert!(channels.iter().any(|channel| channel.name == "Dir 1"));
        assert!(channels.iter().any(|channel| channel.name == "Dir M3U"));
    }

    #[tokio::test]
    async fn fetch_local_sources_reads_hls_dir_by_filename() {
        let dir = tempfile::tempdir().unwrap();
        let hls_dir = dir.path().join("hls");
        fs::create_dir(&hls_dir).unwrap();
        fs::write(hls_dir.join("CCTV-1.m3u8"), "#EXTM3U\n").unwrap();
        let fetcher = Fetcher::new(10, "");

        let channels = fetcher
            .fetch_local_sources(&LocalConfig {
                enabled: true,
                file_path: String::new(),
                dir_path: String::new(),
                match_aliases: true,
                hls_enabled: true,
                hls_dir_path: hls_dir.to_string_lossy().into_owned(),
                hls_temp_path: String::new(),
                nginx_dir_path: String::new(),
                nginx_http_port: 8080,
                nginx_rtmp_port: 1935,
                rtmp_idle_timeout: 300,
                rtmp_max_streams: 10,
            })
            .await
            .unwrap();

        assert_eq!(channels.len(), 1);
        assert_eq!(channels[0].origin, ChannelOrigin::Hls);
        assert_eq!(channels[0].name, "CCTV-1");
        assert_eq!(
            channels[0].url,
            hls_dir.join("CCTV-1.m3u8").to_string_lossy()
        );
        assert!(channels[0].is_online);
    }

    #[tokio::test]
    async fn fetch_source_reads_local_txt_files() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("demo.txt");
        fs::write(
            &path,
            "Local,#genre#
Local 1,http://stream.example/local.m3u8
",
        )
        .unwrap();
        let fetcher = Fetcher::new(10, "");
        let channels = fetcher
            .fetch_source(&SourceConfig {
                name: "local".to_owned(),
                url: path.to_string_lossy().into_owned(),
                source_type: SourceType::Txt,
            })
            .await
            .unwrap();

        assert_eq!(channels.len(), 1);
        assert_eq!(channels[0].group, "Local");
        assert_eq!(channels[0].name, "Local 1");
    }

    #[tokio::test]
    async fn fetch_source_reads_file_scheme_m3u_files() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("demo.m3u");
        fs::write(
            &path,
            r#"#EXTM3U
#EXTINF:-1 group-title="Local",Local 1
http://stream.example/local.m3u8
"#,
        )
        .unwrap();
        let fetcher = Fetcher::new(10, "");
        let channels = fetcher
            .fetch_source(&SourceConfig {
                name: "local".to_owned(),
                url: format!("file://{}", path.display()),
                source_type: SourceType::M3u,
            })
            .await
            .unwrap();

        assert_eq!(channels.len(), 1);
        assert_eq!(channels[0].group, "Local");
        assert_eq!(channels[0].name, "Local 1");
    }

    #[test]
    fn test_parse_m3u() {
        let fetcher = Fetcher::new(10, "");
        let content = r#"#EXTM3U
#EXTINF:-1 tvg-logo="http://logo.com/cctv1.png" group-title="CCTV",CCTV 1
http://stream.com/cctv1
#EXTINF:-1 group-title="CCTV",CCTV 2
http://stream.com/cctv2
"#;
        let channels = fetcher.parse_m3u(content).unwrap();
        assert_eq!(channels.len(), 2);
        assert_eq!(channels[0].name, "CCTV 1");
        assert_eq!(channels[0].group, "CCTV");
        assert_eq!(channels[0].url, "http://stream.com/cctv1");
        assert_eq!(
            channels[0].logo,
            Some("http://logo.com/cctv1.png".to_string())
        );
        assert_eq!(channels[1].name, "CCTV 2");
        assert_eq!(channels[1].url, "http://stream.com/cctv2");
    }

    #[test]
    fn parses_txt_with_chinese_comma_and_url_commas() {
        let fetcher = Fetcher::new(10, "");
        let content = "央视，#genre#\nCCTV 1，http://stream.example/live.m3u8?token=a,b\n";
        let channels = fetcher.parse_txt(content).unwrap();

        assert_eq!(channels.len(), 1);
        assert_eq!(channels[0].group, "央视");
        assert_eq!(channels[0].name, "CCTV 1");
        assert_eq!(channels[0].url, "http://stream.example/live.m3u8?token=a,b");
    }

    #[test]
    fn parses_m3u_with_chinese_comma_separator() {
        let fetcher = Fetcher::new(10, "");
        let content = r#"#EXTM3U
#EXTINF:-1 group-title="央视"，CCTV 1
http://stream.example/cctv1.m3u8
"#;
        let channels = fetcher.parse_m3u(content).unwrap();

        assert_eq!(channels.len(), 1);
        assert_eq!(channels[0].group, "央视");
        assert_eq!(channels[0].name, "CCTV 1");
    }

    #[test]
    fn test_parse_txt() {
        let fetcher = Fetcher::new(10, "");
        let content = r#"CCTV,#genre#
CCTV 1,http://stream.com/cctv1
CCTV 2,http://stream.com/cctv2

Other,#genre#
Channel 3,http://stream.com/c3
"#;
        let channels = fetcher.parse_txt(content).unwrap();
        assert_eq!(channels.len(), 3);
        assert_eq!(channels[0].name, "CCTV 1");
        assert_eq!(channels[0].group, "CCTV");
        assert_eq!(channels[2].name, "Channel 3");
        assert_eq!(channels[2].group, "Other");
    }
}
