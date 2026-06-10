use std::{
    collections::HashMap,
    net::{IpAddr, ToSocketAddrs},
};

use crate::models::{Channel, ChannelOrigin, OutputConfig};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum IpFamily {
    Ipv4,
    Ipv6,
}

pub(crate) fn ip_family(url: &str) -> IpFamily {
    let host = url_host(url);
    if let Some(host) = host.as_deref() {
        let host = host.trim_start_matches('[').trim_end_matches(']');
        if let Ok(ip) = host.parse::<IpAddr>() {
            return match ip {
                IpAddr::V4(_) => IpFamily::Ipv4,
                IpAddr::V6(_) => IpFamily::Ipv6,
            };
        }
        return dns_ip_family(host);
    }
    IpFamily::Ipv4
}

pub(crate) fn url_host(value: &str) -> Option<String> {
    if let Ok(url) = url::Url::parse(value) {
        return url.host_str().map(str::to_owned);
    }
    value
        .trim()
        .trim_start_matches('[')
        .trim_end_matches(']')
        .split('/')
        .next()
        .filter(|host| !host.is_empty())
        .map(str::to_owned)
}

fn dns_ip_family(host: &str) -> IpFamily {
    match (host, 0).to_socket_addrs() {
        Ok(addrs) => addrs
            .into_iter()
            .find(|addr| addr.is_ipv6())
            .map(|_| IpFamily::Ipv6)
            .unwrap_or(IpFamily::Ipv4),
        Err(error) => {
            tracing::debug!(host = %host, error = %error, "Failed to resolve host IP family; defaulting to IPv4");
            IpFamily::Ipv4
        }
    }
}

pub(crate) fn ip_family_name(family: IpFamily) -> &'static str {
    match family {
        IpFamily::Ipv4 => "ipv4",
        IpFamily::Ipv6 => "ipv6",
    }
}

pub(crate) fn sort_channels_by_preferences(config: &OutputConfig, channels: &mut [Channel]) {
    if config.origin_type_prefer.is_empty() && config.ipv_type_prefer.is_empty() {
        return;
    }
    channels.sort_by_key(|channel| channel_preference_key(config, channel));
}

pub(crate) fn sort_channel_refs_by_preferences(config: &OutputConfig, channels: &mut [&Channel]) {
    if config.origin_type_prefer.is_empty() && config.ipv_type_prefer.is_empty() {
        return;
    }
    channels.sort_by_key(|channel| channel_preference_key(config, channel));
}

fn channel_preference_key(config: &OutputConfig, channel: &Channel) -> (usize, usize) {
    (
        origin_rank(config, channel.origin),
        ip_rank(config, ip_family(&channel.url)),
    )
}

fn origin_rank(config: &OutputConfig, origin: ChannelOrigin) -> usize {
    config
        .origin_type_prefer
        .iter()
        .position(|preferred| *preferred == origin)
        .unwrap_or(config.origin_type_prefer.len())
}

fn ip_rank(config: &OutputConfig, family: IpFamily) -> usize {
    if config.ipv_type_prefer.is_empty() {
        return 0;
    }
    let family_name = ip_family_name(family);
    config
        .ipv_type_prefer
        .iter()
        .position(|preferred| preferred == family_name || preferred == "all")
        .unwrap_or(config.ipv_type_prefer.len())
}

pub(crate) fn origin_limit(config: &OutputConfig, origin: ChannelOrigin) -> usize {
    match origin {
        ChannelOrigin::Local => config.local_num,
        ChannelOrigin::Subscribe => config.subscribe_num,
        ChannelOrigin::Whitelist | ChannelOrigin::Hls => config.urls_limit,
    }
}

pub(crate) fn origin_is_within_limit(
    config: &OutputConfig,
    origin: ChannelOrigin,
    local_count: &mut usize,
    subscribe_count: &mut usize,
) -> bool {
    match origin {
        ChannelOrigin::Local => {
            if *local_count >= origin_limit(config, origin) {
                return false;
            }
            *local_count += 1;
            true
        }
        ChannelOrigin::Subscribe => {
            if *subscribe_count >= origin_limit(config, origin) {
                return false;
            }
            *subscribe_count += 1;
            true
        }
        ChannelOrigin::Whitelist | ChannelOrigin::Hls => true,
    }
}

pub(crate) fn channel_logo(
    config: &OutputConfig,
    name: &str,
    explicit_logo: Option<&str>,
    request_base_url: Option<&str>,
) -> String {
    if let Some(logo) = explicit_logo.filter(|value| !value.is_empty()) {
        return logo.to_owned();
    }
    let name = name.trim();
    if name.is_empty() {
        return String::new();
    }
    let logo_base = config.logo_url.trim().trim_end_matches('/');
    if !logo_base.is_empty() {
        return format!(
            "{logo_base}/{}.{}",
            m3u_tvg_name(config, name),
            config.logo_type
        );
    }
    let public_base = config.public_base_url.trim().trim_end_matches('/');
    if !public_base.is_empty() {
        return format!("{public_base}/logo/{name}.{}", config.logo_type);
    }
    request_base_url
        .map(|base| {
            format!(
                "{}/logo/{name}.{}",
                base.trim_end_matches('/'),
                config.logo_type
            )
        })
        .unwrap_or_default()
}

pub(crate) fn m3u_tvg_id(
    name_ids: &mut HashMap<String, usize>,
    next_id: &mut usize,
    tvg_name: &str,
) -> usize {
    if let Some(id) = name_ids.get(tvg_name) {
        return *id;
    }
    let id = *next_id;
    name_ids.insert(tvg_name.to_owned(), id);
    *next_id += 1;
    id
}

pub(crate) fn m3u_tvg_name(config: &OutputConfig, name: &str) -> String {
    let trimmed = name.trim();
    if !config
        .logo_url
        .contains("https://raw.githubusercontent.com/fanmingming/live/main/tv")
    {
        return trimmed.to_owned();
    }
    normalize_fanmingming_logo_name(trimmed)
}

fn normalize_fanmingming_logo_name(name: &str) -> String {
    // Python convert_to_m3u strips the dash only for CCTV/CETV numeric logo names
    // when using fanmingming's logo tree: r"(CCTV|CETV)-(\d+)(\+.*)?".
    for prefix in ["CCTV-", "CETV-"] {
        let Some(rest) = name.strip_prefix(prefix) else {
            continue;
        };
        let digit_len = rest
            .chars()
            .take_while(|ch| ch.is_ascii_digit())
            .map(char::len_utf8)
            .sum::<usize>();
        if digit_len == 0 {
            continue;
        }
        let digits = &rest[..digit_len];
        let suffix = &rest[digit_len..];
        let plus = if suffix.starts_with('+') { "+" } else { "" };
        return format!("{}{}{}", prefix.trim_end_matches('-'), digits, plus);
    }
    name.to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ip_family_detects_literal_hosts_and_defaults_failed_dns_to_ipv4() {
        assert_eq!(ip_family("http://127.0.0.1/live.m3u8"), IpFamily::Ipv4);
        assert_eq!(ip_family("http://[::1]/live.m3u8"), IpFamily::Ipv6);
        assert_eq!(ip_family("not a valid host name"), IpFamily::Ipv4);
    }

    #[test]
    fn preference_sort_orders_origin_then_ip_family() {
        let output = OutputConfig {
            origin_type_prefer: vec![ChannelOrigin::Subscribe, ChannelOrigin::Local],
            ipv_type_prefer: vec!["ipv6".to_owned(), "ipv4".to_owned()],
            ..Default::default()
        };
        let mut channels = vec![
            test_channel(
                "local-v4",
                ChannelOrigin::Local,
                "http://127.0.0.1/live.m3u8",
            ),
            test_channel(
                "sub-v4",
                ChannelOrigin::Subscribe,
                "http://127.0.0.1/sub.m3u8",
            ),
            test_channel("sub-v6", ChannelOrigin::Subscribe, "http://[::1]/sub.m3u8"),
        ];

        sort_channels_by_preferences(&output, &mut channels);

        assert_eq!(
            channels
                .iter()
                .map(|channel| channel.name.as_str())
                .collect::<Vec<_>>(),
            vec!["sub-v6", "sub-v4", "local-v4"]
        );
    }

    fn test_channel(name: &str, origin: ChannelOrigin, url: &str) -> Channel {
        Channel {
            origin,
            name: name.to_owned(),
            group: "Test".to_owned(),
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
            latency: None,
            last_checked: None,
            is_online: true,
        }
    }

    #[test]
    fn logo_fallback_can_use_configured_public_base_url() {
        let output = OutputConfig {
            public_base_url: "https://iptv.example:8443".to_owned(),
            logo_type: "webp".to_owned(),
            ..Default::default()
        };

        assert_eq!(
            channel_logo(&output, "CCTV-1", None, Some("http://internal:12315")),
            "https://iptv.example:8443/logo/CCTV-1.webp"
        );
    }

    #[test]
    fn fanmingming_logo_name_matches_python_conversion() {
        let output = OutputConfig {
            logo_url: "https://raw.githubusercontent.com/fanmingming/live/main/tv".to_owned(),
            ..Default::default()
        };

        assert_eq!(m3u_tvg_name(&output, "CCTV-1"), "CCTV1");
        assert_eq!(m3u_tvg_name(&output, "CETV-4+"), "CETV4+");
        assert_eq!(m3u_tvg_name(&output, "湖南卫视"), "湖南卫视");
    }
}
