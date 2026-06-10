use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::Path;

#[derive(Debug, Clone, Default)]
pub struct Whitelist {
    pub exact: HashMap<String, HashSet<String>>,
    pub keywords: HashMap<String, Vec<String>>,
}

impl Whitelist {
    pub fn load<P: AsRef<Path>>(path: P) -> Self {
        let mut whitelist = Self::default();
        if let Ok(content) = fs::read_to_string(path) {
            let mut in_keyword_section = false;
            for line in content.lines() {
                let line = line.trim();
                if line.is_empty() || line.starts_with('#') {
                    continue;
                }

                if line.starts_with('[') && line.ends_with(']') {
                    in_keyword_section = line.to_uppercase() == "[KEYWORDS]";
                    continue;
                }

                let (name, value) = line
                    .split_once(',')
                    .map(|(name, value)| (name.trim().to_string(), value.trim().to_string()))
                    .unwrap_or_else(|| (String::new(), line.to_string()));

                if in_keyword_section {
                    whitelist.keywords.entry(name).or_default().push(value);
                } else {
                    whitelist.exact.entry(name).or_default().insert(value);
                }
            }
        }
        whitelist
    }

    pub fn is_whitelisted(&self, url: &str, channel_name: &str) -> bool {
        // 1. Exact match (channel specific)
        if self
            .exact
            .get(channel_name)
            .is_some_and(|set| set.contains(url))
        {
            return true;
        }
        // 2. Exact match (global)
        if self.exact.get("").is_some_and(|set| set.contains(url)) {
            return true;
        }
        // 3. Keyword match (channel specific)
        if let Some(kws) = self.keywords.get(channel_name)
            && kws.iter().any(|kw| url.contains(kw))
        {
            return true;
        }
        // 4. Keyword match (global)
        if let Some(kws) = self.keywords.get("")
            && kws.iter().any(|kw| url.contains(kw))
        {
            return true;
        }
        false
    }
}

#[derive(Debug, Clone, Default)]
pub struct Blacklist {
    pub keywords: Vec<String>,
}

impl Blacklist {
    pub fn load<P: AsRef<Path>>(path: P) -> Self {
        let mut blacklist = Self::default();
        if let Ok(content) = fs::read_to_string(path) {
            for line in content.lines() {
                let line = line.trim();
                if !line.is_empty() && !line.starts_with('#') {
                    blacklist.keywords.push(line.to_string());
                }
            }
        }
        blacklist
    }

    pub fn is_blacklisted(&self, url: &str) -> bool {
        self.keywords.iter().any(|kw| url.contains(kw))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_whitelist() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, "CCTV 1,http://stream.com/cctv1").unwrap();
        writeln!(file, "http://stream.com/global").unwrap();
        writeln!(file, "[KEYWORDS]").unwrap();
        writeln!(file, "CCTV 2,keyword1").unwrap();
        writeln!(file, "keyword_global").unwrap();

        let whitelist = Whitelist::load(file.path());

        // Exact channel match
        assert!(whitelist.is_whitelisted("http://stream.com/cctv1", "CCTV 1"));
        // Exact global match
        assert!(whitelist.is_whitelisted("http://stream.com/global", "Any"));
        // Keyword channel match
        assert!(whitelist.is_whitelisted("http://stream.com/keyword1/extra", "CCTV 2"));
        // Keyword global match
        assert!(whitelist.is_whitelisted("http://stream.com/keyword_global/extra", "Any"));
        // No match
        assert!(!whitelist.is_whitelisted("http://stream.com/other", "Any"));
    }

    #[test]
    fn test_blacklist() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, "bad_url").unwrap();
        writeln!(file, "malware").unwrap();

        let blacklist = Blacklist::load(file.path());
        assert!(blacklist.is_blacklisted("http://stream.com/bad_url/123"));
        assert!(blacklist.is_blacklisted("http://malware.com"));
        assert!(!blacklist.is_blacklisted("http://good.com"));
    }
}
