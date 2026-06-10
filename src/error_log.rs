use chrono::{DateTime, Utc};
use serde::Serialize;
use std::{collections::VecDeque, sync::OnceLock};
use tokio::sync::Mutex;

const MAX_RECENT_ERRORS: usize = 50;

#[derive(Debug, Clone, Serialize)]
pub struct RecentError {
    pub at: DateTime<Utc>,
    pub scope: String,
    pub message: String,
}

static RECENT_ERRORS: OnceLock<Mutex<VecDeque<RecentError>>> = OnceLock::new();

pub async fn push(scope: impl Into<String>, message: impl Into<String>) {
    let mut errors = recent_errors().lock().await;
    if errors.len() >= MAX_RECENT_ERRORS {
        errors.pop_front();
    }
    errors.push_back(RecentError {
        at: Utc::now(),
        scope: scope.into(),
        message: message.into(),
    });
}

pub async fn snapshot() -> Vec<RecentError> {
    recent_errors().lock().await.iter().cloned().rev().collect()
}

fn recent_errors() -> &'static Mutex<VecDeque<RecentError>> {
    RECENT_ERRORS.get_or_init(|| Mutex::new(VecDeque::with_capacity(MAX_RECENT_ERRORS)))
}
