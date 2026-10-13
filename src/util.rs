//! Shared utilities used across modules.

use std::time::Duration;

/// Apply ±25% jitter to a duration for exponential backoff.
/// Prevents thundering-herd when multiple instances retry in lockstep after
/// a coordinated failure (e.g., PG or SR outage affecting all pipelines).
/// Uses fastrand for cheap non-cryptographic randomness (no syscall overhead).
pub fn jitter_duration(d: Duration) -> Duration {
    let secs = d.as_secs_f64();
    if secs <= 0.0 {
        return d;
    }
    // 0.75 .. 1.25
    let jitter = 0.75 + fastrand::f64() * 0.5;
    Duration::from_secs_f64(secs * jitter)
}

/// Aborts a tokio JoinHandle on drop. Prevents zombie background tasks
/// (e.g. PG connection handlers) from being silently detached.
pub(crate) struct AbortOnDrop(pub(crate) tokio::task::JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Format seconds as "1d 2h 3m 4s" — shared between the /health HTTP endpoint
/// and the periodic HEALTH SUMMARY log. Consolidated from two divergent copies
/// (main.rs used years/months; metrics_server.rs used days/hours).
pub(crate) fn format_duration(total_secs: u64) -> String {
    let days = total_secs / 86400;
    let hours = (total_secs % 86400) / 3600;
    let mins = (total_secs % 3600) / 60;
    let secs = total_secs % 60;
    let parts: Vec<String> = [
        (days, "d"),
        (hours, "h"),
        (mins, "m"),
        (secs, "s"),
    ]
    .iter()
    .filter(|(v, _)| *v > 0)
    .map(|(v, u)| format!("{v}{u}"))
    .collect();
    if parts.is_empty() {
        "0s".to_string()
    } else {
        parts.join(" ")
    }
}

/// Safely truncate a string to at most `max_chars` characters, avoiding
/// panics on multi-byte UTF-8 boundaries.  Returns the full string if it
/// fits, or a truncated copy otherwise.
#[allow(dead_code)]
pub(crate) fn truncate_str(s: &str, max_chars: usize) -> String {
    if s.chars().count() <= max_chars {
        s.to_string()
    } else {
        s.chars().take(max_chars).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_truncate_str_short_returns_full() {
        assert_eq!(truncate_str("hello", 10), "hello");
    }

    #[test]
    fn test_truncate_str_long_truncates() {
        assert_eq!(truncate_str("hello world", 5), "hello");
    }

    #[test]
    fn test_truncate_str_multibyte_safe() {
        let s = "hélloñ中world";
        let result = truncate_str(s, 5);
        assert_eq!(result.chars().count(), 5);
        assert_eq!(result, "héllo");
    }

    #[test]
    fn test_truncate_str_empty() {
        assert_eq!(truncate_str("", 5), "");
    }

    #[test]
    fn test_truncate_str_zero_chars() {
        assert_eq!(truncate_str("hello", 0), "");
    }

    // ── jitter_duration ──

    #[test]
    fn test_jitter_duration_zero_is_unchanged() {
        let d = jitter_duration(Duration::ZERO);
        assert_eq!(d, Duration::ZERO);
    }

    #[test]
    fn test_jitter_duration_within_range() {
        // Run 1000 samples and verify each is within ±25% of the input
        let input = Duration::from_secs(10);
        for _ in 0..1000 {
            let j = jitter_duration(input);
            let secs = j.as_secs_f64();
            assert!(
                (7.5..=12.5).contains(&secs),
                "jitter {secs}s out of [7.5, 12.5] range for 10s input"
            );
        }
    }

    #[test]
    fn test_jitter_duration_minimum_positive() {
        // Very small durations should stay positive
        let input = Duration::from_nanos(1);
        let j = jitter_duration(input);
        assert!(j.as_nanos() > 0, "jitter must remain positive");
    }

    // ── format_duration ──

    #[test]
    fn test_format_duration_zero() {
        assert_eq!(format_duration(0), "0s");
    }

    #[test]
    fn test_format_duration_seconds_only() {
        assert_eq!(format_duration(45), "45s");
    }

    #[test]
    fn test_format_duration_minutes() {
        assert_eq!(format_duration(125), "2m 5s");
    }

    #[test]
    fn test_format_duration_hours() {
        assert_eq!(format_duration(3661), "1h 1m 1s");
    }

    #[test]
    fn test_format_duration_days() {
        assert_eq!(format_duration(90061), "1d 1h 1m 1s");
    }

    #[test]
    fn test_format_duration_weeks_as_days() {
        assert_eq!(format_duration(604800), "7d");
    }
}
