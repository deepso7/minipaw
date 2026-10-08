//! Human-readable numbers for the terminal UIs.

use std::time::Duration;

use minipaw::PeerId;

const UNITS: [&str; 5] = ["KiB", "MiB", "GiB", "TiB", "PiB"];

/// `512 B`, `1.0 KiB`, `4.1 MiB`, `2.3 GiB`: binary units, one decimal.
pub fn bytes(n: u64) -> String {
    if n < 1024 {
        return format!("{n} B");
    }
    // Precision loss past 2^53 bytes does not show at one decimal.
    #[allow(clippy::cast_precision_loss)]
    let mut value = n as f64 / 1024.0;
    let mut unit = 0;
    // Round first, so 1023.96 KiB shows as 1.0 MiB, not 1024.0 KiB.
    while (value * 10.0).round() / 10.0 >= 1024.0 && unit + 1 < UNITS.len() {
        value /= 1024.0;
        unit += 1;
    }
    format!("{value:.1} {}", UNITS[unit])
}

/// `4.1 MiB/s`, from bytes per second.
pub fn rate(bytes_per_sec: f64) -> String {
    // Saturating float-to-int cast; negative and NaN become 0.
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let n = bytes_per_sec.round() as u64;
    format!("{}/s", bytes(n))
}

/// `0:42`, `12:05`, `1:02:03`: minutes and seconds, with hours once there
/// are any. Fractions of a second are dropped.
pub fn duration(d: Duration) -> String {
    let secs = d.as_secs();
    let (h, m, s) = (secs / 3600, secs / 60 % 60, secs % 60);
    if h > 0 {
        format!("{h}:{m:02}:{s:02}")
    } else {
        format!("{m}:{s:02}")
    }
}

/// `12D3KooW…k3Fz`: the first 8 and last 4 characters of a peer id.
pub fn short_peer(peer: &PeerId) -> String {
    short_id(&peer.to_string())
}

/// [`short_peer`] for an id already in text form. Short ids are kept whole.
pub fn short_id(id: &str) -> String {
    let chars: Vec<char> = id.chars().collect();
    if chars.len() <= 13 {
        return id.to_owned();
    }
    let head: String = chars[..8].iter().collect();
    let tail: String = chars[chars.len() - 4..].iter().collect();
    format!("{head}…{tail}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn byte_counts() {
        assert_eq!(bytes(0), "0 B");
        assert_eq!(bytes(1023), "1023 B");
        assert_eq!(bytes(1024), "1.0 KiB");
        assert_eq!(bytes(1536), "1.5 KiB");
        assert_eq!(bytes(4_300_000), "4.1 MiB");
        assert_eq!(bytes(1024 * 1024 - 1), "1.0 MiB");
        assert_eq!(bytes(5 * 1024 * 1024 * 1024 / 2), "2.5 GiB");
        assert_eq!(bytes(3 << 40), "3.0 TiB");
        assert_eq!(bytes(u64::MAX), "16384.0 PiB");
    }

    #[test]
    fn rates() {
        assert_eq!(rate(0.0), "0 B/s");
        assert_eq!(rate(-5.0), "0 B/s");
        assert_eq!(rate(f64::NAN), "0 B/s");
        assert_eq!(rate(4_300_000.0), "4.1 MiB/s");
        assert_eq!(rate(999.6), "1000 B/s");
    }

    #[test]
    fn durations() {
        assert_eq!(duration(Duration::ZERO), "0:00");
        assert_eq!(duration(Duration::from_millis(42_900)), "0:42");
        assert_eq!(duration(Duration::from_secs(725)), "12:05");
        assert_eq!(duration(Duration::from_secs(3723)), "1:02:03");
        assert_eq!(duration(Duration::from_secs(100 * 3600)), "100:00:00");
    }

    #[test]
    fn peer_ids() {
        let peer: PeerId = "12D3KooWNAHhp6rp11SvCDA84zua3hhEYTLNjgKmEDmt1BddtLdf"
            .parse()
            .expect("peer id");
        assert_eq!(short_peer(&peer), "12D3KooW…tLdf");
        assert_eq!(short_id("short"), "short");
        assert_eq!(short_id("1234567890abc"), "1234567890abc");
        assert_eq!(short_id("1234567890abcd"), "12345678…abcd");
    }
}
