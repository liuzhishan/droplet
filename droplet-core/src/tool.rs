#![feature(portable_simd)]
use std::arch::x86_64::*;
use std::simd::prelude::*;

use anyhow::{bail, Result};
use likely_stable::{likely, unlikely};
use log::{error, info};
use std::{io::Write, sync::Once};

use tokio::signal::unix::{signal, SignalKind};

/// Message limit for gRPC.
pub const MESSAGE_LIMIT: usize = 20 * 1024 * 1024;

/// Init log. Set log format.
pub fn init_log() {
    env_logger::builder()
        .format(|buf, record| {
            writeln!(
                buf,
                "{} [{}] {}:{} - {}",
                chrono::Local::now().format("%Y-%m-%dT%H:%M:%S"),
                record.level(),
                record.file().unwrap_or("unknown"),
                record.line().unwrap_or(0),
                record.args()
            )
        })
        .filter_level(log::LevelFilter::Info)
        .init();
}

static INIT_LOG: Once = Once::new();

/// Setup.
pub fn setup_log() {
    INIT_LOG.call_once(|| {
        init_log();
    });
}

#[macro_export]
macro_rules! error_bail {
    ($msg:literal $(,)?) => {
        error!($msg);
        bail!($msg)
    };
    ($err:expr $(,)?) => {
        error!($err);
        bail!(err)
    };
    ($fmt:expr, $($arg:tt)*) => {
        error!($fmt, $($arg)*);
        bail!($fmt, $($arg)*)
    };
}

/// Check if two keys are equal, using SIMD to speed up.
#[inline]
pub fn is_keys_equal(a: &[u32], b: &[u32]) -> bool {
    if unlikely(a.len() != b.len()) {
        return false;
    }

    for pos in (0..a.len()).step_by(4) {
        let a_simd = Simd::<u32, 4>::from_slice(&a[pos..pos + 4]);
        let b_simd = Simd::<u32, 4>::from_slice(&b[pos..pos + 4]);
        if !a_simd.eq(&b_simd) {
            return false;
        }
    }

    let n = a.len();
    if n % 4 != 0 {
        for i in (n - n % 4)..n {
            if a[i] != b[i] {
                return false;
            }
        }
    }

    true
}

async fn wait_for_signal_impl() {
    // Infos here:
    // https://www.gnu.org/software/libc/manual/html_node/Termination-Signals.html
    let mut signal_terminate = signal(SignalKind::terminate()).unwrap();
    let mut signal_interrupt = signal(SignalKind::interrupt()).unwrap();

    tokio::select! {
        _ = signal_terminate.recv() => {
            info!("Received SIGTERM.");
        }
        _ = signal_interrupt.recv() => {
            info!("Received SIGINT.");
        }
    };
}

pub async fn wait_for_signal() {
    wait_for_signal_impl().await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_keys_equal() {
        // Test equal keys
        let a = vec![1, 2, 3, 4, 5, 6, 7, 8];
        let b = vec![1, 2, 3, 4, 5, 6, 7, 8];
        assert!(is_keys_equal(&a, &b));

        // Test unequal keys
        let c = vec![1, 2, 3, 4, 5, 6, 7, 9];
        assert!(!is_keys_equal(&a, &c));

        // Test keys with different lengths
        let d = vec![1, 2, 3, 4, 5, 6, 7];
        assert!(!is_keys_equal(&a, &d));

        // Test empty keys
        let e: Vec<u32> = vec![];
        let f: Vec<u32> = vec![];
        assert!(is_keys_equal(&e, &f));

        // Test keys with length not divisible by 4
        let g = vec![1, 2, 3, 4, 5, 6, 7];
        let h = vec![1, 2, 3, 4, 5, 6, 7];
        assert!(is_keys_equal(&g, &h));

        // Test keys with length not divisible by 4 and unequal
        let i = vec![1, 2, 3, 4, 5, 6, 8];
        assert!(!is_keys_equal(&g, &i));

        // Test large keys
        let j: Vec<u32> = (0..1000).collect();
        let k: Vec<u32> = (0..1000).collect();
        assert!(is_keys_equal(&j, &k));

        // Test large unequal keys
        let mut l: Vec<u32> = (0..1000).collect();
        l[999] = 1001;
        assert!(!is_keys_equal(&j, &l));
    }
}
