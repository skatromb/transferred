//! Tracks prepared INSERT identities across the two forwarding tasks.

#![expect(
    clippy::little_endian_bytes,
    reason = "MySQL protocol integers are little endian"
)]

use std::collections::BTreeSet;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

#[derive(Default)]
pub(crate) struct WireState {
    intercept: AtomicBool,
    inserts: AtomicUsize,
    parameters: AtomicUsize,
    preparing_insert: AtomicBool,
    statements: Mutex<BTreeSet<u32>>,
}

impl WireState {
    pub(crate) const fn intercept(&self) -> &AtomicBool {
        &self.intercept
    }

    pub(crate) fn insert_commands(&self) -> usize {
        self.inserts.load(Ordering::SeqCst)
    }

    pub(crate) fn maximum_parameters(&self) -> usize {
        self.parameters.load(Ordering::SeqCst)
    }

    pub(crate) fn command(&self, packet: &[u8]) -> bool {
        if packet[3] != 0 {
            return false;
        }
        if packet[4] == 22 {
            self.preparing_insert
                .store(packet[5..].starts_with(b"INSERT INTO "), Ordering::SeqCst);
        }
        if packet[4] == 23
            && self
                .statements
                .lock()
                .unwrap()
                .contains(&identifier(packet))
        {
            _ = self.inserts.fetch_add(1, Ordering::SeqCst);
            return true;
        }
        false
    }

    pub(crate) fn response(&self, packet: &[u8]) {
        if packet[3] == 1 && self.preparing_insert.swap(false, Ordering::SeqCst) && packet[4] == 0 {
            _ = self.statements.lock().unwrap().insert(identifier(packet));
            let count = u16::from_le_bytes([packet[11], packet[12]]);
            _ = self
                .parameters
                .fetch_max(usize::from(count), Ordering::SeqCst);
        }
    }
}

const fn identifier(packet: &[u8]) -> u32 {
    u32::from_le_bytes([packet[5], packet[6], packet[7], packet[8]])
}
