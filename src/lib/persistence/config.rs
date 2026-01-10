use std::time::Duration;

#[derive(Debug, Clone)]
pub struct PersistenceConfig {
    pub wal_max_size: usize,
    pub wal_rotation_count: usize,
    pub buffer_max_size: usize,
    pub buffer_max_entries: usize,
    pub flush_interval: Duration,
    pub sync_interval: Duration,
    pub snapshot_interval: Duration,
    pub snapshot_wal_threshold: usize,
    pub enable_compression: bool,
    pub compression_level: u32,
}

impl Default for PersistenceConfig {
    fn default() -> Self {
        Self {
            wal_max_size: 100 * 1024 * 1024,
            wal_rotation_count: 10,
            buffer_max_size: 1024 * 1024,
            buffer_max_entries: 1000,
            flush_interval: Duration::from_millis(100),
            sync_interval: Duration::from_secs(1),
            snapshot_interval: Duration::from_secs(300),
            snapshot_wal_threshold: 10000,
            enable_compression: true,
            compression_level: 3,
        }
    }
}
