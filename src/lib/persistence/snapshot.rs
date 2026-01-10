use std::{
    env,
    ffi::OsString,
    path::{Path, PathBuf},
};

use bincode::{config::standard, Decode, Encode};
use tokio::fs;

use crate::message::{MessageBox, MessageStatus};

use super::config::PersistenceConfig;

#[allow(dead_code)]
const SNAPSHOT_MAGIC: &[u8; 4] = b"AMQS";
#[allow(dead_code)]
const SNAPSHOT_VERSION: u32 = 1;

#[derive(Debug, Clone, Decode, Encode)]
pub struct SnapshotData {
    pub messages: Vec<MessageBox>,
    pub next_message_id: u64,
    pub next_connection_id: u64,
    pub timestamp: u64,
}

#[derive(Clone)]
pub struct SnapshotManager {
    #[allow(dead_code)]
    config: PersistenceConfig,
    snapshot_dir: PathBuf,
}

impl SnapshotManager {
    pub async fn new(config: PersistenceConfig) -> Result<Self, Box<dyn std::error::Error>> {
        let home_dir = env::var_os("HOME")
            .or_else(|| env::var_os("USERPROFILE"))
            .unwrap_or(OsString::from("./"));
        let snapshot_dir = Path::new(&home_dir).join(".ahriknow/ahrimq/snapshot");
        fs::create_dir_all(&snapshot_dir).await?;

        Ok(Self {
            config,
            snapshot_dir,
        })
    }

    pub async fn create_snapshot(
        &self,
        messages: &[MessageBox],
        next_message_id: u64,
        next_connection_id: u64,
    ) -> Result<u64, Box<dyn std::error::Error>> {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let snapshot_data = SnapshotData {
            messages: messages
                .iter()
                .filter(|m| m.status != MessageStatus::Acked)
                .cloned()
                .collect(),
            next_message_id,
            next_connection_id,
            timestamp,
        };

        let config = standard()
            .with_variable_int_encoding()
            .with_little_endian();
        let encoded = bincode::encode_to_vec(&snapshot_data, config)?;

        let filename = format!("snapshot_{}.amq", timestamp);
        let path = self.snapshot_dir.join(filename);

        fs::write(&path, encoded).await?;

        self.cleanup_old_snapshots().await?;

        Ok(timestamp)
    }

    pub async fn load_latest_snapshot(&self) -> Result<Option<SnapshotData>, Box<dyn std::error::Error>> {
        let mut snapshots: Vec<(u64, PathBuf)> = Vec::new();

        let mut dir = fs::read_dir(&self.snapshot_dir).await?;
        while let Some(entry) = dir.next_entry().await? {
            let path = entry.path();
            if path.extension().map_or(false, |e| e == "amq") {
                if let Some(filename) = path.file_stem().and_then(|s| s.to_str()) {
                    if filename.starts_with("snapshot_") {
                        if let Ok(timestamp) = filename[9..].parse::<u64>() {
                            snapshots.push((timestamp, path));
                        }
                    }
                }
            }
        }

        if snapshots.is_empty() {
            return Ok(None);
        }

        snapshots.sort_by_key(|(ts, _)| *ts);
        let (_, latest_path) = snapshots.last().unwrap();

        let encoded = fs::read(latest_path).await?;
        let config = standard()
            .with_variable_int_encoding()
            .with_little_endian();
        let snapshot_data: SnapshotData = bincode::decode_from_slice(&encoded, config)?.0;

        Ok(Some(snapshot_data))
    }

    async fn cleanup_old_snapshots(&self) -> Result<(), Box<dyn std::error::Error>> {
        let mut snapshots: Vec<(u64, PathBuf)> = Vec::new();

        let mut dir = fs::read_dir(&self.snapshot_dir).await?;
        while let Some(entry) = dir.next_entry().await? {
            let path = entry.path();
            if path.extension().map_or(false, |e| e == "amq") {
                if let Some(filename) = path.file_stem().and_then(|s| s.to_str()) {
                    if filename.starts_with("snapshot_") {
                        if let Ok(timestamp) = filename[9..].parse::<u64>() {
                            snapshots.push((timestamp, path));
                        }
                    }
                }
            }
        }

        if snapshots.len() <= 3 {
            return Ok(());
        }

        snapshots.sort_by_key(|(ts, _)| *ts);
        snapshots.reverse();

        let to_remove = snapshots.into_iter().skip(3);

        for (_, path) in to_remove {
            let _ = fs::remove_file(path).await;
        }

        Ok(())
    }
}
