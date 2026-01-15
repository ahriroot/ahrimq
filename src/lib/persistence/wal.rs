use std::{
    env,
    error::Error,
    ffi::OsString,
    fs::{File, OpenOptions},
    io::{BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

use bincode::{Decode, Encode};
use tokio::{
    fs,
    sync::{mpsc, RwLock, Semaphore},
};

use crate::message::{MessageBox, MessageHistory, MessageStatus};

use super::config::PersistenceConfig;

const WAL_MAGIC: &[u8; 4] = b"AMQW";
const WAL_VERSION: u32 = 1;

#[derive(Debug, Clone, Decode, Encode)]
pub enum WalEntry {
    AddMessage(MessageBox),
    UpdateStatus { id: u64, status: MessageStatus },
    AddHistory { id: u64, history: MessageHistory },
    RemoveMessage(u64),
    UpdateTimestamp { id: u64, timestamp: u64 },
}

impl WalEntry {
    pub fn serialize(&self) -> Result<Vec<u8>, Box<dyn Error>> {
        let config = bincode::config::standard()
            .with_variable_int_encoding()
            .with_little_endian();
        Ok(bincode::encode_to_vec(self, config)?)
    }

    pub fn deserialize(bytes: &[u8]) -> Result<Self, Box<dyn Error>> {
        let config = bincode::config::standard()
            .with_variable_int_encoding()
            .with_little_endian();
        Ok(bincode::decode_from_slice(bytes, config)?.0)
    }
}

#[derive(Clone)]
pub struct WalManager {
    config: PersistenceConfig,
    wal_dir: PathBuf,
    current_wal: Arc<RwLock<Option<WalFile>>>,
    entry_sender: mpsc::Sender<WalEntry>,
    write_semaphore: Arc<Semaphore>,
}

struct WalFile {
    #[allow(dead_code)]
    path: PathBuf,
    writer: BufWriter<File>,
    size: usize,
    entry_count: usize,
}

impl WalManager {
    pub async fn new(config: PersistenceConfig) -> Result<Self, Box<dyn Error>> {
        let home_dir = env::var_os("HOME")
            .or_else(|| env::var_os("USERPROFILE"))
            .unwrap_or(OsString::from("./"));
        let wal_dir = Path::new(&home_dir).join(".ahriknow/ahrimq/wal");
        fs::create_dir_all(&wal_dir).await?;

        let (entry_sender, entry_receiver) = mpsc::channel(config.buffer_max_entries);
        let write_semaphore = Arc::new(Semaphore::new(1));

        let manager = Self {
            config: config.clone(),
            wal_dir,
            current_wal: Arc::new(RwLock::new(None)),
            entry_sender,
            write_semaphore,
        };

        manager.start_background_writer(entry_receiver).await?;

        Ok(manager)
    }

    async fn start_background_writer(
        &self,
        mut entry_receiver: mpsc::Receiver<WalEntry>,
    ) -> Result<(), Box<dyn Error>> {
        let config = self.config.clone();
        let wal_dir = self.wal_dir.clone();
        let current_wal = Arc::clone(&self.current_wal);
        let write_semaphore = Arc::clone(&self.write_semaphore);

        tokio::spawn(async move {
            let mut buffer = Vec::with_capacity(config.buffer_max_entries);
            let mut buffer_size = 0;
            let mut last_flush = tokio::time::Instant::now();

            while let Some(entry) = entry_receiver.recv().await {
                let serialized = match entry.serialize() {
                    Ok(data) => data,
                    Err(e) => {
                        eprintln!("Failed to serialize WAL entry: {}", e);
                        continue;
                    }
                };

                let entry_size = serialized.len();
                if buffer_size + entry_size > config.buffer_max_size
                    || buffer.len() >= config.buffer_max_entries
                {
                    Self::flush_entries(
                        &wal_dir,
                        &current_wal,
                        &write_semaphore,
                        &config,
                        &mut buffer,
                        &mut buffer_size,
                    )
                    .await;
                    last_flush = tokio::time::Instant::now();
                }

                buffer.push(entry);
                buffer_size += entry_size;

                if last_flush.elapsed() >= config.flush_interval {
                    Self::flush_entries(
                        &wal_dir,
                        &current_wal,
                        &write_semaphore,
                        &config,
                        &mut buffer,
                        &mut buffer_size,
                    )
                    .await;
                    last_flush = tokio::time::Instant::now();
                }
            }

            if !buffer.is_empty() {
                Self::flush_entries(
                    &wal_dir,
                    &current_wal,
                    &write_semaphore,
                    &config,
                    &mut buffer,
                    &mut buffer_size,
                )
                .await;
            }
        });

        Ok(())
    }

    async fn flush_entries(
        wal_dir: &Path,
        current_wal: &Arc<RwLock<Option<WalFile>>>,
        write_semaphore: &Arc<Semaphore>,
        config: &PersistenceConfig,
        buffer: &mut Vec<WalEntry>,
        buffer_size: &mut usize,
    ) {
        if buffer.is_empty() {
            return;
        }

        let _permit = write_semaphore.acquire().await.unwrap();

        let entries: Vec<WalEntry> = buffer.drain(..).collect();
        *buffer_size = 0;

        let mut wal_guard = current_wal.write().await;

        if wal_guard.is_none() || wal_guard.as_ref().unwrap().size > config.wal_max_size {
            if let Some(mut old_wal) = wal_guard.take() {
                let _ = old_wal.writer.flush();
            }
            *wal_guard = Self::create_new_wal(wal_dir).await;
        }

        if let Some(wal) = wal_guard.as_mut() {
            for entry in &entries {
                if let Ok(data) = entry.serialize() {
                    let len = data.len() as u32;
                    if wal.writer.write_all(&len.to_le_bytes()).is_ok()
                        && wal.writer.write_all(&data).is_ok()
                    {
                        wal.size += 4 + data.len();
                        wal.entry_count += 1;
                    }
                }
            }
            let _ = wal.writer.flush();
        }
    }

    async fn create_new_wal(wal_dir: &Path) -> Option<WalFile> {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let filename = format!("wal_{}.amq", timestamp);
        let path = wal_dir.join(filename);

        let file = match OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&path)
        {
            Ok(f) => f,
            Err(e) => {
                eprintln!("Failed to create WAL file: {}", e);
                return None;
            }
        };

        let mut writer = BufWriter::new(file);

        if writer.write_all(WAL_MAGIC).is_ok()
            && writer.write_all(&WAL_VERSION.to_le_bytes()).is_ok()
        {
            Some(WalFile {
                path,
                writer,
                size: 8,
                entry_count: 0,
            })
        } else {
            None
        }
    }

    pub async fn append(&self, entry: WalEntry) -> Result<(), Box<dyn Error>> {
        self.entry_sender.send(entry).await?;
        Ok(())
    }

    pub async fn append_batch(&self, entries: Vec<WalEntry>) -> Result<(), Box<dyn Error>> {
        for entry in entries {
            self.entry_sender.send(entry).await?;
        }
        Ok(())
    }

    pub async fn recover(&self) -> Result<Vec<WalEntry>, Box<dyn Error>> {
        let mut entries = Vec::new();
        let mut wal_files: Vec<(u64, PathBuf)> = Vec::new();

        let mut dir = fs::read_dir(&self.wal_dir).await?;
        while let Some(entry) = dir.next_entry().await? {
            let path = entry.path();
            if path.extension().map_or(false, |e| e == "amq") {
                if let Some(filename) = path.file_stem().and_then(|s| s.to_str()) {
                    if filename.starts_with("wal_") {
                        if let Ok(timestamp) = filename[4..].parse::<u64>() {
                            wal_files.push((timestamp, path));
                        }
                    }
                }
            }
        }

        wal_files.sort_by_key(|(ts, _)| *ts);

        for (_, path) in wal_files {
            if let Ok(file_entries) = Self::read_wal_file(&path).await {
                entries.extend(file_entries);
            }
        }

        Ok(entries)
    }

    async fn read_wal_file(path: &Path) -> Result<Vec<WalEntry>, Box<dyn Error>> {
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);

        let mut magic = [0u8; 4];
        reader.read_exact(&mut magic)?;
        if &magic != WAL_MAGIC {
            return Ok(Vec::new());
        }

        let mut version_bytes = [0u8; 4];
        reader.read_exact(&mut version_bytes)?;
        let _version = u32::from_le_bytes(version_bytes);

        let mut entries = Vec::new();

        loop {
            let mut len_bytes = [0u8; 4];
            match reader.read_exact(&mut len_bytes) {
                Ok(_) => {}
                Err(_) if entries.is_empty() => return Ok(entries),
                Err(_) => break,
            }

            let len = u32::from_le_bytes(len_bytes) as usize;
            let mut data = vec![0u8; len];
            if reader.read_exact(&mut data).is_err() {
                break;
            }

            if let Ok(entry) = WalEntry::deserialize(&data) {
                entries.push(entry);
            }
        }

        Ok(entries)
    }

    pub async fn cleanup_old_wals(&self, snapshot_timestamp: u64) -> Result<(), Box<dyn Error>> {
        let mut dir = fs::read_dir(&self.wal_dir).await?;
        let mut wal_files: Vec<(u64, PathBuf)> = Vec::new();

        while let Some(entry) = dir.next_entry().await? {
            let path = entry.path();
            if path.extension().map_or(false, |e| e == "amq") {
                if let Some(filename) = path.file_stem().and_then(|s| s.to_str()) {
                    if filename.starts_with("wal_") {
                        if let Ok(timestamp) = filename[4..].parse::<u64>() {
                            wal_files.push((timestamp, path));
                        }
                    }
                }
            }
        }

        wal_files.sort_by_key(|(ts, _)| *ts);

        let to_remove = wal_files
            .into_iter()
            .filter(|(ts, _)| *ts < snapshot_timestamp)
            .take(self.config.wal_rotation_count);

        for (_, path) in to_remove {
            let _ = fs::remove_file(path).await;
        }

        Ok(())
    }

    pub async fn get_entry_count(&self) -> usize {
        let wal_guard = self.current_wal.read().await;
        wal_guard.as_ref().map(|w| w.entry_count).unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wal_entry_serialize() {
        let entry = WalEntry::AddMessage(MessageBox {
            id: 1,
            status: MessageStatus::New,
            timestamp: 0,
            message: crate::message::Message::ReqPing(crate::message::ReqMsgPing {}),
            history: vec![],
        });

        let serialized = entry.serialize().unwrap();
        let deserialized = WalEntry::deserialize(&serialized).unwrap();

        match deserialized {
            WalEntry::AddMessage(msg) => {
                assert_eq!(msg.id, 1);
            }
            _ => panic!("Unexpected entry type"),
        }
    }
}
