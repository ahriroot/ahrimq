use tokio::{sync::mpsc, time::interval};

use crate::{
    message::{MessageBox, MessageStatus},
    persistence::{
        config::PersistenceConfig, snapshot::SnapshotManager, wal::WalEntry, WalManager,
    },
};

#[derive(Clone)]
pub struct PersistenceEngine {
    #[allow(dead_code)]
    config: PersistenceConfig,
    wal_manager: WalManager,
    snapshot_manager: SnapshotManager,
    #[allow(dead_code)]
    snapshot_trigger: mpsc::Sender<()>,
}

impl PersistenceEngine {
    pub async fn new(config: PersistenceConfig) -> Result<Self, Box<dyn std::error::Error>> {
        let wal_manager = WalManager::new(config.clone()).await?;
        let snapshot_manager = SnapshotManager::new(config.clone()).await?;

        let (snapshot_trigger, mut snapshot_receiver) = mpsc::channel::<()>(1);

        let engine = Self {
            config: config.clone(),
            wal_manager,
            snapshot_manager,
            snapshot_trigger: snapshot_trigger.clone(),
        };

        let _snapshot_manager = engine.snapshot_manager.clone();
        let wal_manager = engine.wal_manager.clone();
        let config = config.clone();

        tokio::spawn(async move {
            let mut snapshot_timer = interval(config.snapshot_interval);
            snapshot_timer.tick().await;

            loop {
                tokio::select! {
                    _ = snapshot_timer.tick() => {
                        let count = wal_manager.get_entry_count().await;
                        if count >= config.snapshot_wal_threshold {
                            let _ = snapshot_trigger.send(()).await;
                        }
                    }
                    Some(_) = snapshot_receiver.recv() => {
                    }
                }
            }
        });

        Ok(engine)
    }

    pub async fn append_message(&self, message: &MessageBox) -> Result<(), Box<dyn std::error::Error>> {
        self.wal_manager
            .append(WalEntry::AddMessage(message.clone()))
            .await
    }

    pub async fn update_message_status(
        &self,
        id: u64,
        status: MessageStatus,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.wal_manager
            .append(WalEntry::UpdateStatus { id, status })
            .await
    }

    pub async fn remove_message(&self, id: u64) -> Result<(), Box<dyn std::error::Error>> {
        self.wal_manager.append(WalEntry::RemoveMessage(id)).await
    }

    pub async fn create_snapshot(
        &self,
        messages: &[MessageBox],
        next_message_id: u64,
        next_connection_id: u64,
    ) -> Result<u64, Box<dyn std::error::Error>> {
        let timestamp = self
            .snapshot_manager
            .create_snapshot(messages, next_message_id, next_connection_id)
            .await?;

        self.wal_manager.cleanup_old_wals(timestamp).await?;

        Ok(timestamp)
    }

    pub async fn recover(
        &self,
    ) -> Result<(Vec<MessageBox>, u64, u64), Box<dyn std::error::Error>> {
        let mut messages = Vec::new();
        let mut next_message_id = 0;
        let mut next_connection_id = 0;

        if let Some(snapshot) = self.snapshot_manager.load_latest_snapshot().await? {
            messages = snapshot.messages;
            next_message_id = snapshot.next_message_id;
            next_connection_id = snapshot.next_connection_id;
        }

        let wal_entries = self.wal_manager.recover().await?;

        for entry in wal_entries {
            match entry {
                WalEntry::AddMessage(msg) => {
                    messages.push(msg);
                }
                WalEntry::UpdateStatus { id, status } => {
                    if let Some(msg) = messages.iter_mut().find(|m| m.id == id) {
                        msg.status = status;
                    }
                }
                WalEntry::AddHistory { id, history } => {
                    if let Some(msg) = messages.iter_mut().find(|m| m.id == id) {
                        msg.history.push(history);
                    }
                }
                WalEntry::RemoveMessage(id) => {
                    messages.retain(|m| m.id != id);
                }
            }
        }

        Ok((messages, next_message_id, next_connection_id))
    }

    pub async fn shutdown(&self) -> Result<(), Box<dyn std::error::Error>> {
        Ok(())
    }
}
