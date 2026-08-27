//! Stateful write-ahead log façade.

use super::error::WalError;
use super::log_syncer::LogSyncer;
use super::log_writer::LogWriter;
use super::options::LogOptions;
use super::{Log, Sequence};
use crate::vfs::{StandardVfs, VfsI};
use async_trait::async_trait;
use bytes::Bytes;
use meta::utils::promise::Promise;
use std::sync::Arc;

/// A buffered write-ahead log backed by Lyra's segment format.
pub struct SegmentLog {
    // Control state
    writer: LogWriter,
    syncer: LogSyncer,
}

impl SegmentLog {
    /// Opens the WAL at `options.dir`.
    pub async fn open(options: LogOptions) -> Result<Arc<Self>, WalError> {
        let vfs = VfsI::Standard(StandardVfs);
        let (syncer, sync_handle) = LogSyncer::new(vfs.clone(), &options)?;
        let writer = match LogWriter::new(vfs, options, sync_handle).await {
            Ok(writer) => writer,
            Err(error) => {
                syncer.close().await;
                return Err(error);
            }
        };
        Ok(Arc::new(Self {
            // Control state
            writer,
            syncer,
        }))
    }
}

#[async_trait]
impl Log for SegmentLog {
    fn append(&self, payload: Bytes) -> Promise<Sequence, WalError> {
        self.writer.append(payload)
    }

    async fn close(&self) {
        self.writer.close().await;
        self.syncer.close().await;
    }
}
