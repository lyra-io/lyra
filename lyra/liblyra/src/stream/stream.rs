use super::cursor::EventStream;
use super::state_machine::StateMachine;
use crate::conn::conn_pool::ConnPool;
use crate::error::LyraError;
use crate::{Event as UserEvent, FetchOptions, Offset, StartPosition, StreamOptions};
use catalog::CatalogRef;
use std::sync::Arc;
use tracing::info;

struct StreamInner {
    stream_id: i64,
    stream_name: String,
    catalog: CatalogRef,
    pool: Arc<ConnPool>,
    state_machine: Option<StateMachine>,
}

#[derive(Clone)]
pub struct Stream {
    inner: Arc<StreamInner>,
}

impl Stream {
    pub async fn open(
        catalog: CatalogRef,
        pool: Arc<ConnPool>,
        name: &str,
        options: StreamOptions,
    ) -> Result<Self, LyraError> {
        let sm = StateMachine::start(name, catalog.clone(), pool.clone(), &options).await?;

        info!(
            stream_id = sm.stream_id(),
            stream = %name,
            "stream opened"
        );

        Ok(Self {
            inner: Arc::new(StreamInner {
                stream_id: sm.stream_id(),
                stream_name: name.to_string(),
                catalog,
                pool,
                state_machine: Some(sm),
            }),
        })
    }

    pub async fn open_readonly(
        catalog: CatalogRef,
        pool: Arc<ConnPool>,
        name: &str,
        _options: StreamOptions,
    ) -> Result<Self, LyraError> {
        let tc = catalog
            .get_stream(name)
            .await
            .map_err(|_| LyraError::StreamNotFound(name.to_string()))?;

        info!(
            stream_id = tc.stream_id,
            stream = %name,
            "stream opened (readonly)"
        );

        Ok(Self {
            inner: Arc::new(StreamInner {
                stream_id: tc.stream_id,
                stream_name: name.to_string(),
                catalog,
                pool,
                state_machine: None,
            }),
        })
    }

    pub fn stream_id(&self) -> i64 {
        self.inner.stream_id
    }

    pub async fn record(&self, event: UserEvent) -> Result<Offset, LyraError> {
        let sm = self
            .inner
            .state_machine
            .as_ref()
            .ok_or_else(|| LyraError::Internal("stream is read-only".into()))?;
        sm.record(event).await
    }

    pub async fn fetch(&self, options: FetchOptions) -> Result<EventStream, LyraError> {
        let start_offset = match options.start {
            StartPosition::Earliest => 1,
            StartPosition::Latest => {
                let tc = self
                    .inner
                    .catalog
                    .get_stream(&self.inner.stream_name)
                    .await
                    .map_err(|e| LyraError::Internal(format!("failed to get stream: {}", e)))?;
                tc.lra + 1
            }
            StartPosition::Offset(offset) => offset,
            StartPosition::Index { .. } => {
                return Err(LyraError::Internal(
                    "index-based fetch not yet implemented".into(),
                ));
            }
        };

        let mut stream = EventStream::new(
            self.inner.stream_id,
            self.inner.stream_name.clone(),
            self.inner.catalog.clone(),
            self.inner.pool.clone(),
            start_offset,
        );

        if let Some(limit) = options.limit {
            stream = stream.with_limit(limit);
        }
        if let Some(timeout) = options.timeout {
            stream = stream.with_timeout(timeout);
        }
        if matches!(options.start, StartPosition::Latest) {
            stream = stream.with_tail();
        }

        Ok(stream)
    }

    pub async fn close(&self) {
        if let Some(sm) = &self.inner.state_machine {
            sm.stop().await;
        }
        info!(stream_id = self.inner.stream_id, "stream closed");
    }
}
