use super::cursor::EventStream;
use super::state_machine::StateMachine;
use crate::conn::conn_pool::ConnPool;
use crate::error::ChronicleError;
use crate::{Event as UserEvent, FetchOptions, Offset, StartPosition, TimelineOptions};
use chronicle_catalog::CatalogRef;
use std::sync::Arc;
use tracing::info;

struct TimelineInner {
    timeline_id: i64,
    timeline_name: String,
    catalog: CatalogRef,
    pool: Arc<ConnPool>,
    state_machine: Option<StateMachine>,
}

#[derive(Clone)]
pub struct Timeline {
    inner: Arc<TimelineInner>,
}

impl Timeline {
    pub async fn open(
        catalog: CatalogRef,
        pool: Arc<ConnPool>,
        name: &str,
        options: TimelineOptions,
    ) -> Result<Self, ChronicleError> {
        let sm = StateMachine::start(name, catalog.clone(), pool.clone(), &options).await?;

        info!(
            timeline_id = sm.timeline_id(),
            timeline = %name,
            "timeline opened"
        );

        Ok(Self {
            inner: Arc::new(TimelineInner {
                timeline_id: sm.timeline_id(),
                timeline_name: name.to_string(),
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
        _options: TimelineOptions,
    ) -> Result<Self, ChronicleError> {
        let tc = catalog
            .get_timeline(name)
            .await
            .map_err(|_| ChronicleError::TimelineNotFound(name.to_string()))?;

        info!(
            timeline_id = tc.timeline_id,
            timeline = %name,
            "timeline opened (readonly)"
        );

        Ok(Self {
            inner: Arc::new(TimelineInner {
                timeline_id: tc.timeline_id,
                timeline_name: name.to_string(),
                catalog,
                pool,
                state_machine: None,
            }),
        })
    }

    pub fn timeline_id(&self) -> i64 {
        self.inner.timeline_id
    }

    pub async fn record(&self, event: UserEvent) -> Result<Offset, ChronicleError> {
        let sm = self
            .inner
            .state_machine
            .as_ref()
            .ok_or_else(|| ChronicleError::Internal("timeline is read-only".into()))?;
        sm.record(event).await
    }

    pub async fn fetch(&self, options: FetchOptions) -> Result<EventStream, ChronicleError> {
        let start_offset = match options.start {
            StartPosition::Earliest => 0,
            StartPosition::Latest => {
                let tc = self
                    .inner
                    .catalog
                    .get_timeline(&self.inner.timeline_name)
                    .await
                    .map_err(|e| {
                        ChronicleError::Internal(format!("failed to get timeline: {}", e))
                    })?;
                tc.lra + 1
            }
            StartPosition::Offset(offset) => offset,
            StartPosition::Index { .. } => {
                return Err(ChronicleError::Internal(
                    "index-based fetch not yet implemented".into(),
                ));
            }
        };

        let mut stream = EventStream::new(
            self.inner.timeline_id,
            self.inner.timeline_name.clone(),
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
        info!(timeline_id = self.inner.timeline_id, "timeline closed");
    }
}
