use crate::conn::ConnOptions;
use crate::conn::conn_pool::ConnPool;
use crate::error::LyraError;
use crate::stream::Stream;
use crate::xunit::{AppendRowsRequest, RowData, ScanRequest, XunitClient};
use crate::{Event, Offset, StreamOptions};
use meta::{MetadataRef, Dataset, OffsetRange, Versioned};
use opentelemetry::metrics::Meter;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

#[derive(Default)]
pub struct LyraOptions {
    pub(crate) conn_opts: ConnOptions,
    _meter: Option<Meter>,
}

impl LyraOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn conns_per_unit(mut self, n: usize) -> Self {
        self.conn_opts.conns_per_unit = n.max(1);
        self
    }

    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.conn_opts.connect_timeout = timeout;
        self
    }

    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.conn_opts.request_timeout = timeout;
        self
    }

    pub fn keep_alive_interval(mut self, interval: Duration) -> Self {
        self.conn_opts.keep_alive_interval = interval;
        self
    }

    pub fn keep_alive_timeout(mut self, timeout: Duration) -> Self {
        self.conn_opts.keep_alive_timeout = timeout;
        self
    }

    pub fn meter(mut self, meter: Meter) -> Self {
        self._meter = Some(meter);
        self
    }
}

pub struct Lyra {
    metadata: MetadataRef,
    pool: Arc<ConnPool>,
    xunit: Option<Arc<dyn XunitClient>>,
}

impl Lyra {
    pub fn new(metadata: MetadataRef, options: LyraOptions) -> Self {
        Self {
            pool: Arc::new(ConnPool::new(options.conn_opts.clone())),
            metadata,
            xunit: None,
        }
    }

    pub fn with_xunit(
        metadata: MetadataRef,
        options: LyraOptions,
        xunit: Arc<dyn XunitClient>,
    ) -> Self {
        Self {
            pool: Arc::new(ConnPool::new(options.conn_opts.clone())),
            metadata,
            xunit: Some(xunit),
        }
    }

    pub async fn create_dataset(&self, dataset: Dataset) -> Result<Versioned<Dataset>, LyraError> {
        Ok(self.metadata.create_dataset(dataset).await?)
    }

    pub async fn open_dataset(&self, name: &str) -> Result<DatasetClient, LyraError> {
        let dataset = self.metadata.get_dataset(name).await?;
        let xunit = self.xunit.clone().ok_or_else(|| {
            LyraError::Internal("xunit client is required for dataset read/write in the MVP".into())
        })?;
        let next_offset = next_dataset_offset(xunit.as_ref(), &dataset.value.name).await?;

        Ok(DatasetClient {
            dataset,
            xunit,
            partition: "default".to_string(),
            next_offset: AtomicI64::new(next_offset),
        })
    }

    pub async fn open_stream(
        &self,
        name: &str,
        options: StreamOptions,
    ) -> Result<Stream, LyraError> {
        Stream::open(self.metadata.clone(), self.pool.clone(), name, options).await
    }

    pub async fn open_readonly_stream(
        &self,
        name: &str,
        options: StreamOptions,
    ) -> Result<Stream, LyraError> {
        Stream::open_readonly(self.metadata.clone(), self.pool.clone(), name, options).await
    }

    pub async fn drop_stream(&self, name: &str) -> Result<(), LyraError> {
        let tc = self.metadata.get_stream(name).await?;
        self.metadata.delete_stream(name, tc.version).await?;
        Ok(())
    }
}

pub struct DatasetClient {
    dataset: Versioned<Dataset>,
    xunit: Arc<dyn XunitClient>,
    partition: String,
    next_offset: AtomicI64,
}

impl DatasetClient {
    pub fn dataset(&self) -> &Dataset {
        &self.dataset.value
    }

    pub async fn write(&self, event: Event) -> Result<Offset, LyraError> {
        let offset = self.next_offset.fetch_add(1, Ordering::SeqCst);
        let response = self
            .xunit
            .append_rows(AppendRowsRequest::new(
                self.dataset.value.name.clone(),
                self.partition.clone(),
                self.dataset.value.schema.version,
                OffsetRange::new(offset, offset + 1),
                vec![RowData::new(offset, event.payload)],
            ))
            .await?;
        Ok(Offset(response.committed_offset))
    }

    pub async fn write_payload(&self, payload: impl Into<Vec<u8>>) -> Result<Offset, LyraError> {
        self.write(Event::new(payload.into())).await
    }

    pub async fn read_all(&self) -> Result<Vec<Event>, LyraError> {
        let response = self
            .xunit
            .scan(ScanRequest::all(self.dataset.value.name.clone()))
            .await?;
        Ok(response
            .batches
            .into_iter()
            .flat_map(|batch| batch.rows)
            .map(|row| Event {
                offset: Some(row.offset),
                timestamp: None,
                payload: row.payload,
                key: None,
                txn_id: None,
            })
            .collect())
    }
}

async fn next_dataset_offset(xunit: &dyn XunitClient, dataset: &str) -> Result<i64, LyraError> {
    let response = xunit.scan(ScanRequest::all(dataset)).await?;
    Ok(response
        .batches
        .iter()
        .flat_map(|batch| batch.rows.iter())
        .map(|row| row.offset)
        .max()
        .map(|offset| offset + 1)
        .unwrap_or(0))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::xunit::{AppendRowsResponse, RowBatch, ScanResponse, error::XunitClientError};
    use meta::{
        Action, ActionRequest, DataType, DatasetField, DatasetSchema, build_memory_metadata,
    };
    use std::sync::Mutex;

    #[derive(Default)]
    struct TestXunit {
        rows: Mutex<Vec<RowData>>,
    }

    #[async_trait::async_trait]
    impl XunitClient for TestXunit {
        async fn append_rows(
            &self,
            request: AppendRowsRequest,
        ) -> Result<AppendRowsResponse, XunitClientError> {
            self.rows.lock().unwrap().extend(request.rows);
            Ok(AppendRowsResponse {
                committed_offset: request.offset_range.end - 1,
            })
        }

        async fn scan(&self, _request: ScanRequest) -> Result<ScanResponse, XunitClientError> {
            let rows = self.rows.lock().unwrap().clone();
            Ok(ScanResponse {
                batches: vec![RowBatch { schema_id: 1, rows }],
            })
        }

        async fn submit_action(
            &self,
            _request: ActionRequest,
        ) -> Result<Versioned<Action>, XunitClientError> {
            Err(XunitClientError::Internal(
                "test xunit does not submit actions".into(),
            ))
        }
    }

    #[tokio::test]
    async fn dataset_create_write_and_read_round_trip() {
        let metadata = build_memory_metadata();
        let xunit = Arc::new(TestXunit::default());
        let lyra = Lyra::with_xunit(metadata, LyraOptions::new(), xunit);

        lyra.create_dataset(Dataset::new(
            "events",
            DatasetSchema::new(vec![DatasetField::new("payload", DataType::Json)]),
        ))
        .await
        .unwrap();

        let dataset = lyra.open_dataset("events").await.unwrap();
        let offset = dataset
            .write_payload(br#"{"id":1}"#.to_vec())
            .await
            .unwrap();
        assert_eq!(offset, Offset(0));

        let rows = dataset.read_all().await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].offset, Some(0));
        assert_eq!(rows[0].payload, br#"{"id":1}"#);
    }
}
