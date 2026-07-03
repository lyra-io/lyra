use crate::{Lens, LensOutput};
use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::sql::server::{FlightSqlService, PeekableFlightDataStream};
use arrow_flight::sql::{
    CommandStatementQuery, CommandStatementUpdate, ProstMessageExt, SqlInfo, TicketStatementQuery,
};
use arrow_flight::utils::batches_to_flight_data;
use arrow_flight::{FlightData, FlightDescriptor, FlightEndpoint, FlightInfo, Ticket};
use arrow_schema::{DataType as ArrowDataType, Field, Schema, SchemaRef};
use futures_util::{Stream, stream};
use prost::Message;
use std::collections::HashMap;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::Mutex;
use tonic::{Request, Response, Status};
use tracing::info;

type FlightDataStream = Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send + 'static>>;

#[derive(Clone)]
pub struct LensFlightSqlService {
    lens: Arc<Lens>,
    next_handle: Arc<AtomicU64>,
    results: Arc<Mutex<HashMap<String, QueryResult>>>,
}

impl LensFlightSqlService {
    pub fn new(lens: Lens) -> Self {
        Self {
            lens: Arc::new(lens),
            next_handle: Arc::new(AtomicU64::new(1)),
            results: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    async fn stage_result(&self, output: LensOutput) -> Result<(String, QueryResult), Status> {
        let result = lens_output_to_query_result(output).map_err(status_internal)?;
        let id = self.next_handle.fetch_add(1, Ordering::Relaxed);
        let handle = format!("chronicle-query-{id}");
        self.results
            .lock()
            .await
            .insert(handle.clone(), result.clone());
        Ok((handle, result))
    }
}

pub async fn serve_with_shutdown<S>(
    lens: Lens,
    bind_address: SocketAddr,
    shutdown: S,
) -> Result<(), tonic::transport::Error>
where
    S: Future<Output = ()> + Send + 'static,
{
    let service = LensFlightSqlService::new(lens);
    let service = FlightServiceServer::new(service);
    info!(addr = %bind_address, "lens Flight SQL service starting");
    tonic::transport::Server::builder()
        .add_service(service)
        .serve_with_shutdown(bind_address, shutdown)
        .await
}

#[tonic::async_trait]
impl FlightSqlService for LensFlightSqlService {
    type FlightService = Self;

    async fn get_flight_info_statement(
        &self,
        query: CommandStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let output = self
            .lens
            .execute(&query.query)
            .await
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let (handle, result) = self.stage_result(output).await?;
        let ticket = TicketStatementQuery {
            statement_handle: handle.into_bytes().into(),
        };
        let endpoint =
            FlightEndpoint::new().with_ticket(Ticket::new(ticket.as_any().encode_to_vec()));
        let info = FlightInfo::new()
            .try_with_schema(result.schema.as_ref())
            .map_err(status_internal)?
            .with_descriptor(request.into_inner())
            .with_endpoint(endpoint)
            .with_total_records(result.total_records)
            .with_total_bytes(result.total_bytes);
        Ok(Response::new(info))
    }

    async fn do_get_statement(
        &self,
        ticket: TicketStatementQuery,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let handle = String::from_utf8(ticket.statement_handle.to_vec())
            .map_err(|_| Status::invalid_argument("invalid statement handle"))?;
        let result = self
            .results
            .lock()
            .await
            .remove(&handle)
            .ok_or_else(|| Status::not_found("statement handle not found"))?;
        Ok(Response::new(result.into_stream()?))
    }

    async fn do_put_statement_update(
        &self,
        query: CommandStatementUpdate,
        _request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        let output = self
            .lens
            .execute(&query.query)
            .await
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        Ok(affected_rows(&output))
    }

    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}
}

#[derive(Clone)]
struct QueryResult {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
    total_records: i64,
    total_bytes: i64,
}

impl QueryResult {
    fn new(schema: SchemaRef, batches: Vec<RecordBatch>) -> Self {
        let total_records = batches.iter().map(|batch| batch.num_rows() as i64).sum();
        let total_bytes = batches
            .iter()
            .map(|batch| batch.get_array_memory_size() as i64)
            .sum();
        Self {
            schema,
            batches,
            total_records,
            total_bytes,
        }
    }

    fn into_stream(self) -> Result<FlightDataStream, Status> {
        let flight_data =
            batches_to_flight_data(self.schema.as_ref(), self.batches).map_err(status_internal)?;
        Ok(Box::pin(stream::iter(flight_data.into_iter().map(Ok))))
    }
}

fn lens_output_to_query_result(
    output: LensOutput,
) -> Result<QueryResult, arrow_schema::ArrowError> {
    let batch = match output {
        LensOutput::Empty => message_batch("OK")?,
        LensOutput::Message(message) => message_batch(&message)?,
        LensOutput::Datasets(datasets) => {
            let schema = Arc::new(Schema::new(vec![
                Field::new("name", ArrowDataType::Utf8, false),
                Field::new("version", ArrowDataType::Int64, false),
                Field::new("fields", ArrowDataType::Int64, false),
                Field::new("status", ArrowDataType::Utf8, false),
            ]));
            RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(StringArray::from_iter_values(
                        datasets.iter().map(|dataset| dataset.value.name.as_str()),
                    )) as ArrayRef,
                    Arc::new(Int64Array::from_iter_values(
                        datasets.iter().map(|dataset| dataset.version),
                    )),
                    Arc::new(Int64Array::from_iter_values(
                        datasets
                            .iter()
                            .map(|dataset| dataset.value.schema.fields.len() as i64),
                    )),
                    Arc::new(StringArray::from_iter_values(
                        datasets
                            .iter()
                            .map(|dataset| format!("{:?}", dataset.value.status)),
                    )),
                ],
            )?
        }
        LensOutput::Action(action) => {
            let schema = Arc::new(Schema::new(vec![
                Field::new("id", ArrowDataType::Utf8, false),
                Field::new("status", ArrowDataType::Utf8, false),
                Field::new("kind", ArrowDataType::Utf8, false),
                Field::new("dataset", ArrowDataType::Utf8, false),
            ]));
            RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(StringArray::from(vec![action.value.id])) as ArrayRef,
                    Arc::new(StringArray::from(vec![format!(
                        "{:?}",
                        action.value.status
                    )])),
                    Arc::new(StringArray::from(vec![format!(
                        "{:?}",
                        action.value.request.kind
                    )])),
                    Arc::new(StringArray::from(vec![action.value.request.dataset])),
                ],
            )?
        }
        LensOutput::Rows(row_batches) => {
            let schema = Arc::new(Schema::new(vec![
                Field::new("offset", ArrowDataType::Int64, false),
                Field::new("schema_id", ArrowDataType::Int64, false),
                Field::new("payload", ArrowDataType::Utf8, false),
            ]));
            let row_count: usize = row_batches.iter().map(|batch| batch.rows.len()).sum();
            let mut offsets = Vec::with_capacity(row_count);
            let mut schema_ids = Vec::with_capacity(row_count);
            let mut payloads = Vec::with_capacity(row_count);
            for batch in row_batches {
                for row in batch.rows {
                    offsets.push(row.offset);
                    schema_ids.push(batch.schema_id);
                    payloads.push(String::from_utf8_lossy(&row.payload).into_owned());
                }
            }
            RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(Int64Array::from(offsets)) as ArrayRef,
                    Arc::new(Int64Array::from(schema_ids)),
                    Arc::new(StringArray::from(payloads)),
                ],
            )?
        }
    };

    Ok(QueryResult::new(batch.schema(), vec![batch]))
}

fn message_batch(message: &str) -> Result<RecordBatch, arrow_schema::ArrowError> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "message",
        ArrowDataType::Utf8,
        false,
    )]));
    RecordBatch::try_new(
        schema,
        vec![Arc::new(StringArray::from(vec![message.to_string()]))],
    )
}

fn affected_rows(output: &LensOutput) -> i64 {
    match output {
        LensOutput::Datasets(datasets) => datasets.len() as i64,
        LensOutput::Action(_) => 1,
        LensOutput::Rows(batches) => batches.iter().map(|batch| batch.rows.len() as i64).sum(),
        LensOutput::Empty | LensOutput::Message(_) => 0,
    }
}

fn status_internal(error: impl std::fmt::Display) -> Status {
    Status::internal(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_flight::sql::client::FlightSqlServiceClient;
    use catalog::{
        DataType, Dataset, DatasetField, DatasetSchema, Versioned, build_memory_catalog,
    };
    use futures_util::TryStreamExt;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::transport::Channel;

    #[test]
    fn datasets_are_encoded_as_arrow_rows() {
        let output = LensOutput::Datasets(vec![Versioned::new(
            Dataset::new(
                "events",
                DatasetSchema::new(vec![DatasetField::new("payload", DataType::Json)]),
            ),
            7,
        )]);

        let result = lens_output_to_query_result(output).unwrap();

        assert_eq!(result.total_records, 1);
        assert_eq!(result.schema.field(0).name(), "name");
        assert_eq!(result.batches[0].num_columns(), 4);
    }

    #[tokio::test]
    async fn flight_sql_client_executes_query_and_fetches_rows() {
        let catalog = build_memory_catalog();
        catalog
            .create_dataset(Dataset::new(
                "events",
                DatasetSchema::new(vec![DatasetField::new("payload", DataType::Json)]),
            ))
            .await
            .unwrap();

        let lens = Lens::new(catalog);
        let service = LensFlightSqlService::new(lens);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(FlightServiceServer::new(service))
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                    let _ = shutdown_rx.await;
                })
                .await
        });

        let channel = Channel::from_shared(format!("http://{addr}"))
            .unwrap()
            .connect()
            .await
            .unwrap();
        let mut client = FlightSqlServiceClient::new(channel);
        let info = client
            .execute("show datasets".to_string(), None)
            .await
            .unwrap();
        let ticket = info.endpoint[0].ticket.clone().unwrap();
        let batches: Vec<RecordBatch> = client
            .do_get(ticket)
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);

        shutdown_tx.send(()).unwrap();
        server.await.unwrap().unwrap();
    }
}
