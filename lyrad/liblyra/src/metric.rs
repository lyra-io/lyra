use opentelemetry::metrics::{Counter, Histogram, Meter, MeterProvider};

#[derive(Clone)]
pub struct ClientMetrics {
    pub write_requests: Counter<u64>,
    pub write_errors: Counter<u64>,
    pub write_latency: Histogram<f64>,

    pub read_requests: Counter<u64>,
    pub read_errors: Counter<u64>,
    pub read_latency: Histogram<f64>,
    pub read_events: Counter<u64>,

    pub reconciliation_runs: Counter<u64>,
    pub reconciliation_latency: Histogram<f64>,
}

impl ClientMetrics {
    pub fn new(meter: &Meter) -> Self {
        Self {
            write_requests: meter
                .u64_counter("lyra.client.write.requests")
                .with_description("Total write requests sent")
                .build(),
            write_errors: meter
                .u64_counter("lyra.client.write.errors")
                .with_description("Total write errors")
                .build(),
            write_latency: meter
                .f64_histogram("lyra.client.write.latency")
                .with_description("Write request latency in seconds")
                .with_unit("s")
                .build(),

            read_requests: meter
                .u64_counter("lyra.client.read.requests")
                .with_description("Total read requests sent")
                .build(),
            read_errors: meter
                .u64_counter("lyra.client.read.errors")
                .with_description("Total read errors")
                .build(),
            read_latency: meter
                .f64_histogram("lyra.client.read.latency")
                .with_description("Read request latency in seconds")
                .with_unit("s")
                .build(),
            read_events: meter
                .u64_counter("lyra.client.read.events")
                .with_description("Total events read")
                .build(),

            reconciliation_runs: meter
                .u64_counter("lyra.client.reconciliation.runs")
                .with_description("Total reconciliation runs")
                .build(),
            reconciliation_latency: meter
                .f64_histogram("lyra.client.reconciliation.latency")
                .with_description("Reconciliation latency in seconds")
                .with_unit("s")
                .build(),
        }
    }

    pub fn noop() -> Self {
        let provider = opentelemetry_sdk::metrics::SdkMeterProvider::builder().build();
        let meter = provider.meter("noop");
        Self::new(&meter)
    }
}
