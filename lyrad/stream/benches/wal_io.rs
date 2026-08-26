use bytes::Bytes;
use std::sync::Arc;
use std::time::{Duration, Instant};
use stream::{Log, LogOptions, SegmentLog};

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let records = env_usize("WAL_BENCH_RECORDS", 10_000);
    let payload_size = env_usize("WAL_BENCH_PAYLOAD", 1024);
    let concurrency = env_usize("WAL_BENCH_CONCURRENCY", 32).max(1);

    println!(
        "WAL benchmark: records={records}, payload={payload_size}B, concurrency={concurrency}, target={}",
        std::env::consts::OS,
    );

    run(records, payload_size, concurrency).await;
}

async fn run(records: usize, payload_size: usize, concurrency: usize) {
    let dir = tempfile::tempdir().expect("create benchmark directory");
    let options = LogOptions::new(dir.path());

    let wal = match SegmentLog::open(options).await {
        Ok(wal) => wal,
        Err(error) => {
            println!("buffered: SKIPPED ({error})");
            return;
        }
    };

    let started = Instant::now();
    let mut tasks = Vec::with_capacity(concurrency);
    for worker in 0..concurrency {
        let wal = Arc::clone(&wal);
        let payload = Bytes::from(vec![worker as u8; payload_size]);
        let count = records / concurrency + usize::from(worker < records % concurrency);
        tasks.push(tokio::spawn(async move {
            let mut latencies = Vec::with_capacity(count);
            for _ in 0..count {
                let append_started = Instant::now();
                wal.append(payload.clone(), true).await?;
                latencies.push(append_started.elapsed());
            }
            Ok::<_, stream::WalError>(latencies)
        }));
    }

    let mut latencies = Vec::with_capacity(records);
    for task in tasks {
        match task.await.expect("benchmark worker") {
            Ok(worker_latencies) => latencies.extend(worker_latencies),
            Err(error) => {
                wal.close().await;
                println!("buffered: SKIPPED ({error})");
                return;
            }
        }
    }
    let elapsed = started.elapsed();
    wal.close().await;

    latencies.sort_unstable();
    let operations_per_second = records as f64 / elapsed.as_secs_f64();
    let mib_per_second = operations_per_second * payload_size as f64 / (1024.0 * 1024.0);
    println!(
        "buffered: {:.0} ops/s, {:.2} MiB/s, p50={:?}, p95={:?}, p99={:?}, elapsed={:?}",
        operations_per_second,
        mib_per_second,
        percentile(&latencies, 50),
        percentile(&latencies, 95),
        percentile(&latencies, 99),
        elapsed,
    );
}

fn percentile(values: &[Duration], percentile: usize) -> Duration {
    if values.is_empty() {
        return Duration::ZERO;
    }
    let index = ((values.len() - 1) * percentile / 100).min(values.len() - 1);
    values[index]
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}
