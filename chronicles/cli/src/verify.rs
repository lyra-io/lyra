use futures_util::StreamExt;
use libchronicle::TimelineOptions;
use libchronicle::chronicle::{Chronicle, ChronicleOptions};
use libchronicle::{Event, FetchOptions};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::signal;
use tokio::sync::Mutex;
use tracing::{error, info, warn};

#[derive(clap::Args)]
pub struct VerifyArgs {
    #[arg(long, default_value = "localhost:6648")]
    pub catalog: String,

    #[arg(long)]
    pub units: Option<String>,

    #[arg(long, default_value = "4")]
    pub timelines: usize,

    #[arg(long, default_value = "100")]
    pub rate: u64,

    #[arg(long, default_value = "3")]
    pub replication_factor: usize,

    #[arg(long, default_value = "256")]
    pub payload_size: usize,

    #[arg(long, default_value = "5")]
    pub report_interval: u64,

    #[arg(long, default_value = "0")]
    pub duration: u64,
}

struct TimelineVerifier {
    name: String,
    written_payloads: BTreeMap<i64, Vec<u8>>,
    acked_offsets: BTreeSet<i64>,
    read_offsets: BTreeSet<i64>,
    last_read_offset: i64,
    violations: Vec<String>,
}

impl TimelineVerifier {
    fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            written_payloads: BTreeMap::new(),
            acked_offsets: BTreeSet::new(),
            read_offsets: BTreeSet::new(),
            last_read_offset: 0,
            violations: Vec::new(),
        }
    }

    fn record_ack(&mut self, offset: i64, payload: Vec<u8>) {
        self.acked_offsets.insert(offset);
        self.written_payloads.insert(offset, payload);
    }

    fn record_read(&mut self, offset: i64, payload: &[u8], payload_size: usize) {
        if offset <= self.last_read_offset {
            self.violations.push(format!(
                "[{}] ORDER: offset {} read after {} (not monotonically increasing)",
                self.name, offset, self.last_read_offset
            ));
        }
        self.last_read_offset = offset;

        if self.read_offsets.contains(&offset) {
            self.violations.push(format!(
                "[{}] DUPLICATE: offset {} read more than once",
                self.name, offset
            ));
        }
        self.read_offsets.insert(offset);

        if payload.len() >= 8 {
            let seq = u64::from_be_bytes(payload[..8].try_into().unwrap());
            let expected_fill = (seq % 256) as u8;
            if payload.len() != payload_size {
                self.violations.push(format!(
                    "[{}] CORRUPTION: offset {} payload size {} != expected {}",
                    self.name,
                    offset,
                    payload.len(),
                    payload_size
                ));
            } else if !payload[8..].iter().all(|&b| b == expected_fill) {
                self.violations.push(format!(
                    "[{}] CORRUPTION: offset {} fill byte mismatch (seq={})",
                    self.name, offset, seq
                ));
            }
        } else {
            self.violations.push(format!(
                "[{}] CORRUPTION: offset {} payload too short ({}B)",
                self.name,
                offset,
                payload.len()
            ));
        }

        if let Some(written) = self.written_payloads.get(&offset)
            && payload != written.as_slice()
        {
            self.violations.push(format!(
                "[{}] PAYLOAD_MISMATCH: offset {} read payload differs from written \
                 (written_len={}, read_len={}, first_diff={})",
                self.name,
                offset,
                written.len(),
                payload.len(),
                written
                    .iter()
                    .zip(payload.iter())
                    .position(|(a, b)| a != b)
                    .unwrap_or(written.len().min(payload.len()))
            ));
        }
    }

    fn verify_final(&mut self) {
        if let (Some(&min), Some(&max)) = (self.acked_offsets.first(), self.acked_offsets.last()) {
            for expected in min..=max {
                if !self.acked_offsets.contains(&expected) {
                    self.violations.push(format!(
                        "[{}] GAP: acked offset {} missing (range {}..{})",
                        self.name, expected, min, max
                    ));
                }
            }
        }

        for &offset in &self.acked_offsets {
            if !self.read_offsets.contains(&offset) {
                self.violations.push(format!(
                    "[{}] DURABILITY: acked offset {} not found in read",
                    self.name, offset
                ));
            }
        }

        for &offset in &self.read_offsets {
            if !self.acked_offsets.contains(&offset) {
                self.violations.push(format!(
                    "[{}] PHANTOM: offset {} was read but never acked",
                    self.name, offset
                ));
            }
        }
    }
}

struct Stats {
    written: AtomicU64,
    read: AtomicU64,
    verified: AtomicU64,
    write_errors: AtomicU64,
    read_errors: AtomicU64,
}

impl Stats {
    fn new() -> Self {
        Self {
            written: AtomicU64::new(0),
            read: AtomicU64::new(0),
            verified: AtomicU64::new(0),
            write_errors: AtomicU64::new(0),
            read_errors: AtomicU64::new(0),
        }
    }
}

pub async fn run(args: VerifyArgs) -> Result<(), Box<dyn std::error::Error>> {
    let catalog_opts = chronicle_catalog::CatalogOptions {
        service_address: args.catalog.clone(),
        ..Default::default()
    };
    let catalog = chronicle_catalog::build_catalog(&catalog_opts).await?;

    match catalog.list_units().await {
        Ok(units) => {
            info!(count = units.len(), "units found in catalog");
            for u in &units {
                let addr = u
                    .unit
                    .as_ref()
                    .map(|ui| ui.address.as_str())
                    .unwrap_or("unknown");
                info!(address = %addr, status = ?u.status(), "unit");
            }
        }
        Err(e) => warn!(error = %e, "failed to list units from catalog (non-fatal)"),
    }

    match catalog.list_timelines().await {
        Ok(timelines) => info!(count = timelines.len(), "timelines found in catalog"),
        Err(e) => warn!(error = %e, "failed to list timelines from catalog (non-fatal)"),
    }

    let chronicle = Arc::new(Chronicle::new(catalog, ChronicleOptions::new()));

    info!(
        timelines = args.timelines,
        rate = args.rate,
        payload_size = args.payload_size,
        replication_factor = args.replication_factor,
        "verification client starting — testing TLA+ invariants"
    );

    let running = Arc::new(AtomicBool::new(true));
    let reading = Arc::new(AtomicBool::new(true));
    let stats = Arc::new(Stats::new());

    let verifiers: Vec<Arc<Mutex<TimelineVerifier>>> = (0..args.timelines)
        .map(|i| Arc::new(Mutex::new(TimelineVerifier::new(&format!("verify-{}", i)))))
        .collect();

    let mut handles = Vec::new();

    let mut timelines = Vec::new();
    for i in 0..args.timelines {
        let name = format!("verify-{}", i);
        let timeline = match chronicle
            .open_timeline(
                &name,
                TimelineOptions::new()
                    .replication_factor(args.replication_factor)
                    .max_batch_size(256)
                    .linger(Duration::from_millis(5)),
            )
            .await
        {
            Ok(t) => t,
            Err(create_err) => {
                info!(timeline = name, error = %create_err, "create failed, trying open");
                match chronicle
                    .open_timeline(
                        &name,
                        TimelineOptions::new()
                            .replication_factor(args.replication_factor)
                            .max_batch_size(256)
                            .linger(Duration::from_millis(5)),
                    )
                    .await
                {
                    Ok(t) => t,
                    Err(e) => {
                        error!(timeline = name, create_error = %create_err, open_error = %e, "failed to create/open timeline");
                        continue;
                    }
                }
            }
        };
        timelines.push((i, timeline));
    }

    for (i, timeline) in timelines {
        let stats = stats.clone();
        let running = running.clone();
        let verifier = verifiers[i].clone();
        let payload_size = args.payload_size;
        let rate = args.rate;

        handles.push(tokio::spawn(async move {
            let name = format!("verify-{}", i);
            info!(timeline = name, "writer started");

            let interval = if rate > 0 {
                Some(Duration::from_micros(1_000_000 / rate))
            } else {
                None
            };

            let mut seq = 0u64;
            while running.load(Ordering::Relaxed) {
                let mut payload = Vec::with_capacity(payload_size);
                payload.extend_from_slice(&seq.to_be_bytes());
                payload.resize(payload_size, (seq % 256) as u8);

                match timeline.record(Event::new(payload.clone())).await {
                    Ok(result) => {
                        verifier.lock().await.record_ack(result.0, payload);
                        stats.written.fetch_add(1, Ordering::Relaxed);
                        seq += 1;
                    }
                    Err(e) => {
                        stats.write_errors.fetch_add(1, Ordering::Relaxed);
                        warn!(timeline = name, error = %e, "write error");
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                }

                if let Some(interval) = interval {
                    tokio::time::sleep(interval).await;
                }
            }

            tokio::time::sleep(Duration::from_secs(3)).await;
            drop(timeline);
            info!(timeline = name, events = seq, "writer stopped");
        }));
    }

    tokio::time::sleep(Duration::from_secs(3)).await;

    for (i, verifier) in verifiers.iter().enumerate().take(args.timelines) {
        let chronicle = chronicle.clone();
        let stats = stats.clone();
        let reading = reading.clone();
        let verifier = verifier.clone();
        let payload_size = args.payload_size;

        handles.push(tokio::spawn(async move {
            let name = format!("verify-{}", i);

            let mut stream = loop {
                match chronicle
                    .open_timeline(
                        &name,
                        TimelineOptions::new()
                            .replication_factor(args.replication_factor)
                            .max_batch_size(256)
                            .linger(Duration::from_millis(5)),
                    )
                    .await
                {
                    Ok(t) => break t.fetch(FetchOptions::earliest()).await.unwrap(),
                    Err(_) => {
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        if !reading.load(Ordering::Relaxed) {
                            return;
                        }
                    }
                }
            };
            info!(timeline = name, "reader started");

            while reading.load(Ordering::Relaxed) {
                match stream.next().await {
                    Some(Ok(event)) => {
                        stats.read.fetch_add(1, Ordering::Relaxed);
                        verifier.lock().await.record_read(
                            event.offset.unwrap_or(0),
                            &event.payload,
                            payload_size,
                        );
                        stats.verified.fetch_add(1, Ordering::Relaxed);
                    }
                    Some(Err(e)) => {
                        stats.read_errors.fetch_add(1, Ordering::Relaxed);
                        warn!(timeline = name, error = %e, "read error");
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                    None => break,
                }
            }
        }));
    }

    let stats_clone = stats.clone();
    let running_clone = running.clone();
    let report_interval = args.report_interval;
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_secs(report_interval));
        let start = Instant::now();
        let mut prev_written = 0u64;
        let mut prev_read = 0u64;
        loop {
            ticker.tick().await;
            if !running_clone.load(Ordering::Relaxed) {
                break;
            }
            let written = stats_clone.written.load(Ordering::Relaxed);
            let read = stats_clone.read.load(Ordering::Relaxed);
            let verified = stats_clone.verified.load(Ordering::Relaxed);
            let write_errors = stats_clone.write_errors.load(Ordering::Relaxed);
            let read_errors = stats_clone.read_errors.load(Ordering::Relaxed);
            let elapsed = start.elapsed().as_secs();

            let write_rate = (written - prev_written) / report_interval;
            let read_rate = (read - prev_read) / report_interval;
            prev_written = written;
            prev_read = read;

            info!(
                elapsed_s = elapsed,
                written,
                read,
                verified,
                "w/s" = write_rate,
                "r/s" = read_rate,
                write_errors,
                read_errors,
                "stats"
            );
        }
    });

    if args.duration > 0 {
        let duration = Duration::from_secs(args.duration);
        tokio::select! {
            _ = signal::ctrl_c() => {}
            _ = tokio::time::sleep(duration) => {
                info!(duration_s = args.duration, "duration reached");
            }
        }
    } else {
        signal::ctrl_c().await?;
    }
    info!("shutting down — stopping writers...");
    running.store(false, Ordering::Relaxed);

    tokio::time::sleep(Duration::from_secs(5)).await;

    info!("stopping readers — running final TLA+ invariant checks...");
    reading.store(false, Ordering::Relaxed);
    tokio::time::sleep(Duration::from_secs(2)).await;

    let mut total_violations = 0usize;
    for v in &verifiers {
        let mut verifier = v.lock().await;
        verifier.verify_final();

        let acked = verifier.acked_offsets.len();
        let read = verifier.read_offsets.len();

        if verifier.violations.is_empty() {
            info!(
                timeline = verifier.name,
                acked, read, "PASS — all TLA+ invariants hold"
            );
        } else {
            for violation in &verifier.violations {
                error!("{}", violation);
            }
            total_violations += verifier.violations.len();
            error!(
                timeline = verifier.name,
                acked,
                read,
                violations = verifier.violations.len(),
                "FAIL"
            );
        }
    }

    let written = stats.written.load(Ordering::Relaxed);
    let read = stats.read.load(Ordering::Relaxed);

    info!(
        "invariants checked: monotonic-order, no-gaps, no-duplicates, \
         durability, no-phantoms, payload-integrity, exact-payload-match"
    );

    if total_violations > 0 {
        error!(
            written,
            read,
            violations = total_violations,
            "VERIFICATION FAILED"
        );
        std::process::exit(1);
    } else {
        info!(written, read, "VERIFICATION PASSED — all invariants hold");
    }

    Ok(())
}
