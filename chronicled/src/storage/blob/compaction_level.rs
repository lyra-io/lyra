use std::collections::HashSet;
use std::fs::File;
use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use super::manager::SegmentManager;
use crate::error::unit_error::UnitError;
use crate::storage::index::{IndexEntry, Storage};

pub(crate) type TimelineOffset = (i64, i64);
pub(crate) type IndexedEntry = (TimelineOffset, IndexEntry);

struct ContiguousRun {
    segment_id: u64,
    start_offset: u64,
    total_length: u64,
    entries: Vec<(TimelineOffset, u32)>,
}

fn detect_runs(group: &[IndexedEntry]) -> Vec<ContiguousRun> {
    let mut runs = Vec::new();
    if group.is_empty() {
        return runs;
    }

    let (key, entry) = &group[0];
    let mut current = ContiguousRun {
        segment_id: entry.segment_id,
        start_offset: entry.byte_offset,
        total_length: entry.length as u64,
        entries: vec![(*key, entry.length)],
    };

    for &(key, ref entry) in &group[1..] {
        let current_end = current.start_offset + current.total_length;
        if entry.segment_id == current.segment_id && entry.byte_offset == current_end {
            current.total_length += entry.length as u64;
            current.entries.push((key, entry.length));
        } else {
            runs.push(current);
            current = ContiguousRun {
                segment_id: entry.segment_id,
                start_offset: entry.byte_offset,
                total_length: entry.length as u64,
                entries: vec![(key, entry.length)],
            };
        }
    }
    runs.push(current);

    runs
}

pub(crate) trait CompactionLevel: Send + Sync {
    fn name(&self) -> &'static str;
    fn source_level(&self) -> u32;
    fn target_level(&self) -> u32;
    fn trigger(&self) -> usize;
    fn interval(&self) -> Duration;
    fn segment_manager(&self) -> &Arc<SegmentManager>;
    fn index(&self) -> &Storage;

    fn group_entries(&self, entries: Vec<IndexedEntry>) -> Vec<Vec<IndexedEntry>>;

    fn compact(&self) -> impl std::future::Future<Output = Result<(), UnitError>> + Send {
        async {
            let source_segments = self
                .segment_manager()
                .segments_at_level(self.source_level());
            if source_segments.len() < self.trigger() {
                return Ok(());
            }
            let source_ids: HashSet<u64> = source_segments.iter().map(|m| m.id).collect();
            let entries = self.index().scan_by_segment_ids(&source_ids);

            if entries.is_empty() {
                let ids: Vec<u64> = source_ids.into_iter().collect();
                self.segment_manager().remove_segments(&ids);
                return Ok(());
            }

            let mut source_files: std::collections::HashMap<u64, File> =
                std::collections::HashMap::new();
            for meta in &source_segments {
                if let Some(path) = self.segment_manager().segment_path_for(meta.id) {
                    let file = File::open(&path).map_err(|e| {
                        UnitError::Storage(format!("failed to open segment {}: {}", meta.id, e))
                    })?;
                    source_files.insert(meta.id, file);
                }
            }

            let groups = self.group_entries(entries);
            let mut all_new_entries = Vec::new();

            for group in groups {
                let runs = detect_runs(&group);
                let mut writer = self
                    .segment_manager()
                    .new_writer_at_level(self.target_level())
                    .await?;
                let new_segment_id = writer.segment_id();

                for run in &runs {
                    let src_file = source_files.get(&run.segment_id).ok_or_else(|| {
                        UnitError::Storage(format!("segment {} file not opened", run.segment_id))
                    })?;

                    let dst_start = writer.write_range_from(
                        src_file,
                        run.start_offset,
                        run.total_length,
                        run.entries.len() as u64,
                    )?;

                    let mut offset_within_run = 0u64;
                    for &(key, entry_len) in &run.entries {
                        all_new_entries.push((
                            key,
                            IndexEntry {
                                segment_id: new_segment_id,
                                byte_offset: dst_start + offset_within_run,
                                length: entry_len,
                            },
                        ));
                        offset_within_run += entry_len as u64;
                    }
                }

                let size = writer.size();
                let entry_count = writer.entry_count();
                writer.finish().await?;
                self.segment_manager()
                    .update_meta(new_segment_id, size, entry_count);
            }

            self.index().put_index_batch(&all_new_entries)?;
            let ids: Vec<u64> = source_ids.into_iter().collect();
            self.segment_manager().remove_segments(&ids);

            info!(
                name = self.name(),
                source_count = source_segments.len(),
                entries = all_new_entries.len(),
                "compaction complete"
            );

            Ok(())
        }
    }

    fn run(&self, context: CancellationToken) -> impl std::future::Future<Output = ()> + Send {
        async move {
            let mut ticker = tokio::time::interval(self.interval());
            loop {
                tokio::select! {
                    _ = context.cancelled() => {
                        let _ = self.compact().await;
                        info!(name = self.name(), "compaction task stopped");
                        break;
                    }
                    _ = ticker.tick() => {
                        if let Err(e) = self.compact().await {
                            warn!(name = self.name(), error = ?e, "compaction failed");
                        }
                    }
                }
            }
        }
    }
}
