use std::collections::HashSet;
use std::fs;
use std::sync::Arc;

use rocksdb::{
    BlockBasedOptions, Cache, DB, DBCompressionType, LogLevel, Options, SliceTransform, WriteBatch,
    WriteOptions, statistics::Ticker,
};

use crate::error::unit_error::UnitError;
use crate::option::unit_options::ResolvedIndexOptions;

use super::entry::{IndexEntry, decode_key, encode_key};

const SEGMENT_META_PREFIX: u8 = 0xFF;

fn encode_segment_meta_key(segment_id: u64) -> [u8; 9] {
    let mut key = [0u8; 9];
    key[0] = SEGMENT_META_PREFIX;
    key[1..9].copy_from_slice(&segment_id.to_be_bytes());
    key
}

fn decode_segment_meta_key(key: &[u8]) -> Option<u64> {
    if key.len() == 9 && key[0] == SEGMENT_META_PREFIX {
        Some(u64::from_be_bytes(key[1..9].try_into().unwrap()))
    } else {
        None
    }
}

pub(crate) struct Inner {
    pub database: DB,
    pub write_options: WriteOptions,
    pub db_options: Options,
}

#[derive(Clone)]
pub struct Storage {
    inner: Arc<Inner>,
}

pub struct StorageOptions {
    pub path: String,
    pub index: Option<ResolvedIndexOptions>,
}

impl Storage {
    pub fn new(options: StorageOptions) -> Result<Storage, UnitError> {
        fs::create_dir_all(&options.path).map_err(|e| {
            UnitError::Storage(format!("failed to create storage directory: {}", e))
        })?;

        let idx = options.index.unwrap_or(ResolvedIndexOptions {
            block_cache_bytes: 256 * 1024 * 1024,
            write_buffer_size: 4 * 1024 * 1024,
            num_levels: 4,
            target_file_size_base: 4 * 1024 * 1024,
            max_bytes_for_level_base: 16 * 1024 * 1024,
        });

        let mut db_options = Options::default();
        db_options.create_if_missing(true);
        db_options.set_log_level(LogLevel::Info);
        db_options.set_keep_log_file_num(10);
        db_options.enable_statistics();

        db_options.set_manual_wal_flush(true);

        db_options.set_compression_type(DBCompressionType::Lz4);
        db_options.set_bottommost_compression_type(DBCompressionType::Zstd);

        db_options.set_prefix_extractor(SliceTransform::create_fixed_prefix(8));

        db_options.set_write_buffer_size(idx.write_buffer_size);
        db_options.set_max_write_buffer_number(3);
        db_options.set_min_write_buffer_number_to_merge(2);

        db_options.set_num_levels(idx.num_levels);
        db_options.set_target_file_size_base(idx.target_file_size_base);
        db_options.set_max_bytes_for_level_base(idx.max_bytes_for_level_base);
        db_options.set_level_compaction_dynamic_level_bytes(true);

        let cache = Cache::new_lru_cache(idx.block_cache_bytes);
        let mut block_options = BlockBasedOptions::default();
        block_options.set_block_cache(&cache);
        block_options.set_block_size(4 * 1024);
        block_options.set_bloom_filter(10.0, false);
        block_options.set_cache_index_and_filter_blocks(true);
        block_options.set_pin_l0_filter_and_index_blocks_in_cache(true);
        db_options.set_block_based_table_factory(&block_options);

        let db = DB::open(&db_options, &options.path)
            .map_err(|err| UnitError::Storage(err.to_string()))?;

        let mut write_options = WriteOptions::default();
        write_options.disable_wal(true);

        Ok(Storage {
            inner: Arc::new(Inner {
                database: db,
                write_options,
                db_options,
            }),
        })
    }

    pub fn put_index_batch(&self, entries: &[((i64, i64), IndexEntry)]) -> Result<(), UnitError> {
        let mut batch = WriteBatch::default();
        for &((timeline_id, offset), ref entry) in entries {
            let key = encode_key(timeline_id, offset);
            let value = entry.encode();
            batch.put(key, value);
        }
        self.inner
            .database
            .write_opt(batch, &self.inner.write_options)
            .map_err(|e| UnitError::Storage(e.to_string()))
    }

    pub fn delete_index_batch(&self, keys: &[(i64, i64)]) -> Result<(), UnitError> {
        let mut batch = WriteBatch::default();
        for &(timeline_id, offset) in keys {
            let key = encode_key(timeline_id, offset);
            batch.delete(key);
        }
        self.inner
            .database
            .write_opt(batch, &self.inner.write_options)
            .map_err(|e| UnitError::Storage(e.to_string()))
    }

    pub fn delete_index_range(&self, timeline_id: i64, from_offset: i64) -> Result<(), UnitError> {
        let db = &self.inner.database;
        let cf = db
            .cf_handle(rocksdb::DEFAULT_COLUMN_FAMILY_NAME)
            .ok_or_else(|| UnitError::Storage("default column family not found".into()))?;
        let start_key = encode_key(timeline_id, from_offset);
        let end_key = encode_key(timeline_id, i64::MAX);
        db.delete_range_cf(&cf, &start_key, &end_key)
            .map_err(|e| UnitError::Storage(e.to_string()))
    }

    pub fn scan_index(
        &self,
        timeline_id: i64,
        start_offset: i64,
        end_offset: i64,
    ) -> Vec<(i64, IndexEntry)> {
        let start_key = encode_key(timeline_id, start_offset);
        let iter = self.inner.database.iterator(rocksdb::IteratorMode::From(
            &start_key,
            rocksdb::Direction::Forward,
        ));

        let mut results = Vec::new();
        for item in iter {
            match item {
                Ok((key, value)) => {
                    if key.len() != 16 || value.len() != 20 {
                        continue;
                    }
                    let (key_timeline_id, key_offset) = decode_key(&key);
                    if key_timeline_id != timeline_id {
                        break;
                    }
                    if key_offset > end_offset {
                        break;
                    }
                    results.push((key_offset, IndexEntry::decode(&value)));
                }
                Err(_) => break,
            }
        }
        results
    }

    pub fn scan_by_segment_ids(&self, segment_ids: &HashSet<u64>) -> Vec<((i64, i64), IndexEntry)> {
        let iter = self.inner.database.iterator(rocksdb::IteratorMode::Start);
        let mut results = Vec::new();

        for item in iter {
            match item {
                Ok((key, value)) => {
                    if key.len() != 16 || value.len() != 20 {
                        continue;
                    }
                    let entry = IndexEntry::decode(&value);
                    if segment_ids.contains(&entry.segment_id) {
                        let (timeline_id, offset) = decode_key(&key);
                        results.push(((timeline_id, offset), entry));
                    }
                }
                Err(_) => break,
            }
        }

        results
    }

    pub fn all_referenced_segment_ids(&self) -> HashSet<u64> {
        let iter = self.inner.database.iterator(rocksdb::IteratorMode::Start);
        let mut ids = HashSet::new();

        for item in iter {
            match item {
                Ok((_, value)) => {
                    if value.len() == 20 {
                        let entry = IndexEntry::decode(&value);
                        ids.insert(entry.segment_id);
                    }
                }
                Err(_) => break,
            }
        }

        ids
    }

    pub fn put_segment_meta_raw(&self, segment_id: u64, value: &[u8]) -> Result<(), UnitError> {
        let key = encode_segment_meta_key(segment_id);
        self.inner
            .database
            .put_opt(key, value, &self.inner.write_options)
            .map_err(|e| UnitError::Storage(e.to_string()))
    }

    pub fn get_segment_meta_raw(&self, segment_id: u64) -> Option<Vec<u8>> {
        let key = encode_segment_meta_key(segment_id);
        self.inner.database.get(key).ok().flatten()
    }

    pub fn delete_segment_meta(&self, segment_id: u64) -> Result<(), UnitError> {
        let key = encode_segment_meta_key(segment_id);
        self.inner
            .database
            .delete_opt(key, &self.inner.write_options)
            .map_err(|e| UnitError::Storage(e.to_string()))
    }

    pub fn put_raw(&self, key: &[u8], value: &[u8]) -> Result<(), UnitError> {
        self.inner
            .database
            .put_opt(key, value, &self.inner.write_options)
            .map_err(|e| UnitError::Storage(e.to_string()))
    }

    pub fn get_raw(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.inner.database.get(key).ok().flatten()
    }

    pub fn ticker(&self, ticker: Ticker) -> u64 {
        self.inner.db_options.get_ticker_count(ticker)
    }

    pub fn all_segment_meta_raw(&self) -> Vec<(u64, Vec<u8>)> {
        let prefix = [SEGMENT_META_PREFIX];
        let iter = self.inner.database.iterator(rocksdb::IteratorMode::From(
            &prefix,
            rocksdb::Direction::Forward,
        ));
        let mut results = Vec::new();

        for item in iter {
            match item {
                Ok((key, value)) => {
                    if let Some(segment_id) = decode_segment_meta_key(&key) {
                        results.push((segment_id, value.to_vec()));
                    } else {
                        break;
                    }
                }
                Err(_) => break,
            }
        }

        results
    }
}
