use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

use chronicle_proto::pb_ext::Event;
use crossbeam_skiplist::SkipMap;
use prost::Message;
use tokio::sync::Notify;

pub struct WriteCacheInner {
    pub buffer: boxcar::Vec<Vec<u8>>,
    pub index: SkipMap<(i64, i64), u32>,
    pub size: AtomicUsize,
}

impl WriteCacheInner {
    fn new() -> Self {
        Self {
            buffer: boxcar::Vec::new(),
            index: SkipMap::new(),
            size: AtomicUsize::new(0),
        }
    }
}

#[derive(Clone)]
pub struct WriteCache {
    active: Arc<RwLock<WriteCacheInner>>,
    sealed: Arc<RwLock<Option<Arc<WriteCacheInner>>>>,
    capacity: usize,
    flush_notify: Arc<Notify>,
    space_available: Arc<Notify>,
}

impl WriteCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            active: Arc::new(RwLock::new(WriteCacheInner::new())),
            sealed: Arc::new(RwLock::new(None)),
            capacity,
            flush_notify: Arc::new(Notify::new()),
            space_available: Arc::new(Notify::new()),
        }
    }

    pub fn flush_notify(&self) -> Arc<Notify> {
        self.flush_notify.clone()
    }

    pub async fn put(&self, event: Event, truncate: bool) {
        loop {
            let size = self.active.read().unwrap().size.load(Ordering::Relaxed);
            if size < self.capacity {
                break;
            }
            if self.try_seal() {
                break;
            }
            self.space_available.notified().await;
        }
        self.put_inner(event, truncate);
    }

    pub fn put_direct(&self, event: Event, truncate: bool) {
        self.put_inner(event, truncate);
    }

    fn put_inner(&self, event: Event, truncate: bool) {
        let timeline_id = event.timeline_id;
        let offset = event.offset;

        let active = self.active.read().unwrap();

        if truncate {
            let keys_to_remove: Vec<_> = active
                .index
                .range((timeline_id, offset)..)
                .take_while(|e| e.key().0 == timeline_id)
                .map(|e| *e.key())
                .collect();
            for k in keys_to_remove {
                active.index.remove(&k);
            }
        }

        let proto_data = event.encode_to_vec();
        active.size.fetch_add(proto_data.len(), Ordering::Relaxed);
        let idx = active.buffer.push(proto_data);
        active.index.insert((timeline_id, offset), idx as u32);
    }

    pub fn try_seal(&self) -> bool {
        let mut sealed = self.sealed.write().unwrap();
        if sealed.is_some() {
            return false;
        }
        let mut active = self.active.write().unwrap();
        if active.index.is_empty() {
            return true;
        }
        let old = std::mem::replace(&mut *active, WriteCacheInner::new());
        *sealed = Some(Arc::new(old));
        self.flush_notify.notify_one();
        true
    }

    pub fn scan(&self, timeline_id: i64, start_offset: i64, end_offset: i64) -> Vec<Event> {
        let mut events = Vec::new();

        if let Some(ref s) = *self.sealed.read().unwrap() {
            Self::scan_inner(s, timeline_id, start_offset, end_offset, &mut events);
        }

        let active = self.active.read().unwrap();
        Self::scan_inner(&active, timeline_id, start_offset, end_offset, &mut events);

        events
    }

    fn scan_inner(
        inner: &WriteCacheInner,
        timeline_id: i64,
        start_offset: i64,
        end_offset: i64,
        events: &mut Vec<Event>,
    ) {
        for entry in inner
            .index
            .range((timeline_id, start_offset)..=(timeline_id, end_offset))
        {
            if entry.key().0 != timeline_id {
                break;
            }
            let &idx = entry.value();
            let data = &inner.buffer[idx as usize];
            if let Ok(event) = Event::decode(data.as_slice()) {
                events.push(event);
            }
        }
    }

    pub fn sealed_data(&self) -> Option<Arc<WriteCacheInner>> {
        self.sealed.read().unwrap().clone()
    }

    pub fn clear_sealed(&self) {
        *self.sealed.write().unwrap() = None;
        self.space_available.notify_waiters();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_CAPACITY: usize = 1024 * 1024;

    fn make_event(timeline_id: i64, offset: i64, payload: &[u8]) -> Event {
        Event {
            timeline_id,
            term: 1,
            offset,
            payload: Some(payload.to_vec().into()),
            crc32: None,
            timestamp: offset * 100,
            schema_id: 0,
        }
    }

    #[test]
    fn test_put_and_scan() {
        let cache = WriteCache::new(TEST_CAPACITY);
        cache.put_direct(make_event(1, 0, b"a"), false);
        cache.put_direct(make_event(1, 1, b"b"), false);
        cache.put_direct(make_event(1, 2, b"c"), false);

        let events = cache.scan(1, 0, 2);
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].offset, 0);
        assert_eq!(events[1].offset, 1);
        assert_eq!(events[2].offset, 2);
    }

    #[test]
    fn test_scan_range() {
        let cache = WriteCache::new(TEST_CAPACITY);
        for i in 0..10 {
            cache.put_direct(make_event(1, i, b"data"), false);
        }

        let events = cache.scan(1, 3, 7);
        assert_eq!(events.len(), 5);
        assert_eq!(events[0].offset, 3);
        assert_eq!(events[4].offset, 7);
    }

    #[test]
    fn test_scan_different_timelines() {
        let cache = WriteCache::new(TEST_CAPACITY);
        cache.put_direct(make_event(1, 0, b"t1"), false);
        cache.put_direct(make_event(2, 0, b"t2"), false);
        cache.put_direct(make_event(1, 1, b"t1"), false);

        let t1 = cache.scan(1, 0, 10);
        assert_eq!(t1.len(), 2);

        let t2 = cache.scan(2, 0, 10);
        assert_eq!(t2.len(), 1);
    }

    #[test]
    fn test_put_with_truncate() {
        let cache = WriteCache::new(TEST_CAPACITY);
        cache.put_direct(make_event(1, 0, b"a"), false);
        cache.put_direct(make_event(1, 1, b"b"), false);
        cache.put_direct(make_event(1, 2, b"c"), false);

        cache.put_direct(make_event(1, 1, b"new_b"), true);

        let events = cache.scan(1, 0, 10);
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].offset, 0);
        assert_eq!(events[1].offset, 1);
        assert_eq!(events[1].payload, Some(b"new_b".to_vec().into()));
    }

    #[test]
    fn test_seal_and_scan() {
        let cache = WriteCache::new(TEST_CAPACITY);
        cache.put_direct(make_event(1, 0, b"a"), false);
        cache.put_direct(make_event(1, 1, b"b"), false);

        assert!(cache.try_seal());

        let events = cache.scan(1, 0, 10);
        assert_eq!(events.len(), 2);

        cache.put_direct(make_event(1, 2, b"c"), false);
        let events = cache.scan(1, 0, 10);
        assert_eq!(events.len(), 3);

        cache.clear_sealed();
        let events = cache.scan(1, 0, 10);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].offset, 2);
    }

    #[test]
    fn test_seal_blocked_when_sealed_exists() {
        let cache = WriteCache::new(TEST_CAPACITY);
        cache.put_direct(make_event(1, 0, b"a"), false);

        assert!(cache.try_seal());
        assert!(!cache.try_seal());

        cache.clear_sealed();
        cache.put_direct(make_event(1, 1, b"b"), false);
        assert!(cache.try_seal());
    }

    #[tokio::test]
    async fn test_backpressure_with_seal() {
        let cache = WriteCache::new(50);
        cache.put_direct(make_event(1, 0, b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), false);
        cache.put_direct(make_event(1, 1, b"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"), false);

        let cache2 = cache.clone();
        let handle = tokio::spawn(async move {
            cache2.put(make_event(1, 2, b"c"), false).await;
        });

        tokio::task::yield_now().await;

        tokio::time::timeout(std::time::Duration::from_secs(1), handle)
            .await
            .expect("put should auto-seal and continue")
            .unwrap();
    }

    #[tokio::test]
    async fn test_backpressure_both_full() {
        let cache = WriteCache::new(50);

        cache.put_direct(make_event(1, 0, b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), false);
        cache.put_direct(make_event(1, 1, b"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"), false);

        assert!(cache.try_seal());

        cache.put_direct(make_event(1, 2, b"ccccccccccccccccccccccccccccccc"), false);
        cache.put_direct(make_event(1, 3, b"ddddddddddddddddddddddddddddddd"), false);

        let cache2 = cache.clone();
        let handle = tokio::spawn(async move {
            cache2.put(make_event(1, 4, b"e"), false).await;
        });

        tokio::task::yield_now().await;
        assert!(!handle.is_finished());

        cache.clear_sealed();

        tokio::time::timeout(std::time::Duration::from_secs(1), handle)
            .await
            .expect("put should unblock after clear_sealed")
            .unwrap();
    }
}
