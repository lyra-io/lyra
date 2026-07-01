use std::sync::{Arc, RwLock};

use chronicle_proto::pb_ext::Event;
use crossbeam_skiplist::SkipMap;
use prost::Message;

pub struct WriteCacheInner {
    buffer: boxcar::Vec<Vec<u8>>,
    index: SkipMap<(i64, i64), u32>,
}

impl WriteCacheInner {
    fn new() -> Self {
        Self {
            buffer: boxcar::Vec::new(),
            index: SkipMap::new(),
        }
    }
}

#[derive(Clone)]
pub struct WriteCache {
    active: Arc<RwLock<WriteCacheInner>>,
}

impl Default for WriteCache {
    fn default() -> Self {
        Self::new()
    }
}

impl WriteCache {
    pub fn new() -> Self {
        Self {
            active: Arc::new(RwLock::new(WriteCacheInner::new())),
        }
    }

    pub async fn put(&self, event: Event, truncate: bool) {
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
        let idx = active.buffer.push(proto_data);
        active.index.insert((timeline_id, offset), idx as u32);
    }

    pub fn scan(&self, timeline_id: i64, start_offset: i64, end_offset: i64) -> Vec<Event> {
        let mut events = Vec::new();
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
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let cache = WriteCache::new();
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
        let cache = WriteCache::new();
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
        let cache = WriteCache::new();
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
        let cache = WriteCache::new();
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
}
