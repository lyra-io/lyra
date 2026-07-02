use dashmap::DashMap;

#[derive(Debug, Clone)]
pub struct TimelineState {
    pub term: i64,
    pub lra: i64,
}

impl TimelineState {
    pub fn new() -> Self {
        TimelineState { term: 0, lra: -1 }
    }
}

impl Default for TimelineState {
    fn default() -> Self {
        Self::new()
    }
}

pub struct TimelineStateManager {
    states: DashMap<i64, TimelineState>,
}

impl TimelineStateManager {
    pub fn new() -> Self {
        TimelineStateManager {
            states: DashMap::new(),
        }
    }

    pub fn check_term(&self, timeline_id: i64, request_term: i64) -> Result<(), i64> {
        match self.states.get(&timeline_id) {
            Some(state) => {
                if request_term < state.term {
                    Err(state.term)
                } else {
                    Ok(())
                }
            }
            None => Ok(()),
        }
    }

    pub fn fence(&self, timeline_id: i64, new_term: i64) -> Result<i64, i64> {
        let mut entry = self.states.entry(timeline_id).or_default();
        let state = entry.value_mut();

        if new_term <= state.term {
            return Err(state.term);
        }

        state.term = new_term;
        Ok(state.lra)
    }

    pub fn update_lra(&self, timeline_id: i64, lra: i64) {
        let mut entry = self.states.entry(timeline_id).or_default();
        let state = entry.value_mut();
        if lra > state.lra {
            state.lra = lra;
        }
    }

    pub fn get_state(&self, timeline_id: i64) -> Option<TimelineState> {
        self.states.get(&timeline_id).map(|s| s.clone())
    }
}

impl Default for TimelineStateManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_check_term_rejects_stale() {
        let mgr = TimelineStateManager::new();
        mgr.fence(1, 5).unwrap();

        assert!(mgr.check_term(1, 5).is_ok());
        assert!(mgr.check_term(1, 6).is_ok());
        assert_eq!(mgr.check_term(1, 4), Err(5));
        assert_eq!(mgr.check_term(1, 0), Err(5));
    }

    #[test]
    fn test_check_term_unknown_timeline() {
        let mgr = TimelineStateManager::new();
        assert!(mgr.check_term(999, 0).is_ok());
    }

    #[test]
    fn test_fence_returns_lra() {
        let mgr = TimelineStateManager::new();
        assert_eq!(mgr.fence(1, 1).unwrap(), -1);

        mgr.update_lra(1, 10);

        assert_eq!(mgr.fence(1, 2).unwrap(), 10);
    }

    #[test]
    fn test_fence_rejects_stale_term() {
        let mgr = TimelineStateManager::new();
        mgr.fence(1, 5).unwrap();
        assert_eq!(mgr.fence(1, 3), Err(5));
        assert_eq!(mgr.fence(1, 5), Err(5));
    }

    #[test]
    fn test_update_lra_only_advances() {
        let mgr = TimelineStateManager::new();
        mgr.fence(1, 1).unwrap();

        mgr.update_lra(1, 10);
        assert_eq!(mgr.get_state(1).unwrap().lra, 10);

        mgr.update_lra(1, 5);
        assert_eq!(mgr.get_state(1).unwrap().lra, 10);

        mgr.update_lra(1, 20);
        assert_eq!(mgr.get_state(1).unwrap().lra, 20);
    }

    #[test]
    fn test_independent_timelines() {
        let mgr = TimelineStateManager::new();
        mgr.fence(1, 5).unwrap();
        mgr.fence(2, 10).unwrap();

        assert!(mgr.check_term(1, 5).is_ok());
        assert_eq!(mgr.check_term(1, 4), Err(5));
        assert!(mgr.check_term(2, 10).is_ok());
        assert_eq!(mgr.check_term(2, 9), Err(10));
    }
}
