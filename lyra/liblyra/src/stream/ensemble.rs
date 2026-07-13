use lyra_proto::pb_catalog::{UnitInfo, UnitRegistration, UnitStatus};
use std::collections::{HashMap, VecDeque};

fn unit_info(u: &UnitRegistration) -> UnitInfo {
    u.unit.clone().expect("unit info required")
}

fn address(u: &UnitRegistration) -> &str {
    &u.unit.as_ref().expect("unit info required").address
}

fn pressure(u: &UnitRegistration) -> f64 {
    0.4 * u.cpu_usage + 0.4 * u.memory_usage + 0.2 * u.disk_usage
}

/// Select an ensemble of `rf` units with a three-tier priority:
///
/// 1. **Previous ensemble** — keep members from the prior ensemble if still
///    writable (minimizes data movement on term changes)
/// 2. **Zone diversity** — round-robin across zones so replicas span
///    failure domains
/// 3. **Lowest pressure** — within each zone, pick the unit with the
///    lowest pressure (CPU/memory/disk)
///
/// - `include_candidates`: already fenced units — preferred if still writable
/// - `exclude_candidates`: units to never select
pub fn select_ensemble(
    candidates: &[UnitRegistration],
    include_candidates: &VecDeque<UnitInfo>,
    exclude_candidates: &VecDeque<UnitInfo>,
) -> Option<Vec<UnitInfo>> {
    let rf = include_candidates.len() + exclude_candidates.len();
    let exclude_addrs: Vec<&str> = exclude_candidates
        .iter()
        .map(|u| u.address.as_str())
        .collect();
    let writable: Vec<&UnitRegistration> = candidates
        .iter()
        .filter(|u| u.status() == UnitStatus::Writable && !exclude_addrs.contains(&address(u)))
        .collect();

    if writable.len() < rf {
        return None;
    }

    let mut ensemble: Vec<UnitInfo> = Vec::with_capacity(rf);
    let mut used_zones: HashMap<&str, usize> = HashMap::new();

    // Phase 1: retain included (already fenced) members that are still writable
    for inc in include_candidates {
        if ensemble.len() >= rf {
            break;
        }
        if let Some(u) = writable.iter().find(|u| address(u) == inc.address) {
            ensemble.push(unit_info(u));
            let zone = if u.zone.is_empty() {
                "default"
            } else {
                u.zone.as_str()
            };
            *used_zones.entry(zone).or_default() += 1;
        }
    }

    if ensemble.len() >= rf {
        return Some(ensemble);
    }

    // Phase 2: fill remaining slots with zone-diverse, low-pressure units
    let selected_addrs: Vec<&str> = ensemble.iter().map(|u| u.address.as_str()).collect();
    let remaining: Vec<&UnitRegistration> = writable
        .iter()
        .filter(|u| !selected_addrs.contains(&address(u)))
        .copied()
        .collect();

    // Group by zone, sort each group by pressure (ascending)
    let mut by_zone: HashMap<&str, Vec<&UnitRegistration>> = HashMap::new();
    for u in &remaining {
        let zone = if u.zone.is_empty() {
            "default"
        } else {
            u.zone.as_str()
        };
        by_zone.entry(zone).or_default().push(u);
    }
    for group in by_zone.values_mut() {
        group.sort_by(|a, b| {
            pressure(a)
                .partial_cmp(&pressure(b))
                .unwrap_or(std::cmp::Ordering::Equal)
        });
    }

    // Sort zones: prefer zones with fewer already-selected units
    let mut zone_keys: Vec<&str> = by_zone.keys().copied().collect();
    zone_keys.sort_by(|a, b| {
        let count_a = used_zones.get(a).copied().unwrap_or(0);
        let count_b = used_zones.get(b).copied().unwrap_or(0);
        count_a.cmp(&count_b).then(a.cmp(b))
    });

    let mut zone_cursors = vec![0usize; zone_keys.len()];

    let mut passes = 0;
    while ensemble.len() < rf {
        let mut added_this_pass = false;
        for (zi, zone) in zone_keys.iter().enumerate() {
            if ensemble.len() >= rf {
                break;
            }
            let group = &by_zone[zone];
            if zone_cursors[zi] < group.len() {
                ensemble.push(unit_info(group[zone_cursors[zi]]));
                zone_cursors[zi] += 1;
                added_this_pass = true;
            }
        }
        if !added_this_pass {
            break;
        }
        passes += 1;
        if passes > rf {
            break;
        }
    }

    if ensemble.len() >= rf {
        Some(ensemble)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reg(addr: &str, zone: &str) -> UnitRegistration {
        UnitRegistration {
            unit: Some(UnitInfo {
                id: addr.into(),
                address: addr.into(),
            }),
            zone: zone.into(),
            status: UnitStatus::Writable as i32,
            cpu_usage: 0.0,
            memory_usage: 0.0,
            disk_usage: 0.0,
        }
    }

    fn reg_with_load(addr: &str, zone: &str, cpu: f64, mem: f64, disk: f64) -> UnitRegistration {
        UnitRegistration {
            unit: Some(UnitInfo {
                id: addr.into(),
                address: addr.into(),
            }),
            zone: zone.into(),
            status: UnitStatus::Writable as i32,
            cpu_usage: cpu,
            memory_usage: mem,
            disk_usage: disk,
        }
    }

    fn info(addr: &str) -> UnitInfo {
        UnitInfo {
            id: addr.into(),
            address: addr.into(),
        }
    }

    fn include(addrs: &[&str]) -> VecDeque<UnitInfo> {
        addrs.iter().map(|a| info(a)).collect()
    }

    fn exclude(addrs: &[&str]) -> VecDeque<UnitInfo> {
        addrs.iter().map(|a| info(a)).collect()
    }

    #[test]
    fn basic_select() {
        let units = vec![
            reg("a", "us-east-1"),
            reg("b", "us-west-2"),
            reg("c", "eu-west-1"),
        ];
        // rf = include(0) + exclude(2) = 2
        let result = select_ensemble(&units, &include(&[]), &exclude(&["x", "y"])).unwrap();
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn include_preferred() {
        let units = vec![
            reg("a", "zone-a"),
            reg("b", "zone-b"),
            reg("c", "zone-c"),
            reg("d", "zone-d"),
        ];
        // rf = 2 + 1 = 3, include b and c
        let result =
            select_ensemble(&units, &include(&["b", "c"]), &exclude(&["placeholder"])).unwrap();
        assert!(result.iter().any(|u| u.address == "b"));
        assert!(result.iter().any(|u| u.address == "c"));
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn include_skips_non_writable() {
        let mut b = reg("b", "zone-b");
        b.status = UnitStatus::Readonly as i32;
        let units = vec![
            reg("a", "zone-a"),
            b,
            reg("c", "zone-c"),
            reg("d", "zone-d"),
        ];
        // rf = 1 + 1 = 2, include b but b is readonly
        let result = select_ensemble(&units, &include(&["b"]), &exclude(&["placeholder"])).unwrap();
        assert!(!result.iter().any(|u| u.address == "b"));
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn zone_spread() {
        let units = vec![
            reg("a1", "zone-a"),
            reg("a2", "zone-a"),
            reg("b1", "zone-b"),
            reg("b2", "zone-b"),
            reg("c1", "zone-c"),
        ];
        // rf = 0 + 3 = 3
        let result = select_ensemble(&units, &include(&[]), &exclude(&["x", "y", "z"])).unwrap();
        let zones: Vec<&str> = result
            .iter()
            .map(|ui| {
                let u = units.iter().find(|u| address(u) == ui.address).unwrap();
                u.zone.as_str()
            })
            .collect();
        let mut unique = zones.clone();
        unique.sort();
        unique.dedup();
        assert_eq!(unique.len(), 3, "should pick from 3 different zones");
    }

    #[test]
    fn prefers_low_pressure() {
        let units = vec![
            reg_with_load("hot", "zone-a", 0.9, 0.8, 0.5),
            reg_with_load("cold", "zone-a", 0.1, 0.1, 0.1),
            reg_with_load("other", "zone-b", 0.2, 0.2, 0.1),
        ];
        // rf = 0 + 2 = 2
        let result = select_ensemble(&units, &include(&[]), &exclude(&["x", "y"])).unwrap();
        assert!(result.iter().any(|u| u.address == "cold"));
        assert!(result.iter().any(|u| u.address == "other"));
    }

    #[test]
    fn respects_exclude() {
        let units = vec![reg("a", "zone-a"), reg("b", "zone-b"), reg("c", "zone-c")];
        // rf = 0 + 2 = 2, exclude "a"
        let result =
            select_ensemble(&units, &include(&[]), &exclude(&["a", "placeholder"])).unwrap();
        assert!(!result.iter().any(|u| u.address == "a"));
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn insufficient_units() {
        let units = vec![reg("a", "zone-a")];
        // rf = 0 + 2 = 2, only 1 unit
        assert!(select_ensemble(&units, &include(&[]), &exclude(&["x", "y"])).is_none());
    }

    #[test]
    fn skips_non_writable() {
        let mut u = reg("a", "zone-a");
        u.status = UnitStatus::Readonly as i32;
        let units = vec![u, reg("b", "zone-b")];
        // rf = 0 + 2 = 2
        assert!(select_ensemble(&units, &include(&[]), &exclude(&["x", "y"])).is_none());
    }

    #[test]
    fn single_zone_fallback() {
        let units = vec![reg("a", "zone-a"), reg("b", "zone-a"), reg("c", "zone-a")];
        // rf = 0 + 3 = 3
        let result = select_ensemble(&units, &include(&[]), &exclude(&["x", "y", "z"])).unwrap();
        assert_eq!(result.len(), 3);
    }
}
