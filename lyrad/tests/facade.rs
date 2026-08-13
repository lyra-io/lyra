use lyrad::tiered_storage as _;

#[test]
fn preserves_storage_module_paths() {
    let _ = std::mem::size_of::<lyrad::wal::WalOptions>();
    let _ = lyrad::segment::IoMode::Standard;
    let _ = std::mem::size_of::<lyrad::stream_storage::WalOptions>();
}
