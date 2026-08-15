use meta::MetadataOptions;

#[test]
fn metadata_options_do_not_accept_runtime_backend_selector() {
    let value = serde_json::json!({
        "backend": "memory",
        "service_address": "localhost:6648",
        "namespace": "default"
    });

    let error = serde_json::from_value::<MetadataOptions>(value).unwrap_err();

    assert!(
        error.to_string().contains("unknown field `backend`"),
        "unexpected error: {error}"
    );
}
