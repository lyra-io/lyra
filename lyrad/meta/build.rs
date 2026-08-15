fn main() {
    let mut config = prost_build::Config::new();
    config.bytes(["."]);

    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .compile_protos_with_config(
            config,
            &[
                "proto/pb_external.proto",
                "proto/pb_storage.proto",
                "proto/pb_catalog.proto",
            ],
            &["proto"],
        )
        .unwrap();
}
