fn main() {
    let mut config = prost_build::Config::new();
    config.bytes(["."]);
    config.type_attribute(".io.lyra.proto.catalog.v1.Scram", "#[derive(Eq)]");

    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .compile_protos_with_config(
            config,
            &[
                "proto/pb_external.proto",
                "proto/pb_catalog_identity.proto",
                "proto/pb_catalog_io.proto",
                "proto/pb_catalog_namespace.proto",
                "proto/pb_catalog_stream.proto",
                "proto/pb_catalog_value.proto",
            ],
            &["proto"],
        )
        .unwrap();
}
