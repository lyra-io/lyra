pub mod pb_ext {
    tonic::include_proto!("io.lyra.proto.ext.v1");
}

pub mod pb_storage {
    tonic::include_proto!("io.lyra.proto.storage.v1");
}

pub mod pb_catalog {
    tonic::include_proto!("io.lyra.proto.catalog.v1");
}
