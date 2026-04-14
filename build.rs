fn main() -> Result<(), Box<dyn std::error::Error>> {
    std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path().unwrap());

    let mut config = prost_build::Config::new();
    config.disable_comments(&["."]);

    tonic_build::configure()
        .build_server(true)
        .compile_with_config(
            config,
            &[
                "../java-pubsub/proto-google-cloud-pubsub-v1/src/main/proto/google/pubsub/v1/pubsub.proto",
                "../java-pubsub/proto-google-cloud-pubsub-v1/src/main/proto/google/pubsub/v1/schema.proto",
            ],
            &[
                "../java-pubsub/proto-google-cloud-pubsub-v1/src/main/proto/",
                "googleapis/",
            ],
        )?;
    Ok(())
}
