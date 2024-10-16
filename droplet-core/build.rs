fn main() -> Result<(), Box<dyn std::error::Error>> {
    let iface_files = &[
        "src/proto/feature.proto",
        "src/proto/meta.proto",
        "src/proto/service.proto",
    ];

    let dirs = &["src"];

    println!("start build proto");

    tonic_build::configure()
        .build_client(true)
        .compile_protos(iface_files, dirs)
        .unwrap_or_else(|e| panic!("protobuf compilation failed: {}", e));

    for file in iface_files {
        println!("cargo:rerun-if-changed={}", file);
    }

    Ok(())
}