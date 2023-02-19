fn main() -> anyhow::Result<()> {
    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .out_dir("./src")
        .compile(&["./proto/sumo.proto"], &["proto/"])?;
    Ok(())
}
