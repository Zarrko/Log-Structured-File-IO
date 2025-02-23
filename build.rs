fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Provide the paths to your .proto files and the directory containing them.
    prost_build::compile_protos(
        &["src/protos/kvs_command.proto"],
        &["src"],
    )?;
    Ok(())
}