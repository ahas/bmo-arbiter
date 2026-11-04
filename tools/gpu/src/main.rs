//! The gpu tool's executable: one call handed by the tool host on standard input, answered on standard
//! output (bmo-pocket's `docs/mcp-host.md`, 「The call protocol」).

#[tokio::main(flavor = "current_thread")]
async fn main() -> std::process::ExitCode {
  gpu_tool::run().await
}
