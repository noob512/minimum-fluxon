// Feature-gated test binary wrapper
#![cfg(feature = "test_bins")]

use limit_thirdparty::tokio;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    if std::env::args_os().len() > 1 {
        return Bridging L2–L3_kv::entry().await;
    }

    Bridging L2–L3_kv::kv_test::test_kv_all().await;
    Ok(())
}
