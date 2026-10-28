use std::{collections::BTreeSet, env, io::IsTerminal, path::PathBuf, time::Duration};

use anyhow::{Result, ensure};
use engineering_challenge::{
    collector::{Config, run},
    output::Destination,
};

#[tokio::main]
async fn main() -> Result<()> {
    if env::args().nth(1).as_deref() == Some("--help") {
        println!(
            "Polymarket order-book collector\n\nBy default, writes binary SBE to stdout. Set OUTPUT_PATH for file output.\nSet OUTPUT_STDOUT=1 with OUTPUT_PATH to write both. Existing files are refused.\nASSETS_PER_SOCKET=200 and DISCOVERY_INTERVAL_SECS=60 control subscriptions.\nASSET_IDS=id,id restricts capture for smoke tests; omit to collect all markets.\nCtrl-C closes sockets and flushes output. See README.md for details."
        );
        return Ok(());
    }
    ensure!(env::args().len() == 1, "unknown arguments; use --help");
    let mut config = Config::default();
    if let Ok(value) = env::var("ASSETS_PER_SOCKET") {
        config.assets_per_socket = value.parse()?;
    }
    if let Ok(value) = env::var("DISCOVERY_INTERVAL_SECS") {
        config.discovery_interval = Duration::from_secs(value.parse()?);
    }
    if let Ok(value) = env::var("DISCOVERY_URL") {
        config.discovery_url = value;
    }
    if let Ok(value) = env::var("WEBSOCKET_URL") {
        config.websocket_url = value;
    }
    if let Ok(value) = env::var("ASSET_IDS") {
        let assets: BTreeSet<_> = value.split(',').map(|s| s.trim().to_owned()).collect();
        ensure!(
            assets.iter().all(|id| !id.is_empty()
                && id.len() <= 100
                && id.bytes().all(|c| c.is_ascii_digit())),
            "invalid ASSET_IDS"
        );
        config.fixed_assets = Some(assets);
        eprintln!("ASSET_IDS set: restricted capture; automatic discovery disabled");
    }
    let path = env::var_os("OUTPUT_PATH").map(PathBuf::from);
    let stdout = match env::var("OUTPUT_STDOUT").as_deref() {
        Ok("1") => true,
        Ok("0") => false,
        Err(_) => path.is_none(),
        _ => anyhow::bail!("OUTPUT_STDOUT must be 0 or 1"),
    };
    ensure!(
        !stdout || !std::io::stdout().is_terminal(),
        "binary stdout requires redirection; set OUTPUT_PATH or redirect stdout"
    );
    let destination = match (path, stdout) {
        (Some(path), true) => Destination::Both(path),
        (Some(path), false) => Destination::File(path),
        (None, true) => Destination::Stdout,
        (None, false) => anyhow::bail!("OUTPUT_PATH required when stdout is disabled"),
    };
    run(config, destination.open()?, async {
        tokio::signal::ctrl_c().await?;
        eprintln!("SIGINT: closing sockets and draining output");
        Ok(())
    })
    .await?;
    eprintln!("capture closed cleanly");
    Ok(())
}
