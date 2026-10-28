mod config;
mod extract;
mod index;
mod io;
mod lookup;
mod s3;
mod works;

#[cfg(test)]
mod tests;

use anyhow::Result;
use clap::Parser;
use config::Config;
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(name = "openalex-snapshot")]
struct Args {
    #[arg(long, default_value = "config/snapshot.toml")]
    config: PathBuf,
    #[arg(long)]
    release: Option<String>,
    #[arg(long)]
    index: bool,
}

fn main() -> Result<()> {
    let args = Args::parse();
    extract::run(&Config::load(&args.config, args.release)?, args.index)
}
