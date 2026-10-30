mod awards;
mod config;
mod extract;
mod index;
mod io;
mod links;
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
    #[arg(long)]
    awards: bool,
    #[arg(long)]
    links: bool,
}

fn main() -> Result<()> {
    let args = Args::parse();
    extract::run(
        &Config::load(&args.config, args.release)?,
        args.index,
        args.awards,
        args.links,
    )
}
