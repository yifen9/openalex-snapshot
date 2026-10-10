use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub root: PathBuf,
    pub snapshot: Snapshot,
    pub corpus: Corpus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Snapshot {
    pub release: String,
    pub bucket: String,
    pub region: String,
    pub prefix: String,
    pub concurrency: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Corpus {
    pub min_year: u16,
    pub max_year: u16,
}

impl Config {
    pub fn load(path: &Path, release: Option<String>) -> Result<Self> {
        let text =
            std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
        let mut config: Config =
            toml::from_str(&text).with_context(|| format!("parsing {}", path.display()))?;
        if let Some(release) = release {
            config.snapshot.release = release;
        }
        if config.corpus.min_year > config.corpus.max_year {
            bail!("corpus.min_year is after corpus.max_year");
        }
        Ok(config)
    }

    pub fn snapshot_dir(&self) -> PathBuf {
        let root = match (self.root.strip_prefix("~"), std::env::var_os("HOME")) {
            (Ok(rest), Some(home)) => PathBuf::from(home).join(rest),
            _ => self.root.clone(),
        };
        root.join(&self.snapshot.release)
    }
}
