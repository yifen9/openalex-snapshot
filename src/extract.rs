use crate::config::Config;
use crate::io::{write_atomic, write_json};
use crate::lookup::{Leaf, extract_lookup};
use crate::s3::{SourceFile, manifest, store};
use crate::works::{FileStats, Outputs, completed_stats, extract_file};
use anyhow::{Context, Result, bail};
use futures::StreamExt;
use object_store::ObjectStore;
use serde::Serialize;
use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

const RETRIES: u32 = 3;
const LOOKUPS: [(&str, &[Leaf]); 6] = [
    (
        "institutions",
        &[
            ("id", "id"),
            ("display_name", "display_name"),
            ("ror", "ror"),
            ("country_code", "country_code"),
            ("type", "type"),
            ("lineage.list.element", "lineage"),
            ("geo.latitude", "latitude"),
            ("geo.longitude", "longitude"),
            ("geo.geonames_city_id", "geonames_city_id"),
        ],
    ),
    (
        "sources",
        &[
            ("id", "id"),
            ("display_name", "display_name"),
            ("type", "type"),
            ("country_code", "country_code"),
        ],
    ),
    ("fields", &[("id", "id"), ("display_name", "display_name")]),
    (
        "subfields",
        &[("id", "id"), ("display_name", "display_name")],
    ),
    ("domains", &[("id", "id"), ("display_name", "display_name")]),
    (
        "topics",
        &[
            ("id", "id"),
            ("display_name", "display_name"),
            ("subfield.id", "subfield_id"),
            ("field.id", "field_id"),
            ("domain.id", "domain_id"),
        ],
    ),
];

#[derive(Debug, Default, Serialize)]
struct WorksSummary {
    files: u64,
    records: u64,
    works: u64,
    rows: u64,
    topics: u64,
    titles: u64,
    bytes: u64,
}

pub fn run(config: &Config) -> Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(extract(config))
}

async fn extract(config: &Config) -> Result<()> {
    let snapshot = &config.snapshot;
    let store = store(snapshot)?;
    let (bytes, manifest) = manifest(&store, snapshot).await?;
    let dir = config.snapshot_dir();
    keep_manifest(&dir.join("manifest.json"), &bytes)?;
    for (entity, leaves) in LOOKUPS {
        extract_lookup(
            &store,
            manifest.sources(entity, &snapshot.bucket)?,
            leaves,
            &dir.join(format!("{entity}.parquet")),
            snapshot.concurrency,
        )
        .await?;
    }
    extract_works(
        &store,
        manifest.sources("works", &snapshot.bucket)?,
        &dir,
        config,
    )
    .await
}

fn keep_manifest(path: &Path, bytes: &[u8]) -> Result<()> {
    match std::fs::read(path) {
        Ok(kept) if kept == bytes => Ok(()),
        Ok(_) => bail!("{} differs from the published manifest", path.display()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            write_atomic(path, |w| Ok(w.write_all(bytes)?))
        }
        Err(error) => Err(error).with_context(|| format!("reading {}", path.display())),
    }
}

async fn extract_works(
    store: &Arc<dyn ObjectStore>,
    sources: Vec<SourceFile>,
    dir: &Path,
    config: &Config,
) -> Result<()> {
    let (min_year, max_year) = (config.corpus.min_year, config.corpus.max_year);
    let concurrency = config.snapshot.concurrency;
    let total = sources.len();
    let started = Instant::now();
    let mut tasks = futures::stream::iter(sources)
        .map(|source| {
            let store = Arc::clone(store);
            let outputs = Outputs::new(dir, &source.name);
            tokio::spawn(async move {
                let outcome = with_retries(&store, &source, &outputs, min_year, max_year).await;
                (source.name.clone(), outcome)
            })
        })
        .buffer_unordered(concurrency);
    let mut summary = WorksSummary::default();
    let mut fetched = 0u64;
    let mut failed = Vec::new();
    let mut done = 0usize;
    while let Some(joined) = tasks.next().await {
        let (name, outcome) = joined?;
        match outcome {
            Ok((stats, bytes)) => {
                summary.files += 1;
                summary.records += stats.records;
                summary.works += stats.works;
                summary.rows += stats.rows;
                summary.topics += stats.topics;
                summary.titles += stats.titles;
                summary.bytes += stats.bytes;
                fetched += bytes;
            }
            Err(error) => {
                eprintln!("failed {name}: {error:#}");
                failed.push(name);
            }
        }
        done += 1;
        if done.is_multiple_of(concurrency) || done == total {
            let elapsed = started.elapsed().as_secs_f64();
            eprintln!(
                "works {done}/{total} files, {} rows, {elapsed:.0}s, {:.1} MB/s",
                summary.rows,
                fetched as f64 / 1e6 / elapsed.max(1e-9)
            );
        }
    }
    write_json(&dir.join("works-summary.json"), &summary)?;
    if !failed.is_empty() {
        failed.sort();
        bail!(
            "{} of {total} works files failed, rerun to resume: {}",
            failed.len(),
            failed.join(", ")
        );
    }
    Ok(())
}

async fn with_retries(
    store: &Arc<dyn ObjectStore>,
    source: &SourceFile,
    outputs: &Outputs,
    min_year: u16,
    max_year: u16,
) -> Result<(FileStats, u64)> {
    if let Some(stats) = completed_stats(outputs, min_year, max_year)
        && stats.records == source.records
    {
        return Ok((stats, 0));
    }
    let mut attempt = 0;
    loop {
        match extract_file(store, source, outputs, min_year, max_year).await {
            Ok(stats) => return Ok((stats, source.size)),
            Err(error) if attempt < RETRIES => {
                attempt += 1;
                eprintln!("retrying {} ({attempt}/{RETRIES}): {error:#}", source.name);
            }
            Err(error) => return Err(error),
        }
    }
}
