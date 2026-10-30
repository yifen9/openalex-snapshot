use crate::awards::{award_file, completed_awards};
use crate::config::Config;
use crate::index::{completed_index, index_file};
use crate::io::{write_atomic, write_json};
use crate::links::{LinkOutputs, completed_links, link_file};
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
const FUNDERS: &[Leaf] = &[
    ("id", "id"),
    ("display_name", "display_name"),
    ("country_code", "country_code"),
];
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
            ("issn_l", "issn_l"),
            ("issn.list.element", "issn"),
            ("is_core", "is_core"),
            ("is_oa", "is_oa"),
            ("is_in_doaj", "is_in_doaj"),
            ("is_preprint_repository", "is_preprint_repository"),
            ("ids.wikidata", "wikidata"),
            ("homepage_url", "homepage_url"),
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

trait Summary: Default + Serialize {
    const NAME: &'static str;
    fn add(&mut self, stats: &FileStats);
}

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

impl Summary for WorksSummary {
    const NAME: &'static str = "works";

    fn add(&mut self, stats: &FileStats) {
        self.files += 1;
        self.records += stats.records;
        self.works += stats.works;
        self.rows += stats.rows;
        self.topics += stats.topics;
        self.titles += stats.titles;
        self.bytes += stats.bytes;
    }
}

#[derive(Debug, Default, Serialize)]
struct IndexSummary {
    files: u64,
    records: u64,
    works: u64,
    bytes: u64,
}

impl Summary for IndexSummary {
    const NAME: &'static str = "index";

    fn add(&mut self, stats: &FileStats) {
        self.files += 1;
        self.records += stats.records;
        self.works += stats.works;
        self.bytes += stats.bytes;
    }
}

#[derive(Debug, Default, Serialize)]
struct AwardsSummary {
    files: u64,
    records: u64,
    awards: u64,
    works: u64,
    bytes: u64,
}

#[derive(Debug, Default, Serialize)]
struct LinksSummary {
    files: u64,
    records: u64,
    works: u64,
    references: u64,
    bytes: u64,
}

impl Summary for LinksSummary {
    const NAME: &'static str = "links";

    fn add(&mut self, stats: &FileStats) {
        self.files += 1;
        self.records += stats.records;
        self.works += stats.works;
        self.references += stats.rows;
        self.bytes += stats.bytes;
    }
}

impl Summary for AwardsSummary {
    const NAME: &'static str = "awards";

    fn add(&mut self, stats: &FileStats) {
        self.files += 1;
        self.records += stats.records;
        self.awards += stats.rows;
        self.works += stats.works;
        self.bytes += stats.bytes;
    }
}

pub fn run(config: &Config, index: bool, awards: bool, links: bool) -> Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(extract(config, index, awards, links))
}

async fn extract(config: &Config, index: bool, awards: bool, links: bool) -> Result<()> {
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
    let works = manifest.sources("works", &snapshot.bucket)?;
    if links {
        return extract_links(&store, works, &dir, config).await;
    }
    if index {
        return extract_index(&store, works, &dir, config).await;
    }
    if awards {
        extract_lookup(
            &store,
            manifest.sources("funders", &snapshot.bucket)?,
            FUNDERS,
            &dir.join("funders.parquet"),
            snapshot.concurrency,
        )
        .await?;
        let sources = manifest.sources("awards", &snapshot.bucket)?;
        return extract_awards(&store, sources, &dir, config.snapshot.concurrency).await;
    }
    extract_works(&store, works, &dir, config).await
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
    extract_files::<WorksSummary, _>(sources, dir, config.snapshot.concurrency, |source| {
        let store = Arc::clone(store);
        let outputs = Outputs::new(dir, &source.name);
        async move {
            let completed = completed_stats(&outputs, min_year, max_year);
            with_retries(&source, completed, || {
                extract_file(&store, &source, &outputs, min_year, max_year)
            })
            .await
        }
    })
    .await
}

async fn extract_index(
    store: &Arc<dyn ObjectStore>,
    sources: Vec<SourceFile>,
    dir: &Path,
    config: &Config,
) -> Result<()> {
    let (min_year, max_year) = (config.corpus.min_year, config.corpus.max_year);
    extract_files::<IndexSummary, _>(sources, dir, config.snapshot.concurrency, |source| {
        let store = Arc::clone(store);
        let path = dir.join("index").join(&source.name);
        async move {
            let completed = completed_index(&path, min_year, max_year);
            with_retries(&source, completed, || {
                index_file(&store, &source, &path, min_year, max_year)
            })
            .await
        }
    })
    .await
}

async fn extract_links(
    store: &Arc<dyn ObjectStore>,
    sources: Vec<SourceFile>,
    dir: &Path,
    config: &Config,
) -> Result<()> {
    let (min_year, max_year) = (config.corpus.min_year, config.corpus.max_year);
    extract_files::<LinksSummary, _>(sources, dir, config.snapshot.concurrency, |source| {
        let store = Arc::clone(store);
        let outputs = LinkOutputs::new(dir, &source.name);
        async move {
            let completed = completed_links(&outputs, min_year, max_year);
            with_retries(&source, completed, || {
                link_file(&store, &source, &outputs, min_year, max_year)
            })
            .await
        }
    })
    .await
}

async fn extract_awards(
    store: &Arc<dyn ObjectStore>,
    sources: Vec<SourceFile>,
    dir: &Path,
    concurrency: usize,
) -> Result<()> {
    extract_files::<AwardsSummary, _>(sources, dir, concurrency, |source| {
        let store = Arc::clone(store);
        let path = dir.join("awards").join(&source.name);
        async move {
            let completed = completed_awards(&path);
            with_retries(&source, completed, || award_file(&store, &source, &path)).await
        }
    })
    .await
}

async fn extract_files<S: Summary, F>(
    sources: Vec<SourceFile>,
    dir: &Path,
    concurrency: usize,
    mut task: impl FnMut(SourceFile) -> F,
) -> Result<()>
where
    F: Future<Output = Result<(FileStats, u64)>> + Send + 'static,
{
    let total = sources.len();
    let started = Instant::now();
    let mut tasks = futures::stream::iter(sources)
        .map(|source| {
            let name = source.name.clone();
            let handle = tokio::spawn(task(source));
            async move { (name, handle.await) }
        })
        .buffer_unordered(concurrency);
    let mut summary = S::default();
    let mut rows = 0u64;
    let mut fetched = 0u64;
    let mut failed = Vec::new();
    let mut done = 0usize;
    while let Some((name, joined)) = tasks.next().await {
        match joined? {
            Ok((stats, bytes)) => {
                summary.add(&stats);
                rows += stats.rows;
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
                "{} {done}/{total} files, {rows} rows, {elapsed:.0}s, {:.1} MB/s",
                S::NAME,
                fetched as f64 / 1e6 / elapsed.max(1e-9)
            );
        }
    }
    write_json(&dir.join(format!("{}-summary.json", S::NAME)), &summary)?;
    if !failed.is_empty() {
        failed.sort();
        bail!(
            "{} of {total} {} files failed, rerun to resume: {}",
            failed.len(),
            S::NAME,
            failed.join(", ")
        );
    }
    Ok(())
}

async fn with_retries<F>(
    source: &SourceFile,
    completed: Option<FileStats>,
    mut extract: impl FnMut() -> F,
) -> Result<(FileStats, u64)>
where
    F: Future<Output = Result<FileStats>>,
{
    if let Some(stats) = completed
        && stats.records == source.records
    {
        return Ok((stats, 0));
    }
    let mut attempt = 0;
    loop {
        match extract().await {
            Ok(stats) => return Ok((stats, source.size)),
            Err(error) if attempt < RETRIES => {
                attempt += 1;
                eprintln!("retrying {} ({attempt}/{RETRIES}): {error:#}", source.name);
            }
            Err(error) => return Err(error),
        }
    }
}
