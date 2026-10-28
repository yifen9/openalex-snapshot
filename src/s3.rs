use crate::config::Snapshot;
use anyhow::{Context, Result, bail};
use arrow_array::{Array, ArrayRef, RecordBatch, StringArray, StructArray};
use futures::Stream;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt};
use parquet::arrow::ProjectionMask;
use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
use parquet::errors::ParquetError;
use serde::Deserialize;
use std::str::FromStr;
use std::sync::Arc;

#[derive(Debug, Deserialize)]
pub struct Manifest {
    pub date: String,
    pub entities: Vec<Entity>,
}

#[derive(Debug, Deserialize)]
pub struct Entity {
    pub entity: String,
    pub files: Vec<EntityFile>,
}

#[derive(Debug, Deserialize)]
pub struct EntityFile {
    pub url: String,
    pub meta: FileMeta,
}

#[derive(Debug, Clone, Copy, Deserialize)]
pub struct FileMeta {
    pub content_length: u64,
    pub record_count: u64,
}

#[derive(Debug, Clone)]
pub struct SourceFile {
    pub location: ObjectPath,
    pub name: String,
    pub size: u64,
    pub records: u64,
}

impl SourceFile {
    pub fn check_records(&self, read: u64) -> Result<()> {
        if read != self.records {
            bail!(
                "read {read} records but the manifest lists {}",
                self.records
            );
        }
        Ok(())
    }
}

impl Manifest {
    pub fn parse(bytes: &[u8]) -> Result<Self> {
        serde_json::from_slice(bytes).context("parsing the snapshot manifest")
    }

    pub fn sources(&self, entity: &str, bucket: &str) -> Result<Vec<SourceFile>> {
        let files = self
            .entities
            .iter()
            .find(|e| e.entity == entity)
            .with_context(|| format!("the manifest lists no {entity}"))?;
        let scheme = format!("s3://{bucket}/");
        files
            .files
            .iter()
            .map(|file| source(file, &scheme))
            .collect()
    }
}

fn source(file: &EntityFile, scheme: &str) -> Result<SourceFile> {
    let Some(key) = file.url.strip_prefix(scheme) else {
        bail!("manifest url {} is outside {scheme}", file.url);
    };
    let partition = key
        .rsplit('/')
        .nth(1)
        .and_then(|dir| dir.strip_prefix("updated_date="));
    let stem = key
        .rsplit('/')
        .next()
        .and_then(|name| name.strip_suffix(".parquet"));
    let (Some(date), Some(stem)) = (partition, stem) else {
        bail!(
            "manifest url {} is not an updated_date parquet part",
            file.url
        );
    };
    Ok(SourceFile {
        location: ObjectPath::from(key),
        name: format!("{date}-{stem}.parquet"),
        size: file.meta.content_length,
        records: file.meta.record_count,
    })
}

pub fn store(snapshot: &Snapshot) -> Result<Arc<dyn ObjectStore>> {
    let store = AmazonS3Builder::new()
        .with_bucket_name(&snapshot.bucket)
        .with_region(&snapshot.region)
        .with_skip_signature(true)
        .build()?;
    Ok(Arc::new(store))
}

pub async fn manifest(
    store: &Arc<dyn ObjectStore>,
    snapshot: &Snapshot,
) -> Result<(Vec<u8>, Manifest)> {
    let location = ObjectPath::from(format!("{}/manifest.json", snapshot.prefix));
    let bytes = store
        .get(&location)
        .await
        .with_context(|| format!("fetching {location}"))?
        .bytes()
        .await?
        .to_vec();
    let manifest = Manifest::parse(&bytes)?;
    if manifest.date != snapshot.release {
        bail!(
            "the manifest is dated {} but the configured release is {}",
            manifest.date,
            snapshot.release
        );
    }
    Ok((bytes, manifest))
}

#[expect(deprecated)]
pub async fn open(
    store: &Arc<dyn ObjectStore>,
    source: &SourceFile,
    leaves: &[&str],
) -> Result<impl Stream<Item = Result<RecordBatch, ParquetError>> + Send + Unpin + use<>> {
    let reader = parquet::arrow::async_reader::ParquetObjectReader::new(
        Arc::clone(store),
        source.location.clone(),
    )
    .with_file_size(source.size);
    let builder = ParquetRecordBatchStreamBuilder::new(reader)
        .await
        .with_context(|| format!("reading the footer of {}", source.location))?;
    let schema = builder.parquet_schema();
    let indices = leaves
        .iter()
        .map(|leaf| {
            schema
                .columns()
                .iter()
                .position(|column| column.path().string() == *leaf)
                .with_context(|| format!("{} has no column {leaf}", source.location))
        })
        .collect::<Result<Vec<_>>>()?;
    let mask = ProjectionMask::leaves(schema, indices);
    Ok(builder.with_projection(mask).build()?)
}

pub fn typed<'a, T: Array + 'static>(array: &'a dyn Array, what: &str) -> Result<&'a T> {
    array.as_any().downcast_ref::<T>().with_context(|| {
        format!(
            "{what} has type {}, expected {}",
            array.data_type(),
            std::any::type_name::<T>()
        )
    })
}

pub fn column<'a, T: Array + 'static>(batch: &'a RecordBatch, path: &str) -> Result<&'a T> {
    let (first, rest) = path.split_once('.').unwrap_or((path, ""));
    let array = batch
        .column_by_name(first)
        .with_context(|| format!("batch has no column {first}"))?;
    member(array, rest)
}

pub fn member<'a, T: Array + 'static>(array: &'a ArrayRef, path: &str) -> Result<&'a T> {
    let mut current = array;
    for part in path.split('.').filter(|part| !part.is_empty()) {
        current = typed::<StructArray>(current.as_ref(), path)?
            .column_by_name(part)
            .with_context(|| format!("{path} has no member {part}"))?;
    }
    typed(current.as_ref(), path)
}

pub fn parse_id<T: FromStr>(value: &str) -> Result<T> {
    let tail = value.rsplit_once('/').map_or(value, |(_, tail)| tail);
    tail.trim_start_matches(|c: char| c.is_ascii_alphabetic())
        .parse()
        .ok()
        .with_context(|| format!("unparsable id {value:?}"))
}

pub fn optional(array: &StringArray, index: usize) -> Option<&str> {
    array.is_valid(index).then(|| array.value(index))
}
