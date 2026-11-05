use crate::io::{write_atomic, zstd};
use crate::s3::{SourceFile, column, open, optional, parse_id};
use crate::works::{FileStats, bare_orcid, entry, metadata, names, number};
use anyhow::{Context, Result};
use arrow_array::builder::{StringBuilder, UInt32Builder, UInt64Builder};
use arrow_array::{Array, ArrayRef, Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use futures::TryStreamExt;
use object_store::ObjectStore;
use parquet::arrow::ArrowWriter;
use parquet::file::metadata::KeyValue;
use std::path::Path;
use std::sync::{Arc, LazyLock};

pub const AUTHOR_LEAVES: [&str; 4] = ["id", "orcid", "display_name", "works_count"];

pub static AUTHOR_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("orcid_id", DataType::Utf8, true),
        Field::new("display_name", DataType::Utf8, true),
        Field::new("works_count", DataType::UInt32, true),
    ]))
});

pub static AUTHOR_COLUMNS: LazyLock<String> = LazyLock::new(|| names(&AUTHOR_SCHEMA));

pub fn author_rows(batch: &RecordBatch) -> Result<RecordBatch> {
    let id: &StringArray = column(batch, "id")?;
    let orcid: &StringArray = column(batch, "orcid")?;
    let display_name: &StringArray = column(batch, "display_name")?;
    let works_count: &Int32Array = column(batch, "works_count")?;
    let mut ids = UInt64Builder::new();
    let mut orcids = StringBuilder::new();
    let mut display_names = StringBuilder::new();
    let mut counts = UInt32Builder::new();
    for row in 0..batch.num_rows() {
        ids.append_value(parse_id::<u64>(
            optional(id, row).context("author without id")?,
        )?);
        orcids.append_option(
            optional(orcid, row)
                .map(bare_orcid)
                .filter(|bare| !bare.is_empty())
                .map(str::to_ascii_uppercase),
        );
        display_names.append_option(optional(display_name, row));
        counts.append_option(
            works_count
                .is_valid(row)
                .then(|| u32::try_from(works_count.value(row)).ok())
                .flatten(),
        );
    }
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(ids.finish()),
        Arc::new(orcids.finish()),
        Arc::new(display_names.finish()),
        Arc::new(counts.finish()),
    ];
    Ok(RecordBatch::try_new(Arc::clone(&AUTHOR_SCHEMA), arrays)?)
}

fn stats(records: u64, orcids: u64, bytes: u64) -> FileStats {
    FileStats {
        records,
        rows: records,
        orcids,
        bytes,
        ..FileStats::default()
    }
}

pub async fn author_file(
    store: &Arc<dyn ObjectStore>,
    source: &SourceFile,
    path: &Path,
) -> Result<FileStats> {
    let (records, batches) = read_authors(store, source)
        .await
        .with_context(|| format!("reading {}", source.location))?;
    let orcids: u64 = batches
        .iter()
        .map(|batch| {
            column::<StringArray>(batch, "orcid_id")
                .map_or(0, |orcids| (orcids.len() - orcids.null_count()) as u64)
        })
        .sum();
    write_atomic(path, |w| {
        let mut writer = ArrowWriter::try_new(w, Arc::clone(&AUTHOR_SCHEMA), Some(zstd()))?;
        for batch in &batches {
            writer.write(batch)?;
        }
        for (key, value) in [
            ("columns", AUTHOR_COLUMNS.to_string()),
            ("records", records.to_string()),
            ("orcids", orcids.to_string()),
        ] {
            writer.append_key_value_metadata(KeyValue::new(key.to_string(), value));
        }
        writer.close()?;
        Ok(())
    })?;
    let bytes = std::fs::metadata(path)
        .with_context(|| format!("reading {}", path.display()))?
        .len();
    Ok(stats(records, orcids, bytes))
}

async fn read_authors(
    store: &Arc<dyn ObjectStore>,
    source: &SourceFile,
) -> Result<(u64, Vec<RecordBatch>)> {
    let mut stream = open(store, source, &AUTHOR_LEAVES).await?;
    let mut records = 0u64;
    let mut batches = Vec::new();
    while let Some(batch) = stream.try_next().await? {
        records += batch.num_rows() as u64;
        batches.push(author_rows(&batch)?);
    }
    source.check_records(records)?;
    Ok((records, batches))
}

pub fn completed_authors(path: &Path) -> Option<FileStats> {
    let (bytes, entries) = metadata(path)?;
    if entry(&entries, "columns")? != AUTHOR_COLUMNS.as_str() {
        return None;
    }
    Some(stats(
        number(&entries, "records")?,
        number(&entries, "orcids")?,
        bytes,
    ))
}
