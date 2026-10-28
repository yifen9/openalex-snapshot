use crate::io::{write_atomic, zstd};
use crate::s3::{SourceFile, column, open, optional, parse_id};
use crate::works::{FileStats, bare_doi, entry, kept, metadata, names, number, years};
use anyhow::{Context, Result};
use arrow_array::builder::StringBuilder;
use arrow_array::{
    ArrayRef, BooleanArray, Int32Array, RecordBatch, StringArray, UInt16Array, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use futures::TryStreamExt;
use object_store::ObjectStore;
use parquet::arrow::ArrowWriter;
use parquet::file::metadata::KeyValue;
use std::ops::Range;
use std::path::Path;
use std::sync::{Arc, LazyLock};

pub const INDEX_LEAVES: [&str; 5] = ["id", "doi", "publication_year", "type", "is_xpac"];

pub static INDEX_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("work_id", DataType::UInt64, false),
        Field::new("year", DataType::UInt16, false),
        Field::new("work_type", DataType::Utf8, false),
        Field::new("doi", DataType::Utf8, true),
    ]))
});

pub static INDEX_COLUMNS: LazyLock<String> = LazyLock::new(|| names(&INDEX_SCHEMA));

pub fn index_rows(batch: &RecordBatch, years: &Range<i32>) -> Result<RecordBatch> {
    let id: &StringArray = column(batch, "id")?;
    let doi: &StringArray = column(batch, "doi")?;
    let year: &Int32Array = column(batch, "publication_year")?;
    let work_type: &StringArray = column(batch, "type")?;
    let xpac: &BooleanArray = column(batch, "is_xpac")?;
    let mut works = Vec::new();
    let mut kept_years = Vec::new();
    let mut types = StringBuilder::new();
    let mut dois = StringBuilder::new();
    for index in (0..batch.num_rows()).filter(|&index| kept(years, xpac, year, index)) {
        works.push(parse_id::<u64>(
            optional(id, index).context("work without id")?,
        )?);
        kept_years.push(u16::try_from(year.value(index))?);
        types.append_value(optional(work_type, index).unwrap_or_default());
        dois.append_option(optional(doi, index).map(bare_doi));
    }
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(UInt64Array::from(works)),
        Arc::new(UInt16Array::from(kept_years)),
        Arc::new(types.finish()),
        Arc::new(dois.finish()),
    ];
    Ok(RecordBatch::try_new(Arc::clone(&INDEX_SCHEMA), arrays)?)
}

fn stats(records: u64, works: u64, bytes: u64) -> FileStats {
    FileStats {
        records,
        works,
        rows: works,
        bytes,
        ..FileStats::default()
    }
}

pub async fn index_file(
    store: &Arc<dyn ObjectStore>,
    source: &SourceFile,
    path: &Path,
    min_year: u16,
    max_year: u16,
) -> Result<FileStats> {
    let years = years(min_year, max_year);
    let (records, batches) = read_index(store, source, &years)
        .await
        .with_context(|| format!("indexing {}", source.location))?;
    let works: u64 = batches.iter().map(|batch| batch.num_rows() as u64).sum();
    write_atomic(path, |w| {
        let mut writer = ArrowWriter::try_new(w, Arc::clone(&INDEX_SCHEMA), Some(zstd()))?;
        for batch in &batches {
            writer.write(batch)?;
        }
        for (key, value) in [
            ("columns", INDEX_COLUMNS.to_string()),
            ("records", records.to_string()),
            ("works", works.to_string()),
            ("min_year", min_year.to_string()),
            ("max_year", max_year.to_string()),
        ] {
            writer.append_key_value_metadata(KeyValue::new(key.to_string(), value));
        }
        writer.close()?;
        Ok(())
    })?;
    let bytes = std::fs::metadata(path)
        .with_context(|| format!("reading {}", path.display()))?
        .len();
    Ok(stats(records, works, bytes))
}

async fn read_index(
    store: &Arc<dyn ObjectStore>,
    source: &SourceFile,
    years: &Range<i32>,
) -> Result<(u64, Vec<RecordBatch>)> {
    let mut stream = open(store, source, &INDEX_LEAVES).await?;
    let mut records = 0u64;
    let mut batches = Vec::new();
    while let Some(batch) = stream.try_next().await? {
        records += batch.num_rows() as u64;
        batches.push(index_rows(&batch, years)?);
    }
    source.check_records(records)?;
    Ok((records, batches))
}

pub fn completed_index(path: &Path, min_year: u16, max_year: u16) -> Option<FileStats> {
    let (bytes, entries) = metadata(path)?;
    let complete = entry(&entries, "columns")? == INDEX_COLUMNS.as_str()
        && number(&entries, "min_year")? == u64::from(min_year)
        && number(&entries, "max_year")? == u64::from(max_year);
    if !complete {
        return None;
    }
    Some(stats(
        number(&entries, "records")?,
        number(&entries, "works")?,
        bytes,
    ))
}
