use crate::io::{write_atomic, zstd};
use crate::s3::{SourceFile, column, member, open, optional, parse_id};
use crate::works::{FileStats, entry, kept, metadata, names, number, position_code, years};
use anyhow::{Context, Result};
use arrow_array::builder::UInt64Builder;
use arrow_array::{
    ArrayRef, BooleanArray, Int32Array, ListArray, RecordBatch, StringArray, UInt8Array,
    UInt16Array, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use futures::TryStreamExt;
use object_store::ObjectStore;
use parquet::arrow::ArrowWriter;
use parquet::file::metadata::KeyValue;
use std::ops::Range;
use std::path::Path;
use std::sync::{Arc, LazyLock};

pub const AUTHORSHIP_LEAVES: [&str; 5] = [
    "id",
    "publication_year",
    "is_xpac",
    "authorships.list.element.author.id",
    "authorships.list.element.author_position",
];

pub static AUTHORSHIP_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("work_id", DataType::UInt64, false),
        Field::new("rank", DataType::UInt16, false),
        Field::new("author_id", DataType::UInt64, true),
        Field::new("position", DataType::UInt8, false),
    ]))
});

pub static AUTHORSHIP_COLUMNS: LazyLock<String> = LazyLock::new(|| names(&AUTHORSHIP_SCHEMA));

pub fn authorship_rows(batch: &RecordBatch, years: &Range<i32>) -> Result<(RecordBatch, u64)> {
    let id: &StringArray = column(batch, "id")?;
    let year: &Int32Array = column(batch, "publication_year")?;
    let xpac: &BooleanArray = column(batch, "is_xpac")?;
    let authorships: &ListArray = column(batch, "authorships")?;
    let author: &StringArray = member(authorships.values(), "author.id")?;
    let position: &StringArray = member(authorships.values(), "author_position")?;
    let offsets = authorships.value_offsets();
    let mut kept_works = 0u64;
    let mut works = Vec::new();
    let mut ranks = Vec::new();
    let mut authors = UInt64Builder::new();
    let mut positions = Vec::new();
    for index in (0..batch.num_rows()).filter(|&index| kept(years, xpac, year, index)) {
        let work = parse_id::<u64>(optional(id, index).context("work without id")?)?;
        kept_works += 1;
        let start = offsets[index] as usize;
        for authorship in start..offsets[index + 1] as usize {
            works.push(work);
            ranks.push(u16::try_from(authorship - start).unwrap_or(u16::MAX));
            authors.append_option(
                optional(author, authorship)
                    .map(parse_id::<u64>)
                    .transpose()?,
            );
            positions.push(position_code(optional(position, authorship)));
        }
    }
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(UInt64Array::from(works)),
        Arc::new(UInt16Array::from(ranks)),
        Arc::new(authors.finish()),
        Arc::new(UInt8Array::from(positions)),
    ];
    Ok((
        RecordBatch::try_new(Arc::clone(&AUTHORSHIP_SCHEMA), arrays)?,
        kept_works,
    ))
}

fn stats(records: u64, works: u64, rows: u64, bytes: u64) -> FileStats {
    FileStats {
        records,
        works,
        rows,
        bytes,
        ..FileStats::default()
    }
}

pub async fn authorship_file(
    store: &Arc<dyn ObjectStore>,
    source: &SourceFile,
    path: &Path,
    min_year: u16,
    max_year: u16,
) -> Result<FileStats> {
    let years = years(min_year, max_year);
    let (records, works, batches) = read_authorships(store, source, &years)
        .await
        .with_context(|| format!("reading {}", source.location))?;
    let rows: u64 = batches.iter().map(|batch| batch.num_rows() as u64).sum();
    write_atomic(path, |w| {
        let mut writer = ArrowWriter::try_new(w, Arc::clone(&AUTHORSHIP_SCHEMA), Some(zstd()))?;
        for batch in &batches {
            writer.write(batch)?;
        }
        for (key, value) in [
            ("columns", AUTHORSHIP_COLUMNS.to_string()),
            ("records", records.to_string()),
            ("works", works.to_string()),
            ("rows", rows.to_string()),
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
    Ok(stats(records, works, rows, bytes))
}

async fn read_authorships(
    store: &Arc<dyn ObjectStore>,
    source: &SourceFile,
    years: &Range<i32>,
) -> Result<(u64, u64, Vec<RecordBatch>)> {
    let mut stream = open(store, source, &AUTHORSHIP_LEAVES).await?;
    let mut records = 0u64;
    let mut works = 0u64;
    let mut batches = Vec::new();
    while let Some(batch) = stream.try_next().await? {
        records += batch.num_rows() as u64;
        let (rows, kept_works) = authorship_rows(&batch, years)?;
        works += kept_works;
        batches.push(rows);
    }
    source.check_records(records)?;
    Ok((records, works, batches))
}

pub fn completed_authorships(path: &Path, min_year: u16, max_year: u16) -> Option<FileStats> {
    let (bytes, entries) = metadata(path)?;
    let complete = entry(&entries, "columns")? == AUTHORSHIP_COLUMNS.as_str()
        && number(&entries, "min_year")? == u64::from(min_year)
        && number(&entries, "max_year")? == u64::from(max_year);
    if !complete {
        return None;
    }
    Some(stats(
        number(&entries, "records")?,
        number(&entries, "works")?,
        number(&entries, "rows")?,
        bytes,
    ))
}
