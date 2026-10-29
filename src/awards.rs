use crate::io::{write_atomic, zstd};
use crate::s3::{SourceFile, column, member, open, optional, parse_id, typed};
use crate::works::{FileStats, entry, metadata, names, number};
use anyhow::{Context, Result};
use arrow_array::builder::{ListBuilder, StringBuilder, UInt16Builder, UInt64Builder};
use arrow_array::{Array, ArrayRef, Int64Array, ListArray, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use futures::TryStreamExt;
use object_store::ObjectStore;
use parquet::arrow::ArrowWriter;
use parquet::file::metadata::KeyValue;
use std::path::Path;
use std::sync::{Arc, LazyLock};

pub const AWARD_LEAVES: [&str; 11] = [
    "id",
    "display_name",
    "funder.id",
    "funder.display_name",
    "funder_scheme",
    "start_year",
    "end_year",
    "primary_topic.id",
    "primary_topic.subfield.id",
    "funded_outputs.list.element",
    "institution_awarded.list.element.id",
];

fn ids_field(name: &str) -> Field {
    Field::new(
        name,
        DataType::List(Arc::new(Field::new("item", DataType::UInt64, true))),
        false,
    )
}

pub static AWARD_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("award_id", DataType::UInt64, false),
        Field::new("title", DataType::Utf8, true),
        Field::new("funder_id", DataType::UInt64, true),
        Field::new("funder", DataType::Utf8, true),
        Field::new("scheme", DataType::Utf8, true),
        Field::new("start_year", DataType::UInt16, true),
        Field::new("end_year", DataType::UInt16, true),
        Field::new("topic_id", DataType::UInt64, true),
        Field::new("subfield_id", DataType::UInt64, true),
        ids_field("works"),
        ids_field("institutions"),
    ]))
});

pub static AWARD_COLUMNS: LazyLock<String> = LazyLock::new(|| names(&AWARD_SCHEMA));

fn ids(lists: &ListArray, values: &StringArray, row: usize) -> Result<Vec<u64>> {
    if !lists.is_valid(row) {
        return Ok(Vec::new());
    }
    let offsets = lists.value_offsets();
    (offsets[row] as usize..offsets[row + 1] as usize)
        .filter_map(|index| optional(values, index))
        .map(parse_id::<u64>)
        .collect()
}

fn year(years: &Int64Array, row: usize) -> Option<u16> {
    years
        .is_valid(row)
        .then(|| u16::try_from(years.value(row)).ok())
        .flatten()
}

pub fn award_rows(batch: &RecordBatch) -> Result<RecordBatch> {
    let id: &StringArray = column(batch, "id")?;
    let title: &StringArray = column(batch, "display_name")?;
    let funder_id: &StringArray = column(batch, "funder.id")?;
    let funder: &StringArray = column(batch, "funder.display_name")?;
    let scheme: &StringArray = column(batch, "funder_scheme")?;
    let start: &Int64Array = column(batch, "start_year")?;
    let end: &Int64Array = column(batch, "end_year")?;
    let topic: &StringArray = column(batch, "primary_topic.id")?;
    let subfield: &StringArray = column(batch, "primary_topic.subfield.id")?;
    let outputs: &ListArray = column(batch, "funded_outputs")?;
    let output_ids: &StringArray = typed(outputs.values().as_ref(), "funded_outputs")?;
    let awarded: &ListArray = column(batch, "institution_awarded")?;
    let awarded_ids: &StringArray = member(awarded.values(), "id")?;
    let mut award_ids = UInt64Builder::new();
    let mut titles = StringBuilder::new();
    let mut funder_ids = UInt64Builder::new();
    let mut funders = StringBuilder::new();
    let mut schemes = StringBuilder::new();
    let mut starts = UInt16Builder::new();
    let mut ends = UInt16Builder::new();
    let mut topics = UInt64Builder::new();
    let mut subfields = UInt64Builder::new();
    let mut works = ListBuilder::new(UInt64Builder::new());
    let mut institutions = ListBuilder::new(UInt64Builder::new());
    let parsed = |values: &StringArray, row: usize| -> Result<Option<u64>> {
        optional(values, row).map(parse_id::<u64>).transpose()
    };
    for row in 0..batch.num_rows() {
        let funded = ids(outputs, output_ids, row)?;
        if funded.is_empty() {
            continue;
        }
        award_ids.append_value(parse_id::<u64>(
            optional(id, row).context("award without id")?,
        )?);
        titles.append_option(optional(title, row));
        funder_ids.append_option(parsed(funder_id, row)?);
        funders.append_option(optional(funder, row));
        schemes.append_option(optional(scheme, row));
        starts.append_option(year(start, row));
        ends.append_option(year(end, row));
        topics.append_option(parsed(topic, row)?);
        subfields.append_option(parsed(subfield, row)?);
        works.values().append_slice(&funded);
        works.append(true);
        institutions
            .values()
            .append_slice(&ids(awarded, awarded_ids, row)?);
        institutions.append(true);
    }
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(award_ids.finish()),
        Arc::new(titles.finish()),
        Arc::new(funder_ids.finish()),
        Arc::new(funders.finish()),
        Arc::new(schemes.finish()),
        Arc::new(starts.finish()),
        Arc::new(ends.finish()),
        Arc::new(topics.finish()),
        Arc::new(subfields.finish()),
        Arc::new(works.finish()),
        Arc::new(institutions.finish()),
    ];
    Ok(RecordBatch::try_new(Arc::clone(&AWARD_SCHEMA), arrays)?)
}

fn stats(records: u64, awards: u64, works: u64, bytes: u64) -> FileStats {
    FileStats {
        records,
        works,
        rows: awards,
        bytes,
        ..FileStats::default()
    }
}

pub async fn award_file(
    store: &Arc<dyn ObjectStore>,
    source: &SourceFile,
    path: &Path,
) -> Result<FileStats> {
    let (records, batches) = read_awards(store, source)
        .await
        .with_context(|| format!("reading {}", source.location))?;
    let awards: u64 = batches.iter().map(|batch| batch.num_rows() as u64).sum();
    let works: u64 = batches
        .iter()
        .map(|batch| {
            column::<ListArray>(batch, "works").map_or(0, |lists| lists.values().len() as u64)
        })
        .sum();
    write_atomic(path, |w| {
        let mut writer = ArrowWriter::try_new(w, Arc::clone(&AWARD_SCHEMA), Some(zstd()))?;
        for batch in &batches {
            writer.write(batch)?;
        }
        for (key, value) in [
            ("columns", AWARD_COLUMNS.to_string()),
            ("records", records.to_string()),
            ("awards", awards.to_string()),
            ("works", works.to_string()),
        ] {
            writer.append_key_value_metadata(KeyValue::new(key.to_string(), value));
        }
        writer.close()?;
        Ok(())
    })?;
    let bytes = std::fs::metadata(path)
        .with_context(|| format!("reading {}", path.display()))?
        .len();
    Ok(stats(records, awards, works, bytes))
}

async fn read_awards(
    store: &Arc<dyn ObjectStore>,
    source: &SourceFile,
) -> Result<(u64, Vec<RecordBatch>)> {
    let mut stream = open(store, source, &AWARD_LEAVES).await?;
    let mut records = 0u64;
    let mut batches = Vec::new();
    while let Some(batch) = stream.try_next().await? {
        records += batch.num_rows() as u64;
        batches.push(award_rows(&batch)?);
    }
    source.check_records(records)?;
    Ok((records, batches))
}

pub fn completed_awards(path: &Path) -> Option<FileStats> {
    let (bytes, entries) = metadata(path)?;
    if entry(&entries, "columns")? != AWARD_COLUMNS.as_str() {
        return None;
    }
    Some(stats(
        number(&entries, "records")?,
        number(&entries, "awards")?,
        number(&entries, "works")?,
        bytes,
    ))
}
