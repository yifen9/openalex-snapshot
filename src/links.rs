use crate::io::{write_atomic, zstd};
use crate::s3::{SourceFile, column, open, optional, parse_id, typed};
use crate::works::{FileStats, entry, kept, metadata, names, number, years};
use anyhow::{Context, Result};
use arrow_array::{
    Array, ArrayRef, BooleanArray, Int32Array, ListArray, RecordBatch, StringArray, StructArray,
    UInt64Array,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use futures::TryStreamExt;
use object_store::ObjectStore;
use parquet::arrow::ArrowWriter;
use parquet::file::metadata::KeyValue;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock};

pub const LINK_LEAVES: [&str; 7] = [
    "id",
    "publication_year",
    "is_xpac",
    "referenced_works.list.element",
    "primary_location.source.id",
    "is_retracted",
    "is_paratext",
];

pub static REFERENCE_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("work_id", DataType::UInt64, false),
        Field::new("referenced_work_id", DataType::UInt64, false),
    ]))
});

pub static WORK_SOURCE_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("work_id", DataType::UInt64, false),
        Field::new("source_id", DataType::UInt64, true),
        Field::new("is_retracted", DataType::Boolean, true),
        Field::new("is_paratext", DataType::Boolean, true),
    ]))
});

pub static REFERENCE_COLUMNS: LazyLock<String> = LazyLock::new(|| names(&REFERENCE_SCHEMA));
pub static WORK_SOURCE_COLUMNS: LazyLock<String> = LazyLock::new(|| names(&WORK_SOURCE_SCHEMA));

pub struct LinkOutputs {
    pub references: PathBuf,
    pub work_sources: PathBuf,
}

impl LinkOutputs {
    pub fn new(dir: &Path, name: &str) -> Self {
        LinkOutputs {
            references: dir.join("references").join(name),
            work_sources: dir.join("work_sources").join(name),
        }
    }
}

#[derive(Default)]
pub struct Links {
    pairs: Vec<(u64, u64)>,
    works: Vec<u64>,
    sources: Vec<Option<u64>>,
    retracted: Vec<Option<bool>>,
    paratext: Vec<Option<bool>>,
}

fn flag(array: &BooleanArray, index: usize) -> Option<bool> {
    array.is_valid(index).then(|| array.value(index))
}

impl Links {
    pub fn push(&mut self, batch: &RecordBatch, years: &Range<i32>) -> Result<()> {
        let id: &StringArray = column(batch, "id")?;
        let year: &Int32Array = column(batch, "publication_year")?;
        let xpac: &BooleanArray = column(batch, "is_xpac")?;
        let location: &StructArray = column(batch, "primary_location")?;
        let source: &StructArray = column(batch, "primary_location.source")?;
        let source_id: &StringArray = column(batch, "primary_location.source.id")?;
        let retracted: &BooleanArray = column(batch, "is_retracted")?;
        let paratext: &BooleanArray = column(batch, "is_paratext")?;
        let references: &ListArray = column(batch, "referenced_works")?;
        let targets: &StringArray = typed(references.values().as_ref(), "referenced_works")?;
        let offsets = references.value_offsets();
        for index in (0..batch.num_rows()).filter(|&index| kept(years, xpac, year, index)) {
            let work = parse_id::<u64>(optional(id, index).context("work without id")?)?;
            self.works.push(work);
            self.sources.push(
                (location.is_valid(index) && source.is_valid(index))
                    .then(|| optional(source_id, index))
                    .flatten()
                    .map(parse_id)
                    .transpose()?,
            );
            self.retracted.push(flag(retracted, index));
            self.paratext.push(flag(paratext, index));
            if references.is_valid(index) {
                for slot in offsets[index] as usize..offsets[index + 1] as usize {
                    if let Some(target) = optional(targets, slot) {
                        self.pairs.push((work, parse_id(target)?));
                    }
                }
            }
        }
        Ok(())
    }

    pub fn finish(mut self) -> Result<(RecordBatch, RecordBatch)> {
        self.pairs.sort_unstable();
        let (citing, cited): (Vec<u64>, Vec<u64>) = self.pairs.into_iter().unzip();
        let references: Vec<ArrayRef> = vec![
            Arc::new(UInt64Array::from(citing)),
            Arc::new(UInt64Array::from(cited)),
        ];
        let work_sources: Vec<ArrayRef> = vec![
            Arc::new(UInt64Array::from(self.works)),
            Arc::new(UInt64Array::from(self.sources)),
            Arc::new(BooleanArray::from(self.retracted)),
            Arc::new(BooleanArray::from(self.paratext)),
        ];
        Ok((
            RecordBatch::try_new(Arc::clone(&REFERENCE_SCHEMA), references)?,
            RecordBatch::try_new(Arc::clone(&WORK_SOURCE_SCHEMA), work_sources)?,
        ))
    }
}

fn stats(records: u64, works: u64, references: u64, bytes: u64) -> FileStats {
    FileStats {
        records,
        works,
        rows: references,
        bytes,
        ..FileStats::default()
    }
}

fn write(path: &Path, batch: &RecordBatch, entries: &[(&str, String)]) -> Result<u64> {
    write_atomic(path, |w| {
        let mut writer = ArrowWriter::try_new(w, batch.schema(), Some(zstd()))?;
        writer.write(batch)?;
        for (key, value) in entries {
            writer.append_key_value_metadata(KeyValue::new(key.to_string(), value.clone()));
        }
        writer.close()?;
        Ok(())
    })?;
    Ok(std::fs::metadata(path)
        .with_context(|| format!("reading {}", path.display()))?
        .len())
}

pub async fn link_file(
    store: &Arc<dyn ObjectStore>,
    source: &SourceFile,
    outputs: &LinkOutputs,
    min_year: u16,
    max_year: u16,
) -> Result<FileStats> {
    let years = years(min_year, max_year);
    let mut stream = open(store, source, &LINK_LEAVES)
        .await
        .with_context(|| format!("linking {}", source.location))?;
    let mut links = Links::default();
    let mut records = 0u64;
    while let Some(batch) = stream.try_next().await? {
        records += batch.num_rows() as u64;
        links.push(&batch, &years)?;
    }
    source.check_records(records)?;
    let (references, work_sources) = links.finish()?;
    let works = work_sources.num_rows() as u64;
    let count = references.num_rows() as u64;
    let bytes = write(
        &outputs.work_sources,
        &work_sources,
        &[
            ("columns", WORK_SOURCE_COLUMNS.to_string()),
            ("works", works.to_string()),
        ],
    )? + write(
        &outputs.references,
        &references,
        &[
            ("columns", REFERENCE_COLUMNS.to_string()),
            ("records", records.to_string()),
            ("works", works.to_string()),
            ("references", count.to_string()),
            ("min_year", min_year.to_string()),
            ("max_year", max_year.to_string()),
        ],
    )?;
    Ok(stats(records, works, count, bytes))
}

pub fn completed_links(outputs: &LinkOutputs, min_year: u16, max_year: u16) -> Option<FileStats> {
    let (sources_bytes, sources) = metadata(&outputs.work_sources)?;
    let (references_bytes, references) = metadata(&outputs.references)?;
    let complete = entry(&references, "columns")? == REFERENCE_COLUMNS.as_str()
        && entry(&sources, "columns")? == WORK_SOURCE_COLUMNS.as_str()
        && number(&references, "min_year")? == u64::from(min_year)
        && number(&references, "max_year")? == u64::from(max_year)
        && number(&references, "works")? == number(&sources, "works")?;
    if !complete {
        return None;
    }
    Some(stats(
        number(&references, "records")?,
        number(&references, "works")?,
        number(&references, "references")?,
        sources_bytes + references_bytes,
    ))
}
