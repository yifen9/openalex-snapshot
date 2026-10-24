use crate::io::zstd;
use crate::s3::{SourceFile, column, member, open, optional, parse_id};
use anyhow::{Context, Result, bail};
use arrow_array::builder::StringBuilder;
use arrow_array::{
    Array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int32Array, ListArray, RecordBatch,
    StringArray, StructArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use futures::TryStreamExt;
use object_store::ObjectStore;
use parquet::arrow::ArrowWriter;
use parquet::file::metadata::KeyValue;
use parquet::file::reader::{FileReader, SerializedFileReader};
use std::fs::File;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::{Arc, LazyLock};

pub const WORK_LEAVES: [&str; 20] = [
    "id",
    "doi",
    "language",
    "publication_year",
    "type",
    "is_xpac",
    "cited_by_count",
    "fwci",
    "citation_normalized_percentile.is_in_top_10_percent",
    "citation_normalized_percentile.is_in_top_1_percent",
    "primary_topic.field.id",
    "primary_topic.subfield.id",
    "primary_location.source.id",
    "authorships.list.element.author.id",
    "authorships.list.element.author.orcid",
    "authorships.list.element.author_position",
    "authorships.list.element.institutions.list.element.id",
    "title",
    "topics.list.element.id",
    "topics.list.element.score",
];

pub static ROW_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("work_id", DataType::UInt64, false),
        Field::new("year", DataType::UInt16, false),
        Field::new("work_type", DataType::Utf8, false),
        Field::new("field_id", DataType::UInt16, false),
        Field::new("subfield_id", DataType::UInt16, false),
        Field::new("source_id", DataType::UInt64, false),
        Field::new("cited", DataType::UInt32, false),
        Field::new("fwci", DataType::Float32, true),
        Field::new("top10", DataType::Boolean, true),
        Field::new("top1", DataType::Boolean, true),
        Field::new("n_authors", DataType::UInt16, false),
        Field::new("author_id", DataType::UInt64, false),
        Field::new("orcid", DataType::Boolean, false),
        Field::new("position", DataType::UInt8, false),
        Field::new("institution_id", DataType::UInt64, false),
        Field::new("n_institutions", DataType::UInt16, false),
        Field::new("language", DataType::Utf8, true),
        Field::new("doi", DataType::Utf8, true),
        Field::new("orcid_id", DataType::Utf8, true),
    ]))
});

pub static TOPIC_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("work_id", DataType::UInt64, false),
        Field::new("rank", DataType::UInt8, false),
        Field::new("topic_id", DataType::UInt16, false),
        Field::new("score", DataType::Float32, false),
    ]))
});

pub static TITLE_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("work_id", DataType::UInt64, false),
        Field::new("title", DataType::Utf8, false),
    ]))
});

fn names(schema: &SchemaRef) -> String {
    schema
        .fields()
        .iter()
        .map(|field| field.name().as_str())
        .collect::<Vec<_>>()
        .join(",")
}

pub static ROW_COLUMNS: LazyLock<String> = LazyLock::new(|| names(&ROW_SCHEMA));
pub static TOPIC_COLUMNS: LazyLock<String> = LazyLock::new(|| names(&TOPIC_SCHEMA));
pub static TITLE_COLUMNS: LazyLock<String> = LazyLock::new(|| names(&TITLE_SCHEMA));

pub struct Outputs {
    pub works: PathBuf,
    pub topics: PathBuf,
    pub titles: PathBuf,
}

impl Outputs {
    pub fn new(dir: &Path, name: &str) -> Self {
        Outputs {
            works: dir.join("works").join(name),
            topics: dir.join("topics").join(name),
            titles: dir.join("titles").join(name),
        }
    }

    fn all(&self) -> [&Path; 3] {
        [&self.titles, &self.topics, &self.works]
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FileStats {
    pub records: u64,
    pub works: u64,
    pub rows: u64,
    pub topics: u64,
    pub titles: u64,
    pub bytes: u64,
}

#[derive(Clone, Copy)]
struct Slot {
    author: u64,
    authorship: usize,
    orcid: bool,
    position: u8,
    institution: u64,
    count: u16,
}

#[derive(Default)]
struct RowColumns {
    work: Vec<u64>,
    year: Vec<u16>,
    work_type: StringBuilder,
    field: Vec<u16>,
    subfield: Vec<u16>,
    source: Vec<u64>,
    cited: Vec<u32>,
    fwci: Vec<Option<f32>>,
    top10: Vec<Option<bool>>,
    top1: Vec<Option<bool>>,
    authors: Vec<u16>,
    author: Vec<u64>,
    orcid: Vec<bool>,
    position: Vec<u8>,
    institution: Vec<u64>,
    institutions: Vec<u16>,
    language: StringBuilder,
    doi: StringBuilder,
    orcid_id: StringBuilder,
}

#[derive(Default)]
struct TopicColumns {
    work: Vec<u64>,
    rank: Vec<u8>,
    topic: Vec<u16>,
    score: Vec<f32>,
}

#[derive(Default)]
struct TitleColumns {
    work: Vec<u64>,
    title: StringBuilder,
}

pub struct Batches {
    pub rows: Option<RecordBatch>,
    pub topics: Option<RecordBatch>,
    pub titles: Option<RecordBatch>,
}

struct WorkColumns<'a> {
    id: &'a StringArray,
    doi: &'a StringArray,
    language: &'a StringArray,
    year: &'a Int32Array,
    work_type: &'a StringArray,
    xpac: &'a BooleanArray,
    cited: &'a Int32Array,
    fwci: &'a Float64Array,
    percentile: &'a StructArray,
    top10: &'a BooleanArray,
    top1: &'a BooleanArray,
    field: &'a StringArray,
    subfield: &'a StringArray,
    source: &'a StringArray,
    authorships: &'a ListArray,
    author: &'a StringArray,
    orcid: &'a StringArray,
    position: &'a StringArray,
    institutions: &'a ListArray,
    institution: &'a StringArray,
    title: &'a StringArray,
    topics: &'a ListArray,
    topic: &'a StringArray,
    score: &'a Float32Array,
}

impl<'a> WorkColumns<'a> {
    fn of(batch: &'a RecordBatch) -> Result<Self> {
        let authorships: &ListArray = column(batch, "authorships")?;
        let institutions: &ListArray = member(authorships.values(), "institutions")?;
        let topics: &ListArray = column(batch, "topics")?;
        Ok(WorkColumns {
            id: column(batch, "id")?,
            doi: column(batch, "doi")?,
            language: column(batch, "language")?,
            year: column(batch, "publication_year")?,
            work_type: column(batch, "type")?,
            xpac: column(batch, "is_xpac")?,
            cited: column(batch, "cited_by_count")?,
            fwci: column(batch, "fwci")?,
            percentile: column(batch, "citation_normalized_percentile")?,
            top10: column(batch, "citation_normalized_percentile.is_in_top_10_percent")?,
            top1: column(batch, "citation_normalized_percentile.is_in_top_1_percent")?,
            field: column(batch, "primary_topic.field.id")?,
            subfield: column(batch, "primary_topic.subfield.id")?,
            source: column(batch, "primary_location.source.id")?,
            authorships,
            author: member(authorships.values(), "author.id")?,
            orcid: member(authorships.values(), "author.orcid")?,
            position: member(authorships.values(), "author_position")?,
            institutions,
            institution: member(institutions.values(), "id")?,
            title: column(batch, "title")?,
            topics,
            topic: member(topics.values(), "id")?,
            score: member(topics.values(), "score")?,
        })
    }

    fn flag(&self, array: &BooleanArray, index: usize) -> Option<bool> {
        (self.percentile.is_valid(index) && array.is_valid(index)).then(|| array.value(index))
    }
}

fn optional_id<T: FromStr + Default>(array: &StringArray, index: usize) -> Result<T> {
    optional(array, index).map_or(Ok(T::default()), parse_id)
}

pub fn bare_orcid(value: &str) -> &str {
    value
        .strip_prefix("https://orcid.org/")
        .or_else(|| value.strip_prefix("http://orcid.org/"))
        .unwrap_or(value)
}

pub fn bare_doi(value: &str) -> &str {
    value
        .strip_prefix("https://doi.org/")
        .or_else(|| value.strip_prefix("http://doi.org/"))
        .unwrap_or(value)
}

fn position_code(value: Option<&str>) -> u8 {
    match value {
        Some("first") => 0,
        Some("middle") => 1,
        Some("last") => 2,
        _ => 3,
    }
}

pub struct Explode {
    years: Range<i32>,
    slots: Vec<Slot>,
    ids: Vec<u64>,
    columns: RowColumns,
    topics: TopicColumns,
    titles: TitleColumns,
    pub stats: FileStats,
}

impl Explode {
    pub fn new(min_year: u16, max_year: u16) -> Self {
        Explode {
            years: i32::from(min_year)..i32::from(max_year) + 1,
            slots: Vec::new(),
            ids: Vec::new(),
            columns: RowColumns::default(),
            topics: TopicColumns::default(),
            titles: TitleColumns::default(),
            stats: FileStats::default(),
        }
    }

    pub fn push(&mut self, batch: &RecordBatch) -> Result<Batches> {
        let works = WorkColumns::of(batch)?;
        for index in 0..batch.num_rows() {
            self.push_work(&works, index)?;
        }
        self.stats.records += batch.num_rows() as u64;
        self.finish_batch()
    }

    fn collect_slots(&mut self, works: &WorkColumns, start: usize, end: usize) -> Result<()> {
        self.slots.clear();
        let offsets = works.institutions.value_offsets();
        for authorship in start..end {
            let Some(author) = optional(works.author, authorship)
                .map(parse_id::<u64>)
                .transpose()?
            else {
                continue;
            };
            self.ids.clear();
            for slot in offsets[authorship] as usize..offsets[authorship + 1] as usize {
                if let Some(id) = optional(works.institution, slot)
                    .map(parse_id::<u64>)
                    .transpose()?
                {
                    self.ids.push(id);
                }
            }
            self.ids.sort_unstable();
            self.ids.dedup();
            let count = u16::try_from(self.ids.len()).unwrap_or(u16::MAX);
            let orcid = optional(works.orcid, authorship).is_some_and(|s| !s.is_empty());
            let position = position_code(optional(works.position, authorship));
            for &institution in &self.ids {
                self.slots.push(Slot {
                    author,
                    authorship,
                    orcid,
                    position,
                    institution,
                    count,
                });
            }
        }
        self.slots.sort_by_key(|s| (s.author, s.institution));
        self.slots.dedup_by_key(|s| (s.author, s.institution));
        Ok(())
    }

    fn push_work(&mut self, works: &WorkColumns, index: usize) -> Result<()> {
        if works.xpac.is_valid(index) && works.xpac.value(index) {
            return Ok(());
        }
        if works.year.is_null(index) || !self.years.contains(&works.year.value(index)) {
            return Ok(());
        }
        let work: u64 = parse_id(optional(works.id, index).context("work without id")?)?;
        let offsets = works.authorships.value_offsets();
        let (start, end) = (offsets[index] as usize, offsets[index + 1] as usize);
        self.collect_slots(works, start, end)?;
        self.stats.works += 1;
        self.push_topics(works, index, work)?;
        let top10 = works.flag(works.top10, index);
        if top10 == Some(true)
            && let Some(title) = optional(works.title, index)
        {
            self.titles.work.push(work);
            self.titles.title.append_value(title);
            self.stats.titles += 1;
        }
        if self.slots.is_empty() {
            return Ok(());
        }
        let year = u16::try_from(works.year.value(index))?;
        let work_type = optional(works.work_type, index).unwrap_or_default();
        let field = optional_id(works.field, index)?;
        let subfield = optional_id(works.subfield, index)?;
        let source = optional_id(works.source, index)?;
        let cited = if works.cited.is_valid(index) {
            u32::try_from(works.cited.value(index)).unwrap_or(0)
        } else {
            0
        };
        let fwci = works
            .fwci
            .is_valid(index)
            .then(|| works.fwci.value(index) as f32);
        let top1 = works.flag(works.top1, index);
        let authors = u16::try_from(end - start).unwrap_or(u16::MAX);
        let language = optional(works.language, index);
        let doi = optional(works.doi, index).map(bare_doi);
        let columns = &mut self.columns;
        for slot in &self.slots {
            columns.work.push(work);
            columns.year.push(year);
            columns.work_type.append_value(work_type);
            columns.field.push(field);
            columns.subfield.push(subfield);
            columns.source.push(source);
            columns.cited.push(cited);
            columns.fwci.push(fwci);
            columns.top10.push(top10);
            columns.top1.push(top1);
            columns.authors.push(authors);
            columns.author.push(slot.author);
            columns.orcid.push(slot.orcid);
            columns.position.push(slot.position);
            columns.institution.push(slot.institution);
            columns.institutions.push(slot.count);
            columns.language.append_option(language);
            columns.doi.append_option(doi);
            columns.orcid_id.append_option(
                optional(works.orcid, slot.authorship)
                    .filter(|s| !s.is_empty())
                    .map(bare_orcid),
            );
        }
        self.stats.rows += self.slots.len() as u64;
        Ok(())
    }

    fn push_topics(&mut self, works: &WorkColumns, index: usize, work: u64) -> Result<()> {
        if works.topics.is_null(index) {
            return Ok(());
        }
        let offsets = works.topics.value_offsets();
        let (start, end) = (offsets[index] as usize, offsets[index + 1] as usize);
        let mut rank = 0u8;
        for slot in start..end {
            let Some(id) = optional(works.topic, slot) else {
                continue;
            };
            if works.score.is_null(slot) {
                continue;
            }
            self.topics.work.push(work);
            self.topics.rank.push(rank);
            self.topics.topic.push(parse_id(id)?);
            self.topics.score.push(works.score.value(slot));
            rank = rank.saturating_add(1);
            self.stats.topics += 1;
        }
        Ok(())
    }

    fn finish_batch(&mut self) -> Result<Batches> {
        Ok(Batches {
            rows: self.finish_rows()?,
            topics: self.finish_topics()?,
            titles: self.finish_titles()?,
        })
    }

    fn finish_topics(&mut self) -> Result<Option<RecordBatch>> {
        if self.topics.work.is_empty() {
            return Ok(None);
        }
        let t = std::mem::take(&mut self.topics);
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(UInt64Array::from(t.work)),
            Arc::new(UInt8Array::from(t.rank)),
            Arc::new(UInt16Array::from(t.topic)),
            Arc::new(Float32Array::from(t.score)),
        ];
        Ok(Some(RecordBatch::try_new(
            Arc::clone(&TOPIC_SCHEMA),
            arrays,
        )?))
    }

    fn finish_titles(&mut self) -> Result<Option<RecordBatch>> {
        if self.titles.work.is_empty() {
            return Ok(None);
        }
        let t = std::mem::take(&mut self.titles);
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(UInt64Array::from(t.work)),
            Arc::new({ t.title }.finish()),
        ];
        Ok(Some(RecordBatch::try_new(
            Arc::clone(&TITLE_SCHEMA),
            arrays,
        )?))
    }

    fn finish_rows(&mut self) -> Result<Option<RecordBatch>> {
        if self.columns.work.is_empty() {
            return Ok(None);
        }
        let c = std::mem::take(&mut self.columns);
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(UInt64Array::from(c.work)),
            Arc::new(UInt16Array::from(c.year)),
            Arc::new({ c.work_type }.finish()),
            Arc::new(UInt16Array::from(c.field)),
            Arc::new(UInt16Array::from(c.subfield)),
            Arc::new(UInt64Array::from(c.source)),
            Arc::new(UInt32Array::from(c.cited)),
            Arc::new(Float32Array::from(c.fwci)),
            Arc::new(BooleanArray::from(c.top10)),
            Arc::new(BooleanArray::from(c.top1)),
            Arc::new(UInt16Array::from(c.authors)),
            Arc::new(UInt64Array::from(c.author)),
            Arc::new(BooleanArray::from(c.orcid)),
            Arc::new(UInt8Array::from(c.position)),
            Arc::new(UInt64Array::from(c.institution)),
            Arc::new(UInt16Array::from(c.institutions)),
            Arc::new({ c.language }.finish()),
            Arc::new({ c.doi }.finish()),
            Arc::new({ c.orcid_id }.finish()),
        ];
        Ok(Some(RecordBatch::try_new(Arc::clone(&ROW_SCHEMA), arrays)?))
    }
}

fn temporary(path: &Path) -> PathBuf {
    let mut name = path.as_os_str().to_os_string();
    name.push(".tmp");
    PathBuf::from(name)
}

pub async fn extract_file(
    store: &Arc<dyn ObjectStore>,
    source: &SourceFile,
    outputs: &Outputs,
    min_year: u16,
    max_year: u16,
) -> Result<FileStats> {
    let written = write_file(store, source, outputs, min_year, max_year)
        .await
        .and_then(|stats| {
            for path in outputs.all() {
                std::fs::rename(temporary(path), path)
                    .with_context(|| format!("renaming to {}", path.display()))?;
            }
            Ok(stats)
        });
    if written.is_err() {
        for path in outputs.all() {
            std::fs::remove_file(temporary(path)).ok();
        }
    }
    written.with_context(|| format!("extracting {}", source.location))
}

fn writer(path: &Path, schema: &SchemaRef) -> Result<ArrowWriter<File>> {
    let temporary = temporary(path);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating {}", parent.display()))?;
    }
    let file =
        File::create(&temporary).with_context(|| format!("creating {}", temporary.display()))?;
    Ok(ArrowWriter::try_new(
        file,
        Arc::clone(schema),
        Some(zstd()),
    )?)
}

fn close(mut writer: ArrowWriter<File>, columns: &str, count: u64) -> Result<u64> {
    writer.append_key_value_metadata(KeyValue::new("columns".to_string(), columns.to_string()));
    writer.append_key_value_metadata(KeyValue::new("count".to_string(), count.to_string()));
    let file = writer.into_inner()?;
    file.sync_all()?;
    Ok(file.metadata()?.len())
}

async fn write_file(
    store: &Arc<dyn ObjectStore>,
    source: &SourceFile,
    outputs: &Outputs,
    min_year: u16,
    max_year: u16,
) -> Result<FileStats> {
    let mut stream = open(store, source, &WORK_LEAVES).await?;
    let mut rows = writer(&outputs.works, &ROW_SCHEMA)?;
    let mut topics = writer(&outputs.topics, &TOPIC_SCHEMA)?;
    let mut titles = writer(&outputs.titles, &TITLE_SCHEMA)?;
    let mut explode = Explode::new(min_year, max_year);
    while let Some(batch) = stream.try_next().await? {
        let batches = explode.push(&batch)?;
        if let Some(batch) = batches.rows {
            rows.write(&batch)?;
        }
        if let Some(batch) = batches.topics {
            topics.write(&batch)?;
        }
        if let Some(batch) = batches.titles {
            titles.write(&batch)?;
        }
    }
    let mut stats = explode.stats;
    if stats.records != source.records {
        bail!(
            "read {} records but the manifest lists {}",
            stats.records,
            source.records
        );
    }
    for (key, value) in [
        ("records", stats.records.to_string()),
        ("works", stats.works.to_string()),
        ("rows", stats.rows.to_string()),
        ("topics", stats.topics.to_string()),
        ("titles", stats.titles.to_string()),
        ("min_year", min_year.to_string()),
        ("max_year", max_year.to_string()),
    ] {
        rows.append_key_value_metadata(KeyValue::new(key.to_string(), value));
    }
    stats.bytes = close(titles, &TITLE_COLUMNS, stats.titles)?
        + close(topics, &TOPIC_COLUMNS, stats.topics)?
        + close(rows, &ROW_COLUMNS, stats.rows)?;
    Ok(stats)
}

fn metadata(path: &Path) -> Option<(u64, Vec<KeyValue>)> {
    let file = File::open(path).ok()?;
    let bytes = file.metadata().ok()?.len();
    let reader = SerializedFileReader::new(file).ok()?;
    let entries = reader
        .metadata()
        .file_metadata()
        .key_value_metadata()?
        .clone();
    Some((bytes, entries))
}

fn entry<'a>(entries: &'a [KeyValue], key: &str) -> Option<&'a str> {
    entries
        .iter()
        .find(|entry| entry.key == key)?
        .value
        .as_deref()
}

fn number(entries: &[KeyValue], key: &str) -> Option<u64> {
    entry(entries, key)?.parse().ok()
}

pub fn completed_stats(outputs: &Outputs, min_year: u16, max_year: u16) -> Option<FileStats> {
    let (works_bytes, works) = metadata(&outputs.works)?;
    let (topics_bytes, topics) = metadata(&outputs.topics)?;
    let (titles_bytes, titles) = metadata(&outputs.titles)?;
    let complete = entry(&works, "columns")? == ROW_COLUMNS.as_str()
        && entry(&topics, "columns")? == TOPIC_COLUMNS.as_str()
        && entry(&titles, "columns")? == TITLE_COLUMNS.as_str()
        && number(&works, "min_year")? == u64::from(min_year)
        && number(&works, "max_year")? == u64::from(max_year)
        && number(&works, "topics")? == number(&topics, "count")?
        && number(&works, "titles")? == number(&titles, "count")?;
    if !complete {
        return None;
    }
    Some(FileStats {
        records: number(&works, "records")?,
        works: number(&works, "works")?,
        rows: number(&works, "rows")?,
        topics: number(&works, "topics")?,
        titles: number(&works, "titles")?,
        bytes: works_bytes + topics_bytes + titles_bytes,
    })
}
