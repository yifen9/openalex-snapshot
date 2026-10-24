use crate::io::{write_atomic, zstd};
use crate::s3::{SourceFile, open, optional, parse_id, typed};
use anyhow::{Context, Result, bail};
use arrow_array::{
    Array, ArrayRef, Float64Array, ListArray, RecordBatch, StringArray, StructArray, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema};
use futures::{StreamExt, TryStreamExt};
use object_store::ObjectStore;
use parquet::arrow::ArrowWriter;
use std::path::Path;
use std::sync::Arc;

pub type Leaf = (&'static str, &'static str);

const IDS: [&str; 3] = ["subfield_id", "field_id", "domain_id"];
type Record = (u64, Vec<Option<String>>);

pub async fn extract_lookup(
    store: &Arc<dyn ObjectStore>,
    sources: Vec<SourceFile>,
    leaves: &'static [Leaf],
    target: &Path,
    concurrency: usize,
) -> Result<()> {
    if target.exists() {
        return Ok(());
    }
    let parts: Vec<Vec<Record>> = futures::stream::iter(sources)
        .map(|source| {
            let store = Arc::clone(store);
            tokio::spawn(async move {
                read_source(&store, &source, leaves)
                    .await
                    .with_context(|| format!("reading {}", source.location))
            })
        })
        .buffered(concurrency)
        .map(|joined| joined?)
        .try_collect()
        .await?;
    let mut records: Vec<Record> = parts.into_iter().flatten().collect();
    records.sort_unstable_by_key(|record| record.0);
    if let Some(pair) = records.windows(2).find(|pair| pair[0].0 == pair[1].0) {
        bail!(
            "{} appears more than once in {}",
            pair[0].0,
            target.display()
        );
    }
    let mut fields = vec![Field::new("id", DataType::UInt64, false)];
    let mut arrays: Vec<ArrayRef> = vec![Arc::new(
        records.iter().map(|r| r.0).collect::<UInt64Array>(),
    )];
    for (slot, (_, name)) in leaves[1..].iter().enumerate() {
        let values = records.iter().map(|r| r.1[slot].as_deref());
        if IDS.contains(name) {
            fields.push(Field::new(*name, DataType::UInt64, true));
            arrays.push(Arc::new(
                values
                    .map(|value| value.map(parse_id::<u64>).transpose())
                    .collect::<Result<UInt64Array>>()?,
            ));
        } else {
            fields.push(Field::new(*name, DataType::Utf8, true));
            arrays.push(Arc::new(values.collect::<StringArray>()));
        }
    }
    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(Arc::clone(&schema), arrays)?;
    write_atomic(target, |w| {
        let mut writer = ArrowWriter::try_new(w, schema, Some(zstd()))?;
        writer.write(&batch)?;
        writer.close()?;
        Ok(())
    })
}

fn resolve(batch: &RecordBatch, leaf: &str) -> Result<ArrayRef> {
    let path = leaf.replace(".list.element", "");
    let mut parts = path.split('.');
    let first = parts.next().unwrap_or_default();
    let mut array = Arc::clone(
        batch
            .column_by_name(first)
            .with_context(|| format!("batch has no column {first}"))?,
    );
    for part in parts {
        let parent: &StructArray = typed(array.as_ref(), leaf)?;
        array = Arc::clone(
            parent
                .column_by_name(part)
                .with_context(|| format!("{leaf} has no member {part}"))?,
        );
    }
    Ok(array)
}

fn texts(batch: &RecordBatch, leaf: &str) -> Result<Vec<Option<String>>> {
    let array = resolve(batch, leaf)?;
    let rows = 0..array.len();
    Ok(match array.data_type() {
        DataType::Utf8 => {
            let values: &StringArray = typed(array.as_ref(), leaf)?;
            rows.map(|i| optional(values, i).map(str::to_string))
                .collect()
        }
        DataType::Float64 => {
            let values: &Float64Array = typed(array.as_ref(), leaf)?;
            rows.map(|i| values.is_valid(i).then(|| values.value(i).to_string()))
                .collect()
        }
        DataType::List(_) => {
            let lists: &ListArray = typed(array.as_ref(), leaf)?;
            let values: &StringArray = typed(lists.values().as_ref(), leaf)?;
            let offsets = lists.value_offsets();
            rows.map(|i| {
                lists.is_valid(i).then(|| {
                    (offsets[i] as usize..offsets[i + 1] as usize)
                        .filter_map(|j| optional(values, j))
                        .collect::<Vec<_>>()
                        .join(" ")
                })
            })
            .collect()
        }
        other => bail!("{leaf} has unsupported type {other}"),
    })
}

async fn read_source(
    store: &Arc<dyn ObjectStore>,
    source: &SourceFile,
    leaves: &[Leaf],
) -> Result<Vec<Record>> {
    let paths: Vec<&str> = leaves.iter().map(|(leaf, _)| *leaf).collect();
    let mut stream = open(store, source, &paths).await?;
    let mut records = Vec::new();
    while let Some(batch) = stream.try_next().await? {
        let ids = texts(&batch, leaves[0].0)?;
        let columns = leaves[1..]
            .iter()
            .map(|(leaf, _)| texts(&batch, leaf))
            .collect::<Result<Vec<_>>>()?;
        for (index, id) in ids.into_iter().enumerate() {
            let id = id.context("lookup row without id")?;
            records.push((
                parse_id(&id)?,
                columns.iter().map(|column| column[index].clone()).collect(),
            ));
        }
    }
    if records.len() as u64 != source.records {
        bail!(
            "read {} records but the manifest lists {}",
            records.len(),
            source.records
        );
    }
    Ok(records)
}
