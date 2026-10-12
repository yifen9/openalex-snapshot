use crate::s3::{Manifest, parse_id};
use crate::works::bare_doi;

#[test]
fn dois_lose_their_resolver_prefix() {
    assert_eq!(bare_doi("https://doi.org/10.1000/xyz"), "10.1000/xyz");
    assert_eq!(bare_doi("http://doi.org/10.1000/xyz"), "10.1000/xyz");
    assert_eq!(bare_doi("10.1000/xyz"), "10.1000/xyz");
}

#[test]
fn ids_parse_from_urls_and_prefixed_strings() {
    assert_eq!(
        parse_id::<u64>("https://openalex.org/W2741809807").ok(),
        Some(2741809807)
    );
    assert_eq!(parse_id::<u64>("I136199984").ok(), Some(136199984));
    assert!(parse_id::<u64>("https://openalex.org/").is_err());
}

#[test]
fn manifest_files_map_to_partition_named_parts() {
    let text = r#"{"date": "2026-09-23", "entities": [{"entity": "works", "files": [
        {"url": "s3://openalex/data/parquet/works/updated_date=2026-09-22/part_0003.parquet",
         "meta": {"content_length": 10, "record_count": 2}}]}]}"#;
    let manifest = Manifest::parse(text.as_bytes()).unwrap_or_else(|e| panic!("{e:#}"));
    let sources = manifest
        .sources("works", "openalex")
        .unwrap_or_else(|e| panic!("{e:#}"));
    assert_eq!(sources[0].name, "2026-09-22-part_0003.parquet");
    assert_eq!(sources[0].records, 2);
    assert!(manifest.sources("authors", "openalex").is_err());
}
