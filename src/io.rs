use anyhow::{Context, Result};
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use std::fs::File;
use std::io::BufWriter;
use std::path::{Path, PathBuf};

pub fn write_atomic(
    path: &Path,
    write: impl FnOnce(&mut BufWriter<File>) -> Result<()>,
) -> Result<()> {
    let parent = path
        .parent()
        .with_context(|| format!("{} has no parent", path.display()))?;
    std::fs::create_dir_all(parent).with_context(|| format!("creating {}", parent.display()))?;
    let mut name = path.file_name().unwrap_or_default().to_os_string();
    name.push(".tmp");
    let temporary = PathBuf::from(parent).join(name);
    let file =
        File::create(&temporary).with_context(|| format!("creating {}", temporary.display()))?;
    let mut writer = BufWriter::with_capacity(1 << 20, file);
    write(&mut writer)?;
    writer
        .into_inner()
        .map_err(|e| e.into_error())
        .with_context(|| format!("flushing {}", temporary.display()))?
        .sync_all()
        .with_context(|| format!("syncing {}", temporary.display()))?;
    std::fs::rename(&temporary, path).with_context(|| format!("renaming to {}", path.display()))
}

pub fn zstd() -> WriterProperties {
    WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .build()
}

pub fn write_json(path: &Path, value: &impl serde::Serialize) -> Result<()> {
    write_atomic(path, |w| Ok(serde_json::to_writer_pretty(&mut *w, value)?))
}
