use std::collections::BTreeSet;
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

pub fn collect_files(input_dir: &Path, exts: &[&str], recursive: bool) -> Result<Vec<PathBuf>> {
    fn walk(dir: &Path, exts: &[&str], recursive: bool, out: &mut Vec<PathBuf>) -> Result<()> {
        for entry in
            std::fs::read_dir(dir).with_context(|| format!("读取目录失败: {}", dir.display()))?
        {
            let entry = entry?;
            let path = entry.path();
            let ft = entry.file_type()?;
            if ft.is_dir() && recursive {
                walk(&path, exts, recursive, out)?;
                continue;
            }
            if ft.is_file()
                && path
                    .extension()
                    .and_then(|s| s.to_str())
                    .is_some_and(|ext| exts.iter().any(|x| ext.eq_ignore_ascii_case(x)))
            {
                out.push(path);
            }
        }
        Ok(())
    }

    let mut files = Vec::new();
    walk(input_dir, exts, recursive, &mut files)?;
    files.sort();
    Ok(files)
}

pub fn parse_opt_f64(v: Option<&str>) -> Option<f64> {
    let raw = v?.trim();
    if raw.is_empty() {
        return None;
    }
    raw.parse::<f64>().ok()
}

pub fn write_file(path: &Path, bytes: Vec<u8>) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("创建输出目录失败: {}", parent.display()))?;
    }
    std::fs::write(path, bytes).with_context(|| format!("写入文件失败: {}", path.display()))?;
    Ok(())
}

pub fn source_key(path: &Path, input_dir: &Path) -> Result<String> {
    let meta = std::fs::metadata(path)?;
    let size = meta.len();
    let mtime_secs = meta
        .modified()
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_secs())
        .unwrap_or_default();
    let relative = path
        .strip_prefix(input_dir)
        .unwrap_or(path)
        .to_string_lossy();
    Ok(format!("{}\t{}\t{}", relative, size, mtime_secs))
}

pub fn load_manifest(path: &Path) -> Result<BTreeSet<String>> {
    if !path.exists() {
        return Ok(BTreeSet::new());
    }
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("读取 manifest 失败: {}", path.display()))?;
    Ok(raw
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(str::to_string)
        .collect())
}

pub fn append_manifest_line(path: &Path, key: &str) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("创建 manifest 目录失败: {}", parent.display()))?;
    }
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("打开 manifest 失败: {}", path.display()))?;
    writeln!(f, "{key}").with_context(|| format!("写入 manifest 失败: {}", path.display()))?;
    Ok(())
}

pub fn output_id(path: &Path, input_dir: &Path, idx: usize) -> String {
    let relative = path.strip_prefix(input_dir).unwrap_or(path);
    let raw = relative.with_extension("").to_string_lossy().into_owned();
    let cleaned: String = raw
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();
    format!("src-{idx:05}-{cleaned}")
}

pub fn open_file(path: &Path) -> Result<File> {
    File::open(path).with_context(|| format!("打开文件失败: {}", path.display()))
}
