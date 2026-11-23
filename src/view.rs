use anyhow::{Result, anyhow};
use polars::prelude::*;
use std::fs::File;
use std::path::{Path, PathBuf};

use crate::workspace::{FletchWorkspace, validate_path_component};

struct ViewFile {
    path: PathBuf,
    run_id: String,
}

/// Builds a view of one row stream: its rows from one run or every run,
/// optionally filtered, ordered and narrowed by its declared columns.
///
/// A view never joins or aligns streams: row streams have no shared clock.
/// Each row carries a `run_id` column, taken from the storage layout.
pub struct FletchViewBuilder<'a> {
    workspace: &'a FletchWorkspace,
    stream_name: String,
    run_id: Option<String>,
    columns: Option<Vec<String>>,
    filters: Vec<Expr>,
    order_by: Vec<String>,
}

impl<'a> FletchViewBuilder<'a> {
    pub fn new(workspace: &'a FletchWorkspace, stream_name: impl Into<String>) -> Self {
        Self {
            workspace,
            stream_name: stream_name.into(),
            run_id: None,
            columns: None,
            filters: Vec::new(),
            order_by: Vec::new(),
        }
    }

    /// Reads only this run. Without it the view reads every run.
    pub fn run_id(mut self, run_id: impl Into<String>) -> Self {
        self.run_id = Some(run_id.into());
        self
    }

    /// Keeps only these columns, after `run_id`. Without it the view keeps
    /// every declared column.
    pub fn columns(mut self, columns: &[&str]) -> Self {
        self.columns = Some(columns.iter().map(|column| column.to_string()).collect());
        self
    }

    /// Keeps only rows matching `predicate`. Several filters must all match.
    pub fn filter(mut self, predicate: Expr) -> Self {
        self.filters.push(predicate);
        self
    }

    /// Orders rows by these columns, ascending, in turn. Rows that tie keep
    /// their stored order, which within one file is append order. Without it
    /// rows keep their stored order: run by run, then file by file.
    pub fn order_by(mut self, columns: &[&str]) -> Self {
        self.order_by = columns.iter().map(|column| column.to_string()).collect();
        self
    }

    pub fn build(self) -> Result<FletchView> {
        let files = parquet_files_for_stream(
            self.workspace.root(),
            self.run_id.as_deref(),
            &self.stream_name,
        )?;
        if files.is_empty() {
            return Err(anyhow!(
                "no parquet files found for stream {}",
                self.stream_name
            ));
        }

        let mut frames = Vec::with_capacity(files.len());
        for file in files {
            let frame = LazyFrame::scan_parquet(
                PlRefPath::new(file.path.to_string_lossy().as_ref()),
                ScanArgsParquet::default(),
            )?
            .with_columns([lit(file.run_id).alias("run_id")]);
            frames.push(frame);
        }
        let mut frame = concat(frames, Default::default())?;

        for predicate in self.filters {
            frame = frame.filter(predicate);
        }
        if !self.order_by.is_empty() {
            frame = frame.sort(
                self.order_by,
                SortMultipleOptions::default().with_maintain_order(true),
            );
        }
        let rest = match self.columns {
            Some(columns) => columns.iter().map(|column| col(column.as_str())).collect(),
            None => vec![all().exclude_cols(["run_id"]).as_expr()],
        };
        frame = frame.select([vec![col("run_id")], rest].concat());

        Ok(FletchView { lazy_frame: frame })
    }
}

fn parquet_files_for_stream(
    root: &Path,
    run_id: Option<&str>,
    stream_name: &str,
) -> Result<Vec<ViewFile>> {
    validate_path_component("stream_name", stream_name)?;

    let mut files = Vec::new();
    let runs_dir = root.join("runs");
    if !runs_dir.exists() {
        return Ok(files);
    }

    if let Some(run_id) = run_id {
        validate_path_component("run_id", run_id)?;
        collect_stream_files(&runs_dir.join(run_id).join(stream_name), run_id, &mut files)?;
    } else {
        for run_entry in std::fs::read_dir(runs_dir)? {
            let run_entry = run_entry?;
            if run_entry.file_type()?.is_dir() {
                let run_id = run_entry.file_name().into_string().map_err(|_| {
                    anyhow!(
                        "run directory name is not valid UTF-8: {:?}",
                        run_entry.path()
                    )
                })?;
                collect_stream_files(&run_entry.path().join(stream_name), &run_id, &mut files)?;
            }
        }
    }

    files.sort_by(|left, right| left.path.cmp(&right.path));
    Ok(files)
}

fn collect_stream_files(stream_dir: &Path, run_id: &str, files: &mut Vec<ViewFile>) -> Result<()> {
    if !stream_dir.exists() {
        return Ok(());
    }
    for entry in std::fs::read_dir(stream_dir)? {
        let path = entry?.path();
        if path.extension().and_then(|value| value.to_str()) == Some("parquet") {
            files.push(ViewFile {
                path,
                run_id: run_id.to_string(),
            });
        }
    }
    Ok(())
}

pub struct FletchView {
    lazy_frame: LazyFrame,
}

impl FletchView {
    pub fn collect(self) -> Result<DataFrame> {
        self.lazy_frame
            .collect()
            .map_err(|e| anyhow!("failed to compute view: {}", e))
    }

    pub fn into_lazy(self) -> LazyFrame {
        self.lazy_frame
    }

    pub fn to_csv<P: AsRef<Path>>(self, path: P) -> Result<()> {
        let mut df = self.collect()?;
        let mut file =
            File::create(path).map_err(|e| anyhow!("failed to create CSV file: {}", e))?;
        CsvWriter::new(&mut file)
            .include_header(true)
            .finish(&mut df)
            .map_err(|e| anyhow!("failed to write CSV: {}", e))?;
        Ok(())
    }

    pub fn to_parquet<P: AsRef<Path>>(self, path: P) -> Result<()> {
        let mut df = self.collect()?;
        let mut file =
            File::create(path).map_err(|e| anyhow!("failed to create Parquet file: {}", e))?;
        ParquetWriter::new(&mut file)
            .finish(&mut df)
            .map_err(|e| anyhow!("failed to write Parquet: {}", e))?;
        Ok(())
    }
}
