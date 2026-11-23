use anyhow::{Result, anyhow};
use polars::prelude::{
    AnyValue, DataFrame, DataType as PolarsType, Expr, LazyFrame, SortMultipleOptions, col, lit,
};
use std::marker::PhantomData;
use std::ops::{BitAnd, BitOr, Not};

use crate::column::{Column, ColumnLiteral, ColumnValue, OrderedColumn};
use crate::schema::{FletchSchema, NamedSchema};
use crate::types::RowValue;
use crate::view::scan_stream;
use crate::workspace::FletchWorkspace;

/// A typed view predicate, built from a [`Column`] and combined with
/// [`and`](Self::and), [`or`](Self::or) and `!` (or [`Not::not`]).
///
/// Column methods accept only values of the column's type:
///
/// ```
/// use fletch::{FletchSchema, Predicate};
///
/// #[derive(FletchSchema)]
/// struct DaqRecord {
///     source: u8,
///     ticks: u64,
///     label: Option<String>,
/// }
///
/// let early_source_one: Predicate = DaqRecord::SOURCE
///     .eq(1)
///     .and(DaqRecord::TICKS.between(100, 500))
///     .and(!DaqRecord::LABEL.is_null());
/// ```
///
/// Comparing a `Column<u8>` with a string does not compile:
///
/// ```compile_fail
/// # use fletch::FletchSchema;
/// # #[derive(FletchSchema)]
/// # struct DaqRecord { source: u8 }
/// let _ = DaqRecord::SOURCE.eq("1");
/// ```
///
/// Nor does a misspelled column:
///
/// ```compile_fail
/// # use fletch::FletchSchema;
/// # #[derive(FletchSchema)]
/// # struct DaqRecord { source: u8 }
/// let _ = DaqRecord::SORCE.eq(1);
/// ```
///
/// An enum column compares only with the enum, not with its stored name:
///
/// ```compile_fail
/// # use fletch::{FletchEnum, FletchSchema};
/// # #[derive(FletchEnum)]
/// # enum Channel { AnIn1 }
/// # #[derive(FletchSchema)]
/// # struct DaqRecord { channel: Channel }
/// let _ = DaqRecord::CHANNEL.eq("AnIn1");
/// ```
#[derive(Clone, Debug)]
pub struct Predicate {
    expr: Expr,
}

impl Predicate {
    /// Matches rows matching both predicates.
    pub fn and(self, other: Predicate) -> Predicate {
        Predicate {
            expr: self.expr.and(other.expr),
        }
    }

    /// Matches rows matching either predicate.
    pub fn or(self, other: Predicate) -> Predicate {
        Predicate {
            expr: self.expr.or(other.expr),
        }
    }

    /// The Polars expression, for a string-based [`FletchViewBuilder`](crate::FletchViewBuilder).
    pub fn into_expr(self) -> Expr {
        self.expr
    }
}

impl Not for Predicate {
    type Output = Predicate;

    fn not(self) -> Predicate {
        Predicate {
            expr: self.expr.not(),
        }
    }
}

impl BitAnd for Predicate {
    type Output = Predicate;

    fn bitand(self, other: Predicate) -> Predicate {
        self.and(other)
    }
}

impl BitOr for Predicate {
    type Output = Predicate;

    fn bitor(self, other: Predicate) -> Predicate {
        self.or(other)
    }
}

impl From<Predicate> for Expr {
    fn from(predicate: Predicate) -> Expr {
        predicate.expr
    }
}

/// One ordering key of a typed view: a column, ascending or descending.
#[derive(Clone, Copy, Debug)]
pub struct SortKey {
    column: &'static str,
    descending: bool,
}

impl<T> From<Column<T>> for SortKey {
    fn from(column: Column<T>) -> SortKey {
        column.asc()
    }
}

fn literal(value: ColumnLiteral) -> Expr {
    match value {
        ColumnLiteral::Bool(value) => lit(value),
        ColumnLiteral::U8(value) => lit(value),
        ColumnLiteral::U16(value) => lit(value),
        ColumnLiteral::U32(value) => lit(value),
        ColumnLiteral::U64(value) => lit(value),
        ColumnLiteral::I32(value) => lit(value),
        ColumnLiteral::I64(value) => lit(value),
        ColumnLiteral::F32(value) => lit(value),
        ColumnLiteral::F64(value) => lit(value),
        ColumnLiteral::Str(value) => lit(value),
    }
}

impl<T> Column<T> {
    fn expr(self) -> Expr {
        col(self.name())
    }

    fn compare(self, value: impl ColumnValue<T>, op: fn(Expr, Expr) -> Expr) -> Predicate {
        Predicate {
            expr: op(self.expr(), literal(value.into_literal())),
        }
    }

    pub fn eq(self, value: impl ColumnValue<T>) -> Predicate {
        self.compare(value, Expr::eq)
    }

    pub fn ne(self, value: impl ColumnValue<T>) -> Predicate {
        self.compare(value, Expr::neq)
    }

    /// Matches rows whose value is any of `values`; no values matches none.
    pub fn is_in<V: ColumnValue<T>>(self, values: impl IntoIterator<Item = V>) -> Predicate {
        let expr = values
            .into_iter()
            .map(|value| self.expr().eq(literal(value.into_literal())))
            .reduce(Expr::or)
            .unwrap_or_else(|| lit(false));
        Predicate { expr }
    }

    /// Matches nulls. Every column is nullable; an `Option` field writes one
    /// for `None`.
    pub fn is_null(self) -> Predicate {
        Predicate {
            expr: self.expr().is_null(),
        }
    }

    pub fn is_not_null(self) -> Predicate {
        Predicate {
            expr: self.expr().is_not_null(),
        }
    }

    pub fn asc(self) -> SortKey {
        SortKey {
            column: self.name(),
            descending: false,
        }
    }

    pub fn desc(self) -> SortKey {
        SortKey {
            column: self.name(),
            descending: true,
        }
    }
}

impl<T: OrderedColumn> Column<T> {
    pub fn lt(self, value: impl ColumnValue<T>) -> Predicate {
        self.compare(value, Expr::lt)
    }

    pub fn le(self, value: impl ColumnValue<T>) -> Predicate {
        self.compare(value, Expr::lt_eq)
    }

    pub fn gt(self, value: impl ColumnValue<T>) -> Predicate {
        self.compare(value, Expr::gt)
    }

    pub fn ge(self, value: impl ColumnValue<T>) -> Predicate {
        self.compare(value, Expr::gt_eq)
    }

    /// Matches `low <= value <= high`.
    pub fn between(self, low: impl ColumnValue<T>, high: impl ColumnValue<T>) -> Predicate {
        self.ge(low).and(self.le(high))
    }
}

/// A typed view of one row stream, read back as `T` values.
///
/// It is built by [`View::builder`] or [`View::builder_named`]; see
/// [`TypedViewBuilder`]. Like every view, it never joins or aligns streams.
pub struct View<T> {
    lazy_frame: LazyFrame,
    columns: Vec<&'static str>,
    _marker: PhantomData<fn() -> T>,
}

impl<T: NamedSchema> View<T> {
    /// A view of the stream named by the type's `#[fletch(stream = "Name")]`.
    pub fn builder(workspace: &FletchWorkspace) -> TypedViewBuilder<'_, T> {
        Self::builder_named(workspace, T::STREAM_NAME)
    }
}

impl<T: FletchSchema> View<T> {
    /// A view of the named stream, for types without
    /// `#[fletch(stream = "Name")]`.
    pub fn builder_named(
        workspace: &FletchWorkspace,
        stream_name: impl Into<String>,
    ) -> TypedViewBuilder<'_, T> {
        TypedViewBuilder {
            workspace,
            stream_name: stream_name.into(),
            run_id: None,
            filters: Vec::new(),
            order_by: Vec::new(),
            _marker: PhantomData,
        }
    }

    /// The view's rows, as `T` values in view order. A null in a field that
    /// is not an `Option` is an error naming its column.
    pub fn rows(self) -> Result<Vec<T>> {
        let columns = self.columns.clone();
        let df = self.collect()?;
        let series = columns
            .iter()
            .map(|name| df.column(name))
            .collect::<Result<Vec<_>, _>>()?;
        let mut rows = Vec::with_capacity(df.height());
        let mut cells = Vec::with_capacity(columns.len());
        for index in 0..df.height() {
            cells.clear();
            for column in &series {
                cells.push(column.get(index)?);
            }
            let row = cells
                .iter()
                .zip(&columns)
                .map(|(cell, name)| row_value(cell, name))
                .collect::<Result<Vec<_>>>()?;
            rows.push(T::from_row(&row)?);
        }
        Ok(rows)
    }

    /// The view as a DataFrame, one column per field in field order.
    pub fn collect(self) -> Result<DataFrame> {
        self.lazy_frame
            .collect()
            .map_err(|e| anyhow!("failed to compute view: {}", e))
    }

    pub fn into_lazy(self) -> LazyFrame {
        self.lazy_frame
    }
}

fn row_value<'a>(cell: &'a AnyValue<'a>, column: &str) -> Result<RowValue<'a>> {
    Ok(match cell {
        AnyValue::Null => RowValue::Null,
        AnyValue::Boolean(value) => RowValue::Bool(*value),
        AnyValue::UInt8(value) => RowValue::U8(*value),
        AnyValue::UInt16(value) => RowValue::U16(*value),
        AnyValue::UInt32(value) => RowValue::U32(*value),
        AnyValue::UInt64(value) => RowValue::U64(*value),
        AnyValue::Int32(value) => RowValue::I32(*value),
        AnyValue::Int64(value) => RowValue::I64(*value),
        AnyValue::Float32(value) => RowValue::F32(*value),
        AnyValue::Float64(value) => RowValue::F64(*value),
        AnyValue::String(value) => RowValue::Str(value),
        AnyValue::StringOwned(value) => RowValue::Str(value.as_str()),
        other => {
            return Err(anyhow!(
                "column `{column}` holds an unsupported value {other:?}"
            ));
        }
    })
}

/// The Polars type a fletch column type is read back as.
fn polars_type(data_type: &crate::DataType) -> Option<PolarsType> {
    use crate::DataType as Arrow;
    Some(match data_type {
        Arrow::Boolean => PolarsType::Boolean,
        Arrow::UInt8 => PolarsType::UInt8,
        Arrow::UInt16 => PolarsType::UInt16,
        Arrow::UInt32 => PolarsType::UInt32,
        Arrow::UInt64 => PolarsType::UInt64,
        Arrow::Int32 => PolarsType::Int32,
        Arrow::Int64 => PolarsType::Int64,
        Arrow::Float32 => PolarsType::Float32,
        Arrow::Float64 => PolarsType::Float64,
        Arrow::Utf8 => PolarsType::String,
        _ => return None,
    })
}

/// Builds a [`View<T>`]: optional run, typed filters and ordering. The
/// stream's schema is checked against `T` once, at [`build`](Self::build).
pub struct TypedViewBuilder<'a, T> {
    workspace: &'a FletchWorkspace,
    stream_name: String,
    run_id: Option<String>,
    filters: Vec<Expr>,
    order_by: Vec<SortKey>,
    _marker: PhantomData<fn() -> T>,
}

impl<T: FletchSchema> TypedViewBuilder<'_, T> {
    /// Reads only this run. Without it the view reads every run.
    pub fn run_id(mut self, run_id: impl Into<String>) -> Self {
        self.run_id = Some(run_id.into());
        self
    }

    /// Keeps only rows matching `predicate`. Several filters must all match.
    pub fn filter(mut self, predicate: Predicate) -> Self {
        self.filters.push(predicate.expr);
        self
    }

    /// Keeps only rows matching a raw Polars expression, for what typed
    /// predicates cannot say.
    pub fn filter_expr(mut self, predicate: Expr) -> Self {
        self.filters.push(predicate);
        self
    }

    /// Orders rows by these keys in turn. Rows that tie keep their stored
    /// order, which within one file is append order.
    pub fn order_by(mut self, keys: impl IntoIterator<Item = impl Into<SortKey>>) -> Self {
        self.order_by = keys.into_iter().map(Into::into).collect();
        self
    }

    /// Scans the stream and checks that its columns and types match `T`'s
    /// fields exactly.
    pub fn build(self) -> Result<View<T>> {
        let mut frame = scan_stream(self.workspace, self.run_id.as_deref(), &self.stream_name)?;
        let schema = frame
            .collect_schema()
            .map_err(|e| anyhow!("failed to read the schema of {}: {}", self.stream_name, e))?;

        let fields = T::column_types();
        let type_name = std::any::type_name::<T>();
        for (name, data_type) in &fields {
            let expected = polars_type(data_type)
                .ok_or_else(|| anyhow!("column `{name}` has unsupported type {data_type:?}"))?;
            match schema.get(name) {
                None => {
                    return Err(anyhow!(
                        "stream {} has no column `{name}`, which {type_name} declares",
                        self.stream_name
                    ));
                }
                Some(actual) if *actual != expected => {
                    return Err(anyhow!(
                        "column `{name}` of stream {} is {actual}, but {type_name} declares {expected}",
                        self.stream_name
                    ));
                }
                Some(_) => {}
            }
        }
        if let Some(extra) = schema
            .iter_names()
            .map(|name| name.as_str())
            .find(|name| *name != "run_id" && !fields.iter().any(|(field, _)| field == name))
        {
            return Err(anyhow!(
                "stream {} has column `{extra}`, which {type_name} does not declare",
                self.stream_name
            ));
        }

        for predicate in self.filters {
            frame = frame.filter(predicate);
        }
        if !self.order_by.is_empty() {
            let columns = self
                .order_by
                .iter()
                .map(|key| key.column)
                .collect::<Vec<_>>();
            let descending = self
                .order_by
                .iter()
                .map(|key| key.descending)
                .collect::<Vec<_>>();
            frame = frame.sort(
                columns,
                SortMultipleOptions::default()
                    .with_order_descending_multi(descending)
                    .with_maintain_order(true),
            );
        }
        let columns = fields.iter().map(|(name, _)| *name).collect::<Vec<_>>();
        frame = frame.select(columns.iter().map(|name| col(*name)).collect::<Vec<_>>());

        Ok(View {
            lazy_frame: frame,
            columns,
            _marker: PhantomData,
        })
    }
}
