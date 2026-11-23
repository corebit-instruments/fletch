use anyhow::{Result, anyhow};

use crate::types::RowValue;

/// A fieldless enum stored as a string column holding each variant's name.
/// Derive it with `#[derive(FletchEnum)]`, which also makes the enum a
/// [`FletchType`](crate::FletchType), so it can be a `FletchSchema` field,
/// an `Option` field, or a dynamic row-stream column.
///
/// A variant's name is its identifier, unless the enum sets
/// `#[fletch(rename_all = "...")]` or the variant sets
/// `#[fletch(rename = "...")]`. `rename_all` takes `lowercase`, `UPPERCASE`,
/// `PascalCase`, `camelCase`, `snake_case`, `SCREAMING_SNAKE_CASE`,
/// `kebab-case` or `SCREAMING-KEBAB-CASE`.
///
/// ```
/// use fletch::FletchEnum;
///
/// #[derive(Clone, Copy, Debug, PartialEq, FletchEnum)]
/// #[fletch(rename_all = "SCREAMING_SNAKE_CASE")]
/// enum Channel {
///     AnIn1,
///     #[fletch(rename = "DIG_3")]
///     DigIn3,
/// }
///
/// assert_eq!(Channel::AnIn1.name(), "AN_IN1");
/// assert_eq!(Channel::from_name("DIG_3"), Some(Channel::DigIn3));
/// ```
///
/// Only fieldless enums can derive it:
///
/// ```compile_fail
/// #[derive(fletch::FletchEnum)]
/// enum Reading {
///     Raw(i64),
///     Missing,
/// }
/// ```
pub trait FletchEnum: Sized + Send + Sync + 'static {
    /// The name stored for this variant.
    fn name(&self) -> &'static str;

    /// The variant stored as `name`, if any.
    fn from_name(name: &str) -> Option<Self>;
}

/// Reads an enum field from a string cell. Used by derived code.
#[doc(hidden)]
pub fn enum_from_row_value<E: FletchEnum>(value: RowValue<'_>, column: &str) -> Result<E> {
    match value {
        RowValue::Str(name) => E::from_name(name).ok_or_else(|| {
            anyhow!(
                "column `{column}` holds `{name}`, which is not a {} variant",
                std::any::type_name::<E>()
            )
        }),
        RowValue::Null => Err(anyhow!(
            "column `{column}` holds a null, but its field is not an `Option`"
        )),
        other => Err(anyhow!(
            "column `{column}` expects a {} name, got {other:?}",
            std::any::type_name::<E>()
        )),
    }
}
