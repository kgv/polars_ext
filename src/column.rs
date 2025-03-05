use polars::prelude::*;

/// Nullifies the values in a [`Column`] based on a mask.
///
/// This function takes a [`Column`] and applies a nullification process to it.
///
/// # Arguments
///
/// * `column` - A [`Column`] to be nullified.
///
/// # Returns
///
/// * `PolarsResult<Option<Column>>` - The nullified column.
///
/// # Errors
///
/// This function will return an error if the column cannot be converted to a
/// struct or if the nullification process fails.
pub fn nullify(column: Column) -> PolarsResult<Option<Column>> {
    use crate::series::nullify;

    Ok(Some(nullify(&column.struct_()?.fields_as_series())?.into()))
}
