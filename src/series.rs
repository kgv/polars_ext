use polars::prelude::*;

/// Creates a function that applies a transformation to a [`Column`].
///
/// # Arguments
///
/// * `function` - A function that takes a [`Series`] and returns a transformed
///   [`Series`].
///
/// # Returns
///
/// * A function that takes a [`Column`] and returns an optional transformed
///   [`Column`].
pub fn column(
    function: impl Fn(&Series) -> PolarsResult<Series>,
) -> impl Fn(Column) -> PolarsResult<Option<Column>> {
    move |column| {
        let Some(series) = column.as_series() else {
            return Ok(None);
        };
        Ok(Some(function(series)?.into_column()))
    }
}

/// Normalizes the values in a [`Series`].
///
/// # Arguments
///
/// * `series` - A [`Series`] whose values are to be normalized.
///
/// # Returns
///
/// * A normalized [`Series`].
pub fn normalize(series: &Series) -> PolarsResult<Series> {
    let chunked_array = series.f64()?;
    let sum = chunked_array.sum();
    Ok(chunked_array
        .iter()
        .map(|option| Some(option.unwrap_or_default() / sum?))
        .collect::<Float64Chunked>()
        .into_series())
}

/// Nullifies the values in a [`Series`] based on a mask.
///
/// # Arguments
///
/// * `series` - A slice of [`Series`] where the first element is the target
///   series and the second element is the mask.
///
/// # Returns
///
/// * A [`Series`] with nullified values.
pub fn nullify(series: &[Series]) -> PolarsResult<Series> {
    let null =
        Scalar::new(series[0].dtype().clone(), AnyValue::Null).into_series(PlSmallStr::EMPTY);
    series[0].zip_with(series[1].bool()?, &null)
}

/// Rounds the values in a [`Series`] to a specified number of decimal places.
///
/// # Arguments
///
/// * `decimals` - The number of decimal places to round to.
///
/// # Returns
///
/// * A function that takes a [`Series`] and returns a rounded [`Series`].
pub fn round(decimals: u32) -> impl Fn(&Series) -> PolarsResult<Series> {
    move |series| series.round(decimals, RoundMode::HalfToEven)
}
