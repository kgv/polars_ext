use ahash::RandomState;
use polars::prelude::*;

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

pub fn hash(series: &Series) -> PolarsResult<Series> {
    Ok(series
        .iter()
        .map(|value| Ok(Some(RandomState::with_seeds(1, 2, 3, 4).hash_one(value))))
        .collect::<PolarsResult<UInt64Chunked>>()?
        .into_series())
}

pub fn normalize(series: &Series) -> PolarsResult<Series> {
    let chunked_array = series.f64()?;
    let sum = chunked_array.sum();
    Ok(chunked_array
        .iter()
        .map(|option| Some(option.unwrap_or_default() / sum?))
        .collect::<Float64Chunked>()
        .into_series())
}

pub fn nullify(series: &Series, mask: &ChunkedArray<BooleanType>) -> PolarsResult<Series> {
    let null = Scalar::new(series.dtype().clone(), AnyValue::Null).into_series(PlSmallStr::EMPTY);
    series.zip_with(mask, &null)
}

pub fn round(decimals: u32) -> impl Fn(&Series) -> PolarsResult<Series> {
    move |series| series.round(decimals)
}
