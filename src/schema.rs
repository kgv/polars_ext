use polars::prelude::*;

pub fn matches_schema(data_frame: &DataFrame, schema: &Schema) -> PolarsResult<()> {
    let cast = data_frame.schema().matches_schema(schema)?;
    if cast {
        return Err(polars_err!(SchemaMismatch: "the scheme requires a cast"));
    }
    Ok(())
}
