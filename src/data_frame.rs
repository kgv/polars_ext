use polars::prelude::*;

/// Extension methods for [`DataFrame`]
pub trait DataFrameExt {
    fn add_row(&mut self) -> PolarsResult<()>;

    fn delete_row(&mut self, row: usize) -> PolarsResult<()>;
}

impl DataFrameExt for DataFrame {
    fn add_row(&mut self) -> PolarsResult<()> {
        let schema = self.schema();
        let columns = schema
            .iter()
            .map(|(name, _dtype)| Series::new_null(name.clone(), 1).into_column())
            .collect();
        let df = unsafe { DataFrame::new_no_checks(1, columns) };
        *self = self.vstack(&df)?;
        self.as_single_chunk_par();
        Ok(())
    }

    fn delete_row(&mut self, row: usize) -> PolarsResult<()> {
        *self = self
            .slice(0, row)
            .vstack(&self.slice((row + 1) as _, usize::MAX))?;
        self.as_single_chunk_par();
        Ok(())
    }
}
