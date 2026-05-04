pub mod prelude {
    pub use crate::{data_frame::DataFrameExt, expr::ExprExt, option::DisplayOption};

    #[cfg(feature = "array")]
    pub use crate::expr::array::{Array, eval_arr};

    #[cfg(feature = "temporal_conversions")]
    pub use polars_arrow::temporal_conversions::{
        timestamp_ms_to_datetime, timestamp_ns_to_datetime, timestamp_us_to_datetime,
    };

    pub mod r#const {
        pub use crate::option::EM_DASH;

        #[cfg(feature = "array")]
        pub use crate::expr::array::{ARRAY, MEAN, STANDARD_DEVIATION};
    }
}

pub mod column;
pub mod data_frame;
pub mod expr;
pub mod option;
pub mod series;
