use polars::datatypes::AnyValue;
use std::fmt::{Display, from_fn};

use crate::r#const::EM_DASH;

/// Display option
pub trait DisplayOption {
    fn display(self) -> impl Display;
}

impl<T: Display> DisplayOption for Option<T> {
    fn display(self) -> impl Display {
        from_fn(move |f| match &self {
            None if f.alternate() => Display::fmt(&AnyValue::Null, f),
            None => f.write_str(EM_DASH),
            Some(t) => Display::fmt(t, f),
        })
    }
}

impl<T: Display> DisplayOption for &Option<T> {
    fn display(self) -> impl Display {
        self.as_ref().display()
    }
}
