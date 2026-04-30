use std::fmt::{Display, from_fn};

const EM_DASH: &str = "—";

/// Display option
pub trait DisplayOption {
    fn display(self) -> impl Display;
}

impl<T: Display> DisplayOption for Option<T> {
    fn display(self) -> impl Display {
        from_fn(move |f| match &self {
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
