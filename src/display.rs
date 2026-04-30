use crate::r#const::EM_DASH;
use std::fmt::{Display, from_fn};

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

// pub fn option<T: Display>(option: Option<T>) -> impl Display {
//     from_fn(move |f| match &option {
//         None => f.write_str(EM_DASH),
//         Some(t) => Display::fmt(t, f),
//     })
// }
