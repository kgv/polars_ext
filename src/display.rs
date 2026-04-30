use crate::r#const::EM_DASH;
use std::fmt::{Display, from_fn};

pub fn option<T: Display>(option: &Option<T>) -> impl Display {
    from_fn(move |f| match option {
        None => f.write_str(EM_DASH),
        Some(t) => Display::fmt(t, f),
    })
}
