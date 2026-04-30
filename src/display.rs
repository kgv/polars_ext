use std::fmt::{Display, from_fn};

pub const EM_DASH: &str = "—";

pub fn option<T: Display>(option: &Option<T>) -> impl Display {
    from_fn(move |f| match option {
        None => f.write_str(EM_DASH),
        Some(t) => Display::fmt(t, f),
    })
}
