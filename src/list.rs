use std::fmt::Display;
use polars_utils::format_list_truncated;

pub fn format_list_truncated<const N: usize>(
    iter: impl Iterator<Item = impl Display>,
) -> impl Display {
    // iter.format(", ")
    // frames.iter().map(|frame| frame.meta.format(separator))
    // format_with(", ", |option, f| f(&option.display())).to_string()
    format_list_truncated!(iter, N)
}