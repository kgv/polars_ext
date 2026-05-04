use crate::series::{column, hash};
use polars::prelude::*;

/// Extension methods for [`Expr`]
pub trait ExprExt {
    /// Conditionally clips the minimum value of an [`Expr`].
    ///
    /// # Arguments
    ///
    /// * `clip` - A boolean indicating whether to clip the minimum value.
    ///
    /// # Returns
    ///
    /// * A clipped [`Expr`] if `clip` is true, otherwise the original [`Expr`].
    fn clip_unsigned(self, clip: bool) -> Expr;

    /// Destructs an [`Expr`] into multiple fields.
    ///
    /// # Arguments
    ///
    /// * `names` - An iterator of field names to destruct into.
    ///
    /// # Returns
    ///
    /// * A destructed [`Expr`].
    fn destruct(self, names: impl IntoIterator<Item = impl AsRef<str>>) -> Expr;

    /// Hashes the values in an [`Expr`].
    ///
    /// # Returns
    ///
    /// * An [`Expr`] with hashed values.
    fn hash(self) -> Expr;

    /// Normalizes the values in an [`Expr`].
    ///
    /// # Returns
    ///
    /// * An [`Expr`] with normalized values.
    fn normalize(self, normalize: bool) -> Expr;

    /// Nullify the values in an [`Expr`].
    ///
    /// # Returns
    ///
    /// * An [`Expr`] with nullified values.
    fn nullify(self, mask: Expr) -> Expr;

    fn percent(self, percent: bool) -> Expr;

    // #[cfg(feature = "precision")]
    fn precision(self, precision: usize, significant: bool) -> Expr;
}

impl ExprExt for Expr {
    fn clip_unsigned(self, clip: bool) -> Expr {
        if clip { self.clip_min(lit(0)) } else { self }
    }

    fn destruct(mut self, names: impl IntoIterator<Item = impl AsRef<str>>) -> Expr {
        for name in names {
            self = self.struct_().field_by_name(name.as_ref());
        }
        self
    }

    fn hash(self) -> Expr {
        self.apply(column(|series| Ok(hash(series))), |_, field| {
            Ok(Field::new(field.name.clone(), DataType::UInt64))
        })
        .alias("Hash")
    }

    fn normalize(self, normalize: bool) -> Expr {
        if normalize {
            self.clone() / self.sum()
        } else {
            self
        }
    }

    fn nullify(self, mask: Expr) -> Expr {
        ternary_expr(mask, self, lit(NULL))
    }

    fn percent(self, percent: bool) -> Expr {
        if percent { self * lit(100) } else { self }
    }

    // #[cfg(feature = "precision")]
    fn precision(self, precision: usize, significant: bool) -> Expr {
        // Если число меньше 1, то возможно significant.
        ternary_expr(
            self.clone().abs().lt(1).and(significant),
            self.clone().round_sig_figs(precision as _),
            self.round(precision as _, RoundMode::HalfToEven),
        )
    }
}

#[cfg(feature = "array")]
pub mod array;
