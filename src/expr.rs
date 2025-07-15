use crate::series::{column, normalize};
use polars::prelude::*;

/// Extension methods for [`Expr`]
pub trait ExprExt {
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

    /// Normalizes the values in an [`Expr`].
    ///
    /// # Returns
    ///
    /// * An [`Expr`] with normalized values.
    fn normalize(self) -> Expr;

    /// Nullify the values in an [`Expr`].
    ///
    /// # Returns
    ///
    /// * An [`Expr`] with nullified values.
    fn nullify(self, mask: Expr) -> Expr;
}

impl ExprExt for Expr {
    fn destruct(mut self, names: impl IntoIterator<Item = impl AsRef<str>>) -> Expr {
        for name in names {
            self = self.struct_().field_by_name(name.as_ref());
        }
        self
    }

    fn normalize(self) -> Expr {
        self.apply(column(normalize), GetOutput::same_type())
    }

    fn nullify(self, mask: Expr) -> Expr {
        ternary_expr(mask, self, lit(NULL))
    }
}

/// Extension `if` methods for [`Expr`]
pub trait ExprIfExt: ExprExt {
    /// Conditionally clips the minimum value of an [`Expr`].
    ///
    /// # Arguments
    ///
    /// * `clip` - A boolean indicating whether to clip the minimum value.
    ///
    /// # Returns
    ///
    /// * A clipped [`Expr`] if `clip` is true, otherwise the original [`Expr`].
    fn clip_min_if(self, clip: bool) -> Expr;

    /// Conditionally normalizes the [`Expr`] values.
    ///
    /// # Arguments
    ///
    /// * `normalize` - A boolean indicating whether to normalize the [`Expr`]
    ///   values.
    ///
    /// # Returns
    ///
    /// * A normalized [`Expr`] if `normalize` is true, otherwise the original
    ///   [`Expr`].
    fn normalize_if(self, normalize: bool) -> Expr;
}

impl ExprIfExt for Expr {
    fn clip_min_if(self, clip: bool) -> Expr {
        if clip { self.clip_min(lit(0)) } else { self }
    }

    fn normalize_if(self, normalize: bool) -> Expr {
        if normalize { self.normalize() } else { self }
    }
}
