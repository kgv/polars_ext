use super::{column, hash, normalize};
use polars::prelude::*;

/// Extension methods for [`Expr`]
pub trait ExprExt {
    fn destruct(self, names: impl IntoIterator<Item = impl AsRef<str>>) -> Expr;

    fn hash(self) -> Expr;

    fn normalize(self) -> Expr;
}

impl ExprExt for Expr {
    fn destruct(mut self, names: impl IntoIterator<Item = impl AsRef<str>>) -> Expr {
        for name in names {
            self = self.struct_().field_by_name(name.as_ref());
        }
        self
    }

    /// Hash column, type [`u64`], name "Hash"
    fn hash(self) -> Expr {
        self.apply(column(hash), GetOutput::from_type(DataType::UInt64))
            .alias("Hash")
    }

    /// Normalize column, type [`f64`], the same name
    fn normalize(self) -> Expr {
        self.apply(column(normalize), GetOutput::same_type())
    }
}

/// Extension `if` methods for [`Expr`]
pub trait ExprIfExt: ExprExt {
    fn clip_min_if(self, clip: bool) -> Expr;

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
