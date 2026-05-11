use crate::expr::ExprExt;
use const_format::formatcp;
use polars::prelude::*;
use typed_builder::TypedBuilder;

pub const ARRAY: &str = "Array";
pub const MEAN: &str = "Mean";
pub const STANDARD_DEVIATION: &str = "StandardDeviation";

pub fn eval_arr(element: Expr, f: impl Fn(Expr) -> PolarsResult<Expr>) -> PolarsResult<Expr> {
    Ok(concat_arr(vec![f(element
        .arr()
        .to_struct(None)
        .struct_()
        .field_by_name("*"))?])?
    .name()
    .keep())
}

/// Struct with mean, standard deviation and array fields
#[derive(TypedBuilder)]
#[builder(build_method(into=Expr))]
pub struct Array {
    expr: Expr,

    ddof: u8,
    precision: usize,
    significant: bool,

    #[builder(default)]
    percent: bool,
}

impl From<Array> for Expr {
    fn from(value: Array) -> Self {
        as_struct(vec![
            value.expr.clone().arr().eval(
                element()
                    .percent(value.percent)
                    .precision(value.precision, value.significant),
                false,
            ),
            value
                .expr
                .clone()
                .arr()
                .mean()
                .percent(value.percent)
                .precision(value.precision, value.significant)
                .alias(MEAN),
            value
                .expr
                .clone()
                .arr()
                .std(value.ddof)
                .percent(value.percent)
                .precision(value.precision, value.significant)
                .alias(STANDARD_DEVIATION),
        ])
        .struct_()
        .rename_fields(vec![ARRAY, MEAN, STANDARD_DEVIATION])
    }
}
