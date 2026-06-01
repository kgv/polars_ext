use crate::{
    r#const::{ARRAY, MEAN, RELATIVE_STANDARD_DEVIATION, STANDARD_DEVIATION},
    expr::ExprExt,
};
use polars::prelude::*;
use typed_builder::TypedBuilder;

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
    flatten: bool,
    #[builder(default)]
    percent: bool,
}

impl From<Array> for Expr {
    fn from(value: Array) -> Self {
        let mut array = value.expr.clone().arr().eval(
            element()
                .percent(value.percent)
                .precision(value.precision, value.significant),
            false,
        );
        if value.flatten {
            array = array
                .arr()
                .to_struct(Some(PlanCallback::new(move |index| {
                    Ok(format!("{ARRAY}[{index}]"))
                })))
                .struct_()
                .field_by_name("*");
        } else {
            array = array.alias(ARRAY);
        }
        let mean = value.expr.clone().arr().mean();
        let standard_deviation = value.expr.clone().arr().std(value.ddof);
        let mut expr = as_struct(vec![
            array,
            mean.clone()
                .percent(value.percent)
                .precision(value.precision, value.significant)
                .alias(MEAN),
            standard_deviation
                .clone()
                .percent(value.percent)
                .precision(value.precision, value.significant)
                .alias(STANDARD_DEVIATION),
            (standard_deviation / mean)
                .percent(true)
                .precision(value.precision, value.significant)
                .alias(RELATIVE_STANDARD_DEVIATION),
        ])
        .name()
        .keep();
        if value.flatten {
            expr = expr.struct_().field_by_name("*");
        }
        expr
    }
}

// #[test]
// fn test() {
//     use polars::prelude::*;

//     let mut df = df! {
//         "id" => &[1, 2, 3],
//         "my_arrays" => &[
//             Series::new("".into(), &[1.0, 2.0, 3.0]),
//             Series::new("".into(), &[4.0, 5.0, 6.0]),
//             Series::new("".into(), &[7.0, 8.0, 9.0]),
//         ]
//     }
//     .unwrap();
//     df.try_apply("my_arrays", |series| {
//         series.cast(&DataType::Array(Box::new(DataType::Float64), 3))
//     })
//     .unwrap();
//     let lf = df.lazy().with_column(
//         Array::builder()
//             .expr(col("my_arrays"))
//             .ddof(1)
//             .percent(true)
//             .precision(2)
//             .significant(false)
//             .flatten(false)
//             .build().struct_().field_by_name("*"),
//     );
//     println!(": {}", lf.clone().collect().unwrap());
//     // df.lazy().
// }
