use polars::prelude::*;
use pyo3_polars::derive::polars_expr;
use serde::Deserialize;

#[derive(Deserialize)]
struct Offset {
    amount: i64,
}

#[polars_expr(output_type=Int64)]
fn add_offset(inputs: &[Series], kwargs: Offset) -> PolarsResult<Series> {
    Ok(inputs[0]
        .i64()?
        .apply_values(|value| value + kwargs.amount)
        .into_series())
}
