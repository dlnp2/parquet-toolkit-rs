use clap::Arg;
use polars::{lazy::dsl::Expr, prelude::*};
use std::env;

fn main() {
    let mut args = env::args();
    let filename = args.nth(1).unwrap();
    let columns: Vec<Expr> = args
        .nth(2)
        .unwrap()
        .split(',')
        .collect::<Vec<&str>>()
        .into_iter()
        .map(|c| col(c))
        .collect();

    let f = LazyFrame::scan_parquet(filename, Default::default()).unwrap();

    let schema = f.schema().unwrap();
    println!("{schema:?}");

    let head = f.slice(0, 5).select(columns).collect().unwrap();
    println!("{head}");
}
