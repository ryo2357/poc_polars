use anyhow::Result;
use polars::prelude::*;

pub fn get_lazy_df_receipt() -> Result<LazyFrame> {
    let path = "./100knocks-preprocess/docker/work/data/receipt.csv";
    let df: LazyFrame = LazyCsvReader::new(path).has_header(true).finish()?;
    Ok(df)
}

pub fn get_lazy_df_store() -> Result<LazyFrame> {
    let path = "./100knocks-preprocess/docker/work/data/store.csv";
    let df: LazyFrame = LazyCsvReader::new(path).has_header(true).finish()?;
    Ok(df)
}

pub fn get_lazy_df_customer() -> Result<LazyFrame> {
    let path = "./100knocks-preprocess/docker/work/data/customer.csv";
    let df: LazyFrame = LazyCsvReader::new(path).has_header(true).finish()?;
    Ok(df)
}
