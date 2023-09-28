#![allow(dead_code)]

use anyhow::Result;
use poc_polars::*;
use polars::prelude::*;
fn main() -> Result<()> {
    view_problem()?;
    Ok(())
}

fn view_df_test() -> Result<()> {
    let df = get_lazy_df_receipt()?;
    println!("{:?}", df.collect().unwrap());

    Ok(())
}

fn p1() -> Result<()> {
    println!("P-001: レシート明細データ（df_receipt）から全項目の先頭10件を表示し、どのようなデータを保有しているか目視で確認せよ。");
    let df = get_lazy_df_receipt()?.collect()?;
    let head_df = df.head(Some(10));

    println!("{:?}", head_df);

    Ok(())
}

fn p2() -> Result<()> {
    println!("P-002: レシート明細データ（df_receipt）から売上年月日（sales_ymd）、顧客ID（customer_id）、\
        商品コード（product_cd）、売上金額（amount）の順に列を指定し、10件表示せよ。");

    // 処理
    let mut df = get_lazy_df_receipt()?.collect()?;
    df = df.head(Some(10));
    df = df.select(["sales_ymd", "customer_id", "product_cd", "amount"])?;

    // 回答
    println!("{:?}", df);

    Ok(())
}

fn p3() -> Result<()> {
    println!("P-003: レシート明細データ（df_receipt）から売上年月日（sales_ymd）、顧客ID（customer_id）、商品コード（product_cd）、\
        売上金額（amount）の順に列を指定し、10件表示せよ。ただし、sales_ymdをsales_dateに項目名を変更して抽出すること。");

    // 処理
    let mut df = get_lazy_df_receipt()?.collect()?;
    df = df
        .head(Some(10))
        .select(["sales_ymd", "customer_id", "product_cd", "amount"])?;
    // 返り値がPolarsResult<&mut DataFrame>の場合、チェインできない
    df.rename("sales_ymd", "sales_date")?;

    // 回答
    println!("{:?}", df);

    Ok(())
}

fn p4() -> Result<()> {
    println!("P-004: レシート明細データ（df_receipt）から売上日（sales_ymd）、顧客ID（customer_id）、商品コード（product_cd）、\
        売上金額（amount）の順に列を指定し、以下の条件を満たすデータを抽出せよ。\
        顧客ID（customer_id）がCS018205000001");

    // 処理
    let lazy_df = get_lazy_df_receipt()?;

    let df = lazy_df
        .select([
            col("sales_ymd"),
            col("customer_id"),
            col("product_cd"),
            col("amount"),
        ])
        .filter(col("customer_id").eq(lit("CS018205000001")))
        .collect()?;
    // 回答
    println!("{:?}", df);

    Ok(())
}

fn p5() -> Result<()> {
    println!("P-005: レシート明細データ（df_receipt）から売上日（sales_ymd）、顧客ID（customer_id）、商品コード（product_cd）、\
        売上金額（amount）の順に列を指定し、以下の全ての条件を満たすデータを抽出せよ。");
    println!("顧客ID（customer_id）がCS018205000001");

    println!("売上金額（amount）が1,000以上");

    // 処理
    let lazy_df = get_lazy_df_receipt()?;
    let df = lazy_df
        .select([
            col("sales_ymd"),
            col("customer_id"),
            col("product_cd"),
            col("amount"),
        ])
        .filter(col("customer_id").eq(lit("CS018205000001")))
        // grater_than
        .filter(col("amount").gt_eq(lit(1000)))
        .collect()?;

    // 回答
    println!("{:?}", df);

    Ok(())
}

fn p6() -> Result<()> {
    println!("P-006: レシート明細データ（df_receipt）から売上日（sales_ymd）、顧客ID（customer_id）、商品コード（product_cd）、\
            売上数量（quantity）、売上金額（amount）の順に列を指定し、以下の全ての条件を満たすデータを抽出せよ。");
    println!("顧客ID（customer_id）がCS018205000001");
    println!("売上金額（amount）が1,000以上または売上数量（quantity）が5以上");

    // 処理
    let lazy_df = get_lazy_df_receipt()?;
    let df = lazy_df
        .select([
            col("sales_ymd"),
            col("customer_id"),
            col("product_cd"),
            col("quantity"),
            col("amount"),
        ])
        .filter(col("customer_id").eq(lit("CS018205000001")))
        .filter(
            col("amount")
                .gt_eq(lit(1000))
                .or(col("quantity").gt_eq(lit(5))),
        )
        .collect()?;

    // 回答
    println!("{:?}", df);

    Ok(())
}
fn p7() -> Result<()> {
    println!("P-007: レシート明細データ（df_receipt）から売上日（sales_ymd）、顧客ID（customer_id）、商品コード（product_cd）、\
            売上金額（amount）の順に列を指定し、以下の全ての条件を満たすデータを抽出せよ。");
    println!("顧客ID（customer_id）がCS018205000001");
    println!("売上金額（amount）が1,000以上2,000以下");

    // 処理
    let lazy_df = get_lazy_df_receipt()?;
    let df = lazy_df
        .select([
            col("sales_ymd"),
            col("customer_id"),
            col("product_cd"),
            col("amount"),
        ])
        .filter(col("customer_id").eq(lit("CS018205000001")))
        .filter(
            col("amount")
                .gt_eq(lit(1000))
                .and(col("amount").lt_eq(lit(2000))),
        )
        .collect()?;

    // 回答
    println!("{:?}", df);

    Ok(())
}
fn p8() -> Result<()> {
    println!("P-008: レシート明細データ（df_receipt）から売上日（sales_ymd）、顧客ID（customer_id）、商品コード（product_cd）、\
        売上金額（amount）の順に列を指定し、以下の全ての条件を満たすデータを抽出せよ。");
    println!("顧客ID（customer_id）がCS018205000001");
    println!("商品コード（product_cd）がP071401019以外");
    // 処理
    let lazy_df = get_lazy_df_receipt()?;
    let df = lazy_df
        .select([
            col("sales_ymd"),
            col("customer_id"),
            col("product_cd"),
            col("amount"),
        ])
        .filter(col("customer_id").eq(lit("CS018205000001")))
        .filter(col("product_cd").neq(lit("P071401019")))
        .collect()?;
    // そもそもCS018205000001でP071401019がない気がする
    // 回答
    println!("{:?}", df);

    Ok(())
}

fn p9() -> Result<()> {
    println!("P-009: 以下の処理において、出力結果を変えずにORをANDに書き換えよ。");
    println!("df_store.query('not(prefecture_cd == \"13\" | floor_area > 900)')");

    // 処理
    let lazy_df = get_lazy_df_store()?;
    let df = lazy_df
        .filter(col("prefecture_cd").neq(lit(13)))
        .filter(col("floor_area").lt_eq(lit(900)))
        .collect()?;

    // 回答
    println!("{:?}", df);

    Ok(())
}

fn p10() -> Result<()> {
    println!("P-010: 店舗データ（df_store）から、店舗コード（store_cd）がS14で始まるものだけ全項目抽出し、10件表示せよ。");

    // 処理
    let lazy_df = get_lazy_df_store()?;
    let df = lazy_df
        .filter(col("store_cd").str().starts_with(lit("S14")))
        .collect()?
        .head(Some(10));

    // 回答
    println!("{:?}", df);

    Ok(())
}
fn p11() -> Result<()> {
    println!("P-011: 顧客データ（df_customer）から顧客ID（customer_id）の末尾が1のものだけ全項目抽出し、10件表示せよ。");

    // 処理
    let lazy_df = get_lazy_df_customer()?;
    let df = lazy_df
        .filter(col("customer_id").str().ends_with(lit("1")))
        .collect()?
        .head(Some(10));

    // 回答
    println!("{:?}", df);

    Ok(())
}

fn p12() -> Result<()> {
    println!("P-012: 店舗データ（df_store）から、住所 (address) に横浜市が含まれるものだけ全項目表示せよ。");

    // 処理
    let lazy_df = get_lazy_df_store()?;
    let df = lazy_df
        .filter(col("address").str().contains_literal(lit("横浜")))
        .collect()?;

    // 回答
    println!("{:?}", df);

    Ok(())
}

fn p13() -> Result<()> {
    println!("P-013: 顧客データ（df_customer）から、ステータスコード（status_cd）の先頭がアルファベットのA〜Fで始まるデータを全項目抽出し、10件表示せよ。");

    // 処理
    let lazy_df = get_lazy_df_customer()?;

    // let df = lazy_df
    //     .filter(
    //         col("status_cd")
    //             .str()
    //             .starts_with(lit("A"))
    //             .or(col("status_cd").str().starts_with(lit("B")))
    //             .or(col("status_cd").str().starts_with(lit("C")))
    //             .or(col("status_cd").str().starts_with(lit("D")))
    //             .or(col("status_cd").str().starts_with(lit("E")))
    //             .or(col("status_cd").str().starts_with(lit("F"))),
    //     )
    //     .collect()?
    //     .head(Some(10));

    let df = lazy_df
        // polars::prelude::string::StringNameSpace.contain(self, pat: Expr, strict: bool) -> Expr
        // pat: Expr、正規表現をlit()で包めばよい？
        // strict: bool、　無効な正規表現をエラーとするか
        .filter(col("status_cd").str().contains(lit(r"^[A-F]"), false))
        .collect()?
        .head(Some(10));

    // 回答
    println!("{:?}", df);

    Ok(())
}

fn p14() -> Result<()> {
    println!("P-014: 顧客データ（df_customer）から、ステータスコード（status_cd）の末尾が数字の1〜9で終わるデータを全項目抽出し、10件表示せよ。");

    // 処理
    let lazy_df = get_lazy_df_customer()?;
    let df = lazy_df
        .filter(col("status_cd").str().contains(lit(r"[1-9]$"), false))
        .collect()?
        .head(Some(10));

    // 回答
    println!("{:?}", df);

    Ok(())
}

fn p15() -> Result<()> {
    println!("P-015: 顧客データ（df_customer）から、ステータスコード（status_cd）の先頭がアルファベットのA〜Fで始まり、末尾が数字の1〜9で終わるデータを全項目抽出し、10件表示せよ。");

    // 処理
    let lazy_df = get_lazy_df_customer()?;
    // +	直前の文字が１回以上繰り返す場合にマッチ
    // .	任意の１文字にマッチ
    // *	直前の文字が０回以上繰り返す場合にマッチ
    // *?	直前の文字が０回以上繰り返す場合にマッチ
    // \S	すべての非空白文字（非空白文字）
    let df = lazy_df
        .filter(
            col("status_cd")
                .str()
                // .contains(lit(r"^[A-F]+\S+[1-9]$"), false)
                .contains(lit(r"^[A-F]+.*?[1-9]$"), false),
        )
        .collect()?
        .head(Some(10));

    // 回答
    println!("{:?}", df);

    Ok(())
}

fn p16() -> Result<()> {
    println!("P-016: 店舗データ（df_store）から、電話番号（tel_no）が3桁-3桁-4桁のデータを全項目表示せよ。");

    // 処理
    let lazy_df = get_lazy_df_store()?;
    let df = lazy_df
        // \d	すべての数字（１０進数字）
        .filter(
            col("tel_no")
                .str()
                .contains(lit(r"\d{3}-\d{3}-\d{4}"), false),
        )
        .collect()?;

    // 回答
    println!("{:?}", df);

    Ok(())
}
fn p17() -> Result<()> {
    println!("P-017: 顧客データ（df_customer）を生年月日（birth_day）で高齢順にソートし、先頭から全項目を10件表示せよ。");

    // 処理
    let lazy_df = get_lazy_df_customer()?;
    let df = lazy_df
        .sort(
            "birth_day",
            SortOptions {
                descending: (false), // 降順？生年月日（birth_day）  を高齢順は生年月日が早い方が上なので昇順
                nulls_last: (true),
                multithreaded: (false),
                maintain_order: (false), // 順番を保つ？どっちにしても変わらん
            },
        )
        .collect()?
        .head(Some(10));

    // 回答
    println!("{:?}", df);

    Ok(())
}

fn p18() -> Result<()> {
    println!("P-018: 顧客データ（df_customer）を生年月日（birth_day）で若い順にソートし、先頭から全項目を10件表示せよ。");

    // 処理
    let lazy_df = get_lazy_df_customer()?;
    let mut df = lazy_df
        .sort(
            "birth_day",
            SortOptions {
                descending: (true), // 生年月日（birth_day）を若い順が降順
                nulls_last: (true),
                multithreaded: (false),
                maintain_order: (false), // 順番を保つ？どっちにしても変わらん
            },
        )
        .collect()?
        .head(Some(10));

    // 回答
    println!("{:?}", df);

    // 回答ファイルの作成

    let mut file = std::fs::File::create("./answer/p18.csv").unwrap();
    CsvWriter::new(&mut file).finish(&mut df).unwrap();

    Ok(())
}

fn p19() -> Result<()> {
    println!("P-019: レシート明細データ（df_receipt）に対し、1件あたりの売上金額（amount）が高い順にランクを付与し、先頭から10件表示せよ。\
            項目は顧客ID（customer_id）、売上金額（amount）、付与したランクを表示させること。\
            なお、売上金額（amount）が等しい場合は同一順位を付与するものとする。");

    // 処理
    let lazy_df = get_lazy_df_receipt()?;
    let mut df = lazy_df
        .select([
            col("customer_id"),
            col("amount"),
            col("amount")
                .rank(
                    RankOptions {
                        method: RankMethod::Min,
                        descending: true,
                    },
                    None, // seed:乱数？？
                )
                .alias("rank"),
        ])
        .sort(
            "rank",
            SortOptions {
                descending: (false),
                nulls_last: (true),
                multithreaded: (false),
                maintain_order: (false), // 順番を保つ？どっちにしても変わらん
            },
        )
        .collect()?
        .head(Some(10));

    // 回答
    println!("{:?}", df);

    // 回答ファイルの作成

    let mut file = std::fs::File::create("./answer/p19.csv").unwrap();
    CsvWriter::new(&mut file).finish(&mut df).unwrap();

    Ok(())
}
fn p20() -> Result<()> {
    println!("P-020: レシート明細データ（df_receipt）に対し、1件あたりの売上金額（amount）が高い順にランクを付与し、先頭から10件表示せよ。\
        項目は顧客ID（customer_id）、売上金額（amount）、\
        付与したランクを表示させること。なお、売上金額（amount）が等しい場合でも別順位を付与すること。");

    // [polars.Expr.rank — Polars documentation](https://pola-rs.github.io/polars/py-polars/html/reference/expressions/api/polars.Expr.rank.html#polars.Expr.rank)

    // 処理
    let lazy_df = get_lazy_df_receipt()?;
    let mut df = lazy_df
        .select([
            col("customer_id"),
            col("amount"),
            col("amount")
                .rank(
                    RankOptions {
                        method: RankMethod::Ordinal, // 同店時の処理
                        descending: true,            // 降順≒大きい順
                    },
                    None, // seed:乱数？？
                )
                .alias("rank"),
        ])
        .sort(
            "rank",
            SortOptions {
                descending: (false),
                nulls_last: (true),
                multithreaded: (false),
                maintain_order: (false), // 順番を保つ？どっちにしても変わらん
            },
        )
        .collect()?
        .head(Some(10));

    // 回答
    println!("{:?}", df);

    // 回答ファイルの作成

    let mut file = std::fs::File::create("./answer/p20.csv").unwrap();
    CsvWriter::new(&mut file).finish(&mut df).unwrap();

    Ok(())
}

fn p21() -> Result<()> {
    println!("P-021: レシート明細データ（df_receipt）に対し、件数をカウントせよ。");

    // 処理
    let lazy_df = get_lazy_df_receipt()?;
    let df = lazy_df.collect()?;

    let (height, _width) = df.shape();

    // 回答
    println!("{:?}", height);

    // 回答ファイルの作成

    // let mut file = std::fs::File::create("./answer/p21.csv").unwrap();
    // CsvWriter::new(&mut file).finish(&mut df).unwrap();

    Ok(())
}

fn p22() -> Result<()> {
    println!("P-022: レシート明細データ（df_receipt）の顧客ID（customer_id）に対し、ユニーク件数をカウントせよ。");

    // 処理
    let lazy_df = get_lazy_df_receipt()?;
    let df = lazy_df.select([col("customer_id").unique()]).collect()?;

    let (height, _width) = df.shape();

    // 回答
    println!("{:?}", height);

    Ok(())
}

fn p23() -> Result<()> {
    println!("P-023: レシート明細データ（df_receipt）に対し、店舗コード（store_cd）ごとに売上金額（amount）と売上数量（quantity）を合計せよ。");

    // 処理
    let lazy_df = get_lazy_df_receipt()?;
    let df = lazy_df
        .groupby([col("store_cd")])
        .agg([
            col("amount").sum().alias("amount"),
            col("quantity").sum().alias("quantity"),
        ])
        .collect()?;

    // 回答
    println!("{:?}", df);

    Ok(())
}
fn view_problem() -> Result<()> {
    p23()?;
    Ok(())
}
