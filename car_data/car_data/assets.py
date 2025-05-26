import dagster as dg
import polars as pl
import duckdb as duck

csv_url ="https://raw.githubusercontent.com/Azure/carprice/refs/heads/master/dataset/carprice.csv"
csv_path = "mydata/carprice.csv"

# print(pl)

# duckdb
duckdb_path = "duckdb_data/car_data.duckdb"
table_name = "avg_price_per_brand"

@dg.asset
def car_data_file(context: dg.AssetExecutionContext):
    """Downloads the CSV directly with polars and saves it locally."""
    context.log.info("Downloading the CSV file")
    df = pl.read_csv(csv_url)
    print("\n car frame: \n", df.head())
    df = df.with_columns(
        [           # unique one from polars
        pl.col('normalized-losses').cast(pl.Float64, strict=False),
        pl.col('price').cast(pl.Float64, strict=False),
        ]
    )
    df.write_csv(csv_path)


@dg.asset
def test(context: dg.AssetExecutionContext):
    """Just a dummy function to check how its behaving"""
    context.log.info("Dummy data to check and verify")
    # with open("mydatac/carprice.csv", "r") as f:
    #     print("the csv file is", f)
    context.log.info("Now its the end,")
    context.log.info("Now try to enter multiple logs")     
