import dagster as dg
import polars as pl
import duckdb as duck

csv_url ="https://raw.githubusercontent.com/Azure/carprice/refs/heads/master/dataset/carprice.csv"
csv_path = "mydata/carprice.csv"

# print(pl)

# duckdb

# print(duck)

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

 
@dg.asset(deps=[car_data_file])
def avg_price_per_brand(context : dg.AssetExecutionContext):
    """Computes the average price per brand and stores the data in duckDB as mentioned."""
    context.log.inf("from the duckDB")
    df = pl.read_csv(csv_path)
    df = df.drop_nulls(['price'])

    # compute the average price
    avg_price = df.group_by("make").agg(
        pl.col("price").mean().alias('avg_price_per_brand')
    )

    # store in duckdb by converting into list of tuples.
    data = [(row["make"],row["avg_price_per_brand"]) for row in avg_price.to_dicts() ]

    with duckdb.connect(duckdb_path) as conn:
        conn.execute(f"DROP TABLE IF EXISTS {table_name}")
        conn.execute(f"CREATE TABLE {table_name} (make TEXT, avg_price DOUBLE) ")

        # Insert the Data
        conn.executemany(f"INSERT INTO {table_name} (make,avg_price) VALUES (? , ?)", data)





