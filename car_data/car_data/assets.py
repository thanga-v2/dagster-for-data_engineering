import dagster as dg
import polars as pl

csv_url ="https://raw.githubusercontent.com/Azure/carprice/refs/heads/master/dataset/carprice.csv"
csv_path = "mydata/carprice.csv"

# print(pl)

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
