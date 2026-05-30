from pyspark.sql import SparkSession
from pathlib import Path

def create_spark_session():
    return (
        SparkSession.builder
        .appName("IPL Bronze Layer")
        .getOrCreate()
    )

def main():
    spark = create_spark_session()

    project_root = Path(__file__).resolve().parents[2]

    matches_file = project_root / "data/raw/matches.csv"
    deliveries_file = project_root / "data/raw/deliveries.csv"

    bronze_matches = project_root / "data/bronze/matches"
    bronze_deliveries = project_root / "data/bronze/deliveries"

    print("Reading matches.csv...")
    matches_df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(str(matches_file))
    )

    print("Reading deliveries.csv...")
    deliveries_df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(str(deliveries_file))
    )

    print(f"Matches Count: {matches_df.count()}")
    print(f"Deliveries Count: {deliveries_df.count()}")

    matches_df.printSchema()
    deliveries_df.printSchema()

    print("Writing Bronze Parquet files...")

    matches_df.write.mode("overwrite").parquet(str(bronze_matches))
    deliveries_df.write.mode("overwrite").parquet(str(bronze_deliveries))

    print("Bronze Layer Created Successfully")

    spark.stop()

if __name__ == "__main__":
    main()

