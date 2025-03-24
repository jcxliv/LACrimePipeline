import sys
from pyspark.sql import SparkSession

def main():
    if len(sys.argv) != 4:
        print("Usage: spark-submit spark.py [crime_csv] [demo_csv] [output_dir]")
        sys.exit(1)
    
    crime_csv = sys.argv[1]
    demo_csv = sys.argv[2]
    output_dir = sys.argv[3]
    
    print("Initializing Spark Session...")
    spark = SparkSession.builder \
        .appName("CrimeDemographicsProcessing") \
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "20") \
        .getOrCreate()
    
    print("Loading Crime Data (CSV)...")
    crime_df = spark.read.option("header", "true") \
                         .option("inferSchema", "true") \
                         .option("delimiter", ",") \
                         .option("mode", "DROPMALFORMED") \
                         .csv(crime_csv)
    
    print("Loading Demographics Data (CSV)...")
    demo_df = spark.read.csv(demo_csv, header=True, inferSchema=True)
    
    print("Cleaning Data (Removing Duplicates)...")
    crime_df = crime_df.dropDuplicates()
    demo_df = demo_df.dropDuplicates()
    
    print("Saving Crime Data as Parquet...")
    crime_df.write.mode("overwrite").parquet(f"{output_dir}/crime_cleaned.parquet")
    
    print("Saving Demographics Data as Parquet...")
    demo_df.write.mode("overwrite").parquet(f"{output_dir}/demo_cleaned.parquet")
    
    print("Data Processing Completed!")
    spark.stop()

if __name__ == "__main__":
    main()
