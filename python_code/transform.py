from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.sql.functions as fn

# Initialize class:
if __name__ == "__main__":
    print("Transform staging data to Hive Started ...")

    # Enable hive support to write data to hive
    spark = SparkSession \
        .builder \
        .appName("Transform staging data to Hive ") \
        .master("local[*]") \
        .config("spark.sql.warehouse.dir", "/hive/warehouse").config("hive.metastore.uris", "thrift://localhost:9083").config("hive.exec.dynamic.partition","true").config("hive.exec.dynamic.partition.mode","nonstrict").enableHiveSupport().getOrCreate()
    
    #Define schema for the raw data
    schema = StructType([
      StructField("Brand Name",StringType(),True),
      StructField("Device Type",StringType(),True),
      StructField("Model Name",StringType(),True),
      StructField("Color",StringType(),True),
      StructField("Selling Price",DecimalType(),True),
      StructField("Original Price",DecimalType(),True),
      StructField("Display",StringType(),True),
      StructField("Rating (Out of 5)",DoubleType(),True),
      StructField("Strap Material",StringType(),True),
      StructField("Average Battery Life (in days)",IntegerType(),True),
      StructField("Reviews",IntegerType(),True)
    ])
    
    # Handling raw data with schema
    rawDf=spark.read.parquet("hdfs://localhost:9000/datalake/staging")
    df=rawDf.selectExpr("CAST(value AS STRING)")
    valueWithSchema=df.select(from_json(col("value"), schema=schema).alias("sangsan17"))
    allValue = valueWithSchema.select("sangsan17.*")
    # df1= spark.sql("select * from result rs JOIN (select Min(`Selling Price`) as cheapest from result where `Brand Name` = 'APPLE') appleView ON rs.`Selling Price` = appleView.cheapest UNION ALL select * from result rs2 JOIN (select Min(`Selling Price`) as cheapest from result where `Brand Name` = 'SAMSUNG') samsungView ON rs2.`Selling Price` = samsungView.cheapest UNION ALL select * from result rs3 JOIN (select Min(`Selling Price`) as cheapest from result where `Brand Name` = 'FOSSIL') fossilView ON rs3.`Selling Price` = fossilView.cheapest")
    #df=spark.sql("select `Brand Name`,`Model Name`,`Display`,Min(`Selling Price`) cheapest from staging group by `Brand Name`,`Model Name`,`Display` order by `Brand Name`, cheapest")
    
    # Write result to hive     
    allValue.write.format("hive").mode("append").saveAsTable("result")
