import json
import time
from datetime import datetime

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws, lit, sha2
from zoneinfo import ZoneInfo

spark = (
    SparkSession.builder
    # Configs para acessar mysql
    .config("spark.driver.extraClassPath", "mysql-connector-java-8.0.33.jar")
    .config("spark.jars", "mysql-connector-j-8.0.33.jar")
    # # Configs para habilitar delta
    .config("spark.sql.extensions", "io.delta.sql.DeltaparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    # Configs para acessar S3/MinIO
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.2")
    .config("spark.hadoop.fs.s3a.access.key", "h5gH5GULzRMqpEn9pjsw")
    .config("spark.hadoop.fs.s3a.secret.key", "xzYJ0kb3nIZ3nW6FA9QbIKUgkNVNICNHFpgfuSoN")
    .config("fs.s3a.path.style.access", "true")
    .config("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("fs.s3a.connection.ssl.enabled", "true")
    .config("fs.s3a.endpoint", "http://127.0.0.1:9000")
    .config("fs.s3a.connection.timeout", "600000")
    # .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .master("local[*]")
    .appName("Load data to bronze layer")
    .enableHiveSupport()
    .getOrCreate()
)


# Connections variables to MySQL
user = "root"
pwd = "1234"
host = "localhost"
port = 3306
db = "library"
uri = f"jdbc:mysql://{host}:{port}/{db}"

# Read tables from MySQL to load into bronze delta layer
author_df = (
    spark.read.format("jdbc")
    .option("driver", "com.mysql.jdbc.Driver")
    .option("url", uri)
    .option("user", user)
    .option("password", pwd)
    .option("dbtable", "author")
    .load()
)

book_df = (
    spark.read.format("jdbc")
    .option("driver", "com.mysql.jdbc.Driver")
    .option("url", uri)
    .option("user", user)
    .option("password", pwd)
    .option("dbtable", "book")
    .load()
)

customer_df = (
    spark.read.format("jdbc")
    .option("driver", "com.mysql.jdbc.Driver")
    .option("url", uri)
    .option("user", user)
    .option("password", pwd)
    .option("dbtable", "customer")
    .load()
)

rent_df = (
    spark.read.format("jdbc")
    .option("driver", "com.mysql.jdbc.Driver")
    .option("url", uri)
    .option("user", user)
    .option("password", pwd)
    .option("dbtable", "rent")
    .load()
)

# Add metadata information  to the DataFrames for later use in transformations
# identified by "sys_" prefixed column names
now = datetime.now(tz=ZoneInfo("America/Sao_Paulo"))

author_cols = author_df.columns
author_final = (
    author_df.withColumn("sys_hash", sha2(concat_ws("||", *author_cols), 256))
    .withColumn("sys_imported_at", lit(now))
    .withColumn("sys_imported_from", lit("MySQL_author"))
)

book_cols = book_df.columns
book_final = (
    book_df.withColumn("sys_hash", sha2(concat_ws("||", *book_cols), 256))
    .withColumn("sys_imported_at", lit(now))
    .withColumn("sys_imported_from", lit("MySQL_book"))
)

customer_cols = customer_df.columns
customer_final = (
    customer_df.withColumn("sys_hash", sha2(concat_ws("||", *customer_cols), 256))
    .withColumn("sys_imported_at", lit(now))
    .withColumn("sys_imported_from", lit("MySQL_customer"))
)

rent_cols = rent_df.columns
rent_final = (
    rent_df.withColumn("sys_hash", sha2(concat_ws("||", *rent_cols), 256))
    .withColumn("sys_imported_at", lit(now))
    .withColumn("sys_imported_from", lit("MySQL_rent"))
)


# Path variables to delta
author_path = "s3a://library/bronze/author"
book_path = "s3a://library/bronze/book"
customer_path = "s3a://library/bronze/customer"
rent_path = "s3a://library/bronze/rental"

tables_dict = """
[
    {
        "table": "author", "df": "author_final", "primary_key": ["author_key"]
    },
    {
        "table": "book", "df": "book_final", "primary_key": ["id_book"]
    },
    {
        "table": "customer", "df": "customer_final", "primary_key": ["id_customer"]
    },
    {
        "table": "rent", "df": "rent_final", "primary_key": [
            "id_book", "id_customer", "event_date", "event_type"
        ]
    }
]
"""

tables = json.loads(tables_dict)


for t in tables:
    start_t = time.time()

    table = t["table"]
    table_path = f"s3a://library/bronze/{table}"
    df = eval(t["df"])
    pk = t["primary_key"]

    merge_condition = " AND ".join(f"old.{i} = new.{i}" for i in pk)

    print(f"Starting to write into Delta Lake from table: {table}")
    print(f"Total rows in dataframe: {df.count()}")

    # Try load delta to do upserts, if it fails then create a new one
    try:
        delta_table = DeltaTable.forPath(spark, table_path)
    except Exception as e:
        if "is not a Delta table" in str(e):
            print(f"The {table} table is not yet available. Creating from MySQL data.")
            df.write.format("delta").save(table_path)
            print("Creation done!")
        else:
            raise
    else:
        print("Delta table already exist. Trying to update/insert new records...")
        (
            delta_table.alias("old")
            .merge(df.alias("new"), merge_condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        print("Update/Insert done!")

    end_t = time.time()

    print(f"Time spent: {round((end_t - start_t) /60, 2)} minutes.\n-----------------------\n\n")
