from pyspark.sql import SparkSession # type: ignore

# Create a Spark session
spark = SparkSession.builder \
    .appName("Test Spark Installation") \
    .config('spark.sql.warehouse.dir', 'file:///C:/') \
    .getOrCreate()

#print("Spark session created successfully!")

file_path="/Users/manjukb/Desktop/Work_Related/ETL_python_Automation/DataPipeLine/NewEarthData.csv"

df = spark.read.csv(file_path, header=True, inferSchema=True)
df.show(4)

df.createOrReplaceTempView("BaseTable")

True_value_table = spark.sql('''
                             Select *
                             From BaseTable
                             Where Cryosleep = "true"   
                                ''')
file_Out_path = "/Users/manjukb/Desktop/Work_Related/ETL_python_Automation/DataPipeLine/truevalues.csv"
True_value_table.coalesce(2).write.csv(file_Out_path, header= True, mode= "overwrite")

spark.stop()

# Json File  handling
from pyspark.sql import SparkSession  # type: ignore

# Create a Spark session
spark = SparkSession.builder \
    .appName("Test Spark Installation") \
    .config('spark.sql.warehouse.dir', 'file:///C:/') \
    .getOrCreate()

# File path for input JSON
file_path = "/Users/manjukb/Desktop/Work_Related/ETL_python_Automation/DataPipeLine/NewEarthData.json"

# Read JSON file with schema inference
df = spark.read.option("multiline", "true").json(file_path)

# Show a sample of data
df.show(4)

# Create a temporary view
df.createOrReplaceTempView("BaseTable")

# Filter rows where Cryosleep is true
True_value_table = spark.sql('''
    SELECT * 
    FROM BaseTable 
    WHERE Cryosleep = "true"
''')

# File path for output JSON
file_Out_path = "/Users/manjukb/Desktop/Work_Related/ETL_python_Automation/DataPipeLine/truevalues.json"

# Write the filtered data to JSON with overwrite mode
True_value_table.coalesce(2).write.mode("overwrite").json(file_Out_path)

# Stop Spark session
spark.stop()



from pyspark.sql import SparkSession

spark = SparkSession.builder \
        .appName('S3') \
        .config('spark.hadoop.fs.aws.secrete.key:', 'org.apache.hadoop:hadoop-aws:2.7.4') \
        .getOrCreate()


# Read the data from S3
s3_df = spark.read.csv('s3a://bucket_name/file.csv', header=True, inferSchema=True)

s3_df.createOrReplaceTempView('s3_table')


RS_jdbc_url = "jdbc:sqlserver://<server>:<port>;databaseName=<database>"
RS_table = "dbo.table_name"
RS_properties = {
    "user": "<username",
    "password": "<password>",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# Read the data from SQL Server
RS_df = spark.read.jdbc(url=RS_jdbc_url, table=RS_table, properties=RS_properties)

RS_df.createOrReplaceTempView('RS_table')

#Check the data count

s3_count = spark.sql('''Select count(*) 
                     from s3_table''').collect()[0][0]

RS_count = spark.sql('''Select count(*) 
                     from RS_table''').collect()[0][0]

if s3_count == RS_count:
    print("Count is matching")
else:
    print("Count is not matching")

RS_df.coalesce(1).write.csv('s3a://bucket_name/RS_table.csv', header=True, mode='overwrite')

spark.stop()

