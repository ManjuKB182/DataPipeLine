from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder \
    .appName("Test Spark Installation") \
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
