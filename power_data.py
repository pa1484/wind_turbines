from pyspark.sql.types import StructType, StructField, DecimalType, StringType
import datetime
from pyspark.sql.functions import lit
from pyspark.sql.functions import to_date
from pyspark.sql.functions import sum,avg,max,mean,stddev,min,when

def getFolderName():
    today = datetime.datetime.today()

    year = today.strftime("%Y")
    month = today.strftime("%m")
    day = today.strftime("%d")

    retrun "{}{}{}".format(year, month, day)

def readRawfiles():
    foldername = getFolderName()
    save_location = f"/FileStore/tmp/clean/power_output_raw"

    for x in range(1, 4):
    filepath = "/FileStore/tmp/test/{}/data_group_{}.csv".format(foldername,x)
    df = spark.read.csv(path = filepath,
                        header = True,
                        inferSchema=True)
    #adding new columns such filepath and filegroup to keep track and use for later 
    new_df = df.withColumn("filepath", lit(filepath)).withColumn("filegroup",lit(x))

    #overwriting the table as the new csv file has all previous records as well as latest records.
    if x==1:
        new_df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").save(save_location)
    else:
        new_df.write.mode("append").format("delta").option("overwriteSchema", "true").save(save_location)
    #creating table definiation
    spark.sql('DROP TABLE IF EXISTS clean.power_output')
    spark.sql(f'CREATE TABLE IF NOT EXISTS clean.power_output USING DELTA LOCATION "{save_location}"')

def cleanData():
    save_location = "/FileStore/tmp/clean/power_output_clean"
    df = sqlContext.sql("SELECT * from  clean.power_output")
    # Handle missing values - drop rows with missing power_output and turbine_id
    df = df.dropna(subset=['power_output', 'turbine_id'])
    # Assuming ouliers for power output lower bound is 1.0 and upper bound is 5.0
    lower_bound = 1.0
    upper_bound = 10.0
    df = df[(df['power_output'] >= lower_bound) & (df['power_output'] <= upper_bound)]

    #making sure turbineid that are within filegroup are stored.
    firstgroup_df = df.filter((df['filegroup'] == 1) & (df['turbine_id'] >= 1) & (df['turbine_id'] <= 5))
    secondgroup_df = df.filter((df['filegroup'] == 2) & (df['turbine_id'] >= 6) & (df['turbine_id'] <= 10))
    thirdgroup_df = df.filter((df['filegroup'] == 3) & (df['turbine_id'] >= 11) & (df['turbine_id'] <= 15))

    final_df  = firstgroup_df.union(secondgroup_df).union(thirdgroup_df)

    final_df = final_df.select('timestamp','wind_speed','wind_direction','power_output','turbine_id')
    #instead of writing all rows from raw table to a new table, can use water mark to keep track of the latest rows.
    #overwriting the total rows in the table
    final_df.write.mode("overwrite").format("delta").save(save_location)

    #storing out in a delta table
    spark.sql('DROP TABLE IF EXISTS align.power_output')
    spark.sql(f'CREATE TABLE IF NOT EXISTS align.power_output USING DELTA LOCATION "{save_location}"')
    save_location = "/FileStore/tmp/clean/power_output_clean"
    df = sqlContext.sql("SELECT * from  clean.power_output")
    # Handle missing values - drop rows with missing power_output and turbine_id
    df = df.dropna(subset=['power_output', 'turbine_id'])
    # Assuming ouliers for power output lower bound is 1.0 and upper bound is 5.0
    lower_bound = 1.0
    upper_bound = 10.0
    df = df[(df['power_output'] >= lower_bound) & (df['power_output'] <= upper_bound)]

    #making sure turbineid that are within filegroup are stored.
    firstgroup_df = df.filter((df['filegroup'] == 1) & (df['turbine_id'] >= 1) & (df['turbine_id'] <= 5))
    secondgroup_df = df.filter((df['filegroup'] == 2) & (df['turbine_id'] >= 6) & (df['turbine_id'] <= 10))
    thirdgroup_df = df.filter((df['filegroup'] == 3) & (df['turbine_id'] >= 11) & (df['turbine_id'] <= 15))

    final_df  = firstgroup_df.union(secondgroup_df).union(thirdgroup_df)

    final_df = final_df.select('timestamp','wind_speed','wind_direction','power_output','turbine_id')
    #instead of writing all rows from raw table to a new table, can use water mark to keep track of the latest rows.
    #overwriting the total rows in the table
    final_df.write.mode("overwrite").format("delta").save(save_location)

    #storing out in a delta table
    spark.sql('DROP TABLE IF EXISTS align.power_output')
    spark.sql(f'CREATE TABLE IF NOT EXISTS align.power_output USING DELTA LOCATION "{save_location}"')

def calcStats():
    save_location = "/FileStore/tmp/clean/power_output_new"
    df = sqlContext.sql("SELECT * from  align.power_output")
    df = df.withColumn("date", to_date(df["timestamp"]))

    # df.show()
    groupby_df = df.groupBy("turbine_id","date") \
                    .agg(min("power_output").alias("min_power_output"), \
                        max("power_output").alias("max_power_output"), \
                        avg("power_output").alias("avg_power_output"), \
                        mean("power_output").alias("mean_power_output"), \
                        stddev("power_output").alias("stddev_power_output"), \
                        )
    #identify the anomolies
    df2 = groupby_df.withColumn("anomoly", when(groupby_df["mean_power_output"] - groupby_df["stddev_power_output"] >=2,
                                                "Yes")
                                        .when(groupby_df["mean_power_output"] - groupby_df["stddev_power_output"] <= -2,
                                                "Yes")
                                        .otherwise("No"))

    #storing out in a delta table
    df2.write.mode("overwrite").format("delta").save(save_location)

    spark.sql('DROP TABLE IF EXISTS conform.power_output')
    spark.sql(f'CREATE TABLE IF NOT EXISTS conform.power_output USING DELTA LOCATION "{save_location}"')

if __name__ == '__main__':
    readRawfiles()
    cleanData()
    calcStats()
