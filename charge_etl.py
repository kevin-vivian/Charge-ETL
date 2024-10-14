from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round, max, avg

class ChargePointsETLJob:
    input_path = 'data/input/electric-chargepoints-2017.csv'
    output_path = 'data/output/chargepoints-2017-analysis'

    def __init__(self):
        self.spark_session = (SparkSession.builder
                                        .master("local[*]")
                                        .appName("ElectricChargePointsETLJob")
                                        .getOrCreate())

    def extract(self):
        # Read the input CSV file and return a DataFrame
        return self.spark_session.read.csv(self.input_path, header=True, inferSchema=True)

    def transform(self, df):
        # Ensure PluginDuration is of type DECIMAL (or FLOAT)
        df = df.withColumn("PluginDuration", col("PluginDuration").cast("float"))

        # Filter out any rows where PluginDuration is null (or not a number)
        df = df.na.drop(subset=["PluginDuration"])

        # Group by charge point ID and calculate max and average plugin durations
        transformed_df = (df.groupBy("CPID")
                          .agg(
                              round(max("PluginDuration"), 2).alias("max_duration"),
                              round(avg("PluginDuration"), 2).alias("avg_duration")
                          ))
        
        # Rename the column to match the required output
        transformed_df = transformed_df.withColumnRenamed("CPID", "chargepoint_id")
        return transformed_df

    def load(self, df):
        # Write the transformed DataFrame to the specified output path in parquet format
        df.write.mode('overwrite').parquet(self.output_path)

    def run(self):
        self.load(self.transform(self.extract()))

# Example usage
if __name__ == "__main__":
    etl_job = ChargePointsETLJob()
    etl_job.run()