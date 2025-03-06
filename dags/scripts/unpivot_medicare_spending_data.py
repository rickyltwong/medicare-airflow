from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, lit, row_number
from pyspark.sql.window import Window
import json
from typing import List, Set
import logging
import sys

def unpivot_medicare_spending_data():
    """
    Process Medicare spending data with idempotent operations
    Returns a list of newly processed files
    """
    try:
        # Configure Spark with required packages and configs
        spark = SparkSession.builder \
            .appName("Medicare Data Processing") \
            .getOrCreate()

        bucket = "pharmaceutical-data-dashboard"
        
        # Get list of all input files
        input_files = spark.read.format("json") \
            .option("basePath", f"s3a://{bucket}/medicare/spending_data/") \
            .load(f"s3a://{bucket}/medicare/spending_data/*.json") \
            .inputFiles()

        # Get already processed files from command line arguments
        processed_files = set()
        if len(sys.argv) > 1:
            try:
                processed_files = set(json.loads(sys.argv[1]))
                logging.info(f"Received {len(processed_files)} processed files from arguments")
            except Exception as e:
                logging.warning(f"Failed to parse processed files from arguments: {e}")
        
        # Filter for new files
        new_files = [f for f in input_files if f not in processed_files]
        
        if not new_files:
            logging.info("No new files to process")
            # Return empty list as no new files were processed
            print(json.dumps([]))
            return
            
        # Read only new files
        df = spark.read.json(new_files)

        # Process the data
        years = ["2018", "2019", "2020", "2021", "2022"]
        base_cols = ["Brnd_Name", "Gnrc_Name", "Tot_Mftr", "Mftr_Name", 
                    "CAGR_Avg_Spnd_Per_Dsg_Unt_18_22", "Chg_Avg_Spnd_Per_Dsg_Unt_21_22"]
        
        dfs = []
        for year in years:
            metric_cols = [
                "Tot_Spndng",
                "Tot_Dsg_Unts", 
                "Tot_Clms",
                "Tot_Benes",
                "Avg_Spnd_Per_Dsg_Unt_Wghtd",
                "Avg_Spnd_Per_Clm",
                "Avg_Spnd_Per_Bene",
                "Outlier_Flag"
            ]
            
            year_cols = [f"{col}_{year}" for col in metric_cols]
            
            year_df = df.select(
                *base_cols,
                *[col(c).alias(c.replace(f"_{year}", "")) for c in year_cols]
            ).withColumn("year", lit(year))
            
            dfs.append(year_df)

        # Import needed for reduce operation
        from functools import reduce
        from pyspark.sql import DataFrame
        
        final_df = reduce(DataFrame.unionAll, dfs)

        # Convert string columns to appropriate types
        final_df = final_df \
            .withColumn("Tot_Spndng", col("Tot_Spndng").cast("float")) \
            .withColumn("Tot_Dsg_Unts", col("Tot_Dsg_Unts").cast("integer")) \
            .withColumn("Tot_Clms", col("Tot_Clms").cast("integer")) \
            .withColumn("Tot_Benes", col("Tot_Benes").cast("integer")) \
            .withColumn("Avg_Spnd_Per_Dsg_Unt_Wghtd", col("Avg_Spnd_Per_Dsg_Unt_Wghtd").cast("float")) \
            .withColumn("Avg_Spnd_Per_Clm", col("Avg_Spnd_Per_Clm").cast("float")) \
            .withColumn("Avg_Spnd_Per_Bene", col("Avg_Spnd_Per_Bene").cast("float")) \
            .withColumn("Outlier_Flag", col("Outlier_Flag").cast("boolean")) \
            .withColumn("CAGR_Avg_Spnd_Per_Dsg_Unt_18_22", col("CAGR_Avg_Spnd_Per_Dsg_Unt_18_22").cast("float")) \
            .withColumn("Chg_Avg_Spnd_Per_Dsg_Unt_21_22", col("Chg_Avg_Spnd_Per_Dsg_Unt_21_22").cast("float"))

        # Create a dataframe with standardized column names
        output_df = final_df.select(
            col("Brnd_Name").alias("brand_name"),
            col("Gnrc_Name").alias("generic_name"),
            col("Tot_Mftr").alias("total_manufacturer"),
            col("Mftr_Name").alias("manufacturer_name"),
            col("CAGR_Avg_Spnd_Per_Dsg_Unt_18_22").alias("cagr_avg_spend_per_dosage_unit_18_22"),
            col("Chg_Avg_Spnd_Per_Dsg_Unt_21_22").alias("change_avg_spend_per_dosage_unit_21_22"),
            col("Tot_Spndng").alias("total_spending"),
            col("Tot_Dsg_Unts").alias("total_dosage_units"),
            col("Tot_Clms").alias("total_claims"),
            col("Tot_Benes").alias("total_beneficiaries"),
            col("Avg_Spnd_Per_Dsg_Unt_Wghtd").alias("avg_spend_per_dosage_unit"),
            col("Avg_Spnd_Per_Clm").alias("avg_spend_per_claim"),
            col("Avg_Spnd_Per_Bene").alias("avg_spend_per_beneficiary"),
            col("Outlier_Flag").alias("outlier_flag"),
            col("year")
        )
        
        # Check if there's existing data to handle deduplication
        try:
            existing_data_exists = spark._jsparkSession.catalog().tableExists(f"s3a://{bucket}/medicare/processed/")
            if existing_data_exists:
                # Read existing data
                existing_df = spark.read.parquet(f"s3a://{bucket}/medicare/processed/")
                
                # Combine with new data
                combined_df = existing_df.union(output_df)
                
                # Define a window for deduplication
                window_spec = Window.partitionBy(
                    "brand_name", "generic_name", "manufacturer_name", "year"
                ).orderBy(col("total_spending").desc())
                
                # Deduplicate by keeping only the first row in each partition
                deduplicated_df = combined_df.withColumn(
                    "row_num", row_number().over(window_spec)
                ).filter(col("row_num") == 1).drop("row_num")
                
                # Write deduplicated data
                deduplicated_df.write \
                    .mode("overwrite") \
                    .parquet(f"s3a://{bucket}/medicare/processed/")
            else:
                # No existing data, just write new data
                output_df.write \
                    .mode("overwrite") \
                    .parquet(f"s3a://{bucket}/medicare/processed/")
        except Exception as e:
            logging.warning(f"Error checking existing data, writing new data only: {e}")
            # Write new data only
            output_df.write \
                .mode("append") \
                .parquet(f"s3a://{bucket}/medicare/processed/")
        
        logging.info(f"Successfully processed {len(new_files)} new files")
        
        # Print the list of newly processed files as JSON to stdout
        # This will be captured by Airflow
        print(json.dumps(new_files))
        
        return
            
    except Exception as e:
        logging.error(f"Failed to process Medicare spending data: {e}")
        raise

if __name__ == "__main__":
    unpivot_medicare_spending_data() 