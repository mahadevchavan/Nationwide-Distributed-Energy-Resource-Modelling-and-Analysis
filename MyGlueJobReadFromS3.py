import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import logging
from pyspark.sql.functions import col, sum
from pyspark.sql.functions import concat, col, lit, to_date
from awsglue.dynamicframe import DynamicFrame

logger = logging.getLogger('my_logger')
logger.setLevel(logging.INFO)

# Create a handler for CloudWatch
handler = logging.StreamHandler()
handler.setLevel(logging.INFO)
logger.addHandler(handler)

logger.info('My log message')

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Amazon S3
AmazonS3_node1709057065086 = glueContext.create_dynamic_frame.from_catalog(database="mydb", table_name="solar", transformation_ctx="AmazonS3_node1709057065086")

# Script generated for node Change Schema
ChangeSchema_node1709057157917 = ApplyMapping.apply(frame=AmazonS3_node1709057065086, mappings=[("id_1", "long", "id_1", "long"), ("id_2", "string", "id_2", "string"), ("customer_class", "string", "customer_class", "string"), ("zipcode", "double", "zipcode", "double"), ("city", "string", "city", "string"), ("state", "string", "state", "string"), ("state_fips", "long", "state_fips", "long"), ("state_name", "string", "state_name", "string"), ("utility_service_territory", "string", "utility_service_territory", "string"), ("third_party_owned_flag", "boolean", "third_party_owned_flag", "boolean"), ("year", "double", "year", "int"), ("month", "double", "month", "int"), ("day", "double", "day", "int"), ("capacity_dc_kw", "double", "capacity_dc_kw", "double"), ("ground_mount_flag", "boolean", "ground_mount_flag", "boolean"), ("tracking_type", "double", "tracking_type", "double"), ("azimuth_1", "string", "azimuth_1", "string"), ("azimuth_2", "string", "azimuth_2", "string"), ("azimuth_3", "string", "azimuth_3", "string"), ("tilt_1", "string", "tilt_1", "string"), ("tilt_2", "string", "tilt_2", "string"), ("tilt_3", "string", "tilt_3", "string"), ("capacity_mod_1", "double", "capacity_mod_1", "double"), ("capacity_mod_2", "string", "capacity_mod_2", "string"), ("capacity_mod_3", "string", "capacity_mod_3", "string"), ("efficiency_1", "double", "efficiency_1", "double"), ("efficiency_2", "string", "efficiency_2", "string"), ("efficiency_3", "string", "efficiency_3", "string"), ("tech_class_primary", "string", "tech_class_primary", "string"), ("tech_class_primary_2", "string", "tech_class_primary_2", "string"), ("tech_class_primary_3", "string", "tech_class_primary_3", "string"), ("storage_flag", "boolean", "storage_flag", "boolean"), ("storage_info", "long", "storage_info", "long"), ("storage_year", "string", "storage_year", "string"), ("storage_capacity", "string", "storage_capacity", "string"), ("storage_energy", "string", "storage_energy", "string"), ("inverter_loading_ratio", "string", "inverter_loading_ratio", "string"), ("inverter_capacity_1", "double", "inverter_capacity_1", "double"), ("inverter_capacity_2", "string", "inverter_capacity_2", "string"), ("inverter_capacity_3", "string", "inverter_capacity_3", "string"), ("inverter_micro_flag_1", "boolean", "inverter_micro_flag_1", "boolean"), ("inverter_micro_flag_2", "boolean", "inverter_micro_flag_2", "boolean"), ("inverter_micro_flag_3", "boolean", "inverter_micro_flag_3", "boolean"), ("inverter_hybrid_flag_1", "boolean", "inverter_hybrid_flag_1", "boolean"), ("inverter_hybrid_flag_2", "boolean", "inverter_hybrid_flag_2", "boolean"), ("inverter_hybrid_flag_3", "boolean", "inverter_hybrid_flag_3", "boolean"), ("inverter_meter_flag_1", "boolean", "inverter_meter_flag_1", "boolean"), ("inverter_meter_flag_2", "boolean", "inverter_meter_flag_2", "boolean"), ("inverter_meter_flag_3", "boolean", "inverter_meter_flag_3", "boolean"), ("zip_original", "double", "zip_original", "double"), ("zip", "long", "zip", "long"), ("latitude", "double", "latitude", "double"), ("longitude", "double", "longitude", "double"), ("zip_city", "string", "zip_city", "string"), ("sub_id", "double", "sub_id", "double"), ("interconnect", "string", "interconnect", "string")], transformation_ctx="ChangeSchema_node1709057157917")

#convert dynamic dataframe into spark dataframe
logger.info('convert dynamic dataframe ResolveChoice_node into spark dataframe')

#converted from glue dataframe to spark dataframe
logger.info('converted from glue dataframe to spark dataframe')
spark_data_frame=ChangeSchema_node1709057157917.toDF()

logger.info('original DF printing')
spark_data_frame.show()

logger.info('no of columns in DF')
print(len(spark_data_frame.columns))

# logger.info('null count of each column')
null_counts = spark_data_frame.select([sum(col(c).isNull().cast("int")).alias(c) for c in spark_data_frame.columns])

# Display the result
logger.info('null count of each column')
null_counts.show()

# Assuming 'your_df' is your DataFrame
# Replace 'your_df' with your actual DataFrame name

your_df = null_counts

# Show the original DataFrame
your_df.show()

# Threshold for values
value_threshold = 1200000

# Select columns with values less than the threshold
selected_columns = [column for column in your_df.columns if your_df.select(col(column)).first()[0] < value_threshold]

# Select columns from the original DataFrame
selected_df = your_df.select(*selected_columns)

# Show the selected DataFrame
# logger.info('showing ')
# selected_df.show()

new_df = selected_df.select(*selected_columns)
logger.info('printing schema after selecting columns with null values less than threshold which is 1200000')
new_df.show()

logger.info('no of columns remained')
print(len(new_df.columns))

wo_null = new_df.dropna()

logger.info('after dropping rows with any no of null values')
wo_null.show()

logger.info('row count after removing null removing')
wo_null.count()



# Concatenate "day", "month", and "year" columns into a new column called "date_combined"
combined = wo_null.withColumn("date_combined", concat(col("day"), lit("/"), col("month"), lit("/"), col("year")))

combined = combined.withColumn("date_combined", to_date("date_combined", "dd/MM/yyyy"))

# Show the DataFrame after concatenation
logger.info('Updated DataFrame schema:')
combined.show()

# Print the updated schema
logger.info('Updated DataFrame schema:')
combined.printSchema()

combined = combined.drop('day', 'month', 'year')

logger.info('Updated DataFrame schema after combining 3 columns:')
combined.show()

logger.info('Updated DataFrame schema after combine date columns:')
combined.printSchema()

# Convert the data frame back to a dynamic frame
logger.info('convert spark dataframe to dynamic frame ')
dynamic_frame = DynamicFrame.fromDF(combined, glueContext, "dynamic_frame")


logger.info('dynamic frame uploaded in bucket myglue-etl-project/output/newproduct/ in parquet format ')


# Script generated for node Amazon S3
AmazonS3_node1709057164600 = glueContext.write_dynamic_frame.from_options(frame=dynamic_frame, connection_type="s3", format="glueparquet", connection_options={"path": "s3://my-glue-etl-project/output/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1709057164600")

logger.info('etl job processed successfully')

job.commit()