# Databricks notebook source
dbutils.fs.ls("/Volumes/workspace/student_dropout/raw")


# COMMAND ----------

raw_path = "/Volumes/workspace/student_dropout/raw/"
bronze_path = "/Volumes/workspace/student_dropout/bronze/"


# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

students_raw = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(raw_path + "students_master.csv")
    .withColumn("created_timestamp", current_timestamp())
    .withColumn("source_system", lit("synthetic_csv")) #no need
)


# COMMAND ----------

students_raw.printSchema()
students_raw.show(5)


# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS student_dropout")


# COMMAND ----------

students_raw.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("student_dropout.bronze_students")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM student_dropout.bronze_students;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC LOAD ACADEMIC PERFORMANCE

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

academic_raw = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(raw_path + "academic_performance.csv")
    .withColumn("created_timestamp", current_timestamp())
    .withColumn("source_system", lit("synthetic_csv"))
)


# COMMAND ----------

academic_raw.printSchema()
academic_raw.show(5)


# COMMAND ----------

academic_raw.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("student_dropout.bronze_academic_performance")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM student_dropout.bronze_academic_performance;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC LOAD ATTENDANCE DATA

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

attendance_raw = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(raw_path + "attendance_records.csv")
    .withColumn("created_timestamp", current_timestamp())
    .withColumn("source_system", lit("synthetic_csv"))
)


# COMMAND ----------

attendance_raw.printSchema()
attendance_raw.show(5)


# COMMAND ----------

attendance_raw.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("student_dropout.bronze_attendance")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM student_dropout.bronze_attendance;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC LOAD SOCIO-ECONOMIC DATA

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

socio_raw = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(raw_path + "socio_economic_data.csv")
    .withColumn("created_timestamp", current_timestamp())
    .withColumn("source_system", lit("synthetic_csv"))
)


# COMMAND ----------

socio_raw.printSchema()
socio_raw.show(5)


# COMMAND ----------

socio_raw.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("student_dropout.bronze_socio_economic")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM student_dropout.bronze_socio_economic;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC LOAD RETENTION STATUS

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

retention_raw = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(raw_path + "retention_status.csv")
    .withColumn("created_timestamp", current_timestamp())
    .withColumn("source_system", lit("synthetic_csv"))
)


# COMMAND ----------

retention_raw.printSchema()
retention_raw.show(5)


# COMMAND ----------

retention_raw.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("student_dropout.bronze_retention_status")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM student_dropout.bronze_retention_status;
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, LongType

def write_audit_log(
    pipeline_name,
    layer_name,
    table_name,
    record_count,
    run_status,
    error_message=None
):
    audit_schema = StructType([
        StructField("pipeline_name", StringType(), False),
        StructField("layer_name", StringType(), False),
        StructField("table_name", StringType(), False),
        StructField("record_count", LongType(), True),
        StructField("run_status", StringType(), False),
        StructField("error_message", StringType(), True)
    ])

    audit_data = [
        (pipeline_name, layer_name, table_name, record_count, run_status, error_message)
    ]

    audit_df = spark.createDataFrame(audit_data, schema=audit_schema) \
        .withColumn("run_timestamp", current_timestamp())

    audit_df.write.mode("append").saveAsTable("student_dropout.pipeline_audit_log")


# COMMAND ----------

pipeline_name = "student_dropout_retention_pipeline"
layer_name = "bronze"

bronze_tables = [
    "student_dropout.bronze_students",
    "student_dropout.bronze_academic_performance",
    "student_dropout.bronze_attendance",
    "student_dropout.bronze_socio_economic",
    "student_dropout.bronze_retention_status"
]

for table in bronze_tables:
    try:
        record_count = spark.table(table).count()
        write_audit_log(
            pipeline_name,
            layer_name,
            table.split(".")[-1],
            record_count,
            "SUCCESS"
        )
    except Exception as e:
        write_audit_log(
            pipeline_name,
            layer_name,
            table.split(".")[-1],
            None,
            "FAILED",
            str(e)
        )


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM student_dropout.pipeline_audit_log
# MAGIC ORDER BY run_timestamp DESC;
# MAGIC
# MAGIC