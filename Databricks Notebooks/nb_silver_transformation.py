# Databricks notebook source
# MAGIC %md
# MAGIC SILVER LAYER – STUDENTS
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import current_timestamp


# COMMAND ----------

from pyspark.sql.functions import col, upper, when, trim

students_bronze = spark.table("student_dropout.bronze_students")

# Basic profiling (enterprise habit)
students_bronze.select(
    col("student_id"),
    col("gender"),
    col("region"),
    col("grade_level"),
    col("school_id"),
    col("program_type")
).show(5)

students_bronze.printSchema()


# COMMAND ----------

students_silver = (
    students_bronze
    
    # 1. Trim all string columns (very common enterprise issue)
    .withColumn("student_id", trim(col("student_id")))
    .withColumn("gender", trim(col("gender")))
    .withColumn("region", trim(col("region")))
    .withColumn("school_id", trim(col("school_id")))
    .withColumn("program_type", trim(col("program_type")))
    # updated timestamp
    .withColumn("updated_timestamp", current_timestamp())
    
    # 2. Standardize categorical casing
    .withColumn("region", upper(col("region")))
    .withColumn("program_type", upper(col("program_type")))
    
    # 3. Handle missing / invalid gender
    .withColumn(
        "gender",
        when(col("gender").isNull(), "UNKNOWN")
        .when(~col("gender").isin("Male", "Female"), "UNKNOWN")
        .otherwise(col("gender"))
    )
    
    # 4. Validate grade levels (business rule)
    .filter(col("grade_level").between(1, 12))
    
    # 5. Ensure enrollment_year sanity (future-proof)
    .filter(col("enrollment_year") >= 2015)
)

students_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("student_dropout.silver_students")


# COMMAND ----------

students_silver.printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*) AS total_records,
# MAGIC   COUNT(DISTINCT student_id) AS unique_students
# MAGIC FROM student_dropout.silver_students;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC SILVER LAYER — ACADEMIC PERFORMANCE

# COMMAND ----------

from pyspark.sql.functions import col

academic_bronze = spark.table("student_dropout.bronze_academic_performance")

academic_bronze.printSchema()
academic_bronze.show(5)


# COMMAND ----------

from pyspark.sql.functions import when, avg, round as spark_round

# 1. Impute avg_score using global mean (safe, simple, explainable)
mean_score = academic_bronze.select(avg("avg_score")).first()[0]

academic_silver = (
    academic_bronze
    
    # Trim & basic hygiene
    .withColumn("academic_year", col("academic_year"))

    # updated timestamp
    .withColumn("updated_timestamp", current_timestamp())
    
    #if avg score null only then this should happen
    # Handle missing or invalid avg_score
    .withColumn(
        "avg_score",
        when(col("avg_score").isNull(), mean_score)
        .when(col("avg_score") < 0, mean_score)
        .when(col("avg_score") > 100, 100)
        .otherwise(col("avg_score"))
    )
    
    # Cap failed subjects to a reasonable max (enterprise rule)
    .withColumn(
        "failed_subjects",
        when(col("failed_subjects") < 0, 0)
        .when(col("failed_subjects") > 6, 6)
        .otherwise(col("failed_subjects"))
    )
    
    # 2. Derived performance score (0–100)
    .withColumn(
        "performance_score",
        spark_round(
            col("avg_score") - (col("failed_subjects") * 5), 2
        )
    )
    
    # 3. Academic risk flag (early warning)
    .withColumn(
        "academic_risk_flag",
        when(
            (col("performance_score") < 50) | (col("failed_subjects") >= 3),
            True
        ).otherwise(False)
    )
)

academic_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("student_dropout.silver_academic_performance")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*) AS records,
# MAGIC   AVG(performance_score) AS avg_perf_score,
# MAGIC   SUM(CASE WHEN academic_risk_flag THEN 1 ELSE 0 END) AS at_risk_students
# MAGIC FROM student_dropout.silver_academic_performance;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC SILVER LAYER — ATTENDANCE

# COMMAND ----------

from pyspark.sql.functions import col

attendance_bronze = spark.table("student_dropout.bronze_attendance")

attendance_bronze.printSchema()
attendance_bronze.show(5)


# COMMAND ----------

from pyspark.sql.functions import when, round as spark_round

attendance_silver = (
    attendance_bronze
    
    # 1. Enforce logical bounds
    .withColumn(
        "days_present",
        when(col("days_present").isNull(), 0)
        .when(col("days_present") < 0, 0)
        .when(col("days_present") > col("total_days"), col("total_days"))
        .otherwise(col("days_present"))
    )

    # updated timestamp
    .withColumn("updated_timestamp", current_timestamp())
    
    # 2. Ensure total_days sanity (future-proof)
    .withColumn(
        "total_days",
        when(col("total_days").isNull(), 180)
        .when(col("total_days") <= 0, 180)
        .otherwise(col("total_days"))
    )
    
    # 3. Recalculate attendance percentage (authoritative)
    .withColumn(
        "attendance_pct",
        spark_round((col("days_present") / col("total_days")) * 100, 2)
    )
    
    # 4. Attendance risk flag (early warning)
    .withColumn(
        "attendance_risk_flag",
        when(col("attendance_pct") < 75, True).otherwise(False)
    )
)

attendance_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("student_dropout.silver_attendance")


# COMMAND ----------

# MAGIC %md
# MAGIC SILVER LAYER — SOCIO-ECONOMIC DATA

# COMMAND ----------

from pyspark.sql.functions import col

socio_bronze = spark.table("student_dropout.bronze_socio_economic")

socio_bronze.printSchema()
socio_bronze.show(5)


# COMMAND ----------

from pyspark.sql.functions import when, upper, trim

socio_silver = (
    socio_bronze
    
    # 1. Trim string columns
    .withColumn("family_income_band", trim(col("family_income_band")))
    .withColumn("parental_education", trim(col("parental_education")))

    # updated timestamp
    .withColumn("updated_timestamp", current_timestamp())
    
    # 2. Standardize income bands
    .withColumn(
        "family_income_band",
        when(col("family_income_band").isNull(), "UNKNOWN")
        .otherwise(upper(col("family_income_band")))
    )
    
    # 3. Normalize access_to_internet to boolean
    .withColumn(
        "access_to_internet",
        when(col("access_to_internet").isin(True, "True", "Yes", "YES", "Y"), True)
        .otherwise(False)
    )
    
    # 4. Normalize working_part_time
    .withColumn(
        "working_part_time",
        when(col("working_part_time").isin(True, "True", "Yes", "YES", "Y"), True)
        .otherwise(False)
    )
    
    # 5. Normalize scholarship_status
    .withColumn(
        "scholarship_status",
        when(col("scholarship_status").isin(True, "True", "Yes", "YES", "Y"), True)
        .otherwise(False)
    )
    
    # 6. Socio-economic risk flag
    .withColumn(
        "socio_economic_risk_flag",
        when(
            (col("family_income_band") == "LOW") |
            (col("working_part_time") == True),
            True
        ).otherwise(False)
    )
)

socio_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("student_dropout.silver_socio_economic")


# COMMAND ----------

# MAGIC %md
# MAGIC SILVER LAYER — RETENTION STATUS

# COMMAND ----------

from pyspark.sql.functions import col

retention_bronze = spark.table("student_dropout.bronze_retention_status")

retention_bronze.printSchema()
retention_bronze.show(5)


# COMMAND ----------

from pyspark.sql.functions import when, upper

retention_silver = (
    retention_bronze
    
    # 1. Handle NULL dropout_flag (late reporting)
    .withColumn(
        "dropout_flag",
        when(col("dropout_flag").isNull(), False)
        .otherwise(col("dropout_flag"))
    )

    # updated timestamp
    .withColumn("updated_timestamp", current_timestamp())
    
    # 2. Standardize retention_status casing
    .withColumn("retention_status", upper(col("retention_status")))
    
    # 3. Reconcile contradictions
    .withColumn(
        "retention_status",
        when(col("dropout_flag") == True, "DROPPED")
        .otherwise("RETAINED")
    )
    
    # 4. Handle missing dropout_reason
    .withColumn(
        "dropout_reason",
        when(col("retention_status") == "DROPPED",
             when(col("dropout_reason").isNull(), "NOT REPORTED")
             .otherwise(col("dropout_reason")))
        .otherwise("N/A")
    )
    
    # 5. Retention risk flag (used later in Gold)
    .withColumn(
        "retention_risk_flag",
        when(col("retention_status") == "DROPPED", True)
        .otherwise(False)
    )
)

retention_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("student_dropout.silver_retention_status")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   retention_status,
# MAGIC   COUNT(*) AS count
# MAGIC FROM student_dropout.silver_retention_status
# MAGIC GROUP BY retention_status;
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
layer_name = "silver"

silver_tables = [
    "student_dropout.silver_students",
    "student_dropout.silver_academic_performance",
    "student_dropout.silver_attendance",
    "student_dropout.silver_socio_economic",
    "student_dropout.silver_retention_status"
]

for table in silver_tables:
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
