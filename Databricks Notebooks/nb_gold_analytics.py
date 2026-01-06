# Databricks notebook source
# MAGIC %md
# MAGIC Student Risk Profile

# COMMAND ----------

from pyspark.sql.functions import col

students = spark.table("student_dropout.silver_students") \
    .select(
        "student_id",
        "gender",
        "region",
        "school_id",
        "grade_level",
        "program_type"
    )

academics = spark.table("student_dropout.silver_academic_performance") \
    .select(
        "student_id",
        "performance_score",
        "academic_risk_flag"
    )

attendance = spark.table("student_dropout.silver_attendance") \
    .select(
        "student_id",
        "attendance_pct",
        "attendance_risk_flag"
    )

socio = spark.table("student_dropout.silver_socio_economic") \
    .select(
        "student_id",
        "socio_economic_risk_flag"
    )

retention = spark.table("student_dropout.silver_retention_status") \
    .select(
        "student_id",
        "retention_status",
        "retention_risk_flag"
    )


# COMMAND ----------

from pyspark.sql.functions import when, col, greatest

student_risk_gold = (
    students
    .join(academics, "student_id", "left")
    .join(attendance, "student_id", "left")
    .join(socio, "student_id", "left")
    .join(retention, "student_id", "left")
    
    # Risk score logic (explainable)
    .withColumn(
        "risk_score",
        when(col("academic_risk_flag"), 30).otherwise(0) +
        when(col("attendance_risk_flag"), 40).otherwise(0) +
        when(col("socio_economic_risk_flag"), 30).otherwise(0)
    )
    
    # Risk level
    .withColumn(
        "risk_level",
        when(col("risk_score") >= 70, "HIGH")
        .when(col("risk_score") >= 40, "MEDIUM")
        .otherwise("LOW")
    )
    
    # Primary risk driver
    .withColumn(
        "primary_risk_driver",
        greatest(
            when(col("attendance_risk_flag"), "ATTENDANCE"),
            when(col("academic_risk_flag"), "ACADEMIC"),
            when(col("socio_economic_risk_flag"), "SOCIO_ECONOMIC")
        )
    )
    
    # Intervention flag
    .withColumn(
        "intervention_required",
        when(col("risk_level") == "HIGH", True).otherwise(False)
    )
)

student_risk_gold.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("student_dropout.gold_student_risk_profile")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM student_dropout.gold_student_risk_profile
# MAGIC LIMIT 10;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Attendance vs Performance Analytics

# COMMAND ----------

from pyspark.sql.functions import when, col

attendance_performance_gold = (
    students
    .join(attendance, "student_id")
    .join(academics, "student_id")
    
    # Attendance banding
    .withColumn(
        "attendance_band",
        when(col("attendance_pct") < 60, "VERY LOW")
        .when(col("attendance_pct") < 75, "LOW")
        .when(col("attendance_pct") < 90, "MEDIUM")
        .otherwise("HIGH")
    )
    
    # Performance banding
    .withColumn(
        "performance_band",
        when(col("performance_score") < 50, "LOW")
        .when(col("performance_score") < 70, "MEDIUM")
        .otherwise("HIGH")
    )
    
    # Combined risk indicator
    .withColumn(
        "combined_risk_flag",
        when(
            (col("attendance_band").isin("VERY LOW", "LOW")) &
            (col("performance_band") == "LOW"),
            True
        ).otherwise(False)
    )
)

attendance_performance_gold.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("student_dropout.gold_attendance_vs_performance")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM student_dropout.gold_attendance_vs_performance
# MAGIC LIMIT 10;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC gold_dropout_trends
# MAGIC

# COMMAND ----------

#SEPERATE CELL
from pyspark.sql.functions import count, when, col, round as spark_round

dropout_trends_gold = (
    students
    .join(retention, "student_id")
    .groupBy("region", "grade_level")
    .agg(
        count("*").alias("total_students"),
        count(when(col("retention_status") == "DROPPED", True)).alias("dropped_students")
    )
    .withColumn(
        "dropout_rate",
        spark_round(col("dropped_students") / col("total_students"), 4)
    )
)

dropout_trends_gold.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("student_dropout.gold_dropout_trends")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM student_dropout.gold_dropout_trends
# MAGIC LIMIT 10;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC gold_intervention_candidates

# COMMAND ----------

risk_profile = spark.table("student_dropout.gold_student_risk_profile") \
    .select(
        "student_id",
        "grade_level",
        "region",
        "risk_score",
        "risk_level",
        "primary_risk_driver",
        "intervention_required"
    )

retention = spark.table("student_dropout.silver_retention_status") \
    .select(
        "student_id",
        "retention_status"
    )


# COMMAND ----------

intervention_candidates_gold = (
    risk_profile
    .join(retention, "student_id")
    .filter(col("intervention_required") == True)
)

intervention_candidates_gold.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("student_dropout.gold_intervention_candidates")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM student_dropout.gold_intervention_candidates
# MAGIC LIMIT 10;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC gold_retention_overview
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import count, when, col, round as spark_round

retention_overview_gold = (
    retention
    .agg(
        count("*").alias("total_students"),
        count(when(col("retention_status") == "RETAINED", True)).alias("retained_students"),
        count(when(col("retention_status") == "DROPPED", True)).alias("dropped_students")
    )
    .withColumn(
        "retention_rate",
        spark_round(col("retained_students") / col("total_students"), 4)
    )
    .withColumn(
        "dropout_rate",
        spark_round(col("dropped_students") / col("total_students"), 4)
    )
)

retention_overview_gold.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("student_dropout.gold_retention_overview")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM student_dropout.gold_retention_overview;
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
layer_name = "gold"

gold_tables = [
    "student_dropout.gold_student_risk_profile",
    "student_dropout.gold_attendance_vs_performance",
    "student_dropout.gold_retention_overview",
    "student_dropout.gold_intervention_candidates"
]

for table in gold_tables:
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
