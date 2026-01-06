# Databricks notebook source
from pyspark.sql.functions import col

gold_df = spark.table("student_dropout.gold_student_risk_profile")


# COMMAND ----------

# MAGIC %md
# MAGIC Prepare ML Dataset
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import when

ml_df = gold_df.select(

    col("student_id"),
    col("attendance_pct").cast("double"),
    col("performance_score").cast("double"),
    col("risk_score").cast("double"),
    col("grade_level").cast("int"),

    when(col("academic_risk_flag") == True, 1).otherwise(0).alias("academic_risk_flag"),
    when(col("attendance_risk_flag") == True, 1).otherwise(0).alias("attendance_risk_flag"),
    when(col("socio_economic_risk_flag") == True, 1).otherwise(0).alias("socio_economic_risk_flag"),

    when(col("retention_risk_flag") == True, 1).otherwise(0).alias("label")
).dropna()


# COMMAND ----------

# MAGIC %md
# MAGIC Assemble Feature Vector

# COMMAND ----------

from pyspark.ml.feature import VectorAssembler

assembler = VectorAssembler(
    inputCols=[
        "attendance_pct",
        "performance_score",
        "risk_score",
        "grade_level",
        "academic_risk_flag",
        "attendance_risk_flag",
        "socio_economic_risk_flag"
    ],
    outputCol="features"
)

ml_ready_df = assembler.transform(ml_df)


# COMMAND ----------

# MAGIC %md
# MAGIC Train/Test Split

# COMMAND ----------

train_df, test_df = ml_ready_df.randomSplit([0.8, 0.2], seed=42)


# COMMAND ----------

from pyspark.ml.classification import LogisticRegression

lr = LogisticRegression(
    featuresCol="features",
    labelCol="label",
    probabilityCol="dropout_probability"
)

lr_model = lr.fit(train_df)


# COMMAND ----------

predictions = lr_model.transform(test_df)


# COMMAND ----------

# MAGIC %md
# MAGIC Extract Probability of Risk = 1

# COMMAND ----------

from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType

get_prob = udf(lambda v: float(v[1]), DoubleType())

predictions = predictions.withColumn(
    "dropout_probability",
    get_prob(col("dropout_probability"))
)


# COMMAND ----------

final_predictions = predictions.select(
    col("student_id"),
    col("dropout_probability")
)

final_predictions.write \
    .mode("overwrite") \
    .saveAsTable("student_dropout.gold_dropout_predictions")


# COMMAND ----------

from pyspark.ml.evaluation import BinaryClassificationEvaluator

evaluator = BinaryClassificationEvaluator(
    labelCol="label",
    rawPredictionCol="rawPrediction",
    metricName="areaUnderROC"
)

auc = evaluator.evaluate(predictions)
print(f"AUC Score: {auc}")


# COMMAND ----------

# MAGIC %md
# MAGIC QUICK WALKTHROUGH

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   student_id,
# MAGIC   grade_level,
# MAGIC   attendance_pct,
# MAGIC   performance_score,
# MAGIC   risk_score,
# MAGIC   risk_level,
# MAGIC   primary_risk_driver,
# MAGIC   intervention_required
# MAGIC FROM student_dropout.gold_student_risk_profile
# MAGIC LIMIT 20;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ML PREDICTIONS PER STUDENT

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   p.student_id,
# MAGIC   p.dropout_probability,
# MAGIC   g.risk_level,
# MAGIC   g.intervention_required
# MAGIC FROM student_dropout.gold_dropout_predictions p
# MAGIC JOIN student_dropout.gold_student_risk_profile g
# MAGIC   ON p.student_id = g.student_id
# MAGIC ORDER BY p.dropout_probability DESC
# MAGIC LIMIT 20;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC HIGH-RISK STUDENTS ONLY

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   student_id,
# MAGIC   dropout_probability
# MAGIC FROM student_dropout.gold_dropout_predictions
# MAGIC WHERE dropout_probability > 0.7
# MAGIC ORDER BY dropout_probability DESC;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   layer_name,
# MAGIC   table_name,
# MAGIC   record_count,
# MAGIC   run_status,
# MAGIC   run_timestamp
# MAGIC FROM student_dropout.pipeline_audit_log
# MAGIC ORDER BY run_timestamp DESC;
# MAGIC

# COMMAND ----------

