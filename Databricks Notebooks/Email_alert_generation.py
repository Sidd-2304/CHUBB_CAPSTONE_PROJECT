# Databricks notebook source
# MAGIC %md
# MAGIC env variables set explicitly
# MAGIC

# COMMAND ----------

import os

os.environ["SMTP_USER"] = "22071A12J2@vnrvjiet.in"
os.environ["SMTP_PASSWORD"] = "irqw yenx bddk zfxa"


# COMMAND ----------

smtp_user = os.getenv("SMTP_USER")
smtp_password = os.getenv("SMTP_PASSWORD")

print(smtp_user)


# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp

alerts_df = (
    spark.table("student_dropout.gold_dropout_predictions")
    .filter(col("dropout_probability") >= 0.7)
    .withColumn("alert_generated_timestamp", current_timestamp())
)

alerts_df.write \
    .mode("overwrite") \
    .saveAsTable("student_dropout.gold_high_risk_alerts")


# COMMAND ----------

# MAGIC %md
# MAGIC GENERATE ALERT SUMMARY

# COMMAND ----------

high_risk_count = alerts_df.count()

print(f"ALERT: {high_risk_count} students identified with high dropout risk.")


# COMMAND ----------

# MAGIC %md
# MAGIC PREPARE INSIGHTS DATA

# COMMAND ----------

from pyspark.sql.functions import col

# Load alert and risk tables
alerts_df = spark.table("student_dropout.gold_high_risk_alerts")
risk_df = spark.table("student_dropout.gold_student_risk_profile")

high_risk_count = alerts_df.count()

# Top 5 highest risk students
top_students = (
    alerts_df.orderBy(col("dropout_probability").desc())
    .limit(5)
    .select("student_id", "dropout_probability")
    .toPandas()
)

# Join to get risk drivers
drivers_df = (
    alerts_df.join(risk_df, "student_id")
    .groupBy("primary_risk_driver")
    .count()
    .orderBy(col("count").desc())
    .toPandas()
)

# Grade-level concentration
grade_df = (
    alerts_df.join(risk_df, "student_id")
    .groupBy("grade_level")
    .count()
    .orderBy(col("count").desc())
    .toPandas()
)


# COMMAND ----------

email_body = f"""
🚨 STUDENT DROPOUT RISK ALERT

SUMMARY
-------
• Total high-risk students identified: {high_risk_count}

TOP HIGH-RISK STUDENTS
---------------------
{top_students.to_string(index=False)}

PRIMARY RISK DRIVERS
-------------------
{drivers_df.to_string(index=False)}

GRADE-LEVEL HOTSPOTS
-------------------
{grade_df.to_string(index=False)}

RECOMMENDED ACTIONS
-------------------
• Prioritize attendance interventions for students with attendance-related risk
• Initiate academic counseling for low performance groups
• Monitor socio-economic risk students closely

This alert is generated based on ML-driven dropout probability analysis.
"""


# COMMAND ----------

import smtplib
from email.message import EmailMessage
import os

smtp_user = os.getenv("SMTP_USER")
smtp_password = os.getenv("SMTP_PASSWORD")

msg = EmailMessage()
msg["Subject"] = "🚨 Action Required: Student Dropout Risk Insights"
msg["From"] = smtp_user
msg["To"] = "samisettysiddartha2004@gmail.com"

msg.set_content(email_body)

with smtplib.SMTP("smtp.gmail.com", 587) as server:
    server.starttls()
    server.login(smtp_user, smtp_password)
    server.send_message(msg)

print("Alert email sent successfully"))


# COMMAND ----------

dbutils.secrets.listScopes()



# COMMAND ----------

