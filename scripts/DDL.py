# Databricks notebook source
dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %sql
# MAGIC create widget text storage default "abfss://lhcldata@adlsdevelop10.dfs.core.windows.net";
# MAGIC create widget text catalogo default "catalog_dev";

# COMMAND ----------

# MAGIC %md
# MAGIC **CREACION DE CATALOGO**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS catalog_dev;

# COMMAND ----------

# MAGIC %md
# MAGIC **CREACION DE ESQUEMA**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS catalog_dev.plataforma_financiera;