-- Databricks notebook source
-- MAGIC %md
-- MAGIC **GRANDS A OTROS USUARIOS CREADOS Y AGRUPADOS **

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **ACCESO A CATALOGOS**

-- COMMAND ----------

GRANT USE CATALOG ON CATALOG catalog_dev TO `DataEnginiers`;
GRANT USE CATALOG ON CATALOG catalog_dev TO `Admins`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **ACCESO A ESQUEMAS**

-- COMMAND ----------

GRANT USE SCHEMA ON SCHEMA catalog_dev.plataforma_financiera TO `DataEnginiers`;
GRANT CREATE ON SCHEMA catalog_dev.plataforma_financiera TO `Admins`;