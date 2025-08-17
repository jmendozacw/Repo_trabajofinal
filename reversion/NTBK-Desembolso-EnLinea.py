# Databricks notebook source
# MAGIC %md
# MAGIC 1. LIBRERIAS

# COMMAND ----------

import dlt
from pyspark.sql.functions import col, to_date, trim, to_timestamp
from pyspark.sql.functions import col, sum as _sum, date_format

# COMMAND ----------

# MAGIC %md
# MAGIC 2. VARIABLES DE RUTAS AL CONTENEDOR EN ADLS

# COMMAND ----------

# Ruta al contenedor RAW en ADLS
raw_path_sol = "abfss://raw@adlsdevelop10.dfs.core.windows.net/solicitud_creditos"
raw_path_cli = "abfss://raw@adlsdevelop10.dfs.core.windows.net/clientes"
raw_path_age = "abfss://raw@adlsdevelop10.dfs.core.windows.net/agencias"


# COMMAND ----------

# MAGIC %md
# MAGIC 3. INGESTA A CAPA BRONCE

# COMMAND ----------

# -------------------------------
# bronce
# -------------------------------
#------Ingesta de la tabla Solicitudes de creditos------

@dlt.table(
    name="solicitudcred_bronce",
    comment="Tabla bronce de Solicitudes de creditos crudos leídos de forma incremental desde ADLS de azure.",
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def solicitudcred_bronce():
    return (
        spark.readStream
        .format("cloudFiles")  # Auto Loader
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(raw_path_sol)
    )

    #------Ingesta de la tabla Clientes ------

@dlt.table(
    name="clientes_bronce",
    comment="Tabla bronce de Clientes crudos leídos de forma incremental desde ADLS de azure.",
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def clientes_bronce():
    return (
        spark.readStream
        .format("cloudFiles") 
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(raw_path_cli)
    )

 #------Ingesta de la tabla Agencias ------

@dlt.table(
    name="agencias_bronce",
    comment="Tabla bronce de Clientes crudos leídos de forma incremental desde ADLS de azure.",
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def agencias_bronce():
    return (
        spark.readStream
        .format("cloudFiles") 
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(raw_path_age)
    )



# COMMAND ----------

# MAGIC %md
# MAGIC 4. LIMPIEZA EN LA CAPA SILVER

# COMMAND ----------

# -------------------------------
# SILVER
# -------------------------------

#-------Se realiza la limpieza de la tabla Solicitudes de creditos-------
@dlt.table(
    name="solicitudcred_silver",
    comment="Tabla SolicitudCred_silver con datos limpios y tipificados.",
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def solicitudcred_silver():
    return (
        dlt.read_stream("solicitudcred_bronce")
        .withColumn("dFechaRegistro", to_date(col("dFechaRegistro"), "yyyy-MM-dd"))
        .withColumn("cod_solicitud", trim(col("cod_solicitud")))
        .withColumn("cOperacion", trim(col("cOperacion")))
        .withColumn("nCapitalSolicitado", col("nCapitalSolicitado").cast("decimal(18,2)"))
        .withColumn("nCapitalPropuesto", col("nCapitalPropuesto").cast("decimal(18,2)"))
        .withColumn("nCapitalAprobado", col("nCapitalAprobado").cast("decimal(18,2)"))
        .withColumn("subproducto", trim(col("subproducto")))
        .withColumn("estado", trim(col("estado")))
        .withColumn("cOperacion", trim(col("cOperacion")))
        .withColumn("Canal_Registro", trim(col("Canal_Registro")))
        .select(
            "dFechaRegistro",
            "dFecHoraRegistro",
            "cod_solicitud",
            "cod_cliente",
            "subproducto",
            "estado",
            "cOperacion",
            "cod_usuario",
            "nCuotas",
            "nCapitalSolicitado",
            "nCapitalPropuesto",
            "nCapitalAprobado",
            "nTasaCompensatoria",
            "nTasaMoratoria",
            "cod_agencia",
            "Canal_Registro"
        )

    )

#-------Se realiza la limpieza de la tabla Clientes-------
@dlt.table(
    name="clientes_silver",
    comment="Tabla Clientes_silver con datos limpios y tipificados.",
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def clientes_silver():
    return (
        dlt.read_stream("clientes_bronce")
        .withColumn("dFechareg", to_timestamp(col("dFechareg"), "yyyy-MM-dd HH:mm:ss"))
        .withColumn("Nombre_Cliente", trim(col("Nombre_Cliente")))
        .withColumn("Doc_Identidad", trim(col("Doc_Identidad")))
        .withColumn("cTipoCliente", trim(col("cTipoCliente")))
        .withColumn("Nacionalidad", trim(col("Nacionalidad")))
        .select(
            "cod_cliente",
            "Cod_Agencia",
            "Nombre_Cliente",
            "Tipo_Documento",
            "Doc_Identidad",
            "cTipoCliente",
            "Nacionalidad",
            "dFechareg"
        )
    )

#-------Se realiza la limpieza de la tabla Agencias-------
@dlt.table(
    name="agencias_silver",
    comment="Tabla Agencias_silver con datos limpios y tipificados.",
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def agencias_silver():
    return (
        dlt.read_stream("agencias_bronce")
        .withColumn("cDesZona", trim(col("cDesZona")))
        .withColumn("cNombreAge", trim(col("cNombreAge")))
        .withColumn("cNombreEstab", trim(col("cNombreEstab")))
        .select(
            "idAgencia",
            "cDesZona",
            "cNombreAge",
            "cNombreEstab",
            "idEstablecimiento"
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC 5. GENERACION DE LA TABLA PARA REPORTERIA Y DASHBOARD

# COMMAND ----------

# -------------------------------
# GOLD
# -------------------------------
@dlt.table(
    name="desembolsos_EnLinea_gold",
    comment="Tabla Gold con los cruces de las tablas para obtener metricas de desembolsos en linea.",
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def desembolsos_EnLinea_gold():
    solicitudes = dlt.read_stream("solicitudcred_silver")
    clientes = dlt.read("clientes_silver")   
    agencias = dlt.read("agencias_silver")   

    df = (
        solicitudes
        .join(clientes, solicitudes["cod_cliente"] == clientes["cod_cliente"], "inner")
        .join(agencias, solicitudes["cod_agencia"] == agencias["idAgencia"], "inner")
        .withColumnRenamed("nCapitalAprobado", "monto")
        .select(
            solicitudes["dFechaRegistro"],
            solicitudes["cod_solicitud"],
            solicitudes["subproducto"],
            solicitudes["cOperacion"],
            clientes["Nombre_Cliente"],
            clientes["Doc_Identidad"],
            clientes["Nacionalidad"],
            agencias["cNombreAge"],
            agencias["cDesZona"],
            col("monto")
        )
    )
    return df