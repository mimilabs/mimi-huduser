# Databricks notebook source
# MAGIC %run /Workspace/Repos/yubin.park@mimilabs.ai/mimi-common-utils/ingestion_utils

# COMMAND ----------

from itertools import chain
catalog = "mimi_ws_1" # delta table destination catalog
schema = "huduser" # delta table destination schema
path = f"/Volumes/{catalog}/{schema}/src" # where all the input files are located

# COMMAND ----------

# MAGIC %md
# MAGIC ## Census Tract to ZIP

# COMMAND ----------

tablename = "tract_to_zip"
files = []
for filepath in chain(Path(f"{path}").glob("TRACT_ZIP*"),
                      Path(f"{path}").glob("tract_zip*")):
    dt = datetime.strptime(filepath.stem.split('_')[-1], "%m%d%y").date()
    files.append((dt, filepath))
files = sorted(files, key=lambda x: x[0], reverse=True)

# COMMAND ----------

for item in files:
    print(f"processing {item[1].name}...")
    pdf = pd.read_excel(item[1], dtype={"TRACT": str, 
                                        "ZIP": str,
                                        "USPS_ZIP_PREF_CITY": str,
                                        "USPS_ZIP_PREF_STATE": str})
    pdf.columns = change_header(pdf.columns)
    pdf["mimi_src_file_date"] = item[0]
    pdf["mimi_src_file_name"] = item[1].name
    pdf["mimi_dlt_load_date"] = datetime.today().date()
    df = spark.createDataFrame(pdf)
    (df.write
        .format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .option("replaceWhere", f"mimi_src_file_name = '{item[1].name}'")
        .saveAsTable(f"mimi_ws_1.{schema}.{tablename}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## County to ZIP

# COMMAND ----------

tablename = "county_to_zip"
files = []
for filepath in chain(Path(f"{path}").glob("COUNTY_ZIP*"),
                      Path(f"{path}").glob("county_zip*")):
    dt = datetime.strptime(filepath.stem.split('_')[-1], "%m%d%y").date()
    files.append((dt, filepath))
files = sorted(files, key=lambda x: x[0], reverse=True)

# COMMAND ----------

for item in files:
    print(f"processing {item[1].name}...")
    pdf = pd.read_excel(item[1], dtype={"COUNTY": str, "ZIP": str,
                                        "USPS_ZIP_PREF_CITY": str,
                                        "USPS_ZIP_PREF_STATE": str})
    pdf.columns = change_header(pdf.columns)
    pdf["mimi_src_file_date"] = item[0]
    pdf["mimi_src_file_name"] = item[1].name
    pdf["mimi_dlt_load_date"] = datetime.today().date()
    df = spark.createDataFrame(pdf)
    (df.write
        .format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .option("replaceWhere", f"mimi_src_file_name = '{item[1].name}'")
        .saveAsTable(f"mimi_ws_1.{schema}.{tablename}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## CBSA to ZIP

# COMMAND ----------

tablename = "cbsa_to_zip"
files = []
for filepath in chain(Path(f"{path}").glob("CBSA_ZIP*"),
                      Path(f"{path}").glob("cbsa_zip*")):
    dt = datetime.strptime(filepath.stem.split('_')[-1], "%m%d%y").date()
    files.append((dt, filepath))
files = sorted(files, key=lambda x: x[0], reverse=True)

# COMMAND ----------

for item in files:
    print(f"processing {item[1].name}...")
    pdf = pd.read_excel(item[1], dtype={"CBSA": str, "ZIP": str,
                                        "USPS_ZIP_PREF_CITY": str,
                                        "USPS_ZIP_PREF_STATE": str})
    pdf.columns = change_header(pdf.columns)
    pdf["mimi_src_file_date"] = item[0]
    pdf["mimi_src_file_name"] = item[1].name
    pdf["mimi_dlt_load_date"] = datetime.today().date()
    df = spark.createDataFrame(pdf)
    (df.write
        .format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .option("replaceWhere", f"mimi_src_file_name = '{item[1].name}'")
        .saveAsTable(f"mimi_ws_1.{schema}.{tablename}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## ZIP to Census Tract

# COMMAND ----------

tablename = "zip_to_tract"
files = []
for filepath in chain(Path(f"{path}").glob("ZIP_TRACT*"),
                      Path(f"{path}").glob("zip_tract*")):
    dt = datetime.strptime(filepath.stem.split('_')[-1], "%m%d%y").date()
    files.append((dt, filepath))
files = sorted(files, key=lambda x: x[0], reverse=True)

# COMMAND ----------

for item in files:
    print(f"processing {item[1].name}...")
    pdf = pd.read_excel(item[1], dtype={"TRACT": str, "ZIP": str,
                                        "USPS_ZIP_PREF_CITY": str,
                                        "USPS_ZIP_PREF_STATE": str})
    pdf.columns = change_header(pdf.columns)
    pdf["mimi_src_file_date"] = item[0]
    pdf["mimi_src_file_name"] = item[1].name
    pdf["mimi_dlt_load_date"] = datetime.today().date()
    df = spark.createDataFrame(pdf)
    (df.write
        .format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .option("replaceWhere", f"mimi_src_file_name = '{item[1].name}'")
        .saveAsTable(f"mimi_ws_1.{schema}.{tablename}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## ZIP to County

# COMMAND ----------

tablename = "zip_to_county"
files = []
for filepath in chain(Path(f"{path}").glob("ZIP_COUNTY_*"),
                      Path(f"{path}").glob("zip_county_*")):
    if "_COUNTY_SUB_" in filepath.name.upper():
        continue
    dt = datetime.strptime(filepath.stem.split('_')[-1], "%m%d%y").date()
    files.append((dt, filepath))
files = sorted(files, key=lambda x: x[0], reverse=True)

# COMMAND ----------

for item in files:
    print(f"processing {item[1].name}...")
    pdf = pd.read_excel(item[1], dtype={"COUNTY": str, "ZIP": str,
                                        "USPS_ZIP_PREF_CITY": str,
                                        "USPS_ZIP_PREF_STATE": str})
    pdf.columns = change_header(pdf.columns)
    pdf["mimi_src_file_date"] = item[0]
    pdf["mimi_src_file_name"] = item[1].name
    pdf["mimi_dlt_load_date"] = datetime.today().date()
    df = spark.createDataFrame(pdf)
    (df.write
        .format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .option("replaceWhere", f"mimi_src_file_name = '{item[1].name}'")
        .saveAsTable(f"mimi_ws_1.{schema}.{tablename}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## ZIP to CBSA

# COMMAND ----------

tablename = "zip_to_cbsa"
files = []
for filepath in chain(Path(f"{path}").glob("ZIP_CBSA_*"),
                      Path(f"{path}").glob("zip_cbsa_*")):
    if "_CBSA_DIV_" in filepath.name.upper():
        continue
    dt = datetime.strptime(filepath.stem.split('_')[-1], "%m%d%y").date()
    files.append((dt, filepath))
files = sorted(files, key=lambda x: x[0], reverse=True)

# COMMAND ----------

for item in files:
    print(f"processing {item[1].name}...")
    pdf = pd.read_excel(item[1], dtype={"CBSA": str, "ZIP": str,
                                        "USPS_ZIP_PREF_CITY": str,
                                        "USPS_ZIP_PREF_STATE": str})
    pdf.columns = change_header(pdf.columns)
    pdf["mimi_src_file_date"] = item[0]
    pdf["mimi_src_file_name"] = item[1].name
    pdf["mimi_dlt_load_date"] = datetime.today().date()
    df = spark.createDataFrame(pdf)
    (df.write
        .format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .option("replaceWhere", f"mimi_src_file_name = '{item[1].name}'")
        .saveAsTable(f"mimi_ws_1.{schema}.{tablename}"))

# COMMAND ----------


