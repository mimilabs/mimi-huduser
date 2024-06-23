# Databricks notebook source
import pyspark.sql.functions as f

# COMMAND ----------

crosswalk_lst = [('tract', 'zip'),
                 ('zip', 'county'),
                 ('zip', 'cbsa')]

# COMMAND ----------

for crosswalk_pair in crosswalk_lst:
    geo_from = crosswalk_pair[0]
    geo_to = crosswalk_pair[1]
    tablename_org =  f'mimi_ws_1.huduser.{geo_from}_to_{geo_to}'
    tablename_new = f'mimi_ws_1.huduser.{geo_from}_to_{geo_to}_mto'
    df = spark.read.table(tablename_org)
    latest_src_date = df.select(f.max('mimi_src_file_date')).collect()[0][0]
    df_latest = (df.filter(df.mimi_src_file_date == latest_src_date)
                    .withColumn('score', 
                                (f.when(f.col('res_ratio')> 0, f.col('res_ratio'))
                                .otherwise(f.col('tot_ratio')))))
    num_org = df_latest.count()
    df_res_max = (df_latest.groupBy(geo_from)
              .agg(f.max('score').alias('score_max')))
    df_res1to1 = (df_latest.join(df_res_max, [geo_from], 'left')
            .filter(f.col("score") == f.col("score_max"))
            .dropDuplicates([geo_from]))
    num_res1to1 = df_res1to1.count()
    (df_res1to1.write
        .mode('overwrite')
        .option('mergeSchema', 'true')
        .saveAsTable(tablename_new))
    spark.sql(f"""COMMENT ON TABLE {tablename_new} IS '# {geo_from} to {geo_to} crosswalk, many-to-one (mto) mapping based on the residential size\nThe dataset is created from `{tablename_org}` by taking the latest source file, and taking the maximum residential size mapping per `{geo_from}` (smaller geographic area between the two definitions). The original file has a many-to-many mapping, but this table will have a many-to-one mapping. In case of ties, we take a random selection between the ties.\n\n- The latest source file date: `{latest_src_date}`.\n- The size of the latest source: `{num_org}`.\n- After the many-to-one mapping: `{num_res1to1}`.';""")

# COMMAND ----------

crosswalk_lst = [('county', 'zip'),
                 ('cbsa', 'zip'),
                 ('zip', 'tract')]

# COMMAND ----------

for crosswalk_pair in crosswalk_lst:
    geo_from = crosswalk_pair[0]
    geo_to = crosswalk_pair[1]
    tablename_org =  f'mimi_ws_1.huduser.{geo_from}_to_{geo_to}'
    tablename_new = f'mimi_ws_1.huduser.{geo_from}_to_{geo_to}_otm'
    df = spark.read.table(tablename_org)
    latest_src_date = df.select(f.max('mimi_src_file_date')).collect()[0][0]
    df_latest = (df.filter(df.mimi_src_file_date == latest_src_date)
                    .withColumn('score', 
                                (f.when(f.col('res_ratio')> 0, f.col('res_ratio'))
                                .otherwise(f.col('tot_ratio')))))
    num_org = df_latest.count()
    df_res_max = (df_latest.groupBy(geo_to)
              .agg(f.max('score').alias('score_max')))
    df_res1to1 = (df_latest.join(df_res_max, [geo_to], 'left')
            .filter(f.col("score") == f.col("score_max"))
            .dropDuplicates([geo_to]))
    num_res1to1 = df_res1to1.count()
    (df_res1to1.write
        .mode('overwrite')
        .option('mergeSchema', 'true')
        .saveAsTable(tablename_new))
    spark.sql(f"""COMMENT ON TABLE {tablename_new} IS '# {geo_from} to {geo_to} crosswalk, one-to-many (otm) mapping based on the residential size\nThe dataset is created from `{tablename_org}` by taking the latest source file, and taking the maximum residential size mapping per `{geo_to}` (smaller geographic area between the two definitions). The original file has a many-to-many mapping, but this table will have a one-to-many mapping. As a result, the mapping between `{geo_from}` to `{geo_to}` will be one to one. In case of ties, we take a random selection between the ties.\n\n- The latest source file date: `{latest_src_date}`.\n- The size of the latest source: `{num_org}`.\n- After the one-to-many mapping: `{num_res1to1}`.';""")

# COMMAND ----------

# MAGIC %sql
# MAGIC --DROP TABLE IF EXISTS mimi_ws_1.huduser.cbsa_to_zip_res1to1;
# MAGIC --DROP TABLE IF EXISTS mimi_ws_1.huduser.tract_to_zip_res1to1;
# MAGIC --DROP TABLE IF EXISTS mimi_ws_1.huduser.county_to_zip_res1to1;
# MAGIC --DROP TABLE IF EXISTS mimi_ws_1.huduser.zip_to_cbsa_res1to1;
# MAGIC --DROP TABLE IF EXISTS mimi_ws_1.huduser.zip_to_tract_res1to1;
# MAGIC --DROP TABLE IF EXISTS mimi_ws_1.huduser.zip_to_county_res1to1;
# MAGIC

# COMMAND ----------


