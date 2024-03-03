# Synapse Analytics notebook source

# METADATA ********************

# META {
# META   "synapse": {
# META     "lakehouse": {
# META       "default_lakehouse": "52776468-6f4f-40e7-8534-dc45c02193d6",
# META       "default_lakehouse_name": "PIInovatorsLH",
# META       "default_lakehouse_workspace_id": "8917cd64-1bc0-4858-a527-045ba726753a"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # PREPARE DATA FOR GOLD
# ##### Preparing data according the schemas in the Delta tables in gold zone

# MARKDOWN ********************

# **_Function for checking column in DataFrame_**

# CELL ********************

from pyspark.sql import DataFrame
from pyspark.sql.functions import lit
from pyspark.sql.utils import AnalysisException

def noColumn(df: DataFrame, path: str):
    try:
        df[path]
        return False
    except AnalysisException:
        return True

# MARKDOWN ********************

# **_Prepare data according the schema of sender_analysis table_**

# CELL ********************

from pyspark.sql import functions as F
import json

#get row from delta table in df
df_tab=spark.read.option("multiLine","true").load("Files/silver/sender_data").filter(F.col("DocumentID") == DocumentID)

#only response in df
tempdf = spark.read.option("multiLine","true").json(sc.parallelize([df_tab.collect()[0]['response']]))


struct_cols = [  c[0]   for c in tempdf.dtypes if c[1][:6] == "struct"   ]
#if no struct columns
if len(struct_cols)==0:
    df_final=tempdf.select("*")
#if struct columns
if len(struct_cols)>0:
    for field in tempdf.schema.fields:
        jsonkey=field.name

        str_json = tempdf.select(F.col(jsonkey)).first()[0]
        b = sc.parallelize([str_json])
        df=b.toDF()

        struct_cols = [  c[0]   for c in df.dtypes if c[1][:6] == "struct"   ]
        if len(struct_cols)>0:
            for c in struct_cols:
                df_final=df.select("*",col(c+".*")).drop(c)
        df_final=df.select("*")
display(df_final)


df_final.printSchema()


for field in df_final.schema.fields:
    print(field.name)
    if "address" in field.name:

        df_final = df_final.withColumn("address",df_final[field.name])
    if "mail" in field.name:

        df_final = df_final.withColumn("email",df_final[field.name])
    if "name" in field.name:

        df_final = df_final.withColumn("name",df_final[field.name])
    if "phone" in field.name:

        df_final = df_final.withColumn("phone",df_final[field.name])
    if "state" in field.name:

        df_final = df_final.withColumn("state",df_final[field.name])
    if "town" in field.name:

        df_final = df_final.withColumn("town",df_final[field.name])
    if "country"in field.name:

        df_final = df_final.withColumn("country",df_final[field.name])
    if "zip" in field.name:

        df_final = df_final.withColumn("zip_code",df_final[field.name])
print(df_final)
if noColumn(df_final, "address"):
    df_final=df_final.withColumn("address",lit(None))
if noColumn(df_final, "name"):
    df_final=df_final.withColumn("name",lit(None))
if noColumn(df_final, "email"):
    df_final=df_final.withColumn("email",lit(None))
if noColumn(df_final, "phone"):
    df_final=df_final.withColumn("phone",lit(None))
if noColumn(df_final, "state"):
    df_final=df_final.withColumn("state",lit(None))
if noColumn(df_final, "town"):
    df_final=df_final.withColumn("town",lit(None))
if noColumn(df_final, "country"):
    df_final=df_final.withColumn("country",lit(None))
if noColumn(df_final, "zip_code"):
    df_final=df_final.withColumn("zip_code",lit(None))
print(df_final)
df_insert=df_final.join(df_tab).select("DocumentID","address","email","name","phone","state","town","country","zip_code")


display(df_insert)

array_cols = [  c[0]   for c in df_final.dtypes if c[1][:5] == "array"   ]
if len (array_cols)==0:
    df_insert.write.format("delta").mode("append").saveAsTable("sender_analysis")

# MARKDOWN ********************

# **_Prepare data according the schema of document_analysis table_**

# CELL ********************

df_class=spark.read.option("multiLine","true").load("Files/silver/clasiffication").filter(F.col("DocumentID") == DocumentID)

df_category=spark.read.option("multiLine","true").load("Files/silver/category").filter(F.col("DocumentID") == DocumentID)

df_subject=spark.read.option("multiLine","true").load("Files/silver/subject").filter(F.col("DocumentID") == DocumentID)

tempdfclass = spark.read.option("multiLine","true").json(sc.parallelize([df_class.collect()[0]['response']]))
tempdfsubject = spark.read.option("multiLine","true").json(sc.parallelize([df_subject.collect()[0]['response']]))
tempdfcategory = spark.read.option("multiLine","true").json(sc.parallelize([df_category.collect()[0]['response']]))

for field in tempdfclass.schema.fields:
    tempdfclass = tempdfclass.withColumn("iscompliant",tempdfclass[field.name])
for field in tempdfsubject.schema.fields:
    if "subject" in field.name:
        tempdfsubject = tempdfsubject.withColumn("subject",tempdfsubject[field.name])
for field in tempdfcategory.schema.fields:
    if "category" in field.name:
        tempdfcategory = tempdfcategory.withColumn("category",tempdfcategory[field.name])


if noColumn(tempdfclass, "iscompliant"):
    tempdfclass=tempdfclass.withColumn("iscompliant",lit(None))
if noColumn(tempdfsubject, "subject"):
    tempdfsubject=tempdfsubject.withColumn("subject",lit(None))
if noColumn(tempdfcategory, "category"):
    tempdfcategory=tempdfcategory.withColumn("category",lit(None))

df_class=df_class.join(tempdfclass).select("DocumentID","iscompliant")
df_category=df_class.join(tempdfcategory).select("DocumentID","iscompliant","category")
df_import=df_category.join(tempdfsubject).select("DocumentID","iscompliant","category","subject")


df_import.write.format("delta").mode("append").saveAsTable("document_analysis")
