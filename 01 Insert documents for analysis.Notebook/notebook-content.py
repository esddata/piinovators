# Synapse Analytics notebook source

# METADATA ********************

# META {
# META   "synapse": {
# META     "lakehouse": {
# META       "default_lakehouse": "52776468-6f4f-40e7-8534-dc45c02193d6",
# META       "default_lakehouse_name": "PIInovatorsLH",
# META       "default_lakehouse_workspace_id": "8917cd64-1bc0-4858-a527-045ba726753a",
# META       "known_lakehouses": [
# META         {
# META           "id": "52776468-6f4f-40e7-8534-dc45c02193d6"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ###

# MARKDOWN ********************

# # INSERT METADATA FOR THE DOCUMENT

# CELL ********************

columns=["DocumentID","DocumentPath","DocumentExtension","UpdatedAt"]
spark.createDataFrame([(DocumentID, DocumentPath, DocumentExtension, UpdatedAt)],columns).write.format("delta").mode("append").saveAsTable("documents")

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT * FROM documents
