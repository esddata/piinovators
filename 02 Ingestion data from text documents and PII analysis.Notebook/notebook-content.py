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

# # Ingestion data from text documents and PII analysis using OpenAI service
# ##### using GPT4 version 1106-Preview to find PII data, classify if that file is a compliant or not, and do the categorization of the file

# MARKDOWN ********************

# _**install needed libraries**_

# CELL ********************

import datetime
import openai
import os
import base64
import requests
import json
import sys

#from dotenv import load_dotenv
import openai
from IPython.display import Image
import time

from pprint import pprint


from pyspark.sql.functions import *
from delta.tables import *


# MARKDOWN ********************

# **_check OpenAI version_**

# CELL ********************

#%pip install openai --upgrade

def check_openai_version():
    """
    Check Azure Open AI version
    """
    import openai

    installed_version = openai.__version__

    try:
        version_number = float(installed_version[:3])
    except ValueError:
        print("Invalid OpenAI version format")
        return

    print(f"Installed OpenAI version: {installed_version}")

    if version_number < 1.0:
        print("[Warning] You should upgrade OpenAI to have version >= 1.0.0")
        print("To upgrade, run: %pip install openai --upgrade")
    else:
        print(f"[OK] OpenAI version {installed_version} is >= 1.0.0")


check_openai_version()

# MARKDOWN ********************

# _**Connecting OpenAI service using key vault secrets**_

# CELL ********************

from notebookutils.mssparkutils.credentials import getSecret

KEYVAULT_ENDPOINT = "https://mfaiFabricKeyVault.vault.azure.net/"

AZURE_OPENAI_KEY=getSecret(KEYVAULT_ENDPOINT, "openaiKeyGPT4Vision")
AZURE_OPENAI_ENDPOINT=getSecret(KEYVAULT_ENDPOINT, "openaiEndpointGPT4Vision")

openai.api_type = 'azure'
openai.api_key = AZURE_OPENAI_KEY
openai.api_base = AZURE_OPENAI_ENDPOINT # your endpoint should look like the following https://YOUR_RESOURCE_NAME.openai.azure.com/
openai.api_version = '2023-03-15-preview' # this might change in the future

model = "gpt-4-text"

# MARKDOWN ********************

# _**definition gpt4Text**_

# CELL ********************

def gpt4Text(text, query):
    """
    GPT4-Vision
    """
    # Endpoint
    base_url = f"{openai.api_base}/openai/deployments/{model}"
    endpoint = f"{base_url}/chat/completions?api-version=2023-12-01-preview"

    # Header
    headers = {"Content-Type": "application/json", "api-key": openai.api_key}

    prompt = query + "\n" + text


    # Prompt
    data = {
        "messages": [
            {"role": "system", "content": "You are a helpful assistant, and you only replay with JSON."},
            {"role": "user", "content": prompt},
        ],
        "max_tokens": 4000,
    }
    response_format={ "type": "json_object" },
    
    # Results
    response = requests.post(endpoint, headers=headers, data=json.dumps(data))

    if response.status_code == 200:
        result = json.loads(response.text)["choices"][0]["message"]["content"]
        return result
    
    if response.status_code == 429:
        print("[ERROR] Too many requests. Please wait a couple of seconds and try again.")
    
    else:
        print("[ERROR] Error code:", response.status_code)

# MARKDOWN ********************


# MARKDOWN ********************

# _**send the file path as parameter and read the file**_

# CELL ********************

import csv
#"/lakehouse/default/Files/bronze/raw/unprocessed/files/PII_demo_file_2024-02-26T19:12:00.436.csv"
csv_file_path =  f"/lakehouse/default/Files/bronze/raw/unprocessed/files/{DocumentNameFinal}"
#!ls $csv_file_path -lh
with open(csv_file_path, "r", newline="") as file:
    csv_reader = csv.reader(file)
    text = ""

    for row in csv_reader:
        text += ",".join(row) + "\n"


print(text)

# CELL ********************

#prompt (needed for document clasiffication is complaint or not)
time.sleep(100)
result = gpt4Text(text, "Is this document a complaint or not? Answer only with yes or no.")

columns=["DocumentID","response", "UpdatedAt"]
spark.createDataFrame([(DocumentID, result, UpdatedAt)],columns).write.format("delta").mode("append").saveAsTable("clasiffication")

spark.sql("UPDATE PIInovatorsLH.clasiffication  set response=REPLACE(REPLACE(response,'```json',''),'```','') WHERE DocumentID={parDocumentID}",parDocumentID = DocumentID)

print(result)

# CELL ********************

#prompt (needed to return PII data in files to be blured)
time.sleep(60)
result = gpt4Text(text, "What are the Personally identifiable information in this image? Return only adresses, emails, full names, accounts, tepephones.")

time.sleep(40)
columns=["DocumentID","response","UpdatedAt"]
spark.createDataFrame([(DocumentID, result, UpdatedAt)],columns).write.format("delta").mode("append").saveAsTable("pii_data")

print(result)

# CELL ********************

#prompt (needed to return senders PII data)
time.sleep(100)

result = gpt4Text(text, "What are the Personally identifiable information in this text of the sender? \
Return only full adresses,email, name, telephone number, state, town, country, zip_code, date of birth, driver's license number, credit or debit card number or Social Security number of the sender.\
All of the returned values should be strings, not arrays")

columns=["DocumentID","response", "UpdatedAt"]
spark.createDataFrame([(DocumentID, result, UpdatedAt)],columns).write.format("delta").mode("append").saveAsTable("sender_data")

spark.sql("UPDATE PIInovatorsLH.sender_data  set response=REPLACE(REPLACE(response,'```json',''),'```','') WHERE DocumentID={parDocumentID}",parDocumentID = DocumentID)

print(result)

# CELL ********************

#prompt (needed to categorized the complaint)"
time.sleep(100)
result = gpt4Text(text, "In which compliant category is this text? Posible categories are: Product or service, Wait time \
Delivery, Personnel, Online, Continual, Communication. Return only one category.")

columns=["DocumentID","response", "UpdatedAt"]
spark.createDataFrame([(DocumentID, result, UpdatedAt)],columns).write.format("delta").mode("append").saveAsTable("category")

spark.sql("UPDATE PIInovatorsLH.category  set response=REPLACE(REPLACE(response,'```json',''),'```','') WHERE DocumentID={parDocumentID}",parDocumentID = DocumentID)

print(result)

# CELL ********************

#prompt (needed to extract the subject of compliant)
time.sleep(100)
result = gpt4Text(text, "What is the subject(primary topic, idea or content that is discussed within the text) of the text?\
 Return only the subject.")

columns=["DocumentID","response", "UpdatedAt"]
spark.createDataFrame([(DocumentID, result, UpdatedAt)],columns).write.format("delta").mode("append").saveAsTable("subject")

spark.sql("UPDATE PIInovatorsLH.subject  set response=REPLACE(REPLACE(response,'```json',''),'```','') WHERE DocumentID={parDocumentID}",parDocumentID = DocumentID)

print(result)

# CELL ********************

time.sleep(100)
result = gpt4Text(text, " What are the Personally identifiable information in this text of the sender only? \
Return only the first char of all of the Personally identifiable information and mask the rest of the chars with * \
for adresses, emails, full name, date of birth, telephone numbers, driver's license number, credit or debit card number \
or Social Security number of the sender. Return the same text with masked PII data")

columns=["DocumentID","response", "UpdatedAt"]
spark.createDataFrame([(DocumentID, result, UpdatedAt)],columns).write.format("delta").mode("append").saveAsTable("mask_pii_data")

print(result)
