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
# META     },
# META     "environment": {
# META       "environmentId": "53559c07-b5c1-4a02-b66e-aa0e7684ad14",
# META       "workspaceId": "8917cd64-1bc0-4858-a527-045ba726753a"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Ingestion data from image documents and PII analysis using OpenAI service
# ##### using GPT4 version 1106-Preview to find PII data, classify if that image is a complaint or not, and do the categorization of the image

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

from azure.cognitiveservices.vision.computervision import ComputerVisionClient
from msrest.authentication import CognitiveServicesCredentials
from azure.cognitiveservices.vision.computervision.models import OperationStatusCodes
#from PIL import Image
import matplotlib.pyplot as plt
import matplotlib.patches as patches


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
openai.api_version = '2023-05-15' # this might change in the future

model = "gpt-4-vision"

# MARKDOWN ********************

# _**Connecting computer-vision service using key vault secrets**_

# CELL ********************

VISION_KEYVAULT_ENDPOINT = "https://computer-vision-service.vault.azure.net/"

VISION_OPENAI_KEY=getSecret(VISION_KEYVAULT_ENDPOINT, "computer-vision-key")
VISION_OPENAI_ENDPOINT=getSecret(VISION_KEYVAULT_ENDPOINT, "computer-vision-endpoint")

key=VISION_OPENAI_KEY
endpoint = VISION_OPENAI_ENDPOINT


# MARKDOWN ********************

# _**Creates a client using the computer vision service given an endpoint**_

# CELL ********************

computervision_client = ComputerVisionClient(endpoint, CognitiveServicesCredentials(key))

# MARKDOWN ********************

# _**definition gpt4V function which returns responses in JSON format**_

# CELL ********************

def gpt4V(image_file, query):
    """
    GPT4-Vision
    """
    # Endpoint
    base_url = f"{openai.api_base}/openai/deployments/{model}"
    endpoint = f"{base_url}/chat/completions?api-version=2023-12-01-preview"

    # Header
    headers = {"Content-Type": "application/json", "api-key": openai.api_key}

    # Encoded image
    base_64_encoded_image = base64.b64encode(open(image_file, "rb").read()).decode(
        "ascii"
    )

    # Prompt
    data = {
        "messages": [
            {"role": "system", "content": "You are a helpful assistant, and you only replay with JSON."},
            {"role": "user", "content": [query, {"image": base_64_encoded_image}]},
        ],
        "max_tokens": 4000,
    }

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

# _**definition gpt4V_array function which returns responses in array**_

# CELL ********************

def gpt4V_array(image_file, query):
    """
    GPT4-Vision
    """
    # Endpoint
    base_url = f"{openai.api_base}/openai/deployments/{model}"
    endpoint = f"{base_url}/chat/completions?api-version=2023-12-01-preview"

    # Header
    headers = {"Content-Type": "application/json", "api-key": openai.api_key}

    # Encoded image
    base_64_encoded_image = base64.b64encode(open(image_file, "rb").read()).decode(
        "ascii"
    )

    # Prompt
    data = {
        "messages": [
            {"role": "system", "content": "You are a helpful assistant, and you only replay with array."},
            {"role": "user", "content": [query, {"image": base_64_encoded_image}]},
        ],
        "max_tokens": 4000,
    }

    # Results
    response = requests.post(endpoint, headers=headers, data=json.dumps(data))

    if response.status_code == 200:
        result = json.loads(response.text)["choices"][0]["message"]["content"]
        return result
    
    if response.status_code == 429:
        print("[ERROR] Too many requests. Please wait a couple of seconds and try again.")
    
    else:
        print("[ERROR] Error code:", response.status_code)

# CELL ********************

from IPython.display import Image

imagefile = f"/lakehouse/default/Files/bronze/raw/unprocessed/images/{DocumentNameFinal}"

PIIdataarray=[]

Image(filename=imagefile)

# CELL ********************

#prompt (needed for document clasiffication is complaint or not)
time.sleep(60)
result = gpt4V(imagefile, "Is this document a complaint or not? Answer only with yes or no.")
print(result)

time.sleep(40)
columns=["DocumentID","response","UpdatedAt"]
spark.createDataFrame([(DocumentID, result, UpdatedAt)],columns).write.format("delta").mode("append").saveAsTable("clasiffication")

spark.sql("UPDATE PIInovatorsLH.clasiffication  set response=REPLACE(REPLACE(response,'```json',''),'```','') WHERE DocumentID={parDocumentID}",parDocumentID = DocumentID)



# CELL ********************

#prompt (needed to return PII data in image to be blured)
time.sleep(60)
result = gpt4V(imagefile, "What are the Personally identifiable information in this image? Return only adresses, emails, full names, accounts, tepephones.")
text_from_image= result
print(result)

time.sleep(40)
columns=["DocumentID","response","UpdatedAt"]
spark.createDataFrame([(DocumentID, result, UpdatedAt)],columns).write.format("delta").mode("append").saveAsTable("pii_data")

# CELL ********************

#prompt (needed to return senders PII data)
time.sleep(60)
result = gpt4V(imagefile, "Who is the sender and from which town and country and which is sender's mail and phone?")

time.sleep(40)
columns=["DocumentID","response","UpdatedAt"]
spark.createDataFrame([(DocumentID, result, UpdatedAt)],columns).write.format("delta").mode("append").saveAsTable("sender_data")

spark.sql("UPDATE PIInovatorsLH.sender_data  set response=REPLACE(REPLACE(response,'```json',''),'```','') WHERE DocumentID={parDocumentID}",parDocumentID = DocumentID)

print(result)

# CELL ********************

#prompt (needed to categorized the complaint)"
time.sleep(60)
result = gpt4V(imagefile, "In which complaint category is the text in this image? Posible categories are:Product or service,Wait time"
"Delivery,Personnel,Online,Continual,Communication. Return only one category.")

time.sleep(40)
columns=["DocumentID","response","UpdatedAt"]
spark.createDataFrame([(DocumentID, result, UpdatedAt)],columns).write.format("delta").mode("append").saveAsTable("category")

spark.sql("UPDATE PIInovatorsLH.category  set response=REPLACE(REPLACE(response,'```json',''),'```','') WHERE DocumentID={parDocumentID}",parDocumentID = DocumentID)

print(result)

# CELL ********************

#prompt (needed to extract the subject of complaint)
time.sleep(60)
result = gpt4V(imagefile, "What is the subject of the text from the image? Return only subject.")
time.sleep(40)
columns=["DocumentID","response","UpdatedAt"]
spark.createDataFrame([(DocumentID, result, UpdatedAt)],columns).write.format("delta").mode("append").saveAsTable("subject")

spark.sql("UPDATE PIInovatorsLH.subject  set response=REPLACE(REPLACE(response,'```json',''),'```','') WHERE DocumentID={parDocumentID}",parDocumentID = DocumentID)

print(result)

# MARKDOWN ********************

# _**start the process for blurring the PII data in the image **_

# CELL ********************

#prompt (mask the extracted PII data)
time.sleep(100)
result = gpt4V(imagefile, " What are the Personally identifiable information of the sender only in this text ?\
Return the same text with masked PII data (Masked PII data means display only the first char of all of the Personally \
identifiable information and mask the rest of the chars with * for adresses, emails, full name, date of birth, telephone \
numbers, driver's license number, credit or debit card number or Social Security number of the sender)")

columns=["DocumentID","response","UpdatedAt"]
spark.createDataFrame([(DocumentID, result, UpdatedAt)],columns).write.format("delta").mode("append").saveAsTable("mask_pii_data")

print(result)

# CELL ********************

#prompt (extract the PII data in an array)
time.sleep(90)
result = gpt4V_array(imagefile, "What are the Personally identifiable information of the sender in this image?\
Return them in an array including any punctuation marks which come after them in the text.")
text_from_image= result

print (result)

# MARKDOWN ********************

# **_**Using Azure computer vision service for OCR and OpenCv library for bluring the PII data in the image **_**

# CELL ********************

from PIL import Image
import numpy as np
import time
import cv2
from azure.cognitiveservices.vision.computervision import ComputerVisionClient
from azure.cognitiveservices.vision.computervision.models import OperationStatusCodes
from msrest.authentication import CognitiveServicesCredentials

# Replace with your own values
endpoint = endpoint
subscription_key = key
computervision_client = ComputerVisionClient(endpoint, CognitiveServicesCredentials(subscription_key))

# Open local image file

imagefile = f"/lakehouse/default/Files/bronze/raw/unprocessed/images/{DocumentNameFinal}"
image_path = imagefile
image = open(image_path, "rb")
img = Image.open(image_path)
original_img = img.copy()

# Define the words to blur (replace this with your array)
words_to_blur = text_from_image

# Call the API
read_response = computervision_client.read_in_stream(image, raw=True)

# Get the operation location (URL with an ID at the end)
read_operation_location = read_response.headers["Operation-Location"]

# Grab the ID from the URL
operation_id = read_operation_location.split("/")[-1]

# Retrieve the results 
while True:
    read_result = computervision_client.get_read_result(operation_id)
    if read_result.status not in ['notStarted', 'running']:
        break
    time.sleep(1)

# Check if 'read_result' is defined
if hasattr(read_result, 'analyze_result') and hasattr(read_result.analyze_result, 'read_results'):
    for text_result in read_result.analyze_result.read_results:
        for line in text_result.lines:
            for word in line.words:
                if word.text in words_to_blur:
                    # Get the bounding box of the word
                    xy1 = [int(coord) for coord in word.bounding_box[0:2]]
                    xy3 = [int(coord) for coord in word.bounding_box[4:6]]

                    # Apply Gaussian blur to that region
                    word_region = np.array(img)[xy1[1]:xy3[1], xy1[0]:xy3[0]]
                    blurred_word_region = cv2.GaussianBlur(word_region, (25, 25), 7)
                    img.paste(Image.fromarray(blurred_word_region), (xy1[0], xy1[1]))
else:
    print("Error: No analyze result found in read_result.")

# Display the modified image
img.show()
#original_img.show()

