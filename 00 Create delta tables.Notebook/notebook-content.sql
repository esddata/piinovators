-- Synapse Analytics notebook source

-- METADATA ********************

-- META {
-- META   "synapse": {
-- META     "lakehouse": {
-- META       "default_lakehouse": "52776468-6f4f-40e7-8534-dc45c02193d6",
-- META       "default_lakehouse_name": "PIInovatorsLH",
-- META       "default_lakehouse_workspace_id": "8917cd64-1bc0-4858-a527-045ba726753a",
-- META       "known_lakehouses": [
-- META         {
-- META           "id": "52776468-6f4f-40e7-8534-dc45c02193d6"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- MARKDOWN ********************

-- # CREATE DELTA TABLES
-- ## Create all tables in Silver and Gold zone

-- CELL ********************

 drop table documents

-- CELL ********************

-- Metadata for the document
CREATE TABLE documents (
    
    DocumentID string,
    DocumentPath string,
    DocumentExtension string,
    UpdatedAt string

)
USING DELTA 
LOCATION 'Files/silver/documents'

-- CELL ********************

 drop table if exists clasiffication

-- CELL ********************

--Store the response in json string format from prompt: "if this document is complaint or not"
CREATE TABLE clasiffication (
    DocumentID string,
    response string,
    UpdatedAt string
)

USING DELTA
LOCATION 'Files/silver/clasiffication'

-- CELL ********************

 drop table if exists pii_data

-- CELL ********************

--Store the response in json string format from prompt: "What are the Personally identifiable information in this image? Return only adresses, emails, full names, accounts, tepephones."
CREATE TABLE pii_data (
    DocumentID string,
    response string,
    UpdatedAt string

)

USING DELTA
LOCATION 'Files/silver/pii_data'

-- CELL ********************

 drop table if exists category

-- CELL ********************

--Store the response in json string format from prompt: "In which compliant category is the text in this image? Posible categories are:Product or service,Wait time". "Delivery,Personnel,Online,Continual,Communication. Return only one category.")
CREATE TABLE category (
    DocumentID string,
    response string,
    UpdatedAt string
)

USING DELTA
LOCATION 'Files/silver/category'

-- CELL ********************

 drop table if exists subject

-- CELL ********************

--Store the response in json string format from prompt: "What is the subject of the text from the image? Return only subject."
CREATE TABLE subject (
    DocumentID string,
    response string,
    UpdatedAt string
)

USING DELTA
LOCATION 'Files/silver/subject'

-- CELL ********************

 drop table if exists sender_data

-- CELL ********************

--Store the response in json string format from prompt: "What is the subject of the text from the image? Return only subject."
CREATE TABLE sender_data (
    DocumentID string,
    response string,
    UpdatedAt string
)

USING DELTA
LOCATION 'Files/silver/sender_data'

-- CELL ********************

 drop table if exists document_analysis

-- CELL ********************

--Store documents data for reporting in gold zone
CREATE TABLE document_analysis (
    DocumentID string,
    iscompliant string,
    category string,
    subject string
)
 
USING DELTA
LOCATION 'Files/gold/document_analysis'

-- CELL ********************

 drop table if exists sender_analysis

-- CELL ********************

--Store senders data for reporting in gold zone
CREATE TABLE sender_analysis (
    DocumentID string,
    address string,
    email string,
    name string,
    phone string,
    state string,
    town string,
    country string,
    zip_code string
)
 
USING DELTA
LOCATION 'Files/gold/sender_analysis'

-- CELL ********************

 drop table if exists mask_pii_data

-- CELL ********************

--Store the response in json string format from prompt: "What are the Personally identifiable information of the sender only in this text ?\Return the same text with masked PII data (Masked PII data means display only the first char of all of the Personally \ identifiable information and mask the rest of the chars with * for adresses, emails, full name, date of birth, telephone \ numbers, driver's license number, credit or debit card number or Social Security number of the sender)" 
CREATE TABLE mask_pii_data (
    DocumentID string,
    response string,
    UpdatedAt string

)

USING DELTA 
LOCATION 'Files/gold/mask_pii_data'
