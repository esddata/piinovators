PIInovators Cloud-Native Data Solution Documentation

Overview

PIInovators is a cloud-native data solution developed in Microsoft Fabric, integrating OpenAI for document analysis, particularly for detecting Personal Identification Information (PII) in files and images. The solution classifies documents into complaint and non-complaint categories, further categorizing them based on predefined types (Delivery, Personnel, Online, Continual, Communication). The solution is based on a medallion architecture where data is stored in three zones (Bronze, Silver, Gold) within Microsoft Fabric Lakehouse. Subsequently, the data is prepared for analytical use in Power BI reports.

Open AI models used:

    •	model: GPT-4, version: 1106-preview for files,
  
    •	model: GPT-4, version: vision-preview for images.
 
Components

  1. External Source Integration
     
    •	Client-Side Storage: External storage on the client side where files and images are stored for analysis.
    
  2. Microsoft Fabric Lakehouse - PIInovatorsLH

    Bronze Zone
    
      •	Raw Section:
      
        •	Unprocessed:
        
            •	Files: Raw data for files lands here.
            
            •	Images: Raw data for images lands here.
            
        •	Processed:
        
            •	Files: Processed files are moved here.
            
            •	Images: Processed images are moved here.
            
        •	Failed:
        
            •	Files: Files with processing errors are moved here.
            
            •	Images: Images with processing errors are moved here.
            
      Silver Zone
      
            •	Delta Tables: Populated with transformed data, including metadata, classification, sender data, category, subject, PII data, masked PII data, sender analysis, and document analysis.
            
      Gold Zone
      
•	Analytical Data Preparation:
•	Parsed JSON strings from Silver Delta Tables are structured into a fixed schema in the Gold Zone for Power BI reports.
4. Objects
Notebooks
1.	00 Create Delta Tables:
•	Creates all the delta tables used in the project.
2.	01 Insert Documents for Analysis:
•	Stores metadata for documents.
3.	02 Ingestion Data from Text Documents and PII Analysis:
•	Reads, detects, and stores PII data from text into Silver Zone Delta Tables.
4.	03 Ingestion Data from Image Documents and PII Analysis:
•	Extracts text from images, reads, detects, and stores PII data from text into Silver Zone Delta Tables.
5.	04 Load and Prepare Data in Gold Zone:
•	Parses stored JSON strings from Silver Delta Tables into a fixed schema structure in the Gold Zone Delta Table.
Delta Tables
•	documents: Metadata for documents.
•	classification: Stores responses in JSON string format for document complain classification.
•	sender_data: Stores responses in JSON string format for sender information.
•	category: Stores responses in JSON string format for document categorization.
•	subject: Stores responses in JSON string format for document subject identification.
•	pii_data: Stores responses in JSON string format for PII identification.
•	mask_pii_data: Stores responses in JSON string format for masked PII data.
•	sender_analysis: Stores sender data for reporting in the Gold Zone.
•	document_analysis: Stores document data for reporting in the Gold Zone.
Pipelines
•	pl-adls-external-source:
 
 
  
Pipeline Activities:
Get Document Names
•	GetMetadata Activity:
•	Provides a list of child items from the external source.
ForEachDocumentName
•	ForEach Activity:
•	Takes child items one by one from the GetMetadata activity.


Variables:
•	varDocumentName: Document name (without extension).
•	varDocumentExtension: Document extension (.txt, .csv, .png, .jpg, .jpeg).
•	varDocumentNameFinal: Concatenates Document Name with a timestamp for the current UTC date.
•	VarBronzePathFileOrImage: Indicates if the document is a file or an image for document path.
•	VarDocumentId: Creates DocumentID (GUID).
Copy from External Source
•	CopyData Activity:
•	Copies data from the external source to the Bronze Zone (Raw/Unprocessed/VarBronzePathFileOrImage).
Insert Document
•	Notebook Activity:
•	Executes the [01 Insert Documents for Analysis] notebook.
Check Extension
•	IfCondition Activity:
•	Checks the file extension and runs the corresponding notebook for files or images.
Analyze PII Files
•	Notebook Activity:
•	Executes the [02 Ingestion Data from Text Documents and PII Analysis] notebook.
Copy to Processed Files
•	CopyData Activity:
•	Moves files from Unprocessed to Processed folder.
Prepare Data for Gold Zone Files
•	Notebook Activity:
•	Executes the [04 Load and Prepare Data in Gold Zone] notebook.
Copy to Failed Files
•	CopyData Activity:
•	Moves data from Unprocessed/Files to Failed/Files in case of processing errors.
Analyze PII Images
•	Notebook Activity:
•	Executes the [03 Ingestion Data from Image Documents and PII Analysis] notebook.
Copy to Processed Images
•	CopyData Activity:
•	Moves images from Unprocessed to Processed folder.
Prepare Data for Gold Zone Images
•	Notebook Activity:
•	Executes the [04 Load and Prepare Data in Gold Zone] notebook.
Copy to Failed Images
•	CopyData Activity:
•	Moves data from Unprocessed/Images to Failed/Images in case of processing errors.
This documentation provides an overview of the PIInovators cloud-native data solution, outlining its architecture, components, objects, and pipeline activities. It serves as a comprehensive guide for understanding the solution's design and functionality.

