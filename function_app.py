# =============================================================================
# IMPORTS
# =============================================================================
import azure.functions as func            # Azure Functions SDK
import azure.durable_functions as df      # Durable Functions extension
from azure.data.tables import TableServiceClient, TableClient  # Table Storage SDK
from PIL import Image                     # Pillow - image processing library
import logging                            # Python built-in logging
import json                               # Python built-in JSON handling
import io                                 # Python built-in for byte stream handling
import os                                 # Python built-in for environment variables
import uuid                               # Python built-in for generating unique IDs
from datetime import datetime             # Python built-in for timestamps
from typing import Dict, List             # Access to the dictionary and list types for type hints [AI suggestion]
import PyPDF2                             # PyPDF2 - PDF processing library [AI suggestion]
import re                                 # Python built-in for regular expressions (used for sensitive data detection) [AI suggestion]
from pdf2image import convert_from_bytes  # pdf2image - Convert PDF pages to images for OCR fallback [AI suggestion]
import pytesseract                        # pytesseract - OCR library for text extraction from images [AI suggestion]
import fitz                               # PyMuPDF
# =============================================================================
# CREATE THE DURABLE FUNCTION APP
# =============================================================================
myApp = df.DFApp(http_auth_level=func.AuthLevel.ANONYMOUS)

# =============================================================================
# TABLE STORAGE HELPER
# =============================================================================
# This helper function creates a connection to Azure Table Storage.
# It uses the same connection string as our Blob trigger.
# For local development, this connects to Azurite's Table Storage emulator.
TABLE_NAME = "PdfAnalysisResults"

def get_table_client():
    """Get a TableClient for storing/retrieving analysis results."""
    connection_string = os.environ["PdfStorageConnection"] ## updates connection string
    table_service = TableServiceClient.from_connection_string(connection_string)
    # create_table_if_not_exists ensures the table exists before we use it
    table_service.create_table_if_not_exists(TABLE_NAME)
    return table_service.get_table_client(TABLE_NAME)

# =============================================================================
# 1. CLIENT FUNCTION (Blob Trigger - The Entry Point)
# =============================================================================
# Function triggers automatically when a PDF image is uploaded to the "pdfs" container.
@myApp.blob_trigger(
    arg_name="myblob",
    path="pdfs/{name}", ## Uses a new container
    connection="PdfStorageConnection"
)
@myApp.durable_client_input(client_name="client")
async def blob_trigger(myblob: func.InputStream, client):
    # Get the blob name (e.g., "pdfs/pdf1.jpg")
    blob_name = myblob.name
    # Read the blob content as bytes (the actual image data)
    blob_bytes = myblob.read()
    # Get the file size in KB
    blob_size_kb = round(len(blob_bytes) / 1024, 2)

    logging.info(f"New pdf detected: {blob_name} ({blob_size_kb} KB)") ## change makes logs more logical

    # Prepare input data for the orchestrator
    # We pass the blob name and the raw image bytes (as a list of integers)
    # Note: We convert bytes to a list because Durable Functions serialize
    # inputs as JSON, and JSON doesn't support raw bytes
    input_data = {
        "blob_name": blob_name,
        "blob_bytes": list(blob_bytes),
        "blob_size_kb": blob_size_kb
    }

    # Start the orchestrator
    instance_id = await client.start_new(
        "pdf_analyzer_orchestrator", # change orchestration function's name to match new function
        client_input=input_data
    )

    logging.info(f"Started orchestration {instance_id} for {blob_name}")

# =============================================================================
# 2. ORCHESTRATOR FUNCTION (The Workflow Manager)
# =============================================================================
# This orchestrator implements a HYBRID pattern:
#   - Fan-Out/Fan-In: Run 4 analyses in parallel
#   - Chaining: Then generate report -> store results (sequential)
@myApp.orchestration_trigger(context_name="context")
def pdf_analyzer_orchestrator(context):
    # Get input from the blob trigger
    input_data = context.get_input()
    logging.info(f"Orchestrator started for: {input_data['blob_name']}")

    # -------------------------
    # STEP 1: Extract text first
    # -------------------------
    text_result = yield context.call_activity("extract_text", input_data)
    logging.info(f"Text extraction completed: {len(text_result.get('pages', []))} pages found")

    # -------------------------
    # STEP 2: Run other analyses in parallel
    # -------------------------
    analysis_tasks = [
        context.call_activity("extract_metadata", input_data),         # metadata can use original blob
        context.call_activity("analyze_statistics", text_result),      # statistics need extracted text
        context.call_activity("detect_sensitive_data", text_result),   # sensitive data detection needs text
    ]

    metadata_result, stats_result, sensitive_result = yield context.task_all(analysis_tasks)
    logging.info("Parallel analyses completed")

    # -------------------------
    # STEP 3: Generate combined report
    # -------------------------
    report_input = {
        "blob_name": input_data["blob_name"],
        "text": text_result,
        "metadata": metadata_result,
        "stats": stats_result,
        "sensitive_data": sensitive_result,
    }

    report = yield context.call_activity("generate_report", report_input)
    logging.info(f"Report generated for: {input_data['blob_name']}")

    # -------------------------
    # STEP 4: Store results in Table Storage
    # -------------------------
    record = yield context.call_activity("store_results", report)
    logging.info(f"Results stored with ID: {record.get('id')}")

    return record

# =============================================================================
# 3. ACTIVITY: extract text
# =============================================================================
@myApp.activity_trigger(input_name="inputData")
def extract_text(inputData: dict) -> dict:
    logging.info(f"Starting PDF text extraction for {inputData.get('blob_name')}")
    pages_text = []

    try:
        pdf_bytes = bytes(inputData["blob_bytes"])
        pdf_stream = io.BytesIO(pdf_bytes)
        doc = fitz.open(stream=pdf_stream, filetype="pdf")

        for page_number in range(len(doc)):
            page = doc[page_number]
            text = page.get_text("text")  # Extract plain text
            pages_text.append({
                "page_number": page_number + 1,
                "text": text
            })

        logging.info(f"Extracted text from {len(pages_text)} pages.")
        return {"pages": pages_text}

    except Exception as e:
        logging.error(f"PDF text extraction failed: {str(e)}")
        return {"pages": [], "error": str(e)}
    
# =============================================================================
# 4. ACTIVITY: Extract Metadata
# =============================================================================
@myApp.activity_trigger(input_name="inputData")
def extract_metadata(inputData: dict) -> Dict[str, str]: # [proposed by AI]
    logging.info(f"Extracting metadata for {inputData.get('blob_name')}")
    try:
        pdf_bytes = bytes(inputData["blob_bytes"])
        pdf_stream = io.BytesIO(pdf_bytes)
        reader = PyPDF2.PdfReader(pdf_stream)
        info = reader.metadata

        return {
            "author": info.author if info.author else "",
            "title": info.title if info.title else "",
            "subject": info.subject if info.subject else "",
            "keywords": info.keywords if info.keywords else "",
            "creator": info.creator if info.creator else "",
            "producer": info.producer if info.producer else "",
            "creation_date": str(info.creation_date) if info.creation_date else "",
            "mod_date": str(info.modification_date) if info.modification_date else ""
        }

    except Exception as e:
        logging.error(f"Metadata extraction failed: {str(e)}")
        return {"error": str(e)}

# =============================================================================
# 5. ACTIVITY: Analyze Statistics
# =============================================================================
@myApp.activity_trigger(input_name="inputData")
def analyze_statistics(inputData: dict) -> Dict[str, float]: # [proposed by AI]
    pages = inputData.get("pages", [])
    total_words = sum(len(page["text"].split()) for page in pages)
    page_count = len(pages)
    avg_words_per_page = total_words / page_count if page_count > 0 else 0
    estimated_reading_time_min = total_words / 200  # assuming 200 wpm

    logging.info(f"Stats: {total_words} words across {page_count} pages.")
    return {
        "page_count": page_count,
        "word_count": total_words,
        "avg_words_per_page": avg_words_per_page,
        "estimated_reading_time_min": round(estimated_reading_time_min, 2)
    }

# =============================================================================
# 6. ACTIVITY: Detect Sensitive Data
# =============================================================================
EMAIL_REGEX = r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+"
PHONE_REGEX = r"\+?\d[\d\s\-\(\)]{7,}\d"
URL_REGEX = r"https?://[^\s]+"
DATE_REGEX = r"\b(?:\d{1,2}[-/]\d{1,2}[-/]\d{2,4}|\d{4}-\d{2}-\d{2})\b"

@myApp.activity_trigger(input_name="textData")
def detect_sensitive_data(textData: dict) -> Dict[str, List[str]]:
    pages = textData.get("pages", [])
    emails, phones, urls, dates = [], [], [], []

    for page in pages:
        text = page.get("text", "")
        emails.extend(re.findall(EMAIL_REGEX, text))
        phones.extend(re.findall(PHONE_REGEX, text))
        urls.extend(re.findall(URL_REGEX, text))
        dates.extend(re.findall(DATE_REGEX, text))

    return {
        "emails": list(set(emails)),
        "phone_numbers": list(set(phones)),
        "urls": list(set(urls)),
        "dates": list(set(dates))
    }

# =============================================================================
# 7. ACTIVITY: Generate Report
# =============================================================================
# This activity takes the results from all 4 analyses and combines them
@myApp.activity_trigger(input_name="reportData")
def generate_report(reportData: dict):
    logging.info("Generating combined report...")

    blob_name = reportData["blob_name"]
    # Extract just the filename from the full path (e.g., "pdf/class1.jpg" -> "class1.jpg")
    filename = blob_name.split("/")[-1] if "/" in blob_name else blob_name

    report = {
        "id": str(uuid.uuid4()),
        "fileName": filename,
        "blobPath": blob_name,
        "analyzedAt": datetime.utcnow().isoformat(),
        "analyses": {
            "text": reportData.get("text", {}),
            "metadata": reportData.get("metadata", {}),
            "statistics": reportData.get("stats", {}),
            "sensitive_data": reportData.get("sensitive_data", {}),
        },
        "summary": {
            "pageCount": reportData.get("stats", {}).get("page_count", 0),
            "wordCount": reportData.get("stats", {}).get("word_count", 0),
            "avgWordsPerPage": reportData.get("stats", {}).get("avg_words_per_page", 0),
            "estimatedReadingTimeMin": reportData.get("stats", {}).get("estimated_reading_time_min", 0),
            "hasText": any(page.get("text") for page in reportData.get("text", {}).get("pages", [])),
            "emailsDetected": len(reportData.get("sensitive_data", {}).get("emails", [])),
            "phoneNumbersDetected": len(reportData.get("sensitive_data", {}).get("phone_numbers", [])),
            "urlsDetected": len(reportData.get("sensitive_data", {}).get("urls", [])),
            "datesDetected": len(reportData.get("sensitive_data", {}).get("dates", [])),
            "author": reportData.get("metadata", {}).get("author", "None"),
            "title": reportData.get("metadata", {}).get("title", "None"),
            "subject": reportData.get("metadata", {}).get("subject", "None"),
            "keywords": reportData.get("metadata", {}).get("keywords", "None"),
            "creator": reportData.get("metadata", {}).get("creator", "None"),
            "producer": reportData.get("metadata", {}).get("producer", "None"),
            "creation_date": reportData.get("metadata", {}).get("creation_date", "None"),
            "modification_date": reportData.get("metadata", {}).get("mod_date", "None")
        }
    }

    logging.info(f"Report generated: {report['id']}")
    return report

# =============================================================================
# 8. ACTIVITY: Store Results in Table Storage
# =============================================================================
# This activity saves the generated report to Azure Table Storage.
#
# Table Storage requires two keys:
#   - PartitionKey: Groups related entities (we use "PDFAnalysis")
#   - RowKey: Unique identifier within the partition (we use the report ID)
@myApp.activity_trigger(input_name="report")
def store_results(report: dict):
    logging.info(f"Storing results for {report['fileName']}...")

    try:
        table_client = get_table_client()

        # Table Storage entities are flat key-value pairs.
        # Complex nested data (like our analyses) must be serialized as JSON strings.
        entity = {
            "PartitionKey": "PDFAnalysis", # updated partition key to reflect PDF analysis
            "RowKey": report["id"],
            "FileName": report["fileName"],
            "BlobPath": report["blobPath"],
            "AnalyzedAt": report["analyzedAt"],
            # Store complex data as JSON strings
            "Summary": json.dumps(report["summary"]),
            "TextAnalysis": json.dumps(report["analyses"]["text"]),
            "MetadataAnalysis": json.dumps(report["analyses"]["metadata"]),
            "StatisticsAnalysis": json.dumps(report["analyses"]["statistics"]),
            "SensitiveDataAnalysis": json.dumps(report["analyses"]["sensitive_data"]),
        }

        table_client.upsert_entity(entity)

        logging.info(f"Results stored with ID: {report['id']}")

        return {
            "id": report["id"],
            "fileName": report["fileName"],
            "status": "stored",
            "analyzedAt": report["analyzedAt"],
            "summary": report["summary"]
        }

    except Exception as e:
        logging.error(f"Failed to store results: {str(e)}")
        return {
            "id": report.get("id", "unknown"),
            "status": "error",
            "error": str(e)
        }

# =============================================================================
# 9. HTTP FUNCTION: Get Analysis Results
# =============================================================================
# This is a regular HTTP function (like Week 2) that retrieves stored results
# from Table Storage. It's NOT part of the orchestration - it's a separate
# endpoint for users to query past analyses.
#
# Usage:
#   GET /api/results          - Get all results (last 10)
#   GET /api/results?limit=5  - Get last 5 results
#   GET /api/results/{id}     - Get a specific result by ID
@myApp.route(route="results/{id?}")
def get_results(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Get results endpoint called")

    try:
        table_client = get_table_client()
        result_id = req.route_params.get("id")

        if result_id:
            # Get a specific result by ID
            try:
                entity = table_client.get_entity(
                    partition_key="PDFAnalysis", # Updated partition key
                    row_key=result_id
                )

                # Parse JSON strings back into objects
                result = {
                    "id": entity["RowKey"],
                    "fileName": entity["FileName"],
                    "blobPath": entity["BlobPath"],
                    "analyzedAt": entity["AnalyzedAt"],
                    "summary": json.loads(entity["Summary"]),
                    "analyses": { # updated for new activity functions
                        "text": json.loads(entity["TextAnalysis"]),
                        "metadata": json.loads(entity["MetadataAnalysis"]),
                        "statistics": json.loads(entity["StatisticsAnalysis"]),
                        "sensitive_data": json.loads(entity["SensitiveDataAnalysis"]),
                    }
                }

                return func.HttpResponse(
                    json.dumps(result, indent=2),
                    mimetype="application/json",
                    status_code=200
                )

            except Exception:
                return func.HttpResponse(
                    json.dumps({"error": f"Result not found: {result_id}"}),
                    mimetype="application/json",
                    status_code=404
                )
        else:
            # Get all results (with optional limit)
            limit = int(req.params.get("limit", "10"))

            entities = table_client.query_entities(
                query_filter="PartitionKey eq 'PDFAnalysis'" # Updated partition key
            )

            results = []
            for entity in entities:
                results.append({
                    "id": entity["RowKey"],
                    "fileName": entity["FileName"],
                    "analyzedAt": entity["AnalyzedAt"],
                    "summary": json.loads(entity["Summary"]),
                })

            # Sort by analyzedAt descending (most recent first)
            results.sort(key=lambda x: x["analyzedAt"], reverse=True)
            results = results[:limit]

            return func.HttpResponse(
                json.dumps({"count": len(results), "results": results}, indent=2),
                mimetype="application/json",
                status_code=200
            )

    except Exception as e:
        logging.error(f"Failed to retrieve results: {str(e)}")
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            mimetype="application/json",
            status_code=500
        )
        
## http://localhost:7071/api/results/b99cf27c-e684-4bb2-ae4c-ef6874ea8001