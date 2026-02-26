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

# =============================================================================
# CREATE THE DURABLE FUNCTION APP
# =============================================================================
# Same as Week 4: df.DFApp instead of func.FunctionApp
myApp = df.DFApp(http_auth_level=func.AuthLevel.ANONYMOUS)

# =============================================================================
# TABLE STORAGE HELPER
# =============================================================================
# This helper function creates a connection to Azure Table Storage.
# It uses the same connection string as our Blob trigger.
# For local development, this connects to Azurite's Table Storage emulator.
TABLE_NAME = "ImageAnalysisResults"

def get_table_client():
    """Get a TableClient for storing/retrieving analysis results."""
    connection_string = os.environ["ImageStorageConnection"]
    table_service = TableServiceClient.from_connection_string(connection_string)
    # create_table_if_not_exists ensures the table exists before we use it
    table_service.create_table_if_not_exists(TABLE_NAME)
    return table_service.get_table_client(TABLE_NAME)

# =============================================================================
# 1. CLIENT FUNCTION (Blob Trigger - The Entry Point)
# =============================================================================
# Unlike Week 4 where you used an HTTP trigger, this function triggers
# automatically when an image is uploaded to the "images" container.
#
# How it works:
#   1. User uploads an image to the "images" container in Blob Storage
#   2. Azure detects the new blob and triggers this function
#   3. This function starts the orchestrator, passing the blob name
#
# The path "images/{name}" means:
#   - Watch the "images" container
#   - {name} captures the filename (e.g., "photo.jpg")
@myApp.blob_trigger(
    arg_name="myblob",
    path="images/{name}",
    connection="ImageStorageConnection"
)
@myApp.durable_client_input(client_name="client")
async def blob_trigger(myblob: func.InputStream, client):
    # Get the blob name (e.g., "images/photo.jpg")
    blob_name = myblob.name
    # Read the blob content as bytes (the actual image data)
    blob_bytes = myblob.read()
    # Get the file size in KB
    blob_size_kb = round(len(blob_bytes) / 1024, 2)

    logging.info(f"New image detected: {blob_name} ({blob_size_kb} KB)")

    # Prepare input data for the orchestrator
    # We pass the blob name and the raw image bytes (as a list of integers)
    # Note: We convert bytes to a list because Durable Functions serialize
    # inputs as JSON, and JSON doesn't support raw bytes
    input_data = {
        "blob_name": blob_name,
        "blob_bytes": list(blob_bytes),
        "blob_size_kb": blob_size_kb
    }

    # Start the orchestrator (same concept as Week 4's client.start_new)
    instance_id = await client.start_new(
        "image_analyzer_orchestrator",
        client_input=input_data
    )

    logging.info(f"Started orchestration {instance_id} for {blob_name}")

# =============================================================================
# 2. ORCHESTRATOR FUNCTION (The Workflow Manager)
# =============================================================================
# This orchestrator implements a HYBRID pattern:
#   - Fan-Out/Fan-In: Run 4 analyses in parallel
#   - Chaining: Then generate report -> store results (sequential)
#
# Compare to Week 4's orchestrator:
#   Week 4: yield call_activity(...) three times sequentially
#   Lab 2:  yield context.task_all([...]) for parallel, then yield for sequential
@myApp.orchestration_trigger(context_name="context")
def image_analyzer_orchestrator(context):
    # Get the input data passed from the blob trigger
    input_data = context.get_input()

    logging.info(f"Orchestrator started for: {input_data['blob_name']}")

    # =========================================================================
    # STEP 1: FAN-OUT - Run all 4 analyses in parallel
    # =========================================================================
    # Create a list of tasks WITHOUT yielding each one individually.
    # Each call_activity starts a task but doesn't wait for it.
    analysis_tasks = [
        context.call_activity("analyze_colors", input_data),
        context.call_activity("analyze_objects", input_data),
        context.call_activity("analyze_text", input_data),
        context.call_activity("analyze_metadata", input_data),
    ]

    # FAN-IN: yield context.task_all() waits for ALL tasks to complete.
    # This is the key difference from Week 4's sequential yield.
    # All 4 activities run simultaneously, and we get all results at once.
    results = yield context.task_all(analysis_tasks)

    # results is a list in the same order as analysis_tasks:
    # results[0] = analyze_colors result
    # results[1] = analyze_objects result
    # results[2] = analyze_text result
    # results[3] = analyze_metadata result

    # =========================================================================
    # STEP 2: CHAIN - Generate report from combined results
    # =========================================================================
    # Now we chain: take the parallel results and combine them into a report.
    # This must happen AFTER all analyses complete (sequential).
    report_input = {
        "blob_name": input_data["blob_name"],
        "colors": results[0],
        "objects": results[1],
        "text": results[2],
        "metadata": results[3],
    }

    report = yield context.call_activity("generate_report", report_input)

    # =========================================================================
    # STEP 3: CHAIN - Store the report in Table Storage
    # =========================================================================
    # Final step: persist the report to Azure Table Storage.
    record = yield context.call_activity("store_results", report)

    return record

# =============================================================================
# 3. ACTIVITY: Analyze Colors
# =============================================================================
# This activity extracts dominant colors from the image using Pillow.
# It samples pixels from the image and identifies the most common colors.
#
# In a production app, you might use Azure Computer Vision API instead.
@myApp.activity_trigger(input_name="inputData")
def analyze_colors(inputData: dict):
    logging.info("Analyzing colors...")

    try:
        # Convert the byte list back to bytes, then open as an image
        image_bytes = bytes(inputData["blob_bytes"])
        image = Image.open(io.BytesIO(image_bytes))

        # Convert to RGB if necessary (handles PNG with alpha, grayscale, etc.)
        if image.mode != "RGB":
            image = image.convert("RGB")

        # Resize to a small size for faster color sampling
        # We don't need full resolution just to get dominant colors
        small_image = image.resize((50, 50))

        # Get all pixels as a list of (R, G, B) tuples
        pixels = list(small_image.getdata())

        # Count occurrences of each color (rounded to nearest 10 for grouping)
        color_counts = {}
        for r, g, b in pixels:
            # Round to nearest 32 to group similar colors together
            key = (r // 32 * 32, g // 32 * 32, b // 32 * 32)
            color_counts[key] = color_counts.get(key, 0) + 1

        # Sort by frequency and get top 5 colors
        sorted_colors = sorted(color_counts.items(), key=lambda x: x[1], reverse=True)
        top_colors = []
        for (r, g, b), count in sorted_colors[:5]:
            hex_color = f"#{r:02x}{g:02x}{b:02x}"
            top_colors.append({
                "hex": hex_color,
                "rgb": {"r": r, "g": g, "b": b},
                "percentage": round(count / len(pixels) * 100, 1)
            })

        # Determine if image is mostly grayscale
        # In grayscale images, R, G, and B values are very close to each other
        grayscale_pixels = sum(1 for r, g, b in pixels if abs(r - g) < 30 and abs(g - b) < 30)
        is_grayscale = grayscale_pixels / len(pixels) > 0.9

        return {
            "dominantColors": top_colors,
            "isGrayscale": is_grayscale,
            "totalPixelsSampled": len(pixels)
        }

    except Exception as e:
        logging.error(f"Color analysis failed: {str(e)}")
        return {
            "dominantColors": [],
            "isGrayscale": False,
            "totalPixelsSampled": 0,
            "error": str(e)
        }

# =============================================================================
# 4. ACTIVITY: Analyze Objects (Mock)
# =============================================================================
# This activity simulates object detection.
# In a production app, you would call Azure Computer Vision API here.
#
# Why mock? Calling an external API requires API keys and costs money.
# The mock lets you focus on learning the Durable Functions pattern.
# The "Stretch Goal" section at the end shows how to swap in the real API.
@myApp.activity_trigger(input_name="inputData")
def analyze_objects(inputData: dict):
    logging.info("Analyzing objects...")

    try:
        image_bytes = bytes(inputData["blob_bytes"])
        image = Image.open(io.BytesIO(image_bytes))
        width, height = image.size

        # Mock object detection based on image characteristics
        # A real API would return actual detected objects
        mock_objects = []

        # Simulate detection based on image dimensions and properties
        if width > height:
            mock_objects.append({"name": "landscape", "confidence": 0.85})
        elif height > width:
            mock_objects.append({"name": "portrait", "confidence": 0.82})
        else:
            mock_objects.append({"name": "square composition", "confidence": 0.90})

        # Add some generic objects based on image size
        if width * height > 1000000:  # > 1 megapixel
            mock_objects.append({"name": "high-resolution scene", "confidence": 0.78})

        mock_objects.append({"name": "digital image", "confidence": 0.99})

        return {
            "objects": mock_objects,
            "objectCount": len(mock_objects),
            "note": "Mock analysis - replace with Azure Computer Vision for real detection"
        }

    except Exception as e:
        logging.error(f"Object analysis failed: {str(e)}")
        return {
            "objects": [],
            "objectCount": 0,
            "error": str(e)
        }

# =============================================================================
# 5. ACTIVITY: Analyze Text / OCR (Mock)
# =============================================================================
# This activity simulates Optical Character Recognition (OCR).
# In a production app, you would call Azure Computer Vision Read API.
@myApp.activity_trigger(input_name="inputData")
def analyze_text(inputData: dict):
    logging.info("Analyzing text (OCR)...")

    try:
        image_bytes = bytes(inputData["blob_bytes"])
        image = Image.open(io.BytesIO(image_bytes))

        # Mock OCR analysis
        # Real OCR would scan the image for any visible text
        # Here we simulate by checking image properties
        width, height = image.size

        return {
            "hasText": False,
            "extractedText": "",
            "confidence": 0.0,
            "language": "unknown",
            "note": "Mock OCR - replace with Azure Computer Vision Read API for real text extraction"
        }

    except Exception as e:
        logging.error(f"Text analysis failed: {str(e)}")
        return {
            "hasText": False,
            "extractedText": "",
            "confidence": 0.0,
            "error": str(e)
        }

# =============================================================================
# 6. ACTIVITY: Analyze Metadata (Real Analysis)
# =============================================================================
# Unlike the mock activities above, this one performs REAL analysis.
# Pillow can extract actual image metadata: dimensions, format, color mode,
# and EXIF data (camera info, GPS coordinates, etc.)
@myApp.activity_trigger(input_name="inputData")
def analyze_metadata(inputData: dict):
    logging.info("Analyzing metadata...")

    try:
        image_bytes = bytes(inputData["blob_bytes"])
        blob_size_kb = inputData["blob_size_kb"]
        image = Image.open(io.BytesIO(image_bytes))

        width, height = image.size
        total_pixels = width * height

        # Try to extract EXIF data (camera info, date taken, etc.)
        exif_data = {}
        try:
            raw_exif = image._getexif()
            if raw_exif:
                # Map EXIF tag numbers to human-readable names
                from PIL.ExifTags import TAGS
                for tag_id, value in raw_exif.items():
                    tag_name = TAGS.get(tag_id, tag_id)
                    # Only include string/number values (skip binary data)
                    if isinstance(value, (str, int, float)):
                        exif_data[str(tag_name)] = str(value)
        except (AttributeError, Exception):
            # Not all image formats support EXIF
            pass

        return {
            "width": width,
            "height": height,
            "format": image.format or "Unknown",
            "mode": image.mode,
            "totalPixels": total_pixels,
            "megapixels": round(total_pixels / 1000000, 2),
            "sizeKB": blob_size_kb,
            "aspectRatio": f"{width}:{height}",
            "hasExifData": len(exif_data) > 0,
            "exifData": exif_data
        }

    except Exception as e:
        logging.error(f"Metadata analysis failed: {str(e)}")
        return {
            "width": 0,
            "height": 0,
            "format": "Unknown",
            "error": str(e)
        }

# =============================================================================
# 7. ACTIVITY: Generate Report
# =============================================================================
# This activity takes the results from all 4 analyses and combines them
# into a single unified report. This is the "reduce" step after the fan-in.
@myApp.activity_trigger(input_name="reportData")
def generate_report(reportData: dict):
    logging.info("Generating combined report...")

    blob_name = reportData["blob_name"]
    # Extract just the filename from the full path (e.g., "images/photo.jpg" -> "photo.jpg")
    filename = blob_name.split("/")[-1] if "/" in blob_name else blob_name

    report = {
        "id": str(uuid.uuid4()),
        "fileName": filename,
        "blobPath": blob_name,
        "analyzedAt": datetime.utcnow().isoformat(),
        "analyses": {
            "colors": reportData["colors"],
            "objects": reportData["objects"],
            "text": reportData["text"],
            "metadata": reportData["metadata"],
        },
        "summary": {
            "imageSize": f"{reportData['metadata'].get('width', 0)}x{reportData['metadata'].get('height', 0)}",
            "format": reportData["metadata"].get("format", "Unknown"),
            "dominantColor": reportData["colors"]["dominantColors"][0]["hex"] if reportData["colors"].get("dominantColors") else "N/A",
            "objectsDetected": reportData["objects"].get("objectCount", 0),
            "hasText": reportData["text"].get("hasText", False),
            "isGrayscale": reportData["colors"].get("isGrayscale", False),
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
#   - PartitionKey: Groups related entities (we use "ImageAnalysis")
#   - RowKey: Unique identifier within the partition (we use the report ID)
@myApp.activity_trigger(input_name="report")
def store_results(report: dict):
    logging.info(f"Storing results for {report['fileName']}...")

    try:
        table_client = get_table_client()

        # Table Storage entities are flat key-value pairs.
        # Complex nested data (like our analyses) must be serialized as JSON strings.
        entity = {
            "PartitionKey": "ImageAnalysis",
            "RowKey": report["id"],
            "FileName": report["fileName"],
            "BlobPath": report["blobPath"],
            "AnalyzedAt": report["analyzedAt"],
            # Store complex data as JSON strings
            "Summary": json.dumps(report["summary"]),
            "ColorAnalysis": json.dumps(report["analyses"]["colors"]),
            "ObjectAnalysis": json.dumps(report["analyses"]["objects"]),
            "TextAnalysis": json.dumps(report["analyses"]["text"]),
            "MetadataAnalysis": json.dumps(report["analyses"]["metadata"]),
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
                    partition_key="ImageAnalysis",
                    row_key=result_id
                )

                # Parse JSON strings back into objects
                result = {
                    "id": entity["RowKey"],
                    "fileName": entity["FileName"],
                    "blobPath": entity["BlobPath"],
                    "analyzedAt": entity["AnalyzedAt"],
                    "summary": json.loads(entity["Summary"]),
                    "analyses": {
                        "colors": json.loads(entity["ColorAnalysis"]),
                        "objects": json.loads(entity["ObjectAnalysis"]),
                        "text": json.loads(entity["TextAnalysis"]),
                        "metadata": json.loads(entity["MetadataAnalysis"]),
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
                query_filter="PartitionKey eq 'ImageAnalysis'"
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