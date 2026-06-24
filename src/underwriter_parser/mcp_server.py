# mcp_server.py
import os
import json
import asyncio
from datetime import datetime
from typing import Optional, Dict, Any
from dotenv import load_dotenv
from fastmcp import FastMCP
from pydantic import BaseModel, Field

from  src.underwriter_parser.orchestrator import Orchestrator
from src.underwriter_parser.mongodb_storage import MongoDBSubmissionStore
from src.underwriter_parser.parser_worker import ParserWorker
from src.underwriter_parser.streamer import KafkaHandoff

load_dotenv()

# Initialize services
orchestrator = Orchestrator()
db_store = MongoDBSubmissionStore()


mcp = FastMCP("extractor-mcp-server")

class SubmitDocumentInput(BaseModel):
    file_content: bytes = Field(..., description="PDF file content as bytes")
    filename: str = Field("document.pdf", description="Original filename")
    stream_updates: bool = Field(True, description="Stream progress updates via SSE")

class GetStatusInput(BaseModel):
    correlation_id: str = Field(..., description="Correlation ID of the submission")

class GetArtifactInput(BaseModel):
    correlation_id: str = Field(..., description="Correlation ID of the artifact to retrieve")

@mcp.tool()
async def submit_document(file_content: bytes, filename: str = "document.pdf", stream_updates: bool = True) -> str:
    """
    Submit a PDF document for parsing with SSE streaming support.
    Returns the correlation_id and initial status.
    """
    print(f"🔍 submit_document called with: filename={filename}, size={len(file_content)} bytes")
    
    try:
        # Get the current SSE session if available
        sse_session = getattr(mcp, '_current_session', None)
        
        if stream_updates and sse_session:
            await sse_session.send_event({
                "type": "progress",
                "stage": "submission",
                "message": f"Processing {filename}...",
                "timestamp": datetime.now().isoformat()
            })
        
        print("📤 Calling orchestrator.process_submission...")
        result = orchestrator.process_submission(file_content, filename)
        print(f"✅ Orchestrator result: {result}")
        
        if stream_updates and sse_session:
            await sse_session.send_event({
                "type": "submitted",
                "correlation_id": result["correlation_id"],
                "status": result["status"],
                "message": "Document submitted successfully",
                "timestamp": datetime.now().isoformat()
            })
        
        json_result = json.dumps(result, indent=2, default=str)
        print(f"📤 Returning: {json_result[:200]}...")
        return json_result
        
    except Exception as e:
        print(f"❌ Error in submit_document: {e}")
        import traceback
        traceback.print_exc()
        
        if stream_updates and sse_session:
            await sse_session.send_event({
                "type": "error",
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            })
        
        error_result = {"error": str(e), "status": "FAILED"}
        return json.dumps(error_result, default=str)

@mcp.tool()
async def get_submission_status(correlation_id: str) -> str:
    """
    Get the status of a submission by correlation_id.
    """
    try:
        record = db_store.get_submission_record(correlation_id)
        if record:
            record["_id"] = str(record["_id"])
            if "created_at" in record:
                record["created_at"] = record["created_at"].isoformat()
            if "updated_at" in record:
                record["updated_at"] = record["updated_at"].isoformat()
            return json.dumps(record, indent=2, default=str)
        else:
            return f"Submission not found: {correlation_id}"
    except Exception as e:
        return f"Error: {str(e)}"

@mcp.tool()
async def get_artifact(correlation_id: str) -> str:
    """
    Retrieve a previously extracted artifact by correlation_id.
    """
    try:
        artifact_dict = db_store.get_artifact(correlation_id)
        if artifact_dict:
            return json.dumps(artifact_dict, indent=2, default=str)
        else:
            return f"Artifact not found: {correlation_id}"
    except Exception as e:
        return f"Error: {str(e)}"

@mcp.tool()
async def stream_parse_progress(correlation_id: str) -> str:
    """
    Stream parsing progress for a specific submission via SSE.
    """
    try:
        sse_session = getattr(mcp, '_current_session', None)
        
        if not sse_session:
            return "Error: No SSE session available"
        
        record = db_store.get_submission_record(correlation_id)
        if not record:
            await sse_session.send_event({
                "type": "error",
                "error": f"Submission not found: {correlation_id}",
                "timestamp": datetime.now().isoformat()
            })
            return f"Submission not found: {correlation_id}"
        
        await sse_session.send_event({
            "type": "status",
            "correlation_id": correlation_id,
            "status": record.get("status"),
            "message": "Checking submission status...",
            "timestamp": datetime.now().isoformat()
        })
        
        for _ in range(10):
            await asyncio.sleep(2)
            updated_record = db_store.get_submission_record(correlation_id)
            if updated_record:
                await sse_session.send_event({
                    "type": "status_update",
                    "correlation_id": correlation_id,
                    "status": updated_record.get("status"),
                    "message": f"Status: {updated_record.get('status')}",
                    "timestamp": datetime.now().isoformat()
                })
                
                if updated_record.get("status") == "COMPLETED":
                    await sse_session.send_event({
                        "type": "completed",
                        "correlation_id": correlation_id,
                        "parsed_json_path": updated_record.get("parsed_json_path"),
                        "message": "Parsing completed successfully!",
                        "timestamp": datetime.now().isoformat()
                    })
                    break
                elif updated_record.get("status") == "FAILED":
                    await sse_session.send_event({
                        "type": "failed",
                        "correlation_id": correlation_id,
                        "error_type": updated_record.get("error_type"),
                        "error_message": updated_record.get("error_message"),
                        "timestamp": datetime.now().isoformat()
                    })
                    break
        
        return "Streaming progress updates..."
        
    except Exception as e:
        return f"Error: {str(e)}"

@mcp.resource("submission://{correlation_id}")
async def get_submission_resource(correlation_id: str) -> str:
    """
    Get submission data as a resource.
    """
    record = db_store.get_submission_record(correlation_id)
    if record:
        return json.dumps(record, indent=2, default=str)
    return f"Submission not found: {correlation_id}"

def main():
    """Entry point – runs the FastMCP server with SSE transport."""
    host = os.getenv("MCP_SERVER_HOST", "0.0.0.0")
    port = int(os.getenv("MCP_SERVER_PORT", 8000))
    
    print("🚀 Starting MCP Server with SSE Transport")
    print(f"📡 SSE endpoint: http://{host}:{port}/sse")
    print(f"📡 Tools available:")
    print("   - submit_document")
    print("   - get_submission_status")
    print("   - get_artifact")
    print("   - stream_parse_progress")
    print("\nPress Ctrl+C to stop")
    
    # ✅ Pass host and port to run_http_async
    import asyncio
    asyncio.run(mcp.run_http_async(host=host, port=port))

if __name__ == "__main__":
    main()