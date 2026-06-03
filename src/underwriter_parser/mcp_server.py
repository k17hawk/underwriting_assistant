#!/usr/bin/env python3
import os
import json
import asyncio
from dotenv import load_dotenv
from fastmcp import FastMCP
from pydantic import BaseModel, Field

from .extractor import ExtractorHandoff
from .storage import MongoArtifactStore

load_dotenv()

# Initialize services
extractor = ExtractorHandoff()
artifact_store = MongoArtifactStore()

# Create FastMCP server
mcp = FastMCP("extractor-mcp-server")

# Define input schemas as Pydantic models (optional but cleaner)
class ExtractSubmissionInput(BaseModel):
    raw_text: str = Field(..., description="Plain text content of the submission document")
    correlation_id: str = Field(None, description="Optional unique ID for idempotency")

class GetArtifactInput(BaseModel):
    correlation_id: str = Field(..., description="Correlation ID of the artifact to retrieve")

@mcp.tool()
async def extract_submission(raw_text: str, correlation_id: str = None) -> str:
    """
    Extract structured underwriting submission from raw document text.
    Returns the validated artifact as a JSON string.
    """
    try:
        artifact = extractor.extract(raw_text, correlation_id=correlation_id)
        return artifact.model_dump_json(indent=2)
    except Exception as e:
        return f"Error: {str(e)}"

@mcp.tool()
async def get_artifact(correlation_id: str) -> str:
    """
    Retrieve a previously extracted artifact by correlation_id.
    """
    try:
        artifact_dict = artifact_store.get_artifact(correlation_id)
        if artifact_dict:
            return json.dumps(artifact_dict, indent=2)
        else:
            return f"Artifact not found: {correlation_id}"
    except Exception as e:
        return f"Error: {str(e)}"

def main():
    """Entry point – runs the FastMCP server over stdio."""
    mcp.run(transport="stdio")

if __name__ == "__main__":
    main()