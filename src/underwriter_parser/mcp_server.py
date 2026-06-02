import os
import asyncio
from dotenv import load_dotenv
from mcp.server import Server, NotificationOptions
from mcp.server.models import InitializationOptions
import mcp.server.stdio
import mcp.types as types

from .extractor import ExtractorHandoff
from .storage import MongoArtifactStore

load_dotenv()

extractor = ExtractorHandoff()
artifact_store = MongoArtifactStore()

server = Server("extractor-mcp-server")

@server.list_tools()
async def handle_list_tools() -> list[types.Tool]:
    return [
        types.Tool(
            name="extract_submission",
            description="Extract structured underwriting submission from raw document text (or file content). Returns validated artifact.",
            inputSchema={
                "type": "object",
                "properties": {
                    "raw_text": {"type": "string", "description": "Plain text content of the submission document"},
                    "correlation_id": {"type": "string", "description": "Optional unique ID for idempotency"}
                },
                "required": ["raw_text"]
            }
        ),
        types.Tool(
            name="get_artifact",
            description="Retrieve a previously extracted artifact by correlation_id",
            inputSchema={
                "type": "object",
                "properties": {"correlation_id": {"type": "string"}},
                "required": ["correlation_id"]
            }
        )
    ]

@server.call_tool()
async def handle_call_tool(name: str, arguments: dict) -> list[types.TextContent]:
    if name == "extract_submission":
        raw_text = arguments["raw_text"]
        corr_id = arguments.get("correlation_id")
        try:
            artifact = extractor.extract(raw_text, correlation_id=corr_id)
            return [types.TextContent(type="text", text=artifact.model_dump_json(indent=2))]
        except Exception as e:
            return [types.TextContent(type="text", text=f"Error: {str(e)}")]
    elif name == "get_artifact":
        corr_id = arguments["correlation_id"]
        try:
            artifact_dict = artifact_store.get_artifact(corr_id)
            if artifact_dict:
                return [types.TextContent(type="text", text=json.dumps(artifact_dict, indent=2))]
            else:
                return [types.TextContent(type="text", text=f"Artifact not found: {corr_id}")]
        except Exception as e:
            return [types.TextContent(type="text", text=f"Error: {str(e)}")]
    else:
        raise ValueError(f"Unknown tool: {name}")

async def main():
    async with mcp.server.stdio.stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            InitializationOptions(
                server_name="extractor-mcp-server",
                server_version="1.0.0"
            ),
            notification_options=NotificationOptions(),
        )

if __name__ == "__main__":
    import json
    asyncio.run(main())