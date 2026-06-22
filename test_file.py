# test_webp.py
import sys
sys.path.insert(0, '.')

import asyncio
import json
from src.underwriter_parser.mcp_server import submit_document, get_submission_status

async def test_webp():
    # Read your WebP file
    webp_path = "file.webp"  
    
    print(f"📤 Reading WebP file: {webp_path}")
    with open(webp_path, "rb") as f:
        webp_content = f.read()
    
    print(f"📊 File size: {len(webp_content)} bytes")
    
    # Submit - the system will convert to PDF
    print("\n📤 Submitting document...")
    result_str = await submit_document(webp_content, "document.webp")
    result = json.loads(result_str)
    print(f"✅ Submitted: {json.dumps(result, indent=2)}")
    
    correlation_id = result['correlation_id']
    print(f"\n📌 Correlation ID: {correlation_id}")
    
    print("\n⏳ Waiting for processing (30 seconds)...")
    await asyncio.sleep(30)
    
    print("\n📊 Checking status...")
    status_str = await get_submission_status(correlation_id)
    try:
        status = json.loads(status_str)
        print(f"Status: {json.dumps(status, indent=2)}")
    except:
        print(f"Status raw: {status_str}")

if __name__ == "__main__":
    asyncio.run(test_webp())