import asyncio
import streamlit as st
from mcp import ClientSession, StdioServerParameters

# ----------------------------------------------------------------------
# Helper to run async calls from Streamlit (which is sync)
def run_async(coro):
    """Run an async coroutine and return result."""
    try:
        loop = asyncio.get_running_loop()
        # If already in a running loop (e.g., Streamlit's own), create a new task
        # but for simplicity we use asyncio.run() for top-level.
        # Streamlit runs on its own event loop, so we need to ensure we use that.
        # The simplest: use asyncio.run_coroutine_threadsafe with a new loop?
        # Actually, Streamlit's asyncio support: we can use st.runtime.scriptrunner.add_async_task.
    except RuntimeError:
        # No running loop – safe to use asyncio.run()
        return asyncio.run(coro)
    # If we are inside a running loop (Streamlit's own), we need to manage.
    # Workaround: use nest_asyncio or just create a new event loop in a thread.
    # Simpler: use asyncio.new_event_loop + run_until_complete.
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    return loop.run_until_complete(coro)

# ----------------------------------------------------------------------
# Initialize the MCP client session (cached so we don't restart server every time)
@st.cache_resource
def get_mcp_session():
    """Start the MCP server subprocess and return a ClientSession."""
    server_params = StdioServerParameters(
        command="python",
        args=["run_mcp.py"]   # relative to project root
    )
    # The context manager will start the server and connect
    # We want to keep the session alive for the whole Streamlit app.
    # We'll create it once and store in st.session_state.
    session = ClientSession(server_params)
    # Initialize the connection (this is async)
    run_async(session.initialize())
    return session

# ----------------------------------------------------------------------
# Streamlit UI
st.set_page_config(page_title="Underwriting Extractor Client", layout="wide")
st.title("📄 Underwriting Submission Extractor (MCP Client)")

# Get or create the MCP session
if "mcp_session" not in st.session_state:
    try:
        st.session_state.mcp_session = get_mcp_session()
        st.success("✅ Connected to MCP server")
    except Exception as e:
        st.error(f"Failed to connect: {e}")
        st.stop()

session = st.session_state.mcp_session

# ------------------------------------------------------------
# Tabs: Extract and Retrieve
tab1, tab2 = st.tabs(["🔍 Extract New Submission", "📂 Retrieve Existing Artifact"])

with tab1:
    st.subheader("Submit a raw document text")
    raw_text = st.text_area("Document content (plain text extraction from PDF/OCR)", height=300,
                            placeholder="Paste the full text extracted from the submission document...")
    correlation_id = st.text_input("Correlation ID (optional)", value="", help="Unique ID for idempotency; auto-generated if empty.")
    
    if st.button("🚀 Extract Submission", type="primary"):
        if not raw_text.strip():
            st.warning("Please provide document text.")
        else:
            with st.spinner("Calling extractor agent..."):
                try:
                    args = {"raw_text": raw_text}
                    if correlation_id.strip():
                        args["correlation_id"] = correlation_id.strip()
                    result = run_async(session.call_tool("extract_submission", arguments=args))
                    # result is a list of TextContent objects
                    response_text = result.content[0].text
                    # Display JSON nicely
                    st.subheader("Extracted Artifact")
                    st.json(response_text)  # assumes JSON string
                except Exception as e:
                    st.error(f"Extraction failed: {e}")

with tab2:
    st.subheader("Retrieve an already extracted artifact")
    corr_id_retrieve = st.text_input("Correlation ID", key="retrieve_id", help="The ID you used or was returned during extraction.")
    if st.button("📥 Get Artifact"):
        if not corr_id_retrieve.strip():
            st.warning("Please enter a correlation ID.")
        else:
            with st.spinner("Fetching from artifact store..."):
                try:
                    result = run_async(session.call_tool("get_artifact", arguments={"correlation_id": corr_id_retrieve.strip()}))
                    response_text = result.content[0].text
                    if response_text.startswith("Artifact not found"):
                        st.error(response_text)
                    else:
                        st.subheader("Artifact JSON")
                        st.json(response_text)
                except Exception as e:
                    st.error(f"Retrieval failed: {e}")

if st.sidebar.checkbox("Show available tools"):
    try:
        tools = run_async(session.list_tools())
        st.sidebar.subheader("Tools")
        for tool in tools.tools:
            st.sidebar.markdown(f"**{tool.name}** – {tool.description}")
    except Exception as e:
        st.sidebar.error(f"Error listing tools: {e}")