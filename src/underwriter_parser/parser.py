import os
import json
import requests
from typing import Dict, Any

PROMPTS_DIR = "src/underwriter_parser/prompts"

def load_system_prompt(version: str = None) -> str:
    if version is None:
        version_file = os.path.join(PROMPTS_DIR, "version.txt")
        if os.path.exists(version_file):
            with open(version_file, "r") as f:
                version = f.read().strip()
        else:
            version = "v3"
    prompt_file = os.path.join(PROMPTS_DIR, f"system_prompt_{version}.txt")
    if not os.path.exists(prompt_file):
        raise FileNotFoundError(f"Prompt file not found: {prompt_file}")
    with open(prompt_file, "r") as f:
        return f.read()

class LLMParser:
    def __init__(self, api_key: str = None, model: str = None, endpoint: str = None, prompt_version: str = None):
        self.api_key = api_key or os.getenv("DEEPSEEK_API_KEY")
        self.model = model or os.getenv("DEEPSEEK_MODEL", "deepseek-chat")
        self.endpoint = endpoint or os.getenv("DEEPSEEK_ENDPOINT", "https://api.deepseek.com/v1/chat/completions")
        self.system_prompt = load_system_prompt(prompt_version or os.getenv("PROMPT_VERSION"))

    def parse(self, raw_text: str) -> Dict[str, Any]:
        # ✅ Contains "json"
        user_prompt = f"""Document content:
        {raw_text}

        Extract all submission data and return as a json object only. No markdown, no explanation."""

        try:
                response = requests.post(
                    self.endpoint,
                    headers={
                        "Authorization": f"Bearer {self.api_key}",
                        "Content-Type": "application/json"
                    },
                    json={
                        "model": self.model,
                        "messages": [
                            {"role": "system", "content": self.system_prompt},
                            {"role": "user", "content": user_prompt}
                        ],
                        "temperature": 0.0,
                        "max_tokens": 8192,
                        "response_format": {"type": "json_object"}
                    },
                    timeout=30
                )
                response.raise_for_status()
                result = response.json()
                content = result["choices"][0]["message"]["content"].strip()
                return json.loads(content)
        except Exception as e:
            raise RuntimeError(f"DeepSeek extraction failed: {e}")