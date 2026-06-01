import pandas as pd
import requests
import time
from tqdm import tqdm

OLLAMA_URL = "http://localhost:11434/api/generate"
MODEL = "llama3.2:3b"


def call_llama(prompt, max_tokens=120, temperature=0.7):
    """Call Ollama and return clean single-sentence text, or None on failure."""
    payload = {
        "model": MODEL,
        "prompt": prompt,
        "stream": False,
        "options": {
            "temperature": temperature,
            "num_predict": max_tokens,
            "stop": ["\n", "Note:", "Recommendation:", "Comment:", "Description:"]
        }
    }
    try:
        response = requests.post(OLLAMA_URL, json=payload, timeout=60)
        response.raise_for_status()
        raw = response.json()["response"].strip()

        for prefix in [
            "Recommendation:", "Note:", "Comment:", "Description:",
            "Here is", "Here's", "Based on", "I recommend", "I suggest",
            "As a", "The ", '"', "'"
        ]:
            if raw.lower().startswith(prefix.lower()):
                raw = raw[len(prefix):].strip()

        # Take only the first sentence
        for sep in [".", "!", "?"]:
            if sep in raw:
                raw = raw[:raw.index(sep) + 1].strip()
                break

        return raw if raw else None

    except requests.exceptions.ConnectionError:
        print("  ✗ Cannot connect to Ollama. Run: ollama serve")
        return None
    except requests.exceptions.Timeout:
        print("  ✗ Ollama timed out.")
        return None
    except Exception as e:
        print(f"  ✗ Error: {e}")
        return None


def is_placeholder(value):
    return isinstance(value, str) and value.strip().startswith("LLM:")


# -------------------------------------------------------------------
# 1. survey_findings.recommendations
# -------------------------------------------------------------------
def enrich_survey_recommendations(df):
    mask = df["recommendations"].apply(is_placeholder)
    print(f"  Found {mask.sum()} placeholders in 'recommendations'.")
    for idx, row in tqdm(df[mask].iterrows(), total=mask.sum(), desc="  Survey recs"):
        prompt = (
            f"Roof: {row['roof_condition']}. "
            f"Electrical: {row['electrical_rating']}. "
            f"Housekeeping: {row['housekeeping_score']}/100. "
            f"Grade: {row['overall_grade']}. "
            f"A commercial property surveyor writes one actionable recommendation sentence:"
        )
        result = call_llama(prompt, max_tokens=80, temperature=0.6)
        if result:
            df.at[idx, "recommendations"] = result
        time.sleep(0.05)
    return df


# -------------------------------------------------------------------
# 2. claims.description
# -------------------------------------------------------------------
def enrich_claims_description(df):
    mask = df["description"].apply(is_placeholder)
    print(f"  Found {mask.sum()} placeholders in 'description'.")
    for idx, row in tqdm(df[mask].iterrows(), total=mask.sum(), desc="  Claims desc"):
        prompt = (
            f"Write one realistic sentence describing a commercial property insurance claim. "
            f"Peril: {row['peril']}. "
            f"Amount: £{row['gross_incurred']:,.0f}. "
            f"Status: {row['claim_status']}. "
            f"Claim description:"
        )
        result = call_llama(prompt, max_tokens=80, temperature=0.8)
        if result:
            df.at[idx, "description"] = result
        time.sleep(0.05)
    return df


# -------------------------------------------------------------------
# 3. requirements.notes
# -------------------------------------------------------------------
def enrich_requirements_notes(df):
    mask = df["notes"].apply(lambda v: is_placeholder(v) if pd.notna(v) else False)
    print(f"  Found {mask.sum()} placeholders in 'notes'.")
    for idx, row in tqdm(df[mask].iterrows(), total=mask.sum(), desc="  Req notes"):
        prompt = (
            f"An underwriter writes one short note about an outstanding requirement. "
            f"Type: {row['requirement_type']}. "
            f"Due: {row['due_date']}. "
            f"Status: {row['status']}. "
            f"Note:"
        )
        result = call_llama(prompt, max_tokens=60, temperature=0.6)
        if result:
            df.at[idx, "notes"] = result
        time.sleep(0.05)
    return df


# -------------------------------------------------------------------
# 4. historical_decisions.underwriter_comments
# -------------------------------------------------------------------
def enrich_underwriter_comments(df):
    mask = df["underwriter_comments"].apply(is_placeholder)
    print(f"  Found {mask.sum()} placeholders in 'underwriter_comments'.")
    for idx, row in tqdm(df[mask].iterrows(), total=mask.sum(), desc="  UW comments"):
        prompt = (
            f"Decision: {row['decision']}. "
            f"Flood zone: {row['flood_zone']}. "
            f"Claim frequency score: {row['claim_frequency_score']}/100. "
            f"Loading: {row['loading_pct']}%. "
            f"An underwriter writes one sentence explaining this decision:"
        )
        result = call_llama(prompt, max_tokens=80, temperature=0.7)
        if result:
            df.at[idx, "underwriter_comments"] = result
        time.sleep(0.05)
    return df


# -------------------------------------------------------------------
# Sanity check
# -------------------------------------------------------------------
def sanity_check():
    print("─" * 50)
    print("Sanity check: pinging Ollama...")
    result = call_llama("Complete this sentence with exactly three words: The sky is", max_tokens=10, temperature=0.0)
    if result is None:
        print("✗ Ollama not responding. Run: ollama serve && ollama pull llama3.2:3b")
        return False
    print(f"✓ Ollama responded: '{result}'")
    print("─" * 50)
    return True


# -------------------------------------------------------------------
# MAIN
# -------------------------------------------------------------------
def main():
    data_dir = '/home/lang-chain/Documents/underwriting_assistant/raw_data'

    if not sanity_check():
        return

    steps = [
        ("survey_findings",     "survey_findings_enriched",     enrich_survey_recommendations),
        ("claims",              "claims_enriched",              enrich_claims_description),
        ("requirements",        "requirements_enriched",        enrich_requirements_notes),
        ("historical_decisions","historical_decisions_enriched", enrich_underwriter_comments),
    ]

    for i, (src, dst, fn) in enumerate(steps, 1):
        print(f"\n[{i}/{len(steps)}] {src}.csv")
        df = pd.read_csv(f"{data_dir}/{src}.csv")
        df = fn(df)
        out_path = f"{data_dir}/{dst}.csv"
        df.to_csv(out_path, index=False)
        print(f"  ✓ Saved {dst}.csv")

    print("\n✓ Done — all fields enriched.")


if __name__ == "__main__":
    main()