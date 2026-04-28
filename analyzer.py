import streamlit as st
import os
import glob
import time
import json
import re
import pickle
import hashlib
import shutil
import multiprocessing
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import fitz  # PyMuPDF
import ollama

# ---------- Critical macOS Fork Safety ----------
if __name__ == '__main__':
    try:
        multiprocessing.set_start_method('spawn', force=True)
    except RuntimeError:
        pass
os.environ["OBJC_DISABLE_INITIALIZE_FORK_SAFETY"] = "YES"

# ---------- Page Config ----------
st.set_page_config(page_title="PDF Batch LLM Analyzer", layout="wide")

# ---------- Session State ----------
if "pdf_files" not in st.session_state:
    st.session_state.pdf_files = []
if "results" not in st.session_state:
    st.session_state.results = []
if "processing" not in st.session_state:
    st.session_state.processing = False
if "status_msg" not in st.session_state:
    st.session_state.status_msg = "Ready"
if "source_folder" not in st.session_state:
    st.session_state.source_folder = os.path.expanduser("~/Desktop")
if "resume_file" not in st.session_state:
    st.session_state.resume_file = "analysis_progress.pkl"

# ---------- Cache Directory ----------
CACHE_DIR = "./pdf_cache"
os.makedirs(CACHE_DIR, exist_ok=True)

# ---------- Helper Functions ----------
def extract_pdf_text_fast(pdf_path, max_pages=None, max_chars=20000):
    """Extract text from PDF with optional page/character limits."""
    try:
        doc = fitz.open(pdf_path)
        total_pages = len(doc)
        pages_to_read = doc if max_pages is None else doc[:max_pages]
        text_parts = [page.get_text() for page in pages_to_read if page.get_text()]
        doc.close()
        text = "\n".join(text_parts)
        if max_chars and len(text) > max_chars:
            text = text[:max_chars] + "\n...[truncated]..."
        return text, total_pages
    except Exception as e:
        st.error(f"Error reading {pdf_path}: {e}")
        return None, 0

def get_cached_text(pdf_path, max_pages, max_chars):
    """Cache extracted text to avoid re‑processing."""
    mtime = os.path.getmtime(pdf_path)
    cache_key = hashlib.md5(f"{pdf_path}_{mtime}_{max_pages}_{max_chars}".encode()).hexdigest()
    cache_file = os.path.join(CACHE_DIR, cache_key)
    if os.path.exists(cache_file):
        try:
            with open(cache_file, "rb") as f:
                return pickle.load(f)
        except:
            pass
    text, pages = extract_pdf_text_fast(pdf_path, max_pages, max_chars)
    if text is not None:
        with open(cache_file, "wb") as f:
            pickle.dump((text, pages), f)
    return text, pages

def get_prompt_text(prompt_mode, sum_length="standard", template="exec", custom_query=""):
    """Build the analysis prompt based on user selection."""
    if prompt_mode == "simple":
        if sum_length == "brief":
            return "Summarize in 1-2 sentences."
        elif sum_length == "detailed":
            return "Provide a detailed 1-paragraph summary."
        else:
            return "Summarize in 3-5 sentences."
    elif prompt_mode == "template":
        templates = {
            "exec": "Provide an executive summary (Purpose, Key Findings, Implications).",
            "findings": "List the key findings with supporting evidence.",
            "methods": "Describe the methodology and data collection techniques.",
            "questions": "What research questions does this address?",
            "limitations": "What limitations and future work are mentioned?"
        }
        return templates.get(template, "Summarize.")
    else:  # custom
        return custom_query.strip() if custom_query.strip() else "Summarize."

def call_ollama(prompt, model="llama3.2", max_retries=3):
    """Call Ollama with retry logic."""
    for attempt in range(max_retries):
        try:
            response = ollama.chat(model=model, messages=[{"role": "user", "content": prompt}])
            return response["message"]["content"]
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
            else:
                return f"Error: {str(e)}"

def parse_json_output(raw_response):
    """Extract and repair JSON from LLM output."""
    # Remove markdown code fences
    cleaned = re.sub(r'```json\s*|\s*```', '', raw_response)
    cleaned = re.sub(r'```\s*', '', cleaned)
    # Find first { and last }
    start = cleaned.find('{')
    end = cleaned.rfind('}')
    if start == -1 or end == -1:
        return None
    json_str = cleaned[start:end+1]
    # Remove trailing commas
    json_str = re.sub(r',\s*}', '}', json_str)
    json_str = re.sub(r',\s*]', ']', json_str)
    try:
        return json.loads(json_str)
    except:
        # Try to fix unescaped quotes inside values
        def fix_quotes(match):
            inside = match.group(1)
            inside = re.sub(r'(?<!\\)"', "'", inside)
            return f'"{inside}"'
        json_str = re.sub(r':\s*"([^"]*)"', fix_quotes, json_str)
        try:
            return json.loads(json_str)
        except:
            return None

def process_one_pdf_structured(pdf_path, prompt_text, output_schema, model_name, max_pages, max_chars):
    """Process PDF and return structured JSON according to schema."""
    filename = os.path.basename(pdf_path)
    content, num_pages = get_cached_text(pdf_path, max_pages, max_chars)
    if content is None:
        return {"file": filename, "pages": num_pages, "status": "ERROR", "data": None, "raw": None}

    full_prompt = f"""{prompt_text}

OUTPUT SCHEMA (return ONLY a JSON object with these keys):
{output_schema}

IMPORTANT: Respond with ONLY valid JSON. No extra text, no markdown.
Example: {{"key1": "value", "key2": 123}}

CONTENT:
{content}"""

    raw = call_ollama(full_prompt, model=model_name)
    data = parse_json_output(raw)
    if data:
        return {"file": filename, "pages": num_pages, "status": "OK", "data": data, "raw": None}
    else:
        return {"file": filename, "pages": num_pages, "status": "PARSE_ERROR", "data": None, "raw": raw}

def process_one_pdf_unstructured(pdf_path, prompt_text, model_name, max_pages, max_chars):
    """Simple free‑text analysis."""
    filename = os.path.basename(pdf_path)
    content, num_pages = get_cached_text(pdf_path, max_pages, max_chars)
    if content is None:
        return {"file": filename, "pages": num_pages, "response": "Failed to read PDF", "status": "ERROR"}
    full_prompt = f"{prompt_text}\n\n--- CONTENT ---\n{content}"
    response = call_ollama(full_prompt, model=model_name)
    return {"file": filename, "pages": num_pages, "response": response, "status": "OK" if not response.startswith("Error") else "ERROR"}

def save_progress(results, filepath):
    """Save partial results to resume later."""
    with open(filepath, "wb") as f:
        pickle.dump(results, f)

def load_progress(filepath):
    """Load previously saved results."""
    if os.path.exists(filepath):
        try:
            with open(filepath, "rb") as f:
                return pickle.load(f)
        except:
            pass
    return []

def get_already_processed(files, results):
    """Return set of filenames already in results."""
    processed = {r["file"] for r in results if r["status"] in ("OK", "ERROR", "PARSE_ERROR")}
    return processed

# ---------- Main UI ----------
st.title("📄 PDF Batch LLM Analyzer (Local Ollama)")

col_side, col_main = st.columns([1, 2])

with col_side:
    st.header("⚙️ Configuration")
    model_name = st.selectbox("Ollama Model", ["llama3.2:3b", "phi4", "mistral", "llama3.2", "gemma3:12b"])
    ollama_url = st.text_input("Ollama URL", "http://localhost:11434")
    os.environ["OLLAMA_HOST"] = ollama_url

    st.divider()
    st.header("⚡ Performance & Caching")
    max_workers = st.slider("Parallel PDFs", 1, 8, 4)
    max_pages = st.number_input("Max pages per PDF (0 = all)", 0, 500, 0)
    max_chars = st.number_input("Max characters per PDF", 1000, 100000, 20000)
    use_cache = st.checkbox("Use caching", True)
    if not use_cache and st.button("Clear Cache"):
        shutil.rmtree(CACHE_DIR, ignore_errors=True)
        os.makedirs(CACHE_DIR, exist_ok=True)
        st.success("Cache cleared")

    st.divider()
    st.header("📂 Data Source")
    source_folder = st.text_input("Folder with PDFs", value=st.session_state.source_folder)
    if source_folder != st.session_state.source_folder:
        st.session_state.source_folder = source_folder
        st.session_state.pdf_files = []
        st.session_state.results = []
    if os.path.isdir(st.session_state.source_folder):
        if st.button("🔍 Scan for PDFs", use_container_width=True):
            pdfs = glob.glob(os.path.join(st.session_state.source_folder, "*.pdf")) + \
                   glob.glob(os.path.join(st.session_state.source_folder, "*.PDF"))
            st.session_state.pdf_files = sorted(pdfs)
            st.session_state.results = []  # reset results on new folder
            st.success(f"Found {len(pdfs)} PDF files")
    else:
        st.error("Folder not found")

    st.divider()
    st.header("✏️ Analysis Prompt")
    prompt_mode = st.radio("Mode", ["Quick Summary", "Template", "Custom Query"], horizontal=True)

    if prompt_mode == "Quick Summary":
        sum_length = st.selectbox("Length", ["Brief", "Standard", "Detailed"])
        prompt_text = get_prompt_text("simple", sum_length.lower())
    elif prompt_mode == "Template":
        template_name = st.selectbox("Template", ["Executive Summary", "Key Findings", "Methods", "Research Questions", "Limitations"])
        template_map = {
            "Executive Summary": "exec",
            "Key Findings": "findings",
            "Methods": "methods",
            "Research Questions": "questions",
            "Limitations": "limitations"
        }
        prompt_text = get_prompt_text("template", template=template_map[template_name])
    else:
        custom_query = st.text_area("Your Question", height=100, placeholder="e.g., What statistical methods were used?")
        prompt_text = get_prompt_text("custom", custom_query=custom_query)

    st.divider()
    st.header("📊 Output Format")
    structured = st.checkbox("Enable structured output (JSON / Excel)")
    output_schema = ""
    if structured:
        output_schema = st.text_area(
            "JSON schema (one key per line or comma‑separated)",
            placeholder="title\nauthors\nmain_finding\nsupports_hypothesis (YES/NO)",
            height=120
        )
        # Convert simple list to a JSON schema string for the prompt
        schema_keys = [k.strip() for k in re.split(r'[,\n]', output_schema) if k.strip()]
        if schema_keys:
            output_schema = ", ".join(schema_keys)
        else:
            output_schema = "key1, key2, key3"

    st.divider()
    if st.button("🚀 Start Analysis", type="primary", use_container_width=True):
        if not st.session_state.pdf_files:
            st.error("No PDFs found. Scan a folder first.")
        else:
            st.session_state.processing = True
            st.session_state.status_msg = "Initializing..."
            st.rerun()

with col_main:
    st.subheader("📄 Available PDFs")
    if st.session_state.pdf_files:
        for f in st.session_state.pdf_files:
            st.write(f"• {os.path.basename(f)}")
    else:
        st.info("No PDFs loaded. Scan a folder on the left.")

    st.subheader("📋 Analysis Results")

    # Resume / load previous results
    if not st.session_state.processing and st.session_state.pdf_files and st.button("Load previous results (resume)"):
        loaded = load_progress(st.session_state.resume_file)
        if loaded:
            st.session_state.results = loaded
            st.success(f"Loaded {len(loaded)} previous results")
        else:
            st.info("No saved progress found")

    if st.session_state.processing:
        total = len(st.session_state.pdf_files)
        # Load previously processed files to avoid re‑doing them
        existing_results = load_progress(st.session_state.resume_file) if os.path.exists(st.session_state.resume_file) else []
        processed_files = get_already_processed(st.session_state.pdf_files, existing_results)
        remaining_files = [p for p in st.session_state.pdf_files if os.path.basename(p) not in processed_files]
        total_remaining = len(remaining_files)
        if total_remaining == 0 and total > 0:
            st.session_state.results = existing_results
            st.session_state.processing = False
            st.success("All files already processed. Loaded results.")
            st.rerun()

        progress_bar = st.progress(0)
        status_text = st.empty()
        new_results = []  # gather new results

        max_pages_val = None if max_pages == 0 else max_pages
        # Choose processing function
        if structured:
            process_func = lambda p: process_one_pdf_structured(p, prompt_text, output_schema, model_name, max_pages_val, max_chars)
        else:
            process_func = lambda p: process_one_pdf_unstructured(p, prompt_text, model_name, max_pages_val, max_chars)

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(process_func, p): p for p in remaining_files}
            completed = 0
            for future in as_completed(futures):
                res = future.result()
                new_results.append(res)
                completed += 1
                progress_bar.progress(completed / total_remaining)
                status_text.text(f"Completed {completed}/{total_remaining} new files")
                # Save incremental progress
                all_results = existing_results + new_results
                save_progress(all_results, st.session_state.resume_file)

        st.session_state.results = existing_results + new_results
        st.session_state.processing = False
        st.session_state.status_msg = f"Done. Processed {len(new_results)} files."
        st.rerun()

    # Display results
    if st.session_state.results:
        # Show structured results as table if possible
        if structured and st.session_state.results:
            # Try to create a DataFrame from 'data' entries
            valid_data = [r for r in st.session_state.results if r.get("status") == "OK" and r.get("data")]
            if valid_data:
                rows = []
                for r in valid_data:
                    row = {"File": r["file"], "Pages": r["pages"]}
                    row.update(r["data"])
                    rows.append(row)
                df_results = pd.DataFrame(rows)
                st.dataframe(df_results, use_container_width=True)
                # Export options
                csv_data = df_results.to_csv(index=False).encode('utf-8')
                st.download_button("💾 Download Table as CSV", csv_data, "structured_results.csv", "text/csv")
            else:
                # Fallback to raw text expanders
                for res in st.session_state.results:
                    with st.expander(f"{res['file']} ({res['pages']} pages)"):
                        if res["status"] == "OK":
                            st.json(res.get("data", {}))
                        elif res["status"] == "PARSE_ERROR":
                            st.error("Could not parse JSON from LLM output.")
                            st.code(res.get("raw", "No raw output"), language="json")
                        else:
                            st.error(res.get("response", "Unknown error"))
        else:
            # Unstructured output
            for res in st.session_state.results:
                with st.expander(f"{res['file']} ({res['pages']} pages)"):
                    if res["status"] == "OK":
                        st.write(res["response"])
                    else:
                        st.error(res["response"])
            # Export unstructured as CSV
            df_unstructured = pd.DataFrame([(r["file"], r["pages"], r["response"]) for r in st.session_state.results],
                                           columns=["File", "Pages", "Analysis"])
            csv_unstructured = df_unstructured.to_csv(index=False).encode('utf-8')
            st.download_button("💾 Download Results (CSV)", csv_unstructured, "analysis_results.csv", "text/csv")

        # Option to clear results
        if st.button("Clear results"):
            st.session_state.results = []
            if os.path.exists(st.session_state.resume_file):
                os.remove(st.session_state.resume_file)
            st.rerun()

    st.divider()
    st.caption(f"Status: {st.session_state.status_msg}")
    if st.session_state.processing:
        st.info("Processing in background – you can close the browser, results are saved incrementally.")