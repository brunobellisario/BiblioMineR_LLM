import streamlit as st
import requests
import os
import pandas as pd
import cloudscraper
import time
import re
import glob
import hashlib
import pickle
import multiprocessing
from concurrent.futures import ThreadPoolExecutor, as_completed
import fitz  # PyMuPDF
import ollama

# ---------- Critical macOS Fork Safety Fix ----------
if __name__ == '__main__':
    try:
        if multiprocessing.get_start_method(allow_none=True) != 'spawn':
            multiprocessing.set_start_method('spawn', force=True)
    except RuntimeError:
        pass 

os.environ["OBJC_DISABLE_INITIALIZE_FORK_SAFETY"] = "YES"

# ---------- Setup & Session State ----------
st.set_page_config(page_title="Researcher OS v3.5", layout="wide")
DOWNLOAD_DIR = "./downloads"
CACHE_DIR = "./pdf_cache"
for d in [DOWNLOAD_DIR, CACHE_DIR]: 
    os.makedirs(d, exist_ok=True)

# Initialize Session States
if "dl_queue" not in st.session_state: st.session_state.dl_queue = ""
if "results_df" not in st.session_state: st.session_state.results_df = None
if "pdf_files" not in st.session_state: st.session_state.pdf_files = []
if "ai_results" not in st.session_state: st.session_state.ai_results = []
if "processing" not in st.session_state: st.session_state.processing = False
if "source_folder" not in st.session_state: st.session_state.source_folder = os.path.abspath(DOWNLOAD_DIR)

# --- HELPER FUNCTIONS: SEARCH & CLEAN ---
def clean_abstract(text):
    if not text: return "No abstract available."
    clean = re.sub(r'<[^>]+>', '', str(text))
    return " ".join(clean.split())

def search_openalex(query, limit=100):
    url = "https://api.openalex.org/works"
    params = {"search": query, "per_page": limit, "select": "doi,display_name,publication_year,authorships,abstract_inverted_index,host_venue"}
    try:
        r = requests.get(url, params=params, timeout=10)
        results = r.json().get('results', [])
        data = []
        for item in results:
            abstract = "No abstract available."
            index = item.get('abstract_inverted_index')
            if index:
                words = {}
                for word, pos_list in index.items():
                    for pos in pos_list: words[pos] = word
                abstract = " ".join([words[i] for i in sorted(words.keys())])
            authors = ", ".join([a.get('author', {}).get('display_name', '') for a in item.get('authorships', [])])
            data.append({
                'Source': 'OpenAlex', 
                'Title': item.get('display_name'), 
                'Authors': authors, 
                'Journal': item.get('host_venue', {}).get('display_name', 'N/A'), 
                'Year': str(item.get('publication_year')), 
                'DOI': item.get('doi', '').replace('https://doi.org/', ''), 
                'Abstract': clean_abstract(abstract)
            })
        return data
    except: return []

def search_scopus(query, api_key, limit=100):
    if not api_key: return []
    url = "https://api.elsevier.com/content/search/scopus"
    headers = {"X-ELS-APIKey": api_key, "Accept": "application/json"}
    params = {"query": query, "count": limit}
    try:
        r = requests.get(url, headers=headers, params=params, timeout=10)
        entries = r.json().get('search-results', {}).get('entry', [])
        return [{
            'Source': 'Scopus', 
            'Title': i.get('dc:title'), 
            'Authors': i.get('dc:creator', 'N/A'), 
            'Journal': i.get('prism:publicationName', 'N/A'), 
            'Year': i.get('prism:coverDate', '')[:4], 
            'DOI': i.get('prism:doi'), 
            'Abstract': 'Restricted'
        } for i in entries]
    except: return []

def smart_download(url, filepath, extra_headers=None):
    headers = {'User-Agent': 'Mozilla/5.0', 'Accept': 'application/pdf'}
    if extra_headers: headers.update(extra_headers)
    try:
        scraper = cloudscraper.create_scraper()
        response = scraper.get(url, headers=headers, timeout=30, stream=True)
        if response.status_code == 200 and 'pdf' in response.headers.get('Content-Type', '').lower():
            with open(filepath, 'wb') as f: f.write(response.content)
            return True, "Success"
        return False, f"Status {response.status_code}"
    except Exception as e: return False, str(e)

# --- HELPER FUNCTIONS: AI ANALYSIS & CACHING ---
def extract_pdf_text_fast(pdf_path, max_pages=None, max_chars=20000):
    try:
        doc = fitz.open(pdf_path)
        total_pages = len(doc)
        pages_to_read = doc if max_pages is None or max_pages == 0 else doc[:max_pages]
        text_parts = [page.get_text() for page in pages_to_read if page.get_text()]
        doc.close()
        text = "\n".join(text_parts)
        if max_chars and len(text) > max_chars: 
            text = text[:max_chars] + "\n...[truncated]..."
        return text, total_pages
    except: return None, 0

def get_cached_text(pdf_path, max_pages, max_chars):
    mtime = os.path.getmtime(pdf_path)
    cache_key = hashlib.md5(f"{pdf_path}_{mtime}_{max_pages}_{max_chars}".encode()).hexdigest()
    cache_file = os.path.join(CACHE_DIR, cache_key)
    if os.path.exists(cache_file):
        try:
            with open(cache_file, "rb") as f: return pickle.load(f)
        except: pass
    text, pages = extract_pdf_text_fast(pdf_path, max_pages, max_chars)
    if text is not None:
        with open(cache_file, "wb") as f: pickle.dump((text, pages), f)
    return text, pages

def get_prompt(prompt_mode, sum_length="standard", template="exec", custom_query=""):
    if prompt_mode == "Quick Summary":
        return f"Summarize in {sum_length} sentences."
    elif prompt_mode == "Template":
        templates = {
            "Executive Summary": "Provide an executive summary (Purpose, Findings, Implications).",
            "Key Findings": "List the key findings with supporting evidence.",
            "Methods": "Describe the methodology and data collection techniques.",
            "Research Questions": "What research questions does this address?",
            "Limitations": "What limitations and future work are mentioned?"
        }
        return templates.get(template, "Summarize.")
    return custom_query if custom_query.strip() else "Summarize."

def call_ollama(prompt, model="llama3.2", max_retries=3):
    for attempt in range(max_retries):
        try:
            response = ollama.chat(model=model, messages=[{"role": "user", "content": prompt}])
            return response["message"]["content"]
        except:
            if attempt < max_retries - 1: time.sleep(2 ** attempt)
            else: return "Error connecting to Ollama."

def process_one_pdf(pdf_path, prompt_text, model_name, max_pages, max_chars):
    content, num_pages = get_cached_text(pdf_path, max_pages, max_chars)
    if not content: 
        return {"file": os.path.basename(pdf_path), "pages": num_pages, "response": "Extraction failed", "status": "ERROR"}
    res = call_ollama(f"{prompt_text}\n\nCONTENT:\n{content}", model=model_name)
    return {"file": os.path.basename(pdf_path), "pages": num_pages, "response": res, "status": "OK"}

# --- UI LAYOUT ---
with st.sidebar:
    st.header("⚙️ Configuration")
    scopus_key = st.text_input("Scopus API Key", type="password")
    wiley_token = st.text_input("Wiley TDM Token", type="password")
    ollama_url = st.text_input("Ollama URL", "http://localhost:11434")
    os.environ["OLLAMA_HOST"] = ollama_url
    
    st.divider()
    st.header("⚡ AI Engine")
    model_name = st.selectbox("Ollama Model", ["llama3.2", "phi4", "mistral", "llama3.2:3b"])
    max_workers = st.slider("Parallel Threads", 1, 8, 4)
    max_pages_val = st.number_input("Max Pages (0=all)", 0, 500, 5)
    max_chars_val = st.number_input("Max Characters", 1000, 100000, 20000)

tabs = st.tabs(["🔍 1. Discovery", "📥 2. Download Queue", "🧠 3. AI Analysis"])

# TAB 1: SEARCH
with tabs[0]:
    c1, c2 = st.columns([3, 1])
    query = c1.text_input("Search Global Databases:", placeholder="e.g. 'deep learning' AND 'healthcare'")
    limit = c2.number_input("Limit/DB", 5, 100, 10)
    
    if st.button("Search All"):
        with st.spinner("Fetching metadata..."):
            res = search_openalex(query, limit)
            if scopus_key: res += search_scopus(query, scopus_key, limit)
            st.session_state.results_df = pd.DataFrame(res)
    
    if st.session_state.results_df is not None:
        st.dataframe(st.session_state.results_df, use_container_width=True)
        if st.button("🚀 Add all to Queue"):
            dois = "\n".join(st.session_state.results_df['DOI'].dropna().unique())
            st.session_state.dl_queue += f"{dois}\n"
            st.success("DOIs synced to Tab 2")

# TAB 2: DOWNLOAD
with tabs[1]:
    st.header("PDF Acquisition")
    q_val = st.text_area("Queue (one DOI per line):", value=st.session_state.dl_queue, height=200)
    if st.button("Start Bulk Download"):
        dois = list(set([d.strip() for d in q_val.split('\n') if d.strip()]))
        p = st.progress(0)
        for i, doi in enumerate(dois):
            target, headers = f"https://link.springer.com/content/pdf/{doi}.pdf", {}
            if "10.1016" in doi:
                target, headers = f"https://api.elsevier.com/content/article/doi/{doi}", {"X-ELS-APIKey": scopus_key}
            elif any(x in doi for x in ["10.1111", "10.1002"]):
                if wiley_token:
                    target, headers = f"https://api.wiley.com/onlinelibrary/tdm/v1/articles/{doi}", {"CR-Clickthrough-Client-Token": wiley_token}
                else: target = f"https://onlinelibrary.wiley.com/doi/pdfdirect/{doi}"
            
            st.write(f"Downloading `{doi}`...")
            smart_download(target, f"{DOWNLOAD_DIR}/{doi.replace('/', '_')}.pdf", headers)
            p.progress((i + 1) / len(dois))

# TAB 3: AI ANALYSIS
with tabs[2]:
    st.header("Local LLM Analyzer")
    src_folder = st.text_input("Folder to analyze:", value=st.session_state.source_folder)
    
    if st.button("Scan for PDFs"):
        st.session_state.pdf_files = glob.glob(os.path.join(src_folder, "*.pdf"))
    
    st.write(f"PDFs found: **{len(st.session_state.pdf_files)}**")
    
    st.divider()
    prompt_mode = st.radio("Prompt Mode", ["Quick Summary", "Template", "Custom Query"], horizontal=True)
    
    col_p1, col_p2 = st.columns(2)
    with col_p1:
        s_len = st.selectbox("Length", ["3", "5", "10"], index=1) if prompt_mode == "Quick Summary" else "standard"
        t_sel = st.selectbox("Template", ["Executive Summary", "Key Findings", "Methods", "Research Questions", "Limitations"]) if prompt_mode == "Template" else "exec"
    with col_p2:
        c_query = st.text_area("Custom Query") if prompt_mode == "Custom Query" else ""

    final_prompt = get_prompt(prompt_mode, s_len, t_sel, c_query)

    if st.button("🚀 Start AI Batch Analysis") and st.session_state.pdf_files:
        st.session_state.processing = True
        results = []
        p_bar = st.progress(0)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(process_one_pdf, f, final_prompt, model_name, max_pages_val, max_chars_val) for f in st.session_state.pdf_files]
            for i, fut in enumerate(as_completed(futures)):
                results.append(fut.result())
                p_bar.progress((i+1)/len(futures))
        st.session_state.ai_results = results
        st.session_state.processing = False

    for res in st.session_state.ai_results:
        with st.expander(f"📄 {res['file']} ({res.get('pages', '?')} pgs)"):
            st.write(res['response'])
    
    if st.session_state.ai_results:
        df_out = pd.DataFrame(st.session_state.ai_results)
        st.download_button("💾 Download Analysis CSV", df_out.to_csv(index=False), "analysis.csv", "text/csv")