import streamlit as st
import requests
import os
import pandas as pd
import cloudscraper
import time
import re
import io

# --- Helper Functions ---

def clean_abstract(text):
    """Cleans XML tags and normalizes whitespace."""
    if not text: return "No abstract available."
    # If the abstract is an OpenAlex inverted index (dict), it's handled in the search function
    if isinstance(text, dict): return "Structured Abstract (See Table)"
    clean = re.sub(r'<[^>]+>', '', str(text))
    return " ".join(clean.split())

# --- 1. SEARCH LOGIC ---

def search_openalex(query, limit=10):
    url = "https://api.openalex.org/works"
    params = {
        "search": query,
        "per_page": limit,
        "select": "doi,display_name,publication_year,authorships,abstract_inverted_index,host_venue"
    }
    try:
        r = requests.get(url, params=params, timeout=10)
        results = r.json().get('results', [])
        data = []
        for item in results:
            # Reconstruct abstract from inverted index
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
    except Exception:
        return []

def search_scopus(query, api_key, limit=10):
    if not api_key: return []
    url = "https://api.elsevier.com/content/search/scopus"
    headers = {"X-ELS-APIKey": api_key, "Accept": "application/json"}
    params = {"query": query, "count": limit}
    try:
        r = requests.get(url, headers=headers, params=params, timeout=10)
        entries = r.json().get('search-results', {}).get('entry', [])
        data = []
        for item in entries:
            data.append({
                'Source': 'Scopus',
                'Title': item.get('dc:title'),
                'Authors': item.get('dc:creator', 'N/A'),
                'Journal': item.get('prism:publicationName', 'N/A'),
                'Year': item.get('prism:coverDate', '')[:4],
                'DOI': item.get('prism:doi'),
                'Abstract': 'Abstract retrieval restricted in basic search.'
            })
        return data
    except Exception:
        return []

def search_crossref(query, limit=10):
    url = "https://api.crossref.org/works"
    params = {"query": query, "rows": limit}
    try:
        r = requests.get(url, params=params, timeout=10)
        items = r.json().get('message', {}).get('items', [])
        data = []
        for item in items:
            year = item.get('published-print', {}).get('date-parts', [[None]])[0][0]
            authors = ", ".join([f"{a.get('given', '')} {a.get('family', '')}" for a in item.get('author', [])])
            data.append({
                'Source': 'CrossRef',
                'Title': item.get('title', [''])[0],
                'Authors': authors,
                'Journal': item.get('container-title', [''])[0],
                'Year': str(year) if year else "N/A",
                'DOI': item.get('DOI'),
                'Abstract': clean_abstract(item.get('abstract'))
            })
        return data
    except Exception:
        return []

# --- 2. DOWNLOAD LOGIC ---

def smart_download(url, filepath, extra_headers=None):
    headers = {'User-Agent': 'Mozilla/5.0', 'Accept': 'application/pdf'}
    if extra_headers: headers.update(extra_headers)
    try:
        if any(x in url for x in ["api.wiley.com", "api.elsevier.com"]):
            response = requests.get(url, headers=headers, timeout=30, stream=True)
        else:
            scraper = cloudscraper.create_scraper()
            response = scraper.get(url, headers=headers, timeout=30, stream=True)
        
        if response.status_code == 200 and 'pdf' in response.headers.get('Content-Type', '').lower():
            with open(filepath, 'wb') as f:
                f.write(response.content)
            return True, "Success"
        return False, f"Status {response.status_code}"
    except Exception as e:
        return False, str(e)

# --- 3. UI LAYOUT ---
st.set_page_config(page_title="Researcher OS v2.1", layout="wide")

with st.sidebar:
    st.title("🔑 API Settings")
    scopus_key = st.text_input("Scopus (Elsevier) API Key", type="password")
    wiley_token = st.text_input("Wiley TDM Token", type="password")

tab_search, tab_dl = st.tabs(["🔍 Database Explorer", "📥 PDF Downloader"])

with tab_search:
    st.header("Global Literature Search")
    c1, c2 = st.columns([3, 1])
    with c1:
        query = st.text_input("Search terms:", placeholder="e.g. 'bioinformatics' AND 'CRISPR'")
    with c2:
        limit = st.number_input("Results per DB", 5, 100, 20)

    if st.button("Search All Databases"):
        with st.spinner("Fetching data..."):
            results = []
            results += search_openalex(query, limit)
            results += search_crossref(query, limit)
            if scopus_key: results += search_scopus(query, scopus_key, limit)
            
            if results:
                st.session_state['results_df'] = pd.DataFrame(results)
                st.success(f"Aggregated {len(results)} results.")
            else:
                st.error("No results found.")

    if 'results_df' in st.session_state:
        df = st.session_state['results_df']
        
        # --- NEW: DOWNLOAD LIST SECTION ---
        st.subheader("Results List")
        st.dataframe(df, use_container_width=True)
        
        col_export1, col_export2 = st.columns(2)
        with col_export1:
            # Export to CSV
            csv = df.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="📥 Download Search Results as CSV",
                data=csv,
                file_name=f"search_results_{int(time.time())}.csv",
                mime='text/csv',
            )
        
        with col_export2:
            # Export to Excel (requires openpyxl installed: pip install openpyxl)
            buffer = io.BytesIO()
            with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                df.to_excel(writer, index=False, sheet_name='Search Results')
            st.download_button(
                label="📊 Download Search Results as Excel",
                data=buffer.getvalue(),
                file_name=f"search_results_{int(time.time())}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        
        st.divider()
        # --- QUEUE MANAGEMENT ---
        st.subheader("Manage Download Queue")
        if st.button("🚀 ADD ALL RESULTS TO DOWNLOAD QUEUE"):
            all_dois = "\n".join(df['DOI'].dropna().unique())
            st.session_state['dl_queue'] = st.session_state.get('dl_queue', "") + f"{all_dois}\n"
            st.success("All DOIs added to the Downloader tab!")

with tab_dl:
    st.header("Bulk Download Manager")
    q_val = st.text_area("List of DOIs to fetch:", value=st.session_state.get('dl_queue', ""), height=300)
    
    col_dl, col_clr = st.columns(2)
    if col_clr.button("Clear Queue"):
        st.session_state['dl_queue'] = ""
        st.rerun()

    if col_dl.button("Start Bulk Download"):
        if not os.path.exists("downloads"): os.makedirs("downloads")
        dois = list(set([d.strip() for d in q_val.split('\n') if d.strip()]))
        
        p_bar = st.progress(0)
        for i, doi in enumerate(dois):
            target, headers, prov = None, {}, ""
            
            # Simple DOI Routing
            if "10.1016" in doi:
                prov, target = "Elsevier", f"https://api.elsevier.com/content/article/doi/{doi}"
                headers = {"X-ELS-APIKey": scopus_key, "Accept": "application/pdf"}
            elif any(x in doi for x in ["10.1111", "10.1002"]):
                prov = "Wiley"
                if wiley_token:
                    target = f"https://api.wiley.com/onlinelibrary/tdm/v1/articles/{doi}"
                    headers = {"CR-Clickthrough-Client-Token": wiley_token, "Accept": "application/pdf"}
                else: target = f"https://onlinelibrary.wiley.com/doi/pdfdirect/{doi}"
            elif any(x in doi for x in ["10.1007", "10.1038"]):
                prov, target = "Springer/Nature", f"https://link.springer.com/content/pdf/{doi}.pdf"
            
            if target:
                st.write(f"📥 `{doi}` ({prov})")
                path = f"downloads/{doi.replace('/', '_')}.pdf"
                ok, msg = smart_download(target, path, headers)
                if ok: st.success(f"Done: {doi}")
                else: st.error(f"Error {doi}: {msg}")
            
            p_bar.progress((i + 1) / len(dois))
            time.sleep(1)