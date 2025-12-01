
import streamlit as st
import requests

BACKEND = "http://127.0.0.1:8000"

st.set_page_config(page_title="Function Tester", layout="centered")
st.title("🔍 Business Rule Function Runner")

@st.cache_data
def fetch_functions():
    try:
        return requests.get(f"{BACKEND}/functions").json()
    except:
        return []

funcs = fetch_functions()
choices = [f['function_name'] for f in funcs]
selected = st.selectbox("Select Function", choices)

params = next((f['parameters'] for f in funcs if f['function_name'] == selected), [])
st.write("### Fill Parameters")

with st.form("run-form"):
    filled = {}
    for p in params:
        v = st.text_input(f"{p['name']} ({p['type']})", value="" if p['default'] == "Required" else str(p['default']))
        if v != "":
            filled[p['name']] = v
    run = st.form_submit_button("Run")

if run:
    payload = {"function_name": selected, "parameters": filled}
    r = requests.post(f"{BACKEND}/run_function", json=payload)
    if r.status_code == 200:
        st.success("✅ Function Output:")
        st.dataframe(r.json())
    else:
        st.error("❌ Error:")
        st.json(r.json())
