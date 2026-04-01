import sys
from pathlib import Path

import streamlit as st

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.db import ensure_tables

ensure_tables()

st.set_page_config(page_title="Tender Monitor Cloud", layout="wide", initial_sidebar_state="expanded")

st.title("Tender Monitor Cloud")
st.caption("Website Watcher import, deep enrichment, and review.")

st.markdown(
    """
Use the sidebar to navigate:

- **Dashboard** → overview of imported detections
- **Detections** → review imported items
- **Website Watcher Import** → upload report, preview, deep-enrich, import
"""
)
