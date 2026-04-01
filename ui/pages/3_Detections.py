import json
import sys
from pathlib import Path

import streamlit as st

ROOT_DIR = Path(__file__).resolve().parent.parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.db import ensure_tables, get_conn

ensure_tables()

st.set_page_config(page_title="Detections", layout="wide")
st.title("Detections")

with get_conn() as conn:
    source_rows = conn.execute("SELECT DISTINCT name FROM sources ORDER BY name").fetchall()

source_options = ["All"] + [r["name"] for r in source_rows]
selected_source = st.selectbox("Source", source_options)

status_options = ["All", "new", "approved", "rejected", "import_ready"]
selected_status = st.selectbox("Status", status_options)

relevance_options = ["All", "Relevant", "Irrelevant"]
selected_relevance = st.selectbox("Relevance", relevance_options)

query = """
SELECT d.id,
       d.detected_at,
       s.name AS source,
       d.is_relevant,
       d.confidence,
       d.content_type,
       d.classifier_reason,
       d.extracted_json,
       d.status
FROM detections d
JOIN sources s ON s.id = d.source_id
WHERE 1 = 1
"""
params = []

if selected_source != "All":
    query += " AND s.name = %s"
    params.append(selected_source)

if selected_status != "All":
    query += " AND d.status = %s"
    params.append(selected_status)

if selected_relevance == "Relevant":
    query += " AND d.is_relevant = 1"
elif selected_relevance == "Irrelevant":
    query += " AND d.is_relevant = 0"

query += " ORDER BY d.detected_at DESC"

with get_conn() as conn:
    rows = conn.execute(query, params).fetchall()

st.caption(f"{len(rows)} detections found")

for row in rows:
    st.divider()
    col1, col2 = st.columns([1, 1])

    extracted = {}
    if row["extracted_json"]:
        if isinstance(row["extracted_json"], dict):
            extracted = row["extracted_json"]
        else:
            try:
                extracted = json.loads(row["extracted_json"])
            except Exception:
                extracted = {"raw": row["extracted_json"]}

    with col1:
        st.subheader(f"{row['source']} — #{row['id']}")
        st.write(f"Detected at: {row['detected_at']}")
        st.write(f"Relevant: {bool(row['is_relevant'])}")
        st.write(f"Confidence: {row['confidence']}")
        st.write(f"Type: {row['content_type']}")
        st.write(f"Status: {row['status']}")
        st.write(f"Reason: {row['classifier_reason']}")

    with col2:
        st.subheader("Extracted JSON")
        st.json(extracted)

    c1, c2, c3 = st.columns(3)

    if c1.button(f"Approve #{row['id']}"):
        with get_conn() as conn:
            conn.execute("UPDATE detections SET status = 'approved' WHERE id = %s", (row["id"],))
        st.rerun()

    if c2.button(f"Reject #{row['id']}"):
        with get_conn() as conn:
            conn.execute("UPDATE detections SET status = 'rejected' WHERE id = %s", (row["id"],))
        st.rerun()

    if c3.button(f"Import-ready #{row['id']}"):
        with get_conn() as conn:
            conn.execute("UPDATE detections SET status = 'import_ready' WHERE id = %s", (row["id"],))
        st.rerun()
