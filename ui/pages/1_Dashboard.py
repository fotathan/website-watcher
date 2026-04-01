import sys
from pathlib import Path

import pandas as pd
import streamlit as st

ROOT_DIR = Path(__file__).resolve().parent.parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.db import ensure_tables, get_conn

ensure_tables()

st.set_page_config(page_title="Dashboard", layout="wide")
st.title("Dashboard")

with get_conn() as conn:
    total_sources = conn.execute("SELECT COUNT(*) AS cnt FROM sources").fetchone()["cnt"]
    total_detections = conn.execute("SELECT COUNT(*) AS cnt FROM detections").fetchone()["cnt"]
    relevant = conn.execute("SELECT COUNT(*) AS cnt FROM detections WHERE is_relevant = 1").fetchone()["cnt"]
    irrelevant = conn.execute("SELECT COUNT(*) AS cnt FROM detections WHERE is_relevant = 0").fetchone()["cnt"]
    new_count = conn.execute("SELECT COUNT(*) AS cnt FROM detections WHERE status = 'new'").fetchone()["cnt"]

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Sources", total_sources)
c2.metric("Detections", total_detections)
c3.metric("Relevant", relevant)
c4.metric("Irrelevant", irrelevant)
c5.metric("Pending Review", new_count)

st.subheader("Detections by source")
with get_conn() as conn:
    rows = conn.execute(
        """
        SELECT s.name AS source,
               COUNT(*) AS total,
               SUM(CASE WHEN d.is_relevant = 1 THEN 1 ELSE 0 END) AS relevant,
               SUM(CASE WHEN d.is_relevant = 0 THEN 1 ELSE 0 END) AS irrelevant
        FROM detections d
        JOIN sources s ON s.id = d.source_id
        GROUP BY s.name
        ORDER BY total DESC
        """
    ).fetchall()

df = pd.DataFrame([dict(r) for r in rows]) if rows else pd.DataFrame(columns=["source", "total", "relevant", "irrelevant"])
st.dataframe(df, use_container_width=True)
if not df.empty:
    st.bar_chart(df.set_index("source")[["relevant", "irrelevant"]])
