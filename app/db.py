import os
from contextlib import contextmanager

import psycopg
from psycopg.rows import dict_row


def _get_database_url() -> str:
    try:
        import streamlit as st
        if "DATABASE_URL" in st.secrets:
            return st.secrets["DATABASE_URL"]
        if "db" in st.secrets and "url" in st.secrets["db"]:
            return st.secrets["db"]["url"]
    except Exception:
        pass

    url = os.getenv("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL not configured. Set it in Streamlit secrets or environment.")
    return url


@contextmanager
def get_conn():
    conn = psycopg.connect(_get_database_url(), row_factory=dict_row)
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


DDL = """
CREATE TABLE IF NOT EXISTS sources (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    url TEXT,
    main_selector TEXT,
    item_selector TEXT,
    detail_link_selector TEXT,
    language TEXT,
    poll_minutes INTEGER DEFAULT 0,
    active INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS detections (
    id SERIAL PRIMARY KEY,
    source_id INTEGER REFERENCES sources(id),
    block_hash TEXT NOT NULL,
    detected_at TEXT NOT NULL,
    is_relevant INTEGER,
    confidence DOUBLE PRECISION,
    content_type TEXT,
    classifier_reason TEXT,
    extracted_json JSONB,
    status TEXT DEFAULT 'new'
);

CREATE INDEX IF NOT EXISTS idx_detections_source_id ON detections(source_id);
CREATE INDEX IF NOT EXISTS idx_detections_status ON detections(status);
CREATE INDEX IF NOT EXISTS idx_detections_detected_at ON detections(detected_at);

CREATE TABLE IF NOT EXISTS attachments (
    id SERIAL PRIMARY KEY,
    detection_id INTEGER REFERENCES detections(id),
    source_url TEXT,
    attachment_url TEXT NOT NULL,
    filename TEXT,
    extension TEXT,
    content_type TEXT,
    file_size INTEGER,
    sha256 TEXT,
    local_path TEXT,
    download_status TEXT,
    extracted_text_path TEXT,
    parser_used TEXT,
    extraction_quality TEXT,
    metadata_json JSONB,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_attachments_detection_id ON attachments(detection_id);
"""


def ensure_tables() -> None:
    with get_conn() as conn:
        conn.execute(DDL)
