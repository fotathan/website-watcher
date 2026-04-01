import os
from contextlib import contextmanager

import psycopg
import streamlit as st
from psycopg.rows import dict_row


def _get_database_url() -> str:
    if "DATABASE_URL" in st.secrets:
        return st.secrets["DATABASE_URL"]

    value = os.getenv("DATABASE_URL")
    if value:
        return value

    raise RuntimeError("DATABASE_URL not found in Streamlit secrets or environment")


@contextmanager
def get_conn():
    try:
        conn = psycopg.connect(_get_database_url(), row_factory=dict_row)
    except Exception as e:
        st.error(f"Postgres connection failed: {type(e).__name__}: {e}")
        st.stop()

    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def ensure_tables():
    statements = [
        """
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
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS detections (
            id SERIAL PRIMARY KEY,
            source_id INTEGER REFERENCES sources(id),
            block_hash TEXT NOT NULL,
            detected_at TEXT NOT NULL,
            is_relevant INTEGER,
            confidence DOUBLE PRECISION,
            content_type TEXT,
            classifier_reason TEXT,
            extracted_json TEXT,
            status TEXT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS attachments (
            id SERIAL PRIMARY KEY,
            detection_id INTEGER REFERENCES detections(id),
            source_url TEXT,
            attachment_url TEXT NOT NULL,
            filename TEXT,
            extension TEXT,
            content_type TEXT,
            file_size BIGINT,
            sha256 TEXT,
            local_path TEXT,
            download_status TEXT,
            extracted_text_path TEXT,
            parser_used TEXT,
            extraction_quality TEXT,
            metadata_json TEXT,
            created_at TEXT NOT NULL
        )
        """,
    ]

    with get_conn() as conn:
        with conn.cursor() as cur:
            for stmt in statements:
                cur.execute(stmt)
