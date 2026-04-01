import sys
from pathlib import Path

import pandas as pd
import streamlit as st

ROOT_DIR = Path(__file__).resolve().parent.parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.db import ensure_tables
from app.watcher_importer import (
    deep_enrich_candidates,
    import_candidates_into_detections,
    parse_candidates_from_uploaded_bytes,
)

ensure_tables()

st.set_page_config(page_title="Website Watcher Import", layout="wide")
st.title("Website Watcher Import")

st.caption("Upload a Website Watcher .htm/.html export, preview parsed candidates, optionally deep-enrich via linked page/PDF, and import them into detections.")

uploaded_file = st.file_uploader("Upload Website Watcher report", type=["htm", "html"])

include_irrelevant_preview = st.checkbox("Also preview irrelevant entries", value=True)
include_irrelevant_import = st.checkbox("Import irrelevant entries too", value=False)

deep_enrich = st.checkbox("Deep enrich from linked page / PDF", value=True)
enrich_only_tenders = st.checkbox("Deep enrich only tender-like candidates", value=True)
min_confidence = st.slider("Minimum confidence for deep enrichment", min_value=0.0, max_value=1.0, value=0.6, step=0.05)

if uploaded_file is not None:
    file_bytes = uploaded_file.read()

    try:
        candidates = parse_candidates_from_uploaded_bytes(file_bytes)
    except Exception as e:
        st.error(f"Failed to parse report: {e}")
        st.stop()

    if not candidates:
        st.warning("No candidates found in the uploaded report.")
        st.stop()

    if deep_enrich:
        with st.spinner("Deep enriching candidates via linked page / PDF..."):
            candidates = deep_enrich_candidates(
                candidates,
                enrich_only_tenders=enrich_only_tenders,
                min_confidence=min_confidence,
            )

    total = len(candidates)
    tenders = sum(1 for c in candidates if c.is_tender)
    irrelevant = total - tenders
    enriched_count = 0

    for c in candidates:
        d = c.__dict__
        if d.get("detail_enriched") or d.get("detail_page_url") or d.get("attachments"):
            enriched_count += 1

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total candidates", total)
    c2.metric("Tender-like", tenders)
    c3.metric("Irrelevant-like", irrelevant)
    c4.metric("Deep-enriched", enriched_count)

    preview_rows = []
    for c in candidates:
        if not include_irrelevant_preview and not c.is_tender:
            continue

        d = c.__dict__
        preview_rows.append(
            {
                "is_tender": c.is_tender,
                "confidence": c.confidence,
                "category": c.category,
                "source_name": c.source_name,
                "bookmark_name": c.bookmark_name,
                "tender_id": c.tender_id,
                "title": c.title,
                "publication_date": c.publication_date,
                "deadline_date": c.deadline_date,
                "estimated_price": c.estimated_price,
                "currency": c.currency,
                "authority_name": c.authority_name,
                "document_url": c.document_url,
                "detail_url": c.detail_url,
                "detail_page_url": d.get("detail_page_url"),
                "pdf_links_found": len(d.get("detail_page_pdf_links", [])),
                "attachments_count": len(d.get("attachments", [])) if isinstance(d.get("attachments"), list) else 0,
            }
        )

    st.subheader("Preview")
    st.dataframe(pd.DataFrame(preview_rows), use_container_width=True)

    st.subheader("Detailed candidates")
    for idx, c in enumerate(candidates, start=1):
        if not include_irrelevant_preview and not c.is_tender:
            continue

        d = c.__dict__

        st.divider()
        col1, col2 = st.columns([1, 1])

        with col1:
            st.markdown(f"**#{idx} — {c.title or '(no title)'}**")
            st.write(f"Source: {c.source_name}")
            st.write(f"Bookmark: {c.bookmark_name}")
            st.write(f"Tender-like: {c.is_tender}")
            st.write(f"Confidence: {c.confidence}")
            st.write(f"Category: {c.category}")
            st.write(f"Tender ID: {c.tender_id}")
            st.write(f"Publication date: {c.publication_date}")
            st.write(f"Deadline date: {c.deadline_date}")
            st.write(f"Estimated price: {c.estimated_price}")
            st.write(f"Currency: {c.currency}")
            st.write(f"Authority: {c.authority_name}")
            st.write(f"Procedure: {c.procedure_type}")
            st.write(f"Web page URL: {c.web_page_url}")
            st.write(f"Detail URL: {c.detail_url}")
            st.write(f"Document URL: {c.document_url}")
            st.write(f"Detail page enriched URL: {d.get('detail_page_url')}")
            st.write(f"PDF links found: {len(d.get('detail_page_pdf_links', []))}")
            st.write(f"Attachments: {len(d.get('attachments', [])) if isinstance(d.get('attachments'), list) else 0}")

        with col2:
            st.markdown("**Raw text**")
            st.text(c.raw_text[:4000])
            st.markdown("**JSON**")
            st.json(d)

    st.subheader("Import")
    if st.button("Import parsed candidates into detections", use_container_width=True):
        try:
            result = import_candidates_into_detections(candidates, include_irrelevant=include_irrelevant_import)
            st.success(
                f"Import finished. Inserted: {result['inserted']}, "
                f"Duplicates skipped: {result['skipped_duplicates']}, "
                f"Irrelevant skipped: {result['skipped_irrelevant']}"
            )
        except Exception as e:
            st.error(f"Import failed: {e}")
