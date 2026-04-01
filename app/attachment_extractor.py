import hashlib
import json
import os
import re
from pathlib import Path
from urllib.parse import urlparse

import fitz
import requests

from .db import get_conn
from .utils import normalize_text, utc_now_iso


DATE_RE = re.compile(r"\b(\d{1,2}/\d{1,2}/\d{2,4})(?:\s+(\d{1,2}:\d{2}))?\b")
PRICE_RE = re.compile(
    r"(?P<amount>\d{1,3}(?:[.\s]\d{3})*(?:,\d{2})?|\d+(?:,\d{2})?)\s*(?P<currency>€|EUR)",
    re.IGNORECASE,
)
ID_RE = re.compile(r"\b([A-Z]{0,6}\d{1,10}(?:/\d{2,4})?(?:-\d+)?)\b")


def _sha256_bytes(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def safe_filename_from_url(url: str) -> str:
    parsed = urlparse(url)
    name = os.path.basename(parsed.path) or "attachment"
    return re.sub(r"[^A-Za-z0-9._-]+", "_", name)


def extension_from_filename(filename: str) -> str | None:
    suffix = Path(filename).suffix.lower().strip(".")
    return suffix or None


def download_file_bytes(url: str) -> dict:
    response = requests.get(
        url,
        timeout=60,
        headers={"User-Agent": "Mozilla/5.0 Tender Monitor"},
    )
    response.raise_for_status()

    content = response.content
    sha256 = _sha256_bytes(content)
    filename = safe_filename_from_url(url)
    return {
        "attachment_url": url,
        "filename": filename,
        "extension": extension_from_filename(filename),
        "content_type": response.headers.get("Content-Type"),
        "file_size": len(content),
        "sha256": sha256,
        "download_status": "downloaded",
        "bytes": content,
    }


def extract_pdf_text_from_bytes(content: bytes, filename: str = "document.pdf") -> dict:
    doc = fitz.open(stream=content, filetype="pdf")

    pages_text = []
    page_lengths = []

    for page in doc:
        text = normalize_text(page.get_text("text"))
        pages_text.append(text)
        page_lengths.append(len(text))

    full_text = "\n\n".join(p for p in pages_text if p).strip()
    text_length = len(full_text)
    page_count = len(doc)

    if text_length > 3000:
        quality = "high"
    elif text_length > 500:
        quality = "medium"
    else:
        quality = "low"

    metadata = {
        "page_count": page_count,
        "text_length": text_length,
        "page_lengths": page_lengths,
        "pdf_metadata": doc.metadata,
        "filename": filename,
    }

    doc.close()

    return {
        "parser_used": "pymupdf",
        "text": full_text,
        "extraction_quality": quality,
        "metadata": metadata,
    }


def extract_tender_id(text: str) -> str | None:
    m = ID_RE.search(text)
    return m.group(1) if m else None


def extract_dates(text: str) -> tuple[str | None, str | None]:
    matches = list(DATE_RE.finditer(text))
    if not matches:
        return None, None

    publication_date = None
    deadline_date = None

    if len(matches) >= 1:
        d1, t1 = matches[0].group(1), matches[0].group(2)
        publication_date = f"{d1} {t1}".strip() if t1 else d1

    if len(matches) >= 2:
        d2, t2 = matches[1].group(1), matches[1].group(2)
        deadline_date = f"{d2} {t2}".strip() if t2 else d2

    lower = text.lower()
    if "plazo presentación de ofertas" in lower or "plazo presentacion de ofertas" in lower or "fecha límite" in lower or "fecha limite" in lower:
        if len(matches) >= 2:
            d2, t2 = matches[1].group(1), matches[1].group(2)
            deadline_date = f"{d2} {t2}".strip() if t2 else d2

    return publication_date, deadline_date


def extract_price(text: str) -> tuple[float | None, str | None]:
    m = PRICE_RE.search(text)
    if not m:
        return None, None

    amount_raw = m.group("amount").replace(".", "").replace(" ", "").replace(",", ".")
    try:
        amount = float(amount_raw)
    except Exception:
        amount = None

    currency = m.group("currency")
    return amount, currency


def extract_procedure_type(text: str) -> str | None:
    lower = text.lower()
    candidates = [
        "procedimiento abierto simplificado abreviado",
        "procedimiento abierto simplificado",
        "procedimiento abierto",
        "contrato menor",
        "contrato mixto",
        "concesión administrativa",
        "concesion administrativa",
        "concurso público",
        "concurso publico",
        "subasta",
    ]
    for c in candidates:
        if c in lower:
            return c
    return None


def extract_authority(text: str) -> str | None:
    patterns = [
        r"órgano de contratación[:\s]+(.+)",
        r"organo de contratacion[:\s]+(.+)",
        r"entidad adjudicadora[:\s]+(.+)",
        r"authority[:\s]+(.+)",
    ]
    lines = [normalize_text(line) for line in text.splitlines() if normalize_text(line)]
    for line in lines[:50]:
        lower = line.lower()
        for p in patterns:
            m = re.search(p, lower, re.IGNORECASE)
            if m:
                return line.split(":", 1)[-1].strip()
    return None


def extract_title_and_description(text: str) -> tuple[str | None, str | None]:
    lines = [normalize_text(line) for line in text.splitlines() if normalize_text(line)]
    if not lines:
        return None, None

    filtered = []
    for line in lines[:25]:
        lower = line.lower()
        if lower in {"ver pdf", "criterios y características", "criterios y caracteristicas"}:
            continue
        filtered.append(line)

    if not filtered:
        return None, None

    title = filtered[0]
    title = re.sub(r"^\s*[A-Z]{0,6}\d{1,10}(?:/\d{2,4})?(?:-\d+)?\s*[-:]*\s*", "", title).strip()

    description = None
    if len(filtered) >= 2:
        description = filtered[1]
        if description.lower() == title.lower():
            description = None

    return title or None, description


def extract_structured_fields_from_text(text: str) -> dict:
    publication_date, deadline_date = extract_dates(text)
    estimated_price, currency = extract_price(text)
    title, description = extract_title_and_description(text)

    return {
        "title": title,
        "description": description,
        "publication_date": publication_date,
        "deadline_date": deadline_date,
        "estimated_price": estimated_price,
        "currency": currency,
        "tender_id": extract_tender_id(text),
        "authority_name": extract_authority(text),
        "procedure_type": extract_procedure_type(text),
    }


def save_attachment_record(
    detection_id: int | None,
    source_url: str | None,
    attachment_info: dict,
    extraction_result: dict,
) -> None:
    with get_conn() as conn:
        if detection_id is not None:
            existing = conn.execute(
                """
                SELECT id FROM attachments
                WHERE detection_id = %s AND sha256 = %s
                LIMIT 1
                """,
                (detection_id, attachment_info["sha256"]),
            ).fetchone()
            if existing:
                return

        conn.execute(
            """
            INSERT INTO attachments
            (
                detection_id,
                source_url,
                attachment_url,
                filename,
                extension,
                content_type,
                file_size,
                sha256,
                local_path,
                download_status,
                extracted_text_path,
                parser_used,
                extraction_quality,
                metadata_json,
                created_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s)
            """,
            (
                detection_id,
                source_url,
                attachment_info["attachment_url"],
                attachment_info["filename"],
                attachment_info["extension"],
                attachment_info["content_type"],
                attachment_info["file_size"],
                attachment_info["sha256"],
                None,
                attachment_info["download_status"],
                None,
                extraction_result["parser_used"],
                extraction_result["extraction_quality"],
                json.dumps(extraction_result["metadata"], ensure_ascii=False),
                utc_now_iso(),
            ),
        )


def get_attachments_for_detection(detection_id: int) -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT *
            FROM attachments
            WHERE detection_id = %s
            ORDER BY id DESC
            """,
            (detection_id,),
        ).fetchall()
    return [dict(r) for r in rows]
