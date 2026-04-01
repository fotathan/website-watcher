import requests
from urllib.parse import urljoin
from bs4 import BeautifulSoup

from .attachment_extractor import (
    download_file_bytes,
    extract_pdf_text_from_bytes,
    extract_structured_fields_from_text,
)
from .utils import normalize_text, utc_now_iso


PDF_TEXT_HINTS = {
    "pdf", "pliego", "pliegos", "anuncio", "documentación", "documentacion",
    "bases", "expediente", "memoria", "licitación", "licitacion", "descargar",
    "descarga", "documento", "document", "download", "archivo", "adjunto",
    "fichero", "condiciones", "prescripciones", "técnicas", "tecnicas",
}

PDF_URL_HINTS = {
    ".pdf", "pdf", "download", "descarga", "document", "documento",
    "archivo", "adjunto", "pliego", "anuncio", "expediente", "memoria",
}


def _pick_start_url(payload: dict) -> str | None:
    for key in ["detail_url", "document_url", "web_page_url", "source_url"]:
        value = payload.get(key)
        if isinstance(value, str) and value.startswith(("http://", "https://")):
            return value
    return None


def _is_pdf_url(url: str) -> bool:
    lower = url.lower()
    return lower.endswith(".pdf") or ".pdf?" in lower


def _looks_like_document_link(url: str, link_text: str, nearby_text: str) -> int:
    score = 0
    lower_url = (url or "").lower()
    lower_text = (link_text or "").lower()
    lower_nearby = (nearby_text or "").lower()

    if _is_pdf_url(lower_url):
        score += 10

    for hint in PDF_URL_HINTS:
        if hint in lower_url:
            score += 2

    for hint in PDF_TEXT_HINTS:
        if hint in lower_text:
            score += 3
        if hint in lower_nearby:
            score += 1

    if any(ext in lower_url for ext in [".ashx", ".aspx", ".php", ".do", ".jsp"]):
        score += 1

    if "download" in lower_url or "descarga" in lower_url:
        score += 3

    return score


def _head_or_get_content_type(url: str) -> dict:
    headers = {"User-Agent": "Mozilla/5.0"}

    try:
        r = requests.head(url, allow_redirects=True, timeout=20, headers=headers)
        return {
            "final_url": r.url,
            "content_type": (r.headers.get("Content-Type", "") or "").lower(),
            "content_disposition": (r.headers.get("Content-Disposition", "") or "").lower(),
        }
    except Exception:
        pass

    try:
        r = requests.get(url, allow_redirects=True, timeout=20, headers=headers, stream=True)
        result = {
            "final_url": r.url,
            "content_type": (r.headers.get("Content-Type", "") or "").lower(),
            "content_disposition": (r.headers.get("Content-Disposition", "") or "").lower(),
        }
        r.close()
        return result
    except Exception:
        return {"final_url": url, "content_type": "", "content_disposition": ""}


def fetch_html_page(url: str) -> dict:
    response = requests.get(url, timeout=60, headers={"User-Agent": "Mozilla/5.0"})
    response.raise_for_status()

    html = response.text
    soup = BeautifulSoup(html, "lxml")
    text = normalize_text(soup.get_text("\n", strip=True))

    return {
        "url": response.url,
        "html": html,
        "text": text,
        "title": soup.title.get_text(strip=True) if soup.title else None,
    }


def extract_pdf_links_from_html(html: str, base_url: str) -> list[str]:
    soup = BeautifulSoup(html, "lxml")
    candidates = []

    for a in soup.select("a[href]"):
        href = a.get("href")
        if not href:
            continue

        full_url = urljoin(base_url, href)
        link_text = normalize_text(a.get_text(" ", strip=True))
        parent_text = normalize_text(a.parent.get_text(" ", strip=True)) if a.parent else ""

        score = _looks_like_document_link(full_url, link_text, parent_text)
        lower_url = full_url.lower()
        if lower_url.startswith("javascript:") or lower_url.startswith("mailto:"):
            continue
        if score < 2:
            continue

        probe = _head_or_get_content_type(full_url)
        final_url = probe["final_url"] or full_url
        content_type = probe["content_type"]
        content_disposition = probe["content_disposition"]

        is_pdf = False
        if "application/pdf" in content_type:
            is_pdf = True
            score += 10
        if ".pdf" in final_url.lower():
            is_pdf = True
            score += 5
        if ".pdf" in content_disposition:
            is_pdf = True
            score += 5

        candidates.append({"url": final_url, "score": score, "is_pdf": is_pdf})

    dedup = {}
    for item in candidates:
        url = item["url"]
        if url not in dedup or item["score"] > dedup[url]["score"]:
            dedup[url] = item

    ranked = sorted(dedup.values(), key=lambda x: x["score"], reverse=True)
    selected = []
    for item in ranked:
        if item["is_pdf"] or item["score"] >= 6:
            selected.append(item["url"])

    return list(dict.fromkeys(selected))


def _merge_fields(base: dict, new: dict, overwrite: bool = False) -> dict:
    result = dict(base)
    for key, value in new.items():
        if value in (None, "", []):
            continue
        if overwrite or not result.get(key):
            result[key] = value
    return result


def enrich_payload_from_start_url(payload: dict) -> dict:
    start_url = _pick_start_url(payload)
    if not start_url:
        return payload

    enriched = dict(payload)

    if _is_pdf_url(start_url):
        try:
            attachment = download_file_bytes(start_url)
            extraction = extract_pdf_text_from_bytes(attachment["bytes"], attachment["filename"])
            fields = extract_structured_fields_from_text(extraction["text"])

            enriched = _merge_fields(enriched, fields, overwrite=True)
            attachments = enriched.get("attachments")
            if not isinstance(attachments, list):
                attachments = []
            attachments.append(
                {
                    "url": start_url,
                    "quality": extraction["extraction_quality"],
                    "parser_used": extraction["parser_used"],
                    "text_length": extraction["metadata"]["text_length"],
                }
            )
            enriched["attachments"] = attachments
            enriched["detail_enriched"] = True
            enriched["detail_enriched_at"] = utc_now_iso()
        except Exception:
            pass

        return enriched

    try:
        page = fetch_html_page(start_url)
    except Exception:
        return enriched

    html_fields = extract_structured_fields_from_text(page["text"])
    pdf_links = extract_pdf_links_from_html(page["html"], page["url"])

    enriched = _merge_fields(enriched, html_fields)
    enriched["detail_page_url"] = page["url"]
    enriched["detail_page_text_length"] = len(page["text"])
    enriched["detail_page_pdf_links"] = pdf_links
    enriched["detail_page_title"] = page.get("title")
    enriched["detail_enriched"] = True
    enriched["detail_enriched_at"] = utc_now_iso()

    pdf_results = []
    attachments = enriched.get("attachments")
    if not isinstance(attachments, list):
        attachments = []

    for pdf_url in pdf_links:
        try:
            attachment = download_file_bytes(pdf_url)
            content_type = (attachment.get("content_type") or "").lower()
            extension = (attachment.get("extension") or "").lower()
            if extension != "pdf" and "application/pdf" not in content_type:
                pdf_results.append({"url": pdf_url, "status": "skipped_not_pdf_after_download"})
                continue

            extraction = extract_pdf_text_from_bytes(attachment["bytes"], attachment["filename"])
            pdf_fields = extract_structured_fields_from_text(extraction["text"])

            enriched = _merge_fields(enriched, pdf_fields, overwrite=True)
            attachments.append(
                {
                    "url": pdf_url,
                    "quality": extraction["extraction_quality"],
                    "parser_used": extraction["parser_used"],
                    "text_length": extraction["metadata"]["text_length"],
                }
            )
            pdf_results.append(
                {
                    "url": pdf_url,
                    "status": "processed",
                    "quality": extraction["extraction_quality"],
                    "text_length": extraction["metadata"]["text_length"],
                }
            )
        except Exception as e:
            pdf_results.append({"url": pdf_url, "status": "error", "error": str(e)})

    enriched["attachments"] = attachments
    enriched["detail_page_pdf_results"] = pdf_results
    return enriched
