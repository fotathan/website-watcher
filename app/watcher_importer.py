import json
import re
from dataclasses import dataclass, asdict
from typing import Any

from bs4 import BeautifulSoup

from .db import get_conn
from .detail_enricher import enrich_payload_from_start_url
from .utils import normalize_text, stable_hash, utc_now_iso


DATE_RE = re.compile(r"\b(\d{1,2}/\d{1,2}/\d{2,4})(?:\s+(\d{1,2}:\d{2}))?\b")
PRICE_RE = re.compile(
    r"(?P<amount>\d{1,3}(?:[.\s]\d{3})*(?:,\d{2})?|\d+(?:,\d{2})?)\s*(?P<currency>€|EUR|\?)",
    re.IGNORECASE,
)
ID_RE = re.compile(r"\b([A-Z]*\d{1,10}(?:/\d{2,4})?(?:-\d+)?)\b")
MULTI_ITEM_ID_RE = re.compile(r"(?<!\w)(\d{1,10}/\d{2,4}|[A-Z]{0,5}\d{1,10}/\d{2,4})(?!\w)")

TENDER_KEYWORDS = {
    "contrato", "contratación", "contratacion", "licitación", "licitacion", "licitaciones",
    "suministro", "servicio", "servicios", "obra", "obras", "procedimiento", "ofertas",
    "perfil del contratante", "adjudicación", "adjudicacion", "expediente", "expdte",
    "concurso", "subasta", "contrato menor", "contratos menores", "procedimiento abierto",
    "procedimiento abierto simplificado", "anuncio perfil del contratante",
    "prestación de servicios", "prestacion de servicios", "concesión administrativa",
    "concesion administrativa", "contrato mixto", "contratación - obra", "contratacion - obra",
}

IRRELEVANT_KEYWORDS = {
    "no se han encontrado elementos", "consulta ciudadana", "información litúrgica",
    "informacion liturgica", "semana santa", "pleno", "edicto", "tablon de anuncios",
    "aprobación inicial modificación relación puestos de trabajo",
    "aprobacion inicial modificacion relacion puestos de trabajo",
    "audiencia de información pública", "audiencia de informacion publica",
    "audiencias e informaciones públicas", "audiencias e informaciones publicas",
}


@dataclass
class WatcherEntry:
    bookmark_name: str
    scan_date: str | None
    web_page_url: str | None
    local_page_url: str | None
    note: str | None
    highlighted_html: str
    highlighted_text: str
    links: list[str]


@dataclass
class TenderCandidate:
    bookmark_name: str
    source_name: str
    report_date: str | None
    web_page_url: str | None
    local_page_url: str | None
    operator_note: str | None
    is_tender: bool
    confidence: float
    category: str
    tender_id: str | None
    title: str | None
    description: str | None
    publication_date: str | None
    deadline_date: str | None
    authority_name: str | None
    estimated_price: float | None
    currency: str | None
    document_url: str | None
    detail_url: str | None
    procedure_type: str | None
    raw_text: str
    raw_links: list[str]


def _extract_anchor_hrefs(container) -> list[str]:
    links = []
    for a in container.select("a[href]"):
        href = a.get("href")
        if href:
            links.append(href)
    return links


def _clean_bookmark_to_source_name(bookmark_name: str) -> str:
    text = bookmark_name.strip()
    parts = [p.strip() for p in text.split(" - ")]
    core = parts[1] if len(parts) >= 2 else text
    core = re.sub(r"\s+\((?:TODO|LIC\.?|ADJ\.?|Menores|Otros).*$", "", core, flags=re.IGNORECASE)
    return core.strip()


def _extract_header_data(header_td) -> tuple[str, str | None, str | None, str | None]:
    bookmark_name = ""
    scan_date = None
    web_page_url = None
    local_page_url = None

    b = header_td.find("b")
    if b:
        bookmark_name = normalize_text(b.get_text(" ", strip=True))

    header_text = normalize_text(header_td.get_text(" ", strip=True))
    m = re.search(r"\b(\d{2}-\d{2}-\d{4})\b", header_text)
    if m:
        scan_date = m.group(1)

    anchors = header_td.select("a[href]")
    for a in anchors:
        label = normalize_text(a.get_text(" ", strip=True)).lower()
        href = a.get("href")
        if label == "web page":
            web_page_url = href
        elif label == "local page":
            local_page_url = href

    return bookmark_name, scan_date, web_page_url, local_page_url


def parse_watcher_report_from_html(html: str) -> list[WatcherEntry]:
    soup = BeautifulSoup(html, "lxml")
    tables = soup.find_all("table", attrs={"width": "100%"})
    entries: list[WatcherEntry] = []

    for table in tables:
        rows = table.find_all("tr")
        if len(rows) < 3:
            continue

        header_td = rows[0].find("td")
        note_td = rows[1].find("td") if len(rows) > 1 else None
        body_td = rows[2].find("td") if len(rows) > 2 else None

        if not header_td or not body_td:
            continue

        bookmark_name, scan_date, web_page_url, local_page_url = _extract_header_data(header_td)
        if not bookmark_name:
            continue

        note = normalize_text(note_td.get_text(" ", strip=True)) if note_td else None
        if note == "":
            note = None

        blockquote = body_td.find("blockquote")
        if blockquote:
            highlighted_html = str(blockquote)
            highlighted_text = normalize_text(blockquote.get_text("\n", strip=True))
            links = _extract_anchor_hrefs(blockquote)
        else:
            highlighted_html = str(body_td)
            highlighted_text = normalize_text(body_td.get_text("\n", strip=True))
            links = _extract_anchor_hrefs(body_td)

        entries.append(
            WatcherEntry(
                bookmark_name=bookmark_name,
                scan_date=scan_date,
                web_page_url=web_page_url,
                local_page_url=local_page_url,
                note=note,
                highlighted_html=highlighted_html,
                highlighted_text=highlighted_text,
                links=links,
            )
        )
    return entries


def _detect_category(text: str) -> str:
    lower = text.lower()
    if any(x in lower for x in ["adjudicación", "adjudicacion", "formalización", "formalizacion", "fase de fiscalización", "fase de fiscalizacion"]):
        return "award_notice"
    if any(x in lower for x in ["prórroga", "prorroga", "convenio"]):
        return "procurement_update"
    if any(x in lower for x in ["contrato menor", "contratos menores"]):
        return "minor_contract"
    if any(x in lower for x in ["licitación", "licitacion", "procedimiento abierto", "ofertas", "suministro", "obra", "servicio"]):
        return "tender_notice"
    return "unknown"


def _score_tender_likelihood(text: str, note: str | None = None) -> tuple[bool, float, str]:
    lower = text.lower()
    score = 0.0

    if not lower or lower.strip() == "":
        return False, 0.01, "irrelevant"
    if any(k in lower for k in IRRELEVANT_KEYWORDS):
        return False, 0.02, "irrelevant"
    if "no se han encontrado elementos" in lower:
        return False, 0.01, "irrelevant"

    keyword_hits = sum(1 for k in TENDER_KEYWORDS if k in lower)
    score += min(keyword_hits * 0.18, 0.72)

    if ID_RE.search(text):
        score += 0.10

    dates = DATE_RE.findall(text)
    if len(dates) >= 1:
        score += 0.08
    if len(dates) >= 2:
        score += 0.10

    if PRICE_RE.search(text):
        score += 0.10

    if "ver pdf" in lower or ".pdf" in lower:
        score += 0.08

    if note:
        note_lower = note.lower()
        if "contratos menores" in note_lower:
            score += 0.06
        if "no incluir" in note_lower and "adjudicación" in lower:
            score -= 0.10
        if "clicar en" in note_lower and len(text) < 40:
            score -= 0.20

    category = _detect_category(text)
    is_tender = score >= 0.35
    confidence = max(0.01, min(score, 0.99))
    return is_tender, confidence, category


def _extract_authority_name(entry: WatcherEntry) -> str | None:
    return _clean_bookmark_to_source_name(entry.bookmark_name)


def _extract_tender_id(text: str) -> str | None:
    matches = MULTI_ITEM_ID_RE.findall(text)
    if matches:
        return matches[0]
    m = ID_RE.search(text)
    return m.group(1) if m else None


def _extract_dates(text: str) -> tuple[str | None, str | None]:
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


def _extract_price(text: str) -> tuple[float | None, str | None]:
    m = PRICE_RE.search(text)
    if not m:
        return None, None

    raw_amount = m.group("amount").replace(".", "").replace(" ", "").replace(",", ".")
    try:
        amount = float(raw_amount)
    except Exception:
        amount = None

    currency = m.group("currency")
    if currency == "?":
        currency = None
    return amount, currency


def _extract_document_url(entry: WatcherEntry) -> str | None:
    for link in entry.links:
        if ".pdf" in link.lower():
            return link
    return None


def _extract_detail_url(entry: WatcherEntry) -> str | None:
    if not entry.links:
        return None
    pdf = _extract_document_url(entry)
    for link in entry.links:
        if link != pdf:
            return link
    return entry.links[0]


def _extract_procedure_type(text: str) -> str | None:
    lower = text.lower()
    candidates = [
        "procedimiento abierto simplificado abreviado",
        "procedimiento abierto simplificado",
        "procedimiento abierto",
        "contrato menor",
        "contrato mixto",
        "concesión administrativa",
        "concesion administrativa",
        "subasta",
        "concurso público",
        "concurso publico",
    ]
    for c in candidates:
        if c in lower:
            return c
    return None


def _extract_title_and_description(text: str) -> tuple[str | None, str | None]:
    compact = normalize_text(text)
    compact = re.sub(r"^\s*[A-Z]*\d{1,10}(?:/\d{2,4})?(?:-\d+)?\s*-\s*", "", compact)

    parts = [p.strip(" -") for p in re.split(r"\s*-\s*", compact) if p.strip()]
    if not parts:
        return None, None

    title = parts[0]
    description = None
    if len(parts) >= 2:
        description = " - ".join(parts[1:]).strip()
        if description.lower() == title.lower():
            description = None

    return title, description


def _split_multi_item_text(text: str) -> list[str]:
    parts = [normalize_text(p) for p in re.split(r"\n\s*\n|<br\s*/?><br\s*/?>", text) if normalize_text(p)]
    if len(parts) <= 1:
        parts = [p.strip() for p in re.split(r"\s{2,}", text) if p.strip()]

    if len(parts) == 1:
        candidates = []
        tokens = re.split(r"(?=(?:[A-Z]*\d{1,10}/\d{2,4}))", text)
        for tok in tokens:
            tok = normalize_text(tok)
            if tok:
                candidates.append(tok)
        if len(candidates) > 1:
            parts = candidates

    cleaned = [normalize_text(p) for p in parts if normalize_text(p)]
    return cleaned if cleaned else [normalize_text(text)]


def split_entry_into_candidates(entry: WatcherEntry) -> list[str]:
    text = entry.highlighted_text
    if not text:
        return []
    if "no se han encontrado elementos" in text.lower():
        return [text]

    parts = _split_multi_item_text(text)
    if len(parts) == 1:
        ids = MULTI_ITEM_ID_RE.findall(text)
        if len(ids) >= 2:
            rough = re.split(r"(?=(?:[A-Z]*\d{1,10}/\d{2,4}\s*-))", text)
            rough = [normalize_text(x) for x in rough if normalize_text(x)]
            if len(rough) >= 2:
                parts = rough
    return parts


def build_candidate(entry: WatcherEntry, candidate_text: str) -> TenderCandidate:
    is_tender, confidence, category = _score_tender_likelihood(candidate_text, entry.note)
    tender_id = _extract_tender_id(candidate_text)
    publication_date, deadline_date = _extract_dates(candidate_text)
    estimated_price, currency = _extract_price(candidate_text)
    title, description = _extract_title_and_description(candidate_text)
    authority_name = _extract_authority_name(entry)
    document_url = _extract_document_url(entry)
    detail_url = _extract_detail_url(entry)
    procedure_type = _extract_procedure_type(candidate_text)

    if title and len(title) < 6 and description:
        title = description[:180]

    return TenderCandidate(
        bookmark_name=entry.bookmark_name,
        source_name=authority_name or entry.bookmark_name,
        report_date=entry.scan_date,
        web_page_url=entry.web_page_url,
        local_page_url=entry.local_page_url,
        operator_note=entry.note,
        is_tender=is_tender,
        confidence=round(confidence, 2),
        category=category,
        tender_id=tender_id,
        title=title,
        description=description,
        publication_date=publication_date,
        deadline_date=deadline_date,
        authority_name=authority_name,
        estimated_price=estimated_price,
        currency=currency,
        document_url=document_url,
        detail_url=detail_url,
        procedure_type=procedure_type,
        raw_text=candidate_text,
        raw_links=entry.links,
    )


def parse_candidates_from_report_html(html: str) -> list[TenderCandidate]:
    entries = parse_watcher_report_from_html(html)
    candidates: list[TenderCandidate] = []
    for entry in entries:
        parts = split_entry_into_candidates(entry) or [entry.highlighted_text]
        for part in parts:
            part = normalize_text(part)
            if part:
                candidates.append(build_candidate(entry, part))
    return candidates


def parse_candidates_from_uploaded_bytes(file_bytes: bytes) -> list[TenderCandidate]:
    html = file_bytes.decode("utf-8", errors="ignore")
    return parse_candidates_from_report_html(html)


def deep_enrich_candidates(candidates: list[TenderCandidate], enrich_only_tenders: bool = True, min_confidence: float = 0.6) -> list[TenderCandidate]:
    enriched: list[TenderCandidate] = []
    for cand in candidates:
        payload = asdict(cand)
        should_enrich = True
        if enrich_only_tenders:
            should_enrich = cand.is_tender and cand.confidence >= min_confidence

        if should_enrich:
            try:
                payload = enrich_payload_from_start_url(payload)
            except Exception:
                pass

        enriched.append(TenderCandidate(
            bookmark_name=payload.get("bookmark_name"),
            source_name=payload.get("source_name"),
            report_date=payload.get("report_date"),
            web_page_url=payload.get("web_page_url"),
            local_page_url=payload.get("local_page_url"),
            operator_note=payload.get("operator_note"),
            is_tender=payload.get("is_tender"),
            confidence=payload.get("confidence"),
            category=payload.get("category"),
            tender_id=payload.get("tender_id"),
            title=payload.get("title"),
            description=payload.get("description"),
            publication_date=payload.get("publication_date"),
            deadline_date=payload.get("deadline_date"),
            authority_name=payload.get("authority_name"),
            estimated_price=payload.get("estimated_price"),
            currency=payload.get("currency"),
            document_url=payload.get("document_url"),
            detail_url=payload.get("detail_url"),
            procedure_type=payload.get("procedure_type"),
            raw_text=payload.get("raw_text"),
            raw_links=payload.get("raw_links"),
        ))
        # extra enriched keys remain accessible in __dict__ after assignment below
        enriched[-1].__dict__.update({k: v for k, v in payload.items() if k not in enriched[-1].__dict__})
    return enriched


def ensure_source_exists(source_name: str, web_page_url: str | None) -> int:
    with get_conn() as conn:
        existing = conn.execute(
            "SELECT id FROM sources WHERE name = %s",
            (source_name,),
        ).fetchone()
        if existing:
            return existing["id"]

        cursor = conn.execute(
            """
            INSERT INTO sources
            (name, url, main_selector, item_selector, detail_link_selector, language, poll_minutes, active)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (source_name, web_page_url or "", None, None, None, "es", 0, 0),
        )
        return cursor.fetchone()["id"]


def import_candidates_into_detections(candidates: list[TenderCandidate], include_irrelevant: bool = False) -> dict[str, Any]:
    inserted = 0
    skipped_duplicates = 0
    skipped_irrelevant = 0

    for cand in candidates:
        if not include_irrelevant and not cand.is_tender:
            skipped_irrelevant += 1
            continue

        source_id = ensure_source_exists(cand.source_name, cand.web_page_url)
        payload = cand.__dict__.copy()
        raw_text = payload["raw_text"]
        block_hash = stable_hash(f"{cand.source_name}::{raw_text}")

        with get_conn() as conn:
            existing = conn.execute(
                """
                SELECT 1 FROM detections
                WHERE source_id = %s AND block_hash = %s
                LIMIT 1
                """,
                (source_id, block_hash),
            ).fetchone()
            if existing:
                skipped_duplicates += 1
                continue

            conn.execute(
                """
                INSERT INTO detections
                (source_id, block_hash, detected_at, is_relevant, confidence, content_type, classifier_reason, extracted_json, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s)
                """,
                (
                    source_id,
                    block_hash,
                    utc_now_iso(),
                    int(cand.is_tender),
                    float(cand.confidence),
                    cand.category,
                    "Imported from Website Watcher report",
                    json.dumps(payload, ensure_ascii=False),
                    "new",
                ),
            )
            inserted += 1

    return {
        "inserted": inserted,
        "skipped_duplicates": skipped_duplicates,
        "skipped_irrelevant": skipped_irrelevant,
        "total_candidates": len(candidates),
    }
