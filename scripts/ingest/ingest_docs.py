"""
TAILOR — 4: Document Ingest (PDF, Excel, CSV, Word, PowerPoint)
Scans configured folders (e.g. OneDrive) and ingests documents into the KB.

Features:
- PDF: extracts text per page via PyMuPDF
- Excel (.xlsx, .xls): extracts text per sheet via openpyxl
- CSV/TSV: extracts text as table
- Word (.docx): extracts text per paragraph + tables via python-docx
- PowerPoint (.pptx): extracts text per slide via python-pptx
- Chunking with v2 logic (target ~1200 chars)
- Incremental: tracks already-processed files (SHA256 hash)
- Source tag: "document" in metadata

Uso:
  python 4_ingest_docs.py              # Incremental ingest
  python 4_ingest_docs.py --full       # Re-ingest all documents
  python 4_ingest_docs.py --list       # Show found files without ingesting
  python 4_ingest_docs.py --status     # Show file status (new/modified/unchanged)

Requirements:
  pip install pymupdf openpyxl requests chromadb python-docx python-pptx

Configuration:
  Edit cloud_sync paths in config/tailor.yaml to point to desired folders.
"""

import csv
import hashlib
import json
import os
import sys
import time
import requests
import chromadb

# ============================================================
# CONFIGURAZIONE (from config/tailor.yaml)
# ============================================================
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "lib"))
from embedding import get_embedding, get_embeddings
from config import get as cfg

# Cartelle da scansionare (ricorsivamente) — from YAML
WATCH_FOLDERS = cfg("ingest", "document_paths") or []

# Estensioni supportate
SUPPORTED_EXTENSIONS = set(cfg("ingest", "supported_extensions") or [".pdf", ".xlsx", ".xls", ".csv", ".tsv", ".docx", ".pptx"])

# Subfolders to ignore (exact names, case-insensitive)
IGNORE_FOLDERS = set(cfg("ingest", "ignore_folders") or ["Food Tracker"])

# Files to ignore (partial patterns in filename)
IGNORE_PATTERNS = set(cfg("ingest", "ignore_patterns") or ["~$", ".tmp", "Thumbs.db", ".DS_Store"])

DB_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "db")
REGISTRY_FILE = os.path.join(DB_DIR, "doc_registry.json")

COLLECTION_NAME = cfg("kb", "collection") or "tailor_kb"

BATCH_SIZE = 10
MAX_TEXT_CHARS = 4000

# Chunking config (aligned with 2_chunk.py v2)
TARGET_CHUNK_CHARS = 1200
MAX_CHUNK_CHARS = 2000
OVERLAP_CHARS = 200


# ============================================================
# FOLDER + DOC TYPE INFERENCE
# ============================================================

import re as _re

# Folder → readable label map (built from cloud_sync config)
def _build_folder_labels():
    try:
        from config import get as _cfg
        labels = {}
        for sync in (_cfg("cloud_sync") or []):
            for folder in (sync.get("folders") or []):
                labels[folder] = folder
        return labels if labels else {}
    except Exception:
        return {}

FOLDER_LABELS = _build_folder_labels()

DOC_TYPE_PATTERNS = [
    # Financial statements and accounting
    (r"nota.integr",                    "nota_integrativa"),
    (r"bilancio.?(d['\s]esercizio|consolidat|abbreviat|annuale|ordinario)", "bilancio"),
    (r"stato.patrimoniale",             "bilancio"),
    (r"conto.economico",                "bilancio"),
    (r"xbrl",                           "bilancio"),
    (r"relazione.?(degli.amministratori|sulla.gestione)", "relazione_gestione"),
    (r"verbale.?(assemblea|cda|consiglio)", "verbale"),
    (r"statuto",                        "statuto"),
    # Fatture e pagamenti
    (r"fattur[ae]",                     "fattura"),
    (r"invoice",                        "fattura"),
    (r"ricevut[ae]",                    "ricevuta"),
    (r"bonifico",                       "pagamento"),
    # Estratti conto e investimenti
    (r"estratto.conto",                 "estratto_conto"),
    (r"rendiconto",                     "estratto_conto"),
    (r"portafoglio|portfolio",          "rendiconto_investimenti"),
    # Contratti e atti legali
    (r"atto.notarile|rogito",           "atto_notarile"),
    (r"contratto.?(di.?)?(locazione|affitto|compravendita|fornitura)", "contratto"),
    (r"scrittura.privata",              "contratto"),
    (r"visura",                         "visura_camerale"),
    (r"sentenza|decreto|ordinanza",     "atto_legale"),
    (r"ricorso|citazione",              "atto_legale"),
    # Tasse e fisco
    (r"dichiarazione.?(dei.redditi|iva|730|unico)", "dichiarazione_fiscale"),
    (r"\bf24\b|\bf24[_\-]|\bf23\b",     "pagamento_fiscale"),
    (r"unico\b",                        "dichiarazione_fiscale"),
    # Salute
    (r"refert[oi]|diagnos|anamnesi",    "referto_medico"),
    (r"esami?.?(del.sangue|clinici|laboratorio)", "referto_medico"),
    (r"cartella.clinica",               "referto_medico"),
    (r"ricetta",                        "prescrizione"),
    # Assicurazioni
    (r"polizza|assicuraz",              "polizza_assicurativa"),
    (r"sinistro|denuncia.sinistro",     "denuncia_sinistro"),
    # Immobili
    (r"catasto|planimetria",            "documento_catastale"),
    (r"mutuo|ipoteca",                  "mutuo"),
    # Presentazioni e piani
    (r"presentazione|pitch|deck|slide", "presentazione"),
    (r"piano.?(integrazione|strategico|operativo|marketing)", "piano"),
]

def infer_folder(filepath):
    """Estrae la cartella radice dal path del documento."""
    parts = filepath.replace("\\", "/").split("/")
    try:
        idx = parts.index("Personale")
        if idx + 1 < len(parts):
            candidate = parts[idx + 1]
            return FOLDER_LABELS.get(candidate, candidate)
    except ValueError:
        pass
    # Fallback: controlla direttamente i WATCH_FOLDERS
    for folder in WATCH_FOLDERS:
        if filepath.startswith(folder):
            return os.path.basename(folder)
    return ""

def infer_doc_type(filename, text_preview=""):
    """Inferisce il tipo documento dal nome file, con fallback sul testo."""
    combined = filename.lower()
    for pattern, doc_type in DOC_TYPE_PATTERNS:
        if _re.search(pattern, combined, _re.IGNORECASE):
            return doc_type
    # Fallback sul testo (primi 500 chars)
    if text_preview:
        preview = text_preview[:500].lower()
        for pattern, doc_type in DOC_TYPE_PATTERNS:
            if _re.search(pattern, preview, _re.IGNORECASE):
                return doc_type
    # Fallback per estensione
    ext = os.path.splitext(filename)[1].lower()
    if ext in (".xlsx", ".xls"):
        return "spreadsheet"
    if ext in (".csv", ".tsv"):
        return "csv"
    if ext == ".docx":
        return "documento_word"
    if ext == ".pptx":
        return "presentazione"
    return "documento"


# ============================================================
# FILE REGISTRY (tracking incrementale)
# ============================================================

def load_registry():
    """Load the registry of already-processed files."""
    if os.path.exists(REGISTRY_FILE):
        with open(REGISTRY_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_registry(registry):
    """Salva il registro."""
    os.makedirs(os.path.dirname(REGISTRY_FILE), exist_ok=True)
    with open(REGISTRY_FILE, "w", encoding="utf-8") as f:
        json.dump(registry, f, indent=2, ensure_ascii=False)


def file_hash(filepath):
    """SHA256 del file per detect modifiche."""
    h = hashlib.sha256()
    with open(filepath, "rb") as f:
        for block in iter(lambda: f.read(65536), b""):
            h.update(block)
    return h.hexdigest()


# ============================================================
# TEXT EXTRACTION
# ============================================================

def _ocr_vision(page):
    """OCR via Apple Vision framework (macOS)."""
    try:
        import Vision
        from Quartz import CGImageSourceCreateWithData, CGImageSourceCreateImageAtIndex
        from Foundation import NSData
        pix = page.get_pixmap(dpi=300)
        png = pix.tobytes("png")
        ns_data = NSData.dataWithBytes_length_(png, len(png))
        src = CGImageSourceCreateWithData(ns_data, None)
        if not src: return ""
        cg = CGImageSourceCreateImageAtIndex(src, 0, None)
        if not cg: return ""
        req = Vision.VNRecognizeTextRequest.alloc().init()
        req.setRecognitionLanguages_(["it", "en"])
        req.setRecognitionLevel_(1)
        req.setUsesLanguageCorrection_(True)
        handler = Vision.VNImageRequestHandler.alloc().initWithCGImage_options_(cg, None)
        success, err = handler.performRequests_error_([req], None)
        if not success: return ""
        results = req.results()
        if not results: return ""
        return "\n".join(r.topCandidates_(1)[0].string() for r in results)
    except Exception:
        return ""


def extract_pdf(filepath):
    """Estrae testo da PDF. Pipeline: testo nativo -> Tesseract -> Apple Vision."""
    import fitz
    sections = []
    ocr_used = False
    vision_used = False
    try:
        doc = fitz.open(filepath)
        for page_num in range(len(doc)):
            page = doc[page_num]
            text = page.get_text().strip()
            if not text or len(text) < 20:
                try:
                    tp = page.get_textpage_ocr(language="ita+eng", dpi=300, full=True)
                    tess_text = page.get_text(textpage=tp).strip()
                    if len(tess_text) > len(text):
                        text = tess_text
                        ocr_used = True
                except Exception: pass
            if not text or len(text) < 20:
                vision_text = _ocr_vision(page)
                if len(vision_text) > len(text):
                    text = vision_text
                    vision_used = True
            if text:
                sections.append({
                    "text": text,
                    "metadata": {"page": page_num + 1, "total_pages": len(doc), "ocr": ocr_used or vision_used}
                })
        doc.close()
        if vision_used:
            print(f"    (Apple Vision OCR applicato su alcune pagine)")
        elif ocr_used:
            print(f"    (Tesseract OCR applicato su alcune pagine)")
    except Exception as e:
        print(f"    ERRORE PDF {filepath}: {e}")
    return sections


def extract_excel(filepath):
    """Estrae testo da Excel, una sezione per sheet. Supporta .xlsx e .xls."""
    ext = os.path.splitext(filepath)[1].lower()
    if ext == ".xls":
        return _extract_xls(filepath)
    else:
        return _extract_xlsx(filepath)


def _extract_xlsx(filepath):
    """Estrae testo da .xlsx via openpyxl."""
    from openpyxl import load_workbook

    sections = []
    try:
        wb = load_workbook(filepath, read_only=True, data_only=True)
        for sheet_name in wb.sheetnames:
            ws = wb[sheet_name]
            rows = []
            for row in ws.iter_rows(values_only=True):
                cells = [str(c) if c is not None else "" for c in row]
                if any(c.strip() for c in cells):
                    rows.append(" | ".join(cells))

            if rows:
                text = f"[Sheet: {sheet_name}]\n" + "\n".join(rows)
                sections.append({
                    "text": text,
                    "metadata": {"sheet": sheet_name, "total_sheets": len(wb.sheetnames), "row_count": len(rows)}
                })
        wb.close()
    except Exception as e:
        print(f"    ERRORE Excel {filepath}: {e}")
    return sections


def _extract_xls(filepath):
    """Estrae testo da .xls via xlrd."""
    import xlrd

    sections = []
    try:
        wb = xlrd.open_workbook(filepath)
        for sheet_name in wb.sheet_names():
            ws = wb.sheet_by_name(sheet_name)
            rows = []
            for row_idx in range(ws.nrows):
                cells = [str(ws.cell_value(row_idx, col_idx)) for col_idx in range(ws.ncols)]
                if any(c.strip() for c in cells):
                    rows.append(" | ".join(cells))

            if rows:
                text = f"[Sheet: {sheet_name}]\n" + "\n".join(rows)
                sections.append({
                    "text": text,
                    "metadata": {"sheet": sheet_name, "total_sheets": wb.nsheets, "row_count": len(rows)}
                })
    except Exception as e:
        print(f"    ERRORE Excel {filepath}: {e}")
    return sections


def extract_csv(filepath):
    """Estrae testo da CSV/TSV."""
    sections = []
    try:
        ext = os.path.splitext(filepath)[1].lower()
        delimiter = "\t" if ext == ".tsv" else ","

        with open(filepath, "r", encoding="utf-8", errors="replace") as f:
            reader = csv.reader(f, delimiter=delimiter)
            rows = []
            for row in reader:
                cells = [str(c).strip() for c in row]
                if any(c for c in cells):
                    rows.append(" | ".join(cells))

        if rows:
            text = "\n".join(rows)
            sections.append({
                "text": text,
                "metadata": {"row_count": len(rows)}
            })
    except Exception as e:
        print(f"    ERRORE CSV {filepath}: {e}")
    return sections


def extract_docx(filepath):
    """Estrae testo da .docx via python-docx. Paragrafi + tabelle in ordine."""
    from docx import Document

    sections = []
    try:
        doc = Document(filepath)
        parts = []

        # Itera sugli elementi del body in ordine di apparizione
        for element in doc.element.body:
            tag = element.tag.split("}")[-1] if "}" in element.tag else element.tag

            if tag == "p":
                text = element.text
                if text and text.strip():
                    parts.append(text.strip())

            elif tag == "tbl":
                for table in doc.tables:
                    if table._element is element:
                        table_rows = []
                        for row in table.rows:
                            cells = [cell.text.strip() for cell in row.cells]
                            if any(c for c in cells):
                                table_rows.append(" | ".join(cells))
                        if table_rows:
                            parts.append("[Tabella]\n" + "\n".join(table_rows))
                        break

        if parts:
            full_text = "\n\n".join(parts)
            sections.append({
                "text": full_text,
                "metadata": {
                    "paragraph_count": len([p for p in doc.paragraphs if p.text.strip()]),
                    "table_count": len(doc.tables),
                }
            })

    except Exception as e:
        print(f"    ERRORE DOCX {filepath}: {e}")
    return sections


def extract_pptx(filepath):
    """Estrae testo da .pptx via python-pptx. Una sezione per slide."""
    from pptx import Presentation

    sections = []
    try:
        prs = Presentation(filepath)
        total_slides = len(prs.slides)

        for slide_num, slide in enumerate(prs.slides, 1):
            parts = []

            # Titolo
            if slide.shapes.title and slide.shapes.title.has_text_frame:
                title_text = slide.shapes.title.text.strip()
                if title_text:
                    parts.append(f"[Titolo] {title_text}")

            # Contenuto shape (escluso titolo)
            for shape in slide.shapes:
                if shape == slide.shapes.title:
                    continue

                if shape.has_text_frame:
                    for paragraph in shape.text_frame.paragraphs:
                        p_text = paragraph.text.strip()
                        if p_text:
                            parts.append(p_text)

                # Tabelle nelle slide
                if shape.has_table:
                    table_rows = []
                    for row in shape.table.rows:
                        cells = [cell.text.strip() for cell in row.cells]
                        if any(c for c in cells):
                            table_rows.append(" | ".join(cells))
                    if table_rows:
                        parts.append("[Tabella]\n" + "\n".join(table_rows))

            # Note speaker
            if slide.has_notes_slide and slide.notes_slide.notes_text_frame:
                notes_text = slide.notes_slide.notes_text_frame.text.strip()
                if notes_text:
                    parts.append(f"[Note] {notes_text}")

            if parts:
                text = f"[Slide {slide_num}/{total_slides}]\n" + "\n".join(parts)
                sections.append({
                    "text": text,
                    "metadata": {
                        "slide_number": slide_num,
                        "total_slides": total_slides,
                    }
                })

    except Exception as e:
        print(f"    ERRORE PPTX {filepath}: {e}")
    return sections


def extract_text(filepath):
    """Router: estrae testo in base all'estensione."""
    ext = os.path.splitext(filepath)[1].lower()
    if ext == ".pdf":
        return extract_pdf(filepath)
    elif ext in (".xlsx", ".xls"):
        return extract_excel(filepath)
    elif ext in (".csv", ".tsv"):
        return extract_csv(filepath)
    elif ext == ".docx":
        return extract_docx(filepath)
    elif ext == ".pptx":
        return extract_pptx(filepath)
    return []


# ============================================================
# CHUNKING v3 (semantic paragraph-first)
# ============================================================

def split_text_at_boundaries(text, max_chars):
    """Spezza un testo lungo in blocchi a confini naturali — v3 (paragraph-first).
    
    Strategia: splitta per paragrafi, raggruppa fino al target, merge frammenti piccoli.
    Rispetta confini semantici naturali invece di tagliare a max_chars e cercare all'indietro.
    """
    import re as _re
    MIN_CHUNK = 300
    
    if len(text) <= max_chars:
        return [text]
    
    # Step 1: split per paragrafi (doppio newline)
    paragraphs = _re.split(r'\n\s*\n', text)
    paragraphs = [p.strip() for p in paragraphs if p.strip()]
    
    if not paragraphs:
        return [text[:max_chars]]
    
    # Step 2: raggruppa paragrafi fino al target
    chunks = []
    current_parts = []
    current_chars = 0
    
    for para in paragraphs:
        para_len = len(para)
        
        # Paragrafo singolo troppo lungo -> spezza a frase
        if para_len > max_chars:
            if current_parts:
                chunks.append('\n\n'.join(current_parts))
                current_parts = []
                current_chars = 0
            remaining = para
            while len(remaining) > max_chars:
                cut = -1
                seg = remaining[:max_chars]
                for sep in ['. ', '.\n', '? ', '! ', '; ']:
                    pos = seg.rfind(sep)
                    if pos > max_chars * 0.3:
                        cut = pos + len(sep)
                        break
                if cut == -1:
                    pos = seg.rfind(' ')
                    cut = pos if pos > max_chars * 0.3 else max_chars
                chunks.append(remaining[:cut].strip())
                remaining = remaining[cut:].strip()
            if remaining:
                current_parts = [remaining]
                current_chars = len(remaining)
            continue
        
        would_be = current_chars + para_len + (2 if current_parts else 0)
        
        if would_be > max_chars and current_chars >= MIN_CHUNK:
            chunks.append('\n\n'.join(current_parts))
            current_parts = [para]
            current_chars = para_len
        else:
            current_parts.append(para)
            current_chars = would_be
    
    if current_parts:
        chunks.append('\n\n'.join(current_parts))
    
    # Step 3: post-hoc merge — chunks too small get merged
    merged = []
    i = 0
    while i < len(chunks):
        chunk = chunks[i]
        while len(chunk) < MIN_CHUNK and i + 1 < len(chunks):
            i += 1
            chunk = chunk + '\n\n' + chunks[i]
        if len(chunk) < MIN_CHUNK and merged:
            merged[-1] = merged[-1] + '\n\n' + chunk
        else:
            merged.append(chunk)
        i += 1
    
    return merged


def chunk_document(sections, file_info):
    """Converte le sezioni estratte in chunk per ChromaDB."""
    chunks = []
    chunk_index = 0
    prev_chunk_text = ""

    # Compute folder and doc_type once per file
    folder = infer_folder(file_info["filepath"])
    text_preview = sections[0]["text"] if sections else ""
    doc_type = infer_doc_type(file_info["filename"], text_preview)

    for section in sections:
        text = section["text"]
        sec_meta = section["metadata"]

        # Spezza sezioni lunghe
        parts = split_text_at_boundaries(text, TARGET_CHUNK_CHARS)

        for part in parts:
            # Aggiungi overlap
            overlap_text = ""
            if prev_chunk_text and OVERLAP_CHARS > 0:
                overlap_text = prev_chunk_text[-OVERLAP_CHARS:].strip()
                space_pos = overlap_text.find(" ")
                if space_pos > 0:
                    overlap_text = overlap_text[space_pos + 1:]

            if overlap_text:
                full_text = f"[...contesto precedente:] {overlap_text}\n\n{part}"
            else:
                full_text = part

            # Genera ID unico
            file_hash_short = file_info["hash"][:12]
            chunk_id = f"doc_{file_hash_short}_chunk_{chunk_index:04d}"

            # Metadata
            meta = {
                "conv_id": f"doc_{file_hash_short}",
                "title": file_info["filename"],
                "date": file_info["modified_date"],
                "create_time": file_info["modified_ts"],
                "default_model": "",
                "chunk_index": chunk_index,
                "char_count": len(full_text),
                "turn_count": 1,
                "source": "document",
                "category": file_info.get("category", ""),
                "folder": folder,
                "doc_type": doc_type,
                "file_type": file_info["extension"],
                "file_path": file_info["rel_path"],
            }
            # Aggiungi metadata sezione-specifici
            meta.update({f"sec_{k}": v for k, v in sec_meta.items()})

            chunks.append({
                "chunk_id": chunk_id,
                "text": full_text,
                "metadata": meta
            })

            prev_chunk_text = part
            chunk_index += 1

    return chunks


# ============================================================
# EMBEDDING + INGEST
# ============================================================



def ingest_chunks(collection, chunks):
    """Scrive chunk in ChromaDB in batch."""
    total = len(chunks)
    processed = 0
    errors = 0

    for batch_start in range(0, total, BATCH_SIZE):
        batch = chunks[batch_start:batch_start + BATCH_SIZE]

        texts = []
        for c in batch:
            folder = c['metadata'].get('folder', '')
            doc_type = c['metadata'].get('doc_type', '')
            title = c['metadata']['title']
            prefix_parts = []
            if folder:
                prefix_parts.append(f"Cartella: {folder}")
            if doc_type:
                prefix_parts.append(f"Tipo: {doc_type}")
            prefix_parts.append(f"File: {title}")
            prefix = " | ".join(prefix_parts)
            texts.append(f"{prefix}\n\n{c['text']}")

        embeddings = get_embeddings(texts)
        if embeddings is None:
            errors += len(batch)
            continue

        ids = [c["chunk_id"] for c in batch]
        documents = [c["text"][:MAX_TEXT_CHARS] for c in batch]
        metadatas = [c["metadata"] for c in batch]

        try:
            collection.upsert(
                ids=ids,
                embeddings=embeddings,
                documents=documents,
                metadatas=metadatas
            )
        except Exception as e:
            print(f"\n  ChromaDB errore: {e}")
            errors += len(batch)
            continue

        processed += len(batch)

    return processed, errors


def delete_file_chunks(collection, file_hash_short):
    """Cancella i chunk di un file specifico dalla collection."""
    try:
        results = collection.get(
            where={"conv_id": f"doc_{file_hash_short}"},
            include=[]
        )
        if results["ids"]:
            collection.delete(ids=results["ids"])
            return len(results["ids"])
    except Exception:
        pass
    return 0


# ============================================================
# FILE DISCOVERY
# ============================================================

def scan_folders():
    """Scansiona le cartelle configurate e restituisce i file supportati."""
    files = []
    for folder in WATCH_FOLDERS:
        if not os.path.exists(folder):
            print(f"  WARNING: folder not found: {folder}")
            continue

        for root, dirs, filenames in os.walk(folder):
            # Escludi sottocartelle in IGNORE_FOLDERS (modifica dirs in-place)
            dirs[:] = [d for d in dirs if d not in IGNORE_FOLDERS]

            for fname in filenames:
                # Ignore temporary files
                if any(p in fname for p in IGNORE_PATTERNS):
                    continue

                ext = os.path.splitext(fname)[1].lower()
                if ext not in SUPPORTED_EXTENSIONS:
                    continue

                filepath = os.path.join(root, fname)
                rel_path = os.path.relpath(filepath, os.path.commonpath(WATCH_FOLDERS + [filepath]))

                # Category from first subfolder
                parts = os.path.relpath(filepath, folder).split(os.sep)
                category = parts[0] if len(parts) > 1 else ""

                try:
                    stat = os.stat(filepath)
                    mod_ts = stat.st_mtime
                    mod_date = time.strftime("%Y-%m-%d", time.localtime(mod_ts))
                    size_kb = stat.st_size / 1024
                except Exception:
                    mod_ts = 0
                    mod_date = ""
                    size_kb = 0

                files.append({
                    "filepath": filepath,
                    "filename": fname,
                    "rel_path": rel_path,
                    "extension": ext,
                    "category": category,
                    "modified_ts": mod_ts,
                    "modified_date": mod_date,
                    "size_kb": size_kb,
                })

    return files


def classify_files(files, registry, full_mode=False):
    """Classifica file in: new, modified, unchanged. Deduplica per hash."""
    new_files = []
    modified_files = []
    unchanged_files = []
    seen_hashes = set()
    duplicates = 0
    permanently_failed = 0

    for f in files:
        fhash = file_hash(f["filepath"])
        f["hash"] = fhash

        # Dedup: skip if same content already seen
        if fhash in seen_hashes:
            duplicates += 1
            continue
        seen_hashes.add(fhash)

        # Skip permanently failed files (3+ extraction failures)
        reg_entry = registry.get(f["filepath"], {})
        if reg_entry.get("status") == "failed" and reg_entry.get("fail_count", 0) >= 3:
            permanently_failed += 1
            print(f"  [SKIP] {f['filename']} — permanently failed after {reg_entry['fail_count']} attempts")
            continue

        if full_mode:
            new_files.append(f)
        elif f["filepath"] not in registry:
            new_files.append(f)
        elif registry[f["filepath"]]["hash"] != fhash:
            modified_files.append(f)
        else:
            unchanged_files.append(f)

    if duplicates > 0:
        print(f"  Duplicates skipped (same content): {duplicates}")
    if permanently_failed > 0:
        print(f"  Permanently failed (skipped): {permanently_failed}")

    return new_files, modified_files, unchanged_files


# ============================================================
# MAIN
# ============================================================

def main():
    full_mode = "--full" in sys.argv
    list_mode = "--list" in sys.argv
    status_mode = "--status" in sys.argv

    print(f"TAILOR Document Ingest")
    print(f"Cartelle monitorate: {len(WATCH_FOLDERS)}")
    for folder in WATCH_FOLDERS:
        exists = "OK" if os.path.exists(folder) else "NON TROVATA"
        print(f"  [{exists}] {folder}")

    # Scan
    print(f"\nScansione file...")
    files = scan_folders()
    print(f"  Trovati {len(files)} file supportati")

    if not files:
        print("No files to process.")
        return

    # List mode: mostra e esci
    if list_mode:
        print(f"\nFile trovati:")
        for f in sorted(files, key=lambda x: x["filepath"]):
            print(f"  [{f['extension']}] {f['filename']} ({f['size_kb']:.0f} KB) - {f['modified_date']}")
            print(f"         {f['filepath']}")
        return

    # Classifica
    registry = load_registry()
    new_files, modified_files, unchanged_files = classify_files(files, registry, full_mode)

    if status_mode:
        print(f"\nStato file:")
        print(f"  Nuovi:      {len(new_files)}")
        print(f"  Modificati: {len(modified_files)}")
        print(f"  Unchanged:  {len(unchanged_files)}")
        for f in new_files:
            print(f"    [NUOVO]      {f['filename']}")
        for f in modified_files:
            print(f"    [MODIFICATO] {f['filename']}")
        return

    to_process = new_files + modified_files
    if not to_process:
        print(f"\nAll files already in KB ({len(unchanged_files)} unchanged). Nothing to do.")
        return

    mode_label = "FULL RE-INGEST" if full_mode else "INCREMENTALE"
    print(f"\nMode: {mode_label}")
    print(f"  Nuovi:      {len(new_files)}")
    print(f"  Modificati: {len(modified_files)}")
    print(f"  Unchanged:  {len(unchanged_files)} (skipped)")

    # Setup
    os.makedirs(DB_DIR, exist_ok=True)
    client = chromadb.PersistentClient(path=DB_DIR)
    collection = client.get_collection(name=COLLECTION_NAME)

    total_chunks_added = 0
    total_errors = 0
    start_time = time.time()

    for idx, f in enumerate(to_process):
        print(f"\n[{idx+1}/{len(to_process)}] {f['filename']} ({f['size_kb']:.0f} KB)")

        # If file was modified, delete old chunks
        if f in modified_files:
            old_hash = registry.get(f["filepath"], {}).get("hash", "")[:12]
            if old_hash:
                deleted = delete_file_chunks(collection, old_hash)
                print(f"  Rimossi {deleted} chunk vecchi")

        # Estrai testo
        sections = extract_text(f["filepath"])
        if not sections:
            # Track extraction failure in registry
            existing = registry.get(f["filepath"], {})
            fail_count = existing.get("fail_count", 0) + 1
            registry[f["filepath"]] = {
                "hash": f["hash"],
                "filename": f["filename"],
                "status": "failed",
                "fail_count": fail_count,
                "date_last_attempt": time.strftime("%Y-%m-%d %H:%M:%S"),
            }
            print(f"  No text extracted, skip (fail_count={fail_count})")
            continue

        total_chars = sum(len(s["text"]) for s in sections)
        print(f"  Estratte {len(sections)} sezioni ({total_chars:,} chars)")

        # Chunk
        chunks = chunk_document(sections, f)
        print(f"  Generati {len(chunks)} chunk")

        # Ingest
        processed, errors = ingest_chunks(collection, chunks)
        total_chunks_added += processed
        total_errors += errors
        print(f"  Ingestati {processed} chunk" + (f" ({errors} errori)" if errors else ""))

        # Aggiorna registry
        registry[f["filepath"]] = {
            "hash": f["hash"],
            "filename": f["filename"],
            "date_ingested": time.strftime("%Y-%m-%d %H:%M:%S"),
            "chunks": len(chunks),
            "chars": total_chars,
            "sections": len(sections),
        }

    # Salva registry
    save_registry(registry)

    elapsed = time.time() - start_time
    print(f"\n{'='*50}")
    print(f"Result:")
    print(f"  File processati: {len(to_process)}")
    print(f"  Chunk aggiunti: {total_chunks_added:,}")
    print(f"  Errori: {total_errors}")
    print(f"  Tempo: {elapsed:.1f}s")
    print(f"  Totale chunk in KB: {collection.count():,}")


if __name__ == "__main__":
    main()