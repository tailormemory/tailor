"""
Semantic chunking v3 — paragraph-first with intelligent grouping.

Replaces split_text_at_boundaries() and chunk_document() in ingest_docs.py.

Strategy:
1. Split text into "natural blocks" (paragraphs, sections, headings)
2. Group consecutive blocks up to TARGET without breaking a paragraph
3. If a single paragraph exceeds MAX, split at sentence boundary
4. Textual overlap between consecutive chunks for context

Differences from v2:
- v2: cuts at TARGET_CHARS and looks for the nearest boundary backwards
- v3: starts from natural boundaries and groups forward up to TARGET
- v3: respects headings/section titles as hard boundaries
- v3: more semantically coherent chunks, fewer mid-concept splits
"""

import re

TARGET_CHUNK_CHARS = 1200
MAX_CHUNK_CHARS = 2000
MIN_CHUNK_CHARS = 200
OVERLAP_CHARS = 200


def detect_section_breaks(text):
    """Identifica i punti di rottura naturali nel testo.
    
    Returns a list of blocks, each a dict with:
    - text: il testo del blocco
    - is_heading: True se sembra un titolo di sezione
    """
    blocks = []
    
    # Split per doppio newline (paragrafi)
    raw_blocks = re.split(r'\n\s*\n', text)
    
    for block in raw_blocks:
        block = block.strip()
        if not block:
            continue
        
        # Identifica heading: righe brevi, maiuscole, numerate, o con pattern tipici
        is_heading = False
        lines = block.split('\n')
        first_line = lines[0].strip()
        
        if len(first_line) < 80 and first_line:
            # Pattern heading: "Art. 4", "SECTION 2", "1.2 REPORTING", "TITOLO I", numeri + punto + testo
            heading_patterns = [
                r'^(Art\.|ARTICLE|Section|SECTION|TITOLO|TITLE|CAPO|CHAPTER)\s',
                r'^\d+[\.\)]\s+[A-Z]',          # "1. SERVICES", "2) TERM"
                r'^[A-Z][A-Z\s]{5,}$',            # "TERMINATION FOR CAUSE"
                r'^[IVXLC]+[\.\)]\s',              # "IV. PENALTIES"
                r'^\d+\.\d+\s+[A-Z]',             # "1.2 REPORTING"
            ]
            for pattern in heading_patterns:
                if re.match(pattern, first_line):
                    is_heading = True
                    break
        
        blocks.append({
            'text': block,
            'is_heading': is_heading,
            'char_count': len(block)
        })
    
    return blocks


def split_at_sentences(text, max_chars):
    """Spezza un testo lungo a confini di frase. Fallback per paragrafi troppo lunghi."""
    if len(text) <= max_chars:
        return [text]
    
    # Split per frase (punto + spazio + maiuscola, o punto + newline)
    sentences = re.split(r'(?<=[.!?])\s+(?=[A-Z])|(?<=[.!?])\n', text)
    
    segments = []
    current = ""
    
    for sentence in sentences:
        if not sentence.strip():
            continue
        if len(current) + len(sentence) + 1 > max_chars and current:
            segments.append(current.strip())
            current = sentence
        else:
            current = current + " " + sentence if current else sentence
    
    if current.strip():
        segments.append(current.strip())
    
    # Fallback: if a single sentence exceeds max_chars, split at space
    final = []
    for seg in segments:
        if len(seg) <= max_chars:
            final.append(seg)
        else:
            # Taglio duro a spazio
            remaining = seg
            while len(remaining) > max_chars:
                pos = remaining[:max_chars].rfind(' ')
                if pos < max_chars * 0.3:
                    pos = max_chars
                final.append(remaining[:pos].strip())
                remaining = remaining[pos:].strip()
            if remaining:
                final.append(remaining)
    
    return final


def semantic_chunk_text(text):
    """Chunking semantico: splitta per paragrafi, raggruppa fino al target.
    
    Returns a list of strings (the chunks).
    """
    if not text or not text.strip():
        return []
    
    text = text.strip()
    
    # Caso banale: testo corto
    if len(text) <= TARGET_CHUNK_CHARS:
        return [text]
    
    # Step 1: identifica blocchi naturali
    blocks = detect_section_breaks(text)
    
    if not blocks:
        return [text[:TARGET_CHUNK_CHARS]]
    
    # Step 2: group blocks into chunks
    chunks = []
    current_parts = []
    current_chars = 0
    
    for i, block in enumerate(blocks):
        block_text = block['text']
        block_chars = block['char_count']
        
        # Se il blocco singolo supera MAX, spezzalo a frasi
        if block_chars > MAX_CHUNK_CHARS:
            # First, close current chunk if not empty
            if current_parts:
                chunks.append('\n\n'.join(current_parts))
                current_parts = []
                current_chars = 0
            
            # Spezza il blocco lungo
            sub_parts = split_at_sentences(block_text, TARGET_CHUNK_CHARS)
            for sp in sub_parts:
                chunks.append(sp)
            continue
        
        # Heading = hard boundary (close current chunk and start new)
        if block['is_heading'] and current_parts and current_chars > MIN_CHUNK_CHARS:
            chunks.append('\n\n'.join(current_parts))
            current_parts = []
            current_chars = 0
        
        # Add block to current chunk
        would_be = current_chars + block_chars + (2 if current_parts else 0)  # +2 per \n\n
        
        if would_be > TARGET_CHUNK_CHARS and current_parts:
            # Current chunk is full enough, close it
            chunks.append('\n\n'.join(current_parts))
            current_parts = [block_text]
            current_chars = block_chars
        else:
            current_parts.append(block_text)
            current_chars = would_be
    
    # Close the last chunk
    if current_parts:
        last_chunk = '\n\n'.join(current_parts)
        # If the last chunk is too small, merge it with the previous one
        if len(last_chunk) < MIN_CHUNK_CHARS and chunks:
            chunks[-1] = chunks[-1] + '\n\n' + last_chunk
        else:
            chunks.append(last_chunk)
    
    return chunks


# ============================================================
# TEST
# ============================================================

if __name__ == "__main__":
    import sys
    
    # Test con testo di esempio
    test_text = """EXECUTION COPY

1&1 Mail & Media GmbH

This AGREEMENT shall be effective from the 1. January 2016 ("the Effective Date")

PARTIES:

1&1 Mail & Media GmbH, a company with limited liability, incorporated under the laws of Germany, with its registered office at Elgendorfer Str. 57, 56410 Montabaur, Germany ("1&1").

Acme Corp Ltd., a company incorporated in Ireland, with its registered office at 123 Business Park, Dublin 8, Ireland ("PARTNER").

1. SERVICES

1.1 SERVICE DESCRIPTION
PARTNER shall provide online media and marketing services including search to 1&1 for the purposes of sending traffic to 1&1's Advertisers, together with such related consultancy, design and technical services as may be required by 1&1 from time to time.

PARTNER must inform 1&1 prior to the implementation of traffic from third party sources. These third party sources need a prior written approval from 1&1. PARTNER shall provide 1&1 with copies of all relevant third party agreements and will notify 1&1 within ten business days about any changes to or termination of these third party agreements.

1.2 REPORTING
1&1 will provide to PARTNER direct access to the relevant revenue data during the term of this agreement.

2. TERM AND TERMINATION

2.1 TERM
The initial term of this Agreement shall be for twelve months commencing on the Effective Date.

2.2 EXTENSION
This Agreement may be extended on the same terms and conditions for additional twelve months upon written notice.

2.3 TERMINATION FOR CAUSE
Either party may terminate this Agreement with immediate effect upon written notice if the other party commits a material breach.

3. PAYMENT

3.1 REVENUE SHARE
1&1 shall pay PARTNER a revenue share of 85% of net revenue generated from traffic sent by PARTNER.

3.2 PAYMENT TERMS
Payment shall be made within 45 days after the end of each calendar month."""

    chunks = semantic_chunk_text(test_text)
    
    print(f"Input: {len(test_text)} chars")
    print(f"Output: {len(chunks)} chunk\n")
    
    for i, chunk in enumerate(chunks):
        print(f"=== Chunk {i} ({len(chunk)} chars) ===")
        print(chunk[:200])
        if len(chunk) > 200:
            print("[...]")
            print(chunk[-100:])
        print()
