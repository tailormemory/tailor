"""Contratto unico per il testo da embeddare.

Il corpus e' 98.5% "nudo" (testo grezzo senza header/prefissi di source):
misurato su campione n=1630, discriminante esatto-1.0. La query lato
retrieval e' gia' embeddata nuda. Quindi l'unico comportamento corretto
e coerente e': embeddare il documento nudo, troncato.

Ogni ramo su `source` sarebbe uno schema implicito: due documenti identici
embeddati diversamente a seconda della provenienza -> asimmetria query/corpus
-> drift silenzioso del retrieval. Per questo NON esistono rami.
"""

MAX_EMBED_CHARS = 4000


def embedding_text(source: str, meta: dict, document: str) -> str:
    """Unica funzione che decide il testo da embeddare.

    Pura: nessun I/O, nessuno stato, stesso input -> stesso output.
    Comportamento unico: testo nudo, troncato a MAX_EMBED_CHARS.

    `source` e `meta` fanno parte della firma per stabilita' dei chiamanti
    (non devono cambiare call-site quando in futuro servisse contesto), ma
    NON sono usati. Questo e' intenzionale: ogni ramo su `source` sarebbe
    un futuro schema implicito che romperebbe la simmetria con la query,
    gia' embeddata nuda. Il corpus e' 98.5% nudo (campione n=1630,
    discriminante esatto-1.0): il ramo unico e' anche quello misurato.
    """
    if not document:
        return ""
    return document[:MAX_EMBED_CHARS]
