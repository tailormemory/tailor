"""Shared fake chromadb.Collection for tests that exercise verified_upsert
and its callers.

Used by:
    tests/test_ingest_helpers.py       — verified_upsert unit tests
    tests/test_mcp_kb_write_paths.py   — mcp_server callsite tests (A1 B2)

Extracted from tests/test_ingest_helpers.py in commit "refactor(tests):
extract FakeCollection to shared helper for cross-file reuse" so a
second test file can import the same mock without duplicating it or
importing across test files (fragile coupling).
"""

from __future__ import annotations


def _is_exception_value(v) -> bool:
    """True if v is an Exception instance or an Exception subclass.

    Shared validator for FakeCollection.program_get / program_upsert: both
    methods accept the same exception forms (instance or class) and route
    them through the same raise path. Anything matching this is suitable
    as a `raise` target — Python's `raise` syntax accepts both forms.
    """
    return isinstance(v, BaseException) or (
        isinstance(v, type) and issubclass(v, BaseException)
    )


class FakeCollection:
    """Fake chromadb.Collection for verified_upsert tests.

    Truth model:
        upsert(ids=[i], embeddings=[e], ...) writes i -> e to self.sql.
        get(ids=[i]) returns {"ids": [i]} iff i is in self.sql,
        else {"ids": []}.

    Silent-drop / failure simulation via .program_get(*responses) and
    .program_upsert(*outcomes). Both enqueue FIFO entries consumed
    one-per-call by the corresponding method; once a queue is exhausted,
    the method reverts to the truthful default.

    Queue entry types:
        FakeCollection.DEFAULT
            sentinel — use truthful behaviour for this single call
            (upsert writes to self.sql; get reads from self.sql).
        Exception INSTANCE or SUBCLASS (e.g. RuntimeError or RuntimeError("boom"))
            the method `raise`s it. Used to simulate chromadb-side
            failures (e.g. SIGSEGV-recovered exception, frozen-collection
            raise variants). Validated by _is_exception_value.
        dict with key "ids" (program_get only)
            returned verbatim from get(). Used to simulate silent-drop
            (`{"ids": []}` = sample missing).

    program_upsert does NOT accept dict outcomes — upsert has no
    user-facing return value, only side-effects.

    Common patterns (used by this file and by A1 retry/backoff tests):
        coll.program_get({"ids": []})
            -> silent-drop only on the first get() call.
        coll.program_get({"ids": []}, FakeCollection.DEFAULT)
            -> drop on call 1, truthful on call 2 (retry recovery).
        coll.program_get({"ids": []}, {"ids": []}, {"ids": []})
            -> drop on first 3 calls (retry=2 exhausted, 3 attempts).
        coll.program_upsert(RuntimeError("boom"), FakeCollection.DEFAULT)
            -> first upsert raises, second succeeds (retry recovery).

    Attributes for assertions:
        sql           dict[id -> embedding]
        upsert_calls  list[dict] — one entry per upsert() with {"ids": [...]},
                      recorded BEFORE the queue is consumed so raising
                      attempts are counted too.
        get_calls     list[list[str]] — one entry per get() with the ids requested,
                      recorded BEFORE the queue is consumed (same rule).
    """

    DEFAULT = object()  # sentinel: queue entry meaning "use truthful default"

    def __init__(self):
        self.sql: dict[str, list[float]] = {}
        self.upsert_calls: list[dict] = []
        self.get_calls: list[list[str]] = []
        self._get_queue: list = []
        self._upsert_queue: list = []

    def program_get(self, *responses) -> None:
        """Enqueue FIFO responses for the next get() calls.

        Each response must be a dict with key "ids", FakeCollection.DEFAULT,
        or an Exception instance/subclass. Anything else raises TypeError
        — fail-fast on malformed test setup.
        """
        for r in responses:
            if r is FakeCollection.DEFAULT:
                self._get_queue.append(r)
                continue
            if isinstance(r, dict) and "ids" in r:
                self._get_queue.append(r)
                continue
            if _is_exception_value(r):
                self._get_queue.append(r)
                continue
            raise TypeError(
                f"program_get: each response must be a dict with key 'ids', "
                f"FakeCollection.DEFAULT, or an Exception instance/subclass; "
                f"got {type(r).__name__}: {r!r}"
            )

    def program_upsert(self, *outcomes) -> None:
        """Enqueue FIFO outcomes for the next upsert() calls.

        Each outcome must be FakeCollection.DEFAULT (truthful write) or
        an Exception instance/subclass (raised from upsert()). Anything
        else raises TypeError — fail-fast on malformed test setup.
        """
        for o in outcomes:
            if o is FakeCollection.DEFAULT or _is_exception_value(o):
                self._upsert_queue.append(o)
                continue
            raise TypeError(
                f"program_upsert: each outcome must be FakeCollection.DEFAULT "
                f"or an Exception instance/subclass; "
                f"got {type(o).__name__}: {o!r}"
            )

    def upsert(self, *, ids, embeddings, documents, metadatas):
        self.upsert_calls.append({"ids": list(ids)})
        if self._upsert_queue:
            outcome = self._upsert_queue.pop(0)
            if outcome is not FakeCollection.DEFAULT:
                raise outcome
        for i, eid in enumerate(ids):
            self.sql[eid] = list(embeddings[i])

    def get(self, *, ids):
        ids_list = list(ids)
        self.get_calls.append(ids_list)
        if self._get_queue:
            resp = self._get_queue.pop(0)
            if resp is FakeCollection.DEFAULT:
                pass  # fall through to truthful default
            elif _is_exception_value(resp):
                raise resp
            else:
                return resp
        present = [i for i in ids_list if i in self.sql]
        return {"ids": present}
