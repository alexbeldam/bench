"""Minimal engine contract: the smallest interface the harness needs.

Pure standard library: nothing here imports a database client.

Operations: index create / update / drop, write and delete sync, query, readiness, capability declaration.
Every engine returns hits in a *total order*: the query's explicit sort keys (score counts as a key), then the
document id ascending.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable

# ---- errors (named, never carry field values: -------------------------------------------------------


class KitTestError(Exception):
    """Base class of the harness's named errors."""


class IndexNeverQueryableError(KitTestError):
    def __init__(self, entity: str, index: str, timeout: float, status: str) -> None:
        super().__init__(f"index {index!r} of {entity!r} not queryable after {timeout:.1f}s (last status: {status})")
        self.entity, self.index, self.timeout, self.status = entity, index, timeout, status


class DocumentNeverSearchableError(KitTestError):
    def __init__(self, entity: str, index: str, doc_id: Any, timeout: float) -> None:
        super().__init__(f"document {doc_id!s} of {entity!r} not searchable in {index!r} after {timeout:.1f}s")
        self.entity, self.index, self.doc_id, self.timeout = entity, index, doc_id, timeout


class DocumentNeverUnsearchableError(DocumentNeverSearchableError):
    def __init__(self, entity: str, index: str, doc_id: Any, timeout: float) -> None:
        KitTestError.__init__(
            self, f"document {doc_id!s} of {entity!r} still in {index!r} after {timeout:.1f}s (delete not visible)"
        )
        self.entity, self.index, self.doc_id, self.timeout = entity, index, doc_id, timeout


class UnsupportedClauseError(KitTestError):
    def __init__(self, engine: str, clause: str) -> None:
        super().__init__(f"engine {engine!r} does not support clause {clause!r}")


class IndexNotFoundError(KitTestError):
    def __init__(self, index: str) -> None:
        super().__init__(f"index {index!r} does not exist")


# ---- index definition -----------------------------------------------------------------------------------------


@dataclass(frozen=True)
class FieldDef:
    """kind: 'string' (analyzed text), 'token' (exact string), 'number', 'date', 'boolean', 'objectId'."""

    path: str
    kind: str


@dataclass(frozen=True)
class IndexDef:
    name: str
    fields: tuple[FieldDef, ...]

    def kind_of(self, path: str) -> str | None:
        for f in self.fields:
            if f.path == path:
                return f.kind
        return None


# ---- query model (common clause set, -------------------------------------------------------


@dataclass(frozen=True)
class Text:
    query: str
    paths: tuple[str, ...]
    max_edits: int = 0  # 0 = exact, 1 or 2 = fuzzy


@dataclass(frozen=True)
class Phrase:
    query: str
    path: str
    slop: int = 0  #


@dataclass(frozen=True)
class Equals:
    path: str
    value: Any


@dataclass(frozen=True)
class In:
    path: str
    values: tuple[Any, ...]


@dataclass(frozen=True)
class Range:
    path: str
    gt: Any = None
    gte: Any = None
    lt: Any = None
    lte: Any = None


@dataclass(frozen=True)
class Compound:
    must: tuple[Any, ...] = ()
    should: tuple[Any, ...] = ()
    filter: tuple[Any, ...] = ()
    must_not: tuple[Any, ...] = ()
    minimum_should_match: int = 0  #


Clause = Text | Phrase | Equals | In | Range | Compound


@dataclass(frozen=True)
class SortKey:
    path: str  # the pseudo path "score" means relevance
    direction: int = -1  # 1 ascending, -1 descending


@dataclass(frozen=True)
class Query:
    root: Compound
    sort: tuple[SortKey, ...] = ()  # empty = relevance (score desc); `id` asc is always appended
    limit: int = 20  #
    skip: int = 0  # paging


@dataclass(frozen=True)
class Hit:
    id: Any
    score: float


@dataclass
class SearchResult:
    hits: list[Hit] = field(default_factory=list)
    total: int = 0  # exact number of matches


@dataclass(frozen=True)
class IndexableDoc:
    """What the engine needs to index one document: the id, the revision token and the raw field values."""

    id: Any
    rev: Any
    fields: dict[str, Any]


COMMON_CLAUSES = frozenset({"text", "text.fuzzy", "phrase", "equals", "in", "range", "compound", "sort", "paging", "score"})


@runtime_checkable
class SearchEngine(Protocol):
    """The engine port, reduced to what the harness needs."""

    name: str

    def capabilities(self) -> frozenset[str]: ...

    async def create_index(self, entity: str, index: IndexDef) -> None: ...

    async def update_index(self, entity: str, index: IndexDef) -> None: ...

    async def drop_index(self, entity: str, index: str) -> None: ...

    async def index_status(self, entity: str, index: str) -> str:
        """One of 'missing', 'building', 'queryable', 'failed'."""
        ...

    async def wait_queryable(self, entity: str, index: str, timeout: float) -> None:
        """Raises IndexNeverQueryableError at the bound."""
        ...

    async def sync_write(self, entity: str, index: str, docs: Sequence[IndexableDoc]) -> None: ...

    async def sync_delete(self, entity: str, index: str, ids: Sequence[Any]) -> None: ...

    async def probe(self, entity: str, index: str, pairs: Sequence[tuple[Any, Any]]) -> set[Any]:
        """Return the ids among (id, rev) pairs whose revision is visible in the index."""
        ...

    async def probe_absent(self, entity: str, index: str, ids: Sequence[Any]) -> set[Any]:
        """Return the ids that are still present in the index."""
        ...

    async def search(self, entity: str, index: str, query: Query) -> SearchResult: ...
