"""Named errors of the prototype. Messages carry names only, never field values."""
from __future__ import annotations


class TxError(Exception):
    """Base class."""


class TransactionError(TxError):
    """Raised by a body to signal an anticipated conflict. Only the outermost `on_conflict` absorbs it."""


class UnmanagedSessionError(TxError):
    def __init__(self, entity: str) -> None:
        super().__init__(f"a session= was passed to a write on {entity!r} outside @transactional and defer(); use @transactional")


class InvalidSessionTypeError(TxError):
    def __init__(self, got: type) -> None:
        super().__init__(f"session= must be an AsyncClientSession, got {got.__name__}")


class SearchInTransactionError(TxError):
    def __init__(self, entity: str) -> None:
        super().__init__(f"search on {entity!r} inside an active transaction scope: index changes are applied only after commit")


class StockBulkWriterError(TxError):
    def __init__(self, entity: str) -> None:
        super().__init__(
            f"a beanie.BulkWriter that this layer did not create was passed to a write on {entity!r} inside @transactional; "
            f"create it with {entity}.bulk_writer() instead"
        )


class TransactionsUnsupportedError(TxError):
    """Design (i): @transactional refused on a backend with no sessions (mongomock)."""

    def __init__(self) -> None:
        super().__init__("@transactional is not supported on mongomock (no sessions, no rollback); use a real MongoDB replica set")


class NotInitializedError(TxError):
    pass
