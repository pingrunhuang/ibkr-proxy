import os
import sqlite3
from datetime import datetime, timezone


class OrderOwnershipStore:
    """Durable mapping from IB broker order ids to Engine strategy identities."""

    def __init__(self, path: str):
        self.path = path
        if path != ":memory:":
            parent = os.path.dirname(path)
            if parent:
                os.makedirs(parent, exist_ok=True)
        self.connection = sqlite3.connect(path)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA journal_mode = WAL")
        self.connection.execute("PRAGMA synchronous = FULL")
        self.connection.execute("PRAGMA busy_timeout = 5000")
        self.connection.execute(
            """
            CREATE TABLE IF NOT EXISTS ib_order_ownership (
                client_order_id TEXT PRIMARY KEY,
                broker_order_id TEXT UNIQUE,
                client_id TEXT NOT NULL,
                strategy_id TEXT NOT NULL,
                account_id TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        self.connection.commit()

    def reserve(
        self,
        *,
        client_order_id: str,
        client_id: str,
        strategy_id: str,
        account_id: str = "",
    ) -> None:
        now = datetime.now(timezone.utc).isoformat(timespec="microseconds")
        self.connection.execute(
            """
            INSERT INTO ib_order_ownership(
                client_order_id, broker_order_id, client_id, strategy_id,
                account_id, created_at, updated_at
            ) VALUES(?, NULL, ?, ?, ?, ?, ?)
            ON CONFLICT(client_order_id) DO UPDATE SET
                client_id = excluded.client_id,
                strategy_id = excluded.strategy_id,
                account_id = excluded.account_id,
                updated_at = excluded.updated_at
            """,
            (
                client_order_id,
                client_id,
                strategy_id,
                account_id,
                now,
                now,
            ),
        )
        self.connection.commit()

    def upsert(
        self,
        *,
        client_order_id: str,
        broker_order_id: str,
        client_id: str,
        strategy_id: str,
        account_id: str = "",
    ) -> None:
        now = datetime.now(timezone.utc).isoformat(timespec="microseconds")
        self.connection.execute(
            """
            DELETE FROM ib_order_ownership
            WHERE broker_order_id = ? AND client_order_id != ?
            """,
            (broker_order_id, client_order_id),
        )
        self.connection.execute(
            """
            INSERT INTO ib_order_ownership(
                client_order_id, broker_order_id, client_id, strategy_id,
                account_id, created_at, updated_at
            ) VALUES(?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(client_order_id) DO UPDATE SET
                broker_order_id = excluded.broker_order_id,
                client_id = excluded.client_id,
                strategy_id = excluded.strategy_id,
                account_id = excluded.account_id,
                updated_at = excluded.updated_at
            """,
            (
                client_order_id,
                broker_order_id,
                client_id,
                strategy_id,
                account_id,
                now,
                now,
            ),
        )
        self.connection.commit()

    def find(
        self,
        *,
        client_order_id: str = "",
        broker_order_id: str = "",
    ):
        if client_order_id:
            row = self.connection.execute(
                "SELECT * FROM ib_order_ownership WHERE client_order_id = ?",
                (client_order_id,),
            ).fetchone()
            if row:
                return dict(row)
        if broker_order_id:
            row = self.connection.execute(
                "SELECT * FROM ib_order_ownership WHERE broker_order_id = ?",
                (broker_order_id,),
            ).fetchone()
            if row:
                return dict(row)
        return None

    def close(self) -> None:
        self.connection.close()
