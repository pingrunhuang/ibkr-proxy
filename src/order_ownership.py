import os
import json
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
        self.connection.execute(
            """
            CREATE TABLE IF NOT EXISTS ib_trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT NOT NULL UNIQUE,
                client_id TEXT NOT NULL DEFAULT '',
                strategy_id TEXT NOT NULL DEFAULT '',
                account_id TEXT NOT NULL DEFAULT '',
                trading_day TEXT NOT NULL DEFAULT '',
                trade_id TEXT NOT NULL DEFAULT '',
                payload_json TEXT NOT NULL,
                received_at TEXT NOT NULL
            )
            """
        )
        self.connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_ib_trades_owner_cursor
            ON ib_trades(client_id, strategy_id, id)
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

    def record_trade(self, payload: dict) -> bool:
        event_id = str(payload.get("event_id") or "").strip()
        if not event_id:
            raise ValueError("trade payload requires event_id")
        now = datetime.now(timezone.utc).isoformat(timespec="microseconds")
        cursor = self.connection.execute(
            """
            INSERT OR IGNORE INTO ib_trades(
                event_id, client_id, strategy_id, account_id,
                trading_day, trade_id, payload_json, received_at
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event_id,
                str(payload.get("client_id") or ""),
                str(payload.get("strategy_id") or ""),
                str(payload.get("account_id") or ""),
                str(payload.get("trading_day") or ""),
                str(payload.get("trade_id") or ""),
                json.dumps(payload, ensure_ascii=False, separators=(",", ":"), default=str),
                now,
            ),
        )
        self.connection.commit()
        return cursor.rowcount > 0

    def list_trades(
        self,
        client_id: str,
        strategy_id: str,
        *,
        after_id: int = 0,
        limit: int = 500,
    ) -> dict:
        page_size = min(max(int(limit), 1), 1000)
        cursor = max(int(after_id), 0)
        rows = self.connection.execute(
            """
            SELECT id, payload_json
            FROM ib_trades
            WHERE client_id = ? AND strategy_id = ? AND id > ?
            ORDER BY id ASC
            LIMIT ?
            """,
            (client_id, strategy_id, cursor, page_size + 1),
        ).fetchall()
        has_more = len(rows) > page_size
        page = rows[:page_size]
        return {
            "trades": [json.loads(row["payload_json"]) for row in page],
            "next_after_id": int(page[-1]["id"]) if page else cursor,
            "has_more": has_more,
        }

    def is_healthy(self) -> bool:
        try:
            return self.connection.execute("SELECT 1").fetchone()[0] == 1
        except sqlite3.Error:
            return False

    def close(self) -> None:
        self.connection.close()
