#!/usr/bin/env python3
# /// script
# requires-python = ">=3.11"
# dependencies = ["PyMySQL>=1.1.2,<2", "filelock>=3.16,<4"]
# ///
"""Neue MariaDB-Zeilen anhand einer aufsteigenden Ganzzahl-ID abfragen.

Start (Python >= 3.11; uv installiert):
    DB_HOST, DB_PORT (optional, Standard 3306), DB_NAME, DB_USER und DB_PASSWORD
    als Umgebungsvariablen setzen, dann:
    uv run mariadb_insert_monitor.py --table auftraege --id-column id --interval 5

Verhalten:
    Beim ersten Start wird MAX(id) als Ausgangspunkt gespeichert. Altbestand
    wird nur mit --include-existing und einer NEUEN Zustandsdatei ausgegeben.
    Neustarts verwenden immer den gespeicherten Fortschritt. Die Zustandsdatei
    liegt standardmaessig im aktuellen Arbeitsverzeichnis; fuer einen Dienst
    mit --state-file einen festen absoluten Pfad konfigurieren.
    Pro neuer Zeile: handle_new_row() aufrufen, dann den Fortschritt speichern.
    stdout enthaelt JSON-Zeilen, stderr Betriebs- und Fehlermeldungen.
    --ssl-ca aktiviert TLS mit Zertifikats- und Hostnamenpruefung.

Voraussetzungen und Grenzen:
    Positive, eindeutige, unveraenderliche, indizierte Ganzzahl-ID, typischerweise
    ein AUTO_INCREMENT PRIMARY KEY. Sichtbarkeit/Commits muessen in aufsteigender
    ID-Reihenfolge erfolgen; AUTO_INCREMENT allein garantiert das NICHT.
    Spaeter committete kleinere IDs werden uebersehen. Ebenso: kurzlebige Zeilen,
    wiederverwendete IDs und TRUNCATE/Neuaufbau. Keine Update-/Delete-Erkennung.
    Gelesen wird der aktuelle Zeileninhalt, kein historischer Insert-Snapshot.
    Nach einem Absturz zwischen Verarbeitung und Speichern sind Wiederholungen
    moeglich. Eigene Folgeaktionen deshalb anhand der ID idempotent gestalten.
    Eine lokale Dateisperre verhindert mehrere Prozesse mit derselben
    Zustandsdatei; separate Zustandsdateien sind unabhaengige Beobachter.
    Fuer lueckenlose Ereigniserfassung: CDC/Binlog oder quittierte Outbox nutzen.

Referenzen:
    https://pymysql.readthedocs.io/en/latest/modules/connections.html
    https://docs.astral.sh/uv/guides/scripts/
    https://mariadb.com/docs/server/server-usage/storage-engines/innodb/auto_increment-handling-in-innodb
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import os
import re
import ssl
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pymysql
from filelock import FileLock, Timeout
from pymysql.connections import Connection
from pymysql.cursors import DictCursor

LOGGER = logging.getLogger("mariadb_insert_monitor")
RETRYABLE_ERRORS = {1040, 1053, 1205, 1213, 2002, 2003, 2006, 2013, 2055}
IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")


@dataclass(frozen=True)
class Config:
    host: str
    port: int
    database: str
    user: str
    password: str = field(repr=False)
    table: str
    id_column: str
    interval: float
    batch_size: int
    state_file: Path
    include_existing: bool
    ssl_ca: str | None = None

    @property
    def source(self) -> dict[str, str | int]:
        return {
            "host": self.host, "port": self.port, "database": self.database,
            "table": self.table, "id_column": self.id_column,
        }


def quote_identifier(value: str) -> str:
    """SQL-Namen separat pruefen; SQL-Werte werden dagegen parametrisiert."""
    if not IDENTIFIER.fullmatch(value) or len(value) > 64:
        raise ValueError(f"Ungueltiger SQL-Name: {value!r}")
    return f"`{value}`"


def read_config() -> Config:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--table", required=True)
    parser.add_argument("--id-column", default="id")
    parser.add_argument("--interval", type=float, default=5.0, help="Sekunden; Standard: 5")
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--state-file", type=Path,
                        default=Path("mariadb_insert_monitor.state.json"))
    parser.add_argument("--include-existing", action="store_true")
    parser.add_argument("--ssl-ca", help="PEM-Datei der vertrauenswuerdigen Server-CA")
    args = parser.parse_args()
    for name in ("DB_NAME", "DB_USER", "DB_PASSWORD"):
        if name not in os.environ or (name != "DB_PASSWORD" and not os.environ[name]):
            parser.error(f"Umgebungsvariable {name} fehlt.")
    port = int(os.environ.get("DB_PORT", "3306"))
    if not 1 <= port <= 65535:
        parser.error("DB_PORT muss zwischen 1 und 65535 liegen.")
    if not math.isfinite(args.interval) or args.interval <= 0 or args.batch_size <= 0:
        parser.error("Intervall und Batch-Groesse muessen positiv sein.")
    quote_identifier(args.table)
    quote_identifier(args.id_column)
    return Config(
        host=os.environ.get("DB_HOST", "127.0.0.1"), port=port,
        database=os.environ["DB_NAME"], user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"], table=args.table, id_column=args.id_column,
        interval=args.interval, batch_size=args.batch_size,
        state_file=args.state_file.expanduser().resolve(),
        include_existing=args.include_existing, ssl_ca=args.ssl_ca,
    )


def validate_id(value: Any, *, allow_zero: bool = False) -> int:
    minimum = 0 if allow_zero else 1
    if type(value) is not int or value < minimum:
        raise ValueError("Die ID muss eine positive Ganzzahl sein (Startmarke: 0 erlaubt).")
    return value


def load_checkpoint(config: Config) -> int | None:
    try:
        state = json.loads(config.state_file.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    if not isinstance(state, dict) or state.get("version") != 1:
        raise ValueError("Ungueltige Zustandsdatei; kein automatisches Zuruecksetzen.")
    if state.get("source") != config.source:
        raise ValueError("Zustandsdatei gehoert zu einer anderen Datenquelle. --state-file anpassen.")
    return validate_id(state.get("last_id"), allow_zero=True)


def save_checkpoint(config: Config, last_id: int) -> None:
    """Datei erst vollstaendig schreiben, dann am gleichen Ort atomar ersetzen."""
    state = {"version": 1, "source": config.source, "last_id": last_id}
    descriptor, name = tempfile.mkstemp(prefix=f".{config.state_file.name}.",
                                       dir=config.state_file.parent)
    temporary = Path(name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(state, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, config.state_file)
    finally:
        temporary.unlink(missing_ok=True)


def connect(config: Config) -> Connection:
    tls = ssl.create_default_context(cafile=config.ssl_ca) if config.ssl_ca else None
    connection = pymysql.connect(
        host=config.host, port=config.port, database=config.database,
        user=config.user, password=config.password, charset="utf8mb4",
        cursorclass=DictCursor, autocommit=True,
        connect_timeout=10, read_timeout=30, write_timeout=30, ssl=tls,
    )
    try:
        if tls is not None:
            with connection.cursor() as cursor:
                cursor.execute("SHOW SESSION STATUS LIKE 'Ssl_cipher'")
                status = cursor.fetchone()
            if not status or not status.get("Value"):
                raise RuntimeError("TLS angefordert, aber keine verschluesselte Verbindung aktiv.")
        return connection
    except BaseException:
        connection.close()
        raise


def current_max_id(connection: Connection, config: Config) -> int:
    table, column = quote_identifier(config.table), quote_identifier(config.id_column)
    with connection.cursor() as cursor:
        cursor.execute(f"SELECT COALESCE(MAX({column}), 0) AS last_id FROM {table}")
        return validate_id(cursor.fetchone()["last_id"], allow_zero=True)


def fetch_new_rows(connection: Connection, config: Config, last_id: int) -> list[dict[str, Any]]:
    table, column = quote_identifier(config.table), quote_identifier(config.id_column)
    with connection.cursor() as cursor:
        cursor.execute(
            f"SELECT * FROM {table} WHERE {column} > %s ORDER BY {column} ASC LIMIT %s",
            (last_id, config.batch_size),
        )
        return list(cursor.fetchall())


def handle_new_row(row: dict[str, Any]) -> None:
    """Hier eigene Verarbeitung einsetzen; bei Fehler eine Exception ausloesen.

    Die Standardausgabe ist eine JSON-Zeile pro Datensatz. Nicht direkt von JSON
    unterstuetzte Werte (z. B. Decimal oder datetime) werden als Text ausgegeben.
    Binaerdaten werden ebenfalls nur als Textdarstellung ausgegeben.
    """
    print(json.dumps(row, ensure_ascii=False, default=str), flush=True)


def process_rows(rows: list[dict[str, Any]], config: Config, last_id: int) -> int:
    for row in rows:
        row_id = validate_id(row[config.id_column])
        if row_id <= last_id:
            raise ValueError("IDs sind nicht strikt aufsteigend; eindeutigen Index pruefen.")
        handle_new_row(row)
        save_checkpoint(config, row_id)
        last_id = row_id
    return last_id


def monitor(config: Config) -> None:
    last_id = load_checkpoint(config)
    connection: Connection | None = None
    LOGGER.warning("ID-Polling: spaeter sichtbare kleinere IDs werden nicht erkannt.")
    LOGGER.info("Beobachte %s.%s; Intervall %.1f s", config.database, config.table, config.interval)
    try:
        while True:
            try:
                if connection is None:
                    connection = connect(config)
                if last_id is None:
                    baseline = 0 if config.include_existing else current_max_id(connection, config)
                    save_checkpoint(config, baseline)
                    last_id = baseline
                    LOGGER.info("Ausgangspunkt gespeichert: ID %d", last_id)
                rows = fetch_new_rows(connection, config, last_id)
            except (pymysql.OperationalError, pymysql.InterfaceError) as error:
                if connection is not None:
                    connection.close()
                    connection = None
                code = error.args[0] if error.args else None
                if not isinstance(error, pymysql.InterfaceError) and code not in RETRYABLE_ERRORS:
                    raise
                LOGGER.warning("Datenbank voruebergehend nicht erreichbar (Code %s); neuer Versuch in %.1f s.",
                               code, config.interval)
                time.sleep(config.interval)
                continue
            # Callback- oder Dateifehler NICHT als Verbindungsfehler verschlucken.
            last_id = process_rows(rows, config, last_id)
            if len(rows) < config.batch_size:
                time.sleep(config.interval)
    finally:
        if connection is not None:
            connection.close()


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        config = read_config()
        config.state_file.parent.mkdir(parents=True, exist_ok=True)
        with FileLock(str(config.state_file) + ".lock", timeout=0):
            monitor(config)
    except KeyboardInterrupt:
        LOGGER.info("Beendet.")
    except Timeout:
        LOGGER.error("Ein anderer Prozess verwendet bereits diese Zustandsdatei.")
        return 1
    except Exception:
        LOGGER.exception("Monitor abgebrochen. Ursache beheben und mit derselben Zustandsdatei neu starten.")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
