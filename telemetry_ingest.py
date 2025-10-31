#!/usr/bin/env python3
import argparse
import json
import os
import re
import signal
import sqlite3
import sys
import time
from datetime import datetime, UTC
from typing import List, Optional

try:
    import serial
    from serial import SerialException
except ImportError:
    print("Missing dependency: pyserial\nInstall with: pip install pyserial", file=sys.stderr)
    sys.exit(1)

DEFAULT_PORT = "/dev/tty.usbmodem207435A554301"
DEFAULT_BAUD = 115200
RECONNECT_DELAY_SEC = 0

# Regex that tolerates noisy prefixes/suffixes and both "(a,b,c)" or "a, b, c"
PACKET_RE = re.compile(
    r"""
    ^\s*
    (?:[A-Za-z_][\w:]*:\s*)?            # optional label like on_radio_packet:
    \{                                   # opening brace
      \s*Type:\s*(?P<type>[A-Za-z0-9_-]+)\s*,\s*
      (?:(?:Data\ \s*Size)|Size):\s*(?P<size>\d+)\s*,\s*  # 'Data Size:' or 'Size:' (space escaped)
      Sender:\s*(?P<sender>[^,]+?)\s*,\s*
      Endpoints:\s*\[(?P<endpoints>[^\]]*)]\s*,\s*
      Timestamp:\s*(?P<ts_ms>\d+)
      (?:\s*\((?P<ts_human>[^)]*)\))?    # optional human-readable timestamp
      \s*,\s*
      (?:Data|Error):\s*
      (?:
        \((?P<data_paren>.*?)\)          # data wrapped in parens
        |
        (?P<data_noparen>[^}]+?)         # or bare list until the close brace
      )
    \}\s*$
    """,
    re.IGNORECASE | re.VERBOSE | re.DOTALL,
)


# ---------------------- Database Schema ----------------------
def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS telemetry_packets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            type TEXT NOT NULL,
            size_bytes INTEGER NOT NULL,
            sender TEXT NOT NULL,
            endpoints TEXT NOT NULL,
            timestamp_ms INTEGER NOT NULL,
            received_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS telemetry_values (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            packet_id INTEGER NOT NULL,
            idx INTEGER NOT NULL,
            value REAL,
            FOREIGN KEY(packet_id) REFERENCES telemetry_packets(id) ON DELETE CASCADE
        )
    """)
    conn.commit()

# ---------------------- Helpers ----------------------
def parse_endpoints(s: str) -> List[str]:
    return [p.strip() for p in s.split(",") if p.strip()]

def parse_data_values(s: str) -> List[float]:
    vals = []
    for piece in s.split(","):
        piece = piece.strip()
        if not piece:
            continue
        try:
            vals.append(float(piece))
        except ValueError:
            # ignore non-floats silently
            pass
    return vals

def parse_packet_line(line: str, verbose: bool = False):
    # use search() to handle extra leading/trailing noise
    m = PACKET_RE.search(line)
    if not m:
        if verbose:
            print(f"[skip] regex no-match: {line}", file=sys.stderr)
        return None

    ts_human = m.group("ts_human")
    ts_human = ts_human.strip() if ts_human else ""

    data_raw = m.group("data_paren")
    if data_raw is None:
        data_raw = m.group("data_noparen") or ""
    data_raw = data_raw.strip()

    pkt = {
        "type": m.group("type").upper(),
        "size_bytes": int(m.group("size")),
        "sender": m.group("sender").strip(),
        "endpoints": parse_endpoints(m.group("endpoints")),
        "timestamp_ms": int(m.group("ts_ms")),
        "timestamp_human": ts_human,
        "values": parse_data_values(data_raw),
    }

    if verbose:
        print(f"[parse] {pkt['type']} t={pkt['timestamp_ms']} "
              f"sender={pkt['sender']} endpoints={pkt['endpoints']} "
              f"vals={len(pkt['values'])}", file=sys.stderr)
    return pkt

def insert_packet(conn: sqlite3.Connection, pkt: dict, verbose: bool = False) -> int:
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO telemetry_packets(type, size_bytes, sender, endpoints, timestamp_ms, received_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            pkt["type"],
            pkt["size_bytes"],
            pkt["sender"],
            json.dumps(pkt["endpoints"]),
            pkt["timestamp_ms"],
            datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S"),
        ),
    )
    packet_id = cur.lastrowid

    if pkt["values"]:
        cur.executemany(
            "INSERT INTO telemetry_values(packet_id, idx, value) VALUES (?, ?, ?)",
            [(packet_id, i, v) for i, v in enumerate(pkt["values"])],
        )
    conn.commit()
    if verbose:
        print(f"[db] inserted packet_id={packet_id} values={len(pkt['values'])}", file=sys.stderr)
    return packet_id

def append_jsonl(path: str, obj: dict) -> None:
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")

def try_open_serial(port: str, baud: int, timeout: float = 1.0) -> Optional[serial.Serial]:
    try:
        return serial.Serial(port=port, baudrate=baud, timeout=timeout)
    except (SerialException, OSError):
        return None

def read_lines(ser: serial.Serial):
    while True:
        try:
            bline = ser.readline()
        except SerialException:
            break
        if not bline:
            continue
        try:
            line = bline.decode("utf-8", errors="replace").rstrip("\r\n")
        except Exception:
            continue
        yield line

# ---------------------- Main ----------------------
def main():
    ap = argparse.ArgumentParser(description="Telemetry serial → SQLite ingestor with optional JSONL mirroring.")
    ap.add_argument("--port", default=DEFAULT_PORT, help=f"Serial port (default: {DEFAULT_PORT})")
    ap.add_argument("--baud", type=int, default=DEFAULT_BAUD, help=f"Baud rate (default: {DEFAULT_BAUD})")
    ap.add_argument("--db", default="telemetry.db", help="SQLite database file (default: telemetry.db)")
    ap.add_argument("--out-jsonl", help="Mirror parsed telemetry packets to this JSONL file")
    ap.add_argument("--file-flag", help="Optional flag to insert in JSONL record (e.g., TELEM)")
    ap.add_argument("--out-txt", help="Append parsed telemetry text to this file")
    ap.add_argument("--no-db", action="store_true", help="Do not write to the database")
    ap.add_argument("--verbose", action="store_true", help="Verbose logging")
    args = ap.parse_args()

    stopping = {"stop": False}
    def _sigint(_sig, _frm):
        stopping["stop"] = True
        print("\nStopping...", file=sys.stderr)
    signal.signal(signal.SIGINT, _sigint)
    signal.signal(signal.SIGTERM, _sigint)

    conn = None
    if not args.no_db:
        conn = sqlite3.connect(args.db, isolation_level=None, timeout=30.0)
        ensure_schema(conn)
        if args.verbose:
            print(f"[db] open '{args.db}'", file=sys.stderr)

    print(f"[{datetime.now().isoformat(timespec='seconds')}] Starting. DB='{args.db}' Port='{args.port}' Baud={args.baud}")
    if args.out_jsonl:
        print(f"[info] JSONL mirror -> {args.out_jsonl} (flag={args.file_flag!r})")
    if args.out_txt:
        print(f"[info] txt mirror -> {args.out_txt}")

    while not stopping["stop"]:
        ser = try_open_serial(args.port, args.baud, timeout=1.0)
        if ser is None:
            if args.verbose:
                print(f"[warn] serial not available on {args.port}; retrying...", file=sys.stderr)
            time.sleep(RECONNECT_DELAY_SEC)
            continue

        print(f"[ok] Connected to {args.port} at {args.baud} baud.", file=sys.stderr)
        try:
            for line in read_lines(ser):
                if stopping["stop"]:
                    break

                pkt = parse_packet_line(line, verbose=args.verbose)
                if not pkt:
                    print(f"[skip] unrecognized line: {line}", file=sys.stderr)
                    continue

                if not args.no_db and conn:
                    insert_packet(conn, pkt, verbose=args.verbose)

                if args.out_txt:
                    os.makedirs(os.path.dirname(os.path.abspath(args.out_txt)), exist_ok=True)
                    with open(args.out_txt, "a", encoding="utf-8") as txtf:
                        txtf.write(f"[{datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S')}] ")
                        txtf.write(f"Type={pkt['type']} Sender={pkt['sender']} ")
                        txtf.write(f"Endpoints={pkt['endpoints']} Timestamp={pkt['timestamp_ms']} ")
                        txtf.write(f"Human={pkt['timestamp_human']} Data={pkt['values']}\n")

                if args.out_jsonl:
                    out_obj = {
                        "type": pkt["type"],
                        "size_bytes": pkt["size_bytes"],
                        "sender": pkt["sender"],
                        "endpoints": pkt["endpoints"],
                        "timestamp_ms": pkt["timestamp_ms"],
                        "timestamp_human": pkt["timestamp_human"],
                        "values": pkt["values"],
                        "received_at": datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S") + "Z",
                    }
                    if args.file_flag:
                        out_obj["flag"] = args.file_flag
                    append_jsonl(args.out_jsonl, out_obj)

        except (SerialException, OSError) as e:
            print(f"[warn] Serial error: {e}. Will attempt to reconnect...", file=sys.stderr)
        finally:
            try:
                ser.close()
            except Exception:
                pass
            if not stopping["stop"]:
                time.sleep(RECONNECT_DELAY_SEC)

    if conn:
        conn.close()
    print("[done] Clean exit.")

if __name__ == "__main__":
    main()
