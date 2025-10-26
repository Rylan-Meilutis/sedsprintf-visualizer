#!/usr/bin/env python3
import argparse
import json
import os
import re
import signal
import sqlite3
import sys
import time
from datetime import datetime
from typing import List, Optional, Tuple

try:
    import serial
    from serial import SerialException
except ImportError:
    print("Missing dependency: pyserial\nInstall with: pip install pyserial", file=sys.stderr)
    sys.exit(1)

TELEM_PREFIX = "on_radio_packet:"
DEFAULT_PORT = "/dev/tty.usbmodem207435A554301"
DEFAULT_BAUD = 115200
RECONNECT_DELAY_SEC = 0

# Example line:
# on_radio_packet: {Type: BAROMETER_DATA, Size: 12, Sender: CrashNBurn, Endpoints: [SD_CARD, RADIO], Timestamp: 3076 (3s 076ms), Data: 100551.117187500000, 22.666557312012, -0.454471111298}
PACKET_RE = re.compile(
    r"^on_radio_packet:\s*\{"
    r"\s*Type:\s*(?P<type>[A-Za-z0-9_-]+)\s*,\s*"
    r"Size:\s*(?P<size>\d+)\s*,\s*"
    r"Sender:\s*(?P<sender>[^,]+)\s*,\s*"
    r"Endpoints:\s*\[(?P<endpoints>[^\]]*)\]\s*,\s*"
    r"Timestamp:\s*(?P<ts_ms>\d+)\s*\((?P<ts_human>[^\)]*)\)\s*,\s*"
    r"Data:\s*(?P<data>.*)"
    r"\}\s*$",
    re.IGNORECASE
)

def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS telemetry_packets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            type TEXT NOT NULL,
            size_bytes INTEGER NOT NULL,
            sender TEXT NOT NULL,
            endpoints TEXT NOT NULL, -- stored as JSON array string
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

def parse_endpoints(s: str) -> List[str]:
    # s like: "SD_CARD, RADIO"
    parts = [p.strip() for p in s.split(",") if p.strip()]
    return parts

def parse_data_values(s: str) -> List[float]:
    # s like: "100551.11, 22.66, -0.45"
    vals = []
    for piece in s.split(","):
        piece = piece.strip()
        if not piece:
            continue
        try:
            vals.append(float(piece))
        except ValueError:
            # Not a float; skip silently (or could raise)
            pass
    return vals

def parse_packet_line(line: str):
    m = PACKET_RE.match(line.strip())
    if not m:
        return None

    ty = m.group("type").upper()
    size = int(m.group("size"))
    sender = m.group("sender").strip()
    endpoints_raw = m.group("endpoints")
    endpoints = parse_endpoints(endpoints_raw)
    ts_ms = int(m.group("ts_ms"))
    ts_human = m.group("ts_human").strip()  # new
    data_raw = m.group("data")
    values = parse_data_values(data_raw)

    return {
        "type": ty,
        "size_bytes": size,
        "sender": sender,
        "endpoints": endpoints,
        "timestamp_ms": ts_ms,
        "timestamp_human": ts_human,  # use the one from the raw string
        "values": values,
    }

def insert_packet(conn: sqlite3.Connection, pkt: dict) -> int:
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
            datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        )
    )
    packet_id = cur.lastrowid

    if pkt["values"]:
        cur.executemany(
            "INSERT INTO telemetry_values(packet_id, idx, value) VALUES (?, ?, ?)",
            [(packet_id, i, v) for i, v in enumerate(pkt["values"])]
        )
    conn.commit()
    return packet_id

def append_jsonl(path: str, obj: dict) -> None:
    # Makes parent dir if needed; appends one JSON object per line
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")

def try_open_serial(port: str, baud: int, timeout: float = 1.0) -> Optional[serial.Serial]:
    try:
        ser = serial.Serial(port=port, baudrate=baud, timeout=timeout)
        return ser
    except (SerialException, OSError):
        return None

def read_lines(ser: serial.Serial):
    """Yield lines from serial until it breaks. .readline uses timeout."""
    buf = b""
    while True:
        try:
            bline = ser.readline()  # returns on '\n' or timeout
        except SerialException:
            break
        if not bline:
            # timeout; continue so we can check for shutdown signals
            continue
        try:
            line = bline.decode("utf-8", errors="replace").rstrip("\r\n")
        except Exception:
            continue
        yield line

def main():
    ap = argparse.ArgumentParser(description="Telemetry serial -> SQLite ingestor with optional JSONL mirroring.")
    ap.add_argument("--port", default=DEFAULT_PORT, help=f"Serial port to use (default: {DEFAULT_PORT})")
    ap.add_argument("--baud", type=int, default=DEFAULT_BAUD, help=f"Baud rate (default: {DEFAULT_BAUD})")
    ap.add_argument("--db", default="telemetry.db", help="SQLite database file (default: telemetry.db)")
    ap.add_argument("--out-jsonl", default=None, help="If set, mirror each parsed telemetry packet to this JSONL file")
    ap.add_argument("--file-flag", default=None, help="Optional flag inserted into each JSONL record (e.g., TELEM)")
    ap.add_argument("--out-txt", default=None,
                    help="If set, append telemetry lines (raw text) to this file")
    ap.add_argument("--no-db", default=None, type=bool,
                    help="If set, No data is saved to the database")
    args = ap.parse_args()

    # graceful shutdown
    stopping = {"stop": False}
    def _sigint(_sig, _frm):
        stopping["stop"] = True
        print("\nStopping...", file=sys.stderr)
    signal.signal(signal.SIGINT, _sigint)
    signal.signal(signal.SIGTERM, _sigint)

    # prepare DB
    if args.no_db is None and args.no_db != False:
        conn = sqlite3.connect(args.db, isolation_level=None, timeout=30.0)
        ensure_schema(conn)

    print(f"[{datetime.now().isoformat(timespec='seconds')}] Starting. DB='{args.db}' Port='{args.port}' Baud={args.baud}")
    if args.out_jsonl:
        print(f"[info] JSONL mirror -> {args.out_jsonl} (flag={args.file_flag!r})")
    if args.out_txt:
        print(f"[info] txt mirror -> {args.out_txt}")
    while not stopping["stop"]:
        ser = try_open_serial(args.port, args.baud, timeout=1.0)
        if ser is None:
            # Keep retrying until the USB serial shows up
            # print(f"[warn] Serial {args.port} not available. Retrying in {RECONNECT_DELAY_SEC}s...", file=sys.stderr)
            time.sleep(RECONNECT_DELAY_SEC)
            continue

        print(f"[ok] Connected to {args.port} at {args.baud} baud.", file=sys.stderr)
        try:
            for line in read_lines(ser):
                if stopping["stop"]:
                    break

                # Only process telemetry lines with the exact expected prefix
                if not line.startswith(TELEM_PREFIX):
                    # discard non-telemetry messages
                    continue

                pkt = parse_packet_line(line)
                if not pkt:
                    # malformed telemetry line—ignore silently or log:
                    print(f"[skip] Unparsable line: {line}", file=sys.stderr)
                    continue

                # Insert into DB
                if args.no_db is None and args.no_db != False:
                    insert_packet(conn, pkt)
                if args.out_txt:
                    os.makedirs(os.path.dirname(os.path.abspath(args.out_txt)), exist_ok=True)
                    with open(args.out_txt, "a", encoding="utf-8") as txtf:
                        txtf.write(f"[{datetime.utcnow().isoformat(timespec='seconds')}] ")
                        txtf.write(f"Type={pkt['type']} Sender={pkt['sender']} ")
                        txtf.write(f"Endpoints={pkt['endpoints']} ")
                        txtf.write(f"Timestamp={pkt['timestamp_ms']} ")
                        txtf.write(f"Timestamp Human={pkt['timestamp_human']} ")
                        txtf.write(f"Data={pkt['values']}\n")
                # Optional mirror to file as structured JSON
                if args.out_jsonl:
                    out_obj = {
                        "type": pkt["type"],
                        "size_bytes": pkt["size_bytes"],
                        "sender": pkt["sender"],
                        "endpoints": pkt["endpoints"],
                        "timestamp_ms": pkt["timestamp_ms"],
                        "timestamp_human": pkt["timestamp_human"],
                        "values": pkt["values"],
                        "received_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
                    }
                    if args.file_flag is not None:
                        out_obj["flag"] = args.file_flag
                    append_jsonl(args.out_jsonl, out_obj)

        except (SerialException, OSError) as e:
            print(f"[warn] Serial error: {e}. Will attempt to reconnect...", file=sys.stderr)
            # fall through to reconnect loop
        finally:
            try:
                ser.close()
            except Exception:
                pass
            if not stopping["stop"]:
                time.sleep(RECONNECT_DELAY_SEC)
    if args.no_db is None and args.no_db != False:
        conn.close()
    print("[done] Clean exit.")

if __name__ == "__main__":
    main()

