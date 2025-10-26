#!/usr/bin/env python3
import argparse
import json
import sqlite3
from typing import Optional, List, Dict

import pandas as pd
import matplotlib
matplotlib.use("TkAgg")  # embed Matplotlib inside Tkinter
from matplotlib.figure import Figure
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import matplotlib as mpl
from matplotlib.ticker import ScalarFormatter
mpl.rcParams["agg.path.chunksize"] = 10000   # improves anti-aliasing
mpl.rcParams["figure.dpi"] = 200             # internal render quality
mpl.rcParams["savefig.dpi"] = 300
mpl.rcParams["figure.autolayout"] = True
import tkinter as tk
from tkinter import ttk

# ---------- Database utilities ----------

def fetch_dataframe(
    conn: sqlite3.Connection,
    packet_type: Optional[str],
    sender: Optional[str],
    start_ms: Optional[int],
    end_ms: Optional[int],
    use_received_time: bool = True,  # graph by time RECEIVED (host clock)
) -> pd.DataFrame:
    q = [
        "SELECT p.id, p.type, p.sender, p.endpoints, p.timestamp_ms, p.received_at, v.idx, v.value",
        "FROM telemetry_packets p",
        "JOIN telemetry_values v ON v.packet_id = p.id",
        "WHERE 1=1",
    ]
    args: List = []

    if packet_type:
        q.append("AND p.type = ?")
        args.append(packet_type)
    if sender:
        q.append("AND p.sender = ?")
        args.append(sender)
    if start_ms is not None:
        q.append("AND p.timestamp_ms >= ?")
        args.append(start_ms)
    if end_ms is not None:
        q.append("AND p.timestamp_ms <= ?")
        args.append(end_ms)

    # Sort by receive time so plots reflect ingestion order/spacing
    q.append("ORDER BY p.received_at ASC, v.idx ASC")
    sql = " ".join(q)

    df = pd.read_sql_query(sql, conn, params=args)
    if df.empty:
        return df

    df["endpoints"] = df["endpoints"].apply(
        lambda s: json.loads(s) if isinstance(s, str) and s.startswith("[") else s
    )

    # convert received_at text -> datetime, and produce seconds since first receive
    df["received_dt"] = pd.to_datetime(df["received_at"], errors="coerce")
    df["t_received_s"] = (df["received_dt"] - df["received_dt"].min()).dt.total_seconds()

    # keep device-sent time too (seconds)
    df["t_sent_s"] = df["timestamp_ms"] / 1000.0

    # pick x-axis
    df["t_plot"] = df["t_received_s"] if use_received_time else df["t_sent_s"]
    return df


def list_types_and_senders(conn: sqlite3.Connection) -> Dict[str, List[str]]:
    """
    Returns mapping {type: [senders]} from telemetry_packets table.
    """
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT type, sender FROM telemetry_packets")
    mapping: Dict[str, set] = {}
    for t, s in cur.fetchall():
        mapping.setdefault(t, set()).add(s)
    return {k: sorted(v) for k, v in mapping.items()}


# ---------- Plotting function ----------

def plot_all_indices_overlay(df: pd.DataFrame, title_suffix: str, fig: Figure):
    """
    Overlay all indices on one graph with separate y-axes (different scales),
    shared x-axis (time). Uses multiple right-side axes with offset spines.

    Text/line sizes are specified in *pixels* and converted to points so they
    remain visually constant even when DPI changes.
    """
    fig.clf()

    # ---- pixel→point converter bound to this figure's effective DPI ----
    def px2pt(px: float) -> float:
        # points that render as 'px' pixels at current fig.dpi
        return 72.0 * px / float(fig.dpi)

    # ---- choose visual targets in pixels (tweak to taste) ----
    TITLE_PX   = 18
    LABEL_PX   = 12
    TICK_PX    = 10
    LEGEND_PX  = 10
    ANNO_PX    = 10
    LW_PX      = 2.0   # line width in visual pixels
    GRID_ALPHA = 0.25

    if df.empty:
        ax = fig.add_subplot(1, 1, 1)
        ax.set_title("No data", fontsize=px2pt(TITLE_PX))
        ax.set_xlabel("Time (s)", fontsize=px2pt(LABEL_PX))
        ax.tick_params(axis="both", labelsize=px2pt(TICK_PX))
        fig.tight_layout()
        return

    indices = sorted(int(i) for i in df["idx"].unique())
    n = len(indices)

    ax_main = fig.add_subplot(1, 1, 1)
    ax_main.set_xlabel("Time (s)", fontsize=px2pt(LABEL_PX))
    ax_main.grid(True, which="both", axis="both", linestyle="-", alpha=GRID_ALPHA)
    ax_main.tick_params(axis="both", labelsize=px2pt(TICK_PX))

    lines, labels = [], []
    color_cycle = [f"C{i % 10}" for i in range(n)]

    def _plot_on_axis(ax, idx, color):
        df_idx = df[df["idx"] == idx]
        if df_idx.empty:
            return None, None
        t = df_idx["t_plot"].to_numpy()
        v = df_idx["value"].to_numpy()

        (line,) = ax.plot(
            t, v,
            label=f"Idx {idx}",
            color=color,
            linewidth=px2pt(LW_PX),
            antialiased=True,
        )

        ax.set_ylabel(f"Idx {idx}", color=color, fontsize=px2pt(LABEL_PX))
        ax.tick_params(axis="y", colors=color, labelsize=px2pt(TICK_PX))

        # --- NEW: make the offset text (e.g., +1.005e5) scale like ticks ---
        fmt = ScalarFormatter(useMathText=True)
        # tweak powerlimits if you want to control when sci/offset appears:
        fmt.set_powerlimits((-3, 3))
        ax.yaxis.set_major_formatter(fmt)
        off = ax.yaxis.get_offset_text()
        off.set_fontsize(px2pt(TICK_PX))
        off.set_color(color)

        # Optional: put the offset near the axis label instead of default spot
        # off.set_va("baseline"); off.set_ha("left")  # adjust to taste

        # Annotate latest value near last point
        lt, lv = t[-1], v[-1]
        ax.text(
            lt, lv, f" {lv:.3f}",
            va="center", ha="left",
            fontsize=px2pt(ANNO_PX), color=color, fontweight="bold",
            clip_on=True,
        )
        return line, f"Idx {idx}"

    # First (left) axis
    first_idx = indices[0]
    line, lab = _plot_on_axis(ax_main, first_idx, color_cycle[0])
    if line:
        lines.append(line)
        labels.append(lab)

    # Additional right-side axes with offset spines
    for k, idx in enumerate(indices[1:], start=1):
        ax_k = ax_main.twinx()
        ax_k.spines["right"].set_position(("axes", 1 + 0.08 * (k - 1)))
        ax_k.set_frame_on(True)
        ax_k.patch.set_visible(False)

        line, lab = _plot_on_axis(ax_k, idx, color_cycle[k])
        if line:
            lines.append(line)
            labels.append(lab)

    # Title and legend
    base_title = f"{df['type'].iloc[0]}" if not df.empty else "Telemetry"
    sender = df["sender"].iloc[0] if not df.empty else ""
    fig.suptitle(
        f"{base_title} {title_suffix}".strip() + (f" — {sender}" if sender else ""),
        fontsize=px2pt(TITLE_PX),
        fontweight="bold",
    )

    if lines:
        leg = fig.legend(
            lines, labels,
            loc="upper center",
            bbox_to_anchor=(0.5, 0.96),
            ncol=len(labels),
            frameon=True,
            fontsize=px2pt(LEGEND_PX),
        )
        # Legend frame line width in pixel-constant units too
        leg.get_frame().set_linewidth(px2pt(LW_PX))

    fig.tight_layout(rect=[0, 0, 1, 0.94])



# ---------- Tkinter App (multi-level tabs) ----------

class TelemetryTabsApp:
    def __init__(self, conn, start_ms, end_ms, refresh_sec, dpi: int):
        self.conn = conn
        self.start_ms = start_ms
        self.end_ms = end_ms
        self.refresh_ms = max(100, int(refresh_sec * 1000))
        self.fig_dpi = dpi

        self.root = tk.Tk()
        self.root.title("Telemetry Viewer — Types & Senders")
        self.root.protocol("WM_DELETE_WINDOW", self.on_close)
        self.root.lift()
        self.root.focus_force()  # try to give it keyboard focus
        # --- DPI / zoom fix: prevent Tk from scaling widgets on HiDPI (Retina) ---
        # This keeps visual size constant; DPI will now control crispness, not zoom.
        try:
            self.ui_scale = float(self.root.tk.call('tk', 'scaling'))  # e.g. 2.0 on Retina
        except tk.TclError:
            self.ui_scale = 1.0

        # Optional: set global Matplotlib DPI defaults for consistency
        # matplotlib.rcParams["figure.dpi"] = self.fig_dpi
        # matplotlib.rcParams["savefig.dpi"] = max(300, self.fig_dpi)

        self.nb_types = ttk.Notebook(self.root)
        self.nb_types.pack(fill="both", expand=True)

        # Structure: {type: {"nb": inner_notebook, "senders": {sender: {fig, canvas}}}}
        self.tabs: Dict[str, Dict[str, Dict]] = {}

        self.rebuild_tabs()
        self.running = True
        self.root.after(self.refresh_ms, self.update_loop)

    def rebuild_tabs(self):
        """Ensure tabs exist for all (type, sender) combinations."""
        type_sender_map = list_types_and_senders(self.conn)
        for t, senders in type_sender_map.items():
            # Create outer tab for type if missing
            if t not in self.tabs:
                frame_outer = ttk.Frame(self.nb_types)
                self.nb_types.add(frame_outer, text=t)
                nb_senders = ttk.Notebook(frame_outer)
                nb_senders.pack(fill="both", expand=True)
                self.tabs[t] = {"nb": nb_senders, "senders": {}}

            nb_senders = self.tabs[t]["nb"]

            # Create sender tabs
            for s in senders:
                if s in self.tabs[t]["senders"]:
                    continue

                # Fixed pixel size for the plot area
                PX_W, PX_H = 1280, 800

                frame_inner = ttk.Frame(nb_senders, width=PX_W, height=PX_H)
                frame_inner.pack_propagate(False)  # don't let children resize the frame
                nb_senders.add(frame_inner, text=s)

                # --- CRITICAL: tie inches to dpi so pixels stay constant ---
                fig = Figure(figsize=(PX_W / self.fig_dpi, PX_H / self.fig_dpi),
                             dpi=self.fig_dpi)

                canvas = FigureCanvasTkAgg(fig, master=frame_inner)
                w = canvas.get_tk_widget()

                # Fix the Tk widget to the same pixel size; do not allow expansion.
                w.configure(width=PX_W, height=PX_H)
                w.pack(fill="none", expand=False)

                # Safety: neutralize Tk zoom for this widget too
                try:
                    w.tk.call("tk", "scaling", 1.0)
                except tk.TclError:
                    pass

                canvas.draw_idle()
                self.tabs[t]["senders"][s] = {"fig": fig, "canvas": canvas}

    def update_loop(self):
        if not self.running:
            return

        # End any stale read snapshot so new rows are visible
        try:
            self.conn.commit()
        except sqlite3.Error:
            pass

        # Check for new types/senders
        self.rebuild_tabs()

        # Update all plots
        for t, type_data in self.tabs.items():
            for s, plot in type_data["senders"].items():
                df = fetch_dataframe(self.conn, packet_type=t, sender=s,
                                     start_ms=self.start_ms, end_ms=self.end_ms)
                title_suffix = f"(sender={s})"
                plot_all_indices_overlay(df, title_suffix, plot["fig"])
                plot["canvas"].draw()

        self.root.after(self.refresh_ms, self.update_loop)

    def on_close(self):
        self.running = False
        self.root.after(50, self.root.destroy)

    def run(self):
        self.root.mainloop()


# ---------- CLI ----------

def main():
    ap = argparse.ArgumentParser(description="Telemetry viewer: one tab per type, one sub-tab per sender.")
    ap.add_argument("--db", default="telemetry.db", help="Path to SQLite DB")
    ap.add_argument("--start-ms", type=int, default=None, help="Min timestamp_ms")
    ap.add_argument("--end-ms", type=int, default=None, help="Max timestamp_ms")
    ap.add_argument("--refresh-sec", type=float, default=1.0, help="Refresh period (default 1s)")
    ap.add_argument("--dpi", type=int, default=200, help="Render DPI for crispness (does not change on-screen size)")
    args = ap.parse_args()

    # Open in autocommit mode; configure for concurrent writer/reader
    conn = sqlite3.connect(
        args.db,
        isolation_level=None,     # autocommit (avoid long-lived read transactions)
        check_same_thread=False,  # safe for Tk callbacks
    )
    # Improve live-reading behavior
    conn.execute("PRAGMA busy_timeout=5000")
    try:
        conn.execute("PRAGMA journal_mode=WAL")  # helps concurrent read/write
    except sqlite3.OperationalError:
        pass

    try:
        app = TelemetryTabsApp(
            conn=conn,
            start_ms=args.start_ms,
            end_ms=args.end_ms,
            refresh_sec=args.refresh_sec,
            dpi=args.dpi,
        )
        app.run()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
