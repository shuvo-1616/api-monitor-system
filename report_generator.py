
import sqlite3
import os
from datetime import datetime, timedelta
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import json
try:
    from reportlab.lib.pagesizes import A4
    from reportlab.pdfgen import canvas
    REPORTLAB_AVAILABLE = True
except Exception:
    REPORTLAB_AVAILABLE = False

DB_PATH = "api_monitor.db"
OUT_DIR = "reports"
os.makedirs(OUT_DIR, exist_ok=True)

def fetch_rows(endpoint, minutes=1440):
    if not os.path.exists(DB_PATH):
        return []
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cutoff = (datetime.utcnow()-timedelta(minutes=minutes)).isoformat()
    cur.execute("SELECT ts_utc, status, dns, connect, tls, request_send, server_processing FROM samples WHERE endpoint=? AND ts_utc>=? ORDER BY ts_utc ASC", (endpoint, cutoff))
    rows = cur.fetchall()
    con.close()
    return rows

def aggregate_times(rows):
    times = []
    for r in rows:
        try:
            total = (r[5] or 0.0) + (r[2] or 0.0) + (r[3] or 0.0) + (r[4] or 0.0) + (r[6] or 0.0)
            times.append((r[0], total))
        except Exception:
            continue
    return times

def plot_times(times, out_png):
    if not times:
        return None
    xs = [t[0] for t in times]
    ys = [t[1]*1000 for t in times]
    plt.figure(figsize=(10,4))
    plt.plot(xs, ys)
    plt.xticks(rotation=45)
    plt.ylabel("Latency (ms)")
    plt.tight_layout()
    plt.savefig(out_png)
    plt.close()
    return out_png

def build_pdf(endpoint, png_path, out_pdf):
    if not REPORTLAB_AVAILABLE:
        return None
    c = canvas.Canvas(out_pdf, pagesize=A4)
    c.setFont("Helvetica", 14)
    c.drawString(40, 800, f"API Performance Report - {endpoint}")
    c.drawString(40, 785, f"Generated at: {datetime.utcnow().isoformat()} UTC")
    c.drawImage(png_path, 40, 300, width=500, height=300)
    c.showPage()
    c.save()
    return out_pdf

def generate(endpoint):
    rows = fetch_rows(endpoint)
    times = aggregate_times(rows)
    if not times:
        return None
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    png = os.path.join(OUT_DIR, f"{endpoint.replace('://','_').replace('/','_')}_{ts}.png")
    pdf = os.path.join(OUT_DIR, f"{endpoint.replace('://','_').replace('/','_')}_{ts}.pdf")
    p = plot_times(times, png)
    if REPORTLAB_AVAILABLE:
        build_pdf(endpoint, p, pdf)
        return pdf
    return p

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: report_generator.py <endpoint>")
        sys.exit(1)
    endpoint = sys.argv[1]
    out = generate(endpoint)
    if out:
        print("Report generated:", out)
    else:
        print("No data for endpoint")