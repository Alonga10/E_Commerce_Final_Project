import json
import os
from datetime import datetime
import happybase

# =========================
# EDIT IF NEEDED
# =========================
HBASE_HOST = "localhost"
HBASE_PORT = 9090
TABLE_NAME = "session"
COLUMN_FAMILY = "cf"

SESSIONS_FILE = r"C:\Users\mpalo\Documents\BIG DATA CLASS\BIG DATA ANALYTICS\Final\E_Commerce_Final_Project\data_raw\sessions_0.json"

BATCH_SIZE = 2000  # increase if stable, decrease if errors


def safe_str(x):
    if x is None:
        return ""
    if isinstance(x, (dict, list)):
        return json.dumps(x, ensure_ascii=False)
    return str(x)


def make_rowkey(obj: dict) -> str:
    """
    Strong rowkey:
    user_id#start_time#session_id
    (good for scans by user and time)
    """
    user_id = safe_str(obj.get("user_id"))
    start_time = safe_str(obj.get("start_time"))
    session_id = safe_str(obj.get("session_id"))

    if not start_time:
        start_time = "unknown_time"
    if not session_id:
        session_id = "unknown_session"
    if not user_id:
        user_id = "unknown_user"

    return f"{user_id}#{start_time}#{session_id}"


def flatten_to_hbase_columns(obj: dict) -> dict:
    """
    Map your JSON into cf:* columns (bytes).
    Keep it simple + useful for analytics.
    """
    geo = obj.get("geo_data", {}) or {}
    dev = obj.get("device_profile", {}) or {}
    viewed_products = obj.get("viewed_products", []) or []
    page_views = obj.get("page_views", []) or []
    cart = obj.get("cart_contents", {}) or {}

    columns = {
        f"{COLUMN_FAMILY}:session_id": safe_str(obj.get("session_id")),
        f"{COLUMN_FAMILY}:user_id": safe_str(obj.get("user_id")),
        f"{COLUMN_FAMILY}:start_time": safe_str(obj.get("start_time")),
        f"{COLUMN_FAMILY}:end_time": safe_str(obj.get("end_time")),
        f"{COLUMN_FAMILY}:duration_seconds": safe_str(obj.get("duration_seconds")),
        f"{COLUMN_FAMILY}:conversion_status": safe_str(obj.get("conversion_status")),
        f"{COLUMN_FAMILY}:referrer": safe_str(obj.get("referrer")),

        # geo
        f"{COLUMN_FAMILY}:geo_city": safe_str(geo.get("city")),
        f"{COLUMN_FAMILY}:geo_state": safe_str(geo.get("state")),
        f"{COLUMN_FAMILY}:geo_country": safe_str(geo.get("country")),
        f"{COLUMN_FAMILY}:geo_ip": safe_str(geo.get("ip_address")),

        # device
        f"{COLUMN_FAMILY}:device_type": safe_str(dev.get("type")),
        f"{COLUMN_FAMILY}:device_os": safe_str(dev.get("os")),
        f"{COLUMN_FAMILY}:device_browser": safe_str(dev.get("browser")),

        # counts (very useful)
        f"{COLUMN_FAMILY}:viewed_products_count": safe_str(len(viewed_products)),
        f"{COLUMN_FAMILY}:page_views_count": safe_str(len(page_views)),
        f"{COLUMN_FAMILY}:cart_items_count": safe_str(len(cart)),

        # optional raw JSON for debugging / reprocessing
        f"{COLUMN_FAMILY}:raw_json": safe_str(obj),
    }

    # Convert keys/values to bytes for happybase
    return {k.encode("utf-8"): v.encode("utf-8") for k, v in columns.items()}


def detect_format_and_iter_records(path: str):
    """
    Supports:
    1) JSON Lines (one object per line)
    2) JSON array file ([ {...}, {...} ])
    """
    with open(path, "r", encoding="utf-8") as f:
        first = f.read(1)
        f.seek(0)

        if first == "[":
            data = json.load(f)
            for obj in data:
                if isinstance(obj, dict):
                    yield obj
        else:
            # JSON Lines
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                    if isinstance(obj, dict):
                        yield obj
                except json.JSONDecodeError:
                    # skip bad line
                    continue


def main():
    if not os.path.exists(SESSIONS_FILE):
        raise FileNotFoundError(f"Not found: {SESSIONS_FILE}")

    conn = happybase.Connection(HBASE_HOST, HBASE_PORT, timeout=20000)
    conn.open()

    table = conn.table(TABLE_NAME)

    total = 0
    bad = 0
    batch = table.batch(batch_size=BATCH_SIZE)

    print("Import started...")

    for obj in detect_format_and_iter_records(SESSIONS_FILE):
        try:
            rowkey = make_rowkey(obj).encode("utf-8")
            cols = flatten_to_hbase_columns(obj)
            batch.put(rowkey, cols)
            total += 1

            if total % 50000 == 0:
                batch.send()
                print(f"Inserted {total:,} records...")

        except Exception:
            bad += 1
            continue

    batch.send()
    conn.close()

    print(f"✅ Done. Inserted: {total:,} | Skipped(bad): {bad:,}")


if __name__ == "__main__":
    main()
