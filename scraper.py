"""
LeafLink Sales Report Scraper  (Medfarms 100 Shafer — ALL brands)
-----------------------------------------------------------
Pulls Orders Received from the LeafLink Marketplace V2 API, flattens to
per-line-item rows, keeps every brand on the Medfarms (100 Shafer) account, trims
to the fields the dashboard needs, and writes a compact sales_data.json.

Field mapping is based on the real LeafLink response:
  - order:  number (uuid), short_id (display #), created_on (date),
            status, customer.display_name (buyer), total.amount, brand_ids
  - line:   ordered_unit_price.amount, sale_price.amount, quantity,
            unit_multiplier, is_sample, frozen_data.product.{name,sku,
            product_line_name,price,...}
  - revenue per line = effective_price * (quantity / unit_multiplier)
"""

import csv
import json
import gzip
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv()

API_BASE = os.getenv("LEAFLINK_API_BASE", "https://www.leaflink.com")
ENDPOINT = os.getenv("LEAFLINK_ENDPOINT", "/api/v2/orders-received/")
# Customers endpoint — used to enrich orders with the assigned sales rep (and
# state/license when present), since the orders feed doesn't carry them.
CUSTOMERS_ENDPOINT = os.getenv("LEAFLINK_CUSTOMERS_ENDPOINT", "/api/v2/customers/")
# Customers carry the assigned rep as the `managers` field — a list of user IDs.
# These endpoints (tried in order) resolve those IDs to rep names.
USERS_ENDPOINTS = [e.strip() for e in os.getenv(
    "LEAFLINK_USERS_ENDPOINTS",
    "/api/v2/users/,/api/v2/company-staff/,/api/v2/staff/,/api/v2/team-members/"
).split(",") if e.strip()]
API_KEY = os.getenv("LEAFLINK_API_KEY", "")

# Keep only line items whose product name / brand contains this (case-insensitive).
BRAND_FILTER = os.getenv("LEAFLINK_BRAND", "")   # "" = ALL brands on the account
INCLUDE_CHILDREN = os.getenv("LEAFLINK_INCLUDE_CHILDREN", "line_items")

# This is a Michigan-only LeafLink account, so every buyer is in MI. Some buyer
# records have a mistyped or blank state (e.g. "Nirvana Center - Escanaba" shows
# "NM"), which clutters the dashboard's state filter. When HOME_STATE is set,
# every buyer_state is coerced to it; anomalies are still logged so real
# data-entry issues stay visible. Set LEAFLINK_HOME_STATE="" to disable.
HOME_STATE = os.getenv("LEAFLINK_HOME_STATE", "MI").strip()

# Keep only orders on/after this date (matched against created_on). Blank = no floor.
# Set low to pull ALL historical data from the start (MI adult-use began Dec 2019).
FROM_DATE = os.getenv("LEAFLINK_FROM_DATE", "2020-01-01")

# Incremental mode. When > 0 AND the committed data was already backfilled at the
# current FROM_DATE, each run pulls only the last N days and MERGES into the
# existing sales_data.json instead of re-scanning all history. This makes the
# scheduled (every-15-min) runs fast. The very first run (or any run where the
# committed data isn't yet backfilled at FROM_DATE) does a one-time FULL pull.
INCREMENTAL_DAYS = int(os.getenv("LEAFLINK_INCREMENTAL_DAYS", "0"))

# Restrict to one company by seller id. Medfarms - 100 Shafer Processing = 9105.
# The App token is already scoped to a single company, but this enforces it
# explicitly. Blank = no company filter.
SELLER_ID = os.getenv("LEAFLINK_SELLER_ID", "9105")

# Statuses to exclude — a "products sold" report does not count these. Comma-sep.
EXCLUDE_STATUSES = [s.strip().lower() for s in
                    os.getenv("LEAFLINK_EXCLUDE_STATUSES", "Cancelled,Rejected,Combined,Draft").split(",")
                    if s.strip()]
# Send the date floor to the server too (created_on__gte) to avoid pulling all
# history. If LeafLink rejects it (400), the scraper drops it and falls back to
# client-side filtering automatically. Set "0" to disable.
SERVER_DATE_FILTER = os.getenv("LEAFLINK_SERVER_DATE_FILTER", "1") != "0"

PAGE_SIZE = int(os.getenv("LEAFLINK_PAGE_SIZE", "500"))
MAX_PAGES = int(os.getenv("LEAFLINK_MAX_PAGES", "0"))

# Current inventory isn't in the orders feed — it lives in the seller's product
# catalog. We pull it once and join by product id / sku. Endpoints tried in order;
# inventory field names tried in order (first non-null wins). Both best-effort.
PRODUCTS_ENDPOINTS = [e.strip() for e in os.getenv(
    "LEAFLINK_PRODUCTS_ENDPOINTS",
    "/api/v2/products/"
).split(",") if e.strip()]

# Brand-name resolution. LeafLink puts brands on the order as numeric `brand_ids`,
# not on the product line, so we map id -> name from the brands endpoint. Several
# candidate paths are tried in order; the first that returns brands wins.
BRANDS_ENDPOINTS = [e.strip() for e in os.getenv(
    "LEAFLINK_BRANDS_ENDPOINTS",
    ",".join([
        f"/api/v2/brands/?company={SELLER_ID}" if SELLER_ID else "/api/v2/brands/",
        f"/api/v2/brands/?seller={SELLER_ID}" if SELLER_ID else "",
        f"/api/v2/companies/{SELLER_ID}/brands/" if SELLER_ID else "",
        "/api/v2/brands/",
    ])
).split(",") if e.strip()]

# Resolve each line's brand from the product catalog (SKU -> brand). This splits
# multi-brand orders to the correct brand per line. On by default; needs the
# catalog pull (same endpoint as inventory).
RESOLVE_BRANDS_FROM_CATALOG = os.getenv("LEAFLINK_RESOLVE_BRANDS_FROM_CATALOG", "1") != "0"
# Real LeafLink product inventory fields (per API docs): available_inventory =
# quantity minus reserved; quantity = total inventory level. Prefer available.
INV_FIELDS = [f.strip() for f in os.getenv(
    "LEAFLINK_INV_FIELDS",
    "available_inventory,quantity,quantity_available,inventory"
).split(",") if f.strip()]

# Inventory pull is OFF by default: it can be slow/huge on big catalogs and
# must never block the orders backfill. Turn on with LEAFLINK_PULL_INVENTORY=1
# once the right endpoint/field is confirmed. Bounded by INV_MAX_PAGES.
PULL_INVENTORY = os.getenv("LEAFLINK_PULL_INVENTORY", "0") == "1"
INV_MAX_PAGES = int(os.getenv("LEAFLINK_INV_MAX_PAGES", "60"))

# Per-brand inventory sweep. LeafLink's company-scoped product list can omit
# whole brands (it returned only 3 of 5 here). So after the company list, we
# also query products brand-by-brand using the brand ids auto-discovered from
# the brands endpoint, and merge (dedup by id/sku). Endpoints/params are tried
# in order until one returns products for that brand; everything is logged.
INV_PER_BRAND = os.getenv("LEAFLINK_INV_PER_BRAND", "1") != "0"
INV_BRAND_PARAMS = [p.strip() for p in os.getenv(
    "LEAFLINK_INV_BRAND_PARAMS", "brand,brand_id,brand__id").split(",") if p.strip()]
_DEF_BRAND_EPS = ",".join(filter(None, [
    f"/api/v2/companies/{SELLER_ID}/products/" if SELLER_ID else "",
    "/api/v2/products/",
]))
INV_BRAND_ENDPOINTS = [e.strip() for e in os.getenv(
    "LEAFLINK_INV_BRAND_ENDPOINTS", _DEF_BRAND_EPS).split(",") if e.strip()]

# Only keep products in these listing states (matches the LeafLink "Available"
# export — drops Archived/Unlisted/etc., e.g. the stray 100k gummy). Field name
# is tried across candidates since the API key isn't documented explicitly.
# Committed LeafLink "Inventory Overview" CSV export. The products API can hand
# back a truncated catalog, so when this file is present in the repo we build the
# dashboard's inventory list straight from it (authoritative). Drop it in as
# `inventory.csv` (or keep the dated export name; newest matching inventory*.csv
# wins). Set LEAFLINK_INVENTORY_CSV to a path to force one, or "off" to disable.
INVENTORY_CSV = os.getenv("LEAFLINK_INVENTORY_CSV", "")

LISTED_STATES = {s.strip().lower() for s in
                 os.getenv("LEAFLINK_LISTED_STATES", "available").split(",") if s.strip()}
STATUS_FIELDS = [f.strip() for f in os.getenv(
    "LEAFLINK_STATUS_FIELDS",
    "listing_state,status,product_status,state,listing_status,product_state"
).split(",") if f.strip()]
OUTPUT_FILE = Path(__file__).parent / "sales_data.json.gz"


# ----------------------------------------------------------------------------
def auth_headers() -> dict:
    return {
        "Authorization": f"App {API_KEY}",
        "Accept": "application/json",
        "Content-Type": "application/json",
        "User-Agent": "chill-sales-dashboard",
    }


def _get(url, params):
    # Robust GET: retry on 429 and transient 5xx so a single hiccup never
    # silently truncates the pull.
    last = None
    for attempt in range(6):
        try:
            resp = requests.get(url, headers=auth_headers(), params=params, timeout=120)
        except requests.RequestException as e:
            last = e
            time.sleep(5 * (attempt + 1))
            continue
        if resp.status_code == 429 or 500 <= resp.status_code < 600:
            wait = 5 * (attempt + 1)
            print(f"  {resp.status_code} — backing off {wait}s (attempt {attempt+1})")
            time.sleep(wait)
            last = resp
            continue
        return resp
    if isinstance(last, requests.Response):
        return last
    raise RuntimeError(f"request failed after retries: {last}")


def _month_windows(from_date, to_date):
    """Yield (gte, lt) date-string pairs, one calendar month each, covering the
    range. Windowing keeps every request well under LeafLink's ~6,050-result
    pagination ceiling, so the full history is retrievable."""
    y, m = int(from_date[:4]), int(from_date[5:7])
    windows = []
    while True:
        start = f"{y:04d}-{m:02d}-01"
        ny, nm = (y + 1, 1) if m == 12 else (y, m + 1)
        nxt = f"{ny:04d}-{nm:02d}-01"
        windows.append((start, nxt))
        if start[:7] >= to_date[:7]:
            break
        y, m = ny, nm
    return windows


def _fetch_window(gte, lt):
    """Page through one date window completely, following `next`."""
    params = {"page_size": PAGE_SIZE, "page": 1,
              "created_on__gte": gte, "created_on__lt": lt,
              "ordering": "created_on"}
    if INCLUDE_CHILDREN:
        params["include_children"] = INCLUDE_CHILDREN
    url = f"{API_BASE}{ENDPOINT}"
    out, page, reported = [], 0, None
    resp = _get(url, params)
    if resp.status_code == 400:
        # Date filter rejected — surface clearly rather than silently mis-pulling.
        print(f"ERROR 400 on window {gte}..{lt}: {resp.text[:200]}")
        sys.exit(1)
    if resp.status_code == 401:
        print("ERROR: 401 Unauthorized — key missing/wrong/revoked."); sys.exit(1)
    if resp.status_code == 403:
        print("ERROR: 403 Forbidden — app lacks Orders read permission."); sys.exit(1)
    if resp.status_code != 200:
        print(f"ERROR: status {resp.status_code}\n{resp.text[:300]}"); sys.exit(1)
    while True:
        data = resp.json()
        if reported is None:
            reported = data.get("count")
        batch = data.get("results", data if isinstance(data, list) else [])
        out.extend(batch)
        page += 1
        nxt = data.get("next") if isinstance(data, dict) else None
        if not nxt or (MAX_PAGES and page >= MAX_PAGES):
            break
        resp = _get(nxt, None)
        if resp.status_code != 200:
            # Don't return a half window — fail loudly so partial data is never committed.
            print(f"ERROR: window {gte}..{lt} page fetch returned {resp.status_code}; aborting.")
            sys.exit(1)
    # If a single month ever exceeds the cap, warn (would need finer windows).
    if reported and len(out) < reported:
        print(f"  WARNING: window {gte}..{lt} returned {len(out)} of {reported} "
              "(month exceeds pagination cap — needs finer windows).")
    return out, reported


def fetch_all(start_override: str = "") -> list:
    if not API_KEY:
        print("ERROR: LEAFLINK_API_KEY is empty.")
        sys.exit(1)
    today = datetime.now().strftime("%Y-%m-%d")
    start = start_override or FROM_DATE or "2020-01-01"
    windows = _month_windows(start, today)
    print(f"Pulling {len(windows)} monthly windows ({start} .. {today})")
    seen, orders = set(), []
    for gte, lt in windows:
        batch, reported = _fetch_window(gte, lt)
        new = 0
        for o in batch:
            key = o.get("number") or o.get("id")
            if key in seen:
                continue
            seen.add(key)
            orders.append(o)
            new += 1
        print(f"  {gte} -> {lt}: {len(batch)} fetched ({new} new) | total {len(orders)}")
    return orders


# ----------------------------------------------------------------------------
def _first(d, *keys):
    for k in keys:
        if isinstance(d, dict) and d.get(k) not in (None, "", []):
            return d.get(k)
    return None


def _amount(v):
    if isinstance(v, dict):
        v = v.get("amount")
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _name_of(v):
    if isinstance(v, list):
        return ", ".join(p for p in (_name_of(x) for x in v) if p)
    if isinstance(v, dict):
        return _first(v, "name", "title", "display_name", "full_name") or ""
    if isinstance(v, str):
        return v
    return ""


def _date_key(s):
    if not s:
        return None
    s = str(s)
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        return s[:10]
    return None


def _frozen_product(li):
    fd = li.get("frozen_data")
    prod = fd.get("product") if isinstance(fd, dict) else None
    return prod if isinstance(prod, dict) else {}


def _payment_status(o):
    if o.get("paid"):
        return "Paid"
    due = _date_key(o.get("payment_due_date"))
    today = datetime.now().strftime("%Y-%m-%d")
    if due and due < today:
        return "Overdue"
    return "Unpaid"


# --- Customer enrichment ----------------------------------------------------
# LeafLink order payloads don't include the sales rep / buyer state. The
# customers endpoint does (it's the seller's customer list). We pull it once,
# build lookup maps keyed by customer id AND normalized buyer name, and stamp
# each order. All best-effort + fail-safe: any failure leaves fields blank.

def _person_name(v):
    """Name of a rep/person. Handles dicts with full_name/name or first+last."""
    if isinstance(v, list):
        return ", ".join(p for p in (_person_name(x) for x in v) if p)
    if isinstance(v, dict):
        n = _first(v, "full_name", "name", "display_name", "title")
        if n:
            return n
        fn = str(v.get("first_name") or "").strip()
        ln = str(v.get("last_name") or "").strip()
        combo = (fn + " " + ln).strip()
        if combo:
            return combo
        return _first(v, "email", "username") or ""
    if isinstance(v, str):
        return v
    return ""


def _manager_ids(c):
    """Assigned rep(s) on a customer = the `managers` field (list of user IDs).
    Also tolerate a few alternate shapes / names."""
    if not isinstance(c, dict):
        return []
    ids = []
    for k in ("managers", "sales_reps", "assigned_sales_reps", "account_managers",
              "sales_rep", "account_manager"):
        v = c.get(k)
        if v in (None, "", []):
            continue
        items = v if isinstance(v, list) else [v]
        for x in items:
            if isinstance(x, bool):
                continue
            if isinstance(x, int):
                ids.append(str(x))
            elif isinstance(x, dict):
                xid = _first(x, "id", "pk", "user_id")
                nm = _person_name(x)
                ids.append(nm if nm else (str(xid) if xid is not None else ""))
            elif isinstance(x, str) and x.strip():
                ids.append(x.strip())
    out, seen = [], set()
    for i in ids:
        if i and i not in seen:
            seen.add(i)
            out.append(i)
    return out


def fetch_users():
    """Build {user_id(str): name} by paging the users endpoint(s). {} on failure."""
    if not API_KEY:
        return {}
    out = {}
    for ep in USERS_ENDPOINTS:
        url = f"{API_BASE}{ep}"
        resp = _get(url, {"page_size": PAGE_SIZE, "page": 1})
        if resp.status_code != 200:
            continue
        try:
            data = resp.json()
        except Exception:
            continue
        added = 0
        while True:
            batch = data.get("results", data if isinstance(data, list) else [])
            for u in batch:
                if not isinstance(u, dict):
                    continue
                uid = _first(u, "id", "pk", "user_id")
                nm = _person_name(u)
                if uid is not None and nm:
                    out.setdefault(str(uid), nm)
                    added += 1
            nxt = data.get("next") if isinstance(data, dict) else None
            if not nxt:
                break
            resp = _get(nxt, None)
            if resp.status_code != 200:
                break
            try:
                data = resp.json()
            except Exception:
                break
        if added:
            print(f"  resolved {added} user name(s) from {ep}")
    return out


def fetch_brands():
    """Build {brand_id(str): name} by paging the brands endpoint(s). {} on failure."""
    if not API_KEY:
        return {}
    out = {}
    for ep in BRANDS_ENDPOINTS:
        url = f"{API_BASE}{ep}"
        resp = _get(url, {"page_size": PAGE_SIZE, "page": 1})
        if resp.status_code != 200:
            continue
        try:
            data = resp.json()
        except Exception:
            continue
        added = 0
        while True:
            batch = data.get("results", data if isinstance(data, list) else [])
            for b in batch:
                if not isinstance(b, dict):
                    continue
                bid = _first(b, "id", "pk", "brand_id")
                nm = _name_of(b)
                if bid is not None and nm:
                    out.setdefault(str(bid), nm)
                    added += 1
            nxt = data.get("next") if isinstance(data, dict) else None
            if not nxt:
                break
            resp = _get(nxt, None)
            if resp.status_code != 200:
                break
            try:
                data = resp.json()
            except Exception:
                break
        if added:
            print(f"  resolved {added} brand name(s) from {ep}")
            break
    return out


def _state_of(c):
    if not isinstance(c, dict):
        return ""
    for k in ("state", "state_code", "region"):
        v = c.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    for k in ("buyer", "company", "address", "billing_address", "shipping_address",
              "default_address", "location"):
        sub = c.get(k)
        if isinstance(sub, dict):
            for kk in ("state", "state_code", "region"):
                v = sub.get(kk)
                if isinstance(v, str) and v.strip():
                    return v.strip()
    return ""


def _license_of(c):
    if not isinstance(c, dict):
        return ""
    for k in ("license", "license_number", "license_no"):
        v = c.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    for sk in ("buyer", "company"):
        sub = c.get(sk)
        if isinstance(sub, dict):
            for k in ("license", "license_number", "license_no"):
                v = sub.get(k)
                if isinstance(v, str) and v.strip():
                    return v.strip()
    return ""


def _city_of(c):
    if not isinstance(c, dict):
        return ""
    v = c.get("city")
    if isinstance(v, str) and v.strip():
        return v.strip()
    for sk in ("address", "delivery_address", "corporate_address", "buyer", "company"):
        sub = c.get(sk)
        if isinstance(sub, dict):
            v = sub.get("city")
            if isinstance(v, str) and v.strip():
                return v.strip()
    return ""


def _cust_names(c):
    """All plausible name strings for a customer, normalized for matching."""
    names = set()
    for nm in (_name_of(c), _name_of(c.get("buyer") if isinstance(c, dict) else None),
               _first(c, "name", "display_name", "company_name") if isinstance(c, dict) else None):
        if nm:
            names.add(str(nm).strip().lower())
    return names


def fetch_customers():
    """Page through the customers endpoint. Returns [] on any failure (fail-safe)."""
    if not API_KEY:
        return []
    url = f"{API_BASE}{CUSTOMERS_ENDPOINT}"
    resp = _get(url, {"page_size": PAGE_SIZE, "page": 1})
    if resp.status_code == 403:
        print("  NOTE: 403 on customers endpoint — the App lacks 'Customers' read "
              "permission. Add it in LeafLink (Settings > Applications) to enable "
              "sales-rep enrichment. Skipping for now.")
        return []
    if resp.status_code == 404:
        print(f"  NOTE: 404 on {CUSTOMERS_ENDPOINT} — endpoint path may differ; "
              "set LEAFLINK_CUSTOMERS_ENDPOINT. Skipping rep enrichment.")
        return []
    if resp.status_code != 200:
        print(f"  NOTE: customers endpoint returned {resp.status_code}; "
              "skipping rep enrichment.")
        return []
    out = []
    while True:
        data = resp.json()
        batch = data.get("results", data if isinstance(data, list) else [])
        out.extend(batch)
        nxt = data.get("next") if isinstance(data, dict) else None
        if not nxt:
            break
        resp = _get(nxt, None)
        if resp.status_code != 200:
            break
    return out


def build_enrichment(customers, user_map):
    """Build id/name -> rep/state/license maps from the customer list.
    Rep = the customer's `managers` (user IDs) resolved via user_map; falls back
    to a 'Rep #<id>' label when a name can't be resolved."""
    enrich = {"rep_by_id": {}, "rep_by_name": {},
              "state_by_id": {}, "state_by_name": {},
              "lic_by_id": {}, "lic_by_name": {},
              "city_by_id": {}, "city_by_name": {}}
    reps_found = 0
    for c in customers:
        cid = _first(c, "id", "pk", "customer_id")
        mids = _manager_ids(c)
        rep = ", ".join(user_map.get(i) or (i if not i.isdigit() else f"Rep #{i}")
                        for i in mids) if mids else ""
        state, lic, city = _state_of(c), _license_of(c), _city_of(c)
        if rep:
            reps_found += 1
        if cid is not None:
            if rep:
                enrich["rep_by_id"].setdefault(str(cid), rep)
            if state:
                enrich["state_by_id"].setdefault(str(cid), state)
            if lic:
                enrich["lic_by_id"].setdefault(str(cid), lic)
            if city:
                enrich["city_by_id"].setdefault(str(cid), city)
        for nm in _cust_names(c):
            if rep:
                enrich["rep_by_name"].setdefault(nm, rep)
            if state:
                enrich["state_by_name"].setdefault(nm, state)
            if lic:
                enrich["lic_by_name"].setdefault(nm, lic)
            if city:
                enrich["city_by_name"].setdefault(nm, city)
    enrich["_reps_found"] = reps_found
    return enrich


def _order_customer_keys(o):
    """Return (customer_id_str, normalized_name) for joining to the enrich maps."""
    cust = o.get("customer")
    cid = ""
    if isinstance(cust, dict):
        v = _first(cust, "id", "pk", "customer_id")
        cid = str(v) if v is not None else ""
    elif isinstance(cust, (int, str)) and str(cust).strip():
        cid = str(cust).strip()
    nm = (_name_of(cust) or _name_of(o.get("buyer")) or "").strip().lower()
    return cid, nm


def flatten(orders, brand_q, from_date="", seller_id="", enrich=None, inv=None, brand_map=None):
    enrich = enrich or {}
    rep_by_id = enrich.get("rep_by_id", {}); rep_by_name = enrich.get("rep_by_name", {})
    state_by_id = enrich.get("state_by_id", {}); state_by_name = enrich.get("state_by_name", {})
    lic_by_id = enrich.get("lic_by_id", {}); lic_by_name = enrich.get("lic_by_name", {})
    city_by_id = enrich.get("city_by_id", {}); city_by_name = enrich.get("city_by_name", {})
    inv = inv or {}
    inv_by_id = inv.get("by_id", {}); inv_by_sku = inv.get("by_sku", {})
    inv_brand_by_id = inv.get("brand_by_id", {}); inv_brand_by_sku = inv.get("brand_by_sku", {})
    brand_map = brand_map or {}
    rows = []
    seller_ids, brand_ids_seen = set(), set()
    matched = total_lines = skipped_old = skipped_company = skipped_status = 0
    name_fallback = 0
    rep_orders = 0
    state_fixed, state_blank_filled = {}, 0
    recon_order_total = recon_net_total = recon_gross_total = 0.0
    brand_q = (brand_q or "").strip().lower()
    from_date = (from_date or "").strip()
    seller_id = str(seller_id or "").strip()

    for o in orders:
        s = o.get("seller")
        sid = s if not isinstance(s, dict) else s.get("id")
        if s is not None:
            seller_ids.add(sid)
        for b in (o.get("brand_ids") or []):
            brand_ids_seen.add(b)
        _oids = [str(b) for b in (o.get("brand_ids") or [])]
        _onames = [brand_map.get(i) for i in _oids if brand_map.get(i)]
        order_brand = ", ".join(_onames) if _onames else (("Brand " + _oids[0]) if _oids else "")

        # Company filter: only Medfarms (seller id).
        if seller_id and str(sid) != seller_id:
            skipped_company += 1
            continue

        # Status filter: drop non-sold statuses (Cancelled/Rejected by default).
        ostatus = _first(o, "status", "order_status")
        if EXCLUDE_STATUSES and str(ostatus or "").lower() in EXCLUDE_STATUSES:
            skipped_status += 1
            continue

        order_date = _first(o, "created_on", "created", "order_placed_date", "date")
        if from_date:
            dk = _date_key(order_date)
            if dk and dk < from_date:
                skipped_old += 1
                continue

        order_total = _amount(o.get("total"))
        if order_total is not None:
            recon_order_total += order_total

        # Enrich: assigned sales rep / state / license from the customers endpoint,
        # joined by customer id first, then normalized buyer name.
        _cid, _cnm = _order_customer_keys(o)
        _rep = (rep_by_id.get(_cid) or rep_by_name.get(_cnm)
                or _name_of(_first(o, "sales_rep", "sales_reps")) or "")
        _state = state_by_id.get(_cid) or state_by_name.get(_cnm) or ""
        _lic = lic_by_id.get(_cid) or lic_by_name.get(_cnm) or ""
        _city = city_by_id.get(_cid) or city_by_name.get(_cnm) or ""
        if HOME_STATE:
            _raw_state = (_state or "").strip()
            if _raw_state.upper() != HOME_STATE.upper():
                if _raw_state:
                    state_fixed[_raw_state.upper()] = state_fixed.get(_raw_state.upper(), 0) + 1
                else:
                    state_blank_filled += 1
                _state = HOME_STATE
        if _rep:
            rep_orders += 1

        common = {
            "order_number": _first(o, "short_id", "number", "id"),
            "order_uid": _first(o, "number", "id"),
            "order_status": _first(o, "status", "order_status"),
            "order_date": order_date,
            "delivery_date": _first(o, "ship_date", "delivery_date"),
            "buyer_name": _name_of(o.get("customer")) or _name_of(o.get("buyer")),
            "buyer_state": _state,
            "buyer_city": _city,
            "buyer_license": _lic,
            "sales_rep": _rep,
            "payment_status": _payment_status(o),
            "paid": bool(o.get("paid")),
            "payment_term": o.get("payment_term") or "",
            "order_total": order_total,
        }

        # Pass 1: parse every line, compute gross, and the order's gross subtotal.
        parsed = []
        order_gross = 0.0
        for li in (o.get("line_items") or o.get("lineitems") or []):
            if not isinstance(li, dict):
                continue
            prod = _frozen_product(li)
            pname = prod.get("name") or _first(li, "product_name") or ""
            _sku = (prod.get("sku") or "").strip()
            _lpid = li.get("product")
            _lpid = str(_lpid) if _lpid is not None else (str(prod.get("id")) if prod.get("id") is not None else "")
            _cat_brand = inv_brand_by_sku.get(_sku) or inv_brand_by_id.get(_lpid) or ""
            _pbid = prod.get("brand")
            if isinstance(_pbid, dict):
                _pbid = _pbid.get("id")
            _line_brand = brand_map.get(str(_pbid)) if _pbid is not None else ""
            # Name-based fallback, MULTI-BRAND ORDERS ONLY: when a line has no precise
            # brand (SKU not in catalog), match its product name against THIS order's
            # own resolved brand names. Accept only when exactly one of them matches,
            # so we never reassign to a brand the order didn't actually contain.
            _name_brand = ""
            if not _cat_brand and not _line_brand and len(_onames) > 1:
                _pn = pname.lower()
                _hits = list({nm for nm in _onames if nm and nm.lower() in _pn})
                if len(_hits) == 1:
                    _name_brand = _hits[0]
                    name_fallback += 1
            # Precise catalog/line brand wins, then the constrained name match, then
            # the order-level brand_ids label for anything still ambiguous.
            brand = (_cat_brand or _line_brand or _name_brand or order_brand
                     or _name_of(prod.get("brand")) or _name_of(prod.get("brand_name")) or pname)
            qty = _amount(li.get("quantity")) or 0.0
            mult = _amount(li.get("unit_multiplier")) or 1.0
            sold_units = qty / mult if mult else qty
            unit_price = _amount(li.get("ordered_unit_price"))
            sale_price = _amount(li.get("sale_price"))
            on_sale = bool(li.get("on_sale")) or (sale_price or 0) > 0
            eff = sale_price if (on_sale and (sale_price or 0) > 0) else unit_price
            gross = (eff or 0) * sold_units
            order_gross += gross
            parsed.append((li, prod, pname, brand, qty, mult, sold_units,
                           unit_price, sale_price, gross))

        # Net allocation: scale each line's gross so the order's lines sum to the
        # actual order total (distributes order-level discount/tax/shipping). This
        # makes the dashboard's revenue tie out to LeafLink's order totals exactly.
        scale = (order_total / order_gross) if (order_total is not None and order_gross) else 1.0
        recon_gross_total += order_gross
        recon_net_total += order_gross * scale

        # Pass 2: emit rows for brand matches.
        for (li, prod, pname, brand, qty, mult, sold_units,
             unit_price, sale_price, gross) in parsed:
            total_lines += 1
            if brand_q and brand_q not in brand.lower():
                continue
            matched += 1
            net = gross * scale
            _pid = li.get("product")
            _pid = str(_pid) if _pid is not None else (str(prod.get("id")) if prod.get("id") is not None else "")
            _sku = prod.get("sku") or ""
            _cur_inv = (inv_by_id.get(_pid) if _pid else None)
            if _cur_inv is None and _sku:
                _cur_inv = inv_by_sku.get(_sku)
            rows.append({
                **common,
                "brand": brand,
                "product_name": pname,
                "product_sku": prod.get("sku") or "",
                "current_inventory": _cur_inv,
                "product_line": prod.get("product_line_name") or "",
                "product_category": _name_of(prod.get("category")) or prod.get("product_line_name") or "",
                "product_type": prod.get("product_line_name") or "",
                "quantity": qty,
                "unit_multiplier": mult,
                "units_sold": sold_units,
                "unit_price": unit_price,
                "sale_price": sale_price,
                "is_sample": bool(li.get("is_sample")),
                "gross": round(gross, 2),
                "discount": round(gross - net, 2),
                "revenue": round(net, 2),
            })

    stats = {
        "seller_ids": sorted(x for x in seller_ids if x is not None),
        "brand_ids": sorted(brand_ids_seen),
        "matched": matched, "total_lines": total_lines, "skipped_old": skipped_old,
        "skipped_company": skipped_company, "skipped_status": skipped_status,
        "name_fallback": name_fallback,
        "rep_orders": rep_orders,
        "state_fixed": state_fixed, "state_blank_filled": state_blank_filled,
        "recon_order_total": round(recon_order_total, 2),
        "recon_net_total": round(recon_net_total, 2),
        "recon_gross_total": round(recon_gross_total, 2),
    }
    return rows, stats


# ----------------------------------------------------------------------------
def _inv_value(p):
    """First recognizable inventory field on a product row -> (value, field)."""
    for f in INV_FIELDS:
        if f in p and p[f] is not None:
            v = _amount(p[f])
            if v is not None:
                return v, f
    return None, None


def _product_status(p):
    """Lowercased listing status of a product, trying candidate field names.
    Returns ("", None) if no recognizable status field is present."""
    for f in STATUS_FIELDS:
        if f in p and p[f] is not None:
            return str(_name_of(p[f]) or p[f]).strip().lower(), f
    return "", None


def fetch_inventory(brand_map=None):
    """Current-inventory pull from the seller catalog, in two phases.

      Phase 1 — the company-scoped product list. Fast, but LeafLink sometimes
                truncates it (here it omitted Homiez and Hyman entirely).
      Phase 2 — a per-brand sweep: for every brand id auto-discovered from the
                brands endpoint, query products filtered to that brand, so brands
                missing from phase 1 still load. Results from both phases are
                merged and deduped by product id/sku.

    Returns {"by_id","by_sku","catalog","brand_by_id","brand_by_sku"}. Any
    failure leaves the maps empty and the dashboard simply shows '—'. The pull
    prints a per-endpoint / per-brand diagnostic so a real run reveals exactly
    which path surfaces each brand.
    """
    inv_by_id, inv_by_sku = {}, {}
    brand_by_id, brand_by_sku = {}, {}
    brand_map = brand_map or {}
    catalog, catalog_seen = [], set()
    field_hits = {}
    _sample = {"keys": None}

    def ingest(p):
        """Fold one product dict into the shared accumulators.
        Returns (counted, kept_in_catalog, status_string)."""
        if not isinstance(p, dict):
            return (False, False, "")
        if _sample["keys"] is None:
            _sample["keys"] = sorted(p.keys())
        val, f = _inv_value(p)
        pid = p.get("id")
        sku = str(p.get("sku") or "").strip()
        name = p.get("name") or ""
        line = (p.get("product_line_name") or _name_of(p.get("product_line"))
                or _name_of(p.get("category")) or "")
        _pb = p.get("brand")
        _pbid = _pb.get("id") if isinstance(_pb, dict) else _pb
        _pbname = _name_of(p.get("brand")) or _name_of(p.get("brand_name")) or ""
        if not _pbname and _pbid is not None:
            _pbname = brand_map.get(str(_pbid), "")
        if _pbname:
            if pid is not None:
                brand_by_id[str(pid)] = _pbname
            if sku:
                brand_by_sku[sku] = _pbname
        if val is not None:
            field_hits[f] = field_hits.get(f, 0) + 1
            if pid is not None:
                inv_by_id[str(pid)] = val
            if sku:
                inv_by_sku[sku] = val
        # Brand filter (BRAND_FILTER is "" on the multi-brand board, so kept).
        brand_str = (_name_of(p.get("brand")) or _name_of(p.get("brand_name")) or name).lower()
        if BRAND_FILTER.strip() and BRAND_FILTER.lower() not in brand_str:
            return (True, False, "")
        # Listing-state filter: keep only Available-type products. If no status
        # field is present, keep it rather than risk dropping the whole catalog.
        status, sf = _product_status(p)
        if sf is not None and LISTED_STATES and status not in LISTED_STATES:
            return (True, False, status)
        ckey = str(pid) if pid is not None else sku
        if ckey and ckey not in catalog_seen:
            catalog_seen.add(ckey)
            catalog.append({"id": str(pid) if pid is not None else "",
                            "sku": sku, "name": name, "line": line,
                            "status": status,
                            "brand": _pbname or _name_of(p.get("brand")) or "",
                            "inventory": _amount(p.get("quantity")),
                            "reserved": _amount(p.get("reserved_qty")),
                            "available": val})
        return (True, True, status)

    def scan(url, params, label):
        """Page through one endpoint, ingesting every product. Returns stats
        ({seen,kept,added,pages}) or None if the endpoint isn't usable."""
        before = len(catalog)
        seen = kept = pages = 0
        status_counts = {}
        resp = _get(url, params)
        if resp.status_code == 404:
            print(f"  inv[{label}]: 404 — skipped")
            return None
        if resp.status_code != 200:
            print(f"  inv[{label}]: HTTP {resp.status_code} — skipped")
            return None
        while resp is not None and pages < INV_MAX_PAGES:
            if resp.status_code != 200:
                break
            try:
                data = resp.json()
            except Exception:
                break
            items = data.get("results") if isinstance(data, dict) else data
            if not items:
                break
            for p in items:
                counted, keptone, status = ingest(p)
                if counted:
                    seen += 1
                    k = status or "(none)"
                    status_counts[k] = status_counts.get(k, 0) + 1
                    if keptone:
                        kept += 1
            pages += 1
            nxt = data.get("next") if isinstance(data, dict) else None
            if not nxt:
                break
            resp = _get(nxt, None)
        if pages >= INV_MAX_PAGES:
            print(f"  inv[{label}]: hit INV_MAX_PAGES={INV_MAX_PAGES} — raise LEAFLINK_INV_MAX_PAGES if more remain")
        added = len(catalog) - before
        print(f"  inv[{label}]: {seen} products, {kept} available, +{added} new "
              f"({pages}p) status={status_counts}")
        return {"seen": seen, "kept": kept, "added": added, "pages": pages}

    # ---- Phase 1: company-scoped product list ----
    if SELLER_ID:
        scan(f"{API_BASE}/api/v2/companies/{SELLER_ID}/products/",
             {"page_size": PAGE_SIZE, "page": 1}, f"company/{SELLER_ID}")
    for ep in PRODUCTS_ENDPOINTS:
        if "/companies/" in ep:
            continue  # already covered above
        # An unscoped /products/ would return the whole marketplace; only sweep
        # it if the user explicitly points PRODUCTS_ENDPOINTS at a scoped path.

    # ---- Phase 2: per-brand sweep over auto-discovered brand ids ----
    if INV_PER_BRAND and brand_map:
        print(f"  inv: per-brand sweep over {len(brand_map)} discovered brand(s) "
              f"via {INV_BRAND_ENDPOINTS} params={INV_BRAND_PARAMS}…")
        for bid, bname in sorted(brand_map.items(), key=lambda kv: (kv[1] or "").lower()):
            got = False
            for ep in INV_BRAND_ENDPOINTS:
                url = f"{API_BASE}{ep}"
                scoped = "/companies/" not in ep  # add company scope on global /products/
                for param in INV_BRAND_PARAMS:
                    params = {param: bid, "page_size": PAGE_SIZE, "page": 1}
                    if scoped and SELLER_ID:
                        params["company"] = SELLER_ID
                        params["seller"] = SELLER_ID
                    st = scan(url, params, f"{bname}#{bid}:{ep.split('/api/v2/')[-1]}?{param}")
                    if st and st["seen"] > 0:
                        got = True
                        break
                if got:
                    break
            if not got:
                print(f"  inv[{bname}#{bid}]: no products returned by any endpoint/param")

    # ---- Summary ----
    cat_by_brand = {}
    for c in catalog:
        b = c.get("brand") or "(unbranded)"
        cat_by_brand[b] = cat_by_brand.get(b, 0) + 1
    cat_by_brand = dict(sorted(cat_by_brand.items(), key=lambda kv: -kv[1]))
    print(f"Inventory: {len(catalog)} available SKUs across {len(cat_by_brand)} "
          f"brand(s): {cat_by_brand}")
    print(f"  inv maps: {len(inv_by_id)} by id / {len(inv_by_sku)} by sku; "
          f"inv fields hit={field_hits}; listed filter={sorted(LISTED_STATES)}")
    if _sample["keys"]:
        print(f"  first product keys: {_sample['keys']}")
    if not catalog:
        print("  Inventory pull found nothing — column will show '—'. Check the "
              "diagnostics above for which endpoint/param returns products.")
    return {"by_id": inv_by_id, "by_sku": inv_by_sku, "catalog": catalog,
            "brand_by_id": brand_by_id, "brand_by_sku": brand_by_sku}


def _csv_num(s):
    """Parse a CSV numeric cell like '12,100' or '10.0' -> float (None if blank)."""
    s = (s or "").replace(",", "").replace("$", "").strip()
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _find_inventory_csv():
    """Locate a committed inventory CSV in the repo. Returns a Path or None."""
    flag = INVENTORY_CSV.strip()
    if flag.lower() in ("off", "none", "0", "false"):
        return None
    if flag:
        p = Path(flag)
        return p if p.is_file() else None
    here = OUTPUT_FILE.parent
    exact = here / "inventory.csv"
    if exact.is_file():
        return exact
    # LeafLink's dated export names (inventory-overview_YYYY-MM-DD_...) sort by
    # date, so the lexicographically-last match is the newest export.
    cands = sorted(here.glob("inventory*.csv"))
    return cands[-1] if cands else None


def _csv_col(row, *names):
    """Tolerant column lookup: exact header first, then case-insensitive match."""
    for n in names:
        if n in row and row[n] not in (None, ""):
            return row[n]
    low = {k.lower(): k for k in row}
    for n in names:
        if n.lower() in low:
            return row[low[n.lower()]]
    return ""


def inventory_catalog_from_csv():
    """Build the dashboard inventory list from a committed LeafLink CSV export.

    Returns a list of catalog dicts (same shape fetch_inventory produces) or
    None when no CSV is present (so the API catalog is used as fallback).
    Filtered to LISTED_STATES (default 'available'), exactly like the API path.
    Touches only the displayed inventory — order/brand resolution is unaffected.
    """
    path = _find_inventory_csv()
    if not path:
        return None
    try:
        with open(path, encoding="utf-8-sig", newline="") as fh:
            reader = csv.DictReader(fh)
            out, seen, states = [], set(), {}
            for r in reader:
                status = str(_csv_col(r, "Listing State") or "").strip()
                states[status or "(blank)"] = states.get(status or "(blank)", 0) + 1
                if LISTED_STATES and status.lower() not in LISTED_STATES:
                    continue
                pid = str(_csv_col(r, "Product ID") or "").strip()
                sku = str(_csv_col(r, "SKU") or "").strip()
                key = pid or sku
                if key and key in seen:
                    continue
                if key:
                    seen.add(key)
                out.append({
                    "id": pid,
                    "sku": sku,
                    "name": str(_csv_col(r, "Name") or "").strip(),
                    "line": str(_csv_col(r, "Product Line") or "").strip(),
                    "status": status,
                    "brand": str(_csv_col(r, "Brand") or "").strip(),
                    "inventory": _csv_num(_csv_col(r, "Inventory (Units)")),
                    "reserved": _csv_num(_csv_col(r, "Reserved Inventory (Units)")),
                    "available": _csv_num(_csv_col(r, "Available Inventory (Units)")),
                })
    except Exception as e:
        print(f"  NOTE: could not read inventory CSV {path} ({e}); using API catalog.")
        return None
    by_brand = {}
    for e in out:
        b = e["brand"] or "(blank)"
        by_brand[b] = by_brand.get(b, 0) + 1
    print(f"Inventory: built from {path.name} -> {len(out)} listed products "
          f"(filter={sorted(LISTED_STATES) or 'all states'}; "
          f"file states={states}; by brand={by_brand})")
    return out


def load_existing():
    """Return (rows, from_date) from the committed sales_data.json, or ([], "")."""
    if OUTPUT_FILE.exists():
        try:
            with gzip.open(OUTPUT_FILE, "rt", encoding="utf-8") as f:
                d = json.load(f)
            return d.get("rows", []) or [], d.get("from_date", "") or ""
        except Exception as e:
            print(f"  NOTE: could not read existing {OUTPUT_FILE.name} ({e}); full pull.")
    return [], ""


def main():
    # Full vs incremental. Incremental only when enabled AND the committed data was
    # already backfilled at this FROM_DATE, so we never silently skip history.
    existing_rows, existing_from = load_existing()
    incremental = (INCREMENTAL_DAYS > 0 and bool(existing_rows) and existing_from == FROM_DATE)
    if incremental:
        inc_start = (datetime.now() - timedelta(days=INCREMENTAL_DAYS)).strftime("%Y-%m-%d")
        pull_start = max(inc_start, FROM_DATE)
        print(f"INCREMENTAL run: pulling {pull_start} .. today, "
              f"merging into {len(existing_rows)} existing rows (last {INCREMENTAL_DAYS} days).")
        orders = fetch_all(start_override=pull_start)
    else:
        why = ("incremental disabled (LEAFLINK_INCREMENTAL_DAYS=0)" if INCREMENTAL_DAYS <= 0
               else "no existing data yet" if not existing_rows
               else f"existing data backfilled at '{existing_from}', need '{FROM_DATE}' — backfilling")
        print(f"FULL pull ({why}).")
        orders = fetch_all()
    print(f"Fetched {len(orders)} orders.")

    # Enrich with sales rep (and state/license when available) from customers.
    print(f"Fetching customers from {CUSTOMERS_ENDPOINT} for sales-rep enrichment...")
    customers = fetch_customers()
    print("Resolving rep names from users endpoint(s)...")
    user_map = fetch_users() if customers else {}
    enrich = build_enrichment(customers, user_map)
    print(f"Users resolved to names: {len(user_map)} | Customers fetched: {len(customers)} "
          f"| customers with a rep: {enrich.get('_reps_found', 0)}")
    if customers and enrich.get("_reps_found", 0) == 0:
        print("  No 'managers' on customers. First customer keys: "
              f"{sorted((customers[0] or {}).keys())}")

    # Current inventory from the seller product catalog (best-effort, bounded).
    # OFF by default so it can never stall the orders backfill.
    print("Fetching brands for brand-name resolution...")
    brand_map = fetch_brands()
    for _kv in [x for x in os.getenv("LEAFLINK_BRAND_OVERRIDES", "").split(",") if ":" in x]:
        _k, _v = _kv.split(":", 1); brand_map[_k.strip()] = _v.strip()
    if brand_map:
        print(f"Brands resolved: {len(brand_map)} | e.g. {list(brand_map.items())[:6]}")
    else:
        print("Brands resolved: 0 (set LEAFLINK_BRANDS_ENDPOINTS / LEAFLINK_BRAND_OVERRIDES)")

    # Product catalog: needed for the SKU->brand map (splits multi-brand orders to
    # the correct brand per line) and, when enabled, current inventory quantities.
    if PULL_INVENTORY or RESOLVE_BRANDS_FROM_CATALOG:
        print("Fetching product catalog (SKU->brand map"
              + (" + current inventory" if PULL_INVENTORY else "") + ")...")
        inv = fetch_inventory(brand_map)
        print(f"Catalog brand map: {len(inv.get('brand_by_sku', {}))} sku / "
              f"{len(inv.get('brand_by_id', {}))} id entries")
    else:
        inv = {"by_id": {}, "by_sku": {}, "brand_by_id": {}, "brand_by_sku": {}}

    # If a LeafLink inventory CSV is committed to the repo, it is the source of
    # truth for the dashboard's Current Inventory section (the products API can
    # under-return the catalog). This replaces ONLY the displayed catalog; the
    # SKU->brand map and order resolution above are left exactly as they were.
    _csv_catalog = inventory_catalog_from_csv()
    if _csv_catalog is not None:
        inv["catalog"] = _csv_catalog

    if orders:
        first = orders[0]
        order_lite = {k: v for k, v in first.items() if k not in ("line_items", "lineitems")}
        print("\n--- FIRST ORDER (line_items removed) ---")
        print(json.dumps(order_lite, default=str)[:2500])
        lis = first.get("line_items") or first.get("lineitems") or []
        if lis and isinstance(lis[0], dict):
            print("\n--- FIRST LINE ITEM ---")
            print(json.dumps(lis[0], default=str)[:2500])
        print("--- end sample ---\n")

    rows, st = flatten(orders, BRAND_FILTER, FROM_DATE, SELLER_ID, enrich, inv, brand_map)

    if BRAND_FILTER.strip() and st["matched"] == 0 and st["total_lines"] > 0:
        print(f"WARNING: brand '{BRAND_FILTER}' matched 0 of {st['total_lines']} lines.")
        print("Keeping ALL rows so you still get data — check the product-name field.")
        rows, st = flatten(orders, "", FROM_DATE, SELLER_ID, enrich, inv, brand_map)

    if incremental:
        fresh_uids = {str(o.get("number") or o.get("id")) for o in orders}
        kept = [r for r in existing_rows if str(r.get("order_uid")) not in fresh_uids]
        # Refresh current inventory on the kept (older) rows too, joined by sku,
        # so the Current Inv. column stays current for products not sold recently.
        by_sku = (inv or {}).get("by_sku", {})
        if by_sku:
            for r in kept:
                s = r.get("product_sku")
                if s and s in by_sku:
                    r["current_inventory"] = by_sku[s]
        fresh_n = len(rows)
        rows = kept + rows
        print(f"MERGED: {len(kept)} older rows kept + {fresh_n} fresh = {len(rows)} total "
              f"(replaced {len(existing_rows) - len(kept)} rows for {len(fresh_uids)} window orders).")

    print(f"\nSeller id(s) seen: {st['seller_ids']}  (Medfarms = 9105)")
    print(f"Company filter: seller {SELLER_ID or '(none)'}  ->  skipped {st['skipped_company']} other-company orders")
    print(f"Status filter: excluded {EXCLUDE_STATUSES or '(none)'}  ->  skipped {st['skipped_status']} orders")
    print(f"Brand id(s) in data:  {st['brand_ids']}   (Chill Medicated = 2425)")
    print(f"Date floor: {FROM_DATE or '(none)'}  ->  skipped {st['skipped_old']} older orders")
    if HOME_STATE:
        print(f"State normalize -> {HOME_STATE}: fixed non-{HOME_STATE} states {st.get('state_fixed') or '{}'}, "
              f"filled {st.get('state_blank_filled', 0)} blank line(s)")
    print(f"Line items: {st['total_lines']} in range -> {st['matched']} kept (brand '{BRAND_FILTER}')")
    print(f"Name-based fallback reassigned {st['name_fallback']} line(s) in multi-brand orders.")
    print(f"Sales-rep enrichment: {st['rep_orders']} orders matched a rep "
          f"(0 = customers endpoint had no rep data / not permitted)")
    # After net allocation, sum of net should equal sum of order totals (ratio ~1.000).
    ot, nt, gt = st["recon_order_total"], st["recon_net_total"], st["recon_gross_total"]
    ratio = (nt / ot) if ot else 0
    print(f"RECONCILE (all brands): net ${nt:,.2f} vs order totals ${ot:,.2f} "
          f"(ratio {ratio:.4f}; should be ~1.0000)")
    print(f"  gross (list price) was ${gt:,.2f} -> order-level discounts/adj ${gt-nt:,.2f}")

    payload = {
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "source": "leaflink",
        "seller_ids": st["seller_ids"],
        "company_filter": SELLER_ID,
        "brand_filter": BRAND_FILTER,
        "from_date": FROM_DATE,
        "order_count": len({str(r.get("order_uid")) for r in rows if r.get("order_uid")}),
        "row_count": len(rows),
        "inventory": (inv or {}).get("catalog", []),
        "rows": rows,
    }
    with gzip.open(OUTPUT_FILE, "wt", encoding="utf-8") as f:
        json.dump(payload, f, separators=(",", ":"), default=str)
    size_mb = OUTPUT_FILE.stat().st_size / 1e6
    print(f"\nSaved -> {OUTPUT_FILE} ({size_mb:.2f} MB, {len(rows)} rows)")


if __name__ == "__main__":
    main()
