"""
setup/generate_landing_data.py   (NOT part of the pipeline - keep it outside src/)

Writes CSV files into the landing volumes:
    /Volumes/<catalog>/landing/orders         (orders: discovered from existing CSV or synthetic)
    /Volumes/<catalog>/landing/customers_cdc   (CDC feed: op I/U/D + event_ts)
    /Volumes/<catalog>/landing/products        (delta feed: changed/new products)
    /Volumes/<catalog>/landing/payments        (payments per order)

Design rules (so later levels never mismatch):
  * customer_id / product_id / order_id are DISCOVERED from your existing orders CSV,
    so joins in later levels (fact_sales etc.) always match. Fallback: synthetic ids.
  * ~5% extra BAD rows are appended to every file (they must land in quarantine).
  * The 3rd file of every feed has ONE EXTRA COLUMN (always LAST) -> goes to _rescued_data.
  * Everything is deterministic (SEED). Existing files are never overwritten unless FORCE.
  * Stage the arrival: DAYS=[1] -> run pipeline -> DAYS=[2] -> run -> DAYS=[3] -> run.
"""
import csv, glob, math, os, random
from datetime import datetime, timedelta

# ----------------------------- settings -----------------------------
CATALOG = os.environ.get("RETAIL_CATALOG", "retail_dev")
ROOT    = os.environ.get("RETAIL_ROOT", f"/Volumes/{CATALOG}/landing")
ORDER_DIRS = [f"{ROOT}/orders", f"/Volumes/{CATALOG}/bronze/orders"]  # where your orders.csv lives
DAYS    = [1, 2, 3]       # which daily files to write this run
FORCE   = False           # True = overwrite existing files (then FULL REFRESH the pipeline)
BAD_RATE = 0.05
SEED     = 42
BASE_DAY = datetime(2026, 10, 1)
# --------------------------------------------------------------------

rnd = random.Random(SEED)
FIRST = ["Ali", "Sara", "Mei", "Omar", "Lena", "Hugo", "Aisha", "Tom", "Nina", "Raj", "Eva", "Karim"]
LAST  = ["Tan", "Rahimi", "Lee", "Khan", "Novak", "Silva", "Chen", "Meyer", "Haddad", "Kumar"]
CITIES = [("Kuala Lumpur", "MY"), ("Cyberjaya", "MY"), ("Penang", "MY"), ("Singapore", "SG"),
          ("Frankfurt", "DE"), ("Berlin", "DE"), ("Dubai", "AE"), ("Istanbul", "TR")]
CATEGORIES = {"Beverages": ["Tea", "Coffee", "Juice"], "Snacks": ["Chips", "Cookies", "Nuts"],
              "Household": ["Soap", "Detergent", "Sponge"], "Electronics": ["Cable", "Charger", "Earbuds"]}
BRANDS = ["Acme", "Zenith", "Orchid", "Nimbus", "Karma"]
METHODS = ["CARD", "CASH", "WALLET", "BANK_TRANSFER"]


def ts(day, secs):
    return (BASE_DAY + timedelta(days=day - 1, seconds=secs)).strftime("%Y-%m-%d %H:%M:%S")


def n_bad(n):
    return max(1, math.ceil(n * BAD_RATE))


def write_csv(volume, filename, header, rows):
    path = f"{ROOT}/{volume}/{filename}"
    if os.path.exists(path) and not FORCE:
        print(f"  skip (exists): {path}")
        return
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="") as fh:
        w = csv.writer(fh, lineterminator="\n")
        w.writerow(header)
        w.writerows(rows)
    print(f"  wrote {len(rows):>5} rows -> {path}")


# ------------------------- discover ids from orders -------------------------
def discover_orders(limit=5000):
    for d in ORDER_DIRS:
        found = {}
        for f in sorted(glob.glob(f"{d}/*.csv")):
            with open(f, newline="") as fh:
                for r in csv.DictReader(fh):
                    try:
                        oid = int(r["order_id"])
                        total = round(int(r["quantity"]) * float(r["unit_price"]), 2)
                    except (KeyError, TypeError, ValueError):
                        continue
                    cid, pid = (r.get("customer_id") or "").strip(), (r.get("product_id") or "").strip()
                    if oid in found or not cid or not pid or total <= 0:
                        continue
                    found[oid] = dict(order_id=oid, customer_id=cid, product_id=pid,
                                      quantity=int(r["quantity"]), unit_price=float(r["unit_price"]), total=total)
                    if len(found) >= limit:
                        return list(found.values()), d
        if found:
            return list(found.values()), d
    return [], None


def synthetic_orders(n=600):
    rows = []
    for i in range(1, n + 1):
        qty, price = rnd.randint(1, 5), round(rnd.uniform(3, 90), 2)
        rows.append(dict(order_id=i, customer_id=f"C{rnd.randint(1, 120):04d}", product_id=f"P{rnd.randint(1, 80):04d}",
                         quantity=qty, unit_price=price, total=round(qty * price, 2)))
    return rows


ORDERS, SRC = discover_orders()
if ORDERS:
    print(f"Discovered {len(ORDERS)} valid orders in {SRC}")
else:
    print("WARNING: no valid orders CSV found -> using SYNTHETIC ids (C0001.., P0001..). "
          "Joins with your real orders will NOT match.")
    ORDERS = synthetic_orders()
CUSTOMER_IDS = sorted({o["customer_id"] for o in ORDERS})[:500]
PRODUCT_IDS  = sorted({o["product_id"] for o in ORDERS})[:300]
print(f"Universe: {len(CUSTOMER_IDS)} customers, {len(PRODUCT_IDS)} products, {len(ORDERS)} orders")


# ================================ CUSTOMERS (CDC) ================================
CUST_COLS = ["customer_id", "first_name", "last_name", "email", "city", "country",
             "loyalty_tier", "op", "event_ts"]


def new_customer(cid, i):
    first, last = rnd.choice(FIRST), rnd.choice(LAST)
    city, country = rnd.choice(CITIES)
    return dict(customer_id=cid, first_name=first, last_name=last,
                email=f"{first}.{last}.{i}@example.com".lower(), city=city, country=country,
                loyalty_tier="BRONZE")


def crow(c, op, t):
    return [c["customer_id"], c["first_name"], c["last_name"], c["email"], c["city"],
            c["country"], c["loyalty_tier"], op, t]


def build_customers():
    k = max(1, len(CUSTOMER_IDS) // 10)
    initial, later = CUSTOMER_IDS[:-k], CUSTOMER_IDS[-k:]
    cust, files, info = {}, {}, {}
    # day 1: initial load
    files[1] = []
    for i, cid in enumerate(initial):
        cust[cid] = new_customer(cid, i)
        files[1].append(crow(cust[cid], "I", ts(1, 28800 + i)))
    # day 2: updates, deletes, brand-new customers
    rows, t = [], 28800
    upd = rnd.sample(sorted(cust), max(1, int(len(cust) * 0.30)))
    for cid in upd:
        c = cust[cid]
        c["city"], c["country"] = rnd.choice(CITIES)
        c["loyalty_tier"] = rnd.choice(["SILVER", "GOLD"])
        rows.append(crow(c, "U", ts(2, t))); t += 1
    upd_set = set(upd)
    pool = [c for c in sorted(cust) if c not in upd_set]
    deleted = set(rnd.sample(pool, max(1, int(len(cust) * 0.05))))
    for cid in sorted(deleted):
        rows.append(crow(cust[cid], "D", ts(2, t))); t += 1
    for i, cid in enumerate(later):
        cust[cid] = new_customer(cid, 10000 + i)
        rows.append(crow(cust[cid], "I", ts(2, t))); t += 1
    files[2] = rows
    # day 3: more updates + ONE late out-of-order event (dated day 1, 12:00)
    rows, t = [], 28800
    alive = [c for c in sorted(cust) if c not in deleted]
    for cid in rnd.sample(alive, max(1, int(len(alive) * 0.20))):
        c = cust[cid]
        c["loyalty_tier"] = rnd.choice(["GOLD", "PLATINUM"])
        rows.append(crow(c, "U", ts(3, t))); t += 1
    late_cid = upd[0]
    late = dict(cust[late_cid]); late["city"], late["country"] = "Lisbon", "PT"
    rows.append(crow(late, "U", ts(1, 43200)))
    files[3] = [r + [rnd.choice(["true", "false"])] for r in rows]   # extra LAST column
    info.update(late_customer=late_cid, deleted=sorted(deleted))
    return files, info


def bad_customers(rows):
    out = []
    for j, r in enumerate(rnd.sample(rows, min(len(rows), n_bad(len(rows))))):
        r = list(r)
        kind = j % 4
        if kind == 0: r[0] = ""                       # null customer_id
        elif kind == 1: r[7] = "X"                    # invalid op
        elif kind == 2: r[3], r[7] = "not-an-email", "U"
        else: r[8] = "not-a-date"                     # unparseable event_ts
        out.append(r)
    return out


# ================================ PRODUCTS ================================
PROD_COLS = ["product_id", "product_name", "category", "brand", "unit_cost",
             "list_price", "is_active", "updated_at"]


def new_product(pid):
    cat = rnd.choice(list(CATEGORIES)); brand = rnd.choice(BRANDS)
    cost = round(rnd.uniform(2, 80), 2)
    return dict(product_id=pid, product_name=f"{brand} {rnd.choice(CATEGORIES[cat])}", category=cat,
                brand=brand, unit_cost=cost, list_price=round(cost * rnd.uniform(1.2, 1.9), 2), is_active=True)


def prow(p, t):
    return [p["product_id"], p["product_name"], p["category"], p["brand"], f"{p['unit_cost']:.2f}",
            f"{p['list_price']:.2f}", str(p["is_active"]).lower(), t]


def build_products():
    k = max(1, len(PRODUCT_IDS) // 10)
    initial, later = PRODUCT_IDS[:-k], PRODUCT_IDS[-k:]
    prod, files = {}, {}
    files[1] = []
    for i, pid in enumerate(initial):
        prod[pid] = new_product(pid); files[1].append(prow(prod[pid], ts(1, 21600 + i)))
    rows, t = [], 21600
    changed = rnd.sample(sorted(prod), max(1, int(len(prod) * 0.15)))
    for pid in changed:
        prod[pid]["list_price"] = round(prod[pid]["list_price"] * 1.10, 2)
        rows.append(prow(prod[pid], ts(2, t))); t += 1
    changed_set = set(changed)
    off = rnd.sample([p for p in sorted(prod) if p not in changed_set], max(1, int(len(prod) * 0.05)))
    for pid in off:
        prod[pid]["is_active"] = False; rows.append(prow(prod[pid], ts(2, t))); t += 1
    for pid in later:
        prod[pid] = new_product(pid); rows.append(prow(prod[pid], ts(2, t))); t += 1
    files[2] = rows
    rows, t = [], 21600
    for pid in rnd.sample(sorted(prod), max(1, int(len(prod) * 0.10))):
        if pid == off[0]:
            continue
        prod[pid]["list_price"] = round(prod[pid]["list_price"] * 0.95, 2)
        rows.append(prow(prod[pid], ts(3, t))); t += 1
    prod[off[0]]["is_active"] = True; rows.append(prow(prod[off[0]], ts(3, t)))   # reactivation
    files[3] = [r + [f"SUP{rnd.randint(1, 9):03d}"] for r in rows]                  # extra LAST column
    return files


def bad_products(rows):
    out = []
    for j, r in enumerate(rnd.sample(rows, min(len(rows), n_bad(len(rows))))):
        r = list(r)
        kind = j % 4
        if kind == 0: r[0] = ""
        elif kind == 1: r[4] = "abc"       # unit_cost not numeric
        elif kind == 2: r[5] = "-9.99"     # negative list_price
        else: r[7] = "n/a"                 # bad updated_at
        out.append(r)
    return out


# ================================ PAYMENTS ================================
PAY_COLS = ["payment_id", "order_id", "payment_method", "amount", "currency", "payment_status", "payment_ts"]


def build_payments():
    orders = sorted(ORDERS, key=lambda o: o["order_id"])[:900]
    rnd.shuffle(orders)
    third = math.ceil(len(orders) / 3)
    files, n = {}, 0
    for day in (1, 2, 3):
        rows = []
        for i, o in enumerate(orders[(day - 1) * third: day * third]):
            n += 1
            status = rnd.choices(["SUCCESS", "FAILED", "PENDING", "REFUNDED"], [85, 8, 4, 3])[0]
            amount = o["total"]
            if status == "SUCCESS" and rnd.random() < 0.08:
                amount = round(o["total"] * 0.5, 2)          # partial payment -> PARTIAL in gold
            rows.append([f"PAY{n:06d}", o["order_id"], rnd.choice(METHODS), f"{amount:.2f}", "USD",
                         status, ts(day, 28800 + i * 20)])
        files[day] = rows
    files[3] = [r + [f"GW-{rnd.randint(100000, 999999)}"] for r in files[3]]       # extra LAST column
    return files


def bad_payments(rows):
    out = []
    for j, r in enumerate(rnd.sample(rows, min(len(rows), n_bad(len(rows))))):
        r = list(r)
        kind = j % 4
        if kind == 0: r[0] = ""
        elif kind == 1: r[3] = "abc"
        elif kind == 2: r[3] = "-25.00"
        else: r[5] = "UNKNOWN"
        out.append(r)
    return out


# ================================ ORDERS ================================
ORDER_COLS = ["order_id", "customer_id", "product_id", "order_date", "quantity", "unit_price"]


def build_orders():
    files = {}
    orders = sorted(ORDERS, key=lambda o: o["order_id"])
    third = math.ceil(len(orders) / 3)
    for day in (1, 2, 3):
        rows = []
        for o in orders[(day - 1) * third: day * third]:
            rows.append([o["order_id"], o["customer_id"], o["product_id"],
                        (BASE_DAY + timedelta(days=day - 1)).strftime("%Y-%m-%d"),
                        o["quantity"], f"{o['unit_price']:.2f}"])
        files[day] = rows
    files[3] = [r + [f"DC{rnd.randint(1, 99):03d}"] for r in files[3]]   # extra LAST column
    return files


def bad_orders(rows):
    out = []
    for j, r in enumerate(rnd.sample(rows, min(len(rows), n_bad(len(rows))))):
        r = list(r)
        kind = j % 4
        if kind == 0: r[0] = ""           # null order_id
        elif kind == 1: r[4] = "0"        # quantity not positive
        elif kind == 2: r[5] = "-9.99"    # negative unit_price
        else: r[3] = ""                   # null order_date
        out.append(r)
    return out


# ================================ ORDER ITEMS ================================
ORDER_ITEM_COLS = ["order_item_id", "order_id", "product_id", "quantity", "unit_price"]

def build_order_items():
    files = {}
    for day in (1, 2, 3):
        rows = []
        orders = sorted(ORDERS, key=lambda o: o["order_id"])
        third = math.ceil(len(orders) / 3)
        day_orders = orders[(day - 1) * third: day * third]
        n = (day - 1) * 5000
        for o in day_orders:
            num_items = rnd.randint(1, 3)
            for i in range(num_items):
                n += 1
                qty = rnd.randint(1, 5)
                price = round(rnd.uniform(3, 90), 2)
                rows.append([n, o["order_id"], o["product_id"], qty, f"{price:.2f}"])
        files[day] = rows
    files[3] = [r + [f"DC{rnd.randint(1, 99):03d}"] for r in files[3]]
    return files

def bad_order_items(rows):
    out = []
    for j, r in enumerate(rnd.sample(rows, min(len(rows), n_bad(len(rows))))):
        r = list(r)
        kind = j % 4
        if kind == 0: r[0] = ""
        elif kind == 1: r[1] = ""
        elif kind == 2: r[3] = "0"
        else: r[4] = "-9.99"
        out.append(r)
    return out


# ================================ ORDER RETURNS ================================
ORDER_RETURN_COLS = ["return_id", "order_id", "product_id", "return_date", "return_reason", "refund_amount"]
RETURN_REASONS = ["DAMAGED", "WRONG_ITEM", "NOT_AS_DESCRIBED", "LATE_DELIVERY"]

def build_order_returns():
    files = {}
    for day in (1, 2, 3):
        rows = []
        n = (day - 1) * 100
        orders = sorted(ORDERS, key=lambda o: o["order_id"])
        third = math.ceil(len(orders) / 3)
        day_orders = orders[(day - 1) * third: day * third]
        for o in day_orders:
            if rnd.random() < 0.10:
                n += 1
                return_day = min(day + 1, 3)
                rows.append([f"RET{n:06d}", o["order_id"], o["product_id"],
                            (BASE_DAY + timedelta(days=return_day - 1)).strftime("%Y-%m-%d"),
                            rnd.choice(RETURN_REASONS), f"{o['total']:.2f}"])
        files[day] = rows
    files[3] = [r + [f"RMA{rnd.randint(1, 999):03d}"] for r in files[3]]
    return files

def bad_order_returns(rows):
    out = []
    for j, r in enumerate(rnd.sample(rows, min(len(rows), n_bad(len(rows))))):
        r = list(r)
        kind = j % 4
        if kind == 0: r[0] = ""
        elif kind == 1: r[1] = ""
        elif kind == 2: r[5] = "-25.00"
        else: r[3] = ""
        out.append(r)
    return out


# ================================ CUSTOMER ADDRESSES ================================
ADDR_COLS = ["address_id", "customer_id", "address_type", "street", "city", "country", "is_current", "updated_at"]
ADDR_TYPES = ["HOME", "WORK", "SHIPPING"]
STREETS = ["Jalan Main 1", "Jalan Side 5", "Orchard Rd 12", "Frankfurt St 8", "Dubai Blvd 3", "Istanbul Ave 7"]

def build_customer_addresses():
    files = {}
    for day in (1, 2, 3):
        rows = []
        n = (day - 1) * 500
        for i, cid in enumerate(CUSTOMER_IDS):
            n += 1
            city, country = rnd.choice(CITIES)
            rows.append([f"ADDR{n:06d}", cid, rnd.choice(ADDR_TYPES), rnd.choice(STREETS),
                        city, country, str(rnd.random() < 0.8).lower(), ts(day, 28800 + i)])
        files[day] = rows
    files[3] = [r + [rnd.choice(["true", "false"])] for r in files[3]]
    return files

def bad_customer_addresses(rows):
    out = []
    for j, r in enumerate(rnd.sample(rows, min(len(rows), n_bad(len(rows))))):
        r = list(r)
        kind = j % 3
        if kind == 0: r[0] = ""
        elif kind == 1: r[1] = ""
        else: r[4] = ""
        out.append(r)
    return out


# ================================ CUSTOMER PREFERENCES ================================
PREF_COLS = ["preference_id", "customer_id", "preference_key", "preference_value", "updated_at"]
PREF_KEYS = ["EMAIL_OPT_IN", "SMS_OPT_IN", "LANGUAGE", "CATALOG"]

def build_customer_preferences():
    files = {}
    for day in (1, 2, 3):
        rows = []
        n = (day - 1) * 500
        for i, cid in enumerate(CUSTOMER_IDS):
            n += 1
            key = rnd.choice(PREF_KEYS)
            if key in ("EMAIL_OPT_IN", "SMS_OPT_IN", "CATALOG"):
                val = rnd.choice(["true", "false"])
            else:
                val = rnd.choice(["en", "es", "de", "fr"])
            rows.append([f"PRF{n:06d}", cid, key, val, ts(day, 28800 + i)])
        files[day] = rows
    files[3] = [r + [rnd.choice(["true", "false"])] for r in files[3]]
    return files

def bad_customer_preferences(rows):
    out = []
    for j, r in enumerate(rnd.sample(rows, min(len(rows), n_bad(len(rows))))):
        r = list(r)
        kind = j % 3
        if kind == 0: r[0] = ""
        elif kind == 1: r[1] = ""
        else: r[2] = ""
        out.append(r)
    return out


# ================================ PRODUCT CATEGORIES ================================
CAT_COLS = ["category_id", "category_name", "parent_category", "is_active"]

def build_product_categories():
    files = {}
    cats = [
        ("CAT001", "Beverages", "Food & Beverage", True),
        ("CAT002", "Snacks", "Food & Beverage", True),
        ("CAT003", "Household", "Home & Living", True),
        ("CAT004", "Electronics", "Electronics", True),
        ("CAT005", "Tea", "Beverages", True),
        ("CAT006", "Coffee", "Beverages", True),
        ("CAT007", "Juice", "Beverages", True),
        ("CAT008", "Chips", "Snacks", True),
        ("CAT009", "Cookies", "Snacks", True),
        ("CAT010", "Cable", "Electronics", True),
        ("CAT011", "Charger", "Electronics", True),
        ("CAT012", "Soap", "Household", True),
    ]
    for day in (1, 2, 3):
        rows = []
        for c in cats:
            rows.append([c[0], c[1], c[2], str(c[3]).lower()])
        files[day] = rows
    files[3] = [r + [f"CAT-{rnd.randint(1, 99):03d}"] for r in files[3]]
    return files

def bad_product_categories(rows):
    out = []
    for j, r in enumerate(rnd.sample(rows, min(len(rows), n_bad(len(rows))))):
        r = list(r)
        kind = j % 2
        if kind == 0: r[0] = ""
        else: r[1] = ""
        out.append(r)
    return out


# ================================ PRODUCT SUPPLIERS ================================
SUPP_COLS = ["supplier_id", "supplier_name", "country", "rating", "is_active"]

def build_product_suppliers():
    files = {}
    suppliers = [
        ("SUP001", "Acme Supplies", "MY", 4.5, True),
        ("SUP002", "Zenith Trading", "SG", 4.2, True),
        ("SUP003", "Orchid Imports", "DE", 3.8, True),
        ("SUP004", "Nimbus Corp", "AE", 4.9, True),
        ("SUP005", "Karma Wholesale", "TR", 4.0, True),
        ("SUP006", "Global Source", "US", 4.7, True),
        ("SUP007", "Mega Distributor", "MY", 3.5, True),
        ("SUP008", "Prime Supply Co", "SG", 4.3, True),
    ]
    for day in (1, 2, 3):
        rows = []
        for s in suppliers:
            rows.append([s[0], s[1], s[2], f"{s[3]:.2f}", str(s[4]).lower()])
        files[day] = rows
    files[3] = [r + [f"GST-{rnd.randint(1, 99):03d}"] for r in files[3]]
    return files

def bad_product_suppliers(rows):
    out = []
    for j, r in enumerate(rnd.sample(rows, min(len(rows), n_bad(len(rows))))):
        r = list(r)
        kind = j % 2
        if kind == 0: r[0] = ""
        else: r[1] = ""
        out.append(r)
    return out


# ================================ PAYMENT REFUNDS ================================
REFUND_COLS = ["refund_id", "payment_id", "refund_amount", "refund_date", "refund_reason"]

def build_payment_refunds():
    pay_files = build_payments()
    files = {}
    n = 0
    for day in (1, 2, 3):
        rows = []
        for r in pay_files[day]:
            if r[5] == "REFUNDED":
                n += 1
                rows.append([f"REF{n:06d}", r[0], r[3],
                            (BASE_DAY + timedelta(days=day)).strftime("%Y-%m-%d"),
                            rnd.choice(["CUSTOMER_REQUEST", "FRAUD", "DUPLICATE", "CANCELLED"])])
        files[day] = rows
    files[3] = [r + [f"BANK-{rnd.randint(1, 999):03d}"] for r in files[3]]
    return files

def bad_payment_refunds(rows):
    out = []
    for j, r in enumerate(rnd.sample(rows, min(len(rows), n_bad(len(rows))))):
        r = list(r)
        kind = j % 4
        if kind == 0: r[0] = ""
        elif kind == 1: r[1] = ""
        elif kind == 2: r[2] = "-25.00"
        else: r[3] = ""
        out.append(r)
    return out


# ================================ PAYMENT METHODS ================================
METHOD_COLS = ["method_id", "method_name", "processing_fee_pct", "is_active"]

def build_payment_methods():
    files = {}
    methods = [
        ("M001", "CARD", 2.5, True),
        ("M002", "CASH", 0.0, True),
        ("M003", "WALLET", 1.5, True),
        ("M004", "BANK_TRANSFER", 0.5, True),
    ]
    for day in (1, 2, 3):
        rows = []
        for m in methods:
            rows.append([m[0], m[1], f"{m[2]:.2f}", str(m[3]).lower()])
        files[day] = rows
    files[3] = [r + [f"V{rnd.randint(1, 9):03d}"] for r in files[3]]
    return files

def bad_payment_methods(rows):
    out = []
    for j, r in enumerate(rnd.sample(rows, min(len(rows), n_bad(len(rows))))):
        r = list(r)
        kind = j % 2
        if kind == 0: r[0] = ""
        else: r[1] = ""
        out.append(r)
    return out


# ================================ main ================================
def emit(volume, prefix, header, files, bad_fn, extra_col):
    total_bad = 0
    for day in DAYS:
        good = files[day]
        bad = bad_fn(good)
        total_bad += len(bad)
        rows = good + bad
        rnd.shuffle(rows)
        hdr = header + ([extra_col] if day == 3 else [])
        stamp = (BASE_DAY + timedelta(days=day - 1)).strftime("%Y%m%d")
        write_csv(volume, f"{prefix}_{stamp}.csv", hdr, rows)
        print(f"     good={len(good)} bad={len(bad)}" + (f" extra_col={extra_col}" if day == 3 else ""))
    return total_bad


if __name__ == "__main__":
    cust_files, cinfo = build_customers()
    prod_files = build_products()
    pay_files = build_payments()
    order_files = build_orders()
    oi_files = build_order_items()
    or_files = build_order_returns()
    ca_files = build_customer_addresses()
    cp_files = build_customer_preferences()
    pc_files = build_product_categories()
    ps_files = build_product_suppliers()
    pr_files = build_payment_refunds()
    pm_files = build_payment_methods()
    print(f"\nDAYS this run: {DAYS}")
    print("orders:");        ob = emit("orders", "orders", ORDER_COLS, order_files, bad_orders, "discount_code")
    print("customers_cdc:"); cb = emit("customers_cdc", "customers_cdc", CUST_COLS, cust_files, bad_customers, "marketing_opt_in")
    print("products:");      pb = emit("products", "products", PROD_COLS, prod_files, bad_products, "supplier_id")
    print("payments:");      yb = emit("payments", "payments", PAY_COLS, pay_files, bad_payments, "gateway_ref")
    print("order_items:");   oib = emit("order_items", "order_items", ORDER_ITEM_COLS, oi_files, bad_order_items, "discount_code")
    print("order_returns:"); orb = emit("order_returns", "order_returns", ORDER_RETURN_COLS, or_files, bad_order_returns, "rma_number")
    print("customer_addresses:"); cab = emit("customer_addresses", "customer_addresses", ADDR_COLS, ca_files, bad_customer_addresses, "is_verified")
    print("customer_preferences:"); cpb = emit("customer_preferences", "customer_preferences", PREF_COLS, cp_files, bad_customer_preferences, "is_confirmed")
    print("product_categories:"); pcb = emit("product_categories", "product_categories", CAT_COLS, pc_files, bad_product_categories, "category_code")
    print("product_suppliers:"); psb = emit("product_suppliers", "product_suppliers", SUPP_COLS, ps_files, bad_product_suppliers, "tax_id")
    print("payment_refunds:"); prb = emit("payment_refunds", "payment_refunds", REFUND_COLS, pr_files, bad_payment_refunds, "bank_ref")
    print("payment_methods:"); pmb = emit("payment_methods", "payment_methods", METHOD_COLS, pm_files, bad_payment_methods, "version")
    print(f"\nEXPECTED quarantine rows after processing DAYS {DAYS}: "
          f"orders={ob} customers={cb} products={pb} payments={yb}")
    print(f"  order_items={oib} order_returns={orb} customer_addresses={cab} customer_preferences={cpb}")
    print(f"  product_categories={pcb} product_suppliers={psb} payment_refunds={prb} payment_methods={pmb}")
    print(f"Late out-of-order event for customer_id={cinfo['late_customer']} (arrives in day 3 file) "
          f"-> check its SCD2 history in gold.dim_customer")
