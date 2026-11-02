"""
shopflow_gen.py : générateur de données synthétiques ShopFlow (e-commerce + fraude au paiement).

Le même générateur alimente toutes les séances du cours Architecture Big Data :
  - S1 data warehouse : customers, products, orders, order_items, payments
  - S2 lakehouse      : + clickstream
  - S3 traitement     : facteurs d'échelle plus grands (--sf 10, 50)
  - S4/S5 streaming   : relecture du flux (à venir)

Les scénarios de fraude sont inspirés du simulateur du « Fraud Detection Handbook »
(Le Borgne et al., ULB / Worldline, chap. 3.2) :
https://fraud-detection-handbook.github.io/fraud-detection-handbook/Chapter_3_GettingStarted/SimulatedDataset.html
adaptés au e-commerce :
  - compte_compromis      : un compte client est piraté pendant 14 jours ; le fraudeur passe
                            des commandes de valeur élevée, livrées dans un autre pays.
  - test_de_cartes        : rafale de petits paiements, depuis une même IP, avec des cartes
                            différentes (vérification de cartes volées), en commande invité.
  - nouveau_compte_montant: compte créé moins de 2 jours avant une commande de grande valeur.

Dépendances : numpy, pandas, pyarrow (déjà présents sur Databricks et Colab).

Utilisation :
    python shopflow_gen.py --out ./shopflow_sf1 --sf 1
    python shopflow_gen.py --out ./shopflow_sf1 --sf 1 --tables customers products orders order_items payments

Depuis un notebook (Databricks, Colab) :
    import shopflow_gen
    shopflow_gen.generate(out="/Volumes/workspace/shopflow/raw/sf1", sf=1)

Avec la même graine et le même facteur d'échelle, les données produites sont identiques.
"""

from __future__ import annotations

import argparse
import os
import time

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

START = pd.Timestamp("2024-01-01")
END = pd.Timestamp("2026-01-01")  # exclu : 24 mois de données (2024 et 2025)
ALL_TABLES = ["customers", "products", "orders", "order_items", "payments", "clickstream"]

COUNTRIES = ["FR", "DE", "ES", "IT", "BE", "NL", "GB", "PT", "CH", "US"]
COUNTRY_W = [0.40, 0.14, 0.10, 0.09, 0.07, 0.06, 0.06, 0.03, 0.03, 0.02]
FRAUD_SHIP_COUNTRIES = ["NG", "RO", "UA", "BR", "US", "GB"]

CATEGORIES = {
    # catégorie : (poids de popularité, prix médian en euros)
    "Mode": (0.22, 35),
    "Maison": (0.15, 45),
    "Beauté": (0.12, 20),
    "Sport": (0.10, 50),
    "Livres": (0.10, 15),
    "Jouets": (0.08, 30),
    "Électronique": (0.10, 250),
    "Informatique": (0.06, 700),
    "Téléphonie": (0.05, 600),
    "Cartes cadeaux": (0.02, 50),
}
HIGH_VALUE = ["Électronique", "Informatique", "Téléphonie"]
SEGMENTS = ["standard", "premium", "occasionnel"]
DEVICES = ["mobile", "desktop", "tablette"]
PAGES = ["home", "search", "category", "product", "cart", "checkout", "confirmation"]

N_CUSTOMERS_PER_SF = 50_000
N_PRODUCTS = 2_000
N_ORDERS_PER_SF = 1_000_000


def _ips(rng: np.random.Generator, n: int) -> np.ndarray:
    a = rng.integers(1, 224, n)
    b, c, d = rng.integers(0, 256, (3, n))
    return np.char.add(
        np.char.add(np.char.add(a.astype(str), "."), np.char.add(b.astype(str), ".")),
        np.char.add(np.char.add(c.astype(str), "."), d.astype(str)),
    )


def _write(df: pd.DataFrame, out: str, table: str, part: str | None = None) -> None:
    folder = os.path.join(out, table)
    os.makedirs(folder, exist_ok=True)
    name = f"part-{part}.parquet" if part else "part-0.parquet"
    # horodatages en UTC : lus comme TIMESTAMP par BigQuery et par Spark (et non DATETIME / TIMESTAMP_NTZ)
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]) and df[col].dt.tz is None:
            df = df.assign(**{col: df[col].dt.tz_localize("UTC")})
    pq.write_table(pa.Table.from_pandas(df, preserve_index=False), os.path.join(folder, name),
                   compression="snappy")


def _month_weights() -> tuple[list[pd.Timestamp], np.ndarray]:
    months = list(pd.date_range(START, END, freq="MS", inclusive="left"))
    season = {1: 0.95, 2: 0.80, 3: 0.90, 4: 0.90, 5: 0.92, 6: 0.95,
              7: 0.85, 8: 0.75, 9: 0.92, 10: 1.00, 11: 1.45, 12: 1.60}
    growth = np.linspace(0.85, 1.15, len(months))  # l'activité croît de 35 % sur 2 ans
    w = np.array([season[m.month] for m in months]) * growth
    return months, w / w.sum()


def _random_ts(rng: np.random.Generator, month: pd.Timestamp, n: int) -> np.ndarray:
    """Horodatages dans le mois, plus denses le soir et le week-end."""
    next_month = month + pd.offsets.MonthBegin(1)
    n_days = (next_month - month).days
    day = rng.integers(0, n_days, n)
    weekday = (month + pd.to_timedelta(day, "D")).weekday.to_numpy()
    # les week-ends reçoivent 30 % de commandes en plus : on retire une partie des jours de semaine
    redraw = (weekday < 5) & (rng.random(n) < 0.23)
    day[redraw] = rng.integers(0, n_days, redraw.sum())
    hour = rng.choice(24, n, p=_HOUR_W)
    sec = rng.integers(0, 3600, n)
    return (np.datetime64(month.to_datetime64(), "s")
            + (day * 86400 + hour * 3600 + sec).astype("timedelta64[s]"))


_HOUR_W = np.array([1, 0.6, 0.4, 0.3, 0.3, 0.4, 0.8, 1.5, 2.5, 3, 3.2, 3.4,
                    3.8, 3.6, 3.2, 3.2, 3.4, 3.8, 4.5, 5.2, 5.5, 4.8, 3.5, 2.0])
_HOUR_W = _HOUR_W / _HOUR_W.sum()


def generate(out: str, sf: float = 1, seed: int = 42, tables: list[str] | None = None,
             verbose: bool = True) -> dict:
    """Génère les tables ShopFlow en Parquet dans `out`. Retourne le nombre de lignes par table."""
    tables = tables or ALL_TABLES
    unknown = set(tables) - set(ALL_TABLES)
    if unknown:
        raise ValueError(f"Tables inconnues : {unknown}")
    t0 = time.time()
    rng = np.random.default_rng(seed)
    counts = {t: 0 for t in tables}

    # ------------------------------------------------------------------ produits
    cat_names = np.array(list(CATEGORIES))
    cat_w = np.array([v[0] for v in CATEGORIES.values()])
    cat_w = cat_w / cat_w.sum()
    p_cat = rng.choice(cat_names, N_PRODUCTS, p=cat_w)
    p_median = np.array([CATEGORIES[c][1] for c in p_cat])
    p_price = np.round(p_median * rng.lognormal(0, 0.5, N_PRODUCTS), 2)
    gift = p_cat == "Cartes cadeaux"
    p_price[gift] = rng.choice([5, 10, 25, 50, 100], gift.sum())
    p_price = np.maximum(p_price, 1.0)
    products = pd.DataFrame({
        "product_id": np.arange(1, N_PRODUCTS + 1, dtype=np.int64),
        "category": p_cat,
        "brand": np.char.add("Marque-", rng.integers(1, 301, N_PRODUCTS).astype(str)),
        "price": p_price,
    })
    # popularité de type Zipf : quelques produits concentrent l'essentiel des ventes
    pop = 1 / np.arange(1, N_PRODUCTS + 1) ** 0.9
    pop = rng.permutation(pop)
    pop = pop / pop.sum()
    high_ids = products.product_id[products.category.isin(HIGH_VALUE)].to_numpy()
    gift5_id = int(products.product_id[(products.category == "Cartes cadeaux")
                                       & (products.price == 5)].iloc[0])
    if "products" in tables:
        _write(products, out, "products")
        counts["products"] = len(products)

    # ------------------------------------------------------------------ clients
    n_cust = int(N_CUSTOMERS_PER_SF * sf)
    # dates d'inscription croissantes : customer_id suit l'ordre d'inscription
    signup_offsets = np.sort(rng.beta(1.6, 1.0, n_cust)) * (END - pd.Timestamp("2022-01-01")).days
    signup = (np.datetime64("2022-01-01") + signup_offsets.astype(int).astype("timedelta64[D]"))
    c_country = rng.choice(COUNTRIES, n_cust, p=COUNTRY_W)
    customers = pd.DataFrame({
        "customer_id": np.arange(1, n_cust + 1, dtype=np.int64),
        "email_hash": [f"{x:016x}" for x in rng.integers(0, 2**63, n_cust)],
        "country": c_country,
        "signup_date": signup.astype("datetime64[D]"),
        "segment": rng.choice(SEGMENTS, n_cust, p=[0.7, 0.1, 0.2]),
    })
    # client invité (commandes sans compte) : membre « inconnu » au sens de Kimball
    guest = pd.DataFrame({"customer_id": [0], "email_hash": [None], "country": [None],
                          "signup_date": [pd.NaT], "segment": ["invité"]})
    customers_out = pd.concat([guest, customers], ignore_index=True)
    customers_out["signup_date"] = pd.to_datetime(customers_out["signup_date"]).dt.date
    if "customers" in tables:
        _write(customers_out, out, "customers")
        counts["customers"] = len(customers_out)

    # cartes : 1 ou 2 par client ; card_id = customer_id * 10 + rang
    n_cards = np.where(rng.random(n_cust) < 0.75, 1, 2)
    home_ip = _ips(rng, n_cust)
    home_device = rng.choice(DEVICES, n_cust, p=[0.6, 0.32, 0.08])
    signup_ns = signup.astype("datetime64[s]")

    months, mw = _month_weights()
    n_orders_total = int(N_ORDERS_PER_SF * sf)
    order_seq, pay_seq, sess_seq, event_seq = 1, 1, 1, 1

    for month, w in zip(months, mw):
        m_rng = np.random.default_rng([seed, month.year, month.month])
        tag = month.strftime("%Y-%m")
        n = int(round(n_orders_total * w))
        month_end = np.datetime64((month + pd.offsets.MonthBegin(1)).to_datetime64(), "s")

        # ---------------------------------------------- commandes légitimes
        ts = _random_ts(m_rng, month, n)
        k = int(np.searchsorted(signup_ns, np.datetime64(month.to_datetime64(), "s"), "right"))
        k = max(k, 1000)
        cust_idx = np.minimum((k * m_rng.random(n) ** 1.3).astype(np.int64), k - 1)
        o = pd.DataFrame({
            "customer_id": cust_idx + 1,
            "order_ts": ts,
            "shipping_country": c_country[cust_idx],
            "device": home_device[cust_idx],
            "ip": home_ip[cust_idx],
            "fraud_scenario": None,
        })
        travel = m_rng.random(n) < 0.15  # 15 % des commandes depuis une autre IP (mobile, travail)
        o.loc[travel, "ip"] = _ips(m_rng, travel.sum())
        o["card_rank"] = np.where(n_cards[cust_idx] == 2, m_rng.integers(1, 3, n), 1)

        # ---------------------------------------------- fraude : compte compromis
        n_ato = max(1, int(round(90 * sf * w * 24)))  # ~90 comptes par mois à SF1
        victims = m_rng.integers(0, k, n_ato)
        start = _random_ts(m_rng, month, n_ato)
        per_victim = m_rng.poisson(2.5, n_ato) + 1
        v_idx = np.repeat(victims, per_victim)
        v_ts = np.repeat(start, per_victim) + (m_rng.random(v_idx.size) * 14 * 86400).astype("timedelta64[s]")
        keep = v_ts < month_end
        v_idx, v_ts = v_idx[keep], v_ts[keep]
        fraud_ip = _ips(m_rng, n_ato)
        ato = pd.DataFrame({
            "customer_id": v_idx + 1, "order_ts": v_ts,
            "shipping_country": m_rng.choice(FRAUD_SHIP_COUNTRIES, v_idx.size),
            "device": "desktop",
            "ip": np.repeat(fraud_ip, per_victim)[keep],
            "fraud_scenario": "compte_compromis",
            "card_rank": 1,
        })

        # ---------------------------------------------- fraude : nouveau compte, montant élevé
        new_accounts = np.where((signup_ns >= np.datetime64(month.to_datetime64(), "s"))
                                & (signup_ns < month_end - np.timedelta64(2, "D")))[0]
        n_new = min(len(new_accounts), max(1, int(round(40 * sf * w * 24))))
        na = m_rng.choice(new_accounts, n_new, replace=False) if n_new else np.array([], int)
        na_ts = signup_ns[na] + (m_rng.random(n_new) * 2 * 86400).astype("timedelta64[s]")
        newacc = pd.DataFrame({
            "customer_id": na + 1, "order_ts": na_ts,
            "shipping_country": m_rng.choice(FRAUD_SHIP_COUNTRIES, n_new),
            "device": "desktop", "ip": _ips(m_rng, n_new),
            "fraud_scenario": "nouveau_compte_montant", "card_rank": 1,
        })

        # ---------------------------------------------- fraude : test de cartes (commande invité)
        n_bursts = max(1, int(round(8 * sf * w * 24)))
        burst_size = m_rng.integers(15, 41, n_bursts)
        b_start = _random_ts(m_rng, month, n_bursts)
        b_ip = _ips(m_rng, n_bursts)
        b_n = burst_size.sum()
        tests = pd.DataFrame({
            "customer_id": np.zeros(b_n, dtype=np.int64),
            "order_ts": np.repeat(b_start, burst_size) + (m_rng.random(b_n) * 1800).astype("timedelta64[s]"),
            "shipping_country": None, "device": "desktop",
            "ip": np.repeat(b_ip, burst_size),
            "fraud_scenario": "test_de_cartes", "card_rank": 0,
        })
        # cartes volées : cartes de clients existants, utilisées hors de leur compte
        stolen_cust = m_rng.integers(0, k, b_n)

        orders = pd.concat([o, ato, newacc, tests], ignore_index=True)
        orders = orders.sort_values("order_ts", kind="stable").reset_index(drop=True)
        n_all = len(orders)
        orders.insert(0, "order_id", np.arange(order_seq, order_seq + n_all, dtype=np.int64))
        order_seq += n_all

        # ---------------------------------------------- lignes de commande
        scen = orders["fraud_scenario"].to_numpy()
        n_items = np.where(pd.isna(scen), 1 + m_rng.poisson(0.8, n_all), 0)
        n_items[scen == "compte_compromis"] = m_rng.integers(1, 4, (scen == "compte_compromis").sum())
        n_items[scen == "nouveau_compte_montant"] = m_rng.integers(2, 5, (scen == "nouveau_compte_montant").sum())
        n_items[scen == "test_de_cartes"] = 1
        it_order = np.repeat(orders["order_id"].to_numpy(), n_items)
        it_scen = np.repeat(scen, n_items)
        line_no = np.arange(it_order.size) - np.repeat(np.cumsum(n_items) - n_items, n_items) + 1
        prod = m_rng.choice(N_PRODUCTS, it_order.size, p=pop) + 1
        hv = (it_scen == "compte_compromis") | (it_scen == "nouveau_compte_montant")
        prod[hv] = m_rng.choice(high_ids, hv.sum())
        prod[it_scen == "test_de_cartes"] = gift5_id
        qty = np.where(m_rng.random(it_order.size) < 0.85, 1, m_rng.integers(2, 4, it_order.size))
        qty[hv] = m_rng.integers(1, 4, hv.sum())
        qty[it_scen == "test_de_cartes"] = 1
        unit_price = products.price.to_numpy()[prod - 1]
        discount = np.where(m_rng.random(it_order.size) < 0.2, 0.9, 1.0)
        unit_price = np.round(unit_price * discount, 2)
        items = pd.DataFrame({"order_id": it_order, "line_no": line_no.astype(np.int64),
                              "product_id": prod.astype(np.int64), "quantity": qty.astype(np.int64),
                              "unit_price": unit_price})
        totals = (items.quantity * items.unit_price).groupby(items.order_id).sum().round(2)
        orders["total_amount"] = orders["order_id"].map(totals).to_numpy()

        # ---------------------------------------------- paiements
        is_fraud = orders["fraud_scenario"].notna().to_numpy()
        cust = orders["customer_id"].to_numpy()
        card_id = cust * 10 + orders["card_rank"].to_numpy()
        tmask = scen == "test_de_cartes"
        card_id[tmask] = (stolen_cust + 1) * 10 + 1
        method = m_rng.choice(["card", "paypal", "apple_pay"], n_all, p=[0.8, 0.12, 0.08])
        method[is_fraud] = "card"
        card_country = np.where(cust > 0, c_country[np.maximum(cust - 1, 0)], "")
        card_country[tmask] = c_country[stolen_cust]
        card_id_out = np.where(method == "paypal", -1, card_id)
        status = np.full(n_all, "authorized", dtype=object)
        status[tmask] = np.where(m_rng.random(tmask.sum()) < 0.8, "declined", "authorized")
        pay = pd.DataFrame({
            "order_id": orders["order_id"].to_numpy(),
            "customer_id": cust,
            "card_id": card_id_out,
            "payment_ts": orders["order_ts"].to_numpy() + m_rng.integers(5, 120, n_all).astype("timedelta64[s]"),
            "amount": orders["total_amount"].to_numpy(),
            "currency": "EUR",
            "payment_method": method,
            "card_country": card_country,
            "ip": orders["ip"].to_numpy(),
            "status": status,
            "is_fraud": is_fraud,
            "fraud_scenario": scen,
        })
        # 2 % des paiements légitimes sont d'abord refusés (fonds insuffisants) puis réessayés
        retry = (~is_fraud) & (m_rng.random(n_all) < 0.02)
        declined = pay[retry].copy()
        declined["payment_ts"] = declined["payment_ts"] - m_rng.integers(60, 600, retry.sum()).astype("timedelta64[s]")
        declined["status"] = "declined"
        pay = pd.concat([pay, declined], ignore_index=True).sort_values("payment_ts", kind="stable")
        pay.insert(0, "payment_id", np.arange(pay_seq, pay_seq + len(pay), dtype=np.int64))
        pay_seq += len(pay)
        # rétrofacturation : 90 % des fraudes autorisées sont signalées 7 à 60 jours plus tard
        cb = pay["is_fraud"].to_numpy() & (pay["status"].to_numpy() == "authorized") & (m_rng.random(len(pay)) < 0.9)
        cb_ts = pay["payment_ts"].to_numpy() + (m_rng.uniform(7, 60, len(pay)) * 86400).astype("timedelta64[s]")
        pay["chargeback_ts"] = np.where(cb, cb_ts, np.datetime64("NaT"))
        pay["card_id"] = pay["card_id"].astype("Int64").where(pay["card_id"] >= 0)
        pay["customer_id"] = pay["customer_id"].astype(np.int64)
        pay["card_country"] = pay["card_country"].replace("", None)

        # ---------------------------------------------- statut des commandes
        pay_auth = pay[pay.status == "authorized"].order_id.unique()
        orders["status"] = np.where(orders.order_id.isin(pay_auth), "paid", "cancelled")
        delivered = (orders.status == "paid") & (orders.order_ts < np.datetime64("2025-12-20"))
        orders.loc[delivered, "status"] = "delivered"
        returned = delivered & (m_rng.random(n_all) < 0.04)
        orders.loc[returned, "status"] = "returned"

        orders_out = orders[["order_id", "customer_id", "order_ts", "status", "shipping_country",
                             "total_amount", "device", "ip"]]
        pay_out = pay[["payment_id", "order_id", "customer_id", "card_id", "payment_ts", "amount",
                       "currency", "payment_method", "card_country", "ip", "status", "is_fraud",
                       "fraud_scenario", "chargeback_ts"]]
        for col in ["order_ts"]:
            orders_out = orders_out.assign(**{col: orders_out[col].astype("datetime64[us]")})
        pay_out = pay_out.assign(payment_ts=pay_out.payment_ts.astype("datetime64[us]"),
                                 chargeback_ts=pay_out.chargeback_ts.astype("datetime64[us]"))

        if "orders" in tables:
            _write(orders_out, out, "orders", tag)
            counts["orders"] += len(orders_out)
        if "order_items" in tables:
            _write(items, out, "order_items", tag)
            counts["order_items"] += len(items)
        if "payments" in tables:
            _write(pay_out, out, "payments", tag)
            counts["payments"] += len(pay_out)

        # ---------------------------------------------- clickstream
        if "clickstream" in tables:
            ev = _clickstream(m_rng, orders, items, month, pop, sess_seq, event_seq, k,
                              home_device, home_ip)
            sess_seq = int(ev.session_id.max()) + 1
            event_seq = int(ev.event_id.max()) + 1
            _write(ev, out, "clickstream", tag)
            counts["clickstream"] += len(ev)

        if verbose:
            print(f"{tag} : {n_all:>9,} commandes ({time.time() - t0:5.1f} s)")

    if verbose:
        print("\nLignes générées :")
        for t, c in counts.items():
            print(f"  {t:<12} {c:>12,}")
    return counts


def _clickstream(rng, orders, items, month, pop, sess_seq, event_seq, k, home_device, home_ip):
    """Une session par commande (vues, panier, paiement) + 2 sessions sans achat par commande."""
    n = len(orders)
    # sessions avec achat
    n_views = 1 + rng.poisson(4, n)
    n_cart = items.groupby("order_id").size().reindex(orders.order_id).fillna(1).to_numpy().astype(int)
    per = n_views + n_cart + 2  # + checkout + confirmation
    sess = np.arange(sess_seq, sess_seq + n)
    s_rep = np.repeat(sess, per)
    pos = np.arange(s_rep.size) - np.repeat(np.cumsum(per) - per, per)
    nv_rep = np.repeat(n_views, per)
    nc_rep = np.repeat(n_cart, per)
    etype = np.where(pos < nv_rep, "view",
                     np.where(pos < nv_rep + nc_rep, "add_to_cart",
                              np.where(pos == nv_rep + nc_rep, "checkout", "purchase")))
    page = np.where(etype == "view", rng.choice(["home", "search", "category", "product"],
                                                s_rep.size, p=[0.15, 0.2, 0.2, 0.45]),
                    np.where(etype == "add_to_cart", "product",
                             np.where(etype == "checkout", "checkout", "confirmation")))
    end_ts = np.repeat(orders.order_ts.to_numpy(), per)
    back = (np.repeat(per, per) - pos) * rng.integers(10, 120, s_rep.size)
    ts = end_ts - back.astype("timedelta64[s]")
    product = np.where(np.isin(etype, ["view", "add_to_cart"]),
                       rng.choice(len(pop), s_rep.size, p=pop) + 1, 0)
    cust = np.repeat(orders.customer_id.to_numpy(), per)
    buy = pd.DataFrame({
        "session_id": s_rep, "customer_id": cust, "event_ts": ts, "event_type": etype,
        "page": page, "product_id": product,
        "device": np.repeat(orders.device.to_numpy(), per),
        "ip": np.repeat(orders.ip.to_numpy(), per),
    })
    # sessions sans achat (navigation), 40 % anonymes
    m = 2 * n
    nv = 1 + rng.poisson(2.5, m)
    sess2 = np.arange(sess_seq + n, sess_seq + n + m)
    s2 = np.repeat(sess2, nv)
    pos2 = np.arange(s2.size) - np.repeat(np.cumsum(nv) - nv, nv)
    start = np.repeat(_random_ts(rng, month, m), nv)
    c_idx = rng.integers(0, k, m)
    anon = rng.random(m) < 0.4
    cust2 = np.where(anon, 0, c_idx + 1)
    browse = pd.DataFrame({
        "session_id": s2, "customer_id": np.repeat(cust2, nv),
        "event_ts": start + (pos2 * rng.integers(10, 120, s2.size)).astype("timedelta64[s]"),
        "event_type": np.where(rng.random(s2.size) < 0.08, "add_to_cart", "view"),
        "page": rng.choice(["home", "search", "category", "product"], s2.size, p=[0.2, 0.25, 0.2, 0.35]),
        "product_id": rng.choice(len(pop), s2.size, p=pop) + 1,
        "device": np.repeat(np.where(anon, rng.choice(DEVICES, m), home_device[c_idx]), nv),
        "ip": np.repeat(np.where(anon, _ips(rng, m), home_ip[c_idx]), nv),
    })
    ev = pd.concat([buy, browse], ignore_index=True).sort_values("event_ts", kind="stable")
    ev["product_id"] = ev["product_id"].astype("Int64").where(ev["product_id"] > 0)
    ev["customer_id"] = ev["customer_id"].astype("Int64").where(ev["customer_id"] > 0)
    ev.insert(0, "event_id", np.arange(event_seq, event_seq + len(ev), dtype=np.int64))
    ev["event_ts"] = ev["event_ts"].astype("datetime64[us]")
    ev["session_id"] = ev["session_id"].astype(np.int64)
    return ev[["event_id", "session_id", "customer_id", "event_ts", "event_type", "page",
               "product_id", "device", "ip"]]


def main() -> None:
    ap = argparse.ArgumentParser(description="Générateur de données ShopFlow")
    ap.add_argument("--out", required=True, help="dossier de sortie")
    ap.add_argument("--sf", type=float, default=1, help="facteur d'échelle (1 = 1 M de commandes)")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--tables", nargs="*", default=None, help=f"parmi {ALL_TABLES}")
    a = ap.parse_args()
    generate(a.out, a.sf, a.seed, a.tables)


if __name__ == "__main__":
    main()
