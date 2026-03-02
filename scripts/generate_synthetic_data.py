"""
Generate synthetic data for GreenTrail Outdoor Co. portfolio project.

12 stores, 18 months POS data, ~78K daily_store_sales, ~120K transactions,
500 SKUs across 6 categories, 15K customers.

ENCODED INSIGHTS:
- 3 declining stores (Milltown, Creekside, Hilltop): population shrinking 4-7%,
  revenue decline proportional across all categories
- 1 declining store (Riverside): stable population, but Kathmandu opened 800m away
  Oct 2023. Lost ~31% camping + hiking revenue. Fishing/hunting stable.
- 8 growing stores: growing populations, no nearby competitors, revenue +5-12%
"""

import numpy as np
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timedelta
import random

np.random.seed(42)
random.seed(42)

# ─── Date range: 18 months ending Feb 2025 ───
START_DATE = datetime(2023, 9, 1)
END_DATE = datetime(2025, 2, 28)
date_range = pd.date_range(START_DATE, END_DATE, freq='D')
NUM_DAYS = len(date_range)  # ~547 days

# ─── Store definitions ───
stores = [
    # Declining: population shrinkage
    {"store_id": 1,  "store_name": "Milltown",    "town_name": "Milltown",    "population": 8200,  "population_change_pct": -5.2, "store_size_sqm": 320, "nearest_competitor_km": 45.0, "opened_date": "2019-03-15", "trend": "declining_pop", "growth_rate": -0.052},
    {"store_id": 2,  "store_name": "Creekside",   "town_name": "Creekside",   "population": 5400,  "population_change_pct": -6.8, "store_size_sqm": 280, "nearest_competitor_km": 52.0, "opened_date": "2020-01-10", "trend": "declining_pop", "growth_rate": -0.068},
    {"store_id": 3,  "store_name": "Hilltop",     "town_name": "Hilltop",     "population": 6100,  "population_change_pct": -4.1, "store_size_sqm": 300, "nearest_competitor_km": 38.0, "opened_date": "2018-07-22", "trend": "declining_pop", "growth_rate": -0.041},
    # Declining: competitive displacement
    {"store_id": 4,  "store_name": "Riverside",    "town_name": "Riverside",   "population": 12500, "population_change_pct": 0.3,  "store_size_sqm": 410, "nearest_competitor_km": 0.8,  "opened_date": "2017-11-05", "trend": "competitor",    "growth_rate": 0.0},
    # Growing stores
    {"store_id": 5,  "store_name": "Lakewood",     "town_name": "Lakewood",    "population": 15800, "population_change_pct": 3.2,  "store_size_sqm": 450, "nearest_competitor_km": 62.0, "opened_date": "2016-05-18", "trend": "growing", "growth_rate": 0.072},
    {"store_id": 6,  "store_name": "Pine Ridge",   "town_name": "Pine Ridge",  "population": 22400, "population_change_pct": 4.5,  "store_size_sqm": 520, "nearest_competitor_km": 78.0, "opened_date": "2015-09-01", "trend": "growing", "growth_rate": 0.095},
    {"store_id": 7,  "store_name": "Cedar Falls",  "town_name": "Cedar Falls", "population": 18200, "population_change_pct": 2.8,  "store_size_sqm": 380, "nearest_competitor_km": 55.0, "opened_date": "2019-02-14", "trend": "growing", "growth_rate": 0.058},
    {"store_id": 8,  "store_name": "Oakdale",      "town_name": "Oakdale",     "population": 11300, "population_change_pct": 1.9,  "store_size_sqm": 340, "nearest_competitor_km": 41.0, "opened_date": "2020-06-20", "trend": "growing", "growth_rate": 0.050},
    {"store_id": 9,  "store_name": "Summit View",  "town_name": "Summit View", "population": 28500, "population_change_pct": 5.1,  "store_size_sqm": 580, "nearest_competitor_km": 85.0, "opened_date": "2014-04-10", "trend": "growing", "growth_rate": 0.112},
    {"store_id": 10, "store_name": "Willow Creek", "town_name": "Willow Creek","population": 9800,  "population_change_pct": 2.1,  "store_size_sqm": 310, "nearest_competitor_km": 48.0, "opened_date": "2021-01-08", "trend": "growing", "growth_rate": 0.055},
    {"store_id": 11, "store_name": "Birchwood",    "town_name": "Birchwood",   "population": 14600, "population_change_pct": 3.8,  "store_size_sqm": 420, "nearest_competitor_km": 67.0, "opened_date": "2017-08-30", "trend": "growing", "growth_rate": 0.085},
    {"store_id": 12, "store_name": "Eagle Point",  "town_name": "Eagle Point", "population": 19700, "population_change_pct": 4.2,  "store_size_sqm": 490, "nearest_competitor_km": 72.0, "opened_date": "2016-12-01", "trend": "growing", "growth_rate": 0.098},
]

stores_df = pd.DataFrame(stores)

# ─── Product catalog: 500 SKUs across 6 categories ───
CATEGORIES = ["camping", "hiking", "fishing", "hunting", "cycling", "general_outdoor"]
CATEGORY_BASE_SHARE = {
    "camping":         0.22,
    "hiking":          0.20,
    "fishing":         0.15,
    "hunting":         0.13,
    "cycling":         0.16,
    "general_outdoor": 0.14,
}
SKUS_PER_CATEGORY = {
    "camping": 95, "hiking": 90, "fishing": 80,
    "hunting": 75, "cycling": 85, "general_outdoor": 75,
}

products = []
sku_id = 1
for cat, count in SKUS_PER_CATEGORY.items():
    for i in range(count):
        price = round(np.random.lognormal(mean=3.5, sigma=0.8), 2)
        price = max(5.0, min(price, 500.0))
        products.append({
            "product_id": sku_id,
            "sku": f"GT-{cat[:3].upper()}-{sku_id:04d}",
            "product_name": f"{cat.replace('_', ' ').title()} Item {i+1}",
            "category": cat,
            "unit_price": price,
        })
        sku_id += 1

products_df = pd.DataFrame(products)

# ─── Customers: 15,000 ───
AGE_GROUPS = ["18-24", "25-34", "35-44", "45-54", "55-64", "65+"]
GENDERS = ["M", "F", "Other"]

customers = []
for cid in range(1, 15001):
    age_group = np.random.choice(AGE_GROUPS, p=[0.10, 0.25, 0.25, 0.20, 0.12, 0.08])
    gender = np.random.choice(GENDERS, p=[0.48, 0.48, 0.04])
    postcode = f"{np.random.randint(2000, 4999)}"
    primary_store = np.random.choice([s["store_id"] for s in stores],
                                     p=[0.06, 0.05, 0.05, 0.09,
                                        0.10, 0.12, 0.10, 0.08,
                                        0.12, 0.06, 0.09, 0.08])
    customers.append({
        "customer_id": cid,
        "age_group": age_group,
        "gender": gender,
        "postcode": postcode,
        "primary_store_id": primary_store,
    })

customers_df = pd.DataFrame(customers)

# ─── Transaction generation ───
# We need ~120K transactions across 18 months for 12 stores
# That's ~220 transactions/store/day on average for large stores, fewer for small

COMPETITOR_OPEN_DATE = datetime(2023, 10, 1)

def get_daily_base_revenue(store, date):
    """Calculate base daily revenue for a store on a given date."""
    # Base revenue scaled by store size
    # Target: $8M annual = $22K/day across 12 stores = ~$1,833/store/day avg
    base = store["store_size_sqm"] * 2.7  # ~$756-$1,566/day base → ~$8M annual

    # Seasonality: outdoor retail peaks in spring/summer
    month = date.month
    seasonal = {1: 0.75, 2: 0.70, 3: 0.85, 4: 1.05, 5: 1.20, 6: 1.30,
                7: 1.25, 8: 1.15, 9: 1.00, 10: 0.90, 11: 0.80, 12: 0.95}
    base *= seasonal[month]

    # Day of week effect
    dow = date.weekday()
    if dow == 5:  # Saturday
        base *= 1.35
    elif dow == 6:  # Sunday
        base *= 1.20
    elif dow == 0:  # Monday
        base *= 0.85

    return base


def get_store_trend_multiplier(store, date):
    """Apply growth/decline trend over time."""
    days_from_start = (date - START_DATE).days
    total_days = (END_DATE - START_DATE).days
    progress = days_from_start / total_days  # 0 to 1

    trend = store["trend"]
    annual_rate = store["growth_rate"]

    if trend == "declining_pop":
        # Gradual decline proportional to population change
        # Apply the annual rate spread over 18 months
        multiplier = 1.0 + (annual_rate * 1.5 * progress)
        # Add slight acceleration in last 6 months
        if progress > 0.67:
            multiplier -= 0.01 * (progress - 0.67) / 0.33
        return max(0.85, multiplier)

    elif trend == "competitor":
        # Riverside: category-specific decline handled by get_category_multiplier
        # Overall store multiplier stays at 1.0 — no double-counting
        return 1.0

    elif trend == "growing":
        multiplier = 1.0 + (annual_rate * 1.5 * progress)
        return min(1.20, multiplier)

    return 1.0


def get_category_multiplier(store, category, date):
    """Category-specific adjustment, especially for Riverside."""
    if store["trend"] != "competitor":
        return 1.0

    if date < COMPETITOR_OPEN_DATE:
        return 1.0

    days_since = (date - COMPETITOR_OPEN_DATE).days
    impact_progress = min(1.0, days_since / 90)

    # Camping and hiking: lose 31% and 28% respectively (competitor overlap)
    if category == "camping":
        return 1.0 - (0.31 * impact_progress)
    elif category == "hiking":
        return 1.0 - (0.28 * impact_progress)
    elif category == "fishing":
        return 1.0 + (0.02 * impact_progress)  # slight gain — no competitor overlap
    elif category == "hunting":
        return 1.0 + (0.03 * impact_progress)  # slight gain — no competitor overlap
    else:
        return 1.0  # cycling, general_outdoor: unaffected


print("Generating transactions...")

all_transactions = []
daily_store_sales = []
txn_id = 1

for date in date_range:
    dt = date.to_pydatetime()

    for store in stores:
        base_revenue = get_daily_base_revenue(store, dt)
        trend_mult = get_store_trend_multiplier(store, dt)

        # Category-level generation
        day_revenue = 0
        day_txn_count = 0
        day_customers = set()
        cat_revenues = {}

        for cat in CATEGORIES:
            cat_share = CATEGORY_BASE_SHARE[cat]
            cat_mult = get_category_multiplier(store, cat, dt)
            cat_revenue_target = base_revenue * cat_share * trend_mult * cat_mult

            # Add daily noise (±15%)
            cat_revenue_target *= (1 + np.random.normal(0, 0.15))
            cat_revenue_target = max(0, cat_revenue_target)

            # Number of transactions for this category today
            cat_products = products_df[products_df["category"] == cat]
            avg_price = cat_products["unit_price"].mean()
            avg_basket_items = np.random.uniform(1.2, 2.2)
            avg_basket_value = avg_price * avg_basket_items

            n_txns = max(0, int(cat_revenue_target / avg_basket_value))

            for _ in range(n_txns):
                # Pick a customer (70% from primary store customers, 30% random)
                store_customers = customers_df[
                    customers_df["primary_store_id"] == store["store_id"]
                ]["customer_id"].values

                if len(store_customers) > 0 and np.random.random() < 0.7:
                    cust_id = int(np.random.choice(store_customers))
                else:
                    cust_id = int(np.random.choice(customers_df["customer_id"].values))

                # Pick products
                n_items = max(1, int(np.random.exponential(1.8) + 1))
                n_items = min(n_items, 8)
                selected_products = cat_products.sample(n=min(n_items, len(cat_products)))

                for _, prod in selected_products.iterrows():
                    qty = max(1, int(np.random.exponential(0.8) + 1))
                    qty = min(qty, 5)
                    line_total = round(prod["unit_price"] * qty, 2)

                    all_transactions.append({
                        "transaction_id": txn_id,
                        "store_id": store["store_id"],
                        "customer_id": cust_id,
                        "product_id": int(prod["product_id"]),
                        "transaction_date": dt.strftime("%Y-%m-%d"),
                        "quantity": qty,
                        "unit_price": float(prod["unit_price"]),
                        "line_total": line_total,
                    })

                    day_revenue += line_total
                    day_customers.add(cust_id)

                day_txn_count += 1
                txn_id += 1

            cat_revenues[cat] = cat_revenue_target

        daily_store_sales.append({
            "store_id": store["store_id"],
            "sale_date": dt.strftime("%Y-%m-%d"),
            "revenue": round(day_revenue, 2),
            "transaction_count": day_txn_count,
            "unique_customers": len(day_customers),
        })

    if dt.day == 1:
        print(f"  Generated through {dt.strftime('%Y-%m')}, txns so far: {len(all_transactions):,}")

print(f"\nTotal transactions: {len(all_transactions):,}")
print(f"Total daily_store_sales rows: {len(daily_store_sales):,}")

transactions_df = pd.DataFrame(all_transactions)
daily_sales_df = pd.DataFrame(daily_store_sales)

# ─── Summary stats ───
print(f"\nStores: {len(stores_df)}")
print(f"Products: {len(products_df)}")
print(f"Customers: {len(customers_df)}")
print(f"Transactions (line items): {len(transactions_df):,}")
print(f"Daily store sales: {len(daily_sales_df):,}")
total_rev = transactions_df["line_total"].sum()
print(f"Total revenue: ${total_rev:,.0f}")

# ─── Load to PostgreSQL ───
print("\nLoading to PostgreSQL schema: greentrail...")

conn = psycopg2.connect(
    host="localhost", port=5432,
    user="portfolio", password="portfolio_dev",
    dbname="portfolio"
)
conn.autocommit = True
cur = conn.cursor()

# Drop existing tables
for table in ["transactions", "daily_store_sales", "customers", "products", "stores"]:
    cur.execute(f"DROP TABLE IF EXISTS greentrail.{table} CASCADE;")

# Create tables
cur.execute("""
CREATE TABLE greentrail.stores (
    store_id INTEGER PRIMARY KEY,
    store_name VARCHAR(100),
    town_name VARCHAR(100),
    population INTEGER,
    population_change_pct NUMERIC(5,2),
    store_size_sqm INTEGER,
    nearest_competitor_km NUMERIC(6,2),
    opened_date DATE
);
""")

cur.execute("""
CREATE TABLE greentrail.products (
    product_id INTEGER PRIMARY KEY,
    sku VARCHAR(20),
    product_name VARCHAR(200),
    category VARCHAR(50),
    unit_price NUMERIC(10,2)
);
""")

cur.execute("""
CREATE TABLE greentrail.customers (
    customer_id INTEGER PRIMARY KEY,
    age_group VARCHAR(10),
    gender VARCHAR(10),
    postcode VARCHAR(10),
    primary_store_id INTEGER REFERENCES greentrail.stores(store_id)
);
""")

cur.execute("""
CREATE TABLE greentrail.transactions (
    transaction_id INTEGER,
    store_id INTEGER REFERENCES greentrail.stores(store_id),
    customer_id INTEGER REFERENCES greentrail.customers(customer_id),
    product_id INTEGER REFERENCES greentrail.products(product_id),
    transaction_date DATE,
    quantity INTEGER,
    unit_price NUMERIC(10,2),
    line_total NUMERIC(10,2)
);
""")

cur.execute("""
CREATE TABLE greentrail.daily_store_sales (
    store_id INTEGER REFERENCES greentrail.stores(store_id),
    sale_date DATE,
    revenue NUMERIC(12,2),
    transaction_count INTEGER,
    unique_customers INTEGER
);
""")

# Insert data
print("  Loading stores...")
execute_values(cur,
    "INSERT INTO greentrail.stores (store_id, store_name, town_name, population, population_change_pct, store_size_sqm, nearest_competitor_km, opened_date) VALUES %s",
    stores_df[["store_id", "store_name", "town_name", "population", "population_change_pct", "store_size_sqm", "nearest_competitor_km", "opened_date"]].values.tolist()
)

print("  Loading products...")
execute_values(cur,
    "INSERT INTO greentrail.products (product_id, sku, product_name, category, unit_price) VALUES %s",
    products_df.values.tolist()
)

print("  Loading customers...")
execute_values(cur,
    "INSERT INTO greentrail.customers (customer_id, age_group, gender, postcode, primary_store_id) VALUES %s",
    customers_df.values.tolist()
)

print("  Loading transactions (this may take a moment)...")
# Batch insert for large dataset
batch_size = 10000
txn_data = transactions_df.values.tolist()
for i in range(0, len(txn_data), batch_size):
    execute_values(cur,
        "INSERT INTO greentrail.transactions (transaction_id, store_id, customer_id, product_id, transaction_date, quantity, unit_price, line_total) VALUES %s",
        txn_data[i:i+batch_size]
    )
    if (i // batch_size) % 10 == 0:
        print(f"    {i:,} / {len(txn_data):,}")

print("  Loading daily_store_sales...")
execute_values(cur,
    "INSERT INTO greentrail.daily_store_sales (store_id, sale_date, revenue, transaction_count, unique_customers) VALUES %s",
    daily_sales_df.values.tolist()
)

# Create indexes
print("  Creating indexes...")
cur.execute("CREATE INDEX idx_txn_store_date ON greentrail.transactions(store_id, transaction_date);")
cur.execute("CREATE INDEX idx_txn_product ON greentrail.transactions(product_id);")
cur.execute("CREATE INDEX idx_txn_customer ON greentrail.transactions(customer_id);")
cur.execute("CREATE INDEX idx_daily_store ON greentrail.daily_store_sales(store_id, sale_date);")

cur.close()
conn.close()

print("\nData generation and loading complete!")

# Quick validation
conn = psycopg2.connect(
    host="localhost", port=5432,
    user="portfolio", password="portfolio_dev",
    dbname="portfolio"
)
cur = conn.cursor()
for table in ["stores", "products", "customers", "transactions", "daily_store_sales"]:
    cur.execute(f"SELECT COUNT(*) FROM greentrail.{table}")
    count = cur.fetchone()[0]
    print(f"  greentrail.{table}: {count:,} rows")

cur.execute("SELECT SUM(line_total) FROM greentrail.transactions")
total = cur.fetchone()[0]
print(f"\n  Total transaction revenue: ${total:,.0f}")

cur.close()
conn.close()
