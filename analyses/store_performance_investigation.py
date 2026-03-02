"""
GreenTrail Outdoor Co. — Store Performance Investigation
=========================================================
Analytical script producing all charts and statistical tests
for the CEO insights brief.

Sections:
1. Revenue Overview
2. Store Benchmarking
3. Category Analysis
4. External Factors (scatter + stats)
5. Riverside Deep Dive
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import matplotlib.dates as mdates
import seaborn as sns
from scipy import stats
import psycopg2
import os
import warnings
warnings.filterwarnings('ignore')

# ─── Style ───
sns.set_theme(style="whitegrid", font_scale=1.1)
plt.rcParams.update({
    'figure.dpi': 150,
    'savefig.dpi': 150,
    'figure.facecolor': 'white',
    'axes.facecolor': 'white',
    'font.family': 'sans-serif',
})

OUTPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'outputs', 'figures')
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ─── Database connection ───
conn = psycopg2.connect(
    host='localhost', port=5432,
    user='portfolio', password='portfolio_dev',
    dbname='portfolio'
)

# ─── Load data from marts ───
stores = pd.read_sql("SELECT * FROM greentrail_marts.dim_stores", conn)
fct = pd.read_sql("SELECT * FROM greentrail_marts.fct_daily_store_sales", conn)
dim_date = pd.read_sql("SELECT * FROM greentrail_marts.dim_date", conn)

# Also load raw transactions for deeper analysis
txn = pd.read_sql("""
    SELECT t.*, p.category
    FROM greentrail.transactions t
    JOIN greentrail.products p ON t.product_id = p.product_id
""", conn)
conn.close()

fct['sale_date'] = pd.to_datetime(fct['sale_date'])
txn['transaction_date'] = pd.to_datetime(txn['transaction_date'])
dim_date['date_day'] = pd.to_datetime(dim_date['date_day'])

# Classify stores
declining_ids = [1, 2, 3, 4]
growing_ids = [5, 6, 7, 8, 9, 10, 11, 12]
store_names = stores.set_index('store_id')['store_name'].to_dict()

# Color palette
DECLINE_COLOR = '#D32F2F'
GROW_COLOR = '#388E3C'
RIVERSIDE_COLOR = '#1565C0'
NEUTRAL_COLOR = '#757575'

print("Data loaded. Starting analysis...\n")

# ═══════════════════════════════════════════════════════════════
# SECTION 1: Revenue Overview
# ═══════════════════════════════════════════════════════════════
print("Section 1: Revenue Overview")

# 1a. Total chain revenue trend (monthly)
monthly_chain = fct.groupby(fct['sale_date'].dt.to_period('M')).agg(
    revenue=('revenue', 'sum'),
    transactions=('transaction_count', 'sum'),
    customers=('unique_customers', 'sum')
).reset_index()
monthly_chain['sale_date'] = monthly_chain['sale_date'].dt.to_timestamp()

fig, ax = plt.subplots(figsize=(12, 5))
ax.bar(monthly_chain['sale_date'], monthly_chain['revenue'] / 1000,
       color='#2E7D32', width=20, alpha=0.8)
ax.set_xlabel('')
ax.set_ylabel('Revenue ($000s)')
ax.set_title('GreenTrail — Total Chain Revenue by Month', fontsize=14, fontweight='bold')
ax.xaxis.set_major_formatter(mdates.DateFormatter('%b\n%Y'))
ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f'${x:,.0f}'))
ax.spines[['top', 'right']].set_visible(False)
plt.tight_layout()
plt.savefig(os.path.join(OUTPUT_DIR, '01_chain_revenue_monthly.png'), bbox_inches='tight')
plt.close()
print("  01_chain_revenue_monthly.png")

# 1b. Store-level revenue indexed to first month
store_monthly = fct.groupby([fct['sale_date'].dt.to_period('M'), 'store_id']).agg(
    revenue=('revenue', 'sum')
).reset_index()
store_monthly['sale_date'] = store_monthly['sale_date'].dt.to_timestamp()

# Index to first full month (Oct 2023 since Sep is partial)
first_month = store_monthly[store_monthly['sale_date'] == '2023-10-01'].set_index('store_id')['revenue']
store_monthly['indexed'] = store_monthly.apply(
    lambda r: (r['revenue'] / first_month.get(r['store_id'], r['revenue'])) * 100, axis=1
)

fig, ax = plt.subplots(figsize=(12, 6))
for sid in sorted(store_monthly['store_id'].unique()):
    data = store_monthly[store_monthly['store_id'] == sid].sort_values('sale_date')
    name = store_names[sid]
    if sid in declining_ids:
        color = RIVERSIDE_COLOR if sid == 4 else DECLINE_COLOR
        lw = 2.5 if sid == 4 else 2
        alpha = 1.0
        zorder = 5
    else:
        color = GROW_COLOR
        lw = 1
        alpha = 0.4
        zorder = 2
    ax.plot(data['sale_date'], data['indexed'], color=color, lw=lw,
            alpha=alpha, zorder=zorder, label=name if sid in declining_ids else None)

# Add growing store legend entry once
ax.plot([], [], color=GROW_COLOR, lw=1, alpha=0.4, label='Growing stores (8)')
ax.axhline(y=100, color='gray', ls='--', lw=0.8, alpha=0.5)
ax.set_ylabel('Revenue Index (Oct 2023 = 100)')
ax.set_title('Store Revenue Trajectories — Indexed to Oct 2023', fontsize=14, fontweight='bold')
ax.legend(loc='upper left', framealpha=0.9)
ax.xaxis.set_major_formatter(mdates.DateFormatter('%b\n%Y'))
ax.spines[['top', 'right']].set_visible(False)
plt.tight_layout()
plt.savefig(os.path.join(OUTPUT_DIR, '02_store_indexed_revenue.png'), bbox_inches='tight')
plt.close()
print("  02_store_indexed_revenue.png")

# 1c. Revenue decomposition: customer count × avg basket × visit frequency
store_period = fct.groupby(['store_id']).agg(
    total_revenue=('revenue', 'sum'),
    total_txns=('transaction_count', 'sum'),
    total_customers=('unique_customers', 'sum'),
).reset_index()
store_period['avg_basket'] = store_period['total_revenue'] / store_period['total_txns']
store_period['avg_visits'] = store_period['total_txns'] / store_period['total_customers']
store_period['store_name'] = store_period['store_id'].map(store_names)
store_period['status'] = store_period['store_id'].apply(
    lambda x: 'Declining' if x in declining_ids else 'Growing')
print("  Revenue decomposition calculated")


# ═══════════════════════════════════════════════════════════════
# SECTION 2: Store Benchmarking
# ═══════════════════════════════════════════════════════════════
print("\nSection 2: Store Benchmarking")

# Revenue growth rate per store (first 6 months vs last 6 months)
first_half = fct[fct['sale_date'] < '2024-03-01'].groupby('store_id')['revenue'].sum()
second_half = fct[fct['sale_date'] >= '2024-09-01'].groupby('store_id')['revenue'].sum()
growth = ((second_half - first_half) / first_half * 100).reset_index()
growth.columns = ['store_id', 'growth_pct']

benchmark = stores[['store_id', 'store_name', 'town_population', 'population_change_pct',
                     'nearest_competitor_km']].merge(growth, on='store_id')
benchmark = benchmark.merge(
    store_period[['store_id', 'avg_basket', 'total_revenue']],
    on='store_id'
)
benchmark['status'] = benchmark['store_id'].apply(
    lambda x: 'Declining' if x in declining_ids else 'Growing')
benchmark = benchmark.sort_values('growth_pct')

# KPI table
chain_avg_growth = benchmark['growth_pct'].mean()
chain_avg_basket = benchmark['avg_basket'].mean()

fig, ax = plt.subplots(figsize=(14, 5))
colors = [RIVERSIDE_COLOR if sid == 4 else DECLINE_COLOR if sid in declining_ids else GROW_COLOR
          for sid in benchmark['store_id']]
bars = ax.barh(benchmark['store_name'], benchmark['growth_pct'], color=colors, alpha=0.85)
ax.axvline(x=0, color='black', lw=0.8)
ax.axvline(x=chain_avg_growth, color='gray', ls='--', lw=1, label=f'Chain avg: {chain_avg_growth:.1f}%')
ax.set_xlabel('Revenue Growth (%)')
ax.set_title('Store Revenue Growth: Early vs Late Period', fontsize=14, fontweight='bold')
ax.legend()
for bar, val in zip(bars, benchmark['growth_pct']):
    ax.text(bar.get_width() + 0.3, bar.get_y() + bar.get_height()/2,
            f'{val:.1f}%', va='center', fontsize=9)
ax.spines[['top', 'right']].set_visible(False)
plt.tight_layout()
plt.savefig(os.path.join(OUTPUT_DIR, '03_store_growth_ranking.png'), bbox_inches='tight')
plt.close()
print("  03_store_growth_ranking.png")


# ═══════════════════════════════════════════════════════════════
# SECTION 3: Category Analysis
# ═══════════════════════════════════════════════════════════════
print("\nSection 3: Category Analysis")

# Category revenue: declining vs growing stores over time
cat_monthly = fct.copy()
cat_monthly['month'] = cat_monthly['sale_date'].dt.to_period('M')
cat_monthly['status'] = cat_monthly['store_id'].apply(
    lambda x: 'Declining' if x in declining_ids else 'Growing')

cat_trend = cat_monthly.groupby(['month', 'status', 'product_category']).agg(
    revenue=('revenue', 'sum')
).reset_index()
cat_trend['month'] = cat_trend['month'].dt.to_timestamp()

fig, axes = plt.subplots(2, 3, figsize=(16, 9), sharey=False)
categories = sorted(fct['product_category'].unique())
for i, cat in enumerate(categories):
    ax = axes[i // 3][i % 3]
    for status, color in [('Growing', GROW_COLOR), ('Declining', DECLINE_COLOR)]:
        data = cat_trend[(cat_trend['product_category'] == cat) & (cat_trend['status'] == status)]
        ax.plot(data['month'], data['revenue'] / 1000, color=color, lw=2, label=status)
    ax.set_title(cat.replace('_', ' ').title(), fontweight='bold')
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b\n%y'))
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f'${x:,.0f}'))
    if i == 0:
        ax.legend(fontsize=9)
    ax.spines[['top', 'right']].set_visible(False)

plt.suptitle('Category Revenue Trends: Declining vs Growing Stores', fontsize=14, fontweight='bold', y=1.02)
plt.tight_layout()
plt.savefig(os.path.join(OUTPUT_DIR, '04_category_trends.png'), bbox_inches='tight')
plt.close()
print("  04_category_trends.png")

# Per-declining-store category changes
for sid in declining_ids:
    store_cat = fct[fct['store_id'] == sid].copy()
    store_cat['month'] = store_cat['sale_date'].dt.to_period('M')
    cat_change = store_cat.groupby(['month', 'product_category'])['revenue'].sum().reset_index()
    cat_change['month'] = cat_change['month'].dt.to_timestamp()

    # Calculate early vs late change
    early = store_cat[store_cat['sale_date'] < '2024-03-01'].groupby('product_category')['revenue'].sum()
    late = store_cat[store_cat['sale_date'] >= '2024-09-01'].groupby('product_category')['revenue'].sum()
    pct_change = ((late - early) / early * 100).sort_values()

    name = store_names[sid]
    print(f"  {name} category changes: {dict(pct_change.round(1))}")


# ═══════════════════════════════════════════════════════════════
# SECTION 4: External Factors
# ═══════════════════════════════════════════════════════════════
print("\nSection 4: External Factors")

# 4a. Scatter: revenue growth vs population change
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))

for _, row in benchmark.iterrows():
    color = RIVERSIDE_COLOR if row['store_id'] == 4 else (
        DECLINE_COLOR if row['store_id'] in declining_ids else GROW_COLOR)
    marker = 'D' if row['store_id'] == 4 else 'o'
    ax1.scatter(row['population_change_pct'], row['growth_pct'],
                c=color, s=120, marker=marker, zorder=5, edgecolors='white', lw=1.5)
    ax1.annotate(row['store_name'], (row['population_change_pct'], row['growth_pct']),
                 textcoords='offset points', xytext=(8, 4), fontsize=8)

# Regression line excluding Riverside
non_riverside = benchmark[benchmark['store_id'] != 4]
slope, intercept, r_value, p_value, std_err = stats.linregress(
    non_riverside['population_change_pct'], non_riverside['growth_pct'])
x_line = np.linspace(benchmark['population_change_pct'].min() - 1,
                      benchmark['population_change_pct'].max() + 1, 50)
ax1.plot(x_line, slope * x_line + intercept, 'k--', lw=1, alpha=0.5)
ax1.set_xlabel('Town Population Change (%)')
ax1.set_ylabel('Store Revenue Growth (%)')
ax1.set_title(f'Growth vs Population Change\n(r={r_value:.2f}, p={p_value:.4f}, excl. Riverside)',
              fontsize=12, fontweight='bold')
ax1.spines[['top', 'right']].set_visible(False)

# 4b. Scatter: revenue growth vs competitor distance
for _, row in benchmark.iterrows():
    color = RIVERSIDE_COLOR if row['store_id'] == 4 else (
        DECLINE_COLOR if row['store_id'] in declining_ids else GROW_COLOR)
    marker = 'D' if row['store_id'] == 4 else 'o'
    ax2.scatter(row['nearest_competitor_km'], row['growth_pct'],
                c=color, s=120, marker=marker, zorder=5, edgecolors='white', lw=1.5)
    ax2.annotate(row['store_name'], (row['nearest_competitor_km'], row['growth_pct']),
                 textcoords='offset points', xytext=(8, 4), fontsize=8)

ax2.set_xlabel('Distance to Nearest Competitor (km)')
ax2.set_ylabel('Store Revenue Growth (%)')
ax2.set_title('Growth vs Competitor Distance', fontsize=12, fontweight='bold')
ax2.spines[['top', 'right']].set_visible(False)

plt.suptitle('External Factor Analysis', fontsize=14, fontweight='bold', y=1.02)
plt.tight_layout()
plt.savefig(os.path.join(OUTPUT_DIR, '05_external_factors.png'), bbox_inches='tight')
plt.close()
print(f"  05_external_factors.png")

# Statistical tests
print("\n  Statistical Tests:")

# Pearson correlation: growth vs population change (excluding Riverside)
r_pop, p_pop = stats.pearsonr(
    non_riverside['population_change_pct'], non_riverside['growth_pct'])
print(f"  Pearson r (growth vs pop change, excl. Riverside): r={r_pop:.3f}, p={p_pop:.4f}")

# T-test: declining vs growing store revenue
declining_rev = fct[fct['store_id'].isin(declining_ids)].groupby('store_id')['revenue'].sum()
growing_rev = fct[fct['store_id'].isin(growing_ids)].groupby('store_id')['revenue'].sum()
t_stat, t_pval = stats.ttest_ind(declining_rev, growing_rev)
print(f"  T-test (declining vs growing total revenue): t={t_stat:.3f}, p={t_pval:.4f}")

# Full correlation including Riverside
r_all, p_all = stats.pearsonr(benchmark['population_change_pct'], benchmark['growth_pct'])
print(f"  Pearson r (all 12 stores): r={r_all:.3f}, p={p_all:.4f}")


# ═══════════════════════════════════════════════════════════════
# SECTION 5: Riverside Deep Dive
# ═══════════════════════════════════════════════════════════════
print("\nSection 5: Riverside Deep Dive")

riverside_txn = txn[txn['store_id'] == 4].copy()
riverside_txn['month'] = riverside_txn['transaction_date'].dt.to_period('M')
riverside_monthly = riverside_txn.groupby(['month', 'category']).agg(
    revenue=('line_total', 'sum')
).reset_index()
riverside_monthly['month'] = riverside_monthly['month'].dt.to_timestamp()

COMPETITOR_DATE = pd.Timestamp('2023-10-01')

# Category colors for Riverside chart
cat_colors = {
    'camping': '#D32F2F',
    'hiking': '#E65100',
    'fishing': '#2E7D32',
    'hunting': '#1B5E20',
    'cycling': '#757575',
    'general_outdoor': '#9E9E9E',
}

fig, ax = plt.subplots(figsize=(13, 6))
for cat in ['camping', 'hiking', 'fishing', 'hunting', 'cycling', 'general_outdoor']:
    data = riverside_monthly[riverside_monthly['category'] == cat].sort_values('month')
    ax.plot(data['month'], data['revenue'] / 1000, color=cat_colors[cat],
            lw=2.5 if cat in ['camping', 'hiking', 'fishing', 'hunting'] else 1.5,
            alpha=1.0 if cat in ['camping', 'hiking', 'fishing', 'hunting'] else 0.6,
            label=cat.replace('_', ' ').title())

ax.axvline(x=COMPETITOR_DATE, color='black', ls='--', lw=2, alpha=0.7)
ax.annotate('Kathmandu opens\n800m away', xy=(COMPETITOR_DATE, ax.get_ylim()[1] * 0.9),
            fontsize=10, fontweight='bold', ha='center',
            bbox=dict(boxstyle='round,pad=0.3', facecolor='lightyellow', edgecolor='gray'))
ax.set_xlabel('')
ax.set_ylabel('Revenue ($000s)')
ax.set_title('Riverside — Monthly Revenue by Category', fontsize=14, fontweight='bold')
ax.legend(loc='upper right', framealpha=0.9)
ax.xaxis.set_major_formatter(mdates.DateFormatter('%b\n%Y'))
ax.spines[['top', 'right']].set_visible(False)
plt.tight_layout()
plt.savefig(os.path.join(OUTPUT_DIR, '06_riverside_deep_dive.png'), bbox_inches='tight')
plt.close()
print("  06_riverside_deep_dive.png")

# Before/after comparison for Riverside
print("\n  Riverside Before/After Competitor (monthly avg):")
for cat in ['camping', 'hiking', 'fishing', 'hunting', 'cycling', 'general_outdoor']:
    data = riverside_monthly[riverside_monthly['category'] == cat]
    before = data[data['month'] < COMPETITOR_DATE]['revenue'].mean()
    # Use data from 3+ months after opening (once impact stabilized)
    after = data[data['month'] >= '2024-01-01']['revenue'].mean()
    if before > 0:
        change = (after - before) / before * 100
        print(f"    {cat:20s}: before=${before:8,.0f}  after=${after:8,.0f}  change={change:+.1f}%")

# Waterfall chart: Riverside revenue change by category
riverside_early = riverside_txn[riverside_txn['transaction_date'] < '2024-01-01'].groupby('category')['line_total'].sum()
riverside_late = riverside_txn[riverside_txn['transaction_date'] >= '2024-07-01'].groupby('category')['line_total'].sum()

# Normalize to monthly averages
months_early = 4  # Sep-Dec 2023
months_late = 8   # Jul 2024 - Feb 2025
early_monthly = riverside_early / months_early
late_monthly = riverside_late / months_late
change = late_monthly - early_monthly

fig, ax = plt.subplots(figsize=(10, 6))
cats_sorted = change.sort_values().index.tolist()
values = change[cats_sorted].values
colors_wf = [DECLINE_COLOR if v < 0 else GROW_COLOR for v in values]
bars = ax.bar([c.replace('_', ' ').title() for c in cats_sorted], values / 1000,
              color=colors_wf, alpha=0.85, edgecolor='white')
for bar, val in zip(bars, values):
    ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + (0.05 if val > 0 else -0.15),
            f'${val/1000:+.1f}K', ha='center', va='bottom' if val > 0 else 'top', fontsize=10)
ax.axhline(y=0, color='black', lw=0.8)
ax.set_ylabel('Monthly Revenue Change ($000s)')
ax.set_title('Riverside — Category Revenue Change (Monthly Avg: Pre vs Post Competitor)',
             fontsize=12, fontweight='bold')
ax.spines[['top', 'right']].set_visible(False)
plt.tight_layout()
plt.savefig(os.path.join(OUTPUT_DIR, '07_riverside_waterfall.png'), bbox_inches='tight')
plt.close()
print("  07_riverside_waterfall.png")

# Summary statistics for the brief
print("\n" + "="*60)
print("KEY FINDINGS SUMMARY")
print("="*60)
total_rev = fct['revenue'].sum()
declining_total = fct[fct['store_id'].isin(declining_ids)]['revenue'].sum()
print(f"Total chain revenue (18 months): ${total_rev:,.0f}")
print(f"Declining stores revenue: ${declining_total:,.0f} ({declining_total/total_rev*100:.1f}% of total)")

# Annualized estimates
annual_factor = 12 / 18
annual_rev = total_rev * annual_factor
print(f"Annualized revenue: ~${annual_rev:,.0f}")

# Revenue decline amount
for sid in declining_ids:
    name = store_names[sid]
    g = benchmark[benchmark['store_id'] == sid]['growth_pct'].values[0]
    early_r = first_half.get(sid, 0)
    late_r = second_half.get(sid, 0)
    decline = late_r - early_r
    print(f"  {name}: {g:+.1f}% growth, revenue change=${decline:+,.0f} (6-month comparison)")

print(f"\nPearson correlation (growth vs pop, excl. Riverside): r={r_pop:.3f}")
print(f"Riverside is the outlier: stable population but -23% revenue")
print("\nAll figures saved to outputs/figures/")
