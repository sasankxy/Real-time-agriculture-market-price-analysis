"""
Historical Context-Aware Baseline Calculator.
Computes context-aware statistics (Mean, StdDev, Median, Q1, Q3, IQR, Min, Max)
and persists into SQLite / DuckDB and an in-memory lookup cache.
Matches Phase 1 - Step 2 of the methodology.
"""

import json
import os
import sqlite3
import duckdb
import numpy as np
import pandas as pd


def compute_baselines(cleaned_csv_path: str = "data/agmarknet_cleaned.csv",
                      db_path: str = "data/baselines.db",
                      json_cache_path: str = "data/baselines_cache.json"):
    print(f"Loading cleaned data from {cleaned_csv_path}...")
    df = pd.read_csv(cleaned_csv_path)

    # Convert prices to float
    df["Modal_Price"] = pd.to_numeric(df["Modal_Price"], errors="coerce")
    df = df.dropna(subset=["Modal_Price"])

    baselines_list = []

    def calc_stats(group_df, context_level: str, key_dict: dict):
        prices = group_df["Modal_Price"].to_numpy()
        n = len(prices)
        if n == 0:
            return None

        mean_val = float(np.mean(prices))
        std_val = float(np.std(prices, ddof=1)) if n > 1 else max(mean_val * 0.08, 50.0)
        # Avoid zero or near-zero std dev for anomaly division
        if std_val < 1.0:
            std_val = max(mean_val * 0.05, 50.0)

        q1 = float(np.percentile(prices, 25))
        median_val = float(np.median(prices))
        q3 = float(np.percentile(prices, 75))
        iqr = float(q3 - q1)
        if iqr <= 0:
            iqr = max(std_val * 1.35, 40.0)

        min_val = float(np.min(prices))
        max_val = float(np.max(prices))

        lower_fence = max(0.0, q1 - 1.5 * iqr)
        upper_fence = q3 + 1.5 * iqr

        lower_2sigma = max(0.0, mean_val - 2.0 * std_val)
        upper_2sigma = mean_val + 2.0 * std_val

        return {
            "context_level": context_level,
            "commodity": key_dict.get("commodity", "ALL"),
            "state": key_dict.get("state", "ALL"),
            "district": key_dict.get("district", "ALL"),
            "market": key_dict.get("market", "ALL"),
            "variety": key_dict.get("variety", "ALL"),
            "season": key_dict.get("season", "ALL"),
            "sample_count": int(n),
            "mean_price": round(mean_val, 2),
            "std_dev": round(std_val, 2),
            "median_price": round(median_val, 2),
            "q1": round(q1, 2),
            "q3": round(q3, 2),
            "iqr": round(iqr, 2),
            "min_price": round(min_val, 2),
            "max_price": round(max_val, 2),
            "lower_fence": round(lower_fence, 2),
            "upper_fence": round(upper_fence, 2),
            "lower_2sigma": round(lower_2sigma, 2),
            "upper_2sigma": round(upper_2sigma, 2),
        }

    # 1. Level 1: Full Context (Commodity, State, Market, Variety, Season)
    grouped_full = df.groupby(["Commodity", "State", "District", "Market", "Variety", "Season"])
    for (comm, st, dt, mkt, var, seas), grp in grouped_full:
        stats = calc_stats(grp, "market_variety_season", {
            "commodity": comm, "state": st, "district": dt,
            "market": mkt, "variety": var, "season": seas
        })
        if stats:
            baselines_list.append(stats)

    # 2. Level 2: Market Level (Commodity, State, Market)
    grouped_market = df.groupby(["Commodity", "State", "Market"])
    for (comm, st, mkt), grp in grouped_market:
        stats = calc_stats(grp, "market", {
            "commodity": comm, "state": st, "market": mkt
        })
        if stats:
            baselines_list.append(stats)

    # 3. Level 3: District Level (Commodity, State, District)
    grouped_district = df.groupby(["Commodity", "State", "District"])
    for (comm, st, dt), grp in grouped_district:
        stats = calc_stats(grp, "district", {
            "commodity": comm, "state": st, "district": dt
        })
        if stats:
            baselines_list.append(stats)

    # 4. Level 4: State Level (Commodity, State)
    grouped_state = df.groupby(["Commodity", "State"])
    for (comm, st), grp in grouped_state:
        stats = calc_stats(grp, "state", {
            "commodity": comm, "state": st
        })
        if stats:
            baselines_list.append(stats)

    # 5. Level 5: Global Commodity Level
    grouped_comm = df.groupby("Commodity")
    for comm, grp in grouped_comm:
        stats = calc_stats(grp, "commodity_global", {
            "commodity": comm
        })
        if stats:
            baselines_list.append(stats)

    baselines_df = pd.DataFrame(baselines_list)
    print(f"Generated {len(baselines_df)} context baseline entries across 5 hierarchy levels.")

    # Save to SQLite
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    baselines_df.to_sql("baselines", conn, if_exists="replace", index=False)
    # Create indexes for sub-millisecond lookups
    conn.execute("CREATE INDEX IF NOT EXISTS idx_full ON baselines(commodity, market, variety, season);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_market ON baselines(commodity, market);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_district ON baselines(commodity, district);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_state ON baselines(commodity, state);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_comm ON baselines(commodity);")
    conn.commit()
    conn.close()
    print(f"Saved SQLite baselines database to {db_path}")

    # Also export DuckDB Parquet
    duckdb_path = "data/baselines.duckdb"
    ddb = duckdb.connect(duckdb_path)
    ddb.execute("CREATE OR REPLACE TABLE baselines AS SELECT * FROM baselines_df")
    ddb.close()
    print(f"Saved DuckDB baselines database to {duckdb_path}")

    # Build ultra-fast dictionary cache for Stream Engine
    cache_dict = {}
    for item in baselines_list:
        lvl = item["context_level"]
        comm = item["commodity"].lower().strip()
        st = item["state"].lower().strip()
        dt = item["district"].lower().strip()
        mkt = item["market"].lower().strip()
        var = item["variety"].lower().strip()
        seas = item["season"].lower().strip()

        if lvl == "market_variety_season":
            k = f"{comm}|{mkt}|{var}|{seas}"
        elif lvl == "market":
            k = f"{comm}|{mkt}"
        elif lvl == "district":
            k = f"{comm}|{st}|{dt}"
        elif lvl == "state":
            k = f"{comm}|{st}"
        else:
            k = f"{comm}"
        cache_dict[k] = item

    with open(json_cache_path, "w", encoding="utf-8") as f:
        json.dump(cache_dict, f)
    print(f"Saved fast in-memory baseline cache with {len(cache_dict)} lookup keys to {json_cache_path}")

    return baselines_df


if __name__ == "__main__":
    compute_baselines()
