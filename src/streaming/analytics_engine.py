"""
Real-Time Stream Processing & Analytics Core.
Matches Phase 3:
1. Context-aware Anomaly Detection (Z-Score + IQR)
2. Cross-Market Spread & Regional Arbitrage
3. Volatility & Rolling Trend (Windowed EWMA)
4. Concept Drift & Adaptive Baselines
"""

import json
import os
import math
from collections import defaultdict, deque
from datetime import datetime
from typing import Dict, Any, Optional, Tuple

class AnalyticsEngine:
    def __init__(self, cache_path: str = "data/baselines_cache.json"):
        self.cache_path = cache_path
        self.baselines_cache: Dict[str, dict] = {}
        self.load_baselines()

        # Sliding windows for Cross-Market Arbitrage (by commodity)
        # Stores recent reports: {commodity: deque([{market, state, modal_price, timestamp}, ...], maxlen=50)}
        self.market_windows = defaultdict(lambda: deque(maxlen=60))

        # Rolling EWMA & Volatility per commodity
        # {commodity: {"ewma_price": float, "ewma_var": float, "last_updated": datetime, "window": deque(maxlen=30)}}
        self.commodity_stats = {}

        # Adaptive baselines to handle concept drift (alpha = 0.05)
        # {key: {"adaptive_mean": float, "adaptive_std": float, "drift_pct": float}}
        self.adaptive_baselines = {}
        self.alpha_drift = 0.05

        # Counters
        self.stats = {
            "total_processed": 0,
            "normal_count": 0,
            "warning_count": 0,
            "critical_count": 0,
            "arbitrage_alerts": 0
        }

    def load_baselines(self):
        if os.path.exists(self.cache_path):
            with open(self.cache_path, "r", encoding="utf-8") as f:
                self.baselines_cache = json.load(f)
            print(f"[AnalyticsEngine] Loaded {len(self.baselines_cache)} baseline keys into memory.")
        else:
            print(f"[AnalyticsEngine] Warning: Baselines cache not found at {self.cache_path}")

    def get_context_baseline(self, event: dict) -> Tuple[Optional[dict], str]:
        """
        Hierarchical Context Matching:
        1. (Commodity | Market | Variety | Season)
        2. (Commodity | Market)
        3. (Commodity | State | District)
        4. (Commodity | State)
        5. (Commodity) Global Fallback
        """
        comm = event.get("Commodity", "").lower().strip()
        st = event.get("State", "").lower().strip()
        dt = event.get("District", "").lower().strip()
        mkt = event.get("Market", "").lower().strip()
        var = event.get("Variety", "").lower().strip()
        seas = event.get("Season", "").lower().strip()

        # Level 1: Market + Variety + Season
        k1 = f"{comm}|{mkt}|{var}|{seas}"
        if k1 in self.baselines_cache:
            return self.baselines_cache[k1], "market_variety_season"

        # Level 2: Market
        k2 = f"{comm}|{mkt}"
        if k2 in self.baselines_cache:
            return self.baselines_cache[k2], "market"

        # Level 3: District
        k3 = f"{comm}|{st}|{dt}"
        if k3 in self.baselines_cache:
            return self.baselines_cache[k3], "district"

        # Level 4: State
        k4 = f"{comm}|{st}"
        if k4 in self.baselines_cache:
            return self.baselines_cache[k4], "state"

        # Level 5: Commodity Global
        if comm in self.baselines_cache:
            return self.baselines_cache[comm], "commodity_global"

        # Synthetic Default if completely new commodity
        p = float(event.get("Modal_Price", 2000.0))
        return {
            "mean_price": p,
            "std_dev": max(p * 0.15, 100.0),
            "median_price": p,
            "q1": p * 0.9,
            "q3": p * 1.1,
            "iqr": p * 0.2,
            "lower_fence": max(0.0, p * 0.6),
            "upper_fence": p * 1.4,
            "lower_2sigma": max(0.0, p * 0.7),
            "upper_2sigma": p * 1.3
        }, "synthetic_fallback"

    def evaluate_event(self, event: dict) -> Tuple[dict, Optional[dict]]:
        """
        Processes an incoming mandi price event.
        Returns:
            enriched_event (dict), alert (Optional[dict])
        """
        self.stats["total_processed"] += 1
        modal_price = float(event.get("Modal_Price", 0.0))
        commodity = event.get("Commodity", "Unknown")
        market = event.get("Market", "Unknown")
        state = event.get("State", "Unknown")

        # 1. Baseline Lookup
        baseline, level = self.get_context_baseline(event)
        mean_p = float(baseline.get("mean_price", modal_price))
        std_p = max(float(baseline.get("std_dev", 100.0)), 1.0)
        iqr = max(float(baseline.get("iqr", 150.0)), 1.0)
        q1 = float(baseline.get("q1", mean_p * 0.9))
        q3 = float(baseline.get("q3", mean_p * 1.1))

        # 2. Anomaly Detection (Z-Score & IQR)
        z_score = round((modal_price - mean_p) / std_p, 2)
        abs_z = abs(z_score)

        iqr_lower = max(0.0, q1 - 1.5 * iqr)
        iqr_upper = q3 + 1.5 * iqr
        is_iqr_outlier = (modal_price < iqr_lower) or (modal_price > iqr_upper)

        pct_deviation = round(((modal_price - mean_p) / mean_p) * 100.0, 1) if mean_p > 0 else 0.0

        # Classification
        severity = "NORMAL"
        anomaly_tags = []

        if abs_z >= 3.0 or abs(pct_deviation) >= 50.0 or modal_price <= 0:
            severity = "CRITICAL"
            self.stats["critical_count"] += 1
            if modal_price > mean_p:
                anomaly_tags.append("CRITICAL_PRICE_SPIKE")
            else:
                anomaly_tags.append("CRITICAL_PRICE_CRASH")
        elif abs_z >= 2.0 or is_iqr_outlier:
            severity = "WARNING"
            self.stats["warning_count"] += 1
            if is_iqr_outlier:
                anomaly_tags.append("IQR_OUTLIER")
            if abs_z >= 2.0:
                anomaly_tags.append("HIGH_VOLATILITY")
        else:
            self.stats["normal_count"] += 1

        # 3. Concept Drift & Adaptive Baseline Update (EWMA)
        context_key = f"{commodity}|{market}".lower()
        if context_key not in self.adaptive_baselines:
            self.adaptive_baselines[context_key] = {
                "adaptive_mean": mean_p,
                "adaptive_std": std_p,
                "drift_pct": 0.0
            }
        
        adapt = self.adaptive_baselines[context_key]
        # Only update adaptive baseline with non-critical records to prevent contamination
        if severity != "CRITICAL":
            old_mean = adapt["adaptive_mean"]
            new_mean = (self.alpha_drift * modal_price) + ((1.0 - self.alpha_drift) * old_mean)
            adapt["adaptive_mean"] = round(new_mean, 2)
            drift_pct = round(((new_mean - mean_p) / mean_p) * 100.0, 2) if mean_p > 0 else 0.0
            adapt["drift_pct"] = drift_pct

        # 4. Rolling Volatility and Trend
        if commodity not in self.commodity_stats:
            self.commodity_stats[commodity] = {
                "ewma_price": modal_price,
                "rolling_samples": deque(maxlen=20)
            }
        c_stat = self.commodity_stats[commodity]
        c_stat["ewma_price"] = round(0.15 * modal_price + 0.85 * c_stat["ewma_price"], 2)
        c_stat["rolling_samples"].append(modal_price)
        rolling_std = round(float(np_std(c_stat["rolling_samples"])), 2)

        # 5. Cross-Market Spread & Arbitrage Detection
        arbitrage_info = self._check_cross_market_spread(commodity, market, state, modal_price)
        if arbitrage_info and arbitrage_info.get("arbitrage_flag"):
            self.stats["arbitrage_alerts"] += 1

        # Build enriched payload
        enriched_event = {
            **event,
            "Enriched_Timestamp": datetime.utcnow().isoformat(),
            "Baseline_Mean": round(mean_p, 2),
            "Baseline_StdDev": round(std_p, 2),
            "Baseline_Q1": round(q1, 2),
            "Baseline_Q3": round(q3, 2),
            "Baseline_IQR": round(iqr, 2),
            "Expected_Min": round(max(0.0, mean_p - 2.0 * std_p), 2),
            "Expected_Max": round(mean_p + 2.0 * std_p, 2),
            "Context_Level": level,
            "Z_Score": z_score,
            "Percent_Deviation": pct_deviation,
            "Is_IQR_Outlier": is_iqr_outlier,
            "Severity": severity,
            "Anomaly_Tags": anomaly_tags,
            "Adaptive_Mean": adapt["adaptive_mean"],
            "Drift_Percent": adapt["drift_pct"],
            "Commodity_EWMA": c_stat["ewma_price"],
            "Rolling_Volatility": rolling_std,
            "Cross_Market_Arbitrage": arbitrage_info
        }

        # Generate alert object if non-normal
        alert = None
        if severity in ["WARNING", "CRITICAL"]:
            alert = {
                "alert_id": f"ALT-{self.stats['total_processed']}",
                "timestamp": datetime.now().strftime("%H:%M:%S"),
                "commodity": commodity,
                "market": market,
                "state": state,
                "modal_price": modal_price,
                "baseline_mean": round(mean_p, 2),
                "z_score": z_score,
                "percent_deviation": pct_deviation,
                "severity": severity,
                "tags": anomaly_tags,
                "reason": (
                    f"{'Spike' if pct_deviation > 0 else 'Drop'} of {abs(pct_deviation)}% "
                    f"vs expected baseline ₹{round(mean_p, 1)} (Z={z_score})"
                )
            }

        return enriched_event, alert

    def _check_cross_market_spread(self, commodity: str, market: str, state: str, price: float) -> Optional[dict]:
        """Calculates price spread between this market and recent reports in neighboring markets."""
        window = self.market_windows[commodity]
        
        # Compare with recent markets in window
        max_spread = 0.0
        best_target = None

        for item in reversed(window):
            if item["market"] != market:
                spread = price - item["modal_price"]
                abs_spread = abs(spread)
                if abs_spread > max_spread:
                    max_spread = abs_spread
                    best_target = item

        # Append current report to window
        window.append({
            "market": market,
            "state": state,
            "modal_price": price,
            "timestamp": datetime.utcnow().isoformat()
        })

        if best_target and max_spread > 0:
            pct_diff = round((max_spread / min(price, best_target["modal_price"])) * 100.0, 1)
            # Arbitrage flag if spread exceeds 25% price difference
            return {
                "compare_market": best_target["market"],
                "compare_state": best_target["state"],
                "compare_price": best_target["modal_price"],
                "price_spread": round(price - best_target["modal_price"], 2),
                "spread_percent": pct_diff,
                "arbitrage_flag": pct_diff >= 25.0
            }

        return None


def np_std(vals):
    if len(vals) < 2:
        return 0.0
    mean = sum(vals) / len(vals)
    var = sum((x - mean) ** 2 for x in vals) / (len(vals) - 1)
    return math.sqrt(var)
