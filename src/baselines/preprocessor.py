"""
Preprocessing & Schema Normalization for AGMARKNET Agricultural Market Datasets.
Matches Phase 1 - Objectives 1 & 2 of the project methodology.
"""

import os
import re
import pandas as pd
from datetime import datetime

# Approximate coordinates for Indian States/Districts/Markets for Map Visualization
GEO_COORDINATES = {
    # States fallback coordinates
    "andhra pradesh": (15.9129, 79.7400),
    "telangana": (17.8749, 78.1008),
    "karnataka": (15.3173, 75.7139),
    "maharashtra": (19.7515, 75.7139),
    "keralam": (10.8505, 76.2711),
    "kerala": (10.8505, 76.2711),
    "tamil nadu": (11.1271, 78.6569),
    "madhya pradesh": (22.9734, 78.6569),
    "gujarat": (22.2587, 71.1924),
    "rajasthan": (27.0238, 74.2179),
    "punjab": (31.1471, 75.3412),
    "haryana": (29.0588, 76.0856),
    "uttar pradesh": (26.8467, 80.9462),
    "odisha": (20.9517, 85.0985),
    "west bengal": (22.9868, 87.8550),
    "bihar": (25.0961, 85.3131),
    "chattisgarh": (21.2787, 81.8661),
    "chhattisgarh": (21.2787, 81.8661),
    "delhi": (28.7041, 77.1025),
    "himachal pradesh": (31.1048, 77.1734),
    "uttarakhand": (30.0668, 79.0193),

    # Key districts / major markets
    ("maharashtra", "nashik"): (20.0059, 73.7910),
    ("maharashtra", "lasalgaon"): (20.1477, 74.2255),
    ("maharashtra", "pimpalgaon"): (20.1706, 73.9858),
    ("maharashtra", "pune"): (18.5204, 73.8567),
    ("maharashtra", "thane"): (19.2183, 72.9781),
    ("maharashtra", "nagpur"): (21.1458, 79.0882),
    ("karnataka", "kolar"): (13.1367, 78.1291),
    ("karnataka", "bangalore"): (12.9716, 77.5946),
    ("karnataka", "bengaluru"): (12.9716, 77.5946),
    ("andhra pradesh", "guntur"): (16.3067, 80.4365),
    ("andhra pradesh", "chittoor"): (13.2172, 79.1003),
    ("delhi", "azadpur"): (28.7159, 77.1784),
    ("punjab", "ludhiana"): (30.9010, 75.8573),
    ("punjab", "khanna"): (30.7068, 76.2166),
    ("uttar pradesh", "agra"): (27.1767, 78.0081),
    ("uttar pradesh", "farrukhabad"): (27.3826, 79.5843),
    ("madhya pradesh", "indore"): (22.7196, 75.8577),
    ("gujarat", "surat"): (21.1702, 72.8311),
    ("gujarat", "rajkot"): (22.3039, 70.8022),
    ("keralam", "kozhikode"): (11.2588, 75.7804),
    ("keralam", "wayanad"): (11.6854, 76.1320),
    ("telangana", "hyderabad"): (17.3850, 78.4867),
}


def get_season(month: int) -> str:
    """Classify agricultural season based on standard Indian crop cycles."""
    if month in [7, 8, 9, 10]:
        return "Kharif"
    elif month in [11, 12, 1, 2, 3, 4]:
        return "Rabi"
    else:
        return "Zaid"


def clean_string(val) -> str:
    """Trim duplicate spaces, title-case, handle nulls."""
    if pd.isna(val):
        return "Unknown"
    text = re.sub(r"\s+", " ", str(val)).strip()
    return text


def parse_date(date_val):
    """Parse various arrival date formats (DD/MM/YYYY, YYYY-MM-DD, etc.) into ISO date string."""
    if pd.isna(date_val):
        return None
    date_str = str(date_val).strip()
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y", "%Y/%m/%d", "%m/%d/%Y"):
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            pass
    return None


def get_coordinates(state: str, district: str, market: str):
    """Retrieve latitude/longitude using market, district, or state."""
    st = state.lower().strip()
    dt = district.lower().strip()
    mk = market.lower().strip()

    # Match by market keyword
    for k, coords in GEO_COORDINATES.items():
        if isinstance(k, tuple) and k[0] == st and (k[1] in mk or k[1] in dt):
            # Add slight jitter so markets in the same district don't overlap completely
            jitter = (hash(market) % 50 - 25) * 0.002
            return coords[0] + jitter, coords[1] + jitter

    # Match by state
    if st in GEO_COORDINATES:
        coords = GEO_COORDINATES[st]
        jitter = (hash(market + district) % 100 - 50) * 0.01
        return coords[0] + jitter, coords[1] + jitter

    # Default central India
    return 20.5937, 78.9629


def clean_agmarknet_dataset(raw_csv_path: str, output_csv_path: str = "data/agmarknet_cleaned.csv"):
    print(f"Reading raw dataset from: {raw_csv_path}")
    df = pd.read_csv(raw_csv_path)

    # Standardize column names (handling data.gov.in XML entity _x0020_ for space)
    rename_map = {}
    for col in df.columns:
        clean_col = col.replace("_x0020_", " ").replace("_", " ").strip()
        clean_col_lower = clean_col.lower()
        if "min" in clean_col_lower and "price" in clean_col_lower:
            rename_map[col] = "Min_Price"
        elif "max" in clean_col_lower and "price" in clean_col_lower:
            rename_map[col] = "Max_Price"
        elif "modal" in clean_col_lower and "price" in clean_col_lower:
            rename_map[col] = "Modal_Price"
        elif "arrival" in clean_col_lower and "date" in clean_col_lower:
            rename_map[col] = "Arrival_Date"
        elif "arrival" in clean_col_lower and "ton" in clean_col_lower:
            rename_map[col] = "Arrivals_Tonnes"
        elif "state" in clean_col_lower:
            rename_map[col] = "State"
        elif "district" in clean_col_lower:
            rename_map[col] = "District"
        elif "market" in clean_col_lower:
            rename_map[col] = "Market"
        elif "commodity" in clean_col_lower:
            rename_map[col] = "Commodity"
        elif "variety" in clean_col_lower:
            rename_map[col] = "Variety"
        elif "grade" in clean_col_lower:
            rename_map[col] = "Grade"

    df = df.rename(columns=rename_map)

    # Clean text columns
    for col in ["State", "District", "Market", "Commodity", "Variety", "Grade"]:
        if col in df.columns:
            df[col] = df[col].apply(clean_string)
        else:
            df[col] = "Standard"

    # Clean and parse prices
    for p_col in ["Min_Price", "Max_Price", "Modal_Price"]:
        df[p_col] = pd.to_numeric(df[p_col], errors="coerce")

    # Drop records with missing or non-positive modal prices
    initial_len = len(df)
    df = df.dropna(subset=["Modal_Price"])
    df = df[df["Modal_Price"] > 0]

    # Fill Min and Max if missing
    df["Min_Price"] = df["Min_Price"].fillna(df["Modal_Price"])
    df["Max_Price"] = df["Max_Price"].fillna(df["Modal_Price"])

    # Ensure Min <= Modal <= Max (swap if corrupted)
    def fix_price_order(row):
        prices = sorted([row["Min_Price"], row["Modal_Price"], row["Max_Price"]])
        row["Min_Price"] = prices[0]
        row["Modal_Price"] = prices[1]
        row["Max_Price"] = prices[2]
        return row

    df = df.apply(fix_price_order, axis=1)

    # Parse Arrival_Date into standard ISO (YYYY-MM-DD)
    parsed_dates = df["Arrival_Date"].apply(parse_date)
    # If date missing or invalid, fallback to current or default date
    now = datetime.now()
    parsed_dates = parsed_dates.apply(lambda d: d if pd.notna(d) else now)
    df["Arrival_Date"] = parsed_dates.dt.strftime("%Y-%m-%d")
    df["Year"] = parsed_dates.dt.year
    df["Month"] = parsed_dates.dt.month
    df["Day"] = parsed_dates.dt.day
    df["Season"] = df["Month"].apply(get_season)

    # Add Arrivals in Tonnes if missing
    if "Arrivals_Tonnes" not in df.columns:
        df["Arrivals_Tonnes"] = 25.0
    else:
        df["Arrivals_Tonnes"] = pd.to_numeric(df["Arrivals_Tonnes"], errors="coerce").fillna(20.0)

    # Attach Coordinates
    coords = [get_coordinates(row["State"], row["District"], row["Market"]) for _, row in df.iterrows()]
    df["Latitude"] = [c[0] for c in coords]
    df["Longitude"] = [c[1] for c in coords]

    # Sort deterministically
    df = df.sort_values(by=["Arrival_Date", "State", "Commodity", "Market"]).reset_index(drop=True)

    os.makedirs(os.path.dirname(output_csv_path), exist_ok=True)
    df.to_csv(output_csv_path, index=False)
    print(f"Cleaned dataset: {len(df)} records (filtered from {initial_len}). Saved to: {output_csv_path}")
    return df


if __name__ == "__main__":
    raw_path = "data/agmarknet_raw.csv"
    if os.path.exists(raw_path):
        clean_agmarknet_dataset(raw_path, "data/agmarknet_cleaned.csv")
    else:
        print(f"Raw data file not found at {raw_path}")
