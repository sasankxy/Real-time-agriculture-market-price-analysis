"""
Generates realistic AGMARKNET agricultural market datasets.
Includes historical data for baseline estimation and stream data for live replay.
"""

import csv
import os
import random
from datetime import datetime, timedelta

MARKETS = [
    # Maharashtra
    {"state": "Maharashtra", "district": "Nashik", "market": "Lasalgaon", "lat": 20.1477, "lng": 74.2255},
    {"state": "Maharashtra", "district": "Nashik", "market": "Pimpalgaon", "lat": 20.1706, "lng": 73.9858},
    {"state": "Maharashtra", "district": "Thane", "market": "Vashi APMC", "lat": 19.0771, "lng": 73.0039},
    {"state": "Maharashtra", "district": "Pune", "market": "Pune Market Yard", "lat": 18.5204, "lng": 73.8567},
    {"state": "Maharashtra", "district": "Nagpur", "market": "Nagpur APMC", "lat": 21.1458, "lng": 79.0882},
    # Karnataka
    {"state": "Karnataka", "district": "Kolar", "market": "Kolar APMC", "lat": 13.1367, "lng": 78.1291},
    {"state": "Karnataka", "district": "Bangalore", "market": "Yeshwanthpur", "lat": 13.0238, "lng": 77.5529},
    {"state": "Karnataka", "district": "Belgaum", "market": "Belgaum APMC", "lat": 15.8497, "lng": 74.4977},
    # Andhra Pradesh
    {"state": "Andhra Pradesh", "district": "Chittoor", "market": "Madanapalle APMC", "lat": 13.5560, "lng": 78.5010},
    {"state": "Andhra Pradesh", "district": "Guntur", "market": "Guntur Mirchi Yard", "lat": 16.3067, "lng": 80.4365},
    # Delhi
    {"state": "Delhi", "district": "North Delhi", "market": "Azadpur Mandi", "lat": 28.7159, "lng": 77.1784},
    {"state": "Delhi", "district": "East Delhi", "market": "Ghazipur Mandi", "lat": 28.6258, "lng": 77.3308},
    # Punjab
    {"state": "Punjab", "district": "Ludhiana", "market": "Khanna Mandi", "lat": 30.7068, "lng": 76.2166},
    # Uttar Pradesh
    {"state": "Uttar Pradesh", "district": "Agra", "market": "Agra APMC", "lat": 27.1767, "lng": 78.0081},
    {"state": "Uttar Pradesh", "district": "Farrukhabad", "market": "Farrukhabad APMC", "lat": 27.3826, "lng": 79.5843},
    # Madhya Pradesh
    {"state": "Madhya Pradesh", "district": "Indore", "market": "Indore Mandi", "lat": 22.7196, "lng": 75.8577},
    # Gujarat
    {"state": "Gujarat", "district": "Surat", "market": "Surat APMC", "lat": 21.1702, "lng": 72.8311},
    {"state": "Gujarat", "district": "Rajkot", "market": "Rajkot APMC", "lat": 22.3039, "lng": 70.8022},
]

COMMODITIES = {
    "Tomato": {
        "varieties": ["Hybrid", "Desi", "Local"],
        "base_price": 2200,      # Rs per Quintal (100 kg)
        "std_dev": 450,
        "primary_markets": ["Kolar APMC", "Pimpalgaon", "Madanapalle APMC", "Azadpur Mandi", "Vashi APMC"],
        "seasonal_factors": {
            "Kharif": 1.15,
            "Rabi": 0.85,
            "Zaid": 1.30,
        }
    },
    "Onion": {
        "varieties": ["Red", "White", "Garhwa"],
        "base_price": 2600,
        "std_dev": 380,
        "primary_markets": ["Lasalgaon", "Pune Market Yard", "Azadpur Mandi", "Yeshwanthpur", "Surat APMC"],
        "seasonal_factors": {
            "Kharif": 1.25,
            "Rabi": 0.90,
            "Zaid": 1.10,
        }
    },
    "Potato": {
        "varieties": ["Jyoti", "Kufri Bahar", "Desi"],
        "base_price": 1650,
        "std_dev": 220,
        "primary_markets": ["Agra APMC", "Farrukhabad APMC", "Azadpur Mandi", "Indore Mandi", "Vashi APMC"],
        "seasonal_factors": {
            "Kharif": 1.10,
            "Rabi": 0.80,
            "Zaid": 1.15,
        }
    },
    "Wheat": {
        "varieties": ["Sharbati", "Dara", "Kalyan"],
        "base_price": 2450,
        "std_dev": 180,
        "primary_markets": ["Khanna Mandi", "Indore Mandi", "Agra APMC", "Azadpur Mandi"],
        "seasonal_factors": {
            "Kharif": 1.05,
            "Rabi": 0.95,
            "Zaid": 1.00,
        }
    },
    "Rice": {
        "varieties": ["Basmati 1121", "Common", "IR-64"],
        "base_price": 3800,
        "std_dev": 420,
        "primary_markets": ["Khanna Mandi", "Guntur Mirchi Yard", "Belgaum APMC"],
        "seasonal_factors": {
            "Kharif": 0.95,
            "Rabi": 1.05,
            "Zaid": 1.00,
        }
    },
    "Cotton": {
        "varieties": ["Medium Staple", "Long Staple", "Desi"],
        "base_price": 6800,
        "std_dev": 650,
        "primary_markets": ["Nagpur APMC", "Rajkot APMC", "Guntur Mirchi Yard"],
        "seasonal_factors": {
            "Kharif": 0.90,
            "Rabi": 1.10,
            "Zaid": 1.15,
        }
    },
    "Mustard": {
        "varieties": ["Yellow", "Black", "Bold"],
        "base_price": 5400,
        "std_dev": 350,
        "primary_markets": ["Agra APMC", "Indore Mandi", "Azadpur Mandi"],
        "seasonal_factors": {
            "Kharif": 1.10,
            "Rabi": 0.90,
            "Zaid": 1.05,
        }
    }
}


def get_season(month: int) -> str:
    """Classify calendar month into Indian agricultural crop seasons."""
    if month in [7, 8, 9, 10]:
        return "Kharif"
    elif month in [11, 12, 1, 2, 3, 4]:
        return "Rabi"
    else:
        return "Zaid"


def generate_records(start_date: datetime, end_date: datetime, count_per_day: int = 15):
    records = []
    delta_days = (end_date - start_date).days
    
    for day_offset in range(delta_days + 1):
        current_date = start_date + timedelta(days=day_offset)
        date_str = current_date.strftime("%Y-%m-%d")
        season = get_season(current_date.month)
        
        # Pick a batch of market-commodity pairs for the day
        for _ in range(count_per_day):
            market_info = random.choice(MARKETS)
            commodity_name = random.choice(list(COMMODITIES.keys()))
            comm_spec = COMMODITIES[commodity_name]
            
            variety = random.choice(comm_spec["varieties"])
            
            # Base price adjustment with season factor and mandi location bias
            season_factor = comm_spec["seasonal_factors"].get(season, 1.0)
            
            # Producing markets tend to have slightly lower prices than terminal consumption markets
            is_primary = market_info["market"] in comm_spec["primary_markets"]
            loc_bias = 0.95 if is_primary else 1.05
            
            base = comm_spec["base_price"] * season_factor * loc_bias
            modal_price = int(random.gauss(base, comm_spec["std_dev"]))
            modal_price = max(400, modal_price)
            
            # Spread between min, modal, and max
            spread_down = random.randint(50, int(modal_price * 0.12))
            spread_up = random.randint(50, int(modal_price * 0.14))
            
            min_price = max(300, modal_price - spread_down)
            max_price = modal_price + spread_up
            arrivals = round(random.uniform(5.0, 350.0), 1)
            
            records.append({
                "State": market_info["state"],
                "District": market_info["district"],
                "Market": market_info["market"],
                "Commodity": commodity_name,
                "Variety": variety,
                "Arrival_Date": date_str,
                "Min_Price": min_price,
                "Max_Price": max_price,
                "Modal_Price": modal_price,
                "Arrivals_Tonnes": arrivals,
                "Latitude": market_info["lat"],
                "Longitude": market_info["lng"]
            })
            
    return records


def save_csv(filename: str, records: list):
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    fieldnames = [
        "State", "District", "Market", "Commodity", "Variety",
        "Arrival_Date", "Min_Price", "Max_Price", "Modal_Price",
        "Arrivals_Tonnes", "Latitude", "Longitude"
    ]
    with open(filename, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)
    print(f"Saved {len(records)} records to {filename}")


if __name__ == "__main__":
    random.seed(42)
    # Generate 1 year of historical data for baselines (Nov 2024 - Oct 2025)
    hist_start = datetime(2024, 11, 1)
    hist_end = datetime(2025, 10, 31)
    print("Generating AGMARKNET historical training dataset...")
    historical_records = generate_records(hist_start, hist_end, count_per_day=20)
    save_csv("data/agmarknet_historical_sample.csv", historical_records)
    
    # Generate recent 30-day feed for streaming ingestion & replay (Nov 2025 - Dec 2025)
    stream_start = datetime(2025, 11, 1)
    stream_end = datetime(2025, 11, 30)
    print("Generating AGMARKNET streaming replay dataset...")
    stream_records = generate_records(stream_start, stream_end, count_per_day=30)
    save_csv("data/agmarknet_stream_feed.csv", stream_records)
