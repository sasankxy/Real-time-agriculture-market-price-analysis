"""
Official data.gov.in AGMARKNET Live API Client.
Pulls real-time daily mandi prices directly from the Ministry of Agriculture and Farmers Welfare.
Resource ID: 9ef84268-d588-465a-a308-a864a43d0070
"""

import logging
from typing import Dict, List, Optional
import httpx
from src.baselines.preprocessor import clean_string, get_coordinates, get_season, parse_date

logger = logging.getLogger("GovAPIClient")

RESOURCE_ID = "9ef84268-d588-465a-a308-a864a43d0070"
BASE_URL = f"https://api.data.gov.in/resource/{RESOURCE_ID}"
DEFAULT_API_KEY = "579b464db66ec23bdd0000013a6fd2e825af4fbc57cecaf8e25ecb00"


class GovAPIClient:
    def __init__(self, api_key: str = DEFAULT_API_KEY):
        self.api_key = api_key

    async def fetch_records(self, limit: int = 50, offset: int = 0, commodity: Optional[str] = None, state: Optional[str] = None) -> Dict:
        """
        Fetches live records directly from data.gov.in API.
        """
        params = {
            "api-key": self.api_key,
            "format": "json",
            "limit": str(limit),
            "offset": str(offset)
        }
        if commodity:
            params["filters[commodity]"] = commodity
        if state:
            params["filters[state]"] = state

        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko)",
            "Accept": "application/json"
        }
        async with httpx.AsyncClient(timeout=25.0, headers=headers) as client:
            try:
                response = await client.get(BASE_URL, params=params)
                if response.status_code == 200:
                    data = response.json()
                    records = data.get("records", [])
                    total = data.get("total", 0)
                    normalized = [self._normalize_api_record(r) for r in records]
                    return {
                        "status": "success",
                        "total": total,
                        "count": len(normalized),
                        "records": normalized
                    }
                else:
                    logger.error(f"Gov API error HTTP {response.status_code}: {response.text}")
                    return {"status": "error", "code": response.status_code, "records": []}
            except Exception as e:
                logger.error(f"Failed to query data.gov.in: {e}")
                return {"status": "error", "message": str(e), "records": []}

    def _normalize_api_record(self, raw: dict) -> dict:
        """Transforms data.gov.in JSON object to standardized pipeline schema."""
        st = clean_string(raw.get("state", "Unknown"))
        dt = clean_string(raw.get("district", "Unknown"))
        mkt = clean_string(raw.get("market", "Unknown"))
        comm = clean_string(raw.get("commodity", "Unknown"))
        var = clean_string(raw.get("variety", "Standard"))

        try:
            min_p = float(raw.get("min_price", 0))
            max_p = float(raw.get("max_price", 0))
            modal_p = float(raw.get("modal_price", 0))
        except (ValueError, TypeError):
            min_p, max_p, modal_p = 2000.0, 2000.0, 2000.0

        # Swap if order is corrupted
        prices = sorted([min_p, modal_p, max_p])
        min_p, modal_p, max_p = prices[0], prices[1], prices[2]

        dt_parsed = parse_date(raw.get("arrival_date"))
        date_str = dt_parsed.strftime("%Y-%m-%d") if dt_parsed else "2026-10-31"
        month = dt_parsed.month if dt_parsed else 10
        season = get_season(month)

        lat, lng = get_coordinates(st, dt, mkt)

        return {
            "State": st,
            "District": dt,
            "Market": mkt,
            "Commodity": comm,
            "Variety": var,
            "Arrival_Date": date_str,
            "Min_Price": min_p,
            "Max_Price": max_p,
            "Modal_Price": modal_p,
            "Arrivals_Tonnes": 25.0,
            "Season": season,
            "Latitude": lat,
            "Longitude": lng
        }


# Global API client instance
gov_client = GovAPIClient()
