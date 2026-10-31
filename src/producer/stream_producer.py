"""
Streaming Producer (Data Replayer & Live Gov API Poller).
Replays AGMARKNET records into Kafka / message broker at configurable throughput.
Supports both local replay and live data.gov.in API streaming.
"""

import asyncio
import csv
import os
from datetime import datetime
from src.streaming.broker_adapter import broker
from src.producer.gov_api_client import gov_client

class StreamProducer:
    def __init__(self, cleaned_csv_path: str = "data/agmarknet_cleaned.csv"):
        self.cleaned_csv_path = cleaned_csv_path
        self.records = []
        self.is_running = False
        self.delay_seconds = 0.5  # ~2 events/sec default
        self.current_index = 0
        self.pending_injections = []
        self.source_mode = "LOCAL_DATASET"  # "LOCAL_DATASET" or "LIVE_GOV_API"
        self.api_offset = 0
        self.load_dataset()

    def load_dataset(self):
        if os.path.exists(self.cleaned_csv_path):
            with open(self.cleaned_csv_path, mode="r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                self.records = list(reader)
            print(f"[StreamProducer] Loaded {len(self.records)} records for replay.")
        else:
            print(f"[StreamProducer] Dataset not found at {self.cleaned_csv_path}")

    def set_source_mode(self, mode: str):
        if mode in ["LOCAL_DATASET", "LIVE_GOV_API"]:
            self.source_mode = mode
            print(f"[StreamProducer] Switched streaming source to: {mode}")

    def inject_synthetic_anomaly(self, anomaly_type: str = "PRICE_SPIKE", commodity: str = "Tomato", market: str = "Kolar APMC"):
        """Queues a synthetic anomaly to be injected on the next tick."""
        self.pending_injections.append({
            "type": anomaly_type,
            "commodity": commodity,
            "market": market
        })
        print(f"[StreamProducer] Anomaly queued: {anomaly_type} for {commodity} @ {market}")

    def set_speed(self, events_per_second: float):
        if events_per_second <= 0:
            events_per_second = 1.0
        self.delay_seconds = 1.0 / events_per_second

    async def get_next_record(self) -> dict:
        """Retrieves the next record either from live data.gov.in API or local dataset."""
        if self.source_mode == "LIVE_GOV_API":
            try:
                # Fetch small page from government API
                res = await gov_client.fetch_records(limit=10, offset=self.api_offset)
                recs = res.get("records", [])
                if recs:
                    self.api_offset = (self.api_offset + 10) % max(res.get("total", 1000), 10)
                    return recs[0]
            except Exception as e:
                print(f"[StreamProducer] Gov API fetch error: {e}. Falling back to local.")

        # Default local dataset
        raw = dict(self.records[self.current_index])
        self.current_index = (self.current_index + 1) % len(self.records)
        return raw

    async def start(self):
        if not self.records:
            print("[StreamProducer] No records to stream.")
            return

        self.is_running = True
        print(f"[StreamProducer] Live stream started ({self.source_mode}) at {1.0 / self.delay_seconds:.1f} events/sec.")

        while self.is_running:
            raw_event = await self.get_next_record()

            # Update event with real-time observation timestamp
            raw_event["Timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            # Check for synthetic anomaly injection
            if self.pending_injections:
                inj = self.pending_injections.pop(0)
                raw_event["Commodity"] = inj.get("commodity", raw_event.get("Commodity", "Tomato"))
                raw_event["Market"] = inj.get("market", raw_event.get("Market", "Kolar APMC"))
                orig_price = float(raw_event.get("Modal_Price", 2500))

                if inj["type"] == "PRICE_SPIKE":
                    raw_event["Modal_Price"] = round(orig_price * 2.8, 2)
                    raw_event["Max_Price"] = round(raw_event["Modal_Price"] * 1.1, 2)
                    raw_event["Min_Price"] = round(raw_event["Modal_Price"] * 0.9, 2)
                elif inj["type"] == "PRICE_CRASH":
                    raw_event["Modal_Price"] = round(orig_price * 0.25, 2)
                    raw_event["Max_Price"] = round(raw_event["Modal_Price"] * 1.1, 2)
                    raw_event["Min_Price"] = round(raw_event["Modal_Price"] * 0.8, 2)
                elif inj["type"] == "FAT_FINGER":
                    raw_event["Modal_Price"] = round(orig_price * 10.0, 2)
                    raw_event["Max_Price"] = raw_event["Modal_Price"]
                elif inj["type"] == "ZERO_PRICE":
                    raw_event["Modal_Price"] = 0.0

            # Publish raw event to Kafka topic 'mandi-raw-events'
            await broker.send("mandi-raw-events", raw_event)
            await asyncio.sleep(self.delay_seconds)

    def stop(self):
        self.is_running = False
        print("[StreamProducer] Stream paused.")


# Global producer singleton
producer = StreamProducer()
