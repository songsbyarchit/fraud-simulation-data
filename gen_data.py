import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import uuid

# Set seed for reproducibility
np.random.seed(42)
random.seed(42)

# Config
start_date = datetime(2024, 12, 15)
end_date = datetime(2025, 1, 14)
num_days = (end_date - start_date).days + 1
num_messages = 100000
num_partners = 10
channels = ["SMS", "RCS", "WhatsApp"]
status_choices = ["delivered", "failed", "blocked"]
industries = ["banking", "retail", "healthcare", "gov", "logistics"]

# Generate partners
partners = pd.DataFrame({
    "partner_id": [f"P{str(i).zfill(3)}" for i in range(1, num_partners + 1)],
    "name": [f"Partner_{i}" for i in range(1, num_partners + 1)],
    "region": np.random.choice(["UK", "EU", "NA"], num_partners),
    "industry": np.random.choice(industries, num_partners),
    "onboarding_date": [start_date - timedelta(days=random.randint(30, 300)) for _ in range(num_partners)],
    "is_active": np.random.choice([True, True, True, False], num_partners)
})

# Generate messages
date_range = [start_date + timedelta(days=i) for i in range(num_days)]
message_data = []

for _ in range(num_messages):
    ts = random.choice(date_range) + timedelta(
        hours=random.randint(0, 23), minutes=random.randint(0, 59), seconds=random.randint(0, 59)
    )
    message_data.append({
        "message_id": str(uuid.uuid4()),
        "timestamp": ts,
        "partner_id": random.choice(partners["partner_id"]),
        "channel": random.choice(channels),
        "status": np.random.choice(status_choices, p=[0.9, 0.07, 0.03]),
        "fraud_flag": False,
        "anomaly_score": round(np.random.normal(loc=0.2, scale=0.1), 3),
        "message_content": random.choice(["Promo", "2FA", "Reminder", "Update", "Alert"])
    })

messages = pd.DataFrame(message_data)

# Inject more fraud around Christmas and NYE
for idx in messages.sample(frac=0.05).index:
    ts = messages.loc[idx, "timestamp"]
    if ts.day in [24, 25, 26, 30, 31, 1]:
        messages.at[idx, "fraud_flag"] = True
        messages.at[idx, "anomaly_score"] = round(np.random.normal(loc=0.9, scale=0.05), 3)

# Generate routing
routing = messages[["message_id"]].copy()
routing["route_id"] = [f"R{random.randint(1000, 9999)}" for _ in range(len(routing))]
routing["latency_ms"] = np.random.normal(loc=180, scale=50, size=len(routing)).astype(int)
routing["delivery_status"] = np.where(messages["status"] == "delivered", "ok", "error")
routing["hop_count"] = np.random.randint(1, 5, size=len(routing))

# Generate anomalies
anomalies = messages[messages["fraud_flag"]].copy()
anomalies["anomaly_type"] = np.random.choice(["volume_spike", "routing_mismatch", "duplicate_content"], len(anomalies))
anomalies["reason"] = np.random.choice(["suspicious timing", "high score", "pattern match"], len(anomalies))
anomalies["action_taken"] = np.random.choice(["flagged", "investigated", "blocked"], len(anomalies))
anomalies = anomalies[["message_id", "anomaly_type", "anomaly_score", "reason", "action_taken"]]

# Generate blocked entities
blocked_entities = pd.DataFrame({
    "entity_type": np.random.choice(["partner_id", "ip_address", "route_id"], 100),
    "entity_id": [random.choice(partners["partner_id"]) if t == "partner_id" 
                  else f"IP_{random.randint(1,255)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}"
                  if t == "ip_address" 
                  else f"R{random.randint(1000, 9999)}" for t in np.random.choice(["partner_id", "ip_address", "route_id"], 100)],
    "reason": np.random.choice(["high fraud rate", "manual review", "policy breach"], 100),
    "block_time": [start_date + timedelta(days=random.randint(0, num_days)) for _ in range(100)],
    "unblocked_timestamp": [None]*100
})

# Save all tables to CSV
messages.to_csv("/mnt/data/messages.csv", index=False)
partners.to_csv("/mnt/data/partners.csv", index=False)
routing.to_csv("/mnt/data/routing.csv", index=False)
anomalies.to_csv("/mnt/data/anomalies.csv", index=False)
blocked_entities.to_csv("/mnt/data/blocked_entities.csv", index=False)