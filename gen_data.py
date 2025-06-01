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

# Hour weights (simulate waking hour activity)
hour_weights = [0.01]*6 + [0.03, 0.06, 0.08, 0.1, 0.12, 0.12, 0.1, 0.08, 0.06, 0.05, 0.04, 0.03] + [0.01]*6
hour_bins = list(range(24))

# Generate messages with realistic behaviour
message_data = []
industry_weekend_modifier = {
    "banking": 0.5,
    "healthcare": 0.6,
    "gov": 0.4,
    "retail": 1.0,
    "logistics": 0.9
}

for _ in range(num_messages):
    day = random.randint(0, num_days - 1)
    base_date = start_date + timedelta(days=day)

    # Subtle weighting for pre-holiday spikes
    pre_holiday_boost = 1.0
    if base_date.day in [22, 23, 24, 30, 31]:
        pre_holiday_boost = random.uniform(1.00, 1.15)  # 5–15% subtle volume bump
    if random.random() > pre_holiday_boost:
        continue

    hour = random.choices(hour_bins, weights=hour_weights, k=1)[0]
    minute = random.randint(0, 59)
    second = random.randint(0, 59)
    ts = base_date + timedelta(hours=hour, minutes=minute, seconds=second)

    is_weekend = ts.weekday() >= 5
    is_holiday = ts.day in [24, 25, 26, 30, 31, 1]
    partner = partners.sample(1).iloc[0]
    industry = partner["industry"]

    # Adjust volume based on weekend + industry
    if is_weekend and random.random() > industry_weekend_modifier[industry]:
        continue

    # Base fraud + anomaly
    fraud_flag = False
    anomaly_score = round(np.random.normal(loc=0.2, scale=0.1), 3)

    # Holiday fraud spike
    base_fraud_chance = max(0, np.random.normal(loc=0.007, scale=0.002))  # centre ~0.7%, ~95% within 0.3%–1.1%
    holiday_fraud_chance = max(0, np.random.normal(loc=0.02, scale=0.005)) if is_holiday else 0  # ~2% mean, flexible

    # Apply fraud logic
    if random.random() < (holiday_fraud_chance or base_fraud_chance):
        fraud_flag = True
        anomaly_score = round(np.random.normal(
            loc=0.85 if is_holiday else 0.7,
            scale=0.08 if is_holiday else 0.12
        ), 3)

    # Subtle legit message drop on key holidays (but not total cutoff)
    if ts.day in [25, 1] and not fraud_flag:
        if random.random() > np.random.normal(loc=0.5, scale=0.1):  # ~40% chance drop, not 70%
            continue

    message_data.append({
        "message_id": str(uuid.uuid4()),
        "timestamp": ts,
        "partner_id": partner["partner_id"],
        "channel": random.choice(channels),
        "status": np.random.choice(status_choices, p=[0.9, 0.07, 0.03]),
        "fraud_flag": fraud_flag,
        "anomaly_score": anomaly_score,
        "message_content": random.choice(["Promo", "2FA", "Reminder", "Update", "Alert"])
    })

messages = pd.DataFrame(message_data)

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
messages.to_csv("data/messages.csv", index=False)
partners.to_csv("data/partners.csv", index=False)
routing.to_csv("data/routing.csv", index=False)
anomalies.to_csv("data/anomalies.csv", index=False)
blocked_entities.to_csv("data/blocked_entities.csv", index=False)