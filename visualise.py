import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime

# Load data
messages = pd.read_csv("data/messages.csv", parse_dates=["timestamp"])

# Time-based features
messages["hour"] = messages["timestamp"].dt.hour
messages["date"] = messages["timestamp"].dt.date
messages["weekday"] = messages["timestamp"].dt.day_name()
messages["weekend"] = messages["timestamp"].dt.weekday >= 5

# Create figure and axes
fig, axs = plt.subplots(6, 1, figsize=(14, 30))
fig.suptitle("Messaging Platform Trends (Hourly + Fraud)", fontsize=16)

# Plot 0: Daily Volume and Fraud Rate with Holiday Spike Highlight
daily_stats = messages.groupby("date").agg(
    total_messages=("message_id", "count"),
    fraud_messages=("fraud_flag", "sum")
)
daily_stats["fraud_rate"] = daily_stats["fraud_messages"] / daily_stats["total_messages"]
daily_stats.index = pd.to_datetime(daily_stats.index)

# Left Y-axis: total volume
axs[0].plot(daily_stats.index, daily_stats["total_messages"], marker='o', label="Daily Volume")
axs[0].plot(daily_stats["total_messages"].rolling(3).mean(), linestyle='--', color='grey', alpha=0.6, label="3-day Volume Trend")
axs[0].set_ylabel("Message Count")
axs[0].set_xlabel("Date")

# Right Y-axis: fraud rate
ax0b = axs[0].twinx()
ax0b.plot(daily_stats.index, daily_stats["fraud_rate"], color='red', linestyle='-', marker='x', label="Fraud Rate")
ax0b.set_ylabel("Fraud Rate (%)")
ax0b.set_ylim(0, daily_stats["fraud_rate"].max() * 1.2)

# Holidays
for holiday in [datetime(2024, 12, 24).date(), datetime(2024, 12, 25).date(),
                datetime(2024, 12, 31).date(), datetime(2025, 1, 1).date()]:
    axs[0].axvline(holiday, color='orange', linestyle='--', alpha=0.7)
    axs[0].annotate(holiday.strftime('%b %d'), xy=(holiday, axs[0].get_ylim()[1]*0.9), color='orange')

# Legends
lines1, labels1 = axs[0].get_legend_handles_labels()
lines2, labels2 = ax0b.get_legend_handles_labels()
axs[0].legend(lines1 + lines2, labels1 + labels2, loc='upper left')
axs[0].set_title("Daily Message Volume & Fraud Rate with Holiday Markers")

# Plot 1: Hourly Volume
hourly_volume = messages.groupby("hour")["message_id"].count()
axs[1].bar(hourly_volume.index, hourly_volume.values)
axs[1].set_title("Hourly Message Volume (All Days)")
axs[1].set_xlabel("Hour of Day")
axs[1].set_ylabel("Message Count")

# Plot 2: Hourly Fraud
hourly_fraud = messages[messages["fraud_flag"]].groupby("hour")["message_id"].count()
axs[2].bar(hourly_fraud.index, hourly_fraud.values, color='red')
axs[2].set_title("Hourly Fraud Message Volume")
axs[2].set_xlabel("Hour of Day")
axs[2].set_ylabel("Fraud Count")

# Plot 3: Hourly Volume Over Time
hourly_by_day = messages.groupby(messages["timestamp"].dt.floor("H"))["message_id"].count()
axs[3].plot(hourly_by_day.index, hourly_by_day.values)
axs[3].set_title("Total Hourly Message Volume Over Time")
axs[3].set_xlabel("Timestamp (Hourly)")
axs[3].set_ylabel("Message Count")

# Plot 4: Weekday vs Weekend Hourly
weekend_compare = messages.groupby(["weekend", "hour"])["message_id"].count().unstack().T
weekend_compare.columns = ["Weekday", "Weekend"]
axs[4].plot(weekend_compare.index, weekend_compare["Weekday"], label="Weekday")
axs[4].plot(weekend_compare.index, weekend_compare["Weekend"], label="Weekend", linestyle="--")
axs[4].set_title("Hourly Volume: Weekdays vs Weekends")
axs[4].set_xlabel("Hour")
axs[4].set_ylabel("Messages")
axs[4].legend()

# Plot 5: Hourly Volume by Weekday
pivot = messages.groupby(["weekday", "hour"]).size().unstack().fillna(0)
ordered_days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
pivot = pivot.reindex(ordered_days)
for day in ordered_days:
    axs[5].plot(pivot.columns, pivot.loc[day], label=day)
axs[5].set_title("Hourly Volume by Weekday")
axs[5].set_xlabel("Hour")
axs[5].set_ylabel("Message Count")
axs[5].legend()

# Render all plots cleanly
plt.tight_layout(rect=[0, 0.03, 1, 0.97])
plt.show()