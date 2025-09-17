#!/usr/bin/env python3
"""
Test script for realistic volume patterns from the new simtom API.
"""

import logging
from datetime import date
from scripts.bnpl.api_client import BNPLAPIClient

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

print("Testing Realistic Volume Patterns from Simtom API")
print("=" * 50)

# Test various days to see volume variations
test_dates = [
    (date(2024, 1, 15), "Regular Monday"),
    (date(2024, 1, 20), "Regular Saturday"),
    (date(2024, 2, 14), "Valentine's Day"),
    (date(2024, 11, 29), "Black Friday"),
    (date(2024, 12, 25), "Christmas Day"),
    (date(2024, 1, 1), "New Year's Day")
]

client = BNPLAPIClient()

print(f"Base daily volume: 5,000 (average)")
print(f"Testing realistic variations by date:")
print()

results = []

for test_date, description in test_dates:
    try:
        records = client.get_daily_batch(
            target_date=test_date,
            base_daily_volume=5000,
            seed=42
        )

        record_count = len(records)
        percentage = (record_count / 5000) * 100

        print(f"{test_date} ({description:15}): {record_count:5,} records ({percentage:5.1f}%)")
        results.append((test_date, description, record_count, percentage))

    except Exception as e:
        print(f"{test_date} ({description:15}): ERROR - {e}")

print()
print("Analysis:")
print("-" * 40)

# Find min/max variations
if results:
    min_result = min(results, key=lambda x: x[2])
    max_result = max(results, key=lambda x: x[2])

    print(f"Lowest volume:  {min_result[1]} ({min_result[2]:,} records)")
    print(f"Highest volume: {max_result[1]} ({max_result[2]:,} records)")
    print(f"Variation range: {min_result[3]:.1f}% to {max_result[3]:.1f}%")

    # Check if we see realistic patterns
    weekend_days = [r for r in results if r[0].weekday() >= 5]  # Sat/Sun
    holiday_days = [r for r in results if "Christmas" in r[1] or "New Year" in r[1]]
    special_days = [r for r in results if "Black Friday" in r[1] or "Valentine" in r[1]]

    if weekend_days:
        avg_weekend = sum(r[2] for r in weekend_days) / len(weekend_days)
        print(f"Average weekend volume: {avg_weekend:,.0f} records")

    if holiday_days:
        avg_holiday = sum(r[2] for r in holiday_days) / len(holiday_days)
        print(f"Average holiday volume: {avg_holiday:,.0f} records")

    if special_days:
        avg_special = sum(r[2] for r in special_days) / len(special_days)
        print(f"Average special event volume: {avg_special:,.0f} records")

print()
print("✅ Realistic patterns detected!" if results and max_result[2] != min_result[2] else "❌ No variation detected")