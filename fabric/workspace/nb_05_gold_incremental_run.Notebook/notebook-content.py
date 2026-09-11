# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "09ba38d0-4e10-422e-a1ab-e5d75b1a0160",
# META       "default_lakehouse_name": "lh_gridpulse",
# META       "default_lakehouse_workspace_id": "76887489-1772-4da4-9b27-184dff4f24b9",
# META       "known_lakehouses": [
# META         {
# META           "id": "09ba38d0-4e10-422e-a1ab-e5d75b1a0160"
# META         }
# META       ]
# META     },
# META     "warehouse": {
# META       "known_warehouses": []
# META     }
# META   }
# META }

# CELL ********************

import importlib
import builtin.gridpulse.gold as gold

importlib.invalidate_caches()
gold = importlib.reload(gold)

DATASETS = [
    "market_demand_hourly",
    "zonal_demand_hourly",
    "generation_hourly",
    "day_ahead_price_hourly",
    "realtime_price_5min",
]

print("GridPulse Gold incremental runner initialized.")
print("Gold module:", gold.__file__)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

results = []

print("=== GRIDPULSE GOLD INCREMENTAL RUN ===")

for dataset_name in DATASETS:
    result = gold.run_incremental_gold_dataset(
        spark,
        dataset_name,
    )

    results.append(result)

    print(
        f"{dataset_name} | "
        f"{result['previous_version']} -> {result['ending_version']} | "
        f"CDF={result['cdf_rows']} | "
        f"keys={result['changed_keys']} | "
        f"processed={result['processed_rows']} | "
        f"status={result['status']}"
    )

assert len(results) == 5
assert all(
    result["status"] == "SUCCESS"
    for result in results
)

print("\nGRIDPULSE GOLD INCREMENTAL RUN PASSED.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
