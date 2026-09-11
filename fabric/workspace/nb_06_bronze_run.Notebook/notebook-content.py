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
# META     }
# META   }
# META }

# CELL ********************

import importlib
import builtin.gridpulse.orchestration as orchestration

importlib.invalidate_caches()
orchestration = importlib.reload(orchestration)

SOURCES = [
    {
        "source_name": "ieso_hourly_demand",
        "source_url": (
            "https://reports-public.ieso.ca/public/Demand/"
            "PUB_Demand_2026.csv"
        ),
        "source_file": "PUB_Demand_2026.csv",
        "logical_key_kwargs": {
            "year": 2026,
        },
    },
    {
        "source_name": "ieso_hourly_zonal_demand",
        "source_url": (
            "https://reports-public.ieso.ca/public/DemandZonal/"
            "PUB_DemandZonal_2026.csv"
        ),
        "source_file": "PUB_DemandZonal_2026.csv",
        "logical_key_kwargs": {
            "year": 2026,
        },
    },
    {
        "source_name": "ieso_generation_by_fuel_hourly",
        "source_url": (
            "https://reports-public.ieso.ca/public/"
            "GenOutputbyFuelHourly/"
            "PUB_GenOutputbyFuelHourly_2026.xml"
        ),
        "source_file": "PUB_GenOutputbyFuelHourly_2026.xml",
        "logical_key_kwargs": {
            "year": 2026,
        },
    },
    {
        "source_name": "ieso_day_ahead_ontario_zonal_price",
        "source_url": (
            "https://reports-public.ieso.ca/public/"
            "DAHourlyOntarioZonalPrice/"
            "PUB_DAHourlyOntarioZonalPrice_20260816.xml"
        ),
        "source_file": "PUB_DAHourlyOntarioZonalPrice_20260816.xml",
        "logical_key_kwargs": {
            "delivery_date": "2026-08-16",
        },
    },
    {
        "source_name": "ieso_realtime_ontario_zonal_price",
        "source_url": (
            "https://reports-public.ieso.ca/public/"
            "RealtimeOntarioZonalPrice/"
            "PUB_RealtimeOntarioZonalPrice.xml"
        ),
        "source_file": "PUB_RealtimeOntarioZonalPrice.xml",
        "logical_key_kwargs": {
            "source_file": "PUB_RealtimeOntarioZonalPrice.xml",
        },
    },
]

print("GridPulse Bronze production runner initialized.")
print("Sources:", len(SOURCES))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

results = []

print("=== GRIDPULSE BRONZE RUN ===")

for config in SOURCES:
    print(f"\nProcessing: {config['source_name']}")

    result = orchestration.ingest_http_source(
        spark=spark,
        source_name=config["source_name"],
        source_url=config["source_url"],
        source_file=config["source_file"],
        logical_key_kwargs=config["logical_key_kwargs"],
    )

    results.append({
        "source_name": config["source_name"],
        **result,
    })

    print(
        f"{config['source_name']} | "
        f"classification={result['classification']} | "
        f"bronze_write={result['bronze_write']} | "
        f"reused={result['reused_existing_file']}"
    )

assert len(results) == 5

print("\nGRIDPULSE BRONZE RUN PASSED.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
