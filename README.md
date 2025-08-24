This project ingests cryptocurrency market data from APIs, stores it in an HDFS data lake, and processes it using PySpark for category-based market correlation analysis. The pipeline supports incremental ingestion, partitioned Parquet storage, and automated orchestration via Apache Airflow.

## Repository Structure
```
crypto-analytics/
├── airflow_dags/        # Airflow DAGs to orchestrate end-to-end data pipelines
├── .idea/               # IDE configuration and project metadata (PyCharm settings)
├── docker/              # Dockerfiles and supporting configs for containerization 
├── ingestion/           # Python scripts to extract and ingest raw data from APIs
├── processing/          # PySpark jobs to clean, transform, and enrich ingested data
├── storage/             # Local data lake storin (sandbox) raw and processed Parquet files
├── dashboard/           # Dashboard implementation and assets
├── docs/                # Project documentation
├── README.md            # Project overview and setup instructions
└── docker-compose.yml/  # Multi-container orchestration for local development
```



## Architecture Diagram 
API/WebSocket → PySpark (Streaming or Batch) → HDFS (raw)
HDFS (raw) → PySpark Batch → data warehouse (processed) → Hive/Presto → Dashboard

## Key Features
Incremental ingestion from public crypto APIs.

Partitioned Parquet storage in HDFS.

Batch analytics with PySpark for large-scale correlation computations.

Automated orchestration with Airflow.

## Categories and tokens

    Payment: USDT, USDC, DAI, BUSD, TUSD, XRP
    DeFi: UNI, AAVE, COMP, SUSHI, MKR, CAKE, CRV 
    Layer 1: BTC, ETH, SOL, AVAX, ADA, DOT
    Meme tokens: DOGE, SHIB, FLOKI, PEPE, CKB
    NFT: SAND, MANA, AXS, ENJ, THETA 
    Infrastructures: LINK, GRT, RUNE, AGIX, OCEAN, NEAR, TAO 


