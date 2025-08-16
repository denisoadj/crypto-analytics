This project ingests cryptocurrency market data from APIs, stores it in an HDFS data lake, and processes it using PySpark for category-based market correlation analysis. The pipeline supports incremental ingestion, partitioned Parquet storage, and automated orchestration via Apache Airflow.

## Repository Structure
```
crypto-analytics/
├── airflow_dags/        # Airflow DAGs for orchestrating workflows
├── .venv/               # Virtual Machine acting as a "sandbox host"
├── ingestion/           # Python scripts for data ingestion
├── processing/          # PySpark jobs for data processing
├── dashboard/           # Dashboard implementation and assets
├── configs/             # Configuration files (API keys, category mappings, etc.)
├── docs/                 # Project documentation
├── requirements.txt     # Python dependencies
└── README.md            # Project overview and setup instructions
```



## Architecture Diagram 
API/WebSocket → Kafka → PySpark (Streaming or Batch) → HDFS (raw)
HDFS (raw) → PySpark Batch → data warehouse (processed) → Hive/Presto → Dashboard

## Key Features
Incremental ingestion from public crypto APIs.

Partitioned Parquet storage in HDFS.

Real-time streaming support via Kafka

Batch analytics with PySpark for large-scale correlation computations.

Automated orchestration with Airflow.

Category-based insights: 
    Payment: USDT, USDC, DAI, BUSD, TUSD, XRP
    DeFi: UNI, AAVE, COMP, SUSHI, MKR, CAKE, CRV
    Layer 1: BTC, ETH, SOL, AVAX, ADA, DOT
    Meme tokens: DOGE, SHIB, FLOKI, PEPE, CKB
    NFT: SAND, MANA, AXS, ENJ, THETA 
    Infrastructures: LINK, GRT, RUNE, AGIX, OCEAN, NEAR, TAO 


