This project ingests cryptocurrency market data via APIs and WebSockets, stores raw dumps in a local Parquet-based data lake, and processes them with PySpark for feature engineering and unification. Transformed data is loaded into Snowflake as a central ML-ready repository. The pipeline supports incremental ingestion, is orchestrated with Apache Airflow, and runs in Docker containers. Insights and model predictions are visualized through an interactive Streamlit dashboard.

## Repository Structure
```
crypto-analytics/
├── airflow_dags/        # Airflow DAGs orchestrating ingestion, ETL, and ML workflows
├── docker/              # Dockerfiles + configs (PySpark, Airflow, Snowflake connector)
├── ingestion/           # Python scripts: fetch raw data (APIs/WebSocket) & dump into local lake
├── processing/          # PySpark jobs: transformations (bronze → silver → gold)
│   ├── bronze/          # Cleaning, normalization from raw → bronze
│   ├── silver/          # Feature engineering, enrichment → silver
│   └── gold/            # Unified ML-ready tables (Snowflake)
├── storage/             # Local data lake (sandbox): raw/processed parquet files
│   └── raw/             # Raw API dumps in parquet format
├── dashboard/           # Streamlit app + assets
│   ├── pages/           # Multi-page dashboard support
│   └── utils/           # Data loaders, chart builders
├── docs/                # Architecture diagrams, API schema references
├── README.md            # Setup + project overview
└── docker-compose.yml   # Multi-container orchestration (Airflow, Spark, Streamlit)

```



## Architecture Diagram 
API / WebSocket → Python Ingestion Scripts → Local Data Lake (storage/raw) 
       → PySpark Processing (ETL / Feature Engineering) → Snowflake (processed, ML-ready) 
       → Streamlit Dashboard


## Categories and tokens

    Payment: USDT, USDC, DAI, BUSD, TUSD, XRP
    DeFi: UNI, AAVE, COMP, SUSHI, MKR, CAKE, CRV 
    Layer 1: BTC, ETH, SOL, AVAX, ADA, DOT
    Meme tokens: DOGE, SHIB, FLOKI, PEPE, CKB
    NFT: SAND, MANA, AXS, ENJ, THETA 
    Infrastructures: LINK, GRT, RUNE, AGIX, OCEAN, NEAR, TAO 


