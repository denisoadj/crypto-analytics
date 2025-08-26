This project ingests cryptocurrency market data via APIs and WebSockets, stores raw dumps in a local Parquet-based data lake, and processes them with PySpark for feature engineering and unification. Transformed data is loaded into Snowflake as a central ML-ready repository. The pipeline supports incremental ingestion, is orchestrated with Apache Airflow, and runs in Docker containers. Insights and model predictions are visualized through an interactive Streamlit dashboard.

## Repository Structure
```
crypto-analytics/
├── airflow_dags/         # Airflow DAGs orchestrating ingestion, ETL, and ML workflows
├── docker/               # Dockerfiles and configs (PySpark, Airflow, Snowflake connector)
├── ingestion/            # Python scripts: fetch raw data (APIs/WebSockets) → local data lake
├── processing/           # PySpark jobs: transformations (Bronze → Silver → Gold)
│   ├── bronze/           # Initial cleaning & normalization (raw → bronze)
│   ├── silver/           # Feature engineering & enrichment (bronze → silver)
│   │   ├── config/       # Config files for transformations
│   │   ├── silver_main/  # Core silver layer transformation scripts
│   │   ├── transformers/ # Modular transformation logic
│   │   ├── utils/        # Helper functions for processing
│   │   └── validators/   # Data validation & quality checks
│   └── gold/             # Unified ML/analytics-ready tables (silver → gold → Snowflake)
├── storage/              # Local data lake (sandbox): raw + processed parquet files
│   └── raw/              # Raw API/WebSocket dumps in parquet format
├── dashboard/            # Streamlit dashboard for analytics & visualization
│   ├── pages/            # Multi-page Streamlit components
│   └── utils/            # Data loaders & chart builders
├── docs/                 # Documentation: architecture diagrams, API schema references
├── README.md             # Project overview & setup instructions
└── docker-compose.yml    # Multi-container orchestration (Airflow, Spark, Streamlit)


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


