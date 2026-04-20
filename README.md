# MegaFraudDetector9000+

Real-time fraud detection system with rule-based risk scoring, Neo4j graph analysis, and a Streamlit dashboard.

## Prerequisites

- **Python 3.13+**
- **Neo4j Community Edition 2026.03.1**

## 1. Set up Neo4j

1. Download [Neo4j Community 2026.03.1](https://neo4j.com/deployment-center/) and extract it. (Zip file is also on sharepoint)
2.  Go to neo4j-community-2026.03.1\bin\neo4j directory and set the password to `capgemini` (or any password you choose)
   ```bash
   # Windows
   .\neo4j-admin.ps1 dbms set-initial-password capgemini

   # Linux / macOS
   ./bin/neo4j-admin dbms set-initial-password capgemini
   ```
3. Start the database. Go to neo4j-community-2026.03.1\bin\neo4j directory and run:
   ```bash
   # Windows
   .\neo4j-admin.ps1 server console 

   # Linux / macOS
   ./bin/neo4j console
   ```
3. Open the Neo4j Browser at <http://localhost:7474> and login with password to `capgemini` (or any password of your choice) 

## 2. Set up Python environment

```bash
python -m venv .venv
.venv\Scripts\activate
pip install uv
uv sync
```

## 3. Run the application

```bash
streamlit run presentation/app.py
```

The dashboard will open at <http://localhost:8501>.

## 4. Run tests

```bash
python -m pytest test/ -v
```

> **Note:** Three graph-integration tests are skipped unless a running Neo4j instance is available.

## Project structure

```
domain/          Pure business logic (entities, rules, risk scoring)
application/     Use-case orchestration and port interfaces
infrastructure/  External integrations (Neo4j, CSV export, config)
presentation/    Streamlit UI and dashboards
test/            Unit & integration tests
```