# Deployment Guide - Financial Reconciliation System

## Local Run
1. Install dependencies:
   pip install -r requirements.txt
2. Run frontend:
   streamlit run streamlit_app.py
3. Backend script (optional):
   python reconciliation_system.py
4. Run tests:
   python -m unittest discover -s tests -v

## Recommended Deployment
Use **Streamlit Community Cloud** for fastest deployment and best fit for this dashboard.

### Why this is the best option
- Purpose-built for Python data apps
- Free tier for demos and submissions
- Native support for pandas and CSV workflows
- Minimal DevOps setup

### Deploy Steps (Streamlit Community Cloud)
1. Push this project to a GitHub repository.
2. Go to: https://share.streamlit.io
3. Click "New app" and connect your GitHub repo.
4. Set:
   - Main file path: `streamlit_app.py`
   - Python dependencies: `requirements.txt`
5. Deploy.

## Alternative (Production-Control Option)
Use **Render** if you need custom domains, private networking, or stricter runtime control.

### Render setup
1. Create a new Web Service from your repo.
2. Build command:
   pip install -r requirements.txt
3. Start command:
   streamlit run streamlit_app.py --server.port $PORT --server.address 0.0.0.0

## What to submit
- `reconciliation_system.py`
- `streamlit_app.py`
- `tests/test_reconciliation_system.py`
- `requirements.txt`
- Sample `reconciliation_report.csv`
- Optional: hosted app URL from Streamlit Cloud
