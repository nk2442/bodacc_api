# Procfile — fallback si Railway détecte pas le toml
web: uvicorn main:app --host 0.0.0.0 --port $PORT
worker: python bodacc_scraper.py
