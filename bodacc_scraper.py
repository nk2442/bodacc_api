# bodacc_scraper.py
import httpx
import asyncio
from datetime import datetime, timedelta
from typing import Optional
import supabase
import logging

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("bodacc")

# Config
SUPABASE_URL = "https://xxx.supabase.co"
SUPABASE_KEY = "votre_service_role_key"
BODACC_BASE  = "https://bodacc-datadila.opendatasoft.com/api/explore/v2.1/catalog/datasets"

# Les 3 datasets BODACC les plus utiles
DATASETS = {
    "bodacc-a": "Ventes et cessions de fonds de commerce",
    "bodacc-b": "Créations, modifications, radiations",
    "bodacc-c": "Procédures collectives (liquidations, redressements)",
}

db = supabase.create_client(SUPABASE_URL, SUPABASE_KEY)


async def fetch_page(client: httpx.AsyncClient, dataset: str, offset: int, since: str) -> dict:
    """Récupère une page de 100 annonces depuis l'API BODACC."""
    url = f"{BODACC_BASE}/{dataset}/records"
    params = {
        "limit": 100,
        "offset": offset,
        "order_by": "dateparution desc",
        "where": f"dateparution >= '{since}'",
        "timezone": "Europe/Paris",
    }
    resp = await client.get(url, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def normalize_record(record: dict, dataset: str) -> dict:
    """Normalise un enregistrement BODACC vers notre schéma unifié."""
    fields = record.get("fields", record)  # v2.1 retourne directement les champs
    
    # Extraction du nom de l'entreprise selon le dataset
    company_name = (
        fields.get("commercant") or
        fields.get("denomination") or
        fields.get("registre", {}).get("denomination", "") if isinstance(fields.get("registre"), dict) else "" or
        "Inconnu"
    )
    
    # Type d'événement normalisé
    event_map = {
        "bodacc-a": "vente_fonds",
        "bodacc-b": fields.get("typeannonce", "modification"),
        "bodacc-c": fields.get("familleavis_lib", "procedure_collective"),
    }
    
    return {
        "bodacc_id":      fields.get("id") or fields.get("numerodepartement", "") + fields.get("numeroannonce", ""),
        "dataset":        dataset,
        "company_name":   str(company_name).strip()[:500],
        "siren":          fields.get("registre", {}).get("siren", "") if isinstance(fields.get("registre"), dict) else fields.get("siren", ""),
        "event_type":     str(event_map.get(dataset, "inconnu"))[:100],
        "tribunal":       fields.get("tribunal", "")[:200],
        "department":     fields.get("numerodepartement", ""),
        "published_at":   fields.get("dateparution"),
        "raw_json":       fields,
        "created_at":     datetime.utcnow().isoformat(),
    }


async def scrape_dataset(dataset: str, since: str):
    """Scrape un dataset BODACC complet depuis une date donnée."""
    log.info(f"Scraping {dataset} depuis {since}...")
    total_inserted = 0
    offset = 0

    async with httpx.AsyncClient() as client:
        while True:
            try:
                data = await fetch_page(client, dataset, offset, since)
            except httpx.HTTPStatusError as e:
                log.error(f"Erreur HTTP {e.response.status_code} sur {dataset} offset={offset}")
                break

            records = data.get("results", [])
            if not records:
                break  # Plus de données

            # Normalise et insère par batch
            normalized = [normalize_record(r, dataset) for r in records]
            
            # Upsert — évite les doublons sur bodacc_id
            result = db.table("bodacc_announcements") \
                       .upsert(normalized, on_conflict="bodacc_id,dataset") \
                       .execute()
            
            total_inserted += len(normalized)
            log.info(f"  {dataset} — offset {offset} : +{len(normalized)} annonces")

            # Pagination
            total_count = data.get("total_count", 0)
            offset += 100
            if offset >= total_count:
                break

            await asyncio.sleep(0.3)  # Respecte le rate limit de l'API

    log.info(f"[OK] {dataset} : {total_inserted} annonces insérées/mises à jour")
    return total_inserted


async def run_full_scrape(days_back: int = 7):
    """Point d'entrée principal — scrape les N derniers jours."""
    since = (datetime.utcnow() - timedelta(days=days_back)).strftime("%Y-%m-%d")
    log.info(f"=== Scraping BODACC depuis {since} ===")
    
    tasks = [scrape_dataset(ds, since) for ds in DATASETS]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    for dataset, result in zip(DATASETS.keys(), results):
        if isinstance(result, Exception):
            log.error(f"[ERREUR] {dataset}: {result}")
        else:
            log.info(f"[OK] {dataset}: {result} entrées")


# ── Scheduler ──────────────────────────────────────────────
import schedule, time

def job():
    asyncio.run(run_full_scrape(days_back=1))  # Run quotidien : dernières 24h

if __name__ == "__main__":
    # Scrape initial sur 30 jours pour peupler la DB
    asyncio.run(run_full_scrape(days_back=30))
    
    # Puis toutes les 24h à 6h du matin
    schedule.every().day.at("06:00").do(job)
    log.info("Scheduler actif — prochain run dans le planning")
    while True:
        schedule.run_pending()
        time.sleep(60)
