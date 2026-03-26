# main.py
from fastapi import FastAPI, Depends, HTTPException, Request, Query
from fastapi.security import APIKeyHeader
from fastapi.middleware.cors import CORSMiddleware
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from typing import Optional
from datetime import date
import supabase
import hashlib
import os

# ── Config ─────────────────────────────────────────────────
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

db      = supabase.create_client(SUPABASE_URL, SUPABASE_KEY)
limiter = Limiter(key_func=get_remote_address)

app = FastAPI(
    title="BODACC Regulatory API",
    description="Accès structuré aux annonces légales françaises (BODACC A/B/C)",
    version="1.0.0",
)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["GET"])

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=True)


# ── Auth + usage tracking ───────────────────────────────────
def get_customer(api_key: str = Depends(api_key_header)):
    key_hash = hashlib.sha256(api_key.encode()).hexdigest()
    result   = db.table("api_keys").select("*").eq("key_hash", key_hash).single().execute()

    if not result.data:
        raise HTTPException(status_code=403, detail="Clé API invalide ou expirée")

    customer = result.data

    if customer["calls_this_month"] >= customer["monthly_limit"]:
        raise HTTPException(
            status_code=429,
            detail=f"Quota mensuel atteint ({customer['monthly_limit']} appels). Upgradez votre plan."
        )

    # Incrémente l'usage (pour Stripe metered billing)
    db.table("api_keys") \
      .update({"calls_this_month": customer["calls_this_month"] + 1}) \
      .eq("id", customer["id"]) \
      .execute()

    return customer


# ── Helpers ─────────────────────────────────────────────────
def quota_header(customer: dict) -> dict:
    remaining = max(0, customer["monthly_limit"] - customer["calls_this_month"] - 1)
    return {
        "X-RateLimit-Limit":     str(customer["monthly_limit"]),
        "X-RateLimit-Remaining": str(remaining),
    }


# ── Endpoints ───────────────────────────────────────────────

@app.get("/v1/bodacc/search", summary="Recherche par nom d'entreprise")
@limiter.limit("60/minute")
async def search_by_name(
    request:    Request,
    q:          str            = Query(..., description="Nom ou fragment du nom de l'entreprise"),
    event_type: Optional[str]  = Query(None, description="liquidation | creation | modification | vente_fonds"),
    dataset:    Optional[str]  = Query(None, description="bodacc-a | bodacc-b | bodacc-c"),
    since:      Optional[date] = Query(None, description="Date minimale de parution (YYYY-MM-DD)"),
    limit:      int            = Query(20, ge=1, le=100),
    customer = Depends(get_customer),
):
    query = db.table("bodacc_announcements") \
              .select("id,company_name,siren,event_type,tribunal,department,published_at,dataset") \
              .ilike("company_name", f"%{q}%") \
              .order("published_at", desc=True) \
              .limit(limit)

    if event_type: query = query.ilike("event_type", f"%{event_type}%")
    if dataset:    query = query.eq("dataset", dataset)
    if since:      query = query.gte("published_at", since.isoformat())

    result = query.execute()

    return {
        "query":   q,
        "count":   len(result.data),
        "results": result.data,
    }, quota_header(customer)


@app.get("/v1/bodacc/siren/{siren}", summary="Historique complet d'une entreprise par SIREN")
@limiter.limit("60/minute")
async def get_by_siren(
    request:  Request,
    siren:    str,
    limit:    int = Query(50, ge=1, le=200),
    customer = Depends(get_customer),
):
    if not siren.isdigit() or len(siren) != 9:
        raise HTTPException(status_code=422, detail="SIREN invalide — 9 chiffres attendus")

    result = db.table("bodacc_announcements") \
               .select("*") \
               .eq("siren", siren) \
               .order("published_at", desc=True) \
               .limit(limit) \
               .execute()

    if not result.data:
        raise HTTPException(status_code=404, detail=f"Aucune annonce trouvée pour le SIREN {siren}")

    return {
        "siren":   siren,
        "company": result.data[0]["company_name"],
        "count":   len(result.data),
        "history": result.data,
    }, quota_header(customer)


@app.get("/v1/bodacc/procedures", summary="Flux temps réel des procédures collectives")
@limiter.limit("30/minute")
async def get_procedures(
    request:    Request,
    since:      Optional[date] = Query(None, description="Depuis (défaut : 7 derniers jours)"),
    department: Optional[str]  = Query(None, description="Numéro de département (ex: 75)"),
    limit:      int            = Query(50, ge=1, le=200),
    customer = Depends(get_customer),
):
    from datetime import datetime, timedelta
    default_since = (datetime.utcnow() - timedelta(days=7)).strftime("%Y-%m-%d")
    since_str     = since.isoformat() if since else default_since

    query = db.table("bodacc_announcements") \
              .select("company_name,siren,event_type,tribunal,department,published_at") \
              .eq("dataset", "bodacc-c") \
              .gte("published_at", since_str) \
              .order("published_at", desc=True) \
              .limit(limit)

    if department:
        query = query.eq("department", department)

    result = query.execute()

    return {
        "since":   since_str,
        "count":   len(result.data),
        "results": result.data,
    }, quota_header(customer)


@app.get("/v1/bodacc/stats", summary="Statistiques agrégées par département et type")
@limiter.limit("20/minute")
async def get_stats(
    request:  Request,
    since:    Optional[date] = Query(None),
    customer = Depends(get_customer),
):
    from datetime import datetime, timedelta
    since_str = since.isoformat() if since else (datetime.utcnow() - timedelta(days=30)).strftime("%Y-%m-%d")

    result = db.rpc("bodacc_stats", {"since_date": since_str}).execute()

    return {"since": since_str, "stats": result.data}, quota_header(customer)


# ── Healthcheck (pas d'auth) ────────────────────────────────
@app.get("/health", include_in_schema=False)
async def health():
    return {"status": "ok"}
