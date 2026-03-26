# generate_key.py — script one-shot pour créer un client
import hashlib, secrets, supabase, os

db  = supabase.create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])
key = "sk_live_" + secrets.token_urlsafe(32)

db.table("api_keys").insert({
    "key_hash":      hashlib.sha256(key.encode()).hexdigest(),
    "customer_name": "Cabinet Dupont & Associés",
    "email":         "tech@dupont.fr",
    "plan":          "pro",
    "monthly_limit": 10000,
}).execute()

print(f"Clé générée (à envoyer au client) : {key}")
# → sk_live_Xk9mP2...  (jamais stockée en clair dans la DB)
