"""
EU Partner Intel — FastAPI proxy
"""

import json
import httpx
from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse

app = FastAPI(title="EU Partner Intel Proxy")

SEDIA_URL = "https://api.tech.ec.europa.eu/search-api/prod/rest/search"

COUNTRY_MAP = {
    # Core EU member states
    "20000839": "BE", "20000908": "BG", "20000905": "CZ", "20000875": "DK",
    "20000879": "EE", "20000884": "FI", "20000890": "FR", "20000873": "DE",
    "20000902": "GR", "20000886": "HU", "20000887": "IE", "20000922": "IT",
    "20000894": "LV", "20000892": "LT", "20000895": "MT", "20000906": "NL",
    "20000897": "AT", "20000909": "PL", "20000898": "PT", "20000899": "RO",
    "20000901": "SI", "20000883": "ES", "20000903": "SE", "20000880": "EE",  # 20000880=EE confirmed
    "20000885": "FI",  # confirmed: AIEDU OY is Finnish
    # Non-EU Europe
    "20000904": "CH", "20000888": "IS", "20000896": "NO", "20000912": "GB",
    "20000913": "UA", "20000914": "RS", "20000910": "TR",
    "20001026": "TR",  # confirmed: FIRAT UNIVERSITESI is Turkish
    "20000825": "AL",
    "20000986": "PL",  # confirmed: TECH2MARKET is Polish
    "20000893": "GB",  # confirmed: 21C CONSULTANCY is UK
    "20000973": "NL",  # confirmed: 8D RESEARCH is Dutch
    "20000994": "RO",  # confirmed: ROHEALTH and EFFECTIVE DECISIONS are Romanian
    "20001001": "SE",  # confirmed: Intersectionality Lab is Swedish
    "31008860": "XK",  # Kosovo (non-standard ID in SEDIA)
    # Others
    "20000907": "CY", "20000871": "CY", "20000841": "BG", "20000919": "IL", "20000920": "MA",
    "20000878": "HR", "20000882": "HU", "20000891": "LV", "20000900": "RO",
    "20000872": "CZ", "20000830": "BA", "20000876": "EE", "20000877": "FI",
    "20000990": "PT", "20000832": "AT",
}

ORG_TYPE_MAP = {
    "31079048": "SME", "31079049": "SME",
    "31079050": "University/Higher Education",
    "31079051": "Research Centre",
    "31079052": "Large Enterprise",
    "31079053": "Public Authority",
    "31079054": "Other", "31079055": "NGO",
    "31079056": "International Org",
    "31079047": "Non-profit Organisation",
}

HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json, text/plain, */*",
    "Origin": "https://ec.europa.eu",
    "Referer": "https://ec.europa.eu/",
}


def extract(meta: dict, key: str) -> str:
    val = meta.get(key, [])
    return val[0] if val else ""


def normalize_partner(hit: dict, topic_id: str) -> dict:
    meta = hit.get("metadata", {})
    pic = extract(meta, "pic")
    country_id = extract(meta, "country")
    org_type_id = extract(meta, "organisationType")
    keywords = meta.get("keywords", [])

    public_projects_raw = extract(meta, "publicProjects")
    programs = []
    try:
        projects = json.loads(public_projects_raw) if public_projects_raw else []
        programs = list({p.get("program", {}).get("abbreviation", "") for p in projects if isinstance(p, dict)})
        programs = [p for p in programs if p]
    except Exception:
        pass

    # sedia_description: keywords autodichiarati del profilo SEDIA dell'org
    # NOTA: la descrizione testuale dell'annuncio (quella visibile nel portale)
    # è disponibile via FT-Announcements endpoint separato, non in SEDIA_PERSON
    raw_keywords = meta.get("keywords") or []
    sedia_description = ", ".join(raw_keywords[:20]) if raw_keywords else ""

    return {
        "legal_name":           extract(meta, "name") or hit.get("summary", ""),
        "pic_number":           pic,
        "city":                 extract(meta, "city"),
        "country":              COUNTRY_MAP.get(country_id, country_id),
        "organization_type":    ORG_TYPE_MAP.get(org_type_id, org_type_id),
        "sedia_description":    sedia_description,
        "keywords":             keywords,
        "all_active_calls_count": len(meta.get("topics", [])),
        "topics_active":        meta.get("topics", []),
        "projects_count":       extract(meta, "noOfProjects"),
        "programs":             programs,
        "open_calls_interest":  topic_id,
        "cordis_url":           f"https://ec.europa.eu/info/funding-tenders/opportunities/portal/screen/how-to-participate/org-details/{pic}" if pic else "",
    }


@app.get("/")
def root():
    return {"status": "ok", "service": "EU Partner Intel Proxy"}


@app.get("/partners")
async def get_partners(
    topic_id: str = Query(..., description="Es: HORIZON-INFRA-2026-01-EOSC-01"),
    country: str = Query("", description="Filtra per paese ISO, es: DE"),
):
    """
    Recupera TUTTI i partner per il topic_id, paginando automaticamente SEDIA
    e deduplicando per pic_number. Ritorna solo i partner con topic_id nel campo topics.
    """
    exact_query = f'"{topic_id}"'
    seen_pics = set()
    partners = []
    page = 1
    page_size = 50

    async with httpx.AsyncClient(timeout=30.0) as client:
        while True:
            r = await client.post(
                SEDIA_URL,
                params={
                    "apiKey": "SEDIA_PERSON",
                    "text": exact_query,
                    "pageSize": page_size,
                    "pageNumber": page,
                },
                json={},
                headers=HEADERS,
            )
            if r.status_code != 200:
                break

            data = r.json()
            hits = data.get("results") or data.get("hits") or data.get("items") or []
            total = data.get("totalResults") or data.get("total") or 0

            if not hits:
                break

            for hit in hits:
                meta = hit.get("metadata", {})
                topics_field = meta.get("topics") or []
                if topic_id not in topics_field:
                    continue

                # Deduplica per pic_number
                pic = (meta.get("pic") or [""])[0]
                dedup_key = pic if pic else (meta.get("name") or [""])[0]
                if dedup_key in seen_pics:
                    continue
                seen_pics.add(dedup_key)

                partner = normalize_partner(hit, topic_id)
                if country and partner["country"].upper() != country.upper():
                    continue
                partners.append(partner)

            # Smetti di paginare se abbiamo esaurito i risultati
            if page * page_size >= total or len(hits) < page_size:
                break
            page += 1

    return {
        "topic_id": topic_id,
        "total_unique": len(partners),
        "pages_fetched": page,
        "partners": partners,
    }


@app.get("/org")
async def get_org_track_record(
    pic: str = Query("", description="PIC number a 9 cifre"),
    name: str = Query("", description="Nome organizzazione"),
):
    """
    Track record da SEDIA_PERSON.
    NOTA: SEDIA indicizza per nome (full-text), non per PIC.
    Se viene passato solo pic, restituisce il link diretto senza dati di profilo.
    Se viene passato name (o name+pic), cerca per nome e arricchisce con i dati.
    """
    import json as _json

    # Se abbiamo solo il PIC senza nome, restituiamo almeno il link diretto
    if pic and not name:
        return {
            "pic": pic,
            "portal_url": f"https://ec.europa.eu/info/funding-tenders/opportunities/portal/screen/how-to-participate/org-details/{pic}",
            "note": "Per ottenere il profilo completo chiama /org?pic={pic}&name=NOME_LEGALE oppure /org?name=NOME_LEGALE",
        }

    if not name:
        return JSONResponse(status_code=400, content={"error": "Fornire almeno name= per la ricerca profilo"})

    # Ricerca per nome su SEDIA (funziona perché il nome è full-text)
    search_text = f'"{name}"' if len(name) > 6 else name

    async with httpx.AsyncClient(timeout=20.0) as client:
        r = await client.post(
            SEDIA_URL,
            params={"apiKey": "SEDIA_PERSON", "text": search_text, "pageSize": 10, "pageNumber": 1},
            json={},
            headers=HEADERS,
        )

    if r.status_code != 200:
        return JSONResponse(status_code=r.status_code, content={"error": r.text[:300]})

    hits = r.json().get("results") or r.json().get("hits") or []

    # Trova il match migliore
    match = None
    for hit in hits:
        meta = hit.get("metadata", {})
        hit_pic = (meta.get("pic") or [""])[0]
        hit_name = (meta.get("name") or [""])[0].lower()
        # Match esatto per PIC se disponibile
        if pic and hit_pic == pic:
            match = hit
            break
        # Match per nome
        if name.lower() in hit_name or hit_name in name.lower():
            match = hit
            break

    # Fallback al primo risultato
    if not match and hits:
        match = hits[0]

    if not match:
        # Anche senza profilo SEDIA, restituiamo almeno il link se abbiamo il PIC
        base = {"searched_name": name, "note": "Profilo non trovato in SEDIA"}
        if pic:
            base["pic"] = pic
            base["portal_url"] = f"https://ec.europa.eu/info/funding-tenders/opportunities/portal/screen/how-to-participate/org-details/{pic}"
        return JSONResponse(status_code=404, content=base)

    meta = match.get("metadata", {})
    pic_found = (meta.get("pic") or [""])[0] or pic

    public_projects_raw = (meta.get("publicProjects") or [""])[0]
    projects = []
    try:
        projects = _json.loads(public_projects_raw) if public_projects_raw else []
    except Exception:
        pass

    programs = set()
    for p in projects:
        prog = p.get("program", {}).get("abbreviation", "")
        if prog:
            programs.add(prog)

    total_projects = int((meta.get("noOfProjects") or ["0"])[0] or 0)
    country_id = (meta.get("country") or [""])[0]

    return {
        "pic":               pic_found,
        "legal_name":        (meta.get("name") or [""])[0],
        "country":           COUNTRY_MAP.get(country_id, country_id),
        "organization_type": ORG_TYPE_MAP.get((meta.get("organisationType") or [""])[0], ""),
        "projects_total":    total_projects,
        "programs":          sorted(programs),
        "recent_projects": [
            {
                "acronym": p.get("acronym", ""),
                "title":   p.get("title", ""),
                "program": p.get("program", {}).get("abbreviation", ""),
                "call":    p.get("call", {}).get("abbreviation", ""),
                "status":  p.get("phase", ""),
            }
            for p in projects[:5]
        ],
        "portal_url":     f"https://ec.europa.eu/info/funding-tenders/opportunities/portal/screen/how-to-participate/org-details/{pic_found}",
        "sedia_keywords": meta.get("keywords", [])[:10],
    }


@app.get("/debug-raw")
async def debug_raw(topic_id: str = Query(...), page_size: int = Query(5)):
    exact_query = f'"{topic_id}"'
    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.post(
            SEDIA_URL,
            params={"apiKey": "SEDIA_PERSON", "text": exact_query, "pageSize": page_size, "pageNumber": 1},
            json={}, headers=HEADERS,
        )
    data = r.json()
    hits = data.get("results") or []
    return {
        "status": r.status_code,
        "total": data.get("totalResults") or data.get("total"),
        "hits": [
            {
                "summary":       h.get("summary"),
                "name":          h.get("metadata", {}).get("name"),
                "pic":           h.get("metadata", {}).get("pic"),
                "topics":        h.get("metadata", {}).get("topics"),
                "hasPartnerSearch": h.get("metadata", {}).get("hasPartnerSearch"),
                "country":       h.get("metadata", {}).get("country"),
                "orgType":       h.get("metadata", {}).get("organisationType"),
                "keywords_topics": h.get("metadata", {}).get("keywords", []),
            }
            for h in hits
        ],
    }
