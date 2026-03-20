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
    "20000873": "DE", "20000890": "FR", "20000922": "IT", "20000883": "ES",
    "20000839": "BE", "20000875": "DK", "20000884": "FI", "20000906": "NL",
    "20000909": "PL", "20000879": "EE", "20000880": "GR", "20000885": "HR",
    "20000886": "HU", "20000887": "IE", "20000888": "IS", "20000892": "LT",
    "20000893": "LU", "20000894": "LV", "20000895": "MT", "20000896": "NO",
    "20000897": "AT", "20000898": "PT", "20000899": "RO", "20000901": "SI",
    "20000902": "SK", "20000903": "SE", "20000904": "CH", "20000905": "CZ",
    "20000907": "CY", "20000908": "BG", "20000910": "TR", "20000912": "GB",
    "20000913": "UA", "20000914": "RS", "20000919": "IL", "20000920": "MA",
    "20000825": "AL", "20000832": "AT", "20000871": "CY", "20000878": "HR",
    "20000881": "GR", "20000882": "HU", "20000891": "LV", "20000900": "RO",
    # Extra IDs found in real data
    "20000841": "BG", "20000872": "CZ", "20000986": "XK", "20000973": "TR",
    "20000994": "PT", "20001026": "KS", "20001001": "ME", "20000990": "PT",
    "20000876": "EE", "20000877": "FI", "20000830": "BA",
}

ORG_TYPE_MAP = {
    "31079048": "SME", "31079049": "SME",
    "31079050": "University/Higher Education",
    "31079051": "Research Centre",
    "31079052": "Large Enterprise",
    "31079053": "Public Authority",
    "31079054": "Other", "31079055": "NGO",
    "31079056": "International Org",
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
        "_raw_country_id":      country_id,
        "sedia_description":    sedia_description,
        "keywords":             keywords,
        "all_active_calls_count": len(meta.get("topics", [])),
        "topics_active":        meta.get("topics", []),
        "projects_count":       extract(meta, "noOfProjects"),
        "programs":             programs,
        "open_calls_interest":  topic_id,
        "cordis_url":           f"https://cordis.europa.eu/search/result_en?q=contenttype%3Dproject+AND+relations%2Forganisations%2Fpic%3D{pic}" if pic else "",
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
    if not pic and not name:
        return JSONResponse(status_code=400, content={"error": "Fornire pic o name"})

    q = f"contenttype=project AND relations/organisations/pic={pic}" if pic else f'contenttype=project AND relations/organisations/legalName="{name}"'

    async with httpx.AsyncClient(timeout=20.0) as client:
        r = await client.get("https://cordis.europa.eu/api/search", params={"q": q, "p": 1, "num": 200, "format": "json"})

    if r.status_code != 200:
        return JSONResponse(status_code=r.status_code, content={"error": r.text[:300]})

    data = r.json()
    projects = data.get("results", {}).get("result", [])
    if isinstance(projects, dict):
        projects = [projects]

    total = data.get("results", {}).get("totalCount", len(projects))
    coordinator_count = 0
    total_budget = 0.0
    programs = set()

    for p in projects:
        proj = p.get("project", p)
        if proj.get("frameworkProgramme"):
            programs.add(proj["frameworkProgramme"])
        try:
            total_budget += float(proj.get("ecMaxContribution", 0))
        except (ValueError, TypeError):
            pass
        orgs = proj.get("relations", {}).get("organizations", {}).get("organization", [])
        if isinstance(orgs, dict):
            orgs = [orgs]
        for org in orgs:
            if str(org.get("pic", "")) == str(pic) and org.get("role", "").upper() in ["COORDINATOR", "COORD"]:
                coordinator_count += 1

    return {
        "pic": pic, "name": name,
        "projects_total": total,
        "coordinator_count": coordinator_count,
        "total_ec_budget_meur": round(total_budget / 1_000_000, 2),
        "programs": sorted(programs),
        "cordis_url": f"https://cordis.europa.eu/search/result_en?q=contenttype%3Dproject+AND+relations%2Forganisations%2Fpic%3D{pic}",
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
