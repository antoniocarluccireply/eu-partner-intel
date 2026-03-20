"""
EU Partner Intel — FastAPI proxy
Traduce chiamate GET semplici (da Neurons) in POST verso SEDIA.
Deploy: Railway, zero config.
"""

import httpx
from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse

app = FastAPI(title="EU Partner Intel Proxy")

SEDIA_URL = "https://api.tech.ec.europa.eu/search-api/prod/rest/search"

ORG_TYPE = {
    "HES": "University",
    "REC": "Research Centre",
    "PRC": "Large Enterprise",
    "SME": "SME",
    "PUB": "Public Authority",
    "OTH": "Other",
}


@app.get("/")
def root():
    return {"status": "ok", "service": "EU Partner Intel Proxy"}


@app.get("/partners")
async def get_partners(
    topic_id: str = Query(..., description="Es: HORIZON-INFRA-2026-01-EOSC-01"),
    page_size: int = Query(50, le=100),
    page_number: int = Query(1),
    country: str = Query("", description="Filtra per paese ISO, es: DE"),
):
    """
    Ritorna tutti gli annunci partner pubblicati su F&T Portal per un topic.
    Chiama POST SEDIA_PERSON e restituisce JSON normalizzato.
    """

    # Body JSON — se SEDIA risponde 400 passiamo al fallback protobuf
    body = {
        "query": topic_id,
        "pageSize": page_size,
        "pageNumber": page_number,
    }

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json, text/plain, */*",
        "Origin": "https://ec.europa.eu",
        "Referer": "https://ec.europa.eu/",
    }

    params = {
        "apiKey": "SEDIA_PERSON",
        "text": "***",
        "pageSize": page_size,
        "pageNumber": page_number,
    }

    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.post(SEDIA_URL, params=params, json=body, headers=headers)

    if r.status_code != 200:
        return JSONResponse(
            status_code=r.status_code,
            content={
                "error": f"SEDIA returned {r.status_code}",
                "detail": r.text[:500],
                "hint": "Il body protobuf potrebbe essere necessario — vedi /debug",
            },
        )

    data = r.json()

    # Normalizza i risultati
    raw_hits = (
        data.get("results")
        or data.get("hits")
        or data.get("items")
        or data.get("content")
        or []
    )

    partners = []
    for hit in raw_hits:
        meta = hit.get("metadata", hit)
        org = meta.get("organisation", {}) if isinstance(meta, dict) else {}

        legal_name = (
            hit.get("organisationName")
            or hit.get("legalName")
            or org.get("legalName", "")
        )
        pic = str(hit.get("pic") or hit.get("picNumber") or org.get("pic", ""))
        country_code = hit.get("country") or org.get("country", {}).get("isoCode", "") if isinstance(org.get("country"), dict) else org.get("country", "")
        org_type_raw = hit.get("organisationType") or org.get("activityType", "")
        description = hit.get("description") or hit.get("expertiseDescription") or meta.get("description", "")
        keywords = hit.get("keywords") or hit.get("tags") or []
        ann_type = hit.get("announcementType") or hit.get("type", "")
        pub_date = hit.get("publicationDate") or hit.get("creationDate", "")
        contact_name = hit.get("contactName", "")
        contact_email = hit.get("contactEmail", "")

        # Salta se filtro paese attivo
        if country and country_code.upper() != country.upper():
            continue

        partners.append({
            "legal_name": legal_name,
            "pic_number": pic,
            "country": country_code,
            "organization_type": ORG_TYPE.get(org_type_raw, org_type_raw),
            "announcement_type": ann_type,          # OFFER o REQUEST
            "description": description,
            "keywords": keywords if isinstance(keywords, list) else [keywords],
            "contact_name": contact_name,
            "contact_email": contact_email,
            "publication_date": pub_date,
            "cordis_url": f"https://cordis.europa.eu/search/result_it?q=contenttype%3Dproject+AND+relations%2Forganisations%2Fpic%3D{pic}" if pic else "",
            "open_calls_interest": topic_id,
        })

    return {
        "topic_id": topic_id,
        "total_in_sedia": data.get("totalResults") or data.get("total") or len(partners),
        "returned": len(partners),
        "page_number": page_number,
        "partners": partners,
    }


@app.get("/org")
async def get_org_track_record(
    pic: str = Query("", description="PIC number a 9 cifre"),
    name: str = Query("", description="Nome organizzazione (alternativo al PIC)"),
):
    """
    Track record da CORDIS: progetti vinti, budget totale, ruolo coordinator.
    """
    if not pic and not name:
        return JSONResponse(status_code=400, content={"error": "Fornire pic o name"})

    if pic:
        q = f"contenttype=project AND relations/organisations/pic={pic}"
    else:
        q = f'contenttype=project AND relations/organisations/legalName="{name}"'

    async with httpx.AsyncClient(timeout=20.0) as client:
        r = await client.get(
            "https://cordis.europa.eu/api/search",
            params={"q": q, "p": 1, "num": 200, "format": "json"},
        )

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
        framework = proj.get("frameworkProgramme", "")
        if framework:
            programs.add(framework)
        try:
            total_budget += float(proj.get("ecMaxContribution", 0))
        except (ValueError, TypeError):
            pass

        orgs = proj.get("relations", {}).get("organizations", {}).get("organization", [])
        if isinstance(orgs, dict):
            orgs = [orgs]
        for org in orgs:
            match = str(org.get("pic", "")) == str(pic) or org.get("legalName", "").lower() == name.lower()
            if match and org.get("role", "").upper() in ["COORDINATOR", "COORD"]:
                coordinator_count += 1

    return {
        "pic": pic,
        "name": name,
        "horizon_projects_total": total,
        "coordinator_count": coordinator_count,
        "total_ec_budget_meur": round(total_budget / 1_000_000, 2),
        "programs": sorted(programs),
        "cordis_url": f"https://cordis.europa.eu/search/result_it?q=contenttype%3Dproject+AND+relations%2Forganisations%2Fpic%3D{pic}",
    }


@app.get("/count")
async def get_count(topic_id: str = Query(...)):
    """
    Contatore rapido annunci da FT-Announcements (lightweight).
    """
    async with httpx.AsyncClient(timeout=10.0) as client:
        r = await client.get(
            "https://api.sedia-backoffice-production.eu/public/ehelp/module/FT-Announcements",
            params={"topicId": topic_id},
        )
    return {"topic_id": topic_id, "raw_response": r.json() if r.status_code == 200 else r.text}
