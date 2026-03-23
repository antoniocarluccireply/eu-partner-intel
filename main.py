"""
EU Partner Intel -- FastAPI proxy
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
    # ? disponibile via FT-Announcements endpoint separato, non in SEDIA_PERSON
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
            f"note": f"Per ottenere il profilo completo chiama /org?pic={pic}&name=NOME_LEGALE oppure /org?name=NOME_LEGALE",
        }

    if not name:
        return JSONResponse(status_code=400, content={"error": "Fornire almeno name= per la ricerca profilo"})

    # Ricerca per nome su SEDIA (funziona perch? il nome ? full-text)
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






# ---- Budget/description helpers ported from euft ----

def _euft_safe_json(raw):
    if raw is None: return None
    if isinstance(raw, (dict, list)): return raw
    if not isinstance(raw, str): raw = str(raw)
    raw = raw.strip()
    if not raw: return None
    try:
        import json as _j
        return _j.loads(raw)
    except Exception:
        return None

def _euft_first_text(value):
    if value is None: return ""
    if isinstance(value, str): return value.strip()
    if isinstance(value, (int, float)): return str(value).strip()
    if isinstance(value, list):
        for item in value:
            t = _euft_first_text(item)
            if t: return t
        return ""
    if isinstance(value, dict):
        for k in ("value", "label", "name", "title", "text"):
            if k in value:
                t = _euft_first_text(value[k])
                if t: return t
        for v in value.values():
            t = _euft_first_text(v)
            if t: return t
        return ""
    return str(value).strip()

def _euft_safe_float(v):
    if v is None: return None
    if isinstance(v, (int, float)): return float(v)
    try:
        s = str(v).strip().replace(" ", "").replace("\u00A0", "")
        return float(s) if s else None
    except Exception:
        return None

def _euft_budget_overviews(md):
    raw = md.get("budgetOverview")
    parsed = _euft_safe_json(raw)
    if isinstance(parsed, list):
        out = []
        for item in parsed:
            if isinstance(item, dict): out.append(item)
            elif isinstance(item, str):
                p = _euft_safe_json(item)
                if isinstance(p, dict): out.append(p)
        return out
    if isinstance(parsed, dict): return [parsed]
    return []

def _euft_actions(md):
    raw = md.get("actions")
    parsed = _euft_safe_json(raw)
    out = []
    if isinstance(parsed, dict): return [parsed]
    if isinstance(parsed, list):
        for item in parsed:
            if isinstance(item, dict): out.append(item)
            elif isinstance(item, str):
                p = _euft_safe_json(item)
                if isinstance(p, list): out.extend([x for x in p if isinstance(x, dict)])
                elif isinstance(p, dict): out.append(p)
        return out
    return []

def _euft_extract_budget(md, topic_id):
    """Extract min_meur, max_meur from budgetOverview or actions.
    
    Strategy:
    1. Try exact topic_id match in budgetTopicActionMap
    2. If not found or budget seems per-topic (very small), sum all entries in the call
    3. Fallback to actions expectedGrant
    """
    min_meur = None
    max_meur = None

    for overview in _euft_budget_overviews(md):
        topic_map = overview.get("budgetTopicActionMap")
        if not isinstance(topic_map, dict): continue

        # Collect ALL entries across all topic_ids in this call
        all_entries = []
        matched_entry = None

        for _tid, entries in topic_map.items():
            if not isinstance(entries, list): continue
            for entry in entries:
                if not isinstance(entry, dict): continue
                all_entries.append(entry)
                action_full = str(entry.get("action") or "").strip()
                action_code = action_full.split(" - ", 1)[0].strip()
                if topic_id and action_code and (
                    action_code == topic_id or
                    topic_id.startswith(action_code + "-") or
                    action_code.startswith(topic_id + "-")
                ):
                    matched_entry = entry

        # Try matched entry first
        entry_to_use = matched_entry or (all_entries[0] if all_entries else None)
        if entry_to_use:
            mn = _euft_safe_float(entry_to_use.get("minContribution"))
            mx = _euft_safe_float(entry_to_use.get("maxContribution"))
            if mn is not None: min_meur = mn / 1_000_000
            if mx is not None: max_meur = mx / 1_000_000

            # Try budgetYearMap
            if not min_meur and not max_meur:
                bym = entry_to_use.get("budgetYearMap")
                if isinstance(bym, dict):
                    for yr in ["2026", "2025", "2027", "2024"]:
                        yval = _euft_safe_float(bym.get(yr))
                        if yval:
                            min_meur = max_meur = yval / 1_000_000
                            break

            # If budget found but seems per-topic (< 20M for Horizon), 
            # try summing all entries to get call-level budget
            if min_meur is not None and min_meur < 20 and len(all_entries) > 1 and not matched_entry:
                total = 0
                for e in all_entries:
                    mx_e = _euft_safe_float(e.get("maxContribution"))
                    if mx_e: total += mx_e / 1_000_000
                if total > min_meur:
                    min_meur = max_meur = total

            if min_meur is not None or max_meur is not None:
                return min_meur, max_meur

    # Fallback: actions -> expectedGrant
    for action in _euft_actions(md):
        eg = _euft_safe_float(action.get("expectedGrant"))
        if eg is not None:
            return eg / 1_000_000, eg / 1_000_000

    return None, None

def _euft_extract_description(root, md):
    """Extract description/destination from SEDIA hit."""
    dest = (
        _euft_first_text(root.get("destinationDescription")) or
        _euft_first_text(root.get("destinationGroup")) or
        _euft_first_text(md.get("destinationDescription")) or
        _euft_first_text(md.get("destinationGroup")) or
        _euft_first_text(md.get("topicConditions")) or
        ""
    )
    return dest[:500] if dest else ""

def _euft_extract_programme_division(md):
    """Extract programme division / focus area."""
    return (
        _euft_first_text(md.get("programmeDivision")) or
        _euft_first_text(md.get("focusArea")) or
        ""
    )


import re as _re_html

def _strip_html(text):
    """Remove HTML tags and decode HTML entities from text."""
    if not text:
        return ""
    import html as _html_mod
    # Decode HTML entities first (&gt; -> >, &amp; -> &, etc.)
    decoded = _html_mod.unescape(str(text))
    # Remove HTML tags
    clean = _re_html.sub(r"<[^>]+>", " ", decoded)
    # Collapse whitespace
    clean = _re_html.sub(r"\s+", " ", clean).strip()
    # Remove leading punctuation artifacts like "> " at start
    clean = _re_html.sub(r'^[">\s]+', "", clean).strip()
    return clean[:500]

# SEDIA type_of_action numeric ID -> human readable
TYPE_OF_ACTION_MAP = {
    # EDF
    "44175699": "DA",
    "44175709": "RA",
    # Horizon Europe (confirmed from portal)
    "43027846": "RIA",       # Research & Innovation Action
    "43027847": "IA",        # Innovation Action
    "43027848": "CSA",       # Coordination & Support Action
    "43027849": "RIA",       # confirmed: HORIZON-CL3-2026-02-CS-ECCC-01
    "43027850": "COFUND",
    "43027851": "ERC",
    "43027852": "MSCA",
    "43027853": "PRIZE",
    "43027854": "PCP",
    "43027855": "PPI",
    "43027856": "IA",
    "43027857": "LUMP",
    # Legacy codes
    "31094902": "RIA",
    "31094903": "IA",
    "31094904": "CSA",
    "31094905": "COFUND",
    "31094906": "ERC",
    "44175700": "RIA",
    "44175701": "IA",
    "44175702": "CSA",
    "44175703": "COFUND",
    "44175704": "ERC",
    "44175710": "SME",
    "44175711": "PRIZE",
    "44175712": "LUMP",
}

# SEDIA programme_division numeric ID -> human readable (partial)
PROG_DIVISION_MAP = {
    "44181033": "EDF-DA",     # EDF Development Actions
    "44181034": "EDF-RA",     # EDF Research Actions
    "43298664": "AGRIP",
    "43251814": "CREA-MEDIA",
}

# ---- End helpers ----

@app.get("/calls")
async def search_calls(
    keywords: str = Query("", description="Parole chiave nel topic_id: INFRA, DATA, CYBER, TWIN, 2026"),
    programme: str = Query("", description="Programma: HORIZON, EDF, DIGITAL, CEF, LIFE, CREA, ERASMUS"),
    cluster: str = Query("", description="Cluster Horizon: CL3, CL4, CL5, INFRA, HLTH"),
    status: str = Query("open", description="open | forthcoming | all"),
    deadline_after: str = Query("", description="Filtra deadline dopo questa data: YYYY-MM-DD, es: 2026-05-01"),
    deadline_before: str = Query("", description="Filtra deadline prima di questa data: YYYY-MM-DD, es: 2026-08-31"),
    page_size: int = Query(20, le=100),
    page_number: int = Query(1),
):
    """
    Cerca call EU aperte/future con title e deadline reali via SEDIA (type=1, multipart).
    Filtra per programme sul prefisso del topic_id (identifier field).
    """
    import uuid as _uuid
    import urllib.parse as _urlparse
    import json as _json

    STATUS_OPEN        = "31094502"
    STATUS_FORTHCOMING = "31094501"

    if status == "open":
        status_terms = [STATUS_OPEN]
    elif status == "forthcoming":
        status_terms = [STATUS_FORTHCOMING]
    else:
        status_terms = [STATUS_OPEN, STATUS_FORTHCOMING]

    # Query DSL — type=1 = grants/calls, confermato dal debug
    query_obj  = {"bool": {"must": [
        {"terms": {"type": ["1"]}},
        {"terms": {"status": status_terms}},
    ]}}
    languages_obj = ["en"]
    sort_obj   = [{"field": "identifier", "order": "ASC"}]

    prog_upper    = programme.upper().strip() if programme else ""
    cluster_upper = cluster.upper().strip() if cluster else ""
    kw_tokens     = [k.strip() for k in keywords.upper().split()] if keywords else []

    collected = []
    seen_ids  = set()
    api_page  = 1
    total_api = None

    # If deadline filters are active, fetch ALL pages internally (ignore pagination)
    # so the agent gets complete results in one call
    fetch_all = bool(deadline_after or deadline_before or not programme)
    FETCH_LIMIT = 20  # max API pages (1000 calls max)

    while api_page <= FETCH_LIMIT:
        boundary = f"----euft-{_uuid.uuid4().hex}"
        chunks = []
        for fname, (fn, fval, fct) in {
            "query":     ("blob", _json.dumps(query_obj),     "application/json"),
            "languages": ("blob", _json.dumps(languages_obj), "application/json"),
            "sort":      ("blob", _json.dumps(sort_obj),      "application/json"),
        }.items():
            chunks.append(f"--{boundary}\r\n".encode())
            chunks.append(f'Content-Disposition: form-data; name="{fname}"; filename="{fn}"\r\nContent-Type: {fct}\r\n\r\n'.encode())
            chunks.append(fval.encode())
            chunks.append(b"\r\n")
        chunks.append(f"--{boundary}--\r\n".encode())
        body = b"".join(chunks)

        params = {"pageSize": "50", "pageNumber": str(api_page), "text": "***", "apiKey": "SEDIA"}
        url = "https://api.tech.ec.europa.eu/search-api/prod/rest/search?" + _urlparse.urlencode(params)

        async with httpx.AsyncClient(timeout=25.0) as client:
            r = await client.post(url, content=body, headers={
                "Content-Type": f"multipart/form-data; boundary={boundary}",
                "Accept": "application/json",
                "Origin": "https://ec.europa.eu",
            })

        if r.status_code != 200:
            return JSONResponse(status_code=r.status_code, content={"error": r.text[:300]})

        data = r.json()
        if total_api is None:
            total_api = data.get("totalResults") or 0

        hits = data.get("results") or []
        if not hits:
            break

        for hit in hits:
            meta = hit.get("metadata", {}) if isinstance(hit.get("metadata"), dict) else {}

            # topic_id viene dall'identifier field (es. HORIZON-CL4-2026-04-DATA-06)
            ident_raw = meta.get("identifier") or []
            topic_id  = (ident_raw[0] if isinstance(ident_raw, list) and ident_raw else str(ident_raw)).strip().upper()
            if not topic_id or topic_id in seen_ids:
                continue
            seen_ids.add(topic_id)

            # Filtro programme: prefisso del topic_id
            if prog_upper and not topic_id.startswith(prog_upper + "-"):
                continue
            # Filtro cluster: sottostringa con trattini
            if cluster_upper and f"-{cluster_upper}-" not in topic_id:
                continue
            # Filtro keywords: AND logic
            if kw_tokens and not all(tok in topic_id for tok in kw_tokens):
                continue

            # Title
            title = hit.get("title") or hit.get("summary") or topic_id

            # Deadline
            dl_raw = meta.get("deadlineDate") or []
            deadline = (dl_raw[0] if isinstance(dl_raw, list) and dl_raw else str(dl_raw)).strip()
            # Normalize: 2026-04-23T00:00:00.000+0000 -> 2026-04-23
            if deadline and "T" in deadline:
                deadline = deadline.split("T")[0]

            # Call identifier (parent call)
            call_id_raw = meta.get("callIdentifier") or []
            call_id = (call_id_raw[0] if isinstance(call_id_raw, list) and call_id_raw else "").strip()

            # Deadline range filter
            if deadline_after and deadline:
                if deadline < deadline_after:
                    continue
            if deadline_before and deadline:
                if deadline > deadline_before:
                    continue

            # Extract additional metadata fields
            def _first(lst, default=""):
                if isinstance(lst, list) and lst:
                    v = lst[0]
                    return str(v).strip() if v is not None else default
                if lst is not None and not isinstance(lst, list):
                    return str(lst).strip()
                return default

            call_title      = _first(meta.get("callTitle"))
            type_raw = _first(meta.get("typeOfMGAs"))
            type_of_action = TYPE_OF_ACTION_MAP.get(type_raw, "")
            # EDF: refine DA vs RA from call_id pattern
            if type_of_action in ("DA", "") and call_id:
                if "-RA-" in call_id or call_id.endswith("-RA") or "-LS-RA-" in call_id:
                    type_of_action = "RA"
                elif "-DA-" in call_id or call_id.endswith("-DA"):
                    type_of_action = "DA"
            # Fallback: extract from title (e.g. "Open Internet Stack (RIA)")
            if not type_of_action or type_of_action == type_raw:
                import re as _re_type
                m = _re_type.search(r"\((RIA|IA|CSA|COFUND|PRIZE|ERC|MSCA|DA|RA|PCP|PPI)\)", title or "")
                if m:
                    type_of_action = m.group(1)
            if not type_of_action:
                type_of_action = type_raw or ""
            keywords_raw    = meta.get("keywords") or []
            keywords_list   = [
                str(k).strip() for k in keywords_raw
                if k and not str(k).strip().startswith(("HORIZON-","EDF-","DIGITAL-","ERASMUS-","CREA-","CERV-","CEF-"))
            ][:15]
            cross_cutting   = meta.get("crossCuttingPriorities") or []
            cross_list      = [str(c).strip() for c in cross_cutting if c]
            prog_period     = _first(meta.get("programmePeriod"))
            ccm2id          = _first(meta.get("ccm2Id"))
            topic_cond_raw  = meta.get("topicConditions") or []
            topic_conditions = [_strip_html(str(t)) for t in topic_cond_raw if t][:3]

            # primary_url from root or construct from topic_id
            primary_url = (
                _euft_first_text(hit.get("url") or hit.get("link")) or
                f"https://ec.europa.eu/info/funding-tenders/opportunities/portal/screen/opportunities/topic-details/{topic_id}"
            )

            # publication_date from budgetEntry (already computed in _euft_extract_budget)
            # Re-extract for publication date
            pub_date = ""
            for overview in _euft_budget_overviews(meta):
                topic_map = overview.get("budgetTopicActionMap")
                if not isinstance(topic_map, dict): continue
                for _tid, entries in topic_map.items():
                    if not isinstance(entries, list): continue
                    for entry in entries:
                        if not isinstance(entry, dict): continue
                        pd = _euft_first_text(entry.get("plannedOpeningDate"))
                        if pd:
                            pub_date = pd[:10] if "T" not in pd else pd.split("T")[0]
                            break
                    if pub_date: break
                if pub_date: break
            if not pub_date:
                for action in _euft_actions(meta):
                    pd = _euft_first_text(action.get("plannedOpeningDate"))
                    if pd:
                        pub_date = pd[:10] if "T" not in pd else pd.split("T")[0]
                        break

            # Budget extraction
            min_meur, max_meur = _euft_extract_budget(meta, topic_id)
            if max_meur is not None and min_meur is not None and min_meur != max_meur:
                budget_meur = f"{min_meur:.1f}-{max_meur:.1f}M EUR/progetto"
            elif max_meur is not None:
                budget_meur = f"{max_meur:.1f}M EUR/progetto"
            elif min_meur is not None:
                budget_meur = f"{min_meur:.1f}M EUR/progetto"
            else:
                budget_meur = ""

            # Description and programme division
            description = _strip_html(_euft_extract_description(hit, meta))
            prog_division_raw = _euft_extract_programme_division(meta)
            prog_division = PROG_DIVISION_MAP.get(prog_division_raw, prog_division_raw)

            # TRL: extract from description or topic_conditions (after description is defined)
            import re as _re_trl
            trl = ""
            for src in [description, " ".join(topic_conditions)]:
                m = _re_trl.search(r"TRL\s*([3-9]|[3-9]\s*[-–]\s*[4-9])", src or "", _re_trl.IGNORECASE)
                if m:
                    trl = "TRL " + m.group(1).replace(" ", "")
                    break

            status_val = "open" if STATUS_OPEN in (meta.get("status") or []) else "forthcoming"

            collected.append({
                "topic_id":           topic_id,
                "title":              str(title)[:200],
                "call_id":            call_id,
                "call_title":         call_title,
                "status":             status_val,
                "deadline":           deadline,
                "publication_date":   pub_date,
                "type_of_action":     type_of_action,
                "budget":             budget_meur,
                "min_grant_meur":     round(min_meur, 2) if min_meur else None,
                "max_grant_meur":     round(max_meur, 2) if max_meur else None,
                "description":        description,
                "keywords":           keywords_list,
                "cross_cutting_priorities": cross_list,
                "programme_division": prog_division,
                "programme_period":   prog_period,
                "topic_conditions":   topic_conditions,
                "ccm2id":             ccm2id,
                "trl":                trl,
                "portal_url":         f"https://ec.europa.eu/info/funding-tenders/opportunities/portal/screen/opportunities/topic-details/{topic_id}",
                "primary_url":        primary_url,
                "partner_search_url": f"https://eu-partner-intel-production.up.railway.app/partners?topic_id={topic_id}",
            })

        if len(hits) < 50:
            break
        # If not fetching all, stop once we have enough for current page
        if not fetch_all and len(collected) >= page_number * page_size:
            break
        api_page += 1

    start_idx  = (page_number - 1) * page_size if not fetch_all else 0
    page_size_effective = page_size if not fetch_all else len(collected)
    page_items = collected[start_idx:start_idx + page_size_effective]

    return {
        "filters":       {"programme": programme, "cluster": cluster, "keywords": keywords, "status": status, "deadline_after": deadline_after, "deadline_before": deadline_before},
        "total_matched": len(collected),
        "returned":      len(page_items),
        "page":          page_number,
        "page_size":     page_size,
        "calls":         page_items,
        "note":          "title e deadline reali da SEDIA. Usa partner_search_url per cercare partner.",
    }


@app.get("/programmes")
async def list_programmes(
    status: str = Query("open", description="open | forthcoming | all"),
):
    """
    Scopre tutti i programmi EU disponibili in SEDIA con conteggio call.
    Usa type=1 + status filter, poi raggruppa per prefisso del topic_id (identifier).
    """
    import uuid as _uuid
    import urllib.parse as _urlparse
    import json as _json
    from collections import defaultdict

    STATUS_OPEN        = "31094502"
    STATUS_FORTHCOMING = "31094501"

    if status == "open":
        status_terms = [STATUS_OPEN]
    elif status == "forthcoming":
        status_terms = [STATUS_FORTHCOMING]
    else:
        status_terms = [STATUS_OPEN, STATUS_FORTHCOMING]

    query_obj     = {"bool": {"must": [
        {"terms": {"type": ["1"]}},
        {"terms": {"status": status_terms}},
    ]}}
    languages_obj = ["en"]
    sort_obj      = [{"field": "identifier", "order": "ASC"}]

    all_identifiers = []
    seen_ids = set()
    api_page = 1

    while True:
        boundary = f"----euft-{_uuid.uuid4().hex}"
        chunks = []
        for fname, (fn, fval, fct) in {
            "query":     ("blob", _json.dumps(query_obj),     "application/json"),
            "languages": ("blob", _json.dumps(languages_obj), "application/json"),
            "sort":      ("blob", _json.dumps(sort_obj),      "application/json"),
        }.items():
            chunks.append(f"--{boundary}\r\n".encode())
            chunks.append(f'Content-Disposition: form-data; name="{fname}"; filename="{fn}"\r\nContent-Type: {fct}\r\n\r\n'.encode())
            chunks.append(fval.encode())
            chunks.append(b"\r\n")
        chunks.append(f"--{boundary}--\r\n".encode())
        body = b"".join(chunks)

        # SEDIA caps at 50 per page regardless of pageSize param
        params = {"pageSize": "50", "pageNumber": str(api_page), "text": "***", "apiKey": "SEDIA"}
        url = "https://api.tech.ec.europa.eu/search-api/prod/rest/search?" + _urlparse.urlencode(params)

        async with httpx.AsyncClient(timeout=30.0) as client:
            r = await client.post(url, content=body, headers={
                "Content-Type": f"multipart/form-data; boundary={boundary}",
                "Accept": "application/json",
                "Origin": "https://ec.europa.eu",
            })

        if r.status_code != 200:
            break

        data = r.json()
        hits = data.get("results") or []
        total = data.get("totalResults") or 0

        for hit in hits:
            meta = hit.get("metadata", {}) if isinstance(hit.get("metadata"), dict) else {}
            ident_raw = meta.get("identifier") or []
            tid = (ident_raw[0] if isinstance(ident_raw, list) and ident_raw else str(ident_raw)).strip().upper()
            if tid and tid not in seen_ids:
                seen_ids.add(tid)
                all_identifiers.append(tid)

        if not hits or len(hits) < 50 or len(all_identifiers) >= total:
            break
        # Safety: max 20 pages (1000 calls)
        if api_page >= 20:
            break
        api_page += 1

    # Raggruppa per prefisso (primo segmento prima del secondo trattino)
    # es. HORIZON-CL3-... -> HORIZON
    #     EDF-2026-... -> EDF
    #     DIGITAL-2026-... -> DIGITAL
    #     AGRIP-MULTI-... -> AGRIP
    programme_counts = defaultdict(int)
    for tid in all_identifiers:
        parts = tid.split("-")
        prefix = parts[0] if parts else tid
        programme_counts[prefix] += 1

    programmes = sorted(
        [{"programme": k, "call_count": v} for k, v in programme_counts.items()],
        key=lambda x: -x["call_count"]
    )

    return {
        "status_filter": status,
        "total_calls":   len(all_identifiers),
        "programmes":    programmes,
    }


@app.get("/debug-types")
async def debug_types():
    """Fetch all open calls and return unique typeOfMGAs IDs with examples."""
    import uuid as _uuid
    import urllib.parse as _urlparse
    import json as _json
    from collections import defaultdict

    query_obj     = {"bool": {"must": [
        {"terms": {"type": ["1"]}},
        {"terms": {"status": ["31094502", "31094501"]}},
    ]}}
    languages_obj = ["en"]
    sort_obj      = [{"field": "identifier", "order": "ASC"}]

    type_map = defaultdict(list)
    api_page = 1

    while api_page <= 20:
        boundary = f"----euft-{_uuid.uuid4().hex}"
        chunks = []
        for fname, (fn, fval, fct) in {
            "query":     ("blob", _json.dumps(query_obj),     "application/json"),
            "languages": ("blob", _json.dumps(languages_obj), "application/json"),
            "sort":      ("blob", _json.dumps(sort_obj),      "application/json"),
        }.items():
            chunks.append(f"--{boundary}\r\n".encode())
            chunks.append(f'Content-Disposition: form-data; name="{fname}"; filename="{fn}"\r\nContent-Type: {fct}\r\n\r\n'.encode())
            chunks.append(fval.encode())
            chunks.append(b"\r\n")
        chunks.append(f"--{boundary}--\r\n".encode())
        body = b"".join(chunks)

        params = {"pageSize": "50", "pageNumber": str(api_page), "text": "***", "apiKey": "SEDIA"}
        url = "https://api.tech.ec.europa.eu/search-api/prod/rest/search?" + _urlparse.urlencode(params)

        async with httpx.AsyncClient(timeout=25.0) as client:
            r = await client.post(url, content=body, headers={
                "Content-Type": f"multipart/form-data; boundary={boundary}",
                "Accept": "application/json",
                "Origin": "https://ec.europa.eu",
            })

        if r.status_code != 200:
            break

        data = r.json()
        hits = data.get("results") or []
        total = data.get("totalResults") or 0

        for hit in hits:
            meta = hit.get("metadata", {}) if isinstance(hit.get("metadata"), dict) else {}
            type_ids = meta.get("typeOfMGAs") or []
            ident_raw = meta.get("identifier") or []
            tid = (ident_raw[0] if isinstance(ident_raw, list) and ident_raw else "").strip()
            title = hit.get("title") or hit.get("summary") or ""

            for t in type_ids:
                if t and len(type_map[t]) < 3:
                    type_map[t].append({"topic_id": tid, "title": str(title)[:80]})

        if not hits or len(hits) < 50:
            break
        api_page += 1

    # Build result with current mapping status
    result = []
    for type_id, examples in sorted(type_map.items()):
        mapped = TYPE_OF_ACTION_MAP.get(str(type_id), "UNKNOWN")
        result.append({
            "type_id": type_id,
            "mapped_to": mapped,
            "known": mapped != "UNKNOWN",
            "examples": examples,
        })

    unknown = [r for r in result if not r["known"]]
    known   = [r for r in result if r["known"]]

    return {
        "total_unique_type_ids": len(result),
        "unknown_ids": unknown,
        "known_ids": known,
    }


@app.get("/debug-budget")
async def debug_budget(
    topic_id: str = Query(..., description="Es: HORIZON-CL4-2026-04-DATA-06"),
):
    """Dump raw budgetOverview, typeOfMGAs, actions and all metadata for a topic."""
    import uuid as _uuid
    import urllib.parse as _urlparse
    import json as _json

    query_obj     = {"bool": {"must": [
        {"terms": {"type": ["1"]}},
        {"terms": {"status": ["31094502", "31094501"]}},
    ]}}
    languages_obj = ["en"]
    sort_obj      = [{"field": "identifier", "order": "ASC"}]

    boundary = f"----euft-{_uuid.uuid4().hex}"
    chunks = []
    for fname, (fn, fval, fct) in {
        "query":     ("blob", _json.dumps(query_obj),     "application/json"),
        "languages": ("blob", _json.dumps(languages_obj), "application/json"),
        "sort":      ("blob", _json.dumps(sort_obj),      "application/json"),
    }.items():
        chunks.append(f"--{boundary}\r\n".encode())
        chunks.append(f'Content-Disposition: form-data; name="{fname}"; filename="{fn}"\r\nContent-Type: {fct}\r\n\r\n'.encode())
        chunks.append(fval.encode())
        chunks.append(b"\r\n")
    chunks.append(f"--{boundary}--\r\n".encode())
    body = b"".join(chunks)

    params = {"pageSize": "50", "pageNumber": "1", "text": "***", "apiKey": "SEDIA"}
    url = "https://api.tech.ec.europa.eu/search-api/prod/rest/search?" + _urlparse.urlencode(params)

    # Search all pages for the topic
    hit = None
    for page in range(1, 20):
        params["pageNumber"] = str(page)
        url = "https://api.tech.ec.europa.eu/search-api/prod/rest/search?" + _urlparse.urlencode(params)
        async with httpx.AsyncClient(timeout=25.0) as client:
            r = await client.post(url, content=body, headers={
                "Content-Type": f"multipart/form-data; boundary={boundary}",
                "Accept": "application/json",
                "Origin": "https://ec.europa.eu",
            })
        if r.status_code != 200:
            break
        data = r.json()
        hits = data.get("results") or []
        for h in hits:
            meta = h.get("metadata", {}) if isinstance(h.get("metadata"), dict) else {}
            ident_raw = meta.get("identifier") or []
            tid = (ident_raw[0] if isinstance(ident_raw, list) and ident_raw else "").strip().upper()
            if tid == topic_id.upper():
                hit = h
                break
        if hit or len(hits) < 50:
            break

    if not hit:
        return JSONResponse(status_code=404, content={"error": f"Topic {topic_id} not found"})

    meta = hit.get("metadata", {}) if isinstance(hit.get("metadata"), dict) else {}

    # Parse budgetOverview
    budget_overview_raw = meta.get("budgetOverview")
    budget_overview_parsed = _euft_safe_json(budget_overview_raw) if budget_overview_raw else None

    # Parse actions
    actions_raw = meta.get("actions")
    actions_parsed = _euft_safe_json(actions_raw) if actions_raw else None

    return {
        "topic_id": topic_id,
        "ALL_META_KEYS": sorted(meta.keys()),
        "typeOfMGAs": meta.get("typeOfMGAs"),
        "programmeDivision": meta.get("programmeDivision"),
        "focusArea": meta.get("focusArea"),
        "topicConditions_raw": (meta.get("topicConditions") or [])[:1],
        "budgetOverview_parsed": budget_overview_parsed,
        "actions_parsed": actions_parsed,
        "identifier": meta.get("identifier"),
        "callIdentifier": meta.get("callIdentifier"),
        "callTitle": meta.get("callTitle"),
    }


@app.get("/debug-calls")
async def debug_calls(
    programme: str = Query("EDF"),
    status: str = Query("open"),
):
    """Debug endpoint: mostra raw SEDIA response per capire struttura calls."""
    import uuid as _uuid
    import urllib.parse as _urlparse
    import json as _json

    STATUS_OPEN        = "31094502"
    STATUS_FORTHCOMING = "31094501"
    status_terms = [STATUS_OPEN] if status == "open" else [STATUS_FORTHCOMING] if status == "forthcoming" else [STATUS_OPEN, STATUS_FORTHCOMING]

    # Try 1: with frameworkProgramme filter
    must_with = [
        {"terms": {"type": ["1"]}},
        {"terms": {"status": status_terms}},
        {"terms": {"frameworkProgramme": [programme.upper()]}},
    ]
    # Try 2: without type filter (maybe EDF uses different type)
    must_no_type = [
        {"terms": {"status": status_terms}},
        {"terms": {"frameworkProgramme": [programme.upper()]}},
    ]
    # Try 3: just status, no programme filter - see what frameworkProgramme values exist
    must_bare = [
        {"terms": {"status": status_terms}},
    ]

    results = {}


    # Try euft DEFAULT_GRANTS_QUERY exactly
    must_grants = [
        {"terms": {"type": ["1", "2"]}},
        {"terms": {"status": ["31094502"]}},
    ]
    # Try type=2 only
    must_type2 = [
        {"terms": {"type": ["2"]}},
        {"terms": {"status": ["31094502"]}},
    ]
    # Try type=1 only
    must_type1 = [
        {"terms": {"type": ["1"]}},
        {"terms": {"status": ["31094502"]}},
    ]
    # Bare - no type filter, show what types exist
    must_bare = [
        {"terms": {"status": ["31094502"]}},
    ]
    # Try forthcoming (31094501)
    must_forthcoming = [
        {"terms": {"type": ["1", "2"]}},
        {"terms": {"status": ["31094501"]}},
    ]

    for label, must in [
        ("grants_type_1_2", must_grants),
        ("type_2_only", must_type2),
        ("type_1_only", must_type1),
        ("forthcoming_type_1_2", must_forthcoming),
        ("bare_all_types", must_bare),
    ]:
        query_obj = {"bool": {"must": must}}
        languages_obj = ["en"]
        sort_obj = [{"field": "identifier", "order": "ASC"}]

        boundary = f"----euft-{_uuid.uuid4().hex}"
        chunks = []
        for field_name, (filename, payload_str, ct) in {
            "query":     ("blob", _json.dumps(query_obj),     "application/json"),
            "languages": ("blob", _json.dumps(languages_obj), "application/json"),
            "sort":      ("blob", _json.dumps(sort_obj),      "application/json"),
        }.items():
            chunks.append(f"--{boundary}\r\n".encode())
            chunks.append(f'Content-Disposition: form-data; name="{field_name}"; filename="{filename}"\r\nContent-Type: {ct}\r\n\r\n'.encode())
            chunks.append(payload_str.encode())
            chunks.append(b"\r\n")
        chunks.append(f"--{boundary}--\r\n".encode())
        body = b"".join(chunks)

        params = {"pageSize": "3", "pageNumber": "1", "text": "***", "apiKey": "SEDIA"}
        url = "https://api.tech.ec.europa.eu/search-api/prod/rest/search?" + _urlparse.urlencode(params)

        async with httpx.AsyncClient(timeout=25.0) as client:
            r = await client.post(url, content=body, headers={
                "Content-Type": f"multipart/form-data; boundary={boundary}",
                "Accept": "application/json",
                "Origin": "https://ec.europa.eu",
            })

        if r.status_code == 200:
            data = r.json()
            total = data.get("totalResults", 0)
            hits = data.get("results") or []
            sample = []
            for h in hits[:2]:
                meta = h.get("metadata", {}) if isinstance(h.get("metadata"), dict) else {}
                sample.append({
                    "reference": h.get("reference"),
                    "title": h.get("content", "")[:80],
                    "summary": h.get("summary", "")[:80],
                    "frameworkProgramme": meta.get("frameworkProgramme"),
                    "frameworkProgrammeLabel": meta.get("frameworkProgrammeLabel") or meta.get("programmeName"),
                    "type": meta.get("type"),
                    "status": meta.get("status"),
                    "identifier": meta.get("identifier"),
                    "callIdentifier": meta.get("callIdentifier"),
                    "topicIdentifier": meta.get("topicIdentifier"),
                    "deadlineDate": meta.get("deadlineDate"),
                    "openingDate": meta.get("openingDate"),
                    "ALL_META_KEYS": list(meta.keys())[:30],
                })
            results[label] = {"total": total, "sample": sample}
        else:
            results[label] = {"error": r.status_code, "text": r.text[:200]}

    return results


@app.get("/announcements")
async def get_announcements_with_descriptions(
    topic_id: str = Query(..., description="Es: HORIZON-CL4-2026-04-DATA-06"),
):
    """
    Tenta di recuperare le descrizioni reali degli annunci partner
    (quelle visibili nel portale) tramite endpoint FT-Announcements.
    
    Step 1: cerca il ccm2Id numerico del topic su SEDIA
    Step 2: chiama FT-Announcements con ccm2Id
    Step 3: se fallisce, ritorna i dati SEDIA_PERSON con nota
    """
    import json as _json

    headers = {
        "Accept": "application/json, text/plain, */*",
        "Origin": "https://ec.europa.eu",
        "Referer": "https://ec.europa.eu/",
    }

    async with httpx.AsyncClient(timeout=20.0) as client:

        # Step 1: trova ccm2Id cercando il topic su SEDIA (apiKey=SEDIA)
        ccm2id = None
        try:
            r_topic = await client.post(
                SEDIA_URL,
                params={"apiKey": "SEDIA", "text": f'"{topic_id}"', "pageSize": 5, "pageNumber": 1},
                json={},
                headers=headers,
            )
            topic_hits = r_topic.json().get("results") or []
            for hit in topic_hits:
                meta = hit.get("metadata", {})
                # Cerca il ccm2Id nei metadati
                for key in ["ccm2Id", "id", "topicId", "identifier"]:
                    val = meta.get(key, [])
                    if val:
                        candidate = val[0] if isinstance(val, list) else val
                        if str(candidate).isdigit():
                            ccm2id = candidate
                            break
                # Alternativa: il reference del hit potrebbe essere il ccm2Id
                ref = hit.get("reference", "")
                if ref and str(ref).isdigit():
                    ccm2id = ref
                    break
                if ccm2id:
                    break
        except Exception as e:
            pass

        # Step 2: chiama FT-Announcements con ccm2Id
        ft_data = None
        all_attempts = []
        if ccm2id:
            attempts = [
                ("GET",  "https://api.sedia-backoffice-production.eu/public/ehelp/module/FT-Announcements", {"ccm2Id": ccm2id}, {}),
                ("GET",  "https://api.sedia-backoffice-production.eu/public/ehelp/module/FT-Announcements", {"topicId": ccm2id}, {}),
                ("GET",  "https://api.sedia-backoffice-production.eu/public/ehelp/module/FT-Announcements", {"id": ccm2id}, {}),
                ("GET",  "https://api.sedia-backoffice-production.eu/public/ehelp/module/FT-Announcements", {"ccm2Id": topic_id}, {}),
                ("GET",  f"https://api.sedia-backoffice-production.eu/public/ehelp/module/FT-Announcements/{ccm2id}", {}, {}),
                # Try POST with body (SPA uses POST for many SEDIA endpoints)
                ("POST", "https://api.sedia-backoffice-production.eu/public/ehelp/module/FT-Announcements", {}, {"ccm2Id": ccm2id}),
                ("POST", "https://api.sedia-backoffice-production.eu/public/ehelp/module/FT-Announcements", {}, {"topicId": ccm2id}),
                # Try with apiKey param
                ("GET",  "https://api.sedia-backoffice-production.eu/public/ehelp/module/FT-Announcements", {"apiKey": "SEDIA", "ccm2Id": ccm2id}, {}),
            ]
            for method, url, params, body in attempts:
                attempt_result = {"method": method, "url": url, "params": params}
                try:
                    if method == "POST":
                        r_ft = await client.post(url, params=params, json=body, headers=headers)
                    else:
                        r_ft = await client.get(url, params=params, headers=headers)
                    attempt_result["status"] = r_ft.status_code
                    attempt_result["response"] = r_ft.text[:400]
                    if r_ft.status_code == 200:
                        try:
                            ft_data = r_ft.json()
                            ft_data["_winning_attempt"] = attempt_result
                            all_attempts.append(attempt_result)
                            break
                        except Exception:
                            attempt_result["parse_error"] = "not JSON"
                except Exception as e:
                    attempt_result["exception"] = str(e)[:200]
                all_attempts.append(attempt_result)

        # Step 3: chiama SEDIA_PERSON per la lista partner (come /partners)
        exact_query = f'"{topic_id}"'
        seen_pics = set()
        partners = []
        page = 1

        while True:
            r = await client.post(
                SEDIA_URL,
                params={"apiKey": "SEDIA_PERSON", "text": exact_query, "pageSize": 50, "pageNumber": page},
                json={},
                headers=headers,
            )
            if r.status_code != 200:
                break

            data = r.json()
            hits = data.get("results") or []
            total = data.get("totalResults") or 0

            if not hits:
                break

            for hit in hits:
                meta = hit.get("metadata", {})
                topics_field = meta.get("topics") or []
                if topic_id not in topics_field:
                    continue
                pic = (meta.get("pic") or [""])[0]
                dedup_key = pic if pic else (meta.get("name") or [""])[0]
                if dedup_key in seen_pics:
                    continue
                seen_pics.add(dedup_key)

                country_id = (meta.get("country") or [""])[0]
                org_type_raw = (meta.get("organisationType") or [""])[0]
                keywords = meta.get("keywords", [])

                partners.append({
                    "legal_name":        (meta.get("name") or [""])[0] or hit.get("summary", ""),
                    "pic_number":        pic,
                    "country":           COUNTRY_MAP.get(country_id, country_id),
                    "organization_type": ORG_TYPE_MAP.get(org_type_raw, org_type_raw),
                    "sedia_keywords":    keywords[:10],
                    "all_active_calls":  len(meta.get("topics", [])),
                    "projects_count":    (meta.get("noOfProjects") or [""])[0],
                    "portal_url":        f"https://ec.europa.eu/info/funding-tenders/opportunities/portal/screen/how-to-participate/org-details/{pic}" if pic else "",
                    # Placeholder per description da FT-Announcements
                    "announcement_description": "",
                })

            if page * 50 >= total or len(hits) < 50:
                break
            page += 1

    return {
        "topic_id":      topic_id,
        "ccm2id_found":  ccm2id,
        "ft_raw":        ft_data,
        "total_partners": len(partners),
        "partners":      partners,
        "ft_attempts": all_attempts,
        "note": "Vedere ft_attempts per capire quale endpoint funziona",
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
