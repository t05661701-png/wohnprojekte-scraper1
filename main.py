# main_simple_cleaned.py
import asyncio
import re
from urllib.parse import urljoin, urlparse
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
import pandas as pd
import requests
import os
import httpx
import json
import httpx
from dotenv import load_dotenv

load_dotenv()
WEBAPP_URL = os.getenv("WEBAPP_URL")
LINKS_FILE = "input_links/links.txt"
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

# Regex für Straße + Hausnummer, PLZ und Ort (DE/AT)
RE_ADDRESS = re.compile(
    r"\b([A-ZÄÖÜ][a-zäöüß]+(?:\s+[A-ZÄÖÜa-zäöüß\-]+)*\s+\d+[a-zA-Z]?)\s*,?\s*(\d{4})\s+([A-ZÄÖÜ][\w\-]+)",
    re.UNICODE
)

def extract_traeger_from_url(url):
    domain = urlparse(url).netloc.replace("www.", "").split(".")[0]
    return domain.replace("-", " ").title()

def extract_project_name(tag):
    # Suche nach typischen Projektname-Elementen
    for heading in tag.find_all(["h1", "h2", "h3", "strong", "b"]):
        txt = heading.get_text(" ", strip=True)
        if 3 <= len(txt) <= 80 and not txt.isdigit():
            return txt
    # Fallback: erster sinnvolle Text
    lines = [l.strip() for l in tag.get_text("\n", strip=True).split("\n") if l.strip()]
    for line in lines:
        if 3 <= len(line) <= 80 and not line.isdigit():
            return line
    return ""

def extract_address(text):
    m = RE_ADDRESS.search(text)
    if m:
        street, plz, ort = m.groups()
        return f"{street}, {plz} {ort}"
    # Fallback: Suche nach PLZ und Ort
    m2 = re.search(r"(\d{4})\s+([A-ZÄÖÜ][\w\-]+)", text)
    if m2:
        plz, ort = m2.groups()
        return f"{plz} {ort}"
    return ""

async def scrape_candidates(start_url):
    import pytesseract
    from PIL import Image
    import io
    # ...existing code...
    # 1. Generische Formular-Erkennung und Ausfüllen
    async def fill_and_submit_forms(page):
        try:
            forms = await page.query_selector_all('form')
            for form in forms:
                inputs = await form.query_selector_all('input[type="text"], input:not([type]), input[type="search"]')
                for inp in inputs:
                    try:
                        await inp.fill("Wohnung")
                    except Exception:
                        pass
                selects = await form.query_selector_all('select')
                for sel in selects:
                    try:
                        await sel.select_option(value="Wohnung")
                    except Exception:
                        try:
                            opts = await sel.query_selector_all('option')
                            if opts:
                                val = await opts[0].get_attribute('value')
                                if val:
                                    await sel.select_option(value=val)
                        except Exception:
                            pass
                btns = await form.query_selector_all('button, input[type="submit"]')
                for btn in btns:
                    txt = (await btn.get_attribute('value') or await btn.inner_text()).lower()
                    if any(x in txt for x in ["suche", "start", "anzeigen", "finden"]):
                        try:
                            await btn.click()
                            await asyncio.sleep(3)
                        except Exception:
                            pass
        except Exception as e:
            print(f"Generische Formular-Erkennung Fehler: {e}")

    # 2. Mitschneiden von XHR/API-Requests, um dynamisch geladene Daten zu extrahieren
    api_results = []
    def handle_response(response):
        try:
            ct = response.headers.get('content-type', '')
            if 'application/json' in ct:
                body = response.body()
                if body:
                    try:
                        data = json.loads(body)
                        api_results.append(data)
                    except Exception:
                        pass
        except Exception:
            pass

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        page = await browser.new_page()
        page.on('response', handle_response)
        await page.goto(start_url, timeout=60000)
        await page.wait_for_load_state("networkidle")
        await fill_and_submit_forms(page)
        # Spezialfall: ooewohnbau.at/immobiliensuche – Formular ausfüllen und absenden
        if "ooewohnbau.at/immobiliensuche" in start_url:
            try:
                await page.select_option("select[name='objektart']", value="Wohnung")
            except Exception:
                pass
            try:
                btn = await page.query_selector("button:has-text('Suche starten')")
                if btn:
                    await btn.click()
                    await asyncio.sleep(3)
            except Exception as e:
                print(f"Formular-Fehler: {e}")
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto(start_url, timeout=60000)
        await page.wait_for_load_state("networkidle")
        # Spezialfall: ooewohnbau.at/immobiliensuche – Formular ausfüllen und absenden
        if "ooewohnbau.at/immobiliensuche" in start_url:
            try:
                # Wähle z.B. "Wohnung" als Kategorie, falls vorhanden
                await page.select_option("select[name='objektart']", value="Wohnung")
            except Exception:
                pass
            try:
                # Klicke auf "Suche starten" Button
                btn = await page.query_selector("button:has-text('Suche starten')")
                if btn:
                    await btn.click()
                    await asyncio.sleep(3)
            except Exception as e:
                print(f"Formular-Fehler: {e}")
        last_h = await page.evaluate("document.body.scrollHeight")
        same = 0
        # Automatisches Klicken auf 'Mehr laden', 'Weitere anzeigen', 'Suche starten' Buttons
        max_clicks = 10
        for i in range(max_clicks):
            found = False
            # Suche nach Buttons mit typischen Texten
            for btn_text in ["mehr", "weiter", "anzeigen", "laden", "suche", "filter", "anzeigen", "weitere", "anzeigen", "anzeigen", "anzeigen"]:
                try:
                    btn = await page.query_selector(f'button:has-text("{btn_text}")')
                    if btn:
                        await btn.click()
                        await asyncio.sleep(2)
                        found = True
                        print(f"Button '{btn_text}' geklickt.")
                except Exception:
                    continue
            if not found:
                break
        while same < 3:
            try:
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            except Exception as e:
                print(f"Warnung: Scroll-Fehler: {e}")
            await asyncio.sleep(1)
            try:
                new_h = await page.evaluate("document.body.scrollHeight")
            except Exception as e:
                print(f"Warnung: Scroll-Höhe Fehler: {e}")
                break
            if new_h == last_h:
                same += 1
            else:
                same = 0
                last_h = new_h
        html = await page.content()
        # Fallback: Screenshot & OCR, falls keine Kandidaten
        screenshot_bytes = None
        if not screenshot_bytes:
            try:
                screenshot_bytes = await page.screenshot(full_page=True)
            except Exception as e:
                print(f"Screenshot-Fehler: {e}")
        await browser.close()

    soup = BeautifulSoup(html, "html.parser")
    res, seen = [], set()
    # 1. Finde alle relevanten Detail-Links auf der Startseite
    detail_links = set()
    for a in soup.find_all("a", href=True):
        ltxt = a.get_text(" ", strip=True).lower()
        lhref = a["href"]
        if any(x in ltxt for x in ["mehr", "details", "objekt", "expose", "angebot", "wohnung", "haus", "ansehen", "anzeigen", "projekt"]):
            if lhref.startswith("http"):
                detail_links.add(lhref)
            else:
                detail_links.add(urljoin(start_url, lhref))
    # 2. Finde Paginierungs-Links und folge ihnen (max. 5 Seiten)
    page_links = set()
    for a in soup.find_all("a", href=True):
        ltxt = a.get_text(" ", strip=True).lower()
        lhref = a["href"]
        if any(x in ltxt for x in ["weiter", "nächste", "seite", "vor"]):
            if lhref.startswith("http"):
                page_links.add(lhref)
            else:
                page_links.add(urljoin(start_url, lhref))

    # 3. Falls keine HTML-Kandidaten gefunden wurden, versuche API-Responses auszuwerten
    if not detail_links and api_results:
        for api_data in api_results:
            # ...wie bisher...
            if isinstance(api_data, dict):
                for k, v in api_data.items():
                    if isinstance(v, list):
                        for item in v:
                            name = item.get("projektname") or item.get("name") or item.get("titel") or ""
                            addr = item.get("adresse") or item.get("ort") or item.get("location") or ""
                            link = item.get("link") or item.get("url") or start_url
                            if name and addr:
                                key = (name.lower().strip(), addr.lower().strip(), link.lower().strip())
                                if key in seen:
                                    continue
                                seen.add(key)
                                res.append({"tag": None, "text": f"{name} {addr}", "link": link})
            elif isinstance(api_data, list):
                for item in api_data:
                    name = item.get("projektname") or item.get("name") or item.get("titel") or ""
                    addr = item.get("adresse") or item.get("ort") or item.get("location") or ""
                    link = item.get("link") or item.get("url") or start_url
                    if name and addr:
                        key = (name.lower().strip(), addr.lower().strip(), link.lower().strip())
                        if key in seen:
                            continue
                        seen.add(key)
                        res.append({"tag": None, "text": f"{name} {addr}", "link": link})

    # 4. Fallback: Wenn immer noch keine Kandidaten, nutze Screenshot und OCR
    if not res and screenshot_bytes:
        try:
            img = Image.open(io.BytesIO(screenshot_bytes))
            ocr_text = pytesseract.image_to_string(img, lang='deu')
            for line in ocr_text.splitlines():
                addr = extract_address(line)
                name = line.strip() if 3 <= len(line.strip()) <= 80 and not line.strip().isdigit() else ""
                if addr and name:
                    key = (name.lower().strip(), addr.lower().strip(), start_url)
                    if key in seen:
                        continue
                    seen.add(key)
                    res.append({"tag": None, "text": line, "link": start_url})
            print("OCR-Fallback verwendet!")
        except Exception as e:
            print(f"OCR/Screenshot-Fehler: {e}")
    async def scrape_detail(url):
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True)
            context = await browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36")
            page = await context.new_page()
            try:
                await page.goto(url, timeout=60000)
                await page.wait_for_load_state("networkidle", timeout=60000)
            except Exception as e:
                print(f"Warnung: Timeout oder Fehler bei {url}: {e}")
                await asyncio.sleep(3)
            html = await page.content()
            await browser.close()
        soup = BeautifulSoup(html, "html.parser")
        for tag in soup.find_all(['article', 'section', 'div', 'li', 'tr']):
            txt = tag.get_text(" ", strip=True)
            name = ""
            for h in tag.find_all(["h1", "h2", "h3", "strong", "b", "button"]):
                t = h.get_text(" ", strip=True)
                if 3 <= len(t) <= 80 and not t.isdigit():
                    name = t
                    break
            if not name:
                for a in tag.find_all("a", href=True):
                    t = a.get_text(" ", strip=True)
                    if 3 <= len(t) <= 80 and not t.isdigit() and "projekt" in t.lower():
                        name = t
                        break
            if not name:
                for img in tag.find_all("img", alt=True):
                    t = img["alt"].strip()
                    if 3 <= len(t) <= 80 and not t.isdigit():
                        name = t
                        break
            if not name:
                meta = tag.find("meta", attrs={"name": "title"})
                if meta and meta.get("content"):
                    t = meta["content"].strip()
                    if 3 <= len(t) <= 80 and not t.isdigit():
                        name = t
            if not name:
                lines = [l.strip() for l in txt.split("\n") if l.strip()]
                for line in lines:
                    if 3 <= len(line) <= 80 and not line.isdigit():
                        name = line
                        break
            addr = ""
            m = RE_ADDRESS.search(txt)
            if m:
                street, plz, ort = m.groups()
                addr = f"{street}, {plz} {ort}"
            else:
                m2 = re.search(r"(\d{4})\s+([A-ZÄÖÜ][\w\-]+)", txt)
                if m2:
                    plz, ort = m2.groups()
                    addr = f"{plz} {ort}"
            if not addr:
                for td in tag.find_all(["td", "li"]):
                    t = td.get_text(" ", strip=True)
                    m = RE_ADDRESS.search(t)
                    if m:
                        street, plz, ort = m.groups()
                        addr = f"{street}, {plz} {ort}"
                        break
            # Link direkt aus dem Projekt-Container, nur individuelle Projektseiten
            link = None
            for a in tag.find_all("a", href=True):
                l = a["href"]
                # Nur Links mit eindeutiger ID/Slug (z.B. Zahl, Name, kein /projekt/ oder /projekte/)
                if re.search(r"/(projekt|projekte)/[\w\-]+", l) and not l.endswith("/projekt") and not l.endswith("/projekte"):
                    link = l if l.startswith("http") else urljoin(url, l)
                    break
            if not link:
                continue
            if len(txt) < 30 or not name or not addr:
                continue
            if any(x in name.lower() for x in ["cookiebot", "youtube", "facebook", "instagram", "twitter", "notwendig", "vormerkung", "kundencenter", "kontakt", "impressum", "datenschutz"]):
                continue
            if "tel:" in link or "mailto:" in link:
                continue
            if any(x in name.lower() for x in ["lage", "über uns", "ihr ansprechpartner", "ansprechpartner", "qualität", "fertigstellung", "baubeginn", "elementum marchtrenk", "verfügbare wohnungen", "ähnliche projekte"]):
                continue
            key = (name.lower().strip(), addr.lower().strip(), link.lower().strip())
            if key in seen:
                continue
            # Filtere Links, die auf generische Projektübersicht zeigen
            if re.search(r"/projekt/?$", link):
                continue
            seen.add(key)
            res.append({"tag": tag, "text": txt, "link": link})
    # 4. Starte Scrape für alle Detailseiten
    tasks = [scrape_detail(l) for l in detail_links]
    # 5. Folge Paginierung (max. 5 Seiten)
    for i, pl in enumerate(list(page_links)[:2]):
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True)
            page = await browser.new_page()
            await page.goto(pl, timeout=60000)
            await page.wait_for_load_state("networkidle")
            html = await page.content()
            await browser.close()
        soup = BeautifulSoup(html, "html.parser")
        for a in soup.find_all("a", href=True):
            ltxt = a.get_text(" ", strip=True).lower()
            lhref = a["href"]
            if any(x in ltxt for x in ["mehr", "details", "objekt", "expose", "angebot", "wohnung", "haus", "ansehen", "anzeigen", "projekt"]):
                if lhref.startswith("http"):
                    tasks.append(scrape_detail(lhref))
                else:
                    tasks.append(scrape_detail(urljoin(pl, lhref)))
    # 6. Starte alle Detail-Scrapes mit Begrenzung auf 3 gleichzeitig
    sem = asyncio.Semaphore(3)
    async def sem_task(task):
        async with sem:
            await task
    await asyncio.gather(*(sem_task(t) for t in tasks))
    return res

def map_to_table(candidates):
    # traeger wird jetzt dynamisch übergeben
    def get_traeger(c):
        return extract_traeger_from_url(c["link"])
    rows = []
    for idx, c in enumerate(candidates, start=1):
        name = extract_project_name(c["tag"])
        addr = extract_address(c["text"])
        if not name or not addr:
            continue
        traeger = get_traeger(c)
        rows.append({
            "Nr.": idx,
            "Wohnbauträger": traeger,
            "Projektname": name,
            "Adresse": addr,
            "Link": c["link"]
        })
    return rows
    rows = []
    for idx, c in enumerate(candidates, start=1):
        name = extract_project_name(c["tag"])
        addr = extract_address(c["text"])
        if not name or not addr:
            continue
        rows.append({
            "Nr.": idx,
            "Wohnbauträger": traeger,
            "Projektname": name,
            "Adresse": addr,
            "Link": c["link"]
        })
    return rows

def upload_to_google_sheet(rows):
    if not WEBAPP_URL:
        print("⚠️ Keine WEBAPP_URL gesetzt.")
        return False
    df = pd.DataFrame(rows)
    values = [list(df.columns)] + df.values.tolist()
    try:
        r = requests.post(WEBAPP_URL, json=values, timeout=30)
        print("Upload:", r.status_code, r.text[:200])
        return r.ok
    except Exception as e:
        print("Upload-Fehler:", e)
        return False

async def main():
    all_rows = []
    with open(LINKS_FILE, "r", encoding="utf-8") as f:
        urls = [line.strip() for line in f if line.strip()]
    for url in urls:
        print(f"Starte Scrape für: {url}")
        candidates = await scrape_candidates(url)
        print(f"Rohkandidaten für {url}: {len(candidates)}")
        rows = map_to_table(candidates)
        all_rows.extend(rows)
    df = pd.DataFrame(all_rows)
    csv_name = "projekte_clean.csv"
    df.to_csv(csv_name, index=False, encoding="utf-8")
    print("CSV:", csv_name)
    print("Upload erfolgreich." if upload_to_google_sheet(all_rows) else "Upload fehlgeschlagen.")

if __name__ == "__main__":
    asyncio.run(main())
