import os
import hashlib
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import traceback
import concurrent.futures
import threading

BASE = "http://ufcstats.com"
CACHE_DIR = "cache_html"
os.makedirs(CACHE_DIR, exist_ok=True)

RATE_LIMITER = threading.Semaphore(8)

# Fighter listing pages (A–Z index) — always re-fetch so new signees appear
LISTING_URL_PATTERNS = [
    "statistics/fighters?char=",
]


def is_listing_url(url: str) -> bool:
    return any(p in url for p in LISTING_URL_PATTERNS)


def cache_path(url: str):
    h = hashlib.md5(url.encode()).hexdigest()
    return os.path.join(CACHE_DIR, f"{h}.html")


def fetch_cached(url: str, verbose_cache=False):
    path = cache_path(url)

    # Always re-fetch listing/index pages
    if is_listing_url(url) and os.path.exists(path):
        os.remove(path)
        if verbose_cache:
            print(f"[CACHE BUST] {url}")

    if os.path.exists(path):
        if verbose_cache:
            print(f"[CACHE HIT] {url}")
        with open(path, "r", encoding="utf-8") as f:
            return f.read()

    if verbose_cache:
        print(f"[CACHE MISS] {url}")

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/123.0 Safari/537.36"
        )
    }

    with RATE_LIMITER:
        for attempt in range(5):
            try:
                r = requests.get(url, headers=headers, timeout=12)

                if r.status_code == 200:
                    html = r.text
                    with open(path, "w", encoding="utf-8") as f:
                        f.write(html)
                    return html

                elif r.status_code == 429:
                    wait = 2 ** attempt
                    print(f"[WARN] 429 for {url} — backing off {wait}s (attempt {attempt+1}/5)")
                    time.sleep(wait)

                else:
                    print(f"[WARN] Status {r.status_code} for {url}")
                    time.sleep(1)

            except Exception as e:
                wait = 2 ** attempt
                print(f"[WARN] Error fetching {url}: {e} — retrying in {wait}s")
                time.sleep(wait)

    print(f"[ERROR] Failed to fetch {url} after 5 attempts")
    return None


def get_soup(url, verbose_cache=False):
    html = fetch_cached(url, verbose_cache=verbose_cache)
    if html is None:
        return None
    return BeautifulSoup(html, "html.parser")


def normalize_url(href):
    if not href:
        return None
    href = href.strip()
    if href.startswith("//"):
        return "http:" + href
    if href.startswith("/"):
        return BASE + href
    if href.startswith("http"):
        return href
    return BASE + "/" + href


def get_all_fighter_links():
    chars = "abcdefghijklmnopqrstuvwxyz"
    fighter_links = set()

    for c in chars:
        url = f"{BASE}/statistics/fighters?char={c}&page=all"
        soup = get_soup(url)
        if not soup:
            continue

        for a in soup.select("a.b-link.b-link_style_black[href*='fighter-details']"):
            fighter_links.add(normalize_url(a["href"]))

        print(f"Loaded {c.upper()} — Total so far: {len(fighter_links)}")

    return sorted(fighter_links)


def parse_fighter(url):
    soup = get_soup(url)
    if not soup:
        return None

    name = soup.select_one("span.b-content__title-highlight")
    name = name.text.strip() if name else None

    record = soup.select_one("span.b-content__title-record")
    record = record.text.replace("Record:", "").strip() if record else None

    info = soup.select("div.b-list__info-box ul.b-list__box-list li")

    def extract(label):
        for li in info:
            title = li.select_one("i.b-list__box-item-title")
            if not title:
                continue
            if title.text.strip().lower().startswith(label.lower()):
                raw = li.text.replace(title.text, "").strip()
                return raw if raw != "" else None
        return None

    height = extract("Height")
    weight = extract("Weight")
    reach  = extract("Reach")
    stance = extract("STANCE")
    dob    = extract("DOB")

    left_stats = soup.select("div.b-list__info-box-left li")

    def get_left(prefix):
        for li in left_stats:
            t = li.text.strip()
            if t.startswith(prefix):
                return t.replace(prefix, "").strip()
        return None

    SLpM    = get_left("SLpM:")
    Str_Acc = get_left("Str. Acc.:")
    SApM    = get_left("SApM:")
    Str_Def = get_left("Str. Def.")

    right_stats = soup.select("div.b-list__info-box-right li")

    def get_right(prefix):
        for li in right_stats:
            t = li.text.strip()
            if t.startswith(prefix):
                return t.replace(prefix, "").strip()
        return None

    TD_Avg  = get_right("TD Avg.:")
    TD_Acc  = get_right("TD Acc.:")
    TD_Def  = get_right("TD Def.:")
    Sub_Avg = get_right("Sub. Avg.:")

    return {
        "fighter_url": url,
        "name":        name,
        "record":      record,
        "height":      height,
        "weight":      weight,
        "reach":       reach,
        "stance":      stance,
        "dob":         dob,
        "SLpM":        SLpM,
        "SApM":        SApM,
        "Str_Acc":     Str_Acc,
        "Str_Def":     Str_Def,
        "TD_Avg":      TD_Avg,
        "TD_Acc":      TD_Acc,
        "TD_Def":      TD_Def,
        "Sub_Avg":     Sub_Avg,
    }


def scrape_fighters(threads=8, save_every=500):
    fighter_links = get_all_fighter_links()
    total = len(fighter_links)
    print(f"[+] Found {total} fighters")

    data = []
    completed = 0
    lock = threading.Lock()

    print(f"[+] Starting multithreaded scrape with {threads} workers...")

    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
        futures = {executor.submit(parse_fighter, url): url for url in fighter_links}

        for future in concurrent.futures.as_completed(futures):
            url = futures[future]
            completed += 1

            try:
                result = future.result()
                if result:
                    with lock:
                        data.append(result)
            except Exception as e:
                print(f"[ERROR] {url}: {e}")
                traceback.print_exc()

            # Incremental save so a crash doesn't lose everything
            if completed % save_every == 0 or completed == total:
                with lock:
                    pd.DataFrame(data).to_csv("ufc_fighter_stats.csv", index=False)
                print(f"[PROGRESS] {completed}/{total} processed ({len(data)} saved)")

    print(f"[+] Done — ufc_fighter_stats.csv with {len(data)} fighters")


if __name__ == "__main__":
    scrape_fighters()