import os
import hashlib
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import traceback
import concurrent.futures
import threading
from dateutil.parser import parse

BASE = "http://ufcstats.com"
CACHE_DIR = "cache_html"
os.makedirs(CACHE_DIR, exist_ok=True)

# Max concurrent requests to avoid rate limiting
RATE_LIMITER = threading.Semaphore(8)

# Pages that list events/fights — always re-fetch so new events appear
LISTING_URL_PATTERNS = [
    "statistics/events/completed",
    "statistics/events/upcoming",
]


def is_listing_url(url: str) -> bool:
    return any(p in url for p in LISTING_URL_PATTERNS)


def cache_path(url: str):
    """Convert URL to md5 hashed cache filename."""
    h = hashlib.md5(url.encode()).hexdigest()
    return os.path.join(CACHE_DIR, f"{h}.html")


def bust_cache(url: str):
    """Delete cached file for a URL if it exists."""
    path = cache_path(url)
    if os.path.exists(path):
        os.remove(path)
        print(f"[CACHE BUST] {url}")


def fetch_cached(url: str, verbose_cache=False, force_refresh=False):
    """
    Return HTML from cache if available (and not a listing page or forced refresh).
    Otherwise download and save with exponential backoff on 429s.
    """
    path = cache_path(url)

    # Always re-fetch listing/index pages so newly completed events appear
    if force_refresh or is_listing_url(url):
        if os.path.exists(path):
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
                    print(f"[WARN] Status 429 for {url} — backing off {wait}s (attempt {attempt+1}/5)")
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


def get_soup(url, verbose_cache=False, force_refresh=False):
    """Return BeautifulSoup object using cached HTML."""
    html = fetch_cached(url, verbose_cache=verbose_cache, force_refresh=force_refresh)
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
    if href.startswith("http://") or href.startswith("https://"):
        return href
    return BASE + "/" + href


def cache_stats(urls):
    """Print how many URLs are already cached."""
    total = len(urls)
    hits = sum(1 for url in urls if os.path.exists(cache_path(url)))
    misses = total - hits
    pct = (100 * hits // total) if total > 0 else 0
    print(f"[CACHE] {hits}/{total} already cached ({pct}%) — {misses} will hit the network")
    return hits, misses


def get_all_event_links():
    """
    Returns all completed event detail page URLs.
    The listing page is always re-fetched (cache busted) so newly completed
    events are picked up on every run.
    """
    url = f"{BASE}/statistics/events/completed?page=all"
    # force_refresh=True is redundant here since is_listing_url() catches it,
    # but explicit is clearer.
    soup = get_soup(url, force_refresh=True)
    if soup is None:
        print("[ERROR] Could not load events page.")
        return []

    events = set()
    for a in soup.find_all("a", href=True):
        if "event-details" in a["href"]:
            events.add(normalize_url(a["href"]))

    events = sorted(events)
    print(f"[OK] Found {len(events)} events")
    return events


def get_event_fights(event_url):
    """
    Return all fight URLs from an event page.
    Event index pages are also cache-busted so any late-added bouts appear.
    """
    # Event detail pages aren't "listing" pages by pattern, but we still want
    # fresh data for events that were recently added.  Use force_refresh only
    # for events whose cache is older than 30 days — approximated here by
    # always refreshing (cheap since HTML is tiny).  If you want to preserve
    # the old behaviour for historical events, swap to get_soup(event_url).
    soup = get_soup(event_url, force_refresh=False)
    if soup is None:
        return []

    fights = set()
    for a in soup.select("tr.b-fight-details__table-row a[href*='fight-details']"):
        fights.add(normalize_url(a["href"]))

    if not fights:  # fallback
        for a in soup.find_all("a", href=True):
            if "fight-details" in a["href"]:
                fights.add(normalize_url(a["href"]))

    fights = sorted(fights)
    print(f"Event {event_url} | {len(fights)} fights")
    return fights


def safe_text(t):
    return t.get_text(strip=True) if t else None


def parse_fight(fight_url):
    soup = get_soup(fight_url)
    if soup is None:
        return None

    # Event link
    event_url = None
    event_link = soup.select_one("a.b-fight-details__event-link")
    if event_link:
        event_url = normalize_url(event_link.get("href"))
    else:
        alt = soup.find("a", href=lambda x: x and "event-details" in x)
        if alt:
            event_url = normalize_url(alt["href"])

    # Event date
    date = None
    if event_url:
        event_soup = get_soup(event_url)
        if event_soup:
            try:
                label = event_soup.find(
                    "i",
                    class_="b-list__box-item-title",
                    string=lambda x: x and "Date" in x
                )
                if label:
                    raw = label.parent.get_text(" ", strip=True)
                    raw = raw.replace("Date:", "").strip()
                    date = parse(raw, fuzzy=True).date()
            except:
                pass

    # Fighters + Winner
    persons = soup.select("div.b-fight-details__person")
    if len(persons) != 2:
        print("Bad fighter structure:", fight_url)
        return None

    red_name = safe_text(persons[0].select_one("h3.b-fight-details__person-name"))
    blue_name = safe_text(persons[1].select_one("h3.b-fight-details__person-name"))

    red_status = safe_text(persons[0].select_one("i.b-fight-details__person-status")) or ""
    blue_status = safe_text(persons[1].select_one("i.b-fight-details__person-status")) or ""

    winner = "red" if red_status == "W" else "blue" if blue_status == "W" else "none"

    # Method / Round / Time
    fight_info = soup.select_one("div.b-fight-details__content")
    method = round_ = time_ = None

    if fight_info:
        method_tag = fight_info.select_one("i.b-fight-details__text-item_first")
        if method_tag:
            method = method_tag.text.replace("Method:", "").strip()

        for item in fight_info.select("i.b-fight-details__text-item"):
            t = item.text.strip()
            if t.startswith("Round:"):
                round_ = t.replace("Round:", "").strip()
            if t.startswith("Time:"):
                time_ = t.replace("Time:", "").strip()

    # Totals table
    totals_section = soup.find("p", string=lambda x: x and "Totals" in x)
    if not totals_section:
        print(f"[SKIP] Totals section missing (likely old fight with no stats): {fight_url}")
        return None

    totals_table = totals_section.find_next("table")
    rows = totals_table.select("tbody tr")

    labels = ["KD", "SIG_STR", "SIG_STR_pct", "TOTAL_STR",
              "TD", "TD_pct", "SUB_ATT", "REV", "CTRL"]

    stats = {}

    if len(rows) == 1:
        tds = rows[0].find_all("td")[1:]
        for i, td in enumerate(tds):
            ps = td.find_all("p")
            red = ps[0].text.strip() if len(ps) else None
            blue = ps[1].text.strip() if len(ps) > 1 else None
            stats[f"red_{labels[i]}"] = red
            stats[f"blue_{labels[i]}"] = blue
    else:
        print(f"[SKIP] Unexpected totals row count ({len(rows)}): {fight_url}")
        return None

    return {
        "fight_url": fight_url,
        "event_url": event_url,
        "date": date,
        "winner": winner,
        "red_name": red_name,
        "blue_name": blue_name,
        "method": method,
        "round": round_,
        "time": time_,
        **stats
    }


def scrape_all(threads=8):
    events = get_all_event_links()

    fight_links = set()
    for e in events:
        for f in get_event_fights(e):
            fight_links.add(f)

    fight_links = sorted(fight_links)
    print(f"[+] Total fight links: {len(fight_links)}")

    cache_stats(fight_links)

    data = []
    completed = 0
    total = len(fight_links)

    print(f"[+] Starting multithreaded scrape with {threads} workers...")

    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
        futures = {executor.submit(parse_fight, url): url for url in fight_links}

        for future in concurrent.futures.as_completed(futures):
            url = futures[future]
            completed += 1

            try:
                result = future.result()
                if result:
                    data.append(result)
            except Exception as e:
                print(f"[ERROR] scrape failed {url}: {e}")
                traceback.print_exc()

            if completed % 100 == 0 or completed == total:
                print(f"[PROGRESS] {completed}/{total} fights processed ({len(data)} with data)")

    df = pd.DataFrame(data)
    df.to_csv("ufc_fight_data.csv", index=False)
    print(f"[+] Saved ufc_fight_data.csv — {len(df)} rows")


if __name__ == "__main__":
    scrape_all()