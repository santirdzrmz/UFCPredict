import requests
from bs4 import BeautifulSoup
import pandas as pd
import time

BASE = "http://ufcstats.com"

def get_soup(url):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/123.0 Safari/537.36"
    }
    for _ in range(5):
        try:
            r = requests.get(url, headers=headers, timeout=10)
            if r.status_code == 200:
                return BeautifulSoup(r.text, "html.parser")
        except:
            pass
        time.sleep(1)
    print(f"[ERROR] Could not fetch {url}")
    return None


# ------------------------------------------------------------
# NORMALIZE URL
# ------------------------------------------------------------
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


# ------------------------------------------------------------
# GET ALL FIGHTERS (A–Z)
# ------------------------------------------------------------
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


# ------------------------------------------------------------
# PARSE SINGLE FIGHTER PAGE
# ------------------------------------------------------------
def parse_fighter(url):
    soup = get_soup(url)
    if not soup:
        return None

    # ---------------------------
    # NAME + RECORD
    # ---------------------------
    name = soup.select_one("span.b-content__title-highlight")
    name = name.text.strip() if name else None

    record = soup.select_one("span.b-content__title-record")
    record = record.text.replace("Record:", "").strip() if record else None

    # ---------------------------
    # PERSONAL INFO (Height, Weight, Reach, Stance, DOB)
    # ---------------------------
    info = soup.select("div.b-list__info-box ul.b-list__box-list li")

    def extract(label):
        for li in info:
            title = li.select_one("i.b-list__box-item-title")
            if not title:
                continue
            if title.text.strip().lower().startswith(label.lower()):
                # Value is the remaining text after the label
                raw = li.text.replace(title.text, "").strip()
                return raw if raw != "" else None
        return None

    height = extract("Height")
    weight = extract("Weight")
    reach = extract("Reach")
    stance = extract("STANCE")
    dob = extract("DOB")

    # ---------------------------
    # CAREER STATS (Left Side)
    # ---------------------------
    left_stats = soup.select("div.b-list__info-box-left li")

    def get_left(prefix):
        for li in left_stats:
            t = li.text.strip()
            if t.startswith(prefix):
                return t.replace(prefix, "").strip()
        return None

    SLpM     = get_left("SLpM:")
    Str_Acc  = get_left("Str. Acc.:")
    SApM     = get_left("SApM:")
    Str_Def  = get_left("Str. Def.")

    # ---------------------------
    # CAREER STATS (Right Side)
    # ---------------------------
    right_stats = soup.select("div.b-list__info-box-right li")

    def get_right(prefix):
        for li in right_stats:
            t = li.text.strip()
            if t.startswith(prefix):
                return t.replace(prefix, "").strip()
        return None

    TD_Avg   = get_right("TD Avg.:")
    TD_Acc   = get_right("TD Acc.:")
    TD_Def   = get_right("TD Def.:")
    Sub_Avg  = get_right("Sub. Avg.:")

    return {
        "fighter_url": url,
        "name": name,
        "record": record,
        "height": height,
        "weight": weight,
        "reach": reach,
        "stance": stance,
        "dob": dob,
        "SLpM": SLpM,
        "SApM": SApM,
        "Str_Acc": Str_Acc,
        "Str_Def": Str_Def,
        "TD_Avg": TD_Avg,
        "TD_Acc": TD_Acc,
        "TD_Def": TD_Def,
        "Sub_Avg": Sub_Avg
    }


# ------------------------------------------------------------
# MAIN SCRAPER
# ------------------------------------------------------------
def scrape_fighters(save_every=1000):
    fighter_links = get_all_fighter_links()
    print(f"[+] Found {len(fighter_links)} fighters")

    data = []
    for i, url in enumerate(fighter_links, start=1):
        print(f"[{i}/{len(fighter_links)}] {url}")
        info = parse_fighter(url)
        if info:
            data.append(info)
        time.sleep(0.1)
        
        if (i + 1) % save_every == 0:
            df_partial = pd.DataFrame(data)
            partial_name = f"ufc_fighter_data_partial_{i+1}.csv"
            df_partial.to_csv(partial_name, index=False)
            print(f"[+] Saved partial CSV: {partial_name} (fights scraped: {i+1})")

    df = pd.DataFrame(data)
    df.to_csv("ufc_fighter_stats.csv", index=False)
    print("[+] Saved ufc_fighter_stats.csv")


if __name__ == "__main__":
    scrape_fighters()
