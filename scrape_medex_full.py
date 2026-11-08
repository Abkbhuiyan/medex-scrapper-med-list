import argparse
import json
import os
import random
import re
import time
from collections import deque
from urllib.parse import urljoin, urlparse, urlunparse

import pandas as pd
import requests
from bs4 import BeautifulSoup
from openpyxl.styles import Alignment, Font, PatternFill

BASE_LIST_URL = "https://medex.com.bd/brands?page={}"
BASE_SITE = "https://medex.com.bd"

TEMPLATE_COLUMNS = [
    "name",
    "category",
    "cost price",
    "sales price",
    "unit",
    "generic name",
    "manufacturer",
    "Dosage",
    "SKU",
    "DAR number",
]

RAW_COLUMNS = [
    "product_name",
    "base_brand",
    "generic_name",
    "strength",
    "dosage_form",
    "manufacturer",
    "unit_price",
    "strip_price",
    "brand_url",
    "has_available_as",
    "available_as_count",
    "source_type",
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    )
}


def get_env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or str(value).strip() == "":
        return int(default)
    return int(value)


def get_env_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None or str(value).strip() == "":
        return float(default)
    return float(value)


session = requests.Session()
session.headers.update(HEADERS)


def load_cookies_from_env_or_file():
    """Load MedEx cookies so requests can pass the site security check."""
    cookie_header = os.getenv("MEDEX_COOKIE", "").strip()
    if cookie_header:
        for part in cookie_header.split(";"):
            if "=" not in part:
                continue
            name, value = part.split("=", 1)
            session.cookies.set(name.strip(), value.strip(), domain="medex.com.bd")
        print("[INFO] Loaded cookies from MEDEX_COOKIE")
        return

    cookie_file = os.getenv("MEDEX_COOKIE_FILE", "medex_cookies.json").strip()
    if cookie_file and os.path.exists(cookie_file):
        with open(cookie_file, encoding="utf-8") as fh:
            data = json.load(fh)
        if isinstance(data, dict):
            for name, value in data.items():
                session.cookies.set(str(name), str(value), domain="medex.com.bd")
            print(f"[INFO] Loaded cookies from {cookie_file}")


def is_security_challenge(response: requests.Response) -> bool:
    if response is None:
        return False
    if "captcha-challenge" in (response.url or ""):
        return True
    text_head = response.text[:4000].lower() if response.text else ""
    return "security check" in text_head and ("captcha" in text_head or "turnstile" in text_head or "unusual traffic" in text_head)


def safe_get(url: str, retries=None, sleep_sec=None, timeout=None):
    retries = get_env_int("MAX_RETRIES", 5) if retries is None else retries
    sleep_sec = get_env_float("INITIAL_BACKOFF_SECONDS", 2.0) if sleep_sec is None else sleep_sec
    timeout = get_env_int("REQUEST_TIMEOUT_SECONDS", 30) if timeout is None else timeout
    jitter = get_env_float("RANDOM_JITTER_SECONDS", 0.8)
    challenge_wait = get_env_float("SECURITY_CHALLENGE_WAIT_SECONDS", 180.0)

    retryable_statuses = {403, 429, 500, 502, 503, 504}

    for attempt in range(1, retries + 1):
        try:
            response = session.get(url, timeout=timeout)

            if response.status_code == 200:
                if is_security_challenge(response):
                    wait_time = challenge_wait + random.uniform(0, jitter)
                    print(
                        f"[WARN] Security check intercepted {url} "
                        f"(attempt {attempt}/{retries}) -> sleeping {wait_time:.2f}s. "
                        f"Refresh medex_cookies.json in the browser if this keeps happening."
                    )
                    time.sleep(wait_time)
                    continue
                return response

            if response.status_code in retryable_statuses:
                wait_time = (sleep_sec * attempt) + random.uniform(0, jitter)
                print(
                    f"[WARN] {url} returned {response.status_code} "
                    f"(attempt {attempt}/{retries}) -> sleeping {wait_time:.2f}s"
                )
                time.sleep(wait_time)
                continue

            print(
                f"[WARN] {url} returned non-retryable status {response.status_code} "
                f"(attempt {attempt}/{retries})"
            )
            return None

        except Exception as exc:
            wait_time = (sleep_sec * attempt) + random.uniform(0, jitter)
            print(
                f"[WARN] Error fetching {url} (attempt {attempt}/{retries}): {exc} "
                f"-> sleeping {wait_time:.2f}s"
            )
            time.sleep(wait_time)

    return None


def clean_text(text):
    if text is None:
        return ""
    return re.sub(r"\s+", " ", str(text)).strip()


def canonicalize_brand_url(url: str) -> str:
    """Normalize brand URLs so language suffixes and query params do not create duplicates."""
    if not url:
        return ""

    parsed = urlparse(urljoin(BASE_SITE, url))
    path = re.sub(r"/(bn|en)/?$", "", parsed.path)
    return urlunparse((parsed.scheme, parsed.netloc, path, "", "", ""))


def normalize_strength(text: str) -> str:
    if not text:
        return ""

    text = clean_text(text)

    patterns = [
        r"\d+(?:\.\d+)?\s*(?:mg|mcg|µg|g|kg|ml|l|IU|%|units?)\s*/\s*\d+(?:\.\d+)?\s*(?:ml|l|g)",
        r"\d+(?:\.\d+)?\s*(?:mg|mcg|µg|g|kg|ml|l|IU|%|units?)",
    ]

    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            return clean_text(match.group(0))

    return ""


def split_brand_name_and_strength(title_text: str):
    if not title_text:
        return "", "", ""

    parts = [clean_text(x) for x in title_text.split("|") if clean_text(x)]
    brand = parts[0] if len(parts) >= 1 else ""
    strength = parts[1] if len(parts) >= 2 else ""
    dosage = parts[2] if len(parts) >= 3 else ""
    return brand, strength, dosage


def parse_brand_list_page(html: str):
    soup = BeautifulSoup(html, "lxml")
    links = []

    # MedEx currently uses a.brand-card; older pages used a.hoverable-block.
    anchors = soup.find_all("a", class_="brand-card")
    if not anchors:
        anchors = soup.find_all("a", class_="hoverable-block")

    for anchor in anchors:
        href = anchor.get("href", "")
        if href and "/brands/" in href:
            links.append(canonicalize_brand_url(href))

    if not links:
        for anchor in soup.find_all("a", href=True):
            href = anchor.get("href", "")
            if re.search(r"/brands/\d+/", href):
                links.append(canonicalize_brand_url(href))

    seen = set()
    unique_links = []
    for link in links:
        if link and link not in seen:
            seen.add(link)
            unique_links.append(link)

    return unique_links


def extract_unit_price(soup: BeautifulSoup) -> str:
    text = soup.get_text("\n", strip=True)
    match = re.search(
        r"Unit\s*Price:\s*(?:\n\s*)?৳\s*([\d,]+(?:\.\d+)?)",
        text,
        flags=re.IGNORECASE,
    )
    return match.group(1).replace(",", "") if match else ""


def extract_strip_price(soup: BeautifulSoup) -> str:
    text = soup.get_text("\n", strip=True)
    match = re.search(
        r"Strip\s*Price:\s*(?:\n\s*)?৳\s*([\d,]+(?:\.\d+)?)",
        text,
        flags=re.IGNORECASE,
    )
    return match.group(1).replace(",", "") if match else ""


def parse_available_as_variants(soup: BeautifulSoup, base_brand_name: str):
    variants = []

    text_nodes = soup.find_all(string=lambda value: value and "Also available as" in value)
    if not text_nodes:
        return variants

    label_node = text_nodes[0]
    parent = label_node.parent
    candidate_links = []
    next_element = parent.find_next()
    scan_limit = 0

    while next_element and scan_limit < 30:
        scan_limit += 1

        if hasattr(next_element, "name") and next_element.name in ["h2", "h3", "h4"]:
            heading_text = clean_text(next_element.get_text(" ", strip=True)).lower()
            if "also available as" not in heading_text:
                break

        if hasattr(next_element, "find_all"):
            links = next_element.find_all("a", href=True)
            if links:
                candidate_links.extend(links)

        next_element = next_element.find_next()

    for anchor in candidate_links:
        href = anchor.get("href", "")
        text = clean_text(anchor.get_text(" ", strip=True))
        if not href or "/brands/" not in href or not text:
            continue

        full_url = canonicalize_brand_url(href)
        if not full_url:
            continue

        strength = normalize_strength(text)

        dosage = ""
        dosage_match = re.search(r"\((.*?)\)", text)
        if dosage_match:
            dosage = clean_text(dosage_match.group(1))

        variant_name = clean_text(f"{base_brand_name} {strength}") if strength else base_brand_name

        variants.append(
            {
                "variant_name": variant_name,
                "variant_strength": strength,
                "variant_dosage": dosage,
                "variant_url": full_url,
            }
        )

    deduped = []
    seen = set()
    for variant in variants:
        key = variant["variant_url"]
        if key not in seen:
            seen.add(key)
            deduped.append(variant)

    return deduped


def detect_dosage_form(soup: BeautifulSoup, fallback: str = "") -> str:
    if fallback:
        return clean_text(fallback)

    body_text = soup.get_text("\n", strip=True)
    dosage_candidates = [
        "Chewable Tablet",
        "Chew. Tablet",
        "Flash Tablet",
        "Tablet",
        "Capsule",
        "Syrup",
        "Suspension",
        "Injection",
        "Cream",
        "Ointment",
        "Gel",
        "Drops",
        "Powder",
        "Solution",
        "Inhaler",
        "Suppository",
        "Sachet",
        "Lotion",
        "Spray",
    ]

    for dosage in dosage_candidates:
        if re.search(rf"\b{re.escape(dosage)}\b", body_text, flags=re.IGNORECASE):
            return dosage
    return ""


def normalize_dosage_form(value: str) -> str:
    value = clean_text(value)
    mapping = {
        "Chew. Tablet": "Chewable Tablet",
        "Cap.": "Capsule",
        "Tab.": "Tablet",
        "Inj.": "Injection",
    }
    return mapping.get(value, value)


def infer_unit(dosage_form: str, unit_price: str, strip_price: str) -> str:
    dosage = clean_text(dosage_form).lower()

    if any(token in dosage for token in ["tablet", "capsule", "caplet", "suppository"]):
        return "pcs" if unit_price else ("strip" if strip_price else "pcs")
    if "sachet" in dosage:
        return "sachet"
    if any(token in dosage for token in ["syrup", "suspension", "solution", "drops", "lotion", "spray"]):
        return "bottle"
    if any(token in dosage for token in ["cream", "ointment", "gel"]):
        return "tube"
    if "inhaler" in dosage:
        return "inhaler"
    if "injection" in dosage:
        return "vial"
    if "powder" in dosage:
        return "pack"

    return ""


def select_sales_price(unit_price: str, strip_price: str) -> str:
    if unit_price not in [None, ""]:
        return unit_price
    if strip_price not in [None, ""]:
        return strip_price
    return ""


def parse_product_page(html: str, url: str):
    soup = BeautifulSoup(html, "lxml")

    data = {
        "product_name": "",
        "base_brand": "",
        "generic_name": "",
        "strength": "",
        "dosage_form": "",
        "manufacturer": "",
        "unit_price": "",
        "strip_price": "",
        "brand_url": url,
        "has_available_as": False,
        "available_as_count": 0,
    }

    title_tag = soup.find("title")
    page_title = clean_text(title_tag.get_text(" ", strip=True)) if title_tag else ""
    brand_from_title, strength_from_title, dosage_from_title = split_brand_name_and_strength(page_title)

    h1 = soup.find("h1")
    heading_text = clean_text(h1.get_text(" ", strip=True)) if h1 else ""

    base_brand = brand_from_title.strip() if brand_from_title else heading_text
    if strength_from_title and "+" in strength_from_title:
        strength = clean_text(strength_from_title)
    else:
        strength = normalize_strength(strength_from_title)

    generic_link = soup.find("a", href=lambda value: value and "/generics/" in value)
    if generic_link:
        data["generic_name"] = clean_text(generic_link.get_text(" ", strip=True))

    company_link = soup.find("a", href=lambda value: value and "/companies/" in value)
    if company_link:
        data["manufacturer"] = clean_text(company_link.get_text(" ", strip=True))

    data["unit_price"] = extract_unit_price(soup)
    data["strip_price"] = extract_strip_price(soup)

    dosage_form = normalize_dosage_form(detect_dosage_form(soup, dosage_from_title))

    if base_brand and strength:
        product_name = clean_text(f"{base_brand} {strength}")
    elif heading_text:
        product_name = heading_text
    else:
        product_name = base_brand

    data["product_name"] = product_name
    data["base_brand"] = base_brand
    data["strength"] = strength
    data["dosage_form"] = dosage_form

    variants = parse_available_as_variants(soup, base_brand)
    data["has_available_as"] = len(variants) > 0
    data["available_as_count"] = len(variants)

    return data, variants


def map_to_pos_row(row: dict):
    dosage = normalize_dosage_form(row.get("dosage_form", ""))
    unit_price = row.get("unit_price", "")
    strip_price = row.get("strip_price", "")
    sales_price = select_sales_price(unit_price, strip_price)

    return {
        "name": clean_text(row.get("product_name", "")),
        "category": "",
        "cost price": "",
        "sales price": sales_price,
        "unit": infer_unit(dosage, unit_price, strip_price),
        "generic name": clean_text(row.get("generic_name", "")),
        "manufacturer": clean_text(row.get("manufacturer", "")),
        "Dosage": dosage,
        "SKU": "",
        "DAR number": "",
    }


def crawl_product_cluster(seed_url: str, sleep_between_requests=None):
    sleep_between_requests = (
        get_env_float("SLEEP_BETWEEN_PRODUCTS", 1.2)
        if sleep_between_requests is None
        else sleep_between_requests
    )
    jitter = get_env_float("RANDOM_JITTER_SECONDS", 0.8)
    # Full-catalog runs already visit every brand list page, so variant hops are optional.
    follow_variants = os.getenv("FOLLOW_VARIANTS", "0").strip() not in {"0", "false", "False"}

    cluster_rows = []
    seed_url = canonicalize_brand_url(seed_url)
    queue = deque([seed_url])
    visited_urls = set()

    while queue:
        current_url = canonicalize_brand_url(queue.popleft())
        if not current_url or current_url in visited_urls:
            continue

        visited_urls.add(current_url)
        response = safe_get(current_url)
        if not response:
            print(f"      [SKIP] Could not fetch product page {current_url}")
            continue

        try:
            product_data, variants = parse_product_page(response.text, current_url)
            product_data["brand_url"] = canonicalize_brand_url(product_data.get("brand_url", current_url))
            product_data["source_type"] = "main_page" if current_url == seed_url else "variant_page"
            cluster_rows.append(product_data)

            if follow_variants and variants:
                print(f"      Found {len(variants)} variants in 'Also available as' for {current_url}")
                for variant in variants:
                    variant_url = canonicalize_brand_url(variant["variant_url"])
                    if variant_url and variant_url not in visited_urls:
                        queue.append(variant_url)

        except Exception as exc:
            print(f"      [WARN] Error parsing product page {current_url}: {exc}")

        wait_time = sleep_between_requests + random.uniform(0, jitter)
        time.sleep(wait_time)

    return cluster_rows


def scrape_medex_pages(start_page: int, end_page: int, sleep_between_products=None, sleep_between_pages=None):
    sleep_between_products = (
        get_env_float("SLEEP_BETWEEN_PRODUCTS", 1.2)
        if sleep_between_products is None
        else sleep_between_products
    )
    sleep_between_pages = (
        get_env_float("SLEEP_BETWEEN_PAGES", 3.0)
        if sleep_between_pages is None
        else sleep_between_pages
    )

    print(f"Scraping brand pages {start_page} to {end_page}")
    print(
        f"Configured delays: products={sleep_between_products}s, "
        f"pages={sleep_between_pages}s"
    )

    all_rows = []
    globally_seen_urls = set()

    for page in range(start_page, end_page + 1):
        list_url = BASE_LIST_URL.format(page)
        print(f"\n[LIST] Page {page}/{end_page}: {list_url}")

        response = safe_get(list_url)
        if not response:
            print(f"[SKIP] Failed to fetch list page {page}")
            continue

        brand_links = parse_brand_list_page(response.text)
        print(f"  Found {len(brand_links)} brand links")

        for index, brand_url in enumerate(brand_links, start=1):
            if brand_url in globally_seen_urls:
                continue

            print(f"    [{index}/{len(brand_links)}] Product: {brand_url}")

            cluster_rows = crawl_product_cluster(
                seed_url=brand_url,
                sleep_between_requests=sleep_between_products,
            )

            for row in cluster_rows:
                brand_row_url = row.get("brand_url", "")
                if brand_row_url and brand_row_url not in globally_seen_urls:
                    globally_seen_urls.add(brand_row_url)
                    all_rows.append(row)

        page_wait = sleep_between_pages + random.uniform(
            0, get_env_float("RANDOM_JITTER_SECONDS", 0.8)
        )
        print(f"  Sleeping {page_wait:.2f}s before next list page")
        time.sleep(page_wait)

    dataframe = pd.DataFrame(all_rows)

    if not dataframe.empty:
        before = len(dataframe)

        dataframe = dataframe.drop_duplicates(subset=["brand_url"], keep="first")
        dataframe = dataframe.drop_duplicates(
            subset=["product_name", "manufacturer", "dosage_form"],
            keep="first",
        )

        after = len(dataframe)
        print(f"\nDuplicates removed: {before - after}")
        dataframe = dataframe.reset_index(drop=True)

    return dataframe


def autosize_worksheet_columns(worksheet, min_width=12, max_width=40):
    for column_cells in worksheet.columns:
        values = [clean_text(cell.value) for cell in column_cells if cell.value is not None]
        max_length = max((len(value) for value in values), default=0)
        adjusted_width = min(max(max_length + 2, min_width), max_width)
        worksheet.column_dimensions[column_cells[0].column_letter].width = adjusted_width


def style_header_row(worksheet):
    header_font = Font(bold=True, color="000000")
    header_fill = PatternFill(fill_type="solid", fgColor="D9EAF7")
    header_alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)

    for cell in worksheet[1]:
        cell.font = header_font
        cell.fill = header_fill
        cell.alignment = header_alignment


def build_output_workbook(raw_dataframe: pd.DataFrame, output_path: str):
    if raw_dataframe.empty:
        raw_dataframe = pd.DataFrame(columns=RAW_COLUMNS)
        pos_dataframe = pd.DataFrame(columns=TEMPLATE_COLUMNS)
    else:
        raw_dataframe = raw_dataframe.copy()
        pos_dataframe = pd.DataFrame([map_to_pos_row(row) for row in raw_dataframe.to_dict("records")])
        pos_dataframe = pos_dataframe[TEMPLATE_COLUMNS].drop_duplicates().reset_index(drop=True)

    summary_rows = [
        ["total raw rows", len(raw_dataframe)],
        ["total pos rows", len(pos_dataframe)],
        [
            "rows with sales price",
            int(pos_dataframe["sales price"].astype(str).str.strip().ne("").sum()) if not pos_dataframe.empty else 0,
        ],
        [
            "rows without sales price",
            int(pos_dataframe["sales price"].astype(str).str.strip().eq("").sum()) if not pos_dataframe.empty else 0,
        ],
        [
            "rows with dosage",
            int(pos_dataframe["Dosage"].astype(str).str.strip().ne("").sum()) if not pos_dataframe.empty else 0,
        ],
    ]
    summary_dataframe = pd.DataFrame(summary_rows, columns=["metric", "value"])

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        pos_dataframe.to_excel(writer, index=False, sheet_name="Sheet1")
        raw_dataframe.to_excel(writer, index=False, sheet_name="MedEx_Raw")
        summary_dataframe.to_excel(writer, index=False, sheet_name="Summary")

        for sheet_name in ["Sheet1", "MedEx_Raw", "Summary"]:
            worksheet = writer.sheets[sheet_name]
            worksheet.freeze_panes = "A2"
            autosize_worksheet_columns(worksheet)
            style_header_row(worksheet)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-page", type=int, required=True)
    parser.add_argument("--end-page", type=int, required=True)
    args = parser.parse_args()

    if args.start_page > args.end_page:
        raise ValueError("start-page cannot be greater than end-page")

    load_cookies_from_env_or_file()

    dataframe = scrape_medex_pages(
        start_page=args.start_page,
        end_page=args.end_page,
    )

    output_dir = "output"
    os.makedirs(output_dir, exist_ok=True)

    file_name = f"medex_pos_mapped_pages_{args.start_page}_to_{args.end_page}.xlsx"
    output_path = os.path.join(output_dir, file_name)

    build_output_workbook(dataframe, output_path)

    print(f"\nSaved Excel file: {output_path}")
    print(f"Total raw rows: {len(dataframe)}")


if __name__ == "__main__":
    main()