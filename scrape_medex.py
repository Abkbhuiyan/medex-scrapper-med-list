import argparse
import os
import re
import time
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup

BASE_LIST_URL = "https://medex.com.bd/brands?page={}"
BASE_SITE = "https://medex.com.bd"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    )
}

session = requests.Session()
session.headers.update(HEADERS)


def safe_get(url, retries=3, sleep_sec=1.5, timeout=30):
    for attempt in range(1, retries + 1):
        try:
            response = session.get(url, timeout=timeout)
            if response.status_code == 200:
                return response
            print(f"[WARN] {url} returned {response.status_code} (attempt {attempt}/{retries})")
        except Exception as exc:
            print(f"[WARN] Error fetching {url} (attempt {attempt}/{retries}): {exc}")
        time.sleep(sleep_sec * attempt)
    return None


def clean_text(text):
    if not text:
        return ""
    return re.sub(r"\s+", " ", str(text)).strip()


def normalize_strength(text):
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


def split_brand_name_and_strength(title_text):
    if not title_text:
        return "", "", ""

    parts = [clean_text(x) for x in title_text.split("|") if clean_text(x)]
    brand = parts[0] if len(parts) >= 1 else ""
    strength = parts[1] if len(parts) >= 2 else ""
    dosage = parts[2] if len(parts) >= 3 else ""
    return brand, strength, dosage


def parse_brand_list_page(html):
    soup = BeautifulSoup(html, "lxml")
    links = []

    for anchor in soup.find_all("a", class_="hoverable-block"):
        href = anchor.get("href", "")
        if href and "/brands/" in href:
            links.append(urljoin(BASE_SITE, href))

    seen = set()
    unique_links = []
    for link in links:
        if link not in seen:
            seen.add(link)
            unique_links.append(link)
    return unique_links


def extract_unit_price(soup):
    text = soup.get_text("\n", strip=True)
    match = re.search(r"Unit\s*Price:\s*৳\s*([\d,]+(?:\.\d+)?)", text, flags=re.IGNORECASE)
    return match.group(1).replace(",", "") if match else ""


def extract_strip_price(soup):
    text = soup.get_text("\n", strip=True)
    match = re.search(r"Strip\s*Price:\s*৳\s*([\d,]+(?:\.\d+)?)", text, flags=re.IGNORECASE)
    return match.group(1).replace(",", "") if match else ""


def parse_available_as_variants(soup, base_brand_name):
    variants = []
    text_nodes = soup.find_all(string=lambda value: value and "Also available as" in value)
    if not text_nodes:
        return variants

    label_node = text_nodes[0]
    parent = label_node.parent
    candidate_links = []
    next_element = parent.find_next()
    scan_limit = 0

    while next_element and scan_limit < 20:
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

        full_url = urljoin(BASE_SITE, href)
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


def detect_dosage_form(soup, fallback=""):
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


def parse_product_page(html, url):
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
    strength = normalize_strength(strength_from_title)

    generic_link = soup.find("a", href=lambda value: value and "/generics/" in value)
    if generic_link:
        data["generic_name"] = clean_text(generic_link.get_text(" ", strip=True))

    company_link = soup.find("a", href=lambda value: value and "/companies/" in value)
    if company_link:
        data["manufacturer"] = clean_text(company_link.get_text(" ", strip=True))

    data["unit_price"] = extract_unit_price(soup)
    data["strip_price"] = extract_strip_price(soup)
    dosage_form = detect_dosage_form(soup, dosage_from_title)

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


def scrape_medex_pages(start_page, end_page, sleep_between_products=0.7, sleep_between_pages=1.5):
    print(f"Scraping brand pages {start_page} to {end_page}")

    all_rows = []
    seen_product_urls = set()
    seen_variant_urls = set()

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
            if brand_url in seen_product_urls:
                continue

            print(f"    [{index}/{len(brand_links)}] Product: {brand_url}")
            seen_product_urls.add(brand_url)

            product_response = safe_get(brand_url)
            if not product_response:
                print("      [SKIP] Could not fetch product page")
                continue

            try:
                product_data, variants = parse_product_page(product_response.text, brand_url)
                product_data["source_type"] = "main_page"
                all_rows.append(product_data)

                if variants:
                    print(f"      Found {len(variants)} variants in 'Also available as'")

                for variant in variants:
                    variant_url = variant["variant_url"]
                    if variant_url in seen_variant_urls or variant_url in seen_product_urls:
                        continue

                    seen_variant_urls.add(variant_url)
                    variant_response = safe_get(variant_url)

                    if not variant_response:
                        all_rows.append(
                            {
                                "product_name": variant["variant_name"],
                                "base_brand": product_data["base_brand"],
                                "generic_name": product_data["generic_name"],
                                "strength": variant["variant_strength"],
                                "dosage_form": variant["variant_dosage"],
                                "manufacturer": product_data["manufacturer"],
                                "unit_price": "",
                                "strip_price": "",
                                "brand_url": variant_url,
                                "has_available_as": False,
                                "available_as_count": 0,
                                "source_type": "variant_from_available_as_fallback",
                            }
                        )
                        continue

                    try:
                        variant_data, _ = parse_product_page(variant_response.text, variant_url)
                        variant_data["source_type"] = "variant_page"
                        all_rows.append(variant_data)
                    except Exception as exc:
                        print(f"      [WARN] Error parsing variant page {variant_url}: {exc}")
                        all_rows.append(
                            {
                                "product_name": variant["variant_name"],
                                "base_brand": product_data["base_brand"],
                                "generic_name": product_data["generic_name"],
                                "strength": variant["variant_strength"],
                                "dosage_form": variant["variant_dosage"],
                                "manufacturer": product_data["manufacturer"],
                                "unit_price": "",
                                "strip_price": "",
                                "brand_url": variant_url,
                                "has_available_as": False,
                                "available_as_count": 0,
                                "source_type": "variant_from_available_as_fallback",
                            }
                        )

                time.sleep(sleep_between_products)

            except Exception as exc:
                print(f"      [WARN] Error parsing product page {brand_url}: {exc}")

        time.sleep(sleep_between_pages)

    dataframe = pd.DataFrame(all_rows)

    if not dataframe.empty:
        before = len(dataframe)
        dataframe = dataframe.drop_duplicates(subset=["brand_url"], keep="first")
        dataframe = dataframe.drop_duplicates(subset=["product_name", "manufacturer", "dosage_form"], keep="first")
        after = len(dataframe)
        print(f"\nDuplicates removed: {before - after}")
        dataframe = dataframe.reset_index(drop=True)

    return dataframe


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-page", type=int, required=True)
    parser.add_argument("--end-page", type=int, required=True)
    args = parser.parse_args()

    if args.start_page > args.end_page:
        raise ValueError("start-page cannot be greater than end-page")

    dataframe = scrape_medex_pages(start_page=args.start_page, end_page=args.end_page)

    output_dir = "output"
    os.makedirs(output_dir, exist_ok=True)

    file_name = f"medex_brands_pages_{args.start_page}_to_{args.end_page}.xlsx"
    output_path = os.path.join(output_dir, file_name)

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        dataframe.to_excel(writer, index=False, sheet_name="Medicines")

    print(f"\nSaved Excel file: {output_path}")
    print(f"Total rows: {len(dataframe)}")


if __name__ == "__main__":
    main()
