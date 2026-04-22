# MedEx Scraper -> POS Template Output

This repository runs a MedEx scraper on GitHub Actions and saves the result as an Excel file already mapped to this POS column structure:

- name
- category
- cost price
- sales price
- unit
- generic name
- manufacturer
- Dosage
- SKU
- DAR number

## Sheets in output workbook

### Sheet1
Main POS-ready sheet in the mapped template format.

### MedEx_Raw
Raw scraped MedEx fields for review and traceability.

### Summary
Basic run totals and price coverage summary.

## How to run

1. Open the repository on GitHub
2. Click the **Actions** tab
3. Open **Scrape MedEx Manual** or **Scrape MedEx Parallel**
4. Click **Run workflow**
5. Enter the page range if using manual mode
6. Wait for the workflow to finish
7. Download the Excel file from the **Artifacts** section

## Example ranges

- 1 to 2 for testing
- 1 to 30
- 31 to 60
- 61 to 90

## Important notes

- `sales price` is populated from MedEx unit price first, then strip price if unit price is unavailable.
- `cost price`, `category`, `SKU`, and `DAR number` are intentionally left blank by default because they cannot be safely derived from MedEx alone.
- `unit` is inferred conservatively from dosage form.
