import os
import sys
import time
import json
import math
import requests
import numpy_financial as npf

def safe_number(text):
    """Convert text to float safely; treat empty / None as 0.0"""
    if text is None:
        return 0.0
    text = str(text).strip().replace(",", "")
    if text == "":
        return 0.0
    try:
        return float(text)
    except ValueError:
        return 0.0

def http_post_with_retries(url, payload, headers, max_retries=5):
    delay = 1
    for attempt in range(max_retries):
        try:
            resp = requests.post(url, json=payload, headers=headers)
            resp.raise_for_status()
            return resp
        except Exception as e:
            if attempt < max_retries-1:
                time.sleep(delay)
                delay *= 2
            else:
                raise RuntimeError(f"HTTP request failed after {max_retries} attempts: {e}")

def print_columns(items):
    print("Column IDs for mapping (first item):")
    if not items:
        print("No items.")
        return
    cols = items[0].get("column_values", [])
    for col in cols:
        print(f"  id: {col['id']:<20} | text: {col['text']}")

def main():
    api_key = os.getenv("MONDAY_API_KEY")
    board_id = os.getenv("MONDAY_BOARD_ID") or "18320495966"
    dry_run = os.getenv("DRY_RUN") == "1"
    print_columns_flag = os.getenv("PRINT_COLUMNS") == "1"

    # Command line overrides:
    for arg in sys.argv[1:]:
        if "--dry-run" in arg:
            dry_run = True
        if "--print-columns" in arg:
            print_columns_flag = True

    if not api_key:
        raise RuntimeError("MONDAY_API_KEY is not set in the environment.")

    headers = {
        "Authorization": api_key,
        "Content-Type": "application/json",
    }
    query = f"""
    query {{
      boards(ids: {board_id}) {{
        items {{
          id
          name
          column_values {{
            id
            text
          }}
        }}
      }}
    }}
    """
    resp = http_post_with_retries("https://api.monday.com/v2", {"query": query}, headers)
    data = resp.json()
    items = data.get("data", {}).get("boards", [])
    if not items:
        print("No boards returned or board id incorrect.")
        return
    items = items[0].get("items", [])

    if print_columns_flag:
        print_columns(items)
        return

    for item in items:
        cv = {c["id"]: c.get("text") for c in item.get("column_values", [])}
        try:
            # Edit these to match your board's actual column ids if needed
            equity = abs(safe_number(cv.get("equity_investment")))
            y1 = safe_number(cv.get("year_1_cf"))
            y2 = safe_number(cv.get("year_2_cf"))
            y3 = safe_number(cv.get("year_3_cf"))
            y4 = safe_number(cv.get("year_4_cf"))
            y5 = safe_number(cv.get("year_5_cf"))
            sale = safe_number(cv.get("sale_proceeds"))

            cashflows = [-equity, y1, y2, y3, y4, y5 + sale]
            irr = npf.irr(cashflows)
            irr_value = None if irr is None or (isinstance(irr, float) and math.isnan(irr)) else irr*100.0
            em = sum(cashflows[1:]) / equity if equity > 0 else None

            column_values = {}
            if irr_value is not None:
                column_values["irr"] = {"number": f"{irr_value:.2f}"}
            else:
                column_values["irr"] = {}
            if em is not None:
                column_values["equity_multiple"] = {"number": f"{em:.2f}"}
            else:
                column_values["equity_multiple"] = {}

            column_values_str = json.dumps(column_values)
            update_mutation = f"""
            mutation {{
              change_multiple_column_values(item_id: {item['id']}, board_id: {board_id}, column_values: {json.dumps(column_values_str)}) {{
                id
              }}
            }}
            """

            if dry_run:
                print(f"\nWould update item {item['name']} ({item['id']}):\n{update_mutation}")
            else:
                update_resp = http_post_with_retries(
                    "https://api.monday.com/v2",
                    {"query": update_mutation},
                    headers
                )
                update_data = update_resp.json()
                if "errors" in update_data:
                    print(f"Error updating item {item.get('name')} ({item.get('id')}): {update_data['errors']}")

        except Exception as e:
            print(f"Error on {item.get('name')}: {e}")

if __name__ == "__main__":
    main()
