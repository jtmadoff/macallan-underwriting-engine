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

def main():
    api_key = os.getenv("MONDAY_API_KEY")
    board_id = os.getenv("MONDAY_BOARD_ID")
    dry_run = os.getenv("DRY_RUN") == "1"

    if not api_key:
        raise RuntimeError("MONDAY_API_KEY is not set in the environment.")
    if not board_id:
        raise RuntimeError("MONDAY_BOARD_ID is not set in the environment.")

    headers = {
        "Authorization": api_key,
        "Content-Type": "application/json",
    }
    # Use items_page for modern Monday.com API
    query = f"""
    query {{
      boards(ids: {board_id}) {{
        items_page(limit: 100) {{
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
    }}
    """
    resp = http_post_with_retries("https://api.monday.com/v2", {"query": query}, headers)
    data = resp.json()
    print("DEBUG: Board ID used:", board_id)
    print("DEBUG: API response:", json.dumps(data, indent=2))
    boards = data.get("data", {}).get("boards", [])
    if not boards or "items_page" not in boards[0] or "items" not in boards[0]["items_page"]:
        print("No boards returned, or items_page/items missing.")
        return
    items = boards[0]["items_page"]["items"]

    # Use internal column IDs
    COL_EQUITY_INVESTMENT = "text_mkx8fh02"
    COL_YEAR_1_CF         = "text_mkx8wy34"
    COL_YEAR_2_CF         = "text_mkx85r06"
    COL_YEAR_3_CF         = "text_mkx853zq"
    COL_YEAR_4_CF         = "text_mkx8cq2k"
    COL_YEAR_5_CF         = "text_mkx8p02k"
    COL_SALE_PROCEEDS     = "text_mkx846jm"
    COL_IRR               = "text_mkx86wgr"
    COL_EQUITY_MULTIPLE   = "text_mkx8xted"

    for item in items:
        cv = {c["id"]: c.get("text") for c in item.get("column_values", [])}
        try:
            equity = abs(safe_number(cv.get(COL_EQUITY_INVESTMENT)))
            y1 = safe_number(cv.get(COL_YEAR_1_CF))
            y2 = safe_number(cv.get(COL_YEAR_2_CF))
            y3 = safe_number(cv.get(COL_YEAR_3_CF))
            y4 = safe_number(cv.get(COL_YEAR_4_CF))
            y5 = safe_number(cv.get(COL_YEAR_5_CF))
            sale = safe_number(cv.get(COL_SALE_PROCEEDS))
            cashflows = [-equity, y1, y2, y3, y4, y5 + sale]
            irr = npf.irr(cashflows)
            irr_value = None if irr is None or (isinstance(irr, float) and math.isnan(irr)) else irr*100.0
            em = sum(cashflows[1:]) / equity if equity > 0 else None

            column_values = {}
            if irr_value is not None:
                column_values[COL_IRR] = {"number": f"{irr_value:.2f}"}
            else:
                column_values[COL_IRR] = {}
            if em is not None:
                column_values[COL_EQUITY_MULTIPLE] = {"number": f"{em:.2f}"}
            else:
                column_values[COL_EQUITY_MULTIPLE] = {}

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
