import os
import sys
import time
import json
import math
import requests
import numpy_financial as npf

def safe_number_colval(column_value):
    """Extracts the number from a Monday.com numeric column value dict."""
    if not column_value:
        return 0.0
    val = column_value.get("value")
    if val:
        try:
            val = json.loads(val)
            return float(val)
        except Exception:
            pass
    text = column_value.get("text")
    try:
        return float(text.replace(",", "")) if text else 0.0
    except Exception:
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
    group_id = "group_mkx8xn8e"  # Underwriting Engine group

    if not api_key:
        raise RuntimeError("MONDAY_API_KEY is not set in the environment.")
    if not board_id:
        raise RuntimeError("MONDAY_BOARD_ID is not set in the environment.")

    headers = {
        "Authorization": api_key,
        "Content-Type": "application/json",
    }

    # Query only for items in the specified group
    query = f"""
    query {{
      boards(ids: {board_id}) {{
        groups(ids: ["{group_id}"]) {{
          id
          title
          items {{
            id
            name
            column_values {{
              id
              text
              value
              type
            }}
          }}
        }}
      }}
    }}
    """
    resp = http_post_with_retries("https://api.monday.com/v2", {"query": query}, headers)
    data = resp.json()
    print("DEBUG: Board ID used:", board_id)
    print("DEBUG: Group ID used:", group_id)
    print("DEBUG: API response:", json.dumps(data, indent=2))
    boards = data.get("data", {}).get("boards", [])
    if not boards or "groups" not in boards[0] or not boards[0]["groups"]:
        print("No groups returned, or group missing.")
        return
    group = boards[0]["groups"][0]
    if "items" not in group:
        print("No items returned for group.")
        return
    items = group["items"]

    # Column IDs for required input fields
    COL_NOI                = "numeric_mkxam1rv"
    COL_TOTAL_PROJECT_COST = "numeric_mkx8vtv"
    COL_LOAN_AMOUNT        = "numeric_mkx856za"
    COL_MARKET_CAP_RATE    = "numeric_mkxam49"
    COL_EXIT_CAP_RATE      = "numeric_mkxarhhr"
    COL_YEAR_1_CF          = "numeric_mkxary42"
    COL_EQUITY_INVESTMENT  = "numeric_mkxapdxt"

    # Column IDs for calculated outputs (all confirmed from your columns list)
    COL_CAP_RATE           = "numeric_mkxasdx8"
    COL_LTV                = "numeric_mkxa901y"
    COL_YIELD_ON_COST      = "numeric_mkxagcrj"
    COL_SPREAD             = "numeric_mkxa1nb4"
    COL_REVERSION_VALUE    = "numeric_mkxaacq4"
    COL_CASH_ON_CASH       = "numeric_mkxahsqj"
    COL_IRR                = "numeric_mkxav001"
    COL_EQUITY_MULTIPLE    = "numeric_mkxag7qd"

    # Also needed for IRR calculation
    COL_YEAR_2_CF          = "numeric_mkxavbzw"
    COL_YEAR_3_CF          = "numeric_mkxadz1f"
    COL_YEAR_4_CF          = "numeric_mkxasbp9"
    COL_YEAR_5_CF          = "numeric_mkxarrfz"
    COL_SALE_PROCEEDS      = "numeric_mkxaaxrp"

    for item in items:
        cv_dict = {c["id"]: c for c in item.get("column_values", [])}
        try:
            # Read all required inputs
            noi = safe_number_colval(cv_dict.get(COL_NOI))
            total_project_cost = safe_number_colval(cv_dict.get(COL_TOTAL_PROJECT_COST))
            loan_amount = safe_number_colval(cv_dict.get(COL_LOAN_AMOUNT))
            market_cap_rate = safe_number_colval(cv_dict.get(COL_MARKET_CAP_RATE))
            exit_cap_rate = safe_number_colval(cv_dict.get(COL_EXIT_CAP_RATE))
            year_1_cf = safe_number_colval(cv_dict.get(COL_YEAR_1_CF))
            equity_investment = abs(safe_number_colval(cv_dict.get(COL_EQUITY_INVESTMENT)))
            y2 = safe_number_colval(cv_dict.get(COL_YEAR_2_CF))
            y3 = safe_number_colval(cv_dict.get(COL_YEAR_3_CF))
            y4 = safe_number_colval(cv_dict.get(COL_YEAR_4_CF))
            y5 = safe_number_colval(cv_dict.get(COL_YEAR_5_CF))
            sale = safe_number_colval(cv_dict.get(COL_SALE_PROCEEDS))

            # Calculations
            cap_rate = (noi / total_project_cost * 100) if total_project_cost > 0 else None
            ltv = (loan_amount / total_project_cost * 100) if total_project_cost > 0 else None
            yield_on_cost = (noi / total_project_cost * 100) if total_project_cost > 0 else None
            spread = (yield_on_cost - market_cap_rate) if yield_on_cost is not None and market_cap_rate > 0 else None
            reversion_value = (noi / (exit_cap_rate / 100)) if exit_cap_rate > 0 else None
            cash_on_cash = (year_1_cf / equity_investment * 100) if equity_investment > 0 else None

            # IRR/Equity Multiple as before
            cashflows = [-equity_investment, year_1_cf, y2, y3, y4, y5 + sale]
            irr = npf.irr(cashflows)
            irr_value = None if irr is None or (isinstance(irr, float) and math.isnan(irr)) else irr*100.0
            em = sum(cashflows[1:]) / equity_investment if equity_investment > 0 else None

            # Build column_values for mutation
            column_values = {}
            if cap_rate is not None:
                column_values[COL_CAP_RATE] = f"{cap_rate:.2f}"
            if ltv is not None:
                column_values[COL_LTV] = f"{ltv:.2f}"
            if yield_on_cost is not None:
                column_values[COL_YIELD_ON_COST] = f"{yield_on_cost:.2f}"
            if spread is not None:
                column_values[COL_SPREAD] = f"{spread:.2f}"
            if reversion_value is not None:
                column_values[COL_REVERSION_VALUE] = f"{reversion_value:.2f}"
            if cash_on_cash is not None:
                column_values[COL_CASH_ON_CASH] = f"{cash_on_cash:.2f}"
            if irr_value is not None:
                column_values[COL_IRR] = f"{irr_value:.2f}"
            if em is not None:
                column_values[COL_EQUITY_MULTIPLE] = f"{em:.2f}"

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
