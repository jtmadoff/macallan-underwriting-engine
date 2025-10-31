import os, requests, numpy_financial as npf

api_key = os.getenv("MONDAY_API_KEY")
board_id = os.getenv("BOARD_ID")
headers = {"Authorization": api_key}

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

resp = requests.post("https://api.monday.com/v2", json={"query": query}, headers=headers)
items = resp.json()["data"]["boards"][0]["items"]

for item in items:
    cv = {c["id"]: c["text"] for c in item["column_values"]}
    try:
        # Parse inputs
        equity = abs(float(cv["equity_investment"].replace(",","")))
        cashflows = [
            -equity,
            float(cv["year_1_cf"] or 0),
            float(cv["year_2_cf"] or 0),
            float(cv["year_3_cf"] or 0),
            float(cv["year_4_cf"] or 0),
            float(cv["year_5_cf"] or 0) + float(cv["sale_proceeds"] or 0)
        ]

        irr = npf.irr(cashflows)
        em = sum(cashflows[1:]) / equity

        update_mutation = f"""
        mutation {{
          change_multiple_column_values(item_id: {item['id']}, board_id: {board_id},
          column_values: {{
            "irr": "{{\\"number\\": {irr*100:.2f}}}",
            "equity_multiple": "{{\\"number\\": {em:.2f}}}"
          }}) {{ id }}
        }}
        """
        requests.post("https://api.monday.com/v2", json={"query": update_mutation}, headers=headers)

    except Exception as e:
        print(f"Error on {item['name']}: {e}")
