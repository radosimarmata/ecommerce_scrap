import psycopg2
import os
import requests
from dotenv import load_dotenv

load_dotenv()

# =====================
# DB CONNECTION
# =====================
conn = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    port=5450,
    user="admin",
    password="admin",
    dbname=os.getenv("DB_NAME"),
    connect_timeout=5
)
conn.autocommit = True
cursor = conn.cursor()

# =====================
# OLLAMA CONFIG
# =====================
OLLAMA_URL = "http://localhost:11434/api/generate"

PROMPT_TEMPLATE = """
You are a deterministic extractor.

TASK:
Extract ONLY the core product name (product type) from the noisy title.
Not the specs, not the size, not the variant, not the model number, not the color.

### STRICT RULES:
- Output ONLY the core product name.
- Do NOT add or invent words.
- Do NOT remove or rewrite core words.
- Do NOT include inch, cm, watt, GB, model numbers, year, variant, or color.
- Do NOT rephrase or translate.
- Do NOT output brand unless it is part of the product naming convention (e.g., “Honda Vario”).

### EXAMPLES:
"Round Air Grill 4 inch Circular Air Diffuser" → "Air Grill"
"Rumah/cover Depan kipas angin maspion" → "cover kipas angin"
"iPhone 14 Pro Max 128GB Purple" → "iPhone 14"
"ASUS ROG Strix Z490 Gaming Motherboard" → "ASUS ROG Strix Motherboard"

### CATEGORY LOCK:
L1: {l1}
L2: {l2}
L3: {l3}
You MUST NOT output anything not in the original title.

### OUTPUT:
Return ONLY the core product name. Nothing else.

Product Title:
"{title}"
"""



def clean_title_with_phi3(title: str, l1: str, l2: str, l3: str) -> str:
  """Send request to Ollama (phi3:mini) to clean noise from product title."""
  try:
    payload = {
      "model": "phi3:mini",
      "prompt": PROMPT_TEMPLATE.format(
        title=title,
        l1=l1,
        l2=l2,
        l3=l3
      ),
      "stream": False
    }

    res = requests.post(OLLAMA_URL, json=payload, timeout=60)
    res.raise_for_status()

    data = res.json()
    cleaned = data.get("response", "").strip()

    return cleaned if cleaned else title
  except Exception as e:
    print("❌ Ollama Error:", e)
    return title


# =====================
# MAIN PROCESS
# =====================
BATCH_SIZE = 10 

def process_batch():
  """Process data per batch."""
  cursor.execute("""
    select
      p.id as product_id,
      p.name as product_title,
      c.name as category_level_3,
      c2.name as category_level_2,
      c3.name as category_level_1
    from products p 
    inner join categories c on c.id = p.category_id
    inner join categories c2 on c.parent_id = c2.id
    inner join categories c3 on c2.parent_id = c3.id
    order by p.created_at asc
    limit %s;
  """, (BATCH_SIZE,))
  
  rows = cursor.fetchall()
  if not rows:
    print("🎉 Semua data selesai diproses.")
    return False

  print(f"Processing {len(rows)} rows...")

  for product_id, title, l3, l2, l1 in rows:
    print(f"title: {title}")

    cleaned = clean_title_with_phi3(title, l1, l2, l3)
    print(f"results: {cleaned}")
    print("-"*50)
    # cursor.execute("""
    #   UPDATE products 
    #   SET clean_title = %s 
    #   WHERE id = %s;
    # """, (cleaned, product_id))

  # print(f"✔ {len(rows)} rows updated.\n")
  return True


def main():
  while True:
    has_more = process_batch()
    if not has_more:
      break


if __name__ == "__main__":
  main()
