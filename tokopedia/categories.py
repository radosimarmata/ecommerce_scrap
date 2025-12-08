import requests
from bs4 import BeautifulSoup
import psycopg2
import uuid
import time
import os
from dotenv import load_dotenv

load_dotenv()

from openai import OpenAI
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

HEADERS = {
  "User-Agent": (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
  )
}

# ------------------------------------------------------------
# POSTGRES CONNECTION
# ------------------------------------------------------------
conn = psycopg2.connect(
  host=os.getenv("DB_HOST"),
  port=os.getenv("DB_PORT"),
  user=os.getenv("DB_USER"),
  password=os.getenv("DB_PASSWORD"),
  dbname=os.getenv("DB_NAME")
)
conn.autocommit = True
cur = conn.cursor()

# ------------------------------------------------------------
# AUTO CREATE TABLE IF NOT EXISTS
# ------------------------------------------------------------
def ensure_table():
  print("Checking table...")

  cur.execute("""
    SELECT EXISTS (
      SELECT FROM information_schema.tables 
      WHERE table_name = 'categories'
    );
  """)
  exists = cur.fetchone()[0]

  if exists:
    print("Table 'categories' already exists.")
    return

  print("Table not found → creating table...")

  # Enable required extension
  cur.execute("""CREATE EXTENSION IF NOT EXISTS "uuid-ossp";""")
  cur.execute("""CREATE EXTENSION IF NOT EXISTS vector;""")

  cur.execute("""
    CREATE TABLE categories (
      id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
      ecommerce       TEXT NOT NULL,
      name            TEXT NOT NULL,
      url             TEXT,
      level           INT NOT NULL,
      parent_id       UUID REFERENCES categories(id) ON DELETE SET NULL,
      embedding       VECTOR(1536),
      created_at      TIMESTAMP DEFAULT NOW(),
      updated_at      TIMESTAMP DEFAULT NOW(),
      UNIQUE(ecommerce, name, level, parent_id)
    );
  """)

  print("Table 'categories' created successfully.\n")

# ------------------------------------------------------------
# generate_embedding
# - Calls Ollama embedding API
# ------------------------------------------------------------
def generate_embedding(text):
  try:
    response  = client.embeddings.create(
      model="text-embedding-3-small",
      input=text
    )

    embedding = response.data[0].embedding
    return embedding

  except Exception as e:
    print("Embedding error:", e)
    return None

# ------------------------------------------------------------
# get_or_create_category
# ------------------------------------------------------------
def get_or_create_category(name, url, level, parent_id=None):
  name = name.strip() if name else None
  url = url.strip() if url else None

  if not name:
    return None

  if parent_id is None:
    cur.execute("""
      SELECT id FROM categories
      WHERE name = %s AND level = %s AND parent_id IS NULL
      LIMIT 1
    """, (name, level))
  else:
    cur.execute("""
      SELECT id FROM categories
      WHERE name = %s AND level = %s AND parent_id = %s
      LIMIT 1
    """, (name, level, parent_id))

  row = cur.fetchone()
  if row:
    return row[0]

  new_id = str(uuid.uuid4())
  cur.execute("""
      INSERT INTO categories (id, ecommerce, name, url, level, parent_id)
      VALUES (%s, 'tokopedia', %s, %s, %s, %s)
      ON CONFLICT (ecommerce, name, level, parent_id) DO NOTHING
    """, (new_id, name, url, level, parent_id))
  
  embedding = generate_embedding(name)

  if embedding:
    cur.execute("""
      UPDATE categories
      SET embedding = %s
      WHERE id = %s
    """, (embedding, new_id))

  if parent_id is None:
    cur.execute("""
      SELECT id FROM categories
      WHERE name = %s AND level = %s AND parent_id IS NULL
      LIMIT 1
    """, (name, level))
  else:
    cur.execute("""
      SELECT id FROM categories
      WHERE name = %s AND level = %s AND parent_id = %s
      LIMIT 1
    """, (name, level, parent_id))

  row = cur.fetchone()
  return row[0] if row else None

# ------------------------------------------------------------
# Improved scraping logic:
# - More tolerant to nested divs
# - Finds the first anchor that looks like the subcategory
# - Skips empty anchors
# - Logs counts
# ------------------------------------------------------------
def scrape_and_insert_categories(url="https://www.tokopedia.com/p"):
  ensure_table()

  r = requests.get(url, headers=HEADERS, timeout=20)
  soup = BeautifulSoup(r.text, "html.parser")

  created_counts = {"master": 0, "sub": 0, "child": 0}

  master_blocks = soup.select("div.css-s7tck8")
  print("Master blocks found:", len(master_blocks))

  for master in master_blocks:
    master_els = master.select("div.css-2wmm3i a")    
    master_map = {}
    for master_el in master_els:
      master_name = master_el.get_text(strip=True)
      master_url = master_el.get("href")
      if master_url and not master_url.startswith("http"):
        master_url = "https://www.tokopedia.com" + master_url
      
      master_map[master_name] = master_url

    detail_blocks = master.select("div.css-16mwuw1")
    for detail_block in detail_blocks:
      master_span = detail_block.select_one("span.css-38r5l3.e13h6i9f1")
      if not master_span:
        continue
      current_master_name = master_span.get_text(strip=True)
      current_master_url = master_map.get(current_master_name)
      master_id = get_or_create_category(current_master_name, current_master_url, 1, None)
      created_counts["master"] += 1

      print(f"\nMASTER DITEMUKAN: {current_master_name}")
      sub_blocks = detail_block.select("div.css-cdv2tj.e13h6i9f2") 

      for sb in sub_blocks:
        sub_a = sb.find("a", recursive=False)         
        if not sub_a:
          continue

        subcat_name = sub_a.get_text(strip=True)
        subcat_url = sub_a.get("href")
        
        if subcat_url and not subcat_url.startswith("http"):
          subcat_url = "https://www.tokopedia.com" + subcat_url

        sub_id = get_or_create_category(subcat_name, subcat_url, 2, master_id)
        created_counts["sub"] += 1
        print(f"  SUB: {subcat_name}")

        child_links = sb.select("div.css-79elbk.e13h6i9f3 a")
        for ch in child_links:
          child_name = ch.get_text(strip=True)
          child_url = ch.get("href")
          
          if child_url and not child_url.startswith("http"):
            child_url = "https://www.tokopedia.com" + child_url

          get_or_create_category(child_name, child_url, 3, sub_id)
          created_counts["child"] += 1

    time.sleep(0.2)

  print("\n== DONE INSERT ==")
  print("Created counts:", created_counts)

if __name__ == "__main__":
  try:
    scrape_and_insert_categories()
  finally:
    conn.close()
