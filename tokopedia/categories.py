import requests
from bs4 import BeautifulSoup
import psycopg2
import uuid
import time
import random
from dotenv import load_dotenv
import os

load_dotenv()

# ================================================
# USER AGENT ROTATOR (anti ban)
# ================================================
UA_LIST = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
]

def random_headers():
    return {
        "User-Agent": random.choice(UA_LIST),
        "Accept-Language": "en-US,en;q=0.9,id;q=0.8",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9",
        "Connection": "keep-alive"
    }

# ================================================
# SAFE GET (retry + random delay)
# ================================================
def safe_get(url, retries=5, timeout=40):
    for i in range(retries):
        try:
            print(f"🌐 Requesting: {url} (try {i+1}/{retries})")
            r = requests.get(url, headers=random_headers(), timeout=timeout)
            r.raise_for_status()
            time.sleep(random.uniform(0.7, 1.6))  # anti-ban delay
            return r
        except Exception as e:
            print(f"⚠️ Error accessing {url}: {e}")
            time.sleep(random.uniform(2, 5))
    print(f"❌ FAILED after {retries} attempts: {url}")
    return None

# ================================================
# POSTGRES CONNECTION
# ================================================
conn = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    dbname=os.getenv("DB_NAME")
)
conn.autocommit = True
cur = conn.cursor()

# ================================================
# CREATE TABLE IF NOT EXISTS
# ================================================
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
          embedding       VECTOR(384),
          created_at      TIMESTAMP DEFAULT NOW(),
          updated_at      TIMESTAMP DEFAULT NOW(),
          UNIQUE(ecommerce, name, level, parent_id)
        );
    """)

    print("Table 'categories' created successfully.\n")

# ================================================
# GENERATE EMBEDDING (with retry)
# ================================================
def generate_embedding(text, retries=3):
    for i in range(retries):
        try:
            r = requests.post(
                "http://localhost:11434/api/embed",
                json={"model": "all-minilm:l6-v2", "input": text},
                timeout=20
            )
            if r.status_code != 200:
                raise Exception(f"Ollama HTTP {r.status_code}")

            return r.json()["embeddings"][0]

        except Exception as e:
            print(f"⚠️ Embedding error (try {i+1}): {e}")
            time.sleep(2)

    print("❌ Embedding failed:", text)
    return None

# ================================================
# GET OR CREATE CATEGORY
# ================================================
def get_or_create_category(name, url, level, parent_id=None):
    name = name.strip() if name else None
    url = url.strip() if url else None

    if not name:
        return None

    # Check existing
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

    # Insert new
    new_id = str(uuid.uuid4())
    cur.execute("""
        INSERT INTO categories (id, ecommerce, name, url, level, parent_id)
        VALUES (%s, 'tokopedia', %s, %s, %s, %s)
        ON CONFLICT DO NOTHING
    """, (new_id, name, url, level, parent_id))

    emb = generate_embedding(name)
    if emb:
        cur.execute("""
            UPDATE categories SET embedding = %s WHERE id = %s
        """, (emb, new_id))

    # Return inserted/existing
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

# ================================================
# SCRAPE & INSERT CATEGORIES
# ================================================
def scrape_and_insert_categories(url="https://www.tokopedia.com/p"):
    ensure_table()

    r = safe_get(url)
    if not r:
        print("❌ Cannot fetch Tokopedia categories page.")
        return

    soup = BeautifulSoup(r.text, "html.parser")

    created_counts = {"master": 0, "sub": 0, "child": 0}

    master_blocks = soup.select("div.css-s7tck8")
    print("Found master categories:", len(master_blocks))

    for master in master_blocks:
        master_el = master.select_one("div.css-2wmm3i a")
        if not master_el:
            continue

        master_name = master_el.get_text(strip=True)
        master_url = master_el.get("href")
        if master_url and not master_url.startswith("http"):
            master_url = "https://www.tokopedia.com" + master_url

        master_id = get_or_create_category(master_name, master_url, 1, None)
        created_counts["master"] += 1

        sub_blocks = master.select("div.css-cdv2tj.e13h6i9f2")
        for sb in sub_blocks:
            sub_a = sb.find("a", recursive=False)
            if not sub_a:
                continue

            sub_name = sub_a.get_text(strip=True)
            sub_url = sub_a.get("href")
            if sub_url and not sub_url.startswith("http"):
                sub_url = "https://www.tokopedia.com" + sub_url

            sub_id = get_or_create_category(sub_name, sub_url, 2, master_id)
            created_counts["sub"] += 1

            child_links = sb.select("div.css-79elbk.e13h6i9f3 a")
            for ch in child_links:
                child_name = ch.get_text(strip=True)
                child_url = ch.get("href")
                if child_url and not child_url.startswith("http"):
                    child_url = "https://www.tokopedia.com" + child_url
                get_or_create_category(child_name, child_url, 3, sub_id)
                created_counts["child"] += 1

        # breathing room
        time.sleep(random.uniform(0.6, 1.2))

    print("\n== DONE INSERT ==")
    print("Created counts:", created_counts)

# ================================================
# MAIN
# ================================================
if __name__ == "__main__":
    try:
        scrape_and_insert_categories()
    finally:
        conn.close()
