import time
import random
import psycopg2
import requests
import re
import json
from product import TokopediaScraper
import os
import logging
from dotenv import load_dotenv

load_dotenv()

from openai import OpenAI
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

HEADERS = {
  "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
  "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
}

# ------------------------------------------------------------
# POSTGRES CONNECTION
# ------------------------------------------------------------
def get_connection():
  while True:
    try:
      conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        dbname=os.getenv("DB_NAME"),
        connect_timeout=5
      )
      conn.autocommit = True
      print("PostgreSQL Connected.")
      return conn
    except Exception as e:
      print("Gagal konek DB, retry 3 detik...", e)
      time.sleep(3)

def ensure_connection():
  global conn
  try:
    with conn.cursor() as cur:
      cur.execute("SELECT 1;")
    return conn
  except Exception:
    print("Koneksi DB terputus! Reconnecting...")
    conn = get_connection()
    return conn

# ------------------------------------------------------------
# EMBEDDING GENERATION
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
# CLEAN PRODUCT TITLE
# ------------------------------------------------------------
PROMPT_TEMPLATE = """
  You are a deterministic product title normalizer for semantic embedding.

  TASK:
  Extract a clean and compact product title suitable for semantic embedding.

  RULES:
  - Keep the product type. (e.g., Battery, Battery Charger)
  - KEEP brand names. (Canon, Brica, Citycall, Energizer)
  - KEEP essential series/model identifiers. (LP-E8, AE1, AE2, M26, E91)
  - REMOVE promo, bundle, count, B2G1, variant, color, etc.

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

  OUTPUT FORMAT:
  "<Product Type> <Brand> <Series>"

  Product Title:
  "{title}"
"""
def clean_title_with_openai(title: str, l1: str, l2: str, l3: str) -> str:
  """Clean product title using OpenAI model."""
  try:
    prompt = PROMPT_TEMPLATE.format(
      title=title,
      l1=l1,
      l2=l2,
      l3=l3
    )

    response = client.chat.completions.create(
      model="gpt-4.1-mini",
      messages=[
        {"role": "system", "content": "You are a deterministic extractor."},
        {"role": "user", "content": prompt}
      ],
      temperature=0
    )

    cleaned = response.choices[0].message.content.strip()
    return cleaned if cleaned else title
  except Exception as e:
    print("❌ OpenAI Error:", e)
    return title
  
# ------------------------------------------------------------
# SAVE PRODUCT AND CHUNKS
# ------------------------------------------------------------
def save_product_and_chunks(products_data, l1, l2, l3):
  conn = ensure_connection()
  with conn.cursor() as cur:
    product_id_first = None  
    insert_query_base = """
      INSERT INTO products (
        ecommerce, 
        category_id, 
        shop_name, 
        shop_location, 
        name, 
        url, 
        price, 
        stock, 
        sold, 
        variant_spec, 
        detail, 
        media, 
        reviews, 
        parent_id, 
        is_parent, 
        updated_at
      ) VALUES (
        %s, %s, %s, %s, %s, 
        %s, %s, %s, %s, %s, 
        %s, %s, %s, %s, %s, 
        NOW()
      ) ON CONFLICT (url) DO UPDATE SET
          shop_name = EXCLUDED.shop_name,
          shop_location = EXCLUDED.shop_location,
          name = EXCLUDED.name,
          price = EXCLUDED.price,
          stock = EXCLUDED.stock,
          sold = EXCLUDED.sold,
          variant_spec = EXCLUDED.variant_spec,
          detail = EXCLUDED.detail,
          media = EXCLUDED.media,
          reviews = EXCLUDED.reviews,
          updated_at = NOW()
        RETURNING id;
    """
    insert_chunk_query = """
      INSERT INTO product_chunks (
        product_id, 
        chunk_text, 
        embedding 
      ) VALUES (
        %s, %s, %s::VECTOR
      );
    """
    delete_old_chunks_query = "DELETE FROM product_chunks WHERE product_id = %s;"
    
    for i, product_data in enumerate(products_data, start=0): 
      current_parent_id = None
      is_parent = False
      
      if i == 0:
        if len(products_data) > 1:
          is_parent = True
      else:
        current_parent_id = product_id_first

      name = product_data.get('product_name', '')

      cur.execute(insert_query_base, (
        'tokopedia',
        l3[0],
        product_data.get('shop_name'),
        product_data.get('shop_location'),
        product_data.get('product_name'),
        product_data.get('product_url'),
        product_data.get('product_price'),
        product_data.get('product_stock'),
        product_data.get('product_sold'),
        json.dumps(product_data.get('variant_spec', {})),
        json.dumps(product_data.get('product_detail', {})),
        json.dumps(product_data.get('product_media', {})),
        json.dumps(product_data.get('product_reviews', {})),
        current_parent_id,
        is_parent
      ))
      product_id = cur.fetchone()[0]
      if i == 0:
        product_id_first = product_id
        
      print(f"Save product: {name}")
      print("="*50)
      
      # if current_parent_id is None:
      cur.execute(delete_old_chunks_query, (product_id,))
    
      chunk_text = clean_title_with_openai(name, l1, l2, l3)
      print(f"results: {chunk_text}")
      print("-"*50)

      embedding = generate_embedding(chunk_text)

      if embedding:
        embedding_str = f"[{','.join(map(str, embedding))}]"

        cur.execute(
          insert_chunk_query,
          (
            product_id,
            chunk_text,
            embedding_str
          )
        )

  
# ------------------------------------------------------------
# GET CATEGORY BY LEVEL
# ------------------------------------------------------------
def get_categories(level, parent_id=None):
  conn = ensure_connection()
  with conn.cursor() as cur:
    if parent_id:
      cur.execute("""
        SELECT id, name, url 
        FROM categories
        WHERE level = %s 
          AND parent_id = %s
          AND ecommerce = 'tokopedia'
        ORDER BY name;
      """, (level, parent_id))
    else:
      cur.execute("""
        SELECT id, name, url 
        FROM categories
        WHERE level = %s
          AND ecommerce = 'tokopedia'
        ORDER BY name;
      """, (level,))
    
    return cur.fetchall()

# ------------------------------------------------------------
# PRINT CATEGORY LIST
# ------------------------------------------------------------
def print_categories(title, categories):
  print(f"\n=== {title} ===")
  for idx, (cid, name, url) in enumerate(categories, start=1):
    print(f"{idx}. {name} ({cid})")


def select_category(categories, level_name):
  if not categories:
    print(f"Tidak ada kategori {level_name}.")
    return None

  choice = int(input(f"\nPilih kategori {level_name} (nomor): "))
  selected = categories[choice - 1]
  print(f"Anda memilih: {selected[1]} (ID: {selected[0]})")

  return selected

def scrape_page(url, l1_selected, l2_selected, l3_selected):
  L3_NAME = l3_selected[1]
  try:
    r = requests.get(url, headers=HEADERS, timeout=50)
    r.raise_for_status()
    html_content = r.text

    pattern = r'window.__cache\s*=\s*(\{.*?\})\s*;'
    match = re.search(pattern, html_content, re.DOTALL)
    if not match:
      match = re.search(r'window.__cache\s*=\s*(\{.*\})\s*', html_content, re.DOTALL)
    
    json_string = None
    if match:
      json_string = match.group(1).strip()
    else:
      pattern_loose = r'window.__cache\s*=\s*(\{.*\})\s*'
      match_loose = re.search(pattern_loose, html_content, re.DOTALL)
      if match_loose:
        json_string = match_loose.group(1).strip()
      else:
        print("Gagal menemukan pola 'window.__cache = {JSON}' dalam HTML. Melewati halaman.")
        return

    if json_string:
      try:
        json_data = json.loads(json_string)
        json_root = json_data.get("ROOT_QUERY", {})
      except json.JSONDecodeError as e:
        print(f"Gagal mem-parsing JSON: {e}. Melewati halaman.")
        return

      try:
        search_keys = [
          key for key in json_root.keys() 
          if "searchProduct" in key 
        ]
        if search_keys:
          json_search = json_root[search_keys[0]]
          search_id = json_search.get("id", None)
          print("ID produk dari pencarian:", search_id)

          json_search_product = json_data.get(search_id, {})
          json_ace_product = json_search_product.get("products", [])

          print(f"Jumlah produk ditemukan: {len(json_ace_product)}")

          for idx in json_ace_product:
            ace_product_id = idx.get("id", None)
            product = json_data.get(ace_product_id, {})
            product_url = product.get('url', '')
            try:
              scraper = TokopediaScraper()
              results = scraper.scrape(product_url)
              
              save_product_and_chunks(results, l1_selected, l2_selected, l3_selected)
            except Exception as product_e:
              logging.error(f"[{L3_NAME}] GAGAL SCRAPE PRODUK (URL: {product_url}): {product_e}. Lanjut ke produk berikutnya.")
            
      except Exception as e:
        logging.error(f"[{L3_NAME}] Gagal memproses data produk dari JSON: {e}. Melewati halaman: {url}")
  except requests.exceptions.RequestException as http_e:
    logging.error(f"[{L3_NAME}] ERROR HTTP/KONEKSI pada URL {url}: {http_e}. Melewati halaman.")
  except Exception as general_e:
    logging.error(f"[{L3_NAME}] ERROR UMUM tak terduga di scrape_page pada URL {url}: {general_e}. Melewati halaman.")
# ------------------------------------------------------------
# MAIN PROGRAM
# ------------------------------------------------------------
if __name__ == "__main__":
  try:
    print("-" * 20 + " [ START ] " + "-" * 20)

    # ========================
    # STEP 1 → PILIH LEVEL 1
    # ========================
    l1_all = get_categories(level=1)
    print_categories("Category Level 1", l1_all)
    l1_selected = select_category(l1_all, "L1")
    l1_selected_id = l1_selected[0]
    print(f"\nAnda memilih L1: {l1_selected[1]} (ID: {l1_selected_id})")

    l1_safe_name = "".join(c for c in l1_selected[1] if c.isalnum() or c in (' ', '_')).rstrip()
    l1_safe_name = l1_safe_name.replace(" ", "_").lower()
    
    
    # ------------------------------------------------------------
    # START LOGGING SETUP
    # ------------------------------------------------------------
    OUTPUT_FOLDER = f'log/'
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    LOG_FILE = os.path.join(OUTPUT_FOLDER, f'{l1_safe_name}.txt')
    
    for handler in logging.root.handlers[:]:
      logging.root.removeHandler(handler)
    
    logging.getLogger().setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    file_handler = logging.FileHandler(LOG_FILE, mode='w')
    file_handler.setFormatter(formatter)
    logging.getLogger().addHandler(file_handler)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logging.getLogger().addHandler(stream_handler)

    # ------------------------------------------------------------
    # END LOGGING SETUP
    # ------------------------------------------------------------

    total_pages = 100
    os.system(f'title " {l1_selected[1]}"')
    logging.info(f"Scraping untuk setiap kategori {l1_selected[1]} setiap child kategori {total_pages} halaman.\n")

    # # ========================
    # # LOOP LEVEL 2
    # # ========================
    # l2_all = get_categories(level=2, parent_id=l1_selected_id)

    # for l2 in l2_all:
    #   l2_id, l2_name, _ = l2
    #   logging.info(f"\n=== START L2: {l2_name} ===")

    #   # ========================
    #   # LOOP LEVEL 3
    #   # ========================
    #   l3_all = get_categories(level=3, parent_id=l2_id)

    #   for l3 in l3_all:
    #     l3_id, l3_name, l3_url = l3
    #     logging.info(f"\n=== START L3: {l3_name} ===")
    #     logging.info(f"     Base URL: {l3_url}")

    #     # ========================
    #     # SCRAPE 50 HALAMAN
    #     # ========================
    #     for page in range(1, total_pages + 1):
    #       if "?" in l3_url:
    #         page_url = f"{l3_url}&page={page}"
    #       else:
    #         page_url = f"{l3_url}?page={page}"

    #       logging.info(f"     Scraping halaman {page}/{total_pages}")
    #       scrape_page(page_url, l1_selected, l2, l3)

    #       time.sleep(random.uniform(1, 3))
        
    #     logging.info(f"  -> END L3: {l3_name} ({l3_id}) SEMUA HALAMAN SELESAI.")

    # ========================
    # STEP 2 → Level 2
    # ========================
    l2 = get_categories(level=2, parent_id=l1_selected_id)
    print_categories("Category Level 2", l2)
    l2_selected = select_category(l2, "L2")
    l2_selected_id = l2_selected[0]
    l2_selected_name = l2

    # ========================
    # STEP 3 → Level 3
    # ========================
    l3 = get_categories(level=3, parent_id=l2_selected_id)
    print_categories("Category Level 3", l3)

    if not l3:
      print("Tidak ada kategori level 3.")
    else:
      print("\n=== Pilih Level 3 untuk mendapatkan URL ===")
      selected_l3 = select_category(l3, "L3 (ambil URL)")

      selected_l3_id = selected_l3[0]
      selected_l3_name = selected_l3[1]
      selected_l3_url = selected_l3[2]

      print("\n====================================")
      print(f"Kategori Level 3 yang Anda pilih:")
      print(f"Nama : {selected_l3_name}")
      print(f"URL  : {selected_l3_url}")
      print("====================================\n")

    # ==========================================
      # INPUT JUMLAH HALAMAN UNTUK DI-SCRAPE
      # ==========================================
      total_pages = int(input("Masukkan jumlah halaman yang akan di-scrape: "))
      print(f"\nMulai scraping {total_pages} halaman...\n")

      for page in range(1, total_pages + 1):
        if "?" in selected_l3_url:
          page_url = f"{selected_l3_url}&page={page}"
        else:
          page_url = f"{selected_l3_url}?page={page}"

        print(f"Scraping halaman {page} → {page_url}")
        scrape_page(page_url, l1_selected, l2_selected, selected_l3)


  finally:
    print("\nConnection closed.")


