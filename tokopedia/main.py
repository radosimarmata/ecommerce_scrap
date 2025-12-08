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
# GET CHUNK TYPE
# ------------------------------------------------------------
def get_or_create_chunk_type_id(cur, chunk_type_name):
  cur.execute(
    "SELECT id FROM chunk_types WHERE name = %s",
    (chunk_type_name,)
  )
  row = cur.fetchone()
  if row:
    return row[0]

  cur.execute(
    """
    INSERT INTO chunk_types (name, description)
    VALUES (%s, %s)
    RETURNING id
    """,
    (chunk_type_name, f"Tipe chunk: {chunk_type_name}")
  )
  return cur.fetchone()[0]

# ------------------------------------------------------------
# SAVE PRODUCT AND CHUNKS
# ------------------------------------------------------------
def save_product_and_chunks(products_data, category_id, full_category_path):
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
        chunk_type_id, 
        embedding, 
        chunk_meta
      ) VALUES (
        %s, %s, %s, %s::VECTOR, %s
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

      shop_name = product_data.get('shop_name', '')
      shop_location = product_data.get('shop_location', '')
      name = product_data.get('product_name', '')
      detail = product_data.get('product_detail', {})
      reviews = product_data.get('product_reviews', {})
      variant_spec = product_data.get('variant_spec', {})
      description = detail.get('deskripsi', '')
      price_val = product_data.get('product_price')
      stock_val = product_data.get('product_stock')
      sold_val = product_data.get('product_sold')
      price_num = 0 
      stock_num = 0
      sold_num = 0
      try:
        price_num = int(price_val) if price_val else 0
      except (ValueError, TypeError):
        price_num = 0
          
      try:
        stock_num = int(stock_val) if stock_val else 0
      except (ValueError, TypeError):
        stock_num = 0
      try:
        sold_num = int(sold_val) if sold_val else 0
      except (ValueError, TypeError):
        sold_num = 0

      cur.execute(insert_query_base, (
        'tokopedia',
        category_id,
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

      cur.execute(delete_old_chunks_query, (product_id,))

      total_reviews = reviews.get('total_rating', 0)
      main_rating = reviews.get('average_score', 'N/A')
      topics = reviews.get('topics', {})

      # ====== CHUNKING GRANULAR ======
      chunks = []
      # nama produk
      chunks.append(("nama", f"Nama produk adalah {name}", {}))
      # toko
      if shop_name:
        chunks.append(("toko", f"Produk ini dijual oleh {shop_name}", {}))
      # lokasi toko
      if shop_location:
        chunks.append(("lokasi", f"Lokasi toko adalah {shop_location}", {}))
      # variant spec (setiap key jadi chunk)
      if isinstance(variant_spec, dict):
        for key, value in variant_spec.items():
          if value:
            chunks.append((f"variant::{key}", f"{key}: {value}", {"key": key, "value": value}))
      # detail (setiap key jadi chunk, kecuali deskripsi)
      if isinstance(detail, dict):
        for key, value in detail.items():
          if key.lower() != "deskripsi" and value:
            if isinstance(value, (str, int, float, bool)):
              chunks.append((f"detail::{key}", f"{key}: {value}", {"key": key, "value": value}))
            else:
              chunks.append((f"detail::{key}", f"{key}: {str(value)}", {"key": key, "value": value}))
      # rating
      if main_rating:
        chunks.append(("rating", f"Rating produk adalah {main_rating}", {"rating": main_rating}))
      # total reviews
      chunks.append(("total_reviews", f"Total ulasan produk adalah {total_reviews}", {"total_reviews": total_reviews}))
      # topics (setiap key → chunk)
      if isinstance(topics, dict):
        for topic, topic_data in topics.items():
          score = topic_data.get("score", None)
          if score is not None:
            txt = f"Topik {topic} memiliki skor {score}"
            chunks.append((f"topic::{topic}", txt, {"topic": topic, "score": score}))

      for chunk_type_name, chunk_text, meta_dict in chunks:
        if chunk_text and chunk_text.strip():
          chunk_type_id = get_or_create_chunk_type_id(cur, chunk_type_name)

          embedding = generate_embedding(chunk_text)
          chunk_meta_json = json.dumps(meta_dict)

          if embedding:
            embedding_str = f"[{','.join(map(str, embedding))}]"

            cur.execute(
              insert_chunk_query,
              (
                product_id,
                chunk_text,
                chunk_type_id,
                embedding_str,
                chunk_meta_json
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
              category_id = l3_selected[0]
              full_category_path = f"{l1_selected[1]} > {l2_selected[1]} > {l3_selected[1]}"
              save_product_and_chunks(results, category_id, full_category_path)
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


