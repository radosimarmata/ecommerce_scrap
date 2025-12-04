import time
import random
import psycopg2
import requests
import re
import json
from product import TokopediaScraper
from dotenv import load_dotenv
import os

load_dotenv()

HEADERS = {
  "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
  "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
}

# ------------------------------------------------------------
# POSTGRES CONNECTION
# ------------------------------------------------------------
# Pastikan detail koneksi ini sudah sesuai dengan server Anda
conn = psycopg2.connect(
  host=os.getenv("DB_HOST"),
  port=os.getenv("DB_PORT"),
  user=os.getenv("DB_USER"),
  password=os.getenv("DB_PASSWORD"),
  dbname=os.getenv("DB_NAME")
)
conn.autocommit = True

# ------------------------------------------------------------
# KONFIGURASI OTOMATIS
# ------------------------------------------------------------
# Jumlah halaman yang akan di-scrape untuk SETIAP kategori L3
MAX_PAGES_PER_CATEGORY = 50 
# Jeda antar halaman (untuk menghindari banned)
SCRAPE_DELAY_SECONDS = 1 

# ------------------------------------------------------------
# EMBEDDING GENERATION (SAMA)
# ------------------------------------------------------------
def generate_embedding(text):
  try:
    response = requests.post(
      "http://localhost:11434/api/embed",
      json={"model": "all-minilm:l6-v2", "input": text},
      timeout=20
    )
    if response.status_code != 200:
      print("HTTP Error:", response.status_code)
      return None

    res = response.json()
    if 'embeddings' in res and isinstance(res['embeddings'], list) and len(res['embeddings']) > 0:
      embedding = res["embeddings"][0]
      return embedding
    else:
      print("Respons embedding tidak valid.")
      return None

  except Exception as e:
    print(f"Embedding error: {e}")
    return None

# ------------------------------------------------------------
# SAVE PRODUCT AND CHUNKS (SAMA - Sudah mengandung UPSERT)
# ------------------------------------------------------------
def save_product_and_chunks(products_data, category_id, full_category_path):
  with conn.cursor() as cur:
      product_id_first = None  
      
      insert_query_base = """
        INSERT INTO products (
          ecommerce, category_id, shop_name, shop_location, name, url, 
          price, stock, sold, variant_spec, detail, media, reviews, 
          parent_id, is_parent, search_tsv, updated_at
        ) VALUES (
          %s, %s, %s, %s, %s, 
          %s, %s, %s, %s, %s, 
          %s, %s, %s, %s, %s, 
          to_tsvector('indonesian', %s),
          NOW()
        ) 
        ON CONFLICT (url) DO UPDATE SET
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
          search_tsv = EXCLUDED.search_tsv,
          updated_at = NOW()
        RETURNING id;
      """
      
      insert_chunk_query = """
        INSERT INTO product_chunks (
          product_id, chunk_text, chunk_type, embedding, chunk_meta
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
        price_num, stock_num, sold_num = 0, 0, 0
        try: price_num = int(price_val) if price_val else 0
        except (ValueError, TypeError): price_num = 0
        try: stock_num = int(stock_val) if stock_val else 0
        except (ValueError, TypeError): stock_num = 0
        try: sold_num = int(sold_val) if sold_val else 0
        except (ValueError, TypeError): sold_num = 0

        search_text = f"{name} {shop_name} {shop_location} {description}"

        cur.execute(insert_query_base, (
          'tokopedia',
          category_id,
          shop_name,
          shop_location,
          name,
          product_data.get('product_url'),
          product_data.get('product_price'),
          product_data.get('product_stock'),
          product_data.get('product_sold'),
          json.dumps(variant_spec),
          json.dumps(detail),
          json.dumps(product_data.get('product_media', {})),
          json.dumps(reviews),
          current_parent_id,
          is_parent,
          search_text
        ))
        
        product_id = cur.fetchone()[0]
        
        if i == 0:
          product_id_first = product_id
        
        print(f"✅ Product saved/updated (ID: {product_id}).")

        cur.execute(delete_old_chunks_query, (product_id,))

        name_chunk_text = f"Nama: {name} (Toko: {shop_name}) (Kategori: {full_category_path})"
        total_reviews = reviews.get('total_rating', 0)
        main_rating = reviews.get('average_score', 'N/A')
        topics = reviews.get('topics', {})
        
        summary_parts = [f"Rating {main_rating} dari {total_reviews} ulasan."]
        if topics:
          topic_rating_texts = [f"{k}: {v.get('score', 'N/A')}" for k, v in topics.items()]
          summary_parts.append("Konsumen menilai berdasarkan topik: " + "; ".join(topic_rating_texts) + ".")
        
        review_texts_list = [r.get('text', '') for r in reviews.get('list', []) if r.get('text')]
        review_samples = " ".join(review_texts_list[:5])
        if review_samples:
          summary_parts.append("Teks ulasan meliputi: " + review_samples)
        review_summary = " ".join(summary_parts)

        detail_attributes = []
        detail_meta = {}
        if price_num > 0:
          price_fmt = f"Rp{price_num:,}".replace(",", ".")
          detail_attributes.append(f"Harga produk adalah {price_fmt}")
          detail_meta['harga'] = price_num
        if stock_num is not None:
          status = f"Status stok: Tersedia sebanyak {stock_num} unit." if stock_num > 0 else "Status stok: Habis."
          detail_attributes.append(status)
          detail_meta['stock'] = stock_num
        if sold_num > 0:
          detail_attributes.append(f"Produk ini telah terjual sebanyak {sold_num} unit.")
          detail_meta['sold'] = sold_num
        
        if detail and isinstance(detail, dict):
          for key, value in detail.items():
            if key.lower() != 'deskripsi' and key and value:
              if isinstance(value, (str, int, float, bool)):
                if key.lower() not in ['harga', 'stock', 'sold']:
                  detail_attributes.append(f"{key}: {value}")
                  detail_meta[key] = value
              else:
                detail_attributes.append(f"{key}: {str(value)}")
        detail_text = ". ".join(detail_attributes)

        variant_text = ''
        if variant_spec and isinstance(variant_spec, dict):
          var_attrs = []
          for key, value in variant_spec.items():
            if key and value: var_attrs.append(f"{key}: {value}")
          variant_text = ". ".join(var_attrs)
        
        chunks_to_create = [
          ('name', name_chunk_text, {}),
          ('description', description, {}),
          ('variant', variant_text, variant_spec),
          ('detail', detail_text, detail_meta),
          ('review_summary', review_summary, { 
            "rating": main_rating, 
            "total_reviews": total_reviews,
            "topics": topics
          })
        ]
        
        for chunk_type, chunk_text, meta_dict in chunks_to_create:
            if chunk_text and chunk_text.strip(): 
              embedding = generate_embedding(chunk_text)
              chunk_meta_json = json.dumps(meta_dict)
              
              if embedding:
                embedding_str = f"[{','.join(map(str, embedding))}]"
                cur.execute(insert_chunk_query, (
                  product_id,
                  chunk_text,
                  chunk_type,
                  embedding_str,
                  chunk_meta_json
                ))
              else:
                print(f"   ❌ Gagal membuat embedding untuk '{chunk_type}'.")


# ------------------------------------------------------------
# GET CATEGORY BY LEVEL (SAMA)
# ------------------------------------------------------------
def get_categories(level, parent_id=None):
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
# SCRAPE PAGE (SAMA)
# ------------------------------------------------------------
def scrape_page(url, l1_tuple, l2_tuple, l3_tuple):
  # l1_tuple: (id, name, url), l2_tuple: (id, name, url), l3_tuple: (id, name, url)
  try:
      # Jeda sebelum memulai scraping (menghindari diblokir)
      time.sleep(random.uniform(SCRAPE_DELAY_SECONDS, SCRAPE_DELAY_SECONDS + 1))
      
      r = requests.get(url, headers=HEADERS, timeout=50)
      r.raise_for_status()
      html_content = r.text

      pattern = r'window.__cache\s*=\s*(\{.*?\})\s*;'
      match = re.search(pattern, html_content, re.DOTALL)
      if not match:
        match = re.search(r'window.__cache\s*=\s*(\{.*\})\s*', html_content, re.DOTALL)
      
      if match:
        json_string = match.group(1).strip()
      else:
        pattern_loose = r'window.__cache\s*=\s*(\{.*\})\s*'
        match_loose = re.search(pattern_loose, html_content, re.DOTALL)
        if match_loose:
          json_string = match_loose.group(1).strip()
        else:
          print("❌ Gagal menemukan pola 'window.__cache' dalam HTML.")
          return 
          
      if json_string:
        json_data = json.loads(json_string)
        json_root = json_data.get("ROOT_QUERY", {})

        search_keys = [
          key for key in json_root.keys() 
          if "searchProduct" in key 
        ]
        if search_keys:
          json_search = json_root[search_keys[0]]
          search_id = json_search.get("id", None)
          print(f"   [INFO] ID produk dari pencarian: {search_id}")

          json_search_product = json_data.get(search_id, {})
          json_ace_product = json_search_product.get("products", [])

          print(f"   [INFO] Jumlah produk ditemukan di halaman: {len(json_ace_product)}")

          for idx in json_ace_product:
            ace_product_id = idx.get("id", None)
            product = json_data.get(ace_product_id, {})
            product_url = product.get('url', '')
            
            if not product_url: 
                continue

            scraper = TokopediaScraper()
            results = scraper.scrape(product_url)
            
            if results:
                category_id = l3_tuple[0]
                full_category_path = f"{l1_tuple[1]} > {l2_tuple[1]} > {l3_tuple[1]}"
                
                save_product_and_chunks(results, category_id, full_category_path)

  except Exception as e:
    print(f"❌ Gagal memproses halaman/produk: {e}")
    # Tambahkan jeda yang lebih panjang setelah error
    time.sleep(10)

# ------------------------------------------------------------
# MAIN PROGRAM (OTOMATIS)
# ------------------------------------------------------------
if __name__ == "__main__":
  try:
    print("-" * 20 + " [ SCRAPER OTOMATIS DIMULAI ] " + "-" * 20)
    print(f"Target: Semua kategori L3, {MAX_PAGES_PER_CATEGORY} halaman per kategori.")

    # 1. Ambil SEMUA kategori Level 1
    l1_categories = get_categories(level=1)
    
    if not l1_categories:
        print("Tidak ada kategori Level 1 ditemukan. Hentikan program.")
        exit()

    for l1_selected in l1_categories:
        l1_selected_id, l1_selected_name, l1_selected_url = l1_selected
        
        # 2. Ambil SEMUA kategori Level 2 berdasarkan L1
        l2_categories = get_categories(level=2, parent_id=l1_selected_id)

        if not l2_categories:
            print(f"   [SKIP] L1 '{l1_selected_name}': Tidak ada L2.")
            continue

        for l2_selected in l2_categories:
            l2_selected_id, l2_selected_name, l2_selected_url = l2_selected

            # 3. Ambil SEMUA kategori Level 3 berdasarkan L2
            l3_categories = get_categories(level=3, parent_id=l2_selected_id)

            if not l3_categories:
                print(f"      [SKIP] L2 '{l2_selected_name}': Tidak ada L3.")
                continue

            for l3_selected in l3_categories:
                l3_selected_id, l3_selected_name, l3_selected_url = l3_selected

                print("\n" + "=" * 60)
                print(f"➡️ MEMULAI: {l1_selected_name} > {l2_selected_name} > {l3_selected_name}")
                print(f"   URL Dasar: {l3_selected_url}")
                print(f"   Target Halaman: 1 sampai {MAX_PAGES_PER_CATEGORY}")
                print("=" * 60)
                
                # 4. Loop Halaman 1 sampai MAX_PAGES_PER_CATEGORY (100)
                for page in range(1, MAX_PAGES_PER_CATEGORY + 1):
                    if "?" in l3_selected_url:
                        page_url = f"{l3_selected_url}&page={page}"
                    else:
                        page_url = f"{l3_selected_url}?page={page}"

                    print(f"   [HALAMAN {page}/{MAX_PAGES_PER_CATEGORY}] Scraping URL: {page_url}")
                    
                    scrape_page(page_url, l1_selected, l2_selected, l3_selected)
                    
                    # Jeda antar halaman
                    time.sleep(random.uniform(SCRAPE_DELAY_SECONDS, SCRAPE_DELAY_SECONDS + 1))


  finally:
    if conn:
        conn.close()
    print("\n" + "-" * 20 + " [ SCRAPER SELESAI ] " + "-" * 20)