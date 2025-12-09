import os
import json
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()

# ENV VARS
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "db_ecommerce")
DB_USER = os.getenv("DB_USER", "admin")
DB_PASSWORD = os.getenv("DB_PASSWORD", "admin")

client = OpenAI(api_key=OPENAI_API_KEY)

# =============================
# Connection
# =============================
def connect_db():
  return psycopg2.connect(
    host=DB_HOST,
    port=DB_PORT,
    user=DB_USER,
    password=DB_PASSWORD,
    dbname=DB_NAME
  )


# =============================
# AI Query Understanding
# =============================
def ai_understand(query: str, categories_level_2: list) -> dict:
  l2_names = [cat['level_2_name'] for cat in categories_level_2]
  l2_str = ", ".join(l2_names)
  
  system_prompt = (
    "Anda adalah sistem Query Understanding e-commerce. Tugas Anda adalah mengekstrak niat pengguna dan **mencocokkan** query dengan kategori Level 2 yang paling relevan dari daftar berikut: "
    f"[{l2_str}]. Kategori yang cocok bisa lebih dari satu.\n\n"

    "Prioritaskan kategori Level 2 yang berisi produk fisik utama (misalnya, jika query berisi 'sepatu', masukkan 'Fashion Pria' atau 'Fashion Wanita'). "
    "Hanya setelah itu, tambahkan kategori yang relevan secara kontekstual (misalnya, 'Olahraga').\n\n"
    "**Output HARUS berupa objek JSON valid** yang mengikuti format yang diberikan.\n\n"
    "Contoh Output:\n"
    "{\n"
    "  \"category_level_2_matches\": [\"Elektronik\", \"Audio\"], "
    "  \"brand\": \"Sony\",\n"
    "  \"filters\": {\n"
        "    \"color\": \"merah\",\n"
        "    \"location\": \"jakarta\",\n"
        "    \"condition\": \"bekas\"\n"
        "    \"storage\": \"128GB\"\n"
        "    \"ram\": \"4GB\"\n"
        "    \"harga_min\": 100000,\n"
        "    \"harga_max\": 5000000\n"
        "  }\n"
    "}"
  )

  resp = client.chat.completions.create(
    model="gpt-4o-mini",
    messages=[
      {"role": "system", "content": system_prompt},
      {"role": "user", "content": query}
    ],
    temperature=0.0,
    response_format={"type": "json_object"}
  )

  try:
    return json.loads(resp.choices[0].message.content.strip())
  except json.JSONDecodeError:
    print("⚠️ Gagal parsing JSON dari AI. Menggunakan kueri mentah.")
    return {"semantic_query": query, "filters": {}}

def ai_select_best_l3(query: str, l3_categories: list) -> dict:
  l3_names = [cat['level_3_name'] for cat in l3_categories]
  l3_str = ", ".join(l3_names)
  
  system_prompt = (
      "Anda adalah sistem pencocokan kategori e-commerce. Tugas Anda adalah memilih "
      "**SATU** nama kategori Level 3 yang paling akurat mencerminkan niat pengguna "
      f"dari daftar berikut: [{l3_str}].\n\n"
      "**ATURAN PENCERMATAN:**\n"
        "1. Jika kueri mengandung nama produk atau Brand (seperti 'iPhone', 'Samsung'), pilih kategori **Sistem Operasi** atau **Brand** yang sesuai (misal: 'iPhone' $\rightarrow$ 'iOS', 'Samsung' $\rightarrow$ 'Android OS').\n"
      "Output HARUS berupa objek JSON valid dengan kunci 'best_l3_match'. "
      "Jika tidak ada yang cocok, kembalikan string kosong.\n\n"
      "Contoh Output:\n"
      "{\n"
      "  \"best_l3_match\": \"Sepatu Lari Pria\"\n"
      "}"
  )

  try:
    resp = client.chat.completions.create(
      model="gpt-4o-mini",
      messages=[
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": f"Kueri: '{query}'"}
      ],
      temperature=0.0,
      response_format={"type": "json_object"}
    )
    return json.loads(resp.choices[0].message.content.strip())
  except Exception as e:
      print(f"⚠️ Gagal memanggil AI untuk seleksi L3: {e}")
      return {"best_l3_match": ""}

# =============================
# Embedding Generator
# =============================
def generate_embedding(text: str):
  resp = client.embeddings.create(
    model="text-embedding-3-small",
    input=text
  )
  return resp.data[0].embedding

def final_product_search(cur, query_vector, l3_category_id, top_k=50, filters=None):
  base_sql = """
    SELECT
      pc.product_id,
      p.name AS product_name,
      p.price AS product_price,
      p.url AS product_url,
      p.stock,
      p.sold,
      p.reviews,
      pc.chunk_text,
      (pc.embedding <=> %s::vector) AS distance
    FROM products p 
    JOIN product_chunks pc ON p.id = pc.product_id
  """
  where_clause = f" WHERE 1=1 AND p.category_id = '{l3_category_id}' "
  filter_params = []

  # print(filters)
  if filters.get("location"):
    where_clause += " AND p.shop_location ILIKE %s "
    filter_params.append(f"%{filters['location']}%")

  if filters.get("color"):
    where_clause += """
      AND EXISTS (
          SELECT 1
          FROM jsonb_each_text(p.variant_spec) AS kv
          WHERE kv.value ILIKE %s
      )
      """
    filter_params.append(f"%{filters['color']}%")
  
  if filters.get("storage"):
    where_clause += """
      AND EXISTS (
          SELECT 1
          FROM jsonb_each_text(p.variant_spec) AS kv
          WHERE kv.value ILIKE %s
      )
      """
    filter_params.append(f"%{filters['storage']}%")

  if filters.get("ram"):
    where_clause += """
      AND EXISTS (
          SELECT 1
          FROM jsonb_each_text(p.variant_spec) AS kv
          WHERE kv.value ILIKE %s
      )
      """
    filter_params.append(f"%{filters['ram']}%")

  if filters.get("condition"):
    where_clause += " AND p.detail ->> 'kondisi' ILIKE %s "
    filter_params.append(f"%{filters['condition']}%")
  
  harga_min_val = filters.get("harga_min")
  harga_max_val = filters.get("harga_max")
  if harga_min_val and harga_max_val and str(harga_min_val) == str(harga_max_val):
    where_clause += " AND p.price =  %s "
    filter_params.append(harga_min_val)
  elif harga_min_val and harga_max_val:
    where_clause += " AND p.price BETWEEN %s AND %s "
    filter_params.append(harga_min_val)
    filter_params.append(harga_max_val)
  elif harga_min_val:
    where_clause += " AND p.price >= %s "
    filter_params.append(harga_min_val)
  elif harga_max_val:
    where_clause += " AND p.price <= %s "
    filter_params.append(harga_max_val)

  final_sql = base_sql + where_clause + """
    ORDER BY pc.embedding <=> %s::vector
    LIMIT %s;
  """

  params = [query_vector] + filter_params + [query_vector, top_k]

  cur.execute(final_sql, tuple(params))

  return cur.fetchall()

# =============================
# SEMANTIC SEARCH
# =============================
def semantic_search(user_query: str, top_k=50):
  conn = connect_db()
  cur = conn.cursor(cursor_factory=RealDictCursor)

  products_results = []
  best_l3_category = None
  # Query Understanding
  cur.execute("""
    SELECT
      c.id as level_2_id,
      c.name as level_2_name
    FROM categories c 
    WHERE 
      "level" = 2;
  """)
  categories_level_2 = cur.fetchall()

  parsed_query = ai_understand(user_query, categories_level_2)

  matched_l2_names = parsed_query.get('category_level_2_matches', [])
  filtered_query = parsed_query.get('filters', [])

  matched_l2_ids = []
  for cat in categories_level_2:
    if cat['level_2_name'] in matched_l2_names:
      matched_l2_ids.append(cat['level_2_id'])

  final_l3_categories = []
  if matched_l2_ids:
    print(f"✅ Ditemukan kecocokan Kategori Level 2: {matched_l2_names}")
    l2_ids_str = ", ".join([f"'{id}'" for id in matched_l2_ids])
    cur.execute(f"""
      SELECT
        c.id as level_3_id,
        c.name as level_3_name,
        c.parent_id as level_2_parent_id
      FROM categories c 
      WHERE 
        c.parent_id IN ({l2_ids_str}) AND c."level" = 3;
    """)
    final_l3_categories = cur.fetchall()
    if final_l3_categories:
      ai_result = ai_select_best_l3(user_query, final_l3_categories)
      best_l3_name = ai_result.get('best_l3_match')
      
      if best_l3_name:
        for cat in final_l3_categories:
          if cat['level_3_name'] == best_l3_name:
            best_l3_category = cat
            break
        
        if best_l3_category:
          l3_id = best_l3_category['level_3_id']
          print(f"🔎 Memulai Pencarian Vektor Produk dengan Filter Kategori: {best_l3_category['level_3_name']}")
          
          try:
            query_vector = generate_embedding(user_query)
            products_results = final_product_search(cur, query_vector, l3_id, top_k, filtered_query)
            
            if products_results:
              print(f"🎉 Ditemukan {len(products_results)} produk yang paling relevan.")
            else:
              print("⚠️ Tidak ada produk ditemukan di kategori L3 tersebut.")
          except Exception as e:
            print(f"🛑 Error saat mencari produk: {e}")
        else:
          print("⚠️ Kategori yang dipilih AI tidak ditemukan dalam daftar L3.")
      else:
        print("⚠️ AI tidak dapat memilih kategori Level 3 yang paling cocok.")
    else:
      print("⚠️ Tidak ditemukan Kategori Level 3 di bawah kategori yang cocok.")
  else:
    print("❌ Tidak ditemukan Kategori Level 2 yang cocok. Melanjutkan dengan pencarian umum.")

  conn.close()
  return products_results

# =============================
# MAIN PROGRAM (Interactive Input)
# =============================
def main():
  print("=== AI Semantic Search (pgvector + OpenAI) ===")
  print("Ketik 'exit' untuk keluar.\n")

  while True:
    query = input("Masukkan query pencarian: ")

    if query.lower() in ["exit", "quit", "keluar"]:
      print("Bye!")
      break

    results = semantic_search(query, top_k=10)

    print("\n=== HASIL PENCARIAN ===\n")

    if not results:
      print("Tidak ada hasil.")
      continue

    for i, r in enumerate(results, start=1):
      print(f"{i}. {r['product_name']} (Rp {r['product_price']})")
      print(f"   stock: {r['stock']}")
      print(f"   url: {r['product_url']}")
      print(f"   reviews: {r['reviews']}")
      print(f"   Score: {r['distance']}\n")

    print("==============================\n")


if __name__ == "__main__":
  main()
