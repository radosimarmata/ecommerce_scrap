# semantic_search_input.py
"""
Semantic search dengan AI Query Understanding (input interaktif)

Jalankan:
  python semantic_search_input.py
"""

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
def ai_understand(query: str) -> str:
  system_prompt = (
    "Anda adalah sistem Query Understanding e-commerce. Tugas Anda adalah mengekstrak niat "
    "pencarian pengguna menjadi format JSON yang terstruktur. "
    "Anda harus mengidentifikasi 2 tipe data:\n"
    "1. semantic_query: Bagian yang mendeskripsikan produk (Model, Brand, Spesifikasi), setelah filter faktual dikeluarkan.\n"
    "2. filters: Atribut faktual (Lokasi, Kondisi, Warna).\n\n"
    
    "ATURAN PENTING:\n"
    "1. Pindahkan semua atribut Warna, Lokasi, dan Kondisi ke dalam objek filters.\n"
    "2. semantic_query HARUS bersih dari atribut yang sudah dipindahkan ke filters.\n"
    "3. Gunakan huruf kecil (lowercase) untuk semua nilai dalam filters.\n"
    "4. Output Anda HANYA berupa objek JSON.\n\n"
    
    "Contoh Output:\n"
    "{\n"
    "  \"semantic_query\": \"infinix\",\n"
    "  \"filters\": {\n"
    "    \"color\": \"hijau\",\n"
    "    \"location\": \"jakarta\",\n"
    "    \"condition\": \"baru\"\n"
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


# =============================
# Embedding Generator
# =============================
def generate_embedding(text: str):
  resp = client.embeddings.create(
    model="text-embedding-3-small",
    input=text
  )
  return resp.data[0].embedding


# =============================
# SEMANTIC SEARCH
# =============================
def semantic_search(user_query: str, top_k=10):
  conn = connect_db()
  cur = conn.cursor(cursor_factory=RealDictCursor)

  # Query Understanding
  parsed_query = ai_understand(user_query)
  understood = parsed_query.get("semantic_query")
  filters = parsed_query.get("filters", {})
  print("\n🧠 AI Pahami Query (Semantic) →", understood)
  print("🔎 Filter Absolut →", filters)

  # Buat embedding query
  emb = generate_embedding(understood)
  emb_str = "[" + ",".join(map(str, emb)) + "]"

  # pgvector search
  base_sql = """
    SELECT
      pc.product_id,
      p.name AS product_name,
      p.price AS product_price,
      p.url AS product_url,
      pc.chunk_text,
      (pc.embedding <=> %s::vector) AS distance
    FROM products p 
    JOIN product_chunks pc ON p.id = pc.product_id
  """
  where_clause = " WHERE 1=1 "
  filter_params = []

  if filters.get("location"):
    where_clause += " AND p.shop_location ILIKE %s "
    filter_params.append(f"%{filters['location']}%")

  if filters.get("color"):
    where_clause += " AND p.variant_spec ->> 'warna' ILIKE %s "
    filter_params.append(f"%{filters['color']}%")

  final_sql = base_sql + where_clause + """
    ORDER BY pc.embedding <=> %s::vector
    LIMIT %s;
  """

  params = [emb_str] + filter_params + [emb_str, top_k]

  cur.execute(final_sql, tuple(params))
  rows = cur.fetchall()

  conn.close()
  filtered = [r for r in rows if float(r["distance"]) <= 0.3]
  return filtered


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

    results = semantic_search(query, top_k=3)

    print("\n=== HASIL PENCARIAN ===\n")

    if not results:
      print("Tidak ada hasil.")
      continue

    for i, r in enumerate(results, start=1):
      print(f"{i}. {r['product_name']} (Rp {r['product_price']})")
      print(f"   URL: {r['product_url']}")
      print(f"   Cocok Karena: {r['chunk_text']}")
      print(f"   Score: {r['distance']}\n")

    print("==============================\n")


if __name__ == "__main__":
  main()
