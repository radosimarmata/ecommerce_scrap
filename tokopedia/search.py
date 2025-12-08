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
    "Ubah kueri pencarian e-commerce menjadi deskripsi yang singkat, jelas, "
    "dan natural. Tidak perlu memasukkan format seperti variant::<key>, detail::<key>, "
    "chunk types, atau struktur khusus lainnya. "
    "Fokus pada maksud dan konteks pencarian pengguna saja.\n\n"
    "Contoh:\n"
    "- 'cari xiaomi terbaru' -> 'Mencari produk smartphone Xiaomi model terbaru.'\n"
    "- 'harga iphone termurah' -> 'Mencari iPhone dengan harga paling murah.'\n"
    "- 'sepatu nike warna hitam ukuran 42' -> 'Mencari sepatu Nike warna hitam ukuran 42.'\n\n"
    "Berikan hanya satu kalimat natural."
  )

  resp = client.chat.completions.create(
    model="gpt-4o-mini",
    messages=[
      {"role": "system", "content": system_prompt},
      {"role": "user", "content": query}
    ],
    temperature=0.0
  )

  return resp.choices[0].message.content.strip()


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
  understood = ai_understand(user_query)
  print("\n🧠 AI Pahami Query →", understood)

  # Buat embedding query
  emb = generate_embedding(understood)
  emb_str = "[" + ",".join(map(str, emb)) + "]"

  # pgvector search
  sql = """
    SELECT
      pc.product_id,
      p.name AS product_name,
      p.price AS product_price,
      p.url AS product_url,
      ct.name AS chunk_type,
      pc.chunk_text,
      (pc.embedding <=> %s::vector) AS distance
    FROM product_chunks pc
    JOIN products p ON p.id = pc.product_id
    LEFT JOIN chunk_types ct ON ct.id = pc.chunk_type_id
    ORDER BY pc.embedding <=> %s::vector
    LIMIT %s;
  """

  cur.execute(sql, (emb_str, emb_str, top_k))
  rows = cur.fetchall()

  conn.close()
  filtered = [r for r in rows if float(r["distance"]) <= 0.5]
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

    results = semantic_search(query, top_k=10)

    print("\n=== HASIL PENCARIAN ===\n")

    if not results:
      print("Tidak ada hasil.")
      continue

    for i, r in enumerate(results, start=1):
      print(f"{i}. {r['product_name']} (Rp {r['product_price']})")
      print(f"   URL: {r['product_url']}")
      print(f"   Chunk Type: {r['chunk_type']}")
      print(f"   Cocok Karena: {r['chunk_text']}")
      print(f"   Score: {r['distance']}\n")

    print("==============================\n")


if __name__ == "__main__":
  main()
