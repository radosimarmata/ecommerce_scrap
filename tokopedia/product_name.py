import re
import spacy
from typing import List, Dict

# =========================
# LOAD SPACY
# =========================
nlp = spacy.load("en_core_web_sm")

# =========================
# STATIC CONFIG
# =========================

ANDROID_OS = [
  "ASUS", "SAMSUNG", "XIAOMI", "OPPO", "VIVO", "SONY", "HUAWEI", "NOKIA", "REALME", "MOTOROLA", "INFINIX", "LG", "HISENSE", "ITEL", "TECNO", "RED MAGIC", "ADVAN", "GOOGLE PIXEL", "IQOO", "SHARP",  "ONEPLUS", "FAIRPHONE", "POCO"
]

IOS = [
  "APPLE", "IPHONE"
]

MODEL_STOPWORDS = {
  "NEW", "GARANSI", "RESMI",
  "INDONESIA", "PAKET", "HEMAT", "-", "|", "HP", "/", "Handphone", "Ibox"
}

# Regex untuk RAM / Storage / Network, akan diabaikan
RAM_STORAGE_NETWORK_REGEX = re.compile(
  r'^(\d+(\+?\d+)?[Gg][Bb]|[45][Gg])$'
)

# Regex model token (alfanumerik + dash / slash)
MODEL_PATTERN = re.compile(r'^[A-Z0-9]+[A-Z0-9\-\/]*$', re.I)

# =========================
# UTIL FUNCTIONS
# =========================

def detect_brand(text: str, BRAND: list) -> str | None:
  text = text.upper()
  for brand in BRAND:
    if brand in text:
      return brand
  return None

def extract_model(text: str, brand: str, max_tokens: int = 6) -> str | None:
  if not brand:
    return None
  
  doc = nlp(text.upper())
  text = text.upper()
  brand_index = text.find(brand)
  substring = text[brand_index + len(brand):].strip()
  
  tokens = [t.text for t in doc if not t.is_punct]
  if brand not in tokens:
    return None
  start = tokens.index(brand) + 1
  candidates = []

  tokens = re.split(r'[\s|,]+', substring)
  model_tokens = []

  for tok in tokens:
    tok_clean = tok.strip()
    if not tok_clean:
      continue
    # hentikan kalau stopword
    if tok_clean in MODEL_STOPWORDS:
      break
    # hentikan kalau token RAM/storage/network
    if re.match(RAM_STORAGE_NETWORK_REGEX, tok_clean):
      break
    # ambil token valid untuk model
    if re.match(MODEL_PATTERN, tok_clean):
      model_tokens.append(tok_clean)
    # safety max token
    if len(model_tokens) >= max_tokens:
      break

  if not model_tokens:
    return None

  return " ".join(model_tokens)

# =========================
# MAIN FUNCTION (FINAL)
# =========================

def classify_product(
  product_name: str,
  category_name: str
) -> Dict:
  category_name = category_name.upper()
  text = product_name.upper()

  CATEGORY_BRANDS = {
      "ANDROID OS": ANDROID_OS,
      "IOS": IOS
  }

  brands_list = CATEGORY_BRANDS.get(category_name)

  if brands_list:
    brand = detect_brand(text, brands_list)
    model = extract_model(product_name, brand)
    normalized = f"{brand} {model}" if brand and model else brand
    return {
      "brand": brand,
      "model": model,
      "normalized_name": normalized
    }

  return {
    "brand": None,
    "model": None,
    "normalized_name": None
  }


