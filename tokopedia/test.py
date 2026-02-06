from product_name import classify_product

products = [
  "HANDPHONE APPLE IPHONE 15 PROMAX 128GB - GREEN"
]

# =========================
# RUN TESTING
# =========================
l3 = "iOS"
for p in products:
  normalized = classify_product(p, l3)
  print(normalized.get('normalized_name'))