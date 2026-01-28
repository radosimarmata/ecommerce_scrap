import requests
import re
import json
import os
import logging
from typing import Dict, List

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class TokopediaScraper:

  def __init__(self, output_dir: str = "data"):
    self.output_dir = output_dir
    self.headers = {
      "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
      "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    }
    os.makedirs(self.output_dir, exist_ok=True)


  def scrape(self, url: str) -> List[Dict]:
    logger.info(f"Mengambil data dari: {url}")
    try:
      resp = requests.get(url, headers=self.headers, timeout=20)
      resp.raise_for_status()
      html = resp.text
      
      # save to html
      debug_html_path = os.path.join(self.output_dir, "debug_page.html")
      with open(debug_html_path, "w", encoding="utf-8") as f:
        f.write(html)

      pattern = r'window.__cache\s*=\s*(\{.*?\})\s*;'
      match = re.search(pattern, html, re.DOTALL)
      if not match:
        match = re.search(r'window.__cache\s*=\s*(\{.*\})\s*', html, re.DOTALL)
      
      if not match:
        logger.error("Gagal menemukan JSON cache di HTML.")
        return []

      json_str = match.group(1).strip()
      data = json.loads(json_str)

      # save for debugging
      debug_path = os.path.join(self.output_dir, "debug_full_cache.json")
      with open(debug_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
      
      root = data.get("ROOT_QUERY", {})
      layout_key = next((k for k in root if k.startswith("pdpMainInfo")), None)
      
      if not layout_key:
        logger.error("Layout key tidak ditemukan.")
        return []

      layout_container = self._resolve(data, root[layout_key].get("id"))
      
      # Basic Info
      data_ref = layout_container.get("data", {})
      basic_info_ref = self._resolve(data, data_ref.get("id"))
      basic_info_id = basic_info_ref.get("basicInfo", {}).get("id")
      basic_info = self._resolve(data, basic_info_id)
      
      # Stats
      stats_ref = basic_info.get("txStats", {})
      stats_info = self._resolve(data, stats_ref.get("id"))

      # Components Iteration
      components = layout_container.get("components", [])
      
      extracted_media = []
      extracted_details = {}
      extracted_variants = []
      location = "N/A"

      for comp in components:
        comp_obj = self._resolve(data, comp.get("id"))
        c_type = comp_obj.get("type")
        c_data = comp_obj.get("data", [])

        if c_type == "product_media":
          extracted_media = self._extract_media(data, c_data)
        elif c_type == "product_detail":
          extracted_details = self._extract_detail_specs(data, c_data)
        elif c_type == "variant":
          extracted_variants = self._extract_variants(data, c_data)
      
      location = self._extract_location(data, components)
      reviews = self._extract_reviews(data)

      final_results = []
      shop_info = {
        "shop_name": basic_info.get("shopName"),
        "shop_location": location,
      }

      def construct_item(core_product_data):
        item = {}
        item.update(shop_info)
        item.update(core_product_data)
        item["product_detail"] = extracted_details
        item["product_media"] = extracted_media
        item["product_reviews"] = reviews
        return item
      
      if extracted_variants:
        for variant in extracted_variants:
          variant_data = {
            "product_name": variant["name"],
            "product_url": variant["url"],
            "product_price": variant["price"],
            "product_price_fmt": variant["price_fmt"],
            "product_stock": variant["stock"],
            "product_sold": stats_info.get("countSold"),
            "variant_spec": variant["variant_spec"]
          }
          final_results.append(construct_item(variant_data))
      else:
        content_comp = next((self._resolve(data, c.get("id")) for c in components if self._resolve(data, c.get("id")).get("type") == "product_content"), None)
        content_data = {}
        if content_comp:
          raw_content = content_comp.get("data", [])[0]
          content_obj = self._resolve(data, raw_content.get("id"))
          price_obj = self._resolve(data, content_obj.get("price", {}).get("id"))
          stock_obj =  self._resolve(data, content_obj.get("stock", {}).get("id"))
          
          content_data = {
            "product_name": content_obj.get("name") or basic_info.get("name"),
            "product_url": basic_info.get("url"),
            "product_price": price_obj.get("value"),
            "product_price_fmt": price_obj.get("priceFmt"),
            "product_stock": stock_obj.get("value"),
            "product_sold": stats_info.get("countSold"),
            "variant_spec": {}
          }
        final_results.append(construct_item(content_data))

      return final_results

    except Exception as e:
      logger.error(f"Error scraping {url}: {str(e)}")
      return []

url = "https://www.tokopedia.com/enterelectronic"