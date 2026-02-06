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

  def _clean_text(self, text: str) -> str:
    if not text:
      return ""
    text = re.sub(r'_{2,}', '', text)
    text = re.sub(r'\n\s*\n\s*\n+', '\n\n', text)
    lines = [line.strip() for line in text.split("\n") if line.strip()]
    cleaned_lines = ["- " + line.lstrip("-").strip() if line.strip().startswith("-") else line for line in lines]
    return "\n".join(cleaned_lines)

  def _normalize_key(self, key: str) -> str:
    if not key:
      return "unknown_key"
    key = key.strip().lower()
    key = key.replace(" ", "_")
    return re.sub(r'[^a-z0-9_]+', '', key)

  def _resolve(self, data_cache: Dict, ref_id: str) -> Dict:
    if not ref_id:
      return {}
    return data_cache.get(ref_id, {})

  def _extract_media(self, data_cache: Dict, component_data: List) -> List[Dict]:
    results = []
    for item in component_data:
      media_group = self._resolve(data_cache, item.get("id"))
      media_list = media_group.get("media", [])
      for m_item in media_list:
        m_obj = self._resolve(data_cache, m_item.get("id"))
        results.append({
          "url_original": m_obj.get("URLOriginal"),
          "url_thumbnail": m_obj.get("URLThumbnail"),
          "url_max_res": m_obj.get("URLMaxRes"),
          "url_video_android": m_obj.get("videoURLAndroid"),
          "prefix": m_obj.get("prefix"),
          "suffix": m_obj.get("suffix")
        })
    return results

  def _extract_detail_specs(self, data_cache: Dict, component_data: List) -> Dict:
    specs = {}
    for item in component_data:
      detail_group = self._resolve(data_cache, item.get("id"))
      content_list = detail_group.get("content", [])
      for content_item in content_list:
        c_obj = self._resolve(data_cache, content_item.get("id"))
        title = c_obj.get("title")
        subtitle = c_obj.get("subtitle")
        
        if title:
          clean_key = self._normalize_key(title)
          if clean_key == "deskripsi":
            specs[clean_key] = self._clean_text(subtitle)
          else:
            specs[clean_key] = subtitle

      if "productDetailDescription" in detail_group:
        description_ref = detail_group.get("productDetailDescription", {})
        description_obj = self._resolve(data_cache, description_ref.get("id"))
        if description_obj:
          desc_text = description_obj.get("content", "")
          specs["deskripsi"] = self._clean_text(desc_text)
    return specs

  def _extract_variants(self, data_cache: Dict, component_data: List) -> List[Dict]:
    variants = []
    variant_keys = []

    for item in component_data:
      v_group = self._resolve(data_cache, item.get("id"))
      
      raw_variants = v_group.get("variants", [])
      for v_meta in raw_variants:
        v_meta_obj = self._resolve(data_cache, v_meta.get("id"))
        if v_meta_obj:
          variant_keys.append(self._normalize_key(v_meta_obj.get("name", "")))

      children = v_group.get("children", [])
      for child in children:
        child_obj = self._resolve(data_cache, child.get("id"))
        if not child_obj:
          continue

        stock_ref = child_obj.get("stock", {})
        stock_obj = self._resolve(data_cache, stock_ref.get("id")) if isinstance(stock_ref, dict) else {}

        variant_map = {}
        option_names = child_obj.get("optionName", {}).get("json", [])
        for idx, opt_val in enumerate(option_names):
          if idx < len(variant_keys):
            variant_map[variant_keys[idx]] = opt_val

        variants.append({
          "name": child_obj.get("productName"),
          "url": child_obj.get("productURL"),
          "price": child_obj.get("price"),
          "price_fmt": f"Rp {child_obj.get("price"):,.0f}".replace(",", "."),
          "stock": stock_obj.get("stock", 0),
          "variant_spec": variant_map,
          "is_cod": child_obj.get("isCOD")
        })
    return variants

  def _extract_location(self, data_cache: Dict, components: List) -> str:
    for comp in components:
      c_obj = self._resolve(data_cache, comp.get("id"))
      
      if c_obj.get("name") == "shipment_v4":
        level1_refs = c_obj.get("data", [])
        
        for l1_ref in level1_refs:
          shipment_container = self._resolve(data_cache, l1_ref.get("id"))
          level2_refs = shipment_container.get("data", [])
          
          for l2_ref in level2_refs:
            shipment_item = self._resolve(data_cache, l2_ref.get("id"))
            wh_info = shipment_item.get("warehouse_info")
            
            if isinstance(wh_info, dict) and wh_info.get("type") == "id":
              wh_obj = self._resolve(data_cache, wh_info.get("id"))
              city = wh_obj.get("city_name")
              if city:
                return city
            
            elif isinstance(wh_info, dict):
              city = wh_info.get("city_name")
              if city:
                return city

    return None

  def _extract_reviews(self, data_cache: Dict) -> Dict:
    root = data_cache.get("ROOT_QUERY", {})
    review_key = next((k for k in root if k.startswith("productrevGetProductRatingAndTopics")), None)
    
    if not review_key:
      return {}

    review_container = self._resolve(data_cache, root[review_key].get("id"))
    rating_data = self._resolve(data_cache, review_container.get("rating", {}).get("id"))
    
    topics_summary = {}
    for t_ref in review_container.get("topics", []):
      t_obj = self._resolve(data_cache, t_ref.get("id"))
      if t_obj:
        topics_summary[self._normalize_key(t_obj.get("formatted"))] = {
          "score": t_obj.get("rating"),
          "count": t_obj.get("reviewCount")
        }

    return {
      "total_rating": rating_data.get("totalRating"),
      "average_score": rating_data.get("ratingScore"),
      "topics": topics_summary
    }
  
  def generate_text_output(self, item: Dict) -> str:
    variant_spec = item.get("variant_spec", {})
    variant_str = "\n".join(
      f"{k.title()}: {v}" for k, v in variant_spec.items() if k and v
    )
    
    product_name = item.get("product_name", "N/A")
    shop_name = item.get('shop_name', 'N/A')
    shop_location = item.get('shop_location', 'N/A')
    
    price = item.get('product_price')

    details = item.get("product_detail", {})
    description = details.get("deskripsi", "Deskripsi produk tidak tersedia.")
    
    reviews = item.get("product_reviews", {})
    avg_score = reviews.get("average_score", 0.0)
    avg_score = float(avg_score)
    total_rating = reviews.get("total_rating", 0)
    
    review_summary = f"Rating {avg_score:.1f} dari {total_rating} ulasan."
    topics = reviews.get("topics", {})
    
    if topics:
      sorted_topics = sorted(topics.items(), key=lambda i: i[1]['count'], reverse=True)
      topic_phrases = []
      
      for key, data in sorted_topics[:5]:
        phrase = key.replace('_', ' ').title()
        topic_phrases.append(f"{phrase} ({data['score']:.1f}/5)") 
          
      if topic_phrases:
        review_summary += "\nKonsumen menilai: " + "; ".join(topic_phrases) + "."
      else:
        review_summary += "\nDetail ulasan topik tidak tersedia."
    else:
      review_summary += "\nDetail ulasan topik tidak tersedia."
        
    detail_lines = []
    for key, value in details.items():
      if key not in ['deskripsi'] and value and value != 'n/a':
        detail_lines.append(f"{key.replace('_', ' ').title()}: {str(value).title()}")
    
    if not detail_lines:
      detail_lines.append("Detail teknis lain tidak tersedia.")

    output = f"Produk: {product_name}\n"
    output += f"{variant_str}\n"
    output += f"Toko: {shop_name}\n"
    output += f"Lokasi: {shop_location}\n"
    output += f"Harga: {price}\n"
    output += "\n"
    
    output += "Deskripsi:\n"
    output += f"{description}\n"
    output += "\n"

    output += "Ringkasan Ulasan:\n"
    output += review_summary
    
    return output.strip()

  def scrape(self, url: str) -> List[Dict]:
    logger.info(f"URL: {url}")
    try:
      resp = requests.get(url, headers=self.headers, timeout=20)
      resp.raise_for_status()
      html = resp.text
      
      # save to html
      # debug_html_path = os.path.join(self.output_dir, f"debug_page.html")
      # with open(debug_html_path, "w", encoding="utf-8") as f:
      #   f.write(html)

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
      # debug_path = os.path.join(self.output_dir, f"debug_full_cache.json")
      # with open(debug_path, "w", encoding="utf-8") as f:
      #   json.dump(data, f, ensure_ascii=False, indent=2)
      
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

  def save_results(self, data: List[Dict], filename_prefix: str = "result"):
    if not data:
      logger.warning("Tidak ada data untuk disimpan.")
      return

    filepath = os.path.join(self.output_dir, f"{filename_prefix}_full.json")
    with open(filepath, "w", encoding="utf-8") as f:
      json.dump(data, f, ensure_ascii=False, indent=2)
    logger.info(f"Data disimpan ke: {filepath}")

    filepath_text = os.path.join(self.output_dir, f"{filename_prefix}_output.txt")
    text_outputs = []
    for idx, item in enumerate(data):
      separator = f"\n{'-' * 30} PRODUK VARIAN {idx+1} {'-' * 30}\n" if idx > 0 else ""
      text_outputs.append(separator + self.generate_text_output(data[0]))

    with open(filepath_text, "w", encoding="utf-8") as f:
      f.write("\n".join(text_outputs))
    logger.info(f"Data Text disimpan ke: {filepath_text}")

# --- Main Execution ---
if __name__ == "__main__":
  # url = "https://www.tokopedia.com/huawei/huawei-matepad-se-11-tablet-4-128gb-fhd-eye-comfort-display-7700mah-metal-unibody-grey-78825?t_id=1770013758049&t_st=1&t_pp=homepage&t_efo=pure_goods_card&t_ef=homepage&t_sm=rec_homepage_outer_flow&t_spt=homepage"
  url = "https://www.tokopedia.com/enterelectronic/lg-oled55c4psa-oled-evo-4k-smart-tv-55-inch-dolby-vision-atmos-120hz-lg-55c4-55c4psa-oled55c4?extParam=ivf%3Dfalse%26search_id%3D2026020603502963923F49E524130CEJ4V"
  
  scraper = TokopediaScraper()
  results = scraper.scrape(url)
  
  scraper.save_results(results, "tokopedia_data")