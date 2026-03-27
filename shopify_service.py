import requests
import logging
import time
import os
import re
from typing import Dict, Any, Tuple, Optional
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

MAX_RETRIES = 4
RETRY_DELAYS = [2, 5, 10, 20]


def _request_with_retry(method, url, **kwargs):
    for delay in RETRY_DELAYS:
        resp = requests.request(method, url, **kwargs)
        if resp.status_code != 429:
            return resp
        time.sleep(delay)
    return resp


class ShopifyService:
    def __init__(self):
        self.api_password = os.getenv("SHOPIFY_API_PASSWORD")
        self.store_url = os.getenv("SHOPIFY_STORE_URL")
        self.api_version = "2024-04"

    def _headers(self):
        return {
            "Content-Type": "application/json",
            "X-Shopify-Access-Token": self.api_password,
        }

    def _base(self):
        return f"https://{self.store_url}/admin/api/{self.api_version}"

    def test_connection(self) -> bool:
        try:
            r = requests.get(f"{self._base()}/shop.json", headers=self._headers())
            return r.status_code == 200
        except:
            return False

    def get_all_skus(self):
        skus = set()
        url = f"{self._base()}/variants.json"

        while url:
            resp = _request_with_retry("GET", url, headers=self._headers())
            if resp.status_code != 200:
                break

            for v in resp.json().get("variants", []):
                sku = str(v.get("sku", "")).strip()
                if sku:
                    skus.add(sku)

            link = resp.headers.get("Link", "")
            if 'rel="next"' in link:
                url = re.search(r'<([^>]+)>', link).group(1)
            else:
                url = None

        return skus

    def create_product_with_inventory(self, producto: Dict[str, Any], imagen_b64: Optional[str] = None) -> Tuple[int, Any]:
        data = {
            "product": {
                "title": producto.get("Nombre"),
                "variants": [{
                    "price": str(producto.get("PrecioListaAI", "0")),
                    "sku": str(producto.get("CodInterno")),
                    "inventory_quantity": int(producto.get("Existencia", 0)),
                }]
            }
        }

        if imagen_b64:
            data["product"]["images"] = [{"attachment": imagen_b64}]

        resp = _request_with_retry(
            "POST",
            f"{self._base()}/products.json",
            headers=self._headers(),
            json=data
        )

        try:
            return resp.status_code, resp.json()
        except:
            return resp.status_code, {}


shopify_service = ShopifyService()