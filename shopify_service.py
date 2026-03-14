import requests
import logging
import time
import os
import re
from typing import Dict, Any, Tuple, Optional
from decimal import Decimal
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

def _to_int(val) -> int:
    """Convierte cualquier valor numerico a int: '2.0' -> 2, 2.0 -> 2, None -> 0."""
    if val is None:
        return 0
    try:
        return int(float(str(val).strip()))
    except Exception:
        return 0

# Reintentos ante rate limit de Shopify
MAX_RETRIES  = 4
RETRY_DELAYS = [2, 5, 10, 20]   # segundos entre reintentos


def _request_with_retry(method: str, url: str, **kwargs) -> requests.Response:
    """Ejecuta una llamada HTTP y reintenta si Shopify responde 429."""
    for attempt, delay in enumerate(RETRY_DELAYS, start=1):
        resp = requests.request(method, url, **kwargs)
        if resp.status_code != 429:
            return resp
        wait = int(resp.headers.get("Retry-After", delay))
        logger.warning(f"Rate limit 429 en {url} — reintento {attempt}/{MAX_RETRIES} en {wait}s")
        time.sleep(wait)
    # Ultimo intento sin capturar 429
    return requests.request(method, url, **kwargs)


class ShopifyService:
    def __init__(self):
        self.api_key      = os.getenv("SHOPIFY_API_KEY", "").strip()
        self.api_password = os.getenv("SHOPIFY_API_PASSWORD", "").strip()
        self.store_url    = os.getenv("SHOPIFY_STORE_URL", "").strip()
        self.api_version  = "2024-04"

        # Cache de location_id — se obtiene una sola vez
        self._location_id: Optional[int] = None

        if not all([self.api_key, self.api_password, self.store_url]):
            logger.warning("Configuracion de Shopify incompleta.")
        else:
            logger.info(f"Shopify configurado: {self.store_url}")

    def _get_headers(self) -> Dict[str, str]:
        return {
            "Content-Type": "application/json",
            "X-Shopify-Access-Token": self.api_password,
        }

    def _get_base_url(self) -> str:
        return f"https://{self.store_url}/admin/api/{self.api_version}"

    def _get_location_id(self) -> Optional[int]:
        """Obtiene y cachea el ID de la primera ubicacion de Shopify."""
        if self._location_id is not None:
            return self._location_id
        try:
            resp = _request_with_retry(
                "GET",
                f"{self._get_base_url()}/locations.json",
                headers=self._get_headers(),
            )
            if resp.status_code == 200:
                locations = resp.json().get("locations", [])
                if locations:
                    self._location_id = locations[0]["id"]
                    logger.info(f"Location ID cacheado: {self._location_id}")
                    return self._location_id
            logger.error(f"No se pudo obtener location_id: {resp.status_code}")
        except Exception as e:
            logger.error(f"Error obteniendo location_id: {e}")
        return None

    def convert_decimal_dict(self, data: Dict[str, Any]) -> Dict[str, Any]:
        converted = {}
        for key, value in data.items():
            if isinstance(value, Decimal):
                converted[key] = float(value)
            elif isinstance(value, dict):
                converted[key] = self.convert_decimal_dict(value)
            else:
                converted[key] = value
        return converted

    # ── Todos los SKUs existentes ──────────────────────────────────────
    def get_all_skus(self) -> set:
        """Descarga todos los SKUs paginando. Devuelve set para busqueda O(1)."""
        if not all([self.api_key, self.api_password, self.store_url]):
            return set()

        skus   = set()
        url    = f"{self._get_base_url()}/variants.json"
        params = {"limit": 250, "fields": "sku"}

        try:
            while url:
                resp = _request_with_retry("GET", url, headers=self._get_headers(), params=params)
                if resp.status_code != 200:
                    logger.error(f"Error obteniendo SKUs: {resp.status_code}")
                    break

                for v in resp.json().get("variants", []):
                    s = v.get("sku", "").strip()
                    if s:
                        skus.add(s)

                link = resp.headers.get("Link", "")
                if 'rel="next"' in link:
                    match = re.search(r'<([^>]+)>;\s*rel="next"', link)
                    url    = match.group(1) if match else None
                    params = {}
                else:
                    url = None

            logger.info(f"SKUs existentes en Shopify: {len(skus)}")
        except Exception as e:
            logger.error(f"Error obteniendo todos los SKUs: {e}")

        return skus

    # ── Crear producto basico ──────────────────────────────────────────
    def create_product_basic(self, producto: Dict[str, Any], imagen_base64: Optional[str] = None) -> Tuple[int, Dict[str, Any]]:
        if not all([self.api_key, self.api_password, self.store_url]):
            return 500, {"error": "Configuracion de Shopify incompleta"}

        producto_s = self.convert_decimal_dict(producto)

        data = {
            "product": {
                "title":        producto_s.get("Nombre", "Producto sin nombre"),
                "body_html":    producto_s.get("NombreCompleto", ""),
                "vendor":       producto_s.get("Linea", ""),
                "product_type": producto_s.get("TipoDeProducto", ""),
                "variants": [{
                    "price":                str(producto_s.get("PrecioListaAI", "0.00")),
                    "sku":                  producto_s.get("CodInterno", ""),
                    "inventory_management": "shopify",
                    "inventory_policy":     "deny",
                }],
            }
        }

        if imagen_base64:
            data["product"]["images"] = [{"attachment": imagen_base64}]

        try:
            logger.info(f"Creando producto: {producto_s.get('CodInterno', 'N/A')}")
            resp = _request_with_retry(
                "POST",
                f"{self._get_base_url()}/products.json",
                headers=self._get_headers(),
                json=data,
            )
            return resp.status_code, resp.json()
        except Exception as e:
            logger.error(f"Error al crear producto: {e}")
            return 500, {"error": str(e)}

    # ── Crear producto con inventario ──────────────────────────────────
    def create_product_with_inventory(self, producto: Dict[str, Any], imagen_base64: Optional[str] = None) -> Tuple[int, Dict[str, Any]]:
        if not all([self.api_key, self.api_password, self.store_url]):
            return 500, {"error": "Configuracion de Shopify incompleta"}

        status_code, response_data = self.create_product_basic(producto, imagen_base64)

        if 200 <= status_code < 300:
            try:
                inventory_item_id = response_data["product"]["variants"][0]["inventory_item_id"]
                existencia        = _to_int(producto.get("Existencia", 0))
                logger.info(f"Configurando inventario: {existencia} unidades")

                inventory_success = self.configure_complete_inventory(inventory_item_id, existencia)
                response_data["inventory_configured"] = inventory_success
                response_data["inventory_quantity"]   = existencia

                if inventory_success:
                    logger.info("Inventario configurado correctamente")
                else:
                    logger.warning("Producto creado pero fallo configuracion de inventario")

            except Exception as e:
                logger.error(f"Error en configuracion de inventario: {e}")
                response_data["inventory_configured"] = False
                response_data["inventory_error"]      = str(e)

        return status_code, response_data

    # ── Configurar inventario (usa location_id cacheado) ──────────────
    def configure_complete_inventory(self, inventory_item_id: int, cantidad) -> bool:
        cantidad = _to_int(cantidad)
        try:
            headers     = self._get_headers()
            location_id = self._get_location_id()

            if not location_id:
                logger.error("Sin location_id — no se puede configurar inventario")
                return False

            # Marcar como tracked
            _request_with_retry(
                "PUT",
                f"{self._get_base_url()}/inventory_items/{inventory_item_id}.json",
                headers=headers,
                json={"inventory_item": {
                    "id":               inventory_item_id,
                    "tracked":          True,
                    "require_shipping": True,
                }},
            )

            # Establecer nivel de inventario
            inv_resp = _request_with_retry(
                "POST",
                f"{self._get_base_url()}/inventory_levels/set.json",
                headers=headers,
                json={
                    "location_id":       location_id,
                    "inventory_item_id": inventory_item_id,
                    "available":         cantidad,
                },
            )
            logger.info(f"Inventario establecido: {inv_resp.status_code}")
            return inv_resp.status_code == 200

        except Exception as e:
            logger.error(f"Error configurando inventario: {e}")
            return False

    # ── Obtener productos ──────────────────────────────────────────────
    def get_products(self, limit: int = 50) -> Tuple[int, Dict[str, Any]]:
        if not all([self.api_key, self.api_password, self.store_url]):
            return 500, {"error": "Configuracion de Shopify incompleta"}
        try:
            resp = _request_with_retry(
                "GET",
                f"{self._get_base_url()}/products.json?limit={limit}",
                headers=self._get_headers(),
            )
            return resp.status_code, resp.json()
        except Exception as e:
            logger.error(f"Error al obtener productos: {e}")
            return 500, {"error": str(e)}

    # ── Test de conexion ───────────────────────────────────────────────
    def test_connection(self) -> bool:
        try:
            status_code, _ = self.get_products(limit=1)
            return 200 <= status_code < 300
        except Exception:
            return False


# Instancia global
shopify_service = ShopifyService()