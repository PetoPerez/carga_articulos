from fastapi import FastAPI, File, UploadFile, Request, HTTPException
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse, StreamingResponse
from fastapi.encoders import jsonable_encoder

import os
import shutil
import base64
import re
import logging
import asyncio
import json
import pandas as pd

from typing import List, Optional
from pathlib import Path
from decimal import Decimal
from datetime import datetime, timezone
from threading import Lock
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv

from shopify_service import shopify_service
from database import db_service

# ========= Config =========
load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent

UPLOAD_DIR = BASE_DIR / "uploads"
EXCEL_DIR  = UPLOAD_DIR / "excel"
IMAGES_DIR = UPLOAD_DIR / "images"
DATA_DIR   = BASE_DIR / "data"

for d in [EXCEL_DIR, IMAGES_DIR, DATA_DIR]:
    d.mkdir(parents=True, exist_ok=True)

# Hilos para llamadas bloqueantes a Shopify (I/O bound)
SHOPIFY_WORKERS = int(os.getenv("SHOPIFY_WORKERS", "5"))
executor = ThreadPoolExecutor(max_workers=SHOPIFY_WORKERS)

app = FastAPI(title="Shopify Product Uploader", version="2.0.0")

templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))
app.mount("/static",  StaticFiles(directory=str(BASE_DIR / "static")),  name="static")
app.mount("/uploads", StaticFiles(directory=str(UPLOAD_DIR)),            name="uploads")

# ========= Estado en memoria =========
productos_data: Optional[List[dict]] = None
imagenes_cargadas: List[dict] = []

# ========= Tracking Excel =========
TRACK_XLSX = DATA_DIR / "productos_creados.xlsx"
TRACK_COLUMNS = [
    "Codigo", "Numero", "Nombre",
    "ShopifyProductId", "ShopifyVariantId", "ShopifyInventoryItemId",
    "LastKnownStock", "CreatedAt",
]
xlsx_lock = Lock()

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _load_tracked_df() -> pd.DataFrame:
    if TRACK_XLSX.exists():
        return pd.read_excel(TRACK_XLSX, engine="openpyxl", dtype={"Codigo": str, "Numero": str})
    return pd.DataFrame(columns=TRACK_COLUMNS)

def _save_tracked_df(df: pd.DataFrame) -> None:
    tmp = TRACK_XLSX.with_suffix(".tmp.xlsx")
    with xlsx_lock:
        df.to_excel(tmp, index=False)
        tmp.replace(TRACK_XLSX)

def _to_number(val):
    if val is None:
        return 0
    try:
        return int(float(str(val).strip()))
    except Exception:
        return 0

def track_created_product(codigo: str, numero: str, producto_db: dict, response_data: Optional[dict]):
    shopify_product_id = shopify_variant_id = shopify_inventory_item_id = None
    if isinstance(response_data, dict):
        product = response_data.get("product") or {}
        shopify_product_id = product.get("id")
        variants = product.get("variants") or []
        if variants:
            v0 = variants[0]
            shopify_variant_id        = v0.get("id")
            shopify_inventory_item_id = v0.get("inventory_item_id")

    existencia = _to_number(producto_db.get("Existencia", 0))
    df  = _load_tracked_df()
    now = _now_iso()
    idx = df.index[df["Codigo"] == str(codigo)].tolist() if not df.empty else []

    if idx:
        i = idx[0]
        df.at[i, "Numero"] = str(numero)
        df.at[i, "Nombre"] = producto_db.get("Nombre")
        if shopify_product_id        is not None: df.at[i, "ShopifyProductId"]       = shopify_product_id
        if shopify_variant_id        is not None: df.at[i, "ShopifyVariantId"]       = shopify_variant_id
        if shopify_inventory_item_id is not None: df.at[i, "ShopifyInventoryItemId"] = shopify_inventory_item_id
        df.at[i, "LastKnownStock"] = existencia
    else:
        df = pd.concat([df, pd.DataFrame([{
            "Codigo":                 str(codigo),
            "Numero":                 str(numero),
            "Nombre":                 producto_db.get("Nombre"),
            "ShopifyProductId":       shopify_product_id,
            "ShopifyVariantId":       shopify_variant_id,
            "ShopifyInventoryItemId": shopify_inventory_item_id,
            "LastKnownStock":         existencia,
            "CreatedAt":              now,
        }])], ignore_index=True)

    _save_tracked_df(df)
    logger.info(f"[TRACK] {codigo} guardado (stock={existencia})")

# ========= Helpers =========
def extraer_numero_referencia(filename: str) -> str:
    m = re.search(r"(\d+)", Path(filename).stem)
    return m.group(1) if m else ""

def codificar_imagen_base64(image_path: str) -> Optional[str]:
    try:
        with open(image_path, "rb") as f:
            return base64.b64encode(f.read()).decode("utf-8")
    except Exception as e:
        logger.error(f"Error codificando imagen {image_path}: {e}")
        return None

def _clear_state():
    global productos_data, imagenes_cargadas
    for directory in [EXCEL_DIR, IMAGES_DIR]:
        for f in directory.glob("*"):
            if f.is_file():
                f.unlink()
    productos_data    = None
    imagenes_cargadas = []

def _procesar_producto(item, productos_db, skus_en_shopify, imagenes):
    """Procesa UN producto — se ejecuta en el ThreadPoolExecutor."""
    codigo = item["Codigo"]
    numero = item["Numero"]

    # 1. SKU ya existe en Shopify
    if codigo.strip() in skus_en_shopify:
        return {
            "codigo": codigo, "numero": numero,
            "status": "omitido", "tiene_imagen": False,
            "mensaje": "SKU ya existe en Shopify",
        }

    # 2. Existe en la BD
    prod_db = productos_db.get(codigo)
    if not prod_db:
        return {
            "codigo": codigo, "numero": numero,
            "status": "error", "tiene_imagen": False,
            "mensaje": "Producto no encontrado en la base de datos",
        }

    # 3. Stock en 0 — no se agrega
    existencia = _to_number(prod_db.get("Existencia", 0))
    if existencia <= 0:
        return {
            "codigo":     codigo,
            "numero":     numero,
            "nombre":     prod_db.get("Nombre", ""),
            "existencia": existencia,
            "status":     "omitido",
            "tiene_imagen": False,
            "mensaje":    "Sin stock (existencia = 0)",
        }

    # 4. Imagen (opcional)
    imagen_b64  = None
    imagen_info = next(
        (img for img in imagenes if img["numero_referencia"] == numero), None
    )
    if imagen_info and os.path.exists(imagen_info["path"]):
        imagen_b64 = codificar_imagen_base64(imagen_info["path"])

    # 5. Crear en Shopify
    try:
        status_code, resp = shopify_service.create_product_with_inventory(prod_db, imagen_b64)
        ok = 200 <= status_code < 300

        resultado = {
            "codigo":                 codigo,
            "numero":                 numero,
            "nombre":                 prod_db.get("Nombre", ""),
            "existencia":             existencia,
            "status":                 "success" if ok else "error",
            "codigo_respuesta":       status_code,
            "tiene_imagen":           imagen_b64 is not None,
            "inventario_configurado": resp.get("inventory_configured", False) if isinstance(resp, dict) else False,
            "shopify_id":             resp.get("product", {}).get("id") if isinstance(resp, dict) else None,
            "mensaje":                "Creado exitosamente" if ok else (
                resp.get("error", "Error desconocido") if isinstance(resp, dict) else "Error"
            ),
        }

        if ok:
            try:
                track_created_product(codigo, numero, prod_db, resp if isinstance(resp, dict) else {})
            except Exception as e:
                logger.error(f"[TRACK] No se pudo registrar {codigo}: {e}")

        return resultado

    except Exception as e:
        logger.error(f"Error procesando {codigo}: {e}")
        return {"codigo": codigo, "numero": numero, "status": "error", "tiene_imagen": False, "mensaje": str(e)}

# ========= Rutas =========

@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    return templates.TemplateResponse("index.html", {
        "request":            request,
        "productos_cargados": productos_data is not None,
        "total_productos":    len(productos_data) if productos_data else 0,
        "imagenes_cargadas":  len(imagenes_cargadas),
        "db_connected":       db_service.test_connection(),
        "shopify_connected":  shopify_service.test_connection(),
    })

@app.get("/status")
async def get_status():
    db_ok      = db_service.test_connection()
    shopify_ok = shopify_service.test_connection()
    return JSONResponse(jsonable_encoder({
        "database": {"connected": db_ok, "info": db_service.get_database_info() if db_ok else {}},
        "shopify":  {"connected": shopify_ok},
        "files": {
            "productos_cargados": productos_data is not None,
            "total_productos":    len(productos_data) if productos_data else 0,
            "imagenes_cargadas":  len(imagenes_cargadas),
        },
    }, custom_encoder={Decimal: float}))

@app.post("/upload-excel")
async def upload_excel(excel_file: UploadFile = File(...)):
    global productos_data

    if not excel_file.filename.lower().endswith((".xlsx", ".xls")):
        raise HTTPException(400, "Solo se permiten archivos Excel (.xlsx, .xls)")

    try:
        path = EXCEL_DIR / excel_file.filename
        with open(path, "wb") as buf:
            shutil.copyfileobj(excel_file.file, buf)

        df = pd.read_excel(path, engine="openpyxl")

        # Normalizar acentos en columnas
        df.columns = [c.replace("\u00f3", "o").replace("\u00fa", "u") for c in df.columns]
        missing = [c for c in ["Codigo", "Numero"] if c not in df.columns]
        if missing:
            raise HTTPException(400, f"Faltan columnas: {', '.join(missing)}. Se esperan: Codigo, Numero")

        df = df[["Codigo", "Numero"]].dropna().astype(str)
        productos_data = df.to_dict("records")
        logger.info(f"Excel cargado: {len(productos_data)} productos")

        return JSONResponse(jsonable_encoder({
            "success":   True,
            "message":   f"{len(productos_data)} productos encontrados.",
            "productos": productos_data[:5],
            "archivo":   excel_file.filename,
        }))
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error procesando Excel: {e}")
        raise HTTPException(500, f"Error al procesar Excel: {e}")

@app.post("/upload-images")
async def upload_images(images: List[UploadFile] = File(...)):
    global imagenes_cargadas
    imagenes_cargadas = []
    procesadas, errores = [], []

    for image in images:
        if not image.filename.lower().endswith((".png", ".jpg", ".jpeg", ".webp")):
            errores.append(f"Formato no valido: {image.filename}")
            continue
        try:
            path = IMAGES_DIR / image.filename
            with open(path, "wb") as buf:
                shutil.copyfileobj(image.file, buf)
            info = {
                "filename":          image.filename,
                "path":              str(path),
                "numero_referencia": extraer_numero_referencia(image.filename),
                "size":              os.path.getsize(path),
            }
            imagenes_cargadas.append(info)
            procesadas.append(info)
        except Exception as e:
            logger.error(f"Error procesando imagen {image.filename}: {e}")
            errores.append(f"Error en {image.filename}: {e}")

    logger.info(f"Imagenes cargadas: {len(imagenes_cargadas)}")
    return JSONResponse(jsonable_encoder({
        "success":  True,
        "message":  f"{len(imagenes_cargadas)} imagenes cargadas.",
        "imagenes": procesadas,
        "errores":  errores or None,
    }))

@app.post("/create-shopify-products")
async def create_shopify_products(selected_codes: Optional[List[str]] = None):
    """
    Endpoint SSE: emite eventos en tiempo real mientras procesa.
    El cliente recibe una linea JSON por producto + un evento final con estadisticas.
    """
    global productos_data, imagenes_cargadas

    if not productos_data:
        raise HTTPException(400, "No hay productos cargados")

    a_procesar = (
        [p for p in productos_data if p["Codigo"] in selected_codes]
        if selected_codes else list(productos_data)
    )

    # Snapshot de imagenes para el executor (evita race conditions)
    imagenes_snap = list(imagenes_cargadas)

    async def event_stream():
        total = len(a_procesar)

        # — Fase 1: cargar datos en paralelo —
        yield _sse("phase", {"msg": "Consultando base de datos...", "pct": 5})
        loop = asyncio.get_event_loop()

        codigos      = [p["Codigo"] for p in a_procesar]
        productos_db = await loop.run_in_executor(
            executor, db_service.get_products_by_codes, codigos
        )

        # Heartbeat mientras carga SKUs (puede tardar con catálogos grandes)
        sku_task = loop.run_in_executor(executor, shopify_service.get_all_skus)
        tick = 0
        while not sku_task.done():
            tick += 1
            yield _sse("phase", {"msg": f"Cargando SKUs de Shopify{'.' * (tick % 4)}",  "pct": 15})
            await asyncio.sleep(1)

        skus_en_shopify = await sku_task
        logger.info(f"SKUs en Shopify: {len(skus_en_shopify)}")
        yield _sse("phase", {"msg": f"{len(skus_en_shopify)} SKUs cargados. Procesando {total} productos...", "pct": 20})

        # — Fase 2: crear en Shopify en paralelo con semaforo —
        resultados  = []
        semaforo    = asyncio.Semaphore(SHOPIFY_WORKERS)
        completados = 0

        async def procesar_uno(item):
            nonlocal completados
            async with semaforo:
                resultado = await loop.run_in_executor(
                    executor,
                    _procesar_producto,
                    item, productos_db, skus_en_shopify, imagenes_snap
                )
                completados += 1
                pct = 20 + int((completados / total) * 75)
                return resultado, pct

        tareas = [procesar_uno(item) for item in a_procesar]

        # Heartbeat mientras no llegan resultados
        pending  = set(asyncio.ensure_future(t) for t in tareas)
        tick = 0
        while pending:
            done_set, pending = await asyncio.wait(pending, timeout=1.0)
            if not done_set:
                # Ningún producto terminó en este segundo — emitir pulso
                tick += 1
                dots = "." * (tick % 4)
                yield _sse("phase", {
                    "msg": f"Procesando{dots} ({completados}/{total})",
                    "pct": 20 + int((completados / total) * 75) if total else 20,
                })
                continue
            for fut in done_set:
                resultado, pct = fut.result()
                resultados.append(resultado)
                yield _sse("progress", {
                    "pct":        pct,
                    "completado": resultado.get("codigo"),
                    "status":     resultado.get("status"),
                    "nombre":     resultado.get("nombre", ""),
                    "done":       completados,
                    "total":      total,
                })

        # — Fase 3: resumen final —
        exitosos = sum(1 for r in resultados if r["status"] == "success")
        omitidos = sum(1 for r in resultados if r["status"] == "omitido")
        errores  = sum(1 for r in resultados if r["status"] == "error")

        payload = {
            "success":    True,
            "resultados": resultados,
            "estadisticas": {
                "total_procesados": len(resultados),
                "exitosos":         exitosos,
                "omitidos":         omitidos,
                "errores":          errores,
            },
        }
        yield _sse("done", json.loads(
            json.dumps(payload, default=lambda o: float(o) if isinstance(o, Decimal) else str(o))
        ))

    return StreamingResponse(event_stream(), media_type="text/event-stream")

def _sse(event: str, data: dict) -> str:
    return f"event: {event}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"

@app.get("/search-products")
async def search_products(q: str, limit: int = 20):
    if not q or len(q) < 2:
        return JSONResponse({"productos": []})
    productos = db_service.search_products(q, limit)
    return JSONResponse(jsonable_encoder({"productos": productos}, custom_encoder={Decimal: float}))

@app.get("/tracked-products")
async def tracked_products():
    df    = _load_tracked_df()
    items = df.fillna("").to_dict(orient="records")
    return JSONResponse(jsonable_encoder({"tracked": items}))

@app.get("/download/tracked.xlsx")
async def download_tracked():
    if not TRACK_XLSX.exists():
        _save_tracked_df(pd.DataFrame(columns=TRACK_COLUMNS))
    return FileResponse(
        path=str(TRACK_XLSX),
        filename="productos_creados.xlsx",
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

@app.delete("/clear-uploads")
@app.post("/clear-uploads")
async def clear_uploads():
    try:
        _clear_state()
        logger.info("Uploads limpiados")
        return JSONResponse({"success": True, "message": "Archivos limpiados exitosamente"})
    except Exception as e:
        logger.error(f"Error limpiando uploads: {e}")
        raise HTTPException(500, f"Error al limpiar: {e}")

@app.get("/results", response_class=HTMLResponse)
async def results_page(request: Request):
    return templates.TemplateResponse("results.html", {"request": request})

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        app,
        host=os.getenv("HOST", "0.0.0.0"),
        port=int(os.getenv("PORT", 8000)),
        reload=os.getenv("DEBUG", "false").lower() == "true",
    )