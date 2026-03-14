from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from urllib.parse import quote_plus
import os
import logging
from typing import Dict, Any, Optional, List
from dotenv import load_dotenv

BATCHSIZE = 100

load_dotenv()

logger = logging.getLogger(__name__)


class DatabaseService:
    def __init__(self):
        user     = os.getenv("DB_USER", "").strip()
        password = os.getenv("DB_PASSWORD", "").strip()
        host     = os.getenv("DB_HOST", "").strip()
        port     = os.getenv("DB_PORT", "1435").strip()

        if not all([user, password, host]):
            logger.warning("Credenciales de BD incompletas en .env")
            self.engine       = None
            self.SessionLocal = None
            return

        # quote_plus escapa caracteres especiales en la contraseña
        # ej: # → %23  |  @ → %40  |  & → %26
        password_encoded = quote_plus(password)
        

        # URL sin nombre de BD — igual que la versión que funcionaba
        self.database_url = (
            f"mssql+pymssql://{user}:{password_encoded}@{host}:{port}"
        )

        self.engine       = None
        self.SessionLocal = None

        try:
            self.engine = create_engine(
                self.database_url,
                connect_args={
                    "login_timeout": 60,
                    "timeout":       60,
                    "tds_version":   "7.0",
                },
            )
            self.SessionLocal = sessionmaker(
                autocommit=False, autoflush=False, bind=self.engine
            )
            if self.test_connection():
                logger.info("Conexión a BD establecida y verificada")
            else:
                logger.error("Fallo en la verificación de conexión")
        except Exception as e:
            logger.error(f"Error al conectar con la BD: {e}")
            self.engine       = None
            self.SessionLocal = None

    def get_product_by_code(self, codigo: str) -> Optional[Dict[str, Any]]:
        if not self.engine or not codigo or not codigo.strip():
            return None
        try:
            with self.engine.connect() as conn:
                sql = text("""
                    SELECT CodInterno, Nombre, NombreCompleto,
                           PrecioListaAI, Existencia, Linea, TipoDeProducto
                    FROM OnLineProductos
                    WHERE CodInterno = :codigo
                """)
                result = conn.execute(sql, {"codigo": codigo.strip()}).mappings().first()
                if result:
                    logger.info(f"Producto encontrado: {codigo}")
                    return dict(result)
                logger.warning(f"Producto no encontrado: {codigo}")
                return None
        except Exception as e:
            logger.error(f"Error al consultar producto {codigo}: {e}")
            return None

    def get_products_by_codes(self, codigos: list) -> Dict[str, Dict[str, Any]]:
        if not self.engine or not codigos:
            return {}

        codigos_limpios = [c.strip() for c in codigos if c and c.strip()]
        if not codigos_limpios:
            return {}

        select_cols = [
            "CodInterno", "Nombre", "NombreCompleto",
            "PrecioListaAI", "Existencia", "Linea", "TipoDeProducto",
        ]
        select_str = ", ".join(select_cols)
        productos: Dict[str, Dict[str, Any]] = {}

        try:
            with self.engine.connect() as conn:
                for i in range(0, len(codigos_limpios), BATCHSIZE):
                    chunk        = codigos_limpios[i:i + BATCHSIZE]
                    placeholders = ", ".join([f":c{j}" for j in range(len(chunk))])
                    sql          = text(f"""
                        SELECT {select_str}
                        FROM OnLineProductos
                        WHERE CodInterno IN ({placeholders})
                    """)
                    params  = {f"c{j}": val for j, val in enumerate(chunk)}
                    results = conn.execute(sql, params).mappings().fetchall()
                    for row in results:
                        d   = dict(row)
                        key = d.get("CodInterno")
                        if key:
                            productos[key] = d

            logger.info(
                f"Encontrados {len(productos)} de {len(codigos_limpios)} productos"
            )
        except Exception as e:
            logger.error(f"Error al consultar múltiples productos: {e}")

        return productos

    def test_connection(self) -> bool:
        if not self.engine:
            return False
        try:
            with self.engine.connect() as conn:
                return conn.execute(text("SELECT 1 AS test")).scalar() == 1
        except Exception as e:
            logger.error(f"Error al probar conexión: {e}")
            return False

    def get_database_info(self) -> Dict[str, Any]:
        if not self.engine:
            return {"error": "Sin conexión", "conexion_exitosa": False}
        try:
            with self.engine.connect() as conn:
                total = conn.execute(
                    text("SELECT COUNT(*) FROM OnLineProductos")
                ).scalar()
                try:
                    ultima = conn.execute(
                        text("SELECT MAX(FechaModificacion) FROM OnLineProductos")
                    ).scalar()
                except Exception:
                    ultima = "No disponible"
                return {
                    "total_productos":     total,
                    "ultima_modificacion": ultima,
                    "conexion_exitosa":    True,
                }
        except Exception as e:
            logger.error(f"Error al obtener info de BD: {e}")
            return {"error": str(e), "conexion_exitosa": False}

    def search_products(self, termino: str, limit: int = 50) -> list:
        if not self.engine or not termino or not termino.strip():
            return []
        try:
            with self.engine.connect() as conn:
                sql = text("""
                    SELECT TOP (:limit)
                        CodInterno, Nombre, NombreCompleto,
                        PrecioListaAI, Existencia, Linea
                    FROM OnLineProductos
                    WHERE CodInterno     LIKE :termino
                       OR Nombre         LIKE :termino
                       OR NombreCompleto LIKE :termino
                    ORDER BY Nombre
                """)
                results = conn.execute(
                    sql,
                    {"limit": limit, "termino": f"%{termino.strip()}%"},
                ).mappings().fetchall()
                productos = [dict(r) for r in results]
                logger.info(f"Búsqueda '{termino}': {len(productos)} resultados")
                return productos
        except Exception as e:
            logger.error(f"Error en búsqueda: {e}")
            return []

    def get_low_stock_products(self, umbral: int = 5) -> list:
        if not self.engine:
            return []
        try:
            with self.engine.connect() as conn:
                sql = text("""
                    SELECT CodInterno, Nombre, Existencia, Linea
                    FROM OnLineProductos
                    WHERE Existencia <= :umbral
                    ORDER BY Existencia ASC, Nombre
                """)
                results  = conn.execute(sql, {"umbral": umbral}).mappings().fetchall()
                productos = [dict(r) for r in results]
                logger.info(f"Productos con stock <= {umbral}: {len(productos)}")
                return productos
        except Exception as e:
            logger.error(f"Error al obtener stock bajo: {e}")
            return []

    def close(self):
        if self.engine:
            self.engine.dispose()
            logger.info("Conexión a BD cerrada")


# Instancia global
db_service = DatabaseService()