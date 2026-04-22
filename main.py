import os
import pymysql
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional, Dict, Any

app = FastAPI(title="API Database Only", version="2.0.0")

# =========================
# CORS
# =========================
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# =========================
# ENV
# =========================
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_NAME = os.getenv("DB_NAME_BASE", "base_ambev")

# =========================
# CONEXÃO
# =========================
def get_conn():
    try:
        return pymysql.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME,
            port=DB_PORT,
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=False,
            charset="utf8mb4",
        )
    except Exception as e:
        print("Erro conexão:", e)
        return None

# =========================
# ROOT
# =========================
@app.get("/")
def root():
    return {"status": "API Database Only 🚀"}

@app.get("/health")
def health():
    return {"status": "ok"}

# =========================
# LISTAR COM FILTROS SIMPLES
# =========================
@app.get("/estoque")
def listar(
    qd: Optional[str] = None,
    rua: Optional[str] = None,
    produto: Optional[str] = None,
    linha: Optional[str] = None,
    area: Optional[str] = None,
    lote: Optional[str] = None,
):
    conn = get_conn()
    if not conn:
        raise HTTPException(500, "Erro conexão banco")

    try:
        sql = "SELECT * FROM estoque WHERE 1=1"
        params = []

        if qd:
            sql += " AND QD LIKE %s"
            params.append(f"%{qd}%")

        if rua:
            sql += " AND RUA LIKE %s"
            params.append(f"%{rua}%")

        if produto:
            sql += " AND PRODUTO LIKE %s"
            params.append(f"%{produto}%")

        if linha:
            sql += " AND linha LIKE %s"
            params.append(f"%{linha}%")

        if area:
            sql += " AND AREA LIKE %s"
            params.append(f"%{area}%")

        if lote:
            sql += " AND lote LIKE %s"
            params.append(f"%{lote}%")

        sql += " ORDER BY ID DESC"

        with conn.cursor() as cursor:
            cursor.execute(sql, params)
            return cursor.fetchall()

    finally:
        conn.close()

# =========================
# OBTER POR ID
# =========================
@app.get("/estoque/{item_id}")
def obter(item_id: int):
    conn = get_conn()
    if not conn:
        raise HTTPException(500, "Erro conexão banco")

    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT * FROM estoque WHERE ID = %s", (item_id,))
            row = cursor.fetchone()

        if not row:
            raise HTTPException(404, "Item não encontrado")

        return row

    finally:
        conn.close()

# =========================
# INSERT GENÉRICO
# =========================
@app.post("/estoque")
def inserir(data: Dict[str, Any]):
    conn = get_conn()
    if not conn:
        raise HTTPException(500, "Erro conexão banco")

    try:
        if not data:
            raise HTTPException(400, "Payload vazio")

        cols = list(data.keys())
        fields_sql = ", ".join(f"`{c}`" for c in cols)
        placeholders = ", ".join(["%s"] * len(cols))
        values = [data[c] for c in cols]

        with conn.cursor() as cursor:
            cursor.execute(
                f"INSERT INTO estoque ({fields_sql}) VALUES ({placeholders})",
                values,
            )
            new_id = cursor.lastrowid

        conn.commit()
        return {"status": "ok", "ID": new_id}

    except Exception as e:
        conn.rollback()
        raise HTTPException(500, str(e))

    finally:
        conn.close()

# =========================
# UPDATE GENÉRICO
# =========================
@app.put("/estoque/{item_id}")
def atualizar(item_id: int, data: Dict[str, Any]):
    conn = get_conn()
    if not conn:
        raise HTTPException(500, "Erro conexão banco")

    try:
        if not data:
            raise HTTPException(400, "Nada para atualizar")

        with conn.cursor() as cursor:
            cursor.execute("SELECT ID FROM estoque WHERE ID = %s", (item_id,))
            if not cursor.fetchone():
                raise HTTPException(404, "Item não encontrado")

            cols = list(data.keys())
            set_sql = ", ".join(f"`{c}` = %s" for c in cols)
            values = [data[c] for c in cols]
            values.append(item_id)

            cursor.execute(
                f"UPDATE estoque SET {set_sql} WHERE ID = %s",
                values,
            )

        conn.commit()
        return {"status": "ok"}

    except HTTPException:
        conn.rollback()
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(500, str(e))

    finally:
        conn.close()

# =========================
# DELETE
# =========================
@app.delete("/estoque/{item_id}")
def deletar(item_id: int):
    conn = get_conn()
    if not conn:
        raise HTTPException(500, "Erro conexão banco")

    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT ID FROM estoque WHERE ID = %s", (item_id,))
            if not cursor.fetchone():
                raise HTTPException(404, "Item não encontrado")

            cursor.execute("DELETE FROM estoque WHERE ID = %s", (item_id,))

        conn.commit()
        return {"status": "ok"}

    except HTTPException:
        conn.rollback()
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(500, str(e))

    finally:
        conn.close()
