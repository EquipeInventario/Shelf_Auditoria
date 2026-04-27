import os
from typing import Any, Dict, Optional

import pymysql
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="API Base Ambev", version="4.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = int(os.getenv("DB_PORT", "3306"))

DB_NAME_BASE = os.getenv("DB_NAME_BASE", "base_ambev")
DB_NAME_INVENTARIO = os.getenv("DB_NAME_INVENTARIO", "inventario")

TABLES = {
    "estoque": {"db": DB_NAME_BASE, "pk": "ID"},
    "historico_alteracoes": {"db": DB_NAME_BASE, "pk": "ID"},
    "produtos": {"db": DB_NAME_BASE, "pk": "ID"},
    "log": {"db": DB_NAME_INVENTARIO, "pk": "id"},
}


def get_conn(database_name: str):
    if not DB_HOST or not DB_USER or not DB_PASSWORD:
        return None

    try:
        return pymysql.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=database_name,
            port=DB_PORT,
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=False,
            charset="utf8mb4",
        )
    except Exception as e:
        print("Erro conexão:", e)
        return None


def get_table_cfg(table_name: str):
    cfg = TABLES.get(table_name)
    if not cfg:
        raise HTTPException(status_code=404, detail="Tabela não suportada")
    return cfg


def _open_db(table_name: str):
    cfg = get_table_cfg(table_name)
    conn = get_conn(cfg["db"])
    if not conn:
        raise HTTPException(
            status_code=500,
            detail=f"Erro na conexão com o banco {cfg['db']}",
        )
    return conn, cfg["pk"]


def _ensure_dict(payload: Any) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="Payload inválido")
    return payload


def _query_params_to_where(request: Request):
    params = dict(request.query_params)
    filters = []
    values = []

    limit = params.pop("limit", None)
    offset = params.pop("offset", None)
    order_by = params.pop("order_by", None)
    order_dir = (params.pop("order_dir", "DESC") or "DESC").upper()

    for key, value in params.items():
        if value is None or str(value).strip() == "":
            continue
        filters.append(f"`{key}` = %s")
        values.append(value)

    return filters, values, limit, offset, order_by, order_dir


def _select_all(table_name: str, request: Request):
    conn, _ = _open_db(table_name)

    try:
        filters, values, limit, offset, order_by, order_dir = _query_params_to_where(request)

        sql = f"SELECT * FROM `{table_name}`"
        if filters:
            sql += " WHERE " + " AND ".join(filters)

        if order_by:
            sql += f" ORDER BY `{order_by}` {order_dir if order_dir in ('ASC', 'DESC') else 'DESC'}"
        else:
            sql += " ORDER BY 1 DESC"

        if limit is not None:
            sql += " LIMIT %s"
            values.append(int(limit))
            if offset is not None:
                sql += " OFFSET %s"
                values.append(int(offset))

        with conn.cursor() as cursor:
            cursor.execute(sql, values)
            return cursor.fetchall()
    finally:
        conn.close()


def _select_by_id(table_name: str, item_id: int):
    conn, pk = _open_db(table_name)

    try:
        with conn.cursor() as cursor:
            cursor.execute(
                f"SELECT * FROM `{table_name}` WHERE `{pk}` = %s LIMIT 1",
                (item_id,),
            )
            row = cursor.fetchone()

        if not row:
            raise HTTPException(status_code=404, detail="Registro não encontrado")

        return row
    finally:
        conn.close()


def _insert_row(table_name: str, data: Dict[str, Any]):
    conn, _ = _open_db(table_name)

    try:
        data = _ensure_dict(data)
        if not data:
            raise HTTPException(status_code=400, detail="Payload vazio")

        cols = list(data.keys())
        fields_sql = ", ".join(f"`{c}`" for c in cols)
        placeholders = ", ".join(["%s"] * len(cols))
        values = [data[c] for c in cols]

        with conn.cursor() as cursor:
            cursor.execute(
                f"INSERT INTO `{table_name}` ({fields_sql}) VALUES ({placeholders})",
                values,
            )
            new_id = cursor.lastrowid

        conn.commit()
        return {"status": "ok", "ID": new_id}
    except HTTPException:
        conn.rollback()
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


def _update_row(table_name: str, item_id: int, data: Dict[str, Any]):
    conn, pk = _open_db(table_name)

    try:
        data = _ensure_dict(data)
        if not data:
            raise HTTPException(status_code=400, detail="Nada para atualizar")

        with conn.cursor() as cursor:
            cursor.execute(
                f"SELECT `{pk}` FROM `{table_name}` WHERE `{pk}` = %s LIMIT 1",
                (item_id,),
            )
            if not cursor.fetchone():
                raise HTTPException(status_code=404, detail="Registro não encontrado")

            cols = list(data.keys())
            set_sql = ", ".join(f"`{c}` = %s" for c in cols)
            values = [data[c] for c in cols]
            values.append(item_id)

            cursor.execute(
                f"UPDATE `{table_name}` SET {set_sql} WHERE `{pk}` = %s",
                values,
            )

        conn.commit()
        return {"status": "ok"}
    except HTTPException:
        conn.rollback()
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


def _delete_row(table_name: str, item_id: int):
    conn, pk = _open_db(table_name)

    try:
        with conn.cursor() as cursor:
            cursor.execute(
                f"SELECT `{pk}` FROM `{table_name}` WHERE `{pk}` = %s LIMIT 1",
                (item_id,),
            )
            if not cursor.fetchone():
                raise HTTPException(status_code=404, detail="Registro não encontrado")

            cursor.execute(
                f"DELETE FROM `{table_name}` WHERE `{pk}` = %s",
                (item_id,),
            )

        conn.commit()
        return {"status": "ok"}
    except HTTPException:
        conn.rollback()
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


def _norm_text(value: Any) -> str:
    return ("" if value is None else str(value)).strip()


def _norm_int(value: Any, default: Optional[int] = None) -> Optional[int]:
    if value is None or isinstance(value, bool):
        return default
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    text = str(value).strip()
    if not text:
        return default
    text = text.replace(".", "").replace(",", ".")
    try:
        return int(float(text))
    except Exception:
        return default


def _fetch_produto_row(conn, produto: str):
    produto = _norm_text(produto)
    if not produto:
        return None

    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT ID, PRODUTO, DESCRICAO, ABREVIACAO, SHELF, VALIDADE_DIAS, PACKS,
                   exigeFornecedor, exigeLote, tipo_produto, exigeLinha, exigeDocExtra
            FROM produtos
            WHERE PRODUTO = %s
            LIMIT 1
            """,
            (produto,),
        )
        return cursor.fetchone()


def _get_produto_packs(produto: str) -> int:
    conn, _ = _open_db("produtos")
    try:
        row = _fetch_produto_row(conn, produto)
        if not row:
            raise HTTPException(status_code=404, detail=f"Produto não encontrado: {produto}")

        packs = _norm_int(row.get("PACKS"), 0)
        if not packs or packs <= 0:
            raise HTTPException(status_code=400, detail=f"PACKS inválido para o produto {produto}")

        return packs
    finally:
        conn.close()


def _prepare_estoque_payload(data: Dict[str, Any], item_id: Optional[int] = None) -> Dict[str, Any]:
    payload = dict(data)

    payload.pop("SITUACAO3", None)
    payload.pop("situacao3", None)

    if not _norm_text(payload.get("CHECKLIST_MASTER")):
        payload["CHECKLIST_MASTER"] = "[00000]"

    if "PRODUTO" in payload:
        payload["PRODUTO"] = _norm_text(payload["PRODUTO"])
    if "lote" in payload:
        payload["lote"] = _norm_text(payload["lote"])
    if "linha" in payload:
        payload["linha"] = _norm_text(payload["linha"])
    if "AZ" in payload:
        payload["AZ"] = _norm_int(payload["AZ"])
    if "RUA" in payload:
        payload["RUA"] = _norm_int(payload["RUA"])
    if "QUANTIDADE_PLT" in payload:
        payload["QUANTIDADE_PLT"] = _norm_int(payload["QUANTIDADE_PLT"], 0)

    payload.pop("QUANTIDADE_PACK", None)

    conn, _ = _open_db("estoque")
    try:
        base_row = None
        if item_id is not None:
            with conn.cursor() as cursor:
                cursor.execute("SELECT * FROM estoque WHERE ID = %s LIMIT 1", (item_id,))
                base_row = cursor.fetchone()

            if not base_row:
                raise HTTPException(status_code=404, detail="Registro não encontrado")

        merged: Dict[str, Any] = {}
        if base_row:
            merged.update(base_row)
        merged.update(payload)

        produto = _norm_text(merged.get("PRODUTO"))
        if not produto:
            raise HTTPException(status_code=400, detail="Campo PRODUTO é obrigatório")

        qtd_plt = _norm_int(merged.get("QUANTIDADE_PLT"), None)
        if qtd_plt is None:
            raise HTTPException(status_code=400, detail="Campo QUANTIDADE_PLT é obrigatório")

        packs = _get_produto_packs(produto)
        merged["QUANTIDADE_PACK"] = int(qtd_plt) * int(packs)

        if not _norm_text(merged.get("CHECKLIST_MASTER")):
            merged["CHECKLIST_MASTER"] = "[00000]"

        merged.pop("SITUACAO3", None)
        merged.pop("situacao3", None)
        merged.pop("DATA_SHELF", None)
        merged.pop("DATA_BLOQ", None)
        merged.pop("DATA_VALIDADE", None)
        
        return merged
    finally:
        conn.close()


@app.get("/")
def root():
    return {
        "status": "ok",
        "message": "API Base Ambev ativa",
        "tables": list(TABLES.keys()),
    }


@app.get("/health")
def health():
    return {"status": "ok"}


# =========================================================
# PRODUTOS
# =========================================================
@app.get("/produtos")
def listar_produtos(request: Request):
    return _select_all("produtos", request)


@app.get("/produtos/{item_id}")
def obter_produto(item_id: int):
    return _select_by_id("produtos", item_id)


@app.get("/produtos/codigo/{produto}")
def obter_produto_por_codigo(produto: str):
    conn, _ = _open_db("produtos")
    try:
        row = _fetch_produto_row(conn, produto)
        if not row:
            raise HTTPException(status_code=404, detail="Produto não encontrado")
        return row
    finally:
        conn.close()


@app.get("/produtos/pack/{produto}")
def obter_pack_produto(produto: str):
    conn, _ = _open_db("produtos")
    try:
        row = _fetch_produto_row(conn, produto)
        if not row:
            raise HTTPException(status_code=404, detail="Produto não encontrado")

        packs = _norm_int(row.get("PACKS"), 0)
        return {"PRODUTO": row.get("PRODUTO"), "PACKS": packs}
    finally:
        conn.close()


@app.get("/produtos/pack/{produto}/calcular")
def calcular_pack_produto(produto: str, qtd_plt: int):
    conn, _ = _open_db("produtos")
    try:
        row = _fetch_produto_row(conn, produto)
        if not row:
            raise HTTPException(status_code=404, detail="Produto não encontrado")

        packs = _norm_int(row.get("PACKS"), 0)
        if not packs or packs <= 0:
            raise HTTPException(status_code=400, detail="PACKS inválido para o produto")

        return {
            "PRODUTO": row.get("PRODUTO"),
            "PACKS": packs,
            "QUANTIDADE_PLT": qtd_plt,
            "QUANTIDADE_PACK": int(qtd_plt) * int(packs),
        }
    finally:
        conn.close()


@app.post("/produtos")
def inserir_produto(data: Dict[str, Any]):
    return _insert_row("produtos", data)


@app.put("/produtos/{item_id}")
def atualizar_produto(item_id: int, data: Dict[str, Any]):
    return _update_row("produtos", item_id, data)


@app.delete("/produtos/{item_id}")
def deletar_produto(item_id: int):
    return _delete_row("produtos", item_id)


# =========================================================
# ESTOQUE
# =========================================================
@app.get("/estoque")
def listar_estoque(request: Request):
    return _select_all("estoque", request)


@app.get("/estoque/{item_id}")
def obter_estoque(item_id: int):
    return _select_by_id("estoque", item_id)


@app.post("/estoque")
def inserir_estoque(data: Dict[str, Any]):
    payload = _prepare_estoque_payload(data)
    return _insert_row("estoque", payload)


@app.put("/estoque/{item_id}")
def atualizar_estoque(item_id: int, data: Dict[str, Any]):
    payload = _prepare_estoque_payload(data, item_id=item_id)
    return _update_row("estoque", item_id, payload)


@app.delete("/estoque/{item_id}")
def deletar_estoque(item_id: int):
    return _delete_row("estoque", item_id)


@app.get("/estoque/sugestoes")
def sugestoes_estoque():
    conn, _ = _open_db("estoque")
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT DISTINCT PRODUTO
                FROM estoque
                WHERE PRODUTO IS NOT NULL AND PRODUTO <> ''
                ORDER BY PRODUTO
                """
            )
            produtos = [r["PRODUTO"] for r in cursor.fetchall()]

            cursor.execute(
                """
                SELECT DISTINCT lote
                FROM estoque
                WHERE lote IS NOT NULL AND lote <> ''
                ORDER BY lote
                """
            )
            lotes = [r["lote"] for r in cursor.fetchall()]

            cursor.execute(
                """
                SELECT DISTINCT linha
                FROM estoque
                WHERE linha IS NOT NULL AND linha <> ''
                ORDER BY linha
                """
            )
            linhas = [r["linha"] for r in cursor.fetchall()]

            cursor.execute(
                """
                SELECT DISTINCT QD
                FROM estoque
                WHERE QD IS NOT NULL AND QD <> ''
                ORDER BY QD
                """
            )
            qds = [r["QD"] for r in cursor.fetchall()]

            cursor.execute(
                """
                SELECT DISTINCT AREA
                FROM estoque
                WHERE AREA IS NOT NULL AND AREA <> ''
                ORDER BY AREA
                """
            )
            areas = [r["AREA"] for r in cursor.fetchall()]

        return {
            "produtos": produtos,
            "lotes": lotes,
            "linhas": linhas,
            "qds": qds,
            "areas": areas,
        }
    finally:
        conn.close()


# =========================================================
# HISTORICO_ALTERACOES
# =========================================================
@app.get("/historico")
def listar_historico(request: Request):
    return _select_all("historico_alteracoes", request)


@app.get("/historico/{item_id}")
def obter_historico(item_id: int):
    return _select_by_id("historico_alteracoes", item_id)


@app.post("/historico")
def inserir_historico(data: Dict[str, Any]):
    return _insert_row("historico_alteracoes", data)


@app.put("/historico/{item_id}")
def atualizar_historico(item_id: int, data: Dict[str, Any]):
    return _update_row("historico_alteracoes", item_id, data)


@app.delete("/historico/{item_id}")
def deletar_historico(item_id: int):
    return _delete_row("historico_alteracoes", item_id)


@app.get("/historico/por-relacao/{id_relacao}")
def historico_por_relacao(id_relacao: str):
    conn, _ = _open_db("historico_alteracoes")
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT *
                FROM historico_alteracoes
                WHERE id_relacao = %s
                ORDER BY ID DESC
                """,
                (id_relacao,),
            )
            return cursor.fetchall()
    finally:
        conn.close()


# =========================================================
# LOG
# =========================================================
@app.get("/log")
def listar_log(request: Request):
    return _select_all("log", request)


@app.get("/log/{item_id}")
def obter_log(item_id: int):
    return _select_by_id("log", item_id)


@app.post("/log")
def inserir_log(data: Dict[str, Any]):
    return _insert_row("log", data)


@app.put("/log/{item_id}")
def atualizar_log(item_id: int, data: Dict[str, Any]):
    return _update_row("log", item_id, data)


@app.delete("/log/{item_id}")
def deletar_log(item_id: int):
    return _delete_row("log", item_id)


# =========================================================
# LOGIN
# =========================================================
@app.post("/auth/login")
def login(data: Dict[str, Any]):
    login_user = _norm_text(data.get("login"))
    senha = _norm_text(data.get("senha"))

    if not login_user or not senha:
        raise HTTPException(status_code=400, detail="login e senha são obrigatórios")

    conn, _ = _open_db("log")
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT id, nome, login
                FROM log
                WHERE login = %s AND senha = %s
                LIMIT 1
                """,
                (login_user, senha),
            )
            user = cursor.fetchone()

        if not user:
            raise HTTPException(status_code=401, detail="Usuário ou senha inválidos")

        return {"status": "ok", "usuario": user}
    finally:
        conn.close()
