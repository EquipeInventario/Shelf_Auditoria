import os
import re
from datetime import datetime, date
from typing import Any, Dict, List, Optional

import pymysql
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

load_dotenv()

app = FastAPI(title="API Base Ambev", version="1.0.0")

# =========================================================
# CORS
# =========================================================
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# =========================================================
# VARIÁVEIS DE AMBIENTE
# =========================================================
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = int(os.getenv("DB_PORT", "3306"))

DB_NAME_BASE = os.getenv("DB_NAME_BASE", "base_ambev")
DB_NAME_INVENTARIO = os.getenv("DB_NAME_INVENTARIO", "inventario")

# =========================================================
# CAMPOS
# =========================================================
STOCK_INT_FIELDS = {
    "AZ",
    "RUA",
    "QUANTIDADE_PACK",
    "QUANTIDADE_PLT",
    "SHELF_DIAS",
    "VALIDADE_DIAS",
}

STOCK_DATE_FIELDS = {
    "DATA_FABRICACAO",
    "DATA_SHELF",
    "DATA_BLOQ",
    "DATA_VALIDADE",
    "DATA_HOJE",
    "DATA_L_COMERC",
    "DATA_DISP",
}

STOCK_UPPER_FIELDS = {
    "AREA",
    "SITUACAO",
    "SITUACAO2",
    "SITUACAO3",
    "PROVISIONADO",
    "DESCARTE_EFETIVO",
    "MOTIVO_DESCARTE",
    "DISPOSICAO",
}

REQUIRED_CREATE_FIELDS = {
    "PRODUTO",
    "lote",
    "linha",
    "AZ",
    "QD",
    "RUA",
    "AREA",
    "QUANTIDADE_PLT",
}

# =========================================================
# MODELOS
# =========================================================
class LoginData(BaseModel):
    login: str
    senha: str


class EstoquePayload(BaseModel):
    CHECKLIST_MASTER: Optional[str] = None
    PRODUTO: Optional[str] = None
    DESCRICAO: Optional[str] = None
    ABREVIACAO: Optional[str] = None
    fornecedor: Optional[str] = None
    documento_extra: Optional[str] = None
    lote: Optional[str] = None
    linha: Optional[str] = None
    AZ: Optional[str] = None
    QD: Optional[str] = None
    RUA: Optional[str] = None
    AREA: Optional[str] = None
    QUANTIDADE_PACK: Optional[str] = None
    QUANTIDADE_PLT: Optional[str] = None
    TIPO_PALLET: Optional[str] = None
    DATA_RECEBIMENTO: Optional[str] = None
    DATA_FABRICACAO: Optional[str] = None
    PROCEDENCIA: Optional[str] = None
    SHELF_DIAS: Optional[str] = None
    VALIDADE_DIAS: Optional[str] = None
    DATA_SHELF: Optional[str] = None
    DATA_BLOQ: Optional[str] = None
    DATA_VALIDADE: Optional[str] = None
    DATA_HOJE: Optional[str] = None
    NF: Optional[str] = None
    SITUACAO: Optional[str] = None
    SITUACAO2: Optional[str] = None
    SITUACAO3: Optional[str] = None
    observacao: Optional[str] = None
    PROVISIONADO: Optional[str] = None
    DESCARTE_EFETIVO: Optional[str] = None
    MOTIVO: Optional[str] = None
    RNC: Optional[str] = None
    DISPOSICAO: Optional[str] = None
    DATA_L_COMERC: Optional[str] = None
    DATA_DISP: Optional[str] = None
    MOTIVO_DESCARTE: Optional[str] = None
    CONFERENTE: Optional[str] = None
    OPERADOR: Optional[str] = None
    USUARIO_SISTEMA: Optional[str] = None
    usuario_app: Optional[str] = None


class LoteUpdateRequest(BaseModel):
    ids: List[int]
    dados: EstoquePayload


class LoteDeleteRequest(BaseModel):
    ids: List[int]
    usuario_app: Optional[str] = None


# =========================================================
# ROOT
# =========================================================
@app.get("/")
def root():
    return {"message": "API Base Ambev funcionando 🚀"}


@app.get("/health")
def health():
    return {"status": "ok"}


# =========================================================
# CONEXÃO
# =========================================================
def get_connection(database_name: str):
    if not DB_HOST or not DB_USER or not DB_PASSWORD or not database_name:
        return None

    try:
        return pymysql.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=database_name,
            port=DB_PORT,
            connect_timeout=10,
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=False,
            charset="utf8mb4",
        )
    except Exception as e:
        print("Erro conexão:", e)
        return None


# =========================================================
# HELPERS
# =========================================================
def model_to_dict(model: BaseModel) -> Dict[str, Any]:
    if hasattr(model, "model_dump"):
        return model.model_dump(exclude_unset=True)
    return model.dict(exclude_unset=True)


def clean_text(value: Any, upper: bool = False) -> Optional[str]:
    if value is None:
        return None

    if isinstance(value, (datetime, date)):
        value = value.isoformat()

    text = str(value).strip()
    if not text:
        return None

    return text.upper() if upper else text


def to_int(value: Any) -> Optional[int]:
    if value is None:
        return None

    if isinstance(value, int):
        return value

    if isinstance(value, float):
        return int(value)

    text = str(value).strip()
    if not text:
        return None

    text = text.replace(".", "").replace(",", ".")
    try:
        return int(float(text))
    except Exception:
        return None


def parse_date_any(value: Any) -> Optional[date]:
    if value is None:
        return None

    if isinstance(value, datetime):
        return value.date()

    if isinstance(value, date):
        return value

    text = str(value).strip()
    if not text:
        return None

    digits = re.sub(r"[^0-9]", "", text)

    if len(digits) == 8 and "/" in text:
        day = int(digits[0:2])
        month = int(digits[2:4])
        year = int(digits[4:8])
        try:
            return date(year, month, day)
        except Exception:
            return None

    try:
        return datetime.fromisoformat(text.replace("T", " ")).date()
    except Exception:
        pass

    try:
        return datetime.strptime(text, "%Y-%m-%d").date()
    except Exception:
        return None


def to_iso_date(value: Any) -> Optional[str]:
    d = parse_date_any(value)
    return d.isoformat() if d else None


def build_usuario_app(nome_usuario: str, dt: Optional[datetime] = None) -> str:
    dt = dt or datetime.now()
    nome = clean_text(nome_usuario) or "Usuário"
    return f"{nome} - {dt.strftime('%d/%m/%Y %H:%M:%S')}"


def normalize_stock_payload(raw: Dict[str, Any]) -> Dict[str, Any]:
    cleaned: Dict[str, Any] = {}

    for key, value in raw.items():
        if key in STOCK_INT_FIELDS:
            cleaned[key] = to_int(value)
        elif key in STOCK_DATE_FIELDS:
            cleaned[key] = to_iso_date(value)
        else:
            cleaned[key] = clean_text(value, upper=(key in STOCK_UPPER_FIELDS))

    return cleaned


def validate_create_payload(data: Dict[str, Any]):
    for field in REQUIRED_CREATE_FIELDS:
        if data.get(field) in (None, "", []):
            raise HTTPException(
                status_code=400,
                detail=f"Campo obrigatório ausente ou vazio: {field}",
            )

    if data.get("AZ") not in (1, 2):
        raise HTTPException(status_code=400, detail="AZ deve ser 1 ou 2")

    if data.get("AREA") and len(str(data["AREA"])) != 1:
        raise HTTPException(status_code=400, detail="AREA deve ter 1 caractere")


def fetch_estoque_by_id(conn, item_id: int):
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                ID, CHECKLIST_MASTER, PRODUTO, DESCRICAO, ABREVIACAO, fornecedor,
                documento_extra, lote, linha, AZ, QD, RUA, AREA, QUANTIDADE_PACK,
                QUANTIDADE_PLT, TIPO_PALLET, DATA_RECEBIMENTO, DATA_FABRICACAO,
                PROCEDENCIA, SHELF_DIAS, VALIDADE_DIAS, DATA_SHELF, DATA_BLOQ,
                DATA_VALIDADE, DATA_HOJE, NF, SITUACAO, SITUACAO2, SITUACAO3,
                observacao, PROVISIONADO, DESCARTE_EFETIVO, MOTIVO, RNC, DISPOSICAO,
                DATA_L_COMERC, DATA_DISP, MOTIVO_DESCARTE, CONFERENTE, OPERADOR,
                USUARIO_SISTEMA, usuario_app
            FROM estoque
            WHERE ID = %s
            LIMIT 1
            """,
            (item_id,),
        )
        return cursor.fetchone()


def build_history_row(before: Dict[str, Any], after: Optional[Dict[str, Any]], usuario_sistema: str, id_relacao: Optional[str]):
    after = after or {}

    def b(key):
        return before.get(key)

    def a(key):
        return after.get(key)

    return {
        "USUARIO_SISTEMA": usuario_sistema,
        "DATA_HORA": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),

        "PRODUTO_ANTES": b("PRODUTO"),
        "PRODUTO_DEPOIS": a("PRODUTO"),

        "LOTE_ANTES": b("lote"),
        "LOTE_DEPOIS": a("lote"),

        "LINHA_ANTES": b("linha"),
        "LINHA_DEPOIS": a("linha"),

        "FORNECEDOR_ANTES": b("fornecedor"),
        "FORNECEDOR_DEPOIS": a("fornecedor"),

        "documento_extra_antes": b("documento_extra"),
        "documento_extra_depois": a("documento_extra"),

        "AZ_ANTES": to_int(b("AZ")),
        "AZ_DEPOIS": clean_text(a("AZ")),

        "QD_ANTES": b("QD"),
        "QD_DEPOIS": clean_text(a("QD")),

        "RUA_ANTES": to_int(b("RUA")),
        "RUA_DEPOIS": clean_text(a("RUA")),

        "AREA_ANTES": b("AREA"),
        "AREA_DEPOIS": clean_text(a("AREA")),

        "QUANTIDADE_PACK_ANTES": to_int(b("QUANTIDADE_PACK")),
        "QUANTIDADE_PACK_DEPOIS": clean_text(a("QUANTIDADE_PACK")),

        "QUANTIDADE_PLT_ANTES": to_int(b("QUANTIDADE_PLT")),
        "QUANTIDADE_PLT_DEPOIS": clean_text(a("QUANTIDADE_PLT")),

        "TIPO_PALLET_ANTES": b("TIPO_PALLET"),
        "TIPO_PALLET_DEPOIS": a("TIPO_PALLET"),

        "PROCEDENCIA_ANTES": b("PROCEDENCIA"),
        "PROCEDENCIA_DEPOIS": a("PROCEDENCIA"),

        "DATA_FABRICACAO_ANTES": parse_date_any(b("DATA_FABRICACAO")),
        "DATA_FABRICACAO_DEPOIS": a("DATA_FABRICACAO"),

        "SITUACAO_ANTES": b("SITUACAO"),
        "SITUACAO_DEPOIS": a("SITUACAO"),

        "OBSERVACAO_ANTES": b("observacao"),
        "OBSERVACAO_DEPOIS": a("observacao"),

        "PROVISIONADO_ANTES": b("PROVISIONADO"),
        "PROVISIONADO_DEPOIS": a("PROVISIONADO"),

        "DESCARTE_EFETIVO_ANTES": b("DESCARTE_EFETIVO"),
        "DESCARTE_EFETIVO_DEPOIS": a("DESCARTE_EFETIVO"),

        "MOTIVO_ANTES": b("MOTIVO"),
        "MOTIVO_DEPOIS": a("MOTIVO"),

        "RNC_ANTES": b("RNC"),
        "RNC_DEPOIS": a("RNC"),

        "DISPOSICAO_ANTES": b("DISPOSICAO"),
        "DISPOSICAO_DEPOIS": a("DISPOSICAO"),

        "DATA_L_COMERC_ANTES": parse_date_any(b("DATA_L_COMERC")),
        "DATA_L_COMERC_DEPOIS": a("DATA_L_COMERC"),

        "DATA_DISP_ANTES": parse_date_any(b("DATA_DISP")),
        "DATA_DISP_DEPOIS": a("DATA_DISP"),

        "MOTIVO_DESCARTE_ANTES": b("MOTIVO_DESCARTE"),
        "MOTIVO_DESCARTE_DEPOIS": a("MOTIVO_DESCARTE"),

        "id_relacao": id_relacao,
    }


def insert_history(conn, before: Dict[str, Any], after: Optional[Dict[str, Any]], usuario_sistema: str, id_relacao: Optional[str]):
    hist = build_history_row(before, after, usuario_sistema, id_relacao)

    cols = list(hist.keys())
    fields_sql = ", ".join(f"`{c}`" for c in cols)
    placeholders = ", ".join(["%s"] * len(cols))
    values = [hist[c] for c in cols]

    sql = f"INSERT INTO historico_alteracoes ({fields_sql}) VALUES ({placeholders})"

    with conn.cursor() as cursor:
        cursor.execute(sql, values)


def query_stock_rows(
    conn,
    qd: Optional[str] = None,
    rua: Optional[str] = None,
    produto: Optional[str] = None,
    linha: Optional[str] = None,
    area: Optional[str] = None,
    lote: Optional[str] = None,
    documento_extra: Optional[str] = None,
    situacao3: Optional[str] = None,
):
    sql = """
        SELECT
            ID, CHECKLIST_MASTER, PRODUTO, DESCRICAO, ABREVIACAO, fornecedor,
            documento_extra, lote, linha, AZ, QD, RUA, AREA, QUANTIDADE_PACK,
            QUANTIDADE_PLT, TIPO_PALLET, DATA_RECEBIMENTO, DATA_FABRICACAO,
            PROCEDENCIA, SHELF_DIAS, VALIDADE_DIAS, DATA_SHELF, DATA_BLOQ,
            DATA_VALIDADE, DATA_HOJE, NF, SITUACAO, SITUACAO2, SITUACAO3,
            observacao, PROVISIONADO, DESCARTE_EFETIVO, MOTIVO, RNC, DISPOSICAO,
            DATA_L_COMERC, DATA_DISP, MOTIVO_DESCARTE, CONFERENTE, OPERADOR,
            USUARIO_SISTEMA, usuario_app
        FROM estoque
        WHERE 1=1
    """
    params = []

    if qd:
        sql += " AND QD LIKE %s"
        params.append(f"%{qd.strip()}%")

    if rua:
        sql += " AND CAST(RUA AS CHAR) LIKE %s"
        params.append(f"%{rua.strip()}%")

    if produto:
        sql += " AND PRODUTO LIKE %s"
        params.append(f"%{produto.strip()}%")

    if linha:
        sql += " AND linha LIKE %s"
        params.append(f"%{linha.strip()}%")

    if area:
        sql += " AND AREA LIKE %s"
        params.append(f"%{area.strip()}%")

    if lote:
        sql += " AND lote LIKE %s"
        params.append(f"%{lote.strip()}%")

    if documento_extra:
        sql += " AND documento_extra LIKE %s"
        params.append(f"%{documento_extra.strip()}%")

    if situacao3:
        sql += " AND SITUACAO3 LIKE %s"
        params.append(f"%{situacao3.strip()}%")

    sql += " ORDER BY QD, RUA, ID"

    with conn.cursor() as cursor:
        cursor.execute(sql, params)
        return cursor.fetchall()


def group_rows(rows: List[Dict[str, Any]]):
    groups: Dict[str, Dict[str, Any]] = {}

    for row in rows:
        qd = clean_text(row.get("QD")) or ""
        rua = clean_text(row.get("RUA")) or ""

        if not qd or not rua:
            continue

        key = f"{qd}_{rua}"
        if key not in groups:
            groups[key] = {
                "qd": qd,
                "rua": rua,
                "itens": [],
            }

        groups[key]["itens"].append(row)

    def distinct_non_empty(values):
        return sorted({clean_text(v) for v in values if clean_text(v)})

    def distinct_zi(values):
        temp = set()
        for v in values:
            t = clean_text(v)
            temp.add(t if t else "(SEM ZI)")
        return sorted(temp)

    result = []
    for g in groups.values():
        itens = g["itens"]

        produtos = distinct_non_empty([i.get("PRODUTO") for i in itens])
        linhas = distinct_non_empty([i.get("linha") for i in itens])
        status = distinct_non_empty([i.get("SITUACAO3") for i in itens])
        zis = distinct_zi([i.get("documento_extra") for i in itens])

        motivos = []
        if len(produtos) > 1:
            motivos.append("produto diferente na mesma posição")
        if len(linhas) > 1:
            motivos.append("linha diferente na mesma posição")
        if len(status) > 1:
            motivos.append("status diferente na mesma posição")
        if len(zis) > 1:
            motivos.append("ZI diferente na mesma posição")

        total_paletes = 0
        for item in itens:
            total_paletes += to_int(item.get("QUANTIDADE_PLT")) or 0

        result.append({
            "qd": g["qd"],
            "rua": g["rua"],
            "itens": itens,
            "ids": [i["ID"] for i in itens if i.get("ID") is not None],
            "total_itens": len(itens),
            "total_paletes": total_paletes,
            "produtos_distintos": produtos,
            "linhas_distintas": linhas,
            "status_distintos": status,
            "zis_distintos": zis,
            "motivos": motivos,
            "tem_divergencia": len(motivos) > 0,
        })

    def sort_key(item):
        def pos_key(v):
            t = str(v).strip()
            try:
                return (0, int(t))
            except Exception:
                return (1, t.upper())

        return (pos_key(item["qd"]), pos_key(item["rua"]))

    result.sort(key=sort_key)
    return result


# =========================================================
# LOGIN
# =========================================================
@app.post("/auth/login")
def login(data: LoginData):
    conn = get_connection(DB_NAME_INVENTARIO)
    if not conn:
        raise HTTPException(status_code=500, detail="Erro na conexão com o banco inventario")

    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT id, nome, login
                FROM log
                WHERE login = %s AND senha = %s
                LIMIT 1
                """,
                (data.login, data.senha),
            )
            user = cursor.fetchone()

        if not user:
            raise HTTPException(status_code=401, detail="Usuário ou senha inválidos")

        return {
            "status": "ok",
            "usuario": user,
        }

    finally:
        conn.close()


# =========================================================
# ESTOQUE - LISTA FLAT
# =========================================================
@app.get("/estoque")
def listar_estoque(
    qd: Optional[str] = None,
    rua: Optional[str] = None,
    produto: Optional[str] = None,
    linha: Optional[str] = None,
    area: Optional[str] = None,
    lote: Optional[str] = None,
    documento_extra: Optional[str] = None,
    situacao3: Optional[str] = None,
):
    conn = get_connection(DB_NAME_BASE)
    if not conn:
        raise HTTPException(status_code=500, detail="Erro na conexão com o banco base_ambev")

    try:
        return query_stock_rows(
            conn,
            qd=qd,
            rua=rua,
            produto=produto,
            linha=linha,
            area=area,
            lote=lote,
            documento_extra=documento_extra,
            situacao3=situacao3,
        )
    finally:
        conn.close()


@app.get("/estoque/{item_id}")
def obter_estoque(item_id: int):
    conn = get_connection(DB_NAME_BASE)
    if not conn:
        raise HTTPException(status_code=500, detail="Erro na conexão com o banco base_ambev")

    try:
        row = fetch_estoque_by_id(conn, item_id)
        if not row:
            raise HTTPException(status_code=404, detail="Item não encontrado")
        return row
    finally:
        conn.close()


# =========================================================
# ESTOQUE - SUGESTÕES
# =========================================================
@app.get("/estoque/sugestoes")
def sugestoes_estoque():
    conn = get_connection(DB_NAME_BASE)
    if not conn:
        raise HTTPException(status_code=500, detail="Erro na conexão com o banco base_ambev")

    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT DISTINCT PRODUTO FROM estoque
                WHERE PRODUTO IS NOT NULL AND PRODUTO <> ''
                ORDER BY PRODUTO
                """
            )
            produtos = [r["PRODUTO"] for r in cursor.fetchall()]

            cursor.execute(
                """
                SELECT DISTINCT linha FROM estoque
                WHERE linha IS NOT NULL AND linha <> ''
                ORDER BY linha
                """
            )
            linhas = [r["linha"] for r in cursor.fetchall()]

            cursor.execute(
                """
                SELECT DISTINCT AREA FROM estoque
                WHERE AREA IS NOT NULL AND AREA <> ''
                ORDER BY AREA
                """
            )
            areas = [r["AREA"] for r in cursor.fetchall()]

            cursor.execute(
                """
                SELECT DISTINCT lote FROM estoque
                WHERE lote IS NOT NULL AND lote <> ''
                ORDER BY lote
                """
            )
            lotes = [r["lote"] for r in cursor.fetchall()]

            cursor.execute(
                """
                SELECT DISTINCT documento_extra FROM estoque
                WHERE documento_extra IS NOT NULL AND documento_extra <> ''
                ORDER BY documento_extra
                """
            )
            documentos = [r["documento_extra"] for r in cursor.fetchall()]

            cursor.execute(
                """
                SELECT DISTINCT QD FROM estoque
                WHERE QD IS NOT NULL AND QD <> ''
                ORDER BY QD
                """
            )
            qds = [r["QD"] for r in cursor.fetchall()]

        return {
            "produtos": produtos,
            "linhas": linhas,
            "areas": areas,
            "lotes": lotes,
            "documentos_extra": documentos,
            "qds": qds,
        }
    finally:
        conn.close()


# =========================================================
# ESTOQUE - CRUD
# =========================================================
@app.post("/estoque")
def criar_estoque(payload: EstoquePayload):
    conn = get_connection(DB_NAME_BASE)
    if not conn:
        raise HTTPException(status_code=500, detail="Erro na conexão com o banco base_ambev")

    try:
        raw = model_to_dict(payload)
        data = normalize_stock_payload(raw)

        validate_create_payload(data)

        agora = datetime.now()
        if not data.get("DATA_HOJE"):
            data["DATA_HOJE"] = agora.date().isoformat()

        actor = data.get("usuario_app") or "API"
        data["usuario_app"] = actor
        data["USUARIO_SISTEMA"] = data.get("USUARIO_SISTEMA") or actor

        with conn.cursor() as cursor:
            cols = list(data.keys())
            fields_sql = ", ".join(f"`{c}`" for c in cols)
            placeholders = ", ".join(["%s"] * len(cols))
            values = [data[c] for c in cols]

            cursor.execute(
                f"INSERT INTO estoque ({fields_sql}) VALUES ({placeholders})",
                values,
            )
            new_id = cursor.lastrowid

        insert_history(
            conn,
            before={},
            after=data,
            usuario_sistema=f"{actor} - USER APP",
            id_relacao=str(new_id),
        )

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


@app.put("/estoque/{item_id}")
def atualizar_estoque(item_id: int, payload: EstoquePayload):
    conn = get_connection(DB_NAME_BASE)
    if not conn:
        raise HTTPException(status_code=500, detail="Erro na conexão com o banco base_ambev")

    try:
        before = fetch_estoque_by_id(conn, item_id)
        if not before:
            raise HTTPException(status_code=404, detail="Item não encontrado")

        raw = model_to_dict(payload)
        data = normalize_stock_payload(raw)

        if not data:
            raise HTTPException(status_code=400, detail="Nenhum campo enviado para atualização")

        actor = data.get("usuario_app") or before.get("usuario_app") or "API"
        data["usuario_app"] = actor
        data["USUARIO_SISTEMA"] = data.get("USUARIO_SISTEMA") or actor

        merged = dict(before)
        merged.update(data)

        with conn.cursor() as cursor:
            cols = list(data.keys())
            set_sql = ", ".join(f"`{c}` = %s" for c in cols)
            values = [data[c] for c in cols]
            values.append(item_id)

            cursor.execute(
                f"UPDATE estoque SET {set_sql} WHERE ID = %s",
                values,
            )

        insert_history(
            conn,
            before=before,
            after=merged,
            usuario_sistema=f"{actor} - USER APP",
            id_relacao=str(item_id),
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


@app.delete("/estoque/{item_id}")
def deletar_estoque(item_id: int, usuario_app: Optional[str] = Query(default=None)):
    conn = get_connection(DB_NAME_BASE)
    if not conn:
        raise HTTPException(status_code=500, detail="Erro na conexão com o banco base_ambev")

    try:
        before = fetch_estoque_by_id(conn, item_id)
        if not before:
            raise HTTPException(status_code=404, detail="Item não encontrado")

        actor = clean_text(usuario_app) or before.get("usuario_app") or "API"

        with conn.cursor() as cursor:
            cursor.execute("DELETE FROM estoque WHERE ID = %s", (item_id,))

        insert_history(
            conn,
            before=before,
            after=None,
            usuario_sistema=f"{actor} - USER APP",
            id_relacao=str(item_id),
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


# =========================================================
# ESTOQUE - LOTE
# =========================================================
@app.put("/estoque/lote")
def atualizar_lote(payload: LoteUpdateRequest):
    conn = get_connection(DB_NAME_BASE)
    if not conn:
        raise HTTPException(status_code=500, detail="Erro na conexão com o banco base_ambev")

    try:
        raw = model_to_dict(payload.dados)
        data = normalize_stock_payload(raw)

        if not payload.ids:
            raise HTTPException(status_code=400, detail="Lista de IDs vazia")

        if not data:
            raise HTTPException(status_code=400, detail="Nenhum campo enviado para atualização")

        actor = data.get("usuario_app") or "API"
        data["usuario_app"] = actor
        data["USUARIO_SISTEMA"] = data.get("USUARIO_SISTEMA") or actor

        with conn.cursor() as cursor:
            for item_id in payload.ids:
                before = fetch_estoque_by_id(conn, item_id)
                if not before:
                    continue

                merged = dict(before)
                merged.update(data)

                cols = list(data.keys())
                set_sql = ", ".join(f"`{c}` = %s" for c in cols)
                values = [data[c] for c in cols]
                values.append(item_id)

                cursor.execute(
                    f"UPDATE estoque SET {set_sql} WHERE ID = %s",
                    values,
                )

                insert_history(
                    conn,
                    before=before,
                    after=merged,
                    usuario_sistema=f"{actor} - USER APP",
                    id_relacao=str(item_id),
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


@app.delete("/estoque/lote")
def deletar_lote(payload: LoteDeleteRequest):
    conn = get_connection(DB_NAME_BASE)
    if not conn:
        raise HTTPException(status_code=500, detail="Erro na conexão com o banco base_ambev")

    try:
        if not payload.ids:
            raise HTTPException(status_code=400, detail="Lista de IDs vazia")

        actor = clean_text(payload.usuario_app) or "API"

        with conn.cursor() as cursor:
            for item_id in payload.ids:
                before = fetch_estoque_by_id(conn, item_id)
                if not before:
                    continue

                cursor.execute("DELETE FROM estoque WHERE ID = %s", (item_id,))

                insert_history(
                    conn,
                    before=before,
                    after=None,
                    usuario_sistema=f"{actor} - USER APP",
                    id_relacao=str(item_id),
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


# =========================================================
# ESTOQUE - GRUPOS / DIVERGÊNCIAS
# =========================================================
@app.get("/estoque/grupos")
def listar_grupos(
    qd: Optional[str] = None,
    rua: Optional[str] = None,
    produto: Optional[str] = None,
    linha: Optional[str] = None,
    area: Optional[str] = None,
    lote: Optional[str] = None,
    documento_extra: Optional[str] = None,
    situacao3: Optional[str] = None,
    somente_divergentes: bool = False,
):
    conn = get_connection(DB_NAME_BASE)
    if not conn:
        raise HTTPException(status_code=500, detail="Erro na conexão com o banco base_ambev")

    try:
        rows = query_stock_rows(
            conn,
            qd=qd,
            rua=rua,
            produto=produto,
            linha=linha,
            area=area,
            lote=lote,
            documento_extra=documento_extra,
            situacao3=situacao3,
        )
        groups = group_rows(rows)

        if somente_divergentes:
            groups = [g for g in groups if g["tem_divergencia"]]

        return groups
    finally:
        conn.close()


@app.get("/estoque/divergencias")
def listar_divergencias():
    conn = get_connection(DB_NAME_BASE)
    if not conn:
        raise HTTPException(status_code=500, detail="Erro na conexão com o banco base_ambev")

    try:
        rows = query_stock_rows(conn)
        groups = group_rows(rows)
        divergentes = [g for g in groups if g["tem_divergencia"]]

        total_itens = sum(g["total_itens"] for g in divergentes)
        total_paletes = sum(g["total_paletes"] for g in divergentes)

        return {
            "resumo": {
                "total_posicoes": len(divergentes),
                "total_itens": total_itens,
                "total_paletes": total_paletes,
            },
            "grupos": divergentes,
        }
    finally:
        conn.close()


# =========================================================
# HISTÓRICO
# =========================================================
@app.get("/historico")
def listar_historico(
    usuario: Optional[str] = None,
    qd: Optional[str] = None,
    rua: Optional[str] = None,
    produto: Optional[str] = None,
    id_relacao: Optional[str] = None,
    data: Optional[str] = None,
):
    conn = get_connection(DB_NAME_BASE)
    if not conn:
        raise HTTPException(status_code=500, detail="Erro na conexão com o banco base_ambev")

    try:
        sql = """
            SELECT
                ID,
                USUARIO_SISTEMA,
                DATA_HORA,
                PRODUTO_ANTES,
                PRODUTO_DEPOIS,
                LOTE_ANTES,
                LOTE_DEPOIS,
                LINHA_ANTES,
                LINHA_DEPOIS,
                FORNECEDOR_ANTES,
                FORNECEDOR_DEPOIS,
                documento_extra_antes,
                documento_extra_depois,
                AZ_ANTES,
                AZ_DEPOIS,
                QD_ANTES,
                QD_DEPOIS,
                RUA_ANTES,
                RUA_DEPOIS,
                AREA_ANTES,
                AREA_DEPOIS,
                QUANTIDADE_PACK_ANTES,
                QUANTIDADE_PACK_DEPOIS,
                QUANTIDADE_PLT_ANTES,
                QUANTIDADE_PLT_DEPOIS,
                TIPO_PALLET_ANTES,
                TIPO_PALLET_DEPOIS,
                PROCEDENCIA_ANTES,
                PROCEDENCIA_DEPOIS,
                DATA_FABRICACAO_ANTES,
                DATA_FABRICACAO_DEPOIS,
                SITUACAO_ANTES,
                SITUACAO_DEPOIS,
                OBSERVACAO_ANTES,
                OBSERVACAO_DEPOIS,
                PROVISIONADO_ANTES,
                PROVISIONADO_DEPOIS,
                DESCARTE_EFETIVO_ANTES,
                DESCARTE_EFETIVO_DEPOIS,
                MOTIVO_ANTES,
                MOTIVO_DEPOIS,
                RNC_ANTES,
                RNC_DEPOIS,
                DISPOSICAO_ANTES,
                DISPOSICAO_DEPOIS,
                DATA_L_COMERC_ANTES,
                DATA_L_COMERC_DEPOIS,
                DATA_DISP_ANTES,
                DATA_DISP_DEPOIS,
                MOTIVO_DESCARTE_ANTES,
                MOTIVO_DESCARTE_DEPOIS,
                id_relacao
            FROM historico_alteracoes
            WHERE 1=1
        """
        params = []

        if usuario:
            sql += " AND USUARIO_SISTEMA LIKE %s"
            params.append(f"%{usuario.strip()}%")

        if qd:
            sql += " AND (QD_ANTES LIKE %s OR QD_DEPOIS LIKE %s)"
            params.extend([f"%{qd.strip()}%", f"%{qd.strip()}%"])

        if rua:
            sql += " AND (RUA_ANTES LIKE %s OR RUA_DEPOIS LIKE %s)"
            params.extend([f"%{rua.strip()}%", f"%{rua.strip()}%"])

        if produto:
            sql += " AND (PRODUTO_ANTES LIKE %s OR PRODUTO_DEPOIS LIKE %s)"
            params.extend([f"%{produto.strip()}%", f"%{produto.strip()}%"])

        if id_relacao:
            sql += " AND id_relacao LIKE %s"
            params.append(f"%{id_relacao.strip()}%")

        if data:
            sql += " AND DATA_HORA LIKE %s"
            params.append(f"%{data.strip()}%")

        sql += " ORDER BY ID DESC"

        with conn.cursor() as cursor:
            cursor.execute(sql, params)
            return cursor.fetchall()
    finally:
        conn.close()


@app.get("/historico/{id_relacao_item}")
def historico_por_relacao(id_relacao_item: str):
    return listar_historico(id_relacao=id_relacao_item)