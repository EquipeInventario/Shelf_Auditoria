import json
import os
import uuid
from contextvars import ContextVar
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, Optional, Set

import pymysql
from fastapi import FastAPI, Header, HTTPException, Request, Response
from fastapi.responses import JSONResponse
from fastapi.routing import APIRoute
from fastapi.middleware.cors import CORSMiddleware

# =========================================================
# APP
# =========================================================
app = FastAPI(title="API Base Ambev", version="6.4.0")
fastapi_app = app  # alias mantido apenas para compatibilidade local

@app.options("/{full_path:path}")
async def preflight_handler(full_path: str):
    return Response(
        status_code=204,
        headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET,POST,PUT,DELETE,OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type,Accept,Authorization,X-Session-UUID,X-Cliente-Operacao",
            "Access-Control-Max-Age": "86400",
        },
    )

# =========================================================
# ENV / DATABASE
# =========================================================
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = int(os.getenv("DB_PORT", "3306"))

DB_NAME_BASE = os.getenv("DB_NAME_BASE", "base_ambev")
# Banco oficial da Heineken, independente da Ambev.
DB_NAME_HEINEKEN = os.getenv("DB_NAME_HEINEKEN", "base_cerveja")
DB_NAME_INVENTARIO = os.getenv("DB_NAME_INVENTARIO", "inventario")

# Uma API, rotas operacionais separadas e bancos definidos no servidor.
CLIENTES = {
    "ambev": {"nome": "Ambev", "db": DB_NAME_BASE},
    "heineken": {"nome": "Heineken", "db": DB_NAME_HEINEKEN},
}
_OPERACAO_ATUAL: ContextVar[str] = ContextVar("shelf_operacao", default="ambev")
_TABELAS_OPERACIONAIS = frozenset({
    "estoque", "historico_alteracoes", "historico_historico",
    "historico_recebimento", "historico_expedicao", "produtos",
})


@app.middleware("http")
async def selecionar_operacao(request: Request, call_next):
    # As rotas antigas continuam exclusivas da Ambev.
    # A operacao Heineken possui rotas proprias: /heineken/...
    operacao = ("heineken" if request.url.path.startswith("/heineken/")
                else "ambev")
    token = _OPERACAO_ATUAL.set(operacao)
    try:
        if operacao == "heineken" and request.method.upper() != "OPTIONS":
            # Operacao disponivel a qualquer usuario autenticado do Shelf.
            # Preserva a verificacao de sessao, sem exigir perfil administrador.
            _exigir_sessao_header(request.headers.get("X-Session-UUID"))
        return await call_next(request)
    except HTTPException as exc:
        return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})
    except Exception as exc:
        # Devolve JSON com CORS mesmo em falhas inesperadas na operacao Heineken.
        import traceback
        print(
            f"[API][{operacao}][{request.method} {request.url.path}] "
            f"{type(exc).__name__}: {exc}",
            flush=True,
        )
        traceback.print_exception(type(exc), exc, exc.__traceback__)
        return JSONResponse(
            status_code=500,
            content={"detail": "Não foi possível concluir a operação."},
        )
    finally:
        _OPERACAO_ATUAL.reset(token)

# CORS deve ficar POR FORA do middleware de autorizacao para que
# respostas 401/403/500 tambem cheguem ao Flutter Web, sem Failed to fetch.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# =========================================================
# TABLES
# =========================================================
TABLES = {
    "estoque": {"db": DB_NAME_BASE, "pk": "ID"},
    "historico_alteracoes": {"db": DB_NAME_BASE, "pk": "ID"},
    "historico_historico": {"db": DB_NAME_BASE, "pk": "ID"},
    "historico_recebimento": {"db": DB_NAME_BASE, "pk": "ID"},
    "historico_expedicao": {"db": DB_NAME_BASE, "pk": "ID"},
    "produtos": {"db": DB_NAME_BASE, "pk": "ID"},
    "log": {"db": DB_NAME_INVENTARIO, "pk": "id"},
    "app_sessoes": {"db": DB_NAME_INVENTARIO, "pk": "id"},
    "app_atividades": {"db": DB_NAME_INVENTARIO, "pk": "id"},
}

# =========================================================
# CONNECTION
# =========================================================
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
        print("Erro conexão MYSQL:", e)
        return None


# =========================================================
# HELPERS
# =========================================================
def get_table_cfg(table_name: str):
    cfg = TABLES.get(table_name)
    if not cfg:
        raise HTTPException(status_code=404, detail="Tabela não suportada")
    if table_name in _TABELAS_OPERACIONAIS:
        return {**cfg, "db": CLIENTES[_OPERACAO_ATUAL.get()]["db"]}
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


def _norm_text(value: Any) -> str:
    return ("" if value is None else str(value)).strip()


def _norm_int(value: Any, default: Optional[int] = None) -> Optional[int]:
    if value is None or isinstance(value, bool):
        return default
    if isinstance(value, int):
        return value
    if isinstance(value, (float, Decimal)):
        return int(value)

    text = str(value).strip()
    if not text:
        return default

    text = text.replace(".", "").replace(",", ".")
    try:
        return int(float(text))
    except Exception:
        return default


def _safe_limit_offset(limit: Any = 200, offset: Any = 0, *, default_limit: int = 200, max_limit: int = 500):
    parsed_limit = _norm_int(limit, default_limit)
    parsed_offset = _norm_int(offset, 0)

    if parsed_limit is None or parsed_limit <= 0:
        parsed_limit = default_limit
    if parsed_offset is None or parsed_offset < 0:
        parsed_offset = 0

    return min(parsed_limit, max_limit), parsed_offset


# =========================================================
# SESSÕES / AUTORIZAÇÃO / PRODUTIVIDADE
# =========================================================
HEARTBEAT_TIMEOUT_SECONDS = max(
    60,
    _norm_int(os.getenv("HEARTBEAT_TIMEOUT_SECONDS"), 180) or 180,
)

PERFIS_GESTAO: Set[str] = {"gestor", "administrador"}
PERFIS_ADMIN: Set[str] = {"administrador"}

# O painel desta aplicação acompanha somente a operação Shelf.
# Usuários do setor ADM podem acessar o sistema quando forem gestor/administrador,
# mas não aparecem como colaboradores produtivos do Shelf.
SETOR_PRODUTIVIDADE = "shelf"
SETORES_GESTAO_GLOBAL: Set[str] = {"adm"}


def _norm_bool(value: Any, default: bool = True) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0

    text = _norm_text(value).lower()
    if text in {"1", "true", "sim", "s", "yes", "y", "ativo"}:
        return True
    if text in {"0", "false", "nao", "não", "n", "no", "inativo"}:
        return False
    return default


def _normalizar_perfil(value: Any) -> str:
    perfil = _norm_text(value).lower()

    if perfil in {"administrador", "admin", "adm"}:
        return "administrador"
    if perfil in {"gestor", "gerente", "supervisor"}:
        return "gestor"

    return "colaborador"


def _normalizar_setor(value: Any) -> str:
    setor = _norm_text(value).lower()

    if setor in {"shelf", "auditoria", "shelf auditoria"}:
        return "shelf"
    if setor in {"inventario", "inventário", "inventory"}:
        return "inventario"
    if setor in {"adm", "administrativo", "administracao", "administração"}:
        return "adm"

    return setor


def _parse_data_ref(value: Optional[str]) -> Optional[date]:
    text = _norm_text(value)
    if not text:
        return None

    try:
        return datetime.strptime(text, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail="data_ref inválida. Use o formato AAAA-MM-DD",
        )


def _get_session_uuid(
    data: Optional[Dict[str, Any]] = None,
    header_value: Optional[str] = None,
) -> str:
    value = header_value
    if not _norm_text(value) and isinstance(data, dict):
        value = data.get("sessao_uuid")

    sessao_uuid = _norm_text(value)
    if not sessao_uuid:
        raise HTTPException(status_code=401, detail="Sessão não informada")

    return sessao_uuid


def _carregar_sessao_conn(conn, sessao_uuid: str, *, for_update: bool = False):
    lock_sql = " FOR UPDATE" if for_update else ""

    with conn.cursor() as cursor:
        cursor.execute(
            f"""
            SELECT
                s.*,
                l.nome,
                l.login,
                l.perfil,
                l.setor,
                TIMESTAMPDIFF(
                    SECOND,
                    COALESCE(s.ultimo_heartbeat, s.login_em),
                    NOW()
                ) AS intervalo_heartbeat
            FROM app_sessoes s
            INNER JOIN log l ON l.id = s.id_usuario
            WHERE s.sessao_uuid = %s
            LIMIT 1{lock_sql}
            """,
            (sessao_uuid,),
        )
        return cursor.fetchone()


def _exigir_sessao_conn(
    conn,
    sessao_uuid: str,
    perfis_permitidos: Optional[Set[str]] = None,
):
    sessao = _carregar_sessao_conn(conn, sessao_uuid)

    if not sessao:
        raise HTTPException(status_code=401, detail="Sessão inválida")

    if _norm_text(sessao.get("status")).upper() != "ATIVA":
        raise HTTPException(status_code=401, detail="Sessão encerrada ou expirada")

    perfil = _normalizar_perfil(sessao.get("perfil"))
    setor = _normalizar_setor(sessao.get("setor"))
    sessao["perfil"] = perfil
    sessao["colaborador"] = perfil
    sessao["setor"] = setor

    if perfis_permitidos and perfil not in perfis_permitidos:
        raise HTTPException(status_code=403, detail="Acesso não autorizado")

    return sessao


def _exigir_sessao_header(
    x_session_uuid: Optional[str],
    perfis_permitidos: Optional[Set[str]] = None,
):
    sessao_uuid = _get_session_uuid(header_value=x_session_uuid)
    conn, _ = _open_db("app_sessoes")

    try:
        return _exigir_sessao_conn(conn, sessao_uuid, perfis_permitidos)
    finally:
        conn.close()


def _registrar_atividade_conn(
    conn,
    *,
    sessao_uuid: str,
    id_usuario: int,
    tipo_evento: str,
    tela: Optional[str] = None,
    referencia_id: Optional[str] = None,
    detalhes: Any = None,
):
    detalhes_json = None
    if _OPERACAO_ATUAL.get() == "heineken":
        detalhes = {**(detalhes if isinstance(detalhes, dict) else {"conteudo": detalhes}),
                    "cliente_operacao": "heineken"}
    if detalhes is not None:
        detalhes_json = json.dumps(
            detalhes,
            ensure_ascii=False,
            default=str,
        )

    with conn.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO app_atividades (
                sessao_uuid,
                id_usuario,
                tipo_evento,
                tela,
                referencia_id,
                detalhes,
                ocorrido_em
            )
            VALUES (%s, %s, %s, %s, %s, %s, NOW())
            """,
            (
                sessao_uuid,
                id_usuario,
                tipo_evento,
                tela,
                referencia_id,
                detalhes_json,
            ),
        )


def _registrar_atividade_segura(
    *,
    sessao: Dict[str, Any],
    tipo_evento: str,
    tela: Optional[str] = None,
    referencia_id: Optional[str] = None,
    detalhes: Any = None,
):
    """Registra atividade sem fazer a operação principal falhar."""
    # O painel de produtividade nao separa eventos por cliente.
    # Evitar misturar atividades da Heineken com os indicadores da Ambev.
    if _OPERACAO_ATUAL.get() == "heineken":
        return
    conn, _ = _open_db("app_atividades")
    try:
        _atualizar_heartbeat_conn(
            conn,
            sessao_uuid=_norm_text(sessao.get("sessao_uuid")),
            em_atividade=True,
        )
        _registrar_atividade_conn(
            conn,
            sessao_uuid=_norm_text(sessao.get("sessao_uuid")),
            id_usuario=int(sessao.get("id_usuario")),
            tipo_evento=tipo_evento,
            tela=tela,
            referencia_id=referencia_id,
            detalhes=detalhes,
        )
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Falha ao registrar atividade {tipo_evento}: {e}")
    finally:
        conn.close()


def _atualizar_heartbeat_conn(
    conn,
    *,
    sessao_uuid: str,
    em_atividade: bool,
):
    sessao = _carregar_sessao_conn(conn, sessao_uuid, for_update=True)

    if not sessao:
        raise HTTPException(status_code=401, detail="Sessão inválida")

    if _norm_text(sessao.get("status")).upper() != "ATIVA":
        raise HTTPException(status_code=409, detail="Sessão não está ativa")

    intervalo = max(0, _norm_int(sessao.get("intervalo_heartbeat"), 0) or 0)

    # Um intervalo grande normalmente significa aplicativo pausado, fechado
    # ou sem rede. Nesse caso, esse período não entra como tempo ativo.
    segundos_contabilizados = (
        intervalo if intervalo <= HEARTBEAT_TIMEOUT_SECONDS else 0
    )

    acrescimo_ativo = segundos_contabilizados if em_atividade else 0
    acrescimo_inativo = segundos_contabilizados if not em_atividade else 0

    with conn.cursor() as cursor:
        cursor.execute(
            """
            UPDATE app_sessoes
            SET
                ultimo_heartbeat = NOW(),
                segundos_ativos = segundos_ativos + %s,
                segundos_inativos = segundos_inativos + %s
            WHERE sessao_uuid = %s
            """,
            (acrescimo_ativo, acrescimo_inativo, sessao_uuid),
        )

        cursor.execute(
            """
            SELECT
                sessao_uuid,
                id_usuario,
                login_em,
                ultimo_heartbeat,
                segundos_ativos,
                segundos_inativos,
                status
            FROM app_sessoes
            WHERE sessao_uuid = %s
            LIMIT 1
            """,
            (sessao_uuid,),
        )
        return cursor.fetchone()


def _json_para_objeto(value: Any):
    if value is None or isinstance(value, (dict, list, int, float, bool)):
        return value

    try:
        return json.loads(str(value))
    except Exception:
        return value


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


def _colunas_gravaveis(conn, table_name: str) -> dict:
    """Leitura do esquema existente. Nao executa DDL ou migracoes."""
    if table_name not in TABLES:
        raise HTTPException(status_code=404, detail="Tabela desconhecida")
    with conn.cursor() as cursor:
        cursor.execute(f"SHOW FULL COLUMNS FROM `{table_name}`")
        rows = cursor.fetchall()
    if not rows:
        raise HTTPException(status_code=500, detail="Esquema da tabela indisponivel")
    return {
        str(row["Field"]).lower(): str(row["Field"])
        for row in rows
        if "GENERATED" not in str(row.get("Extra") or "").upper()
    }


def _ajustar_escrita_cliente(conn, table_name: str, data: dict) -> dict:
    """Somente Heineken: adaptar colunas da API ao esquema atual do banco."""
    if _OPERACAO_ATUAL.get() != "heineken":
        return data
    gravaveis = _colunas_gravaveis(conn, table_name)
    ajustado = {}
    descartadas = []
    for coluna, valor in data.items():
        nome_banco = gravaveis.get(str(coluna).lower())
        if nome_banco:
            if nome_banco.lower() not in {"id"}:
                ajustado[nome_banco] = valor
        else:
            descartadas.append(str(coluna))
    if descartadas:
        print(f"[heineken] {table_name}: campos nao gravaveis ignorados: {', '.join(descartadas)}")
    if not ajustado:
        raise HTTPException(status_code=400, detail="Nenhum campo gravavel recebido")
    return ajustado


def _registrar_historico_estoque_heineken(
    conn, antes: Optional[dict], depois: Optional[dict],
    item_id: int, usuario_app: Optional[str], operacao: str,
):
    """Grava a auditoria na MESMA transacao do estoque da Heineken.

    Compara imagens reais da linha antes/depois. Ausencia de informacao
    recebe a mesma representacao nos dois lados; nao cria mudanca ficticia.
    Nao altera o schema e nao e executada para a Ambev.
    """
    nome = _norm_text(usuario_app).split(" - ")[0].strip().upper()
    if not nome:
        nome = "USUARIO"
    acao = "EXCLUÍDO" if operacao == "excluir" else "EDITADO"
    prefixo = f"{acao} POR "
    nome = nome[:40 - len(prefixo) - len(" APP")]
    payload = {
        "USUARIO_SISTEMA": f"{prefixo}{nome} APP",
        "DATA_HORA": datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
        "id_relacao": str(item_id),
    }

    campos = (
        ("PRODUTO", ("PRODUTO",), "texto", "[00000]"),
        # Colunas historicas do banco oficial: apenas espelhar, nao editar.
        ("LOTE", ("lote", "LOTE"), "texto", "[00000]"),
        ("LINHA", ("linha", "LINHA"), "texto", "[00000]"),
        ("documento_extra", ("documento_extra", "DOCUMENTO_EXTRA"), "texto", "[00000]"),
        ("FORNECEDOR", ("FORNECEDOR", "fornecedor"), "texto", "[00000]"),
        ("AZ", ("AZ",), "inteiro", 0),
        ("QD", ("QD",), "texto", "[00000]"),
        ("RUA", ("RUA",), "inteiro", 0),
        ("AREA", ("AREA",), "area", "A"),
        ("QUANTIDADE_PACK", ("QUANTIDADE_PACK",), "inteiro", 0),
        ("QUANTIDADE_PLT", ("QUANTIDADE_PLT",), "inteiro", 0),
        ("TIPO_PALLET", ("TIPO_PALLET",), "texto", "[00000]"),
        ("PROCEDENCIA", ("PROCEDENCIA",), "texto", "[00000]"),
        ("DATA_FABRICACAO", ("DATA_FABRICACAO",), "data", "2000-01-01"),
        ("SITUACAO", ("SITUACAO2", "SITUACAO"), "texto", "LIBERADO"),
        ("OBSERVACAO", ("observacao", "OBSERVACAO"), "texto", "[00000]"),
        ("PROVISIONADO", ("PROVISIONADO",), "texto", "[00000]"),
        ("DESCARTE_EFETIVO", ("DESCARTE_EFETIVO",), "texto", "[00000]"),
        ("MOTIVO", ("MOTIVO",), "texto", "[00000]"),
        ("RNC", ("RNC",), "texto", "[00000]"),
        ("DISPOSICAO", ("DISPOSICAO",), "texto", "[00000]"),
        ("DATA_L_COMERC", ("DATA_L_COMERC",), "data", "2000-01-01"),
        ("DATA_DISP", ("DATA_DISP",), "data", "2000-01-01"),
        ("MOTIVO_DESCARTE", ("MOTIVO_DESCARTE",), "texto", "[00000]"),
    )

    def campo_valor(registro, chaves, tipo, padrao):
        if registro is None:
            return padrao
        bruto = next((registro[k] for k in chaves
                      if k in registro and registro[k] is not None), None)
        if bruto is None or not str(bruto).strip():
            return padrao
        if tipo == "inteiro":
            numero = _norm_int(bruto)
            return numero if numero is not None and numero >= 0 else padrao
        if tipo == "data":
            if isinstance(bruto, (datetime, date)):
                return bruto.strftime("%Y-%m-%d")
            return str(bruto).strip()[:10]
        if tipo == "area":
            return str(bruto).strip()[:1]
        return str(bruto).strip()

    for nome_campo, origens, tipo, padrao in campos:
        anterior = campo_valor(antes, origens, tipo, padrao)
        posterior = campo_valor(depois, origens, tipo, padrao)
        if operacao == "excluir":
            posterior = 0 if tipo == "inteiro" else (
                padrao if tipo in ("data", "area") else "[EXCLUIDO]"
            )
        payload[nome_campo + "_ANTES"] = anterior
        payload[nome_campo + "_DEPOIS"] = posterior

    payload = _ajustar_escrita_cliente(conn, "historico_alteracoes", payload)
    colunas = list(payload)
    sql_campos = ", ".join(f"`{coluna}`" for coluna in colunas)
    sql_valores = ", ".join("%s" for _ in colunas)
    with conn.cursor() as cursor:
        cursor.execute(
            f"INSERT INTO historico_alteracoes ({sql_campos}) VALUES ({sql_valores})",
            [payload[coluna] for coluna in colunas],
        )


def _insert_row(table_name: str, data: Dict[str, Any], usuario_app: Optional[str] = None,
                registrar_historico: bool = False):
    conn, _ = _open_db(table_name)

    try:
        data = _ensure_dict(data)
        if not data:
            raise HTTPException(status_code=400, detail="Payload vazio")
        if table_name == "historico_alteracoes" and _OPERACAO_ATUAL.get() == "heineken":
            data = dict(data)
            if data.get("USUARIO_SISTEMA") is not None:
                data["USUARIO_SISTEMA"] = str(data["USUARIO_SISTEMA"])[:40]

        data = _ajustar_escrita_cliente(conn, table_name, data)
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
            if registrar_historico and table_name == "estoque" and _OPERACAO_ATUAL.get() == "heineken":
                cursor.execute("SELECT * FROM estoque WHERE ID = %s", (new_id,))
                depois = cursor.fetchone()
                _registrar_historico_estoque_heineken(
                    conn, None, depois, new_id, usuario_app, "inserir"
                )

        conn.commit()
        return {"status": "ok", "ID": new_id}
    except HTTPException:
        conn.rollback()
        raise
    except Exception as e:
        conn.rollback()
        print(f"[SQL INSERT] {table_name} / {_OPERACAO_ATUAL.get()}: {type(e).__name__}: {e}")
        raise HTTPException(status_code=500, detail="Falha ao inserir registro")
    finally:
        conn.close()


def _update_row(table_name: str, item_id: int, data: Dict[str, Any], usuario_app: Optional[str] = None,
                registrar_historico: bool = False):
    conn, pk = _open_db(table_name)

    try:
        data = _ensure_dict(data)
        if not data:
            raise HTTPException(status_code=400, detail="Nada para atualizar")

        with conn.cursor() as cursor:
            if registrar_historico:
                consulta = f"SELECT * FROM `{table_name}` WHERE `{pk}` = %s LIMIT 1 FOR UPDATE"
            else:
                consulta = f"SELECT `{pk}` FROM `{table_name}` WHERE `{pk}` = %s LIMIT 1"
            cursor.execute(consulta, (item_id,))
            antes = cursor.fetchone()
            if not antes:
                raise HTTPException(status_code=404, detail="Registro não encontrado")

            data = _ajustar_escrita_cliente(conn, table_name, data)
            cols = list(data.keys())
            set_sql = ", ".join(f"`{c}` = %s" for c in cols)
            values = [data[c] for c in cols]
            values.append(item_id)

            cursor.execute(
                f"UPDATE `{table_name}` SET {set_sql} WHERE `{pk}` = %s",
                values,
            )
            if registrar_historico and table_name == "estoque" and _OPERACAO_ATUAL.get() == "heineken":
                cursor.execute("SELECT * FROM estoque WHERE ID = %s", (item_id,))
                depois = cursor.fetchone()
                _registrar_historico_estoque_heineken(
                    conn, antes, depois, item_id, usuario_app, "editar"
                )

        conn.commit()
        return {"status": "ok"}
    except HTTPException:
        conn.rollback()
        raise
    except Exception as e:
        conn.rollback()
        print(f"[SQL UPDATE] {table_name} / {_OPERACAO_ATUAL.get()}: {type(e).__name__}: {e}")
        raise HTTPException(status_code=500, detail="Falha ao atualizar registro")
    finally:
        conn.close()


def _delete_row(table_name: str, item_id: int, usuario_app: Optional[str] = None,
                registrar_historico: bool = False):
    conn, pk = _open_db(table_name)

    try:
        with conn.cursor() as cursor:
            if registrar_historico:
                consulta = f"SELECT * FROM `{table_name}` WHERE `{pk}` = %s LIMIT 1 FOR UPDATE"
            else:
                consulta = f"SELECT `{pk}` FROM `{table_name}` WHERE `{pk}` = %s LIMIT 1"
            cursor.execute(consulta, (item_id,))
            antes = cursor.fetchone()
            if not antes:
                raise HTTPException(status_code=404, detail="Registro não encontrado")

            cursor.execute(
                f"DELETE FROM `{table_name}` WHERE `{pk}` = %s",
                (item_id,),
            )
            if registrar_historico and table_name == "estoque" and _OPERACAO_ATUAL.get() == "heineken":
                _registrar_historico_estoque_heineken(
                    conn, antes, None, item_id, usuario_app, "excluir"
                )

        conn.commit()
        return {"status": "ok"}
    except HTTPException:
        conn.rollback()
        raise
    except Exception as e:
        conn.rollback()
        print(f"[SQL DELETE] {table_name} / {_OPERACAO_ATUAL.get()}: {type(e).__name__}: {e}")
        raise HTTPException(status_code=500, detail="Falha ao excluir registro")
    finally:
        conn.close()


# =========================================================
# PRODUTOS HELPERS
# =========================================================
def _fetch_produto_row(conn, produto: str):
    produto = _norm_text(produto)
    if not produto:
        return None

    with conn.cursor() as cursor:
        if _OPERACAO_ATUAL.get() == "heineken":
            # A base Heineken nao possui todas as colunas da Ambev
            # (por exemplo, exigeLinha). SELECT * usa o esquema existente.
            cursor.execute(
                "SELECT * FROM produtos WHERE PRODUTO = %s LIMIT 1",
                (produto,),
            )
        else:
            # Preservar integralmente o contrato atual da Ambev.
            cursor.execute(
                """
                SELECT
                    ID,
                    PRODUTO,
                    DESCRICAO,
                    ABREVIACAO,
                    SHELF,
                    VALIDADE_DIAS,
                    PACKS,
                    exigeFornecedor,
                    exigeLote,
                    tipo_produto,
                    exigeLinha,
                    exigeDocExtra
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
        if _OPERACAO_ATUAL.get() == "heineken":
            # Calcular PACKS usando somente os campos realmente necessarios.
            # Evita depender de colunas opcionais do cadastro de produtos.
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT PRODUTO, PACKS FROM produtos WHERE PRODUTO = %s LIMIT 1",
                    (produto,),
                )
                row = cursor.fetchone()
        else:
            row = _fetch_produto_row(conn, produto)
        if not row:
            raise HTTPException(status_code=404, detail=f"Produto não encontrado: {produto}")

        packs = _norm_int(row.get("PACKS"), 0)
        if not packs or packs <= 0:
            raise HTTPException(status_code=400, detail=f"PACKS inválido para o produto {produto}")

        return packs
    finally:
        conn.close()


# =========================================================
# ESTOQUE PAYLOAD
# =========================================================
def _preparar_estoque_ambev(data: Dict[str, Any], item_id: Optional[int] = None) -> Dict[str, Any]:
    payload = dict(data)

    payload.pop("SITUACAO3", None)
    payload.pop("situacao3", None)

    # Nao sobrescrever checklist existente ao editar.
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


# =========================================================
# HEINEKEN - CONTRATO OPERACIONAL INDEPENDENTE DA AMBEV
# =========================================================
# Campos sao definidos pela auditoria Heineken, nao copiados do cadastro
# de estoque Ambev. SHOW FULL COLUMNS e usado depois APENAS para verificar
# se cada campo permitido existe e pode ser gravado, sem alterar o MySQL.
HEINEKEN_ESTOQUE_CAMPOS = frozenset({
    "PRODUTO", "AZ", "QD", "RUA", "AREA",
    "QUANTIDADE_PLT", "DATA_FABRICACAO", "SITUACAO2",
    "observacao", "DATA_HOJE", "CHECKLIST_MASTER",
    "TIPO_PALLET", "DATA_RECEBIMENTO", "PROCEDENCIA", "NF",
    "CONFERENTE", "OPERADOR",
    "SHELF_DIAS", "VALIDADE_DIAS", "SITUACAO",
})
HEINEKEN_ESTOQUE_NUMERICOS = {"AZ", "RUA", "QUANTIDADE_PLT", "SHELF_DIAS", "VALIDADE_DIAS"}
HEINEKEN_ESTOQUE_CAMPOS_CANONICOS = {
    nome.lower(): nome for nome in HEINEKEN_ESTOQUE_CAMPOS
}


def _configuracao_produto_heineken(produto: str) -> Dict[str, int]:
    # Cadastro proprio da Heineken: somente PACKS, SHELF, VALIDADE_DIAS,
    # comprovadamente existentes em produtos. Nunca consultar exigeLinha.
    conn, _ = _open_db("produtos")
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT PACKS, SHELF, VALIDADE_DIAS FROM produtos "
                "WHERE PRODUTO = %s LIMIT 1",
                (produto,),
            )
            row = cursor.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Produto nao cadastrado")
        packs = _norm_int(row.get("PACKS"))
        if packs is None or packs <= 0:
            raise HTTPException(status_code=400, detail="PACKS nao cadastrado")
        return {
            "PACKS": packs,
            "SHELF": _norm_int(row.get("SHELF"), 0),
            "VALIDADE_DIAS": _norm_int(row.get("VALIDADE_DIAS"), 0),
        }
    finally:
        conn.close()


def _defaults_obrigatorios_heineken(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Preenchimento automatico do cadastro Heineken, nao da Ambev.

    Nao altera o banco. O INSERT filtra os campos pelo schema real.
    Valores existentes e informados pelo usuario nunca sao substituidos.
    """
    agora = datetime.now().strftime("[%d-%m-%Y %H:%M:%S]")
    situacao = _norm_text(payload.get("SITUACAO2")) or "LIBERADO"
    defaults = {
        "CHECKLIST_MASTER": "[00000]",
        "TIPO_PALLET": "PADRÃO",
        "DATA_RECEBIMENTO": agora,
        "PROCEDENCIA": "PRODUÇÃO",
        "NF": "[00000]",
        "CONFERENTE": "[AJUSTE AUDITORIA]",
        "OPERADOR": "[AJUSTE AUDITORIA]",
        "SITUACAO": situacao,
    }
    preenchido = dict(payload)
    for campo, valor in defaults.items():
        if not _norm_text(preenchido.get(campo)):
            preenchido[campo] = valor
    return preenchido


def _preparar_estoque_heineken(
    data: Dict[str, Any],
    item_id: Optional[int] = None,
) -> Dict[str, Any]:
    origem = _ensure_dict(data)
    payload: Dict[str, Any] = {}
    ignorados = []

    for chave, valor in origem.items():
        campo = HEINEKEN_ESTOQUE_CAMPOS_CANONICOS.get(str(chave).lower())
        if campo is None:
            ignorados.append(str(chave))
            continue
        # Campos de shelf derivados do cadastro nao devem ser inventados.
        # Somente aceitar valores de entrada declarados pelo cliente.
        if campo in HEINEKEN_ESTOQUE_NUMERICOS:
            if valor is None or str(valor).strip() == "":
                valor = None
            else:
                numero = _norm_int(valor)
                if numero is None:
                    raise HTTPException(status_code=400, detail=f"Valor invalido em {campo}")
                valor = numero
        elif campo == "PRODUTO":
            valor = _norm_text(valor)
        elif campo == "DATA_HOJE" and valor is not None:
            # O Flutter envia ISO com hora; no banco oficial a coluna e DATE.
            try:
                valor = date.fromisoformat(str(valor).strip()[:10]).isoformat()
            except ValueError:
                raise HTTPException(status_code=400, detail="DATA_HOJE invalida")
        payload[campo] = valor

    if ignorados:
        print("[Heineken] Campos de outro contrato ignorados: " + ", ".join(ignorados))

    conn, _ = _open_db("estoque")
    try:
        anterior = None
        if item_id is not None:
            with conn.cursor() as cursor:
                cursor.execute("SELECT * FROM estoque WHERE ID = %s LIMIT 1", (item_id,))
                anterior = cursor.fetchone()
            if not anterior:
                raise HTTPException(status_code=404, detail="Registro nao encontrado")

        produto = _norm_text(payload.get("PRODUTO", anterior.get("PRODUTO") if anterior else None))
        quantidade = payload.get("QUANTIDADE_PLT",
                                 _norm_int(anterior.get("QUANTIDADE_PLT")) if anterior else None)

        if not produto:
            raise HTTPException(status_code=400, detail="Produto obrigatorio")
        if quantidade is None or quantidade < 0:
            raise HTTPException(status_code=400, detail="Quantidade de paletes invalida")

        # O controle de shelf pertence ao cadastro da Heineken. Sempre
        # atualizar seus parametros quando o SKU for novo ou alterado.
        alterou_produto = (
            anterior is None or produto != _norm_text(anterior.get("PRODUTO"))
        )
        alterou_quantidade = (
            anterior is None or quantidade != _norm_int(anterior.get("QUANTIDADE_PLT"))
        )
        if alterou_produto:
            cadastro = _configuracao_produto_heineken(produto)
            payload["SHELF_DIAS"] = cadastro["SHELF"]
            payload["VALIDADE_DIAS"] = cadastro["VALIDADE_DIAS"]
            payload["QUANTIDADE_PACK"] = quantidade * cadastro["PACKS"]
        elif alterou_quantidade:
            payload["QUANTIDADE_PACK"] = quantidade * _get_produto_packs(produto)

        # Nao sobrescrever checklist existente. No cadastro sem checklist,
        # usar o identificador convencionado para essa situacao.
        if anterior is None:
            # Colunas obrigatorias recebem defaults apenas no cadastro.
            # O _ajustar_escrita_cliente verifica as colunas existentes
            # na tabela real, sem exigir novos campos no banco.
            payload = _defaults_obrigatorios_heineken(payload)

        # Nunca enviar colunas calculadas (DATA_SHELF, DATA_BLOQ,
        # DATA_VALIDADE, SITUACAO3). Nenhuma regra de ZI aqui.
        return payload
    finally:
        conn.close()


def _prepare_estoque_payload(
    data: Dict[str, Any], item_id: Optional[int] = None,
) -> Dict[str, Any]:
    if _OPERACAO_ATUAL.get() == "heineken":
        return _preparar_estoque_heineken(data, item_id=item_id)
    return _preparar_estoque_ambev(data, item_id=item_id)


# =========================================================
# ROOT / HEALTH
# =========================================================
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


@app.get("/clientes")
def listar_clientes_configurados(
    x_session_uuid: Optional[str] = Header(None, alias="X-Session-UUID"),
):
    # Lista de operacoes para qualquer usuario com sessao valida.
    _exigir_sessao_header(x_session_uuid)
    return [{"id": codigo, "nome": cfg["nome"]} for codigo, cfg in CLIENTES.items()]


@app.get("/heineken/validar")
def validar_operacao(
    x_session_uuid: Optional[str] = Header(None, alias="X-Session-UUID"),
):
    _exigir_sessao_header(x_session_uuid)
    # Validacao somente de leitura: banco e colunas indispensaveis.
    tabelas = (
        "estoque", "produtos", "historico_alteracoes",
        "historico_historico", "historico_recebimento", "historico_expedicao",
    )
    obrigatorios = {
        "estoque": {"ID", "PRODUTO", "QUANTIDADE_PLT", "QUANTIDADE_PACK", "DATA_HOJE"},
        "produtos": {"PRODUTO", "PACKS", "SHELF", "VALIDADE_DIAS"},
        "historico_alteracoes": {"ID", "id_relacao", "USUARIO_SISTEMA", "PRODUTO_ANTES", "PRODUTO_DEPOIS"},
    }
    faltantes = {}
    for tabela in tabelas:
        conn, _ = _open_db(tabela)
        try:
            colunas = _colunas_gravaveis(conn, tabela)
            ausentes = sorted(coluna for coluna in obrigatorios.get(tabela, ())
                              if coluna.lower() not in colunas)
            if ausentes:
                faltantes[tabela] = ausentes
        finally:
            conn.close()
    if faltantes:
        raise HTTPException(status_code=503, detail={"colunas_ausentes": faltantes})
    return {
        "status": "ok", "cliente": "heineken", "banco": DB_NAME_HEINEKEN,
        "tabelas_verificadas": list(tabelas), "somente_leitura": True,
    }


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

            if _OPERACAO_ATUAL.get() == "heineken":
                # Heineken nao opera com lote/linha nesta auditoria.
                # Nao consultar colunas que podem nao existir no banco.
                lotes = []
                linhas = []
            else:
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


@app.get("/estoque/{item_id}")
def obter_estoque(item_id: int):
    return _select_by_id("estoque", item_id)


@app.post("/estoque")
def inserir_estoque(
    data: Dict[str, Any],
    x_session_uuid: Optional[str] = Header(None, alias="X-Session-UUID"),
    x_historico_integrado: Optional[str] = Header(None, alias="X-Historico-Integrado"),
):
    sessao = _exigir_sessao_header(x_session_uuid)
    integrado = _OPERACAO_ATUAL.get() == "heineken" and x_historico_integrado == "1"
    payload = _prepare_estoque_payload(data)
    resultado = _insert_row("estoque", payload, usuario_app=sessao.get("nome") or data.get("usuario_app"),
                            registrar_historico=integrado)
    if integrado:
        resultado["historico_integrado"] = True
    novo_id = resultado.get("ID") or resultado.get("id")

    _registrar_atividade_segura(
        sessao=sessao,
        tipo_evento="ITEM_INSERIDO",
        tela="auditoria",
        referencia_id=str(novo_id) if novo_id is not None else None,
        detalhes={
            "produto": payload.get("PRODUTO"),
            "qd": payload.get("QD"),
            "rua": payload.get("RUA"),
        },
    )
    return resultado


@app.put("/estoque/{item_id}")
def atualizar_estoque(
    item_id: int,
    data: Dict[str, Any],
    x_session_uuid: Optional[str] = Header(None, alias="X-Session-UUID"),
    x_historico_integrado: Optional[str] = Header(None, alias="X-Historico-Integrado"),
):
    sessao = _exigir_sessao_header(x_session_uuid)
    integrado = _OPERACAO_ATUAL.get() == "heineken" and x_historico_integrado == "1"
    payload = _prepare_estoque_payload(data, item_id=item_id)
    resultado = _update_row("estoque", item_id, payload, usuario_app=sessao.get("nome") or data.get("usuario_app"),
                            registrar_historico=integrado)
    if integrado:
        resultado["historico_integrado"] = True

    _registrar_atividade_segura(
        sessao=sessao,
        tipo_evento="ITEM_EDITADO",
        tela="auditoria",
        referencia_id=str(item_id),
        detalhes={
            "produto": payload.get("PRODUTO"),
            "qd": payload.get("QD"),
            "rua": payload.get("RUA"),
        },
    )
    return resultado


@app.delete("/estoque/{item_id}")
def deletar_estoque(
    item_id: int,
    x_session_uuid: Optional[str] = Header(None, alias="X-Session-UUID"),
    x_historico_integrado: Optional[str] = Header(None, alias="X-Historico-Integrado"),
):
    sessao = _exigir_sessao_header(x_session_uuid)
    integrado = _OPERACAO_ATUAL.get() == "heineken" and x_historico_integrado == "1"
    resultado = _delete_row("estoque", item_id, usuario_app=sessao.get("nome"),
                            registrar_historico=integrado)
    if integrado:
        resultado["historico_integrado"] = True

    _registrar_atividade_segura(
        sessao=sessao,
        tipo_evento="ITEM_EXCLUIDO",
        tela="auditoria",
        referencia_id=str(item_id),
    )
    return resultado




# =========================================================
# HISTORICO_ALTERACOES
# =========================================================
@app.get("/historico")
def listar_historico(
    request: Request,
    x_session_uuid: Optional[str] = Header(None, alias="X-Session-UUID"),
):
    _exigir_sessao_header(x_session_uuid, PERFIS_GESTAO)
    return _select_all("historico_alteracoes", request)


@app.get("/historico/app")
def listar_historico_app(
    escopo: str = "meu",
    usuario: Optional[str] = None,
    limit: int = 200,
    offset: int = 0,
    x_session_uuid: Optional[str] = Header(None, alias="X-Session-UUID"),
):
    """
    Colaborador comum recebe sempre apenas o próprio histórico.
    Gestor e administrador podem usar escopo=todos para consultar toda a equipe.
    O nome usado para o filtro individual vem da sessão autenticada, e não do app.
    """
    sessao = _exigir_sessao_header(x_session_uuid)
    perfil = _normalizar_perfil(sessao.get("perfil"))
    escopo_normalizado = _norm_text(escopo).lower()

    limit, offset = _safe_limit_offset(
        limit,
        offset,
        default_limit=200,
        max_limit=1000,
    )

    pode_ver_todos = perfil in PERFIS_GESTAO and escopo_normalizado == "todos"

    filters = [
        "USUARIO_SISTEMA IS NOT NULL",
        "LOWER(USUARIO_SISTEMA) LIKE %s",
    ]
    values = ["%app%"]

    if pode_ver_todos:
        nome_filtro = _norm_text(usuario).split(" - ")[0].strip().lower()
        if nome_filtro:
            filters.append("LOWER(USUARIO_SISTEMA) LIKE %s")
            values.append(f"%{nome_filtro}%")
    else:
        nome_sessao = _norm_text(sessao.get("nome")).split(" - ")[0].strip().lower()
        if not nome_sessao:
            raise HTTPException(
                status_code=400,
                detail="Nome do usuário autenticado não está disponível",
            )
        filters.append("LOWER(USUARIO_SISTEMA) LIKE %s")
        values.append(f"%{nome_sessao}%")

    values.extend([limit, offset])

    conn, _ = _open_db("historico_alteracoes")
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT *
                FROM historico_alteracoes
                WHERE {' AND '.join(filters)}
                ORDER BY ID DESC
                LIMIT %s OFFSET %s
                """,
                values,
            )
            return cursor.fetchall()
    finally:
        conn.close()


@app.get("/historico/paginado")
def listar_historico_paginado(
    limit: int = 50,
    offset: int = 0,
    x_session_uuid: Optional[str] = Header(None, alias="X-Session-UUID"),
):
    _exigir_sessao_header(x_session_uuid, PERFIS_GESTAO)
    limit, offset = _safe_limit_offset(limit, offset, default_limit=50, max_limit=500)

    conn, _ = _open_db("historico_alteracoes")
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT *
                FROM historico_alteracoes
                ORDER BY ID DESC
                LIMIT %s OFFSET %s
                """,
                (limit, offset),
            )
            return cursor.fetchall()
    finally:
        conn.close()


@app.get("/historico/{item_id}")
def obter_historico(
    item_id: int,
    x_session_uuid: Optional[str] = Header(None, alias="X-Session-UUID"),
):
    _exigir_sessao_header(x_session_uuid, PERFIS_GESTAO)
    return _select_by_id("historico_alteracoes", item_id)


@app.post("/historico")
def inserir_historico(
    data: Dict[str, Any],
    x_session_uuid: Optional[str] = Header(None, alias="X-Session-UUID"),
):
    _exigir_sessao_header(x_session_uuid)
    return _insert_row("historico_alteracoes", data)


@app.put("/historico/{item_id}")
def atualizar_historico(
    item_id: int,
    data: Dict[str, Any],
    x_session_uuid: Optional[str] = Header(None, alias="X-Session-UUID"),
):
    _exigir_sessao_header(x_session_uuid, PERFIS_GESTAO)
    return _update_row("historico_alteracoes", item_id, data)


@app.delete("/historico/{item_id}")
def deletar_historico(
    item_id: int,
    x_session_uuid: Optional[str] = Header(None, alias="X-Session-UUID"),
):
    _exigir_sessao_header(x_session_uuid, PERFIS_ADMIN)
    return _delete_row("historico_alteracoes", item_id)


@app.get("/historico/por-relacao/{id_relacao}")
def historico_por_relacao(
    id_relacao: str,
    x_session_uuid: Optional[str] = Header(None, alias="X-Session-UUID"),
):
    _exigir_sessao_header(x_session_uuid, PERFIS_GESTAO)

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
# LOG / USUÁRIOS
# =========================================================
@app.get("/log")
def listar_log(
    x_session_uuid: Optional[str] = Header(None, alias="X-Session-UUID"),
):
    _exigir_sessao_header(x_session_uuid, PERFIS_ADMIN)

    conn, _ = _open_db("log")
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT id, nome, login, perfil, setor
                FROM log
                ORDER BY setor, nome, login
                """
            )
            rows = cursor.fetchall()

        for row in rows:
            perfil = _normalizar_perfil(row.get("perfil"))
            row["colaborador"] = perfil
            row["perfil"] = perfil
            row["setor"] = _normalizar_setor(row.get("setor"))

        return rows
    finally:
        conn.close()


@app.get("/log/{item_id}")
def obter_log(
    item_id: int,
    x_session_uuid: Optional[str] = Header(None, alias="X-Session-UUID"),
):
    _exigir_sessao_header(x_session_uuid, PERFIS_ADMIN)

    conn, _ = _open_db("log")
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT id, nome, login, perfil, setor
                FROM log
                WHERE id = %s
                LIMIT 1
                """,
                (item_id,),
            )
            row = cursor.fetchone()

        if not row:
            raise HTTPException(status_code=404, detail="Registro não encontrado")

        perfil = _normalizar_perfil(row.get("perfil"))
        row["colaborador"] = perfil
        row["perfil"] = perfil
        row["setor"] = _normalizar_setor(row.get("setor"))
        return row
    finally:
        conn.close()


@app.post("/log")
def inserir_log(
    data: Dict[str, Any],
    x_session_uuid: Optional[str] = Header(None, alias="X-Session-UUID"),
):
    _exigir_sessao_header(x_session_uuid, PERFIS_ADMIN)

    payload = dict(_ensure_dict(data))

    # A coluna real da tabela inventario.log é "perfil".
    # Aceita temporariamente "colaborador" no payload apenas por
    # compatibilidade com versões anteriores do aplicativo.
    perfil_recebido = payload.get("perfil", payload.get("colaborador"))
    payload.pop("colaborador", None)
    payload["perfil"] = _normalizar_perfil(perfil_recebido)

    if "setor" in payload:
        payload["setor"] = _normalizar_setor(payload.get("setor"))

    return _insert_row("log", payload)


@app.put("/log/{item_id}")
def atualizar_log(
    item_id: int,
    data: Dict[str, Any],
    x_session_uuid: Optional[str] = Header(None, alias="X-Session-UUID"),
):
    _exigir_sessao_header(x_session_uuid, PERFIS_ADMIN)

    payload = dict(_ensure_dict(data))

    # Atualiza sempre a coluna real "perfil". Também entende o nome
    # antigo "colaborador" para não quebrar builds já instalados.
    if "perfil" in payload or "colaborador" in payload:
        perfil_recebido = payload.get("perfil", payload.get("colaborador"))
        payload.pop("colaborador", None)
        payload["perfil"] = _normalizar_perfil(perfil_recebido)

    if "setor" in payload:
        payload["setor"] = _normalizar_setor(payload.get("setor"))

    return _update_row("log", item_id, payload)


@app.delete("/log/{item_id}")
def deletar_log(
    item_id: int,
    x_session_uuid: Optional[str] = Header(None, alias="X-Session-UUID"),
):
    sessao_admin = _exigir_sessao_header(x_session_uuid, PERFIS_ADMIN)

    if int(sessao_admin.get("id_usuario")) == int(item_id):
        raise HTTPException(
            status_code=400,
            detail="O administrador não pode excluir o próprio usuário durante a sessão",
        )

    return _delete_row("log", item_id)


# =========================================================
# LOGIN / SESSÕES
# =========================================================
@app.post("/auth/login")
def login(data: Dict[str, Any]):
    login_user = _norm_text(data.get("login"))
    senha = _norm_text(data.get("senha"))

    if not login_user or not senha:
        raise HTTPException(status_code=400, detail="login e senha são obrigatórios")

    plataforma = _norm_text(data.get("plataforma")) or None
    dispositivo = _norm_text(data.get("dispositivo")) or None
    versao_app = _norm_text(data.get("versao_app")) or None
    setor_app = _normalizar_setor(data.get("setor_app"))

    conn, _ = _open_db("log")
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT id, nome, login, perfil, setor
                FROM log
                WHERE login = %s AND senha = %s
                LIMIT 1
                """,
                (login_user, senha),
            )
            user = cursor.fetchone()

        if not user:
            raise HTTPException(status_code=401, detail="Usuário ou senha inválidos")

        perfil = _normalizar_perfil(user.get("perfil"))
        setor_usuario = _normalizar_setor(user.get("setor"))

        # Quando o aplicativo informa seu setor, bloqueia usuários de outra
        # operação. ADM permanece como setor corporativo autorizado.
        if setor_app:
            acesso_mesmo_setor = setor_usuario == setor_app
            acesso_gestao_global = (
                setor_usuario in SETORES_GESTAO_GLOBAL
                and perfil in PERFIS_GESTAO
            )

            if not acesso_mesmo_setor and not acesso_gestao_global:
                raise HTTPException(
                    status_code=403,
                    detail="Usuário não autorizado para este sistema",
                )

        sessao_uuid = str(uuid.uuid4())

        with conn.cursor() as cursor:
            # Impede que sessões antigas do mesmo usuário permaneçam como ativas.
            cursor.execute(
                """
                UPDATE app_sessoes
                SET
                    logout_em = COALESCE(ultimo_heartbeat, NOW()),
                    status = 'EXPIRADA'
                WHERE id_usuario = %s
                  AND status = 'ATIVA'
                """,
                (user["id"],),
            )

            cursor.execute(
                """
                INSERT INTO app_sessoes (
                    sessao_uuid,
                    id_usuario,
                    login_em,
                    ultimo_heartbeat,
                    segundos_ativos,
                    segundos_inativos,
                    plataforma,
                    dispositivo,
                    versao_app,
                    status,
                    criado_em
                )
                VALUES (
                    %s,
                    %s,
                    NOW(),
                    NOW(),
                    0,
                    0,
                    %s,
                    %s,
                    %s,
                    'ATIVA',
                    NOW()
                )
                """,
                (
                    sessao_uuid,
                    user["id"],
                    plataforma,
                    dispositivo,
                    versao_app,
                ),
            )

        _registrar_atividade_conn(
            conn,
            sessao_uuid=sessao_uuid,
            id_usuario=user["id"],
            tipo_evento="LOGIN_REALIZADO",
            tela="login",
            detalhes={
                "plataforma": plataforma,
                "dispositivo": dispositivo,
                "versao_app": versao_app,
                "setor_app": setor_app or None,
                "setor_usuario": setor_usuario or None,
            },
        )

        conn.commit()

        user["colaborador"] = perfil
        user["perfil"] = perfil
        user["setor"] = setor_usuario

        return {
            "status": "ok",
            "usuario": user,
            "sessao_uuid": sessao_uuid,
            "sessao": {
                "sessao_uuid": sessao_uuid,
                "status": "ATIVA",
            },
        }
    except HTTPException:
        conn.rollback()
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


@app.get("/auth/me")
def auth_me(
    x_session_uuid: Optional[str] = Header(None, alias="X-Session-UUID"),
):
    sessao_uuid = _get_session_uuid(header_value=x_session_uuid)
    conn, _ = _open_db("app_sessoes")

    try:
        sessao = _exigir_sessao_conn(conn, sessao_uuid)
        return {
            "status": "ok",
            "usuario": {
                "id": sessao.get("id_usuario"),
                "nome": sessao.get("nome"),
                "login": sessao.get("login"),
                "colaborador": sessao.get("perfil"),
                "perfil": sessao.get("perfil"),
                "setor": sessao.get("setor"),
            },
            "sessao": {
                "sessao_uuid": sessao.get("sessao_uuid"),
                "login_em": sessao.get("login_em"),
                "ultimo_heartbeat": sessao.get("ultimo_heartbeat"),
                "segundos_ativos": sessao.get("segundos_ativos"),
                "segundos_inativos": sessao.get("segundos_inativos"),
                "status": sessao.get("status"),
            },
        }
    finally:
        conn.close()


@app.post("/auth/heartbeat")
def heartbeat(
    data: Dict[str, Any],
    x_session_uuid: Optional[str] = Header(None, alias="X-Session-UUID"),
):
    sessao_uuid = _get_session_uuid(data, x_session_uuid)
    em_atividade = _norm_bool(data.get("em_atividade"), True)

    conn, _ = _open_db("app_sessoes")
    try:
        resultado = _atualizar_heartbeat_conn(
            conn,
            sessao_uuid=sessao_uuid,
            em_atividade=em_atividade,
        )
        conn.commit()

        return {
            "status": "ok",
            "sessao": resultado,
            "em_atividade": em_atividade,
        }
    except HTTPException:
        conn.rollback()
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


@app.post("/auth/logout")
def logout(
    data: Dict[str, Any],
    x_session_uuid: Optional[str] = Header(None, alias="X-Session-UUID"),
):
    sessao_uuid = _get_session_uuid(data, x_session_uuid)
    em_atividade = _norm_bool(data.get("em_atividade"), True)

    conn, _ = _open_db("app_sessoes")
    try:
        sessao = _carregar_sessao_conn(conn, sessao_uuid, for_update=True)
        if not sessao:
            raise HTTPException(status_code=401, detail="Sessão inválida")

        if _norm_text(sessao.get("status")).upper() != "ATIVA":
            return {"status": "ok", "message": "Sessão já estava encerrada"}

        intervalo = max(0, _norm_int(sessao.get("intervalo_heartbeat"), 0) or 0)
        contabilizado = intervalo if intervalo <= HEARTBEAT_TIMEOUT_SECONDS else 0
        acrescimo_ativo = contabilizado if em_atividade else 0
        acrescimo_inativo = contabilizado if not em_atividade else 0

        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE app_sessoes
                SET
                    logout_em = NOW(),
                    ultimo_heartbeat = NOW(),
                    segundos_ativos = segundos_ativos + %s,
                    segundos_inativos = segundos_inativos + %s,
                    status = 'ENCERRADA'
                WHERE sessao_uuid = %s
                """,
                (acrescimo_ativo, acrescimo_inativo, sessao_uuid),
            )

        _registrar_atividade_conn(
            conn,
            sessao_uuid=sessao_uuid,
            id_usuario=sessao["id_usuario"],
            tipo_evento="LOGOUT_REALIZADO",
            tela=_norm_text(data.get("tela")) or None,
        )

        conn.commit()
        return {"status": "ok", "message": "Sessão encerrada"}
    except HTTPException:
        conn.rollback()
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


# =========================================================
# ATIVIDADES DO APP
# =========================================================
@app.post("/atividades")
def registrar_atividade(
    data: Dict[str, Any],
    x_session_uuid: Optional[str] = Header(None, alias="X-Session-UUID"),
):
    sessao_uuid = _get_session_uuid(data, x_session_uuid)
    tipo_evento = _norm_text(data.get("tipo_evento")).upper()
    tela = _norm_text(data.get("tela")) or None
    referencia_id = _norm_text(data.get("referencia_id")) or None
    detalhes = data.get("detalhes")

    if _norm_text(data.get("cliente_operacao")) == "heineken":
        # A operacao Heineken nao compartilha indicadores com a Ambev.
        _exigir_sessao_header(x_session_uuid, PERFIS_ADMIN)
        return {"status": "ok", "detalhe": "atividade Heineken nao contabilizada no painel Ambev"}

    if not tipo_evento:
        raise HTTPException(status_code=400, detail="tipo_evento é obrigatório")
    if len(tipo_evento) > 60:
        raise HTTPException(status_code=400, detail="tipo_evento excede 60 caracteres")
    if tela and len(tela) > 80:
        raise HTTPException(status_code=400, detail="tela excede 80 caracteres")
    if referencia_id and len(referencia_id) > 100:
        raise HTTPException(status_code=400, detail="referencia_id excede 100 caracteres")

    conn, _ = _open_db("app_atividades")
    try:
        sessao = _exigir_sessao_conn(conn, sessao_uuid)

        _atualizar_heartbeat_conn(
            conn,
            sessao_uuid=sessao_uuid,
            em_atividade=True,
        )

        _registrar_atividade_conn(
            conn,
            sessao_uuid=sessao_uuid,
            id_usuario=sessao["id_usuario"],
            tipo_evento=tipo_evento,
            tela=tela,
            referencia_id=referencia_id,
            detalhes=detalhes,
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
# PAINEL DE PRODUTIVIDADE
# =========================================================
@app.get("/produtividade/painel")
def painel_produtividade(
    data_ref: Optional[str] = None,
    incluir_gestao: bool = False,
    x_session_uuid: Optional[str] = Header(None, alias="X-Session-UUID"),
):
    if _OPERACAO_ATUAL.get() == "heineken":
        raise HTTPException(status_code=409, detail="Produtividade Heineken nao disponivel: indicadores por cliente nao implementados")
    sessao_uuid = _get_session_uuid(header_value=x_session_uuid)
    data_consulta = _parse_data_ref(data_ref)

    conn, _ = _open_db("app_sessoes")
    try:
        solicitante = _exigir_sessao_conn(conn, sessao_uuid, PERFIS_GESTAO)

        with conn.cursor() as cursor:
            cursor.execute("SELECT CURDATE() AS hoje, NOW() AS agora")
            relogio = cursor.fetchone()

        if data_consulta is None:
            data_consulta = relogio["hoje"]

        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    l.id AS id_usuario,
                    l.nome,
                    l.login,
                    l.perfil,
                    l.setor,

                    s.primeiro_login,
                    s.ultimo_login,
                    s.ultima_presenca,
                    s.ultimo_heartbeat_ativo,
                    COALESCE(s.tem_sessao_ativa, 0) AS tem_sessao_ativa,
                    COALESCE(s.quantidade_sessoes, 0) AS quantidade_sessoes,
                    COALESCE(s.segundos_ativos, 0) AS segundos_ativos,
                    COALESCE(s.segundos_inativos, 0) AS segundos_inativos,

                    a.ultima_acao,
                    a.ultima_acao_produtiva,
                    COALESCE(a.total_atividades, 0) AS total_atividades,
                    COALESCE(a.auditorias_realizadas, 0) AS auditorias_realizadas,
                    COALESCE(a.posicoes_verificadas, 0) AS posicoes_verificadas,
                    COALESCE(a.divergencias_registradas, 0) AS divergencias_registradas,
                    COALESCE(a.itens_inseridos, 0) AS itens_inseridos,
                    COALESCE(a.itens_editados, 0) AS itens_editados,
                    COALESCE(a.itens_excluidos, 0) AS itens_excluidos

                FROM log l

                LEFT JOIN (
                    SELECT
                        id_usuario,
                        MIN(login_em) AS primeiro_login,
                        MAX(login_em) AS ultimo_login,
                        MAX(COALESCE(ultimo_heartbeat, logout_em, login_em)) AS ultima_presenca,
                        MAX(CASE WHEN status = 'ATIVA' THEN ultimo_heartbeat END) AS ultimo_heartbeat_ativo,
                        MAX(CASE WHEN status = 'ATIVA' THEN 1 ELSE 0 END) AS tem_sessao_ativa,
                        COUNT(*) AS quantidade_sessoes,
                        SUM(segundos_ativos) AS segundos_ativos,
                        SUM(segundos_inativos) AS segundos_inativos
                    FROM app_sessoes
                    WHERE login_em >= %s
                      AND login_em < DATE_ADD(%s, INTERVAL 1 DAY)
                    GROUP BY id_usuario
                ) s ON s.id_usuario = l.id

                LEFT JOIN (
                    SELECT
                        id_usuario,
                        MAX(ocorrido_em) AS ultima_acao,
                        MAX(
                            CASE
                                WHEN tipo_evento NOT IN (
                                    'LOGIN_REALIZADO',
                                    'LOGOUT_REALIZADO',
                                    'SESSAO_EXPIRADA',
                                    'APP_ABERTO',
                                    'APP_PAUSADO'
                                )
                                THEN ocorrido_em
                            END
                        ) AS ultima_acao_produtiva,
                        COUNT(*) AS total_atividades,
                        SUM(CASE WHEN tipo_evento = 'AUDITORIA_FINALIZADA' THEN 1 ELSE 0 END) AS auditorias_realizadas,
                        SUM(CASE WHEN tipo_evento = 'POSICAO_VERIFICADA' THEN 1 ELSE 0 END) AS posicoes_verificadas,
                        SUM(CASE WHEN tipo_evento = 'DIVERGENCIA_REGISTRADA' THEN 1 ELSE 0 END) AS divergencias_registradas,
                        SUM(CASE WHEN tipo_evento = 'ITEM_INSERIDO' THEN 1 ELSE 0 END) AS itens_inseridos,
                        SUM(CASE WHEN tipo_evento = 'ITEM_EDITADO' THEN 1 ELSE 0 END) AS itens_editados,
                        SUM(CASE WHEN tipo_evento = 'ITEM_EXCLUIDO' THEN 1 ELSE 0 END) AS itens_excluidos
                    FROM app_atividades
                    WHERE ocorrido_em >= %s
                      AND ocorrido_em < DATE_ADD(%s, INTERVAL 1 DAY)
                    GROUP BY id_usuario
                ) a ON a.id_usuario = l.id

                WHERE LOWER(TRIM(COALESCE(l.setor, ''))) = %s

                ORDER BY
                    CASE WHEN s.primeiro_login IS NULL THEN 1 ELSE 0 END,
                    s.primeiro_login,
                    l.nome
                """,
                (
                    data_consulta,
                    data_consulta,
                    data_consulta,
                    data_consulta,
                    SETOR_PRODUTIVIDADE,
                ),
            )
            rows = cursor.fetchall()

        hoje = relogio["hoje"]
        agora = relogio["agora"]
        colaboradores = []

        for row in rows:
            perfil = _normalizar_perfil(row.get("perfil"))
            setor = _normalizar_setor(row.get("setor"))

            # Defesa adicional: a consulta já filtra Shelf, mas o retorno
            # também é validado antes de montar o painel.
            if setor != SETOR_PRODUTIVIDADE:
                continue

            if not incluir_gestao and perfil != "colaborador":
                continue

            primeiro_login = row.get("primeiro_login")
            status_atual = "NAO_ENTROU"

            if primeiro_login is not None:
                if data_consulta != hoje:
                    status_atual = "FINALIZADO"
                else:
                    heartbeat = row.get("ultimo_heartbeat_ativo")
                    sessao_ativa = bool(row.get("tem_sessao_ativa"))

                    if sessao_ativa and heartbeat is not None:
                        atraso_heartbeat = max(
                            0,
                            int((agora - heartbeat).total_seconds()),
                        )

                        if atraso_heartbeat <= HEARTBEAT_TIMEOUT_SECONDS:
                            ultima_produtiva = row.get("ultima_acao_produtiva")
                            if ultima_produtiva is not None:
                                atraso_produtivo = max(
                                    0,
                                    int((agora - ultima_produtiva).total_seconds()),
                                )
                            else:
                                atraso_produtivo = HEARTBEAT_TIMEOUT_SECONDS + 1

                            status_atual = (
                                "ATIVO"
                                if atraso_produtivo <= HEARTBEAT_TIMEOUT_SECONDS
                                else "INATIVO"
                            )
                        else:
                            status_atual = "OFFLINE"
                    else:
                        status_atual = "OFFLINE"

            row["colaborador"] = perfil
            row["perfil"] = perfil
            row["setor"] = setor
            row["status_atual"] = status_atual
            row["alteracoes_realizadas"] = (
                int(row.get("itens_inseridos") or 0)
                + int(row.get("itens_editados") or 0)
                + int(row.get("itens_excluidos") or 0)
            )
            colaboradores.append(row)

        resumo = {
            "total_colaboradores": len(colaboradores),
            "colaboradores_logados": sum(
                1 for item in colaboradores if item.get("primeiro_login") is not None
            ),
            "nao_entraram": sum(
                1 for item in colaboradores if item.get("status_atual") == "NAO_ENTROU"
            ),
            "ativos": sum(
                1 for item in colaboradores if item.get("status_atual") == "ATIVO"
            ),
            "inativos": sum(
                1 for item in colaboradores if item.get("status_atual") == "INATIVO"
            ),
            "offline": sum(
                1 for item in colaboradores if item.get("status_atual") == "OFFLINE"
            ),
            "segundos_ativos": sum(
                int(item.get("segundos_ativos") or 0) for item in colaboradores
            ),
            "segundos_inativos": sum(
                int(item.get("segundos_inativos") or 0) for item in colaboradores
            ),
            "auditorias_realizadas": sum(
                int(item.get("auditorias_realizadas") or 0) for item in colaboradores
            ),
            "posicoes_verificadas": sum(
                int(item.get("posicoes_verificadas") or 0) for item in colaboradores
            ),
            "divergencias_registradas": sum(
                int(item.get("divergencias_registradas") or 0) for item in colaboradores
            ),
            "alteracoes_realizadas": sum(
                int(item.get("alteracoes_realizadas") or 0) for item in colaboradores
            ),
        }

        return {
            "status": "ok",
            "data_ref": data_consulta,
            "setor": SETOR_PRODUTIVIDADE,
            "solicitante": {
                "id": solicitante.get("id_usuario"),
                "nome": solicitante.get("nome"),
                "perfil": solicitante.get("perfil"),
                "setor": solicitante.get("setor"),
            },
            "resumo": resumo,
            "colaboradores": colaboradores,
        }
    finally:
        conn.close()


@app.get("/produtividade/colaborador/{id_usuario}")
def produtividade_colaborador(
    id_usuario: int,
    data_ref: Optional[str] = None,
    limit_atividades: int = 500,
    x_session_uuid: Optional[str] = Header(None, alias="X-Session-UUID"),
):
    if _OPERACAO_ATUAL.get() == "heineken":
        raise HTTPException(status_code=409, detail="Produtividade Heineken nao disponivel: indicadores por cliente nao implementados")
    sessao_uuid = _get_session_uuid(header_value=x_session_uuid)
    data_consulta = _parse_data_ref(data_ref)
    limit_atividades, _ = _safe_limit_offset(
        limit_atividades,
        0,
        default_limit=500,
        max_limit=1000,
    )

    conn, _ = _open_db("app_sessoes")
    try:
        _exigir_sessao_conn(conn, sessao_uuid, PERFIS_GESTAO)

        with conn.cursor() as cursor:
            cursor.execute("SELECT CURDATE() AS hoje")
            hoje = cursor.fetchone()["hoje"]

        if data_consulta is None:
            data_consulta = hoje

        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT id, nome, login, perfil, setor
                FROM log
                WHERE id = %s
                LIMIT 1
                """,
                (id_usuario,),
            )
            usuario = cursor.fetchone()

        if not usuario:
            raise HTTPException(status_code=404, detail="Colaborador não encontrado")

        perfil = _normalizar_perfil(usuario.get("perfil"))
        setor = _normalizar_setor(usuario.get("setor"))

        if setor != SETOR_PRODUTIVIDADE:
            raise HTTPException(
                status_code=404,
                detail="Colaborador não encontrado no setor Shelf",
            )

        usuario["colaborador"] = perfil
        usuario["perfil"] = perfil
        usuario["setor"] = setor

        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    id,
                    sessao_uuid,
                    login_em,
                    logout_em,
                    ultimo_heartbeat,
                    segundos_ativos,
                    segundos_inativos,
                    plataforma,
                    dispositivo,
                    versao_app,
                    status,
                    criado_em
                FROM app_sessoes
                WHERE id_usuario = %s
                  AND login_em >= %s
                  AND login_em < DATE_ADD(%s, INTERVAL 1 DAY)
                ORDER BY login_em DESC
                """,
                (id_usuario, data_consulta, data_consulta),
            )
            sessoes = cursor.fetchall()

            cursor.execute(
                """
                SELECT
                    id,
                    sessao_uuid,
                    tipo_evento,
                    tela,
                    referencia_id,
                    detalhes,
                    ocorrido_em
                FROM app_atividades
                WHERE id_usuario = %s
                  AND ocorrido_em >= %s
                  AND ocorrido_em < DATE_ADD(%s, INTERVAL 1 DAY)
                ORDER BY ocorrido_em DESC
                LIMIT %s
                """,
                (
                    id_usuario,
                    data_consulta,
                    data_consulta,
                    limit_atividades,
                ),
            )
            atividades = cursor.fetchall()

        for atividade in atividades:
            atividade["detalhes"] = _json_para_objeto(atividade.get("detalhes"))

        contagem_tipos: Dict[str, int] = {}
        for atividade in atividades:
            tipo = _norm_text(atividade.get("tipo_evento")).upper()
            contagem_tipos[tipo] = contagem_tipos.get(tipo, 0) + 1

        resumo = {
            "primeiro_login": min(
                (s.get("login_em") for s in sessoes if s.get("login_em") is not None),
                default=None,
            ),
            "ultima_atividade": max(
                (
                    a.get("ocorrido_em")
                    for a in atividades
                    if a.get("ocorrido_em") is not None
                ),
                default=None,
            ),
            "quantidade_sessoes": len(sessoes),
            "segundos_ativos": sum(int(s.get("segundos_ativos") or 0) for s in sessoes),
            "segundos_inativos": sum(int(s.get("segundos_inativos") or 0) for s in sessoes),
            "total_atividades": len(atividades),
            "auditorias_realizadas": contagem_tipos.get("AUDITORIA_FINALIZADA", 0),
            "posicoes_verificadas": contagem_tipos.get("POSICAO_VERIFICADA", 0),
            "divergencias_registradas": contagem_tipos.get("DIVERGENCIA_REGISTRADA", 0),
            "itens_inseridos": contagem_tipos.get("ITEM_INSERIDO", 0),
            "itens_editados": contagem_tipos.get("ITEM_EDITADO", 0),
            "itens_excluidos": contagem_tipos.get("ITEM_EXCLUIDO", 0),
        }

        return {
            "status": "ok",
            "data_ref": data_consulta,
            "usuario": usuario,
            "resumo": resumo,
            "sessoes": sessoes,
            "atividades": atividades,
        }
    finally:
        conn.close()


# =========================================================
# HISTORICO RECEBIMENTO
# =========================================================
@app.get("/historico-recebimento")
def listar_historico_recebimento(request: Request):
    return _select_all("historico_recebimento", request)


@app.get("/historico-recebimento/paginado")
def listar_historico_recebimento_paginado(
    limit: int = 50,
    offset: int = 0,
):
    conn, _ = _open_db("historico_recebimento")

    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT *
                FROM historico_recebimento
                ORDER BY ID DESC
                LIMIT %s OFFSET %s
                """,
                (limit, offset),
            )

            return cursor.fetchall()
    finally:
        conn.close()


@app.get("/historico-recebimento/{item_id}")
def obter_historico_recebimento(item_id: int):
    return _select_by_id("historico_recebimento", item_id)


@app.post("/historico-recebimento")
def inserir_historico_recebimento(data: Dict[str, Any]):
    return _insert_row("historico_recebimento", data)


@app.put("/historico-recebimento/{item_id}")
def atualizar_historico_recebimento(item_id: int, data: Dict[str, Any]):
    return _update_row("historico_recebimento", item_id, data)


@app.delete("/historico-recebimento/{item_id}")
def deletar_historico_recebimento(item_id: int):
    return _delete_row("historico_recebimento", item_id)


@app.get("/historico-recebimento/checklist/{checklist}")
def historico_recebimento_por_checklist(checklist: str):
    conn, _ = _open_db("historico_recebimento")

    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT *
                FROM historico_recebimento
                WHERE CHECKLIST_MASTER = %s
                ORDER BY ID DESC
                """,
                (checklist,),
            )

            return cursor.fetchall()
    finally:
        conn.close()


# =========================================================
# HISTORICO EXPEDICAO
# =========================================================
@app.get("/historico-expedicao")
def listar_historico_expedicao(request: Request):
    return _select_all("historico_expedicao", request)


@app.get("/historico-expedicao/paginado")
def listar_historico_expedicao_paginado(
    limit: int = 50,
    offset: int = 0,
):
    conn, _ = _open_db("historico_expedicao")

    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT *
                FROM historico_expedicao
                ORDER BY ID DESC
                LIMIT %s OFFSET %s
                """,
                (limit, offset),
            )

            return cursor.fetchall()
    finally:
        conn.close()


@app.get("/historico-expedicao/{item_id}")
def obter_historico_expedicao(item_id: int):
    return _select_by_id("historico_expedicao", item_id)


@app.post("/historico-expedicao")
def inserir_historico_expedicao(data: Dict[str, Any]):
    return _insert_row("historico_expedicao", data)


@app.put("/historico-expedicao/{item_id}")
def atualizar_historico_expedicao(item_id: int, data: Dict[str, Any]):
    return _update_row("historico_expedicao", item_id, data)


@app.delete("/historico-expedicao/{item_id}")
def deletar_historico_expedicao(item_id: int):
    return _delete_row("historico_expedicao", item_id)


@app.get("/historico-expedicao/checklist/{checklist}")
def historico_expedicao_por_checklist(checklist: str):
    conn, _ = _open_db("historico_expedicao")

    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT *
                FROM historico_expedicao
                WHERE CHECKLIST_MASTER = %s
                ORDER BY ID DESC
                """,
                (checklist,),
            )

            return cursor.fetchall()
    finally:
        conn.close()
# =========================================================
# HEINEKEN: MESMA API, ROTAS PROPRIAS NO BANCO OFICIAL
# =========================================================
# O mobile inventario_app usa este mesmo principio (rotas distintas por
# operacao). Mantemos os endpoints originais imutaveis para a Ambev.
_PREFIXOS_OPERACIONAIS = (
    "/estoque", "/produtos", "/historico", "/historico-historico",
    "/historico-recebimento", "/historico-expedicao",
)
_rotas_ambev = list(app.routes)
for _rota in _rotas_ambev:
    if not isinstance(_rota, APIRoute):
        continue
    if not any(
        _rota.path == prefix or _rota.path.startswith(prefix + "/")
        for prefix in _PREFIXOS_OPERACIONAIS
    ):
        continue
    app.add_api_route(
        "/heineken" + _rota.path,
        _rota.endpoint,
        methods=sorted(_rota.methods or {"GET"}),
        name=_rota.name + "_heineken",
        include_in_schema=False,
    )
