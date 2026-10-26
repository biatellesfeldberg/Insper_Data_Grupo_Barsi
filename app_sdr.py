"""Interface SDR. Executar: python app_sdr.py (tema incluído no próprio arquivo)."""
from __future__ import annotations
import csv
import io
from html import escape
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import time
import uuid
from urllib.parse import quote
from contextlib import closing
from datetime import datetime
from pathlib import Path
import pandas as pd
import streamlit as st
from preparar_contagens_sdr import load_counts, count_loaded, source_signature
from preparar_base_sdr import (
    DATA_COLUMNS, SETORES, SETOR_IDS, NOMES_SETORES,
    INDEX_KEYS, INDEX_NAMES, PORTABLE_VERSION, normalizar,
)

ROOT = Path(__file__).resolve().parent

THEME_OPTIONS = {
    'base': 'dark',
    'primaryColor': '#F4C928',
    'backgroundColor': '#080808',
    'secondaryBackgroundColor': '#141414',
    'textColor': '#ECECEC',
    'font': 'sans serif',
}


def comando_inicializacao():
    return [sys.executable, '-m', 'streamlit', 'run', str(Path(__file__).resolve()),
            *[f'--theme.{key}={value}' for key, value in THEME_OPTIONS.items()],
            *sys.argv[1:]]


if __name__ == '__main__':
    from streamlit.runtime.scriptrunner import get_script_run_ctx
    if get_script_run_ctx(suppress_warning=True) is None:
        # Python comum inicia o servidor. Dentro do Streamlit, apenas renderiza a tela.
        try:
            server_env = dict(os.environ)
            temp_dir = Path(server_env.get('SDR_EXPORT_DIR', str(ROOT / 'temporarios_exportacao_sdr'))).resolve()
            temp_dir.mkdir(parents=True, exist_ok=True)
            server_env.setdefault('SQLITE_TMPDIR', str(temp_dir))
            server_env.setdefault('TMPDIR', str(temp_dir))
            status = subprocess.call(comando_inicializacao(), cwd=ROOT, env=server_env)
        except KeyboardInterrupt:
            status = 130
        raise SystemExit(status)

DATABASE = ROOT / 'base_sdr_pronta.sqlite3'
PORTES = {'00': 'Não informado', '01': 'Microempresa',
          '03': 'Empresa de pequeno porte', '05': 'Demais empresas'}
EXPORT_LIMIT = 100_000
EXPORT_COLUMNS = DATA_COLUMNS + ['setor']
DEFAULT_COLUMNS = ['cnpj', 'razao_social', 'nome_fantasia', 'setor', 'descricao_cnae',
                   'uf', 'nome_municipio', 'porte_empresa', 'capital_social',
                   'contato_1', 'contato_2', 'correio_eletronico']

COLUMN_LABELS = {
    'cnpj': 'CNPJ', 'razao_social': 'Razão social', 'nome_fantasia': 'Nome fantasia',
    'setor': 'Setor', 'descricao_cnae': 'Atividade principal', 'uf': 'UF',
    'nome_municipio': 'Município', 'porte_empresa': 'Porte', 'capital_social': 'Capital social (R$)',
    'contato_1': 'Telefone principal', 'contato_2': 'Telefone alternativo',
    'correio_eletronico': 'E-mail', 'cnae_fiscal_principal': 'Código CNAE',
    'cep': 'CEP', 'bairro': 'Bairro', 'logradouro': 'Logradouro', 'numero': 'Número',
    'complemento': 'Complemento', 'natureza_juridica': 'Natureza jurídica',
}


def numero_br(valor):
    return f'{valor:,}'.replace(',', '.')


def where(filters, indexed_prefix=None):
    clauses, params = [], []
    for key, column in [('setores', 'setor_id'), ('ufs', 'uf'), ('cidades', 'nome_municipio'),
                        ('cnaes', 'cnae_fiscal_principal'), ('portes', 'porte_empresa')]:
        values = filters.get(key, [])
        if values:
            # Fora do prefixo utilizável, o filtro é residual no índice cobridor.
            # O + evita que o SQLite descarte termos de ordenação intermediários.
            expr = '+' + column if indexed_prefix is not None and column not in indexed_prefix else column
            clauses.append(expr + ' = ?' if len(values) == 1 else expr + ' IN (' + ','.join('?' for _ in values) + ')')
            params.extend([SETOR_IDS[v] for v in values] if key == 'setores' else values)
    if filters.get('busca'):
        clauses.append('lead_id IN (SELECT rowid FROM leads WHERE instr(busca, ?) > 0)')
        params.append(normalizar(filters['busca']))
    if filters.get('email'):
        clauses.append('tem_email = 1')
    if filters.get('usar_capital'):
        clauses.append('capital_num BETWEEN ? AND ?')
        params.extend([filters['capital_min'], filters['capital_max']])
    return (' WHERE ' + ' AND '.join(clauses)) if clauses else '', params



def indice_consulta(filters, stats):
    # Seleciona o filtro indexado com menor população estimada (não só o setor).
    candidates = [(sum(stats[key].get(v, 0) for v in filters[key]), key)
                  for key in INDEX_KEYS if filters.get(key)]
    return min(candidates)[1] if candidates else 'setores'


def consulta_pagina(filters, columns, stats, cursor=None, limit=100):
    key = indice_consulta(filters, stats)
    keys = INDEX_KEYS[key]
    selected_columns = {INDEX_KEYS[k][0] for k in INDEX_KEYS if filters.get(k)}
    prefix = set()
    for column in keys:
        if column not in selected_columns:
            break
        prefix.add(column)
    clause, params = where(filters, indexed_prefix=prefix)
    if cursor:
        clause += (' AND ' if clause else ' WHERE ') + '(' + ','.join(keys) + ') > (' + ','.join('?' for _ in keys) + ')'
        params.extend(cursor)
    sql = 'SELECT lead_id,' + ','.join(keys) + f' FROM filtros_leads INDEXED BY {INDEX_NAMES[key]}' + clause
    sql += ' ORDER BY ' + ','.join(keys) + ' LIMIT ?'
    return sql, params + [limit + 1]


def ler_pagina(database, filters, columns, stats, cursor=None, limit=100):
    sql, params = consulta_pagina(filters, columns, stats, cursor, limit)
    with connect(database) as con:
        found = con.execute(sql, params).fetchall()
        shown = found[:limit]
        # Buscar dados completos apenas para os IDs retornados pelo índice.
        fields = ','.join('"' + c + '"' for c in columns if c != 'setor')
        if not shown:
            return [], None
        ids = [r[0] for r in shown]
        data = {}
        for start in range(0, len(ids), 500):
            part = ids[start:start + 500]
            marks = ','.join('?' for _ in part)
            extra = ',' + fields if fields else ''
            for row in con.execute(f'SELECT rowid{extra} FROM leads WHERE rowid IN ({marks})', part):
                data[row[0]] = dict(zip([c for c in columns if c != 'setor'], row[1:]))
        # O nome do setor é derivado de um inteiro já salvo, sem reclassificar CNAEs.
        sectors = {}
        if 'setor' in columns:
            for start in range(0, len(ids), 500):
                part = ids[start:start + 500]
                marks = ','.join('?' for _ in part)
                sectors.update(con.execute(f'SELECT lead_id,setor_id FROM filtros_leads WHERE lead_id IN ({marks})', part).fetchall())
    rows = [tuple(NOMES_SETORES[sectors[i]] if c == 'setor' else data[i][c] for c in columns) for i in ids]
    next_cursor = list(shown[-1][1:]) if len(found) > limit else None
    return rows, next_cursor

def contagem_salva(filters, catalogo):
    """Usa total persistido só quando ele é exato para a seleção atual."""
    if filters.get('busca') or filters.get('email') or filters.get('usar_capital'):
        return None
    ativos = [key for key in INDEX_KEYS if filters.get(key)]
    if not ativos:
        return catalogo['total']
    if len(ativos) == 1:
        key = ativos[0]
        return sum(catalogo['stats'][key].get(v, 0) for v in filters[key])
    return None


def csv_bytes(rows, columns):
    output = io.StringIO(newline='')
    writer = csv.writer(output, delimiter=';')
    writer.writerow(columns)
    for row in rows:
        # Protege células textuais contra fórmulas ao abrir em planilhas.
        writer.writerow(celulas_csv(row))
    return output.getvalue().encode('utf-8-sig')


def celulas_csv(row):
    return ["'" + str(v) if str(v).lstrip().startswith(('=', '+', '-', '@')) else v for v in row]


def consulta_ids_exportacao(filters, stats, limite=None):
    """Cruza filtros estruturados lendo só índices; evita consultas à tabela por candidato."""
    parts = []
    for key in INDEX_KEYS:
        values = filters.get(key, [])
        if not values or (key == 'cidades' and filters.get('ufs')):
            continue
        terms, args = [], []
        dims = [key, 'cidades'] if key == 'ufs' and filters.get('cidades') else [key]
        for dim in dims:
            selected = filters[dim]
            column = INDEX_KEYS[dim][0]
            terms.append(column + ' IN (' + ','.join('?' for _ in selected) + ')')
            args.extend(SETOR_IDS[v] for v in selected) if dim == 'setores' else args.extend(selected)
        query = ('SELECT lead_id FROM filtros_leads INDEXED BY ' + INDEX_NAMES[key] +
                 ' WHERE ' + ' AND '.join(terms))
        estimate = sum(stats[key].get(v, 0) for v in values)
        parts.append((estimate, key, query, args))
    parts.sort(key=lambda item: (item[0], item[1]))
    residual = {k: filters[k] for k in ('busca', 'email', 'usar_capital', 'capital_min', 'capital_max')
                if k in filters}
    has_residual = any(residual.get(k) for k in ('busca', 'email', 'usar_capital'))
    params = [value for _, _, _, args in parts for value in args]
    if not parts:
        if has_residual:
            clause, params = where(residual)
            sql = 'SELECT lead_id FROM filtros_leads NOT INDEXED' + clause + ' ORDER BY lead_id'
        else:
            sql = ('SELECT lead_id FROM filtros_leads INDEXED BY ' + INDEX_NAMES['setores'] +
                   ' ORDER BY setor_id,lead_id')
    else:
        combined = ' INTERSECT '.join(query for _, _, query, _ in parts)
        if has_residual:
            clause, extra = where(residual)
            sql = ('SELECT lead_id FROM filtros_leads NOT INDEXED WHERE lead_id IN (' + combined +
                   ') AND ' + clause[len(' WHERE '):] + ' ORDER BY lead_id')
            params.extend(extra)
        elif len(parts) == 1:
            sql = combined + ' ORDER BY ' + ','.join(INDEX_KEYS[parts[0][1]])
        else:
            sql = combined + ' ORDER BY lead_id'
    if limite is not None:
        sql += ' LIMIT ?'
        params.append(limite)
    return sql, params


def exportar_csv_hd(database, filters, columns, stats, total, progresso=lambda n: None,
                    lote=5000, limite=None, etapa=lambda texto: None, diagnostico=None):
    """Seleciona uma vez e lê dados por rowid crescente, sem reutilizar a paginação."""
    if lote < 1 or total < 0 or (limite is not None and limite < 0):
        raise ValueError('Tamanho de lote, limite ou total inválido.')
    esperado = min(total, limite) if limite is not None else total
    pasta = Path(os.environ.get('SDR_EXPORT_DIR', str(Path(database).parent / 'exportacoes_sdr')))
    pasta.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix=f'leads_{datetime.now():%Y%m%d_%H%M%S}_{uuid.uuid4().hex}_',
                                suffix='.csv.partial', dir=pasta)
    parcial = Path(name)
    destino = parcial.with_suffix('')
    job_path = None
    started = time.monotonic()
    gravadas = 0
    arquivo = None
    try:
        job_fd, job_name = tempfile.mkstemp(prefix='export_ids_', suffix='.sqlite3.partial', dir=pasta)
        os.close(job_fd)
        job_path = Path(job_name)
        arquivo = os.fdopen(fd, 'w', encoding='utf-8-sig', newline='')
        with arquivo, connect(database) as con:
            con.execute('PRAGMA cache_size=-65536')
            con.execute('ATTACH DATABASE ? AS exportacao', (str(job_path.resolve()),))
            # Só o arquivo de IDs descartável é escrito; a base continua read-only.
            con.execute('PRAGMA exportacao.journal_mode=OFF')
            con.execute('PRAGMA exportacao.synchronous=OFF')
            con.execute('CREATE TABLE exportacao.ids(lead_id INTEGER PRIMARY KEY)')
            select_sql, params = consulta_ids_exportacao(filters, stats, limite)
            sql = 'INSERT INTO exportacao.ids(lead_id) ' + select_sql
            last_report = started
            etapa('1 de 2 · Selecionando empresas…')
            def informar_selecao():
                nonlocal last_report
                now = time.monotonic()
                if now-last_report >= 2:
                    etapa(f'1 de 2 · Selecionando empresas… {now-started:.0f} s')
                    last_report = now
                return 0
            con.set_progress_handler(informar_selecao, 100000)
            try:
                con.execute(sql, params)
                con.commit()
            finally:
                con.set_progress_handler(None, 0)
            selecao_s = time.monotonic()-started
            selected_count = con.execute('SELECT count(*) FROM exportacao.ids').fetchone()[0]
            if selected_count != esperado:
                raise ValueError(f'A seleção encontrou {selected_count:,} empresas; eram esperadas '
                                 f'{esperado:,}. Reaplique os filtros.')
            etapa('2 de 2 · Lendo dados e gravando CSV…')
            writer = csv.writer(arquivo, delimiter=';')
            writer.writerow(columns)
            fields = [c for c in columns if c != 'setor']
            sector_labels = {}
            if 'setor' in columns:
                # O catálogo contém a classificação já preparada por CNAE.
                catalog = json.loads(con.execute('SELECT payload FROM sdr_catalogo').fetchone()[0])
                sector_labels = {code: sector for code, _, sector in catalog['cnaes']}
                if 'cnae_fiscal_principal' not in fields:
                    fields.append('cnae_fiscal_principal')
            quoted = ','.join('"'+c+'"' for c in fields)
            cursor = con.execute('SELECT lead_id FROM exportacao.ids ORDER BY lead_id')
            while True:
                batch = cursor.fetchmany(lote)
                if not batch:
                    break
                # Os IDs já estão ordenados. As buscas mantêm o mesmo sentido no banco,
                # preservando o cache e sem repetir consultas para recuperar o setor.
                for start in range(0, len(batch), 500):
                    part = batch[start:start+500]
                    ids = [row[0] for row in part]
                    marks = ','.join('?' for _ in ids)
                    extra = ','+quoted if quoted else ''
                    query = f'SELECT rowid{extra} FROM leads WHERE rowid IN ({marks}) ORDER BY rowid'
                    records = con.execute(query, ids)
                    for record in records:
                        values = dict(zip(fields, record[1:]))
                        row = [sector_labels[values['cnae_fiscal_principal']] if c == 'setor' else values[c] for c in columns]
                        writer.writerow(celulas_csv(row))
                        gravadas += 1
                progresso(gravadas)
            if gravadas != esperado:
                raise ValueError(f'Foram gravadas {gravadas:,} empresas; eram esperadas {esperado:,}. '
                                 'O arquivo não foi publicado; reaplique os filtros.')
        os.replace(parcial, destino)
        total_s = time.monotonic()-started
        if diagnostico is not None:
            diagnostico.update(selecao_s=selecao_s, leitura_csv_s=total_s-selecao_s,
                               total_s=total_s, linhas=gravadas)
        etapa(f'Concluído · {gravadas:,} empresas em {total_s:.1f} s'.replace(',', '.'))
        return destino, gravadas
    finally:
        if arquivo is None:
            os.close(fd)
        parcial.unlink(missing_ok=True)
        if job_path is not None:
            job_path.unlink(missing_ok=True)



def connect(path=DATABASE):
    return closing(sqlite3.connect(Path(path).resolve().as_uri() + '?mode=ro', uri=True))


def abrir_catalogo(database):
    with connect(database) as con:
        row = con.execute('SELECT versao FROM sdr_portatil').fetchone()
        if not row or row[0] != PORTABLE_VERSION:
            raise ValueError('Este banco ainda não foi publicado como base portátil.')
        return json.loads(con.execute('SELECT payload FROM sdr_catalogo').fetchone()[0])


def contar_banco(database, filters):
    clause, params = where(filters)
    with connect(database) as con:
        return con.execute('SELECT count(*) FROM filtros_leads' + clause, params).fetchone()[0]

@st.cache_data(show_spinner=False, max_entries=8)
def catalogos(path, signature):
    return abrir_catalogo(path)


@st.cache_data(show_spinner=False, max_entries=128)
def buscar_pagina(path, signature, filters, columns, stats, cursor=None):
    return ler_pagina(Path(path), filters, columns, stats, cursor)


@st.cache_data(show_spinner=False, max_entries=128)
def contar(path, signature, filters):
    return contar_banco(Path(path), filters)


@st.cache_resource(show_spinner=False, max_entries=1)
def carregar_contagens(path, counts_signature, database_signature, database_path):
    return load_counts(path, database_signature, source=database_path)




def aplicar_identidade():
    """Interface interna escura, com amarelo reservado às ações e destaques."""
    st.markdown("""
    <style>
    :root { color-scheme:dark; }
    .stApp, [data-testid="stAppViewContainer"] { background:#080808; color:#ECECEC; }
    [data-testid="stHeader"] { background:#080808; }
    .block-container { max-width:1510px; padding:4.5rem 3rem 3rem; }
    [data-testid="stMain"] p, [data-testid="stMain"] label { color:#DEDEDE; }
    [data-testid="stMain"] [data-testid="stCaptionContainer"] p { color:#A6A6A6; line-height:1.6; }
    [data-testid="stSidebar"] { background:#101010; border-right:1px solid #292929; }
    [data-testid="stSidebar"] [data-testid="stSidebarContent"] { padding-top:.6rem; }
    [data-testid="stSidebar"] p, [data-testid="stSidebar"] label,
    [data-testid="stSidebar"] summary, [data-testid="stSidebar"] h2,
    [data-testid="stSidebar"] h3 { color:#E7E7E7; }
    [data-testid="stSidebar"] [data-testid="stCaptionContainer"] p { color:#A6A6A6; font-size:.77rem; }
    [data-testid="stSidebar"] [data-testid="stWidgetLabel"] p { font-size:.81rem; font-weight:550; }
    [data-testid="stExpander"] { border-color:#303030 !important; background:#141414 !important; border-radius:10px; }
    input, textarea { color:#ECECEC !important; -webkit-text-fill-color:#ECECEC !important; }
    [data-baseweb="select"] > div, [data-baseweb="input"] { background:#1B1B1B; border-color:#393939; border-radius:8px; }
    [data-baseweb="select"] span { color:#ECECEC; }
    [data-baseweb="popover"] > div, [data-baseweb="menu"] { background:#1B1B1B; color:#ECECEC; }
    [data-baseweb="tag"] { background:#F4C928 !important; border-radius:5px !important; }
    [data-baseweb="tag"] span { color:#171717 !important; }
    [data-baseweb="tag"] svg { fill:#171717 !important; }
    label[data-baseweb="checkbox"]:has(input:checked) > span:first-child { background:#F4C928; border-color:#F4C928; }
    [data-baseweb="select"] > div:focus-within, [data-baseweb="input"]:focus-within { border-color:#E0BA23 !important; }
    [data-testid="stButton"] button, [data-testid="stDownloadButton"] button,
    [data-testid="stLinkButton"] a { border-radius:8px; font-weight:650; min-height:2.7rem; }
    [data-testid="stButton"] button[kind="primary"],
    [data-testid="stDownloadButton"] button, [data-testid="stLinkButton"] a { background:#F4C928; color:#131313; border:1px solid #E0BA23; }
    [data-testid="stButton"] button[kind="primary"] p,
    [data-testid="stDownloadButton"] button p, [data-testid="stLinkButton"] a p { color:#131313; }
    [data-testid="stButton"] button[kind="primary"]:hover,
    [data-testid="stDownloadButton"] button:hover { background:#FFDA4B; border-color:#FFDA4B; }
    [data-testid="stButton"] button[kind="secondary"] { background:#191919; color:#ECECEC; border-color:#3C3C3C; }
    [data-testid="stButton"] button[kind="secondary"] p { color:#ECECEC; }
    [data-testid="stButton"] button[kind="secondary"]:hover { border-color:#E0BA23; background:#24221A; }
    [data-testid="stButton"] button:focus-visible,
    [data-testid="stDownloadButton"] button:focus-visible { outline:3px solid #E0BA23; outline-offset:3px; }
    [data-testid="stMetric"] { background:#141414; border:1px solid #2D2D2D; border-radius:10px; padding:1.1rem 1.3rem; }
    [data-testid="stMetricLabel"] p { color:#A6A6A6 !important; font-size:.82rem; }
    [data-testid="stMetricValue"] { color:#F4C928; font-size:2.2rem; font-weight:650; letter-spacing:-.04em; }
    [data-testid="stDataFrame"] { border:1px solid #303030; border-radius:9px; overflow:hidden; background:#141414; }
    [data-testid="stMain"] [data-testid="stVerticalBlockBorderWrapper"] > div { border-radius:10px; }
    .barsi-heading { border-bottom:1px solid #292929; padding:.35rem 0 1.25rem; margin-bottom:1rem; overflow:visible; }
    .barsi-heading h1 { color:#F0F0F0; font-size:1.8rem; font-weight:650; letter-spacing:-.035em; line-height:1.35; padding:.15rem 0; margin:0; overflow-wrap:anywhere; }
    .barsi-sidebrand { border-bottom:1px solid #292929; padding:.4rem 0 1rem; margin-bottom:1.2rem; }
    .barsi-sidebrand strong { color:#F4C928; font-size:.9rem; letter-spacing:.08em; }
    .barsi-sidebrand small { color:#9F9F9F; font-size:.68rem; margin-left:.65rem; }
    .barsi-filter-section { color:#E2E2E2; font-size:.73rem; font-weight:650; letter-spacing:.05em; margin:1rem 0 .15rem; }
    .barsi-filter-section span { color:#F4C928; font-size:.63rem; margin-right:.6rem; }
    .barsi-section { display:flex; align-items:center; justify-content:space-between; gap:1rem; margin:1.1rem 0 .5rem; }
    .barsi-section h2 { margin:0; padding:0; font-size:1.05rem; color:#ECECEC; font-weight:600; }
    .barsi-section small { color:#A6A6A6; font-size:.7rem; }
    .barsi-chips { display:flex; flex-wrap:wrap; gap:.4rem; margin:.3rem 0 .7rem; }
    .barsi-chip { background:#262316; border:1px solid #494025; color:#F0D777; padding:.25rem .6rem; border-radius:5px; font-size:.73rem; }
    .barsi-empty { background:#141414; border:1px solid #303030; border-radius:10px; padding:1.4rem; margin-top:1rem; }
    .barsi-empty b { display:block; color:#ECECEC; margin-bottom:.5rem; font-size:1rem; }
    .barsi-empty p { color:#ACACAC !important; font-size:.88rem; line-height:1.65; margin:0; }
    .barsi-export-kicker { color:#A6A6A6; font-size:.65rem; font-weight:650; letter-spacing:.08em; margin:0 0 .5rem; }
    .barsi-export-title { font-size:1.05rem; font-weight:600; color:#ECECEC; margin:0 0 .3rem; }
    @media(max-width:850px) { .block-container { padding:4.5rem 1.1rem 2rem; } .barsi-section { flex-wrap:wrap; } }
    </style>
    """, unsafe_allow_html=True)


def cabecalho():
    st.markdown('<header class="barsi-heading"><h1>Consulta de leads</h1></header>', unsafe_allow_html=True)


def secao(titulo, detalhe=''):
    st.markdown(f'<div class="barsi-section"><h2>{escape(titulo)}</h2>'
                f'<small>{escape(detalhe)}</small></div>', unsafe_allow_html=True)


def resumo_filtros(filters):
    labels = []
    for key in ('ufs', 'cidades', 'setores', 'portes', 'cnaes'):
        values = filters.get(key, [])
        shown = [PORTES.get(v, v) for v in values] if key == 'portes' else values
        labels.extend(shown[:3])
        if len(shown) > 3:
            labels.append(f'+{len(shown)-3} opções')
    if filters.get('busca'):
        labels.append('Busca: ' + filters['busca'])
    if filters.get('email'):
        labels.append('Com e-mail')
    if filters.get('usar_capital'):
        labels.append('Capital social')
    if not labels:
        labels = ['Toda a base · sem filtros']
    st.markdown('<div class="barsi-chips">' + ''.join(
        f'<span class="barsi-chip">{escape(str(label))}</span>' for label in labels) + '</div>',
        unsafe_allow_html=True)



def main():
    st.set_page_config(page_title='Consulta de leads | Grupo Barsi', page_icon=':material/query_stats:', layout='wide')
    aplicar_identidade()
    cabecalho()
    with st.sidebar:
        st.markdown('<div class="barsi-sidebrand"><strong>GRUPO BARSI</strong><small>SDR</small></div>', unsafe_allow_html=True)
    default = ROOT / 'base_sdr_pronta.sqlite3'
    hosted = os.environ.get('SDR_HOSTED') == '1'
    if hosted:
        path = os.environ.get('SDR_DB', str(default))
    else:
        with st.sidebar:
            with st.expander('Configuração da base', expanded=False):
                path = st.text_input('Banco pronto (.sqlite3)', value=os.environ.get('SDR_DB', str(default)))
    database = Path(path.strip().strip('"').strip("'")).expanduser()
    if not database.is_absolute():
        database = ROOT / database
    if not database.is_file():
        st.info('A base está indisponível. Peça ao responsável pela plataforma para verificar a configuração.'
                if hosted else 'Peça ao responsável o arquivo da base pronta e informe o caminho ao lado. Esta interface não executa preparação de dados.')
        return
    stat = database.stat()
    signature = (str(database.resolve()), stat.st_size, stat.st_mtime_ns)
    try:
        catalogo = catalogos(str(database), signature)
    except (OSError, ValueError, sqlite3.Error) as exc:
        st.error(f'Este arquivo não é uma base SDR pronta: {exc}')
        st.info('Peça ao responsável pela plataforma para verificar a base de consulta.' if hosted else
                'O responsável precisa concluir preparar_base_sdr.py. Não é necessário fazer isso em cada computador.')
        return
    if catalogo.get('amostra_linhas'):
        st.warning(f"Base de TESTE: primeiras {catalogo['amostra_linhas']:,} linhas da fonte. Não representa a base completa.")
    locais, cnaes, portes = catalogo['locais'], catalogo['cnaes'], catalogo['portes']
    stats = catalogo['stats']
    with st.sidebar:
        st.subheader('Filtros')
        st.caption('Combine filtros. Você pode escolher mais de uma opção em cada campo.')
        st.markdown('<div class="barsi-filter-section"><span>01</span> LOCALIZAÇÃO</div>', unsafe_allow_html=True)
        ufs = st.multiselect('Estado (UF)', sorted({uf for uf, _ in locais}), placeholder='Selecione os estados')
        cidades_disponiveis = sorted({city for uf, city in locais if city and (not ufs or uf in ufs)})
        # Limpa cidades que deixaram de pertencer aos estados selecionados.
        if 'cidades' in st.session_state:
            st.session_state['cidades'] = [c for c in st.session_state['cidades'] if c in cidades_disponiveis]
        cidades = st.multiselect('Município', cidades_disponiveis, key='cidades', placeholder='Selecione os municípios')
        st.markdown('<div class="barsi-filter-section"><span>02</span> ATIVIDADE</div>', unsafe_allow_html=True)
        grupos = {nome: {'titulo': nome, 'codigos': set()} for nome in SETORES}
        for codigo, descricao, setor in cnaes:
            grupos[setor]['codigos'].add(codigo)
        grupos_selecionados = st.multiselect(
            'Setor da empresa', list(grupos), key='setores_atividade', placeholder='Selecione os setores',
            format_func=lambda g: g + ' (' + numero_br(stats['setores'].get(g, 0)) + ' empresas)',
            help='As atividades foram organizadas em 15 grupos, como Financeiro, Educação e Tecnologia. Você pode selecionar vários e refinar por CNAE. Os números ao lado de cada setor mostram empresas antes dos demais filtros.')
        codigos_grupo = sorted({c for g in grupos_selecionados for c in grupos[g]['codigos']})
        permitidos = set(codigos_grupo) if grupos_selecionados else {c for c, _, _ in cnaes}
        labels = {code: f'{desc or "Sem descrição"} · {code}' for code, desc, _ in cnaes if code in permitidos}
        if 'atividades_especificas' in st.session_state:
            st.session_state['atividades_especificas'] = [
                c for c in st.session_state['atividades_especificas'] if c in permitidos]
        atividades_especificas = st.multiselect(
            'Refinar por atividade / CNAE (opcional)', list(labels), key='atividades_especificas', placeholder='Todas as atividades',
            format_func=lambda c: labels[c],
            help='Sem seleção, inclui todas as atividades dos setores escolhidos. Com seleção, inclui apenas os CNAEs escolhidos.')
        atividades = atividades_especificas
        st.markdown('<div class="barsi-filter-section"><span>03</span> PERFIL DA EMPRESA</div>', unsafe_allow_html=True)
        porte = st.multiselect('Porte da empresa', portes, placeholder='Selecione os portes', format_func=lambda p: PORTES.get(p, p or 'Não informado'))
        email = st.checkbox('Somente com e-mail preenchido')
        with st.expander('Mais refinamentos', expanded=False):
            busca = st.text_input('Buscar empresa ou CNPJ', help='No CNPJ, digite apenas números. A busca ignora acentos. Texto livre pode demorar.')
            usar_capital = st.checkbox('Filtrar capital social', help='Capital cadastral, não faturamento. Faixas livres podem demorar para contar.')
            minimum = st.number_input('Capital mínimo (R$)', min_value=0.0, value=0.0, step=10000.0, disabled=not usar_capital)
            maximum = st.number_input('Capital máximo (R$)', min_value=0.0, value=1000000.0, step=10000.0, disabled=not usar_capital)
        st.write('')
        aplicar = st.button('Aplicar filtros', type='primary', use_container_width=True)
        st.caption('Nenhuma opção selecionada? Incluímos todas. Aplique para atualizar a lista.')
    draft = dict(setores=grupos_selecionados, ufs=ufs, cidades=cidades, cnaes=atividades,
                 portes=porte, busca=busca.strip(), email=email, usar_capital=usar_capital,
                 capital_min=minimum, capital_max=maximum)
    if aplicar:
        if usar_capital and minimum > maximum:
            st.error('O capital mínimo deve ser menor ou igual ao máximo.')
            return
        st.session_state['aplicados'] = draft
        st.session_state['assinatura_aplicada'] = signature
        st.session_state['cursores'] = [None]
        st.session_state.pop('export_csv', None)
    if st.session_state.get('assinatura_aplicada') != signature or 'aplicados' not in st.session_state:
        secao('Base disponível')
        st.metric('Empresas na base de consulta', f"{catalogo['total']:,}".replace(',', '.'))
        st.markdown('<div class="barsi-empty"><b>Nenhuma consulta aplicada</b><p>Selecione os filtros na lateral e clique em <strong>Aplicar filtros</strong>.</p></div>', unsafe_allow_html=True)
        return
    if draft != st.session_state['aplicados']:
        st.info('Você alterou a seleção. Clique em Aplicar filtros para atualizar os resultados.')
        st.session_state.pop('export_csv', None)
        return
    filters = st.session_state['aplicados']
    total = catalogo['total']
    # Contagens de cruzamentos são carregadas uma vez, compartilhadas entre sessões.
    # Texto livre e faixas arbitrárias de capital requerem consulta à base original.
    count = contagem_salva(filters, catalogo)
    if count is None:
        if filters.get('busca') or filters.get('usar_capital'):
            st.info('Busca por nome/CNPJ e capital livre usam contagem direta; podem demorar. '
                    'Os filtros de setor, localização, CNAE, porte e e-mail usam os cruzamentos preparados.')
            with st.spinner('Contando resultados exatos da busca…'):
                count = contar(str(database), signature, filters)
        else:
            counts_path = database.parent / 'contagens_sdr.sqlite3'
            if not counts_path.is_file():
                st.error('As contagens estão indisponíveis. Peça ao responsável pela plataforma para verificar a configuração.'
                         if hosted else 'Falta contagens_sdr.sqlite3 junto do banco. Conclua preparar_contagens_sdr.py '
                         'para mostrar automaticamente o total exato dos cruzamentos.')
                return
            try:
                with st.spinner('Carregando contagens prontas para a memória (primeiro uso)…'):
                    count_data = carregar_contagens(str(counts_path), tuple(source_signature(counts_path)),
                                                    tuple(source_signature(database)), str(database))
                count = count_loaded(count_data, filters, SETOR_IDS)
            except (OSError, ValueError, sqlite3.Error) as exc:
                st.error(f'Não foi possível usar as contagens: {exc}')
                return
    secao('Resultados', 'FILTROS APLICADOS')
    resumo_filtros(filters)
    a, b, c = st.columns(3)
    a.metric('Empresas encontradas', f'{count:,}'.replace(',', '.'))
    b.metric('Empresas na base de consulta', f'{total:,}'.replace(',', '.'))
    c.metric('Filtros aplicados', str(sum(bool(filters.get(k)) for k in INDEX_KEYS) + int(bool(filters.get('busca'))) + int(bool(filters.get('email'))) + int(bool(filters.get('usar_capital')))))
    secao('Empresas selecionadas', '100 EMPRESAS POR PÁGINA')
    with st.expander('Personalizar colunas da lista e do arquivo', expanded=False):
        columns = st.multiselect('Colunas da lista e do download', EXPORT_COLUMNS, default=DEFAULT_COLUMNS, format_func=lambda c: COLUMN_LABELS.get(c, c.replace('_', ' ').capitalize()))
    if not columns:
        st.info('Selecione ao menos uma coluna.')
        return
    pages = st.session_state.setdefault('cursores', [None])
    with st.spinner('Buscando a página pelo índice…'):
        rows, next_cursor = buscar_pagina(str(database), signature, filters, columns, stats, pages[-1])
    if not rows:
        st.info('Nenhuma empresa encontrada nesta página. Amplie os filtros ou volte à página anterior.')
    else:
        st.dataframe(pd.DataFrame(rows, columns=columns), hide_index=True, width='stretch',
                     column_config={c: st.column_config.TextColumn(COLUMN_LABELS.get(c, c.replace('_', ' ').capitalize())) for c in columns})
    st.caption(f'Página {len(pages)} · {len(rows)} empresas nesta página. O total acima inclui todos os resultados.')
    left, right = st.columns(2)
    if left.button('Página anterior', disabled=len(pages) == 1):
        pages.pop()
        st.rerun()
    if right.button('Próxima página', disabled=next_cursor is None):
        pages.append(next_cursor)
        st.rerun()
    if not rows and count == 0:
        return
    token = json.dumps([signature, filters, columns], sort_keys=True)
    if st.session_state.get('export_token') != token:
        st.session_state.pop('export_csv', None)
    if st.session_state.get('export_full_token') != token:
        st.session_state.pop('export_full_path', None)
    secao('Exportar resultados', 'CSV')
    st.caption('As duas opções usam os filtros aplicados e as colunas selecionadas. '
               'A primeira opção exporta até 100 mil empresas; não é uma amostra aleatória. '
               'A ordem do CSV pode ser diferente da prévia.')
    rapido, completo = st.columns(2)
    with rapido:
        with st.container(border=True):
            st.markdown('<div class="barsi-export-kicker">EXPORTAÇÃO LIMITADA</div><div class="barsi-export-title">Até 100 mil empresas</div>', unsafe_allow_html=True)
            st.caption('Exporta os primeiros 100 mil resultados, ou todos se houver menos.')
            gerar_rapida = st.button('Gerar base com até 100.000 CNPJs', disabled=count == 0, use_container_width=True)
    with completo:
        with st.container(border=True):
            st.markdown('<div class="barsi-export-kicker">EXPORTAÇÃO COMPLETA</div><div class="barsi-export-title">Todos os resultados</div>', unsafe_allow_html=True)
            st.caption('Todos os resultados dos filtros aplicados, sem limite de CNPJs.')
            gerar_completa = st.button('Gerar base completa', disabled=count == 0, type='primary', use_container_width=True)
    st.caption('Após gerar, baixe o CSV pelo botão abaixo. A base completa inclui todos os '
               'resultados filtrados. A geração pode demorar; mantenha a página aberta até concluir.'
               if hosted else 'A base completa será salva na pasta exportacoes_sdr, junto do banco, '
               'sem limite de empresas. A geração pode demorar; mantenha o app aberto até concluir.')
    if gerar_rapida or gerar_completa:
        esperado = min(count, EXPORT_LIMIT) if gerar_rapida else count
        progresso = st.progress(0.0, text='Iniciando exportação…')
        status_exportacao = st.empty()
        diagnostico = {}
        def atualizar(n):
            progresso.progress(min(n / max(esperado, 1), 1.0),
                               text=f"{n:,} de {esperado:,} empresas gravadas".replace(',', '.'))
        try:
            with st.spinner('Gerando CSV…'):
                destino, gravadas = exportar_csv_hd(
                    database, filters, columns, stats, count, atualizar,
                    limite=EXPORT_LIMIT if gerar_rapida else None,
                    etapa=status_exportacao.info, diagnostico=diagnostico)
                st.session_state['diagnostico_exportacao'] = diagnostico
                st.session_state['diagnostico_token'] = token
                if gerar_rapida:
                    if not hosted:
                        st.session_state['export_csv'] = destino.read_bytes()
                    st.session_state['export_path'] = str(destino)
                    st.session_state['export_token'] = token
                    st.session_state['export_rows'] = gravadas
                else:
                    st.session_state['export_full_path'] = str(destino)
                    st.session_state['export_full_rows'] = gravadas
                    st.session_state['export_full_token'] = token
        except (OSError, ValueError, sqlite3.Error) as exc:
            st.error(f'Não foi possível concluir a exportação: {exc}')
        finally:
            progresso.empty()
            status_exportacao.empty()
    if st.session_state.get('diagnostico_token') == token:
        tempos = st.session_state['diagnostico_exportacao']
        st.caption(f"Última geração: {numero_br(tempos['linhas'])} empresas · "
                   f"seleção {tempos['selecao_s']:.1f} s · "
                   f"leitura e CSV {tempos['leitura_csv_s']:.1f} s · "
                   f"total {tempos['total_s']:.1f} s")
    if st.session_state.get('export_csv') and st.session_state.get('export_token') == token:
        n = st.session_state['export_rows']
        st.download_button(f"Baixar CSV com {n:,} empresas".replace(',', '.'),
                           data=st.session_state['export_csv'],
                           file_name=f'leads_ate_100mil_{datetime.now():%Y%m%d_%H%M}.csv', mime='text/csv')
    if hosted and st.session_state.get('export_path') and st.session_state.get('export_token') == token:
        n = st.session_state['export_rows']
        nome = Path(st.session_state['export_path']).name
        st.link_button(f"Baixar CSV com {n:,} empresas".replace(',', '.'),
                       '/downloads/' + quote(nome, safe=''), type='primary')
    if st.session_state.get('export_full_path') and st.session_state.get('export_full_token') == token:
        n = st.session_state['export_full_rows']
        st.success(f"Base completa pronta: {n:,} empresas.".replace(',', '.'))
        if hosted:
            nome = Path(st.session_state['export_full_path']).name
            st.link_button('Baixar CSV completo', '/downloads/' + quote(nome, safe=''), type='primary')
        else:
            st.text(st.session_state['export_full_path'])
            st.caption('O arquivo já está salvo no computador que executa a plataforma. '
                       'No seu uso local, abra a pasta exportacoes_sdr no Finder.')
    st.caption('No Excel, importe CNPJ, CEP e telefones como Texto para preservar os zeros iniciais. '
               'Empresas com telefone aprovado por formato; contatos não confirmados.')


if __name__ == '__main__':
    main()
