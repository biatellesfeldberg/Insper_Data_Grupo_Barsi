"""Consulta de leads — executar: python -m streamlit run app_sdr.py

Dependência: python -m pip install 'streamlit>=1.50,<2'
Coloque junto de pipeline.py e base_final.csv.gz. Não modifica a fonte.
A primeira preparação lê a fonte em lotes e cria leads_sdr.sqlite3 (reserve
espaço para a base descompactada e índices). Atualizações exigem nova preparação.
Uso local/interno; autenticação e hospedagem não estão incluídas.
"""
from __future__ import annotations

import csv
import gzip
import io
import json
import math
import os
import sqlite3
import tempfile
import unicodedata
from datetime import datetime
from pathlib import Path

import streamlit as st

from pipeline import COLUNAS_FINAIS, NATUREZAS_EXCLUIDAS, telefone_util

ROOT = Path(__file__).resolve().parent
SOURCE = ROOT / 'base_final.csv.gz'
DATABASE = ROOT / 'leads_sdr.sqlite3'
VERSION = 1
EXPORT_LIMIT = 100_000
PORTES = {'00': 'Não informado', '01': 'Microempresa', '03': 'Empresa de pequeno porte',
          '05': 'Demais empresas'}
EXPORT_COLUMNS = COLUNAS_FINAIS + ['contato_1', 'contato_2']
DEFAULT_COLUMNS = ['cnpj', 'razao_social', 'nome_fantasia', 'descricao_cnae',
                   'uf', 'nome_municipio', 'porte_empresa', 'capital_social',
                   'contato_1', 'contato_2', 'correio_eletronico']


def normalizar(texto):
    return ''.join(c for c in unicodedata.normalize('NFKD', str(texto or '')).casefold()
                   if not unicodedata.combining(c))


def fingerprint(path):
    stat = path.stat()
    return json.dumps([str(path.resolve()), stat.st_size, stat.st_mtime_ns, VERSION])


def connect(path=DATABASE):
    return sqlite3.connect(path.resolve().as_uri() + '?mode=ro', uri=True)


def pronta(source=SOURCE, database=DATABASE):
    if not source.exists() or not database.exists():
        return False
    try:
        with connect(database) as con:
            return con.execute('SELECT assinatura FROM metadados').fetchone()[0] == fingerprint(source)
    except (sqlite3.Error, TypeError):
        return False


def preparar(source=SOURCE, database=DATABASE, progresso=lambda n: None):
    """Importação atômica e em lotes; fonte e banco anterior permanecem intactos em erro."""
    assinatura = fingerprint(source)
    fd, name = tempfile.mkstemp(prefix='sdr_', suffix='.sqlite3', dir=database.parent)
    os.close(fd)
    temp = Path(name)
    lidas = mantidas = 0
    try:
        with sqlite3.connect(temp) as con:
            columns = ', '.join(f'"{c}" TEXT' for c in EXPORT_COLUMNS)
            con.execute(f'CREATE TABLE leads ({columns}, capital_num REAL, busca TEXT)')
            con.execute('CREATE UNIQUE INDEX idx_cnpj ON leads(cnpj)')
            sql = 'INSERT OR IGNORE INTO leads VALUES (' + ','.join('?' for _ in range(len(EXPORT_COLUMNS)+2)) + ')'
            opener = gzip.open if source.suffix == '.gz' else open
            with opener(source, 'rt', encoding='latin-1', newline='') as f:
                reader = csv.DictReader(f, delimiter=';')
                missing = set(COLUNAS_FINAIS) - set(reader.fieldnames or [])
                if missing:
                    raise ValueError('Colunas ausentes: ' + ', '.join(sorted(missing)))
                batch = []
                for row in reader:
                    lidas += 1
                    row = {k: (row.get(k) or '').strip() for k in COLUNAS_FINAIS}
                    if (row['situacao_cadastral'] != '02' or row['identificador_matriz_filial'] != '1'
                            or not row['razao_social'] or not row['natureza_juridica']
                            or row['natureza_juridica'] in NATUREZAS_EXCLUIDAS or not row['uf']
                            or len(row['cnpj']) != 14 or not row['cnpj'].isascii() or not row['cnpj'].isdigit()):
                        continue
                    contacts = []
                    for n in (1, 2):
                        ddd = ''.join(c for c in row[f'ddd_{n}'] if c.isdigit())
                        tel = ''.join(c for c in row[f'telefone_{n}'] if c.isdigit())
                        contato = ddd + tel
                        if telefone_util(ddd, tel) and contato not in contacts:
                            row[f'ddd_{n}'], row[f'telefone_{n}'] = ddd, tel
                            row[f'contato_{n}'] = contato
                            contacts.append(contato)
                        else:
                            row[f'ddd_{n}'] = row[f'telefone_{n}'] = row[f'contato_{n}'] = ''
                    if not contacts:
                        continue
                    try:
                        capital = float(row['capital_social'].replace(',', '.'))
                        if not math.isfinite(capital) or capital < 0:
                            capital = None
                    except ValueError:
                        capital = None
                    busca = normalizar(' '.join(row[c] for c in ('cnpj', 'razao_social', 'nome_fantasia')))
                    batch.append([row[c] for c in EXPORT_COLUMNS] + [capital, busca])
                    if len(batch) >= 5000:
                        con.executemany(sql, batch)
                        con.commit()
                        batch.clear()
                        progresso(lidas)
                if batch:
                    con.executemany(sql, batch)
            progresso(lidas)
            for nome, campos in [('local', 'uf,nome_municipio'), ('cnae', 'cnae_fiscal_principal'), ('porte', 'porte_empresa')]:
                con.execute(f'CREATE INDEX idx_{nome} ON leads({campos})')
            con.execute('CREATE TABLE metadados (assinatura TEXT, lidas INTEGER, mantidas INTEGER)')
            mantidas = con.execute('SELECT count(*) FROM leads').fetchone()[0]
            con.execute('INSERT INTO metadados VALUES (?,?,?)', (assinatura, lidas, mantidas))
            con.commit()
        if assinatura != fingerprint(source):
            raise ValueError('A fonte mudou durante a importação. Prepare novamente após concluir o pipeline.')
        os.replace(temp, database)
        return lidas, mantidas
    finally:
        temp.unlink(missing_ok=True)


def where(filters):
    clauses, params = [], []
    for key, column in [('ufs', 'uf'), ('cidades', 'nome_municipio'),
                        ('cnaes', 'cnae_fiscal_principal'), ('portes', 'porte_empresa')]:
        values = filters.get(key, [])
        if values:
            clauses.append(column + ' IN (' + ','.join('?' for _ in values) + ')')
            params.extend(values)
    if filters.get('busca'):
        clauses.append('instr(busca, ?) > 0')
        params.append(normalizar(filters['busca']))
    if filters.get('email'):
        clauses.append("correio_eletronico <> ''")
    if filters.get('usar_capital'):
        clauses.append('capital_num BETWEEN ? AND ?')
        params.extend([filters['capital_min'], filters['capital_max']])
    return (' WHERE ' + ' AND '.join(clauses)) if clauses else '', params


@st.cache_data(show_spinner=False)
def catalogos(database_path, signature):
    with connect(Path(database_path)) as con:
        locais = con.execute("SELECT DISTINCT uf,nome_municipio FROM leads ORDER BY 1,2").fetchall()
        cnaes = con.execute('SELECT DISTINCT cnae_fiscal_principal,descricao_cnae FROM leads ORDER BY 2,1').fetchall()
        portes = [r[0] for r in con.execute('SELECT DISTINCT porte_empresa FROM leads ORDER BY 1')]
    return locais, cnaes, portes


def csv_bytes(rows, columns):
    output = io.StringIO(newline='')
    writer = csv.writer(output, delimiter=';')
    writer.writerow(columns)
    for row in rows:
        # Protege células textuais contra fórmulas ao abrir em planilhas.
        writer.writerow(["'" + str(v) if str(v).lstrip().startswith(('=', '+', '-', '@')) else v for v in row])
    return output.getvalue().encode('utf-8-sig')


def main():
    st.set_page_config(page_title='Leads | Grupo Barsi', page_icon='🔎', layout='wide')
    st.title('Encontre sua próxima lista de leads')
    st.caption('Grupo Barsi · Insper Data | Seleção de empresas para prospecção')
    if not SOURCE.exists():
        st.info('Gere base_final.csv.gz com pipeline.py e coloque os três arquivos na mesma pasta.')
        st.code('python pipeline.py\npython -m streamlit run app_sdr.py', language='bash')
        return
    if not pronta():
        st.info('Prepare a base de consulta. Na primeira vez, a leitura pode demorar e precisa de espaço em disco para os dados e índices. O CSV original será preservado.')
        if st.button('Preparar / atualizar base', type='primary'):
            status = st.empty()
            try:
                with st.spinner('Preparando base de consulta…'):
                    preparar(progresso=lambda n: status.write(f'{n:,} registros lidos…'))
                st.rerun()
            except (OSError, ValueError, sqlite3.Error, csv.Error) as exc:
                st.error(f'Não foi possível preparar a base: {exc}')
        return
    signature = fingerprint(SOURCE)
    locais, cnaes, portes = catalogos(str(DATABASE), signature)
    with st.sidebar:
        st.header('Monte sua lista')
        st.caption('Filtros vazios incluem todas as opções. Diferentes filtros se combinam.')
        ufs = st.multiselect('Estado (UF)', sorted({uf for uf, _ in locais}))
        cidades_disponiveis = sorted({city for uf, city in locais if city and (not ufs or uf in ufs)})
        # Limpa cidades que deixaram de pertencer aos estados selecionados.
        if 'cidades' in st.session_state:
            st.session_state['cidades'] = [c for c in st.session_state['cidades'] if c in cidades_disponiveis]
        cidades = st.multiselect('Município', cidades_disponiveis, key='cidades')
        labels = {code: f'{desc or "Sem descrição"} · {code}' for code, desc in cnaes}
        atividades = st.multiselect('Profissão / atividade (CNAE principal)', list(labels), format_func=lambda c: labels[c])
        st.caption('Ex.: digite “médica”, “advocacia” ou “engenharia”. O CNAE descreve a empresa; não confirma a profissão de uma pessoa.')
        porte = st.multiselect('Porte da empresa', portes, format_func=lambda p: PORTES.get(p, p or 'Não informado'))
        busca = st.text_input('Buscar empresa ou CNPJ', help='No CNPJ, digite apenas números. A busca ignora acentos e maiúsculas.')
        email = st.checkbox('Somente com e-mail preenchido')
        usar_capital = st.checkbox('Filtrar capital social')
        minimum = st.number_input('Capital mínimo (R$)', min_value=0.0, value=0.0, step=10000.0, disabled=not usar_capital)
        maximum = st.number_input('Capital máximo (R$)', min_value=0.0, value=1000000.0, step=10000.0, disabled=not usar_capital)
        st.caption('Capital social é cadastral; não representa faturamento ou patrimônio do proprietário.')
    if usar_capital and minimum > maximum:
        st.error('O capital mínimo deve ser menor ou igual ao máximo.')
        return
    filters = dict(ufs=ufs, cidades=cidades, cnaes=atividades, portes=porte, busca=busca.strip(),
                   email=email, usar_capital=usar_capital, capital_min=minimum, capital_max=maximum)
    clause, params = where(filters)
    with connect() as con:
        total = con.execute('SELECT count(*) FROM leads').fetchone()[0]
        count = con.execute('SELECT count(*) FROM leads' + clause, params).fetchone()[0]
        a, b = st.columns(2)
        a.metric('Empresas encontradas', f'{count:,}'.replace(',', '.'))
        b.metric('Empresas na base de consulta', f'{total:,}'.replace(',', '.'))
        st.caption('Somente matrizes ativas, com dados empresariais e ao menos um telefone aprovado por formato. Telefones e e-mails não foram confirmados em contato.')
        columns = st.multiselect('Colunas da lista e do download', EXPORT_COLUMNS, default=DEFAULT_COLUMNS)
        if not columns:
            st.info('Selecione ao menos uma coluna.')
            return
        if not count:
            st.info('Nenhuma empresa encontrada. Amplie os filtros.')
            return
        page = int(st.number_input('Página (100 empresas por página)', min_value=1, max_value=max(1, math.ceil(count/100)), value=1))
        fields = ','.join(f'"{c}"' for c in columns)
        query = f'SELECT {fields} FROM leads' + clause + ' ORDER BY cnpj'
        preview = con.execute(query + ' LIMIT 100 OFFSET ?', params + [(page-1)*100]).fetchall()
        import pandas as pd
        st.dataframe(pd.DataFrame(preview, columns=columns), hide_index=True, width='stretch')
        if count > EXPORT_LIMIT:
            st.info(f'A exportação aceita até {EXPORT_LIMIT:,} empresas por arquivo. Refine os filtros; nenhum resultado será cortado silenciosamente.')
        token = json.dumps([signature, filters, columns], sort_keys=True)
        if st.session_state.get('export_token') != token:
            st.session_state.pop('export_csv', None)
        if st.button('Gerar CSV da lista completa', disabled=count > EXPORT_LIMIT, type='primary'):
            with st.spinner('Gerando arquivo com todas as empresas filtradas…'):
                st.session_state['export_csv'] = csv_bytes(con.execute(query, params), columns)
                st.session_state['export_token'] = token
        if st.session_state.get('export_csv') and st.session_state.get('export_token') == token:
            st.download_button('Baixar base filtrada (.csv)', data=st.session_state['export_csv'],
                               file_name=f'leads_{datetime.now():%Y%m%d_%H%M}.csv', mime='text/csv')
            st.caption('CSV UTF-8 com separador ponto e vírgula. No Excel, importe CNPJ, CEP e telefones como Texto para preservar zeros à esquerda. O layout da 3C ainda precisa ser validado.')


if __name__ == '__main__':
    main()
