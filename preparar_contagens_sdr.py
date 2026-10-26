"""Prepara contagens exatas dos filtros usando o banco pronto (sem reler CSV).

Executar: python -u preparar_contagens_sdr.py
O arquivo contagens_sdr.sqlite3 deve acompanhar base_sdr_pronta.sqlite3.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import tempfile
import time

VERSION = 1
DIMENSIONS = ('setor_id', 'uf', 'nome_municipio', 'porte_empresa',
              'cnae_fiscal_principal', 'tem_email')
FILTER_COLUMNS = {'setores': 'setor_id', 'ufs': 'uf', 'cidades': 'nome_municipio',
                  'portes': 'porte_empresa', 'cnaes': 'cnae_fiscal_principal'}


def source_signature(path):
    stat = Path(path).stat()
    # Assinatura de cache local; não identifica uma versão entre computadores.
    return [stat.st_size, stat.st_mtime_ns]


def portable_identity(path):
    """Impressão estável do catálogo e de amostras do arquivo, sem ler toda a base.

    Não é um hash integral para detectar qualquer corrupção. Identifica o par de
    arquivos para transporte, ignorando nome, caminho e data da cópia.
    """
    import sqlite3
    path = Path(path).resolve()
    signature = source_signature(path)
    with sqlite3.connect(path.as_uri() + '?mode=ro', uri=True) as con:
        catalog = con.execute('SELECT payload FROM sdr_catalogo').fetchone()[0]
        version = con.execute('SELECT versao FROM sdr_portatil').fetchone()[0]
    digest = hashlib.sha256()
    digest.update(json.dumps([signature[0], version, catalog], ensure_ascii=False).encode('utf-8'))
    block_size = 8192
    last_offset = max(0, signature[0] - block_size)
    with path.open('rb') as source:
        for number in range(16):
            offset = last_offset * number // 15
            source.seek(offset)
            digest.update(offset.to_bytes(8, 'big'))
            digest.update(source.read(block_size))
    if source_signature(path) != signature:
        raise ValueError('O banco mudou durante a validação. Pare as atualizações e tente novamente.')
    return dict(metodo='catalogo-e-amostras-v1', tamanho=signature[0], sha256=digest.hexdigest())


def register_portability(source, target):
    """Atualiza só os metadados das contagens no computador que preparou o par."""
    import sqlite3
    source, target = Path(source).resolve(), Path(target).resolve()
    if source == target:
        raise ValueError('Informe o banco principal e o banco de contagens como arquivos diferentes.')
    metadata = read_metadata(target)
    if metadata.get('versao') != VERSION:
        raise ValueError('Versão do banco de contagens incompatível com este código.')
    identity = portable_identity(source)
    if metadata.get('identidade_fonte'):
        if metadata['identidade_fonte'] != identity:
            raise ValueError('Este banco principal não corresponde à identidade registrada nas contagens.')
        print('Portabilidade já registrada. Este par pode ser copiado junto.', flush=True)
        return
    if metadata.get('fonte') != source_signature(source):
        raise ValueError('Faça este registro no computador que preparou os arquivos, usando o par original. '
                         'A cópia antiga não contém informação suficiente para confirmar a origem.')
    with sqlite3.connect(source.as_uri() + '?mode=ro', uri=True) as con:
        catalog = json.loads(con.execute('SELECT payload FROM sdr_catalogo').fetchone()[0])
    if metadata.get('total') != catalog['total'] or metadata.get('amostra_linhas') != catalog.get('amostra_linhas'):
        raise ValueError('Os bancos não têm o mesmo total ou não pertencem à mesma amostra.')
    metadata['identidade_fonte'] = identity
    with sqlite3.connect(target.as_uri() + '?mode=rw', uri=True) as con:
        con.execute('UPDATE metadados SET payload=?', (json.dumps(metadata),))
    print('PRONTO: identificação portátil registrada nas contagens. Nenhum índice ou total foi recalculado.', flush=True)
    print('O banco principal não foi alterado. Reenvie apenas contagens_sdr.sqlite3.', flush=True)


def read_metadata(path):
    import sqlite3
    with sqlite3.connect(Path(path).resolve().as_uri() + '?mode=ro', uri=True) as con:
        return json.loads(con.execute('SELECT payload FROM metadados').fetchone()[0])


def prepare(source, target):
    source, target = Path(source).resolve(), Path(target).resolve()
    if source == target:
        raise ValueError('O arquivo de contagens precisa ser diferente do banco original.')
    signature = source_signature(source)
    if target.exists():
        metadata = read_metadata(target)
        if metadata.get('versao') == VERSION and metadata.get('fonte') == signature:
            print(f'Contagens já prontas: {target}', flush=True)
            return
        raise ValueError('Já existe outro arquivo de contagens nesse destino. Use --saida com outro nome.')
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary_dir = target.parent / 'temporarios_contagens_sdr'
    temporary_dir.mkdir(exist_ok=True)
    # Definidos antes de importar SQLite, para manter os temporários neste disco.
    os.environ['SQLITE_TMPDIR'] = str(temporary_dir)
    os.environ['TMPDIR'] = str(temporary_dir)
    import sqlite3
    started = time.monotonic()
    last_report = started
    print(f'Banco de origem (somente leitura): {source}', flush=True)
    print(f'Contagens: {target}', flush=True)
    print(f'Temporários: {temporary_dir}', flush=True)
    fd, name = tempfile.mkstemp(prefix='contagens_', suffix='.partial', dir=target.parent)
    os.close(fd)
    partial = Path(name)
    con = None
    try:
        con = sqlite3.connect(partial, uri=True)
        # Apenas este arquivo novo e descartável usa journal OFF. Fonte é read-only.
        con.execute('PRAGMA journal_mode=OFF')
        con.execute('PRAGMA synchronous=OFF')
        con.execute('PRAGMA temp_store=FILE')
        con.execute('PRAGMA cache_size=-131072')
        con.execute('ATTACH DATABASE ? AS origem', (source.as_uri() + '?mode=ro',))
        if con.execute('SELECT versao FROM origem.sdr_portatil').fetchone() != (1,):
            raise ValueError('A preparação do banco original ainda não está concluída.')
        catalog = json.loads(con.execute('SELECT payload FROM origem.sdr_catalogo').fetchone()[0])
        def progress():
            nonlocal last_report
            now = time.monotonic()
            if now - last_report >= 10:
                print(f'Preparando cruzamentos… {(now-started)/60:.1f} min. Aguarde PRONTO.', flush=True)
                last_report = now
            return 0
        con.set_progress_handler(progress, 100000)
        con.execute('''CREATE TABLE contagens (
            setor_id INTEGER NOT NULL, uf TEXT NOT NULL, nome_municipio TEXT NOT NULL,
            porte_empresa TEXT NOT NULL, cnae_fiscal_principal TEXT NOT NULL,
            tem_email INTEGER NOT NULL, quantidade INTEGER NOT NULL)''')
        columns = ','.join(DIMENSIONS)
        print('Lendo os filtros já preparados e agrupando combinações existentes…', flush=True)
        con.execute(f'INSERT INTO contagens SELECT {columns},count(*) '
                    f'FROM origem.filtros_leads NOT INDEXED GROUP BY {columns}')
        groups, total = con.execute('SELECT count(*),coalesce(sum(quantidade),0) FROM contagens').fetchone()
        if total != catalog['total']:
            raise ValueError(f'Total divergente: contagens={total}; catálogo={catalog["total"]}.')
        if source_signature(source) != signature:
            raise ValueError('O banco original mudou durante a preparação. Execute novamente com ele parado.')
        metadata = dict(versao=VERSION, fonte=signature, total=total, grupos=groups,
                        amostra_linhas=catalog.get('amostra_linhas'),
                        identidade_fonte=portable_identity(source))
        con.execute('CREATE TABLE metadados(payload TEXT NOT NULL)')
        con.execute('INSERT INTO metadados VALUES (?)', (json.dumps(metadata),))
        con.commit()
        con.close()
        con = None
        os.replace(partial, target)
        print(f'PRONTO: {total:,} empresas em {groups:,} combinações.', flush=True)
        print(f'Tempo: {(time.monotonic()-started)/60:.1f} min. Arquivo: {target.stat().st_size/1e6:.1f} MB.', flush=True)
        print('Abra o app atualizado. Guarde este arquivo junto do banco original.', flush=True)
    finally:
        if con is not None:
            con.close()
        partial.unlink(missing_ok=True)


def load_counts(path, signature, source=None):
    """Carrega cruzamentos compactos em memória uma vez, via cache do Streamlit."""
    import sqlite3
    import numpy as np
    metadata = read_metadata(path)
    if metadata.get('versao') != VERSION:
        raise ValueError('Versão do arquivo de contagens incompatível com o código.')
    identity = metadata.get('identidade_fonte')
    if identity and source is not None:
        if identity != portable_identity(source):
            raise ValueError('Os dois arquivos não correspondem. Use a base e as contagens do mesmo conjunto.')
    elif metadata.get('fonte') != list(signature):
        if identity:
            raise ValueError('Atualize app_sdr.py para validar a identificação portátil deste banco.')
        raise ValueError('Este arquivo de contagens usa a validação antiga por data. '
                         'O responsável deve registrar a portabilidade no computador original '
                         'com --registrar-portabilidade e reenviar apenas as contagens. '
                         'Não é necessário refazer a preparação.')
    arrays = {column: np.empty(metadata['grupos'], dtype=np.int32) for column in DIMENSIONS}
    counts = np.empty(metadata['grupos'], dtype=np.int64)
    dictionaries = {column: {} for column in DIMENSIONS}
    con = sqlite3.connect(Path(path).resolve().as_uri() + '?mode=ro', uri=True)
    position = 0
    try:
        cursor = con.execute('SELECT ' + ','.join(DIMENSIONS) + ',quantidade FROM contagens')
        while True:
            rows = cursor.fetchmany(25000)
            if not rows:
                break
            end = position + len(rows)
            for index, column in enumerate(DIMENSIONS):
                mapping = dictionaries[column]
                arrays[column][position:end] = [mapping.setdefault(row[index], len(mapping)) for row in rows]
            counts[position:end] = [row[-1] for row in rows]
            position = end
    finally:
        con.close()
    if position != metadata['grupos'] or int(counts.sum()) != metadata['total']:
        raise ValueError('Arquivo de contagens incompleto ou inconsistente.')
    return arrays, dictionaries, counts


def count_loaded(data, filters, sector_ids):
    import numpy as np
    arrays, dictionaries, counts = data
    mask = None
    selected = [(column, [sector_ids[v] for v in filters[key]] if key == 'setores' else filters[key])
                for key, column in FILTER_COLUMNS.items() if filters.get(key)]
    if filters.get('email'):
        selected.append(('tem_email', [1]))
    for column, values in selected:
        codes = [dictionaries[column][v] for v in values if v in dictionaries[column]]
        if not codes:
            return 0
        part = np.isin(arrays[column], codes)
        mask = part if mask is None else mask & part
    return int(counts.sum() if mask is None else counts[mask].sum())


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    root = Path(__file__).resolve().parent
    parser.add_argument('--banco', type=Path, default=root / 'base_sdr_pronta.sqlite3')
    parser.add_argument('--saida', type=Path, default=root / 'contagens_sdr.sqlite3')
    parser.add_argument('--registrar-portabilidade', action='store_true',
                        help='Atualiza apenas os metadados do par original, sem recalcular contagens.')
    args = parser.parse_args()
    try:
        if args.registrar_portabilidade:
            register_portability(args.banco, args.saida)
        else:
            prepare(args.banco, args.saida)
    except KeyboardInterrupt:
        print('\nPreparação interrompida. O banco original permanece intacto. Repita para reiniciar.', flush=True)
        raise SystemExit(130)
    except Exception as exc:
        parser.exit(1, f'Não foi possível preparar as contagens: {exc}\n')
