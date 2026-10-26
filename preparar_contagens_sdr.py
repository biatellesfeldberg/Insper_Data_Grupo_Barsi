"""Prepara contagens exatas dos filtros usando o banco pronto (sem reler CSV).

Executar: python -u preparar_contagens_sdr.py
O arquivo contagens_sdr.sqlite3 deve acompanhar base_sdr_pronta.sqlite3.
"""
from __future__ import annotations

import argparse
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
    # Não inclui caminho: banco e contagens podem ser transportados juntos.
    return [stat.st_size, stat.st_mtime_ns]


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
                        amostra_linhas=catalog.get('amostra_linhas'))
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


def load_counts(path, signature):
    """Carrega cruzamentos compactos em memória uma vez, via cache do Streamlit."""
    import sqlite3
    import numpy as np
    metadata = read_metadata(path)
    if metadata.get('versao') != VERSION or metadata.get('fonte') != list(signature):
        raise ValueError('As contagens não correspondem à versão deste banco. Prepare-as novamente.')
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
    args = parser.parse_args()
    try:
        prepare(args.banco, args.saida)
    except KeyboardInterrupt:
        print('\nPreparação interrompida. O banco original permanece intacto. Repita para reiniciar.', flush=True)
        raise SystemExit(130)
    except Exception as exc:
        parser.exit(1, f'Não foi possível preparar as contagens: {exc}\n')
