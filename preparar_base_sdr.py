from __future__ import annotations

import argparse
import csv
import gzip
import json
import math
import os
import shutil
import sys
import unicodedata

from pathlib import Path
from time import perf_counter

from pipeline import (
    COLUNAS_FINAIS,
    NATUREZAS_EXCLUIDAS,
    telefone_util,
)

DATA_COLUMNS = COLUNAS_FINAIS + ["contato_1", "contato_2"]
PORTABLE_VERSION = 1

SETORES = {
    "Agronegócio": set(range(1, 4)),
    "Indústria e extração": set(range(5, 34)),
    "Energia, água e saneamento": set(range(35, 40)),
    "Construção": set(range(41, 44)),
    "Comércio e reparação de veículos": set(range(45, 48)),
    "Transporte e logística": set(range(49, 54)),
    "Alimentação e hospedagem": {55, 56},
    "Tecnologia, comunicação e informação": set(range(58, 64)),
    "Financeiro e seguros": {64, 65, 66},
    "Imobiliário": {68},
    "Serviços profissionais e empresariais": (
        set(range(69, 76)) | set(range(77, 83))
    ),
    "Educação": {85},
    "Saúde e assistência social": {86, 87, 88},
    "Cultura, lazer e serviços pessoais": (
        set(range(90, 94)) | {95, 96}
    ),
    "Outros / sem classificação": {84, 94, 97, 99},
}

SETOR_IDS = {
    nome: i + 1
    for i, nome in enumerate(SETORES)
}

NOMES_SETORES = {
    identificador: nome
    for nome, identificador in SETOR_IDS.items()
}

INDEX_KEYS = {
    "setores": ("setor_id", "lead_id"),
    "ufs": ("uf", "nome_municipio", "lead_id"),
    "cidades": ("nome_municipio", "lead_id"),
    "portes": ("porte_empresa", "lead_id"),
    "cnaes": ("cnae_fiscal_principal", "lead_id"),
}

INDEX_NAMES = {
    "setores": "idx_f_setor",
    "ufs": "idx_f_local",
    "cidades": "idx_f_cidade",
    "portes": "idx_f_porte",
    "cnaes": "idx_f_cnae",
}


def normalizar(texto):
    texto = unicodedata.normalize(
        "NFKD", str(texto or "")
    ).casefold()

    return "".join(
        c for c in texto
        if not unicodedata.combining(c)
    )


def setor_do_cnae(codigo):
    valor = (
        str(codigo or "")
        .strip()
        .replace("-", "")
        .replace("/", "")
    )

    if (
        not valor
        or not valor.isascii()
        or not valor.isdigit()
        or len(valor) not in (6, 7)
    ):
        return "Outros / sem classificação"

    divisao = int(valor.zfill(7)[:2])

    for nome, divisoes in SETORES.items():
        if divisao in divisoes:
            return nome

    return "Outros / sem classificação"


def validar(original):
    row = {
        c: (original.get(c) or "").strip()
        for c in COLUNAS_FINAIS
    }

    if (
        row["situacao_cadastral"] != "02"
        or row["identificador_matriz_filial"] != "1"
        or not row["razao_social"]
        or not row["natureza_juridica"]
        or row["natureza_juridica"] in NATUREZAS_EXCLUIDAS
        or not row["uf"]
        or len(row["cnpj"]) != 14
        or not row["cnpj"].isascii()
        or not row["cnpj"].isdigit()
    ):
        return None

    contatos = []

    for n in (1, 2):
        ddd = "".join(
            c for c in row[f"ddd_{n}"]
            if c.isdigit()
        )

        telefone = "".join(
            c for c in row[f"telefone_{n}"]
            if c.isdigit()
        )

        contato = ddd + telefone

        if (
            telefone_util(ddd, telefone)
            and contato not in contatos
        ):
            row[f"ddd_{n}"] = ddd
            row[f"telefone_{n}"] = telefone
            row[f"contato_{n}"] = contato
            contatos.append(contato)
        else:
            row[f"ddd_{n}"] = ""
            row[f"telefone_{n}"] = ""
            row[f"contato_{n}"] = ""

    if not contatos:
        return None

    try:
        capital = float(
            row["capital_social"].replace(",", ".")
        )

        if not math.isfinite(capital) or capital < 0:
            capital = None
    except ValueError:
        capital = None

    busca = normalizar(
        " ".join(
            row[c]
            for c in (
                "cnpj",
                "razao_social",
                "nome_fantasia",
            )
        )
    )

    filtro = [
        SETOR_IDS[
            setor_do_cnae(row["cnae_fiscal_principal"])
        ],
        row["uf"],
        row["nome_municipio"],
        row["porte_empresa"],
        row["cnae_fiscal_principal"],
        int(bool(row["correio_eletronico"])),
        capital,
    ]

    dados = [
        row[c] for c in DATA_COLUMNS
    ] + [capital, busca]

    return dados, filtro


def importar_csv(con, source, limite):
    csv.field_size_limit(sys.maxsize)

    stat = source.stat()

    assinatura = json.dumps([
        stat.st_size,
        stat.st_mtime_ns,
        limite,
    ])

    con.execute("""
        CREATE TABLE IF NOT EXISTS sdr_importacao (
            assinatura TEXT,
            posicao TEXT,
            lidas INTEGER,
            mantidas INTEGER,
            terminou INTEGER
        )
    """)

    estado = con.execute(
        "SELECT * FROM sdr_importacao"
    ).fetchone()

    if estado and estado[0] != assinatura:
        raise ValueError(
            "A fonte ou o limite do teste mudou. "
            "Use outro nome de banco."
        )

    if not estado:
        existe = con.execute("""
            SELECT 1
            FROM sqlite_master
            WHERE name = 'leads'
        """).fetchone()

        if existe:
            raise ValueError(
                "Este banco já contém dados. "
                "Use outro nome de saída."
            )

        colunas = ",".join(
            f'"{c}" TEXT'
            for c in DATA_COLUMNS
        )

        con.execute(
            f"""
            CREATE TABLE leads (
                {colunas},
                capital_num REAL,
                busca TEXT
            )
            """
        )

        con.execute(
            """
            INSERT INTO sdr_importacao
            VALUES (?, ?, 0, 0, 0)
            """,
            (assinatura, "0"),
        )

        con.commit()
        estado = (assinatura, "0", 0, 0, 0)

    con.execute("""
        CREATE TABLE IF NOT EXISTS filtros_leads (
            lead_id INTEGER PRIMARY KEY,
            setor_id INTEGER NOT NULL,
            uf TEXT NOT NULL,
            nome_municipio TEXT NOT NULL,
            porte_empresa TEXT NOT NULL,
            cnae_fiscal_principal TEXT NOT NULL,
            tem_email INTEGER NOT NULL,
            capital_num REAL
        )
    """)

    con.commit()

    if estado[4]:
        print(
            "Leitura já concluída. "
            "Retomando criação dos índices.",
            flush=True,
        )
        return

    with source.open("rb") as arquivo:
        compactado = (
            arquivo.read(2) == b"\x1f\x8b"
        )

    abrir = gzip.open if compactado else open

    with abrir(
        source,
        "rt",
        encoding="latin-1",
        newline="",
    ) as arquivo:
        cabecalho = next(
            csv.reader(
                iter(arquivo.readline, ""),
                delimiter=";",
            )
        )

        if set(COLUNAS_FINAIS) - set(cabecalho):
            raise ValueError(
                "Faltam colunas esperadas do pipeline."
            )

        if int(estado[1]):
            arquivo.seek(int(estado[1]))

        lidas, mantidas = estado[2:4]
        dados_lote = []
        filtros_lote = []

        marcadores = ",".join(
            "?"
            for _ in range(len(DATA_COLUMNS) + 3)
        )

        inserir = (
            "INSERT INTO leads("
            "rowid,"
            + ",".join(DATA_COLUMNS)
            + ",capital_num,busca)"
            + f" VALUES ({marcadores})"
        )

        def salvar(terminou=False):
            con.executemany(
                inserir,
                dados_lote,
            )

            con.executemany(
                """
                INSERT INTO filtros_leads
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                filtros_lote,
            )

            con.execute(
                """
                UPDATE sdr_importacao
                SET posicao = ?,
                    lidas = ?,
                    mantidas = ?,
                    terminou = ?
                """,
                (
                    str(arquivo.tell()),
                    lidas,
                    mantidas,
                    int(terminou),
                ),
            )

            con.commit()

            dados_lote.clear()
            filtros_lote.clear()

            print(
                f"{lidas:,} linhas lidas; "
                f"{mantidas:,} empresas salvas. "
                "Ponto de retomada gravado.",
                flush=True,
            )

        leitor = csv.DictReader(
            iter(arquivo.readline, ""),
            fieldnames=cabecalho,
            delimiter=";",
        )

        try:
            for original in leitor:
                lidas += 1
                resultado = validar(original)

                if resultado:
                    mantidas += 1

                    dados_lote.append(
                        [mantidas] + resultado[0]
                    )

                    filtros_lote.append(
                        [mantidas] + resultado[1]
                    )

                if lidas % 25000 == 0:
                    salvar()

                if (
                    limite is not None
                    and lidas >= limite
                ):
                    break

            stat_final = source.stat()

            if (
                [
                    stat_final.st_size,
                    stat_final.st_mtime_ns,
                ]
                != json.loads(assinatura)[:2]
            ):
                raise ValueError(
                    "O CSV mudou durante a leitura."
                )

            salvar(terminou=True)

        except BaseException:
            con.rollback()
            raise


def finalizar(con):
    print(
        "Criando índice de CNPJ...",
        flush=True,
    )

    con.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_cnpj
        ON leads(cnpj)
    """)

    con.commit()

    for chave, campos in INDEX_KEYS.items():
        nome = INDEX_NAMES[chave]

        # O lead_id é a chave inteira da tabela.
        # SQLite já o inclui nos índices.
        colunas = ",".join(campos[:-1])

        print(
            f"Criando índice de {chave}...",
            flush=True,
        )

        con.execute(
            f"""
            CREATE INDEX IF NOT EXISTS {nome}
            ON filtros_leads({colunas})
            """
        )

        con.commit()

    print(
        "Salvando catálogos e contagens...",
        flush=True,
    )

    locais = con.execute("""
        SELECT DISTINCT uf, nome_municipio
        FROM filtros_leads
        ORDER BY 1, 2
    """).fetchall()

    cnaes = [
        (
            codigo,
            descricao,
            setor_do_cnae(codigo),
        )
        for codigo, descricao in con.execute("""
            SELECT DISTINCT
                cnae_fiscal_principal,
                descricao_cnae
            FROM leads
            ORDER BY 2, 1
        """)
    ]

    portes = [
        row[0]
        for row in con.execute("""
            SELECT DISTINCT porte_empresa
            FROM filtros_leads
            ORDER BY 1
        """)
    ]

    stats = {}

    for chave, campos in INDEX_KEYS.items():
        coluna = campos[0]

        contagens = con.execute(
            f"""
            SELECT {coluna}, count(*)
            FROM filtros_leads
            GROUP BY {coluna}
            """
        ).fetchall()

        stats[chave] = {
            (
                NOMES_SETORES[valor]
                if chave == "setores"
                else valor
            ): quantidade
            for valor, quantidade in contagens
        }

    total = sum(stats["setores"].values())

    assinatura = json.loads(
        con.execute("""
            SELECT assinatura
            FROM sdr_importacao
        """).fetchone()[0]
    )

    if total != con.execute(
        "SELECT count(*) FROM leads"
    ).fetchone()[0]:
        raise ValueError(
            "A tabela de filtros está incompleta."
        )

    catalogo = {
        "locais": locais,
        "cnaes": cnaes,
        "portes": portes,
        "stats": stats,
        "total": total,
        "amostra_linhas": assinatura[2],
    }

    con.execute("""
        CREATE TABLE IF NOT EXISTS sdr_catalogo (
            payload TEXT
        )
    """)

    con.execute(
        "DELETE FROM sdr_catalogo"
    )

    con.execute(
        "INSERT INTO sdr_catalogo VALUES (?)",
        (
            json.dumps(
                catalogo,
                ensure_ascii=False,
            ),
        ),
    )

    con.execute("ANALYZE filtros_leads")

    con.execute("""
        CREATE TABLE IF NOT EXISTS sdr_portatil (
            versao INTEGER
        )
    """)

    con.execute(
        "DELETE FROM sdr_portatil"
    )

    con.execute(
        "INSERT INTO sdr_portatil VALUES (?)",
        (PORTABLE_VERSION,),
    )

    con.commit()

    print(
        f"PRONTO: {total:,} empresas.",
        flush=True,
    )


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--fonte",
        required=True,
        type=Path,
    )

    parser.add_argument(
        "--banco",
        type=Path,
    )

    parser.add_argument(
        "--amostra",
        type=int,
    )

    args = parser.parse_args()

    if (
        args.amostra is not None
        and args.amostra < 1
    ):
        parser.error(
            "A amostra deve ser um número positivo."
        )

    padrao = (
        "teste_sdr.sqlite3"
        if args.amostra
        else "base_sdr_pronta.sqlite3"
    )

    banco = (
        args.banco or Path(padrao)
    ).expanduser().resolve()

    fonte = args.fonte.expanduser().resolve()

    if banco == fonte:
        parser.error(
            "A saída deve ser diferente da fonte."
        )

    banco.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    temporarios = (
        banco.parent / "temporarios_sdr"
    )

    temporarios.mkdir(exist_ok=True)

    # Define os temporários antes de carregar SQLite.
    os.environ["SQLITE_TMPDIR"] = str(temporarios)
    os.environ["TMPDIR"] = str(temporarios)

    import sqlite3
    from contextlib import closing

    if banco.is_file():
        try:
            with closing(
                sqlite3.connect(
                    banco.as_uri() + "?mode=ro",
                    uri=True,
                )
            ) as con:
                versao = con.execute("""
                    SELECT versao
                    FROM sdr_portatil
                """).fetchone()

                if (
                    not versao
                    or versao[0] != PORTABLE_VERSION
                ):
                    raise ValueError(
                        "Preparação incompleta."
                    )

                catalogo = json.loads(
                    con.execute("""
                        SELECT payload
                        FROM sdr_catalogo
                    """).fetchone()[0]
                )

            if (
                catalogo.get("amostra_linhas")
                != args.amostra
            ):
                parser.error(
                    "Esse banco contém outra versão. "
                    "Use outro nome de saída."
                )

            print(
                "Este banco já está pronto:",
                banco,
            )
            return

        except (sqlite3.Error, ValueError):
            pass

    print("Banco:", banco, flush=True)
    print(
        "Temporários:",
        temporarios,
        flush=True,
    )

    print(
        "Espaço livre: "
        f"{shutil.disk_usage(banco.parent).free / (1024**3):.1f} GB",
        flush=True,
    )

    inicio = perf_counter()

    try:
        with closing(
            sqlite3.connect(banco)
        ) as con:
            con.execute(
                "PRAGMA journal_mode=DELETE"
            )

            importar_csv(
                con,
                fonte,
                args.amostra,
            )

            finalizar(con)

        duracao = perf_counter() - inicio
        tamanho = (
            banco.stat().st_size / (1024**2)
        )

        print(
            f"Tempo total: {duracao:.1f} s",
            flush=True,
        )

        print(
            f"Tamanho do banco: {tamanho:.1f} MB",
            flush=True,
        )

        with closing(
            sqlite3.connect(
                banco.as_uri() + "?mode=ro",
                uri=True,
            )
        ) as con:
            exemplo = con.execute("""
                SELECT setor_id, uf, porte_empresa
                FROM filtros_leads
                LIMIT 1
            """).fetchone()

            if exemplo:
                inicio_consulta = perf_counter()

                encontrados = con.execute(
                    """
                    SELECT lead_id
                    FROM filtros_leads
                    INDEXED BY idx_f_setor
                    WHERE setor_id = ?
                      AND uf = ?
                      AND porte_empresa = ?
                    ORDER BY setor_id, lead_id
                    LIMIT 100
                    """,
                    exemplo,
                ).fetchall()

                milissegundos = (
                    perf_counter() - inicio_consulta
                ) * 1000

                print(
                    "Prévia indexada: "
                    f"{len(encontrados)} registros "
                    f"em {milissegundos:.1f} ms",
                    flush=True,
                )

        if args.amostra:
            print(
                "AMOSTRA DE TESTE: "
                f"primeiras {args.amostra:,} linhas. "
                "Não é a base completa.",
                flush=True,
            )

    except KeyboardInterrupt:
        print(
            "\nInterrompido. Repita o mesmo comando "
            "para continuar do último lote confirmado."
        )
        raise SystemExit(130)

    except (
        OSError,
        ValueError,
        sqlite3.Error,
        csv.Error,
    ) as exc:
        print(
            f"Não concluído: {exc}. "
            "Os lotes confirmados foram preservados.",
            file=sys.stderr,
        )
        raise SystemExit(1)


if __name__ == "__main__":
    main()