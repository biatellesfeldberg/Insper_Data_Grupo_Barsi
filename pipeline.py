"""
Pipeline único — Grupo Barsi / Insper Data

Lê os dados brutos da Receita Federal em Bases_RF/ (arquivos .zip oficiais
ou .csv já extraídos), aplica a limpeza completa em streaming, faz o Left Join
Estabelecimentos ⟕ Empresas e enriquece com Cnaes / Naturezas / Municípios.

Não gera arquivos intermediários limpos.
Saída única: base_final.csv.gz na raiz do repositório.
As fontes em Bases_RF/ NÃO são apagadas.

Uso:
  python pipeline.py              # processa o que já está em Bases_RF/
  python pipeline.py --baixar     # baixa da pasta pública RF 2026-09 o que faltar
"""

from __future__ import annotations

import argparse
import base64
import csv
import gzip
import io
import os
import subprocess
import sys
import zipfile
from pathlib import Path
from typing import Iterator

BASE_DIR = Path(__file__).resolve().parent
PASTA = BASE_DIR / "Bases_RF"
ARQUIVO_FINAL = BASE_DIR / "base_final.csv.gz"

SHARE_TOKEN = "YggdBLfdninEJX9"
WEBDAV_BASE = "https://arquivos.receitafederal.gov.br/public.php/webdav/2026-09"

ENCODING = "latin-1"
DELIMITADOR = ";"

DDDS_BR = {
    11, 12, 13, 14, 15, 16, 17, 18, 19,
    21, 22, 24, 27, 28,
    31, 32, 33, 34, 35, 37, 38,
    41, 42, 43, 44, 45, 46, 47, 48, 49,
    51, 53, 54, 55,
    61, 62, 63, 64, 65, 66, 67, 68, 69,
    71, 73, 74, 75, 77, 79,
    81, 82, 83, 84, 85, 86, 87, 88, 89,
    91, 92, 93, 94, 95, 96, 97, 98, 99,
}

TELEFONES_PLACEHOLDER = {
    "00000000", "000000000", "99999999", "999999999",
    "12345678", "123456789",
    "11111111", "111111111", "22222222", "33333333",
    "44444444", "55555555", "66666666", "77777777",
    "88888888", "9999999",
}

# Naturezas jurídicas tipicamente inúteis para prospecção comercial
NATUREZAS_EXCLUIDAS = {
    "0000", "8885",
    "1015", "1023", "1031", "1040", "1058", "1066", "1074", "1082",
    "1104", "1112", "1120", "1139", "1147", "1155", "1163", "1171", "1180",
    "1198", "1210", "1228", "1236", "1244", "1252", "1260", "1279",
    "1287", "1295", "1309", "1317", "1325", "1333", "1341", "1350",
    "2011",
    "3255", "3263", "3271", "3280", "3298", "4090",
    "5010", "5029", "5037",
}

COLUNAS_FINAIS = [
    "cnpj_basico", "cnpj_ordem", "cnpj_dv", "cnpj",
    "identificador_matriz_filial", "razao_social", "nome_fantasia",
    "situacao_cadastral", "data_situacao_cadastral", "data_inicio_atividade",
    "cnae_fiscal_principal", "descricao_cnae",
    "natureza_juridica", "descricao_natureza_juridica",
    "porte_empresa", "capital_social",
    "tipo_logradouro", "logradouro", "numero", "complemento", "bairro", "cep",
    "uf", "municipio", "nome_municipio",
    "ddd_1", "telefone_1", "ddd_2", "telefone_2", "correio_eletronico",
]

ARQUIVOS_RF = (
    [f"Empresas{i}.zip" for i in range(10)]
    + [f"Estabelecimentos{i}.zip" for i in range(10)]
    + ["Cnaes.zip", "Naturezas.zip", "Municipios.zip"]
)


def log(msg: str = "") -> None:
    print(msg, flush=True)


def livre_gb() -> float:
    u = os.statvfs(BASE_DIR)
    return (u.f_bavail * u.f_frsize) / (1024**3)


def campo(row: list[str], idx: int) -> str:
    return row[idx].strip() if len(row) > idx else ""


def apenas_digitos(valor: str) -> str:
    return "".join(ch for ch in valor if ch.isdigit())


def telefone_util(ddd: str, telefone: str) -> bool:
    d = apenas_digitos(ddd)
    t = apenas_digitos(telefone)
    if not d or not t:
        return False
    try:
        ddd_int = int(d)
    except ValueError:
        return False
    if ddd_int not in DDDS_BR:
        return False
    if t in TELEFONES_PLACEHOLDER:
        return False
    if len(set(t)) == 1:
        return False
    if len(t) not in (8, 9):
        return False
    return True


def tem_telefone_util(row: list[str]) -> bool:
    if len(row) < 25:
        return False
    return telefone_util(row[21], row[22]) or telefone_util(row[23], row[24])


def normalizar_telefones(row: list[str]) -> list[str]:
    for idx in (21, 22, 23, 24, 25, 26):
        if idx < len(row) and row[idx]:
            row[idx] = apenas_digitos(row[idx])
    return row


# ---------------------------------------------------------------------------
# Leitura de fontes brutas (.zip da RF, .csv ou .csv.gz)
# ---------------------------------------------------------------------------

def resolver_fonte(prefixo: str, indice: int | None = None) -> Path | None:
    """Encontra a fonte bruta: Prefixo{i}.zip | .csv | .csv.gz (nessa ordem)."""
    base = f"{prefixo}{indice}" if indice is not None else prefixo
    for suf in (".zip", ".csv", ".csv.gz"):
        p = PASTA / f"{base}{suf}"
        if p.exists() and p.stat().st_size > 0:
            return p
    return None


def abrir_texto(path: Path):
    """
    Abre um arquivo bruto como texto latin-1.
    Para .zip, abre o primeiro membro (layout da RF).
    Retorna (handle_texto, closer) — chamar closer() ao terminar.
    """
    if path.suffix == ".zip":
        zf = zipfile.ZipFile(path, "r")
        nomes = [n for n in zf.namelist() if not n.endswith("/")]
        if not nomes:
            zf.close()
            raise RuntimeError(f"Zip vazio: {path}")
        raw = zf.open(nomes[0], "r")
        text = io.TextIOWrapper(raw, encoding=ENCODING, newline="")

        def closer() -> None:
            text.close()
            zf.close()

        return text, closer

    if path.name.endswith(".csv.gz"):
        fh = gzip.open(path, "rt", encoding=ENCODING, newline="")
        return fh, fh.close

    fh = open(path, "r", encoding=ENCODING, newline="")
    return fh, fh.close


def iter_rows(path: Path) -> Iterator[list[str]]:
    text, closer = abrir_texto(path)
    try:
        for row in csv.reader(text, delimiter=DELIMITADOR):
            yield row
    finally:
        closer()


# ---------------------------------------------------------------------------
# Download (opcional)
# ---------------------------------------------------------------------------

def auth_header() -> str:
    token = base64.b64encode(f"{SHARE_TOKEN}:".encode()).decode()
    return f"Basic {token}"


def baixar_arquivo(nome_zip: str, tentativas: int = 12) -> Path:
    PASTA.mkdir(parents=True, exist_ok=True)
    destino = PASTA / nome_zip
    if destino.exists() and destino.stat().st_size > 0:
        log(f"   já existe {destino.name} ({destino.stat().st_size / (1024**2):.1f} MB)")
        return destino

    url = f"{WEBDAV_BASE}/{nome_zip}"
    log(f"↓ Baixando {nome_zip} ... (livre {livre_gb():.2f} GB)")
    tmp = destino.with_suffix(destino.suffix + ".part")
    cmd = [
        "curl", "-L", "--fail", "--retry", "5", "--retry-delay", "3",
        "-C", "-", "-sS", "--show-error",
        "-H", f"Authorization: {auth_header()}",
        "-o", str(tmp), url,
    ]
    ultimo_erro: Exception | None = None
    for tentativa in range(1, tentativas + 1):
        try:
            subprocess.run(cmd, check=True)
            if not tmp.exists() or tmp.stat().st_size == 0:
                raise RuntimeError(f"Download vazio: {nome_zip}")
            tmp.rename(destino)
            log(f"   OK {destino.name} ({destino.stat().st_size / (1024**2):.1f} MB)")
            return destino
        except Exception as exc:  # noqa: BLE001 — retenta queda de conexão da RF
            ultimo_erro = exc
            log(f"   falha tentativa {tentativa}/{tentativas}: {exc}")
    raise RuntimeError(f"Não foi possível baixar {nome_zip}") from ultimo_erro


def baixar_bases() -> None:
    log("\n=== Download das bases brutas (RF 2026-09) → Bases_RF/ ===")
    for nome in ARQUIVOS_RF:
        baixar_arquivo(nome)
    log(f"Download concluído. Livre: {livre_gb():.2f} GB")


# ---------------------------------------------------------------------------
# Limpeza de dicionários (em memória — sem reescrever arquivo)
# ---------------------------------------------------------------------------

def carregar_dicionario_limpo(prefixo: str) -> dict[str, str]:
    path = resolver_fonte(prefixo)
    if path is None:
        raise FileNotFoundError(f"Dicionário não encontrado em Bases_RF/: {prefixo}.zip|.csv")

    log(f"→ Limpando dicionário {path.name} (em memória) ...")
    d: dict[str, str] = {}
    antes = 0
    for row in iter_rows(path):
        antes += 1
        if len(row) < 2:
            continue
        codigo = row[0].strip().strip('"')
        descricao = row[1].strip().strip('"')
        if not codigo or not descricao:
            continue
        if codigo in d:
            continue
        d[codigo] = descricao
    log(f"   {prefixo}: {antes:,} → {len(d):,}".replace(",", "."))
    return d


# ---------------------------------------------------------------------------
# Limpeza de Estabelecimentos (streaming)
# ---------------------------------------------------------------------------

def estabelecimento_passa_limpeza(row: list[str]) -> bool:
    """
    Regras (discagem / leads) — mesmas de limpeza_bases.py:
    1. Situação cadastral Ativa (02)
    2. Apenas matriz (identificador_matriz_filial = 1)
    3. Pelo menos um telefone útil
    4. CNPJ básico e UF preenchidos
    """
    if len(row) < 25:
        return False
    cnpj_basico = campo(row, 0)
    matriz_filial = campo(row, 3)
    situacao = campo(row, 5)
    uf = campo(row, 19)
    if situacao != "02":
        return False
    if matriz_filial != "1":
        return False
    if not cnpj_basico or not uf:
        return False
    if not tem_telefone_util(row):
        return False
    return True


def chave_cnpj_completo(row: list[str]) -> str:
    return f"{campo(row, 0)}{campo(row, 1)}{campo(row, 2)}"


def listar_partes(prefixo: str) -> list[Path]:
    partes: list[Path] = []
    for i in range(10):
        p = resolver_fonte(prefixo, i)
        if p is not None:
            partes.append(p)
    if not partes:
        raise FileNotFoundError(
            f"Nenhuma parte de {prefixo} em Bases_RF/ "
            f"({prefixo}0..9.zip|.csv|.csv.gz). Use --baixar."
        )
    # menor → maior: progresso mais cedo; ordem estável para dedup
    return sorted(partes, key=lambda p: p.stat().st_size)


def coletar_cnpjs_estabelecimentos_limpos(
    partes: list[Path],
) -> tuple[set[str], set[str], dict]:
    """
    1ª passagem nos Estabelecimentos brutos:
    aplica limpeza + dedup de CNPJ completo; retorna
    - set de cnpj_basico mantidos
    - set de CNPJ completo mantidos (para 2ª passagem idêntica)
    """
    cnpjs_basicos: set[str] = set()
    cnpjs_completos: set[str] = set()
    stats = {
        "linhas_antes": 0,
        "linhas_depois": 0,
        "situacao_nao_ativa": 0,
        "nao_matriz": 0,
        "sem_telefone_util": 0,
        "cnpj_ou_uf_vazio": 0,
        "duplicado": 0,
    }

    for path in partes:
        log(f"→ [1/3] Limpando/coletando CNPJs de {path.name} ...")
        lidas = mantidas = 0
        for row in iter_rows(path):
            lidas += 1
            if lidas % 2_000_000 == 0:
                log(
                    f"   ... lidas {lidas:,} | mantidas {mantidas:,} | livre {livre_gb():.2f} GB".replace(
                        ",", "."
                    )
                )
            if len(row) < 25:
                stats["cnpj_ou_uf_vazio"] += 1
                continue
            if campo(row, 5) != "02":
                stats["situacao_nao_ativa"] += 1
                continue
            if campo(row, 3) != "1":
                stats["nao_matriz"] += 1
                continue
            if not campo(row, 0) or not campo(row, 19):
                stats["cnpj_ou_uf_vazio"] += 1
                continue
            if not tem_telefone_util(row):
                stats["sem_telefone_util"] += 1
                continue
            chave = chave_cnpj_completo(row)
            if chave in cnpjs_completos:
                stats["duplicado"] += 1
                continue
            cnpjs_completos.add(chave)
            cnpjs_basicos.add(campo(row, 0))
            mantidas += 1

        stats["linhas_antes"] += lidas
        stats["linhas_depois"] += mantidas
        log(
            f"   {path.name}: {lidas:,} → {mantidas:,} | "
            f"CNPJs básicos acum. {len(cnpjs_basicos):,}".replace(",", ".")
        )

    log(
        f"Estabelecimentos (limpo): {stats['linhas_antes']:,} → {stats['linhas_depois']:,} | "
        f"CNPJs básicos: {len(cnpjs_basicos):,}".replace(",", ".")
    )
    return cnpjs_basicos, cnpjs_completos, stats


# ---------------------------------------------------------------------------
# Limpeza de Empresas (streaming → índice em memória)
# ---------------------------------------------------------------------------

def indexar_empresas_limpas(
    partes: list[Path], cnpjs_validos: set[str]
) -> tuple[dict[str, tuple[str, str, str, str]], dict]:
    """
    Regras:
    1. Só CNPJs básicos presentes nos Estabelecimentos limpos
    2. Remove naturezas excluídas
    3. Remove razão social vazia
    4. Dedup por cnpj_basico (mantém a primeira)
    """
    indice: dict[str, tuple[str, str, str, str]] = {}
    stats = {
        "linhas_antes": 0,
        "linhas_depois": 0,
        "sem_estabelecimento_limpo": 0,
        "natureza_excluida": 0,
        "razao_vazia": 0,
        "duplicado": 0,
    }

    for path in partes:
        log(f"→ [2/3] Limpando/indexando {path.name} ...")
        lidas = 0
        antes_idx = len(indice)
        for row in iter_rows(path):
            lidas += 1
            if lidas % 2_000_000 == 0:
                log(
                    f"   ... lidas {lidas:,} | no índice {len(indice):,} | livre {livre_gb():.2f} GB".replace(
                        ",", "."
                    )
                )
            if not row:
                continue
            cnpj = campo(row, 0)
            razao = campo(row, 1)
            natureza = campo(row, 2)
            capital = campo(row, 4)
            porte = campo(row, 5)

            if cnpj not in cnpjs_validos:
                stats["sem_estabelecimento_limpo"] += 1
                continue
            if natureza in NATUREZAS_EXCLUIDAS:
                stats["natureza_excluida"] += 1
                continue
            if not razao:
                stats["razao_vazia"] += 1
                continue
            if cnpj in indice:
                stats["duplicado"] += 1
                continue
            indice[cnpj] = (razao, natureza, porte, capital)

        stats["linhas_antes"] += lidas
        stats["linhas_depois"] += len(indice) - antes_idx
        log(
            f"   {path.name}: lidas {lidas:,} | índice {len(indice):,} "
            f"(+{len(indice) - antes_idx:,})".replace(",", ".")
        )

    log(f"Empresas no índice (limpo): {len(indice):,}".replace(",", "."))
    return indice, stats


# ---------------------------------------------------------------------------
# Join + escrita da base final
# ---------------------------------------------------------------------------

def gerar_base_final(
    partes_estab: list[Path],
    cnpjs_completos: set[str],
    indice_emp: dict[str, tuple[str, str, str, str]],
    dic_cnaes: dict[str, str],
    dic_naturezas: dict[str, str],
    dic_municipios: dict[str, str],
) -> tuple[int, int]:
    if ARQUIVO_FINAL.exists():
        ARQUIVO_FINAL.unlink()

    total = matches = 0
    ja_escritos: set[str] = set()

    with gzip.open(ARQUIVO_FINAL, "wt", encoding=ENCODING, newline="") as fout:
        writer = csv.writer(fout, delimiter=DELIMITADOR, quoting=csv.QUOTE_MINIMAL)
        writer.writerow(COLUNAS_FINAIS)

        for path in partes_estab:
            log(f"→ [3/3] Join {path.name} → {ARQUIVO_FINAL.name} ...")
            n = m = 0
            for row in iter_rows(path):
                if not estabelecimento_passa_limpeza(row):
                    continue
                chave = chave_cnpj_completo(row)
                if chave not in cnpjs_completos or chave in ja_escritos:
                    continue
                ja_escritos.add(chave)

                row = normalizar_telefones(list(row))
                cnpj_basico = campo(row, 0)
                cnpj_ordem = campo(row, 1)
                cnpj_dv = campo(row, 2)
                cnae = campo(row, 11)
                municipio = campo(row, 20)

                emp = indice_emp.get(cnpj_basico)
                if emp:
                    razao, natureza, porte, capital = emp
                    m += 1
                else:
                    razao = natureza = porte = capital = ""

                writer.writerow(
                    [
                        cnpj_basico, cnpj_ordem, cnpj_dv,
                        f"{cnpj_basico}{cnpj_ordem}{cnpj_dv}",
                        campo(row, 3), razao, campo(row, 4),
                        campo(row, 5), campo(row, 6), campo(row, 10),
                        cnae, dic_cnaes.get(cnae, ""),
                        natureza, dic_naturezas.get(natureza, ""),
                        porte, capital,
                        campo(row, 13), campo(row, 14), campo(row, 15),
                        campo(row, 16), campo(row, 17), campo(row, 18),
                        campo(row, 19), municipio, dic_municipios.get(municipio, ""),
                        campo(row, 21), campo(row, 22), campo(row, 23),
                        campo(row, 24), campo(row, 27),
                    ]
                )
                n += 1
                if (total + n) % 2_000_000 == 0:
                    log(
                        f"   ... escritas {total + n:,} | livre {livre_gb():.2f} GB".replace(
                            ",", "."
                        )
                    )

            fout.flush()
            total += n
            matches += m
            log(
                f"   +{n:,} linhas (com empresa {m:,}) | "
                f"gz={ARQUIVO_FINAL.stat().st_size / (1024**2):.0f} MB".replace(",", ".")
            )

    return total, matches


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    csv.field_size_limit(sys.maxsize)

    parser = argparse.ArgumentParser(
        description="Pipeline: limpeza completa + join → base_final.csv.gz"
    )
    parser.add_argument(
        "--baixar",
        action="store_true",
        help="Baixa de Bases_RF/ os zips oficiais da RF 2026-09 que ainda faltarem",
    )
    parser.add_argument(
        "--somente-baixar",
        action="store_true",
        help="Só baixa as bases brutas; não processa",
    )
    args = parser.parse_args()

    log("=" * 70)
    log("Pipeline CNPJ — limpeza completa + join → base_final.csv.gz")
    log("=" * 70)
    log(f"Espaço livre inicial: {livre_gb():.2f} GB")

    PASTA.mkdir(parents=True, exist_ok=True)

    if args.baixar or args.somente_baixar:
        baixar_bases()
        if args.somente_baixar:
            log("\nFontes brutas salvas em Bases_RF/. Encerrado (--somente-baixar).")
            return 0

    # Dicionários (limpeza em memória)
    log("\n=== Dicionários ===")
    dic_cnaes = carregar_dicionario_limpo("Cnaes")
    dic_naturezas = carregar_dicionario_limpo("Naturezas")
    dic_municipios = carregar_dicionario_limpo("Municipios")

    # Estabelecimentos — 1ª passagem (limpeza + CNPJs)
    log("\n=== Estabelecimentos (limpeza) ===")
    partes_estab = listar_partes("Estabelecimentos")
    cnpjs_basicos, cnpjs_completos, _ = coletar_cnpjs_estabelecimentos_limpos(partes_estab)

    # Empresas — limpeza + índice
    log("\n=== Empresas (limpeza + índice) ===")
    partes_emp = listar_partes("Empresas")
    indice_emp, _ = indexar_empresas_limpas(partes_emp, cnpjs_basicos)
    del cnpjs_basicos

    # Join → base final
    log("\n=== Join + enriquecimento → base_final.csv.gz ===")
    total, matches = gerar_base_final(
        partes_estab, cnpjs_completos, indice_emp,
        dic_cnaes, dic_naturezas, dic_municipios,
    )

    log("\n" + "=" * 70)
    log(f"CONCLUÍDO: {ARQUIVO_FINAL}")
    log(f"Tamanho: {ARQUIVO_FINAL.stat().st_size / (1024**3):.2f} GB")
    log(f"Linhas: {total:,} | com empresa: {matches:,}".replace(",", "."))
    log(f"Espaço livre final: {livre_gb():.2f} GB")
    log("Fontes brutas preservadas em Bases_RF/")
    log("=" * 70)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
