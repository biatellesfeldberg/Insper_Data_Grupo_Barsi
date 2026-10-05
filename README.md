# Insper Data & Grupo Barsi

## Sobre o projeto

Projeto desenvolvido pelo Insper Data em parceria com o Grupo Barsi para estruturar e qualificar a base de leads utilizada pela equipe de SDRs na prospecção de clientes. A partir dos dados públicos de CNPJ da Receita Federal, o projeto busca organizar os cadastros, melhorar a qualidade dos contatos e facilitar a criação de listas segmentadas para discagem na plataforma 3C.

O repositório reunirá os códigos de processamento e análise dos dados, além do desenvolvimento de uma solução de consulta acessível à equipe comercial. O objetivo é reduzir tentativas improdutivas de contato e apoiar a seleção de leads com maior potencial de conversão em reuniões com assessores.

## Etapas previstas

1. **Obter e organizar as bases:** reunir os arquivos de Empresas e Estabelecimentos da Receita Federal, junto às tabelas auxiliares de CNAEs, Municípios e Naturezas Jurídicas, registrando a competência utilizada.

2. **Consolidar os dados:** importar as partes de cada conjunto e relacionar Empresas e Estabelecimentos pelo CNPJ básico. Incorporar as descrições dos códigos das tabelas auxiliares para facilitar consultas e análises.

3. **Limpar e padronizar a base:** identificar duplicidades, campos ausentes e inconsistências, padronizar telefones e demais campos e aplicar os critérios de seleção definidos com o Grupo Barsi, incluindo situação cadastral e regras para contatos inadequados.

4. **Criar buscas e listas segmentadas:** desenvolver filtros por atividade econômica (CNAE), localização, porte e outras características disponíveis, com uma interface simples para os SDRs e sua liderança.

5. **Preparar a integração com a 3C:** gerar listas em formato compatível com a plataforma de discagem e avaliar o envio automático por API, conforme os acessos e recursos disponibilizados pela empresa.

6. **Analisar e qualificar os leads:** explorar o perfil das empresas e a disponibilidade de contato, definir critérios de priorização e avaliar um score de leads com base nos dados disponíveis e nos resultados observados.

7. **Medir e validar os resultados:** comparar as listas originais e tratadas por indicadores como chamadas completadas, contatos qualificados e reuniões agendadas e realizadas, conforme a disponibilidade das métricas. Utilizar os resultados para ajustar os filtros e a priorização.

8. **Documentar e facilitar a atualização:** organizar o processamento em Python e registrar as instruções de execução, atualização das bases e uso da solução, permitindo sua continuidade pela equipe do Grupo Barsi.

## Dados brutos (`Bases_RF/`)

Competência: **2026-09** ([pasta pública da RF](https://arquivos.receitafederal.gov.br/index.php/s/YggdBLfdninEJX9?dir=/2026-09)).

Arquivos esperados (dados **brutos**, sem limpeza prévia):

- `Estabelecimentos0.zip` … `Estabelecimentos9.zip`
- `Empresas0.zip` … `Empresas9.zip`
- `Cnaes.zip`, `Naturezas.zip`, `Municipios.zip`

Os zips são o formato oficial de distribuição da Receita. O pipeline também aceita `.csv` / `.csv.gz` com os mesmos nomes, se você extrair localmente.

> Em disco, os CSVs descompactados somam ~21 GB. Com pouco espaço livre, mantenha os `.zip` (~7,8 GB) — o `pipeline.py` lê direto deles.

Para (re)baixar só as fontes:

```bash
python pipeline.py --somente-baixar
```

## Pipeline (`pipeline.py`)

Um único script: limpeza completa em streaming + Left Join + dicionários → **`base_final.csv.gz`**.  
Não gera CSV intermediário limpo e **não apaga** `Bases_RF/`.

```bash
python pipeline.py              # processa o que já está em Bases_RF/
python pipeline.py --baixar     # baixa o que faltar e processa
```

### Fluxo

1. **Dicionários** — carrega Cnaes / Naturezas / Municípios em memória (remove linhas incompletas e códigos duplicados).
2. **Estabelecimentos (1ª passagem)** — aplica limpeza nos brutos e monta o conjunto de CNPJs válidos.
3. **Empresas** — aplica limpeza nos brutos e indexa em memória só os CNPJs retidos.
4. **Join (2ª passagem nos Estabelecimentos)** — Left Join por `cnpj_basico`, enriquece com descrições e grava só `base_final.csv.gz`.

### Limpeza aplicada (toda sobre dados brutos)

**Estabelecimentos:** situação ativa (`02`); só matriz; telefone útil (DDD BR + 8/9 dígitos, sem placeholder); `cnpj_basico` e UF preenchidos; dedup de CNPJ completo; normalização de DDD/telefone para dígitos.

**Empresas:** só CNPJs presentes nos Estabelecimentos limpos; exclui naturezas públicas/partidos/etc.; razão social não vazia; dedup de `cnpj_basico`.

**Dicionários:** código e descrição preenchidos; sem duplicata de código.

## Evoluções futuras

- Avaliar novas fontes de leads e formas de complementar os cadastros.
- Explorar análises para apoiar campanhas de marketing e comparar o desempenho das listas e dos SDRs.
- Avaliar integrações com a Academia Barsi e organizar a documentação do projeto no Barsi Brain.

As etapas acima representam o planejamento do projeto e serão refinadas conforme a validação com o Grupo Barsi.
