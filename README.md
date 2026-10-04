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

## Evoluções futuras

- Avaliar novas fontes de leads e formas de complementar os cadastros.
- Explorar análises para apoiar campanhas de marketing e comparar o desempenho das listas e dos SDRs.
- Avaliar integrações com a Academia Barsi e organizar a documentação do projeto no Barsi Brain.

As etapas acima representam o planejamento do projeto e serão refinadas conforme a validação com o Grupo Barsi.
