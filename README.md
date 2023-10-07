# de03_dataops


1. [Introdução](#introdução)
2. [Estrutura do Proejeto](#estrutura-do-projeto)
3. [Origem dos Dados](#origem-dos-dados)
4. [Requisitos](#requisitos)

## Introdução

Projeto desenvolvido durante a aula de DataOps do curso de MBA de Engenharia de dados turma 03, oferecido pela  faculdade Impacta.

Nesta disciplica abordamos os seguintes temas:
- Conceito de DataOps
- Níveis de maturidade de projetos em Engenharia de Dados
- Tecnologias em Engenharia de Dados
- Princípios e boas práticas em projetos de Engenharia de Dados
- Orquestração e monitoramento


## Estrutura do projeto
O projeto consiste em uma aplicação simples dos conceitos vistos em sala de aula. Iremos desenvolver um pipeline completo da seguinte forma:

![alt text](../de03_dataops/imgs/projeto.png)

## Origem dos dados
- https://swapi.dev/api/people/?
- https://swapi.dev/api/planets/?
- https://swapi.dev/api/films/?

## Requisitos
- Formato da tabela de entrega: csv 
- Frequência de atualização do dado: frequência de 1x por dia
- Parâmetro de coleta: 1 página por dia 
- Salvar logs do processo
- Armazenamento dos dados brutos
- Armazenamento dos dados saneados: Tratamento de tipos, nomes e nulos
- Armazenamento dos dados agregados e tratados
- Validação de qualidade de dados: Validação de duplicados e Tolerância de nulos
- Orquestração realizada via airflow



