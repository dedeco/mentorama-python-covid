# Exercícios para mentorar pessoas para processar dados de COVID-19 usando PySpark.

## Objetivo:
Implemente o pipeline do PySpark que realiza operações de análise básica nos dados da vacina COVID-19. A amostra de dados para a análise necessária está disponível nos dados na pasta `data`

- `country_vaccinations.csv` 
  - O arquivo CSV contém os dados da vacina COVID-19 que foram registrados de acordo com o progresso da vacinação em todo o mundo ao longo do tempo.
  - O formato dos dados é `country,date,total_vaccinations,vaccines`
  - `country` : país para o qual as informações de vacinação são fornecidas
  - `date` : data para a data de entrada com o formato: `d/M/yy`
  - `total_vaccinations` : número absoluto de imunizações totais no país
  - `vaccines`: nome da vacina (autoridade nacional, organização internacional, organização local)

## Passos:

* Passo 0 - Início do exercício de criar a pipeline, estrutura inicial -> [ir para](step-0)
* Passo 1 - Estrutura criada. Sessão Spark criada e a criação do primeiro teste -> [ir para](step-1)
* Passo 2 - Criação da primeira função para contar a quantidade de vacinas disponíveis -> [ir para](step-2)
* Passo 3 - Criação da segunda função para obter a primeira vacina dada oficialmente ->  [ir para](step-3)
* Passo 4 - Criação da terceira função para obter o total de vacinas por país -> [ir para](step-4)
