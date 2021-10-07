## Ambiente:
- Spark Version: 3.0.1
- Python Version: 3.7

## Estrutura inicial do projeto
```
├── README.md
├── app.py
├── data
│   ├── country_vaccinations.csv
│   └── explore_data.ipynb
├── main
│   ├── __init__.py
│   ├── base
│   │   └── __init__.py
│   └── job
│      ├── __init__.py
│      └── pipeline.py
├── pytest.ini
├── requirements.txt
└── tests
    ├── __init__.py
    └── test_pipelines.py
```

## Objetivo:
Implemente o pipeline do PySpark que realiza operações de análise básica nos dados da vacina COVID-19. A amostra de dados para a análise necessária está disponível nos dados na pasta `data`

- `country_vaccinations.csv` 
  - O arquivo CSV contém os dados da vacina COVID-19 que foram registrados de acordo com o progresso da vacinação em todo o mundo ao longo do tempo.
  - O formato dos dados é `country,date,total_vaccinations,vaccines`
  - `country` : país para o qual as informações de vacinação são fornecidas
  - `date` : data para a data de entrada com o formato: `d/M/yy`
  - `total_vaccinations` : número absoluto de imunizações totais no país
  - `vaccines`: nome da vacina (autoridade nacional, organização internacional, organização local)
  
  
O projeto está parcialmente concluído e existem 4 métodos e uma sessão de ativação a serem implementados na classe `main.job.pipeline.PySparkJob.py`:

- `init_spark_session(self) -> SparkSession`:
  - criar uma sessão spark com o nome `Covid19 Vaccination Progress`

- `count_available_vaccines(self, vaccines: DataFrame) -> int`:
  - conte o número de vacinas exclusivas em todo o mundo

- `find_earliest_used_vaccine(self, vaccines: DataFrame) -> str`:
  - encontrar a primeira vacina (`vaccine`) que foi usada no mundo
  - em cenários onde várias vacinas foram usadas em um dia, classifique a vacina (`vaccine`) em ordem crescente e escolha a primeira
  - <b>Dica</b>: use a data (`date`) para encontrar a primeira vacina (`vaccine`) usada e retorne apenas o nome da vacina.
  - converter data em tipo de data (`date`) para formato date
  - Por exemplo: `Oxford/AstraZeneca`  a primeira vacina usada:
  
    | country       | date        | ...  | vaccine           |
    | ------------- |:-----------:|---:  | -----------------:|
    | UK            | 01/12/20    | ...  | Oxford/AstraZeneca|
    | Russia        | 01/12/20    | ...  | Sputnik V         |
    | US            | 30/12/20    | ...  | Moderna           |
  

- `total_vaccinations_per_country(self, vaccines: DataFrame) -> DataFrame`:
  - Faça a agregação dos dados de vacinas para ver o que é total_vaccinations para cada país
  - <b>Nota</b>: `total_vaccinations` pode estar faltando nos dados reais devido a um erro de entrada de dados. Ignore todos os registros ausentes!
  - Por exemplo:
    
    | country       | date        | total_vaccinations  | vaccine           |
    | ------------- |:-----------:|------------------:  | -----------------:|
    | UK            | 01/12/20    | 1000                | Oxford/AstraZeneca|
    | UK            | 01/12/20    | 2000                | Sputnik V         |
    | US            | 30/12/20    | 5000                | Moderna           |
    
  - Saída esperada:
  
    |country  | total_vaccinations |
    |---------|:------------------:|
    | UK      | 3000               |
    | US      | 5000               |
    
Conclua a implementação de forma que os testes de unidade sejam aprovados durante a execução dos testes. Use os testes de dar para verificar seu progresso enquanto resolve o problema.
