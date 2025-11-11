
-----

# Pipeline de Dados Climáticos (ETL Orquestrado com Airflow)

## 📖 Descrição

Este projeto implementa um pipeline de dados ETL completo, robusto e orquestrado. O pipeline consome dados de previsão do tempo de uma API REST pública (Open-Meteo), transforma os dados em Python (Pandas) e os carrega em um banco de dados PostgreSQL.

O diferencial deste projeto é que **todo o ambiente (Airflow e PostgreSQL) é gerenciado como Infraestrutura como Código (IaC) usando Docker Compose**. A DAG do Airflow é responsável por agendar, executar, monitorar e validar o pipeline, garantindo que os dados sejam carregados de forma confiável e idempotente.

-----

## ✨ Principais Funcionalidades

  * **Orquestração de Pipeline:** A DAG (`dag_etl_clima.py`) usa a TaskFlow API (`@task`) para definir o fluxo, `TaskGroups` para uma UI limpa, e `default_args` para retentativas (`retries`) automáticas em caso de falha.
  * **Pipeline 100% Idempotente:** O pipeline pode ser executado várias vezes no mesmo dia sem *nunca* duplicar dados. Isso é garantido por uma lógica de `DELETE + COMMIT` (usando `PostgresHook` e `conn.begin()`) que limpa os dados do dia *antes* de realizar o `INSERT` (append) dos novos dados.
  * **Infraestrutura como Código (IaC):** O projeto utiliza um único arquivo `docker-compose.yml` que "sobe" um ambiente de produção completo, incluindo todos os serviços do Airflow (Webserver, Scheduler) e o banco de dados PostgreSQL de destino, gerenciando suas redes e dependências.
  * **Portões de Qualidade de Dados (Data Quality Gates):** O pipeline automaticamente valida os dados *após* a carga usando `SQLTableCheckOperator` e `SQLColumnCheckOperator`. A DAG falhará se os dados carregados estiverem vazios ou com valores nulos em colunas-chave.
  * **Gerenciamento de Conexões e Dependências:**
      * **Airflow Connections:** As credenciais do banco de dados são gerenciadas de forma segura na UI do Airflow e acessadas no código via `PostgresHook`.
      * **Airflow Variables:** Configurações (como URLs de API) são armazenadas centralmente.
      * **Docker Requirements:** As dependências do Python (Pandas, SQLAlchemy) são instaladas automaticamente no container do Airflow via `PIP_ADDITIONAL_REQUIREMENTS`.

-----

## 🚀 Tecnologias Utilizadas

  * **Linguagem:** Python 3.9+
  * **Orquestração:** Apache Airflow (TaskFlow API)
  * **Contêineres:** Docker, Docker Compose
  * **Banco de Dados:** PostgreSQL
  * **Bibliotecas Principais:** Pandas, SQLAlchemy, Requests

-----

## 📁 Estrutura do Projeto

```
Projeto-Pipeline-Clima/
│
├── 📜 docker-compose.yml         # Arquivo mestre que define o Airflow e o Postgres
├── 🔑 .env                       # (Adicionar manualmente) Variáveis de ambiente
│
├── 📁 dags/                      # Pasta de DAGs do Airflow
│   │
│   ├── 🎶 pipeline_clima/         # Módulo de lógica customizada
│   │   ├── 🐍 __init__.py
│   │   └── ⚙️ etl_logic.py      # Funções de Extract, Transform e Load
│   │
│   └── 🎼 dag_etl_clima.py        # O "Maestro": arquivo da DAG principal
│
├── 📁 plugins/                   # (Gerado pelo Airflow)
└── 📜 README.md
```

-----

## ⚙️ Como Executar o Projeto

Siga os passos abaixo para executar o pipeline completo no seu ambiente local.

### Pré-requisitos

  * Docker e Docker Compose instalados.

### Passos

1.  **Clone o repositório:**

    ```bash
    git clone [https://github.com/seu-usuario/Projeto-Pipeline-Clima.git](https://github.com/seu-usuario/Projeto-Pipeline-Clima.git)
    cd Projeto-Pipeline-Clima
    ```

2.  **Configure o Ambiente:**

      * Crie um arquivo chamado `.env` na pasta raiz do projeto.
      * Adicione as variáveis de ambiente necessárias que o `docker-compose.yml` espera. No mínimo (com base no nosso projeto):

    <!-- end list -->

    ```env
    POSTGRES_USER=postgres
    POSTGRES_PASSWORD=mysecretpassword
    POSTGRES_DB=clima_db
    AIRFLOW_IMAGE_NAME=apache/airflow:3.1.0
    AIRFLOW_UID=50000
    ```

3.  **Inicie o Ambiente (Airflow + Postgres):**

      * Este comando irá baixar as imagens, criar os containers e instalar as dependências do Python (`pandas`, `sqlalchemy`) automaticamente.

    <!-- end list -->

    ```bash
    docker-compose up -d
    ```

4.  **Configure o Airflow (Primeira Execução):**

      * Aguarde 1-2 minutos para os serviços iniciarem.
      * Abra a interface do Airflow: `http://localhost:8080` (usuário/senha padrão: `airflow`/`airflow`).
      * **Configure a Variável (Admin -\> Variables):**
          * Key: `CLIMA_API_URL`
          * Value: `https://api.open-meteo.com/v1/forecast`
      * **Configure a Conexão (Admin -\> Connections):**
          * **Conn Id:** `clima_postgres_conn`
          * **Conn Type:** `Postgres`
          * **Host:** `clima_postgres` (o nome do serviço no `docker-compose.yml`)
          * **Login:** `postgres`
          * **Password:** `mysecretpassword`
          * **Schema (Database):** `clima_db`
          * **Port:** `5432` (a porta *interna* do Docker)

5.  **Execute o pipeline:**

      * Na tela principal do Airflow, encontre a DAG `dag_clima_pipeline`.
      * Ative-a (clicando no botão "toggle") e clique no botão "Play" (`▶`) para disparar uma execução manual.

-----

## ✅ Resultado Esperado

Após a execução, você verá a sua DAG ficar 100% verde na interface do Airflow.

Você pode então usar seu cliente de banco de dados (como Beekeeper ou DBeaver) para se conectar ao seu banco Postgres (em `localhost:5433`) e inspecionar a tabela `previsao_clima`. Você verá que, mesmo que você dispare a DAG várias vezes, os dados **não serão duplicados**.