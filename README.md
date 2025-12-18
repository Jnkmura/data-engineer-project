# Data Engineer (GCP + Looker) test

## Prerequisites

- Python 3.11 or higher

## Installation

### 1. Create a virtual environment

```bash
python -m venv venv
```

### 2. Using the virtual environment

**Windows:**

```bash
venv\Scripts\activate
```

**Linux/MacOs:**

```bash
source venv/bin/activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Create a .env file

```bash
touch .env
```

Edit `.env` with the following variables:

```text
PROJECT_ID=project_id
DATASET_ID=raw
TABLE_ID=sales_transactions 

BUCKET_ID=bucket_id
GCS_PREFIX=prefix
```

## Part 1 (Scripting)

To get the .json file from gcs and upload to bq

```bash
cd ingestion
python main.py
```

## Part 1 (Arquitetura)

**R:** Qual serviço do GCP você escolheria para orquestrar esse job diariamente? Justifique sua escolha pensando em custo e manutenibilidade.
    - Airflow (composer) seria minha resposta inicial por já estar acostumado a lidar com a ferramenta e criar dags. Estar dentro do ecosistema da GCP é um ponto positivo, conectando com outros serviços e ferramentas da plataforma mais facilmente, além de ser possível escalar para jobs mais pesados e de possuir boa observabilidade. Também é uma ferramenta extremamente flexível, podendo lidar com orquestração de casos mais simples até bem complexos.
    Como também é um serviço já gerenciado, atualizar os pacotes, criar snapshots e escalar facilitam bastante a manutenção.

## Part 2 (SQL & BigQuery Optimization)

1. **Query**: Escreva uma query SQL para calcular a Receita Média por Usuário (ARPU) por mês, considerando apenas pedidos com status "Completed"

```sql
select      format_date('%Y-%m', date(timestamp)) as period,
            round(
            safe_divide(
                sum(item_value),
                count(distinct user_id)
            ), 2) as arpu
from        raw.sales_transactions
where       status = 'Completed'
and         date(timestamp) >= 
                date_sub(current_date('America/Sao_Paulo'), interval 6 month)
group by    1 
```

2. **Otimização:** O time reclama que as consultas nessa tabela custam muito caro.
    - Como você recriaria a DDL dessa tabela para otimizar custos e performance de leitura?
    - Explique qual campo você usaria para particionar e por quê.

**R:** Para otimizar essa query eu particionaria por dia (date) a coluna de timestamp. Pois assim seria possível ganhar flexibilidade caso queira fazer esse tipo de query por dia/semana e ainda assim ser útil para mês/ano.
Outra otimização seria clusterizar por status, pois não teria grande granularidade o que pode ser útil também.

## Part 3 (Modelagem LookML)

2. **Derived Tables**: O time precisa de um relatório complexo que demora muito para rodar em tempo real. Como você usaria uma Persistent Derived Table no LookML para resolver isso? Explique a estratégia de datagroup_trigger

**R:** PDT no LooML pode ser definida como uma materialização de determinada query/view no banco de dados, tornando a leitura mais eficiente. O PDT pode ser incremental, o que pode fazer com que a query realizada seja apenas o "delta", tornando mais a query mais rápida e mantém o histórico intacto. O datagroup_trigger basicamente nos ajuda a definir o quão frequente o Looker vai atualizar esse PDT, podendo ser diário, a cada hora, a partir de um trigger (quantidade de linhas em uma tabela), entre outros.
