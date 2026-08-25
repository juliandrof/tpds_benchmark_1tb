# Benchmark TPC-DS 1 TB no Amazon Redshift (orquestrado a partir do Databricks)

Replica o benchmark do Databricks no **Amazon Redshift**, movendo os dados das
*managed tables* `tpds1tb.benchmark.*` para o Redshift e rodando as **mesmas 103
queries** (versão `queries_redshift/`) de forma **paralela** e **serial**.

> ⚠️ **Estes notebooks não foram testados** — não havia um endpoint Redshift
> disponível. A sintaxe das queries foi adaptada e a lógica está pronta; valide
> ponta a ponta quando tiver um Redshift.

## 📦 Conteúdo

```
redshift/
├── 00_setup.py          # unload DBX→S3, cria tabelas no Redshift (DDL gerado), COPY, bench_results
├── 01_parallel_exec.py  # dispara as 103 queries juntas (Barrier, 1 conexão por thread)
├── 02_serial_exec.py    # roda q1→q99 em ordem, uma por vez
├── 03_cleanup.py        # dropa schema no Redshift + limpa staging S3
└── README.md
```
As queries ficam em `../queries_redshift/` (reaproveitadas).

## 🔌 Como a conexão é feita (a partir do Databricks)

Duas conexões complementares:
1. **Spark → S3 (Parquet)** para descarregar as managed tables (`spark.write.parquet`).
2. **`redshift_connector`** (driver Python da AWS) para DDL, `COPY` e para **rodar
   e cronometrar** as queries. É o análogo do `statement_execution` usado no
   benchmark Databricks.

A carga de ~1 TB usa o caminho canônico do Redshift: **S3 + `COPY FORMAT AS
PARQUET`** (paralelizado pelas slices), e não INSERT/JDBC linha-a-linha.

## ✅ Pré-requisitos

1. **Redshift** (Serverless ou provisionado) com endpoint acessível.
2. **Rede:** rota Databricks → Redshift — endpoint público + Security Group
   liberando os IPs de saída (NAT) do Databricks, **ou** VPC peering/PrivateLink.
3. **Bucket S3 de staging** acessível pelos dois:
   - Databricks **escreve** (instance profile / credencial do cluster);
   - Redshift **lê** via **IAM role** anexada ao cluster (`s3:GetObject` no bucket).
4. **IAM role ARN** para o `COPY` (`IAM_ROLE_ARN`).
5. **Credenciais Redshift** — usuário + senha guardada em **Databricks Secrets**:
   ```bash
   databricks secrets create-scope redshift
   databricks secrets put-secret redshift password   # cole a senha
   ```
6. **Libs** no cluster: `redshift_connector` (instalada pelos notebooks via `%pip`)
   e `boto3` (já presente no DBR).

## ⚙️ Configuração

Edite o bloco de config no topo de cada notebook:

| Variável | Descrição |
|---|---|
| `REDSHIFT_TYPE` | `"serverless"` ou `"provisioned"` (muda a captura do *size*) |
| `REDSHIFT_HOST` / `REDSHIFT_PORT` / `REDSHIFT_DB` | endpoint do Redshift |
| `REDSHIFT_SCHEMA` | schema alvo (default `benchmark`) |
| `REDSHIFT_USER` / `REDSHIFT_PASSWORD` | usuário + senha (via secret) |
| `S3_STAGING` | prefixo S3 de staging (ex.: `s3://meu-bucket/tpcds_staging`) |
| `IAM_ROLE_ARN` | role do Redshift usada no `COPY` |
| `AWS_REGION` + `RS_WORKGROUP` / `RS_CLUSTER_ID` | para capturar o *size* dinamicamente |

## ▶️ Execução (manual, sem job)

1. **`00_setup`** — unload DBX→S3, cria tabelas (DDL gerado do schema real, com
   `DISTKEY`/`SORTKEY`), `COPY` S3→Redshift e cria `bench_results`.
2. **`01_parallel_exec`** — dispara as 103 juntas → grava `tp_exec='parallel'`.
3. **`02_serial_exec`** — q1→q99 em ordem → grava `tp_exec='serial'`.
4. **`03_cleanup`** (opcional) — dropa schema + limpa S3 (edite `CONFIRMAR=True`).

`bench_results` no Redshift tem as **mesmas 6 colunas** do lado Databricks; o
`sql_warehouse_size` guarda o *size* do Redshift (ex.: `128 RPU` no Serverless ou
`ra3.4xlarge x4` no provisionado).

## 🧱 Sobre o DDL

As tabelas são criadas com DDL **gerado dinamicamente** a partir do schema real
das managed tables (mapeando tipos Spark → Redshift), garantindo fidelidade sem
DDL escrito à mão. `DISTKEY`/`SORTKEY` vêm de um mapa documentado no `00_setup`:
fatos distribuídos por `*_item_sk` e ordenados por `*_date_sk`; dimensões pequenas
com `DISTSTYLE ALL`. Colunas de texto usam `VARCHAR(65535)` (sem risco de
truncamento; para um benchmark afinado, ajuste comprimentos).

## ⚠️ Diferença de concorrência (leia antes de comparar!)

- **Databricks Serverless:** 103 queries juntas → autoscale 1→10 clusters
  (no run real: ~24s de relógio).
- **Redshift:** o **WLM** limita queries simultâneas na fila principal. Sem
  **Concurrency Scaling** habilitado, disparar 103 juntas faz a maioria
  **enfileirar** — comportamento real, mas **não é maçã-com-maçã** em
  elasticidade. Habilite Concurrency Scaling para o análogo do autoscale.

## 🔗 Comparação unificada Databricks × Redshift

Do lado Databricks, leia o `bench_results` do Redshift via o data source e una
com o do Databricks:

```python
rs = (spark.read.format("redshift")
        .option("url", "jdbc:redshift://<host>:5439/dev?user=<u>&password=<p>")
        .option("tempdir", "s3://<bucket>/tmp")
        .option("aws_iam_role", "<arn>")
        .option("dbtable", "benchmark.bench_results").load())

(spark.table("tpds1tb.benchmark.bench_results").withColumn("engine", lit("databricks"))
   .unionByName(rs.withColumn("engine", lit("redshift")))
 ).createOrReplaceTempView("bench_all")

# SELECT engine, tp_exec, count(*), round(sum(duration),1), round(avg(duration),2)
# FROM bench_all GROUP BY engine, tp_exec ORDER BY engine, tp_exec
```
