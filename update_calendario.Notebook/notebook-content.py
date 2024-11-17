# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "cb655a3e-3e0d-49fc-83d8-bd7ea25b9b2c",
# META       "default_lakehouse_name": "lakehouse",
# META       "default_lakehouse_workspace_id": "1cbb083e-edf5-4bae-bf20-0e9764b85758",
# META       "known_lakehouses": [
# META         {
# META           "id": "cb655a3e-3e0d-49fc-83d8-bd7ea25b9b2c"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Carrega os pacotes
import math
from pyspark.sql.functions import *
from datetime import datetime, timedelta
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, to_date, month, year, udf

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Parâmetros
data_inicial = "2021-01-01"
data_final = datetime.now().strftime("%Y-12-31")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Função para gerar feriados fixos
def gera_feriados_fixos(ano):
    feriados_fixos = {
        "Confraternização Universal": datetime(ano, 1, 1),
        "Aniversário de São Paulo": datetime(ano, 1, 25),
        "Tiradentes": datetime(ano, 4, 21),
        "Dia do Trabalho": datetime(ano, 5, 1),
        "Revolução Constitucionalista": datetime(ano, 7, 9),
        "Independência do Brasil": datetime(ano, 9, 7),
        "Nossa Senhora Aparecida": datetime(ano, 10, 12),
        "Finados": datetime(ano, 11, 2),
        "Proclamação da República": datetime(ano, 11, 15),
        "Consciência Negra": datetime(ano, 11, 20),
        "Véspera de Natal": datetime(ano, 12, 24),
        "Natal": datetime(ano, 12, 25),
        "Véspera de Ano Novo": datetime(ano, 12, 31)
    }
    return feriados_fixos

# Função para gerar feriados móveis
def mod_maior_que_zero(x, y):
    m = x % y
    return m + y if m < 0 else m

def gera_feriados_moveis(ano):
    pascoa_numeral = math.ceil(
        ((datetime(ano, 4, 1).toordinal() - 693594) / 7)
        + (mod_maior_que_zero(19 * mod_maior_que_zero(ano, 19) - 7, 30) * 0.14)
    ) * 7 - 6 + 693594
    pascoa_data = datetime.fromordinal(int(pascoa_numeral))
    feriados_moveis = {
        "Segunda-feira de Carnaval": pascoa_data - timedelta(days=48),
        "Terça-feira de Carnaval": pascoa_data - timedelta(days=47),
        "Quarta-feira de Cinzas": pascoa_data - timedelta(days=46),
        "Sexta-feira Santa": pascoa_data - timedelta(days=2),
        "Páscoa": pascoa_data,
        "Corpus Christi": pascoa_data + timedelta(days=60)
    }
    return feriados_moveis

# Função para gerar todos os feriados para um ano
def gera_todos_feriados_um_ano(ano):
    feriados_fixos = gera_feriados_fixos(ano)
    feriados_moveis = gera_feriados_moveis(ano)
    feriados = {**feriados_fixos, **feriados_moveis}
    return feriados

# Função para gerar todos os feriados para uma lista de anos
def gera_todos_feriados_lista_anos(ano_inicial, ano_final):
    todos_feriados = {}
    for ano in range(ano_inicial, ano_final + 1):
        todos_feriados[ano] = gera_todos_feriados_um_ano(ano)
    return todos_feriados

# Gera os feriados 
todos_feriados = gera_todos_feriados_lista_anos(int(data_inicial[:4]), int(data_final[:4]))
print(todos_feriados)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cria um dataframe com os feriados
feriados = []
for ano, feriados_dic in todos_feriados.items():
    for feriado, data in feriados_dic.items():
        feriados.append((feriado, data))
feriados_df = spark.createDataFrame(feriados, ["Feriado", "Data"])
feriados_df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Gera um dataframe com todas as datas do intervalo
dias_df = spark.createDataFrame(
    [(data_inicial, data_final)], ["data_inicial", "data_final"]
).selectExpr("sequence(to_date(data_inicial), to_date(data_final), interval 1 day) as date") \
 .selectExpr("explode(date) as Data")
dias_df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Mescla os dois dataframes para identificar os feriados
calendario_df = dias_df.join(feriados_df, dias_df.Data == feriados_df.Data, "left").select(dias_df.Data, feriados_df.Feriado)
calendario_df = calendario_df.withColumn("E_Feriado", when(col("Feriado").isNotNull(), 1).otherwise(0))
calendario_df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Reimporta os pacotes
from pyspark.sql.functions import *

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cria os dicion
pt_br_mes_nome = {
    1: "Janeiro", 2: "Fevereiro", 3: "Março", 4: "Abril", 5: "Maio", 6: "Junho",
    7: "Julho", 8: "Agosto", 9: "Setembro", 10: "Outubro", 11: "Novembro", 12: "Dezembro"
}

pt_br_mes_nome_abrev = {k: v[:3] for k, v in pt_br_mes_nome.items()}

pt_br_dia_semana = {
    0: "Segunda-feira", 1: "Terça-feira", 2: "Quarta-feira", 3: "Quinta-feira", 
    4: "Sexta-feira", 5: "Sábado", 6: "Domingo"
}

pt_br_dia_semana_abrev = {k: v[:3] for k, v in pt_br_dia_semana.items()}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cria as funções para traduzir os nomes
pt_br_mes_nome_udf = udf(lambda x: pt_br_mes_nome[x], StringType())
pt_br_mes_nome_abrev_udf = udf(lambda x: pt_br_mes_nome_abrev[x], StringType())
pt_br_dia_semana_udf = udf(lambda x: pt_br_dia_semana[x], StringType())
pt_br_dia_semana_abrev_udf = udf(lambda x: pt_br_dia_semana_abrev[x], StringType())


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create other columns with pt-BR
calendario_df = calendario_df.withColumn("Ano", year(col("Data")).cast("int")) \
    .withColumn("Dia", date_format(col("Data"), "d").cast("int")) \
    .withColumn("MesNum", month(col("Data")).cast("int")) \
    .withColumn("MesNome", pt_br_mes_nome_udf(col("MesNum"))) \
    .withColumn("MesNomeAbrev", pt_br_mes_nome_abrev_udf(col("MesNum"))) \
    .withColumn("MesAno", col("MesNomeAbrev") + date_format(col("Data"), "yy")) \
    .withColumn("MesInicio", trunc(col("Data"), "month")) \
    .withColumn("MesFinal",  last_day(col("Data"))) \
    .withColumn("TrimentreNum", quarter(col("Data")).cast("int")) \
    .withColumn("DiaSemanaNum", weekday(col("Data")).cast("int")) \
    .withColumn("DiaSemanaNome", pt_br_dia_semana_udf(col("DiaSemanaNum"))) \
    .withColumn("DiaSemanaNomeAbrev", pt_br_dia_semana_abrev_udf(col("DiaSemanaNum"))) \
    .withColumn("SemanaIsoNum", weekofyear(col("Data")).cast("int")) \
    .withColumn("AnoIso", year(date_add(col("Data"), 26 - col("SemanaIsoNum"))).cast("int")) \
    .withColumn("E_FinalSemana", when(col("DiaSemanaNum")>4, 1).otherwise(0).cast("int")) \
    .withColumn("E_DiaUtil", when((col("E_Feriado") == 1) | (col("E_FinalSemana") == 1), 0).otherwise(1).cast("int"))
display(calendario_df.orderBy("Data").toPandas().head(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Salva o dataframe como tabela Delta
calendario_df.write.format("delta").mode("overwrite").saveAsTable("lakehouse.calendario")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- show table from lake
# MAGIC SELECT * FROM lakehouse.calendario

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
