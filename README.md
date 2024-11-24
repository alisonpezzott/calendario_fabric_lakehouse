## Notebook Spark para carregar a tabela calendário no lakehouse

Este README é arquivo de suporte para o vídeo .... 

Instruções:

1. Crie um novo workspace com capacidade Fabric ou Trial;
2. Crie um novo lakehouse e batize-o de lakehouse;
3. A partir do lakehouse crie um novo notebook e dê o nome de gera_calendario;
4. Certifique-se que na barra lateral esquerda o lakehouse está adicionado ao notebook;
5. Copie o código abaixo e cole na primeira célula do código;
6. Confira os parâmetros a partir da linha 8 e rode o código clicando em Run all;
7. A primeira vez poderá demorar alguns segundos devido ao start da sessão;



```python
# Código para criação/atualização de uma tabela calendário via notebook no lakehouse Fabric
# Carrega os pacotes
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime, timedelta
import math

# Parâmetros
data_inicial = "2020-01-01" 
data_atual = datetime.now()
anos_futuros = 5
data_final = data_atual.replace(year=data_atual.year+anos_futuros, month=12, day=31).strftime("%Y-12-31")
mes_inicio_ano_fiscal = 4
nome_lakehouse = "lakehouse"
nome_tabela = "calendario"

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

# Cria um dataframe com os feriados
feriados = []
for ano, feriados_dic in todos_feriados.items():
    for feriado, data in feriados_dic.items():
        feriados.append((feriado, data))
feriados_df = spark.createDataFrame(feriados, ["Feriado", "Data"])

# Gera um dataframe com todas as datas do intervalo
dias_df = spark.createDataFrame(
    [(data_inicial, data_final)], ["data_inicial", "data_final"]
).selectExpr("sequence(to_date(data_inicial), to_date(data_final), interval 1 day) as date") \
 .selectExpr("explode(date) as Data")

# Mescla os dois dataframes para identificar os feriados
calendario_df = dias_df.join(feriados_df, dias_df.Data == feriados_df.Data, "left").select(dias_df.Data, feriados_df.Feriado)
calendario_df = calendario_df.withColumn("E_Feriado", when(col("Feriado").isNotNull(), 1).otherwise(0))

# Cria os dicionários para o pt-BR
pt_br_mes_nome = {
    1: "Janeiro", 2: "Fevereiro", 3: "Março", 4: "Abril", 5: "Maio", 6: "Junho",
    7: "Julho", 8: "Agosto", 9: "Setembro", 10: "Outubro", 11: "Novembro", 12: "Dezembro"
}

pt_br_dia_semana = {
    1: "Segunda-feira", 2: "Terça-feira", 3: "Quarta-feira", 4: "Quinta-feira", 
    5: "Sexta-feira", 6: "Sábado", 7: "Domingo"
}

# Cria as funções para traduzir os nomes
pt_br_mes_nome_udf = udf(lambda x: pt_br_mes_nome[x], StringType())
pt_br_dia_semana_udf = udf(lambda x: pt_br_dia_semana[x], StringType())

# Cria um DataFrame temporário com a data atual
data_atual_df = spark.createDataFrame([(data_atual,)], ["data_atual"])

# Dataframe para variáveis em relação a data atual
data_atual_df = data_atual_df.withColumn("mes_atual", month(col("data_atual")).cast("int")) \
    .withColumn("ano_atual", year(col("data_atual")).cast("int")) \
    .withColumn("mes_ano_num_atual", (col("ano_atual") * 100 + col("mes_atual")).cast("int")) \
    .withColumn("trimestre_ano_num_atual", (col("ano_atual") * 100 + (floor((col("mes_atual") - 1) / 3) + 1)).cast("int")) \
    .withColumn("semana_iso_num_atual", weekofyear(col("data_atual")).cast("int")) \
    .withColumn("ano_iso_num_atual", year(date_add(col("data_atual"), expr("26 - weekofyear(data_atual)"))).cast("int")) \
    .withColumn("semana_ano_iso_num_atual", (col("ano_iso_num_atual") * 100 + col("semana_iso_num_atual")).cast("int"))

# Extrai cada valor como uma variável individual
valores_data = data_atual_df.select(
    "mes_atual", 
    "ano_atual", 
    "mes_ano_num_atual", 
    "trimestre_ano_num_atual", 
    "semana_iso_num_atual", 
    "ano_iso_num_atual", 
    "semana_ano_iso_num_atual"
).first()

# Acessa cada valor individualmente
data_atual = datetime.now().date()
mes_atual = valores_data["mes_atual"]
ano_atual = valores_data["ano_atual"]
mes_ano_num_atual = valores_data["mes_ano_num_atual"]
trimestre_ano_num_atual = valores_data["trimestre_ano_num_atual"]
semana_iso_num_atual = valores_data["semana_iso_num_atual"]
ano_iso_num_atual = valores_data["ano_iso_num_atual"]
semana_ano_iso_num_atual = valores_data["semana_ano_iso_num_atual"]

# Cria outras colunas em "pt-BR"
calendario_df = calendario_df.withColumn("Ano", year(col("Data")).cast("int")) \
    .withColumn("Dia", date_format(col("Data"), "d").cast("int")) \
    .withColumn("MesNum", month(col("Data")).cast("int")) \
    .withColumn("MesNome", pt_br_mes_nome_udf(col("MesNum"))) \
    .withColumn("MesNomeAbrev", col("MesNome").substr(1,3)) \
    .withColumn("MesAnoNome", concat_ws("/", col("MesNomeAbrev"), date_format(col("Data"), "yy"))) \
    .withColumn("MesAnoNum", col("Ano") * 100 + col("MesNum").cast("int")) \
    .withColumn("TrimestreNum", quarter(col("Data")).cast("int")) \
    .withColumn("TrimestreAnoNum", (col("Ano") * 100 + col("TrimestreNum")).cast("int") ) \
    .withColumn("TrimestreAnoNome", concat(lit("T"), col("trimestreNum"), lit("-"), lit(col("ano")) )) \
    .withColumn("DiaSemanaNum", dayofweek(col("Data")).cast("int")) \
    .withColumn("DiaSemanaNome", pt_br_dia_semana_udf(col("DiaSemanaNum"))) \
    .withColumn("DiaSemanaNomeAbrev", col("DiaSemanaNome").substr(1,3)) \
    .withColumn("SemanaIsoNum", weekofyear(col("Data")).cast("int")) \
    .withColumn("AnoIso", year(date_add(col("Data"), 26 - col("SemanaIsoNum"))).cast("int")) \
    .withColumn("SemanaAnoIsoNum", (col("AnoIso") * 100 + col("SemanaIsoNum")).cast("int")) \
    .withColumn("SemanaAnoIsoNome", concat(lit("S"), lit(lpad(col("SemanaIsoNum").cast("string"), 2, "0")), lit("-"), lit(col("ano")) )) \
    .withColumn("E_FinalSemana", when(col("DiaSemanaNum")>5, 1).otherwise(0).cast("int")) \
    .withColumn("E_DiaUtil", when((col("E_Feriado") == 1) | (col("E_FinalSemana") == 1), 0).otherwise(1).cast("int")) \
    .withColumn("DataAtual", when(col("Data") == data_atual, "Data atual").otherwise(col("Data")).cast("string")) \
    .withColumn("SemanaAtual", when(col("SemanaAnoIsoNum") == semana_ano_iso_num_atual, "Semana atual").otherwise(col("SemanaAnoIsoNome")).cast("string")) \
    .withColumn("MesAtual", when(col("MesAnoNum") == mes_ano_num_atual, "Mês atual").otherwise(col("MesAnoNome")).cast("string")) \
    .withColumn("TrimestreAtual", when(col("TrimestreAnoNum") == trimestre_ano_num_atual, "Trimestre atual").otherwise(col("TrimestreAnoNome")).cast("string")) \
    .withColumn("AnoAtual", when(col("Ano") == ano_atual, "Ano atual").otherwise(col("Ano")).cast("string")) \
    .withColumn("AnoFiscal",when(col("MesNum") >= mes_inicio_ano_fiscal, concat_ws("-", col("Ano"), (col("Ano") + 1))).otherwise(concat_ws("/", (col("Ano") - 1), col("Ano"))).cast("string")) \
    .withColumn("MesFiscalNum", when(col("MesNum")> mes_inicio_ano_fiscal-1, col("MesNum")-mes_inicio_ano_fiscal+1).otherwise(col("MesNum")+12-mes_inicio_ano_fiscal+1).cast("int")) \
    .withColumn("MesFiscalNome", col("MesNome").cast("string")) \
    .withColumn("MesFiscalNomeAbrev", col("MesNomeAbrev").cast("string")) \
    .withColumn("TrimestreFiscal", concat(lit("T"),(floor((col("MesFiscalNum") - 1) / 3) + 1)).cast("string")) 


# Salva o dataframe como tabela Delta
calendario_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{nome_lakehouse}.{nome_tabela}")

# Exibe os dados carregados
calendario_df = spark.sql(f"SELECT * FROM {nome_lakehouse}.{nome_tabela} ORDER BY Data ASC")
display(calendario_df)

```

8. Ao final verá a tabela gerada. Feche o notebook e retorne ao lakehouse;
9. Dentro do lakehouse clique em New semantic model;
10. Coloque o nome do modelo como semantic_model;
11. Selecione as colunas e confirme;
12. O modelo será aberto no navegador onde você poderia fazer as mudanças manuais, editar relacionamentos, ordenar colunas, criar medidas, porém eu criei um script csharp para ordenar as colunas, colocar em pastas, relamente organizar todo o processo. Feche o modelo semântico;
13. A partir do workspace clique nos três pontinhos do modelo semântico e vá até Settings;
14. Vá até Server settings e copie a Connection string;
15. Abra o Tabular editor, vá em File > Open > From DB...
16. Cole a string copiada no campo Server;
17. Em authentication escolha WindowsIntegrated or Azure AD Login;
18. Forneça login e senha caso necessário;
19. Escolha o semantic_model e OK;
20. Copie o código abaixo e cole na janela C# Script e rode pressionando o botão de play;

```csharp
// Este script realiza as seguintes operações:
// 1. Faz a ordenação das colunas de texto pelas colunas numéricas
// 2. Organiza as colunas em pastas por granularidade
// 3. Aplica o formato short date para colunas do tipo data
// 4. Remove agregações das colunas numéricas
// 5. Marca a tabela como tabela de data

// Acessa a tabela calendario. O nome da tabela é case-sensitive
var calendario = Model.Tables["calendario"];  

// Cria um mapeamento das colunas de texto e suas respectivas colunas numéricas para ordenação
var columnPairs = new Dictionary<string, string>
{
    {"AnoAtual", "Ano"}, 
    {"DataAtual", "Data"}, 
    {"DiaSemanaNome", "DiaSemanaNum"}, 
    {"DiaSemanaNomeAbrev", "DiaSemanaNum"},
    {"MesNome", "MesNum"},
    {"MesNomeAbrev", "MesNum"},
    {"SemanaAnoIsoNome", "SemanaAnoIsoNum"},
    {"SemanaAtual", "SemanaAnoIsoNum"},
    {"TrimestreAnoNome", "TrimestreAnoNum"},
    {"TrimestreAtual", "TrimestreAnoNum"},
    {"MesAnoNome", "MesAnoNum"}, 
    {"MesAtual", "MesAnoNum"}, 
    {"MesFiscalNome", "MesFiscalNum"},
    {"MesFiscalNomeAbrev", "MesFiscalNum"}
};

// Aplica a ordenação para cada coluna de texto
foreach (var pair in columnPairs)
{
    var textColumn = calendario.Columns[pair.Key];  // Coluna de texto
    var sortColumn = calendario.Columns[pair.Value];  // Coluna numérica correspondente

    // Verifica se ambas as colunas existem e aplica a ordenação
    if (textColumn != null && sortColumn != null)
    {
        textColumn.SortByColumn = sortColumn;  // Ordena a coluna de texto pela coluna numérica
    }
}

// Dicionário para associar as colunas às pastas correspondentes
var displayFolders = new Dictionary<string, string[]>
{
    { "Ano", new[] { "Ano", "AnoAtual", "AnoFiscal", "AnoIso" } },
    { "Dia", new[] { "Data", "DataAtual", "Dia", "DiaSemanaNome", "DiaSemanaNomeAbrev", "DiaSemanaNum" } },
    { "Dias Úteis / Feriados", new[] { "E_DiaUtil", "E_Feriado", "E_FinalSemana", "Feriado" } },
    { "Meses", new[] { "MesAnoNome", "MesAnoNum", "MesAtual", "MesFiscalNome", "MesFiscalNomeAbrev", "MesFiscalNum", "MesNome", "MesNomeAbrev", "MesNum" } },
    { "Semanas", new[] { "SemanaAnoIsoNome", "SemanaAnoIsoNum", "SemanaAtual", "SemanaIsoNum" } },
    { "Trimestres", new[] { "TrimestreAnoNome", "TrimestreAnoNum", "TrimestreAtual", "TrimestreFiscal", "TrimestreNum" } }
};

// Itera sobre as pastas e aplica o DisplayFolder a cada coluna associada
foreach (var folder in displayFolders)
{
    var folderName = folder.Key;
    var columns = folder.Value;

    foreach (var columnName in columns)
    {
        var column = calendario.Columns[columnName];
        if (column != null)
        {
            column.DisplayFolder = folderName; // Atribue as colunas à pasta correspondente
        }
    }
}

// Desabilitar agregações para todas as colunas da tabela
foreach (var column in calendario.Columns)
{
    column.SummarizeBy = AggregateFunction.None;  // Desabilitar agregação
}

// Definir o formato para as colunas do tipo Data
var dateColumns = new[] { "Data" };  // Colunas que contêm datas
foreach (var columnName in dateColumns)
{
    var column = calendario.Columns[columnName];
    if (column != null)
    {
        column.FormatString = "Short Date";  // Aplica o formato de data curta
    }
}

// Marcar como uma tabela de data
calendario.DataCategory = "Time";
calendario.Columns["Data"].IsKey = true; 

```

21. Pressione Ctrl+S e feche o Tabular Editor;
22. Retorne ao modelo semântico e sua tabela calendario estará devidamente configurada;




