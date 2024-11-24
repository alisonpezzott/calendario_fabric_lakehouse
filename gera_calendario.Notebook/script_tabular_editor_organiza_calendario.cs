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



