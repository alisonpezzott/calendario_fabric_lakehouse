# packages
from pyspark.sql.functions import *
from datetime import datetime, timedelta
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, to_date, month, udf

# CELL

#parameters
start_date = "2021-01-01"
end_date = datetime.now().strftime("%Y-12-31")
culture = "pt-BR"

# CELL

#function to gen fixed holidays
def generate_holidays(year):
    fixed_holidays = {
        "Confraternização Universal": datetime(year, 1, 1),
        "Aniversário de São Paulo": datetime(year, 1, 25),
        "Tiradentes": datetime(year, 4, 21),
        "Dia do Trabalho": datetime(year, 5, 1),
        "Revolução Constitucionalista": datetime(year, 7, 9),
        "Independência do Brasil": datetime(year, 9, 7),
        "Nossa Senhora Aparecida": datetime(year, 10, 12),
        "Finados": datetime(year, 11, 2),
        "Proclamação da República": datetime(year, 11, 15),
        "Consciência Negra": datetime(year, 11, 20),
        "Véspera de Natal": datetime(year, 12, 24),
        "Natal": datetime(year, 12, 25),
        "Véspera de Ano Novo": datetime(year, 12, 31)
    }
    return fixed_holidays

# Function to generate movable holidays
def generate_easter(year):
    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day = ((h + l - 7 * m + 114) % 31) + 1
    easter = datetime(year, month, day)
    movable_holidays = {
        "Segunda-feira de Carnaval": easter - timedelta(days=48),
        "Terça-feira de Carnaval": easter - timedelta(days=47),
        "Quarta-feira de Cinzas": easter - timedelta(days=46),
        "Sexta-feira Santa": easter - timedelta(days=2),
        "Páscoa": easter,
        "Corpus Christi": easter + timedelta(days=60)
    }
    return movable_holidays

# Generate holidays for a given year
def generate_all_holidays(year):
    fixed_holidays = generate_holidays(year)
    movable_holidays = generate_easter(year)
    holidays = {**fixed_holidays, **movable_holidays}
    return holidays

# Generate holidays for a range of years
def generate_holidays_for_range(start_year, end_year):
    all_holidays = {}
    for year in range(start_year, end_year + 1):
        all_holidays[year] = generate_all_holidays(year)
    return all_holidays

# Generate holidays for the range of years
all_holidays = generate_holidays_for_range(int(start_date[:4]), int(end_date[:4]))
print(all_holidays)

# CELL

# Generate a DataFrame with all holidays
holidays = []
for year, holidays_dict in all_holidays.items():
    for holiday, date in holidays_dict.items():
        holidays.append((holiday, date))
holidays_df = spark.createDataFrame(holidays, ["holiday", "date"])
holidays_df.show()

# CELL

# Generate a DataFrame with all days in the range
days_df = spark.createDataFrame(
    [(start_date, end_date)], ["start_date", "end_date"]
).selectExpr("sequence(to_date(start_date), to_date(end_date), interval 1 day) as date") \
 .selectExpr("explode(date) as date")
days_df.show()

# CELL

# Join the two DataFrames to mark holidays
calendar_df = days_df.join(holidays_df, days_df.date == holidays_df.date, "left").select(days_df.date, holidays_df.holiday)
calendar_df = calendar_df.withColumn("is_holiday", col("holiday").isNotNull())
calendar_df.show()



# Create other columns
calendar_df_full = calendar_df.withColumn("year", year(col("date")).cast("int")) \
    .withColumn("day", date_format(col("date"), "d").cast("int")) \
    .withColumn("month_number", month(col("date")).cast("int")) \
    .withColumn("month_name", date_format(col("date"), "MMMM")) \
    .withColumn("month_name_short", date_format(col("date"), "MMM"))
calendar_df_full.show()

# CELL






