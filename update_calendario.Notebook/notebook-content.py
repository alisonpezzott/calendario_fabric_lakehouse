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

# packages
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

#parameters
start_date = "2021-01-01"
end_date = datetime.now().strftime("%Y-12-31")
culture = "pt-BR"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#function to gen fixed holidays
def generate_holidays(year_ref):
    fixed_holidays = {
        "Confraternização Universal": datetime(year_ref, 1, 1),
        "Aniversário de São Paulo": datetime(year_ref, 1, 25),
        "Tiradentes": datetime(year_ref, 4, 21),
        "Dia do Trabalho": datetime(year_ref, 5, 1),
        "Revolução Constitucionalista": datetime(year_ref, 7, 9),
        "Independência do Brasil": datetime(year_ref, 9, 7),
        "Nossa Senhora Aparecida": datetime(year_ref, 10, 12),
        "Finados": datetime(year_ref, 11, 2),
        "Proclamação da República": datetime(year_ref, 11, 15),
        "Consciência Negra": datetime(year_ref, 11, 20),
        "Véspera de Natal": datetime(year_ref, 12, 24),
        "Natal": datetime(year_ref, 12, 25),
        "Véspera de Ano Novo": datetime(year_ref, 12, 31)
    }
    return fixed_holidays

def custom_mod(x, y):
    m = x % y
    return m + y if m < 0 else m

def generate_easter(year_ref):
    easter_ordinal = math.ceil(
        ((datetime(year_ref, 4, 1).toordinal() - 693594) / 7)
        + (custom_mod(19 * custom_mod(year_ref, 19) - 7, 30) * 0.14)
    ) * 7 - 6 + 693594
    
    easter = datetime.fromordinal(int(easter_ordinal))
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
def generate_all_holidays(year_ref):
    fixed_holidays = generate_holidays(year_ref)
    movable_holidays = generate_easter(year_ref)
    holidays = {**fixed_holidays, **movable_holidays}
    return holidays

# Generate holidays for a range of years
def generate_holidays_for_range(start_year, end_year):
    all_holidays = {}
    for year_ref in range(start_year, end_year + 1):
        all_holidays[year_ref] = generate_all_holidays(year_ref)
    return all_holidays

# Generate holidays for the range of years
all_holidays = generate_holidays_for_range(int(start_date[:4]), int(end_date[:4]))
print(all_holidays)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Generate a DataFrame with all holidays
holidays = []
for year, holidays_dict in all_holidays.items():
    for holiday, date in holidays_dict.items():
        holidays.append((holiday, date))
holidays_df = spark.createDataFrame(holidays, ["holiday", "date"])
holidays_df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Generate a DataFrame with all days in the range
days_df = spark.createDataFrame(
    [(start_date, end_date)], ["start_date", "end_date"]
).selectExpr("sequence(to_date(start_date), to_date(end_date), interval 1 day) as date") \
 .selectExpr("explode(date) as date")
days_df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Join the two DataFrames to mark holidays
calendar_df = days_df.join(holidays_df, days_df.date == holidays_df.date, "left").select(days_df.date, holidays_df.holiday)
calendar_df = calendar_df.withColumn("is_holiday", col("holiday").isNotNull())
calendar_df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import year

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create other columns
calendar_df_full = calendar_df.withColumn("year", year(col("date")).cast("int")) \
    .withColumn("day", date_format(col("date"), "d").cast("int")) \
    .withColumn("month_name", date_format(col("date"), "MMMM")) \
    .withColumn("month_name_short", date_format(col("date"), "MMM")) \
    .withColumn("month_number", month(col("date")).cast("int")) \
    .withColumn("day_of_week", date_format(col("date"), "EEEE")) \
    .withColumn("day_of_week_short", date_format(col("date"), "EEE")) \
    .withColumn("week_starting_monday", date_format(next_day(col("date"), "Monday"), "u").cast("int")) \
    .withColumn("day of week_number", date_format(col("date"), "u").cast("int")) \
    .withColumn("is_weekend", date_format(col("date"), "u").isin([6, 7]))
calendar_df_full.show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create a dictionary for month names in pt-BR
month_names_ptbr = {
    1: "Janeiro", 2: "Fevereiro", 3: "Março", 4: "Abril", 5: "Maio", 6: "Junho",
    7: "Julho", 8: "Agosto", 9: "Setembro", 10: "Outubro", 11: "Novembro", 12: "Dezembro"
}

# Create a dictionary for short month names in pt-BR by extracting the first three characters
month_names_short_ptbr = {k: v[:3] for k, v in month_names_ptbr.items()}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create UDFs to map month numbers to pt-BR names
month_name_udf = udf(lambda x: month_names_ptbr[x], StringType())
month_name_short_udf = udf(lambda x: month_names_short_ptbr[x], StringType())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create other columns
calendar_df_full = calendar_df.withColumn("year", year(col("date")).cast("int")) \
    .withColumn("day", date_format(col("date"), "d").cast("int")) \
    .withColumn("month_number", month(col("date")).cast("int")) \
    .withColumn("month_name", month_name_udf(col("month_number"))) \
    .withColumn("month_name_short", month_name_short_udf(col("month_number")))
calendar_df_full.show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
