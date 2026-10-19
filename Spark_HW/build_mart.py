import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def main():
    # Проверяем аргументы и выбираем строки из массива
    if len(sys.argv) < 3:
        print("Ошибка: Недостаточно аргументов.")
        print("Использование: spark-submit build_mart.py <path_to_data_folder> <path_to_output_folder>")
        sys.exit(1)

    input_path = sys.argv[1]
    output_path = sys.argv[2]


    spark = SparkSession.builder \
        .appName("Boston Crime Data Mart") \
        .getOrCreate()

    crimes_file = f"{input_path}/crime.csv"
    codes_file = f"{input_path}/offense_codes.csv"

    # 1.Загрузка данных
    # Для crime.csv заголовки точно есть (header=True)
    crimes_df = spark.read.csv(crimes_file, header=True, inferSchema=True)

    # Для offense_codes.csv читаем БЕЗ заголовков (header=False)
    # Spark сам назовет колонки _c0, _c1 и т.д.
    codes_raw = spark.read.csv(codes_file, header=False, inferSchema=True)

    # Принудительно даем имена колонкам справочника
    codes_df = codes_raw.withColumnRenamed("_c0", "CODE").withColumnRenamed("_c1", "NAME")

    # 2.Очистка данных инцидентов
    crimes_cleaned = crimes_df.dropDuplicates(["INCIDENT_NUMBER"]).filter(F.col("DISTRICT").isNotNull())

    # Дедупликация справочника кодов (Выбираем уникальные коды, берем любое из названий)
    code_window = Window.partitionBy("CODE").orderBy("NAME")
    codes_cleaned = codes_df \
        .withColumn("row_num", F.row_number().over(code_window)) \
        .filter(F.col("row_num") == 1) \
        .drop("row_num")

    # Выделяем первую часть NAME до разделителя "-" и убираем пробелы
    codes_cleaned = codes_cleaned.withColumn(
        "crime_type",
        F.trim(F.split(F.col("NAME"), "-").getItem(0))
    )

    # 3.Объединение таблиц (LEFT JOIN)
    # Приводим типы кодов к общему типу (строке), чтобы избежать проблем с типами данных при Join
    crimes_cleaned = crimes_cleaned.withColumn("OFFENSE_CODE_STR", F.col("OFFENSE_CODE").cast("string"))
    codes_cleaned = codes_cleaned.withColumn("CODE_STR", F.col("CODE").cast("string"))

    enriched_crimes = crimes_cleaned.join(
        codes_cleaned,
        crimes_cleaned.OFFENSE_CODE_STR == codes_cleaned.CODE_STR,
        "left"
    )

    # Если код преступления не нашелся в справочнике, пишем "OTHERS", чтобы датасет не стал пустым!
    enriched_crimes = enriched_crimes.withColumn(
        "crime_type",
        F.coalesce(F.col("crime_type"), F.lit("OTHERS"))
    )

    # Кэшируем данные
    enriched_crimes.cache()

    # crimes_total — общее количество преступлений в этом районе
    base_metrics = enriched_crimes.groupBy("DISTRICT").agg(
        F.count("INCIDENT_NUMBER").alias("crimes_total")
    )

    # средние координаты (исключаем пустые и нулевые значения)
    geo_df = enriched_crimes.filter((F.col("Lat").isNotNull()) & (F.col("Long").isNotNull()) & (F.col("Lat") != 0.0) & (F.col("Long") != 0.0))
    avg_geo = geo_df.groupBy("DISTRICT").agg(
        F.avg("Lat").alias("lat"),
        F.avg("Long").alias("lng")
    )

    # crimes_monthly — медиана числа преступлений в месяц (Месяц = YEAR + MONTH)
    monthly_counts = enriched_crimes.groupBy("DISTRICT", "YEAR", "MONTH").agg(
        F.count("INCIDENT_NUMBER").alias("crimes_in_month")
    )
    median_metrics = monthly_counts.groupBy("DISTRICT").agg(
        F.percentile_approx("crimes_in_month", 0.5).alias("crimes_monthly")
    )

    # frequent_crime_types — три самых частых crime_type в порядке убывания частоты
    crime_type_counts = enriched_crimes.groupBy("DISTRICT", "crime_type").agg(
        F.count("INCIDENT_NUMBER").alias("type_count")
    )

    type_window = Window.partitionBy("DISTRICT").orderBy(F.col("type_count").desc(), F.col("crime_type"))
    top_3_types = crime_type_counts.withColumn("rank", F.row_number().over(type_window)).filter(F.col("rank") <= 3)

    frequent_crimes = top_3_types.groupBy("DISTRICT").agg(
        F.concat_ws(", ", F.collect_list("crime_type")).alias("frequent_crime_types")
    )

    # Сборка финальной витрины
    final_mart = base_metrics \
        .join(median_metrics, "DISTRICT", "left") \
        .join(frequent_crimes, "DISTRICT", "left") \
        .join(avg_geo, "DISTRICT", "left") \
        .select(
            F.col("DISTRICT").alias("district"),
            F.col("crimes_total"),
            F.col("crimes_monthly"),
            F.col("frequent_crime_types"),
            F.col("lat"),
            F.col("lng")
        )

    # 4. Сохранение в файл Parquet
    final_mart.coalesce(1).write.mode("overwrite").parquet(output_path)

    enriched_crimes.unpersist()
    spark.stop()
    print("Витрина успешно построена и сохранена!")

if __name__ == "__main__":
    main()
