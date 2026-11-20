"""ETL-пайплайн для построения витрин книжных рекомендаций.

Пример запуска:

    spark-submit \
      --packages org.apache.hadoop:hadoop-aws:3.3.4 \
      project.py \
      --start_date 2024-11-26 \
      --end_date 2025-01-01 \
      --geo_id "Москва, Россия" \
      --columns usage_platform_ru genre \
      --audition_path s3a://s3-ds-source/audition.parquet \
      --content_path s3a://s3-ds-source/content.parquet \
      --output_path s3a://s3-ds-20260726-c5944c8601/results/S24_project_data
"""

import argparse
import logging
import os
from datetime import date
from typing import Dict, Iterable, List

from dotenv import load_dotenv
from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F


LOGGER = logging.getLogger("book_recommendations")
MART_FILE_NAMES = {
    "user_content_mart": "user_content_mart.parquet",
    "user_genre_mart": "user_genre_mart.parquet",
    "user_mart": "user_mart.parquet",
    "content_mart": "content_mart.parquet",
    "daily_user_mart": "daily_user_mart.parquet",
}


def parse_iso_date(value: str) -> date:
    """Проверяет формат даты аргумента командной строки."""
    try:
        return date.fromisoformat(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError(
            f"Некорректная дата {value!r}; ожидается формат YYYY-MM-DD"
        ) from error


def parse_args(args: Iterable[str] = None) -> argparse.Namespace:
    """Парсинг аргументов командной строки с проверками формата вводимых переменных"""
    parser = argparse.ArgumentParser(
        description="Построение пяти витрин для рекомендательной системы книг."
    )
    parser.add_argument("--start_date", required=True, type=parse_iso_date)
    parser.add_argument("--end_date", required=True, type=parse_iso_date)
    parser.add_argument("--geo_id", required=True)
    parser.add_argument(
        "--columns",
        required=True,
        nargs="+",
    )
    parser.add_argument("--audition_path", required=True)
    parser.add_argument("--content_path", required=True)
    parser.add_argument("--output_path", required=True)

    parsed_args = parser.parse_args(args)
    if parsed_args.start_date > parsed_args.end_date:
        parser.error("start_date не может быть позже end_date")

    return parsed_args


def create_spark_session() -> SparkSession:
    """Создание Spark-сессии."""
    builder = (
        SparkSession.builder.appName("book_recommendations_marts")
        # настройки Hadoop S3A
        .config("spark.hadoop.fs.s3a.endpoint", "https://storage.yandexcloud.net")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        # настройки Spark SQL
        .config("spark.sql.adaptive.enabled", "true")
    )

    access_key = os.environ.get("S3_ACCESS_KEY")
    secret_key = os.environ.get("S3_SECRET_KEY")
    if access_key is None or secret_key is None:
        raise ValueError(
            "S3_ACCESS_KEY и S3_SECRET_KEY должны быть заданы одновременно"
        )
    builder = (
        builder.config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
    )

    return builder.getOrCreate()


def read_and_prepare_data(
    spark: SparkSession,
    audition_path: str,
    content_path: str,
    start_date: date,
    end_date: date,
    geo_id: str,
) -> DataFrame:
    """Читает, очищает, объединяет и фильтрует исходные данные.
    
    Args:
        spark (SparkSession): активная Spark-сессия
        audition_path (str): путь к датасету audition
        content_path (str): путь к датасету content
        start_date (date): дата начала рассматриваемого периода 
        end_date (date): дата конца рассматриваемого периода
        geo_id (str): id рассматриваемый регион
    """
    # преобразование формата: строки -> даты
    # фильтрация аномалий: 
    # - длительность сессий от 0 до 24ч, 
    # - длительность контента >=0
    # чистка от дублей
    audition = (
        spark.read.parquet(audition_path)
        .withColumn(
            "msk_business_dt",
            F.to_date("msk_business_dt_str", "yyyy-MM-dd"),
        )
        .drop("msk_business_dt_str")
        .filter((F.col("hours").between(0, 24)) & (F.col("hours_sessions_long").between(0, 24)))
        .dropDuplicates()
    )
    content = (
        spark.read.parquet(content_path)
        .filter(F.col("main_content_duration_hours").isNotNull())
        .filter(F.col("main_content_duration_hours") >= 0)
        .dropDuplicates()
    )

    # объединение датасетов
    # - сплит строковых значений в published_topic_title_list
    # - фильтрация по датам и геопозиции
    prepared = (
        audition.join(content, on="main_content_id", how="left")
        .withColumn(
            "published_topic_title_list",
            F.split(
                F.regexp_replace("published_topic_title_list", r"^\['|'\]$", ""),
                r"',\s*'"
            )
        )
        .filter(
            F.col("msk_business_dt").between(
                F.lit(start_date.isoformat()).cast("date"),
                F.lit(end_date.isoformat()).cast("date"),
            )
        )
        .filter(F.col("usage_geo_id") == geo_id)
    )
    return prepared


def build_all_marts(df: DataFrame) -> Dict[str, DataFrame]:
    """Строит пять витрин и возвращает их по именам."""
    # ------------------------------------------------------------
    # Витрина взаимодействий пользователя с контентом.
    user_content_mart = (
        df.groupBy("puid", "main_content_id")
        .agg(
            F.sum("hours").alias("total_hours"),
            F.count("audition_id").alias("sessions_count"),
            F.first("main_content_duration_hours", ignorenulls=True).alias(
                "content_duration_hours"
            ),
            F.max("msk_business_dt").alias("last_listening_date"),
            F.min("msk_business_dt").alias("first_listening_date"),
            F.avg("hours").alias("avg_session_hours"),
        )
        .withColumn(
            "finished_percent",
            # приведение к диапазону [0, 100] %
            F.least(F.lit(1.0), F.greatest(F.lit(0.0), F.col("total_hours") / F.col("content_duration_hours"))) * 100,
        )
        .withColumn(
            "is_completed",
            F.when(F.col("finished_percent") >= 100, 1).otherwise(0),
        )
        .withColumn(
            "days_spent",
            F.datediff("last_listening_date", "first_listening_date") + 1,
        )
        .select(
            "puid",
            "main_content_id",
            "total_hours",
            "sessions_count",
            "finished_percent",
            "is_completed",
            "last_listening_date",
            "days_spent",
            "avg_session_hours",
        )
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    # ------------------------------------------------------------
    # Витрина пользовательских жанров.
    exploded = df.withColumn(
        "genre", F.explode("published_topic_title_list")
    )
    user_genre_stats = (
        exploded.join(
            user_content_mart.select("puid", "main_content_id", "is_completed"),
            on=["puid", "main_content_id"],
            how="left",
        )
        .groupBy("puid", "genre")
        .agg(
            F.sum("hours").alias("total_hours"),
            F.count("audition_id").alias("sessions_count"),
            F.countDistinct(
                F.when(F.col("is_completed") == 1, F.col("main_content_id"))
            ).alias("completed_count"),
            F.countDistinct(
                F.when(F.col("adult_content_flg"), F.col("main_content_id"))
            ).alias("adult_count"),
            F.countDistinct(
                F.when(F.col("kids_content_flg"), F.col("main_content_id"))
            ).alias("kids_count"),
        )
    )
    genre_window = Window.partitionBy("puid").orderBy(
        F.col("total_hours").desc()
    )
    user_genre_mart = (
        user_genre_stats.withColumn(
            "genre_rank", F.dense_rank().over(genre_window)
        )
        .select(
            "puid",
            "genre",
            "total_hours",
            "sessions_count",
            "completed_count",
            "genre_rank",
            "adult_count",
            "kids_count",
        )
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    def favorite_by_hours(
        source_df: DataFrame, value_column: str, result_column: str
    ) -> DataFrame:
        value_window = Window.partitionBy("puid").orderBy(
            F.col("value_hours").desc(), F.col(value_column).asc()
        )
        return (
            source_df.filter(F.col(value_column).isNotNull())
            .groupBy("puid", value_column)
            .agg(F.sum("hours").alias("value_hours"))
            .withColumn("row_number", F.row_number().over(value_window))
            .filter(F.col("row_number") == 1)
            .select("puid", F.col(value_column).alias(result_column))
        )
    # ------------------------------------------------------------
    # Витрина пользователей.
    user_base = df.groupBy("puid").agg(
        F.sum("hours").alias("total_hours"),
        F.countDistinct("msk_business_dt").alias("active_days"),
        F.countDistinct("main_content_id").alias("unique_content_count"),
        F.max("msk_business_dt").alias("last_date"),
        F.countDistinct(
            F.when(F.col("adult_content_flg"), F.col("main_content_id"))
        ).alias("adult_content_count"),
        F.countDistinct(
            F.when(F.col("kids_content_flg"), F.col("main_content_id"))
        ).alias("kids_content_count"),
    )
    user_completion = user_content_mart.groupBy("puid").agg(
        F.avg("is_completed").alias("completion_rate")  # доля завершенных произведений от 0 до 1
    )
    user_genres = user_genre_mart.groupBy("puid").agg(
        F.countDistinct("genre").alias("unique_genres_count")
    )
    favorite_genre_window = Window.partitionBy("puid").orderBy(
        F.col("total_hours").desc(), F.col("genre").asc()
    )
    favorite_genre = (
        user_genre_mart.withColumn(
            "row_number", F.row_number().over(favorite_genre_window)
        )
        .filter(F.col("row_number") == 1)
        .select("puid", F.col("genre").alias("favorite_genre"))
    )
    favorite_content_type = favorite_by_hours(
        df, "main_content_type", "favorite_content_type"
    )
    favourite_platform = favorite_by_hours(
        df, "usage_platform_ru", "favourite_platform"
    )
    user_mart = (
        user_base.join(user_genres, on="puid", how="left")
        .join(user_completion, on="puid", how="left")
        .join(favorite_genre, on="puid", how="left")
        .join(favorite_content_type, on="puid", how="left")
        .join(favourite_platform, on="puid", how="left")
        .select(
            "puid",
            "total_hours",
            "active_days",
            "unique_content_count",
            "unique_genres_count",
            "completion_rate",
            "favorite_genre",
            "favorite_content_type",
            "favourite_platform",
            "last_date",
            "adult_content_count",
            "kids_content_count",
        )
    )
    # ------------------------------------------------------------
    # Витрина контента.
    content_stats = df.groupBy("main_content_id").agg(
        F.countDistinct("puid").alias("unique_users"),
        F.sum("hours").alias("total_hours"),
        F.countDistinct("usage_platform_ru").alias("platforms_count"),
    )
    content_completion = user_content_mart.groupBy("main_content_id").agg(
        F.avg("finished_percent").alias("avg_finished_percent")
    )
    total_users = df.agg(F.countDistinct("puid").alias("total_users"))
    content_mart = (
        content_stats.join(
            content_completion, on="main_content_id", how="left"
        )
        .crossJoin(total_users)
        .withColumn(
            "popularity", F.col("unique_users") / F.col("total_users")
        )
        .select(
            "main_content_id",
            "unique_users",
            "total_hours",
            "avg_finished_percent",
            "platforms_count",
            "popularity",
        )
    )
    # ------------------------------------------------------------
    # Дневная витрина пользователей.
    daily_user_mart = (
        df.withColumn("session_date", F.to_date("msk_business_dt"))
        .groupBy("puid", "session_date")
        .agg(
            F.countDistinct("main_content_id").alias("content_count"),
            F.sum("hours").alias("hours_spent"),
            F.count("audition_id").alias("sessions_count"),
        )
        .withColumn("day_of_week", F.dayofweek("session_date"))
        .select(
            "puid",
            "session_date",
            "day_of_week",
            "content_count",
            "hours_spent",
            "sessions_count",
        )
    )

    return {
        "user_content_mart": user_content_mart,
        "user_genre_mart": user_genre_mart,
        "user_mart": user_mart,
        "content_mart": content_mart,
        "daily_user_mart": daily_user_mart,
    }


def save_marts(marts: Dict[str, DataFrame], output_path: str) -> None:
    """Записывает витрины в отдельные Parquet-каталоги."""
    base_path = output_path.rstrip("/")
    for mart_name, file_name in MART_FILE_NAMES.items():
        destination = f"{base_path}/{file_name}"
        LOGGER.info("Сохранение %s", mart_name)
        marts[mart_name].write.mode("overwrite").parquet(destination)
        LOGGER.info("Витрина %s сохранена", mart_name)


def main(cli_args: Iterable[str] = None) -> None:
    load_dotenv()
    args = parse_args(cli_args)
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    prepared_data = None
    marts = {}

    try:
        prepared_data = read_and_prepare_data(
            spark=spark,
            audition_path=args.audition_path,
            content_path=args.content_path,
            start_date=args.start_date,
            end_date=args.end_date,
            geo_id=args.geo_id,
        ).persist(StorageLevel.MEMORY_AND_DISK)

        marts = build_all_marts(prepared_data)
        save_marts(marts, args.output_path)
    finally:
        for mart_name in ("user_content_mart", "user_genre_mart"):
            if mart_name in marts:
                marts[mart_name].unpersist()
        if prepared_data is not None:
            prepared_data.unpersist()
        spark.stop()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    main()
