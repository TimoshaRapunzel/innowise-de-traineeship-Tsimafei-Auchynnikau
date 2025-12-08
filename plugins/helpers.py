import pandas as pd
import re
from pathlib import Path
from glob import glob
# --- ИСПРАВЛЕНО: Добавлен импорт MongoHook ---
from airflow.providers.mongo.hooks.mongo import MongoHook


# -----------------------
# CSV Processing Helpers
# -----------------------

def check_if_file_empty(path: str) -> bool:
    try:
        df = pd.read_csv(path)
        return df.empty
    except pd.errors.EmptyDataError:
        # Файл пустой (содержит только заголовки или 0 байт)
        return True


def read_csv(path: str) -> pd.DataFrame:
    # keep_default_na=False гарантирует, что пустые строки не превратятся сразу в NaN при чтении,
    # но лучше обрабатывать это явно функциями ниже.
    return pd.read_csv(path)


def save_csv(df: pd.DataFrame, path: str):
    df.to_csv(path, index=False)


def replace_nulls(df: pd.DataFrame) -> pd.DataFrame:
    """
    Заменяет все варианты пропусков (NaN, null, nan, пустоту) на "-"
    """
    # 1. Заполняем системные NaN/None
    df = df.fillna("-")

    # 2. Заменяем строковые вариации "null", "nan" и пустые строки
    # Используем replace с списком для надежности
    df.replace(["null", "nan", "NaN", ""], "-", inplace=True)

    return df


def sort_by_date(df: pd.DataFrame) -> pd.DataFrame:
    if "created_date" in df.columns:
        df["created_date"] = pd.to_datetime(df["created_date"], errors='coerce')
        df = df.sort_values(by="created_date")
    return df


def clean_content_column(df: pd.DataFrame) -> pd.DataFrame:
    if "content" in df.columns:
        # Принудительно приводим к строке
        df["content"] = df["content"].astype(str)

        # Убираем все лишние символы (оставляем буквы, цифры, знаки препинания)
        df["content"] = df["content"].apply(
            lambda x: re.sub(r"[^a-zA-Zа-яА-Я0-9\s.,!?;:()-]", "", x)
        )

        # ВАЖНО: Если после очистки строка стала пустой (например, были одни смайлики),
        # Pandas может сохранить это как NaN при следующей перезаписи.
        # Поэтому принудительно меняем пустоту на "-"
        df["content"] = df["content"].replace(r'^\s*$', "-", regex=True)
        df["content"] = df["content"].replace(["nan", "NaN", ""], "-")

    return df


# -----------------------
# File helpers
# -----------------------

def get_all_raw_files(raw_dir: str):
    return glob(f"{raw_dir}/*.csv")


def get_processed_file_path(raw_file_path: str, processed_dir: str):
    file_name = Path(raw_file_path).name
    return str(Path(processed_dir) / file_name)


# -----------------------
# MongoDB Helper
# -----------------------

def load_csv_to_mongo(csv_path: str, mongo_conn_id: str, db_name: str, collection_name: str):
    """
    Загружает один CSV-файл в MongoDB, используя Airflow Connection ID.
    """
    # Читаем файл. keep_default_na=False помогает не плодить NaN при загрузке
    df = pd.read_csv(csv_path, keep_default_na=False)

    # На всякий случай, перед загрузкой еще раз меняем пустоты на "-",
    # если вдруг read_csv их создал
    df.replace(["", "nan", "NaN", "null"], "-", inplace=True)
    df = df.fillna("-")

    records = df.to_dict(orient="records")

    if not records:
        print(f"Skipping empty file: {csv_path}")
        return

    print(f"Connecting to MongoDB via hook (conn_id={mongo_conn_id})...")
    hook = MongoHook(mongo_conn_id=mongo_conn_id)
    client = hook.get_conn()
    db = client[db_name]
    collection = db[collection_name]

    print(f"Inserting {len(records)} records from {csv_path} into {db_name}.{collection_name}")
    collection.insert_many(records)