import psycopg2


class DatabaseConnector:
    def __init__(self, db_config):
        self.config = db_config
        self.connection = None
        self.cursor = None

    def __enter__(self):
        try:
            self.connection = psycopg2.connect(**self.config)
            self.cursor = self.connection.cursor()
            print("Соединение с базой данных установлено")
            return self.cursor
        except psycopg2.Error as e:
            print(f"Ошибка подключения к БД: {e}")
            raise e

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.connection:
            if exc_type:
                print(f"Произошла ошибка: {exc_val}. Откат транзакции")
                self.connection.rollback()
            else:
                print("Фиксация транзакции")
                self.connection.commit()

            if self.cursor:
                self.cursor.close()
            self.connection.close()
            print("Соединение с базой данных закрыто")