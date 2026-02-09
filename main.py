from config import DB_CONFIG
from db_connector import DatabaseConnector
from data_services import DataService
from formatter import OutputFormatter

def main():
    students_file_path = 'students.json'
    rooms_file_path = 'rooms.json'
    output_format = input('Введите формат вывода файла (json or xml): ')

    db_connector = DatabaseConnector(DB_CONFIG)
    data_service = DataService()
    formatter = OutputFormatter()
    results = None

    try:
        print("\nУстановка соединения с БД")
        with db_connector as cursor:
            print("\nЗагрузка данных")
            if not data_service.load_data_from_json(cursor, rooms_file_path, students_file_path):
                print("Программа завершена из-за ошибки на этапе загрузки.")
                return

            print("\nВыполнение аналитических запросов")
            results = data_service.run_analysis_queries(cursor)

    except Exception as e:
        print(f"Произошла ошибка при работе с базой данных: {e}")
        print("Работа программы прервана.")
        return

    if results is None:
        print("Не удалось получить результаты для сохранения.")
        return

    print(f"\nФорматирование и сохранение результата в {output_format.upper()}")
    try:
        formatted_output = formatter.format(results, output_format)
        output_filename = f"results.{output_format}"
        with open(output_filename, 'w', encoding='utf-8') as f:
            f.write(formatted_output)
        print(f"Результаты сохранены в файл: {output_filename}")
    except Exception as e:
        print(f"Ошибка при форматировании или сохранении файла: {e}")

    print("\nРабота программы завершена")

if __name__ == '__main__':
    main()