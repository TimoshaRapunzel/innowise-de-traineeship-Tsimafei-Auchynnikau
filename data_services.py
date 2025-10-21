import io
import ijson

STUDENT_COUNT_BY_ROOM = """
                        SELECT r."name" AS room_name, COUNT(s."id") AS students_count
                        FROM "Rooms" r
                                 LEFT JOIN "Students" s ON r."id" = s."room"
                        GROUP BY r."name" 
                        ORDER BY students_count DESC; 
                        """
TOP_5_LOWEST_AVG_AGE = """
                       SELECT r."name" AS room_name, AVG(EXTRACT(YEAR FROM AGE(s."birthday"))) AS avg_age
                       FROM "Rooms" r 
                                JOIN "Students" s ON r."id" = s."room"
                       GROUP BY r."name" 
                       ORDER BY avg_age ASC LIMIT 5; 
                       """
TOP_5_BIGGEST_AGE_DIFF = """
                         SELECT r."name"                                    AS room_name,
                                (MAX(EXTRACT(YEAR FROM AGE(s."birthday"))) - 
                                 MIN(EXTRACT(YEAR FROM AGE(s."birthday")))) AS age_diff
                         FROM "Rooms" r 
                                  JOIN "Students" s ON r."id" = s."room"
                         GROUP BY r."name" 
                         HAVING COUNT(s."id") > 1 
                         ORDER BY age_diff DESC LIMIT 5; 
                         """
ROOMS_WITH_MIXED_SEX = """
                       SELECT r."name" AS room_name 
                       FROM "Rooms" r 
                                JOIN "Students" s ON r."id" = s."room"
                       GROUP BY r."name" 
                       HAVING COUNT(DISTINCT s."sex") > 1; 
                       """


def _read_json_stream(file_path: str):
    try:
        with open(file_path, 'rb') as f:
            yield from ijson.items(f, 'item')
    except FileNotFoundError:
        print(f"Ошибка: Файл '{file_path}' не найден.")
        return iter(())


class DataService:
    def __init__(self):
        pass

    def load_data_from_json(self, cursor, rooms_path: str, students_path: str) -> bool:
        try:
            rooms_data = list(_read_json_stream(rooms_path))
            students_data = list(_read_json_stream(students_path))

            if not rooms_data or not students_data:
                print("Загрузка прервана: один из файлов пуст или не найден")
                return False

            if not cursor: return False

            print("Очистка таблиц")
            cursor.execute('TRUNCATE TABLE "Students", "Rooms" RESTART IDENTITY CASCADE;')

            print(f"Быстрая загрузка {len(rooms_data)} записей в 'Rooms'")
            room_buffer = self._prepare_buffer(rooms_data, ['id', 'name'])
            cursor.copy_expert('COPY "Rooms" ("id", "name") FROM STDIN', room_buffer)

            print(f"Быстрая загрузка {len(students_data)} записей в 'Students'")
            student_buffer = self._prepare_buffer(students_data, ['id', 'birthday', 'name', 'room', 'sex'])
            cursor.copy_expert('COPY "Students" ("id", "birthday", "name", "room", "sex") FROM STDIN',
                               student_buffer)

            print("Данные успешно загружены")
            return True
        except Exception as e:
            print(f"Ошибка во время загрузки данных: {e}")
            raise e

    def _prepare_buffer(self, data: list, columns: list) -> io.StringIO:
        buffer = io.StringIO()
        for row in data:
            values = [str(row.get(col, r'\N')).replace('\n', '\\n') for col in columns]
            buffer.write('\t'.join(values) + '\n')
        buffer.seek(0)
        return buffer

    def run_analysis_queries(self, cursor) -> dict:
        results = {}
        queries = {
            'student_count': STUDENT_COUNT_BY_ROOM,
            'lowest_avg_age': TOP_5_LOWEST_AVG_AGE,
            'biggest_age_diff': TOP_5_BIGGEST_AGE_DIFF,
            'mixed_sex_rooms': ROOMS_WITH_MIXED_SEX
        }
        if not cursor: raise ConnectionError("Не удалось подключиться к БД.")

        for name, sql in queries.items():
            cursor.execute(sql)
            colnames = [desc[0] for desc in cursor.description]
            results[name] = [dict(zip(colnames, row)) for row in cursor.fetchall()]
        print("Запросы выполнены.")
        return results