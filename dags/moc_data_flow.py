# # from airflow import DAG
# # from airflow.operators.python import PythonOperator
# # from datetime import datetime
# # import psycopg2
# # import random
# # import pendulum


# # # ฟังก์ชันสร้าง mock data และบันทึกลง PostgreSQL
# # def insert_mock_data():
# #     conn = psycopg2.connect(
# #         host="10.0.0.158",
# #         port=5432,
# #         user="admin",
# #         password="Admin_password_2568",
# #         database="mydatabase",
# #     )
# #     cursor = conn.cursor()

# #     # สร้าง mock data
# #     runTime = round(random.uniform(1, 8), 2)  # ชั่วโมงการทำงาน
# #     downTime = round(random.uniform(0, 2), 2)  # ชั่วโมงที่เครื่องหยุด
# #     quantity_ALL = random.randint(100, 500)  # จำนวนทั้งหมด
# #     quantity_FG = random.randint(80, quantity_ALL)  # จำนวนของดี
# #     speed_standard = random.randint(80, 100)  # speed มาตรฐาน
# #     speed_actual = random.randint(60, 100)  # speed ที่วัดได้จริง
# #     status = random.choice(["RUN", "STOP", "ERROR"])  # สถานะ
# #     local_tz = pendulum.timezone("Asia/Bangkok")
# #     TimeStamp = datetime.now(local_tz)

# #     insert_query = """
# #         INSERT INTO test_raw_data_a1
# #         ("runTime", "downTime", "quantity_FG", "quantity_ALL", "speed_standard", "speed_actual", "status", "TimeStamp")
# #         VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
# #     """

# #     cursor.execute(
# #         insert_query,
# #         (
# #             runTime,
# #             downTime,
# #             quantity_FG,
# #             quantity_ALL,
# #             speed_standard,
# #             speed_actual,
# #             status,
# #             TimeStamp,
# #         ),
# #     )

# #     conn.commit()
# #     cursor.close()
# #     conn.close()


# # # กำหนด DAG
# # with DAG(
# #     dag_id="dag_mockup_test_raw_data_a1",
# #     start_date=datetime(2025, 9, 22),
# #     schedule_interval="@hourly",  # รันทุกชั่วโมง
# #     catchup=False,
# #     tags=["mockup", "test_raw_data", "hourly"],
# # ) as dag:

# #     task_insert_mock_data = PythonOperator(
# #         task_id="insert_mock_data",
# #         python_callable=insert_mock_data,
# #     )

# #     task_insert_mock_data



















# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from datetime import datetime
# import psycopg2
# import random
# import pendulum


# def insert_mock_data():
#     conn = psycopg2.connect(
#         host="10.0.0.158",
#         port=5432,
#         user="admin",
#         password="Admin_password_2568",
#         database="mydatabase",
#     )
#     cursor = conn.cursor()

#     # โอกาส 5% ที่จะให้ค่าเป็น None หรือ 0
#     error_chance = 0.05

#     def maybe_corrupt(value, allow_zero=True, allow_null=True):
#         r = random.random()
#         if r < error_chance:
#             if allow_null and random.choice([True, False]):
#                 return None
#             elif allow_zero:
#                 return 0
#         return value

#     # สร้าง mock data
#     runTime = maybe_corrupt(round(random.uniform(1, 8), 2))
#     downTime = maybe_corrupt(round(random.uniform(0, 2), 2))
#     quantity_ALL = maybe_corrupt(random.randint(100, 500))
#     quantity_FG = maybe_corrupt(random.randint(80, quantity_ALL if quantity_ALL else 100))
#     speed_standard = maybe_corrupt(random.randint(80, 100))
#     speed_actual = maybe_corrupt(random.randint(60, 100))
#     status = maybe_corrupt(random.choice(["RUN", "STOP", "FIX"]), allow_zero=False)  # status เป็น text → ไม่ใช้ 0
#     local_tz = pendulum.timezone("Asia/Bangkok")
#     TimeStamp = datetime.now(local_tz)

#     insert_query = """
#         INSERT INTO test_raw_data_a1
#         ("runTime", "downTime", "quantity_FG", "quantity_ALL", "speed_standard", "speed_actual", "status", "TimeStamp")
#         VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
#     """

#     cursor.execute(
#         insert_query,
#         (
#             runTime,
#             downTime,
#             quantity_FG,
#             quantity_ALL,
#             speed_standard,
#             speed_actual,
#             status,
#             TimeStamp,
#         ),
#     )

#     conn.commit()
#     cursor.close()
#     conn.close()

# ################################################################################
# def insert_mock_data_a2():
#     conn = psycopg2.connect(
#         host="10.0.0.158",
#         port=5432,
#         user="admin",
#         password="Admin_password_2568",
#         database="mydatabase",
#     )
#     cursor = conn.cursor()

#     # โอกาส 5% ที่จะให้ค่าเป็น None หรือ 0
#     error_chance = 0.05

#     def maybe_corrupt(value, allow_zero=True, allow_null=True):
#         r = random.random()
#         if r < error_chance:
#             if allow_null and random.choice([True, False]):
#                 return None
#             elif allow_zero:
#                 return 0
#         return value

#     # สร้าง mock data
#     runTime = maybe_corrupt(round(random.uniform(1, 8), 2))
#     downTime = maybe_corrupt(round(random.uniform(0, 2), 2))
#     quantity_ALL = maybe_corrupt(random.randint(100, 500))
#     quantity_FG = maybe_corrupt(random.randint(80, quantity_ALL if quantity_ALL else 100))
#     speed_standard = maybe_corrupt(random.randint(80, 100))
#     speed_actual = maybe_corrupt(random.randint(60, 100))
#     status = maybe_corrupt(random.choice(["RUN", "STOP", "FIX"]), allow_zero=False)  # status เป็น text → ไม่ใช้ 0
#     local_tz = pendulum.timezone("Asia/Bangkok")
#     TimeStamp = datetime.now(local_tz)

#     insert_query = """
#         INSERT INTO test_raw_data_a2
#         ("runTime", "downTime", "quantity_FG", "quantity_ALL", "speed_standard", "speed_actual", "status", "TimeStamp")
#         VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
#     """

#     cursor.execute(
#         insert_query,
#         (
#             runTime,
#             downTime,
#             quantity_FG,
#             quantity_ALL,
#             speed_standard,
#             speed_actual,
#             status,
#             TimeStamp,
#         ),
#     )

#     conn.commit()
#     cursor.close()
#     conn.close()

# ################################################################################
# def insert_mock_data_a3():
#     conn = psycopg2.connect(
#         host="10.0.0.158",
#         port=5432,
#         user="admin",
#         password="Admin_password_2568",
#         database="mydatabase",
#     )
#     cursor = conn.cursor()

#     # โอกาส 5% ที่จะให้ค่าเป็น None หรือ 0
#     error_chance = 0.05

#     def maybe_corrupt(value, allow_zero=True, allow_null=True):
#         r = random.random()
#         if r < error_chance:
#             if allow_null and random.choice([True, False]):
#                 return None
#             elif allow_zero:
#                 return 0
#         return value

#     # สร้าง mock data
#     runTime = maybe_corrupt(round(random.uniform(1, 8), 2))
#     downTime = maybe_corrupt(round(random.uniform(0, 2), 2))
#     quantity_ALL = maybe_corrupt(random.randint(100, 500))
#     quantity_FG = maybe_corrupt(random.randint(80, quantity_ALL if quantity_ALL else 100))
#     speed_standard = maybe_corrupt(random.randint(80, 100))
#     speed_actual = maybe_corrupt(random.randint(60, 100))
#     status = maybe_corrupt(random.choice(["RUN", "STOP", "FIX"]), allow_zero=False)  # status เป็น text → ไม่ใช้ 0
#     local_tz = pendulum.timezone("Asia/Bangkok")
#     TimeStamp = datetime.now(local_tz)

#     insert_query = """
#         INSERT INTO test_raw_data_a3
#         ("runTime", "downTime", "quantity_FG", "quantity_ALL", "speed_standard", "speed_actual", "status", "TimeStamp")
#         VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
#     """

#     cursor.execute(
#         insert_query,
#         (
#             runTime,
#             downTime,
#             quantity_FG,
#             quantity_ALL,
#             speed_standard,
#             speed_actual,
#             status,
#             TimeStamp,
#         ),
#     )

#     conn.commit()
#     cursor.close()
#     conn.close()
# ##########################################################################
# ################################################################################
# def insert_mock_data_b7():
#     conn = psycopg2.connect(
#         host="10.0.0.158",
#         port=5432,
#         user="admin",
#         password="Admin_password_2568",
#         database="mydatabase",
#     )
#     cursor = conn.cursor()

#     # โอกาส 5% ที่จะให้ค่าเป็น None หรือ 0
#     error_chance = 0.05

#     def maybe_corrupt(value, allow_zero=True, allow_null=True):
#         r = random.random()
#         if r < error_chance:
#             if allow_null and random.choice([True, False]):
#                 return None
#             elif allow_zero:
#                 return 0
#         return value

#     # สร้าง mock data
#     runTime = maybe_corrupt(round(random.uniform(1, 8), 2))
#     downTime = maybe_corrupt(round(random.uniform(0, 2), 2))
#     quantity_ALL = maybe_corrupt(random.randint(100, 500))
#     quantity_FG = maybe_corrupt(random.randint(80, quantity_ALL if quantity_ALL else 100))
#     speed_standard = maybe_corrupt(random.randint(80, 100))
#     speed_actual = maybe_corrupt(random.randint(60, 100))
#     status = maybe_corrupt(random.choice(["RUN", "STOP", "FIX"]), allow_zero=False)  # status เป็น text → ไม่ใช้ 0
#     local_tz = pendulum.timezone("Asia/Bangkok")
#     TimeStamp = datetime.now(local_tz)

#     insert_query = """
#         INSERT INTO test_raw_data_b7
#         ("runTime", "downTime", "quantity_FG", "quantity_ALL", "speed_standard", "speed_actual", "status", "TimeStamp")
#         VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
#     """

#     cursor.execute(
#         insert_query,
#         (
#             runTime,
#             downTime,
#             quantity_FG,
#             quantity_ALL,
#             speed_standard,
#             speed_actual,
#             status,
#             TimeStamp,
#         ),
#     )

#     conn.commit()
#     cursor.close()
#     conn.close()

# ################################################################################

# ################################################################################
# def insert_mock_data_b8():
#     conn = psycopg2.connect(
#         host="10.0.0.158",
#         port=5432,
#         user="admin",
#         password="Admin_password_2568",
#         database="mydatabase",
#     )
#     cursor = conn.cursor()

#     # โอกาส 5% ที่จะให้ค่าเป็น None หรือ 0
#     error_chance = 0.05

#     def maybe_corrupt(value, allow_zero=True, allow_null=True):
#         r = random.random()
#         if r < error_chance:
#             if allow_null and random.choice([True, False]):
#                 return None
#             elif allow_zero:
#                 return 0
#         return value

#     # สร้าง mock data
#     runTime = maybe_corrupt(round(random.uniform(1, 8), 2))
#     downTime = maybe_corrupt(round(random.uniform(0, 2), 2))
#     quantity_ALL = maybe_corrupt(random.randint(100, 500))
#     quantity_FG = maybe_corrupt(random.randint(80, quantity_ALL if quantity_ALL else 100))
#     speed_standard = maybe_corrupt(random.randint(80, 100))
#     speed_actual = maybe_corrupt(random.randint(60, 100))
#     status = maybe_corrupt(random.choice(["RUN", "STOP", "FIX"]), allow_zero=False)  # status เป็น text → ไม่ใช้ 0
#     local_tz = pendulum.timezone("Asia/Bangkok")
#     TimeStamp = datetime.now(local_tz)

#     insert_query = """
#         INSERT INTO test_raw_data_b8
#         ("runTime", "downTime", "quantity_FG", "quantity_ALL", "speed_standard", "speed_actual", "status", "TimeStamp")
#         VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
#     """

#     cursor.execute(
#         insert_query,
#         (
#             runTime,
#             downTime,
#             quantity_FG,
#             quantity_ALL,
#             speed_standard,
#             speed_actual,
#             status,
#             TimeStamp,
#         ),
#     )

#     conn.commit()
#     cursor.close()
#     conn.close()

# ################################################################################

# ################################################################################
# def insert_mock_data_b9():
#     conn = psycopg2.connect(
#         host="10.0.0.158",
#         port=5432,
#         user="admin",
#         password="Admin_password_2568",
#         database="mydatabase",
#     )
#     cursor = conn.cursor()

#     # โอกาส 5% ที่จะให้ค่าเป็น None หรือ 0
#     error_chance = 0.05

#     def maybe_corrupt(value, allow_zero=True, allow_null=True):
#         r = random.random()
#         if r < error_chance:
#             if allow_null and random.choice([True, False]):
#                 return None
#             elif allow_zero:
#                 return 0
#         return value

#     # สร้าง mock data
#     runTime = maybe_corrupt(round(random.uniform(1, 8), 2))
#     downTime = maybe_corrupt(round(random.uniform(0, 2), 2))
#     quantity_ALL = maybe_corrupt(random.randint(100, 500))
#     quantity_FG = maybe_corrupt(random.randint(80, quantity_ALL if quantity_ALL else 100))
#     speed_standard = maybe_corrupt(random.randint(80, 100))
#     speed_actual = maybe_corrupt(random.randint(60, 100))
#     status = maybe_corrupt(random.choice(["RUN", "STOP", "FIX"]), allow_zero=False)  # status เป็น text → ไม่ใช้ 0
#     local_tz = pendulum.timezone("Asia/Bangkok")
#     TimeStamp = datetime.now(local_tz)

#     insert_query = """
#         INSERT INTO test_raw_data_b9
#         ("runTime", "downTime", "quantity_FG", "quantity_ALL", "speed_standard", "speed_actual", "status", "TimeStamp")
#         VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
#     """

#     cursor.execute(
#         insert_query,
#         (
#             runTime,
#             downTime,
#             quantity_FG,
#             quantity_ALL,
#             speed_standard,
#             speed_actual,
#             status,
#             TimeStamp,
#         ),
#     )

#     conn.commit()
#     cursor.close()
#     conn.close()

# ################################################################################

# ################################################################################
# def insert_mock_data_b10():
#     conn = psycopg2.connect(
#         host="10.0.0.158",
#         port=5432,
#         user="admin",
#         password="Admin_password_2568",
#         database="mydatabase",
#     )
#     cursor = conn.cursor()

#     # โอกาส 5% ที่จะให้ค่าเป็น None หรือ 0
#     error_chance = 0.05

#     def maybe_corrupt(value, allow_zero=True, allow_null=True):
#         r = random.random()
#         if r < error_chance:
#             if allow_null and random.choice([True, False]):
#                 return None
#             elif allow_zero:
#                 return 0
#         return value

#     # สร้าง mock data
#     runTime = maybe_corrupt(round(random.uniform(1, 8), 2))
#     downTime = maybe_corrupt(round(random.uniform(0, 2), 2))
#     quantity_ALL = maybe_corrupt(random.randint(100, 500))
#     quantity_FG = maybe_corrupt(random.randint(80, quantity_ALL if quantity_ALL else 100))
#     speed_standard = maybe_corrupt(random.randint(80, 100))
#     speed_actual = maybe_corrupt(random.randint(60, 100))
#     status = maybe_corrupt(random.choice(["RUN", "STOP", "FIX"]), allow_zero=False)  # status เป็น text → ไม่ใช้ 0
#     local_tz = pendulum.timezone("Asia/Bangkok")
#     TimeStamp = datetime.now(local_tz)

#     insert_query = """
#         INSERT INTO test_raw_data_b10
#         ("runTime", "downTime", "quantity_FG", "quantity_ALL", "speed_standard", "speed_actual", "status", "TimeStamp")
#         VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
#     """

#     cursor.execute(
#         insert_query,
#         (
#             runTime,
#             downTime,
#             quantity_FG,
#             quantity_ALL,
#             speed_standard,
#             speed_actual,
#             status,
#             TimeStamp,
#         ),
#     )

#     conn.commit()
#     cursor.close()
#     conn.close()

# ################################################################################

# ################################################################################
# def insert_mock_data_b12():
#     conn = psycopg2.connect(
#         host="10.0.0.158",
#         port=5432,
#         user="admin",
#         password="Admin_password_2568",
#         database="mydatabase",
#     )
#     cursor = conn.cursor()

#     # โอกาส 5% ที่จะให้ค่าเป็น None หรือ 0
#     error_chance = 0.05

#     def maybe_corrupt(value, allow_zero=True, allow_null=True):
#         r = random.random()
#         if r < error_chance:
#             if allow_null and random.choice([True, False]):
#                 return None
#             elif allow_zero:
#                 return 0
#         return value

#     # สร้าง mock data
#     runTime = maybe_corrupt(round(random.uniform(1, 8), 2))
#     downTime = maybe_corrupt(round(random.uniform(0, 2), 2))
#     quantity_ALL = maybe_corrupt(random.randint(100, 500))
#     quantity_FG = maybe_corrupt(random.randint(80, quantity_ALL if quantity_ALL else 100))
#     speed_standard = maybe_corrupt(random.randint(80, 100))
#     speed_actual = maybe_corrupt(random.randint(60, 100))
#     status = maybe_corrupt(random.choice(["RUN", "STOP", "FIX"]), allow_zero=False)  # status เป็น text → ไม่ใช้ 0
#     local_tz = pendulum.timezone("Asia/Bangkok")
#     TimeStamp = datetime.now(local_tz)

#     insert_query = """
#         INSERT INTO test_raw_data_b12
#         ("runTime", "downTime", "quantity_FG", "quantity_ALL", "speed_standard", "speed_actual", "status", "TimeStamp")
#         VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
#     """

#     cursor.execute(
#         insert_query,
#         (
#             runTime,
#             downTime,
#             quantity_FG,
#             quantity_ALL,
#             speed_standard,
#             speed_actual,
#             status,
#             TimeStamp,
#         ),
#     )

#     conn.commit()
#     cursor.close()
#     conn.close()

# ################################################################################

# ################################################################################
# def insert_mock_data_b13():
#     conn = psycopg2.connect(
#         host="10.0.0.158",
#         port=5432,
#         user="admin",
#         password="Admin_password_2568",
#         database="mydatabase",
#     )
#     cursor = conn.cursor()

#     # โอกาส 5% ที่จะให้ค่าเป็น None หรือ 0
#     error_chance = 0.05

#     def maybe_corrupt(value, allow_zero=True, allow_null=True):
#         r = random.random()
#         if r < error_chance:
#             if allow_null and random.choice([True, False]):
#                 return None
#             elif allow_zero:
#                 return 0
#         return value

#     # สร้าง mock data
#     runTime = maybe_corrupt(round(random.uniform(1, 8), 2))
#     downTime = maybe_corrupt(round(random.uniform(0, 2), 2))
#     quantity_ALL = maybe_corrupt(random.randint(100, 500))
#     quantity_FG = maybe_corrupt(random.randint(80, quantity_ALL if quantity_ALL else 100))
#     speed_standard = maybe_corrupt(random.randint(80, 100))
#     speed_actual = maybe_corrupt(random.randint(60, 100))
#     status = maybe_corrupt(random.choice(["RUN", "STOP", "FIX"]), allow_zero=False)  # status เป็น text → ไม่ใช้ 0
#     local_tz = pendulum.timezone("Asia/Bangkok")
#     TimeStamp = datetime.now(local_tz)

#     insert_query = """
#         INSERT INTO test_raw_data_b13
#         ("runTime", "downTime", "quantity_FG", "quantity_ALL", "speed_standard", "speed_actual", "status", "TimeStamp")
#         VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
#     """

#     cursor.execute(
#         insert_query,
#         (
#             runTime,
#             downTime,
#             quantity_FG,
#             quantity_ALL,
#             speed_standard,
#             speed_actual,
#             status,
#             TimeStamp,
#         ),
#     )

#     conn.commit()
#     cursor.close()
#     conn.close()

# ################################################################################

# ################################################################################
# def insert_mock_data_b16():
#     conn = psycopg2.connect(
#         host="10.0.0.158",
#         port=5432,
#         user="admin",
#         password="Admin_password_2568",
#         database="mydatabase",
#     )
#     cursor = conn.cursor()

#     # โอกาส 5% ที่จะให้ค่าเป็น None หรือ 0
#     error_chance = 0.05

#     def maybe_corrupt(value, allow_zero=True, allow_null=True):
#         r = random.random()
#         if r < error_chance:
#             if allow_null and random.choice([True, False]):
#                 return None
#             elif allow_zero:
#                 return 0
#         return value

#     # สร้าง mock data
#     runTime = maybe_corrupt(round(random.uniform(1, 8), 2))
#     downTime = maybe_corrupt(round(random.uniform(0, 2), 2))
#     quantity_ALL = maybe_corrupt(random.randint(100, 500))
#     quantity_FG = maybe_corrupt(random.randint(80, quantity_ALL if quantity_ALL else 100))
#     speed_standard = maybe_corrupt(random.randint(80, 100))
#     speed_actual = maybe_corrupt(random.randint(60, 100))
#     status = maybe_corrupt(random.choice(["RUN", "STOP", "FIX"]), allow_zero=False)  # status เป็น text → ไม่ใช้ 0
#     local_tz = pendulum.timezone("Asia/Bangkok")
#     TimeStamp = datetime.now(local_tz)

#     insert_query = """
#         INSERT INTO test_raw_data_b16
#         ("runTime", "downTime", "quantity_FG", "quantity_ALL", "speed_standard", "speed_actual", "status", "TimeStamp")
#         VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
#     """

#     cursor.execute(
#         insert_query,
#         (
#             runTime,
#             downTime,
#             quantity_FG,
#             quantity_ALL,
#             speed_standard,
#             speed_actual,
#             status,
#             TimeStamp,
#         ),
#     )

#     conn.commit()
#     cursor.close()
#     conn.close()

# ################################################################################

# ################################################################################
# def insert_mock_data_b17():
#     conn = psycopg2.connect(
#         host="10.0.0.158",
#         port=5432,
#         user="admin",
#         password="Admin_password_2568",
#         database="mydatabase",
#     )
#     cursor = conn.cursor()

#     # โอกาส 5% ที่จะให้ค่าเป็น None หรือ 0
#     error_chance = 0.05

#     def maybe_corrupt(value, allow_zero=True, allow_null=True):
#         r = random.random()
#         if r < error_chance:
#             if allow_null and random.choice([True, False]):
#                 return None
#             elif allow_zero:
#                 return 0
#         return value

#     # สร้าง mock data
#     runTime = maybe_corrupt(round(random.uniform(1, 8), 2))
#     downTime = maybe_corrupt(round(random.uniform(0, 2), 2))
#     quantity_ALL = maybe_corrupt(random.randint(100, 500))
#     quantity_FG = maybe_corrupt(random.randint(80, quantity_ALL if quantity_ALL else 100))
#     speed_standard = maybe_corrupt(random.randint(80, 100))
#     speed_actual = maybe_corrupt(random.randint(60, 100))
#     status = maybe_corrupt(random.choice(["RUN", "STOP", "FIX"]), allow_zero=False)  # status เป็น text → ไม่ใช้ 0
#     local_tz = pendulum.timezone("Asia/Bangkok")
#     TimeStamp = datetime.now(local_tz)

#     insert_query = """
#         INSERT INTO test_raw_data_b17
#         ("runTime", "downTime", "quantity_FG", "quantity_ALL", "speed_standard", "speed_actual", "status", "TimeStamp")
#         VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
#     """

#     cursor.execute(
#         insert_query,
#         (
#             runTime,
#             downTime,
#             quantity_FG,
#             quantity_ALL,
#             speed_standard,
#             speed_actual,
#             status,
#             TimeStamp,
#         ),
#     )

#     conn.commit()
#     cursor.close()
#     conn.close()

# ################################################################################

# ################################################################################
# def insert_mock_data_b18():
#     conn = psycopg2.connect(
#         host="10.0.0.158",
#         port=5432,
#         user="admin",
#         password="Admin_password_2568",
#         database="mydatabase",
#     )
#     cursor = conn.cursor()

#     # โอกาส 5% ที่จะให้ค่าเป็น None หรือ 0
#     error_chance = 0.05

#     def maybe_corrupt(value, allow_zero=True, allow_null=True):
#         r = random.random()
#         if r < error_chance:
#             if allow_null and random.choice([True, False]):
#                 return None
#             elif allow_zero:
#                 return 0
#         return value

#     # สร้าง mock data
#     runTime = maybe_corrupt(round(random.uniform(1, 8), 2))
#     downTime = maybe_corrupt(round(random.uniform(0, 2), 2))
#     quantity_ALL = maybe_corrupt(random.randint(100, 500))
#     quantity_FG = maybe_corrupt(random.randint(80, quantity_ALL if quantity_ALL else 100))
#     speed_standard = maybe_corrupt(random.randint(80, 100))
#     speed_actual = maybe_corrupt(random.randint(60, 100))
#     status = maybe_corrupt(random.choice(["RUN", "STOP", "FIX"]), allow_zero=False)  # status เป็น text → ไม่ใช้ 0
#     local_tz = pendulum.timezone("Asia/Bangkok")
#     TimeStamp = datetime.now(local_tz)

#     insert_query = """
#         INSERT INTO test_raw_data_b18
#         ("runTime", "downTime", "quantity_FG", "quantity_ALL", "speed_standard", "speed_actual", "status", "TimeStamp")
#         VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
#     """

#     cursor.execute(
#         insert_query,
#         (
#             runTime,
#             downTime,
#             quantity_FG,
#             quantity_ALL,
#             speed_standard,
#             speed_actual,
#             status,
#             TimeStamp,
#         ),
#     )

#     conn.commit()
#     cursor.close()
#     conn.close()

# ################################################################################

# with DAG(
#     dag_id="dag_mockup_test_raw_data_a1",
#     start_date=datetime(2025, 9, 22),
#     schedule_interval="@hourly",
#     catchup=False,
#     tags=["mockup", "test_raw_data", "hourly"],
# ) as dag:

#     task_insert_mock_data = PythonOperator(
#         task_id="insert_mock_data",
#         python_callable=insert_mock_data,
#     )

#     task_insert_mock_data_a2 = PythonOperator(
#         task_id="insert_mock_data_a2",
#         python_callable=insert_mock_data_a2,
#     )

#     task_insert_mock_data_a3 = PythonOperator(
#         task_id="insert_mock_data_a3",
#         python_callable=insert_mock_data_a3,
#     )

#     task_insert_mock_data_b7 = PythonOperator(
#         task_id="insert_mock_data_b7",
#         python_callable=insert_mock_data_b7,
#     )

#     task_insert_mock_data_b8 = PythonOperator(
#         task_id="insert_mock_data_b8",
#         python_callable=insert_mock_data_b8,
#     )

#     task_insert_mock_data_b9 = PythonOperator(
#         task_id="insert_mock_data_b9",
#         python_callable=insert_mock_data_b9,
#     )

#     task_insert_mock_data_b10 = PythonOperator(
#         task_id="insert_mock_data_b10",
#         python_callable=insert_mock_data_b10,
#     )

#     task_insert_mock_data_b12 = PythonOperator(
#         task_id="insert_mock_data_b12",
#         python_callable=insert_mock_data_b12,
#     )

#     task_insert_mock_data_b13 = PythonOperator(
#         task_id="insert_mock_data_b13",
#         python_callable=insert_mock_data_b13,
#     )

#     task_insert_mock_data_b16 = PythonOperator(
#         task_id="insert_mock_data_b16",
#         python_callable=insert_mock_data_b16,
#     )

#     task_insert_mock_data_b17 = PythonOperator(
#         task_id="insert_mock_data_b17",
#         python_callable=insert_mock_data_b17,
#     )

#     task_insert_mock_data_b18 = PythonOperator(
#         task_id="insert_mock_data_b18",
#         python_callable=insert_mock_data_b18,
#     )
#     # task_insert_mock_data





















from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import psycopg2
import random
import pendulum


# -------------------- ฟังก์ชันหลัก --------------------
def insert_mock_data(table_name: str):
    conn = psycopg2.connect(
        host="10.0.0.158",
        port=5432,
        user="admin",
        password="Admin_password_2568",
        database="mydatabase",
    )
    cursor = conn.cursor()

    error_chance = 0.05

    def maybe_corrupt(value, allow_zero=True, allow_null=True):
        r = random.random()
        if r < error_chance:
            if allow_null and random.choice([True, False]):
                return None
            elif allow_zero:
                return 0
        return value

    # สร้าง mock data
    runTime = maybe_corrupt(round(random.uniform(1, 8), 2))
    downTime = maybe_corrupt(round(random.uniform(0, 2), 2))
    quantity_ALL = maybe_corrupt(random.randint(100, 500))
    quantity_FG = maybe_corrupt(random.randint(80, quantity_ALL if quantity_ALL else 100))
    speed_standard = maybe_corrupt(random.randint(80, 100))
    speed_actual = maybe_corrupt(random.randint(60, 100))
    status = maybe_corrupt(random.choice(["RUN", "STOP", "FIX"]), allow_zero=False)
    local_tz = pendulum.timezone("Asia/Bangkok")
    TimeStamp = datetime.now(local_tz)

    insert_query = f"""
        INSERT INTO {table_name}
        ("runTime", "downTime", "quantity_FG", "quantity_ALL", 
         "speed_standard", "speed_actual", "status", "TimeStamp")
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
    """

    cursor.execute(
        insert_query,
        (
            runTime,
            downTime,
            quantity_FG,
            quantity_ALL,
            speed_standard,
            speed_actual,
            status,
            TimeStamp,
        ),
    )

    conn.commit()
    cursor.close()
    conn.close()


# -------------------- สร้าง DAG --------------------
with DAG(
    dag_id="dag_mockup_test_raw_data",
    start_date=datetime(2025, 9, 22),
    schedule_interval="@hourly",
    catchup=False,
    tags=["mockup", "test_raw_data", "hourly"],
) as dag:

    # รายการตารางทั้งหมดที่ต้องการ insert mock data
    table_names = [
        "test_raw_data_a1",
        "test_raw_data_a2",
        "test_raw_data_a3",
        "test_raw_data_b7",
        "test_raw_data_b8",
        "test_raw_data_b9",
        "test_raw_data_b10",
        "test_raw_data_b12",
        "test_raw_data_b13",
        "test_raw_data_b16",
        "test_raw_data_b17",
        "test_raw_data_b18",
        "test_raw_data_c1",
        "test_raw_data_c2",
        "test_raw_data_c3",
        "test_raw_data_c4",
        "test_raw_data_c5",
        "test_raw_data_c6",
        "test_raw_data_c7",
        "test_raw_data_c8",
        "test_raw_data_c9",
        "test_raw_data_c10",
        "test_raw_data_c11",
        "test_raw_data_d1",
        "test_raw_data_d2",
        "test_raw_data_d3",
        "test_raw_data_d5",
        "test_raw_data_d6",
        "test_raw_data_d7",
        "test_raw_data_d8",
        "test_raw_data_d9",
        "test_raw_data_d10",
        "test_raw_data_d11",
        "test_raw_data_d12",
        "test_raw_data_d13",
        "test_raw_data_h1",
        "test_raw_data_h2",
        "test_raw_data_h4",
        "test_raw_data_h5",
        "test_raw_data_h6",
        "test_raw_data_h7",
        "test_raw_data_h8",
        "test_raw_data_h9",
        "test_raw_data_h10",
        "test_raw_data_i1",
        "test_raw_data_i2",
        "test_raw_data_i4",
        "test_raw_data_m1",
        "test_raw_data_n1",
        "test_raw_data_o1",
        "test_raw_data_q1",
        "test_raw_data_s1",
    ]

    for table in table_names:
        PythonOperator(
            task_id=f"insert_mock_data_{table.split('_')[-1]}",  # e.g., c1, a2
            python_callable=insert_mock_data,
            op_args=[table],
        )