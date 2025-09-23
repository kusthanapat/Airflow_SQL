# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from datetime import datetime
# import psycopg2
# import random
# import pendulum


# # ฟังก์ชันสร้าง mock data และบันทึกลง PostgreSQL
# def insert_mock_data():
#     conn = psycopg2.connect(
#         host="10.0.0.158",
#         port=5432,
#         user="admin",
#         password="Admin_password_2568",
#         database="mydatabase",
#     )
#     cursor = conn.cursor()

#     # สร้าง mock data
#     runTime = round(random.uniform(1, 8), 2)  # ชั่วโมงการทำงาน
#     downTime = round(random.uniform(0, 2), 2)  # ชั่วโมงที่เครื่องหยุด
#     quantity_ALL = random.randint(100, 500)  # จำนวนทั้งหมด
#     quantity_FG = random.randint(80, quantity_ALL)  # จำนวนของดี
#     speed_standard = random.randint(80, 100)  # speed มาตรฐาน
#     speed_actual = random.randint(60, 100)  # speed ที่วัดได้จริง
#     status = random.choice(["RUN", "STOP", "ERROR"])  # สถานะ
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


# # กำหนด DAG
# with DAG(
#     dag_id="dag_mockup_test_raw_data_a1",
#     start_date=datetime(2025, 9, 22),
#     schedule_interval="@hourly",  # รันทุกชั่วโมง
#     catchup=False,
#     tags=["mockup", "test_raw_data", "hourly"],
# ) as dag:

#     task_insert_mock_data = PythonOperator(
#         task_id="insert_mock_data",
#         python_callable=insert_mock_data,
#     )

#     task_insert_mock_data



















from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import psycopg2
import random
import pendulum


def insert_mock_data():
    conn = psycopg2.connect(
        host="10.0.0.158",
        port=5432,
        user="admin",
        password="Admin_password_2568",
        database="mydatabase",
    )
    cursor = conn.cursor()

    # โอกาส 5% ที่จะให้ค่าเป็น None หรือ 0
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
    status = maybe_corrupt(random.choice(["RUN", "STOP", "FIX"]), allow_zero=False)  # status เป็น text → ไม่ใช้ 0
    local_tz = pendulum.timezone("Asia/Bangkok")
    TimeStamp = datetime.now(local_tz)

    insert_query = """
        INSERT INTO test_raw_data_a1
        ("runTime", "downTime", "quantity_FG", "quantity_ALL", "speed_standard", "speed_actual", "status", "TimeStamp")
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

################################################################################
def insert_mock_data_a2():
    conn = psycopg2.connect(
        host="10.0.0.158",
        port=5432,
        user="admin",
        password="Admin_password_2568",
        database="mydatabase",
    )
    cursor = conn.cursor()

    # โอกาส 5% ที่จะให้ค่าเป็น None หรือ 0
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
    status = maybe_corrupt(random.choice(["RUN", "STOP", "FIX"]), allow_zero=False)  # status เป็น text → ไม่ใช้ 0
    local_tz = pendulum.timezone("Asia/Bangkok")
    TimeStamp = datetime.now(local_tz)

    insert_query = """
        INSERT INTO test_raw_data_a2
        ("runTime", "downTime", "quantity_FG", "quantity_ALL", "speed_standard", "speed_actual", "status", "TimeStamp")
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

################################################################################
def insert_mock_data_a3():
    conn = psycopg2.connect(
        host="10.0.0.158",
        port=5432,
        user="admin",
        password="Admin_password_2568",
        database="mydatabase",
    )
    cursor = conn.cursor()

    # โอกาส 5% ที่จะให้ค่าเป็น None หรือ 0
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
    status = maybe_corrupt(random.choice(["RUN", "STOP", "FIX"]), allow_zero=False)  # status เป็น text → ไม่ใช้ 0
    local_tz = pendulum.timezone("Asia/Bangkok")
    TimeStamp = datetime.now(local_tz)

    insert_query = """
        INSERT INTO test_raw_data_a3
        ("runTime", "downTime", "quantity_FG", "quantity_ALL", "speed_standard", "speed_actual", "status", "TimeStamp")
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
##########################################################################
################################################################################
def insert_mock_data_b7():
    conn = psycopg2.connect(
        host="10.0.0.158",
        port=5432,
        user="admin",
        password="Admin_password_2568",
        database="mydatabase",
    )
    cursor = conn.cursor()

    # โอกาส 5% ที่จะให้ค่าเป็น None หรือ 0
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
    status = maybe_corrupt(random.choice(["RUN", "STOP", "FIX"]), allow_zero=False)  # status เป็น text → ไม่ใช้ 0
    local_tz = pendulum.timezone("Asia/Bangkok")
    TimeStamp = datetime.now(local_tz)

    insert_query = """
        INSERT INTO test_raw_data_b7
        ("runTime", "downTime", "quantity_FG", "quantity_ALL", "speed_standard", "speed_actual", "status", "TimeStamp")
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

################################################################################

################################################################################
def insert_mock_data_b8():
    conn = psycopg2.connect(
        host="10.0.0.158",
        port=5432,
        user="admin",
        password="Admin_password_2568",
        database="mydatabase",
    )
    cursor = conn.cursor()

    # โอกาส 5% ที่จะให้ค่าเป็น None หรือ 0
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
    status = maybe_corrupt(random.choice(["RUN", "STOP", "FIX"]), allow_zero=False)  # status เป็น text → ไม่ใช้ 0
    local_tz = pendulum.timezone("Asia/Bangkok")
    TimeStamp = datetime.now(local_tz)

    insert_query = """
        INSERT INTO test_raw_data_b8
        ("runTime", "downTime", "quantity_FG", "quantity_ALL", "speed_standard", "speed_actual", "status", "TimeStamp")
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

################################################################################

################################################################################
def insert_mock_data_b9():
    conn = psycopg2.connect(
        host="10.0.0.158",
        port=5432,
        user="admin",
        password="Admin_password_2568",
        database="mydatabase",
    )
    cursor = conn.cursor()

    # โอกาส 5% ที่จะให้ค่าเป็น None หรือ 0
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
    status = maybe_corrupt(random.choice(["RUN", "STOP", "FIX"]), allow_zero=False)  # status เป็น text → ไม่ใช้ 0
    local_tz = pendulum.timezone("Asia/Bangkok")
    TimeStamp = datetime.now(local_tz)

    insert_query = """
        INSERT INTO test_raw_data_b9
        ("runTime", "downTime", "quantity_FG", "quantity_ALL", "speed_standard", "speed_actual", "status", "TimeStamp")
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

################################################################################

################################################################################
def insert_mock_data_b10():
    conn = psycopg2.connect(
        host="10.0.0.158",
        port=5432,
        user="admin",
        password="Admin_password_2568",
        database="mydatabase",
    )
    cursor = conn.cursor()

    # โอกาส 5% ที่จะให้ค่าเป็น None หรือ 0
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
    status = maybe_corrupt(random.choice(["RUN", "STOP", "FIX"]), allow_zero=False)  # status เป็น text → ไม่ใช้ 0
    local_tz = pendulum.timezone("Asia/Bangkok")
    TimeStamp = datetime.now(local_tz)

    insert_query = """
        INSERT INTO test_raw_data_b10
        ("runTime", "downTime", "quantity_FG", "quantity_ALL", "speed_standard", "speed_actual", "status", "TimeStamp")
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

################################################################################

with DAG(
    dag_id="dag_mockup_test_raw_data_a1",
    start_date=datetime(2025, 9, 22),
    schedule_interval="@hourly",
    catchup=False,
    tags=["mockup", "test_raw_data", "hourly"],
) as dag:

    task_insert_mock_data = PythonOperator(
        task_id="insert_mock_data",
        python_callable=insert_mock_data,
    )

    task_insert_mock_data_a2 = PythonOperator(
        task_id="insert_mock_data_a2",
        python_callable=insert_mock_data_a2,
    )

    task_insert_mock_data_a3 = PythonOperator(
        task_id="insert_mock_data_a3",
        python_callable=insert_mock_data_a3,
    )

    task_insert_mock_data_b7 = PythonOperator(
        task_id="insert_mock_data_b7",
        python_callable=insert_mock_data_b7,
    )

    task_insert_mock_data_b8 = PythonOperator(
        task_id="insert_mock_data_b8",
        python_callable=insert_mock_data_b8,
    )

    task_insert_mock_data_b9 = PythonOperator(
        task_id="insert_mock_data_b9",
        python_callable=insert_mock_data_b9,
    )

    task_insert_mock_data_b10 = PythonOperator(
        task_id="insert_mock_data_b10",
        python_callable=insert_mock_data_b10,
    )
    # task_insert_mock_data
