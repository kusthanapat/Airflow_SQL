from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import psycopg2
import pandas as pd
import pendulum


def cleanse_data():
    conn = psycopg2.connect(
        host="10.0.0.158",
        port=5432,
        user="admin",
        password="Admin_password_2568",
        database="mydatabase",
    )
    cursor = conn.cursor()

    # ดึงข้อมูลดิบจาก table
    cursor.execute("SELECT * FROM test_raw_data_a1")
    rows = cursor.fetchall()
    colnames = [desc[0] for desc in cursor.description]

    # แปลงเป็น DataFrame
    df = pd.DataFrame(rows, columns=colnames)

    # ======== เริ่มทำ Data Cleansing =========
    
    # 1. เติมค่า runTime/downTime ที่เป็น null หรือ 0 ด้วยค่าเฉลี่ย
    for col in ['runTime', 'downTime', 'speed_standard', 'speed_actual']:
        df[col] = pd.to_numeric(df[col], errors='coerce')
        mean_val = df[col][(df[col].notnull()) & (df[col] > 0)].mean()
        df[col] = df[col].apply(lambda x: mean_val if pd.isnull(x) or x <= 0 else x)

    # 2. เติมค่า quantity_ALL และ quantity_FG
    df['quantity_ALL'] = pd.to_numeric(df['quantity_ALL'], errors='coerce')
    df['quantity_FG'] = pd.to_numeric(df['quantity_FG'], errors='coerce')
    
    df['quantity_ALL'] = df['quantity_ALL'].fillna(df['quantity_ALL'].mean())
    df['quantity_FG'] = df.apply(
        lambda row: min(row['quantity_FG'], row['quantity_ALL']) if pd.notnull(row['quantity_FG']) else row['quantity_ALL'],
        axis=1
    )

    # 3. ถ้า speed_actual > speed_standard ให้ปรับให้เท่ากับ speed_standard
    df['speed_actual'] = df.apply(
        lambda row: min(row['speed_actual'], row['speed_standard']),
        axis=1
    )

    # 4. ถ้า status เป็น null ให้เติมว่า "UNKNOWN"
    df['status'] = df['status'].fillna('UNKNOWN')

    # 5. กรองเฉพาะ record ล่าสุด (optional - เช่นใช้เฉพาะของวันนี้)
    # df = df[df['TimeStamp'] >= datetime.now() - timedelta(days=1)]

    # ======== บันทึกข้อมูลหลัง Cleansing =========

    # ลบข้อมูลเก่าทิ้งจาก cleaned table (ถ้าต้องการ refresh)
    cursor.execute("DELETE FROM cleaned_raw_data_a1")
    conn.commit()

    # แทรกข้อมูลใหม่
    insert_query = """
        INSERT INTO cleaned_raw_data_a1
        ("runTime", "downTime", "quantity_FG", "quantity_ALL", "speed_standard", "speed_actual", "status", "TimeStamp")
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
    """

    for _, row in df.iterrows():
        cursor.execute(insert_query, (
            row['runTime'],
            row['downTime'],
            row['quantity_FG'],
            row['quantity_ALL'],
            row['speed_standard'],
            row['speed_actual'],
            row['status'],
            row['TimeStamp'],
        ))

    conn.commit()
    cursor.close()
    conn.close()


with DAG(
    dag_id="dag_data_cleanse_raw_data_a1",
    start_date=datetime(2025, 9, 22),
    schedule_interval="@hourly",
    catchup=False,
    default_args={
        'retries': 0  # ไม่ retry ถ้า fail
    },
    tags=["cleansing", "data_pipeline", "test_raw_data"],
) as dag:

    task_cleanse_data = PythonOperator(
        task_id="cleanse_data",
        python_callable=cleanse_data,
    )

    task_cleanse_data
