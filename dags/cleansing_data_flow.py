# # from airflow import DAG
# # from airflow.operators.python import PythonOperator
# # from datetime import datetime
# # import psycopg2
# # import pandas as pd
# # import pendulum


# # def cleanse_data():
# #     import psycopg2
# #     import pandas as pd

# #     # เชื่อมต่อกับ PostgreSQL
# #     conn = psycopg2.connect(
# #         host="10.0.0.158",
# #         port=5432,
# #         user="admin",
# #         password="Admin_password_2568",
# #         database="mydatabase",
# #     )
# #     cursor = conn.cursor()

# #     # ดึงข้อมูลดิบ
# #     cursor.execute("SELECT * FROM test_raw_data_a1")
# #     rows = cursor.fetchall()
# #     colnames = [desc[0] for desc in cursor.description]
# #     df = pd.DataFrame(rows, columns=colnames)

# #     # 🔧 แปลงคอลัมน์ที่ต้องใช้ให้เป็นตัวเลข
# #     numeric_cols = ['runTime', 'downTime', 'speed_standard', 'speed_actual', 'quantity_ALL', 'quantity_FG']
# #     for col in numeric_cols:
# #         df[col] = pd.to_numeric(df[col], errors='coerce')

# #     # ======== CLEANSING เฉพาะค่าที่ "เป็น null" หรือ "< 0" เท่านั้น ========

# #     for col in numeric_cols:
# #         # คำนวณค่าเฉลี่ยจากค่าที่ "ไม่เป็น null" และ "> 0"
# #         valid_mean = df.loc[(df[col].notnull()) & (df[col] > 0), col].mean()

# #         # เติมค่าที่เป็น null หรือ < 0 ด้วยค่าเฉลี่ย
# #         df.loc[(df[col].isnull()) | (df[col] < 0), col] = valid_mean

# #     # ตรวจสอบและปรับ quantity_FG ที่มากกว่า quantity_ALL
# #     df.loc[df['quantity_FG'] > df['quantity_ALL'], 'quantity_FG'] = df['quantity_ALL']

# #     # ปรับ speed_actual ให้ไม่มากกว่า speed_standard
# #     df.loc[df['speed_actual'] > df['speed_standard'], 'speed_actual'] = df['speed_standard']

# #     # status ที่เป็น null → "UNKNOWN"
# #     df['status'] = df['status'].fillna('UNKNOWN')

# #     # ======== เขียนกลับลง cleaned table =========
# #     cursor.execute("DELETE FROM cleaned_raw_data_a1")
# #     conn.commit()

# #     insert_query = """
# #         INSERT INTO cleaned_raw_data_a1
# #         ("runTime", "downTime", "quantity_FG", "quantity_ALL", "speed_standard", "speed_actual", "status", "TimeStamp")
# #         VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
# #     """

# #     for _, row in df.iterrows():
# #         cursor.execute(insert_query, (
# #             row['runTime'],
# #             row['downTime'],
# #             row['quantity_FG'],
# #             row['quantity_ALL'],
# #             row['speed_standard'],
# #             row['speed_actual'],
# #             row['status'],
# #             row['TimeStamp'],
# #         ))

# #     conn.commit()
# #     cursor.close()
# #     conn.close()



# # with DAG(
# #     dag_id="dag_data_cleanse_raw_data_a1",
# #     start_date=datetime(2025, 9, 22),
# #     schedule_interval="@hourly",
# #     catchup=False,
# #     default_args={
# #         'retries': 0  # ไม่ retry ถ้า fail
# #     },
# #     tags=["cleansing", "data_pipeline", "test_raw_data"],
# # ) as dag:

# #     task_cleanse_data = PythonOperator(
# #         task_id="cleanse_data",
# #         python_callable=cleanse_data,
# #     )

# #     task_cleanse_data













# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from datetime import datetime
# import psycopg2
# import pandas as pd
# import pendulum


# def cleanse_data():
#     conn = psycopg2.connect(
#         host="10.0.0.158",
#         port=5432,
#         user="admin",
#         password="Admin_password_2568",
#         database="mydatabase",
#     )
#     cursor = conn.cursor()

#     cursor.execute("SELECT * FROM test_raw_data_a1")
#     rows = cursor.fetchall()
#     colnames = [desc[0] for desc in cursor.description]
#     df = pd.DataFrame(rows, columns=colnames)

#     numeric_cols = ['runTime', 'downTime', 'quantity_ALL', 'quantity_FG', 'speed_standard', 'speed_actual']
#     for col in numeric_cols:
#         df[col] = pd.to_numeric(df[col], errors='coerce')

#     for col in numeric_cols:
#         valid_mean = df.loc[(df[col].notnull()) & (df[col] > 0), col].mean()
#         df.loc[(df[col].isnull()) | (df[col] < 0), col] = valid_mean

#     df.loc[df['downTime'] > df['runTime'], 'downTime'] = df['runTime']
#     df.loc[df['quantity_FG'] > df['quantity_ALL'], 'quantity_FG'] = df['quantity_ALL']
#     df.loc[df['speed_actual'] > df['speed_standard'], 'speed_actual'] = df['speed_standard']
#     df['status'] = df['status'].fillna('UNKNOWN')

#     cursor.execute("DELETE FROM cleaned_raw_data_a1")
#     conn.commit()

#     local_tz = pendulum.timezone("Asia/Bangkok")
#     TimeStamp = datetime.now(local_tz)

#     insert_query = """
#         INSERT INTO cleaned_raw_data_a1
#         ("runTime", "downTime", "quantity_FG", "quantity_ALL", "speed_standard", "speed_actual", "status", "TimeStamp")
#         VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
#     """
#     for _, row in df.iterrows():
#         cursor.execute(insert_query, (
#             row['runTime'],
#             row['downTime'],
#             row['quantity_FG'],
#             row['quantity_ALL'],
#             row['speed_standard'],
#             row['speed_actual'],
#             row['status'],
#             TimeStamp,
#         ))

#     conn.commit()
#     cursor.close()
#     conn.close()

# ######################################################################
# def cleanse_data_a2():
#     conn = psycopg2.connect(
#         host="10.0.0.158",
#         port=5432,
#         user="admin",
#         password="Admin_password_2568",
#         database="mydatabase",
#     )
#     cursor = conn.cursor()

#     cursor.execute("SELECT * FROM test_raw_data_a2")
#     rows = cursor.fetchall()
#     colnames = [desc[0] for desc in cursor.description]
#     df = pd.DataFrame(rows, columns=colnames)

#     numeric_cols = ['runTime', 'downTime', 'quantity_ALL', 'quantity_FG', 'speed_standard', 'speed_actual']
#     for col in numeric_cols:
#         df[col] = pd.to_numeric(df[col], errors='coerce')

#     for col in numeric_cols:
#         valid_mean = df.loc[(df[col].notnull()) & (df[col] > 0), col].mean()
#         df.loc[(df[col].isnull()) | (df[col] < 0), col] = valid_mean

#     df.loc[df['downTime'] > df['runTime'], 'downTime'] = df['runTime']
#     df.loc[df['quantity_FG'] > df['quantity_ALL'], 'quantity_FG'] = df['quantity_ALL']
#     df.loc[df['speed_actual'] > df['speed_standard'], 'speed_actual'] = df['speed_standard']
#     df['status'] = df['status'].fillna('UNKNOWN')

#     cursor.execute("DELETE FROM cleaned_raw_data_a2")
#     conn.commit()

#     local_tz = pendulum.timezone("Asia/Bangkok")
#     TimeStamp = datetime.now(local_tz)

#     insert_query = """
#         INSERT INTO cleaned_raw_data_a2
#         ("runTime", "downTime", "quantity_FG", "quantity_ALL", "speed_standard", "speed_actual", "status", "TimeStamp")
#         VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
#     """
#     for _, row in df.iterrows():
#         cursor.execute(insert_query, (
#             row['runTime'],
#             row['downTime'],
#             row['quantity_FG'],
#             row['quantity_ALL'],
#             row['speed_standard'],
#             row['speed_actual'],
#             row['status'],
#             TimeStamp,
#         ))

#     conn.commit()
#     cursor.close()
#     conn.close()

# #####################################################################

# #####################################################################
# def cleanse_data_a3():
#     conn = psycopg2.connect(
#         host="10.0.0.158",
#         port=5432,
#         user="admin",
#         password="Admin_password_2568",
#         database="mydatabase",
#     )
#     cursor = conn.cursor()

#     cursor.execute("SELECT * FROM test_raw_data_a3")
#     rows = cursor.fetchall()
#     colnames = [desc[0] for desc in cursor.description]
#     df = pd.DataFrame(rows, columns=colnames)

#     numeric_cols = ['runTime', 'downTime', 'quantity_ALL', 'quantity_FG', 'speed_standard', 'speed_actual']
#     for col in numeric_cols:
#         df[col] = pd.to_numeric(df[col], errors='coerce')

#     for col in numeric_cols:
#         valid_mean = df.loc[(df[col].notnull()) & (df[col] > 0), col].mean()
#         df.loc[(df[col].isnull()) | (df[col] < 0), col] = valid_mean

#     df.loc[df['downTime'] > df['runTime'], 'downTime'] = df['runTime']
#     df.loc[df['quantity_FG'] > df['quantity_ALL'], 'quantity_FG'] = df['quantity_ALL']
#     df.loc[df['speed_actual'] > df['speed_standard'], 'speed_actual'] = df['speed_standard']
#     df['status'] = df['status'].fillna('UNKNOWN')

#     cursor.execute("DELETE FROM cleaned_raw_data_a3")
#     conn.commit()

#     local_tz = pendulum.timezone("Asia/Bangkok")
#     TimeStamp = datetime.now(local_tz)

#     insert_query = """
#         INSERT INTO cleaned_raw_data_a3
#         ("runTime", "downTime", "quantity_FG", "quantity_ALL", "speed_standard", "speed_actual", "status", "TimeStamp")
#         VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
#     """
#     for _, row in df.iterrows():
#         cursor.execute(insert_query, (
#             row['runTime'],
#             row['downTime'],
#             row['quantity_FG'],
#             row['quantity_ALL'],
#             row['speed_standard'],
#             row['speed_actual'],
#             row['status'],
#             TimeStamp,
#         ))

#     conn.commit()
#     cursor.close()
#     conn.close()
# #####################################################################

# #####################################################################
# def cleanse_data_b7():
#     conn = psycopg2.connect(
#         host="10.0.0.158",
#         port=5432,
#         user="admin",
#         password="Admin_password_2568",
#         database="mydatabase",
#     )
#     cursor = conn.cursor()

#     cursor.execute("SELECT * FROM test_raw_data_b7")
#     rows = cursor.fetchall()
#     colnames = [desc[0] for desc in cursor.description]
#     df = pd.DataFrame(rows, columns=colnames)

#     numeric_cols = ['runTime', 'downTime', 'quantity_ALL', 'quantity_FG', 'speed_standard', 'speed_actual']
#     for col in numeric_cols:
#         df[col] = pd.to_numeric(df[col], errors='coerce')

#     for col in numeric_cols:
#         valid_mean = df.loc[(df[col].notnull()) & (df[col] > 0), col].mean()
#         df.loc[(df[col].isnull()) | (df[col] < 0), col] = valid_mean

#     df.loc[df['downTime'] > df['runTime'], 'downTime'] = df['runTime']
#     df.loc[df['quantity_FG'] > df['quantity_ALL'], 'quantity_FG'] = df['quantity_ALL']
#     df.loc[df['speed_actual'] > df['speed_standard'], 'speed_actual'] = df['speed_standard']
#     df['status'] = df['status'].fillna('UNKNOWN')

#     cursor.execute("DELETE FROM cleaned_raw_data_b7")
#     conn.commit()

#     local_tz = pendulum.timezone("Asia/Bangkok")
#     TimeStamp = datetime.now(local_tz)

#     insert_query = """
#         INSERT INTO cleaned_raw_data_b7
#         ("runTime", "downTime", "quantity_FG", "quantity_ALL", "speed_standard", "speed_actual", "status", "TimeStamp")
#         VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
#     """
#     for _, row in df.iterrows():
#         cursor.execute(insert_query, (
#             row['runTime'],
#             row['downTime'],
#             row['quantity_FG'],
#             row['quantity_ALL'],
#             row['speed_standard'],
#             row['speed_actual'],
#             row['status'],
#             TimeStamp,
#         ))

#     conn.commit()
#     cursor.close()
#     conn.close()
# #####################################################################

# #####################################################################
# def cleanse_data_b8():
#     conn = psycopg2.connect(
#         host="10.0.0.158",
#         port=5432,
#         user="admin",
#         password="Admin_password_2568",
#         database="mydatabase",
#     )
#     cursor = conn.cursor()

#     cursor.execute("SELECT * FROM test_raw_data_b8")
#     rows = cursor.fetchall()
#     colnames = [desc[0] for desc in cursor.description]
#     df = pd.DataFrame(rows, columns=colnames)

#     numeric_cols = ['runTime', 'downTime', 'quantity_ALL', 'quantity_FG', 'speed_standard', 'speed_actual']
#     for col in numeric_cols:
#         df[col] = pd.to_numeric(df[col], errors='coerce')

#     for col in numeric_cols:
#         valid_mean = df.loc[(df[col].notnull()) & (df[col] > 0), col].mean()
#         df.loc[(df[col].isnull()) | (df[col] < 0), col] = valid_mean

#     df.loc[df['downTime'] > df['runTime'], 'downTime'] = df['runTime']
#     df.loc[df['quantity_FG'] > df['quantity_ALL'], 'quantity_FG'] = df['quantity_ALL']
#     df.loc[df['speed_actual'] > df['speed_standard'], 'speed_actual'] = df['speed_standard']
#     df['status'] = df['status'].fillna('UNKNOWN')

#     cursor.execute("DELETE FROM cleaned_raw_data_b8")
#     conn.commit()

#     local_tz = pendulum.timezone("Asia/Bangkok")
#     TimeStamp = datetime.now(local_tz)

#     insert_query = """
#         INSERT INTO cleaned_raw_data_b8
#         ("runTime", "downTime", "quantity_FG", "quantity_ALL", "speed_standard", "speed_actual", "status", "TimeStamp")
#         VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
#     """
#     for _, row in df.iterrows():
#         cursor.execute(insert_query, (
#             row['runTime'],
#             row['downTime'],
#             row['quantity_FG'],
#             row['quantity_ALL'],
#             row['speed_standard'],
#             row['speed_actual'],
#             row['status'],
#             TimeStamp,
#         ))

#     conn.commit()
#     cursor.close()
#     conn.close()
# #####################################################################

# #####################################################################
# def cleanse_data_b9():
#     conn = psycopg2.connect(
#         host="10.0.0.158",
#         port=5432,
#         user="admin",
#         password="Admin_password_2568",
#         database="mydatabase",
#     )
#     cursor = conn.cursor()

#     cursor.execute("SELECT * FROM test_raw_data_b9")
#     rows = cursor.fetchall()
#     colnames = [desc[0] for desc in cursor.description]
#     df = pd.DataFrame(rows, columns=colnames)

#     numeric_cols = ['runTime', 'downTime', 'quantity_ALL', 'quantity_FG', 'speed_standard', 'speed_actual']
#     for col in numeric_cols:
#         df[col] = pd.to_numeric(df[col], errors='coerce')

#     for col in numeric_cols:
#         valid_mean = df.loc[(df[col].notnull()) & (df[col] > 0), col].mean()
#         df.loc[(df[col].isnull()) | (df[col] < 0), col] = valid_mean

#     df.loc[df['downTime'] > df['runTime'], 'downTime'] = df['runTime']
#     df.loc[df['quantity_FG'] > df['quantity_ALL'], 'quantity_FG'] = df['quantity_ALL']
#     df.loc[df['speed_actual'] > df['speed_standard'], 'speed_actual'] = df['speed_standard']
#     df['status'] = df['status'].fillna('UNKNOWN')

#     cursor.execute("DELETE FROM cleaned_raw_data_b9")
#     conn.commit()

#     local_tz = pendulum.timezone("Asia/Bangkok")
#     TimeStamp = datetime.now(local_tz)

#     insert_query = """
#         INSERT INTO cleaned_raw_data_b9
#         ("runTime", "downTime", "quantity_FG", "quantity_ALL", "speed_standard", "speed_actual", "status", "TimeStamp")
#         VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
#     """
#     for _, row in df.iterrows():
#         cursor.execute(insert_query, (
#             row['runTime'],
#             row['downTime'],
#             row['quantity_FG'],
#             row['quantity_ALL'],
#             row['speed_standard'],
#             row['speed_actual'],
#             row['status'],
#             TimeStamp,
#         ))

#     conn.commit()
#     cursor.close()
#     conn.close()
# #####################################################################

# #####################################################################
# def cleanse_data_b10():
#     conn = psycopg2.connect(
#         host="10.0.0.158",
#         port=5432,
#         user="admin",
#         password="Admin_password_2568",
#         database="mydatabase",
#     )
#     cursor = conn.cursor()

#     cursor.execute("SELECT * FROM test_raw_data_b10")
#     rows = cursor.fetchall()
#     colnames = [desc[0] for desc in cursor.description]
#     df = pd.DataFrame(rows, columns=colnames)

#     numeric_cols = ['runTime', 'downTime', 'quantity_ALL', 'quantity_FG', 'speed_standard', 'speed_actual']
#     for col in numeric_cols:
#         df[col] = pd.to_numeric(df[col], errors='coerce')

#     for col in numeric_cols:
#         valid_mean = df.loc[(df[col].notnull()) & (df[col] > 0), col].mean()
#         df.loc[(df[col].isnull()) | (df[col] < 0), col] = valid_mean

#     df.loc[df['downTime'] > df['runTime'], 'downTime'] = df['runTime']
#     df.loc[df['quantity_FG'] > df['quantity_ALL'], 'quantity_FG'] = df['quantity_ALL']
#     df.loc[df['speed_actual'] > df['speed_standard'], 'speed_actual'] = df['speed_standard']
#     df['status'] = df['status'].fillna('UNKNOWN')

#     cursor.execute("DELETE FROM cleaned_raw_data_b10")
#     conn.commit()

#     local_tz = pendulum.timezone("Asia/Bangkok")
#     TimeStamp = datetime.now(local_tz)

#     insert_query = """
#         INSERT INTO cleaned_raw_data_b10
#         ("runTime", "downTime", "quantity_FG", "quantity_ALL", "speed_standard", "speed_actual", "status", "TimeStamp")
#         VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
#     """
#     for _, row in df.iterrows():
#         cursor.execute(insert_query, (
#             row['runTime'],
#             row['downTime'],
#             row['quantity_FG'],
#             row['quantity_ALL'],
#             row['speed_standard'],
#             row['speed_actual'],
#             row['status'],
#             TimeStamp,
#         ))

#     conn.commit()
#     cursor.close()
#     conn.close()
# #####################################################################

# #####################################################################
# def cleanse_data_b12():
#     conn = psycopg2.connect(
#         host="10.0.0.158",
#         port=5432,
#         user="admin",
#         password="Admin_password_2568",
#         database="mydatabase",
#     )
#     cursor = conn.cursor()

#     cursor.execute("SELECT * FROM test_raw_data_b12")
#     rows = cursor.fetchall()
#     colnames = [desc[0] for desc in cursor.description]
#     df = pd.DataFrame(rows, columns=colnames)

#     numeric_cols = ['runTime', 'downTime', 'quantity_ALL', 'quantity_FG', 'speed_standard', 'speed_actual']
#     for col in numeric_cols:
#         df[col] = pd.to_numeric(df[col], errors='coerce')

#     for col in numeric_cols:
#         valid_mean = df.loc[(df[col].notnull()) & (df[col] > 0), col].mean()
#         df.loc[(df[col].isnull()) | (df[col] < 0), col] = valid_mean

#     df.loc[df['downTime'] > df['runTime'], 'downTime'] = df['runTime']
#     df.loc[df['quantity_FG'] > df['quantity_ALL'], 'quantity_FG'] = df['quantity_ALL']
#     df.loc[df['speed_actual'] > df['speed_standard'], 'speed_actual'] = df['speed_standard']
#     df['status'] = df['status'].fillna('UNKNOWN')

#     cursor.execute("DELETE FROM cleaned_raw_data_b12")
#     conn.commit()

#     local_tz = pendulum.timezone("Asia/Bangkok")
#     TimeStamp = datetime.now(local_tz)

#     insert_query = """
#         INSERT INTO cleaned_raw_data_b12
#         ("runTime", "downTime", "quantity_FG", "quantity_ALL", "speed_standard", "speed_actual", "status", "TimeStamp")
#         VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
#     """
#     for _, row in df.iterrows():
#         cursor.execute(insert_query, (
#             row['runTime'],
#             row['downTime'],
#             row['quantity_FG'],
#             row['quantity_ALL'],
#             row['speed_standard'],
#             row['speed_actual'],
#             row['status'],
#             TimeStamp,
#         ))

#     conn.commit()
#     cursor.close()
#     conn.close()
# #####################################################################

# #####################################################################
# def cleanse_data_b13():
#     conn = psycopg2.connect(
#         host="10.0.0.158",
#         port=5432,
#         user="admin",
#         password="Admin_password_2568",
#         database="mydatabase",
#     )
#     cursor = conn.cursor()

#     cursor.execute("SELECT * FROM test_raw_data_b13")
#     rows = cursor.fetchall()
#     colnames = [desc[0] for desc in cursor.description]
#     df = pd.DataFrame(rows, columns=colnames)

#     numeric_cols = ['runTime', 'downTime', 'quantity_ALL', 'quantity_FG', 'speed_standard', 'speed_actual']
#     for col in numeric_cols:
#         df[col] = pd.to_numeric(df[col], errors='coerce')

#     for col in numeric_cols:
#         valid_mean = df.loc[(df[col].notnull()) & (df[col] > 0), col].mean()
#         df.loc[(df[col].isnull()) | (df[col] < 0), col] = valid_mean

#     df.loc[df['downTime'] > df['runTime'], 'downTime'] = df['runTime']
#     df.loc[df['quantity_FG'] > df['quantity_ALL'], 'quantity_FG'] = df['quantity_ALL']
#     df.loc[df['speed_actual'] > df['speed_standard'], 'speed_actual'] = df['speed_standard']
#     df['status'] = df['status'].fillna('UNKNOWN')

#     cursor.execute("DELETE FROM cleaned_raw_data_b13")
#     conn.commit()

#     local_tz = pendulum.timezone("Asia/Bangkok")
#     TimeStamp = datetime.now(local_tz)

#     insert_query = """
#         INSERT INTO cleaned_raw_data_b13
#         ("runTime", "downTime", "quantity_FG", "quantity_ALL", "speed_standard", "speed_actual", "status", "TimeStamp")
#         VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
#     """
#     for _, row in df.iterrows():
#         cursor.execute(insert_query, (
#             row['runTime'],
#             row['downTime'],
#             row['quantity_FG'],
#             row['quantity_ALL'],
#             row['speed_standard'],
#             row['speed_actual'],
#             row['status'],
#             TimeStamp,
#         ))

#     conn.commit()
#     cursor.close()
#     conn.close()
# #####################################################################

# #####################################################################
# def cleanse_data_b16():
#     conn = psycopg2.connect(
#         host="10.0.0.158",
#         port=5432,
#         user="admin",
#         password="Admin_password_2568",
#         database="mydatabase",
#     )
#     cursor = conn.cursor()

#     cursor.execute("SELECT * FROM test_raw_data_b16")
#     rows = cursor.fetchall()
#     colnames = [desc[0] for desc in cursor.description]
#     df = pd.DataFrame(rows, columns=colnames)

#     numeric_cols = ['runTime', 'downTime', 'quantity_ALL', 'quantity_FG', 'speed_standard', 'speed_actual']
#     for col in numeric_cols:
#         df[col] = pd.to_numeric(df[col], errors='coerce')

#     for col in numeric_cols:
#         valid_mean = df.loc[(df[col].notnull()) & (df[col] > 0), col].mean()
#         df.loc[(df[col].isnull()) | (df[col] < 0), col] = valid_mean

#     df.loc[df['downTime'] > df['runTime'], 'downTime'] = df['runTime']
#     df.loc[df['quantity_FG'] > df['quantity_ALL'], 'quantity_FG'] = df['quantity_ALL']
#     df.loc[df['speed_actual'] > df['speed_standard'], 'speed_actual'] = df['speed_standard']
#     df['status'] = df['status'].fillna('UNKNOWN')

#     cursor.execute("DELETE FROM cleaned_raw_data_b16")
#     conn.commit()

#     local_tz = pendulum.timezone("Asia/Bangkok")
#     TimeStamp = datetime.now(local_tz)

#     insert_query = """
#         INSERT INTO cleaned_raw_data_b16
#         ("runTime", "downTime", "quantity_FG", "quantity_ALL", "speed_standard", "speed_actual", "status", "TimeStamp")
#         VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
#     """
#     for _, row in df.iterrows():
#         cursor.execute(insert_query, (
#             row['runTime'],
#             row['downTime'],
#             row['quantity_FG'],
#             row['quantity_ALL'],
#             row['speed_standard'],
#             row['speed_actual'],
#             row['status'],
#             TimeStamp,
#         ))

#     conn.commit()
#     cursor.close()
#     conn.close()
# #####################################################################

# #####################################################################
# def cleanse_data_b17():
#     conn = psycopg2.connect(
#         host="10.0.0.158",
#         port=5432,
#         user="admin",
#         password="Admin_password_2568",
#         database="mydatabase",
#     )
#     cursor = conn.cursor()

#     cursor.execute("SELECT * FROM test_raw_data_b17")
#     rows = cursor.fetchall()
#     colnames = [desc[0] for desc in cursor.description]
#     df = pd.DataFrame(rows, columns=colnames)

#     numeric_cols = ['runTime', 'downTime', 'quantity_ALL', 'quantity_FG', 'speed_standard', 'speed_actual']
#     for col in numeric_cols:
#         df[col] = pd.to_numeric(df[col], errors='coerce')

#     for col in numeric_cols:
#         valid_mean = df.loc[(df[col].notnull()) & (df[col] > 0), col].mean()
#         df.loc[(df[col].isnull()) | (df[col] < 0), col] = valid_mean

#     df.loc[df['downTime'] > df['runTime'], 'downTime'] = df['runTime']
#     df.loc[df['quantity_FG'] > df['quantity_ALL'], 'quantity_FG'] = df['quantity_ALL']
#     df.loc[df['speed_actual'] > df['speed_standard'], 'speed_actual'] = df['speed_standard']
#     df['status'] = df['status'].fillna('UNKNOWN')

#     cursor.execute("DELETE FROM cleaned_raw_data_b17")
#     conn.commit()

#     local_tz = pendulum.timezone("Asia/Bangkok")
#     TimeStamp = datetime.now(local_tz)

#     insert_query = """
#         INSERT INTO cleaned_raw_data_b17
#         ("runTime", "downTime", "quantity_FG", "quantity_ALL", "speed_standard", "speed_actual", "status", "TimeStamp")
#         VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
#     """
#     for _, row in df.iterrows():
#         cursor.execute(insert_query, (
#             row['runTime'],
#             row['downTime'],
#             row['quantity_FG'],
#             row['quantity_ALL'],
#             row['speed_standard'],
#             row['speed_actual'],
#             row['status'],
#             TimeStamp,
#         ))

#     conn.commit()
#     cursor.close()
#     conn.close()
# #####################################################################

# #####################################################################
# def cleanse_data_b18():
#     conn = psycopg2.connect(
#         host="10.0.0.158",
#         port=5432,
#         user="admin",
#         password="Admin_password_2568",
#         database="mydatabase",
#     )
#     cursor = conn.cursor()

#     cursor.execute("SELECT * FROM test_raw_data_b18")
#     rows = cursor.fetchall()
#     colnames = [desc[0] for desc in cursor.description]
#     df = pd.DataFrame(rows, columns=colnames)

#     numeric_cols = ['runTime', 'downTime', 'quantity_ALL', 'quantity_FG', 'speed_standard', 'speed_actual']
#     for col in numeric_cols:
#         df[col] = pd.to_numeric(df[col], errors='coerce')

#     for col in numeric_cols:
#         valid_mean = df.loc[(df[col].notnull()) & (df[col] > 0), col].mean()
#         df.loc[(df[col].isnull()) | (df[col] < 0), col] = valid_mean

#     df.loc[df['downTime'] > df['runTime'], 'downTime'] = df['runTime']
#     df.loc[df['quantity_FG'] > df['quantity_ALL'], 'quantity_FG'] = df['quantity_ALL']
#     df.loc[df['speed_actual'] > df['speed_standard'], 'speed_actual'] = df['speed_standard']
#     df['status'] = df['status'].fillna('UNKNOWN')

#     cursor.execute("DELETE FROM cleaned_raw_data_b18")
#     conn.commit()

#     local_tz = pendulum.timezone("Asia/Bangkok")
#     TimeStamp = datetime.now(local_tz)

#     insert_query = """
#         INSERT INTO cleaned_raw_data_b18
#         ("runTime", "downTime", "quantity_FG", "quantity_ALL", "speed_standard", "speed_actual", "status", "TimeStamp")
#         VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
#     """
#     for _, row in df.iterrows():
#         cursor.execute(insert_query, (
#             row['runTime'],
#             row['downTime'],
#             row['quantity_FG'],
#             row['quantity_ALL'],
#             row['speed_standard'],
#             row['speed_actual'],
#             row['status'],
#             TimeStamp,
#         ))

#     conn.commit()
#     cursor.close()
#     conn.close()
# #####################################################################





# ################################ Calculate #####################################

# def calculate_data():
#     conn = psycopg2.connect(
#         host="10.0.0.158",
#         port=5432,
#         user="admin",
#         password="Admin_password_2568",
#         database="mydatabase",
#     )
#     cursor = conn.cursor()

#     # ดึงข้อมูลจาก cleaned_raw_data_a1
#     cursor.execute("SELECT * FROM cleaned_raw_data_a1")
#     rows = cursor.fetchall()
#     colnames = [desc[0] for desc in cursor.description]
#     df = pd.DataFrame(rows, columns=colnames)

#     # คำนวณ A, P, Q, OEE
#     df['A'] = round(((df['runTime'] - df['downTime']) / df['runTime']) * 100, 2)
#     df['Q'] = round((df['quantity_FG'] / df['quantity_ALL']) * 100, 2)
#     df['P'] = round((df['speed_actual'] / df['speed_standard']) * 100, 2)
#     df['OEE'] = round((df['A']/100 * df['P']/100 * df['Q']/100) * 100, 2)

#     # ลบข้อมูลเก่าออก (ถ้าอยากให้สะสมลบอันนี้ออก)
#     cursor.execute("DELETE FROM calculate_data_a1")
#     conn.commit()

#     local_tz = pendulum.timezone("Asia/Bangkok")
#     TimeStamp = datetime.now(local_tz)

#     insert_query = """
#         INSERT INTO calculate_data_a1
#         ("A", "Q", "P", "OEE", "status", "TimeStamp")
#         VALUES (%s,%s,%s,%s,%s,%s)
#     """
#     for _, row in df.iterrows():
#         cursor.execute(insert_query, (
#             row['A'],
#             row['P'],
#             row['Q'],
#             row['OEE'],
#             row['status'],
#             TimeStamp,
#         ))

#     conn.commit()
#     cursor.close()
#     conn.close()

# ###############################################################
# def calculate_data_a2():
#     conn = psycopg2.connect(
#         host="10.0.0.158",
#         port=5432,
#         user="admin",
#         password="Admin_password_2568",
#         database="mydatabase",
#     )
#     cursor = conn.cursor()

#     # ดึงข้อมูลจาก cleaned_raw_data_a1
#     cursor.execute("SELECT * FROM cleaned_raw_data_a2")
#     rows = cursor.fetchall()
#     colnames = [desc[0] for desc in cursor.description]
#     df = pd.DataFrame(rows, columns=colnames)

#     # คำนวณ A, P, Q, OEE
#     df['A'] = round(((df['runTime'] - df['downTime']) / df['runTime']) * 100, 2)
#     df['Q'] = round((df['quantity_FG'] / df['quantity_ALL']) * 100, 2)
#     df['P'] = round((df['speed_actual'] / df['speed_standard']) * 100, 2)
#     df['OEE'] = round((df['A']/100 * df['P']/100 * df['Q']/100) * 100, 2)

#     # ลบข้อมูลเก่าออก (ถ้าอยากให้สะสมลบอันนี้ออก)
#     cursor.execute("DELETE FROM calculate_data_a2")
#     conn.commit()

#     local_tz = pendulum.timezone("Asia/Bangkok")
#     TimeStamp = datetime.now(local_tz)

#     insert_query = """
#         INSERT INTO calculate_data_a2
#         ("A", "Q", "P", "OEE", "status", "TimeStamp")
#         VALUES (%s,%s,%s,%s,%s,%s)
#     """
#     for _, row in df.iterrows():
#         cursor.execute(insert_query, (
#             row['A'],
#             row['P'],
#             row['Q'],
#             row['OEE'],
#             row['status'],
#             TimeStamp,
#         ))

#     conn.commit()
#     cursor.close()
#     conn.close()
# ####################################################################
# def calculate_data_a3():
#     conn = psycopg2.connect(
#         host="10.0.0.158",
#         port=5432,
#         user="admin",
#         password="Admin_password_2568",
#         database="mydatabase",
#     )
#     cursor = conn.cursor()

#     # ดึงข้อมูลจาก cleaned_raw_data_a1
#     cursor.execute("SELECT * FROM cleaned_raw_data_a3")
#     rows = cursor.fetchall()
#     colnames = [desc[0] for desc in cursor.description]
#     df = pd.DataFrame(rows, columns=colnames)

#     # คำนวณ A, P, Q, OEE
#     df['A'] = round(((df['runTime'] - df['downTime']) / df['runTime']) * 100, 2)
#     df['Q'] = round((df['quantity_FG'] / df['quantity_ALL']) * 100, 2)
#     df['P'] = round((df['speed_actual'] / df['speed_standard']) * 100, 2)
#     df['OEE'] = round((df['A']/100 * df['P']/100 * df['Q']/100) * 100, 2)

#     # ลบข้อมูลเก่าออก (ถ้าอยากให้สะสมลบอันนี้ออก)
#     cursor.execute("DELETE FROM calculate_data_a3")
#     conn.commit()

#     local_tz = pendulum.timezone("Asia/Bangkok")
#     TimeStamp = datetime.now(local_tz)

#     insert_query = """
#         INSERT INTO calculate_data_a3
#         ("A", "Q", "P", "OEE", "status", "TimeStamp")
#         VALUES (%s,%s,%s,%s,%s,%s)
#     """
#     for _, row in df.iterrows():
#         cursor.execute(insert_query, (
#             row['A'],
#             row['P'],
#             row['Q'],
#             row['OEE'],
#             row['status'],
#             TimeStamp,
#         ))

#     conn.commit()
#     cursor.close()
#     conn.close()

# ####################################################################
# def calculate_data_b7():
#     conn = psycopg2.connect(
#         host="10.0.0.158",
#         port=5432,
#         user="admin",
#         password="Admin_password_2568",
#         database="mydatabase",
#     )
#     cursor = conn.cursor()

#     # ดึงข้อมูลจาก cleaned_raw_data_a1
#     cursor.execute("SELECT * FROM cleaned_raw_data_b7")
#     rows = cursor.fetchall()
#     colnames = [desc[0] for desc in cursor.description]
#     df = pd.DataFrame(rows, columns=colnames)

#     # คำนวณ A, P, Q, OEE
#     df['A'] = round(((df['runTime'] - df['downTime']) / df['runTime']) * 100, 2)
#     df['Q'] = round((df['quantity_FG'] / df['quantity_ALL']) * 100, 2)
#     df['P'] = round((df['speed_actual'] / df['speed_standard']) * 100, 2)
#     df['OEE'] = round((df['A']/100 * df['P']/100 * df['Q']/100) * 100, 2)

#     # ลบข้อมูลเก่าออก (ถ้าอยากให้สะสมลบอันนี้ออก)
#     cursor.execute("DELETE FROM calculate_data_b7")
#     conn.commit()

#     local_tz = pendulum.timezone("Asia/Bangkok")
#     TimeStamp = datetime.now(local_tz)

#     insert_query = """
#         INSERT INTO calculate_data_b7
#         ("A", "Q", "P", "OEE", "status", "TimeStamp")
#         VALUES (%s,%s,%s,%s,%s,%s)
#     """
#     for _, row in df.iterrows():
#         cursor.execute(insert_query, (
#             row['A'],
#             row['P'],
#             row['Q'],
#             row['OEE'],
#             row['status'],
#             TimeStamp,
#         ))

#     conn.commit()
#     cursor.close()
#     conn.close()

# ####################################################################
# def calculate_data_b8():
#     conn = psycopg2.connect(
#         host="10.0.0.158",
#         port=5432,
#         user="admin",
#         password="Admin_password_2568",
#         database="mydatabase",
#     )
#     cursor = conn.cursor()

#     # ดึงข้อมูลจาก cleaned_raw_data_a1
#     cursor.execute("SELECT * FROM cleaned_raw_data_b8")
#     rows = cursor.fetchall()
#     colnames = [desc[0] for desc in cursor.description]
#     df = pd.DataFrame(rows, columns=colnames)

#     # คำนวณ A, P, Q, OEE
#     df['A'] = round(((df['runTime'] - df['downTime']) / df['runTime']) * 100, 2)
#     df['Q'] = round((df['quantity_FG'] / df['quantity_ALL']) * 100, 2)
#     df['P'] = round((df['speed_actual'] / df['speed_standard']) * 100, 2)
#     df['OEE'] = round((df['A']/100 * df['P']/100 * df['Q']/100) * 100, 2)

#     # ลบข้อมูลเก่าออก (ถ้าอยากให้สะสมลบอันนี้ออก)
#     cursor.execute("DELETE FROM calculate_data_b8")
#     conn.commit()

#     local_tz = pendulum.timezone("Asia/Bangkok")
#     TimeStamp = datetime.now(local_tz)

#     insert_query = """
#         INSERT INTO calculate_data_b8
#         ("A", "Q", "P", "OEE", "status", "TimeStamp")
#         VALUES (%s,%s,%s,%s,%s,%s)
#     """
#     for _, row in df.iterrows():
#         cursor.execute(insert_query, (
#             row['A'],
#             row['P'],
#             row['Q'],
#             row['OEE'],
#             row['status'],
#             TimeStamp,
#         ))

#     conn.commit()
#     cursor.close()
#     conn.close()

# ####################################################################
# def calculate_data_b9():
#     conn = psycopg2.connect(
#         host="10.0.0.158",
#         port=5432,
#         user="admin",
#         password="Admin_password_2568",
#         database="mydatabase",
#     )
#     cursor = conn.cursor()

#     # ดึงข้อมูลจาก cleaned_raw_data_a1
#     cursor.execute("SELECT * FROM cleaned_raw_data_b9")
#     rows = cursor.fetchall()
#     colnames = [desc[0] for desc in cursor.description]
#     df = pd.DataFrame(rows, columns=colnames)

#     # คำนวณ A, P, Q, OEE
#     df['A'] = round(((df['runTime'] - df['downTime']) / df['runTime']) * 100, 2)
#     df['Q'] = round((df['quantity_FG'] / df['quantity_ALL']) * 100, 2)
#     df['P'] = round((df['speed_actual'] / df['speed_standard']) * 100, 2)
#     df['OEE'] = round((df['A']/100 * df['P']/100 * df['Q']/100) * 100, 2)

#     # ลบข้อมูลเก่าออก (ถ้าอยากให้สะสมลบอันนี้ออก)
#     cursor.execute("DELETE FROM calculate_data_b9")
#     conn.commit()

#     local_tz = pendulum.timezone("Asia/Bangkok")
#     TimeStamp = datetime.now(local_tz)

#     insert_query = """
#         INSERT INTO calculate_data_b9
#         ("A", "Q", "P", "OEE", "status", "TimeStamp")
#         VALUES (%s,%s,%s,%s,%s,%s)
#     """
#     for _, row in df.iterrows():
#         cursor.execute(insert_query, (
#             row['A'],
#             row['P'],
#             row['Q'],
#             row['OEE'],
#             row['status'],
#             TimeStamp,
#         ))

#     conn.commit()
#     cursor.close()
#     conn.close()

# ####################################################################
# def calculate_data_b10():
#     conn = psycopg2.connect(
#         host="10.0.0.158",
#         port=5432,
#         user="admin",
#         password="Admin_password_2568",
#         database="mydatabase",
#     )
#     cursor = conn.cursor()

#     # ดึงข้อมูลจาก cleaned_raw_data_a1
#     cursor.execute("SELECT * FROM cleaned_raw_data_b10")
#     rows = cursor.fetchall()
#     colnames = [desc[0] for desc in cursor.description]
#     df = pd.DataFrame(rows, columns=colnames)

#     # คำนวณ A, P, Q, OEE
#     df['A'] = round(((df['runTime'] - df['downTime']) / df['runTime']) * 100, 2)
#     df['Q'] = round((df['quantity_FG'] / df['quantity_ALL']) * 100, 2)
#     df['P'] = round((df['speed_actual'] / df['speed_standard']) * 100, 2)
#     df['OEE'] = round((df['A']/100 * df['P']/100 * df['Q']/100) * 100, 2)

#     # ลบข้อมูลเก่าออก (ถ้าอยากให้สะสมลบอันนี้ออก)
#     cursor.execute("DELETE FROM calculate_data_b10")
#     conn.commit()

#     local_tz = pendulum.timezone("Asia/Bangkok")
#     TimeStamp = datetime.now(local_tz)

#     insert_query = """
#         INSERT INTO calculate_data_b10
#         ("A", "Q", "P", "OEE", "status", "TimeStamp")
#         VALUES (%s,%s,%s,%s,%s,%s)
#     """
#     for _, row in df.iterrows():
#         cursor.execute(insert_query, (
#             row['A'],
#             row['P'],
#             row['Q'],
#             row['OEE'],
#             row['status'],
#             TimeStamp,
#         ))

#     conn.commit()
#     cursor.close()
#     conn.close()
# #######################################################################################

# ####################################################################
# def calculate_data_b12():
#     conn = psycopg2.connect(
#         host="10.0.0.158",
#         port=5432,
#         user="admin",
#         password="Admin_password_2568",
#         database="mydatabase",
#     )
#     cursor = conn.cursor()

#     # ดึงข้อมูลจาก cleaned_raw_data_a1
#     cursor.execute("SELECT * FROM cleaned_raw_data_b12")
#     rows = cursor.fetchall()
#     colnames = [desc[0] for desc in cursor.description]
#     df = pd.DataFrame(rows, columns=colnames)

#     # คำนวณ A, P, Q, OEE
#     df['A'] = round(((df['runTime'] - df['downTime']) / df['runTime']) * 100, 2)
#     df['Q'] = round((df['quantity_FG'] / df['quantity_ALL']) * 100, 2)
#     df['P'] = round((df['speed_actual'] / df['speed_standard']) * 100, 2)
#     df['OEE'] = round((df['A']/100 * df['P']/100 * df['Q']/100) * 100, 2)

#     # ลบข้อมูลเก่าออก (ถ้าอยากให้สะสมลบอันนี้ออก)
#     cursor.execute("DELETE FROM calculate_data_b12")
#     conn.commit()

#     local_tz = pendulum.timezone("Asia/Bangkok")
#     TimeStamp = datetime.now(local_tz)

#     insert_query = """
#         INSERT INTO calculate_data_b12
#         ("A", "Q", "P", "OEE", "status", "TimeStamp")
#         VALUES (%s,%s,%s,%s,%s,%s)
#     """
#     for _, row in df.iterrows():
#         cursor.execute(insert_query, (
#             row['A'],
#             row['P'],
#             row['Q'],
#             row['OEE'],
#             row['status'],
#             TimeStamp,
#         ))

#     conn.commit()
#     cursor.close()
#     conn.close()
# #######################################################################################

# ####################################################################
# def calculate_data_b13():
#     conn = psycopg2.connect(
#         host="10.0.0.158",
#         port=5432,
#         user="admin",
#         password="Admin_password_2568",
#         database="mydatabase",
#     )
#     cursor = conn.cursor()

#     # ดึงข้อมูลจาก cleaned_raw_data_a1
#     cursor.execute("SELECT * FROM cleaned_raw_data_b13")
#     rows = cursor.fetchall()
#     colnames = [desc[0] for desc in cursor.description]
#     df = pd.DataFrame(rows, columns=colnames)

#     # คำนวณ A, P, Q, OEE
#     df['A'] = round(((df['runTime'] - df['downTime']) / df['runTime']) * 100, 2)
#     df['Q'] = round((df['quantity_FG'] / df['quantity_ALL']) * 100, 2)
#     df['P'] = round((df['speed_actual'] / df['speed_standard']) * 100, 2)
#     df['OEE'] = round((df['A']/100 * df['P']/100 * df['Q']/100) * 100, 2)

#     # ลบข้อมูลเก่าออก (ถ้าอยากให้สะสมลบอันนี้ออก)
#     cursor.execute("DELETE FROM calculate_data_b13")
#     conn.commit()

#     local_tz = pendulum.timezone("Asia/Bangkok")
#     TimeStamp = datetime.now(local_tz)

#     insert_query = """
#         INSERT INTO calculate_data_b13
#         ("A", "Q", "P", "OEE", "status", "TimeStamp")
#         VALUES (%s,%s,%s,%s,%s,%s)
#     """
#     for _, row in df.iterrows():
#         cursor.execute(insert_query, (
#             row['A'],
#             row['P'],
#             row['Q'],
#             row['OEE'],
#             row['status'],
#             TimeStamp,
#         ))

#     conn.commit()
#     cursor.close()
#     conn.close()
# #######################################################################################

# ####################################################################
# def calculate_data_b16():
#     conn = psycopg2.connect(
#         host="10.0.0.158",
#         port=5432,
#         user="admin",
#         password="Admin_password_2568",
#         database="mydatabase",
#     )
#     cursor = conn.cursor()

#     # ดึงข้อมูลจาก cleaned_raw_data_a1
#     cursor.execute("SELECT * FROM cleaned_raw_data_b16")
#     rows = cursor.fetchall()
#     colnames = [desc[0] for desc in cursor.description]
#     df = pd.DataFrame(rows, columns=colnames)

#     # คำนวณ A, P, Q, OEE
#     df['A'] = round(((df['runTime'] - df['downTime']) / df['runTime']) * 100, 2)
#     df['Q'] = round((df['quantity_FG'] / df['quantity_ALL']) * 100, 2)
#     df['P'] = round((df['speed_actual'] / df['speed_standard']) * 100, 2)
#     df['OEE'] = round((df['A']/100 * df['P']/100 * df['Q']/100) * 100, 2)

#     # ลบข้อมูลเก่าออก (ถ้าอยากให้สะสมลบอันนี้ออก)
#     cursor.execute("DELETE FROM calculate_data_b16")
#     conn.commit()

#     local_tz = pendulum.timezone("Asia/Bangkok")
#     TimeStamp = datetime.now(local_tz)

#     insert_query = """
#         INSERT INTO calculate_data_b16
#         ("A", "Q", "P", "OEE", "status", "TimeStamp")
#         VALUES (%s,%s,%s,%s,%s,%s)
#     """
#     for _, row in df.iterrows():
#         cursor.execute(insert_query, (
#             row['A'],
#             row['P'],
#             row['Q'],
#             row['OEE'],
#             row['status'],
#             TimeStamp,
#         ))

#     conn.commit()
#     cursor.close()
#     conn.close()
# #######################################################################################

# ####################################################################
# def calculate_data_b17():
#     conn = psycopg2.connect(
#         host="10.0.0.158",
#         port=5432,
#         user="admin",
#         password="Admin_password_2568",
#         database="mydatabase",
#     )
#     cursor = conn.cursor()

#     # ดึงข้อมูลจาก cleaned_raw_data_a1
#     cursor.execute("SELECT * FROM cleaned_raw_data_b17")
#     rows = cursor.fetchall()
#     colnames = [desc[0] for desc in cursor.description]
#     df = pd.DataFrame(rows, columns=colnames)

#     # คำนวณ A, P, Q, OEE
#     df['A'] = round(((df['runTime'] - df['downTime']) / df['runTime']) * 100, 2)
#     df['Q'] = round((df['quantity_FG'] / df['quantity_ALL']) * 100, 2)
#     df['P'] = round((df['speed_actual'] / df['speed_standard']) * 100, 2)
#     df['OEE'] = round((df['A']/100 * df['P']/100 * df['Q']/100) * 100, 2)

#     # ลบข้อมูลเก่าออก (ถ้าอยากให้สะสมลบอันนี้ออก)
#     cursor.execute("DELETE FROM calculate_data_b17")
#     conn.commit()

#     local_tz = pendulum.timezone("Asia/Bangkok")
#     TimeStamp = datetime.now(local_tz)

#     insert_query = """
#         INSERT INTO calculate_data_b17
#         ("A", "Q", "P", "OEE", "status", "TimeStamp")
#         VALUES (%s,%s,%s,%s,%s,%s)
#     """
#     for _, row in df.iterrows():
#         cursor.execute(insert_query, (
#             row['A'],
#             row['P'],
#             row['Q'],
#             row['OEE'],
#             row['status'],
#             TimeStamp,
#         ))

#     conn.commit()
#     cursor.close()
#     conn.close()
# #######################################################################################

# ####################################################################
# def calculate_data_b18():
#     conn = psycopg2.connect(
#         host="10.0.0.158",
#         port=5432,
#         user="admin",
#         password="Admin_password_2568",
#         database="mydatabase",
#     )
#     cursor = conn.cursor()

#     # ดึงข้อมูลจาก cleaned_raw_data_a1
#     cursor.execute("SELECT * FROM cleaned_raw_data_b18")
#     rows = cursor.fetchall()
#     colnames = [desc[0] for desc in cursor.description]
#     df = pd.DataFrame(rows, columns=colnames)

#     # คำนวณ A, P, Q, OEE
#     df['A'] = round(((df['runTime'] - df['downTime']) / df['runTime']) * 100, 2)
#     df['Q'] = round((df['quantity_FG'] / df['quantity_ALL']) * 100, 2)
#     df['P'] = round((df['speed_actual'] / df['speed_standard']) * 100, 2)
#     df['OEE'] = round((df['A']/100 * df['P']/100 * df['Q']/100) * 100, 2)

#     # ลบข้อมูลเก่าออก (ถ้าอยากให้สะสมลบอันนี้ออก)
#     cursor.execute("DELETE FROM calculate_data_b18")
#     conn.commit()

#     local_tz = pendulum.timezone("Asia/Bangkok")
#     TimeStamp = datetime.now(local_tz)

#     insert_query = """
#         INSERT INTO calculate_data_b18
#         ("A", "Q", "P", "OEE", "status", "TimeStamp")
#         VALUES (%s,%s,%s,%s,%s,%s)
#     """
#     for _, row in df.iterrows():
#         cursor.execute(insert_query, (
#             row['A'],
#             row['P'],
#             row['Q'],
#             row['OEE'],
#             row['status'],
#             TimeStamp,
#         ))

#     conn.commit()
#     cursor.close()
#     conn.close()
# #######################################################################################

# with DAG(
#     dag_id="dag_data_cleanse_raw_data_a1",
#     start_date=datetime(2025, 9, 22),
#     schedule_interval="@hourly",
#     catchup=False,
#     default_args={
#         'retries': 0
#     },
#     tags=["cleansing", "data_pipeline", "test_raw_data"],
# ) as dag:

#     task_cleanse_data = PythonOperator(
#         task_id="cleanse_data",
#         python_callable=cleanse_data,
#     )

#     task_cleanse_data_a2 = PythonOperator(
#         task_id="cleanse_data_a2",
#         python_callable=cleanse_data_a2,
#     )

#     task_cleanse_data_a3 = PythonOperator(
#         task_id="cleanse_data_a3",
#         python_callable=cleanse_data_a3,
#     )

#     task_cleanse_data_b7 = PythonOperator(
#         task_id="cleanse_data_b7",
#         python_callable=cleanse_data_b7,
#     )

#     task_cleanse_data_b8 = PythonOperator(
#         task_id="cleanse_data_b8",
#         python_callable=cleanse_data_b8,
#     )

#     task_cleanse_data_b9 = PythonOperator(
#         task_id="cleanse_data_b9",
#         python_callable=cleanse_data_b9,
#     )

#     task_cleanse_data_b10 = PythonOperator(
#         task_id="cleanse_data_b10",
#         python_callable=cleanse_data_b10,
#     )

#     task_cleanse_data_b12 = PythonOperator(
#         task_id="cleanse_data_b12",
#         python_callable=cleanse_data_b12,
#     )

#     task_cleanse_data_b13 = PythonOperator(
#         task_id="cleanse_data_b13",
#         python_callable=cleanse_data_b13,
#     )

#     task_cleanse_data_b16 = PythonOperator(
#         task_id="cleanse_data_b16",
#         python_callable=cleanse_data_b16,
#     )

#     task_cleanse_data_b17 = PythonOperator(
#         task_id="cleanse_data_b17",
#         python_callable=cleanse_data_b17,
#     )

#     task_cleanse_data_b18 = PythonOperator(
#         task_id="cleanse_data_b18",
#         python_callable=cleanse_data_b18,
#     )

# #############################################################
#     task_calculate_data = PythonOperator(
#         task_id="calculate_data",
#         python_callable=calculate_data,
#     )

#     task_calculate_data_a2 = PythonOperator(
#         task_id="calculate_data_a2",
#         python_callable=calculate_data_a2,
#     )

#     task_calculate_data_a3 = PythonOperator(
#         task_id="calculate_data_a3",
#         python_callable=calculate_data_a3,
#     )

#     task_calculate_data_b7 = PythonOperator(
#         task_id="calculate_data_b7",
#         python_callable=calculate_data_b7,
#     )

#     task_calculate_data_b8 = PythonOperator(
#         task_id="calculate_data_b8",
#         python_callable=calculate_data_b8,
#     )

#     task_calculate_data_b9 = PythonOperator(
#         task_id="calculate_data_b9",
#         python_callable=calculate_data_b9,
#     )

#     task_calculate_data_b10 = PythonOperator(
#         task_id="calculate_data_b10",
#         python_callable=calculate_data_b10,
#     )

#     task_calculate_data_b12 = PythonOperator(
#         task_id="calculate_data_b12",
#         python_callable=calculate_data_b12,
#     )

#     task_calculate_data_b13 = PythonOperator(
#         task_id="calculate_data_b13",
#         python_callable=calculate_data_b13,
#     )

#     task_calculate_data_b16 = PythonOperator(
#         task_id="calculate_data_b16",
#         python_callable=calculate_data_b16,
#     )

#     task_calculate_data_b17 = PythonOperator(
#         task_id="calculate_data_b17",
#         python_callable=calculate_data_b17,
#     )

#     task_calculate_data_b18 = PythonOperator(
#         task_id="calculate_data_b18",
#         python_callable=calculate_data_b18,
#     )

#     task_cleanse_data >> task_calculate_data, task_cleanse_data_a2 >> task_calculate_data_a2, task_cleanse_data_a3 >> task_calculate_data_a3, task_cleanse_data_b7 >> task_calculate_data_b7, task_cleanse_data_b8 >> task_calculate_data_b8, task_cleanse_data_b9 >> task_calculate_data_b9, task_cleanse_data_b10 >> task_calculate_data_b10,
#     task_cleanse_data_b12 >> task_calculate_data_b12, task_cleanse_data_b13 >> task_calculate_data_b13, task_cleanse_data_b16 >> task_calculate_data_b16, task_cleanse_data_b17 >> task_calculate_data_b17, task_cleanse_data_b18 >> task_calculate_data_b18



























from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import psycopg2
import pandas as pd
import pendulum

# ------------------ ฟังก์ชัน Cleanse ------------------
def cleanse_data(machine_id):
    conn = psycopg2.connect(
        host="10.0.0.158",
        port=5432,
        user="admin",
        password="Admin_password_2568",
        database="mydatabase",
    )
    cursor = conn.cursor()

    raw_table = f"test_raw_data_{machine_id}"
    cleaned_table = f"cleaned_raw_data_{machine_id}"

    cursor.execute(f"SELECT * FROM {raw_table}")
    rows = cursor.fetchall()
    colnames = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(rows, columns=colnames)

    numeric_cols = ['runTime', 'downTime', 'quantity_ALL', 'quantity_FG', 'speed_standard', 'speed_actual']
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    for col in numeric_cols:
        valid_mean = df.loc[(df[col].notnull()) & (df[col] > 0), col].mean()
        df.loc[(df[col].isnull()) | (df[col] < 0), col] = valid_mean

    df.loc[df['downTime'] > df['runTime'], 'downTime'] = df['runTime']
    df.loc[df['quantity_FG'] > df['quantity_ALL'], 'quantity_FG'] = df['quantity_ALL']
    df.loc[df['speed_actual'] > df['speed_standard'], 'speed_actual'] = df['speed_standard']
    df['status'] = df['status'].fillna('UNKNOWN')

    cursor.execute(f"DELETE FROM {cleaned_table}")
    conn.commit()

    local_tz = pendulum.timezone("Asia/Bangkok")
    TimeStamp = datetime.now(local_tz)

    insert_query = f"""
        INSERT INTO {cleaned_table}
        ("runTime", "downTime", "quantity_FG", "quantity_ALL", 
         "speed_standard", "speed_actual", "status", "TimeStamp")
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
            TimeStamp,
        ))

    conn.commit()
    cursor.close()
    conn.close()

    pass


# ------------------ ฟังก์ชัน Calculate ------------------
def calculate_data(machine_id):
    conn = psycopg2.connect(
        host="10.0.0.158",
        port=5432,
        user="admin",
        password="Admin_password_2568",
        database="mydatabase",
    )
    cursor = conn.cursor()

    cleaned_table = f"cleaned_raw_data_{machine_id}"
    calculate_table = f"calculate_data_{machine_id}"

    cursor.execute(f"SELECT * FROM {cleaned_table}")
    rows = cursor.fetchall()
    colnames = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(rows, columns=colnames)

    df['A'] = round(((df['runTime'] - df['downTime']) / df['runTime']) * 100, 2)
    df['Q'] = round((df['quantity_FG'] / df['quantity_ALL']) * 100, 2)
    df['P'] = round((df['speed_actual'] / df['speed_standard']) * 100, 2)
    df['OEE'] = round((df['A']/100 * df['P']/100 * df['Q']/100) * 100, 2)

    cursor.execute(f"DELETE FROM {calculate_table}")
    conn.commit()

    local_tz = pendulum.timezone("Asia/Bangkok")
    TimeStamp = datetime.now(local_tz)

    insert_query = f"""
        INSERT INTO {calculate_table}
        ("A", "Q", "P", "OEE", "status", "TimeStamp")
        VALUES (%s,%s,%s,%s,%s,%s)
    """
    for _, row in df.iterrows():
        cursor.execute(insert_query, (
            row['A'],
            row['Q'],
            row['P'],
            row['OEE'],
            row['status'],
            TimeStamp,
        ))

    conn.commit()
    cursor.close()
    conn.close()

    pass

with DAG(
    dag_id="dag_data_cleanse_and_calculate_all",
    start_date=datetime(2025, 9, 22),
    schedule_interval="@hourly",
    catchup=False,
    default_args={'retries': 0},
    tags=["cleansing", "data_pipeline", "dynamic"],
) as dag:

    machine_ids = ["a1", "a2", "a3", "b7", "b8", "b9", "b10", "b12", "b13", "b16", "b17", "b18", "c1", "c2", "c3", "c4", "c5", "c6", "c7", 
                   "c8", "c9", "c10", "c11", "d1", "d2", "d3", "d5", "d6", "d7", "d8", "d9","d10", "d11", "d12", "d13", "h1", "h2", "h4", 
                   "h5", "h6", "h7", "h8", "h9", "h10", "i1", "i2", "i4", "m1", "n1", "o1", "q1", "s1"]

    cleanse_tasks = {}
    calculate_tasks = {}

    for m_id in machine_ids:
        # สร้าง task สำหรับแต่ละเครื่อง
        cleanse_tasks[m_id] = PythonOperator(
            task_id=f"cleanse_data_{m_id}",
            python_callable=cleanse_data,
            op_args=[m_id],
        )

        calculate_tasks[m_id] = PythonOperator(
            task_id=f"calculate_data_{m_id}",
            python_callable=calculate_data,
            op_args=[m_id],
        )

    # สร้าง dependency ด้วย for loop
    for m_id in machine_ids:
        cleanse_tasks[m_id] >> calculate_tasks[m_id]