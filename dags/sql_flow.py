# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from datetime import datetime
# import psycopg2


# # ฟังก์ชันสร้าง View
# def create_test_production_summary():
#     try:
#         connection = psycopg2.connect(
#             host="10.0.0.158",
#             port=5432,
#             user="admin",
#             password="Admin_password_2568",
#             database="mydatabase"
#         )
#         cursor = connection.cursor()

#         # ลบ View เดิมถ้ามีอยู่แล้ว
#         cursor.execute("DROP VIEW IF EXISTS Test_Production_summary;")

#         # สร้าง View ใหม่
#         # create_view_query = """
#         # CREATE OR REPLACE VIEW Test_Production_summary AS
#         # SELECT 
#         #     pj."Route_Card_Journal",
#         #     pj."Date",
#         #     pj."Start_Time",
#         #     pj."Hours",
#         #     COALESCE(SUM(dt."Error_Hours"), 0) AS "Total_Error_Hours"
#         # FROM "Production_Journal" pj
#         # LEFT JOIN "DownTime" dt
#         #     ON pj."Route_Card_Journal" = dt."Route_Card_Jounal"
#         # WHERE pj."Job_Type" = 'Process'
#         # GROUP BY pj."Route_Card_Journal", pj."Date", pj."Start_Time", pj."Hours";
#         # """

#         create_view_query = """
#         CREATE OR REPLACE VIEW Test_Production_summary AS
#         SELECT 
#             pj."Route_Card_Journal",
#             pj."Date",
#             pj."Start_Time",
#             pj."Hours",
#             COALESCE(SUM(dt."Error_Hours"), 0) AS "Total_Error_Hours"
#         FROM "Production_Journal" pj
#         LEFT JOIN "DownTime" dt
#             ON pj."Route_Card_Journal" = dt."Route_Card_Jounal"
#         WHERE pj."Job_Type" = 'Process'
#         GROUP BY pj."Route_Card_Journal", pj."Date", pj."Start_Time", pj."Hours"
#         ORDER BY pj."Date" DESC, pj."Start_Time" DESC;
#         """

#         cursor.execute(create_view_query)
#         connection.commit()

#         print("View Test_Production_summary ถูกสร้างแล้ว")

#         cursor.close()
#         connection.close()

#     except Exception as e:
#         print(f"Error: {e}")
#         raise


# # ฟังก์ชันสร้าง View OEE_A2
# def create_oee_a2():
#     try:
#         connection = psycopg2.connect(
#             host="10.0.0.158",
#             port=5432,
#             user="admin",
#             password="Admin_password_2568",
#             database="mydatabase"
#         )
#         cursor = connection.cursor()

#         # ลบ View เดิมถ้ามีอยู่แล้ว
#         cursor.execute("DROP VIEW IF EXISTS OEE_A2;")

#         # สร้าง View ใหม่
#         create_view_query = """
#         CREATE OR REPLACE VIEW OEE_A2 AS
#         SELECT
#             "Route_Card_Journal",
#             "Date",
#             "Start_Time",
#             CASE
#                 WHEN "Hours" > 0 
#                 THEN ROUND((("Hours" - "Total_Error_Hours") / "Hours") * 100, 2)
#                 ELSE 0
#             END AS "A"
#         FROM "Test_Production_summary";
#         """

#         cursor.execute(create_view_query)
#         connection.commit()

#         print("View OEE_A2 ถูกสร้างแล้ว")

#         cursor.close()
#         connection.close()

#     except Exception as e:
#         print(f"Error: {e}")
#         raise

# # กำหนด DAG
# with DAG(
#     dag_id="dag1_test_production_summary",
#     start_date=datetime(2025, 1, 1),
#     schedule_interval="@daily",  # รันทุกวัน
#     catchup=False,
#     tags=["production", "summary"],
# ) as dag:

#     create_view_task = PythonOperator(
#         task_id="create_test_production_summary",
#         python_callable=create_test_production_summary,
#     )

#     create_view_task



# # DAG2: สร้าง OEE_A2
# with DAG(
#     dag_id="dag2_oee_a2",
#     start_date=datetime(2025, 1, 1),
#     schedule_interval="@daily",
#     catchup=False,
#     tags=["production", "oee"],
# ) as dag2:

#     create_oee_a2_task = PythonOperator(
#         task_id="create_oee_a2",
#         python_callable=create_oee_a2,
#     )









## ---------------------- V2 -----------------------
# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from datetime import datetime
# import psycopg2


# # ฟังก์ชันสร้าง View Test_Production_summary
# def create_test_production_summary():
#     try:
#         connection = psycopg2.connect(
#             host="10.0.0.158",
#             port=5432,
#             user="admin",
#             password="Admin_password_2568",
#             database="mydatabase"
#         )
#         cursor = connection.cursor()

#         # ลบ View เดิมถ้ามีอยู่แล้ว
#         cursor.execute("DROP VIEW IF EXISTS \"Test_Production_summary\";")

#         # สร้าง View ใหม่
#         create_view_query = """
#         CREATE OR REPLACE VIEW "Test_Production_summary" AS
#         SELECT 
#             pj."Route_Card_Journal",
#             pj."Date",
#             pj."Start_Time",
#             pj."Hours",
#             COALESCE(SUM(dt."Error_Hours"), 0) AS "Total_Error_Hours"
#         FROM "Production_Journal" pj
#         LEFT JOIN "DownTime" dt
#             ON pj."Route_Card_Journal" = dt."Route_Card_Jounal"
#         WHERE pj."Job_Type" = 'Process'
#         GROUP BY pj."Route_Card_Journal", pj."Date", pj."Start_Time", pj."Hours"
#         ORDER BY pj."Date" DESC, pj."Start_Time" DESC;
#         """

#         cursor.execute(create_view_query)
#         connection.commit()

#         print("✅ View Test_Production_summary ถูกสร้างแล้ว")

#         cursor.close()
#         connection.close()

#     except Exception as e:
#         print(f"❌ Error (create_test_production_summary): {e}")
#         raise


# # ฟังก์ชันสร้าง View OEE_A2
# def create_oee_a2():
#     try:
#         connection = psycopg2.connect(
#             host="10.0.0.158",
#             port=5432,
#             user="admin",
#             password="Admin_password_2568",
#             database="mydatabase"
#         )
#         cursor = connection.cursor()

#         # ใช้ CASCADE เพื่อลบ view + dependencies
#         cursor.execute("DROP VIEW IF EXISTS OEE_A2 CASCADE;")

#         create_view_query = """
#         CREATE OR REPLACE VIEW OEE_A2 AS
#         SELECT
#             "Route_Card_Journal",
#             "Date",
#             "Start_Time",
#             CASE 
#                 WHEN "Hours" > 0 
#                 THEN ROUND(((("Hours" - "Total_Error_Hours") / "Hours") * 100)::numeric, 2)
#                 ELSE 0
#             END AS A
#         FROM Test_Production_summary;
#         """

#         cursor.execute(create_view_query)
#         connection.commit()

#         print("✅ View OEE_A2 ถูกสร้างแล้ว")

#         cursor.close()
#         connection.close()

#     except Exception as e:
#         print(f"❌ Error (create_oee_a2): {e}")
#         raise


# # กำหนด DAG
# with DAG(
#     dag_id="dag_production_summary_with_oee",
#     start_date=datetime(2025, 1, 1),
#     schedule_interval="@daily",  # รันทุกวัน
#     catchup=False,
#     tags=["production", "summary", "oee"],
# ) as dag:

#     task_create_summary = PythonOperator(
#         task_id="create_test_production_summary",
#         python_callable=create_test_production_summary,
#     )

#     task_create_oee = PythonOperator(
#         task_id="create_oee_a2",
#         python_callable=create_oee_a2,
#     )

#     # ✅ ให้ task 2 รันต่อจาก task 1
#     task_create_summary >> task_create_oee
















## -------------- V3 -----------------
# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from datetime import datetime
# import psycopg2


# # 🔧 ฟังก์ชันสร้าง View
# def create_view_production_hours_daily():
#     try:
#         connection = psycopg2.connect(
#             host="10.0.0.158",
#             port=5432,
#             user="admin",
#             password="Admin_password_2568",
#             database="mydatabase"
#         )
#         cursor = connection.cursor()

#         cursor.execute('DROP VIEW IF EXISTS "View_Production_Hours_Daily" CASCADE;')

#         create_view_query = """
#         CREATE OR REPLACE VIEW "View_Production_Hours_Daily" AS
#         WITH intervals AS (
#             SELECT
#                 "Group_Work_Center",
#                 ("Date"::timestamp + "Start_Time") AS start_ts,
#                 CASE
#                     WHEN "End_Time" < "Start_Time" THEN ("Date"::timestamp + "End_Time" + interval '1 day')
#                     ELSE ("Date"::timestamp + "End_Time")
#                 END AS end_ts
#             FROM "Production_Journal"
#         ),
#         ordered AS (
#             SELECT *
#             FROM intervals
#             ORDER BY "Group_Work_Center", start_ts
#         ),
#         marked AS (
#             SELECT *,
#                 LAG(end_ts) OVER (PARTITION BY "Group_Work_Center" ORDER BY start_ts) AS prev_end
#             FROM ordered
#         ),
#         grouped AS (
#             SELECT *,
#                 CASE
#                     WHEN prev_end IS NULL OR start_ts > prev_end THEN 1
#                     ELSE 0
#                 END AS is_new_group
#             FROM marked
#         ),
#         grp AS (
#             SELECT *,
#                 SUM(is_new_group) OVER (PARTITION BY "Group_Work_Center" ORDER BY start_ts ROWS UNBOUNDED PRECEDING) AS grp_id
#             FROM grouped
#         ),
#         merged AS (
#             SELECT
#                 "Group_Work_Center",
#                 grp_id,
#                 MIN(start_ts) AS merged_start_ts,
#                 MAX(end_ts) AS merged_end_ts
#             FROM grp
#             GROUP BY "Group_Work_Center", grp_id
#         ),
#         split_days AS (
#             SELECT
#                 m."Group_Work_Center" AS gwc,
#                 m.grp_id,
#                 m.merged_start_ts,
#                 m.merged_end_ts,
#                 gs.day_start
#             FROM merged m
#             JOIN LATERAL (
#                 SELECT generate_series(
#                     date_trunc('day', m.merged_start_ts),
#                     date_trunc('day', m.merged_end_ts),
#                     interval '1 day'
#                 ) AS day_start
#             ) gs ON TRUE
#         ),
#         daily_intervals AS (
#             SELECT
#                 sd.gwc,
#                 sd.grp_id,
#                 sd.day_start::date AS "Date",
#                 GREATEST(sd.merged_start_ts, sd.day_start) AS day_start_ts,
#                 LEAST(sd.merged_end_ts, sd.day_start + interval '1 day') AS day_end_ts
#             FROM split_days sd
#         ),
#         valid_daily_intervals AS (
#             SELECT *
#             FROM daily_intervals
#             WHERE day_end_ts > day_start_ts
#         )
#         SELECT
#             vdi.gwc AS "Group_Work_Center",
#             vdi."Date",
#             vdi.day_start_ts,
#             vdi.day_end_ts,
#             vdi.grp_id,
#             EXTRACT(EPOCH FROM (vdi.day_end_ts - vdi.day_start_ts)) / 3600 AS hours_for_day,
#             CONCAT(
#                 FLOOR(EXTRACT(EPOCH FROM (vdi.day_end_ts - vdi.day_start_ts)) / 3600), ':',
#                 LPAD(FLOOR((EXTRACT(EPOCH FROM (vdi.day_end_ts - vdi.day_start_ts)) % 3600) / 60)::text, 2, '0')
#             ) AS hours_minutes_for_day
#         FROM valid_daily_intervals vdi
#         ORDER BY vdi.gwc, vdi."Date", vdi.day_start_ts;
#         """

#         cursor.execute(create_view_query)
#         connection.commit()

#         print("✅ View View_Production_Hours_Daily ถูกสร้างแล้ว")

#         cursor.close()
#         connection.close()

#     except Exception as e:
#         print(f"❌ Error (create_view_production_hours_daily): {e}")
#         raise


# # 🎯 สร้าง DAG
# with DAG(
#     dag_id="dag_production_hours_daily",
#     start_date=datetime(2025, 9, 18),
#     schedule_interval="@daily",  # รันทุกวัน
#     catchup=False,
#     tags=["production", "hours", "intervals"],
# ) as dag:

#     task_create_view = PythonOperator(
#         task_id="create_view_production_hours_daily",
#         python_callable=create_view_production_hours_daily,
#     )

#     task_create_view












# ## -------------------- V4 -----------------------
# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from datetime import datetime
# import psycopg2


# # 🔧 ฟังก์ชันสร้าง View
# def create_view_production_hours_daily():
#     try:
#         connection = psycopg2.connect(
#             host="10.0.0.158",
#             port=5432,
#             user="admin",
#             password="Admin_password_2568",
#             database="mydatabase"
#         )
#         cursor = connection.cursor()

#         cursor.execute('DROP VIEW IF EXISTS "View_Production_Hours_Daily" CASCADE;')

#         create_view_query = """
#         CREATE OR REPLACE VIEW "View_Production_Hours_Daily" AS
#         WITH intervals AS (
#             SELECT
#                 "Group_Work_Center",
#                 ("Date"::timestamp + "Start_Time") AS start_ts,
#                 CASE
#                     WHEN "End_Time" < "Start_Time" THEN ("Date"::timestamp + "End_Time" + interval '1 day')
#                     ELSE ("Date"::timestamp + "End_Time")
#                 END AS end_ts
#             FROM "Production_Journal"
#         ),
#         ordered AS (
#             SELECT *
#             FROM intervals
#             ORDER BY "Group_Work_Center", start_ts
#         ),
#         marked AS (
#             SELECT *,
#                 LAG(end_ts) OVER (PARTITION BY "Group_Work_Center" ORDER BY start_ts) AS prev_end
#             FROM ordered
#         ),
#         grouped AS (
#             SELECT *,
#                 CASE
#                     WHEN prev_end IS NULL OR start_ts > prev_end THEN 1
#                     ELSE 0
#                 END AS is_new_group
#             FROM marked
#         ),
#         grp AS (
#             SELECT *,
#                 SUM(is_new_group) OVER (PARTITION BY "Group_Work_Center" ORDER BY start_ts ROWS UNBOUNDED PRECEDING) AS grp_id
#             FROM grouped
#         ),
#         merged AS (
#             SELECT
#                 "Group_Work_Center",
#                 grp_id,
#                 MIN(start_ts) AS merged_start_ts,
#                 MAX(end_ts) AS merged_end_ts
#             FROM grp
#             GROUP BY "Group_Work_Center", grp_id
#         ),
#         split_days AS (
#             SELECT
#                 m."Group_Work_Center" AS gwc,
#                 m.grp_id,
#                 m.merged_start_ts,
#                 m.merged_end_ts,
#                 gs.day_start
#             FROM merged m
#             JOIN LATERAL (
#                 SELECT generate_series(
#                     date_trunc('day', m.merged_start_ts),
#                     date_trunc('day', m.merged_end_ts),
#                     interval '1 day'
#                 ) AS day_start
#             ) gs ON TRUE
#         ),
#         daily_intervals AS (
#             SELECT
#                 sd.gwc,
#                 sd.grp_id,
#                 sd.day_start::date AS "Date",
#                 GREATEST(sd.merged_start_ts, sd.day_start) AS day_start_ts,
#                 LEAST(sd.merged_end_ts, sd.day_start + interval '1 day') AS day_end_ts
#             FROM split_days sd
#         ),
#         valid_daily_intervals AS (
#             SELECT *
#             FROM daily_intervals
#             WHERE day_end_ts > day_start_ts
#         )
#         SELECT
#             vdi.gwc AS "Group_Work_Center",
#             vdi."Date",
#             vdi.day_start_ts,
#             vdi.day_end_ts,
#             vdi.grp_id,
#             ROUND(EXTRACT(EPOCH FROM (vdi.day_end_ts - vdi.day_start_ts)) / 3600, 2) AS hours_for_day,
#             CONCAT(
#                 FLOOR(EXTRACT(EPOCH FROM (vdi.day_end_ts - vdi.day_start_ts)) / 3600), ':',
#                 LPAD(FLOOR((EXTRACT(EPOCH FROM (vdi.day_end_ts - vdi.day_start_ts)) % 3600) / 60)::text, 2, '0')
#             ) AS hours_minutes_for_day
#         FROM valid_daily_intervals vdi
#         ORDER BY vdi."Date" DESC, vdi.day_start_ts DESC;
#         """

#         cursor.execute(create_view_query)
#         connection.commit()

#         print("✅ View View_Production_Hours_Daily ถูกสร้างแล้ว")

#         cursor.close()
#         connection.close()

#     except Exception as e:
#         print(f"❌ Error (create_view_production_hours_daily): {e}")
#         raise



# def create_view_production_downtime_daily():
#     try:
#         connection = psycopg2.connect(
#             host="10.0.0.158",
#             port=5432,
#             user="admin",
#             password="Admin_password_2568",
#             database="mydatabase"
#         )
#         cursor = connection.cursor()

#         cursor.execute('DROP VIEW IF EXISTS "View_Production_DownTime_Daily" CASCADE;')

#         create_view_query = """
#         CREATE OR REPLACE VIEW "View_Production_DownTime_Daily" AS
#         WITH intervals AS (
#             SELECT
#                 "GroupWork_Center",
#                 ("Date"::timestamp + "StartTime") AS start_ts,
#                 CASE
#                     WHEN "EndTime" < "StartTime" THEN ("Date"::timestamp + "EndTime" + interval '1 day')
#                     ELSE ("Date"::timestamp + "EndTime")
#                 END AS end_ts
#             FROM "DownTime"
#         ),
#         ordered AS (
#             SELECT *
#             FROM intervals
#             ORDER BY "GroupWork_Center", start_ts
#         ),
#         marked AS (
#             SELECT *,
#                 LAG(end_ts) OVER (PARTITION BY "GroupWork_Center" ORDER BY start_ts) AS prev_end
#             FROM ordered
#         ),
#         grouped AS (
#             SELECT *,
#                 CASE
#                     WHEN prev_end IS NULL OR start_ts > prev_end THEN 1
#                     ELSE 0
#                 END AS is_new_group
#             FROM marked
#         ),
#         grp AS (
#             SELECT *,
#                 SUM(is_new_group) OVER (PARTITION BY "GroupWork_Center" ORDER BY start_ts ROWS UNBOUNDED PRECEDING) AS grp_id
#             FROM grouped
#         ),
#         merged AS (
#             SELECT
#                 "GroupWork_Center",
#                 grp_id,
#                 MIN(start_ts) AS merged_start_ts,
#                 MAX(end_ts) AS merged_end_ts
#             FROM grp
#             GROUP BY "GroupWork_Center", grp_id
#         ),
#         split_days AS (
#             SELECT
#                 m."GroupWork_Center" AS gwc,
#                 m.grp_id,
#                 m.merged_start_ts,
#                 m.merged_end_ts,
#                 gs.day_start
#             FROM merged m
#             JOIN LATERAL (
#                 SELECT generate_series(
#                     date_trunc('day', m.merged_start_ts),
#                     date_trunc('day', m.merged_end_ts),
#                     interval '1 day'
#                 ) AS day_start
#             ) gs ON TRUE
#         ),
#         daily_intervals AS (
#             SELECT
#                 sd.gwc,
#                 sd.grp_id,
#                 sd.day_start::date AS "Date",
#                 GREATEST(sd.merged_start_ts, sd.day_start) AS day_start_ts,
#                 LEAST(sd.merged_end_ts, sd.day_start + interval '1 day') AS day_end_ts
#             FROM split_days sd
#         ),
#         valid_daily_intervals AS (
#             SELECT *
#             FROM daily_intervals
#             WHERE day_end_ts > day_start_ts
#         )
#         SELECT
#             vdi.gwc AS "GroupWork_Center",
#             vdi."Date",
#             vdi.day_start_ts,
#             vdi.day_end_ts,
#             vdi.grp_id,
#             ROUND(EXTRACT(EPOCH FROM (vdi.day_end_ts - vdi.day_start_ts)) / 3600, 2) AS hours_for_day,
#             CONCAT(
#                 FLOOR(EXTRACT(EPOCH FROM (vdi.day_end_ts - vdi.day_start_ts)) / 3600), ':',
#                 LPAD(FLOOR((EXTRACT(EPOCH FROM (vdi.day_end_ts - vdi.day_start_ts)) % 3600) / 60)::text, 2, '0')
#             ) AS hours_minutes_for_day
#         FROM valid_daily_intervals vdi
#         ORDER BY vdi."Date" DESC, vdi.day_start_ts DESC;
#         """

#         cursor.execute(create_view_query)
#         connection.commit()

#         print("✅ View View_Production_DownTime_Daily ถูกสร้างแล้ว")

#         cursor.close()
#         connection.close()

#     except Exception as e:
#         print(f"❌ Error (create_view_production_downtime_daily): {e}")
#         raise


# # 🎯 สร้าง DAG
# with DAG(
#     dag_id="dag_production_hours_daily",
#     start_date=datetime(2025, 9, 18),
#     schedule_interval="@daily",  # รันทุกวัน
#     catchup=False,
#     tags=["production", "hours", "intervals"],
# ) as dag:

#     task_create_view_hour = PythonOperator(
#         task_id="create_view_production_hours_daily",
#         python_callable=create_view_production_hours_daily,
#     )

#     task_create_view_downtime = PythonOperator(
#         task_id="create_view_production_downtime_daily",
#         python_callable=create_view_production_downtime_daily,
#     )

#     task_create_view_hour >> task_create_view_downtime
















## -------------------- v used -----------------------------
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import psycopg2


# 🔧 ฟังก์ชันสร้าง View Hours
def create_view_production_hours_daily():
    try:
        connection = psycopg2.connect(
            host="10.0.0.158",
            port=5432,
            user="admin",
            password="Admin_password_2568",
            database="mydatabase"
        )
        cursor = connection.cursor()

        cursor.execute('DROP VIEW IF EXISTS "View_Production_Hours_Daily" CASCADE;')

        create_view_query = """
        CREATE OR REPLACE VIEW "View_Production_Hours_Daily" AS
        WITH intervals AS (
            SELECT
                "Group_Work_Center",
                ("Date"::timestamp + "Start_Time") AS start_ts,
                CASE
                    WHEN "End_Time" < "Start_Time" THEN ("Date"::timestamp + "End_Time" + interval '1 day')
                    ELSE ("Date"::timestamp + "End_Time")
                END AS end_ts
            FROM "Production_Journal"
        ),
        ordered AS (
            SELECT *
            FROM intervals
            ORDER BY "Group_Work_Center", start_ts
        ),
        marked AS (
            SELECT *,
                LAG(end_ts) OVER (PARTITION BY "Group_Work_Center" ORDER BY start_ts) AS prev_end
            FROM ordered
        ),
        grouped AS (
            SELECT *,
                CASE
                    WHEN prev_end IS NULL OR start_ts > prev_end THEN 1
                    ELSE 0
                END AS is_new_group
            FROM marked
        ),
        grp AS (
            SELECT *,
                SUM(is_new_group) OVER (PARTITION BY "Group_Work_Center" ORDER BY start_ts ROWS UNBOUNDED PRECEDING) AS grp_id
            FROM grouped
        ),
        merged AS (
            SELECT
                "Group_Work_Center",
                grp_id,
                MIN(start_ts) AS merged_start_ts,
                MAX(end_ts) AS merged_end_ts
            FROM grp
            GROUP BY "Group_Work_Center", grp_id
        ),
        split_days AS (
            SELECT
                m."Group_Work_Center" AS gwc,
                m.grp_id,
                m.merged_start_ts,
                m.merged_end_ts,
                gs.day_start
            FROM merged m
            JOIN LATERAL (
                SELECT generate_series(
                    date_trunc('day', m.merged_start_ts),
                    date_trunc('day', m.merged_end_ts),
                    interval '1 day'
                ) AS day_start
            ) gs ON TRUE
        ),
        daily_intervals AS (
            SELECT
                sd.gwc,
                sd.grp_id,
                sd.day_start::date AS "Date",
                GREATEST(sd.merged_start_ts, sd.day_start) AS day_start_ts,
                LEAST(sd.merged_end_ts, sd.day_start + interval '1 day') AS day_end_ts
            FROM split_days sd
        ),
        valid_daily_intervals AS (
            SELECT *
            FROM daily_intervals
            WHERE day_end_ts > day_start_ts
        )
        SELECT
            vdi.gwc AS "Group_Work_Center",
            vdi."Date",
            vdi.day_start_ts,
            vdi.day_end_ts,
            vdi.grp_id,
            ROUND(EXTRACT(EPOCH FROM (vdi.day_end_ts - vdi.day_start_ts)) / 3600, 2) AS hours_for_day,
            CONCAT(
                FLOOR(EXTRACT(EPOCH FROM (vdi.day_end_ts - vdi.day_start_ts)) / 3600), ':',
                LPAD(FLOOR((EXTRACT(EPOCH FROM (vdi.day_end_ts - vdi.day_start_ts)) % 3600) / 60)::text, 2, '0')
            ) AS hours_minutes_for_day
        FROM valid_daily_intervals vdi
        ORDER BY vdi."Date" DESC, vdi.day_start_ts DESC;
        """

        cursor.execute(create_view_query)
        connection.commit()
        print("✅ View View_Production_Hours_Daily ถูกสร้างแล้ว")

        cursor.close()
        connection.close()

    except Exception as e:
        print(f"❌ Error (create_view_production_hours_daily): {e}")
        raise


# 🔧 ฟังก์ชันสร้าง View DownTime
def create_view_production_downtime_daily():
    try:
        connection = psycopg2.connect(
            host="10.0.0.158",
            port=5432,
            user="admin",
            password="Admin_password_2568",
            database="mydatabase"
        )
        cursor = connection.cursor()

        cursor.execute('DROP VIEW IF EXISTS "View_Production_DownTime_Daily" CASCADE;')

        create_view_query = """
        CREATE OR REPLACE VIEW "View_Production_DownTime_Daily" AS
        WITH intervals AS (
            SELECT
                "GroupWork_Center",
                ("Date"::timestamp + "StartTime") AS start_ts,
                CASE
                    WHEN "EndTime" < "StartTime" THEN ("Date"::timestamp + "EndTime" + interval '1 day')
                    ELSE ("Date"::timestamp + "EndTime")
                END AS end_ts
            FROM "DownTime"
        ),
        ordered AS (
            SELECT *
            FROM intervals
            ORDER BY "GroupWork_Center", start_ts
        ),
        marked AS (
            SELECT *,
                LAG(end_ts) OVER (PARTITION BY "GroupWork_Center" ORDER BY start_ts) AS prev_end
            FROM ordered
        ),
        grouped AS (
            SELECT *,
                CASE
                    WHEN prev_end IS NULL OR start_ts > prev_end THEN 1
                    ELSE 0
                END AS is_new_group
            FROM marked
        ),
        grp AS (
            SELECT *,
                SUM(is_new_group) OVER (PARTITION BY "GroupWork_Center" ORDER BY start_ts ROWS UNBOUNDED PRECEDING) AS grp_id
            FROM grouped
        ),
        merged AS (
            SELECT
                "GroupWork_Center",
                grp_id,
                MIN(start_ts) AS merged_start_ts,
                MAX(end_ts) AS merged_end_ts
            FROM grp
            GROUP BY "GroupWork_Center", grp_id
        ),
        split_days AS (
            SELECT
                m."GroupWork_Center" AS gwc,
                m.grp_id,
                m.merged_start_ts,
                m.merged_end_ts,
                gs.day_start
            FROM merged m
            JOIN LATERAL (
                SELECT generate_series(
                    date_trunc('day', m.merged_start_ts),
                    date_trunc('day', m.merged_end_ts),
                    interval '1 day'
                ) AS day_start
            ) gs ON TRUE
        ),
        daily_intervals AS (
            SELECT
                sd.gwc,
                sd.grp_id,
                sd.day_start::date AS "Date",
                GREATEST(sd.merged_start_ts, sd.day_start) AS day_start_ts,
                LEAST(sd.merged_end_ts, sd.day_start + interval '1 day') AS day_end_ts
            FROM split_days sd
        ),
        valid_daily_intervals AS (
            SELECT *
            FROM daily_intervals
            WHERE day_end_ts > day_start_ts
        )
        SELECT
            vdi.gwc AS "GroupWork_Center",
            vdi."Date",
            vdi.day_start_ts,
            vdi.day_end_ts,
            vdi.grp_id,
            ROUND(EXTRACT(EPOCH FROM (vdi.day_end_ts - vdi.day_start_ts)) / 3600, 2) AS downtime_for_day,
            CONCAT(
                FLOOR(EXTRACT(EPOCH FROM (vdi.day_end_ts - vdi.day_start_ts)) / 3600), ':',
                LPAD(FLOOR((EXTRACT(EPOCH FROM (vdi.day_end_ts - vdi.day_start_ts)) % 3600) / 60)::text, 2, '0')
            ) AS downtime_minutes_for_day
        FROM valid_daily_intervals vdi
        ORDER BY vdi."Date" DESC, vdi.day_start_ts DESC;
        """

        cursor.execute(create_view_query)
        connection.commit()
        print("✅ View View_Production_DownTime_Daily ถูกสร้างแล้ว")

        cursor.close()
        connection.close()

    except Exception as e:
        print(f"❌ Error (create_view_production_downtime_daily): {e}")
        raise


# 🎯 สร้าง DAG เดียว มี 2 Task
with DAG(
    dag_id="dag_production_views_daily",
    start_date=datetime(2025, 9, 18),
    schedule_interval="@daily",  # รันทุกวัน
    catchup=False,
    tags=["production", "hours", "downtime"],
) as dag:

    task_create_view_hour = PythonOperator(
        task_id="create_view_production_hours_daily",
        python_callable=create_view_production_hours_daily,
    )

    task_create_view_downtime = PythonOperator(
        task_id="create_view_production_downtime_daily",
        python_callable=create_view_production_downtime_daily,
    )

    # ลำดับการทำงาน: Hours -> DownTime
    task_create_view_hour >> task_create_view_downtime
































