# ## -------------------- v used -----------------------------
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


# 🔧 ฟังก์ชันสร้าง View Summary Machine A
def create_summary_availability():
    try:
        connection = psycopg2.connect(
            host="10.0.0.158",
            port=5432,
            user="admin",
            password="Admin_password_2568",
            database="mydatabase"
        )
        cursor = connection.cursor()

        # ลบ view เก่าออกก่อน
        cursor.execute('DROP VIEW IF EXISTS summary_availability_2 CASCADE;')

        create_view_query = """
        CREATE OR REPLACE VIEW summary_availability_2 AS
        WITH hours_agg AS (
            SELECT
                "Group_Work_Center" AS gwc,
                "Date" AS work_date,
                ROUND(SUM(hours_for_day)::numeric, 2) AS total_hours_for_day
            FROM "View_Production_Hours_Daily"
            GROUP BY "Group_Work_Center", "Date"
        ),
        downtime_agg AS (
            SELECT
                "GroupWork_Center" AS gwc,
                "Date" AS work_date,
                ROUND(SUM(downtime_for_day)::numeric, 2) AS total_downtime_for_day
            FROM "View_Production_DownTime_Daily"
            GROUP BY "GroupWork_Center", "Date"
        )
        SELECT
            h.gwc AS "Group_Work_Center",
            h.work_date AS "Date",
            h.total_hours_for_day AS hours_for_day,
            COALESCE(d.total_downtime_for_day, 0) AS downtime_for_day,
            ROUND(
                CASE 
                    WHEN h.total_hours_for_day > 0
                    THEN ((h.total_hours_for_day - COALESCE(d.total_downtime_for_day, 0)) / h.total_hours_for_day) * 100
                    ELSE 0
                END, 2
            ) AS availability_percent
        FROM hours_agg h
        LEFT JOIN downtime_agg d
            ON h.gwc = d.gwc
           AND h.work_date = d.work_date
        ORDER BY h.work_date DESC, h.gwc;
        """


        cursor.execute(create_view_query)
        connection.commit()

        print("View summary_availability ถูกสร้างแล้ว")

        cursor.close()
        connection.close()

    except Exception as e:
        print(f"Error (create_summary_availability): {e}")
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

    task_create_summary_availability = PythonOperator(
        task_id="create_summary_availability",
        python_callable=create_summary_availability,
    )

    # ลำดับการทำงาน: Hours -> DownTime -> Summary
    task_create_view_hour >> task_create_view_downtime >> task_create_summary_availability


#     # ลำดับการทำงาน: Hours -> DownTime
#     # task_create_view_hour >> task_create_view_downtime




































