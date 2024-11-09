"""
    CodeCraft PMS Project
    파일명 : wbs_DB.py
    마지막 수정 날짜 : 2024/11/09
"""

import pymysql
from mysql_connection import db_connect

# 진척도(WBS) 항목 한 개를 추가하는 함수
# 추가하려는 진척도 항목 한 개의 내용과 프로젝트 번호를 매개 변수로 받는다
def add_one_wbs(group1, group2, group3, work, output_file, manager, note, ratio, start_date, end_date, group1no, group2no, group3no, pid):
    connection = db_connect()
    cur = connection.cursor(pymysql.cursors.DictCursor)

    try:
        add_one_progress = """
        INSERT INTO progress (group1, group2, group3, work, output_file, manager, note, ratio, start_date, end_date, group1no, group2no, group3no, p_no)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cur.execute(add_one_progress, (group1, group2, group3, work, output_file, manager, note, ratio, start_date, end_date, group1no, group2no, group3no, pid))
        connection.commit()
        return True
    except Exception as e:
        connection.rollback()
        return False
    finally:
        cur.close()
        connection.close()

# 진척도(WBS) 항목 여러 개를 이차원 배열(리스트)로 받아서 한꺼번에 추가(저장)하는 함수
# 추가하려는 진척도의 항목이 모두 담긴 이차원 배열(리스트) 및 프로젝트 번호를 매개 변수로 받는다
# add_one_wbs() 함수를 반복문으로 계속 돌리는 것보다 add_multiple_wbs() 함수를 사용하는 것이 DB에 INSERT 하는 속도가 빠르고 효율이 더 좋다
# wbs_data 이차원 배열(리스트)에 저장되는 값의 예시 :
# [[group1, group2, group3, work, output_file, manager, note, ratio, start_date, end_date, group1no, group2no, group3no],
#  [group1, group2, group3, work, output_file, manager, note, ratio, start_date, end_date, group1no, group2no, group3no], ...]
def add_multiple_wbs(wbs_data, pid):
    connection = db_connect()
    cur = connection.cursor(pymysql.cursors.DictCursor)
    
    # 각각의 진척도 항목에 프로젝트 번호 pid 추가
    wbs_data_with_pid = [item + [pid] for item in wbs_data]
    
    try:
        add_multiple_wbs = """
            INSERT INTO progress (group1, group2, group3, work, output_file, manager, note, ratio, start_date, end_date, group1no, group2no, group3no, p_no)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cur.executemany(add_multiple_wbs, wbs_data_with_pid)
        connection.commit()
        return True
    except Exception as e:
        connection.rollback()
        return False
    finally:
        cur.close()
        connection.close()

# 진척도(WBS) 항목 한 개를 수정하는 함수
# 수정하려는 진척도 항목의 진척도 번호를 매개 변수로 받는다
def edit_one_wbs(group1, group2, group3, work, output_file, manager, note, ratio, start_date, end_date, group1no, group2no, group3no, progress_no):
    connection = db_connect()
    cur = connection.cursor(pymysql.cursors.DictCursor)

    try:
        edit_one_progress = """
        UPDATE progress
        SET group1 = %s,
            group2 = %s,
            group3 = %s,
            work = %s,
            output_file = %s,
            manager = %s,
            note = %s,
            ratio = %s,
            start_date = %s,
            end_date = %s,
            group1no = %s,
            group2no = %s,
            group3no = %s
        WHERE progress_no = %s
        """
        cur.execute(edit_one_progress, (group1, group2, group3, work, output_file, manager, note, ratio, start_date, end_date, group1no, group2no, group3no, progress_no))
        connection.commit()
        return True
    except Exception as e:
        connection.rollback()
        return False
    finally:
        cur.close()
        connection.close()

# 진척도(WBS) 항목 한 개를 삭제하는 함수
# 삭제하려는 진척도 항목의 진척도 번호를 매개 변수로 받는다
def delete_one_wbs(progress_no):
    connection = db_connect()
    cur = connection.cursor(pymysql.cursors.DictCursor)

    try:
        cur.execute("DELETE FROM progress WHERE progress_no = %s", (progress_no,))
        connection.commit()
        return True
    except Exception as e:
        connection.rollback()
        return False
    finally:
        cur.close()
        connection.close()

# 진척도(WBS) 항목을 모두 삭제하는 함수
# 프로젝트 번호를 매개 변수로 받아서 해당 프로젝트에 속하는 모든 전척도 항목을 삭제한다
def delete_all_wbs(pid):
    connection = db_connect()
    cur = connection.cursor(pymysql.cursors.DictCursor)

    try:
        cur.execute("DELETE FROM progress WHERE p_no = %s", (pid,))
        connection.commit()
        return True
    except Exception as e:
        connection.rollback()
        return False
    finally:
        cur.close()
        connection.close()

# 진척도(WBS)를 모두 조회하는 함수
# 프로젝트 번호를 매개 변수로 받아서 해당 프로젝트에 속하는 모든 전척도 항목을 조회한다
def fetch_all_wbs(pid):
    connection = db_connect()
    cur = connection.cursor(pymysql.cursors.DictCursor)

    try:
        cur.execute("SELECT * FROM progress WHERE p_no = %s ORDER BY group1no ASC, group2no ASC, group3no ASC", (pid,))
        result = cur.fetchall()
        return result
    except Exception as e:
        return False
    finally:
        cur.close()
        connection.close()
