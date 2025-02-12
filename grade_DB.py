"""
    CodeCraft PMS Project
    파일명 : grade_DB.py
    마지막 수정 날짜 : 2025/02/10
"""

import pymysql
from mysql_connection import db_connect

# ------------------------------ 학생별 평가 코멘트 관련 함수 ------------------------------ #
# 교수가 특정 프로젝트의 특정 학생에게 평가 코멘트를 작성하거나 수정하는 함수
# 프로젝트 번호, 학번, 평가 코멘트를 매개 변수로 받는다
def add_comment(pid, univ_id, comment):
    connection = db_connect()
    cur = connection.cursor(pymysql.cursors.DictCursor)

    try:
        cur.execute("UPDATE project_user SET comment = %s WHERE p_no = %s AND s_no = %s", (comment, pid, univ_id))
        connection.commit()
        return True
    except Exception as e:
        connection.rollback()
        print(f"Error [add_comment] : {e}")
        return e
    finally:
        cur.close()
        connection.close()

# 교수가 특정 프로젝트의 특정 학생에게 작성하였던 평가 코멘트를 삭제하는 함수
# 프로젝트 번호와 학번을 매개 변수로 받는다
def delete_comment(pid, univ_id):
    connection = db_connect()
    cur = connection.cursor(pymysql.cursors.DictCursor)

    try:
        cur.execute("UPDATE project_user SET comment = NULL WHERE p_no = %s AND s_no = %s", (pid, univ_id))
        connection.commit()
        return True
    except Exception as e:
        connection.rollback()
        print(f"Error [delete_comment] : {e}")
        return e
    finally:
        cur.close()
        connection.close()

# 특정 프로젝트에서 특정 학생의 평가 코멘트를 조회하여 반환하는 함수
# 프로젝트 번호와 학번을 매개 변수로 받는다
def fetch_one_comment(pid, univ_id):
    connection = db_connect()
    cur = connection.cursor(pymysql.cursors.DictCursor)

    try:
        cur.execute("SELECT comment FROM project_user WHERE p_no = %s AND s_no = %s", (pid, univ_id))
        result = cur.fetchone()
        return result
    except Exception as e:
        print(f"Error [fetch_one_comment] : {e}")
        return e
    finally:
        cur.close()
        connection.close()

# 어떤 학생이 참여하고 있는 모든 프로젝트의 평가 코멘트를 조회하는 함수
# 학번을 매개 변수로 받는다
def fetch_comment_by_student(univ_id):
    connection = db_connect()
    cur = connection.cursor(pymysql.cursors.DictCursor)

    try:
        fetch_grade_by_student = """
        SELECT p.p_no, p.p_name, s.s_no, s.s_name, u.comment
        FROM student s, project p, project_user u
        WHERE s.s_no = u.s_no AND p.p_no = u.p_no AND u.s_no = %s
        """
        cur.execute(fetch_grade_by_student, (univ_id,))
        result = cur.fetchall()
        return result
    except Exception as e:
        print(f"Error [fetch_comment_by_student] : {e}")
        return e
    finally:
        cur.close()
        connection.close()

# 특정 프로젝트에 속한 모든 팀원의 평가 코멘트를 조회하는 함수
# 프로젝트 번호를 매개 변수로 받는다
def fetch_comment_by_project(pid):
    connection = db_connect()
    cur = connection.cursor(pymysql.cursors.DictCursor)

    try:
        fetch_grade_by_project = """
        SELECT p.p_no, p.p_name, s.s_no, s.s_name, u.comment
        FROM student s, project p, project_user u
        WHERE s.s_no = u.s_no AND p.p_no = u.p_no AND u.p_no = %s
        """
        cur.execute(fetch_grade_by_project, (pid,))
        result = cur.fetchall()
        return result
    except Exception as e:
        print(f"Error [fetch_comment_by_project] : {e}")
        return e
    finally:
        cur.close()
        connection.close()

# ------------------------------ 프로젝트별 평가 요소 채점 관련 함수 ------------------------------ #
# 교수가 특정 프로젝트의 평가 요소 점수를 입력하는 함수
# 프로젝트 번호와 각 평가 요소 점수를 매개 변수로 받는다
def assign_grade(pid, plan, require, design, progress, scm, cooperation, quality, tech, presentation, completion):
    connection = db_connect()
    cur = connection.cursor(pymysql.cursors.DictCursor)

    try:
        assign_grade = """
        INSERT INTO grade (p_no, g_plan, g_require, g_design, g_progress, g_scm, g_cooperation, g_quality, g_tech, g_presentation, g_completion)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cur.execute(assign_grade, (pid, plan, require, design, progress, scm, cooperation, quality, tech, presentation, completion))
        connection.commit()
        return True
    except Exception as e:
        connection.rollback()
        print(f"Error [assign_grade] : {e}")
        return e
    finally:
        cur.close()
        connection.close()

# 교수가 특정 프로젝트의 평가 요소 점수를 수정하는 함수
# 프로젝트 번호와 각 평가 요소 점수를 매개 변수로 받는다
def edit_grade(pid, plan, require, design, progress, scm, cooperation, quality, tech, presentation, completion):
    connection = db_connect()
    cur = connection.cursor(pymysql.cursors.DictCursor)

    try:
        edit_grade = """
        UPDATE grade
        SET g_plan = %s,
            g_require = %s,
            g_design = %s,
            g_progress = %s,
            g_scm = %s,
            g_cooperation = %s,
            g_quality = %s,
            g_tech = %s,
            g_presentation = %s,
            g_completion = %s
        WHERE p_no = %s
        """
        cur.execute(edit_grade, (plan, require, design, progress, scm, cooperation, quality, tech, presentation, completion, pid))
        connection.commit()
        return True
    except Exception as e:
        connection.rollback()
        print(f"Error [edit_grade] : {e}")
        return e
    finally:
        cur.close()
        connection.close()

# 교수가 특정 프로젝트의 평가 요소 점수를 삭제하는 함수
# 프로젝트 번호를 매개 변수로 받는다
def delete_grade(pid):
    connection = db_connect()
    cur = connection.cursor(pymysql.cursors.DictCursor)

    try:
        cur.execute("SELECT COUNT(*) AS cnt FROM grade WHERE p_no = %s", (pid,))
        result = cur.fetchone()

        if result['cnt'] == 0:
            print(f"Error [delete_grade] : Grade score of Project UID {pid} does not exist.")
            return False

        cur.execute("DELETE FROM grade WHERE p_no = %s", (pid,))
        connection.commit()
        return True
    except Exception as e:
        connection.rollback()
        print(f"Error [delete_grade] : {e}")
        return e
    finally:
        cur.close()
        connection.close()

# 특정 프로젝트의 평가 요소 점수를 조회하는 함수
# 프로젝트 번호를 매개 변수로 받는다
def fetch_grade(pid):
    connection = db_connect()
    cur = connection.cursor(pymysql.cursors.DictCursor)

    try:
        cur.execute("SELECT * FROM grade WHERE p_no = %s", (pid,))
        result = cur.fetchone()
        return result
    except Exception as e:
        print(f"Error [fetch_grade] : {e}")
        return e
    finally:
        cur.close()
        connection.close()
