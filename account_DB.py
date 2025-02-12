"""
    CodeCraft PMS Project
    파일명 : account_DB.py
    마지막 수정 날짜 : 2025/02/10
"""

import pymysql
from mysql_connection import db_connect
from account import *

# 사용자(학생) 생성 함수
def insert_user(payload, Token):
    connection = db_connect()
    cur = connection.cursor(pymysql.cursors.DictCursor)
    try:
        add_student = """
        INSERT INTO student(s_no, s_id, s_pw, s_name, s_email, s_token, dno)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        # 매개변수 바인딩 방식으로 안전하게 값 전달
        cur.execute(add_student, (payload.univ_id, payload.id, payload.pw, payload.name, payload.email, Token, payload.department))
        connection.commit()
        return True
    except Exception as e:
        connection.rollback()
        print(f"Error [insert_user] : {e}")
        return e
    finally:
        cur.close()
        connection.close()

# 사용자(학생) 로그인 정보 확인 함수
def validate_user(id, pw):
    connection = db_connect()
    cur = connection.cursor(pymysql.cursors.DictCursor)

    try:
        cur.execute("SELECT s_id, s_pw, s_no FROM student WHERE s_id = %s", (id,))
        row = cur.fetchone()
        if row and id == row['s_id'] and pw == row['s_pw']:
            return row['s_no']
        else:
            return None
    except Exception as e:
        print(f"Error [validate_user] : {e}")
        raise e  # 문자열 대신 예외를 직접 발생시킴
    finally:
        cur.close()
        connection.close()

# 사용자(학생) 로그인 성공 후 토큰(세션)을 DB에 저장하는 함수
# 로그인 정보가 일치하여 로그인에 성공하면, 로그인한 학생의 ID와 생성된 토큰을 매개 변수로 받아서 해당 사용자의 현재 세션을 유지하기 위한 토큰을 저장한다
def save_signin_user_token(id, Token):
    connection = db_connect()
    cur = connection.cursor(pymysql.cursors.DictCursor)

    try:
        cur.execute("UPDATE student SET s_token = %s WHERE s_id = %s", (Token, id))
        connection.commit()
        return True
    except Exception as e:
        connection.rollback()
        print(f"Error [save_signin_user_token] : {e}")
        return e
    finally:
        cur.close()
        connection.close()

# 사용자(학생) 로그아웃 함수
def signout_user(Token):
    connection = db_connect()
    cur = connection.cursor(pymysql.cursors.DictCursor)

    try:
        cur.execute("UPDATE student SET s_token = NULL WHERE s_token = %s", (Token,))
        connection.commit()
        return True
    except Exception as e:
        connection.rollback()
        print(f"Error [signout_user] : {e}")
        return e
    finally:
        cur.close()
        connection.close()

# 사용자(학생) 삭제 함수
def delete_user(id):
    connection = db_connect()
    cur = connection.cursor(pymysql.cursors.DictCursor)

    try:
        cur.execute("SELECT COUNT(*) AS cnt FROM student WHERE s_id = %s", (id,))
        result = cur.fetchone()

        if result['cnt'] == 0:
            print(f"Error [delete_user] : User ID {id} does not exist.")
            return False
        
        cur.execute("DELETE FROM student WHERE s_id = %s", (id,))
        connection.commit()
        return True
    except Exception as e:
        connection.rollback()
        print(f"Error [delete_user] : {e}")
        return e
    finally:
        cur.close()
        connection.close()

# 사용자(학생) 토큰 확인 함수
def validate_user_token(id, Token):
    connection = db_connect()
    cur = connection.cursor(pymysql.cursors.DictCursor)

    try:
        cur.execute("SELECT s_token FROM student WHERE s_id = %s", (id,))
        stored_token = cur.fetchone()

        # 매개 변수로 받은 토큰과 DB에 저장된 해당 학생의 토큰 값이 일치하다면 True를 반환
        if Token == stored_token['s_token']:
            return True
        else:
            return False
    except Exception as e:
        print(f"Error [validate_user_token] : {e}")
        return e
    finally:
        cur.close()
        connection.close()

# 학번으로 학생의 이름을 조회하는 함수
# 학번을 매개 변수로 받는다
def fetch_student_name(univ_id):
    connection = db_connect()
    cur = connection.cursor(pymysql.cursors.DictCursor)

    try:
        cur.execute("SELECT s_name FROM student WHERE s_no = %s", (univ_id,))
        result = cur.fetchone()
        return result
    except Exception as e:
        print(f"Error [fetch_student_name] : {e}")
        return e
    finally:
        cur.close()
        connection.close()

# ------------------------------ 교수 계정 ------------------------------ #
# 교수 로그인 정보 확인 함수
def validate_professor(id, pw):
    connection = db_connect()
    cur = connection.cursor(pymysql.cursors.DictCursor)

    try:
        cur.execute("SELECT f_id, f_pw FROM professor WHERE f_id = %s", (id,))
        row = cur.fetchone()
        if row and id == row['f_id'] and pw == row['f_pw']:
            return row['f_no']
        else:
            return None
    except Exception as e:
        print(f"Error [validate_professor] : {e}")
        raise e
    finally:
        cur.close()
        connection.close()

# 교수 로그인 성공 후 토큰(세션)을 DB에 저장하는 함수
# 교수가 로그인에 성공하면, 교수의 ID와 생성된 토큰을 매개 변수로 받아서 해당 관리자의 현재 세션을 유지하기 위한 토큰을 저장한다
def save_signin_professor_token(id, Token):
    connection = db_connect()
    cur = connection.cursor(pymysql.cursors.DictCursor)

    try:
        cur.execute("UPDATE professor SET f_token = %s WHERE f_id = %s", (Token, id))
        connection.commit()
        return True
    except Exception as e:
        connection.rollback()
        print(f"Error [save_signin_professor_token] : {e}")
        return e
    finally:
        cur.close()
        connection.close()

# 교수 로그아웃 함수
def signout_professor(Token):
    connection = db_connect()
    cur = connection.cursor(pymysql.cursors.DictCursor)

    try:
        cur.execute("UPDATE professor SET f_token = NULL WHERE f_token = %s", (Token,))
        connection.commit()
        return True
    except Exception as e:
        connection.rollback()
        print(f"Error [signout_professor] : {e}")
        return e
    finally:
        cur.close()
        connection.close()

# 교수 토큰 확인 함수
def validate_professor_token(id, Token):
    connection = db_connect()
    cur = connection.cursor(pymysql.cursors.DictCursor)

    try:
        cur.execute("SELECT f_token FROM professor WHERE f_id = %s", (id,))
        stored_token = cur.fetchone()

        if Token == stored_token['f_token']:
            return True
        else:
            return False
    except Exception as e:
        print(f"Error [validate_professor_token] : {e}")
        return e
    finally:
        cur.close()
        connection.close()

# 현재 사용자 계정이 학생 또는 교수인지 판단하는 함수
# 토큰을 매개 변수로 받으며, 학생이면 1을 반환하고, 교수이면 2를 반환한다
def check_user_type(Token):
    connection = db_connect()
    cur = connection.cursor(pymysql.cursors.DictCursor)

    try:
        cur.execute("SELECT COUNT(*) AS cnt FROM student WHERE s_token = %s", (Token,))
        student = cur.fetchone()

        cur.execute("SELECT COUNT(*) AS cnt FROM professor WHERE f_token = %s", (Token,))
        professor = cur.fetchone()

        if student['cnt'] == 1 and professor['cnt'] == 0:
            return 1
        elif student['cnt'] == 0 and professor['cnt'] == 1:
            return 2
        elif student['cnt'] == 0 and professor['cnt'] == 0:
            print(f"Warning [check_user_type] : Token [{Token}] value is not exist in DB. (Student Count : {student['cnt']}, Professor Count : {professor['cnt']})")
            return 0
        else:
            print(f"Warning [check_user_type] : Token [{Token}] value is unknown user type. (Student Count : {student['cnt']}, Professor Count : {professor['cnt']})")
            return 0
    except Exception as e:
        print(f"Error [check_user_type] : {e}")
        return e
    finally:
        cur.close()
        connection.close()

# 현재 사용자 소속 학과의 모든 교수 목록을 조회하는 함수
# 학번을 매개 변수로 받는다
def fetch_professor_list(univ_id):
    connection = db_connect()
    cur = connection.cursor(pymysql.cursors.DictCursor)

    try:
        cur.execute("SELECT f_no, f_name FROM professor WHERE dno = (SELECT dno FROM student WHERE s_no = %s)", (univ_id,))
        result = cur.fetchall()
        return result
    except Exception as e:
        print(f"Error [fetch_professor_list] : {e}")
        return e
    finally:
        cur.close()
        connection.close()

# ------------------------------ 계정 찾기, 비밀번호 변경 ------------------------------ #
# 사용자(학생)의 비밀번호(PW)를 찾기 위해 정보를 확인하는 함수
# 학번, 이름, 이메일, 아이디를 매개 변수로 받는다
def find_user_pw(univ_id, name, email, id):
    connection = db_connect()
    cur = connection.cursor(pymysql.cursors.DictCursor)

    try:
        name = name.strip()
        email = email.strip()
        id = id.strip()

        cur.execute("SELECT s_pw FROM student WHERE s_no = %s AND s_name = %s AND s_email = %s AND s_id = %s", (univ_id, name, email, id))
        result = cur.fetchone()
        if result:
            return True
        else:
            return False
    except Exception as e:
        print(f"Error [find_user_pw] : {e}")
        return e
    finally:
        cur.close()
        connection.close()

# 사용자(학생)의 비밀번호(PW)를 변경하는 함수
# 학번과 새로 변경할 비밀번호를 매개 변수로 받는다
def edit_user_pw(univ_id, pw):
    connection = db_connect()
    cur = connection.cursor(pymysql.cursors.DictCursor)

    try:
        cur.execute("UPDATE student SET s_pw = %s WHERE s_no = %s", (pw, univ_id))
        connection.commit()
        return True
    except Exception as e:
        connection.rollback()
        print(f"Error [edit_user_pw] : {e}")
        return e
    finally:
        cur.close()
        connection.close()
