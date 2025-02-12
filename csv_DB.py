"""
    CodeCraft PMS Project
    파일명 : csv_DB.py
    마지막 수정 날짜 : 2025/02/12
"""

import pymysql
from datetime import datetime
from mysql_connection import db_connect
import project_DB

# 프로젝트 정보를 CSV 파일로 내보내는 함수
# 프로젝트 번호를 매개 변수로 받아서 해당 프로젝트의 정보, 업무, 진척도, 각 산출물 정보를 CSV 파일로 내보낸다
# 내보낸 CSV 파일은 /var/lib/mysql/csv/ 경로에 저장된다
def export_csv(pid):
    connection = db_connect()
    cur = connection.cursor(pymysql.cursors.DictCursor)

    try:
        csv_path = "/var/lib/mysql/csv/"
        save_time = datetime.now().strftime("%y%m%d-%H%M%S")

        save_csv_student = f"SELECT s_no, s_id, s_pw, s_name, s_email, dno FROM student INTO OUTFILE '{csv_path}student_{pid}_{save_time}.csv' FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '^' LINES TERMINATED BY '\\n'"
        cur.execute(save_csv_student)

        save_csv_professor = f"SELECT f_no, f_id, f_pw, f_name, f_email, dno FROM professor INTO OUTFILE '{csv_path}professor_{pid}_{save_time}.csv' FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '^' LINES TERMINATED BY '\\n'"
        cur.execute(save_csv_professor)

        save_csv_project = f"SELECT p_no, p_name, p_content, p_method, p_memcount, p_start, p_end, p_subject, p_wizard, dno, f_no FROM project WHERE p_no = {pid} INTO OUTFILE '{csv_path}project_{pid}_{save_time}.csv' FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '^' LINES TERMINATED BY '\\n'"
        cur.execute(save_csv_project)

        save_csv_project_user = f"SELECT p_no, s_no, permission, role, comment FROM project_user WHERE p_no = {pid} INTO OUTFILE '{csv_path}project_user_{pid}_{save_time}.csv' FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '^' LINES TERMINATED BY '\\n'"
        cur.execute(save_csv_project_user)

        save_csv_permission = f"SELECT p_no, s_no, leader, ro, user, wbs, od, mm, ut, rs, rp, om, task, llm FROM permission WHERE p_no = {pid} INTO OUTFILE '{csv_path}permission_{pid}_{save_time}.csv' FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '^' LINES TERMINATED BY '\\n'"
        cur.execute(save_csv_permission)

        save_csv_grade = f"SELECT p_no, g_plan, g_require, g_design, g_progress, g_scm, g_cooperation, g_quality, g_tech, g_presentation, g_completion FROM grade WHERE p_no = {pid} INTO OUTFILE '{csv_path}grade_{pid}_{save_time}.csv' FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '^' LINES TERMINATED BY '\\n'"
        cur.execute(save_csv_grade)

        save_csv_work = f"SELECT w_no, w_name, w_person, w_start, w_end, w_checked, p_no, s_no FROM work WHERE p_no = {pid} INTO OUTFILE '{csv_path}work_{pid}_{save_time}.csv' FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '^' LINES TERMINATED BY '\\n'"
        cur.execute(save_csv_work)

        save_csv_progress = f"SELECT progress_no, group1, group2, group3, group4, work, output_file, manager, note, ratio, start_date, end_date, group1no, group2no, group3no, group4no, p_no FROM progress WHERE p_no = {pid} INTO OUTFILE '{csv_path}progress_{pid}_{save_time}.csv' FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '^' LINES TERMINATED BY '\\n'"
        cur.execute(save_csv_progress)

        save_csv_doc_s = f"SELECT doc_s_no, doc_s_name, doc_s_overview, doc_s_goals, doc_s_range, doc_s_outcomes, doc_s_team, doc_s_stack, doc_s_start, doc_s_end, doc_s_date, p_no FROM doc_summary WHERE p_no = {pid} INTO OUTFILE '{csv_path}doc_s_{pid}_{save_time}.csv' FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '^' LINES TERMINATED BY '\\n'"
        cur.execute(save_csv_doc_s)

        save_csv_doc_r = f"SELECT doc_r_no, doc_r_f_name, doc_r_f_content, doc_r_f_priority, doc_r_nf_name, doc_r_nf_content, doc_r_nf_priority, doc_r_s_name, doc_r_s_content, doc_r_date, p_no FROM doc_require WHERE p_no = {pid} INTO OUTFILE '{csv_path}doc_r_{pid}_{save_time}.csv' FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '^' LINES TERMINATED BY '\\n'"
        cur.execute(save_csv_doc_r)

        save_csv_doc_m = f"SELECT doc_m_no, doc_m_title, doc_m_date, doc_m_loc, doc_m_member, doc_m_manager, doc_m_content, doc_m_result, p_no FROM doc_meeting WHERE p_no = {pid} INTO OUTFILE '{csv_path}doc_m_{pid}_{save_time}.csv' FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '^' LINES TERMINATED BY '\\n'"
        cur.execute(save_csv_doc_m)

        save_csv_doc_m_file = f"SELECT doc_m_f_no, doc_m_f_name, doc_m_f_path, p_no, doc_m_no FROM doc_m_file WHERE p_no = {pid} INTO OUTFILE '{csv_path}doc_m_f_{pid}_{save_time}.csv' FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '^' LINES TERMINATED BY '\\n'"
        cur.execute(save_csv_doc_m_file)

        save_csv_doc_t = f"SELECT doc_t_no, doc_t_name, doc_t_start, doc_t_end, doc_t_pass, p_no FROM doc_test WHERE p_no = {pid} INTO OUTFILE '{csv_path}doc_t_{pid}_{save_time}.csv' FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '^' LINES TERMINATED BY '\\n'"
        cur.execute(save_csv_doc_t)

        save_csv_doc_rep = f"SELECT doc_rep_no, doc_rep_name, doc_rep_writer, doc_rep_date, doc_rep_pname, doc_rep_member, doc_rep_professor, doc_rep_research, doc_rep_design, doc_rep_arch, doc_rep_result, doc_rep_conclusion, p_no FROM doc_report WHERE p_no = {pid} INTO OUTFILE '{csv_path}doc_rep_{pid}_{save_time}.csv' FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '^' LINES TERMINATED BY '\\n'"
        cur.execute(save_csv_doc_rep)

        save_csv_doc_other = f"SELECT file_no, file_name, file_path, file_date, s_no, p_no FROM doc_other WHERE p_no = {pid} INTO OUTFILE '{csv_path}doc_o_{pid}_{save_time}.csv' FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '^' LINES TERMINATED BY '\\n'"
        cur.execute(save_csv_doc_other)

        connection.commit()
        print(f"Info : DB에 저장된 프로젝트 관련 정보를 모두 CSV 파일로 정상적으로 내보냈습니다. 내보낸 시간 : [{save_time}]")
        return True
    except Exception as e:
        print(f"Error [export_csv] : {e}")
        return e
    finally:
        cur.close()
        connection.close()

# CSV 파일로부터 프로젝트 정보를 불러와서 DB에 저장하는 함수
# {산출물 종류(문자열) : CSV 파일 경로(문자열)} 형태의 딕셔너리를 매개 변수로 받아서 CSV 파일의 내용을 DB에 저장한다
# csv_dict = {
#     "student" : "/var/lib/mysql/csv/student_10001_250105-153058.csv",
#     "professor" : "/var/lib/mysql/csv/professor_10001_250105-153058.csv",
#     "project" : "/var/lib/mysql/csv/project_10001_250105-153058.csv",
#     "project_user" : "/var/lib/mysql/csv/project_user_10001_250105-153058.csv",
#     "permission" : "/var/lib/mysql/csv/permission_10001_250105-153058.csv",
#     "grade" : "/var/lib/mysql/csv/grade_10001_250105-153058.csv",
#     "work" : "/var/lib/mysql/csv/work_10001_250105-153058.csv",
#     "progress" : "/var/lib/mysql/csv/progress_10001_250105-153058.csv",
#     "doc_summary" : "/var/lib/mysql/csv/doc_s_10001_250105-153058.csv",
#     "doc_require" : "/var/lib/mysql/csv/doc_r_10001_250105-153058.csv",
#     "doc_meeting" : "/var/lib/mysql/csv/doc_m_10001_250105-153058.csv",
#     "doc_m_file" : "/var/lib/mysql/csv/doc_m_f_10001_250105-153058.csv",
#     "doc_test" : "/var/lib/mysql/csv/doc_t_10001_250105-153058.csv",
#     "doc_report" : "/var/lib/mysql/csv/doc_rep_10001_250105-153058.csv",
#     "doc_other" : "/var/lib/mysql/csv/doc_o_10001_250105-153058.csv"
# }
# 위와 같이 딕셔너리를 만들고, import_csv(csv_dict, pid) 와 같이 함수를 호출하여 사용한다
# 참고 : pid 매개 변수는 프로젝트를 Import 하기 전에 기존의 프로젝트 내용을 삭제하는 데에 사용된다
# 참고 : 딕셔너리의 키는 수정이 불가능하며, CSV 파일은 /var/lib/mysql/csv 경로에 저장되어 있어야 한다
def import_csv(file_paths, pid):
    connection = db_connect()
    cur = connection.cursor(pymysql.cursors.DictCursor)

    import_ok = []
    import_fail = []

    try:
        project_DB.delete_project(pid)

        if "student" in file_paths:
            try:
                load_csv_student = f"LOAD DATA INFILE '{file_paths['student']}' IGNORE INTO TABLE student FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '^' LINES TERMINATED BY '\\n' (s_no, s_id, s_pw, s_name, s_email, dno)"
                cur.execute(load_csv_student)
                import_ok.append("student")
            except Exception as e:
                print(f"Error [import_csv :: student] : {e}")
                import_fail.append("student")

        if "professor" in file_paths:
            try:
                load_csv_professor = f"LOAD DATA INFILE '{file_paths['professor']}' IGNORE INTO TABLE professor FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '^' LINES TERMINATED BY '\\n' (f_no, f_id, f_pw, f_name, f_email, dno)"
                cur.execute(load_csv_professor)
                import_ok.append("professor")
            except Exception as e:
                print(f"Error [import_csv :: professor] : {e}")
                import_fail.append("professor")

        if "project" in file_paths:
            try:
                load_csv_project = f"LOAD DATA INFILE '{file_paths['project']}' INTO TABLE project FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '^' LINES TERMINATED BY '\\n' (p_no, p_name, p_content, p_method, p_memcount, p_start, p_end, p_subject, p_wizard, dno, f_no)"
                cur.execute(load_csv_project)
                import_ok.append("project")
            except Exception as e:
                print(f"Error [import_csv :: project] : {e}")
                import_fail.append("project")

        if "project_user" in file_paths:
            try:
                load_csv_project_user = f"LOAD DATA INFILE '{file_paths['project_user']}' INTO TABLE project_user FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '^' LINES TERMINATED BY '\\n' (p_no, s_no, permission, role, comment)"
                cur.execute(load_csv_project_user)
                import_ok.append("project_user")
            except Exception as e:
                print(f"Error [import_csv :: project_user] : {e}")
                import_fail.append("project_user")

        if "permission" in file_paths:
            try:
                load_csv_permission = f"LOAD DATA INFILE '{file_paths['permission']}' INTO TABLE permission FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '^' LINES TERMINATED BY '\\n' (p_no, s_no, leader, ro, user, wbs, od, mm, ut, rs, rp, om, task, llm)"
                cur.execute(load_csv_permission)
                import_ok.append("permission")
            except Exception as e:
                print(f"Error [import_csv :: permission] : {e}")
                import_fail.append("permission")

        if "grade" in file_paths:
            try:
                load_csv_grade = f"LOAD DATA INFILE '{file_paths['grade']}' INTO TABLE grade FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '^' LINES TERMINATED BY '\\n' (p_no, g_plan, g_require, g_design, g_progress, g_scm, g_cooperation, g_quality, g_tech, g_presentation, g_completion)"
                cur.execute(load_csv_grade)
                import_ok.append("grade")
            except Exception as e:
                print(f"Error [import_csv :: grade] : {e}")
                import_fail.append("grade")

        if "work" in file_paths:
            try:
                load_csv_work = f"LOAD DATA INFILE '{file_paths['work']}' INTO TABLE work FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '^' LINES TERMINATED BY '\\n' (w_no, w_name, w_person, w_start, w_end, w_checked, p_no, s_no)"
                cur.execute(load_csv_work)
                import_ok.append("work")
            except Exception as e:
                print(f"Error [import_csv :: work] : {e}")
                import_fail.append("work")

        if "progress" in file_paths:
            try:
                load_csv_progress = f"LOAD DATA INFILE '{file_paths['progress']}' INTO TABLE progress FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '^' LINES TERMINATED BY '\\n' (progress_no, group1, group2, group3, group4, work, output_file, manager, note, ratio, start_date, end_date, group1no, group2no, group3no, group4no, p_no)"
                cur.execute(load_csv_progress)
                import_ok.append("progress")
            except Exception as e:
                print(f"Error [import_csv :: progress] : {e}")
                import_fail.append("progress")

        if "doc_summary" in file_paths:
            try:
                load_csv_doc_s = f"LOAD DATA INFILE '{file_paths['doc_summary']}' INTO TABLE doc_summary FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '^' LINES TERMINATED BY '\\n' (doc_s_no, doc_s_name, doc_s_overview, doc_s_goals, doc_s_range, doc_s_outcomes, doc_s_team, doc_s_stack, doc_s_start, doc_s_end, doc_s_date, p_no)"
                cur.execute(load_csv_doc_s)
                import_ok.append("doc_summary")
            except Exception as e:
                print(f"Error [import_csv :: doc_summary] : {e}")
                import_fail.append("doc_summary")

        if "doc_require" in file_paths:
            try:
                load_csv_doc_r = f"LOAD DATA INFILE '{file_paths['doc_require']}' INTO TABLE doc_require FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '^' LINES TERMINATED BY '\\n' (doc_r_no, doc_r_f_name, doc_r_f_content, doc_r_f_priority, doc_r_nf_name, doc_r_nf_content, doc_r_nf_priority, doc_r_s_name, doc_r_s_content, doc_r_date, p_no)"
                cur.execute(load_csv_doc_r)
                import_ok.append("doc_require")
            except Exception as e:
                print(f"Error [import_csv :: doc_require] : {e}")
                import_fail.append("doc_require")

        if "doc_meeting" in file_paths:
            try:
                load_csv_doc_m = f"LOAD DATA INFILE '{file_paths['doc_meeting']}' INTO TABLE doc_meeting FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '^' LINES TERMINATED BY '\\n' (doc_m_no, doc_m_title, doc_m_date, doc_m_loc, doc_m_member, doc_m_manager, doc_m_content, doc_m_result, p_no)"
                cur.execute(load_csv_doc_m)
                import_ok.append("doc_meeting")
            except Exception as e:
                print(f"Error [import_csv :: doc_meeting] : {e}")
                import_fail.append("doc_meeting")

        if "doc_m_file" in file_paths:
            try:
                load_csv_doc_m_file = f"LOAD DATA INFILE '{file_paths['doc_m_file']}' INTO TABLE doc_m_file FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '^' LINES TERMINATED BY '\\n' (doc_m_f_no, doc_m_f_name, doc_m_f_path, p_no, doc_m_no)"
                cur.execute(load_csv_doc_m_file)
                import_ok.append("doc_m_file")
            except Exception as e:
                print(f"Error [import_csv :: doc_m_file] : {e}")
                import_fail.append("doc_m_file")

        if "doc_test" in file_paths:
            try:
                load_csv_doc_t = f"LOAD DATA INFILE '{file_paths['doc_test']}' INTO TABLE doc_test FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '^' LINES TERMINATED BY '\\n' (doc_t_no, doc_t_name, doc_t_start, doc_t_end, doc_t_pass, p_no)"
                cur.execute(load_csv_doc_t)
                import_ok.append("doc_test")
            except Exception as e:
                print(f"Error [import_csv :: doc_test] : {e}")
                import_fail.append("doc_test")

        if "doc_report" in file_paths:
            try:
                load_csv_doc_rep = f"LOAD DATA INFILE '{file_paths['doc_report']}' INTO TABLE doc_report FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '^' LINES TERMINATED BY '\\n' (doc_rep_no, doc_rep_name, doc_rep_writer, doc_rep_date, doc_rep_pname, doc_rep_member, doc_rep_professor, doc_rep_research, doc_rep_design, doc_rep_arch, doc_rep_result, doc_rep_conclusion, p_no)"
                cur.execute(load_csv_doc_rep)
                import_ok.append("doc_report")
            except Exception as e:
                print(f"Error [import_csv :: doc_report] : {e}")
                import_fail.append("doc_report")

        if "doc_other" in file_paths:
            try:
                load_csv_doc_other = f"LOAD DATA INFILE '{file_paths['doc_other']}' INTO TABLE doc_other FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '^' LINES TERMINATED BY '\\n' (file_no, file_name, file_path, file_date, s_no, p_no)"
                cur.execute(load_csv_doc_other)
                import_ok.append("doc_other")
            except Exception as e:
                print(f"Error [import_csv :: doc_other] : {e}")
                import_fail.append("doc_other")

        connection.commit()

        if not import_fail:
            print("Info : 모든 CSV 파일로부터 프로젝트 정보를 불러와서 DB에 저장하였습니다.")
            print(f"Info : {len(import_ok)}개의 테이블을 DB에 불러왔습니다.")
            return True
        else:
            print(f"Info : {len(import_ok)}개의 테이블을 DB에 불러왔습니다.")
            print(f"Warning : 불러오지 못한 테이블은 [{', '.join(import_fail)}] 입니다.")
            return False
    except Exception as e:
        print(f"Error [import_csv] : {e}")
        return e
    finally:
        cur.close()
        connection.close()

# 프로젝트 Import/Export 기능에서 현재 프로젝트의 버전 정보를 history 테이블에 저장하는 함수
# 프로젝트 번호, 현재 사용자의 학번, 메시지를 매개 변수로 받는다
# 참고 : msg 매개 변수는 API 서버로부터 'Revert z to x' 형태의 문자열을 그대로 받는다
def insert_csv_history(pid, univ_id, msg):
    connection = db_connect()
    cur = connection.cursor(pymysql.cursors.DictCursor)

    try:
        save_history = f"INSERT INTO history (p_no, ver, date, s_no, msg) VALUES ({pid}, nextval({pid}), NOW(), {univ_id}, '{msg}')"
        cur.execute(save_history)
        connection.commit()
        return True
    except Exception as e:
        print(f"Error [insert_csv_history] : {e}")
        return e
    finally:
        cur.close()
        connection.close()

# 프로젝트 Import/Export 기능에서 현재 프로젝트의 모든 버전 정보를 삭제하는 함수
# 프로젝트 번호를 매개 변수로 받는다
def delete_csv_history(pid):
    connection = db_connect()
    cur = connection.cursor(pymysql.cursors.DictCursor)

    try:
        cur.execute("DELETE FROM history WHERE p_no = %s", (pid,))
        cur.execute("CALL create_sequence(%s)", (pid,))
        connection.commit()
        return True
    except Exception as e:
        print(f"Error [delete_csv_history] : {e}")
        return e
    finally:
        cur.close()
        connection.close()

# 프로젝트 Import/Export 기능에서 현재 프로젝트의 모든 버전 정보를 조회하는 함수
# 프로젝트 번호를 매개 변수로 받는다
def fetch_csv_history(pid):
    connection = db_connect()
    cur = connection.cursor(pymysql.cursors.DictCursor)

    try:
        cur.execute("SELECT * FROM history WHERE p_no = %s ORDER BY ver DESC", (pid,))
        result = cur.fetchall()
        return result
    except Exception as e:
        print(f"Error [fetch_csv_history] : {e}")
        return e
    finally:
        cur.close()
        connection.close()
