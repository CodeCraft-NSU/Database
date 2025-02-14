CREATE DATABASE PMS DEFAULT CHARACTER SET utf8mb4;
USE PMS;

CREATE TABLE student (
 s_no INT NOT NULL PRIMARY KEY,
 s_id VARCHAR(20) NOT NULL UNIQUE,
 s_pw VARCHAR(255) NOT NULL,
 s_name VARCHAR(20) NOT NULL,
 s_email VARCHAR(255) NOT NULL,
 s_token VARCHAR(30) NULL UNIQUE,
 dno INT NOT NULL
);

CREATE TABLE professor (
 f_no INT NOT NULL PRIMARY KEY,
 f_id VARCHAR(20) NOT NULL UNIQUE,
 f_pw VARCHAR(255) NOT NULL,
 f_name VARCHAR(20) NOT NULL,
 f_email VARCHAR(255) NOT NULL,
 f_token VARCHAR(30) NULL UNIQUE,
 dno INT NOT NULL
);

CREATE TABLE project_user (
 p_no INT NOT NULL,
 s_no INT NOT NULL,
 permission BOOLEAN NOT NULL,
 role VARCHAR(100) NULL,
 comment TEXT NULL,
 PRIMARY KEY (p_no, s_no)
);

CREATE TABLE doc_other (
 file_no INT NOT NULL PRIMARY KEY,
 file_name VARCHAR(300) NOT NULL,
 file_path VARCHAR(1000) NOT NULL,
 file_date DATETIME NOT NULL,
 s_no INT NOT NULL,
 p_no INT NOT NULL
);

CREATE TABLE progress (
 progress_no INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
 group1 VARCHAR(300) NULL,
 group2 VARCHAR(300) NULL,
 group3 VARCHAR(300) NULL,
 group4 VARCHAR(300) NULL,
 work VARCHAR(500) NULL,
 output_file VARCHAR(500) NULL,
 manager VARCHAR(20) NULL,
 note VARCHAR(1000) NULL,
 ratio DOUBLE NOT NULL,
 start_date DATE NOT NULL,
 end_date DATE NOT NULL,
 group1no INT NULL,
 group2no INT NULL,
 group3no INT NULL,
 group4no INT NULL,
 p_no INT NOT NULL
);

CREATE TABLE project (
 p_no INT NOT NULL PRIMARY KEY,
 p_name VARCHAR(300) NOT NULL,
 p_content TEXT NOT NULL,
 p_method VARCHAR(20) NOT NULL,
 p_memcount INT NOT NULL,
 p_start DATE NOT NULL,
 p_end DATE NOT NULL,
 p_wizard BOOLEAN NULL,
 subj_no INT NULL,
 dno INT NOT NULL,
 f_no INT NULL
);

CREATE TABLE grade (
 p_no INT NOT NULL PRIMARY KEY,
 g_plan TINYINT NULL,
 g_require TINYINT NULL,
 g_design TINYINT NULL,
 g_progress TINYINT NULL,
 g_scm TINYINT NULL,
 g_cooperation TINYINT NULL,
 g_quality TINYINT NULL,
 g_tech TINYINT NULL,
 g_presentation TINYINT NULL,
 g_completion TINYINT NULL
);

CREATE TABLE work (
 w_no INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
 w_name VARCHAR(300) NOT NULL,
 w_person VARCHAR(20) NULL,
 w_start DATE NOT NULL,
 w_end DATE NOT NULL,
 w_checked BOOLEAN NOT NULL,
 p_no INT NOT NULL,
 s_no INT NOT NULL
);

CREATE TABLE dept (
 dno INT NOT NULL PRIMARY KEY,
 dname VARCHAR(20) NOT NULL
);

CREATE TABLE subject (
 subj_no INT NOT NULL PRIMARY KEY,
 subj_name VARCHAR(50) NOT NULL,
 dno INT NOT NULL
);

CREATE TABLE doc_require (
 doc_r_no INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
 doc_r_f_name TEXT NULL,
 doc_r_f_content TEXT NULL,
 doc_r_f_priority TEXT NULL,
 doc_r_nf_name TEXT NULL,
 doc_r_nf_content TEXT NULL,
 doc_r_nf_priority TEXT NULL,
 doc_r_s_name TEXT NULL,
 doc_r_s_content TEXT NULL,
 doc_r_date DATE NOT NULL,
 p_no INT NOT NULL
);

CREATE TABLE doc_meeting (
 doc_m_no INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
 doc_m_title TEXT NOT NULL,
 doc_m_date DATE NOT NULL,
 doc_m_loc TEXT NOT NULL,
 doc_m_member TEXT NOT NULL,
 doc_m_manager TEXT NOT NULL,
 doc_m_content TEXT NOT NULL,
 doc_m_result TEXT NOT NULL,
 p_no INT NOT NULL
);

CREATE TABLE doc_summary (
 doc_s_no INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
 doc_s_name VARCHAR(300) NULL,
 doc_s_overview TEXT NULL,
 doc_s_goals TEXT NULL,
 doc_s_range TEXT NULL,
 doc_s_outcomes TEXT NULL,
 doc_s_team TEXT NULL,
 doc_s_stack TEXT NULL,
 doc_s_start DATE NOT NULL,
 doc_s_end DATE NOT NULL,
 doc_s_date DATE NOT NULL,
 p_no INT NOT NULL
);

CREATE TABLE doc_test (
 doc_t_no INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
 doc_t_group1 VARCHAR(30) NULL,
 doc_t_name TEXT NOT NULL,
 doc_t_start DATE NOT NULL,
 doc_t_end DATE NOT NULL,
 doc_t_pass BOOLEAN NOT NULL,
 doc_t_group1no INT NULL,
 p_no INT NOT NULL
);

CREATE TABLE doc_report (
 doc_rep_no INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
 doc_rep_name TEXT NOT NULL,
 doc_rep_writer TEXT NOT NULL,
 doc_rep_date DATE NOT NULL,
 doc_rep_pname TEXT NULL,
 doc_rep_member TEXT NULL,
 doc_rep_professor TEXT NULL,
 doc_rep_research TEXT NULL,
 doc_rep_design TEXT NULL,
 doc_rep_arch TEXT NULL,
 doc_rep_result TEXT NULL,
 doc_rep_conclusion TEXT NULL,
 p_no INT NOT NULL
);

CREATE TABLE doc_attach (
 doc_a_no INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
 doc_a_name VARCHAR(300) NOT NULL,
 doc_a_path VARCHAR(1000) NOT NULL,
 doc_type TINYINT NOT NULL,
 doc_no INT NOT NULL,
 p_no INT NOT NULL
);

CREATE TABLE permission (
 p_no INT NOT NULL,
 s_no INT NOT NULL,
 leader TINYINT NOT NULL,
 ro TINYINT NOT NULL,
 user TINYINT NOT NULL,
 wbs TINYINT NOT NULL,
 od TINYINT NOT NULL,
 mm TINYINT NOT NULL,
 ut TINYINT NOT NULL,
 rs TINYINT NOT NULL,
 rp TINYINT NOT NULL,
 om TINYINT NOT NULL,
 task TINYINT NOT NULL,
 llm TINYINT NOT NULL,
 PRIMARY KEY (p_no, s_no)
);

CREATE TABLE history (
 p_no INT NOT NULL,
 ver INT UNSIGNED NOT NULL,
 date DATETIME NOT NULL,
 s_no INT NOT NULL,
 msg TEXT NULL,
 PRIMARY KEY (p_no, ver)
);

CREATE TABLE sequences (
 p_no BIGINT UNSIGNED PRIMARY KEY,
 currval BIGINT UNSIGNED
) ENGINE = InnoDB;

ALTER TABLE student ADD CONSTRAINT FK_dept_TO_student_1 FOREIGN KEY (dno) REFERENCES dept (dno);
ALTER TABLE professor ADD CONSTRAINT FK_dept_TO_professor_1 FOREIGN KEY (dno) REFERENCES dept (dno);
ALTER TABLE subject ADD CONSTRAINT FK_dept_TO_subject_1 FOREIGN KEY (dno) REFERENCES dept (dno);
ALTER TABLE project_user ADD CONSTRAINT FK_project_TO_project_user_1 FOREIGN KEY (p_no) REFERENCES project (p_no) ON DELETE CASCADE;
ALTER TABLE project_user ADD CONSTRAINT FK_student_TO_project_user_1 FOREIGN KEY (s_no) REFERENCES student (s_no) ON DELETE CASCADE;
ALTER TABLE doc_other ADD CONSTRAINT FK_student_TO_doc_other_1 FOREIGN KEY (s_no) REFERENCES student (s_no);
ALTER TABLE doc_other ADD CONSTRAINT FK_project_TO_doc_other_1 FOREIGN KEY (p_no) REFERENCES project (p_no) ON DELETE CASCADE;
ALTER TABLE progress ADD CONSTRAINT FK_project_TO_progress_1 FOREIGN KEY (p_no) REFERENCES project (p_no) ON DELETE CASCADE;
ALTER TABLE project ADD CONSTRAINT FK_dept_TO_project_1 FOREIGN KEY (dno) REFERENCES dept (dno);
ALTER TABLE project ADD CONSTRAINT FK_professor_TO_project_1 FOREIGN KEY (f_no) REFERENCES professor (f_no) ON DELETE SET NULL;
ALTER TABLE project ADD CONSTRAINT FK_subject_TO_project_1 FOREIGN KEY (subj_no) REFERENCES subject (subj_no) ON DELETE SET NULL;
ALTER TABLE grade ADD CONSTRAINT FK_project_TO_grade_1 FOREIGN KEY (p_no) REFERENCES project (p_no) ON DELETE CASCADE;
ALTER TABLE work ADD CONSTRAINT FK_project_user_TO_work FOREIGN KEY (p_no, s_no) REFERENCES project_user (p_no, s_no) ON DELETE CASCADE;
ALTER TABLE doc_require ADD CONSTRAINT FK_project_TO_doc_require_1 FOREIGN KEY (p_no) REFERENCES project (p_no) ON DELETE CASCADE;
ALTER TABLE doc_meeting ADD CONSTRAINT FK_project_TO_doc_meeting_1 FOREIGN KEY (p_no) REFERENCES project (p_no) ON DELETE CASCADE;
ALTER TABLE doc_summary ADD CONSTRAINT FK_project_TO_doc_summary_1 FOREIGN KEY (p_no) REFERENCES project (p_no) ON DELETE CASCADE;
ALTER TABLE doc_test ADD CONSTRAINT FK_project_TO_doc_test_1 FOREIGN KEY (p_no) REFERENCES project (p_no) ON DELETE CASCADE;
ALTER TABLE doc_report ADD CONSTRAINT FK_project_TO_doc_report_1 FOREIGN KEY (p_no) REFERENCES project (p_no) ON DELETE CASCADE;
ALTER TABLE doc_attach ADD CONSTRAINT FK_project_TO_doc_attach_1 FOREIGN KEY (p_no) REFERENCES project (p_no) ON DELETE CASCADE;
ALTER TABLE permission ADD CONSTRAINT FK_project_user_TO_permission FOREIGN KEY (p_no, s_no) REFERENCES project_user (p_no, s_no) ON DELETE CASCADE;

INSERT INTO dept VALUES(10, '컴퓨터소프트웨어학과');
INSERT INTO dept VALUES(11, '가상현실학과');
INSERT INTO dept VALUES(12, '지능정보통신공학과');
INSERT INTO dept VALUES(13, '멀티미디어학과');
INSERT INTO dept VALUES(20, '드론공간정보공학과');
INSERT INTO dept VALUES(21, '빅데이터경영공학과');
INSERT INTO dept VALUES(22, '전자공학과');

INSERT INTO subject VALUES
(13867, '프로그래밍언어1(C언어)', 10),
(13868, '프로그래밍언어2(C언어)', 10),
(11109, '객체지향프로그래밍2', 10),
(11614, '자바프로그래밍1', 10),
(11617, '자바프로그래밍2', 10),
(24952, '파이썬프로그래밍', 10),
(10054, '컴퓨터개론', 10),
(22228, '데이터과학수학', 10),
(22227, 'AI개론', 10),
(11099, '객체지향프로그래밍1', 10),
(11072, '소프트웨어공학', 10),
(11005, '이산수학', 10),
(11050, '데이터통신', 10),
(11098, '자료구조', 10),
(11014, '시스템프로그래밍', 10),
(11616, '알고리즘분석및실습', 10),
(14823, '데이터베이스구축실습', 10),
(10050, '모바일프로그래밍', 10),
(11102, '운영체제', 10),
(13361, '창업실습Ⅰ', 10),
(11111, '컴퓨터네트워크', 10),
(11023, '데이터베이스', 10),
(12970, '[캡스톤]캡스톤디자인1', 10),
(16905, '빅데이터컴퓨팅', 10),
(22234, '머신러닝과데이터분석', 10),
(13230, '[졸업작품][캡스톤]캡스톤디자인2', 10),
(24951, '정보보호', 10),
(22231, '빅데이터분석실습', 10),
(22233, '산학프로젝트1', 10),
(22232, '가상화클라우드컴퓨팅', 10),
(13863, '정보보안관리', 10),
(22235, 'SW안전실습', 10),
(22236, '산학프로젝트2', 10);

INSERT INTO subject VALUES
(13091, '정보통신공학개론', 12),
(14689, '지능정보통신수학', 12),
(14684, '데이터사이언스', 12),
(14690, '통신회로', 12),
(14691, '회로이론', 12),
(14694, 'RF무선통신기초', 12),
(13029, '데이터통신', 12),
(13053, '디지털통신', 12),
(14692, 'C/C++프로그래밍', 12),
(14700, '자료구조·알고리즘', 12),
(14702, '컴퓨터구조', 12),
(14703, 'IoT센서·디바이스 이론및실습', 12),
(14707, 'Python프로그래밍', 12),
(13021, '디지털신호처리', 12),
(14869, '머신러닝이론및실습', 12),
(14704, 'IoT센서공학', 12),
(13704, 'JAVA프로그래밍', 12),
(13206, '모바일프로그래밍', 12),
(14696, '빅데이터', 12),
(14699, '임베디드프로세서 이론및실습', 12),
(14683, '네트워크프로토콜 이론및실습', 12),
(14685, '딥러닝', 12),
(14698, '운영체제이론및실습', 12),
(14687, '웹서버및DB', 12),
(14688, '임베디드프로그래밍', 12),
(14695, '5G이동통신', 12),
(14697, '영상/오디오신호처리', 12),
(14701, '정보보호개론', 12),
(14635, '지능형네트워크', 12),
(14910, 'ICT융합설계기초', 12),
(14705, 'IoT통신', 12),
(14706, 'IoT플랫폼', 12),
(14908, 'ICT융합설계1', 12),
(14693, 'IoT정보보안', 12),
(14909, 'ICT융합설계2', 12);

INSERT INTO subject VALUES
(15116, '고급멀티미디어제작1', 13),
(15020, '멀티미디어프로그래밍1', 13),
(15115, '콘텐츠제작기획1', 13),
(15119, '고급멀티미디어제작2', 13),
(15027, '멀티미디어프로그래밍2', 13),
(13749, '인터넷기초프로그래밍', 13),
(15118, '콘텐츠제작기획2', 13),
(15081, '디지털미디어개론', 13),
(15057, '멀티미디어제작입문1', 13),
(15006, '컴퓨터프로그래밍1', 13),
(15114, '멀티미디어제작입문2', 13),
(15010, '컴퓨터프로그래밍2', 13),
(15029, '영상제작1', 13),
(14715, '컴퓨터애니메이션', 13),
(23430, '3D 애니메이션 제작', 13),
(15035, '영상제작2', 13),
(15084, '디지털영상처리', 13),
(13198, '모바일프로그래밍1', 13),
(9069, '웹그래픽제작1', 13),
(14536, '융합미디어관리운영및제작1', 13),
(15101, '인터넷프로그래밍1', 13),
(15039, '가상현실', 13),
(13747, '디지털디자인응용', 13),
(13199, '모바일프로그래밍2', 13),
(9071, '웹그래픽제작2', 13),
(14533, '융합미디어관리운영및제작2', 13),
(15102, '인터넷프로그래밍2', 13),
(13358, '창업실습Ⅱ', 13),
(14713, '디지털미디어응용프로젝트', 13),
(15085, '디지털영상프로젝트1', 13),
(15091, '미디어콘텐츠프로젝트1', 13),
(15093, '미디어포트폴리오1 [캡스톤디자인]', 13),
(15096, '소프트웨어프로젝트1', 13),
(15086, '디지털영상프로젝트2', 13),
(15092, '미디어콘텐츠프로젝트2', 13),
(15094, '미디어포트폴리오2 [캡스톤디자인]', 13),
(15097, '소프트웨어프로젝트2', 13);

INSERT INTO subject VALUES
(13194, '산업경영공학개론', 21),
(14743, '파이썬프로그래밍1', 21),
(16081, '제조공학', 21),
(16055, '데이터베이스', 21),
(16099, '인간공학', 21),
(16087, '경영과학', 21),
(13730, '생산관리', 21),
(16019, '품질관리', 21),
(14740, '데이터과학입문', 21),
(12089, '창의적공학설계', 21),
(14748, '파이썬프로그래밍2', 21),
(16006, '경제성공학', 21),
(16077, '공업통계학', 21),
(16031, '원가공학', 21),
(13727, '객체지향프로그래밍', 21),
(14744, '경영과컴퓨터', 21),
(13738, '메카트로닉스개론', 21),
(16097, '작업분석및설계', 21),
(16085, '감성공학및제품설계', 21),
(11540, '시스템분석및설계', 21),
(16903, 'CAD', 21),
(13737, '디지털생산공학', 21),
(16041, '물류관리', 21),
(16114, '시스템최적화', 21),
(16030, '신뢰성공학', 21),
(16026, '실험계획법', 21),
(14746, '증강현실과디지털설계', 21),
(14741, '빅데이터분석및R활용', 21),
(16022, '설비관리', 21),
(16029, '안전공학', 21),
(14742, '인공지능과데이터마이닝', 21),
(16092, '품질경영', 21),
(8327, 'ERP구축및실습', 21),
(14745, '사용성공학및실습', 21),
(16102, '생산경영관리및실습', 21),
(16095, '시뮬레이션및실습', 21),
(16042, '프로젝트관리', 21);

SELECT * FROM dept;
COMMIT;
