"""                                                          
   CodeCraft PMS Project                             
                                                                              
   파일명   : project.py                                                          
   생성자   : 김창환                                
                                                                              
   생성일   : 2024/10/16                                                      
   업데이트 : 2024/10/25                                                      
                                                                             
   설명     : 프로젝트의 생성, 수정, 조회를 위한 API 엔드포인트 정의
"""

from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import mysql_connection # MySQL 연결 기능 수행
from project_DB import *

router = APIRouter()

class project_init(BaseModel): # 프로젝트 생성 클래스
    pname: str # 프로젝트 이름
    pdetails: str # 프로젝트 내용
    psize: int # 프로젝트 개발 인원
    pperiod: str # 프로젝트 개발 기간
    pmm: int # 프로젝트 관리 방법론; project management methodologies

class project_edit(BaseModel): # 프로젝트 생성 클래스
    pid: int # 프로젝트의 고유번호
    pname: str # 프로젝트 이름
    pdetails: str # 프로젝트 내용
    psize: int # 프로젝트 개발 인원
    pperiod: str # 프로젝트 개발 기간
    pmm: int # 프로젝트 관리 방법론; project management methodologies

class project_load(BaseModel): #프로젝트 로드 클래스
    univ_id: int # 학번으로 자신이 소유한 프로젝트를 불러옴

@router.post("/project/init")
async def api_prj_init_post(payload: project_init):
    """
    DB에 payload로 전달받은 정보를 기입하는 쿼리 실행
    예시로, 가상의 함수 init_project()를 사용한다고 가정
    init_result = init_project(payload)
    """
    init_result = True # 테스트 코드
    return {"RESULT_CODE": 200,
            "RESULT_MSG": "Success",
            "PAYLOADS": {
                            "init_result": init_result
                        }}

@router.post("/project/edit")
async def api_prj_edit_post(payload: project_edit):
    """
    DB에 payload로 전달받은 정보를 수정하는 쿼리 실행
    예시로, 가상의 함수 edit_project()를 사용한다고 가정
    edit_result = edit_project(payload)
    """
    edit_result = True
    return {"RESULT_CODE": 200,
            "RESULT_MSG": "Success",
            "PAYLOADS": {
                            "edit_result": edit_result
                        }}

@router.get("/project/load")
async def api_prj_load_get(payload: project_load):
    db_connect()  # DB에 접속
    """
    DB에서 데이터를 가져오는 쿼리 실행
    예시로, 가상의 함수 fetch_project_info()를 사용한다고 가정
    project_info = fetch_project_info(payload.univ_id) 학번을 기준으로 프로젝트 정보 조회
    """
    if project_info:
        pid = project_info['pid']
        pname = project_info['pname']
        pdetails = project_info['pdetails']
        psize = project_info['psize']
        pperiod = project_info['pperiod']
        pmm = project_info['pmm']
    else:
        raise HTTPException(status_code=404, detail={"RESULT_CODE": 404,
                                                     "RESULT_MSG": "Not Found",
                                                     "PAYLOADS": {}})
    return {"RESULT_CODE": 200,
            "RESULT_MSG": "Success",
            "PAYLOADS": {
                            "pid": pid,
                            "pname": pname,
                            "pdetails": pdetails,
                            "psize": psize,
                            "pperiod": pperiod,
                            "pmm": pmm
                        }} # P021에 프로젝트 목표?