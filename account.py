"""                                                          
   CodeCraft PMS Project                             
                                                                              
   파일명   : account.py                                                          
   생성자   : 김창환                                
                                                                              
   생성일   : 2024/10/16                                                      
   업데이트 : 2024/10/25                                                      
                                                                             
   설명     : 계정 생성, 로그인, 세션 관리를 위한 API 엔드포인트 정의
"""

from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import mysql_connection  # MySQL 연결 기능 수행
from account_DB import *

router = APIRouter()

class SignUp_Payload(BaseModel):
    name: str
    univ_id: int
    email: str
    id: str
    pw: str
    department: int

class Signin_Payload(BaseModel):
    id: str
    pw: str

def generate_token(user_id: str):
    """
    알파벳 대소문자, 숫자, 특수문자를 섞어 15자 길이의 랜덤 토큰 생성
    """
    # 사용할 문자 목록: 알파벳 대소문자, 숫자, 특수문자
    characters = string.ascii_letters + string.digits + string.punctuation
    # 15자리 랜덤 문자열 생성
    token = ''.join(random.choices(characters, k=15))
    return token

def check_session(user_token: str):
    """
    DB와 통신 후 토큰의 유효성을 체크하는 기능 구현
    """
    return {}

@router.post("/acc/signup")
async def api_acc_signup_post(payload: SignUp_Payload):
    """
    DB에 사용자 정보를 삽입하는 쿼리 실행
    예시로, 가상의 함수 insert_user()를 사용한다고 가정
    """
    Token = generate_token(payload.id) # Session 토큰 생성
    try:
        # 예시: 사용자 정보 데이터베이스에 삽입
        insert_result = insert_user(payload, Token)  # insert_user 함수는 payload의 정보를 DB에 삽입
        return {"RESULT_CODE": 200,
                "RESULT_MSG": "Success",
                "PAYLOADS": {
                                "Token": Token
                            }}
    except Exception as e: # 오류코드 세분화 구현 필요
        raise HTTPException(status_code=500, detail={"RESULT_CODE": 500,
                                                     "RESULT_MSG": "Internal Server Error",
                                                     "PAYLOADS": {}})

@router.post("/acc/signin")
async def api_acc_signin_post(payload: Signin_Payload):
    """
    MySQL과 연동하여 계정 확인 후 로그인 성공 여부 확인
    예시로, 가상의 함수 validate_user()를 사용한다고 가정
    """
    try:
        # 사용자 유효성 검사
        is_valid_user = validate_user(payload.id, payload.pw)  # validate_user 함수는 ID와 PW를 체크
        
        if is_valid_user:
            Token = generate_token(payload.id)  # 'Token' 변수 생성
            return {"RESULT_CODE": 200,
                    "RESULT_MSG": "Success",
                    "PAYLOADS": {
                                    "Token": Token
                                }}
        else:
            return JSONResponse(status_code=401, content={"RESULT_CODE": 401,
                                                          "RESULT_MSG": "Unauthorized",
                                                          "PAYLOADS": {}})
    except Exception as e:
        raise HTTPException(status_code=500, detail={"RESULT_CODE": 500,
                                                     "RESULT_MSG": "Internal Server Error",
                                                     "PAYLOADS": {}})