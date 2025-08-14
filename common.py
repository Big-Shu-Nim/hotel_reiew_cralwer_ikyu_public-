#!/usr/bin/env python
# coding: utf-8

# In[1]:


# 필요라이브러리 로딩
import pymysql
import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings('ignore')

import pandas as pd 
import chardet
import json
import requests
from tqdm import tqdm
from dotenv import load_dotenv
import os 
# 필요라이브러리 로딩
from sqlalchemy import create_engine
import urllib.parse
import time
from datetime import datetime, timedelta
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager
import logging 


# In[2]:


# env파일 로드
load_dotenv('hero_db.env')


# In[3]:


# 필요한 DB 연결하기 (조회용)
def get_db_connection(db_name):
    """
    주어진 데이터베이스 이름에 따라 연결을 생성하는 함수
    DB 종류 :  aws, localdb1, kto, datateam, bigdata, localdb2
    """
    host = os.getenv(f'{db_name}_HOST')
    user = os.getenv(f'{db_name}_USER')
    password = os.getenv(f'{db_name}_PASSWORD')
    port = int(os.getenv(f'{db_name}_PORT'))
    dbname = os.getenv(f'{db_name}_NAME')

    return pymysql.connect(
        host=host,
        user=user,
        port=port, 
        password=password,
        database=dbname
    )


# In[4]:


# 라이트용 
def get_db_engine(db_name):
    """
    주어진 데이터베이스 이름에 따라 SQLAlchemy 엔진을 생성하는 함수.
    DB 종류: aws, localdb1, kto, datateam, bigdata, localdb2 등.
    
    Parameters:
        db_name (str): 환경변수 접두어로 사용할 데이터베이스 이름
    
    Returns:
        engine: 생성된 SQLAlchemy 엔진 객체
    """
    host = os.getenv(f'{db_name}_HOST')
    user = os.getenv(f'{db_name}_USER')
    password = os.getenv(f'{db_name}_PASSWORD')
    port = int(os.getenv(f'{db_name}_PORT'))
    dbname = os.getenv(f'{db_name}_NAME')

    quoted_password = urllib.parse.quote_plus(password)

    # SQLAlchemy 연결 문자열 생성
    db_connection_str = f'mysql+pymysql://{user}:{quoted_password}@{host}:{port}/{dbname}?charset=utf8mb4'

    # SQLAlchemy 엔진 생성
    engine = create_engine(
        db_connection_str,
        echo=False,
        connect_args={'charset': 'utf8mb4'},
        pool_pre_ping=True,
        pool_size=20,
        max_overflow=100
    )

    return engine

# In[5]:


@contextmanager
def session_scope(Session):
    # 세션을 생성하는 팩토리 함수
    
    session = Session()  # 새로운 세션을 생성
    try:
        yield session  # 세션을 반환
        session.commit()  # 작업 완료 후 커밋
    except Exception as e:
        session.rollback()  # 오류 발생 시 롤백
        raise  # 오류를 다시 발생시켜서 문제를 외부로 전달
    finally:
        session.close()  # 세션 종료


# In[6]:


def upload_processor(df, table, session):
    # 청크 크기 설정
    chunk_size = 300000

    # 컨텍스트 매니저를 사용하여 세션 관리
    with session_scope(session) as session:
        try:
            # 데이터 프레임을 청크로 나누어 데이터베이스에 삽입
            print("총작업갯수:", len(df))
            for start in tqdm(range(0, len(df), chunk_size)):
                end = start + chunk_size
                df_chunk = df[start:end]
                df_chunk.to_sql(name=table, con=session.bind, if_exists='append', index=False)
                print(f'Rows {start} to {end} inserted.')
                time.sleep(4)  # 청크 사이에 잠시 대기
        except Exception as e:
            print(f"An error occurred: {e}")
            # 예외가 발생하면 세션에서 자동으로 롤백됨
            raise

