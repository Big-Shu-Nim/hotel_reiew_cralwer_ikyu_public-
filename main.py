from ikyu_crawler import (
    get_hotel_link,
    get_latest_review_ikyu,
    count_reviews_ikyu_split,
    crawl_reviews_ikyu_fresh_new,
    crawl_reviews_ikyu_daily,
    after_processing,
    filter_new_reviews_ikyu_by_date
)

import pymysql
from dotenv import load_dotenv
from sqlalchemy.orm import sessionmaker
from common import get_db_connection,  get_db_engine, session_scope,upload_processor
import pandas as pd 

# 환경변수 로드 
load_dotenv('hero_db.env')
conn_rakuder_revie=  get_db_connection('rakuder_revie')



# 데이터베이스 엔진 생성
engine = get_db_engine('rakuder_revie')


# IKYU에 해당되는 리스트와 링크 가져오기 
df = get_hotel_link(conn_rakuder_revie, 'IKYU')


# 각 호텔별 최신 리뷰 하나씩 가져오기 (DB에서)
lateset_review =get_latest_review_ikyu(df, conn_rakuder_revie)

# 신규호텔 및 기존호텔 리스트 가져오기 
new_hotel_list, exisin_hotel_list = count_reviews_ikyu_split(df,conn_rakuder_revie)

# 신규 업체 처리 
if not new_hotel_list.empty:
    # (신규업체 존재할때) 실행
    fresh_new_result = crawl_reviews_ikyu_fresh_new(new_hotel_list)
    fresh_new_result = after_processing(fresh_new_result)
else:
    # 신규업체 없드면 none반환 
    fresh_new_result = None

# ⑤ 기존 호텔들에 대해 "일상 크롤링" 실행 후 후처리
daily_result = crawl_reviews_ikyu_daily(exisin_hotel_list)
daily_result = after_processing(daily_result)

# 크롤링 결과에서 신규 리뷰만 필터 
daily_new_only = filter_new_reviews_ikyu_by_date(daily_result, lateset_review)

# 8. DB Insert 전에 신규 크롤링 결과와 필터링된 리뷰를 합침
final_insert_data = pd.concat([fresh_new_result, daily_new_only], ignore_index=True)

# final_insert_data.to_csv("test.csv", index=False)

# Session 객체 생성 (세션 팩토리)
Session = sessionmaker(bind=engine, autoflush=False, autocommit=False)

# 이후 upload_processor 함수 호출 시, Session 객체(세션 팩토리)를 전달합니다.
upload_processor(final_insert_data, 'hotel-reviews', Session)


