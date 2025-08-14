import os
import re
import time
import uuid
import warnings
warnings.filterwarnings('ignore')

from dotenv import load_dotenv

# Selenium 관련
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.chrome.options import Options

import pandas as pd
import pymysql
import urllib.parse
from sqlalchemy import create_engine




def get_hotel_link(conn, ota):
    """
    호텔 정보 테이블(hotels)와 호텔별 OTA 링크(hotel-otas) 테이블을 조인하여
    특정 OTA에 대한 hotel_id, 호텔명, 주소, 링크를 DataFrame으로 반환합니다.
    """
    sql = f"""
    SELECT 
       h.id AS hotel_id,
       h.name AS hotel_name,
       h.address,
       urls.ota,
       urls.link
    FROM hotels AS h
    INNER JOIN `hotel-otas` AS urls ON h.id = urls.hotelId
    WHERE urls.ota = '{ota}';
    """
    df = pd.read_sql(sql, conn)
    return df


def get_latest_review_ikyu(df, conn):
    """
    주어진 DataFrame(df) 내 hotel_id 목록을 기준으로,
    각 호텔(IKYU)에서 가장 최신에 저장된 리뷰(1건)를 가져옵니다.
    """
    hotel_ids = df['hotel_id'].unique()
    hotel_ids_str = ",".join(map(str, hotel_ids))
    
    sql = f"""
    SELECT *
    FROM (
      SELECT 
        hr.id, 
        hr.hotelId, 
        hr.authorName, 
        hr.content, 
        hr.reviewCreatedAt, 
        hr.ota,
        ROW_NUMBER() OVER (PARTITION BY hr.hotelId ORDER BY hr.reviewCreatedAt DESC) AS rn
      FROM `hotel-reviews` AS hr
      WHERE hr.ota = 'IKYU' 
        AND hr.hotelId IN ({hotel_ids_str})
    ) AS sub
    WHERE rn = 1
    ORDER BY hotelId, reviewCreatedAt DESC;
    """
    latest_reviews_df = pd.read_sql(sql, conn)
    return latest_reviews_df


def count_reviews_ikyu_split(df, conn):
    """
    주어진 데이터프레임(df)에서 'hotel_id' 컬럼을 기준으로,
    'IKYU' OTA의 전체 리뷰수를 조회하여, 
    원본의 모든 호텔 정보를 유지한 채 review_count 칼럼을 추가하고,
    리뷰가 전혀 없는 호텔(new_hotel_list)과 리뷰가 1개 이상 있는 호텔(exisin_hotel_list)로 분리합니다.
    
    Parameters:
        df: 리뷰 크롤링 대상 호텔 정보가 담긴 DataFrame (반드시 'hotel_id' 컬럼 포함, 그 외 추가 정보 포함)
        conn: 데이터베이스 연결 객체
        
    Returns:
        new_hotel_list: 리뷰가 0개인 호텔들의 DataFrame (원본의 모든 컬럼 + review_count)
        exisin_hotel_list: 리뷰가 1개 이상인 호텔들의 DataFrame (원본의 모든 컬럼 + review_count)
    """
    # 원본 df의 호텔 정보(중복 제거: hotel_id 기준)
    hotel_info = df.drop_duplicates(subset=['hotel_id'])
    hotel_ids = hotel_info['hotel_id'].unique()
    hotel_ids_str = ",".join(map(str, hotel_ids))
    
    # IKYU 리뷰에 대해 호텔별 리뷰 개수를 세는 SQL 쿼리
    sql = f"""
    SELECT hr.hotelId AS hotel_id, COUNT(*) AS review_count
    FROM `hotel-reviews` AS hr
    WHERE hr.ota = 'IKYU' AND hr.hotelId IN ({hotel_ids_str})
    GROUP BY hr.hotelId;
    """
    
    # 쿼리 실행: 리뷰가 있는 호텔에 대해서만 결과가 반환됨
    count_df = pd.read_sql(sql, conn)
    
    # 원본 호텔 정보와 리뷰 개수를 병합 (리뷰 없는 호텔도 포함)
    merged_df = hotel_info.merge(count_df, on='hotel_id', how='left')
    merged_df['review_count'] = merged_df['review_count'].fillna(0).astype(int)
    
    # 리뷰 수에 따라 분리
    new_hotel_list = merged_df[merged_df['review_count'] == 0]
    exisin_hotel_list = merged_df[merged_df['review_count'] > 0]
    
    return new_hotel_list, exisin_hotel_list




def crawl_reviews_ikyu_fresh_new(df):
    """
    처음으로 IKYU 리뷰를 수집하는 호텔들(이전 리뷰 0개 대상)에 대한 풀 크롤링 함수
    """
    review_list = []
    total_hotels = len(df)
    print(f"🔍 총 {total_hotels}개의 [신규 호텔]을 크롤링합니다.")

    chrome_options = Options()
    chrome_options.add_experimental_option("detach", True)
    driver = webdriver.Chrome(options=chrome_options)
    time.sleep(3)

    for idx, row in df.iterrows():
        time.sleep(3)
        hotel_link = row['link']
        print(f"\n🚀 [{idx+1}/{total_hotels}] 호텔 크롤링 시작: {hotel_link}")

        try:
            driver.get(hotel_link)
            driver.set_window_position(0, 0)
            driver.execute_script("window.scrollTo(0, 500);")

            # 리뷰 버튼 클릭
            try:
                element = WebDriverWait(driver, 5).until(
                    EC.element_to_be_clickable((By.XPATH, "//span[contains(@class, 'text-sm') and contains(@class, 'font-normal') and contains(@class, 'text-blue-700')]"))
                )
            except (NoSuchElementException, TimeoutException):
                try:
                    element = WebDriverWait(driver, 5).until(
                        EC.element_to_be_clickable((By.XPATH, "//button[contains(., 'クチコミをすべてみる')]"))
                    )
                except (NoSuchElementException, TimeoutException):
                    print("❌ 리뷰 버튼을 찾을 수 없습니다. 다음 호텔로 진행합니다.")
                    continue

            driver.execute_script("arguments[0].click();", element)

            # 리뷰 목록 로드 대기
            try:
                parent_element = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "bg-gray-100.px-10.py-16"))
                )
                print(f"✅ [{idx+1}] 리뷰 목록 로드됨!")
            except Exception:
                print(f"⚠️ [{idx+1}] 리뷰 목록을 찾을 수 없음!")
                continue

            # "続きをみる" 무한 스크롤(또는 다단 로딩) 처리
            while True:
                try:
                    next_button = driver.find_element(By.XPATH, "//button[contains(., '続きをみる')]")
                    if next_button.is_displayed() and next_button.is_enabled():
                        next_button.click()
                        time.sleep(2)  # 로딩 대기
                    else:
                        break
                except Exception:
                    break

            # 리뷰 추출
            review_elements = parent_element.find_elements(By.XPATH, ".//li[section[@itemprop='reviewRating']]")
            print(f"📌 [{idx+1}] 총 {len(review_elements)}개의 리뷰를 찾음!")

            for review_index, review in enumerate(review_elements, start=1):
                try:
                    rating = review.find_element(By.XPATH, ".//span[@itemprop='ratingValue']").text if review.find_elements(By.XPATH, ".//span[@itemprop='ratingValue']") else ""
                    author_element = review.find_elements(By.XPATH, ".//span[@class='text-gray-800']")
                    if not author_element:
                        author_element = review.find_elements(By.XPATH, ".//span[@class='text-blue-600.cursor-pointer']")
                    author = author_element[0].text if author_element else "Unknown"
                    date = review.find_element(By.XPATH, ".//span[@itemprop='datePublished']").text if review.find_elements(By.XPATH, ".//span[@itemprop='datePublished']") else ""
                    title = review.find_element(By.XPATH, ".//h2").text if review.find_elements(By.XPATH, ".//h2") else ""
                    body = review.find_element(By.XPATH, ".//p[@itemprop='reviewBody']").text if review.find_elements(By.XPATH, ".//p[@itemprop='reviewBody']") else ""

                    review_list.append({
                        'hotelId': row['hotel_id'],
                        'type': 'NORMAL',
                        'status': 'ACTIVE',
                        'score': rating,
                        'authorName': author,
                        'ota': row['ota'],
                        'reviewCreatedAt': date,
                        'content': title + '\n' + body
                    })

                except Exception as e:
                    print(f"⚠️ [{idx+1}] 리뷰 {review_index} 오류: {e}")

        except Exception as e:
            print(f"🚨 [{idx+1}] 호텔 크롤링 중 오류 발생: {e}")

    driver.quit()
    print("🎯 모든 [신규 호텔] 크롤링이 완료되었습니다!")
    return review_list


def crawl_reviews_ikyu_daily(df):
    """
    기존에 리뷰가 있던 호텔들(1개 이상 보유)에 대한 '일상 크롤링' 함수
    """
    review_list = []
    total_hotels = len(df)
    print(f"🔍 총 {total_hotels}개의 [기존 호텔]을 크롤링합니다.")

    chrome_options = Options()
    chrome_options.add_experimental_option("detach", True)
    driver = webdriver.Chrome(options=chrome_options)
    time.sleep(3)

    for idx, row in df.iterrows():
        time.sleep(3)
        hotel_link = row['link']
        print(f"\n🚀 [{idx+1}/{total_hotels}] 호텔 크롤링 시작: {hotel_link}")

        try:
            driver.get(hotel_link)
            driver.set_window_position(0, 0)
            driver.execute_script("window.scrollTo(0, 500);")

            # 리뷰 버튼 클릭
            try:
                element = WebDriverWait(driver, 5).until(
                    EC.element_to_be_clickable((By.XPATH, "//span[contains(@class, 'text-sm') and contains(@class, 'font-normal') and contains(@class, 'text-blue-700')]"))
                )
            except (NoSuchElementException, TimeoutException):
                try:
                    element = WebDriverWait(driver, 5).until(
                        EC.element_to_be_clickable((By.XPATH, "//button[contains(., 'クチコミをすべてみる')]"))
                    )
                except (NoSuchElementException, TimeoutException):
                    print("❌ 리뷰 버튼을 찾을 수 없습니다. 다음 호텔로 진행합니다.")
                    continue

            driver.execute_script("arguments[0].click();", element)

            # 리뷰 목록 로드 대기
            try:
                parent_element = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "bg-gray-100.px-10.py-16"))
                )
                print(f"✅ [{idx+1}] 리뷰 목록 로드됨!")
            except Exception:
                print(f"⚠️ [{idx+1}] 리뷰 목록을 찾을 수 없음!")
                continue

            # "続きをみる" 버튼 1~2회 클릭 (현재 예시는 최대 한 번만 시도)
            try:
                next_button = driver.find_element(By.XPATH, "//button[contains(., '続きをみる')]")
                if next_button.is_displayed() and next_button.is_enabled():
                    next_button.click()
                    time.sleep(2)
                    print("『続きをみる』 버튼 클릭 완료")
            except Exception:
                pass  # 버튼이 없으면 무시

            # 리뷰 추출
            review_elements = parent_element.find_elements(By.XPATH, ".//li[section[@itemprop='reviewRating']]")
            print(f"📌 [{idx+1}] 총 {len(review_elements)}개의 리뷰를 찾음!")

            for review_index, review in enumerate(review_elements, start=1):
                try:
                    rating = review.find_element(By.XPATH, ".//span[@itemprop='ratingValue']").text if review.find_elements(By.XPATH, ".//span[@itemprop='ratingValue']") else ""
                    author_element = review.find_elements(By.XPATH, ".//span[@class='text-gray-800']")
                    if not author_element:
                        author_element = review.find_elements(By.XPATH, ".//span[@class='text-blue-600.cursor-pointer']")
                    author = author_element[0].text if author_element else "Unknown"
                    date = review.find_element(By.XPATH, ".//span[@itemprop='datePublished']").text if review.find_elements(By.XPATH, ".//span[@itemprop='datePublished']") else ""
                    title = review.find_element(By.XPATH, ".//h2").text if review.find_elements(By.XPATH, ".//h2") else ""
                    body = review.find_element(By.XPATH, ".//p[@itemprop='reviewBody']").text if review.find_elements(By.XPATH, ".//p[@itemprop='reviewBody']") else ""

                    review_list.append({
                        'hotelId': row['hotel_id'],
                        'type': 'NORMAL',
                        'status': 'ACTIVE',
                        'score': rating,
                        'authorName': author,
                        'ota': row['ota'],
                        'reviewCreatedAt': date,
                        'content': title + '\n' + body
                    })

                except Exception as e:
                    print(f"⚠️ [{idx+1}] 리뷰 {review_index} 오류: {e}")

        except Exception as e:
            print(f"🚨 [{idx+1}] 호텔 크롤링 중 오류 발생: {e}")

    driver.quit()
    print("🎯 모든 [기존 호텔] 크롤링이 완료되었습니다!")
    return review_list


def after_processing(result):
    """
    (1) 'reviewCreatedAt'에서 YYYY/M/D 추출 & datetime 변환
    (2) 'score'를 -5~0 범위에서 0~10 범위로 스케일링 (단, IKYU 실제 점수체계에 맞춰 조정 가능)
    (3) uuid 생성
    """
    result = pd.DataFrame(result)

    # 날짜 정규식 추출 -> datetime 변환
    def extract_date(x):
        match = re.search(r'(\d{4}/\d{1,2}/\d{1,2})', x)
        if match:
            return match.group(1)
        return x

    result['reviewCreatedAt'] = result['reviewCreatedAt'].apply(extract_date)
    result['reviewCreatedAt'] = pd.to_datetime(result['reviewCreatedAt'], format='%Y/%m/%d', errors='coerce')

    # 평점 변환 (예: 0~5 → 0 ~ 10 범위)
    result['score'] = pd.to_numeric(result['score'], errors='coerce')
    if not result['score'].isna().all():
        old_min = result['score'].min()
        old_max = result['score'].max()
        diff = old_max - old_min
        if diff != 0:
            result['score'] = ((result['score'] - old_min) / diff) * 10
            result['score'] = result['score'].round(1)
        else:
            # 동일 점수만 있는 경우
            result['score'] = 5.0  # 임의의 중앙값 부여

    # UUID
    result['uuid'] = [str(uuid.uuid4()) for _ in range(len(result))]

    return result


def filter_new_reviews_ikyu_by_date(daily_df, latest_df):
    """
    latest_df에 'hotelId'별 마지막(reviewCreatedAt가 가장 늦은) 날짜가 있다고 가정하고,
    daily_df에서 그 날짜보다 더 나중(>)에 작성된 리뷰들만 필터링해서 반환.
    """

    if daily_df.empty:
        return daily_df

    # 혹시 딕셔너리 형태로 들어올 수도 있으므로
    daily_df = pd.DataFrame(daily_df).copy()
    latest_df = pd.DataFrame(latest_df).copy()


    # hotelId가 없으면 그대로 반환
    if 'hotelId' not in daily_df.columns:
        return daily_df

    # 날짜형 변환
    daily_df['reviewCreatedAt'] = pd.to_datetime(daily_df['reviewCreatedAt'], errors='coerce')
    latest_df['reviewCreatedAt'] = pd.to_datetime(latest_df['reviewCreatedAt'], errors='coerce')

    # latest_df를 hotelId -> 마지막 날짜 로 맵핑
    # (실제로는 여러 리뷰 중 "가장 최신 날짜"가 저장되어 있다고 가정)
    last_known_dict = {}
    for row in latest_df.itertuples():
        last_known_dict[row.hotelId] = pd.to_datetime(row.reviewCreatedAt)

    # hotelId 별 필터링
    filtered_rows = []
    for hotel_id, group in daily_df.groupby('hotelId', sort=False):
        group = group.copy()

        # 만약 latest_df에 없으면 => 전부 신규 취급
        if hotel_id not in last_known_dict:
            filtered_rows.append(group)
            continue

        last_date = last_known_dict[hotel_id]
        # last_date보다 나중(>)인 것만 필터
        new_reviews = group[group['reviewCreatedAt'] > last_date]
        filtered_rows.append(new_reviews)

    result_df = pd.concat(filtered_rows, ignore_index=True)
    return result_df

