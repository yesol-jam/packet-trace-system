#!/usr/bin/env python3
"""
api_trce_trgt_receiver.py - API 추적 대상 수신기
작성자 : 제예솔
작성일 : 2025-10-15
"""

import requests
import json
import sys
from pathlib import Path

# settings.py 경로 추가
sys.path.append(str(Path(__file__).parent.parent))
from settings import API_TRCE_TRGT_URL, API_TRCE_TRGT_TIMEOUT, API_HEADERS

# Logger 설정
from logger import setup_logger
logger = setup_logger('DWI.API_TRCE_TRGT_RECEIVER')

def run_api_receiver():
    """API에서 추적 대상 데이터를 가져와 리스트로 반환"""
    logger.info(f"{API_TRCE_TRGT_URL} 호출")
    
    try:
        response = requests.get(API_TRCE_TRGT_URL, headers=API_HEADERS, timeout=API_TRCE_TRGT_TIMEOUT)
        
        if response.status_code != 200:
            logger.error(f"API 호출 실패: {response.status_code}")
            return []
        
        logger.info("응답 성공")
        
        try:
            response_data = response.json()
            
            # JSON 데이터 출력
            logger.debug(f"수신된 JSON 데이터: {json.dumps(response_data, indent=2, ensure_ascii=False)}")
            
            if isinstance(response_data, dict) and 'data' in response_data:
                data = response_data['data']
                
                if isinstance(data, list):
                    logger.info(f"{len(data)}개 항목 수신")
                    return data
                else:
                    logger.info("단일 항목 수신")
                    return [data]
            else:
                logger.warning("응답에 'data' 필드 없음")
                return []
                
        except json.JSONDecodeError as e:
            logger.error(f"JSON 파싱 실패: {e}")
            return []
                
    except requests.exceptions.RequestException as e:
        logger.error(f"HTTP 요청 실패: {e}")
        return []
    except Exception as e:
        logger.error("예상치 못한 오류 발생", exc_info=True)
        return []