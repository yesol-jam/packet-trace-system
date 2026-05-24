#!/usr/bin/env python3
"""
api_screenshot_sender.py - API 스크린샷 전송기
작성자 : 제예솔
작성일 : 2025-10-15
"""

import requests
import json
import os
import sys
from pathlib import Path
from datetime import datetime

# settings.py 경로 추가
sys.path.append(str(Path(__file__).parent.parent))
from settings import API_SCREENSHOT_URL, API_SCREENSHOT_TIMEOUT, API_HEADERS, CAPTURE_DIR

# Logger 설정
from logger import setup_logger
logger = setup_logger('DWI.API_SCREENSHOT_SENDER')

def run_api_screenshot_send(screenshot_result, srvrStts):
    """
    스크린샷 파일을 API로 전송
    
    Args:
        screenshot_result (str): 저장된 스크린샷 파일명
        srvrStts (dict): 서버 상태 정보
    
    Returns:
        bool: 전송 성공 시 True, 실패 시 False
    """
    logger.info("스크린샷 전송 시작")
    logger.info(f"파일명: {screenshot_result}")
    logger.info(f"URL: {API_SCREENSHOT_URL}")
    
    # 파일 경로 생성
    file_path = os.path.join(CAPTURE_DIR, screenshot_result)
    
    # 파일 존재 확인
    if not os.path.exists(file_path):
        logger.error(f"파일이 존재하지 않음: {file_path}")
        return False
    
    # 파일 확장자 추출
    file_ext = os.path.splitext(screenshot_result)[1].lstrip('.')  # '.png' -> 'png'
    
    # 현재 타임스탬프
    current_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    try:
        # multipart/form-data로 전송할 데이터 준비
        files = {
            'file': (screenshot_result, open(file_path, 'rb'), f'image/{file_ext}')
        }
        
        data = {
            'cptrNm': screenshot_result,
            'trceTrgtNo': srvrStts.get('trceTrgtNo', ''),
            'trceTrgtUrl': srvrStts.get('trceTrgtUrl', ''),
            'extnNm': file_ext,
            'cptrDt': current_timestamp
        }
        
        logger.debug(f"전송 데이터: {data}")
        
        # POST 요청 전송
        response = requests.post(
            API_SCREENSHOT_URL,
            headers=API_HEADERS,
            files=files,
            data=data,
            timeout=API_SCREENSHOT_TIMEOUT
        )
        
        # 파일 핸들 닫기
        files['file'][1].close()
        
        if response.status_code == 200:
            logger.info("전송 성공")
            
            # 전송 성공 시 파일 삭제
            try:
                os.remove(file_path)
                logger.info(f"파일 삭제 완료: {file_path}")
            except Exception as e:
                logger.warning(f"파일 삭제 실패: {e}")
            
            try:
                response_data = response.json()
                logger.debug(f"응답 데이터: {json.dumps(response_data, indent=2, ensure_ascii=False)}")
                return True
            except json.JSONDecodeError:
                logger.info("응답 성공 (JSON 파싱 불가)")
                return True
        else:
            logger.error(f"전송 실패: HTTP {response.status_code}")
            logger.error(f"응답 내용: {response.text}")
            return False
            
    except FileNotFoundError as e:
        logger.error(f"파일을 찾을 수 없음: {e}")
        return False
    except requests.exceptions.Timeout:
        logger.error(f"요청 타임아웃 ({API_SCREENSHOT_TIMEOUT}초)")
        return False
    except requests.exceptions.RequestException as e:
        logger.error(f"HTTP 요청 실패: {e}")
        return False
    except Exception as e:
        logger.error("예상치 못한 오류 발생", exc_info=True)
        return False
