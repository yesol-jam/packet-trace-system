#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
백업 관리 모듈
"""

import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path

# 설정 파일 import를 위한 경로 설정
SCRIPT_DIR = Path(__file__).parent.parent
sys.path.append(str(SCRIPT_DIR))

# Logger 설정
from logger import setup_logger
logger = setup_logger('DWI.BACKUP_MANAGER')

from src import common_state
from settings import MAX_BACKUP_FILE_SIZE, TOPIC_NAME

# 백업 파일 관련 변수
current_backup_file = None

# 에러 발생 시 백업 파일에 저장
def save_error_backup(batch):
    global current_backup_file
    
    # 백업 파일 100개 초과 시 가장 오래된 것 삭제
    backup_files = sorted([f for f in os.listdir('.') 
                          if f.startswith('producer_send_err_bk_') 
                          and f.endswith('.json')])
    if len(backup_files) > 100:
        oldest_file = backup_files[0]
        try:
            os.remove(oldest_file)
            logger.warning(f"백업 파일 개수 제한 초과, 오래된 파일 삭제: {oldest_file}")
        except Exception as e:
            logger.error(f"오래된 백업 파일 삭제 실패: {e}")
    
    # 현재 파일이 없거나 크기 초과시 새 파일 생성
    if (
        current_backup_file is None or
        not os.path.exists(current_backup_file) or
        os.path.getsize(current_backup_file) >= MAX_BACKUP_FILE_SIZE):

        timestamp = datetime.now().strftime('%Y%m%d%H%M%S%f')
        current_backup_file = f"producer_send_err_bk_{timestamp}.json"
        logger.info(f"새 백업 파일 생성: {current_backup_file}")

    try:
        with open(current_backup_file, 'a', encoding='utf-8') as f:
            json.dump(batch, f, ensure_ascii=False)
            f.write('\n')
        logger.info(f"에러 데이터 백업 완료: {current_backup_file}")
    except Exception as e:
        logger.error(f"백업 파일 저장 실패: {e}")

# 오버플로우 데이터 배치 저장
def save_overflow_batch(data):
    batch = {
        "collector_ip": common_state.collector_ip,
        "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'),
        "data": data
    }
    save_error_backup(batch)

# 백업파일 카프카 전송
def backup_file_sender():
    while common_state.running:
        try:
            backup_files = [f for f in os.listdir('.') if f.startswith('producer_send_err_bk_') and f.endswith('.json')]
            for backup_file in backup_files:
                if not common_state.running:
                    break

                try:
                    with open(backup_file, 'r', encoding='utf-8') as f:
                        lines = f.readlines()
                    successful_count = 0

                    for line in lines:
                        if not common_state.running:
                            break

                        line = line.strip()
                        if not line:
                            continue

                        try:
                            batch = json.loads(line)
                            common_state.producer.send(TOPIC_NAME, batch)
                            common_state.producer.flush()
                            successful_count += 1
                        except json.JSONDecodeError as e:
                            logger.error(f"JSON 파싱 실패 (손상된 라인 건너뜀): {line[:100]}")
                            successful_count += 1  # 손상된 라인도 처리됨으로 간주
                        except Exception as e:
                            logger.error(f"복구 전송 실패: {e}")

                    if successful_count == len(lines):
                        os.remove(backup_file)
                        logger.info(f"백업 파일 처리 완료 및 삭제: {backup_file}")

                except Exception as e:
                    logger.error(f"백업 파일 처리 오류: {backup_file}, {e}")

        except Exception as e:
            logger.error(f"백업 파일 처리 오류: {e}")

        time.sleep(60)

