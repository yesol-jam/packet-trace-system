#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
공유 상태 관리 모듈
"""

import json
import sys
import time
from pathlib import Path

from kafka import KafkaProducer

# 설정 파일 import를 위한 경로 설정
SCRIPT_DIR = Path(__file__).parent.parent
sys.path.append(str(SCRIPT_DIR))

from settings import KAFKA_BROKER, KAFKA_INIT_MAX_RETRIES, KAFKA_INIT_RETRY_DELAY

# Logger 설정
from logger import setup_logger
logger = setup_logger('DWI.COMMON_STATE')

# 전역 실행 상태
running = True

# Collector IP (dwi_clct.py에서 초기화됨)
collector_ip = None

# Kafka Producer (dwi_clct.py에서 초기화됨)
producer = None


def init_kafka_producer():
    """
    Kafka Producer 초기화 (재시도 포함)
    
    Returns:
        bool: 초기화 성공 여부
    """
    global producer
    
    for attempt in range(KAFKA_INIT_MAX_RETRIES):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                request_timeout_ms=10000,  # 10초 타임아웃
                max_block_ms=10000
            )
            logger.info(f"Kafka Producer 초기화 성공 (브로커: {KAFKA_BROKER})")
            return True
        except Exception as e:
            logger.error(f"Kafka Producer 초기화 실패 (시도 {attempt + 1}/{KAFKA_INIT_MAX_RETRIES}): {e}")
            if attempt < KAFKA_INIT_MAX_RETRIES - 1:
                logger.info(f"{KAFKA_INIT_RETRY_DELAY}초 후 재시도...")
                time.sleep(KAFKA_INIT_RETRY_DELAY)
    
    logger.critical(f"Kafka Producer 초기화 최종 실패: Kafka 서버({KAFKA_BROKER})에 연결할 수 없습니다.")
    return False

