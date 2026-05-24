"""
kafka_status_sender.py - Kafka 서버 상태 전송기
작성자 : 제예솔
작성일 : 2025-10-15
"""
import json
import time
import sys
from pathlib import Path

# settings.py 경로 추가
sys.path.append(str(Path(__file__).parent.parent))
from settings import KAFKA_TOPIC_SERVER_STATUS, KAFKA_MAX_RETRIES, KAFKA_RETRY_DELAY

# 공통 Kafka Producer import
import common_state

# Logger 설정
from logger import setup_logger
logger = setup_logger('DWI.KAFKA_STATUS_SENDER')

def run_server_status_kafka_send(srvrStts):
    """서버 상태 데이터를 Kafka로 전송 (최대 재시도)"""
    if not srvrStts:
        logger.warning("전송할 서버 상태 데이터가 없습니다. 요청을 건너뜁니다.")
        return

    producer = common_state.get_producer()
    if producer is None:
        logger.critical("Kafka Producer가 초기화되지 않았습니다. 서버 상태를 전송할 수 없습니다.")
        return

    for attempt in range(KAFKA_MAX_RETRIES):
        try:
            producer.send(KAFKA_TOPIC_SERVER_STATUS, srvrStts)
            producer.flush()
            logger.info("전송 성공")
            return
        except Exception as e:
            if attempt < KAFKA_MAX_RETRIES - 1:
                time.sleep(KAFKA_RETRY_DELAY)
            else:
                logger.error(f"전송 실패: {e}")

