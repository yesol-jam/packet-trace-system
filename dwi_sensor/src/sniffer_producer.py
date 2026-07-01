import sys
import time
from datetime import datetime
from pathlib import Path
from queue import Queue, Empty

from scapy.all import sniff, IP, TCP

# 설정 파일 import를 위한 경로 설정
SCRIPT_DIR = Path(__file__).parent.parent
sys.path.append(str(SCRIPT_DIR))

# Logger 설정
from logger import setup_logger
logger = setup_logger('DWI.SNIFFER_PRODUCER')

from src import common_state
from src.backup_manager import save_error_backup, save_overflow_batch
from settings import (
    KAFKA_BROKER,
    IFACE,
    TOPIC_NAME,
    BATCH_INTERVAL,
    MAX_BACKUP_FILE_SIZE,
    OVERFLOW_BUFFER_SIZE,
    PACKET_QUEUE_MAXSIZE,
    GC_TERM,
    EXCEPT_DST_IPS
)

# variables =========================================================

# 전역 변수
packet_queue = Queue(maxsize=PACKET_QUEUE_MAXSIZE)

# 오버플로우 큐 (메인 스레드 → overflow_io_worker)
overflow_queue = Queue()

# 패킷 처리 함수
def packet_handler(packet):
    if not common_state.running:
        return
        
    if packet.haslayer(IP) and packet.haslayer(TCP):
        ip_layer = packet[IP]
        tcp_layer = packet[TCP]

        # 패킷 사이즈가 0 또는 536이 아닌 경우만 처리
        payload_size = len(tcp_layer.payload)
        if payload_size > 0 and payload_size != 536:
            data = {
                "ipAddr": common_state.collector_ip,
                "dpplIpAddr": ip_layer.src,
                "dpplPortNo": tcp_layer.sport,
                "destIpAddr": ip_layer.dst,
                "destPortNo": tcp_layer.dport,
                "clctDt": datetime.fromtimestamp(packet.time).strftime('%Y-%m-%d %H:%M:%S.%f'),
                "pcktSize": payload_size
            }
            try:
                packet_queue.put_nowait(data)
            except:
                # 큐가 가득 찬 경우, 오래된 데이터를 버퍼로 이동
                try:
                    old_data = packet_queue.get_nowait()
                    add_to_overflow_buffer(old_data)
                    packet_queue.put_nowait(data)
                except:
                    # 그래도 안되면 새 데이터를 버퍼로 이동
                    add_to_overflow_buffer(data)

# 카프카 배치 전송 함수
def kafka_batch_sender():
    while common_state.running:
        time.sleep(BATCH_INTERVAL)

        batch_data = []
        
        # 큐가 빌 때까지 모든 데이터 수집
        while True:
            try:
                data = packet_queue.get_nowait()
                batch_data.append(data)
            except Empty:
                break

        if not batch_data:
            continue

        batch = {
            "collector_ip": common_state.collector_ip,
            "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'),
            "data": batch_data
        }

        # 3번까지 재시도
        max_retries = 3
        success = False
        
        for attempt in range(max_retries):
            try:
                #logger.debug(f"전송할 배치 데이터: {batch}")
                common_state.producer.send(TOPIC_NAME, batch)
                common_state.producer.flush()  # 전송 확인을 위해 flush 추가
                logger.info(f"Kafka로 배치 전송 완료: {len(batch['data'])}건 (시도 {attempt + 1}/{max_retries})")
                success = True
                break
            except Exception as e:
                logger.error(f"Kafka 전송 실패 (시도 {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:  # 마지막 시도가 아니면 잠시 대기
                    time.sleep(1)  # 1초 대기 후 재시도
        
        # 3번 모두 실패한 경우 백업 파일로 저장
        if not success:
            logger.warning(f"Kafka 전송 {max_retries}번 모두 실패. 백업 파일로 저장합니다.")
            save_error_backup(batch)

# 오버플로우 버퍼에 데이터 추가 (인메모리, 락 없음, I/O 없음)
def add_to_overflow_buffer(data):
    overflow_queue.put_nowait(data)


# 별도 스레드에서 오버플로우 I/O 처리
def overflow_io_worker():
    buffer = []
    while common_state.running:
        try:
            data = overflow_queue.get(timeout=1)
            buffer.append(data)
            if len(buffer) >= OVERFLOW_BUFFER_SIZE:
                save_overflow_batch(buffer.copy())
                buffer.clear()
        except Empty:
            pass

    # 종료 후 남은 데이터 처리
    while not overflow_queue.empty():
        try:
            buffer.append(overflow_queue.get_nowait())
        except Empty:
            break
    if buffer:
        save_overflow_batch(buffer.copy())

# 종료시 데이터 전송
def flush_on_exit():
    # 큐 데이터 처리
    batch_data = []
    while not packet_queue.empty():
        try:
            data = packet_queue.get_nowait()
            batch_data.append(data)
        except Empty:
            break
    
    if batch_data:
        batch = {
            "collector_ip": common_state.collector_ip,
            "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'),
            "data": batch_data
        }
        try:
            common_state.producer.send(TOPIC_NAME, batch)
            common_state.producer.flush()
            logger.info(f"종료 전 최종 배치 전송: {len(batch['data'])}건")
        except Exception as e:
            logger.error(f"종료 전 Kafka 전송 실패: {e}")
            save_error_backup(batch)

