#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
DWI Collector
"""

import argparse
import os
import signal
import sys
import threading
import time
from pathlib import Path

from scapy.all import sniff

SCRIPT_DIR = Path(__file__).parent
SRC_DIR = SCRIPT_DIR / "src"
sys.path.append(str(SRC_DIR))
sys.path.append(str(SCRIPT_DIR))

# Logger 설정
from logger import setup_logger
logger = setup_logger('DWI.COLLECTOR')

from src import common_state
from src.backup_manager import backup_file_sender
from src.gc_worker import run_gc_worker
from src.ip_utils import get_my_ip
from settings import (
    BATCH_INTERVAL,
    OVERFLOW_BUFFER_SIZE,
    EXCEPT_SRC_PORTS,
    EXCEPT_DST_IPS,
    EXCEPT_DST_PORTS,
    IFACE
)
from src.sniffer_producer import kafka_batch_sender, packet_handler, flush_on_exit, overflow_io_worker

# PID 파일 경로
PID_DIR = Path("/vats.apps/pids")
PID_DIR.mkdir(parents=True, exist_ok=True)
PID_FILE = PID_DIR / "dwi_clct.pid"

def save_pid():
    """현재 프로세스 PID를 파일에 저장"""
    with open(PID_FILE, 'w') as f:
        f.write(str(os.getpid()))

def remove_pid():
    """PID 파일 삭제"""
    if PID_FILE.exists():
        PID_FILE.unlink()

def stop_collector():
    """실행 중인 collector 프로세스 종료"""
    if not PID_FILE.exists():
        print("실행 중인 DWI Collector 프로세스가 없습니다.")
        return False
    
    try:
        with open(PID_FILE, 'r') as f:
            pid = int(f.read().strip())
        
        # 프로세스가 실제로 실행 중인지 확인
        try:
            os.kill(pid, 0)  # 프로세스 존재 확인
        except ProcessLookupError:
            print(f"프로세스 {pid}가 이미 종료되었습니다.")
            remove_pid()
            return False
        
        print(f"DWI Collector 종료 중... (PID: {pid})")
        # SIGINT 전송 (KeyboardInterrupt 발생, 안전한 종료)
        os.kill(pid, signal.SIGINT)
        
        # 프로세스가 종료될 때까지 대기 (최대 30초)
        max_wait = 30
        wait_interval = 0.5
        elapsed = 0
        
        while elapsed < max_wait:
            try:
                os.kill(pid, 0)  # 프로세스가 아직 살아있는지 확인
                time.sleep(wait_interval)
                elapsed += wait_interval
                if int(elapsed) % 5 == 0 and elapsed > 0:
                    print(f"종료 대기 중... ({int(elapsed)}초 경과)")
            except ProcessLookupError:
                # 프로세스가 정상 종료됨
                print("종료 완료")
                # PID 파일이 자동으로 삭제되지 않았을 경우를 대비
                if PID_FILE.exists():
                    remove_pid()
                return True
        
        # 타임아웃 발생
        print(f"경고: 프로세스가 {max_wait}초 내에 종료되지 않았습니다.")
        print("강제 종료를 시도합니다...")
        try:
            os.kill(pid, signal.SIGKILL)
            time.sleep(1)
            remove_pid()
            print("강제 종료 완료")
            return True
        except ProcessLookupError:
            remove_pid()
            return True
        
    except PermissionError:
        print(f"프로세스 {pid} 종료 권한이 없습니다.")
        return False
    except ValueError:
        print("PID 파일이 손상되었습니다.")
        remove_pid()
        return False
    except Exception as e:
        print(f"오류 발생: {e}")
        return False

def cleanup_kafka_producer():
    """Kafka Producer를 안전하게 종료"""
    if common_state.producer is not None:
        try:
            logger.info("Kafka Producer flush 시작...")
            # 더 긴 타임아웃으로 flush
            common_state.producer.flush(timeout=10)
            logger.info("Kafka Producer flush 완료")
        except Exception as e:
            logger.warning(f"Kafka flush 중 오류 (무시됨): {e}")
        
        try:
            logger.info("Kafka Producer close 시작...")
            # close에도 타임아웃 설정
            common_state.producer.close(timeout=10)
            logger.info("Kafka Producer close 완료")
        except Exception as e:
            logger.warning(f"Kafka close 중 오류 (무시됨): {e}")

def start_collector():
    # 이미 실행 중인지 확인
    if PID_FILE.exists():
        try:
            with open(PID_FILE, 'r') as f:
                pid = int(f.read().strip())
            # 프로세스가 실제로 실행 중인지 확인
            try:
                os.kill(pid, 0)
                print(f"DWI Collector가 이미 실행 중입니다. (PID: {pid})")
                print("종료하려면: ./dwi_clct stop")
                return False
            except ProcessLookupError:
                # PID 파일은 있지만 프로세스는 없음 (비정상 종료)
                remove_pid()
        except Exception:
            remove_pid()
    
    # PID 파일 저장
    save_pid()
    
    # collector_ip 초기화
    common_state.collector_ip = get_my_ip()
    
    logger.info(f"패킷 스니퍼 v1 시작 - 수집기 IP: {common_state.collector_ip}")
    logger.info(f"배치 간격: {BATCH_INTERVAL}초, 오버플로우 버퍼: {OVERFLOW_BUFFER_SIZE}개")
    
    # Kafka Producer 초기화 (재시도 포함)
    logger.info("Kafka Producer 초기화 중...")
    if not common_state.init_kafka_producer():
        logger.critical("Kafka 서버에 연결할 수 없습니다. 프로그램을 종료합니다.")
        logger.critical("Kafka 서버 상태를 확인하고 다시 시도하세요.")
        remove_pid()
        return False  # 프로그램 종료
    
    logger.info("Kafka Producer 초기화 완료")
    
    # 제외할 IP/port들에 대한 필터 문자열 생성
#    filter_terms = [f"ip src {common_state.collector_ip}"]
#    filter_terms.extend([f"not ip dst {ip}" for ip in EXCEPT_DST_IPS])
    filter_terms = [f"not ip dst {ip}" for ip in EXCEPT_DST_IPS]
    filter_terms.extend([f"not src port {port}" for port in EXCEPT_SRC_PORTS])
    filter_terms.extend([f"not dst port {port}" for port in EXCEPT_DST_PORTS])
    filter_terms.append("tcp")
    pcap_filter = " and ".join(filter_terms)
    
    # 스레드 시작
    kafka_thread = threading.Thread(target=kafka_batch_sender, daemon=True)
    gc_thread = threading.Thread(target=run_gc_worker, daemon=True)
    backup_thread = threading.Thread(target=backup_file_sender, daemon=True)
    overflow_thread = threading.Thread(target=overflow_io_worker, daemon=True)

    kafka_thread.start()
    gc_thread.start()
    backup_thread.start()
    overflow_thread.start()
    
    try:
        # 패킷 캡처 시작
        sniff(iface=IFACE,
        filter=pcap_filter,
        prn=packet_handler,
        store=0)
    except KeyboardInterrupt:
        logger.info("종료 중...")
        common_state.running = False
        
        # 백그라운드 작업 완료 대기 (짧은 시간)
        time.sleep(2)
        
        # 남은 데이터 flush
        flush_on_exit()
        
        # Kafka Producer 안전하게 종료
        cleanup_kafka_producer()
        
        # PID 파일 삭제
        remove_pid()
        logger.info("종료 완료")
    except Exception as e:
        logger.error(f"예기치 않은 오류 발생: {e}")
        common_state.running = False
        cleanup_kafka_producer()
        remove_pid()
        raise

def main():
    """메인 함수 - 명령줄 인자 처리"""
    parser = argparse.ArgumentParser(
        description='DWI Collector - 패킷 스니퍼 및 Kafka 전송',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
사용 예시:
  %(prog)s start    - Collector 시작
  %(prog)s stop     - Collector 종료
  %(prog)s --help   - 도움말 보기
        '''
    )
    parser.add_argument(
        'command',
        nargs='?',
        choices=['start', 'stop'],
        help='start: Collector 시작, stop: Collector 종료'
    )
    
    args = parser.parse_args()
    
    # 인자가 없으면 안내 메시지 출력
    if args.command is None:
        print("=" * 60)
        print("DWI Collector - 패킷 스니퍼 및 Kafka 전송")
        print("=" * 60)
        print()
        print("사용법:")
        print("  ./dwi_clct start    - Collector 시작")
        print("  ./dwi_clct stop     - Collector 종료")
        print("  ./dwi_clct --help   - 상세 도움말 보기")
        print()
        print("=" * 60)
        parser.print_help()
        sys.exit(0)
    
    if args.command == 'start':
        start_collector()
    elif args.command == 'stop':
        stop_collector()

if __name__ == "__main__":
    main()
