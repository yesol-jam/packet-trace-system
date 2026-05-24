"""
status_checker.py - 서버 상태 확인기
작성자 : 제예솔
작성일 : 2025-10-15
"""
import time
import requests
import socks
import socket
import sys
from pathlib import Path
from urllib.parse import urlparse

# settings.py 경로 추가
sys.path.append(str(Path(__file__).parent.parent))
from settings import TOR_PROXY_HOST, TOR_PROXY_PORT, PROXIES

# Logger 설정
from logger import setup_logger
logger = setup_logger('DWI.STATUS_CHECKER')


def run_status_checker(task):
    """서버 상태 확인 메인 함수"""
    logger.info("작업 처리 시작")
    
    if not isinstance(task, dict) or 'trceTrgtUrl' not in task:
        return None
    
    # 서버 상태 객체 생성
    srvrStts = {
        "trceTrgtUrl": task['trceTrgtUrl'],
        "trceTrgtNo": task.get('trceTrgtNo', ''),
        "srvrSttsCd": "",
        "regDt": "",
        "srvcActvYn": "",
        "cptrYn": task.get('cptrYn', '')
    }

    ok, msg = tor_healthcheck()

    if ok:
        classify_status(srvrStts)
    
    logger.info("작업 처리 완료")
    return srvrStts


def classify_status(srvrStts):
    """서버 상태 분류 (HTTP/TCP 프로빙)"""
    trceTrgtUrl = srvrStts['trceTrgtUrl']

    # HTTPS를 HTTP로 변경(혹시 필요한 경우)
    #if trceTrgtUrl.startswith('https://'):
    #    trceTrgtUrl = trceTrgtUrl.replace('https://', 'http://')

    http_success, status_code = http_probe(trceTrgtUrl)
    
    # http 상태 확인
    if http_success:
        srvrStts['srvrSttsCd'] = "10"
        srvrStts['srvcActvYn'] = "Y" if 200 <= status_code < 300 else "N"
    
    #tcp 상태 확인
    else:
        parsed = urlparse(trceTrgtUrl)
        host = parsed.hostname
        port = parsed.port if parsed.port else (443 if parsed.scheme == 'https' else 80)
        
        tcp_success = tcp_probe_via_tor(host, port)
        
        srvrStts['srvrSttsCd'] = "20" if tcp_success else "30"
        srvrStts['srvcActvYn'] = "N"

    srvrStts['regDt'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

# http 연결
def http_probe(url, timeout=30):
    try:
        #TODO : 실환경에서 verify=False 옵션 추가는 위험함
        #response = requests.get(url, proxies=PROXIES, timeout=timeout)
        
        response = requests.get(url, proxies=PROXIES, timeout=timeout, verify=False)
        
        return True, response.status_code
    except Exception as e:
        logger.error(f"http_probe error: {repr(e)}")
        return False, None

# tor tcp 연결
def tcp_probe_via_tor(host, port, timeout=5):
    try:
        socks.setdefaultproxy(socks.SOCKS5, TOR_PROXY_HOST, TOR_PROXY_PORT, True)
        sock = socks.socksocket()
        sock.settimeout(timeout)
        sock.connect((host, port))
        sock.close()
        return True
    except Exception:
        return False

# 토르 프록시 헬스체크
def tor_healthcheck(tor_host=TOR_PROXY_HOST, tor_port=TOR_PROXY_PORT, timeout=3):
    """
    1) tor_host:tor_port 로 TCP 붙는지 확인
    2) test_onion 이 주어지면, SOCKS5h 통해 <onion>:80 으로 CONNECT 시도
    반환: (ok: bool, msg: str)
    """
    
    try:
        socket.create_connection((tor_host, tor_port), timeout=timeout).close()
    except Exception as e:
        logger.error(f"SOCKS 접속 실패: {e}")
        return False, f"SOCKS 접속 실패: {tor_host}:{tor_port} -> {e!r}"
    
    logger.info("토르 프록시 헬스 체크 성공")
    return True, "OK"