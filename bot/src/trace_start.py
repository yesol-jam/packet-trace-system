"""
trace_start.py - 추적 패턴 실행 및 이행 내역 생성
작성자 : 제예솔
작성일 : 2025-10-15
"""
import subprocess
import time
import sys
import threading
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, Optional, Set, Tuple
from urllib.parse import urlparse

# settings.py 경로 추가
sys.path.append(str(Path(__file__).parent.parent))
from settings import TOR_SOCKS5_PROXY as SOCKS5_PROXY

# Logger 설정
from logger import setup_logger
logger = setup_logger('DWI.TRACE_START')

# Tor 컨트롤 관련 import
try:
    from stem import SocketError
    from stem.connection import AuthenticationFailure, MissingPassword
    from stem.control import Controller, EventType
    try:
        from stem.response.events import CircEvent, StreamEvent
    except ImportError:  # Stem < 1.8 fallback
        CircEvent = object  # type: ignore[assignment]
        StreamEvent = object  # type: ignore[assignment]
    TOR_CONTROL_AVAILABLE = True
except ImportError:
    TOR_CONTROL_AVAILABLE = False
    logger.warning("stem 라이브러리가 설치되지 않아 서킷 모니터링 기능을 사용할 수 없습니다.")

# Tor 컨트롤 기본 설정
DEFAULT_CONTROL_HOST = "127.0.0.1"
DEFAULT_CONTROL_PORT = 9051
DEFAULT_COOKIE_PATHS = (
    Path("/run/tor/control.authcookie"),
    Path("/var/run/tor/control.authcookie"),
    Path("/var/lib/tor/control_auth_cookie"),
)


def _format_created(created: Optional[datetime]) -> str:
    """서킷 생성 시각 포맷팅"""
    if not created:
        return "알 수 없음"
    if created.tzinfo is None:
        created = created.replace(tzinfo=timezone.utc)
    return created.astimezone().isoformat()


def _normalize_onion_host(url: str) -> str:
    """URL에서 .onion 호스트 추출"""
    parsed = urlparse(url if "://" in url else f"http://{url}")
    if not parsed.hostname:
        raise ValueError(f".onion 호스트를 추출할 수 없습니다: {url}")
    host = parsed.hostname.lower().rstrip(".")
    if not host.endswith(".onion"):
        raise ValueError(f".onion 호스트가 아닙니다: {parsed.hostname}")
    return host


def _authenticate_controller(controller: Controller) -> None:
    """Tor 컨트롤러 인증"""
    try:
        controller.authenticate()
        return
    except MissingPassword:
        logger.debug("기본 인증 실패, 쿠키 파일 탐색.")

    last_error: Optional[Exception] = None
    for path in DEFAULT_COOKIE_PATHS:
        expanded = path.expanduser()
        if not expanded.exists():
            continue
        try:
            controller.authenticate(expanded.read_bytes().hex())
            logger.debug("쿠키 파일 %s로 인증 성공.", expanded)
            return
        except (AuthenticationFailure, MissingPassword) as exc:
            last_error = exc
            logger.debug("쿠키 파일 %s 인증 실패: %s", expanded, exc)
        except OSError as exc:
            last_error = exc
            logger.debug("쿠키 파일 %s 읽기 실패: %s", expanded, exc)

    if last_error:
        raise last_error
    raise MissingPassword("쿠키 파일을 찾지 못했고 비밀번호도 제공되지 않았습니다.")


class RelayInfoResolver:
    """릴레이 정보 조회 및 캐싱"""
    def __init__(self, controller: Controller) -> None:
        self._controller = controller
        self._cache: Dict[str, Dict[str, Optional[str]]] = {}
        self._lock = threading.Lock()

    def resolve(self, fingerprint: str) -> Dict[str, Optional[str]]:
        fingerprint = fingerprint.lstrip("$").upper()
        with self._lock:
            cached = self._cache.get(fingerprint)
        if cached is not None:
            return cached

        info = {"ipv4": None, "ipv6": None, "port": None}
        try:
            response = self._controller.get_info(f"ns/id/${fingerprint}")
        except Exception as exc:
            logger.debug("릴레이 정보 조회 실패 (%s): %s", fingerprint, exc)
            with self._lock:
                self._cache[fingerprint] = info
            return info

        ipv4: Optional[str] = None
        ipv6: Optional[str] = None
        port: Optional[str] = None

        for raw_line in response.splitlines():
            line = raw_line.strip()
            if line.startswith("r "):
                parts = line.split()
                if len(parts) >= 8:
                    ipv4 = parts[6]
                    port = parts[7]
            elif line.startswith("a "):
                ipv6_part = line[2:].strip()
                if ipv6_part.startswith("[") and "]" in ipv6_part:
                    ipv6_full = ipv6_part.split("]", 1)[0][1:]
                    if ":" in ipv6_full:
                        host_part, sep, tail = ipv6_full.rpartition(":")
                        if sep and tail.isdigit():
                            ipv6 = host_part
                        else:
                            ipv6 = ipv6_full
                    else:
                        ipv6 = ipv6_full

        info.update({"ipv4": ipv4, "ipv6": ipv6, "port": port})
        with self._lock:
            self._cache[fingerprint] = info
        return info


class CircuitMonitor:
    """Tor 서킷 이벤트 모니터링 및 로그 출력"""
    def __init__(
        self,
        controller: Controller,
        target_host: str,
        done_event: threading.Event,
        resolver: RelayInfoResolver,
        circuit_ips: Optional[Dict[str, Optional[str]]] = None,
    ) -> None:
        self._controller = controller
        self._target_host = target_host.lower()
        self._lock = threading.Lock()
        self._seen: Set[Tuple[str, str]] = set()
        self._relevant_circuits: Set[str] = set()
        self._done_event = done_event
        self._resolver = resolver
        self._circuit_ips = circuit_ips  # 마지막 서킷 IP 저장용

    def __call__(self, event) -> None:
        if isinstance(event, StreamEvent):
            self._handle_stream_event(event)
        elif isinstance(event, CircEvent):
            self._handle_circ_event(event)

    def _handle_stream_event(self, event: StreamEvent) -> None:
        target = (getattr(event, "target", "") or "").lower()
        circ_id = getattr(event, "circ_id", None) or ""

        if self._target_host not in target:
            return

        status_name = self._status_name(event.status)

        if circ_id:
            with self._lock:
                self._relevant_circuits.add(circ_id)
            if status_name in {"ATTACHED", "SUCCEEDED"}:
                self._log_circuit(circ_id, f"STREAM {status_name}")

    def _handle_circ_event(self, event: CircEvent) -> None:
        circ_id = getattr(event, "id", None)
        if not circ_id:
            return

        with self._lock:
            if circ_id not in self._relevant_circuits:
                return

        status_name = self._status_name(event.status)
        if status_name in {"BUILT", "EXTENDED"}:
            self._log_circuit(circ_id, f"CIRC {status_name}")

    def _log_circuit(self, circuit_id: str, trigger: str) -> None:
        """서킷 정보를 로그로 출력 및 IP 주소 저장"""
        key = (circuit_id, trigger)
        with self._lock:
            if key in self._seen:
                return
            self._seen.add(key)

        try:
            circuit = self._controller.get_circuit(circuit_id)
        except Exception as exc:
            logger.debug("회로 %s 조회 실패: %s", circuit_id, exc)
            return

        logger.info("=" * 60)
        logger.info("[서킷 경로]")
        logger.info(f"회로 ID: {circuit.id} (트리거: {trigger})")
        logger.info(f"생성 시각: {_format_created(getattr(circuit, 'created', None))}")
        logger.info("경로:")
        
        # 서킷 IP 주소 추출 및 저장
        circuit_ip_addresses = []
        for idx, relay in enumerate(circuit.path, 1):
            fingerprint, nickname = relay
            logger.info(f"  {idx}. {nickname} ({fingerprint})")
            relay_info = self._resolver.resolve(fingerprint)
            details = []
            ipv4 = relay_info.get("ipv4")
            ipv6 = relay_info.get("ipv6")
            
            # IP 주소 저장 (IPv4 우선, 없으면 IPv6)
            if ipv4:
                details.append(f"IPv4: {ipv4}")
                circuit_ip_addresses.append(ipv4)
            elif ipv6:
                details.append(f"IPv6: {ipv6}")
                circuit_ip_addresses.append(ipv6)
            else:
                circuit_ip_addresses.append(None)
            
            # 로그 출력용 (IPv4와 IPv6 모두 표시)
            if ipv4 and ipv6:
                details.append(f"IPv6: {ipv6}")
            
            port = relay_info.get("port")
            if port:
                details.append(f"Port: {port}")
            if details:
                logger.info(f"     ↳ {' | '.join(details)}")
            else:
                logger.info("     ↳ IP 정보 없음")
        logger.info("=" * 60)
        
        # 마지막 서킷 IP 주소 저장 (Entry Guard, Middle, Exit 순서)
        if self._circuit_ips is not None:
            self._circuit_ips["clntGrdIpAddr"] = circuit_ip_addresses[0] if len(circuit_ip_addresses) > 0 else None
            self._circuit_ips["clntMdIpAddr"] = circuit_ip_addresses[1] if len(circuit_ip_addresses) > 1 else None
            self._circuit_ips["clntMdtIpAddr"] = circuit_ip_addresses[2] if len(circuit_ip_addresses) > 2 else None
        
        self._done_event.set()

    @staticmethod
    def _status_name(status) -> str:
        if hasattr(status, "name"):
            return status.name
        return str(status).upper()


def _monitor_curl_request(url: str, circuit_ips: Optional[Dict[str, Optional[str]]] = None) -> None:
    """curl 요청 시 사용된 서킷 모니터링 (동기 실행)
    
    Args:
        url: 요청 URL
        circuit_ips: 서킷 IP 주소를 저장할 딕셔너리 (clntGrdIpAddr, clntMdIpAddr, clntMdtIpAddr)
    """
    if not TOR_CONTROL_AVAILABLE:
        logger.debug("Tor 컨트롤 기능을 사용할 수 없어 서킷 모니터링을 건너뜁니다.")
        return

    try:
        target_host = _normalize_onion_host(url)
    except ValueError:
        logger.debug("URL에서 .onion 호스트를 추출할 수 없어 서킷 모니터링을 건너뜁니다.")
        return

    try:
        with Controller.from_port(address=DEFAULT_CONTROL_HOST, port=DEFAULT_CONTROL_PORT) as controller:
            _authenticate_controller(controller)
            done_event = threading.Event()
            resolver = RelayInfoResolver(controller)
            monitor = CircuitMonitor(controller, target_host, done_event, resolver, circuit_ips)
            controller.add_event_listener(monitor, EventType.STREAM, EventType.CIRC)
            try:
                # 이벤트 수집 대기 (최대 2초)
                done_event.wait(2.0)
            finally:
                controller.remove_event_listener(monitor)
    except (SocketError, MissingPassword, AuthenticationFailure) as exc:
        logger.debug("Tor 컨트롤 포트 연결/인증 실패 (서킷 모니터링 건너뜀): %s", exc)
    except Exception as exc:
        logger.debug("서킷 모니터링 중 오류 발생: %s", exc)


def run_trace_start(trceTrgtUrl, trcePatternArr, patnCd, reptNmTn):
    """
    추적 패턴을 실행하고 추적 이행 내역을 반환
    
    Args:
        trceTrgtUrl: 추적 대상 URL (예: aaa.onion)
        trcePatternArr: 추적 패턴 객체 배열 (회차:trsmTme, 시간 텀 : trsmGpper, 패킷사이즈 : pcktSize)
        patnCd: 추적 패턴 코드
        reptNmTn: 반복 횟수 (사이클 수)
        
    Returns:
        trceFlfmtHst: 추적 이행 내역 객체
    """
    # 입력 데이터 출력
    logger.info(f"추적 패턴 배열: {trcePatternArr}")
    logger.info(f"추적 대상 URL: {trceTrgtUrl}")
    logger.info(f"추적 패턴 코드: {patnCd}")
    
    # 시작 시간 기록
    bgngDt = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
    
    # 마지막 서킷 IP 주소 저장용 딕셔너리
    last_circuit_ips = {
        "clntGrdIpAddr": None,  # Entry Guard IP
        "clntMdIpAddr": None,   # Middle IP
        "clntMdtIpAddr": None   # Exit/Moderator IP
    }
    
    # 추적 패턴 처리 로직
    try:
        logger.info(f"추적 작업 수행 중... (패턴코드: {patnCd}, 반복횟수: {reptNmTn})")
        
        # reptNmTn만큼 사이클 반복
        for cycle in range(int(reptNmTn)):
            logger.info(f"사이클 {cycle + 1}/{reptNmTn} 시작")
            
            # 배열의 각 패턴에 따라 요청 전송
            for idx, pattern in enumerate(trcePatternArr):
                trsmGpper = pattern.get('trsmGpper', 0)
                pcktSize = pattern.get('pcktSize', 0)
                
                # 요청 전 대기
                logger.info(f"{trsmGpper}초 대기 후 요청 {idx + 1}/{len(trcePatternArr)} 전송")
                time.sleep(trsmGpper)
                
                # 패딩 문자열 생성
                padding = 'A' * pcktSize
                url = f"{trceTrgtUrl}/?pad={padding}"
                
                # curl 명령 실행
                cmd = f'curl --socks5-hostname {SOCKS5_PROXY} -k "{url}"'
                logger.info(f"요청 전송 중... (패딩크기: {pcktSize})")
                logger.debug(f"실행 명령: {cmd[:100]}...")  # 디버깅용
                
                # 서킷 모니터링을 위한 스레드 시작 (curl 실행 전에 시작하여 이벤트 수집 준비)
                monitor_thread = threading.Thread(
                    target=_monitor_curl_request,
                    args=(url, last_circuit_ips),
                    daemon=True
                )
                monitor_thread.start()
                
                # curl 실행
                subprocess.run(cmd, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                
                # 서킷 정보 수집을 위한 대기 (모니터 스레드가 이벤트를 수집할 시간 확보)
                time.sleep(1.5)
            
            logger.info(f"사이클 {cycle + 1}/{reptNmTn} 완료")
        
    except Exception as e:
        logger.error("추적 작업 중 오류 발생", exc_info=True)
        
    
    # 분석 이행 여부(default N = 미분석)
    flfmtYn = "N"

    # 종료 시간 기록
    endDt = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
    
    # 추적 이행 내역 객체 생성
    trceFlfmtHst = {
        "patnCd": patnCd,
        "trceTrgtUrl": trceTrgtUrl,
        "bgngDt": bgngDt,
        "endDt": endDt,
        "reptNmTn": str(reptNmTn),
        "flfmtYn": flfmtYn,
        "clntGrdIpAddr": last_circuit_ips["clntGrdIpAddr"],
        "clntMdIpAddr": last_circuit_ips["clntMdIpAddr"],
        "clntMdtIpAddr": last_circuit_ips["clntMdtIpAddr"]
    }
    
    # 결과 객체 출력
    logger.info(f"추적 이행 내역: {trceFlfmtHst}")
    
    return trceFlfmtHst