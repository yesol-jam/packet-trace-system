#!/usr/bin/env python3
"""
api_relay_checker.py - Tor 릴레이 노드 확인 모듈
check_relay_ip.py의 로직을 재사용하여 IP 주소가 Tor 릴레이 노드인지 확인합니다.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional, Sequence, Set, Tuple

from stem import SocketError
from stem.connection import AuthenticationFailure, MissingPassword
from stem.control import Controller

# Logger 설정
import sys
from pathlib import Path as PathLib
sys.path.append(str(PathLib(__file__).parent.parent))
from logger import setup_logger

logger = setup_logger('DWI.API_RELAY_CHECKER')

DEFAULT_CONTROL_HOST = "127.0.0.1"
DEFAULT_CONTROL_PORT = 9051
DEFAULT_COOKIE_PATHS: Sequence[Path] = (
    Path("/run/tor/control.authcookie"),
    Path("/var/run/tor/control.authcookie"),
    Path("/var/lib/tor/control_auth_cookie"),
)


def authenticate(controller: Controller) -> None:
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


def find_relays_by_ip(controller: Controller, ip: str, include_ipv6: bool = True) -> Set[Tuple[str, str, str, str, Tuple[str, ...]]]:
    """
    IP 주소로 Tor 릴레이 노드를 검색합니다.
    
    Args:
        controller: Tor Controller 인스턴스
        ip: 검색할 IP 주소
        include_ipv6: IPv6 주소도 검색할지 여부
    
    Returns:
        릴레이 정보 튜플 집합 (nickname, fingerprint, address, port, flags)
    """
    ip = ip.lower()
    matches: Set[Tuple[str, str, str, str, Tuple[str, ...]]] = set()

    try:
        statuses = list(controller.get_network_statuses())
    except Exception as exc:  # pylint: disable=broad-except
        logger.debug("네트워크 상태 목록 조회 실패: %s", exc)
        statuses = []

    for status in statuses:
        nickname = getattr(status, "nickname", "Unknown")
        fingerprint = getattr(status, "fingerprint", "Unknown")
        address = getattr(status, "address", "").lower()
        or_port = str(getattr(status, "or_port", "")) if getattr(status, "or_port", None) else ""
        flags = tuple(sorted(str(flag) for flag in getattr(status, "flags", []) if flag))

        if address == ip:
            matches.add((nickname, fingerprint, address, or_port, flags))

        if include_ipv6:
            for addr in getattr(status, "or_addresses", []):
                try:
                    address_str = addr.address.lower()
                    port_str = str(addr.port) if addr.port else ""
                except AttributeError:
                    continue
                if address_str == ip:
                    matches.add((nickname, fingerprint, address_str, port_str, flags))

    if not matches:
        try:
            descriptors = list(controller.get_server_descriptors())
        except Exception as exc:  # pylint: disable=broad-except
            logger.debug("서버 디스크립터 조회 실패: %s", exc)
            descriptors = []

        for descriptor in descriptors:
            nickname = getattr(descriptor, "nickname", "Unknown")
            fingerprint = getattr(descriptor, "fingerprint", "Unknown")
            address = getattr(descriptor, "address", "").lower()
            or_port = str(getattr(descriptor, "or_port", "")) if getattr(descriptor, "or_port", None) else ""
            flags = tuple(sorted(str(flag) for flag in getattr(descriptor, "flags", []) if flag))

            if address == ip:
                matches.add((nickname, fingerprint, address, or_port, flags))
                continue

            if include_ipv6:
                for addr in getattr(descriptor, "or_addresses", []):
                    address_str = getattr(addr, "host", "").lower()
                    port_str = str(getattr(addr, "port", "")) if getattr(addr, "port", None) else ""
                    if address_str == ip:
                        matches.add((nickname, fingerprint, address_str, port_str, flags))

    return matches


def is_relay_node(ip: str, control_host: str = DEFAULT_CONTROL_HOST, control_port: int = DEFAULT_CONTROL_PORT, include_ipv6: bool = True) -> bool:
    """
    IP 주소가 Tor 릴레이 노드인지 확인합니다.
    
    Args:
        ip: 확인할 IP 주소
        control_host: Tor 컨트롤 포트 호스트
        control_port: Tor 컨트롤 포트
        include_ipv6: IPv6 주소도 검색할지 여부
    
    Returns:
        Tor 릴레이 노드이면 True, 아니면 False
    """
    try:
        with Controller.from_port(address=control_host, port=control_port) as controller:
            authenticate(controller)
            matches = find_relays_by_ip(controller, ip, include_ipv6)
            return len(matches) > 0
    except (SocketError, MissingPassword, AuthenticationFailure) as exc:
        logger.error("Tor 컨트롤 포트 연결/인증 실패: %s", exc)
        raise
    except Exception as exc:  # pylint: disable=broad-except
        logger.exception("릴레이 확인 중 오류 발생: %s", exc)
        raise

