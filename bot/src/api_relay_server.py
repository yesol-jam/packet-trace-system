#!/usr/bin/env python3
"""
api_relay_server.py - Tor 릴레이 체크 API 서버
Flask 기반 REST API로 IP 주소가 Tor 릴레이 노드인지 확인하는 엔드포인트를 제공합니다.
"""

from __future__ import annotations

import ipaddress
import sys
from pathlib import Path

from flask import Flask, request, jsonify

# settings.py 경로 추가
sys.path.append(str(Path(__file__).parent.parent))
from settings import API_RELAY_CHECK_PORT, TOR_CONTROL_HOST, TOR_CONTROL_PORT

# Logger 설정
from logger import setup_logger
logger = setup_logger('DWI.API_RELAY_SERVER')

# api_relay_checker import (src 디렉토리가 sys.path에 추가된 후)
try:
    from src.api_relay_checker import is_relay_node
except ImportError:
    # 상대 import 시도
    from api_relay_checker import is_relay_node

app = Flask(__name__)


@app.route('/api/v1/check-relay', methods=['POST'])
def check_relay():
    """
    IP 주소가 Tor 릴레이 노드인지 확인하는 API 엔드포인트
    
    요청 형식:
        {
            "ip": "1.2.3.4"
        }
    
    응답 형식:
        {
            "is_relay": true
        }
        또는
        {
            "error": "에러 메시지"
        }
    """
    try:
        # JSON 요청 파싱
        if not request.is_json:
            return jsonify({"error": "Content-Type must be application/json"}), 400
        
        data = request.get_json()
        if not data:
            return jsonify({"error": "JSON 데이터가 없습니다"}), 400
        
        ip = data.get("ip")
        if not ip:
            return jsonify({"error": "ip 필드가 필요합니다"}), 400
        
        # IP 주소 유효성 검증
        try:
            ip_obj = ipaddress.ip_address(ip)
            ip_str = str(ip_obj)
        except ValueError:
            return jsonify({"error": f"유효하지 않은 IP 주소입니다: {ip}"}), 400
        
        logger.info(f"릴레이 확인 요청: {ip_str}")
        
        # Tor 릴레이 노드 확인
        try:
            is_relay = is_relay_node(
                ip_str,
                control_host=TOR_CONTROL_HOST,
                control_port=TOR_CONTROL_PORT,
                include_ipv6=True
            )
            
            logger.info(f"릴레이 확인 결과: {ip_str} -> {is_relay}")
            return jsonify({"is_relay": is_relay}), 200
            
        except Exception as exc:
            logger.error(f"릴레이 확인 중 오류 발생: {exc}", exc_info=True)
            return jsonify({"error": f"릴레이 확인 중 오류가 발생했습니다: {str(exc)}"}), 500
    
    except Exception as exc:
        logger.error(f"API 요청 처리 중 오류 발생: {exc}", exc_info=True)
        return jsonify({"error": f"요청 처리 중 오류가 발생했습니다: {str(exc)}"}), 500


@app.route('/health', methods=['GET'])
def health_check():
    """헬스 체크 엔드포인트"""
    return jsonify({"status": "ok"}), 200


def run_api_server(host: str = "0.0.0.0", port: int = API_RELAY_CHECK_PORT, debug: bool = False):
    """
    Flask API 서버를 실행합니다.
    
    Args:
        host: 바인딩할 호스트 주소
        port: 바인딩할 포트 번호
        debug: 디버그 모드 활성화 여부
    """
    logger.info(f"API 서버 시작: {host}:{port}")
    app.run(host=host, port=port, debug=debug, threaded=True, use_reloader=False)

