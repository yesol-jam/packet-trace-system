#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
IP 유틸리티 모듈
"""

import socket


def get_my_ip():
    """내 IP 조회"""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
    except Exception:
        ip = "127.0.0.1"
    finally:
        s.close()
    return ip

