#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
GC Worker Module
"""

import gc
import sys
import time
from pathlib import Path

# 설정 파일 import를 위한 경로 설정
SCRIPT_DIR = Path(__file__).parent.parent
sys.path.append(str(SCRIPT_DIR))

# Logger 설정
from logger import setup_logger
logger = setup_logger('DWI.GC_WORKER')

from src import common_state
from settings import GC_TERM

def run_gc_worker():
    """GC 워커 함수"""
    while common_state.running:
        time.sleep(GC_TERM)
        collected = gc.collect()
        logger.debug(f"GC 실행: {collected}개 객체 정리")

