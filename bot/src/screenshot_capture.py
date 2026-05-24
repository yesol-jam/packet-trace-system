"""
screenshot_capture.py - 웹사이트 스크린샷 캡처
작성자 : 제예솔
작성일 : 2025-10-15
"""
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import time
import os
import sys
from pathlib import Path
from datetime import datetime
import subprocess
from urllib.request import urlretrieve
from urllib.error import URLError
import json

# settings.py 경로 추가
sys.path.append(str(Path(__file__).parent.parent))
from settings import CAPTURE_DIR, TOR_SOCKS5_PROXY

# Logger 설정
from logger import setup_logger
logger = setup_logger('DWI.SCREENSHOT_CAPTURE')

_FONT_PREPARED = False
_FONT_SANS_FAMILY = None
_FONT_MONO_FAMILY = None

_FONT_DOWNLOADS = {
    "NotoSansCJKkr-Regular.otf": "https://github.com/googlefonts/noto-cjk/raw/main/Sans/OTF/Korean/NotoSansCJKkr-Regular.otf",
    "NotoSansCJKkr-Bold.otf": "https://github.com/googlefonts/noto-cjk/raw/main/Sans/OTF/Korean/NotoSansCJKkr-Bold.otf",
    "NotoSansMonoCJKkr-Regular.otf": "https://github.com/googlefonts/noto-cjk/raw/main/Sans/OTF/Korean/NotoSansMonoCJKkr-Regular.otf",
}


def _list_installed_korean_fonts():
    fonts = set()
    try:
        result = subprocess.run(
            ["fc-list", ":lang=ko", "family"],
            check=False,
            capture_output=True,
            text=True,
        )
        if result.returncode == 0:
            for line in result.stdout.splitlines():
                families = [family.strip() for family in line.split(",")]
                fonts.update(families)
    except FileNotFoundError:
        logger.debug("fc-list 명령을 찾을 수 없습니다.")
    return fonts


def _write_fontconfig(font_dir: Path):
    fontconfig_dir = font_dir / "fontconfig"
    fontconfig_dir.mkdir(parents=True, exist_ok=True)
    fonts_conf = fontconfig_dir / "fonts.conf"
    fonts_conf.write_text(
        f"""<?xml version="1.0"?>
<!DOCTYPE fontconfig SYSTEM "fonts.dtd">
<fontconfig>
    <dir>{font_dir}</dir>
</fontconfig>
""",
        encoding="utf-8",
    )
    os.environ.setdefault("FONTCONFIG_PATH", str(fontconfig_dir))
    os.environ.setdefault("FONTCONFIG_FILE", str(fontconfig_dir / "fonts.conf"))


def prepare_korean_font_support():
    global _FONT_PREPARED, _FONT_SANS_FAMILY, _FONT_MONO_FAMILY

    if _FONT_PREPARED:
        return _FONT_SANS_FAMILY, _FONT_MONO_FAMILY

    installed_fonts = _list_installed_korean_fonts()
    preferred_pairs = [
        ("NanumGothic", "NanumGothicCoding"),
        ("Nanum Gothic", "NanumGothicCoding"),
        ("Noto Sans CJK KR", "Noto Sans Mono CJK KR"),
        ("Noto Sans KR", "Noto Sans Mono CJK KR"),
    ]

    for sans_family, mono_family in preferred_pairs:
        if sans_family in installed_fonts:
            logger.debug(f"시스템 한글 폰트 사용: {sans_family}")
            _FONT_SANS_FAMILY = sans_family
            _FONT_MONO_FAMILY = mono_family if mono_family in installed_fonts else sans_family
            _FONT_PREPARED = True
            return _FONT_SANS_FAMILY, _FONT_MONO_FAMILY

    font_dir = Path(__file__).parent / "fonts"
    font_dir.mkdir(parents=True, exist_ok=True)
    downloaded = False

    for filename, url in _FONT_DOWNLOADS.items():
        dest = font_dir / filename
        if not dest.exists():
            try:
                logger.info(f"한글 폰트 다운로드: {filename}")
                urlretrieve(url, dest)
                downloaded = True
            except URLError as err:
                logger.warning(f"한글 폰트 다운로드 실패 ({filename}): {err}")

    if downloaded:
        try:
            subprocess.run(
                ["fc-cache", "-f", str(font_dir)],
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        except FileNotFoundError:
            logger.debug("fc-cache 명령을 찾을 수 없습니다.")

    if any((font_dir / name).exists() for name in _FONT_DOWNLOADS):
        _write_fontconfig(font_dir)
        _FONT_SANS_FAMILY = "Noto Sans CJK KR"
        _FONT_MONO_FAMILY = "Noto Sans Mono CJK KR"
        _FONT_PREPARED = True
        logger.info("내장 한글 폰트 구성을 완료했습니다.")
        return _FONT_SANS_FAMILY, _FONT_MONO_FAMILY

    _FONT_SANS_FAMILY = "NanumGothic"
    _FONT_MONO_FAMILY = "NanumGothicCoding"
    _FONT_PREPARED = True
    logger.warning("한글 폰트를 준비하지 못했습니다. 시스템 기본 폰트에 의존합니다.")
    return _FONT_SANS_FAMILY, _FONT_MONO_FAMILY


def run_screenshot_capture(srvrStts):
    """
    스크린샷 캡처를 수행하고 파일명을 반환
    
    Returns:
        str: 캡처 성공 시 파일명만, 실패 시 None
    """
    logger.info("작업 처리 시작")
    
    result = capture_website_screenshot(srvrStts['trceTrgtUrl'], srvrStts['trceTrgtNo'])
    
    # 전체 경로가 반환되면 파일명만 추출하여 반환
    if result:
        filename = os.path.basename(result)
        logger.info(f"캡처 성공: {filename}")
        return filename
    else:
        logger.warning("캡처 실패")
        return None
    
def capture_website_screenshot(trceTrgtUrl, trceTrgtNo, output_dir=CAPTURE_DIR):

    # 출력 디렉토리 생성
    os.makedirs(output_dir, exist_ok=True)
    
    # 파일명 생성 (yyyymmdd_hhmmss.png)
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    output_filename = f"cptr_{trceTrgtNo}_{timestamp}.png"
    output_path = os.path.join(output_dir, output_filename)
    
    sans_font, mono_font = prepare_korean_font_support()

    # Chrome 옵션 설정
    chrome_options = Options()
    chrome_options.add_argument('--headless')  # 브라우저 창 숨기기
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--window-size=1920,1080')  # 화면 크기 설정
    chrome_options.add_argument(f'--proxy-server=socks5://{TOR_SOCKS5_PROXY}')  # Tor 프록시 설정
    
    # SSL 인증서 오류 무시
    chrome_options.add_argument('--ignore-certificate-errors')
    chrome_options.add_argument('--ignore-ssl-errors')
    chrome_options.add_argument('--allow-insecure-localhost')
    chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])
    
    # 한글 폰트 설정 (한글 깨짐 방지)
    chrome_options.add_argument('--lang=ko-KR')
    chrome_options.add_argument('--font-render-hinting=medium')
    prefs = {
        'intl.accept_languages': 'ko-KR,ko',
        'profile.default_content_setting_values.fonts': 1,
        'browser.enable_spellchecking': False,
        'webkit.webprefs.font_family.standard': sans_font,
        'webkit.webprefs.font_family.serif': sans_font,
        'webkit.webprefs.font_family.sansserif': sans_font,
        'webkit.webprefs.font_family.fixed': mono_font or sans_font,
        'webkit.webprefs.font_family.cursive': sans_font,
        'webkit.webprefs.font_family.fantasy': sans_font,
    }
    chrome_options.add_experimental_option('prefs', prefs)

    driver = None
    
    try:
        logger.info("ChromeDriver 자동 설치 및 설정 중...")
        
        # ChromeDriver 자동 설치 및 설정
        driver_path = ChromeDriverManager().install()

        # Chrome 142 드라이버 패키지 이슈 대응
        if driver_path.endswith(".chromedriver"):
            candidate = Path(driver_path).parent / "chromedriver"
            if candidate.exists():
                logger.debug(f"실행 파일 경로 보정: {candidate}")
                driver_path = str(candidate)
            else:
                logger.warning("chromedriver 실행 파일을 찾을 수 없습니다.")

        if not os.access(driver_path, os.X_OK):
            logger.debug("chromedriver 실행 권한 부여")
            os.chmod(driver_path, 0o755)

        service = Service(driver_path)
        driver = webdriver.Chrome(service=service, options=chrome_options)

        font_stack_js = "[" + ", ".join(json.dumps(name) for name in [
            sans_font,
            mono_font,
            "Nanum Gothic",
            "Malgun Gothic",
            "Apple SD Gothic Neo",
            "Noto Sans KR",
            "Noto Sans CJK KR",
        ] if name) + "]"

        driver.execute_cdp_cmd(
            "Page.addScriptToEvaluateOnNewDocument",
            {
                "source": (
                    "(() => {"
                    f"const fonts = {font_stack_js};"
                    "const stack = fonts.concat(['sans-serif']).join(', ');"
                    "const style = document.createElement('style');"
                    "style.innerHTML = '* { font-family: ' + stack + ' !important; }';"
                    "document.head.appendChild(style);"
                    "})();"
                )
            },
        )
        
        logger.info(f"웹사이트 접근 중: {trceTrgtUrl}")
        
        # 웹사이트 로드
        driver.get(trceTrgtUrl)
        
        # 페이지 로딩 대기 (Tor는 느리므로 더 긴 대기)
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
        
        # 추가 로딩 시간 (Tor 네트워크 지연 고려)
        time.sleep(10)
        
        # 스크린샷 캡처
        driver.save_screenshot(output_path)
        
        logger.info(f"스크린샷 저장 완료: {output_path}")
        return output_path
        
    except Exception as e:
        logger.error("오류 발생", exc_info=True)
        return None
        
    finally:
        # 브라우저 종료
        if driver:
            driver.quit()