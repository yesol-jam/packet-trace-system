# Packet Trace System

익명 네트워크 환경에서의 실시간 패킷 수집·추적·분석을 위한 통합 에이전트 시스템입니다.  
세 개의 독립적인 에이전트(Bot, DWI Sensor, Sensor-Bot)가 각자의 역할을 수행하며, Apache Kafka를 중심으로 데이터를 수집·전달합니다.

---

## System Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                      Packet Trace System                        │
│                                                                 │
│   ┌─────────────┐   ┌──────────────┐   ┌──────────────────┐   │
│   │     Bot     │   │  DWI Sensor  │   │   Sensor-Bot     │   │
│   │             │   │              │   │                  │   │
│   │ 추적 명령   │   │ 상시 패킷    │   │ 수요 기반 패킷   │   │
│   │ 수신 및     │   │ 수집         │   │ 수집             │   │
│   │ 스크린샷    │   │ (eth0 상시)  │   │ (요청 연동)      │   │
│   └──────┬──────┘   └──────┬───────┘   └────────┬─────────┘   │
│          │                 │                     │             │
└──────────┼─────────────────┼─────────────────────┼─────────────┘
           │                 │                     │
           ▼                 ▼                     ▼
┌──────────────────────────────────────────────────────────────┐
│                   Apache Kafka Cluster                        │
│                                                              │
│  Topic: TRCE_FLFMT_HST  ◄──── Bot (추적 이행 결과)           │
│  Topic: SRVR_STTS_CHG   ◄──── Bot (서버 상태)                │
│  Topic: TOR_NETWK_PCKT  ◄──── DWI Sensor (상시 패킷)         │
│  Topic: DMND_BOT_PCKT   ◄──── Sensor-Bot (수요 기반 패킷)    │
└──────────────────────────────────────────────────────────────┘
```

---

## Components

### 1. `bot/` — 추적 실행 에이전트

외부 API로부터 추적 대상 목록을 수신하여 익명 네트워크 상의 서버 상태 확인, 스크린샷 캡처, 패킷 추적 패턴 실행을 자동화하는 에이전트입니다.

**주요 기능**
- 추적 대상 API 수신 및 작업 큐(deque) 기반 처리
- HTTP/TCP 프로빙으로 대상 서버 활성 상태 확인
- Selenium + SOCKS5 프록시를 통한 스크린샷 자동 캡처 및 API 업로드
- 지정 패킷 크기·대기시간 패턴에 따른 추적 패턴 실행
- 익명 네트워크 릴레이 노드 여부 확인 REST API (`/api/v1/check-relay`)
- 결과 데이터 Kafka 전송 + 스크린샷 REST API 전송

**핵심 구현**

| 파일 | 역할 |
|------|------|
| `api_trce_trgt_receiver.py` | 추적 대상 목록 API 수신 |
| `status_checker.py` | HTTP/TCP 프로빙 기반 서버 상태 판별 |
| `screenshot_capture.py` | Selenium WebDriver 스크린샷 캡처 |
| `trace_start.py` | 패킷 패턴 추적 실행 |
| `api_screenshot_sender.py` | 스크린샷 multipart 업로드 |
| `kafka_flfmt_sender.py` | 추적 이행 결과 Kafka 전송 |
| `kafka_status_sender.py` | 서버 상태 변경 이력 Kafka 전송 |
| `api_relay_checker.py` | 릴레이 노드 여부 판별 로직 |
| `api_relay_server.py` | 릴레이 확인용 Flask REST API 서버 |
| `common_state.py` | Kafka Producer 전역 상태 관리 |

**스크린샷 데이터 흐름**
```
추적 대상 API ──► 작업 큐
                     │
          ┌──────────┴──────────┐
          ▼                     ▼
   서버 상태 확인          병렬 실행
          │           ┌─────────┴─────────┐
          ▼           ▼                   ▼
    Kafka 전송   스크린샷 캡처       추적 패턴 실행
                      │                   │
                 API 업로드          Kafka 전송
```

---

### 2. `dwi_sensor/` — 상시 패킷 수집 센서

네트워크 인터페이스(`eth0`)에서 TCP 패킷을 상시 스니핑하여 Kafka로 전송하는 백그라운드 수집 에이전트입니다. 익명 네트워크 환경의 배경 트래픽 전체를 지속적으로 수집합니다.

**주요 기능**
- Scapy 기반 실시간 TCP 패킷 스니핑 (BPF 필터 적용)
- 10,000건/5초 극한 부하 환경에서 tcpdump 대비 수집 일치율 100% 검증
- `Queue(maxsize=20,000)` + 멀티스레딩으로 초당 2,000개 이상 패킷 무손실 처리
- 5초 배치 단위 Kafka 전송, 3회 재시도
- 큐 오버플로우 시 파일 덤프, Kafka 실패 시 JSON 로컬 백업 → 자동 재전송
- 30초 주기 GC 스레드로 장기 운영 메모리 안정성 확보

**핵심 구현**

| 파일 | 역할 |
|------|------|
| `sniffer_producer.py` | 패킷 캡처, 큐 관리, Kafka 배치 전송 |
| `backup_manager.py` | 오버플로우 파일 덤프, 실패 백업, 자동 재전송 |
| `gc_worker.py` | 주기적 가비지 컬렉션 실행 |
| `ip_utils.py` | 수집기 자신의 IP 조회 |
| `common_state.py` | Kafka Producer 전역 상태 관리 |

**패킷 데이터 구조**
```json
{
  "collector_ip": "수집기 IP",
  "timestamp": "2025-10-15 11:32:50.123456",
  "data": [
    {
      "ipAddr": "수집기 IP",
      "dpplIpAddr": "발신 IP",
      "dpplPortNo": 54321,
      "destIpAddr": "목적지 IP",
      "destPortNo": 443,
      "clctDt": "2025-10-15 11:32:45.123456",
      "pcktSize": 1024
    }
  ]
}
```

**스레드 구조**
```
Main Thread       kafka_batch_sender     gc_worker    backup_file_sender
(scapy sniff)     (5초 배치 전송)        (30초 GC)    (60초 재전송)
     │                   │                  │               │
     ▼                   ▼                  ▼               ▼
packet_handler  ──► packet_queue ──► Kafka Cluster    backup/*.json
                         │
                    [오버플로우]
                         │
                    backup 파일 덤프
```

---

### 3. `sensor-bot/` — 수요 기반 패킷 수집 에이전트

Bot의 추적 요청과 연동되어 특정 대상에 대한 패킷만 수집하는 에이전트입니다. DWI Sensor와 동일한 수집 엔진을 공유하되, 별도 Kafka 토픽으로 분리하여 수요 기반 패킷 이력을 독립적으로 관리합니다.

**주요 기능**
- DWI Sensor와 동일한 Scapy 수집 엔진 사용
- 수요 기반 패킷만 별도 Kafka 토픽(`DMND_BOT_PCKT_HST`)으로 분리 저장
- Bot의 추적 패턴 실행 시점과 패킷 수집 시점 타임스탬프 매칭 가능
- 동일한 이중 백업 구조(큐 오버플로우 파일 덤프 + Kafka 실패 JSON 백업)

**DWI Sensor와의 차이**

| 항목 | DWI Sensor | Sensor-Bot |
|------|------------|------------|
| 수집 범위 | 익명 네트워크 배경 트래픽 전체 | 추적 요청 연동 패킷 |
| Kafka 토픽 | `TOR_NETWK_PCKT_HST` | `DMND_BOT_PCKT_HST` |
| 빌드 파일명 | `dwi.sensor` | `dwi.sensor.bot` |
| 운영 방식 | 상시 백그라운드 | 추적 요청 시 활성 |

---

## Data Flow (통합)

```
외부 API
  │
  ▼
Bot (추적 명령 수신)
  │
  ├── 서버 상태 확인 ──────────────► Kafka: SRVR_STTS_CHG_HST
  │
  ├── 스크린샷 캡처 ───────────────► REST API (이미지 업로드)
  │
  └── 추적 패턴 실행 ─────────────► Kafka: TRCE_FLFMT_HST
            │
            │ (동시 수집)
            ▼
       Sensor-Bot ─────────────────► Kafka: DMND_BOT_PCKT_HST

DWI Sensor (상시 동작)
  │
  └── 배경 트래픽 수집 ───────────► Kafka: TOR_NETWK_PCKT_HST
```

---

## Tech Stack

| 분류 | 기술 |
|------|------|
| Language | Python 3.12 |
| Packet Capture | Scapy 2.5.0 |
| Message Queue | Apache Kafka (KRaft, kafka-python 2.2.15) |
| Browser Automation | Selenium 4.15.2 + ChromeDriver |
| Relay Detection | stem 1.8.1 |
| API Server | Flask 3.0.0 |
| Process Management | psutil 5.9.6 |
| Build | PyInstaller |

---

## Key Design Decisions

**무손실 패킷 수집 이중 보호**
- 1차: `Queue(maxsize=20,000)` 인메모리 버퍼
- 2차: 큐 오버플로우 시 파일 덤프 (`overflow_*.json`)
- 3차: Kafka 전송 실패 시 JSON 백업 + 60초 주기 자동 재전송

**Kafka KRaft 도입**
- ZooKeeper 의존성 제거, 3-브로커 클러스터로 고가용성 확보
- 토픽별 파티션 수를 부하 기반으로 최적화하여 처리 효율 향상

**멀티스레딩 설계**
- 메인 스레드: scapy 스니핑 (블로킹)
- 배치 전송 스레드: 5초 주기 Kafka flush
- GC 스레드: 30초 주기 메모리 정리
- 백업 복구 스레드: 60초 주기 재전송

**Bot 병렬 처리**
- 스크린샷 캡처 + 추적 패턴 실행을 `ThreadPoolExecutor`로 동시 실행
- 서버 상태 확인 완료 후 병렬 작업 시작으로 의존성 보장
