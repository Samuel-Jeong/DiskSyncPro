# SmartBackup Pro  
Mac용 안전한 디스크 백업/동기화 스크립트  
(Carbon Copy Cloner + SuperDuper 컨셉)

---

## 소개

이 프로젝트는 **외장 SSD → 외장 SSD**, 혹은 **폴더 → 폴더** 형태로  
데이터를 안전하게 백업하고 동기화하는 목적의 Python 기반 도구입니다.

Time Machine처럼 자동화할 수도 있고,  
Carbon Copy Cloner·SuperDuper처럼 **스냅샷 느낌의 안전 복사와 Smart Update**를 구현했습니다.

백업 중 오류가 발생하더라도:

- 자동 롤백
- 파일 단위 보호
- SafetyNet 폴더 보존

이 작동하기 때문에 데이터 손상을 최소화합니다.

---

## 주요 기능

| 기능 | 설명 |
|------|------|
| Smart Update | 변경된 파일만 복사하여 빠르고 효율적인 백업 |
| SafetyNet | 삭제되는 파일을 `.SafetyNet/YYYY-MM-DD/`에 보관 |
| Clone 모드 | 대상 폴더를 소스와 완전히 동일하게 미러링 |
| Rollback | 실패 시 자동/수동 되돌리기 지원 |
| Atomic Copy | 임시 파일에 복사 후 교체하여 데이터 안정성 보장 |
| 재시도 시스템 | 복사 실패 시 자동 재시도 |
| Dry-run 지원 | 실제로 실행하기 전 어떤 작업이 수행될지 테스트 |
| Verify (옵션) | 파일 복사 후 해시 비교로 무결성 검증 |
| Multi-job 지원 | 하나의 설정 파일로 여러 백업 작업 실행 |

---

## 요구사항

- macOS / Linux 환경
- Python **3.9 이상**
- 추가 패키지 설치 불필요 (`requirements.txt 없음`)

---

## 설치

```bash
git clone <your-repo-url>
cd SmartBackup
```

원한다면 가상환경을 구성할 수 있습니다:

```bash
python3 -m venv venv
source venv/bin/activate
```

---

## 설정 파일 (`backup_config.json` 예시)

```json
{
  "jobs": [
    {
      "name": "ExtSSD1-to-ExtSSD2-SafetyNet",
      "source": "/Volumes/ExtSSD1",
      "destination": "/Volumes/ExtSSD2/Backup_ExtSSD1",
      "mode": "safety_net",
      "exclude": [
        ".DS_Store",
        ".Spotlight-V100",
        ".fseventsd",
        "node_modules",
        "*.tmp"
      ],
      "safety_net_days": 30,
      "verify": false
    }
  ]
}
```

---

## 사용 방법

### 1) 시뮬레이션(Dry-run)

```bash
python3 pro_smart_backup.py backup -c backup_config.json --dry-run
```

### 2) 실제 백업 실행

```bash
python3 pro_smart_backup.py backup -c backup_config.json
```

특정 Job만 실행하고 싶다면:

```bash
python3 pro_smart_backup.py backup -c backup_config.json -j ExtSSD1-to-ExtSSD2-SafetyNet
```

---

## 롤백

실행 중 오류가 발생하면 자동으로 롤백되지만,  
원하면 특정 시점으로 수동으로 되돌릴 수도 있습니다.

```bash
python3 pro_smart_backup.py rollback -f logs/journal_ExampleJob_20250101_120000.json
```

dry-run 롤백 시뮬레이션:

```bash
python3 pro_smart_backup.py rollback -f journal.json --dry-run
```

---

## 폴더 구조 예시

```
SmartBackup/
 ├── pro_smart_backup.py
 ├── backup_config.json
 ├── logs/
 │    ├── pro_smart_backup_20250101.log
 │    └── journal_ExampleJob_20250101.json
 ├── .gitignore
 ├── README.md
 └── venv/ (옵션)
```

---

## 자동 실행 (macOS launchd)

```bash
launchctl load ~/Library/LaunchAgents/com.smartbackup.auto.plist
```

---

## 앞으로의 계획 (Roadmap)

- tqdm 기반 진행률 표시  
- Slack/Discord 알림 연동  
- 고급 스케줄링 및 UI 제공 (Electron 기반 고려)  
- 멀티스레드 파일 복사 성능 강화  

---

## 라이선스

MIT License
