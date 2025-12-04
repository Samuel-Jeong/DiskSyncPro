# DiskSyncPro  
Mac용 안전한 디스크 백업/동기화 스크립트  
(Carbon Copy Cloner + SuperDuper + Time Machine 컨셉)

---

## 소개

이 프로젝트는 **외장 SSD → 외장 SSD**, 혹은 **폴더 → 폴더** 형태로  
데이터를 안전하게 백업하고 동기화하는 Python 기반 도구입니다.

Time Machine처럼 **변경분만 증분 백업**하고,  
Carbon Copy Cloner·SuperDuper처럼 **스냅샷 느낌의 안전 복사 + Smart Update**를 구현했습니다.

백업 중 오류가 발생하더라도:

- 자동 롤백
- 파일 단위 보호
- SafetyNet 폴더 보존
- 실패한 파일만 스킵하고 나머지는 계속 진행
- 중간에 끊겨도 `--resume` 으로 이어서 재시도

가 되도록 설계되어 있어, 대용량 외장 SSD 백업을 비교적 안전하게 수행할 수 있습니다.

---

## 주요 기능

| 기능 | 설명 |
|------|------|
| Smart Update | 변경된 파일만 복사하여 빠르고 효율적인 백업 (Time Machine 느낌) |
| SafetyNet | 소스에 없는 파일을 삭제 대신 `.SafetyNet/YYYY-MM-DD/`로 이동 |
| Clone 모드 | 대상 폴더를 소스와 완전히 동일하게 미러링 (不要 파일 삭제) |
| Sync 모드 | 추가/변경만 반영, 삭제는 하지 않는 안전 동기화 |
| Rollback | 실패 시 자동/수동 되돌리기 지원 (저널 기반) |
| Atomic Copy | 임시 파일에 복사 후 `os.replace`로 교체해 파일 단위 무결성 보장 |
| 재시도 시스템 | 복사 실패 시 최대 N회까지 자동 재시도 후, 안 되면 그 파일만 스킵 |
| Dry-run 지원 | 실제로 실행하기 전, 어떤 작업이 수행될지 시뮬레이션 |
| Verify (옵션) | 파일 복사 후 해시(sha256) 비교로 무결성 검증 |
| Multi-job 지원 | 하나의 설정 파일로 여러 백업 작업을 정의하고 실행 |
| Snapshot 인덱스 | 백업 완료 시 대상 경로 전체 스냅샷(JSON) 및 인덱스(`index.json`) 생성 |
| Resume Checkpoint | 중간에 끊겨도 `--resume` 옵션으로 이어서 백업 재개 |
| 변경 요약 리포트 | Job별 변경 내역 통계를 `summary_*.json`으로 저장 |
| 진행률 표시 | 전체 대상 파일 기준 0–100% 진행률을 로그로 출력 |

---

## 요구사항

- macOS / Linux 환경
- Python **3.9 이상**
- 표준 라이브러리만 사용 (추가 패키지 설치 불필요, `requirements.txt` 없음)

---

## 설치

```bash
git clone <your-repo-url>
cd DiskSyncPro
