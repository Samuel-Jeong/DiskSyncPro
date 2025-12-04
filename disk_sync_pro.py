#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
disk_sync_pro.py

CCC + SuperDuper 스타일을 참고한, 비교적 안전한 디스크 백업/동기화 스크립트.

포함 기능:
- JSON 설정 파일 기반 다중 Job 관리
- 모드:
  - clone      : 대상 폴더를 소스와 동일하게 미러링 (불필요한 파일 삭제)
  - sync       : 추가/변경만 반영 (삭제는 하지 않음)
  - safety_net : 삭제/덮어쓰기 파일을 .SafetyNet/YYYY-MM-DD/ 로 이동
- 변경된 파일만 복사 (Smart Update 느낌)
- 해시 검증 옵션 (verify)
- dry-run 지원
- 원자적 복사 (임시 파일 → os.replace)
- 롤백 저널:
  - Job 실행 동안 이루어진 변경을 모두 기록
  - 에러 발생 시 자동 롤백 시도
  - 나중에 --rollback 으로 수동 롤백 가능
- 복사 실패 파일은 스킵 (전체 Job은 계속 진행)
- 스냅샷 인덱싱 (snapshot + index.json)
- resume checkpoint (--resume 사용 시, 중단된 지점 이후부터 이어서 실행)
- 변경 요약 리포트(summary_*.json) 생성
- 진행률 로그 (0~100%) 지원
- 멀티스레드 파일 복사 (Queue + Worker Threads)
- curses 기반 TUI:
  - 상단: Job 메타 정보
  - 중단: 진행률 바 + 퍼센트
  - 하단: 로그 스트림 (top 스타일)
  - 백업 중 q 키로 취소 가능
- logs 폴더 기준 메인 메뉴:
  - config 선택 → 백업 실행
  - 최신 저널 롤백
  - journal 목록 보기
  - snapshot 목록 보기

사용 방법:
- 그냥 메뉴로 쓸 때:   python3 disk_sync_pro.py
- 기존 CLI 그대로:     python3 disk_sync_pro.py backup -c config.json ...
"""

import argparse
import hashlib
import json
import logging
import os
import shutil
import sys
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import List, Optional
from threading import Thread, Lock, Event
from queue import Queue, Empty
from types import SimpleNamespace

import locale

locale.setlocale(locale.LC_ALL, "")

try:
    import curses
except ImportError:
    curses = None  # 윈도우/미지원 환경 대비


# ================ 전역 설정 =================

MAX_COPY_RETRY = 3
HASH_ALGO = "sha256"
QUEUE_MAXSIZE = 10000  # 작업 큐 최대 크기 (메모리 폭주 방지)


# ================ 로깅 설정 =================

def setup_logger(log_file: Optional[Path] = None,
                 verbose: bool = True,
                 use_tui: bool = False,
                 tui_obj: "SimpleTUI" = None) -> None:
    logger = logging.getLogger("disk_sync_pro")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    # 일반 stdout 로그 핸들러 (TUI 아닐 때만)
    if verbose and not use_tui:
        ch = logging.StreamHandler(sys.stdout)
        ch.setFormatter(fmt)
        logger.addHandler(ch)

    # 파일 로그 핸들러
    if log_file:
        fh = logging.FileHandler(log_file, encoding="utf-8")
        fh.setFormatter(fmt)
        logger.addHandler(fh)

    # TUI 로그 핸들러
    if use_tui and tui_obj is not None:
        handler = CursesLogHandler(tui_obj)
        handler.setFormatter(fmt)
        logger.addHandler(handler)


logger = logging.getLogger("disk_sync_pro")


# ================ 데이터 클래스 =================

@dataclass
class BackupJob:
    name: str
    source: Path
    destination: Path
    mode: str              # "clone" | "sync" | "safety_net"
    exclude: List[str]
    safety_net_days: int = 30
    verify: bool = False


@dataclass
class JournalOp:
    """
    롤백을 위한 단일 작업 기록
    action:
      - create_file  : 새 파일 생성
      - replace_file : 기존 파일 백업 후 새 파일로 교체
      - delete_file  : 기존 파일 삭제(또는 백업 위치로 이동)
      - create_dir   : 새 디렉토리 생성
    target: 최종 대상 경로
    backup: 백업용으로 옮겨둔 경로 (없는 경우 None)
    """
    action: str
    target: str
    backup: Optional[str] = None


@dataclass
class Journal:
    job_name: str
    timestamp: str
    dest_root: str
    rollback_root: str
    status: str                    # "pending" | "success" | "cancelled" | "rolled_back" | "rollback_failed"
    ops: List[JournalOp]


@dataclass
class Stats:
    created_files: int = 0
    replaced_files: int = 0
    deleted_files: int = 0
    safetynet_files: int = 0
    created_dirs: int = 0
    skipped_same: int = 0
    skipped_excluded: int = 0
    copy_failed: int = 0


@dataclass
class StageProgress:
    """
    AWS Step Function 스타일 Stage 진행 상태
    """
    stage_name: str
    status: str  # "pending" | "running" | "completed" | "failed"
    start_time: Optional[str] = None
    end_time: Optional[str] = None
    items_total: int = 0
    items_processed: int = 0
    error: Optional[str] = None
    
    def to_dict(self):
        return {
            "stage": self.stage_name,
            "status": self.status,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "progress": f"{self.items_processed}/{self.items_total}",
            "error": self.error
        }


# ================ TUI 구현 (Professional Version) =================

class SimpleTUI:
    """
    전문가급 TUI 구현:
    - 헤더: Job 정보
    - 진행률 바
    - 로그 영역 (스크롤 가능)
    - 상태바: 키 가이드
    - 색상 지원
    - 화면 크기 변경 자동 대응
    - 안전한 유니코드/한글 처리
    """
    
    # 레이아웃 상수
    HEADER_HEIGHT = 5
    PROGRESS_HEIGHT = 2
    STATUSBAR_HEIGHT = 1
    LOG_SEPARATOR_HEIGHT = 1
    
    def __init__(self, stdscr):
        self.stdscr = stdscr
        self.lock = Lock()
        self.log_lines: List[str] = []
        self.job_meta = {}
        self.progress = {
            "percent": 0,
            "current": 0,
            "total": 0,
        }
        self.dirty = True
        self.log_scroll_offset = 0  # 로그 스크롤 위치
        self.use_colors = False
        
        self._init_screen()
        self._init_colors()

    def _init_screen(self):
        """화면 초기화"""
        try:
            self.stdscr.clear()
            self.stdscr.nodelay(True)  # non-blocking 입력
            self.stdscr.keypad(True)
            curses.curs_set(0)  # 커서 숨김
        except Exception:
            pass

    def _init_colors(self):
        """색상 초기화"""
        try:
            if curses.has_colors():
                curses.start_color()
                curses.use_default_colors()
                
                # 색상 페어 정의
                curses.init_pair(1, curses.COLOR_CYAN, -1)     # 제목
                curses.init_pair(2, curses.COLOR_GREEN, -1)    # 진행률
                curses.init_pair(3, curses.COLOR_YELLOW, -1)   # 경고
                curses.init_pair(4, curses.COLOR_RED, -1)      # 에러
                curses.init_pair(5, curses.COLOR_WHITE, curses.COLOR_BLUE)  # 상태바
                
                self.use_colors = True
        except Exception:
            self.use_colors = False

    def _get_screen_size(self):
        """안전한 화면 크기 가져오기"""
        try:
            rows, cols = self.stdscr.getmaxyx()
            return max(10, rows), max(40, cols)
        except Exception:
            return 24, 80

    def _safe_addstr(self, row: int, col: int, text: str, attr=0):
        """안전한 문자열 출력 - 유니코드/한글 지원"""
        try:
            rows, cols = self._get_screen_size()
            if row >= rows or col >= cols:
                return
            
            # 문자열을 바이트 단위로 잘라서 안전하게 출력
            available_width = cols - col - 1
            if available_width <= 0:
                return
            
            # 한글 등 멀티바이트 문자 고려
            safe_text = text[:available_width]
            
            # 길이가 넘으면 점진적으로 줄임
            while len(safe_text) > 0:
                try:
                    self.stdscr.addstr(row, col, safe_text, attr)
                    break
                except Exception:
                    safe_text = safe_text[:-1]
        except Exception:
            pass

    def _draw_separator(self, row: int, char: str = "─"):
        """구분선 그리기"""
        try:
            rows, cols = self._get_screen_size()
            if row >= rows:
                return
            self.stdscr.move(row, 0)
            self.stdscr.clrtoeol()
            self._safe_addstr(row, 0, char * cols)
        except Exception:
            pass

    # ===== 상태 업데이트 (멀티스레드 안전) =====
    def set_job_meta(self, **meta):
        with self.lock:
            self.job_meta = meta
            self.dirty = True

    def update_progress(self, percent: int, current: int, total: int):
        with self.lock:
            self.progress["percent"] = min(100, max(0, percent))
            self.progress["current"] = current
            self.progress["total"] = total
            self.dirty = True

    def add_log_line(self, text: str):
        with self.lock:
            # 타임스탬프 제거하고 메시지만 저장 (깔끔한 표시)
            msg = text
            if '] ' in text:
                parts = text.split('] ', 1)
                if len(parts) == 2:
                    msg = parts[1]
            
            self.log_lines.append(msg)
            
            # 메모리 관리
            if len(self.log_lines) > 5000:
                self.log_lines = self.log_lines[-2000:]
            
            self.dirty = True

    # ===== 화면 그리기 (메인 스레드 전용) =====
    def refresh_if_dirty(self):
        """변경사항이 있을 때만 화면 갱신"""
        with self.lock:
            if not self.dirty:
                return
            
            try:
                self._draw_all()
                self.stdscr.refresh()
                self.dirty = False
            except Exception:
                # 화면 크기 변경 등의 이유로 실패하면 재초기화 시도
                try:
                    self.stdscr.clear()
                    self._draw_all()
                    self.stdscr.refresh()
                    self.dirty = False
                except Exception:
                    pass

    def _draw_all(self):
        """전체 화면 그리기"""
        rows, cols = self._get_screen_size()
        
        # 화면 초기화
        try:
            self.stdscr.erase()
        except Exception:
            pass
        
        # 1. 헤더 (Job 정보)
        self._draw_header()
        
        # 2. 구분선
        self._draw_separator(self.HEADER_HEIGHT)
        
        # 3. 진행률
        progress_start = self.HEADER_HEIGHT + 1
        self._draw_progress(progress_start)
        
        # 4. 로그 영역
        log_start = progress_start + self.PROGRESS_HEIGHT + self.LOG_SEPARATOR_HEIGHT
        self._draw_separator(log_start - 1)
        self._draw_logs(log_start)
        
        # 5. 상태바
        self._draw_statusbar()

    def _draw_header(self):
        """헤더 영역 그리기"""
        rows, cols = self._get_screen_size()
        
        # 제목
        title = "DiskSyncPro - Professional Backup System"
        title_attr = curses.color_pair(1) | curses.A_BOLD if self.use_colors else curses.A_BOLD
        self._safe_addstr(0, (cols - len(title)) // 2, title, title_attr)
        
        # Job 정보
        job_name = self.job_meta.get('job_name', 'N/A')
        source = self.job_meta.get('source', 'N/A')
        destination = self.job_meta.get('destination', 'N/A')
        mode = self.job_meta.get('mode', 'N/A')
        
        self._safe_addstr(1, 2, f"Job: {job_name}")
        self._safe_addstr(2, 2, f"Src: {source}")
        self._safe_addstr(3, 2, f"Dst: {destination}")
        
        # 옵션 정보
        verify = "ON" if self.job_meta.get('verify', False) else "OFF"
        threads = self.job_meta.get('threads', 'N/A')
        resume = "ON" if self.job_meta.get('resume', False) else "OFF"
        
        info = f"Mode: {mode} | Verify: {verify} | Threads: {threads} | Resume: {resume}"
        self._safe_addstr(4, 2, info)

    def _draw_progress(self, start_row: int):
        """진행률 바 그리기"""
        rows, cols = self._get_screen_size()
        
        percent = self.progress["percent"]
        current = self.progress["current"]
        total = self.progress["total"]
        
        # 진행률 바
        bar_width = max(20, cols - 30)
        filled = int(bar_width * percent / 100)
        bar = "█" * filled + "░" * (bar_width - filled)
        
        bar_attr = curses.color_pair(2) if self.use_colors else 0
        self._safe_addstr(start_row, 2, f"Progress: [{bar}] {percent}%", bar_attr)
        
        # 파일 수 정보
        if total > 0:
            speed_info = f"Files: {current:,} / {total:,}"
        else:
            speed_info = f"Files: {current:,}"
        
        self._safe_addstr(start_row + 1, 2, speed_info)

    def _draw_logs(self, start_row: int):
        """로그 영역 그리기"""
        rows, cols = self._get_screen_size()
        
        # 로그 영역 높이 계산
        log_height = rows - start_row - self.STATUSBAR_HEIGHT
        if log_height <= 0:
            return
        
        # 표시할 로그 라인 선택
        total_logs = len(self.log_lines)
        if total_logs == 0:
            self._safe_addstr(start_row, 2, "Waiting for log messages...")
            return
        
        # 스크롤 오프셋 조정
        max_offset = max(0, total_logs - log_height)
        self.log_scroll_offset = min(self.log_scroll_offset, max_offset)
        
        # 최신 로그가 항상 보이도록 (자동 스크롤)
        if self.log_scroll_offset == 0 or total_logs <= log_height:
            # 최신 로그 표시
            start_idx = max(0, total_logs - log_height)
            lines_to_show = self.log_lines[start_idx:]
        else:
            # 스크롤된 위치의 로그 표시
            lines_to_show = self.log_lines[self.log_scroll_offset:self.log_scroll_offset + log_height]
        
        # 로그 출력
        for idx, line in enumerate(lines_to_show):
            row = start_row + idx
            if row >= rows - self.STATUSBAR_HEIGHT:
                break
            
            # 로그 레벨에 따른 색상 적용
            attr = 0
            if self.use_colors:
                if 'ERROR' in line or 'FAIL' in line or '실패' in line:
                    attr = curses.color_pair(4)
                elif 'WARN' in line or '경고' in line:
                    attr = curses.color_pair(3)
                elif 'SUCCESS' in line or '성공' in line or '완료' in line:
                    attr = curses.color_pair(2)
            
            self._safe_addstr(row, 2, line, attr)

    def _draw_statusbar(self):
        """하단 상태바 그리기"""
        rows, cols = self._get_screen_size()
        statusbar_row = rows - 1
        
        status_text = " [Q] Cancel  [↑↓] Scroll Logs  [R] Refresh "
        status_attr = curses.color_pair(5) if self.use_colors else curses.A_REVERSE
        
        # 상태바 전체를 색상으로 채우기
        try:
            self.stdscr.move(statusbar_row, 0)
            self.stdscr.clrtoeol()
            
            # 가운데 정렬
            padding = (cols - len(status_text)) // 2
            full_text = " " * padding + status_text + " " * (cols - padding - len(status_text))
            self._safe_addstr(statusbar_row, 0, full_text[:cols], status_attr)
        except Exception:
            pass

    def check_cancel_key(self) -> bool:
        """키 입력 체크 (취소, 스크롤 등)"""
        try:
            ch = self.stdscr.getch()
            if ch == -1:
                return False
            
            if ch in (ord('q'), ord('Q')):
                self.add_log_line("[USER] 취소 요청 - 백업을 중단합니다...")
                return True
            elif ch == curses.KEY_UP:
                # 로그 스크롤 업
                with self.lock:
                    self.log_scroll_offset = max(0, self.log_scroll_offset - 1)
                    self.dirty = True
            elif ch == curses.KEY_DOWN:
                # 로그 스크롤 다운
                with self.lock:
                    rows, cols = self._get_screen_size()
                    log_height = rows - (self.HEADER_HEIGHT + 1 + self.PROGRESS_HEIGHT + self.LOG_SEPARATOR_HEIGHT + self.STATUSBAR_HEIGHT)
                    max_offset = max(0, len(self.log_lines) - log_height)
                    self.log_scroll_offset = min(self.log_scroll_offset + 1, max_offset)
                    self.dirty = True
            elif ch in (ord('r'), ord('R')):
                # 강제 새로고침
                with self.lock:
                    self.dirty = True
            
            return False
        except Exception:
            return False

    def redraw_all(self):
        """전체 화면 강제 다시 그리기"""
        with self.lock:
            self.dirty = True
        self.refresh_if_dirty()


class CursesLogHandler(logging.Handler):
    """
    logging → TUI 로그창으로 보내는 핸들러
    (여기서는 curses 호출 없이 상태만 업데이트)
    """
    def __init__(self, tui: SimpleTUI):
        super().__init__()
        self.tui = tui

    def emit(self, record):
        try:
            msg = self.format(record)
            self.tui.add_log_line(msg)
        except Exception:
            pass


# ================ 설정 로딩 =================

def load_config(config_path: Path) -> List[BackupJob]:
    """
    JSON 설정 파일을 읽어 BackupJob 리스트 생성
    """
    with config_path.open("r", encoding="utf-8") as f:
        raw = json.load(f)

    jobs: List[BackupJob] = []
    for job in raw.get("jobs", []):
        jobs.append(
            BackupJob(
                name=job["name"],
                source=Path(job["source"]).expanduser(),
                destination=Path(job["destination"]).expanduser(),
                mode=job.get("mode", "safety_net"),
                exclude=job.get("exclude", []),
                safety_net_days=job.get("safety_net_days", 30),
                verify=job.get("verify", False),
            )
        )
    return jobs


# ================ 유틸 =================

def path_matches_patterns(path: Path, patterns: List[str]) -> bool:
    """
    제외 패턴 처리:
    - 단순 이름
    - glob 패턴 (*.tmp, *.log 등)
    """
    name = path.name
    for pattern in patterns:
        if pattern == name:
            return True
        if path.match(pattern) or name == pattern:
            return True
    return False


def file_hash(path: Path, algo: str = HASH_ALGO, chunk_size: int = 1024 * 1024) -> str:
    """파일 해시 계산 (검증용)"""
    h = hashlib.new(algo)
    with path.open("rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def is_same_file(src: Path, dst: Path) -> bool:
    """
    성능 우선: 파일 크기 + mtime 으로 동일 여부 판단
    """
    if not dst.exists():
        return False
    s_stat = src.stat()
    d_stat = dst.stat()
    return (s_stat.st_size == d_stat.st_size) and (int(s_stat.st_mtime) == int(d_stat.st_mtime))


def atomic_copy(src: Path, dst: Path) -> None:
    """
    임시 파일에 복사 후 os.replace 로 교체하는 원자적(atomic) 복사.
    """
    dst_parent = dst.parent
    dst_parent.mkdir(parents=True, exist_ok=True)
    tmp_name = f".{dst.name}.sbk_tmp_{os.getpid()}"
    tmp_path = dst_parent / tmp_name

    try:
        if tmp_path.exists():
            tmp_path.unlink()
    except Exception:
        pass

    shutil.copy2(src, tmp_path)
    os.replace(tmp_path, dst)


def ensure_dir(path: Path, journal: Optional[Journal] = None,
               stats: Optional[Stats] = None, dry_run: bool = False) -> None:
    """
    디렉토리 생성. 롤백을 위해 create_dir 기록.
    """
    if path.exists():
        return
    logger.info(f"[MKDIR] {path}")
    if dry_run:
        return
    path.mkdir(parents=True, exist_ok=True)
    if journal:
        journal.ops.append(JournalOp(action="create_dir", target=str(path)))
    if stats:
        stats.created_dirs += 1


# ================ SafetyNet / Rollback 영역 =================

def get_safety_net_dir(destination_root: Path) -> Path:
    today = datetime.now().strftime("%Y-%m-%d")
    sn_root = destination_root / ".SafetyNet" / today
    sn_root.mkdir(parents=True, exist_ok=True)
    return sn_root


def move_to_safety_net(target: Path, dest_root: Path, dry_run: bool = False) -> Path:
    """
    삭제/덮어쓰기 대상 파일을 SafetyNet으로 이동
    """
    sn_root = get_safety_net_dir(dest_root)
    try:
        rel = target.relative_to(dest_root)
    except ValueError:
        rel = Path(target.name)

    sn_path = sn_root / rel
    logger.info(f"[SafetyNet] {target} -> {sn_path}")
    if not dry_run:
        sn_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(target), str(sn_path))
    return sn_path


def prepare_journal(job: BackupJob) -> Journal:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    rollback_root = job.destination / ".Rollback" / f"{job.name}_{ts}"
    rollback_root.mkdir(parents=True, exist_ok=True)
    return Journal(
        job_name=job.name,
        timestamp=ts,
        dest_root=str(job.destination),
        rollback_root=str(rollback_root),
        status="pending",
        ops=[],
    )


def journal_path_for(job: BackupJob, log_dir: Path, ts: str) -> Path:
    return log_dir / f"journal_{job.name}_{ts}.json"


def get_dest_meta_dir(destination_root: Path) -> Path:
    """타겟 디스크의 메타데이터 저장 디렉토리"""
    meta_dir = destination_root / ".DiskSyncPro"
    meta_dir.mkdir(parents=True, exist_ok=True)
    return meta_dir


def save_journal(journal: Journal, path: Path, destination_root: Optional[Path] = None) -> None:
    """저널을 logs 폴더와 타겟 폴더 모두에 저장"""
    serializable = {
        "job_name": journal.job_name,
        "timestamp": journal.timestamp,
        "dest_root": journal.dest_root,
        "rollback_root": journal.rollback_root,
        "status": journal.status,
        "ops": [asdict(op) for op in journal.ops],
    }
    # logs 폴더에 저장
    with path.open("w", encoding="utf-8") as f:
        json.dump(serializable, f, indent=2, ensure_ascii=False)
    
    # 타겟 폴더에도 저장
    if destination_root and destination_root.exists():
        try:
            dest_meta_dir = get_dest_meta_dir(destination_root)
            dest_journal_path = dest_meta_dir / path.name
            with dest_journal_path.open("w", encoding="utf-8") as f:
                json.dump(serializable, f, indent=2, ensure_ascii=False)
            logger.info(f"저널 복사본 저장: {dest_journal_path}")
        except Exception as e:
            logger.warning(f"타겟 폴더 저널 저장 실패: {e}")


def load_journal(path: Path) -> Journal:
    with path.open("r", encoding="utf-8") as f:
        raw = json.load(f)
    ops = [JournalOp(**op) for op in raw.get("ops", [])]
    return Journal(
        job_name=raw["job_name"],
        timestamp=raw["timestamp"],
        dest_root=raw["dest_root"],
        rollback_root=raw["rollback_root"],
        status=raw.get("status", "pending"),
        ops=ops,
    )


# ================ Checkpoint (resume) =================

def load_or_init_checkpoint(job: BackupJob, log_dir: Path) -> dict:
    """
    checkpoint_<job>.json 파일을 읽어오거나 새로 생성.
    status != 'incomplete' 인 경우 processed 는 초기화.
    
    개선: 디렉토리 단위 체크포인트 추가
    """
    path = log_dir / f"checkpoint_{job.name}.json"
    if path.exists():
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        status = data.get("status", "incomplete")
        if status == "incomplete":
            processed = set(data.get("processed_files", []))
            processed_dirs = set(data.get("processed_dirs", []))  # 완료된 디렉토리
        else:
            processed = set()
            processed_dirs = set()
    else:
        status = "incomplete"
        processed = set()
        processed_dirs = set()
    
    cp = {
        "job_name": job.name,
        "status": status,
        "processed": processed,
        "processed_dirs": processed_dirs,  # 디렉토리 단위 추적
        "path": path,
    }
    return cp


def save_checkpoint(cp: dict) -> None:
    """
    체크포인트 저장 (디렉토리 단위 추적 포함)
    """
    if cp is None:
        return
    path: Path = cp["path"]
    data = {
        "job_name": cp["job_name"],
        "status": cp["status"],
        "processed_files": sorted(list(cp["processed"]))[:1000],  # 최근 1000개만 저장 (메모리 절약)
        "processed_dirs": sorted(list(cp.get("processed_dirs", set()))),  # 완료된 디렉토리
        "last_updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "total_processed": len(cp["processed"]),  # 전체 처리 수
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


# ================ 진행률 계산용 =================

def count_total_files_for_job(job: BackupJob) -> int:
    """
    진행률 계산을 위해 소스 아래 '대상 파일 수'를 미리 샘.
    exclude 패턴에 걸리는 파일은 제외.
    """
    total = 0
    for root, dirs, files in os.walk(job.source):
        root_path = Path(root)
        dirs[:] = [d for d in dirs if not path_matches_patterns(root_path / d, job.exclude)]
        for f in files:
            p = root_path / f
            if path_matches_patterns(p, job.exclude):
                continue
            total += 1
    return total


# ================ 롤백 =================

def rollback_journal(journal: Journal, dry_run: bool = False) -> None:
    """
    Journal 을 역순으로 읽어 롤백 수행.
    """
    logger.info(f"=== 롤백 시작: job={journal.job_name}, ts={journal.timestamp} ===")

    for op in reversed(journal.ops):
        target = Path(op.target)
        backup = Path(op.backup) if op.backup else None

        if op.action == "create_file":
            if target.exists():
                logger.info(f"[ROLLBACK delete created file] {target}")
                if not dry_run:
                    try:
                        target.unlink()
                    except Exception as e:
                        logger.error(f"롤백: 파일 삭제 실패 {target}: {e}")

        elif op.action in ("replace_file", "delete_file"):
            if backup and backup.exists():
                logger.info(f"[ROLLBACK restore] {backup} -> {target}")
                if not dry_run:
                    target.parent.mkdir(parents=True, exist_ok=True)
                    try:
                        if target.exists():
                            target.unlink()
                        shutil.move(str(backup), str(target))
                    except Exception as e:
                        logger.error(f"롤백: 복원 실패 {backup} -> {target}: {e}")

        elif op.action == "create_dir":
            if target.exists() and target.is_dir():
                try:
                    target.rmdir()
                    logger.info(f"[ROLLBACK rmdir] {target}")
                except OSError:
                    pass

    logger.info("=== 롤백 종료 ===")


# ================ 핵심 백업 로직 (멀티스레드 복사) =================

def copy_with_retry(src: Path,
                    dst: Path,
                    verify: bool,
                    journal: Journal,
                    stats: Stats,
                    stats_lock: Lock,
                    journal_lock: Lock,
                    dry_run: bool = False) -> bool:
    """
    원자적 복사 + 재시도 + 해시 검증 + 저널 기록
    실패 시 예외를 올리지 않고 False 를 반환해서
    해당 파일만 스킵하도록 동작.
    멀티스레드 환경에서 호출되므로 stats/journal 업데이트는 락으로 보호.
    """
    action = "replace_file" if dst.exists() else "create_file"
    backup_path = None

    if action == "replace_file" and not dry_run:
        backup_path = Path(journal.rollback_root) / dst.relative_to(Path(journal.dest_root))
        backup_path.parent.mkdir(parents=True, exist_ok=True)
        logger.info(f"[BACKUP(before replace)] {dst} -> {backup_path}")
        try:
            shutil.copy2(dst, backup_path)
        except Exception as e:
            logger.error(f"[BACKUP 실패] {dst} -> {backup_path}: {e}")

    if dry_run:
        logger.info(f"[COPY (dry-run)] {src} -> {dst}")
        with journal_lock:
            journal.ops.append(JournalOp(
                action=action,
                target=str(dst),
                backup=str(backup_path) if backup_path else None
            ))
        with stats_lock:
            if action == "create_file":
                stats.created_files += 1
            else:
                stats.replaced_files += 1
        return True
    else:
        success = False
        for attempt in range(1, MAX_COPY_RETRY + 1):
            try:
                logger.info(f"[COPY] {src} -> {dst} (attempt {attempt})")
                atomic_copy(src, dst)
                if verify:
                    src_hash = file_hash(src)
                    dst_hash = file_hash(dst)
                    if src_hash != dst_hash:
                        raise IOError(f"해시 불일치: {src} != {dst}")
                success = True
                break
            except Exception as e:
                logger.error(f"복사 실패 ({attempt}/{MAX_COPY_RETRY}): {src} -> {dst}: {e}")

        if not success:
            logger.error(f"[SKIP] 최대 재시도 실패로 이 파일은 스킵합니다: {src}")
            with stats_lock:
                stats.copy_failed += 1
            return False

    with journal_lock:
        journal.ops.append(JournalOp(
            action=action,
            target=str(dst),
            backup=str(backup_path) if backup_path else None
        ))
    with stats_lock:
        if action == "create_file":
            stats.created_files += 1
        else:
            stats.replaced_files += 1
    return True


# ================ Snapshot & Summary =================

def build_snapshot(job: BackupJob, journal: Journal, log_dir: Path) -> Path:
    """
    백업 완료 후 destination 전체 스냅샷(manifest) 생성.
    logs 폴더와 타겟 폴더 모두에 저장.
    """
    dest_root = job.destination
    snapshot_dir = log_dir / "snapshots" / job.name
    snapshot_dir.mkdir(parents=True, exist_ok=True)

    files_manifest = []

    for root, dirs, files in os.walk(dest_root):
        root_path = Path(root)
        if any(x in root_path.parts for x in (".Rollback", ".SafetyNet", ".DiskSyncPro")):
            dirs[:] = []
            continue

        for f in files:
            file_path = root_path / f
            rel_path = file_path.relative_to(dest_root).as_posix()
            st = file_path.stat()
            entry = {
                "path": rel_path,
                "size": st.st_size,
                "mtime": int(st.st_mtime),
            }
            if job.verify:
                entry["hash"] = file_hash(file_path)
            files_manifest.append(entry)

    snapshot_data = {
        "job_name": job.name,
        "timestamp": journal.timestamp,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "mode": job.mode,
        "source": str(job.source),
        "destination": str(job.destination),
        "file_count": len(files_manifest),
        "files": files_manifest,
    }

    # logs 폴더에 저장
    snapshot_file = snapshot_dir / f"snapshot_{journal.timestamp}.json"
    with snapshot_file.open("w", encoding="utf-8") as f:
        json.dump(snapshot_data, f, indent=2, ensure_ascii=False)

    index_file = snapshot_dir / "index.json"
    if index_file.exists():
        with index_file.open("r", encoding="utf-8") as f:
            index = json.load(f)
    else:
        index = []

    index.append({
        "timestamp": snapshot_data["timestamp"],
        "snapshot_file": snapshot_file.name,
        "file_count": len(files_manifest),
        "generated_at": snapshot_data["generated_at"],
    })

    with index_file.open("w", encoding="utf-8") as f:
        json.dump(index, f, indent=2, ensure_ascii=False)

    # 타겟 폴더에도 저장
    if dest_root.exists():
        try:
            dest_meta_dir = get_dest_meta_dir(dest_root)
            dest_snapshot_dir = dest_meta_dir / "snapshots"
            dest_snapshot_dir.mkdir(parents=True, exist_ok=True)
            
            dest_snapshot_file = dest_snapshot_dir / f"snapshot_{journal.timestamp}.json"
            with dest_snapshot_file.open("w", encoding="utf-8") as f:
                json.dump(snapshot_data, f, indent=2, ensure_ascii=False)
            
            dest_index_file = dest_snapshot_dir / "index.json"
            if dest_index_file.exists():
                with dest_index_file.open("r", encoding="utf-8") as f:
                    dest_index = json.load(f)
            else:
                dest_index = []
            
            dest_index.append({
                "timestamp": snapshot_data["timestamp"],
                "snapshot_file": dest_snapshot_file.name,
                "file_count": len(files_manifest),
                "generated_at": snapshot_data["generated_at"],
            })
            
            with dest_index_file.open("w", encoding="utf-8") as f:
                json.dump(dest_index, f, indent=2, ensure_ascii=False)
            
            logger.info(f"스냅샷 복사본 저장: {dest_snapshot_file}")
        except Exception as e:
            logger.warning(f"타겟 폴더 스냅샷 저장 실패: {e}")

    return snapshot_file


def write_summary(job: BackupJob, journal: Journal, stats: Stats, log_dir: Path) -> Path:
    """
    변경 요약 리포트 JSON 생성.
    logs 폴더와 타겟 폴더 모두에 저장.
    """
    summary = {
        "job_name": job.name,
        "timestamp": journal.timestamp,
        "mode": job.mode,
        "source": str(job.source),
        "destination": str(job.destination),
        "stats": asdict(stats),
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }

    # logs 폴더에 저장
    summary_file = log_dir / f"summary_{job.name}_{journal.timestamp}.json"
    with summary_file.open("w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)

    # 타겟 폴더에도 저장
    if job.destination.exists():
        try:
            dest_meta_dir = get_dest_meta_dir(job.destination)
            dest_summary_file = dest_meta_dir / f"summary_{journal.timestamp}.json"
            with dest_summary_file.open("w", encoding="utf-8") as f:
                json.dump(summary, f, indent=2, ensure_ascii=False)
            logger.info(f"요약 복사본 저장: {dest_summary_file}")
        except Exception as e:
            logger.warning(f"타겟 폴더 요약 저장 실패: {e}")

    logger.info("=== 변경 요약 ===")
    for k, v in summary["stats"].items():
        logger.info(f"{k}: {v}")
    logger.info("================")

    return summary_file


# ================ Backup 실행 (멀티스레드 + TUI) =================

def perform_backup(job: BackupJob,
                   dry_run: bool,
                   log_dir: Path,
                   resume: bool,
                   tui: Optional[SimpleTUI] = None) -> None:
    logger.info(f"=== Job 시작: {job.name} ===")
    logger.info(f"  Source      : {job.source}")
    logger.info(f"  Destination : {job.destination}")
    logger.info(f"  Mode        : {job.mode}")
    logger.info(f"  Exclude     : {job.exclude}")
    logger.info(f"  Verify      : {job.verify}")
    logger.info(f"  Dry-run     : {dry_run}")
    logger.info(f"  Resume      : {resume}")

    if not job.source.exists():
        logger.error(f"소스 경로가 존재하지 않습니다: {job.source}")
        return

    if job.mode not in ("clone", "sync", "safety_net"):
        logger.error(f"지원하지 않는 모드입니다: {job.mode}")
        return

    if not dry_run:
        job.destination.mkdir(parents=True, exist_ok=True)

    # 멀티스레드 설정 (I/O bound 작업이므로 더 많은 스레드 사용)
    cpu_count = os.cpu_count() or 4
    if total_files > 100000:
        # 대량 파일: CPU * 4
        num_threads = min(64, max(8, cpu_count * 4))
    elif total_files > 10000:
        # 중간 파일: CPU * 3
        num_threads = min(48, max(6, cpu_count * 3))
    else:
        # 소량 파일: CPU * 2
        num_threads = min(32, max(4, cpu_count * 2))
    
    logger.info(f"📊 멀티스레드 설정: CPU={cpu_count}코어, 파일={total_files:,}개 → 스레드={num_threads}개")

    if tui is not None:
        tui.set_job_meta(
            job_name=job.name,
            source=str(job.source),
            destination=str(job.destination),
            mode=job.mode,
            verify=job.verify,
            threads=num_threads,
            resume=resume,
        )
        tui.refresh_if_dirty()

    cancel_event = Event()
    cancelled = False

    total_files = count_total_files_for_job(job)
    if total_files == 0:
        logger.info("처리할 대상 파일이 없습니다. (0개)")
    else:
        logger.info(f"총 처리 대상 파일 수: {total_files}")

    journal = prepare_journal(job)
    journal_file = journal_path_for(job, log_dir, journal.timestamp)
    logger.info(f"저널 파일: {journal_file}")
    save_journal(journal, journal_file, destination_root=job.destination)

    stats = Stats()

    cp = None
    already_processed = 0
    cp_lock = Lock()
    if resume and not dry_run:
        cp = load_or_init_checkpoint(job, log_dir)
        cp["status"] = "incomplete"
        already_processed = len(cp["processed"])
        save_checkpoint(cp)
        if total_files > 0 and already_processed > 0:
            logger.info("=" * 60)
            logger.info(f"🔄 RESUME 모드 활성화")
            logger.info(f"   이미 처리된 파일: {already_processed:,}개")
            logger.info(f"   처리할 파일: {total_files - already_processed:,}개")
            logger.info(f"   진행률: {int(already_processed * 100 / total_files)}%")
            logger.info("=" * 60)

    progress_lock = Lock()
    if total_files > 0:
        if resume and already_processed > 0:
            current_processed = min(already_processed, total_files)
            last_percent = int(current_processed * 100 / total_files)
            logger.info(
                f"[PROGRESS] {job.name}: {last_percent}% "
                f"(resume: {current_processed}/{total_files})"
            )
        else:
            current_processed = 0
            last_percent = 0
            logger.info(f"[PROGRESS] {job.name}: 0% (0/{total_files})")
    else:
        current_processed = 0
        last_percent = 100

    if tui is not None:
        if total_files > 0:
            tui.update_progress(last_percent, current_processed, total_files)
        else:
            tui.update_progress(100, 0, 0)
        tui.refresh_if_dirty()

    # 진행률 보고 최적화
    last_log_time = datetime.now()
    progress_log_interval = 5  # 5초마다 로그 출력
    
    def report_progress():
        nonlocal current_processed, last_percent, last_log_time
        if total_files == 0:
            return
        with progress_lock:
            current_processed += 1
            if current_processed > total_files:
                current_processed = total_files
            percent = int(current_processed * 100 / total_files)
            
            # TUI는 항상 업데이트
            if tui is not None:
                tui.update_progress(percent, current_processed, total_files)
            
            # 로그는 퍼센트 변경 또는 5초 경과 시만 출력
            now = datetime.now()
            time_elapsed = (now - last_log_time).total_seconds()
            
            if percent > last_percent or time_elapsed >= progress_log_interval:
                last_percent = percent
                last_log_time = now
                
                if not tui:
                    # 파일 속도 계산
                    if time_elapsed > 0:
                        speed = (current_processed - (current_processed - 1)) / time_elapsed
                        logger.info(
                            f"[PROGRESS] {job.name}: {percent}% "
                            f"({current_processed:,}/{total_files:,}) "
                            f"[{speed:.1f} files/s]"
                        )

    def add_processed_file_safe(rel_path: str):
        if cp is None:
            return
        with cp_lock:
            if rel_path in cp["processed"]:
                return
            cp["processed"].add(rel_path)
            save_checkpoint(cp)

    # ============ AWS Step Function 스타일 Stage 관리 ============
    stages = {
        "STAGE_1_SCAN": StageProgress("파일 스캔 및 계획", "pending"),
        "STAGE_2_COPY": StageProgress("파일 복사 (멀티스레드)", "pending"),
        "STAGE_3_CLEANUP": StageProgress("정리 및 동기화", "pending"),
        "STAGE_4_SNAPSHOT": StageProgress("스냅샷 생성", "pending"),
    }
    
    def update_stage(stage_key: str, status: str, **kwargs):
        """Stage 상태 업데이트"""
        stage = stages[stage_key]
        stage.status = status
        
        if status == "running" and not stage.start_time:
            stage.start_time = datetime.now().strftime("%H:%M:%S")
        elif status in ("completed", "failed"):
            stage.end_time = datetime.now().strftime("%H:%M:%S")
        
        for key, value in kwargs.items():
            setattr(stage, key, value)
        
        # 로그 출력
        logger.info(f"[STAGE] {stage.stage_name}: {status.upper()}")
        if tui:
            tui.add_log_line(f"[STAGE] {stage.stage_name}: {status.upper()}")
    
    # ============ 멀티스레드 Worker 설정 ============
    stats_lock = Lock()
    journal_lock = Lock()
    task_queue: Queue = Queue(maxsize=QUEUE_MAXSIZE)

    def worker():
        """파일 복사 Worker 스레드"""
        while True:
            if cancel_event.is_set() and task_queue.empty():
                break
            try:
                src_file, dst_file, rel_path = task_queue.get(timeout=0.5)
            except Empty:
                if cancel_event.is_set():
                    break
                continue

            try:
                try:
                    if dst_file.exists() and is_same_file(src_file, dst_file):
                        with stats_lock:
                            stats.skipped_same += 1
                        add_processed_file_safe(rel_path)
                        report_progress()
                        continue
                except Exception as e:
                    logger.error(f"[ERROR] same file check 실패: {src_file} -> {dst_file}: {e}")
                    with stats_lock:
                        stats.copy_failed += 1
                    report_progress()
                    continue

                if cancel_event.is_set():
                    logger.info(f"[CANCELLED] 스킵 (resume 시 재처리됨): {src_file}")
                    # 주의: checkpoint에 추가하지 않음 → resume 시 다시 처리됨
                    report_progress()
                    continue

                ok = copy_with_retry(
                    src_file,
                    dst_file,
                    verify=job.verify,
                    journal=journal,
                    stats=stats,
                    stats_lock=stats_lock,
                    journal_lock=journal_lock,
                    dry_run=dry_run,
                )
                if ok:
                    add_processed_file_safe(rel_path)
            except Exception as e:
                logger.error(f"[WORKER ERROR] {src_file} -> {dst_file}: {e}")
                with stats_lock:
                    stats.copy_failed += 1
            finally:
                report_progress()
                task_queue.task_done()

    # Worker 스레드 시작
    logger.info(f"🔧 멀티스레드 Worker Pool 초기화: {num_threads}개 스레드")
    workers: List[Thread] = []
    for i in range(num_threads):
        t = Thread(target=worker, daemon=True, name=f"Worker-{i+1}")
        t.start()
        workers.append(t)
    logger.info(f"✓ {num_threads}개 Worker 스레드 시작 완료")
    
    try:
        # ========== STAGE 1: 파일 스캔 및 계획 ==========
        update_stage("STAGE_1_SCAN", "running")
        logger.info("=" * 60)
        logger.info("STAGE 1: 파일 스캔 및 작업 계획 수립")
        logger.info("=" * 60)
        
        scan_count = 0
        scan_progress_interval = 10000  # 10,000개마다 진행 상황 출력
        last_scan_report = 0
        skipped_dirs = 0
        
        for root, dirs, files in os.walk(job.source):
            if tui is not None:
                tui.refresh_if_dirty()

            root_path = Path(root)
            
            # Resume 최적화: 완료된 디렉토리는 아예 스캔하지 않음!
            if cp is not None:
                rel_root_str = root_path.relative_to(job.source).as_posix()
                if rel_root_str in cp.get("processed_dirs", set()):
                    skipped_dirs += 1
                    dirs[:] = []  # 하위 디렉토리도 스캔하지 않음
                    logger.debug(f"[RESUME SKIP DIR] {rel_root_str}")
                    continue  # 이 디렉토리 전체 스킵!

            if tui is not None and tui.check_cancel_key():
                cancel_event.set()
                cancelled = True
                logger.info("[CANCEL] 사용자 요청으로 파일 스캔 중단")
                break

            dirs[:] = [d for d in dirs if not path_matches_patterns(root_path / d, job.exclude)]

            rel_root = root_path.relative_to(job.source)
            dest_root = job.destination / rel_root

            ensure_dir(dest_root, journal=journal, stats=stats, dry_run=dry_run)

            # 디렉토리 내 파일 처리
            dir_files_queued = 0
            for file in files:
                scan_count += 1
                
                # 진행 상황 출력 (대량 파일 처리 시)
                if scan_count - last_scan_report >= scan_progress_interval:
                    skip_info = f", {skipped_dirs:,}개 디렉토리 스킵" if skipped_dirs > 0 else ""
                    logger.info(f"[SCAN] 진행 중... {scan_count:,}개 파일 스캔 완료{skip_info}")
                    last_scan_report = scan_count
                
                if tui is not None:
                    tui.refresh_if_dirty()
                    if tui.check_cancel_key():
                        cancel_event.set()
                        cancelled = True
                        logger.info("[CANCEL] 사용자 요청으로 파일 스캔 중단 (inner loop)")
                        break

                src_file = root_path / file
                if path_matches_patterns(src_file, job.exclude):
                    with stats_lock:
                        stats.skipped_excluded += 1
                    continue

                rel_path = src_file.relative_to(job.source).as_posix()

                # Resume: 이미 처리된 파일은 건너뜀
                if cp is not None and rel_path in cp["processed"]:
                    # 이미 처리된 파일은 큐에 넣지 않음
                    continue

                dst_file = dest_root / file

                task_queue.put((src_file, dst_file, rel_path))
                dir_files_queued += 1
            
            # 디렉토리의 모든 파일이 이미 처리되었으면 디렉토리 완료 표시
            if cp is not None and dir_files_queued == 0 and len(files) > 0:
                rel_dir = root_path.relative_to(job.source).as_posix()
                cp["processed_dirs"].add(rel_dir)

            if cancelled:
                break
                
        # STAGE 1 완료
        queued_count = task_queue.qsize()
        if skipped_dirs > 0:
            logger.info(f"[SCAN] 완료: 총 {scan_count:,}개 파일 스캔 (Resume: {skipped_dirs:,}개 디렉토리 스킵), {queued_count:,}개 작업 대기열에 추가")
        else:
            logger.info(f"[SCAN] 완료: 총 {scan_count:,}개 파일 스캔, {queued_count:,}개 작업 대기열에 추가")
        update_stage("STAGE_1_SCAN", "completed", 
                    items_total=total_files, items_processed=scan_count)
        
        # ========== STAGE 2: 멀티스레드 파일 복사 ==========
        update_stage("STAGE_2_COPY", "running", items_total=total_files)
        logger.info("=" * 60)
        logger.info(f"STAGE 2: 멀티스레드 파일 복사")
        logger.info(f"   Worker 스레드: {num_threads}개")
        logger.info(f"   처리 대상: {queued_count:,}개 파일")
        logger.info("=" * 60)

        if not cancelled:
            task_queue.join()
            update_stage("STAGE_2_COPY", "completed", 
                        items_processed=current_processed)
        else:
            logger.info("[CANCEL] 큐에 남은 작업은 스레드에서 정리 후 종료 예정")
            update_stage("STAGE_2_COPY", "failed", error="사용자 취소")

        # ========== STAGE 3: 정리 및 동기화 ==========
        if cancelled:
            logger.info("사용자 취소로 STAGE 3, 4는 수행하지 않습니다.")
            update_stage("STAGE_3_CLEANUP", "failed", error="사용자 취소")
            update_stage("STAGE_4_SNAPSHOT", "failed", error="사용자 취소")
            journal.status = "cancelled"
            save_journal(journal, journal_file, destination_root=job.destination)
            if cp is not None and not dry_run:
                cp["status"] = "incomplete"
                save_checkpoint(cp)
            logger.info(f"=== Job 취소됨: {job.name} (status=cancelled) ===")
            if tui is not None:
                tui.update_progress(current_processed * 100 // (total_files or 1), current_processed, total_files)
                tui.refresh_if_dirty()
            return

        update_stage("STAGE_3_CLEANUP", "running")
        logger.info("=" * 60)
        logger.info("STAGE 3: 정리 및 동기화 (불필요 파일 처리)")
        logger.info("=" * 60)
        
        cleanup_count = 0
        if job.mode in ("clone", "safety_net"):
            for root, dirs, files in os.walk(job.destination):
                if tui is not None:
                    tui.refresh_if_dirty()

                root_path = Path(root)

                if any(x in root_path.parts for x in (".Rollback", ".SafetyNet")):
                    dirs[:] = []
                    continue

                dirs[:] = [d for d in dirs if not path_matches_patterns(root_path / d, job.exclude)]

                rel_root = root_path.relative_to(job.destination)
                src_root = job.source / rel_root

                for file in files:
                    dst_file = root_path / file
                    if path_matches_patterns(dst_file, job.exclude):
                        continue

                    rel_file = dst_file.relative_to(job.destination)
                    src_file = job.source / rel_file

                    if not src_file.exists():
                        if job.mode == "clone":
                            backup_path = Path(journal.rollback_root) / dst_file.relative_to(job.destination)
                            if dry_run:
                                logger.info(f"[DELETE (dry-run)] {dst_file}")
                            else:
                                try:
                                    backup_path.parent.mkdir(parents=True, exist_ok=True)
                                    logger.info(f"[BACKUP(before delete)] {dst_file} -> {backup_path}")
                                    shutil.move(str(dst_file), str(backup_path))
                                    journal.ops.append(JournalOp(
                                        action="delete_file",
                                        target=str(dst_file),
                                        backup=str(backup_path),
                                    ))
                                    stats.deleted_files += 1
                                except Exception as e:
                                    logger.error(f"[DELETE BACKUP 실패] {dst_file}: {e}")
                        elif job.mode == "safety_net":
                            try:
                                sn_path = move_to_safety_net(dst_file, job.destination, dry_run=dry_run)
                                journal.ops.append(JournalOp(
                                    action="delete_file",
                                    target=str(dst_file),
                                    backup=str(sn_path),
                                ))
                                stats.safetynet_files += 1
                                cleanup_count += 1
                            except Exception as e:
                                logger.error(f"[SafetyNet 이동 실패] {dst_file}: {e}")

            if job.mode == "clone" and not dry_run:
                for root, dirs, _files in os.walk(job.destination, topdown=False):
                    root_path = Path(root)
                    if any(x in root_path.parts for x in (".Rollback", ".SafetyNet")):
                        continue
                    for d in dirs:
                        dir_path = root_path / d
                        try:
                            dir_path.rmdir()
                            logger.info(f"[RMDIR] {dir_path}")
                            journal.ops.append(JournalOp(action="delete_file",
                                                         target=str(dir_path),
                                                         backup=None))
                        except OSError:
                            pass
        
        update_stage("STAGE_3_CLEANUP", "completed", 
                    items_processed=cleanup_count)

        # ========== STAGE 4: 스냅샷 생성 ==========
        update_stage("STAGE_4_SNAPSHOT", "running")
        logger.info("=" * 60)
        logger.info("STAGE 4: 스냅샷 및 요약 생성")
        logger.info("=" * 60)

        if total_files > 0 and current_processed < total_files:
            with progress_lock:
                current_processed = total_files
                if tui is not None:
                    tui.update_progress(100, current_processed, total_files)
                logger.info(
                    f"[PROGRESS] {job.name}: 100% ({current_processed}/{total_files})"
                )

        journal.status = "success"
        save_journal(journal, journal_file, destination_root=job.destination)

        if cp is not None and not dry_run:
            cp["status"] = "complete"
            save_checkpoint(cp)

        if not dry_run:
            snapshot_file = build_snapshot(job, journal, log_dir)
            summary_file = write_summary(job, journal, stats, log_dir)
            logger.info(f"스냅샷 파일: {snapshot_file}")
            logger.info(f"요약 리포트: {summary_file}")
        
        update_stage("STAGE_4_SNAPSHOT", "completed")
        
        # ========== 전체 Stage 요약 ==========
        logger.info("=" * 60)
        logger.info("전체 Stage 실행 결과")
        logger.info("=" * 60)
        for stage_key, stage in stages.items():
            status_icon = "✓" if stage.status == "completed" else "✗" if stage.status == "failed" else "○"
            logger.info(f"{status_icon} {stage.stage_name}: {stage.status} "
                       f"({stage.start_time or '-'} ~ {stage.end_time or '-'})")
        logger.info("=" * 60)

        logger.info(f"=== Job 성공: {job.name} ===")

        if tui is not None:
            tui.refresh_if_dirty()

    except Exception as e:
        logger.error(f"Job 중 에러 발생: {e}")
        import traceback
        logger.error(f"상세 에러: {traceback.format_exc()}")
        try:
            rollback_journal(journal, dry_run=dry_run)
            journal.status = "rolled_back"
        except Exception as re:
            logger.error(f"자동 롤백 실패: {re}")
            journal.status = "rollback_failed"
        finally:
            save_journal(journal, journal_file, destination_root=job.destination)
        logger.error(f"=== Job 실패 및 롤백 처리 완료 (status={journal.status}) ===")


# ================ CLI (기존) =================

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="CCC + SuperDuper 스타일의 프로급 디스크 백업/동기화 스크립트"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    backup_parser = subparsers.add_parser("backup", help="백업 실행 (TUI + 멀티스레드)")
    backup_parser.add_argument("-c", "--config", required=True, help="백업 설정 JSON 파일 경로")
    backup_parser.add_argument("-j", "--job", help="실행할 Job 이름 (생략 시 전체 Job 실행)")
    backup_parser.add_argument("--dry-run", action="store_true", help="실제 복사/삭제 없이 시뮬레이션만 수행")
    backup_parser.add_argument("--log-dir", help="로그/저널 저장 디렉토리 (기본: ./logs)")
    backup_parser.add_argument("--resume", action="store_true",
                               help="이전 체크포인트를 사용해 중단된 백업을 이어서 실행")

    rollback_parser = subparsers.add_parser("rollback", help="기존 저널 파일을 이용해 롤백 실행")
    rollback_parser.add_argument("-f", "--journal-file", required=True, help="저널 JSON 파일 경로")
    rollback_parser.add_argument("--dry-run", action="store_true", help="실제 롤백 없이 시뮬레이션")

    return parser.parse_args()


def _run_backup(args: argparse.Namespace, tui: Optional[SimpleTUI] = None) -> Path:
    config_path = Path(args.config).expanduser()
    if not config_path.exists():
        print(f"설정 파일을 찾을 수 없습니다: {config_path}", file=sys.stderr)
        sys.exit(1)

    if args.log_dir:
        log_dir = Path(args.log_dir).expanduser()
    else:
        log_dir = Path(__file__).resolve().parent / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = log_dir / f"disk_sync_pro_{ts}.log"

    use_tui = tui is not None
    setup_logger(log_file=log_file, verbose=not use_tui, use_tui=use_tui, tui_obj=tui)

    logger.info(f"설정 파일: {config_path}")
    logger.info(f"로그 파일: {log_file}")

    jobs = load_config(config_path)
    if args.job:
        jobs = [job for job in jobs if job.name == args.job]
        if not jobs:
            logger.error(f"해당 이름의 Job을 찾을 수 없습니다: {args.job}")
            sys.exit(1)

    for job in jobs:
        perform_backup(job, dry_run=args.dry_run, log_dir=log_dir, resume=args.resume, tui=tui)

    logger.info("모든 Job이 완료되었습니다.")
    return log_dir


def main_backup(args: argparse.Namespace) -> None:
    if sys.stdout.isatty() and curses is not None:
        def curses_main(stdscr):
            tui = SimpleTUI(stdscr)
            _run_backup(args, tui=tui)
            tui.add_log_line("백업이 종료되었습니다. 아무 키나 누르면 종료합니다.")
            tui.refresh_if_dirty()
            stdscr.nodelay(False)
            stdscr.getch()
        curses.wrapper(curses_main)
    else:
        _run_backup(args, tui=None)


def main_rollback(args: argparse.Namespace) -> None:
    journal_path = Path(args.journal_file).expanduser()
    if not journal_path.exists():
        print(f"저널 파일을 찾을 수 없습니다: {journal_path}", file=sys.stderr)
        sys.exit(1)

    log_dir = journal_path.parent
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = log_dir / f"disk_sync_pro_rollback_{ts}.log"
    setup_logger(log_file=log_file, verbose=True)

    logger.info(f"저널 파일: {journal_path}")
    journal = load_journal(journal_path)
    logger.info(f"Journal status: {journal.status}")

    dest_root = Path(journal.dest_root) if journal.dest_root else None

    try:
        rollback_journal(journal, dry_run=args.dry_run)
        if not args.dry_run:
            journal.status = "rolled_back"
            save_journal(journal, journal_path, destination_root=dest_root)
    except Exception as e:
        logger.error(f"롤백 중 에러: {e}")
        if not args.dry_run:
            journal.status = "rollback_failed"
            save_journal(journal, journal_path, destination_root=dest_root)


# ================ logs 기반 도우미 (메뉴에서 사용) =================

def get_latest_journal(log_dir: Path) -> Optional[Path]:
    journals = sorted(log_dir.glob("journal_*.json"))
    if not journals:
        return None
    return journals[-1]


def safe_addstr(stdscr, row: int, col: int, text: str, attr=0):
    """
    안전한 addstr 래퍼 - 전역 유틸리티 함수
    - 자동 화면 크기 감지
    - 한글/유니코드 안전 처리
    - 에러 복구
    """
    try:
        rows, cols = stdscr.getmaxyx()
        if row >= rows or col >= cols:
            return
        
        available_width = cols - col - 1
        if available_width <= 0:
            return
        
        # 문자열 길이 제한
        safe_text = text[:available_width]
        
        # 점진적으로 줄이며 안전하게 출력
        while len(safe_text) > 0:
            try:
                stdscr.addstr(row, col, safe_text, attr)
                break
            except Exception:
                safe_text = safe_text[:-1]
    except Exception:
        pass


def show_text_screen(stdscr, title: str, lines: List[str]):
    """텍스트 화면 표시 (목록 등)"""
    stdscr.clear()
    rows, cols = stdscr.getmaxyx()
    header = f" {title} ".center(cols, "=")
    safe_addstr(stdscr, 0, 0, header)

    max_lines = rows - 3
    for i, line in enumerate(lines[:max_lines], start=2):
        safe_addstr(stdscr, i, 0, line)

    footer = "아무 키나 누르면 이전 메뉴로 돌아갑니다."
    safe_addstr(stdscr, rows - 1, 0, footer)
    stdscr.refresh()
    stdscr.getch()


def show_journal_list_screen(stdscr, log_dir: Path):
    lines: List[str] = []
    journals = sorted(log_dir.glob("journal_*.json"))
    if not journals:
        lines.append("journal_*.json 파일이 없습니다.")
    else:
        for j in journals[-100:]:
            try:
                data = load_journal(j)
                lines.append(
                    f"{j.name}  | job={data.job_name} | ts={data.timestamp} | status={data.status}"
                )
            except Exception as e:
                lines.append(f"{j.name}  | (읽기 실패: {e})")

    show_text_screen(stdscr, "Journal 목록", lines)


def show_snapshot_list_screen(stdscr, log_dir: Path):
    lines: List[str] = []
    snapshots_root = log_dir / "snapshots"
    if not snapshots_root.exists():
        lines.append("snapshots 디렉토리가 없습니다.")
        show_text_screen(stdscr, "Snapshot 목록", lines)
        return

    for job_dir in sorted(snapshots_root.iterdir()):
        if not job_dir.is_dir():
            continue
        index_file = job_dir / "index.json"
        if not index_file.exists():
            lines.append(f"[{job_dir.name}] index.json 없음")
            continue
        try:
            with index_file.open("r", encoding="utf-8") as f:
                index = json.load(f)
            lines.append(f"=== Job: {job_dir.name} (snapshots: {len(index)}) ===")
            for entry in index[-20:]:
                lines.append(
                    f"  ts={entry.get('timestamp')} | file={entry.get('snapshot_file')} "
                    f"| count={entry.get('file_count')} | at={entry.get('generated_at')}"
                )
        except Exception as e:
            lines.append(f"[{job_dir.name}] index.json 읽기 실패: {e}")

    if not lines:
        lines.append("표시할 스냅샷이 없습니다.")

    show_text_screen(stdscr, "Snapshot 목록", lines)


# ================ config 선택 / 메뉴 기반 backup 실행 =================

def curses_input_string(stdscr, row: int, col: int, prompt: str = "", maxlen: int = 80, 
                        show_cursor: bool = True) -> str:
    """
    안전한 문자열 입력 함수
    - 한글/유니코드 완전 지원
    - 화면 크기 자동 감지
    - 에러 복구
    """
    try:
        rows, cols = stdscr.getmaxyx()
        
        # 행/열 범위 체크
        if row >= rows or col >= cols:
            return ""
        
        # 라인 클리어 및 프롬프트 출력
        try:
            stdscr.move(row, 0)
            stdscr.clrtoeol()
        except Exception:
            pass
        
        if prompt:
            safe_addstr(stdscr, row, col, prompt)
            input_col = col + len(prompt) + 1
        else:
            input_col = col
        
        # 입력 가능한 최대 너비 계산
        available_width = max(1, cols - input_col - 2)
        input_width = min(maxlen, available_width)
        
        if input_width <= 0:
            return ""
        
        # 커서 표시
        if show_cursor:
            try:
                curses.curs_set(1)
            except Exception:
                pass
        
        # 입력 위치로 이동
        try:
            stdscr.move(row, input_col)
        except Exception:
            pass
        
        stdscr.refresh()
        
        # 입력 받기
        curses.echo()
        try:
            input_bytes = stdscr.getstr(row, input_col, input_width)
            result = input_bytes.decode("utf-8", errors="ignore").strip()
        except Exception as e:
            result = ""
        finally:
            curses.noecho()
            if show_cursor:
                try:
                    curses.curs_set(0)
                except Exception:
                    pass
        
        return result
        
    except Exception:
        return ""


def curses_input_line(stdscr, prompt: str, default: str = "") -> str:
    """
    전체 화면을 사용한 한 줄 입력
    """
    try:
        rows, cols = stdscr.getmaxyx()
        stdscr.clear()
        
        # 제목
        title = " 입력 ".center(cols, "=")
        safe_addstr(stdscr, 0, 0, title)
        
        # 프롬프트
        safe_addstr(stdscr, 2, 2, prompt)
        
        # 기본값 표시
        if default:
            safe_addstr(stdscr, 3, 2, f"(현재: {default})")
            input_row = 5
        else:
            input_row = 4
        
        safe_addstr(stdscr, input_row, 2, "입력: ")
        
        # 안내
        safe_addstr(stdscr, rows - 2, 2, "Enter를 눌러 입력 완료 | 빈 값이면 취소 또는 기본값 사용")
        
        stdscr.refresh()
        
        # 입력 받기
        result = curses_input_string(stdscr, input_row, 8, "", maxlen=cols - 12)
        
        return result if result else default
        
    except Exception:
        return default


def curses_prompt(stdscr, prompt: str, maxlen: int = 40) -> str:
    """
    하단 상태바에서 짧은 입력 받기
    """
    try:
        rows, cols = stdscr.getmaxyx()
        prompt_row = rows - 1
        
        # 프롬프트 출력
        try:
            stdscr.move(prompt_row, 0)
            stdscr.clrtoeol()
        except Exception:
            pass
        
        safe_addstr(stdscr, prompt_row, 0, prompt)
        
        # 입력
        result = curses_input_string(stdscr, prompt_row, len(prompt), "", maxlen=maxlen)
        
        return result
        
    except Exception:
        return ""


def find_config_files() -> List[Path]:
    """
    현재 디렉토리와 ./configs 에서 *.json 검색
    """
    configs: List[Path] = []
    base_dir = Path(__file__).resolve().parent
    candidates = [Path.cwd(), base_dir / "configs"]
    seen = set()
    for d in candidates:
        if not d.exists():
            continue
        for p in sorted(d.glob("*.json")):
            if p.resolve() not in seen:
                seen.add(p.resolve())
                configs.append(p)
    return configs


def get_config_preview_lines(config_path: Path) -> List[str]:
    """
    config JSON 안에 어떤 Job 이 들어있는지 간단히 요약해서 보여주는 용도.
    """
    lines: List[str] = []
    try:
        jobs = load_config(config_path)
    except Exception as e:
        return [f"  (config 읽기 실패: {e})"]

    if not jobs:
        return ["  (jobs: 0)"]

    lines.append(f"  jobs: {len(jobs)}")
    max_show = 3
    for job in jobs[:max_show]:
        lines.append(f"    - {job.name} [{job.mode}]")
        lines.append(f"      src={job.source}")
        lines.append(f"      dst={job.destination}")
    if len(jobs) > max_show:
        lines.append(f"    ... +{len(jobs) - max_show} more job(s)")
    return lines


def curses_get_line(stdscr, prompt: str, default: str = "") -> str:
    """한 줄 입력 래퍼"""
    return curses_input_line(stdscr, prompt, default)




def interactive_select_config_curses(stdscr) -> Optional[Path]:
    """config 파일 선택 화면"""
    configs = find_config_files()
    rows, cols = stdscr.getmaxyx()

    while True:
        stdscr.clear()
        title = " Config 선택 ".center(cols, "=")
        safe_addstr(stdscr, 0, 0, title)

        if not configs:
            safe_addstr(stdscr, 2, 0, "자동으로 찾은 config JSON 파일이 없습니다.")
            safe_addstr(stdscr, 4, 0, "직접 경로를 입력하려면 'p' 키를, 취소하려면 'q' 키를 누르세요.")
            stdscr.refresh()
            ch = stdscr.getch()
            if ch in (ord('q'), ord('Q')):
                return None
            elif ch in (ord('p'), ord('P')):
                path_str = curses_input_line(stdscr, "config JSON 파일 경로를 입력하세요:")
                if not path_str:
                    return None
                p = Path(path_str).expanduser()
                if not p.exists():
                    show_text_screen(stdscr, "오류", [f"파일이 존재하지 않습니다: {p}"])
                    return None
                return p
            else:
                continue

        safe_addstr(stdscr, 2, 0, "아래에서 config JSON 파일을 선택하세요.")
        safe_addstr(stdscr, 3, 0, "(각 config 아래에 jobs 요약이 함께 표시됩니다.)")

        start_row = 5
        row = start_row

        for idx, cfg in enumerate(configs, start=1):
            if row >= rows - 4:
                break

            line = f"{idx}) {cfg}"
            safe_addstr(stdscr, row, 0, line)
            row += 1

            preview_lines = get_config_preview_lines(cfg)
            for pl in preview_lines:
                if row >= rows - 4:
                    break
                safe_addstr(stdscr, row, 0, pl)
                row += 1

            if row < rows - 4:
                row += 1

        safe_addstr(stdscr, rows - 3, 0, "P) 직접 경로 입력")
        safe_addstr(stdscr, rows - 2, 0, "Q) 취소")

        choice = curses_prompt(stdscr, "선택 번호 또는 P/Q 입력 후 Enter: ")

        if not choice:
            continue
        if choice.lower() == 'q':
            return None
        if choice.lower() == 'p':
            path_str = curses_input_line(stdscr, "config JSON 파일 경로를 입력하세요:")
            if not path_str:
                return None
            p = Path(path_str).expanduser()
            if not p.exists():
                show_text_screen(stdscr, "오류", [f"파일이 존재하지 않습니다: {p}"])
                return None
            return p

        try:
            idx = int(choice)
        except ValueError:
            continue

        if 1 <= idx <= len(configs):
            return configs[idx - 1]


def interactive_select_job_curses(stdscr, config_path: Path) -> (Optional[str], bool, bool, bool):
    """Job 선택 화면"""
    jobs = load_config(config_path)
    rows, cols = stdscr.getmaxyx()

    if not jobs:
        show_text_screen(stdscr, "오류", ["config 에 jobs 가 없습니다."])
        return None, False, False, True

    while True:
        stdscr.clear()
        title = f" Job 선택 ({config_path.name}) ".center(cols, "=")
        safe_addstr(stdscr, 0, 0, title)

        safe_addstr(stdscr, 2, 0, "실행할 Job 을 선택하세요. 0번은 전체 Job 실행입니다.")
        start_row = 4
        safe_addstr(stdscr, start_row - 1, 0, "0) 모든 Job 실행")

        for idx, job in enumerate(jobs, start=1):
            line = f"{idx}) {job.name}  (src={job.source}, dst={job.destination}, mode={job.mode})"
            if start_row + idx >= rows - 4:
                break
            safe_addstr(stdscr, start_row + idx - 1, 0, line)

        safe_addstr(stdscr, rows - 3, 0, "Q) 취소")

        sel = curses_prompt(stdscr, "선택 번호 입력 후 Enter: ")

        if not sel:
            continue
        if sel.lower() == 'q':
            return None, False, False, True

        try:
            idx = int(sel)
        except ValueError:
            continue

        if idx == 0:
            job_name = None
        elif 1 <= idx <= len(jobs):
            job_name = jobs[idx - 1].name
        else:
            continue

        stdscr.clear()
        safe_addstr(stdscr, 0, 0, "Dry-run 모드로 실행할까요? (변경 없이 시뮬레이션만 수행) [y/N]")
        stdscr.refresh()
        ch = stdscr.getch()
        dry_run = (ch in (ord('y'), ord('Y')))

        stdscr.clear()
        safe_addstr(stdscr, 0, 0, "이전 체크포인트(resume)를 사용할까요? [y/N]")
        stdscr.refresh()
        ch = stdscr.getch()
        resume = (ch in (ord('y'), ord('Y')))

        return job_name, dry_run, resume, False


def interactive_config_editor_curses(stdscr):
    """Config 편집 화면"""
    rows, cols = stdscr.getmaxyx()
    stdscr.clear()
    title = " Config 생성/수정 ".center(cols, "=")
    safe_addstr(stdscr, 0, 0, title)
    safe_addstr(stdscr, 2, 0, "기존 config 수정(E), 새 config 생성(N), 취소(Q)")
    stdscr.refresh()

    choice = curses_prompt(stdscr, "선택(E/N/Q): ").lower()
    if not choice or choice == 'q':
        return

    if choice == 'n':
        path_str = curses_input_line(stdscr, "새로 만들 config JSON 경로 (예: ./configs/my_backup.json):")
        if not path_str:
            return
        config_path = Path(path_str).expanduser()
        jobs: List[BackupJob] = []
    else:
        config_path = interactive_select_config_curses(stdscr)
        if config_path is None:
            return
        try:
            jobs = load_config(config_path)
        except Exception as e:
            show_text_screen(stdscr, "오류", [f"config 읽기 실패: {e}"])
            return

    while True:
        stdscr.clear()
        title = f" Config 편집: {config_path.name} ".center(cols, "=")
        safe_addstr(stdscr, 0, 0, title)

        if not jobs:
            safe_addstr(stdscr, 2, 0, "현재 등록된 Job 이 없습니다.")
        else:
            safe_addstr(stdscr, 2, 0, "수정할 Job 을 선택하세요.")
            safe_addstr(stdscr, 3, 0, "0) 새 Job 추가")
            row = 4
            for idx, job in enumerate(jobs, start=1):
                line = f"{idx}) {job.name} (mode={job.mode}, src={job.source}, dst={job.destination})"
                if row >= rows - 4:
                    break
                safe_addstr(stdscr, row, 0, line)
                row += 1

        safe_addstr(stdscr, rows - 3, 0, "Q) 취소")
        sel = curses_prompt(stdscr, "선택 번호 입력 (0=새 Job, Q=취소): ").lower()

        if not sel or sel == 'q':
            return

        if sel == '0':
            job = BackupJob(
                name="",
                source=Path("."),
                destination=Path("."),
                mode="safety_net",
                exclude=[],
                safety_net_days=30,
                verify=False,
            )
            jobs.append(job)
            editing_job = job
        else:
            try:
                idx = int(sel)
            except ValueError:
                continue
            if not (1 <= idx <= len(jobs)):
                continue
            editing_job = jobs[idx - 1]

        def edit_field(label: str, current: str) -> str:
            value = curses_input_line(stdscr, label, default=current)
            return value if value else current

        editing_job.name = edit_field("Job 이름", editing_job.name or "")
        editing_job.source = Path(edit_field("Source 경로", str(editing_job.source))).expanduser()
        editing_job.destination = Path(edit_field("Destination 경로", str(editing_job.destination))).expanduser()

        mode_val = edit_field("Mode (clone/sync/safety_net)", editing_job.mode)
        if mode_val in ("clone", "sync", "safety_net"):
            editing_job.mode = mode_val

        excl_str_current = ", ".join(editing_job.exclude) if editing_job.exclude else ""
        excl_str = edit_field("Exclude 패턴 (쉼표 구분, 예: .DS_Store,*.tmp)", excl_str_current)
        if excl_str:
            editing_job.exclude = [x.strip() for x in excl_str.split(",") if x.strip()]

        days_str = edit_field("SafetyNet 보관일 수", str(editing_job.safety_net_days))
        if days_str:
            try:
                editing_job.safety_net_days = int(days_str)
            except ValueError:
                pass

        v_str = edit_field("해시 검증(verify) 사용? (y/n)", "y" if editing_job.verify else "n")
        if v_str.lower() in ("y", "yes"):
            editing_job.verify = True
        elif v_str.lower() in ("n", "no"):
            editing_job.verify = False

        raw = {
            "jobs": [
                {
                    "name": j.name,
                    "source": str(j.source),
                    "destination": str(j.destination),
                    "mode": j.mode,
                    "exclude": j.exclude,
                    "safety_net_days": j.safety_net_days,
                    "verify": j.verify,
                }
                for j in jobs
            ]
        }
        try:
            config_path.parent.mkdir(parents=True, exist_ok=True)
            with config_path.open("w", encoding="utf-8") as f:
                json.dump(raw, f, indent=2, ensure_ascii=False)
        except Exception as e:
            show_text_screen(stdscr, "오류", [f"config 저장 실패: {e}"])
            return

        show_text_screen(
            stdscr,
            "Config 저장 완료",
            [
                f"파일: {config_path}",
                f"Job 수: {len(jobs)}",
                "",
                "Config 저장을 완료했습니다.",
            ],
        )
        return


def interactive_backup_flow_curses(stdscr, base_log_dir: Path):
    config_path = interactive_select_config_curses(stdscr)
    if config_path is None:
        return

    job_name, dry_run, resume, cancelled = interactive_select_job_curses(stdscr, config_path)
    if cancelled:
        return

    args = SimpleNamespace(
        command="backup",
        config=str(config_path),
        job=job_name,
        dry_run=dry_run,
        log_dir=str(base_log_dir),
        resume=resume,
    )

    tui = SimpleTUI(stdscr)
    _run_backup(args, tui=tui)
    tui.add_log_line("백업이 종료되었습니다. 아무 키나 누르면 메인 메뉴로 돌아갑니다.")
    tui.refresh_if_dirty()
    stdscr.nodelay(False)
    stdscr.getch()
    stdscr.nodelay(False)
    stdscr.clear()
    stdscr.refresh()


def interactive_latest_rollback_curses(stdscr, base_log_dir: Path):
    """최근 저널 롤백 화면"""
    stdscr.clear()
    rows, cols = stdscr.getmaxyx()
    latest = get_latest_journal(base_log_dir)
    if latest is None:
        safe_addstr(stdscr, 0, 0, "최근 저널 파일을 찾을 수 없습니다.")
        safe_addstr(stdscr, 2, 0, "아무 키나 누르면 메인 메뉴로 돌아갑니다.")
        stdscr.refresh()
        stdscr.getch()
        return

    try:
        j = load_journal(latest)
        safe_addstr(stdscr, 0, 0, f"최근 저널: {latest.name}")
        safe_addstr(stdscr, 1, 0, f"job={j.job_name}, ts={j.timestamp}, status={j.status}")
        safe_addstr(stdscr, 3, 0, "해당 저널로 롤백을 진행할까요? (y/N)")
        stdscr.refresh()
        c2 = stdscr.getch()
        if c2 in (ord('y'), ord('Y')):
            rollback_journal(journal=j, dry_run=False)
            j.status = "rolled_back"
            dest_root = Path(j.dest_root) if j.dest_root else None
            save_journal(j, latest, destination_root=dest_root)
            safe_addstr(stdscr, 5, 0, "롤백이 완료되었습니다. 아무 키나 누르면 메인 메뉴로 돌아갑니다.")
            stdscr.refresh()
            stdscr.getch()
        else:
            safe_addstr(stdscr, 5, 0, "롤백이 취소되었습니다. 아무 키나 누르면 메인 메뉴로 돌아갑니다.")
            stdscr.refresh()
            stdscr.getch()
    except Exception as e:
        safe_addstr(stdscr, 0, 0, f"저널 로드/롤백 중 오류: {e}")
        safe_addstr(stdscr, 2, 0, "아무 키나 누르면 메인 메뉴로 돌아갑니다.")
        stdscr.refresh()
        stdscr.getch()


# ================ TUI 메인 메뉴 =================

def interactive_main_curses(stdscr):
    """메인 메뉴 화면"""
    try:
        curses.curs_set(0)
    except Exception:
        pass

    base_log_dir = Path(__file__).resolve().parent / "logs"
    base_log_dir.mkdir(parents=True, exist_ok=True)

    # 색상 초기화
    use_colors = False
    try:
        if curses.has_colors():
            curses.start_color()
            curses.use_default_colors()
            curses.init_pair(1, curses.COLOR_CYAN, -1)
            curses.init_pair(2, curses.COLOR_YELLOW, -1)
            use_colors = True
    except Exception:
        pass

    while True:
        stdscr.clear()
        rows, cols = stdscr.getmaxyx()
        
        # 제목
        title = " DiskSyncPro - Main Menu "
        title_line = title.center(cols, "=")
        title_attr = curses.color_pair(1) | curses.A_BOLD if use_colors else curses.A_BOLD
        safe_addstr(stdscr, 0, 0, title_line, title_attr)
        
        # 버전 정보
        version_line = "Professional Backup & Sync Tool v2.0"
        safe_addstr(stdscr, 1, (cols - len(version_line)) // 2, version_line)

        # 메뉴 옵션
        menu_lines = [
            ("", 0),
            ("메뉴를 선택하세요:", 0),
            ("", 0),
            ("1) config 선택 후 백업 실행", 2),
            ("2) 가장 최근 Job 저널로 롤백 실행", 2),
            ("3) journal_*.json 목록 보기", 2),
            ("4) snapshots/ 목록 보기", 2),
            ("5) config JSON 생성/수정", 2),
            ("", 0),
            ("Q) 종료", 2),
        ]

        row = 3
        for line, attr_type in menu_lines:
            if row >= rows - 2:
                break
            if attr_type == 2 and use_colors:
                attr = curses.color_pair(2)
            else:
                attr = 0
            safe_addstr(stdscr, row, 2, line, attr)
            row += 1

        # 하단 안내
        help_line = "[1-5] 메뉴 선택  [Q] 종료  [ESC] 뒤로가기"
        safe_addstr(stdscr, rows - 1, (cols - len(help_line)) // 2, help_line)

        stdscr.refresh()
        
        # 키 입력 대기
        try:
            ch = stdscr.getch()
        except Exception:
            continue

        if ch in (ord('q'), ord('Q'), 27):  # Q 또는 ESC
            break
        elif ch == ord('1'):
            try:
                interactive_backup_flow_curses(stdscr, base_log_dir)
            except Exception as e:
                show_text_screen(stdscr, "오류", [f"백업 실행 중 오류: {e}"])
        elif ch == ord('2'):
            try:
                interactive_latest_rollback_curses(stdscr, base_log_dir)
            except Exception as e:
                show_text_screen(stdscr, "오류", [f"롤백 중 오류: {e}"])
        elif ch == ord('3'):
            try:
                show_journal_list_screen(stdscr, base_log_dir)
            except Exception as e:
                show_text_screen(stdscr, "오류", [f"저널 목록 표시 중 오류: {e}"])
        elif ch == ord('4'):
            try:
                show_snapshot_list_screen(stdscr, base_log_dir)
            except Exception as e:
                show_text_screen(stdscr, "오류", [f"스냅샷 목록 표시 중 오류: {e}"])
        elif ch == ord('5'):
            try:
                interactive_config_editor_curses(stdscr)
            except Exception as e:
                show_text_screen(stdscr, "오류", [f"설정 편집 중 오류: {e}"])
        
        # 잠시 대기 (키 중복 입력 방지)
        try:
            stdscr.nodelay(True)
            while stdscr.getch() != -1:
                pass
            stdscr.nodelay(False)
        except Exception:
            pass


# ================ 텍스트 모드 메인 메뉴 (curses 불가 시) =================

def interactive_main_plain():
    base_log_dir = Path(__file__).resolve().parent / "logs"
    base_log_dir.mkdir(parents=True, exist_ok=True)

    while True:
        print("=" * 60)
        print(" DiskSyncPro - Main Menu (no curses) ".center(60))
        print("=" * 60)
        print("1) config 선택 후 백업 실행")
        print("2) 가장 최근 Job 저널로 롤백 실행")
        print("3) journal_*.json 목록 보기")
        print("4) snapshots/ 목록 보기")
        print("5) config JSON 생성/수정")
        print("Q) 종료")
        choice = input("> ").strip().lower()

        if choice == 'q':
            break
        elif choice == '1':
            configs = find_config_files()
            if not configs:
                print("config JSON 파일을 찾지 못했습니다.")
                path_str = input("직접 경로를 입력하세요 (취소: 빈 줄): ").strip()
                if not path_str:
                    continue
                config_path = Path(path_str).expanduser()
            else:
                print("config 파일 목록:")
                for idx, c in enumerate(configs, start=1):
                    print(f"{idx}) {c}")
                    for pl in get_config_preview_lines(c):
                        print(pl)
                    print()
                sel = input("번호 선택 (또는 직접 경로 입력): ").strip()
                try:
                    idx = int(sel)
                    config_path = configs[idx - 1]
                except (ValueError, IndexError):
                    config_path = Path(sel).expanduser()

            if not config_path.exists():
                print(f"파일이 존재하지 않습니다: {config_path}")
                input("계속하려면 Enter...")
                continue

            jobs = load_config(config_path)
            if not jobs:
                print("config 에 jobs 가 없습니다.")
                input("계속하려면 Enter...")
                continue

            print("Job 목록:")
            print("0) 모든 Job 실행")
            for idx, job in enumerate(jobs, start=1):
                print(f"{idx}) {job.name} (src={job.source}, dst={job.destination}, mode={job.mode})")
            sel = input("번호 선택: ").strip()
            try:
                i = int(sel)
            except ValueError:
                continue
            if i == 0:
                job_name = None
            elif 1 <= i <= len(jobs):
                job_name = jobs[i - 1].name
            else:
                continue

            dry_run = input("Dry-run 모드로 실행할까요? [y/N]: ").strip().lower() == 'y'
            resume = input("resume 체크포인트를 사용할까요? [y/N]: ").strip().lower() == 'y'

            args = SimpleNamespace(
                command="backup",
                config=str(config_path),
                job=job_name,
                dry_run=dry_run,
                log_dir=str(base_log_dir),
                resume=resume,
            )
            _run_backup(args, tui=None)
            input("백업이 종료되었습니다. Enter 를 누르면 메인 메뉴로 돌아갑니다.")

        elif choice == '2':
            latest = get_latest_journal(base_log_dir)
            if not latest:
                print("최근 저널 파일을 찾을 수 없습니다.")
                input("계속하려면 Enter...")
                continue
            j = load_journal(latest)
            print(f"최근 저널: {latest.name}")
            print(f"job={j.job_name}, ts={j.timestamp}, status={j.status}")
            yn = input("이 저널로 롤백할까요? [y/N]: ").strip().lower()
            if yn == 'y':
                rollback_journal(j, dry_run=False)
                j.status = "rolled_back"
                save_journal(j, latest)
                print("롤백 완료.")
            else:
                print("롤백 취소.")
            input("계속하려면 Enter...")

        elif choice == '3':
            journals = sorted(base_log_dir.glob("journal_*.json"))
            if not journals:
                print("journal_*.json 파일이 없습니다.")
            else:
                for j in journals[-100:]:
                    data = load_journal(j)
                    print(f"{j.name} | job={data.job_name} | ts={data.timestamp} | status={data.status}")
            input("계속하려면 Enter...")

        elif choice == '4':
            snapshots_root = base_log_dir / "snapshots"
            if not snapshots_root.exists():
                print("snapshots 디렉토리가 없습니다.")
                input("계속하려면 Enter...")
                continue
            for job_dir in sorted(snapshots_root.iterdir()):
                if not job_dir.is_dir():
                    continue
                index_file = job_dir / "index.json"
                if not index_file.exists():
                    print(f"[{job_dir.name}] index.json 없음")
                    continue
                with index_file.open("r", encoding="utf-8") as f:
                    index = json.load(f)
                print(f"=== Job: {job_dir.name} (snapshots: {len(index)}) ===")
                for entry in index[-20:]:
                    print(
                        f"  ts={entry.get('timestamp')} | file={entry.get('snapshot_file')} "
                        f"| count={entry.get('file_count')} | at={entry.get('generated_at')}"
                    )
            input("계속하려면 Enter...")

        elif choice == '5':
            print("기존 config 수정(E), 새 config 생성(N), 취소(Q)")
            sub = input("> ").strip().lower()
            if sub == 'q' or not sub:
                continue

            if sub == 'n':
                path_str = input("새로 만들 config JSON 경로 (예: ./configs/my_backup.json): ").strip()
                if not path_str:
                    continue
                config_path = Path(path_str).expanduser()
                jobs: List[BackupJob] = []
            else:
                configs = find_config_files()
                if not configs:
                    print("자동 검색된 config 가 없습니다.")
                    path_str = input("직접 경로 입력 (취소: 빈 줄): ").strip()
                    if not path_str:
                        continue
                    config_path = Path(path_str).expanduser()
                else:
                    print("config 목록:")
                    for idx, c in enumerate(configs, start=1):
                        print(f"{idx}) {c}")
                    sel = input("번호 선택 또는 직접 경로 입력: ").strip()
                    try:
                        idx = int(sel)
                        config_path = configs[idx - 1]
                    except (ValueError, IndexError):
                        config_path = Path(sel).expanduser()

                try:
                    jobs = load_config(config_path)
                except Exception as e:
                    print(f"config 읽기 실패: {e}")
                    input("계속하려면 Enter...")
                    continue

            if jobs:
                print("0) 새 Job 추가")
                for idx, j in enumerate(jobs, start=1):
                    print(f"{idx}) {j.name} (mode={j.mode}, src={j.source}, dst={j.destination})")
                sel = input("수정할 Job 번호 선택 (0=새 Job): ").strip()
            else:
                print("현재 Job 이 없습니다. 새 Job 을 생성합니다.")
                sel = "0"

            if sel == "0":
                job = BackupJob(
                    name="",
                    source=Path("."),
                    destination=Path("."),
                    mode="safety_net",
                    exclude=[],
                    safety_net_days=30,
                    verify=False,
                )
                jobs.append(job)
                editing_job = job
            else:
                try:
                    idx = int(sel)
                    editing_job = jobs[idx - 1]
                except Exception:
                    print("잘못된 선택입니다.")
                    input("계속하려면 Enter...")
                    continue

            def edit_field(label: str, current: str) -> str:
                v = input(f"{label} (현재: {current}) 새 값(Enter=유지): ").strip()
                return current if v == "" else v

            editing_job.name = edit_field("Job 이름", editing_job.name or "(빈 값)")
            editing_job.source = Path(edit_field("Source 경로", str(editing_job.source))).expanduser()
            editing_job.destination = Path(edit_field("Destination 경로", str(editing_job.destination))).expanduser()

            mode_val = edit_field("Mode (clone/sync/safety_net)", editing_job.mode)
            if mode_val in ("clone", "sync", "safety_net"):
                editing_job.mode = mode_val

            excl_str_current = ", ".join(editing_job.exclude) if editing_job.exclude else ""
            excl_str = edit_field("Exclude 패턴(쉼표 구분)", excl_str_current)
            if excl_str != "":
                editing_job.exclude = [x.strip() for x in excl_str.split(",") if x.strip()]

            days_str = edit_field("SafetyNet 보관일 수", str(editing_job.safety_net_days))
            if days_str:
                try:
                    editing_job.safety_net_days = int(days_str)
                except ValueError:
                    pass

            verify_str = input(f"해시 검증(verify) 사용? (현재: {editing_job.verify}) [y/N]: ").strip().lower()
            if verify_str in ("y", "yes"):
                editing_job.verify = True
            elif verify_str in ("n", "no"):
                editing_job.verify = False

            raw = {
                "jobs": [
                    {
                        "name": j.name,
                        "source": str(j.source),
                        "destination": str(j.destination),
                        "mode": j.mode,
                        "exclude": j.exclude,
                        "safety_net_days": j.safety_net_days,
                        "verify": j.verify,
                    }
                    for j in jobs
                ]
            }
            try:
                config_path.parent.mkdir(parents=True, exist_ok=True)
                with config_path.open("w", encoding="utf-8") as f:
                    json.dump(raw, f, indent=2, ensure_ascii=False)
                print(f"Config 저장 완료: {config_path}")
            except Exception as e:
                print(f"Config 저장 실패: {e}")

            input("계속하려면 Enter...")

        else:
            continue


# ================ main 진입점 =================

def main() -> None:
    if len(sys.argv) == 1:
        if sys.stdout.isatty() and curses is not None:
            curses.wrapper(interactive_main_curses)
        else:
            interactive_main_plain()
        return

    args = parse_args()
    if args.command == "backup":
        main_backup(args)
    elif args.command == "rollback":
        main_rollback(args)
    else:
        print("알 수 없는 명령입니다.", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
