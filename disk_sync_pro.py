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
import time
import atexit
import fcntl
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

    # 파일 로그 핸들러 (append 모드)
    if log_file:
        fh = logging.FileHandler(log_file, mode='a', encoding="utf-8")
        fh.setFormatter(fmt)
        logger.addHandler(fh)

    # TUI 로그 핸들러
    if use_tui and tui_obj is not None:
        handler = CursesLogHandler(tui_obj)
        handler.setFormatter(fmt)
        logger.addHandler(handler)


logger = logging.getLogger("disk_sync_pro")


# ================ 로그 파일 관리 =================

def cleanup_old_logs(log_dir: Path, keep_days: int = 30):
    """
    오래된 로그 파일 정리
    Args:
        log_dir: 로그 디렉토리
        keep_days: 보관 일수 (기본 30일)
    """
    try:
        if not log_dir.exists():
            return
        
        if keep_days < 1:
            logger.warning("keep_days는 최소 1일 이상이어야 합니다")
            return
        
        from datetime import timedelta
        cutoff_date = datetime.now() - timedelta(days=keep_days)
        
        deleted_count = 0
        error_count = 0
        
        for log_file in log_dir.glob("disk_sync_pro_*.log"):
            try:
                # 파일이 일반 파일인지 확인
                if not log_file.is_file():
                    continue
                
                # 파일 수정 시간 확인
                mtime = datetime.fromtimestamp(log_file.stat().st_mtime)
                
                if mtime < cutoff_date:
                    log_file.unlink()
                    deleted_count += 1
                    logger.debug(f"로그 파일 삭제: {log_file.name} (수정: {mtime.strftime('%Y-%m-%d')})")
            except (IOError, OSError, PermissionError) as e:
                logger.debug(f"로그 파일 삭제 실패: {log_file} - {e}")
                error_count += 1
                continue
            except Exception as e:
                logger.debug(f"로그 파일 처리 중 예외: {log_file} - {e}")
                error_count += 1
                continue
        
        if deleted_count > 0:
            logger.info(f"✓ 오래된 로그 파일 {deleted_count}개 삭제 (보관기간: {keep_days}일)")
        if error_count > 0:
            logger.warning(f"로그 파일 정리 중 {error_count}개 파일 처리 실패")
    except Exception as e:
        logger.warning(f"로그 파일 정리 실패: {e}")


# ================ 프로그램 중복 실행 방지 =================

class SingleInstanceLock:
    """
    fcntl을 사용한 프로그램 중복 실행 방지
    """
    def __init__(self, lock_file: Path):
        self.lock_file = lock_file
        self.pid_file = lock_file.with_suffix('.pid')  # 별도 PID 파일
        self.lock_fd = None
    
    def acquire(self) -> bool:
        """
        Lock 획득 시도
        Returns:
            True: Lock 획득 성공 (프로그램 실행 가능)
            False: Lock 획득 실패 (이미 실행 중)
        """
        try:
            # Lock 파일 생성 (디렉토리가 없으면 생성)
            try:
                self.lock_file.parent.mkdir(parents=True, exist_ok=True)
            except (IOError, OSError, PermissionError) as e:
                logger.error(f"Lock 디렉토리 생성 실패: {e}")
                return False
            
            # 파일 열기 (O_CREAT | O_RDWR)
            try:
                self.lock_fd = open(self.lock_file, 'w')
            except (IOError, OSError, PermissionError) as e:
                logger.error(f"Lock 파일 생성 실패: {e}")
                return False
            
            # Non-blocking lock 시도
            try:
                fcntl.flock(self.lock_fd.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except (IOError, OSError) as e:
                # Lock 획득 실패 (이미 다른 프로세스가 실행 중)
                if self.lock_fd:
                    try:
                        self.lock_fd.close()
                    except Exception:
                        pass
                    self.lock_fd = None
                return False
            
            # Lock 파일에 PID 기록 (flush 필수)
            try:
                self.lock_fd.write(f"{os.getpid()}\n")
                self.lock_fd.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                self.lock_fd.flush()
                os.fsync(self.lock_fd.fileno())  # 디스크에 확실히 쓰기
            except Exception as e:
                logger.warning(f"Lock 파일 쓰기 실패 (계속 진행): {e}")
            
            # 별도 PID 파일에도 기록 (다른 프로세스가 쉽게 읽을 수 있도록)
            try:
                with open(self.pid_file, 'w') as pf:
                    pf.write(f"{os.getpid()}\n")
                    pf.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                    pf.flush()
                    os.fsync(pf.fileno())
            except Exception as e:
                logger.debug(f"PID 파일 생성 실패 (무시): {e}")
                pass  # PID 파일 실패해도 lock은 성공
            
            # 프로그램 종료 시 자동으로 lock 해제
            atexit.register(self.release)
            
            return True
        except Exception as e:
            logger.error(f"Lock 획득 중 예외: {e}")
            if self.lock_fd:
                try:
                    self.lock_fd.close()
                except Exception:
                    pass
                self.lock_fd = None
            return False
    
    def release(self):
        """Lock 해제"""
        if self.lock_fd:
            try:
                fcntl.flock(self.lock_fd.fileno(), fcntl.LOCK_UN)
                self.lock_fd.close()
            except Exception:
                pass
            finally:
                self.lock_fd = None
            
            # Lock 파일 삭제
            try:
                if self.lock_file.exists():
                    self.lock_file.unlink()
            except Exception:
                pass
            
            # PID 파일 삭제
            try:
                if self.pid_file.exists():
                    self.pid_file.unlink()
            except Exception:
                pass
    
    def __enter__(self):
        if not self.acquire():
            raise RuntimeError("프로그램이 이미 실행 중입니다")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()


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
    config_name: str = "default"  # config 파일명 (그룹핑용)


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
    검증 로직 포함
    """
    try:
        with config_path.open("r", encoding="utf-8") as f:
            raw = json.load(f)
    except json.JSONDecodeError as e:
        raise ValueError(f"JSON 파싱 실패 {config_path}: {e}")
    except (IOError, OSError) as e:
        raise IOError(f"Config 파일 읽기 실패 {config_path}: {e}")

    # config 파일명을 그룹 이름으로 사용 (확장자 제외)
    config_name = config_path.stem  # 예: "backup_config" from "backup_config.json"
    
    if "jobs" not in raw:
        raise ValueError(f"Config 파일에 'jobs' 키가 없습니다: {config_path}")
    
    if not isinstance(raw["jobs"], list):
        raise ValueError(f"'jobs'는 리스트여야 합니다: {config_path}")
    
    jobs: List[BackupJob] = []
    for idx, job in enumerate(raw.get("jobs", [])):
        # 필수 필드 검증
        required_fields = ["name", "source", "destination"]
        for field in required_fields:
            if field not in job:
                raise ValueError(f"Job {idx}: 필수 필드 '{field}'가 없습니다")
        
        # 모드 검증
        mode = job.get("mode", "safety_net")
        if mode not in ("clone", "sync", "safety_net"):
            raise ValueError(f"Job {idx} ({job['name']}): 유효하지 않은 mode '{mode}'")
        
        # 경로 검증
        try:
            source = Path(job["source"]).expanduser().resolve()
            destination = Path(job["destination"]).expanduser().resolve()
        except Exception as e:
            raise ValueError(f"Job {idx} ({job['name']}): 경로 처리 실패 - {e}")
        
        # source와 destination이 같은 경로인지 체크
        if source == destination:
            raise ValueError(f"Job {idx} ({job['name']}): source와 destination이 같습니다")
        
        # destination이 source의 하위 디렉토리인지 체크
        try:
            destination.relative_to(source)
            raise ValueError(f"Job {idx} ({job['name']}): destination이 source의 하위 디렉토리입니다")
        except ValueError:
            pass  # 정상 (하위 디렉토리 아님)
        
        jobs.append(
            BackupJob(
                name=job["name"],
                source=source,
                destination=destination,
                mode=mode,
                exclude=job.get("exclude", []),
                safety_net_days=max(1, job.get("safety_net_days", 30)),  # 최소 1일
                verify=job.get("verify", False),
                config_name=config_name,
            )
        )
    
    if not jobs:
        raise ValueError(f"Config 파일에 유효한 job이 없습니다: {config_path}")
    
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
    try:
        h = hashlib.new(algo)
        with path.open("rb") as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                h.update(chunk)
        return h.hexdigest()
    except (IOError, OSError) as e:
        raise IOError(f"파일 해시 계산 실패 {path}: {e}")


def is_same_file(src: Path, dst: Path) -> bool:
    """
    성능 우선: 파일 크기 + mtime 으로 동일 여부 판단
    """
    try:
        if not dst.exists():
            return False
        if not src.exists():
            return False
        # 심볼릭 링크는 제외
        if src.is_symlink() or dst.is_symlink():
            return False
        # 일반 파일만 비교
        if not src.is_file() or not dst.is_file():
            return False
        s_stat = src.stat()
        d_stat = dst.stat()
        return (s_stat.st_size == d_stat.st_size) and (int(s_stat.st_mtime) == int(d_stat.st_mtime))
    except (IOError, OSError, PermissionError):
        return False


def atomic_copy(src: Path, dst: Path) -> None:
    """
    임시 파일에 복사 후 os.replace 로 교체하는 원자적(atomic) 복사.
    """
    # 소스 파일 검증
    if not src.exists():
        raise IOError(f"소스 파일이 존재하지 않음: {src}")
    if src.is_symlink():
        raise IOError(f"심볼릭 링크는 복사할 수 없음: {src}")
    if not src.is_file():
        raise IOError(f"일반 파일이 아님: {src}")
    
    # 대상 디렉토리 생성
    dst_parent = dst.parent
    try:
        dst_parent.mkdir(parents=True, exist_ok=True)
    except (IOError, OSError, PermissionError) as e:
        raise IOError(f"대상 디렉토리 생성 실패 {dst_parent}: {e}")
    
    # 임시 파일 경로
    tmp_name = f".{dst.name}.sbk_tmp_{os.getpid()}_{int(time.time() * 1000000)}"
    tmp_path = dst_parent / tmp_name

    # 기존 임시 파일 정리 (안전하게)
    try:
        if tmp_path.exists():
            tmp_path.unlink()
    except (IOError, OSError, PermissionError):
        # 다른 이름으로 시도
        tmp_name = f".{dst.name}.sbk_tmp_{os.getpid()}_{int(time.time() * 1000000)}_alt"
        tmp_path = dst_parent / tmp_name

    try:
        # 복사 실행
        shutil.copy2(src, tmp_path)
        # 원자적 교체
        os.replace(tmp_path, dst)
    except Exception as e:
        # 실패 시 임시 파일 정리
        try:
            if tmp_path.exists():
                tmp_path.unlink()
        except Exception:
            pass
        raise IOError(f"원자적 복사 실패 {src} -> {dst}: {e}")


def ensure_dir(path: Path, journal: Optional[Journal] = None,
               stats: Optional[Stats] = None, dry_run: bool = False) -> None:
    """
    디렉토리 생성. 롤백을 위해 create_dir 기록.
    """
    try:
        if path.exists():
            if not path.is_dir():
                logger.error(f"경로가 디렉토리가 아닙니다: {path}")
                raise IOError(f"경로가 디렉토리가 아닙니다: {path}")
            return
        
        logger.info(f"[MKDIR] {path}")
        if dry_run:
            return
        
        path.mkdir(parents=True, exist_ok=True)
        
        if journal:
            journal.ops.append(JournalOp(action="create_dir", target=str(path)))
        if stats:
            stats.created_dirs += 1
    except (IOError, OSError, PermissionError) as e:
        logger.error(f"디렉토리 생성 실패 {path}: {e}")
        raise


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
    
    # 상대 경로 계산
    try:
        rel = target.relative_to(dest_root)
    except ValueError:
        # dest_root 밖의 파일인 경우, 안전한 경로 생성
        # target의 절대 경로를 base64로 인코딩하여 충돌 방지
        import base64
        safe_name = base64.urlsafe_b64encode(str(target.resolve()).encode()).decode()
        rel = Path("external") / safe_name[:50] / target.name  # 길이 제한

    sn_path = sn_root / rel
    
    # 이미 같은 이름이 있으면 타임스탬프 추가
    if sn_path.exists() and not dry_run:
        timestamp_suffix = f"_{int(time.time() * 1000000)}"
        sn_path = sn_path.with_stem(sn_path.stem + timestamp_suffix)
    
    logger.info(f"[SafetyNet] {target} -> {sn_path}")
    
    if not dry_run:
        try:
            sn_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(target), str(sn_path))
        except (IOError, OSError, PermissionError) as e:
            logger.error(f"SafetyNet 이동 실패 {target} -> {sn_path}: {e}")
            raise
    
    return sn_path


def prepare_journal(job: BackupJob) -> Journal:
    """저널 준비 (롤백 디렉토리 생성 포함)"""
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    rollback_root = job.destination / ".Rollback" / f"{job.name}_{ts}"
    
    try:
        rollback_root.mkdir(parents=True, exist_ok=True)
    except (IOError, OSError, PermissionError) as e:
        logger.error(f"롤백 디렉토리 생성 실패: {e}")
        raise IOError(f"롤백 디렉토리 생성 실패 {rollback_root}: {e}")
    
    return Journal(
        job_name=job.name,
        timestamp=ts,
        dest_root=str(job.destination.resolve()),
        rollback_root=str(rollback_root.resolve()),
        status="pending",
        ops=[],
    )


def journal_path_for(job: BackupJob, log_dir: Path, ts: str) -> Path:
    """
    Journal 파일 경로 생성
    구조: logs/<config_name>/journals/journal_<job_name>_<timestamp>.json
    """
    config_dir = log_dir / job.config_name
    journals_dir = config_dir / "journals"
    journals_dir.mkdir(parents=True, exist_ok=True)
    return journals_dir / f"journal_{job.name}_{ts}.json"


def get_dest_meta_dir(destination_root: Path) -> Path:
    """타겟 디스크의 메타데이터 저장 디렉토리"""
    meta_dir = destination_root / ".DiskSyncPro"
    meta_dir.mkdir(parents=True, exist_ok=True)
    return meta_dir


def save_journal(journal: Journal, path: Path, destination_root: Optional[Path] = None) -> None:
    """저널을 logs 폴더와 타겟 폴더 모두에 저장 (원자적 쓰기)"""
    serializable = {
        "job_name": journal.job_name,
        "timestamp": journal.timestamp,
        "dest_root": journal.dest_root,
        "rollback_root": journal.rollback_root,
        "status": journal.status,
        "ops": [asdict(op) for op in journal.ops],
    }
    
    # logs 폴더에 저장 (원자적)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = path.with_suffix('.tmp')
        with tmp_path.open("w", encoding="utf-8") as f:
            json.dump(serializable, f, indent=2, ensure_ascii=False)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, path)
    except Exception as e:
        logger.error(f"저널 저장 실패 {path}: {e}")
        try:
            if tmp_path.exists():
                tmp_path.unlink()
        except Exception:
            pass
    
    # 타겟 폴더에도 저장 (원자적)
    if destination_root and destination_root.exists():
        try:
            dest_meta_dir = get_dest_meta_dir(destination_root)
            dest_journal_path = dest_meta_dir / path.name
            dest_tmp_path = dest_journal_path.with_suffix('.tmp')
            
            with dest_tmp_path.open("w", encoding="utf-8") as f:
                json.dump(serializable, f, indent=2, ensure_ascii=False)
                f.flush()
                os.fsync(f.fileno())
            os.replace(dest_tmp_path, dest_journal_path)
            logger.info(f"저널 복사본 저장: {dest_journal_path}")
        except Exception as e:
            logger.warning(f"타겟 폴더 저널 저장 실패: {e}")
            try:
                if dest_tmp_path.exists():
                    dest_tmp_path.unlink()
            except Exception:
                pass


def load_journal(path: Path) -> Journal:
    """저널 파일 로드 (검증 포함)"""
    try:
        with path.open("r", encoding="utf-8") as f:
            raw = json.load(f)
    except json.JSONDecodeError as e:
        raise ValueError(f"저널 JSON 파싱 실패 {path}: {e}")
    except (IOError, OSError) as e:
        raise IOError(f"저널 파일 읽기 실패 {path}: {e}")
    
    # 필수 필드 검증
    required_fields = ["job_name", "timestamp", "dest_root", "rollback_root"]
    for field in required_fields:
        if field not in raw:
            raise ValueError(f"저널 파일에 필수 필드 '{field}'가 없습니다: {path}")
    
    # ops 파싱
    try:
        ops = [JournalOp(**op) for op in raw.get("ops", [])]
    except Exception as e:
        raise ValueError(f"저널 ops 파싱 실패 {path}: {e}")
    
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
    구조: logs/<config_name>/checkpoints/checkpoint_<job_name>.json
    """
    # config별로 디렉토리 구성
    config_dir = log_dir / job.config_name
    checkpoint_dir = config_dir / "checkpoints"
    checkpoint_dir.mkdir(parents=True, exist_ok=True)
    
    path = checkpoint_dir / f"checkpoint_{job.name}.json"
    
    if path.exists():
        try:
            # 파일이 비어있는지 체크
            file_size = path.stat().st_size
            if file_size == 0:
                logger.warning(f"체크포인트 파일이 비어있습니다: {path}")
                status = "incomplete"
                processed = set()
                processed_dirs = set()
                total_processed = 0
            else:
                with path.open("r", encoding="utf-8") as f:
                    data = json.load(f)
                
                # 데이터 검증
                if not isinstance(data, dict):
                    raise ValueError("체크포인트 데이터가 딕셔너리가 아닙니다")
                
                status = data.get("status", "incomplete")
                if status == "incomplete":
                    processed_files = data.get("processed_files", [])
                    if not isinstance(processed_files, list):
                        raise ValueError("processed_files가 리스트가 아닙니다")
                    processed = set(processed_files)
                    
                    processed_dirs_list = data.get("processed_dirs", [])
                    if not isinstance(processed_dirs_list, list):
                        raise ValueError("processed_dirs가 리스트가 아닙니다")
                    processed_dirs = set(processed_dirs_list)
                    
                    total_processed = data.get("total_processed", len(processed))
                    if not isinstance(total_processed, int):
                        total_processed = len(processed)
                else:
                    processed = set()
                    processed_dirs = set()
                    total_processed = 0
        except (json.JSONDecodeError, ValueError, TypeError, KeyError) as e:
            logger.warning(f"체크포인트 파일 읽기 실패 (새로 시작): {path} - {e}")
            # 손상된 파일 백업
            try:
                timestamp = int(time.time())
                backup_path = path.with_suffix(f'.json.corrupt.{timestamp}')
                shutil.copy2(path, backup_path)
                logger.info(f"손상된 체크포인트를 백업했습니다: {backup_path}")
            except Exception as backup_error:
                logger.debug(f"손상된 체크포인트 백업 실패: {backup_error}")
            status = "incomplete"
            processed = set()
            processed_dirs = set()
            total_processed = 0
        except (IOError, OSError, PermissionError) as e:
            logger.error(f"체크포인트 파일 접근 실패: {path} - {e}")
            status = "incomplete"
            processed = set()
            processed_dirs = set()
            total_processed = 0
    else:
        status = "incomplete"
        processed = set()
        processed_dirs = set()
        total_processed = 0
    
    cp = {
        "job_name": job.name,
        "status": status,
        "processed": processed,
        "processed_dirs": processed_dirs,  # 디렉토리 단위 추적
        "total_processed": total_processed,  # 실제 처리된 전체 수
        "path": path,
    }
    return cp


def save_checkpoint(cp: dict) -> None:
    """
    체크포인트 저장 (디렉토리 단위 추적 포함)
    원자적 쓰기 적용
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
        "total_processed": cp.get("total_processed", len(cp["processed"])),  # 실제 처리된 전체 수
    }
    
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        
        # 원자적 쓰기: 임시 파일에 쓴 후 rename
        tmp_path = path.with_suffix('.tmp')
        with tmp_path.open("w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
            f.flush()
            os.fsync(f.fileno())  # 디스크에 확실히 쓰기
        
        # 원자적으로 교체
        os.replace(tmp_path, path)
    except Exception as e:
        logger.error(f"체크포인트 저장 실패: {e}")
        # 임시 파일 정리
        try:
            if tmp_path.exists():
                tmp_path.unlink()
        except Exception:
            pass


# ================ 진행률 계산용 =================

def count_total_files_for_job(job: BackupJob) -> int:
    """
    진행률 계산을 위해 소스 아래 '대상 파일 수'를 미리 샘.
    exclude 패턴에 걸리는 파일은 제외.
    심볼릭 링크, 특수 파일 제외.
    """
    total = 0
    errors = 0
    
    try:
        for root, dirs, files in os.walk(job.source, followlinks=False):
            root_path = Path(root)
            
            # 제외 패턴에 맞는 디렉토리 필터링
            filtered_dirs = []
            for d in dirs:
                try:
                    dir_path = root_path / d
                    if not path_matches_patterns(dir_path, job.exclude):
                        # 심볼릭 링크 제외
                        if not dir_path.is_symlink():
                            filtered_dirs.append(d)
                except (IOError, OSError, PermissionError):
                    errors += 1
                    continue
            dirs[:] = filtered_dirs
            
            # 파일 카운트
            for f in files:
                try:
                    p = root_path / f
                    # 제외 패턴 체크
                    if path_matches_patterns(p, job.exclude):
                        continue
                    # 심볼릭 링크 제외
                    if p.is_symlink():
                        continue
                    # 일반 파일만 카운트
                    if p.is_file():
                        total += 1
                except (IOError, OSError, PermissionError):
                    errors += 1
                    continue
    except (IOError, OSError, PermissionError) as e:
        logger.warning(f"파일 카운트 중 오류 발생: {e}")
    
    if errors > 0:
        logger.warning(f"파일 카운트 중 {errors}개 항목 접근 실패 (권한 또는 I/O 오류)")
    
    return total


# ================ 롤백 =================

def rollback_journal(journal: Journal, dry_run: bool = False) -> None:
    """
    Journal 을 역순으로 읽어 롤백 수행.
    """
    logger.info(f"=== 롤백 시작: job={journal.job_name}, ts={journal.timestamp} ===")
    logger.info(f"총 {len(journal.ops)}개 작업을 롤백합니다.")
    
    success_count = 0
    fail_count = 0

    for idx, op in enumerate(reversed(journal.ops), 1):
        try:
            target = Path(op.target)
            backup = Path(op.backup) if op.backup else None

            if op.action == "create_file":
                if target.exists():
                    logger.info(f"[{idx}/{len(journal.ops)}] [ROLLBACK delete created file] {target}")
                    if not dry_run:
                        try:
                            target.unlink()
                            success_count += 1
                        except (IOError, OSError, PermissionError) as e:
                            logger.error(f"롤백: 파일 삭제 실패 {target}: {e}")
                            fail_count += 1
                else:
                    logger.debug(f"[{idx}/{len(journal.ops)}] 대상 파일이 이미 없음: {target}")
                    success_count += 1

            elif op.action in ("replace_file", "delete_file"):
                if backup and backup.exists():
                    logger.info(f"[{idx}/{len(journal.ops)}] [ROLLBACK restore] {backup} -> {target}")
                    if not dry_run:
                        try:
                            target.parent.mkdir(parents=True, exist_ok=True)
                            if target.exists():
                                target.unlink()
                            shutil.move(str(backup), str(target))
                            success_count += 1
                        except (IOError, OSError, PermissionError) as e:
                            logger.error(f"롤백: 복원 실패 {backup} -> {target}: {e}")
                            fail_count += 1
                elif backup:
                    logger.warning(f"[{idx}/{len(journal.ops)}] 백업 파일 없음: {backup}")
                    fail_count += 1
                else:
                    logger.debug(f"[{idx}/{len(journal.ops)}] 백업 없는 작업: {op.action}")
                    success_count += 1

            elif op.action == "create_dir":
                if target.exists() and target.is_dir():
                    if not dry_run:
                        try:
                            target.rmdir()
                            logger.info(f"[{idx}/{len(journal.ops)}] [ROLLBACK rmdir] {target}")
                            success_count += 1
                        except OSError as e:
                            logger.debug(f"디렉토리 삭제 실패 (비어있지 않음?): {target} - {e}")
                            # 비어있지 않은 디렉토리는 실패로 간주하지 않음
                            success_count += 1
                else:
                    logger.debug(f"[{idx}/{len(journal.ops)}] 대상 디렉토리가 이미 없음: {target}")
                    success_count += 1
            else:
                logger.warning(f"[{idx}/{len(journal.ops)}] 알 수 없는 작업 타입: {op.action}")
                
        except Exception as e:
            logger.error(f"[{idx}/{len(journal.ops)}] 롤백 중 예외 발생: {e}")
            fail_count += 1

    logger.info("=" * 60)
    logger.info(f"롤백 완료: 성공={success_count}, 실패={fail_count}, 전체={len(journal.ops)}")
    logger.info("=" * 60)


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
    구조: logs/<config_name>/snapshots/<job_name>/
    """
    dest_root = job.destination
    
    # config별로 디렉토리 구성
    config_dir = log_dir / job.config_name
    snapshot_dir = config_dir / "snapshots" / job.name
    snapshot_dir.mkdir(parents=True, exist_ok=True)

    files_manifest = []
    errors = 0

    for root, dirs, files in os.walk(dest_root, followlinks=False):
        root_path = Path(root)
        
        # 메타데이터 디렉토리 제외
        if any(x in root_path.parts for x in (".Rollback", ".SafetyNet", ".DiskSyncPro")):
            dirs[:] = []
            continue

        for f in files:
            try:
                file_path = root_path / f
                
                # 심볼릭 링크 제외
                if file_path.is_symlink():
                    continue
                
                # 일반 파일만 포함
                if not file_path.is_file():
                    continue
                
                rel_path = file_path.relative_to(dest_root).as_posix()
                st = file_path.stat()
                entry = {
                    "path": rel_path,
                    "size": st.st_size,
                    "mtime": int(st.st_mtime),
                }
                if job.verify:
                    try:
                        entry["hash"] = file_hash(file_path)
                    except Exception as e:
                        logger.warning(f"해시 계산 실패 (스냅샷에서 제외): {file_path} - {e}")
                        errors += 1
                        continue
                files_manifest.append(entry)
            except (IOError, OSError, PermissionError) as e:
                logger.warning(f"파일 정보 읽기 실패 (스냅샷에서 제외): {file_path} - {e}")
                errors += 1
                continue
    
    if errors > 0:
        logger.warning(f"스냅샷 생성 중 {errors}개 파일 처리 실패")

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
    구조: logs/<config_name>/summaries/summary_<job_name>_<timestamp>.json
    """
    # config별로 디렉토리 구성
    config_dir = log_dir / job.config_name
    summaries_dir = config_dir / "summaries"
    summaries_dir.mkdir(parents=True, exist_ok=True)
    
    summary = {
        "job_name": job.name,
        "timestamp": journal.timestamp,
        "mode": job.mode,
        "source": str(job.source),
        "destination": str(job.destination),
        "stats": asdict(stats),
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }

    # logs/<config_name>/summaries/ 폴더에 저장
    summary_file = summaries_dir / f"summary_{job.name}_{journal.timestamp}.json"
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

def check_disk_space(source: Path, destination: Path) -> tuple[bool, str]:
    """
    디스크 공간 체크
    Returns: (충분한지 여부, 메시지)
    """
    try:
        # 소스 크기 계산 (샘플링)
        source_size = 0
        sample_count = 0
        max_samples = 1000  # 샘플링으로 빠르게 추정
        
        for root, dirs, files in os.walk(source):
            for f in files:
                if sample_count >= max_samples:
                    break
                try:
                    p = Path(root) / f
                    if p.is_file() and not p.is_symlink():
                        source_size += p.stat().st_size
                        sample_count += 1
                except (IOError, OSError):
                    continue
            if sample_count >= max_samples:
                break
        
        # 샘플링 기반 추정
        if sample_count > 0:
            estimated_total = source_size * 2  # 2배 여유
        else:
            estimated_total = 10 * 1024 * 1024 * 1024  # 10GB 기본값
        
        # 대상 디스크 여유 공간 확인
        stat = os.statvfs(destination)
        free_space = stat.f_bavail * stat.f_frsize
        
        if free_space < estimated_total:
            gb_free = free_space / (1024**3)
            gb_need = estimated_total / (1024**3)
            return False, f"디스크 공간 부족: 필요={gb_need:.1f}GB, 여유={gb_free:.1f}GB"
        
        gb_free = free_space / (1024**3)
        return True, f"디스크 여유 공간: {gb_free:.1f}GB"
    except Exception as e:
        return True, f"디스크 공간 체크 실패 (계속 진행): {e}"


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
    
    if not job.source.is_dir():
        logger.error(f"소스가 디렉토리가 아닙니다: {job.source}")
        return

    if job.mode not in ("clone", "sync", "safety_net"):
        logger.error(f"지원하지 않는 모드입니다: {job.mode}")
        return

    if not dry_run:
        try:
            job.destination.mkdir(parents=True, exist_ok=True)
        except (IOError, OSError, PermissionError) as e:
            logger.error(f"대상 디렉토리 생성 실패: {e}")
            return
        
        # 디스크 공간 체크
        space_ok, space_msg = check_disk_space(job.source, job.destination)
        logger.info(f"💾 {space_msg}")
        if not space_ok:
            logger.error("디스크 공간이 부족합니다. 백업을 중단합니다.")
            return

    # 파일 개수 먼저 계산
    cancel_event = Event()
    cancelled = False

    total_files = count_total_files_for_job(job)
    if total_files == 0:
        logger.info("처리할 대상 파일이 없습니다. (0개)")
    else:
        logger.info(f"총 처리 대상 파일 수: {total_files:,}")

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

    journal = prepare_journal(job)
    journal_file = journal_path_for(job, log_dir, journal.timestamp)
    logger.info(f"저널 파일: {journal_file}")
    save_journal(journal, journal_file, destination_root=job.destination)

    stats = Stats()

    cp = None
    already_processed = 0
    cp_lock = Lock()
    
    if not dry_run:
        # dry_run이 아닐 때만 checkpoint 사용
        cp = load_or_init_checkpoint(job, log_dir)
        
        if resume:
            # Resume 모드: 기존 checkpoint 활용
            cp["status"] = "incomplete"
            already_processed = cp.get("total_processed", len(cp["processed"]))
            logger.info(f"✓ Checkpoint 로드: {cp['path']}")
            logger.info(f"   이미 처리된 파일: {already_processed:,}개 (실제)")
            logger.info(f"   checkpoint 파일 수: {len(cp['processed']):,}개 (최근)")
            logger.info(f"   완료된 디렉토리: {len(cp.get('processed_dirs', set())):,}개")
        else:
            # 새 백업: checkpoint 초기화
            cp["processed"].clear()
            cp["processed_dirs"].clear()
            cp["total_processed"] = 0
            cp["status"] = "incomplete"
            logger.info(f"✓ 새 Checkpoint 생성: {cp['path']}")
        
        # 초기 상태 저장
        save_checkpoint(cp)
        
        if resume and total_files > 0 and already_processed > 0:
            logger.info("=" * 60)
            logger.info(f"🔄 RESUME 모드 활성화")
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

    # Checkpoint 저장 최적화 (배치 처리)
    checkpoint_save_counter = 0
    checkpoint_save_interval = 100  # 100개마다 저장
    
    def add_processed_file_safe(rel_path: str):
        nonlocal checkpoint_save_counter
        if cp is None:
            return
        with cp_lock:
            if rel_path in cp["processed"]:
                return
            cp["processed"].add(rel_path)
            cp["total_processed"] = cp.get("total_processed", 0) + 1  # 실제 처리 수 증가
            checkpoint_save_counter += 1
            
            # 100개마다 또는 중요한 시점에 저장
            if checkpoint_save_counter >= checkpoint_save_interval:
                save_checkpoint(cp)
                checkpoint_save_counter = 0
                logger.debug(f"[CHECKPOINT] 저장: {cp['total_processed']:,}개 처리 완료")

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
            # 종료 신호 확인
            if cancel_event.is_set():
                # 큐에 남은 작업 빠르게 소진
                try:
                    while True:
                        task_queue.get_nowait()
                        task_queue.task_done()
                except Empty:
                    break
                break
            
            # 작업 가져오기
            try:
                src_file, dst_file, rel_path = task_queue.get(timeout=0.5)
            except Empty:
                if cancel_event.is_set():
                    break
                continue

            try:
                # 취소 확인
                if cancel_event.is_set():
                    logger.debug(f"[CANCELLED] 스킵 (resume 시 재처리됨): {src_file}")
                    continue
                
                # 동일 파일 체크
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
                    continue

                # 취소 재확인 (복사 전)
                if cancel_event.is_set():
                    logger.debug(f"[CANCELLED] 복사 전 스킵: {src_file}")
                    continue

                # 복사 실행
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
        t = Thread(target=worker, daemon=False, name=f"Worker-{i+1}")  # daemon=False로 변경하여 명시적 종료
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
                
                # 제외 패턴 체크
                if path_matches_patterns(src_file, job.exclude):
                    with stats_lock:
                        stats.skipped_excluded += 1
                    continue
                
                # 심볼릭 링크 제외
                try:
                    if src_file.is_symlink():
                        logger.debug(f"[SKIP] 심볼릭 링크: {src_file}")
                        with stats_lock:
                            stats.skipped_excluded += 1
                        continue
                    
                    # 일반 파일만 처리
                    if not src_file.is_file():
                        logger.debug(f"[SKIP] 일반 파일 아님: {src_file}")
                        with stats_lock:
                            stats.skipped_excluded += 1
                        continue
                except (IOError, OSError, PermissionError) as e:
                    logger.warning(f"[SKIP] 파일 접근 실패: {src_file} - {e}")
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
            
            # 최종 checkpoint 저장
            if cp is not None:
                with cp_lock:
                    save_checkpoint(cp)
                    logger.info(f"[CHECKPOINT] 최종 저장: {cp.get('total_processed', 0):,}개 파일 처리 완료")
            
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
            if cp is not None:
                with cp_lock:
                    cp["status"] = "incomplete"
                    save_checkpoint(cp)
                    logger.info(f"[CHECKPOINT] 취소 시점 저장: {cp.get('total_processed', 0):,}개 파일 처리 완료")
            logger.info(f"=== Job 취소됨: {job.name} (status=cancelled) ===")
            
            # 세션 종료 로그
            session_end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            logger.info("=" * 80)
            logger.info(f"[세션 종료 - 취소] {session_end_time}")
            logger.info("=" * 80)
            
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

        if cp is not None:
            with cp_lock:
                cp["status"] = "complete"
                save_checkpoint(cp)
                logger.info(f"[CHECKPOINT] 완료 상태 저장: {cp.get('total_processed', 0):,}개 파일")

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
        
        # 세션 종료 로그
        session_end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info("=" * 80)
        logger.info(f"[세션 종료] {session_end_time}")
        logger.info("=" * 80)

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
        
        # 세션 종료 로그
        session_end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.error("=" * 80)
        logger.error(f"[세션 종료 - 에러] {session_end_time}")
        logger.error("=" * 80)
    finally:
        # 스레드 정리 (항상 실행)
        logger.info("워커 스레드 정리 중...")
        cancel_event.set()  # 스레드에 종료 신호
        
        # 모든 워커 스레드가 종료될 때까지 대기 (최대 30초)
        for t in workers:
            try:
                t.join(timeout=30)
                if t.is_alive():
                    logger.warning(f"스레드 {t.name} 종료 대기 시간 초과")
            except Exception as e:
                logger.error(f"스레드 정리 중 에러: {e}")
        
        logger.info(f"✓ 워커 스레드 정리 완료 ({len(workers)}개)")


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

    # 날짜별로 로그 파일 생성 (append 모드)
    date_str = datetime.now().strftime("%Y%m%d")
    log_file = log_dir / f"disk_sync_pro_{date_str}.log"

    use_tui = tui is not None
    setup_logger(log_file=log_file, verbose=not use_tui, use_tui=use_tui, tui_obj=tui)

    # 오래된 로그 파일 정리 (30일 이상)
    cleanup_old_logs(log_dir, keep_days=30)

    # 세션 시작 구분자
    session_start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logger.info("=" * 80)
    logger.info(f"[세션 시작] {session_start_time}")
    logger.info("=" * 80)
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
    
    # 날짜별로 rollback 로그 파일 생성 (append 모드)
    date_str = datetime.now().strftime("%Y%m%d")
    log_file = log_dir / f"disk_sync_pro_rollback_{date_str}.log"
    setup_logger(log_file=log_file, verbose=True)

    # 세션 시작 구분자
    session_start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logger.info("=" * 80)
    logger.info(f"[ROLLBACK 세션 시작] {session_start_time}")
    logger.info("=" * 80)
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
    """
    최근 저널 파일 찾기 (새로운 디렉토리 구조 지원)
    구조: logs/<config_name>/journals/journal_*.json
    """
    journals = []
    
    # 새 구조: logs/<config_name>/journals/
    for config_dir in log_dir.iterdir():
        if not config_dir.is_dir():
            continue
        journals_dir = config_dir / "journals"
        if journals_dir.exists():
            journals.extend(journals_dir.glob("journal_*.json"))
    
    # 구 구조: logs/journal_*.json (하위 호환성)
    journals.extend(log_dir.glob("journal_*.json"))
    
    if not journals:
        return None
    
    # 최신 파일 반환 (수정 시간 기준)
    try:
        return max(journals, key=lambda p: p.stat().st_mtime)
    except (IOError, OSError):
        # stat 실패 시 이름 기준 정렬
        return sorted(journals)[-1]


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
    
    # config별로 디렉토리 구조 탐색
    journals = []
    for config_dir in sorted(log_dir.iterdir()):
        if not config_dir.is_dir():
            continue
        journals_dir = config_dir / "journals"
        if journals_dir.exists():
            journals.extend(journals_dir.glob("journal_*.json"))
    
    journals = sorted(journals)
    
    if not journals:
        lines.append("")
        lines.append("   ⚠️  journal 파일이 없습니다.")
        lines.append("")
        lines.append("   백업을 실행하면 journal이 생성됩니다.")
        show_text_screen(stdscr, "📋 Journal 목록", lines)
        return
    
    # Job별로 그룹화
    journals_by_job = {}
    for j in journals:
        try:
            data = load_journal(j)
            job_name = data.job_name
            if job_name not in journals_by_job:
                journals_by_job[job_name] = []
            
            # 파일 크기
            file_size = j.stat().st_size
            if file_size > 1024 * 1024:
                size_str = f"{file_size / (1024 * 1024):.1f} MB"
            elif file_size > 1024:
                size_str = f"{file_size / 1024:.1f} KB"
            else:
                size_str = f"{file_size} B"
            
            journals_by_job[job_name].append({
                'file': j,
                'data': data,
                'size': size_str,
                'ops_count': len(data.ops)
            })
        except Exception as e:
            # 읽기 실패한 경우
            if 'ERROR' not in journals_by_job:
                journals_by_job['ERROR'] = []
            journals_by_job['ERROR'].append({
                'file': j,
                'error': str(e)
            })
    
    # 전체 통계
    total_journals = len(journals)
    total_jobs = len([k for k in journals_by_job.keys() if k != 'ERROR'])
    
    lines.append("")
    lines.append(f"  📊 전체 통계: {total_jobs}개 Job, 총 {total_journals:,}개 Journal")
    lines.append(f"  {'─' * 78}")
    
    # Job별로 표시
    for job_name in sorted(journals_by_job.keys()):
        job_journals = journals_by_job[job_name]
        
        if job_name == 'ERROR':
            # 에러 섹션
            lines.append("")
            lines.append(f"╭{'─' * 78}╮")
            lines.append(f"│ ❌ 읽기 실패{' ' * 64}│")
            lines.append(f"├{'─' * 78}┤")
            
            for item in job_journals:
                filename = item['file'].name
                error = item['error']
                if len(filename) > 60:
                    filename = filename[:57] + "..."
                if len(error) > 60:
                    error = error[:57] + "..."
                lines.append(f"│   {filename:<70}│")
                lines.append(f"│   └─ {error:<68}│")
            
            lines.append(f"╰{'─' * 78}╯")
            continue
        
        # 정상 Job
        lines.append("")
        lines.append(f"╭{'─' * 78}╮")
        lines.append(f"│ 📦 Job: {job_name:<68} │")
        lines.append(f"├{'─' * 78}┤")
        lines.append(f"│   총 Journal: {len(job_journals):,}개{' ' * (63 - len(f'{len(job_journals):,}'))}│")
        lines.append(f"├{'─' * 78}┤")
        
        # 최근 10개만 표시
        recent_journals = sorted(job_journals, key=lambda x: x['file'].name)[-10:]
        
        for idx, item in enumerate(recent_journals, 1):
            data = item['data']
            size_str = item['size']
            ops_count = item['ops_count']
            
            # 날짜/시간 파싱
            try:
                # timestamp: WORK-to-WORK_BACKUP_20251205_141530
                parts = data.timestamp.split('_')
                if len(parts) >= 3:
                    date_part = parts[-2]  # 20251205
                    time_part = parts[-1]  # 141530
                    formatted_date = f"{date_part[:4]}-{date_part[4:6]}-{date_part[6:8]}"
                    formatted_time = f"{time_part[:2]}:{time_part[2:4]}:{time_part[4:6]}"
                    display_datetime = f"{formatted_date} {formatted_time}"
                else:
                    display_datetime = data.timestamp
            except:
                display_datetime = data.timestamp[:30]
            
            # Status 이모지
            if data.status == "complete":
                status_icon = "✅"
            elif data.status == "cancelled":
                status_icon = "⚠️"
            else:
                status_icon = "❌"
            
            lines.append(f"│                                                                              │")
            lines.append(f"│   [{idx:2d}] 📅 {display_datetime:<30} {status_icon}               │")
            lines.append(f"│        📝 작업: {ops_count:,}개 / 크기: {size_str:<10}{' ' * (41 - len(f'{ops_count:,}') - len(size_str))}│")
            lines.append(f"│        📊 상태: {data.status:<62}│")
        
        # 더 많은 journal이 있는 경우
        if len(job_journals) > 10:
            remaining = len(job_journals) - 10
            lines.append(f"│                                                                              │")
            lines.append(f"│   ... 외 {remaining:,}개의 이전 Journal{' ' * (52 - len(f'{remaining:,}'))}│")
        
        lines.append(f"╰{'─' * 78}╯")
    
    show_text_screen(stdscr, "📋 Journal 목록", lines)


def show_snapshot_list_screen(stdscr, log_dir: Path):
    lines: List[str] = []
    
    # config별로 디렉토리 구조 탐색
    config_snapshot_map = {}  # {config_name: [job_dirs]}
    
    for config_dir in sorted(log_dir.iterdir()):
        if not config_dir.is_dir():
            continue
        snapshots_root = config_dir / "snapshots"
        if snapshots_root.exists():
            job_dirs = sorted([d for d in snapshots_root.iterdir() if d.is_dir()])
            if job_dirs:
                config_snapshot_map[config_dir.name] = job_dirs
    
    if not config_snapshot_map:
        lines.append("")
        lines.append("   ⚠️  snapshots 디렉토리가 없습니다.")
        lines.append("")
        lines.append("   스냅샷은 백업 완료 시 자동으로 생성됩니다.")
        show_text_screen(stdscr, "📸 Snapshot 목록", lines)
        return
    
    # 전체 통계 계산
    total_snapshots = 0
    for job_dirs in config_snapshot_map.values():
        for job_dir in job_dirs:
            index_file = job_dir / "index.json"
            if index_file.exists():
                try:
                    with index_file.open("r", encoding="utf-8") as f:
                        index = json.load(f)
                    total_snapshots += len(index)
                except:
                    pass
    
    lines.append("")
    lines.append(f"  📊 전체 통계: {len(config_snapshot_map)}개 Config, 총 {total_snapshots:,}개 스냅샷")
    lines.append(f"  {'─' * 78}")
    
    # Config별로 표시
    for config_name in sorted(config_snapshot_map.keys()):
        job_dirs = config_snapshot_map[config_name]
        
        lines.append("")
        lines.append(f"╔{'═' * 78}╗")
        lines.append(f"║ 🗂️  Config: {config_name:<66} ║")
        lines.append(f"╠{'═' * 78}╣")
        
        config_snapshot_count = 0
        
        # Config 내의 각 Job 처리
        for idx, job_dir in enumerate(job_dirs):
            index_file = job_dir / "index.json"
            
            if not index_file.exists():
                lines.append("")
                lines.append(f"╭{'─' * 78}╮")
                lines.append(f"│ 📦 Job: {job_dir.name:<68} │")
                lines.append(f"├{'─' * 78}┤")
                lines.append(f"│   ⚠️  index.json 파일이 없습니다.{' ' * 44}│")
                lines.append(f"╰{'─' * 78}╯")
                continue
            
            try:
                with index_file.open("r", encoding="utf-8") as f:
                    index = json.load(f)
                
                snapshot_count = len(index)
                config_snapshot_count += snapshot_count
                
                # Job 헤더
                lines.append("")
                lines.append(f"╭{'─' * 78}╮")
                lines.append(f"│ 📦 Job: {job_dir.name:<68} │")
                lines.append(f"├{'─' * 78}┤")
                lines.append(f"│   총 스냅샷: {snapshot_count:,}개{' ' * (64 - len(f'{snapshot_count:,}'))}│")
                lines.append(f"├{'─' * 78}┤")
                
                if snapshot_count == 0:
                    lines.append(f"│   (스냅샷 없음){' ' * 62}│")
                else:
                    # 최근 5개만 표시
                    recent_snapshots = index[-5:]
                    
                    for snap_idx, entry in enumerate(recent_snapshots, 1):
                        timestamp = entry.get('timestamp', 'N/A')
                        file_count = entry.get('file_count', 0)
                        generated_at = entry.get('generated_at', 'N/A')
                        snapshot_file = entry.get('snapshot_file', 'N/A')
                        
                        # 날짜/시간 포맷팅
                        try:
                            parts = timestamp.split('_')
                            if len(parts) >= 3:
                                date_part = parts[-2]
                                time_part = parts[-1]
                                formatted_date = f"{date_part[:4]}-{date_part[4:6]}-{date_part[6:8]}"
                                formatted_time = f"{time_part[:2]}:{time_part[2:4]}:{time_part[4:6]}"
                                display_datetime = f"{formatted_date} {formatted_time}"
                            else:
                                display_datetime = timestamp
                        except:
                            display_datetime = timestamp
                        
                        file_count_str = f"{file_count:,}".rjust(10)
                        
                        lines.append(f"│   [{snap_idx}] 📅 {display_datetime:<30} 📁 {file_count_str}개 │")
                    
                    # 더 많은 스냅샷이 있는 경우
                    if snapshot_count > 5:
                        remaining = snapshot_count - 5
                        lines.append(f"│   ... 외 {remaining:,}개의 이전 스냅샷{' ' * (52 - len(f'{remaining:,}'))}│")
                
                lines.append(f"╰{'─' * 78}╯")
                
            except Exception as e:
                lines.append("")
                lines.append(f"╭{'─' * 78}╮")
                lines.append(f"│ 📦 Job: {job_dir.name:<68} │")
                lines.append(f"├{'─' * 78}┤")
                lines.append(f"│   ❌ 에러: {str(e)[:64]:<64}│")
                lines.append(f"╰{'─' * 78}╯")
        
        # Config 요약
        lines.append(f"╠{'═' * 78}╣")
        lines.append(f"║ Config 합계: {config_snapshot_count:,}개 스냅샷{' ' * (62 - len(f'{config_snapshot_count:,}'))}║")
        lines.append(f"╚{'═' * 78}╝")
    
    show_text_screen(stdscr, "📸 Snapshot 목록", lines)


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

        # Dry-run 선택
        stdscr.clear()
        rows, cols = stdscr.getmaxyx()
        safe_addstr(stdscr, 0, 0, "=" * cols)
        safe_addstr(stdscr, 1, 0, " 옵션 선택 ".center(cols))
        safe_addstr(stdscr, 2, 0, "=" * cols)
        safe_addstr(stdscr, 4, 2, "Dry-run 모드로 실행할까요?")
        safe_addstr(stdscr, 5, 2, "(변경 없이 시뮬레이션만 수행)")
        safe_addstr(stdscr, 7, 2, "[Y] 예  [N] 아니오 (기본)")
        stdscr.refresh()
        
        # 키 버퍼 클리어
        stdscr.nodelay(True)
        while stdscr.getch() != -1:
            pass
        stdscr.nodelay(False)
        
        ch = stdscr.getch()
        dry_run = (ch in (ord('y'), ord('Y')))

        # Resume 선택
        stdscr.clear()
        safe_addstr(stdscr, 0, 0, "=" * cols)
        safe_addstr(stdscr, 1, 0, " 옵션 선택 ".center(cols))
        safe_addstr(stdscr, 2, 0, "=" * cols)
        safe_addstr(stdscr, 4, 2, "이전 체크포인트(Resume)를 사용할까요?")
        safe_addstr(stdscr, 5, 2, "(중단된 지점부터 이어서 실행)")
        safe_addstr(stdscr, 7, 2, "[Y] 예  [N] 아니오 (기본)")
        stdscr.refresh()
        
        # 키 버퍼 클리어
        stdscr.nodelay(True)
        while stdscr.getch() != -1:
            pass
        stdscr.nodelay(False)
        
        ch = stdscr.getch()
        resume = (ch in (ord('y'), ord('Y')))
        
        # 선택 확인 화면
        stdscr.clear()
        safe_addstr(stdscr, 0, 0, "=" * cols)
        safe_addstr(stdscr, 1, 0, " 선택 확인 ".center(cols))
        safe_addstr(stdscr, 2, 0, "=" * cols)
        safe_addstr(stdscr, 4, 2, f"Job: {job_name or '전체'}")
        safe_addstr(stdscr, 5, 2, f"Dry-run: {'예' if dry_run else '아니오'}")
        safe_addstr(stdscr, 6, 2, f"Resume: {'예' if resume else '아니오'}")
        safe_addstr(stdscr, 8, 2, "백업을 시작합니다... (잠시만 기다려주세요)")
        stdscr.refresh()
        
        # 1초 대기 (사용자가 확인할 수 있도록)
        time.sleep(1)

        return job_name, dry_run, resume, False


def show_config_manager_main_menu(stdscr):
    """Config 관리 메인 메뉴 - CRUD"""
    while True:
        stdscr.clear()
        rows, cols = stdscr.getmaxyx()
        
        title = " ⚙️  Config 관리 (CRUD) ".center(cols, "=")
        safe_addstr(stdscr, 0, 0, title)
        
        # Config 파일 목록
        configs = find_config_files()
        
        safe_addstr(stdscr, 2, 2, f"📁 발견된 Config 파일: {len(configs)}개")
        safe_addstr(stdscr, 3, 2, "─" * (cols - 4))
        
        menu_row = 5
        safe_addstr(stdscr, menu_row, 2, "메뉴:")
        menu_row += 1
        safe_addstr(stdscr, menu_row, 4, "1) 📋 Config 목록 보기 (Read)")
        menu_row += 1
        safe_addstr(stdscr, menu_row, 4, "2) ➕ 새 Config 생성 (Create)")
        menu_row += 1
        safe_addstr(stdscr, menu_row, 4, "3) ✏️  Config 수정 (Update)")
        menu_row += 1
        safe_addstr(stdscr, menu_row, 4, "4) 🗑️  Config 삭제 (Delete)")
        menu_row += 1
        safe_addstr(stdscr, menu_row, 4, "5) 📝 템플릿으로 생성")
        menu_row += 1
        safe_addstr(stdscr, menu_row, 4, "6) ✅ Config 검증")
        menu_row += 2
        safe_addstr(stdscr, menu_row, 4, "Q) 메인 메뉴로 돌아가기")
        
        safe_addstr(stdscr, rows - 1, 0, "[1-6] 메뉴 선택  [Q] 돌아가기")
        stdscr.refresh()
        
        ch = stdscr.getch()
        
        if ch in (ord('q'), ord('Q'), 27):
            break
        elif ch == ord('1'):
            show_config_list_screen(stdscr)
        elif ch == ord('2'):
            create_new_config_curses(stdscr)
        elif ch == ord('3'):
            update_config_curses(stdscr)
        elif ch == ord('4'):
            delete_config_curses(stdscr)
        elif ch == ord('5'):
            create_from_template_curses(stdscr)
        elif ch == ord('6'):
            validate_config_curses(stdscr)


def show_config_list_screen(stdscr):
    """Config 목록 상세 보기"""
    configs = find_config_files()
    rows, cols = stdscr.getmaxyx()
    
    lines = []
    lines.append("")
    lines.append(f"  📊 전체 Config 파일: {len(configs)}개")
    lines.append(f"  {'─' * 78}")
    lines.append("")
    
    if not configs:
        lines.append("  ⚠️  Config 파일이 없습니다.")
        lines.append("")
        lines.append("  새 Config를 생성하려면 메뉴에서 '2) 새 Config 생성'을 선택하세요.")
        show_text_screen(stdscr, "📋 Config 목록", lines)
        return
    
    for idx, config_path in enumerate(configs, 1):
        lines.append(f"╭{'─' * 78}╮")
        lines.append(f"│ [{idx}] 📄 {config_path.name:<70} │")
        lines.append(f"├{'─' * 78}┤")
        lines.append(f"│ 경로: {str(config_path)[:74]:<74} │")
        
        try:
            jobs = load_config(config_path)
            lines.append(f"│ Job 수: {len(jobs):<70} │")
            lines.append(f"├{'─' * 78}┤")
            
            if jobs:
                for job_idx, job in enumerate(jobs[:3], 1):  # 최대 3개만 표시
                    lines.append(f"│   [{job_idx}] {job.name:<68} │")
                    lines.append(f"│       모드: {job.mode:<64} │")
                    lines.append(f"│       소스: {str(job.source)[:64]:<64} │")
                    lines.append(f"│       대상: {str(job.destination)[:64]:<64} │")
                    if job_idx < len(jobs) and job_idx < 3:
                        lines.append(f"│{' ' * 78}│")
                
                if len(jobs) > 3:
                    remaining = len(jobs) - 3
                    lines.append(f"│   ... 외 {remaining}개 Job{' ' * (66 - len(str(remaining)))}│")
            else:
                lines.append(f"│   (Job 없음){' ' * 66}│")
        except Exception as e:
            lines.append(f"│ ❌ 로드 실패: {str(e)[:64]:<64} │")
        
        lines.append(f"╰{'─' * 78}╯")
        lines.append("")
    
    show_text_screen(stdscr, "📋 Config 목록", lines)


def create_new_config_curses(stdscr):
    """새 Config 생성 (사용자 친화적)"""
    stdscr.clear()
    rows, cols = stdscr.getmaxyx()
    
    title = " ➕ 새 Config 생성 ".center(cols, "=")
    safe_addstr(stdscr, 0, 0, title)
    safe_addstr(stdscr, 2, 2, "새로 만들 Config 파일 경로를 입력하세요.")
    safe_addstr(stdscr, 3, 2, "예: backup_config.json 또는 configs/my_backup.json")
    stdscr.refresh()
    
    # 경로 입력
    path_str = curses_input_line(stdscr, "Config 파일 경로 (확장자 .json 자동 추가):")
    if not path_str:
        return
    
    # .json 확장자 자동 추가
    if not path_str.endswith('.json'):
        path_str += '.json'
    
    config_path = Path(path_str).expanduser()
    
    # 이미 존재하는지 확인
    if config_path.exists():
        stdscr.clear()
        safe_addstr(stdscr, 0, 0, title)
        safe_addstr(stdscr, 3, 2, f"⚠️  파일이 이미 존재합니다: {config_path}")
        safe_addstr(stdscr, 5, 2, "덮어쓰시겠습니까? [y/N]:")
        stdscr.refresh()
        
        ch = stdscr.getch()
        if ch not in (ord('y'), ord('Y')):
            return
    
    # 빈 Config 생성
    jobs: List[BackupJob] = []
    
    # Job 관리 메뉴로 이동
    manage_jobs_in_config_curses(stdscr, config_path, jobs)


def update_config_curses(stdscr):
    """Config 수정 (Job CRUD 포함)"""
    configs = find_config_files()
    
    if not configs:
        show_text_screen(stdscr, "오류", ["Config 파일이 없습니다.", "", "먼저 새 Config를 생성하세요."])
        return
    
    # Config 선택
    config_path = interactive_select_config_curses(stdscr)
    if config_path is None:
        return
    
    try:
        jobs = load_config(config_path)
    except Exception as e:
        show_text_screen(stdscr, "오류", [f"Config 읽기 실패: {e}"])
        return
    
    # Job 관리 메뉴로 이동
    manage_jobs_in_config_curses(stdscr, config_path, jobs)


def manage_jobs_in_config_curses(stdscr, config_path: Path, jobs: List[BackupJob]):
    """Config 내의 Job 관리 (CRUD)"""
    while True:
        stdscr.clear()
        rows, cols = stdscr.getmaxyx()
        
        title = f" ✏️  Config 편집: {config_path.name} ".center(cols, "=")
        safe_addstr(stdscr, 0, 0, title)
        
        safe_addstr(stdscr, 2, 2, f"📦 현재 Job 수: {len(jobs)}개")
        safe_addstr(stdscr, 3, 2, "─" * (cols - 4))
        
        row = 5
        if jobs:
            safe_addstr(stdscr, row, 2, "Job 목록:")
            row += 1
            for idx, job in enumerate(jobs, start=1):
                if row >= rows - 12:
                    safe_addstr(stdscr, row, 4, "... (더 많은 Job)")
                    break
                safe_addstr(stdscr, row, 4, f"{idx}) {job.name} [{job.mode}]")
                row += 1
                safe_addstr(stdscr, row, 7, f"src: {job.source}")
                row += 1
                safe_addstr(stdscr, row, 7, f"dst: {job.destination}")
                row += 1
        else:
            safe_addstr(stdscr, row, 2, "⚠️  Job이 없습니다.")
            row += 1
        
        row += 1
        menu_start = row
        safe_addstr(stdscr, row, 2, "작업:")
        row += 1
        safe_addstr(stdscr, row, 4, "A) ➕ Job 추가")
        row += 1
        safe_addstr(stdscr, row, 4, "E) ✏️  Job 수정")
        row += 1
        safe_addstr(stdscr, row, 4, "D) 🗑️  Job 삭제")
        row += 1
        safe_addstr(stdscr, row, 4, "C) 📋 Job 복제")
        row += 1
        safe_addstr(stdscr, row, 4, "V) 👁️  Job 상세 보기")
        row += 1
        safe_addstr(stdscr, row, 4, "S) 💾 저장 후 종료")
        row += 1
        safe_addstr(stdscr, row, 4, "Q) 저장 않고 종료")
        
        safe_addstr(stdscr, rows - 1, 0, "[A/E/D/C/V/S/Q] 메뉴 선택")
        stdscr.refresh()
        
        ch = stdscr.getch()
        
        if ch in (ord('q'), ord('Q')):
            # 변경사항 있는지 확인
            stdscr.clear()
            safe_addstr(stdscr, rows // 2, 2, "저장하지 않고 종료하시겠습니까? [y/N]:")
            stdscr.refresh()
            confirm = stdscr.getch()
            if confirm in (ord('y'), ord('Y')):
                break
        elif ch in (ord('s'), ord('S')):
            # 저장
            if save_config_to_file(stdscr, config_path, jobs):
                show_text_screen(stdscr, "저장 완료", [
                    f"파일: {config_path}",
                    f"Job 수: {len(jobs)}",
                    "",
                    "✅ Config가 성공적으로 저장되었습니다."
                ])
                break
        elif ch in (ord('a'), ord('A')):
            # Job 추가
            new_job = create_job_interactive(stdscr)
            if new_job:
                jobs.append(new_job)
        elif ch in (ord('e'), ord('E')):
            # Job 수정
            if not jobs:
                show_text_screen(stdscr, "알림", ["수정할 Job이 없습니다."])
                continue
            job_idx = select_job_from_list(stdscr, jobs, "수정할 Job 선택")
            if job_idx is not None:
                edit_job_interactive(stdscr, jobs[job_idx])
        elif ch in (ord('d'), ord('D')):
            # Job 삭제
            if not jobs:
                show_text_screen(stdscr, "알림", ["삭제할 Job이 없습니다."])
                continue
            job_idx = select_job_from_list(stdscr, jobs, "삭제할 Job 선택")
            if job_idx is not None:
                stdscr.clear()
                safe_addstr(stdscr, rows // 2, 2, f"'{jobs[job_idx].name}' Job을 삭제하시겠습니까? [y/N]:")
                stdscr.refresh()
                confirm = stdscr.getch()
                if confirm in (ord('y'), ord('Y')):
                    jobs.pop(job_idx)
                    show_text_screen(stdscr, "삭제 완료", ["Job이 삭제되었습니다."])
        elif ch in (ord('c'), ord('C')):
            # Job 복제
            if not jobs:
                show_text_screen(stdscr, "알림", ["복제할 Job이 없습니다."])
                continue
            job_idx = select_job_from_list(stdscr, jobs, "복제할 Job 선택")
            if job_idx is not None:
                original_job = jobs[job_idx]
                new_job = BackupJob(
                    name=f"{original_job.name}_copy",
                    source=original_job.source,
                    destination=original_job.destination,
                    mode=original_job.mode,
                    exclude=original_job.exclude.copy(),
                    safety_net_days=original_job.safety_net_days,
                    verify=original_job.verify,
                )
                jobs.append(new_job)
                show_text_screen(stdscr, "복제 완료", [f"'{new_job.name}' Job이 생성되었습니다."])
        elif ch in (ord('v'), ord('V')):
            # Job 상세 보기
            if not jobs:
                show_text_screen(stdscr, "알림", ["보기할 Job이 없습니다."])
                continue
            job_idx = select_job_from_list(stdscr, jobs, "상세 보기할 Job 선택")
            if job_idx is not None:
                show_job_details(stdscr, jobs[job_idx])


def select_job_from_list(stdscr, jobs: List[BackupJob], title: str) -> Optional[int]:
    """Job 목록에서 선택"""
    stdscr.clear()
    rows, cols = stdscr.getmaxyx()
    
    safe_addstr(stdscr, 0, 0, f" {title} ".center(cols, "="))
    
    row = 2
    for idx, job in enumerate(jobs, start=1):
        if row >= rows - 3:
            break
        safe_addstr(stdscr, row, 2, f"{idx}) {job.name} [{job.mode}]")
        row += 1
    
    safe_addstr(stdscr, rows - 2, 0, "번호 입력 (0=취소):")
    stdscr.refresh()
    
    sel = curses_prompt(stdscr, "")
    if not sel or sel == '0':
        return None
    
    try:
        idx = int(sel) - 1
        if 0 <= idx < len(jobs):
            return idx
    except ValueError:
        pass
    
    return None


def create_job_interactive(stdscr) -> Optional[BackupJob]:
    """대화형 Job 생성"""
    name = curses_input_line(stdscr, "Job 이름:")
    if not name:
        return None
    
    source_str = curses_input_line(stdscr, "Source 경로 (소스 디렉토리):")
    if not source_str:
        return None
    source = Path(source_str).expanduser()
    
    dest_str = curses_input_line(stdscr, "Destination 경로 (백업 대상):")
    if not dest_str:
        return None
    destination = Path(dest_str).expanduser()
    
    # 모드 선택
    stdscr.clear()
    rows, cols = stdscr.getmaxyx()
    safe_addstr(stdscr, 0, 0, " 백업 모드 선택 ".center(cols, "="))
    safe_addstr(stdscr, 2, 2, "1) safety_net - 삭제 파일을 SafetyNet으로 이동 (권장)")
    safe_addstr(stdscr, 3, 2, "2) sync       - 추가/변경만 반영, 삭제 안함")
    safe_addstr(stdscr, 4, 2, "3) clone      - 완전 미러링 (대상 불필요 파일 삭제)")
    stdscr.refresh()
    
    mode_choice = curses_prompt(stdscr, "선택 [1/2/3]: ")
    mode_map = {'1': 'safety_net', '2': 'sync', '3': 'clone'}
    mode = mode_map.get(mode_choice, 'safety_net')
    
    # Exclude 패턴
    excl_str = curses_input_line(stdscr, "제외 패턴 (쉼표 구분, 예: .DS_Store,*.tmp) [Enter=기본값]:")
    if excl_str:
        exclude = [x.strip() for x in excl_str.split(",") if x.strip()]
    else:
        exclude = [".DS_Store", ".Spotlight-V100", ".fseventsd", "node_modules", ".terraform", "*.tmp"]
    
    # SafetyNet 보관일
    days_str = curses_input_line(stdscr, "SafetyNet 보관일 수 [기본: 30]:", "30")
    try:
        safety_net_days = int(days_str)
    except ValueError:
        safety_net_days = 30
    
    # Verify
    stdscr.clear()
    safe_addstr(stdscr, rows // 2, 2, "해시 검증 사용? (느리지만 안전) [y/N]:")
    stdscr.refresh()
    ch = stdscr.getch()
    verify = (ch in (ord('y'), ord('Y')))
    
    return BackupJob(
        name=name,
        source=source,
        destination=destination,
        mode=mode,
        exclude=exclude,
        safety_net_days=safety_net_days,
        verify=verify,
    )


def edit_job_interactive(stdscr, job: BackupJob):
    """대화형 Job 수정"""
    def edit_field(label: str, current: str) -> str:
        value = curses_input_line(stdscr, f"{label} [현재: {current}]:", current)
        return value if value else current
    
    job.name = edit_field("Job 이름", job.name)
    job.source = Path(edit_field("Source 경로", str(job.source))).expanduser()
    job.destination = Path(edit_field("Destination 경로", str(job.destination))).expanduser()
    
    # 모드 선택
    rows, cols = stdscr.getmaxyx()
    stdscr.clear()
    safe_addstr(stdscr, 0, 0, " 백업 모드 선택 ".center(cols, "="))
    safe_addstr(stdscr, 2, 2, f"현재: {job.mode}")
    safe_addstr(stdscr, 3, 2, "1) safety_net 2) sync 3) clone")
    stdscr.refresh()
    
    mode_choice = curses_prompt(stdscr, "선택 [1/2/3, Enter=유지]: ")
    mode_map = {'1': 'safety_net', '2': 'sync', '3': 'clone'}
    if mode_choice in mode_map:
        job.mode = mode_map[mode_choice]
    
    excl_str_current = ", ".join(job.exclude) if job.exclude else ""
    excl_str = edit_field("Exclude 패턴", excl_str_current)
    if excl_str:
        job.exclude = [x.strip() for x in excl_str.split(",") if x.strip()]
    
    days_str = edit_field("SafetyNet 보관일", str(job.safety_net_days))
    try:
        job.safety_net_days = int(days_str)
    except ValueError:
        pass
    
    stdscr.clear()
    safe_addstr(stdscr, rows // 2, 2, f"해시 검증 [현재: {'ON' if job.verify else 'OFF'}] [y/n/Enter=유지]:")
    stdscr.refresh()
    ch = stdscr.getch()
    if ch in (ord('y'), ord('Y')):
        job.verify = True
    elif ch in (ord('n'), ord('N')):
        job.verify = False


def show_job_details(stdscr, job: BackupJob):
    """Job 상세 정보 표시"""
    lines = []
    lines.append("")
    lines.append(f"  📦 Job: {job.name}")
    lines.append(f"  {'─' * 78}")
    lines.append(f"  모드: {job.mode}")
    lines.append(f"  소스: {job.source}")
    lines.append(f"  대상: {job.destination}")
    lines.append(f"  제외 패턴: {', '.join(job.exclude) if job.exclude else '(없음)'}")
    lines.append(f"  SafetyNet 보관: {job.safety_net_days}일")
    lines.append(f"  해시 검증: {'ON' if job.verify else 'OFF'}")
    lines.append("")
    
    # 경로 검증
    if job.source.exists():
        lines.append(f"  ✅ 소스 존재: {job.source}")
    else:
        lines.append(f"  ❌ 소스 없음: {job.source}")
    
    if job.destination.exists():
        lines.append(f"  ✅ 대상 존재: {job.destination}")
    else:
        lines.append(f"  ⚠️  대상 없음 (백업 시 생성됨): {job.destination}")
    
    show_text_screen(stdscr, f"Job 상세: {job.name}", lines)


def save_config_to_file(stdscr, config_path: Path, jobs: List[BackupJob]) -> bool:
    """Config를 파일에 저장"""
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
        tmp_path = config_path.with_suffix('.tmp')
        with tmp_path.open("w", encoding="utf-8") as f:
            json.dump(raw, f, indent=2, ensure_ascii=False)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, config_path)
        return True
    except Exception as e:
        show_text_screen(stdscr, "저장 실패", [f"오류: {e}"])
        try:
            if tmp_path.exists():
                tmp_path.unlink()
        except Exception:
            pass
        return False


def delete_config_curses(stdscr):
    """Config 삭제"""
    configs = find_config_files()
    
    if not configs:
        show_text_screen(stdscr, "알림", ["삭제할 Config 파일이 없습니다."])
        return
    
    config_path = interactive_select_config_curses(stdscr)
    if config_path is None:
        return
    
    # 확인
    stdscr.clear()
    rows, cols = stdscr.getmaxyx()
    safe_addstr(stdscr, rows // 2 - 2, 2, f"⚠️  다음 Config를 삭제하시겠습니까?")
    safe_addstr(stdscr, rows // 2, 2, f"   {config_path}")
    safe_addstr(stdscr, rows // 2 + 2, 2, "삭제하려면 'DELETE'를 입력하세요:")
    stdscr.refresh()
    
    confirm = curses_input_line(stdscr, "확인:")
    if confirm == "DELETE":
        try:
            config_path.unlink()
            show_text_screen(stdscr, "삭제 완료", [f"✅ Config가 삭제되었습니다: {config_path.name}"])
        except Exception as e:
            show_text_screen(stdscr, "삭제 실패", [f"오류: {e}"])
    else:
        show_text_screen(stdscr, "취소됨", ["삭제가 취소되었습니다."])


def create_from_template_curses(stdscr):
    """템플릿으로부터 Config 생성"""
    templates = {
        "1": {
            "name": "기본 백업 (Safety Net)",
            "mode": "safety_net",
            "verify": False,
            "exclude": [".DS_Store", ".Spotlight-V100", ".fseventsd", "node_modules", ".terraform", "*.tmp"]
        },
        "2": {
            "name": "완전 미러링 (Clone)",
            "mode": "clone",
            "verify": True,
            "exclude": [".DS_Store", "*.tmp"]
        },
        "3": {
            "name": "증분 동기화 (Sync)",
            "mode": "sync",
            "verify": False,
            "exclude": [".DS_Store", "node_modules", "*.log"]
        }
    }
    
    stdscr.clear()
    rows, cols = stdscr.getmaxyx()
    safe_addstr(stdscr, 0, 0, " 📝 템플릿 선택 ".center(cols, "="))
    
    row = 2
    for key, tmpl in templates.items():
        safe_addstr(stdscr, row, 2, f"{key}) {tmpl['name']}")
        row += 1
        safe_addstr(stdscr, row, 5, f"모드: {tmpl['mode']}, 검증: {'ON' if tmpl['verify'] else 'OFF'}")
        row += 2
    
    stdscr.refresh()
    
    choice = curses_prompt(stdscr, "선택 [1/2/3, Q=취소]: ")
    if choice not in templates:
        return
    
    template = templates[choice]
    
    # 경로 입력
    path_str = curses_input_line(stdscr, "Config 파일명 (예: my_backup.json):")
    if not path_str:
        return
    
    if not path_str.endswith('.json'):
        path_str += '.json'
    
    config_path = Path(path_str).expanduser()
    
    name = curses_input_line(stdscr, "Job 이름:", template["name"])
    source_str = curses_input_line(stdscr, "Source 경로:")
    if not source_str:
        return
    destination_str = curses_input_line(stdscr, "Destination 경로:")
    if not destination_str:
        return
    
    job = BackupJob(
        name=name,
        source=Path(source_str).expanduser(),
        destination=Path(destination_str).expanduser(),
        mode=template["mode"],
        exclude=template["exclude"],
        safety_net_days=30,
        verify=template["verify"],
    )
    
    jobs = [job]
    if save_config_to_file(stdscr, config_path, jobs):
        show_text_screen(stdscr, "생성 완료", [
            f"✅ 템플릿 기반 Config가 생성되었습니다!",
            f"파일: {config_path}",
            f"Job: {job.name}"
        ])


def validate_config_curses(stdscr):
    """Config 검증"""
    configs = find_config_files()
    
    if not configs:
        show_text_screen(stdscr, "알림", ["검증할 Config 파일이 없습니다."])
        return
    
    config_path = interactive_select_config_curses(stdscr)
    if config_path is None:
        return
    
    lines = []
    lines.append("")
    lines.append(f"  ✅ Config 검증: {config_path.name}")
    lines.append(f"  {'─' * 78}")
    lines.append("")
    
    try:
        jobs = load_config(config_path)
        lines.append(f"  ✅ JSON 파싱 성공")
        lines.append(f"  ✅ Job 로드 성공: {len(jobs)}개")
        lines.append("")
        
        for idx, job in enumerate(jobs, 1):
            lines.append(f"  [{idx}] {job.name}")
            
            # 경로 검증
            if job.source.exists():
                lines.append(f"      ✅ 소스 존재")
            else:
                lines.append(f"      ❌ 소스 없음: {job.source}")
            
            if job.destination.exists():
                lines.append(f"      ✅ 대상 존재")
            else:
                lines.append(f"      ⚠️  대상 없음 (백업 시 생성)")
            
            # 모드 검증
            if job.mode in ("clone", "sync", "safety_net"):
                lines.append(f"      ✅ 모드 유효: {job.mode}")
            else:
                lines.append(f"      ❌ 모드 invalid: {job.mode}")
            
            lines.append("")
        
        lines.append("  ✅ 전체 검증 완료!")
        
    except Exception as e:
        lines.append(f"  ❌ 검증 실패: {e}")
    
    show_text_screen(stdscr, "Config 검증 결과", lines)


def interactive_backup_flow_curses(stdscr, base_log_dir: Path, auto_resume_config: dict = None):
    """
    백업 실행 플로우
    auto_resume_config: 자동 Resume용 설정 (config_path, job_name)
    """
    try:
        if auto_resume_config:
            # 자동 Resume 모드
            config_path = auto_resume_config["config_path"]
            job_name = auto_resume_config["job_name"]
            dry_run = False
            resume = True
            logger.info(f"🔄 자동 Resume 모드: {config_path.name} / {job_name or '전체'}")
        else:
            # 일반 모드
            config_path = interactive_select_config_curses(stdscr)
            if config_path is None:
                return {"action": "menu"}  # 취소 시 메뉴로

            job_name, dry_run, resume, cancelled = interactive_select_job_curses(stdscr, config_path)
            if cancelled:
                return {"action": "menu"}  # 취소 시 메뉴로

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
        
        # 백업 결과에 따른 처리
        tui.add_log_line("=" * 60)
        tui.add_log_line("백업이 종료되었습니다.")
        tui.add_log_line("[R] Resume으로 재시작  [M] 메뉴로  [Q] 종료")
        tui.add_log_line("=" * 60)
        tui.refresh_if_dirty()
        
        # 키 버퍼 클리어
        stdscr.nodelay(True)
        while stdscr.getch() != -1:
            pass
        stdscr.nodelay(False)
        
        while True:
            ch = stdscr.getch()
            if ch in (ord('r'), ord('R')):
                # Resume으로 재시작
                stdscr.clear()
                stdscr.refresh()
                return {
                    "action": "resume",
                    "config_path": config_path,
                    "job_name": job_name
                }
            elif ch in (ord('m'), ord('M')):
                # 메뉴로
                stdscr.clear()
                stdscr.refresh()
                return {"action": "menu"}
            elif ch in (ord('q'), ord('Q')):
                # 종료
                return {"action": "quit"}
            elif ch == 27:  # ESC
                stdscr.clear()
                stdscr.refresh()
                return {"action": "menu"}
    except Exception as e:
        logger.error(f"백업 플로우 에러: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return {"action": "menu"}


def show_backup_history_menu(stdscr, base_log_dir: Path):
    """백업 기록 보기 메뉴"""
    while True:
        stdscr.clear()
        rows, cols = stdscr.getmaxyx()
        
        title = " 📊 백업 기록 ".center(cols, "=")
        safe_addstr(stdscr, 0, 0, title)
        
        safe_addstr(stdscr, 2, 2, "백업 작업 기록을 확인할 수 있습니다.")
        safe_addstr(stdscr, 3, 2, "─" * (cols - 4))
        
        menu_row = 5
        safe_addstr(stdscr, menu_row, 2, "메뉴:")
        menu_row += 1
        safe_addstr(stdscr, menu_row, 4, "1) 📋 Journal 목록 (작업 로그)")
        menu_row += 1
        safe_addstr(stdscr, menu_row, 4, "2) 📸 Snapshot 목록 (백업 이력)")
        menu_row += 1
        safe_addstr(stdscr, menu_row, 4, "3) 📈 백업 통계")
        menu_row += 2
        safe_addstr(stdscr, menu_row, 4, "Q) 메인 메뉴로")
        
        safe_addstr(stdscr, rows - 1, 0, "[1-3] 메뉴 선택  [Q] 돌아가기")
        stdscr.refresh()
        
        ch = stdscr.getch()
        
        if ch in (ord('q'), ord('Q'), 27):
            break
        elif ch == ord('1'):
            try:
                show_journal_list_screen(stdscr, base_log_dir)
            except Exception as e:
                show_text_screen(stdscr, "오류", [f"저널 목록 표시 중 오류: {e}"])
        elif ch == ord('2'):
            try:
                show_snapshot_list_screen(stdscr, base_log_dir)
            except Exception as e:
                show_text_screen(stdscr, "오류", [f"스냅샷 목록 표시 중 오류: {e}"])
        elif ch == ord('3'):
            try:
                show_backup_statistics(stdscr, base_log_dir)
            except Exception as e:
                show_text_screen(stdscr, "오류", [f"통계 표시 중 오류: {e}"])


def show_backup_statistics(stdscr, base_log_dir: Path):
    """백업 통계 표시"""
    lines = []
    lines.append("")
    lines.append("  📈 백업 통계")
    lines.append(f"  {'─' * 78}")
    lines.append("")
    
    try:
        # Journal 통계
        journals = []
        for config_dir in base_log_dir.iterdir() if base_log_dir.exists() else []:
            if config_dir.is_dir():
                journals_dir = config_dir / "journals"
                if journals_dir.exists():
                    journals.extend(journals_dir.glob("journal_*.json"))
        
        total_journals = len(journals)
        success_count = 0
        failed_count = 0
        cancelled_count = 0
        
        for j in journals:
            try:
                data = load_journal(j)
                if data.status == "success":
                    success_count += 1
                elif data.status == "cancelled":
                    cancelled_count += 1
                else:
                    failed_count += 1
            except:
                pass
        
        lines.append(f"  📋 Journal 통계:")
        lines.append(f"     전체: {total_journals:,}개")
        lines.append(f"     ✅ 성공: {success_count:,}개 ({success_count*100//total_journals if total_journals else 0}%)")
        lines.append(f"     ⚠️  취소: {cancelled_count:,}개 ({cancelled_count*100//total_journals if total_journals else 0}%)")
        lines.append(f"     ❌ 실패: {failed_count:,}개 ({failed_count*100//total_journals if total_journals else 0}%)")
        lines.append("")
        
        # Snapshot 통계
        total_snapshots = 0
        total_files = 0
        
        for config_dir in base_log_dir.iterdir() if base_log_dir.exists() else []:
            if config_dir.is_dir():
                snapshots_root = config_dir / "snapshots"
                if snapshots_root.exists():
                    for job_dir in snapshots_root.iterdir():
                        if job_dir.is_dir():
                            index_file = job_dir / "index.json"
                            if index_file.exists():
                                try:
                                    with index_file.open("r") as f:
                                        index = json.load(f)
                                    total_snapshots += len(index)
                                    for entry in index:
                                        total_files += entry.get('file_count', 0)
                                except:
                                    pass
        
        lines.append(f"  📸 Snapshot 통계:")
        lines.append(f"     전체 스냅샷: {total_snapshots:,}개")
        lines.append(f"     백업된 파일: {total_files:,}개")
        lines.append("")
        
        # Config 통계
        configs = find_config_files()
        lines.append(f"  ⚙️  Config 통계:")
        lines.append(f"     전체 Config: {len(configs):,}개")
        
        total_jobs = 0
        for cfg in configs:
            try:
                jobs = load_config(cfg)
                total_jobs += len(jobs)
            except:
                pass
        
        lines.append(f"     전체 Job: {total_jobs:,}개")
        lines.append("")
        
    except Exception as e:
        lines.append(f"  ❌ 통계 수집 실패: {e}")
    
    show_text_screen(stdscr, "📈 백업 통계", lines)


def show_advanced_tools_menu(stdscr, base_log_dir: Path):
    """고급 도구 메뉴"""
    while True:
        stdscr.clear()
        rows, cols = stdscr.getmaxyx()
        
        title = " 🔧 고급 도구 ".center(cols, "=")
        safe_addstr(stdscr, 0, 0, title)
        
        safe_addstr(stdscr, 2, 2, "⚠️  주의: 이 도구들은 고급 사용자용입니다.")
        safe_addstr(stdscr, 3, 2, "    잘못 사용하면 데이터 손상이 발생할 수 있습니다.")
        safe_addstr(stdscr, 4, 2, "─" * (cols - 4))
        
        menu_row = 6
        safe_addstr(stdscr, menu_row, 2, "도구:")
        menu_row += 1
        safe_addstr(stdscr, menu_row, 4, "1) 🔄 최근 Journal 롤백")
        menu_row += 1
        safe_addstr(stdscr, menu_row, 4, "2) 🗑️  SafetyNet 정리")
        menu_row += 1
        safe_addstr(stdscr, menu_row, 4, "3) 🧹 로그 파일 정리")
        menu_row += 1
        safe_addstr(stdscr, menu_row, 4, "4) 🔍 Journal 상세 분석")
        menu_row += 2
        safe_addstr(stdscr, menu_row, 4, "Q) 메인 메뉴로")
        
        safe_addstr(stdscr, rows - 1, 0, "[1-4] 메뉴 선택  [Q] 돌아가기")
        stdscr.refresh()
        
        ch = stdscr.getch()
        
        if ch in (ord('q'), ord('Q'), 27):
            break
        elif ch == ord('1'):
            # 롤백
            try:
                interactive_latest_rollback_curses(stdscr, base_log_dir)
            except Exception as e:
                show_text_screen(stdscr, "오류", [f"롤백 중 오류: {e}"])
        elif ch == ord('2'):
            # SafetyNet 정리
            try:
                cleanup_safetynet_curses(stdscr)
            except Exception as e:
                show_text_screen(stdscr, "오류", [f"SafetyNet 정리 중 오류: {e}"])
        elif ch == ord('3'):
            # 로그 정리
            try:
                cleanup_logs_curses(stdscr, base_log_dir)
            except Exception as e:
                show_text_screen(stdscr, "오류", [f"로그 정리 중 오류: {e}"])
        elif ch == ord('4'):
            # Journal 분석
            try:
                analyze_journal_curses(stdscr, base_log_dir)
            except Exception as e:
                show_text_screen(stdscr, "오류", [f"Journal 분석 중 오류: {e}"])


def cleanup_safetynet_curses(stdscr):
    """SafetyNet 정리"""
    lines = []
    lines.append("")
    lines.append("  🗑️  SafetyNet 정리 기능")
    lines.append(f"  {'─' * 78}")
    lines.append("")
    lines.append("  SafetyNet 폴더의 오래된 파일을 삭제합니다.")
    lines.append("")
    lines.append("  ⚠️  이 작업은 구현 예정입니다.")
    lines.append("     현재는 수동으로 .SafetyNet 폴더를 정리하세요.")
    lines.append("")
    
    show_text_screen(stdscr, "SafetyNet 정리", lines)


def cleanup_logs_curses(stdscr, base_log_dir: Path):
    """로그 정리"""
    lines = []
    lines.append("")
    lines.append("  🧹 로그 파일 정리")
    lines.append(f"  {'─' * 78}")
    lines.append("")
    
    try:
        # 30일 이상 된 로그 파일 찾기
        from datetime import timedelta
        cutoff_date = datetime.now() - timedelta(days=30)
        
        old_logs = []
        for log_file in base_log_dir.glob("disk_sync_pro_*.log"):
            try:
                mtime = datetime.fromtimestamp(log_file.stat().st_mtime)
                if mtime < cutoff_date:
                    old_logs.append((log_file, mtime))
            except:
                pass
        
        if not old_logs:
            lines.append("  ✅ 정리할 로그 파일이 없습니다.")
            lines.append(f"     (30일 이내 파일만 보관됨)")
        else:
            lines.append(f"  발견된 오래된 로그: {len(old_logs)}개")
            lines.append("")
            for log_file, mtime in old_logs[:10]:
                lines.append(f"    - {log_file.name} ({mtime.strftime('%Y-%m-%d')})")
            
            if len(old_logs) > 10:
                lines.append(f"    ... 외 {len(old_logs) - 10}개")
            
            lines.append("")
            lines.append("  ⚠️  자동 정리 기능이 활성화되어 있습니다.")
            lines.append("     백업 실행 시 자동으로 정리됩니다.")
    
    except Exception as e:
        lines.append(f"  ❌ 로그 확인 실패: {e}")
    
    show_text_screen(stdscr, "로그 정리", lines)


def analyze_journal_curses(stdscr, base_log_dir: Path):
    """Journal 분석"""
    # Journal 선택
    journals = []
    for config_dir in base_log_dir.iterdir() if base_log_dir.exists() else []:
        if config_dir.is_dir():
            journals_dir = config_dir / "journals"
            if journals_dir.exists():
                journals.extend(journals_dir.glob("journal_*.json"))
    
    journals = sorted(journals, key=lambda p: p.stat().st_mtime, reverse=True)[:20]
    
    if not journals:
        show_text_screen(stdscr, "Journal 분석", ["", "  ⚠️  분석할 Journal이 없습니다.", ""])
        return
    
    stdscr.clear()
    rows, cols = stdscr.getmaxyx()
    safe_addstr(stdscr, 0, 0, " Journal 선택 ".center(cols, "="))
    
    row = 2
    for idx, j in enumerate(journals[:10], 1):
        if row >= rows - 3:
            break
        safe_addstr(stdscr, row, 2, f"{idx}) {j.name}")
        row += 1
    
    safe_addstr(stdscr, rows - 2, 0, "번호 입력 (0=취소):")
    stdscr.refresh()
    
    sel = curses_prompt(stdscr, "")
    if not sel or sel == '0':
        return
    
    try:
        idx = int(sel) - 1
        if 0 <= idx < len(journals):
            journal_path = journals[idx]
        else:
            return
    except ValueError:
        return
    
    # Journal 분석
    lines = []
    lines.append("")
    lines.append(f"  🔍 Journal 분석: {journal_path.name}")
    lines.append(f"  {'─' * 78}")
    lines.append("")
    
    try:
        journal = load_journal(journal_path)
        
        lines.append(f"  Job: {journal.job_name}")
        lines.append(f"  시간: {journal.timestamp}")
        lines.append(f"  상태: {journal.status}")
        lines.append(f"  대상: {journal.dest_root}")
        lines.append(f"  롤백 위치: {journal.rollback_root}")
        lines.append("")
        lines.append(f"  📊 작업 통계:")
        lines.append(f"     전체 작업: {len(journal.ops):,}개")
        
        # 작업 타입별 집계
        ops_by_type = {}
        for op in journal.ops:
            ops_by_type[op.action] = ops_by_type.get(op.action, 0) + 1
        
        for action, count in sorted(ops_by_type.items()):
            lines.append(f"     - {action}: {count:,}개")
        
        lines.append("")
        lines.append(f"  💾 파일 크기: {journal_path.stat().st_size / 1024:.1f} KB")
        
    except Exception as e:
        lines.append(f"  ❌ 분석 실패: {e}")
    
    show_text_screen(stdscr, "Journal 분석", lines)


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
        # 최근 checkpoint 확인
        latest_checkpoint = None
        for cp_file in sorted(base_log_dir.glob("checkpoint_*.json"), reverse=True):
            try:
                with cp_file.open("r") as f:
                    data = json.load(f)
                    if data.get("status") == "incomplete":
                        latest_checkpoint = {
                            "name": data.get("job_name"),
                            "processed": data.get("total_processed", 0)
                        }
                        break
            except Exception:
                pass
        
        menu_lines = [
            ("", 0),
            ("메뉴를 선택하세요:", 0),
            ("", 0),
            ("1) 🚀 백업 실행", 2),
            ("2) ⚙️  Config 관리", 2),
            ("3) 📊 백업 기록 보기", 2),
            ("4) 🔧 고급 도구", 2),
        ]
        
        # Resume 가능한 Job이 있으면 표시
        if latest_checkpoint:
            menu_lines.insert(4, ("", 0))
            menu_lines.insert(4, (f"   💾 Resume 가능: {latest_checkpoint['name']} ({latest_checkpoint['processed']:,}개 처리 완료)", 1))
        
        menu_lines.extend([
            ("", 0),
            ("Q) 종료", 2),
        ])

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
        help_line = "[1-4] 메뉴 선택  [Q] 종료  [ESC] 뒤로가기"
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
            # 🚀 백업 실행
            try:
                auto_resume_cfg = None
                
                while True:
                    result = interactive_backup_flow_curses(stdscr, base_log_dir, auto_resume_cfg)
                    
                    if not result:
                        break
                    
                    action = result.get("action", "menu")
                    
                    if action == "resume":
                        auto_resume_cfg = {
                            "config_path": result["config_path"],
                            "job_name": result["job_name"]
                        }
                        continue
                    elif action == "quit":
                        return
                    else:
                        break
            except Exception as e:
                import traceback
                show_text_screen(stdscr, "오류", [
                    f"백업 실행 중 오류: {e}",
                    "",
                    "상세:",
                    traceback.format_exc()
                ])
        elif ch == ord('2'):
            # ⚙️ Config 관리
            try:
                show_config_manager_main_menu(stdscr)
            except Exception as e:
                show_text_screen(stdscr, "오류", [f"Config 관리 중 오류: {e}"])
        elif ch == ord('3'):
            # 📊 백업 기록 보기
            try:
                show_backup_history_menu(stdscr, base_log_dir)
            except Exception as e:
                show_text_screen(stdscr, "오류", [f"백업 기록 표시 중 오류: {e}"])
        elif ch == ord('4'):
            # 🔧 고급 도구
            try:
                show_advanced_tools_menu(stdscr, base_log_dir)
            except Exception as e:
                show_text_screen(stdscr, "오류", [f"고급 도구 중 오류: {e}"])
        
        # 잠시 대기 (키 중복 입력 방지)
        try:
            stdscr.nodelay(True)
            while stdscr.getch() != -1:
                pass
            stdscr.nodelay(False)
        except Exception:
            pass


# ================ 텍스트 모드 Config 관리 =================

def config_manager_plain_menu():
    """텍스트 모드 Config 관리 메뉴"""
    while True:
        print("\n" + "=" * 60)
        print(" ⚙️  Config 관리 (CRUD) ".center(60))
        print("=" * 60)
        
        configs = find_config_files()
        print(f"📁 발견된 Config 파일: {len(configs)}개\n")
        
        print("1) 📋 Config 목록 보기")
        print("2) ➕ 새 Config 생성")
        print("3) ✏️  Config 수정")
        print("4) 🗑️  Config 삭제")
        print("5) 📝 템플릿으로 생성")
        print("6) ✅ Config 검증")
        print("Q) 메인 메뉴로")
        
        choice = input("\n선택> ").strip().lower()
        
        if choice == 'q':
            break
        elif choice == '1':
            show_config_list_plain()
        elif choice == '2':
            create_new_config_plain()
        elif choice == '3':
            update_config_plain()
        elif choice == '4':
            delete_config_plain()
        elif choice == '5':
            create_from_template_plain()
        elif choice == '6':
            validate_config_plain()


def show_config_list_plain():
    """Config 목록 보기 (텍스트)"""
    configs = find_config_files()
    
    print("\n" + "=" * 60)
    print(" 📋 Config 목록 ".center(60))
    print("=" * 60)
    
    if not configs:
        print("\n⚠️  Config 파일이 없습니다.\n")
        input("계속하려면 Enter...")
        return
    
    for idx, config_path in enumerate(configs, 1):
        print(f"\n[{idx}] 📄 {config_path.name}")
        print(f"경로: {config_path}")
        
        try:
            jobs = load_config(config_path)
            print(f"Job 수: {len(jobs)}")
            
            for job_idx, job in enumerate(jobs[:3], 1):
                print(f"  [{job_idx}] {job.name} [{job.mode}]")
                print(f"      src: {job.source}")
                print(f"      dst: {job.destination}")
            
            if len(jobs) > 3:
                print(f"  ... 외 {len(jobs) - 3}개 Job")
        except Exception as e:
            print(f"❌ 로드 실패: {e}")
        print("-" * 60)
    
    input("\n계속하려면 Enter...")


def create_new_config_plain():
    """새 Config 생성 (텍스트)"""
    print("\n" + "=" * 60)
    print(" ➕ 새 Config 생성 ".center(60))
    print("=" * 60)
    
    path_str = input("\nConfig 파일명 (예: my_backup.json): ").strip()
    if not path_str:
        return
    
    if not path_str.endswith('.json'):
        path_str += '.json'
    
    config_path = Path(path_str).expanduser()
    
    if config_path.exists():
        confirm = input(f"\n⚠️  파일이 이미 존재합니다. 덮어쓰시겠습니까? [y/N]: ").strip().lower()
        if confirm != 'y':
            return
    
    jobs = []
    manage_jobs_plain(config_path, jobs)


def update_config_plain():
    """Config 수정 (텍스트)"""
    configs = find_config_files()
    
    if not configs:
        print("\n⚠️  수정할 Config 파일이 없습니다.")
        input("계속하려면 Enter...")
        return
    
    print("\n" + "=" * 60)
    print(" Config 선택 ".center(60))
    print("=" * 60)
    
    for idx, cfg in enumerate(configs, 1):
        print(f"{idx}) {cfg}")
    
    sel = input("\n번호 선택 (0=취소): ").strip()
    try:
        idx = int(sel) - 1
        if 0 <= idx < len(configs):
            config_path = configs[idx]
        else:
            return
    except ValueError:
        return
    
    try:
        jobs = load_config(config_path)
    except Exception as e:
        print(f"\n❌ Config 읽기 실패: {e}")
        input("계속하려면 Enter...")
        return
    
    manage_jobs_plain(config_path, jobs)


def manage_jobs_plain(config_path: Path, jobs: List[BackupJob]):
    """Job 관리 (텍스트)"""
    while True:
        print("\n" + "=" * 60)
        print(f" Config 편집: {config_path.name} ".center(60))
        print("=" * 60)
        print(f"\n📦 현재 Job 수: {len(jobs)}개\n")
        
        if jobs:
            for idx, job in enumerate(jobs, 1):
                print(f"{idx}) {job.name} [{job.mode}]")
                print(f"   src: {job.source}")
                print(f"   dst: {job.destination}")
        else:
            print("⚠️  Job이 없습니다.\n")
        
        print("\n작업:")
        print("A) ➕ Job 추가")
        print("E) ✏️  Job 수정")
        print("D) 🗑️  Job 삭제")
        print("C) 📋 Job 복제")
        print("V) 👁️  Job 상세 보기")
        print("S) 💾 저장 후 종료")
        print("Q) 저장 않고 종료")
        
        choice = input("\n선택> ").strip().lower()
        
        if choice == 'q':
            confirm = input("저장하지 않고 종료하시겠습니까? [y/N]: ").strip().lower()
            if confirm == 'y':
                break
        elif choice == 's':
            if save_config_plain(config_path, jobs):
                print("\n✅ Config가 성공적으로 저장되었습니다.")
                input("계속하려면 Enter...")
                break
        elif choice == 'a':
            new_job = create_job_plain()
            if new_job:
                jobs.append(new_job)
                print("\n✅ Job이 추가되었습니다.")
        elif choice == 'e':
            if not jobs:
                print("\n⚠️  수정할 Job이 없습니다.")
                input("계속하려면 Enter...")
                continue
            job_idx = select_job_plain(jobs, "수정할 Job 선택")
            if job_idx is not None:
                edit_job_plain(jobs[job_idx])
        elif choice == 'd':
            if not jobs:
                print("\n⚠️  삭제할 Job이 없습니다.")
                input("계속하려면 Enter...")
                continue
            job_idx = select_job_plain(jobs, "삭제할 Job 선택")
            if job_idx is not None:
                confirm = input(f"\n'{jobs[job_idx].name}' Job을 삭제하시겠습니까? [y/N]: ").strip().lower()
                if confirm == 'y':
                    jobs.pop(job_idx)
                    print("\n✅ Job이 삭제되었습니다.")
        elif choice == 'c':
            if not jobs:
                print("\n⚠️  복제할 Job이 없습니다.")
                input("계속하려면 Enter...")
                continue
            job_idx = select_job_plain(jobs, "복제할 Job 선택")
            if job_idx is not None:
                original = jobs[job_idx]
                new_job = BackupJob(
                    name=f"{original.name}_copy",
                    source=original.source,
                    destination=original.destination,
                    mode=original.mode,
                    exclude=original.exclude.copy(),
                    safety_net_days=original.safety_net_days,
                    verify=original.verify,
                )
                jobs.append(new_job)
                print(f"\n✅ '{new_job.name}' Job이 복제되었습니다.")
        elif choice == 'v':
            if not jobs:
                print("\n⚠️  보기할 Job이 없습니다.")
                input("계속하려면 Enter...")
                continue
            job_idx = select_job_plain(jobs, "상세 보기할 Job 선택")
            if job_idx is not None:
                show_job_details_plain(jobs[job_idx])


def select_job_plain(jobs: List[BackupJob], title: str) -> Optional[int]:
    """Job 선택 (텍스트)"""
    print(f"\n{title}")
    for idx, job in enumerate(jobs, 1):
        print(f"{idx}) {job.name} [{job.mode}]")
    
    sel = input("\n번호 입력 (0=취소): ").strip()
    try:
        idx = int(sel) - 1
        if 0 <= idx < len(jobs):
            return idx
    except ValueError:
        pass
    return None


def create_job_plain() -> Optional[BackupJob]:
    """Job 생성 (텍스트)"""
    print("\n" + "=" * 60)
    print(" ➕ 새 Job 추가 ".center(60))
    print("=" * 60)
    
    name = input("\nJob 이름: ").strip()
    if not name:
        return None
    
    source = input("Source 경로: ").strip()
    if not source:
        return None
    
    destination = input("Destination 경로: ").strip()
    if not destination:
        return None
    
    print("\n백업 모드:")
    print("1) safety_net - 삭제 파일을 SafetyNet으로 이동 (권장)")
    print("2) sync       - 추가/변경만, 삭제 안함")
    print("3) clone      - 완전 미러링")
    mode_choice = input("선택 [1/2/3]: ").strip()
    mode_map = {'1': 'safety_net', '2': 'sync', '3': 'clone'}
    mode = mode_map.get(mode_choice, 'safety_net')
    
    excl = input("\n제외 패턴 (쉼표 구분, Enter=기본값): ").strip()
    if excl:
        exclude = [x.strip() for x in excl.split(",") if x.strip()]
    else:
        exclude = [".DS_Store", ".Spotlight-V100", ".fseventsd", "node_modules", ".terraform", "*.tmp"]
    
    days = input("SafetyNet 보관일 [30]: ").strip()
    safety_net_days = int(days) if days else 30
    
    verify = input("해시 검증 사용? [y/N]: ").strip().lower() == 'y'
    
    return BackupJob(
        name=name,
        source=Path(source).expanduser(),
        destination=Path(destination).expanduser(),
        mode=mode,
        exclude=exclude,
        safety_net_days=safety_net_days,
        verify=verify,
    )


def edit_job_plain(job: BackupJob):
    """Job 수정 (텍스트)"""
    print(f"\n=== Job 수정: {job.name} ===")
    
    name = input(f"Job 이름 [{job.name}]: ").strip()
    if name:
        job.name = name
    
    source = input(f"Source [{job.source}]: ").strip()
    if source:
        job.source = Path(source).expanduser()
    
    dest = input(f"Destination [{job.destination}]: ").strip()
    if dest:
        job.destination = Path(dest).expanduser()
    
    print(f"현재 모드: {job.mode}")
    print("1) safety_net 2) sync 3) clone")
    mode_choice = input("변경 [Enter=유지]: ").strip()
    mode_map = {'1': 'safety_net', '2': 'sync', '3': 'clone'}
    if mode_choice in mode_map:
        job.mode = mode_map[mode_choice]
    
    print("\n✅ Job이 수정되었습니다.")


def show_job_details_plain(job: BackupJob):
    """Job 상세 정보 (텍스트)"""
    print("\n" + "=" * 60)
    print(f" 📦 Job: {job.name} ".center(60))
    print("=" * 60)
    print(f"모드: {job.mode}")
    print(f"소스: {job.source}")
    print(f"대상: {job.destination}")
    print(f"제외: {', '.join(job.exclude) if job.exclude else '(없음)'}")
    print(f"SafetyNet: {job.safety_net_days}일")
    print(f"해시 검증: {'ON' if job.verify else 'OFF'}")
    
    if job.source.exists():
        print(f"\n✅ 소스 존재")
    else:
        print(f"\n❌ 소스 없음")
    
    if job.destination.exists():
        print(f"✅ 대상 존재")
    else:
        print(f"⚠️  대상 없음 (백업 시 생성)")
    
    input("\n계속하려면 Enter...")


def save_config_plain(config_path: Path, jobs: List[BackupJob]) -> bool:
    """Config 저장 (텍스트)"""
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
        tmp_path = config_path.with_suffix('.tmp')
        with tmp_path.open("w", encoding="utf-8") as f:
            json.dump(raw, f, indent=2, ensure_ascii=False)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, config_path)
        return True
    except Exception as e:
        print(f"\n❌ 저장 실패: {e}")
        try:
            if tmp_path.exists():
                tmp_path.unlink()
        except Exception:
            pass
        return False


def delete_config_plain():
    """Config 삭제 (텍스트)"""
    configs = find_config_files()
    
    if not configs:
        print("\n⚠️  삭제할 Config 파일이 없습니다.")
        input("계속하려면 Enter...")
        return
    
    print("\n" + "=" * 60)
    print(" Config 선택 ".center(60))
    print("=" * 60)
    
    for idx, cfg in enumerate(configs, 1):
        print(f"{idx}) {cfg}")
    
    sel = input("\n번호 선택 (0=취소): ").strip()
    try:
        idx = int(sel) - 1
        if 0 <= idx < len(configs):
            config_path = configs[idx]
        else:
            return
    except ValueError:
        return
    
    print(f"\n⚠️  다음 Config를 삭제하시겠습니까?")
    print(f"   {config_path}")
    confirm = input("\n삭제하려면 'DELETE'를 입력: ").strip()
    
    if confirm == "DELETE":
        try:
            config_path.unlink()
            print("\n✅ Config가 삭제되었습니다.")
        except Exception as e:
            print(f"\n❌ 삭제 실패: {e}")
    else:
        print("\n취소되었습니다.")
    
    input("계속하려면 Enter...")


def create_from_template_plain():
    """템플릿으로 Config 생성 (텍스트)"""
    print("\n" + "=" * 60)
    print(" 📝 템플릿 선택 ".center(60))
    print("=" * 60)
    
    print("\n1) 기본 백업 (Safety Net)")
    print("2) 완전 미러링 (Clone)")
    print("3) 증분 동기화 (Sync)")
    
    choice = input("\n선택 [1/2/3]: ").strip()
    
    templates = {
        "1": ("safety_net", False, [".DS_Store", ".fseventsd", "node_modules", "*.tmp"]),
        "2": ("clone", True, [".DS_Store", "*.tmp"]),
        "3": ("sync", False, [".DS_Store", "node_modules", "*.log"])
    }
    
    if choice not in templates:
        return
    
    mode, verify, exclude = templates[choice]
    
    path = input("\nConfig 파일명: ").strip()
    if not path:
        return
    if not path.endswith('.json'):
        path += '.json'
    
    name = input("Job 이름: ").strip()
    source = input("Source 경로: ").strip()
    destination = input("Destination 경로: ").strip()
    
    if not (name and source and destination):
        return
    
    job = BackupJob(
        name=name,
        source=Path(source).expanduser(),
        destination=Path(destination).expanduser(),
        mode=mode,
        exclude=exclude,
        safety_net_days=30,
        verify=verify,
    )
    
    config_path = Path(path).expanduser()
    if save_config_plain(config_path, [job]):
        print(f"\n✅ 템플릿 기반 Config가 생성되었습니다!")
        input("계속하려면 Enter...")


def validate_config_plain():
    """Config 검증 (텍스트)"""
    configs = find_config_files()
    
    if not configs:
        print("\n⚠️  검증할 Config 파일이 없습니다.")
        input("계속하려면 Enter...")
        return
    
    print("\n" + "=" * 60)
    print(" Config 선택 ".center(60))
    print("=" * 60)
    
    for idx, cfg in enumerate(configs, 1):
        print(f"{idx}) {cfg}")
    
    sel = input("\n번호 선택 (0=취소): ").strip()
    try:
        idx = int(sel) - 1
        if 0 <= idx < len(configs):
            config_path = configs[idx]
        else:
            return
    except ValueError:
        return
    
    print("\n" + "=" * 60)
    print(f" ✅ Config 검증: {config_path.name} ".center(60))
    print("=" * 60)
    
    try:
        jobs = load_config(config_path)
        print(f"\n✅ JSON 파싱 성공")
        print(f"✅ Job 로드 성공: {len(jobs)}개\n")
        
        for idx, job in enumerate(jobs, 1):
            print(f"[{idx}] {job.name}")
            print(f"    소스: {'✅ 존재' if job.source.exists() else '❌ 없음'}")
            print(f"    대상: {'✅ 존재' if job.destination.exists() else '⚠️  없음 (생성예정)'}")
            print(f"    모드: {job.mode}")
            print()
        
        print("✅ 전체 검증 완료!")
    except Exception as e:
        print(f"\n❌ 검증 실패: {e}")
    
    input("\n계속하려면 Enter...")


# ================ 텍스트 모드 메인 메뉴 (curses 불가 시) =================

def interactive_main_plain():
    base_log_dir = Path(__file__).resolve().parent / "logs"
    base_log_dir.mkdir(parents=True, exist_ok=True)

    while True:
        print("=" * 60)
        print(" DiskSyncPro - Main Menu ".center(60))
        print("=" * 60)
        print("1) 🚀 백업 실행")
        print("2) ⚙️  Config 관리")
        print("3) 📊 백업 기록 보기")
        print("4) 🔧 고급 도구")
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
            # ⚙️ Config 관리
            config_manager_plain_menu()

        elif choice == '3':
            # 📊 백업 기록 보기
            backup_history_plain_menu(base_log_dir)

        elif choice == '4':
            # 🔧 고급 도구
            advanced_tools_plain_menu(base_log_dir)

        else:
            continue


def backup_history_plain_menu(base_log_dir: Path):
    """백업 기록 메뉴 (텍스트)"""
    while True:
        print("\n" + "=" * 60)
        print(" 📊 백업 기록 ".center(60))
        print("=" * 60)
        print("\n1) 📋 Journal 목록 (작업 로그)")
        print("2) 📸 Snapshot 목록 (백업 이력)")
        print("Q) 메인 메뉴로")
        
        choice = input("\n선택> ").strip().lower()
        
        if choice == 'q':
            break
        elif choice == '1':
            # Journal 목록
            journals = []
            for config_dir in base_log_dir.iterdir() if base_log_dir.exists() else []:
                if config_dir.is_dir():
                    journals_dir = config_dir / "journals"
                    if journals_dir.exists():
                        journals.extend(journals_dir.glob("journal_*.json"))
            
            journals = sorted(journals)
            
            if not journals:
                print("\n⚠️  journal 파일이 없습니다.")
            else:
                print("\n" + "=" * 60)
                print(f" Journal 목록 (최근 100개) ".center(60))
                print("=" * 60)
                for j in journals[-100:]:
                    try:
                        data = load_journal(j)
                        print(f"{j.name} | job={data.job_name} | ts={data.timestamp} | status={data.status}")
                    except:
                        print(f"{j.name} | (읽기 실패)")
            input("\n계속하려면 Enter...")
        
        elif choice == '2':
            # Snapshot 목록
            config_found = False
            for config_dir in sorted(base_log_dir.iterdir()) if base_log_dir.exists() else []:
                if not config_dir.is_dir():
                    continue
                snapshots_root = config_dir / "snapshots"
                if not snapshots_root.exists():
                    continue
                
                print(f"\n╔{'═' * 60}╗")
                print(f"║ Config: {config_dir.name:<52} ║")
                print(f"╚{'═' * 60}╝")
                config_found = True
                
                for job_dir in sorted(snapshots_root.iterdir()):
                    if not job_dir.is_dir():
                        continue
                    index_file = job_dir / "index.json"
                    if not index_file.exists():
                        print(f"  [{job_dir.name}] index.json 없음")
                        continue
                    try:
                        with index_file.open("r", encoding="utf-8") as f:
                            index = json.load(f)
                        print(f"\n  Job: {job_dir.name} (snapshots: {len(index)})")
                        for entry in index[-5:]:
                            print(
                                f"    ts={entry.get('timestamp')} | count={entry.get('file_count'):,}"
                            )
                    except:
                        print(f"  [{job_dir.name}] 읽기 실패")
            
            if not config_found:
                print("\n⚠️  snapshots 디렉토리가 없습니다.")
            input("\n계속하려면 Enter...")


def advanced_tools_plain_menu(base_log_dir: Path):
    """고급 도구 메뉴 (텍스트)"""
    while True:
        print("\n" + "=" * 60)
        print(" 🔧 고급 도구 ".center(60))
        print("=" * 60)
        print("\n⚠️  주의: 이 도구들은 고급 사용자용입니다.")
        print("    잘못 사용하면 데이터 손상이 발생할 수 있습니다.")
        print("\n1) 🔄 최근 Journal 롤백")
        print("2) 🧹 로그 파일 정리")
        print("Q) 메인 메뉴로")
        
        choice = input("\n선택> ").strip().lower()
        
        if choice == 'q':
            break
        elif choice == '1':
            # 롤백
            latest = get_latest_journal(base_log_dir)
            if not latest:
                print("\n⚠️  최근 저널 파일을 찾을 수 없습니다.")
                input("계속하려면 Enter...")
                continue
            
            try:
                j = load_journal(latest)
                print(f"\n최근 저널: {latest.name}")
                print(f"Job: {j.job_name}")
                print(f"시간: {j.timestamp}")
                print(f"상태: {j.status}")
                print(f"\n⚠️  롤백은 신중하게 사용하세요!")
                print("   백업된 파일이 원래 상태로 되돌아갑니다.")
                
                yn = input("\n이 저널로 롤백할까요? [y/N]: ").strip().lower()
                if yn == 'y':
                    print("\n롤백 실행 중...")
                    rollback_journal(j, dry_run=False)
                    j.status = "rolled_back"
                    save_journal(j, latest)
                    print("✅ 롤백 완료.")
                else:
                    print("❌ 롤백 취소.")
            except Exception as e:
                print(f"\n❌ 롤백 실패: {e}")
            
            input("\n계속하려면 Enter...")
        
        elif choice == '2':
            # 로그 정리
            print("\n로그 파일 정리 (30일 이상)")
            try:
                from datetime import timedelta
                cutoff_date = datetime.now() - timedelta(days=30)
                
                old_logs = []
                for log_file in base_log_dir.glob("disk_sync_pro_*.log"):
                    try:
                        mtime = datetime.fromtimestamp(log_file.stat().st_mtime)
                        if mtime < cutoff_date:
                            old_logs.append(log_file)
                    except:
                        pass
                
                if not old_logs:
                    print("\n✅ 정리할 로그 파일이 없습니다.")
                else:
                    print(f"\n발견된 오래된 로그: {len(old_logs)}개")
                    print("\n⚠️  자동 정리 기능이 활성화되어 있습니다.")
                    print("   백업 실행 시 자동으로 정리됩니다.")
            except Exception as e:
                print(f"\n❌ 로그 확인 실패: {e}")
            
            input("\n계속하려면 Enter...")


# ================ main 진입점 =================

def main() -> None:
    # 프로그램 중복 실행 방지
    lock_file = Path(__file__).parent / "logs" / ".disk_sync_pro.lock"
    instance_lock = SingleInstanceLock(lock_file)
    
    if not instance_lock.acquire():
        # PID 파일에서 정보 읽기
        pid_file = lock_file.with_suffix('.pid')
        pid = "unknown"
        timestamp = "unknown"
        
        try:
            with open(pid_file, 'r') as f:
                lines = f.readlines()
                pid = lines[0].strip() if len(lines) > 0 else "unknown"
                timestamp = lines[1].strip() if len(lines) > 1 else "unknown"
        except Exception:
            # PID 파일 읽기 실패 시 lock 파일에서 시도
            try:
                with open(lock_file, 'r') as f:
                    lines = f.readlines()
                    pid = lines[0].strip() if len(lines) > 0 else "unknown"
                    timestamp = lines[1].strip() if len(lines) > 1 else "unknown"
            except Exception:
                pass
        
        error_msg = f"""
╔══════════════════════════════════════════════════════════════╗
║                   프로그램 중복 실행 감지                    ║
╚══════════════════════════════════════════════════════════════╝

DiskSyncPro가 이미 실행 중입니다.

실행 중인 프로세스:
  - PID: {pid}
  - 시작 시간: {timestamp}
  - Lock 파일: {lock_file}

동시에 여러 백업을 실행하면 데이터 손상이 발생할 수 있습니다.

해결 방법:
  1. 실행 중인 프로그램이 종료될 때까지 기다리세요
  2. 프로그램이 비정상 종료되었다면:
     rm {lock_file}
     rm {pid_file}
"""
        print(error_msg, file=sys.stderr)
        sys.exit(1)
    
    try:
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
    finally:
        # 명시적으로 lock 해제 (atexit에서도 해제되지만 확실하게)
        instance_lock.release()


if __name__ == "__main__":
    main()
