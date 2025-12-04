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

주의:
- macOS 데이터 디스크/외장 SSD 백업을 염두에 둔 스크립트
- 시스템 부팅 볼륨 완전 클론, APFS 스냅샷 수준까지는 아님
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


# ================ 전역 설정 =================

MAX_COPY_RETRY = 3
HASH_ALGO = "sha256"


# ================ 로깅 설정 =================

def setup_logger(log_file: Optional[Path] = None, verbose: bool = True) -> None:
    logger = logging.getLogger("disk_sync_pro")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    if verbose:
        ch = logging.StreamHandler(sys.stdout)
        ch.setFormatter(fmt)
        logger.addHandler(ch)

    if log_file:
        fh = logging.FileHandler(log_file, encoding="utf-8")
        fh.setFormatter(fmt)
        logger.addHandler(fh)


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
    status: str                    # "pending" | "success" | "rolled_back" | "rollback_failed"
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

    # 혹시 남아있을 수도 있는 이전 tmp 파일 삭제 시도
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


def save_journal(journal: Journal, path: Path) -> None:
    serializable = {
        "job_name": journal.job_name,
        "timestamp": journal.timestamp,
        "dest_root": journal.dest_root,
        "rollback_root": journal.rollback_root,
        "status": journal.status,
        "ops": [asdict(op) for op in journal.ops],
    }
    with path.open("w", encoding="utf-8") as f:
        json.dump(serializable, f, indent=2, ensure_ascii=False)


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
    """
    path = log_dir / f"checkpoint_{job.name}.json"
    if path.exists():
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        status = data.get("status", "incomplete")
        if status == "incomplete":
            processed = set(data.get("processed_files", []))
        else:
            processed = set()
    else:
        status = "incomplete"
        processed = set()
    cp = {
        "job_name": job.name,
        "status": status,
        "processed": processed,
        "path": path,
    }
    return cp


def save_checkpoint(cp: dict) -> None:
    if cp is None:
        return
    path: Path = cp["path"]
    data = {
        "job_name": cp["job_name"],
        "status": cp["status"],
        "processed_files": sorted(list(cp["processed"])),
        "last_updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def add_processed_file(cp: Optional[dict], rel_path: str) -> None:
    """
    체크포인트에 '이미 처리된 파일'로 기록.
    """
    if cp is None:
        return
    if rel_path in cp["processed"]:
        return
    cp["processed"].add(rel_path)
    save_checkpoint(cp)


# ================ 진행률 계산용 =================

def count_total_files_for_job(job: BackupJob) -> int:
    """
    진행률 계산을 위해 소스 아래 '대상 파일 수'를 미리 샘.
    exclude 패턴에 걸리는 파일은 제외.
    """
    total = 0
    for root, dirs, files in os.walk(job.source):
        root_path = Path(root)
        # 제외 디렉토리 필터링
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
            # 새로 만든 파일 삭제
            if target.exists():
                logger.info(f"[ROLLBACK delete created file] {target}")
                if not dry_run:
                    try:
                        target.unlink()
                    except Exception as e:
                        logger.error(f"롤백: 파일 삭제 실패 {target}: {e}")

        elif op.action in ("replace_file", "delete_file"):
            # backup 을 원래 위치로 되돌림
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
            # 새로 만든 디렉토리 삭제 시도 (비어있을 때만)
            if target.exists() and target.is_dir():
                try:
                    target.rmdir()
                    logger.info(f"[ROLLBACK rmdir] {target}")
                except OSError:
                    # 비어 있지 않으면 무시
                    pass

    logger.info("=== 롤백 종료 ===")


# ================ 핵심 백업 로직 =================

def copy_with_retry(src: Path, dst: Path, verify: bool, journal: Journal,
                    stats: Stats, dry_run: bool = False) -> bool:
    """
    원자적 복사 + 재시도 + 해시 검증 + 저널 기록
    실패 시 예외를 올리지 않고 False 를 반환해서
    해당 파일만 스킵하도록 동작.
    """
    # dst 가 이미 존재하면 replace_file, 없으면 create_file
    action = "replace_file" if dst.exists() else "create_file"
    backup_path = None

    # 기존 파일 백업(rollback 용)
    if action == "replace_file" and not dry_run:
        backup_path = Path(journal.rollback_root) / dst.relative_to(Path(journal.dest_root))
        backup_path.parent.mkdir(parents=True, exist_ok=True)
        logger.info(f"[BACKUP(before replace)] {dst} -> {backup_path}")
        try:
            shutil.copy2(dst, backup_path)
        except Exception as e:
            logger.error(f"[BACKUP 실패] {dst} -> {backup_path}: {e}")

    # 실제 복사
    if dry_run:
        logger.info(f"[COPY (dry-run)] {src} -> {dst}")
        journal.ops.append(JournalOp(
            action=action,
            target=str(dst),
            backup=str(backup_path) if backup_path else None
        ))
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
            stats.copy_failed += 1
            return False

    # 성공 시에만 저널 기록
    journal.ops.append(JournalOp(
        action=action,
        target=str(dst),
        backup=str(backup_path) if backup_path else None
    ))
    if action == "create_file":
        stats.created_files += 1
    else:
        stats.replaced_files += 1
    return True


# ================ Snapshot & Summary =================

def build_snapshot(job: BackupJob, journal: Journal, log_dir: Path) -> Path:
    """
    백업 완료 후 destination 전체 스냅샷(manifest) 생성.
    snapshots/<job_name>/snapshot_<timestamp>.json
    + index.json 업데이트.
    """
    dest_root = job.destination
    snapshot_dir = log_dir / "snapshots" / job.name
    snapshot_dir.mkdir(parents=True, exist_ok=True)

    files_manifest = []

    for root, dirs, files in os.walk(dest_root):
        root_path = Path(root)
        # 내부 관리 디렉토리는 스냅샷에서 제외
        if any(x in root_path.parts for x in (".Rollback", ".SafetyNet")):
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

    snapshot_file = snapshot_dir / f"snapshot_{journal.timestamp}.json"
    with snapshot_file.open("w", encoding="utf-8") as f:
        json.dump(snapshot_data, f, indent=2, ensure_ascii=False)

    # index.json 업데이트
    index_file = snapshot_dir / "index.json"
    if index_file.exists():
        with index_file.open("r", encoding="utf-8") as f:
            index = json.load(f)
    else:
        index = []

    index.append({
        "timestamp": journal.timestamp,
        "snapshot_file": snapshot_file.name,
        "file_count": len(files_manifest),
        "generated_at": snapshot_data["generated_at"],
    })

    with index_file.open("w", encoding="utf-8") as f:
        json.dump(index, f, indent=2, ensure_ascii=False)

    return snapshot_file


def write_summary(job: BackupJob, journal: Journal, stats: Stats, log_dir: Path) -> Path:
    """
    변경 요약 리포트 JSON 생성.
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

    summary_file = log_dir / f"summary_{job.name}_{journal.timestamp}.json"
    with summary_file.open("w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)

    # 로그에도 간단 요약 출력
    logger.info("=== 변경 요약 ===")
    for k, v in summary["stats"].items():
        logger.info(f"{k}: {v}")
    logger.info("================")

    return summary_file


# ================ Backup 실행 =================

def perform_backup(job: BackupJob, dry_run: bool, log_dir: Path, resume: bool) -> None:
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

    # 진행률용 전체 파일 수 계산
    total_files = count_total_files_for_job(job)
    if total_files == 0:
        logger.info("처리할 대상 파일이 없습니다. (0개)")
    else:
        logger.info(f"총 처리 대상 파일 수: {total_files}")

    # 저널 준비
    journal = prepare_journal(job)
    journal_file = journal_path_for(job, log_dir, journal.timestamp)
    logger.info(f"저널 파일: {journal_file}")
    save_journal(journal, journal_file)

    # 통계
    stats = Stats()

    # 체크포인트 (dry_run 이면 사용 안 함)
    cp = None
    already_processed = 0
    if resume and not dry_run:
        cp = load_or_init_checkpoint(job, log_dir)
        cp["status"] = "incomplete"
        already_processed = len(cp["processed"])
        save_checkpoint(cp)
        if total_files > 0 and already_processed > 0:
            logger.info(f"Resume 기준 이미 처리된 파일: {already_processed}개")

    # 진행률 상태 변수
    current_processed = min(already_processed, total_files) if total_files > 0 else 0
    last_percent = int(current_processed * 100 / total_files) if total_files > 0 else 100

    if total_files > 0:
        logger.info(f"[PROGRESS] {job.name}: {last_percent}% ({current_processed}/{total_files})")

    def report_progress():
        nonlocal current_processed, last_percent
        if total_files == 0:
            return
        current_processed += 1
        if current_processed > total_files:
            current_processed = total_files
        percent = int(current_processed * 100 / total_files)
        if percent > last_percent:
            last_percent = percent
            logger.info(f"[PROGRESS] {job.name}: {percent}% ({current_processed}/{total_files})")

    try:
        # 1) 소스 기준 복사/업데이트
        for root, dirs, files in os.walk(job.source):
            root_path = Path(root)

            # 제외 디렉토리 필터링
            dirs[:] = [d for d in dirs if not path_matches_patterns(root_path / d, job.exclude)]

            rel_root = root_path.relative_to(job.source)
            dest_root = job.destination / rel_root

            ensure_dir(dest_root, journal=journal, stats=stats, dry_run=dry_run)

            for file in files:
                src_file = root_path / file
                if path_matches_patterns(src_file, job.exclude):
                    stats.skipped_excluded += 1
                    continue

                rel_path = src_file.relative_to(job.source).as_posix()

                # resume 체크: 이미 처리한 파일이면 스킵 (진행률도 이미 반영된 상태로 봄)
                if cp is not None and rel_path in cp["processed"]:
                    continue

                dst_file = dest_root / file

                # 같으면 스킵 (그래도 한 파일은 검사한 것이므로 진행률 1 step)
                if dst_file.exists() and is_same_file(src_file, dst_file):
                    stats.skipped_same += 1
                    add_processed_file(cp, rel_path)
                    report_progress()
                    continue

                ok = copy_with_retry(
                    src_file,
                    dst_file,
                    verify=job.verify,
                    journal=journal,
                    stats=stats,
                    dry_run=dry_run,
                )
                if ok:
                    add_processed_file(cp, rel_path)
                # 실패하더라도 "이 파일에 대한 시도"는 했으므로 진행률 1 step
                report_progress()

        # 2) clone/safety_net 모드에서, 소스에 없는 파일 정리
        if job.mode in ("clone", "safety_net"):
            for root, dirs, files in os.walk(job.destination):
                root_path = Path(root)

                # .Rollback / .SafetyNet 은 건드리지 않음
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
                        # 소스에 없는 파일: 삭제 or SafetyNet 이동
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
                            except Exception as e:
                                logger.error(f"[SafetyNet 이동 실패] {dst_file}: {e}")

            # clone 모드에서 비어있는 디렉토리 정리
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
                            # 비어 있지 않으면 무시
                            pass

        # 안전하게 마지막 100% 보정
        if total_files > 0 and current_processed < total_files:
            current_processed = total_files
            logger.info(f"[PROGRESS] {job.name}: 100% ({current_processed}/{total_files})")

        journal.status = "success"
        save_journal(journal, journal_file)

        # 체크포인트 완료 표시
        if cp is not None and not dry_run:
            cp["status"] = "complete"
            save_checkpoint(cp)

        # 스냅샷 & 요약 리포트 생성 (dry-run 이 아닐 때만)
        if not dry_run:
            snapshot_file = build_snapshot(job, journal, log_dir)
            summary_file = write_summary(job, journal, stats, log_dir)
            logger.info(f"스냅샷 파일: {snapshot_file}")
            logger.info(f"요약 리포트: {summary_file}")

        logger.info(f"=== Job 성공: {job.name} ===")

    except Exception as e:
        logger.error(f"Job 중 에러 발생: {e}")
        # 자동 롤백 시도
        try:
            rollback_journal(journal, dry_run=dry_run)
            journal.status = "rolled_back"
        except Exception as re:
            logger.error(f"자동 롤백 실패: {re}")
            journal.status = "rollback_failed"
        finally:
            save_journal(journal, journal_file)
        logger.error(f"=== Job 실패 및 롤백 처리 완료 (status={journal.status}) ===")


# ================ CLI =================

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="CCC + SuperDuper 스타일의 프로급 디스크 백업/동기화 스크립트"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # backup 명령
    backup_parser = subparsers.add_parser("backup", help="백업 실행")
    backup_parser.add_argument("-c", "--config", required=True, help="백업 설정 JSON 파일 경로")
    backup_parser.add_argument("-j", "--job", help="실행할 Job 이름 (생략 시 전체 Job 실행)")
    backup_parser.add_argument("--dry-run", action="store_true", help="실제 복사/삭제 없이 시뮬레이션만 수행")
    backup_parser.add_argument("--log-dir", help="로그/저널 저장 디렉토리 (기본: ./logs)")
    backup_parser.add_argument("--resume", action="store_true",
                               help="이전 체크포인트를 사용해 중단된 백업을 이어서 실행")

    # rollback 명령
    rollback_parser = subparsers.add_parser("rollback", help="기존 저널 파일을 이용해 롤백 실행")
    rollback_parser.add_argument("-f", "--journal-file", required=True, help="저널 JSON 파일 경로")
    rollback_parser.add_argument("--dry-run", action="store_true", help="실제 롤백 없이 시뮬레이션")

    return parser.parse_args()


def main_backup(args: argparse.Namespace) -> None:
    config_path = Path(args.config).expanduser()
    if not config_path.exists():
        print(f"설정 파일을 찾을 수 없습니다: {config_path}", file=sys.stderr)
        sys.exit(1)

    # 로그 디렉토리
    if args.log_dir:
        log_dir = Path(args.log_dir).expanduser()
    else:
        log_dir = Path(__file__).resolve().parent / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    # 로그 파일
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = log_dir / f"disk_sync_pro_{ts}.log"
    setup_logger(log_file=log_file, verbose=True)

    logger.info(f"설정 파일: {config_path}")
    logger.info(f"로그 파일: {log_file}")

    jobs = load_config(config_path)
    if args.job:
        jobs = [job for job in jobs if job.name == args.job]
        if not jobs:
            logger.error(f"해당 이름의 Job을 찾을 수 없습니다: {args.job}")
            sys.exit(1)

    for job in jobs:
        perform_backup(job, dry_run=args.dry_run, log_dir=log_dir, resume=args.resume)

    logger.info("모든 Job이 완료되었습니다.")


def main_rollback(args: argparse.Namespace) -> None:
    journal_path = Path(args.journal_file).expanduser()
    if not journal_path.exists():
        print(f"저널 파일을 찾을 수 없습니다: {journal_path}", file=sys.stderr)
        sys.exit(1)

    # 롤백도 로그 남김
    log_dir = journal_path.parent
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = log_dir / f"disk_sync_pro_rollback_{ts}.log"
    setup_logger(log_file=log_file, verbose=True)

    logger.info(f"저널 파일: {journal_path}")
    journal = load_journal(journal_path)
    logger.info(f"Journal status: {journal.status}")

    try:
        rollback_journal(journal, dry_run=args.dry_run)
        if not args.dry_run:
            journal.status = "rolled_back"
            save_journal(journal, journal_path)
    except Exception as e:
        logger.error(f"롤백 중 에러: {e}")
        if not args.dry_run:
            journal.status = "rollback_failed"
            save_journal(journal, journal_path)


def main() -> None:
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
