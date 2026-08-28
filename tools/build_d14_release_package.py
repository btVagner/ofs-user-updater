from __future__ import annotations

import argparse
import hashlib
import json
import re
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List, Tuple

ROOT_DIR = Path(__file__).resolve().parents[1]

EXCLUDED_DIRS = {
    ".git",
    ".venv",
    "venv",
    "instance",
    "temp_uploads",
    "__pycache__",
    ".pytest_cache",
    ".mypy_cache",
    ".ruff_cache",
    "node_modules",
}
EXCLUDED_FILENAMES = {".env"}
EXCLUDED_SUFFIXES = {".pyc", ".pyo"}
FORBIDDEN_ARCHIVE_PREFIXES = (".git/", "venv/", ".venv/", "instance/", "temp_uploads/")
FORBIDDEN_SECRET_SUFFIXES = {".pem", ".key", ".p12", ".pfx", ".jks", ".keystore"}
TEXT_SCAN_SUFFIXES = {".py", ".js", ".css", ".html", ".md", ".txt", ".sql", ".service", ".timer", ".example"}
SECRET_PATTERNS = (
    re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----"),
    re.compile(r"(?i)\bAuthorization\s*:\s*Bearer\s+[A-Za-z0-9._~-]{20,}"),
    re.compile(r"(?i)\b(?:DB_PASSWORD|CLIENT_SECRET|ACCESS_TOKEN|REFRESH_TOKEN)\s*=\s*['\"][^'\"$<{][^'\"]{7,}['\"]"),
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Gera ZIP sanitizado da Demanda 14 e calcula SHA-256.")
    parser.add_argument(
        "--output",
        default=str(ROOT_DIR.parent / "ofs-updater-d14-release.zip"),
        help="Destino do ZIP final.",
    )
    parser.add_argument("--manifest", help="Destino opcional do manifesto JSON.")
    return parser.parse_args()


def _is_excluded(relative: Path, output: Path) -> bool:
    if any(part in EXCLUDED_DIRS for part in relative.parts[:-1]):
        return True
    if relative.name in EXCLUDED_FILENAMES:
        return True
    if relative.suffix.lower() in EXCLUDED_SUFFIXES:
        return True
    try:
        if (ROOT_DIR / relative).resolve() == output.resolve():
            return True
    except FileNotFoundError:
        pass
    return False


def _iter_release_files(output: Path) -> Iterable[Tuple[Path, Path]]:
    for path in sorted(ROOT_DIR.rglob("*")):
        if not path.is_file():
            continue
        relative = path.relative_to(ROOT_DIR)
        if _is_excluded(relative, output):
            continue
        yield path, relative


def _scan_file_for_secrets(path: Path, relative: Path) -> List[str]:
    issues: List[str] = []
    if relative.suffix.lower() in FORBIDDEN_SECRET_SUFFIXES:
        issues.append(f"forbidden_secret_file:{relative.as_posix()}")
        return issues
    if relative.suffix.lower() not in TEXT_SCAN_SUFFIXES and relative.name not in {".gitignore"}:
        return issues
    try:
        text = path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        return issues
    for index, pattern in enumerate(SECRET_PATTERNS, start=1):
        if pattern.search(text):
            issues.append(f"secret_pattern_{index}:{relative.as_posix()}")
    return issues


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _validate_archive(output: Path) -> dict:
    forbidden: List[str] = []
    with zipfile.ZipFile(output, "r") as archive:
        names = archive.namelist()
        for name in names:
            normalized = name.lstrip("./")
            if normalized == ".env" or any(normalized.startswith(prefix) for prefix in FORBIDDEN_ARCHIVE_PREFIXES):
                forbidden.append(name)
            if normalized.endswith((".pyc", ".pyo")) or "/__pycache__/" in f"/{normalized}":
                forbidden.append(name)
        return {"files": len(names), "forbidden_members": sorted(set(forbidden))}


def main() -> int:
    args = _parse_args()
    output = Path(args.output).expanduser().resolve()
    output.parent.mkdir(parents=True, exist_ok=True)

    release_files = list(_iter_release_files(output))
    secret_issues: List[str] = []
    for path, relative in release_files:
        secret_issues.extend(_scan_file_for_secrets(path, relative))
    if secret_issues:
        raise SystemExit("Empacotamento bloqueado; material sensivel potencial: " + ", ".join(secret_issues))

    if output.exists():
        output.unlink()
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as archive:
        for path, relative in release_files:
            archive.write(path, arcname=relative.as_posix())

    validation = _validate_archive(output)
    if validation["forbidden_members"]:
        output.unlink(missing_ok=True)
        raise SystemExit("Empacotamento bloqueado; membros proibidos: " + ", ".join(validation["forbidden_members"]))

    manifest = {
        "artifact": output.name,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "sha256": _sha256(output),
        "size_bytes": output.stat().st_size,
        "file_count": validation["files"],
        "forbidden_members": validation["forbidden_members"],
        "secret_scan_issues": secret_issues,
        "excluded_runtime": sorted(EXCLUDED_DIRS),
    }
    manifest_path = Path(args.manifest).expanduser().resolve() if args.manifest else output.with_suffix(".manifest.json")
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    output.with_suffix(output.suffix + ".sha256").write_text(f"{manifest['sha256']}  {output.name}\n", encoding="utf-8")

    print(json.dumps(manifest, ensure_ascii=False, indent=2))
    print(f"release_zip: {output}")
    print(f"manifest: {manifest_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
