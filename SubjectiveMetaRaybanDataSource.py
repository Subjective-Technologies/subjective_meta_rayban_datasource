import hashlib
import json
import os
import time
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Set
from urllib.parse import urlparse
from urllib.request import url2pathname

from subjective_abstract_data_source_package import SubjectiveDataSource
from brainboost_data_source_logger_package.BBLogger import BBLogger

VIDEO_EXTENSIONS = {".mp4", ".mov", ".mkv", ".avi", ".webm"}
PHOTO_EXTENSIONS = {".jpg", ".jpeg", ".png", ".heic", ".heif", ".webp"}
AUDIO_EXTENSIONS = {".m4a", ".aac", ".wav", ".mp3", ".ogg"}


class SubjectiveMetaRaybanDataSource(SubjectiveDataSource):
    """
    Index local Meta Ray-Ban recordings exported from the Meta View app.
    """

    def fetch(self):
        params = self.params if isinstance(self.params, dict) else {}

        recursive = _to_bool(params.get("recursive"), True)
        include_videos = _to_bool(params.get("include_videos"), True)
        include_photos = _to_bool(params.get("include_photos"), True)
        include_audio = _to_bool(params.get("include_audio"), True)
        include_sidecar_metadata = _to_bool(params.get("include_sidecar_metadata"), True)
        include_file_hash = _to_bool(params.get("include_file_hash"), False)
        max_items = _to_positive_int(params.get("max_items"), default=0)

        allowed_exts = self._build_allowed_extensions(
            include_videos=include_videos,
            include_photos=include_photos,
            include_audio=include_audio,
        )
        if not allowed_exts:
            raise ValueError("At least one media type must be enabled.")

        target_value = (
            params.get("recordings_path")
            or params.get("path")
            or params.get("folder_path")
            or params.get("url")
        )
        target_path = self._resolve_target_path(target_value)

        if os.path.isfile(target_path):
            root_path = os.path.dirname(target_path) or os.getcwd()
            media_files = [target_path] if self._matches_extension(target_path, allowed_exts) else []
        else:
            root_path = target_path
            media_files = self._collect_media_files(
                root_path=root_path,
                recursive=recursive,
                allowed_exts=allowed_exts,
            )

        media_files = sorted(media_files, key=self._safe_mtime, reverse=True)
        if max_items > 0:
            media_files = media_files[:max_items]

        self._set_total_items_safe(len(media_files))
        self._set_processed_items_safe(0)
        self._emit_progress_safe()

        started = time.time()
        counts = {"total": 0, "videos": 0, "photos": 0, "audio": 0, "unknown": 0}
        recordings: List[Dict[str, Any]] = []
        errors: List[Dict[str, str]] = []

        for index, media_path in enumerate(media_files, start=1):
            try:
                item = self._build_recording_item(
                    root_path=root_path,
                    file_path=media_path,
                    include_sidecar_metadata=include_sidecar_metadata,
                    include_file_hash=include_file_hash,
                )
                recordings.append(item)
                counts["total"] += 1
                if item["media_type"] == "video":
                    counts["videos"] += 1
                elif item["media_type"] == "photo":
                    counts["photos"] += 1
                elif item["media_type"] == "audio":
                    counts["audio"] += 1
                else:
                    counts["unknown"] += 1
            except Exception as exc:
                errors.append({"path": media_path, "error": str(exc)})
                self._log(f"Failed to process recording '{media_path}': {exc}")

            self._set_processed_items_safe(index)
            self._set_total_processing_time_safe(time.time() - started)
            self._emit_progress_safe()

        self._set_fetch_completed_safe(True)
        self._emit_progress_safe()

        return {
            "source": "meta_rayban",
            "root_path": root_path,
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "options": {
                "recursive": recursive,
                "max_items": max_items,
                "include_videos": include_videos,
                "include_photos": include_photos,
                "include_audio": include_audio,
                "include_sidecar_metadata": include_sidecar_metadata,
                "include_file_hash": include_file_hash,
            },
            "counts": counts,
            "recordings": recordings,
            "errors": errors,
        }

    def get_icon(self) -> str:
        icon_path = os.path.join(os.path.dirname(__file__), "icon.svg")
        try:
            with open(icon_path, "r", encoding="utf-8") as f:
                return f.read()
        except Exception as e:
            BBLogger.log(f"Error reading icon file: {e}")
            return ""

    def get_connection_data(self) -> dict:
        return {
            "connection_type": "META_RAYBAN",
            "fields": [
                {
                    "name": "recordings_path",
                    "label": "Recordings Folder (optional)",
                    "type": "text",
                    "placeholder": "C:/Users/<user>/Downloads/Ray-Ban Meta",
                },
                {
                    "name": "recursive",
                    "label": "Scan Subfolders",
                    "type": "checkbox",
                    "default": True,
                },
                {
                    "name": "max_items",
                    "label": "Max Items (0 = all)",
                    "type": "number",
                    "default": 0,
                },
                {
                    "name": "include_videos",
                    "label": "Include Videos",
                    "type": "checkbox",
                    "default": True,
                },
                {
                    "name": "include_photos",
                    "label": "Include Photos",
                    "type": "checkbox",
                    "default": True,
                },
                {
                    "name": "include_audio",
                    "label": "Include Audio",
                    "type": "checkbox",
                    "default": True,
                },
                {
                    "name": "include_sidecar_metadata",
                    "label": "Load Sidecar Metadata (.json)",
                    "type": "checkbox",
                    "default": True,
                },
                {
                    "name": "include_file_hash",
                    "label": "Compute SHA1 Hash",
                    "type": "checkbox",
                    "default": False,
                },
                {
                    "name": "connection_name",
                    "label": "Connection Name",
                    "type": "text",
                },
            ],
        }

    def _build_allowed_extensions(
        self,
        include_videos: bool,
        include_photos: bool,
        include_audio: bool,
    ) -> Set[str]:
        allowed: Set[str] = set()
        if include_videos:
            allowed.update(VIDEO_EXTENSIONS)
        if include_photos:
            allowed.update(PHOTO_EXTENSIONS)
        if include_audio:
            allowed.update(AUDIO_EXTENSIONS)
        return allowed

    def _resolve_target_path(self, target: Optional[str]) -> str:
        if target:
            normalized = self._normalize_path(target)
            if not os.path.exists(normalized):
                raise FileNotFoundError(f"Path does not exist: {normalized}")
            return normalized

        auto_discovered = self._discover_default_recordings_path()
        if auto_discovered:
            self._log(f"Auto-discovered Meta Ray-Ban recordings folder: {auto_discovered}")
            return auto_discovered

        raise ValueError(
            "No recordings path was provided and no Meta Ray-Ban folder was auto-discovered. "
            "Set params['recordings_path'] to a folder containing exported recordings."
        )

    def _normalize_path(self, value: str) -> str:
        raw = str(value).strip()
        if raw.lower().startswith("file://"):
            parsed = urlparse(raw)
            local_path = parsed.path
            if parsed.netloc and parsed.netloc not in ("", "localhost"):
                local_path = f"//{parsed.netloc}{parsed.path}"
            if os.name == "nt" and len(local_path) >= 3 and local_path[0] == "/" and local_path[2] == ":":
                local_path = local_path[1:]
            normalized = os.path.normpath(url2pathname(local_path))
        else:
            normalized = os.path.normpath(os.path.expanduser(raw))
        return os.path.abspath(normalized)

    def _discover_default_recordings_path(self) -> Optional[str]:
        search_roots = self._build_default_search_roots()
        candidates: List[tuple[int, float, str]] = []
        seen: Set[str] = set()

        for root in search_roots:
            if not os.path.isdir(root):
                continue
            for candidate in self._iter_candidate_directories(root):
                normalized = os.path.abspath(candidate)
                if normalized in seen or not os.path.isdir(normalized):
                    continue
                seen.add(normalized)
                media_count = self._count_media_files(normalized, max_count=50)
                if media_count <= 0:
                    continue
                candidates.append((media_count, self._safe_mtime(normalized), normalized))

        if not candidates:
            return None

        candidates.sort(key=lambda item: (item[0], item[1]), reverse=True)
        return candidates[0][2]

    def _build_default_search_roots(self) -> List[str]:
        home = os.path.expanduser("~")
        roots = [
            os.path.join(home, "Downloads"),
            os.path.join(home, "Pictures"),
            os.path.join(home, "Videos"),
            os.path.join(home, "Desktop"),
            os.path.join(home, "Documents"),
            os.path.join(home, "OneDrive", "Downloads"),
            os.path.join(home, "OneDrive", "Pictures"),
            os.path.join(home, "OneDrive", "Videos"),
        ]
        return [os.path.abspath(path) for path in roots]

    def _iter_candidate_directories(self, root: str) -> Iterable[str]:
        explicit_names = [
            "Ray-Ban Meta",
            "RayBan Meta",
            "RayBanMeta",
            "Ray-Ban Stories",
            "RayBan Stories",
            "Meta View",
            "MetaView",
        ]

        for folder_name in explicit_names:
            path = os.path.join(root, folder_name)
            if os.path.isdir(path):
                yield path

        try:
            with os.scandir(root) as level1:
                for entry in level1:
                    if not entry.is_dir():
                        continue
                    if self._looks_like_meta_rayban_folder(entry.name):
                        yield entry.path

                    try:
                        with os.scandir(entry.path) as level2:
                            for child in level2:
                                if child.is_dir() and self._looks_like_meta_rayban_folder(child.name):
                                    yield child.path
                    except Exception:
                        continue
        except Exception:
            return

    def _looks_like_meta_rayban_folder(self, name: str) -> bool:
        normalized = name.lower().strip()
        return (
            "rayban" in normalized
            or "ray-ban" in normalized
            or "meta view" in normalized
            or ("ray" in normalized and "ban" in normalized)
        )

    def _count_media_files(self, root_path: str, max_count: int = 50) -> int:
        count = 0
        for dirpath, _, filenames in os.walk(root_path):
            for filename in filenames:
                ext = os.path.splitext(filename)[1].lower()
                if ext in VIDEO_EXTENSIONS or ext in PHOTO_EXTENSIONS or ext in AUDIO_EXTENSIONS:
                    count += 1
                    if count >= max_count:
                        return count
        return count

    def _collect_media_files(self, root_path: str, recursive: bool, allowed_exts: Set[str]) -> List[str]:
        files: List[str] = []

        if recursive:
            for dirpath, _, filenames in os.walk(root_path):
                for filename in filenames:
                    candidate = os.path.join(dirpath, filename)
                    if self._matches_extension(candidate, allowed_exts):
                        files.append(os.path.abspath(candidate))
            return files

        try:
            with os.scandir(root_path) as entries:
                for entry in entries:
                    if entry.is_file() and self._matches_extension(entry.path, allowed_exts):
                        files.append(os.path.abspath(entry.path))
        except Exception as exc:
            raise RuntimeError(f"Unable to scan folder '{root_path}': {exc}") from exc
        return files

    def _matches_extension(self, file_path: str, allowed_exts: Set[str]) -> bool:
        ext = os.path.splitext(file_path)[1].lower()
        return ext in allowed_exts

    def _build_recording_item(
        self,
        root_path: str,
        file_path: str,
        include_sidecar_metadata: bool,
        include_file_hash: bool,
    ) -> Dict[str, Any]:
        abs_path = os.path.abspath(file_path)
        stat = os.stat(abs_path)
        ext = os.path.splitext(abs_path)[1].lower()
        media_type = self._media_type_for_extension(ext)

        try:
            relative_path = os.path.relpath(abs_path, root_path)
        except Exception:
            relative_path = os.path.basename(abs_path)

        identifier = hashlib.sha1(
            f"{abs_path}|{stat.st_size}|{int(stat.st_mtime)}".encode("utf-8")
        ).hexdigest()

        item: Dict[str, Any] = {
            "id": identifier,
            "source": "meta_rayban",
            "path": abs_path,
            "relative_path": relative_path,
            "filename": os.path.basename(abs_path),
            "media_type": media_type,
            "extension": ext,
            "size_bytes": int(stat.st_size),
            "created_at": datetime.fromtimestamp(stat.st_ctime).isoformat(timespec="seconds"),
            "modified_at": datetime.fromtimestamp(stat.st_mtime).isoformat(timespec="seconds"),
        }

        if include_sidecar_metadata:
            sidecar = self._load_sidecar_metadata(abs_path)
            if sidecar is not None:
                item["metadata"] = sidecar

        if include_file_hash:
            item["sha1"] = self._sha1_file(abs_path)

        return item

    def _media_type_for_extension(self, ext: str) -> str:
        if ext in VIDEO_EXTENSIONS:
            return "video"
        if ext in PHOTO_EXTENSIONS:
            return "photo"
        if ext in AUDIO_EXTENSIONS:
            return "audio"
        return "unknown"

    def _load_sidecar_metadata(self, media_path: str) -> Optional[Any]:
        base, _ = os.path.splitext(media_path)
        candidates = [f"{base}.json", f"{media_path}.json"]
        for sidecar_path in candidates:
            if not os.path.exists(sidecar_path) or not os.path.isfile(sidecar_path):
                continue
            try:
                if os.path.getsize(sidecar_path) > 5 * 1024 * 1024:
                    self._log(f"Skipping sidecar JSON larger than 5MB: {sidecar_path}")
                    continue
                with open(sidecar_path, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as exc:
                self._log(f"Could not parse sidecar JSON '{sidecar_path}': {exc}")
        return None

    def _sha1_file(self, file_path: str) -> str:
        digest = hashlib.sha1()
        with open(file_path, "rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

    def _safe_mtime(self, path: str) -> float:
        try:
            return os.path.getmtime(path)
        except Exception:
            return 0.0

    def _set_total_items_safe(self, total: int) -> None:
        try:
            self.set_total_items(total)
        except Exception:
            pass

    def _set_processed_items_safe(self, processed: int) -> None:
        try:
            self.set_processed_items(processed)
        except Exception:
            pass

    def _set_total_processing_time_safe(self, seconds_elapsed: float) -> None:
        try:
            self.set_total_processing_time(seconds_elapsed)
        except Exception:
            pass

    def _set_fetch_completed_safe(self, completed: bool) -> None:
        try:
            self.set_fetch_completed(completed)
        except Exception:
            pass

    def _emit_progress_safe(self) -> None:
        try:
            if hasattr(self, "_emit_progress"):
                self._emit_progress()
        except Exception:
            pass

    def _log(self, message: str) -> None:
        try:
            BBLogger.log(message)
        except Exception:
            print(message, flush=True)


def _to_bool(value: Any, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "y", "on"}:
            return True
        if normalized in {"0", "false", "no", "n", "off"}:
            return False
    return default


def _to_positive_int(value: Any, default: int) -> int:
    try:
        parsed = int(value)
        return parsed if parsed >= 0 else default
    except Exception:
        return default
