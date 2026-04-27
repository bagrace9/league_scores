"""
File domain object representing a downloaded UDisc leaderboard export.

Encapsulates file metadata, parsed event dates, and file system operations
(move, delete) for a single downloaded event spreadsheet.
"""
from pathlib import Path
import re
import shutil
from datetime import date, datetime
from typing import Optional


class File:
    """Class representing a downloaded event data file."""

    def __init__(
        self,
        export_url: str,
        filename: str,
        filepath: str,
        success: bool = True,
        error: Optional[str] = None,
        event_end_date: Optional[date] = None,
    ):
        self.export_url = export_url
        self.filename = filename
        self.filepath = filepath
        self.success = success
        self.error = error
        self.event_end_date = event_end_date
        self.download_date = self._parse_download_date_from_filename(filename)

    @staticmethod
    def _parse_download_date_from_filename(filename: str) -> Optional[datetime]:
        """Parse download timestamp from the end of a filename.

        Expected suffix format: *[-_]YYYY-MM-DD_YYYYMMDD_HHMMSS(.ext)
        """
        stem = Path(filename).stem
        match = re.search(r"[-_](?:\d{4}-\d{2}-\d{2}|\d{8})_(\d{8}_\d{6})$", stem)
        if not match:
            return None
        try:
            return datetime.strptime(match.group(1), "%Y%m%d_%H%M%S")
        except ValueError:
            return None

    @classmethod
    def from_download_result(cls, result: dict) -> 'File':
        """Create a File instance from a download result dictionary."""
        return cls(
            export_url=result.get('export_url'),
            filename=result.get('filename', ''),
            filepath=result.get('filepath', ''),
            success=result.get('success', False),
            error=result.get('error'),
            event_end_date=result.get('event_end_date'),
        )

    def exists(self) -> bool:
        """Check if the file exists on disk."""
        return Path(self.filepath).exists()

    def get_file_size(self) -> Optional[int]:
        """Get the file size in bytes, or None if file doesn't exist."""
        try:
            return Path(self.filepath).stat().st_size
        except (FileNotFoundError, OSError):
            return None

    def delete_from_disk(self) -> bool:
        """Delete the file from disk if it exists."""
        try:
            path = Path(self.filepath)
            if path.exists():
                path.unlink()
            return True
        except OSError:
            return False

    def move_to_directory(self, directory) -> bool:
        """Move this file to the provided directory and update file metadata."""
        try:
            source = Path(self.filepath)
            if not source.exists():
                return False

            target_dir = Path(directory)
            target_dir.mkdir(parents=True, exist_ok=True)

            target = target_dir / source.name
            if target.exists():
                stem = source.stem
                suffix = source.suffix
                counter = 1
                while True:
                    candidate = target_dir / f"{stem}_{counter}{suffix}"
                    if not candidate.exists():
                        target = candidate
                        break
                    counter += 1

            moved_path = Path(shutil.move(str(source), str(target)))
            self.filepath = str(moved_path)
            self.filename = moved_path.name
            return True
        except OSError:
            return False

    def upload_to_gcs(self, bucket_name: str, destination_prefix: str = '') -> bool:
        """Upload this file to GCS, update file metadata, and delete local copy."""
        try:
            from google.cloud import storage

            source = Path(self.filepath)
            if not source.exists():
                self.error = f"Source file not found: {self.filepath}"
                return False

            client = storage.Client()
            bucket = client.bucket(bucket_name)

            clean_prefix = destination_prefix.strip('/')
            blob_name = f"{clean_prefix}/{source.name}" if clean_prefix else source.name
            blob = bucket.blob(blob_name)

            if blob.exists(client):
                stem = source.stem
                suffix = source.suffix
                counter = 1
                while True:
                    candidate_name = (
                        f"{clean_prefix}/{stem}_{counter}{suffix}"
                        if clean_prefix
                        else f"{stem}_{counter}{suffix}"
                    )
                    candidate_blob = bucket.blob(candidate_name)
                    if not candidate_blob.exists(client):
                        blob_name = candidate_name
                        blob = candidate_blob
                        break
                    counter += 1

            blob.upload_from_filename(str(source))
            source.unlink()

            self.filepath = f"gs://{bucket_name}/{blob_name}"
            self.filename = Path(blob_name).name
            self.error = None
            return True
        except Exception as error:
            self.error = str(error)
            return False

    def __str__(self):
        status = "✓" if self.success else "✗"
        return f"File({status} {self.filename})"

    def __repr__(self):
        return (
            f"File(filename='{self.filename}', event_end_date={self.event_end_date}, "
            f"download_date={self.download_date}, success={self.success})"
        )
