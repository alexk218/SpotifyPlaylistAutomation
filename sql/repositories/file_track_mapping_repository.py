import hashlib
import os
from datetime import datetime
from typing import Optional

import pyodbc

from sql.models.file_track_mapping import FileTrackMapping
from sql.repositories.base_repository import BaseRepository


class FileTrackMappingRepository(BaseRepository[FileTrackMapping]):
    def __init__(self, connection: pyodbc.Connection):
        super().__init__(connection)
        self.table_name = "FileTrackMappings"
        self.id_column = "MappingId"

    def add_mapping_by_uri(self, file_path: str, spotify_uri: str):
        """Add file mapping using Spotify URI."""
        file_stats = os.stat(file_path)
        file_hash = self._calculate_file_hash(file_path)

        query = """
            INSERT INTO FileTrackMappings 
            (FilePath, FileHash, Uri, FileSize, LastModified, CreatedAt, IsActive)
            VALUES (?, ?, ?, ?, ?, GETDATE(), 1)
        """

        cursor = self.connection.cursor()
        cursor.execute(query, (
            os.path.normpath(file_path),
            file_hash,
            spotify_uri,
            file_stats.st_size,
            datetime.fromtimestamp(file_stats.st_mtime)
        ))

    def get_uri_by_file_path(self, file_path: str) -> Optional[str]:
        """Get Spotify URI for a file path."""
        query = "SELECT Uri FROM FileTrackMappings WHERE FilePath = ? AND IsActive = 1"
        cursor = self.connection.cursor()
        result = cursor.execute(query, (os.path.normpath(file_path),)).fetchone()
        return result.Uri if result else None

    def get_files_by_uri(self, spotify_uri: str) -> list:
        """Get all files linked to a Spotify URI."""
        query = "SELECT FilePath FROM FileTrackMappings WHERE Uri = ? AND IsActive = 1"
        cursor = self.connection.cursor()
        results = cursor.execute(query, (spotify_uri,)).fetchall()
        return [row.FilePath for row in results]

    def delete_by_file_path(self, file_path: str) -> bool:
        """Delete mapping by file path."""
        query = "DELETE FROM FileTrackMappings WHERE FilePath = ?"
        cursor = self.connection.cursor()
        rows_affected = cursor.execute(query, (os.path.normpath(file_path),)).rowcount
        return rows_affected > 0

    @staticmethod
    def _calculate_file_hash(file_path: str) -> str:
        """Calculate file hash for integrity checking."""
        hash_sha256 = hashlib.sha256()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                hash_sha256.update(chunk)
        return hash_sha256.hexdigest()

    def _map_to_model(self, row: pyodbc.Row) -> FileTrackMapping:
        """
        Map a database row to a FileTrackMapping object.

        Args:
            row: Database row from the FileTrackMappings table

        Returns:
            FileTrackMapping object with properties set from the row
        """
        mapping_id = row.MappingId if hasattr(row, 'MappingId') else None
        file_path = row.FilePath if hasattr(row, 'FilePath') else ""
        spotify_uri = row.Uri if hasattr(row, 'Uri') else ""
        file_hash = row.FileHash if hasattr(row, 'FileHash') else None
        file_size = row.FileSize if hasattr(row, 'FileSize') else None
        last_modified = row.LastModified if hasattr(row, 'LastModified') else None
        created_at = row.CreatedAt if hasattr(row, 'CreatedAt') else None
        is_active = bool(row.IsActive) if hasattr(row, 'IsActive') else True

        return FileTrackMapping(
            mapping_id=mapping_id,
            file_path=file_path,
            spotify_uri=spotify_uri,
            file_hash=file_hash,
            file_size=file_size,
            last_modified=last_modified,
            created_at=created_at,
            is_active=is_active
        )
