import os
from datetime import datetime
from typing import Optional


class FileTrackMappingRepository:
    """Enhanced repository for URI-based file mappings."""

    def __init__(self, connection):
        self.connection = connection

    def add_mapping_by_uri(self, file_path: str, spotify_uri: str):
        """Add file mapping using Spotify URI."""
        file_stats = os.stat(file_path)
        file_hash = self._calculate_file_hash(file_path)

        query = """
            INSERT INTO FileTrackMappings 
            (FilePath, FileName, FileHash, Uri, FileSize, LastModified)
            VALUES (?, ?, ?, ?, ?, ?)
        """

        cursor = self.connection.cursor()
        cursor.execute(query, (
            os.path.normpath(file_path),
            os.path.basename(file_path),
            file_hash,
            spotify_uri,  # Store URI instead of TrackId
            file_stats.st_size,
            datetime.fromtimestamp(file_stats.st_mtime)
        ))

    def get_uri_by_file_path(self, file_path: str) -> Optional[str]:
        """Get Spotify URI for a file path."""
        query = "SELECT Uri FROM FileTrackMappings WHERE FilePath = ?"
        cursor = self.connection.cursor()
        result = cursor.execute(query, (os.path.normpath(file_path),)).fetchone()
        return result.Uri if result else None

    def get_files_by_uri(self, spotify_uri: str) -> list:
        """Get all files linked to a Spotify URI."""
        query = "SELECT FilePath FROM FileTrackMappings WHERE Uri = ?"
        cursor = self.connection.cursor()
        results = cursor.execute(query, (spotify_uri,)).fetchall()
        return [row.FilePath for row in results]

    def delete_by_file_path(self, file_path: str) -> bool:
        """Delete mapping by file path."""
        query = "DELETE FROM FileTrackMappings WHERE FilePath = ?"
        cursor = self.connection.cursor()
        rows_affected = cursor.execute(query, (os.path.normpath(file_path),)).rowcount
        return rows_affected > 0

    def _calculate_file_hash(self, file_path: str) -> str:
        """Calculate file hash for integrity checking."""
        import hashlib
        hash_sha256 = hashlib.sha256()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                hash_sha256.update(chunk)
        return hash_sha256.hexdigest()
