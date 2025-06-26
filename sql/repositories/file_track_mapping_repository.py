import hashlib
import os
import sqlite3
from datetime import datetime
from typing import Optional, Dict, Set

from sql.models.file_track_mapping import FileTrackMapping
from sql.repositories.base_repository import BaseRepository


class FileTrackMappingRepository(BaseRepository[FileTrackMapping]):
    def __init__(self, connection: sqlite3.Connection):
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
            VALUES (?, ?, ?, ?, ?, datetime('now'), 1)
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

    def get_all_active_uri_to_file_mappings(self) -> Dict[str, str]:
        """
        Get all active URI-to-file mappings in a single query.

        Returns:
            Dictionary mapping Spotify URIs to file paths
        """
        query = """
            SELECT Uri, FilePath 
            FROM FileTrackMappings 
            WHERE IsActive = 1 AND Uri IS NOT NULL AND FilePath IS NOT NULL
        """

        results = self.fetch_all(query)
        return {row.Uri: row.FilePath for row in results}

    def cleanup_stale_mappings(self) -> Dict[str, int]:
        """Clean up mappings that point to files that no longer exist."""
        query = """
            SELECT MappingId, FilePath, Uri 
            FROM FileTrackMappings 
            WHERE IsActive = 1
        """

        results = self.fetch_all(query)
        stale_mappings = []

        for row in results:
            if not os.path.exists(row.FilePath):
                stale_mappings.append(row.MappingId)

        # Soft delete stale mappings
        cleaned_count = 0
        if stale_mappings:
            placeholders = ','.join(['?' for _ in stale_mappings])
            cleanup_query = f"""
                UPDATE FileTrackMappings 
                SET IsActive = 0 
                WHERE MappingId IN ({placeholders})
            """
            cleaned_count = self.execute_non_query(cleanup_query, stale_mappings)

        return {
            'checked_count': len(results),
            'cleaned_count': cleaned_count
        }

    def get_file_to_uri_mappings(self) -> Dict[str, str]:
        """
        Get all active file-to-URI mappings in a single query.

        Returns:
            Dictionary mapping normalized file paths to Spotify URIs
        """
        query = """
            SELECT Uri, FilePath 
            FROM FileTrackMappings 
            WHERE IsActive = 1 AND Uri IS NOT NULL AND FilePath IS NOT NULL
        """

        results = self.fetch_all(query)
        return {os.path.normpath(row.FilePath): row.Uri for row in results}

    def get_mapped_uris(self) -> Set[str]:
        """
        Get all URIs that are currently mapped to files.

        Returns:
            Set of Spotify URIs that have active file mappings
        """
        query = """
            SELECT DISTINCT Uri 
            FROM FileTrackMappings 
            WHERE IsActive = 1 AND Uri IS NOT NULL
        """

        results = self.fetch_all(query)
        return {row.Uri for row in results}

    def get_uri_mapping_counts(self) -> Dict[str, int]:
        """
        Get count of how many files each URI is mapped to.

        Returns:
            Dictionary mapping URI to count of files
        """
        query = """
            SELECT Uri, COUNT(*) as FileCount
            FROM FileTrackMappings 
            WHERE IsActive = 1 AND Uri IS NOT NULL
            GROUP BY Uri
        """

        results = self.fetch_all(query)
        return {row.Uri: row.FileCount for row in results}

    def find_duplicate_mappings(self) -> Dict[str, list]:
        """
        Find URIs that are mapped to multiple files (potential duplicates).

        Returns:
            Dictionary mapping URI to list of file paths
        """
        query = """
            SELECT Uri, FilePath
            FROM FileTrackMappings 
            WHERE IsActive = 1 AND Uri IN (
                SELECT Uri 
                FROM FileTrackMappings 
                WHERE IsActive = 1 AND Uri IS NOT NULL
                GROUP BY Uri 
                HAVING COUNT(*) > 1
            )
            ORDER BY Uri, FilePath
        """

        results = self.fetch_all(query)
        duplicates = {}
        for row in results:
            if row.Uri not in duplicates:
                duplicates[row.Uri] = []
            duplicates[row.Uri].append(row.FilePath)

        return duplicates

    def check_mapping_exists(self, file_path: str, spotify_uri: str) -> bool:
        """
        Check if a specific file-to-URI mapping already exists.

        Args:
            file_path: Path to the file
            spotify_uri: Spotify URI

        Returns:
            True if mapping exists, False otherwise
        """
        query = """
            SELECT 1 FROM FileTrackMappings 
            WHERE FilePath = ? AND Uri = ? AND IsActive = 1
        """

        result = self.fetch_one(query, (os.path.normpath(file_path), spotify_uri))
        return result is not None

    def delete_by_file_path(self, file_path: str) -> bool:
        """Delete mapping by file path."""
        query = "DELETE FROM FileTrackMappings WHERE FilePath = ?"
        cursor = self.connection.cursor()
        rows_affected = cursor.execute(query, (os.path.normpath(file_path),)).rowcount
        return rows_affected > 0

    def soft_delete_by_file_path(self, file_path: str) -> bool:
        """Soft delete mapping by file path (set IsActive = 0)."""
        query = "UPDATE FileTrackMappings SET IsActive = 0 WHERE FilePath = ?"
        cursor = self.connection.cursor()
        rows_affected = cursor.execute(query, (os.path.normpath(file_path),)).rowcount
        return rows_affected > 0

    def soft_delete_by_uri(self, spotify_uri: str) -> int:
        """
        Soft delete all mappings for a specific URI.

        Args:
            spotify_uri: Spotify URI

        Returns:
            Number of mappings deactivated
        """
        query = "UPDATE FileTrackMappings SET IsActive = 0 WHERE Uri = ?"
        cursor = self.connection.cursor()
        return cursor.execute(query, (spotify_uri,)).rowcount

    def reactivate_mapping(self, file_path: str, spotify_uri: str) -> bool:
        """
        Reactivate a soft-deleted mapping.

        Args:
            file_path: Path to the file
            spotify_uri: Spotify URI

        Returns:
            True if mapping was reactivated, False if not found
        """
        query = """
            UPDATE FileTrackMappings 
            SET IsActive = 1 
            WHERE FilePath = ? AND Uri = ? AND IsActive = 0
        """

        cursor = self.connection.cursor()
        rows_affected = cursor.execute(query, (os.path.normpath(file_path), spotify_uri)).rowcount
        return rows_affected > 0

    @staticmethod
    def _calculate_file_hash(file_path: str) -> str:
        """Calculate file hash for integrity checking."""
        hash_sha256 = hashlib.sha256()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                hash_sha256.update(chunk)
        return hash_sha256.hexdigest()

    def _map_to_model(self, row: sqlite3.Row) -> FileTrackMapping:
        """
        Map a database row to a FileTrackMapping object.

        Args:
            row: Database row from the FileTrackMappings table

        Returns:
            FileTrackMapping object with properties set from the row
        """
        mapping_id = row['MappingId'] if row['MappingId'] else None
        file_path = row['FilePath'] if row['FilePath'] else ""
        spotify_uri = row['Uri'] if row['Uri'] else ""
        file_hash = row['FileHash'] if row['FileHash'] else None
        file_size = row['FileSize'] if row['FileSize'] else None
        last_modified = row['LastModified'] if row['LastModified'] else None
        created_at = row['CreatedAt'] if row['CreatedAt'] else None
        is_active = bool(row['IsActive']) if row['IsActive'] else True

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
