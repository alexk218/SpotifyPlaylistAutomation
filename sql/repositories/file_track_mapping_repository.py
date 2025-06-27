import hashlib
import os
import sqlite3
from datetime import datetime
from typing import Optional, Dict, Set, Any, List

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
        return result['Uri'] if result else None

    def get_files_by_uri(self, spotify_uri: str) -> list:
        """Get all files linked to a Spotify URI."""
        query = "SELECT FilePath FROM FileTrackMappings WHERE Uri = ? AND IsActive = 1"
        cursor = self.connection.cursor()
        results = cursor.execute(query, (spotify_uri,)).fetchall()
        return [row['FilePath'] for row in results]

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
        return {row['Uri']: row['FilePath'] for row in results}

    def cleanup_stale_mappings(self) -> Dict[str, int]:
        """Clean up mappings that point to files that no longer exist."""
        query = """
            SELECT MappingId, FilePath, Uri 
            FROM FileTrackMappings 
            WHERE IsActive = 1
        """

        results = self.fetch_all(query)
        stale_mappings = []
        cleaned_paths = []

        for row in results:
            if not os.path.exists(row['FilePath']):
                stale_mappings.append(row['MappingId'])
                cleaned_paths.append(row['FilePath'])

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
            'cleaned_count': cleaned_count,
            'cleaned_paths': cleaned_paths
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
        return {os.path.normpath(row['FilePath']): row['Uri'] for row in results}

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
        return {row['Uri'] for row in results}

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
        return {row['Uri']: row['FileCount'] for row in results}

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
            if row['Uri'] not in duplicates:
                duplicates[row['Uri']] = []
            duplicates[row['Uri']].append(row['FilePath'])

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

    def delete_all_and_reset(self):
        """Delete all records AND reset auto-increment counter."""
        rows_deleted = self.delete_all()

        # Reset SQLite sequence counter
        cursor = self.connection.cursor()
        cursor.execute("DELETE FROM sqlite_sequence WHERE name='FileTrackMappings'")
        cursor.execute("INSERT INTO sqlite_sequence (name, seq) VALUES ('FileTrackMappings', 0)")
        self.connection.commit()

        return rows_deleted

    def batch_add_mappings_by_uri(self, mappings: List[Dict[str, Any]]) -> int:
        """
        OPTIMIZED: Add multiple file mappings with lightweight hashing.

        Args:
            mappings: List of dicts with 'file_path' and 'uri' keys

        Returns:
            Number of mappings successfully inserted
        """
        if not mappings:
            return 0

        import time

        print(f"  Starting batch insert for {len(mappings)} mappings...")
        prep_start = time.time()

        # Prepare batch data with fast hashing
        batch_data = []
        hash_time_total = 0
        stat_time_total = 0

        for i, mapping in enumerate(mappings):
            file_path = mapping['file_path']
            uri = mapping['uri']

            try:
                # Get file stats (very fast)
                stat_start = time.time()
                file_stats = os.stat(file_path)
                stat_time_total += time.time() - stat_start

                # Calculate lightweight hash (much faster than full file hash)
                hash_start = time.time()
                fast_hash = self._calculate_file_hash(file_path, file_stats.st_size)
                hash_time_total += time.time() - hash_start

                batch_data.append((
                    os.path.normpath(file_path),
                    fast_hash,
                    uri,
                    file_stats.st_size,
                    datetime.fromtimestamp(file_stats.st_mtime)
                ))

                # Progress indicator for large batches
                if (i + 1) % 200 == 0:
                    print(f"    Processed {i + 1}/{len(mappings)} files...")

            except Exception as e:
                print(f"Error preparing batch data for {file_path}: {e}")
                continue

        prep_time = time.time() - prep_start

        if not batch_data:
            return 0

        # Batch insert (very fast)
        insert_start = time.time()
        query = """
            INSERT INTO FileTrackMappings 
            (FilePath, FileHash, Uri, FileSize, LastModified, CreatedAt, IsActive)
            VALUES (?, ?, ?, ?, ?, datetime('now'), 1)
        """

        cursor = self.connection.cursor()
        cursor.executemany(query, batch_data)

        insert_time = time.time() - insert_start

        print(f"  Batch insert timing breakdown:")
        print(f"    File stats: {stat_time_total:.3f}s")
        print(f"    Fast hashing: {hash_time_total:.3f}s ({hash_time_total / len(mappings) * 1000:.1f}ms per file)")
        print(f"    Database insert: {insert_time:.3f}s")
        print(f"    Total prep time: {prep_time:.3f}s")
        print(f"    Successfully prepared {len(batch_data)}/{len(mappings)} mappings")

        return len(batch_data)

    def _calculate_file_hash(self, file_path: str, file_size: int) -> str:
        """
        Ultra-minimal file hash using only 8KB from start + metadata.
        Fastest possible while still detecting file changes.
        """
        import hashlib

        hash_obj = hashlib.md5()

        try:
            with open(file_path, "rb") as f:
                # Read only first 8KB (extremely fast)
                chunk = f.read(8192)  # 8KB
                hash_obj.update(chunk)
        except Exception:
            pass  # Fall back to metadata-only hash

        # Include metadata
        hash_obj.update(str(file_size).encode())
        hash_obj.update(os.path.basename(file_path).encode())

        try:
            mtime = os.path.getmtime(file_path)
            hash_obj.update(str(int(mtime)).encode())
        except:
            pass

        return hash_obj.hexdigest()

    def get_uri_mappings_batch(self, file_paths: List[str]) -> Dict[str, str]:
        """
        OPTIMIZED: Get URI mappings for multiple file paths in one query.

        Args:
            file_paths: List of file paths to check

        Returns:
            Dictionary mapping file_path to uri for existing mappings
        """
        if not file_paths:
            return {}

        # Normalize all paths
        normalized_paths = [os.path.normpath(path) for path in file_paths]

        # Use parameterized query
        placeholders = ','.join(['?' for _ in normalized_paths])
        query = f"""
            SELECT FilePath, Uri 
            FROM FileTrackMappings 
            WHERE FilePath IN ({placeholders}) AND IsActive = 1
        """

        results = self.fetch_all(query, normalized_paths)
        return {row['FilePath']: row['Uri'] for row in results}

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
