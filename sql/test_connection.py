import os
import pyodbc
from dotenv import load_dotenv

load_dotenv()

SERVER_CONNECTION_STRING = os.getenv('SERVER_CONNECTION_STRING')
DATABASE_NAME = os.getenv('DATABASE_NAME')


def get_db_connection():
    try:
        connection = pyodbc.connect(
            r'DRIVER={SQL Server};'
            fr'SERVER={SERVER_CONNECTION_STRING};'
            fr'DATABASE={DATABASE_NAME};'
            r'Trusted_Connection=yes;'
        )
        print(f"Connected to SERVER: {SERVER_CONNECTION_STRING}, DATABASE: {DATABASE_NAME}")
        return connection
    except pyodbc.Error as e:
        print(f"Database connection failed: {e}")
        return None


def test_fetch_playlists():
    connection = get_db_connection()
    if not connection:
        return
    cursor = connection.cursor()
    try:
        cursor.execute("SELECT TOP 5 PlaylistName FROM Playlists")
        playlists = cursor.fetchall()
        print("Sample Playlists:")
        for pl in playlists:
            print(pl.PlaylistName)
    except pyodbc.Error as e:
        print("Error fetching playlists:", e)
    finally:
        cursor.close()
        connection.close()


if __name__ == "__main__":
    test_fetch_playlists()
