from m3u_to_rekordbox import RekordboxXmlGenerator


def generate_rekordbox_xml(playlists_dir, output_xml_path, master_tracks_dir, rating_data=None):
    """
    Generate a Rekordbox XML file from M3U playlists.

    Args:
        playlists_dir: Directory containing M3U playlists
        output_xml_path: Path to write the output XML file
        master_tracks_dir: Directory containing master tracks
        rating_data: Optional dict of track ratings and energy values

    Returns:
        Dictionary with generation results
    """
    generator = RekordboxXmlGenerator(playlists_dir, master_tracks_dir, rating_data)
    total_tracks, total_playlists, total_rated = generator.generate(output_xml_path)

    return {
        "total_tracks": total_tracks,
        "total_playlists": total_playlists,
        "total_rated": total_rated,
        "output_path": output_xml_path
    }
