"""
REAL duration matching test that compares file durations with actual track durations from your database.
This shows the actual confidence improvements you'll get with duration-based boosting.
"""

import os
import time
from typing import Optional, List, Dict, Tuple
from mutagen import File as MutagenFile

from sql.core.unit_of_work import UnitOfWork
from helpers.fuzzy_match_helper import FuzzyMatcher, FuzzyMatchResult

from dotenv import load_dotenv

load_dotenv()


def get_file_duration_ms(file_path: str) -> Optional[int]:
    """Extract duration from audio file in milliseconds."""
    if not os.path.exists(file_path):
        return None

    try:
        audio_file = MutagenFile(file_path)
        if audio_file is None:
            return None

        if hasattr(audio_file, 'info') and hasattr(audio_file.info, 'length'):
            duration_seconds = audio_file.info.length
            return int(duration_seconds * 1000)

        return None

    except Exception as e:
        print(f"Warning: Could not extract duration from {file_path}: {e}")
        return None


def format_duration_ms(duration_ms: int) -> str:
    """Format duration as MM:SS"""
    if not duration_ms:
        return "Unknown"

    total_seconds = duration_ms // 1000
    minutes = total_seconds // 60
    seconds = total_seconds % 60
    return f"{minutes}:{seconds:02d}"


def calculate_duration_boost(file_duration_ms: int, track_duration_ms: int) -> float:
    """Calculate the confidence boost based on duration similarity."""
    if not file_duration_ms or not track_duration_ms:
        return 1.0

    duration_diff_ms = abs(file_duration_ms - track_duration_ms)

    # Thresholds (in milliseconds)
    if duration_diff_ms <= 1000:  # 1 second - very strong boost
        return 1.25  # 25% boost
    elif duration_diff_ms <= 3000:  # 3 seconds - strong boost
        return 1.20  # 20% boost
    elif duration_diff_ms <= 10000:  # 10 seconds - moderate boost
        return 1.15  # 15% boost
    elif duration_diff_ms <= 30000:  # 30 seconds - light boost
        return 1.10  # 10% boost
    else:
        return 1.0  # No boost for large differences


def test_real_duration_matching(master_tracks_dir: str, sample_size: int = 20, confidence_threshold: float = 0.75):
    """
    Test duration matching against REAL tracks from your database.
    Shows actual confidence improvements for real matches.
    """

    print("=== REAL DURATION MATCHING TEST ===")
    print(f"Testing against actual tracks in your database")
    print(f"Sample size: {sample_size} files")
    print(f"Confidence threshold: {confidence_threshold}")
    print()

    # Load tracks and existing mappings from database
    print("Loading tracks from database...")
    with UnitOfWork() as uow:
        all_tracks = uow.track_repository.get_all()
        existing_mappings = uow.file_track_mapping_repository.get_all_active_uri_to_file_mappings()

    print(f"✅ Loaded {len(all_tracks)} tracks from database")
    print(f"✅ Found {len(existing_mappings)} existing mappings")
    print()

    # Get sample audio files
    audio_files = []
    for root, _, files in os.walk(master_tracks_dir):
        for file in files:
            if any(file.lower().endswith(ext) for ext in ['.mp3', '.wav', '.flac', '.m4a']):
                audio_files.append(os.path.join(root, file))
                if len(audio_files) >= sample_size:
                    break
        if len(audio_files) >= sample_size:
            break

    if not audio_files:
        print("❌ No audio files found!")
        return

    print(f"Found {len(audio_files)} audio files to test")
    print()

    # Create fuzzy matcher
    matcher = FuzzyMatcher(all_tracks, existing_mappings)

    # Track results
    total_files = 0
    duration_extraction_success = 0
    original_matches_above_threshold = 0
    boosted_matches_above_threshold = 0
    total_confidence_improvement = 0
    significant_improvements = []

    print("Testing each file...")
    print("-" * 80)

    for i, file_path in enumerate(audio_files):
        filename = os.path.basename(file_path)
        total_files += 1

        print(f"\n{i + 1}. Testing: {filename}")

        # Extract file duration
        file_duration_ms = get_file_duration_ms(file_path)
        if file_duration_ms:
            duration_extraction_success += 1
            print(f"   File duration: {format_duration_ms(file_duration_ms)}")
        else:
            print(f"   ❌ Could not extract file duration")
            continue

        # Get original matches (without duration boost)
        original_matches = matcher.find_matches(filename, threshold=0.1, max_matches=10, file_path=file_path)

        if not original_matches:
            print(f"   ❌ No matches found")
            continue

        # Analyze top matches with duration boost
        print(f"   📊 Top matches:")

        original_best = original_matches[0]
        boosted_best_confidence = original_best.confidence
        boosted_best_match = original_best

        for j, match in enumerate(original_matches[:5]):
            track = match.track
            original_confidence = match.confidence

            # Calculate duration boost if track has duration
            if track.duration_ms:
                duration_boost = calculate_duration_boost(file_duration_ms, track.duration_ms)
                boosted_confidence = min(1.0, original_confidence * duration_boost)

                duration_diff_s = abs(file_duration_ms - track.duration_ms) / 1000
                boost_indicator = "🚀" if duration_boost > 1.0 else "  "

                # Track best boosted match
                if boosted_confidence > boosted_best_confidence:
                    boosted_best_confidence = boosted_confidence
                    boosted_best_match = match

                print(f"      {j + 1}. {boost_indicator} {track.artists} - {track.title}")
                print(f"          Original: {original_confidence:.3f} → Boosted: {boosted_confidence:.3f}")
                print(
                    f"          Track duration: {format_duration_ms(track.duration_ms)} (diff: {duration_diff_s:.1f}s)")

            else:
                print(f"      {j + 1}.    {track.artists} - {track.title}")
                print(f"          Confidence: {original_confidence:.3f} (no duration data)")

        # Check threshold improvements
        original_meets_threshold = original_best.confidence >= confidence_threshold
        boosted_meets_threshold = boosted_best_confidence >= confidence_threshold

        if original_meets_threshold:
            original_matches_above_threshold += 1
        if boosted_meets_threshold:
            boosted_matches_above_threshold += 1

        # Calculate improvement
        confidence_improvement = boosted_best_confidence - original_best.confidence
        total_confidence_improvement += confidence_improvement

        # Track significant improvements
        if confidence_improvement > 0.05:  # 5% improvement
            significant_improvements.append({
                'filename': filename,
                'original_confidence': original_best.confidence,
                'boosted_confidence': boosted_best_confidence,
                'improvement': confidence_improvement,
                'original_meets_threshold': original_meets_threshold,
                'boosted_meets_threshold': boosted_meets_threshold
            })

        # Summary for this file
        threshold_change = ""
        if not original_meets_threshold and boosted_meets_threshold:
            threshold_change = " ✅ NOW PASSES THRESHOLD!"
        elif original_meets_threshold and not boosted_meets_threshold:
            threshold_change = " ❌ Now fails threshold"

        print(f"   📈 Best match: {original_best.confidence:.3f} → {boosted_best_confidence:.3f} "
              f"(+{confidence_improvement:.3f}){threshold_change}")

    # Final Summary
    print("\n" + "=" * 80)
    print("=== FINAL RESULTS ===")
    print()

    print(f"📁 Files tested: {total_files}")
    print(
        f"⏱️  Duration extraction success: {duration_extraction_success}/{total_files} ({duration_extraction_success / total_files * 100:.1f}%)")
    print()

    print(f"🎯 ACCURACY IMPROVEMENT:")
    print(
        f"   Original matches above {confidence_threshold}: {original_matches_above_threshold}/{total_files} ({original_matches_above_threshold / total_files * 100:.1f}%)")
    print(
        f"   Boosted matches above {confidence_threshold}: {boosted_matches_above_threshold}/{total_files} ({boosted_matches_above_threshold / total_files * 100:.1f}%)")

    accuracy_improvement = boosted_matches_above_threshold - original_matches_above_threshold
    print(
        f"   📈 IMPROVEMENT: +{accuracy_improvement} files (+{accuracy_improvement / total_files * 100:.1f} percentage points)")

    if total_files > 0:
        avg_confidence_improvement = total_confidence_improvement / total_files
        print(f"   🚀 Average confidence boost: +{avg_confidence_improvement:.3f}")

    print()
    print(f"📊 SIGNIFICANT IMPROVEMENTS (>5% boost):")
    print(f"   Files with significant improvement: {len(significant_improvements)}")

    # Show top improvements
    significant_improvements.sort(key=lambda x: x['improvement'], reverse=True)
    for improvement in significant_improvements[:5]:
        threshold_note = ""
        if not improvement['original_meets_threshold'] and improvement['boosted_meets_threshold']:
            threshold_note = " [THRESHOLD BREAKTHROUGH!]"

        print(f"   • {improvement['filename']}")
        print(f"     {improvement['original_confidence']:.3f} → {improvement['boosted_confidence']:.3f} "
              f"(+{improvement['improvement']:.3f}){threshold_note}")

    print()
    print("🎯 PROJECTION FOR FULL COLLECTION:")
    if accuracy_improvement > 0:
        print(f"   If you have 2165 files (as mentioned), duration boost could:")
        projected_improvement = int(accuracy_improvement / total_files * 2165)
        print(f"   ✅ Help {projected_improvement} additional files pass the threshold")
        print(f"   📈 Improve accuracy from ~81% to ~{81 + projected_improvement / 2165 * 100:.1f}%")
    else:
        print(f"   No significant threshold improvements detected in this sample")

    print()
    print("🚀 RECOMMENDATION:")
    if accuracy_improvement >= 2:  # At least 2 files improved
        print("   ✅ STRONGLY RECOMMENDED: Implement duration-based confidence boosting")
        print("   📊 Expected significant accuracy improvement")
    elif accuracy_improvement >= 1:
        print("   ✅ RECOMMENDED: Implement duration-based confidence boosting")
        print("   📊 Expected moderate accuracy improvement")
    else:
        print("   🤔 MARGINAL: Duration boost shows limited improvement")
        print("   💡 Consider other fuzzy matching improvements first")

    return {
        'total_files': total_files,
        'duration_extraction_success': duration_extraction_success,
        'original_matches_above_threshold': original_matches_above_threshold,
        'boosted_matches_above_threshold': boosted_matches_above_threshold,
        'accuracy_improvement': accuracy_improvement,
        'avg_confidence_improvement': total_confidence_improvement / total_files if total_files > 0 else 0,
        'significant_improvements': len(significant_improvements)
    }


def test_single_file_real_matching(file_path: str):
    """Test duration matching on a single file against real database tracks."""

    filename = os.path.basename(file_path)
    print(f"=== TESTING SINGLE FILE: {filename} ===")

    if not os.path.exists(file_path):
        print("❌ File does not exist!")
        return

    # Extract file duration
    file_duration_ms = get_file_duration_ms(file_path)
    if not file_duration_ms:
        print("❌ Could not extract file duration")
        return

    print(f"📁 File duration: {format_duration_ms(file_duration_ms)}")
    print()

    # Load database
    print("Loading database...")
    with UnitOfWork() as uow:
        all_tracks = uow.track_repository.get_all()
        existing_mappings = uow.file_track_mapping_repository.get_all_active_uri_to_file_mappings()

    print(f"✅ Loaded {len(all_tracks)} tracks")
    print()

    # Create matcher and get matches
    matcher = FuzzyMatcher(all_tracks, existing_mappings)
    matches = matcher.find_matches(filename, threshold=0.1, max_matches=10, file_path=file_path)

    if not matches:
        print("❌ No matches found")
        return

    print("🎯 MATCHES WITH DURATION ANALYSIS:")
    print("-" * 60)

    for i, match in enumerate(matches):
        track = match.track
        original_confidence = match.confidence

        print(f"\n{i + 1}. {track.artists} - {track.title}")
        print(f"   Original confidence: {original_confidence:.3f}")

        if track.duration_ms:
            track_duration_formatted = format_duration_ms(track.duration_ms)
            duration_diff_ms = abs(file_duration_ms - track.duration_ms)
            duration_diff_s = duration_diff_ms / 1000

            duration_boost = calculate_duration_boost(file_duration_ms, track.duration_ms)
            boosted_confidence = min(1.0, original_confidence * duration_boost)

            print(f"   Track duration: {track_duration_formatted}")
            print(f"   Duration difference: {duration_diff_s:.1f}s")
            print(f"   Duration boost: {duration_boost:.2f}x")
            print(f"   Boosted confidence: {boosted_confidence:.3f}")

            if duration_boost > 1.0:
                improvement = boosted_confidence - original_confidence
                print(f"   🚀 IMPROVEMENT: +{improvement:.3f} confidence boost!")
            else:
                print(f"   📊 No boost applied (duration difference too large)")
        else:
            print(f"   ❌ Track has no duration data")

    # Show best match comparison
    best_original = matches[0]
    best_boosted = max(matches, key=lambda m: min(1.0, m.confidence * calculate_duration_boost(file_duration_ms,
                                                                                               m.track.duration_ms)) if m.track.duration_ms else m.confidence)
    best_boosted_confidence = min(1.0, best_boosted.confidence * calculate_duration_boost(file_duration_ms,
                                                                                          best_boosted.track.duration_ms)) if best_boosted.track.duration_ms else best_boosted.confidence

    print(f"\n🏆 BEST MATCH COMPARISON:")
    print(
        f"   Original best: {best_original.track.artists} - {best_original.track.title} ({best_original.confidence:.3f})")
    print(
        f"   Boosted best:  {best_boosted.track.artists} - {best_boosted.track.title} ({best_boosted_confidence:.3f})")

    if best_boosted_confidence > best_original.confidence:
        print(f"   ✅ Duration boost found better match!")
    elif best_boosted != best_original:
        print(f"   📊 Duration boost changed ranking but not confidence")
    else:
        print(f"   📊 Duration boost improved existing best match")


if __name__ == "__main__":
    # Test with your actual paths

    # OPTION 1: Test a single file
    print("=== SINGLE FILE TEST ===")
    single_file = "K:/tracks_master/Moojo - Secret ID.mp3"  # UPDATE THIS PATH

    if os.path.exists(single_file):
        test_single_file_real_matching(single_file)
    else:
        print("Please update 'single_file' path to test a specific file")

    print("\n" + "=" * 80 + "\n")

    # OPTION 2: Test sample of files
    print("=== SAMPLE COLLECTION TEST ===")
    music_directory = os.getenv('MASTER_TRACKS_DIRECTORY_SSD')

    if os.path.exists(music_directory):
        results = test_real_duration_matching(music_directory, sample_size=10, confidence_threshold=0.75)

        print("\n🎯 IMPLEMENTATION DECISION:")
        if results['accuracy_improvement'] >= 2:
            print("✅ PROCEED with duration-based confidence boosting implementation")
            print("📈 Expected significant accuracy improvement")
        elif results['accuracy_improvement'] >= 1:
            print("✅ PROCEED with duration-based confidence boosting implementation")
            print("📈 Expected moderate accuracy improvement")
        else:
            print("🤔 Consider testing larger sample or other improvements first")
    else:
        print("Please update 'music_directory' path to your actual music directory")
        print("Then run this script to see the REAL impact on your collection!")
