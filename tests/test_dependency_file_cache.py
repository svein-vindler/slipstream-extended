"""Synthetic compatibility at CacheControl's public, real-file cache boundary."""

from cachecontrol.caches import FileCache


def test_public_file_cache_round_trips_and_replaces_existing_value(tmp_path):
    directory = tmp_path / "synthetic-cache"
    key = "https://synthetic.invalid/package-metadata"
    cache = FileCache(directory)

    cache.set(key, b"synthetic older response with trailing bytes")
    assert FileCache(directory).get(key) == b"synthetic older response with trailing bytes"

    cache.set(key, b"new")
    assert FileCache(directory).get(key) == b"new"
