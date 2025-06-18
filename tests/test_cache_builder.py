from pyfoyer import CacheBuilder


def test_cache_builder():
    capacity = 1000
    shards = 135
    builder = CacheBuilder(capacity)
    builder = builder.with_name("test")
    builder = builder.with_shards(shards)
    cache = builder.build()
    assert cache is not None
    assert cache.capacity() == capacity
    assert cache.usage() == 0
    assert cache.shards() == shards
