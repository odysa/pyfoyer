from pyfoyer import Cache, CacheBuilder


def test_cache_insert():
    cache = Cache(capacity=10)
    entry = cache.insert(b"key", b"value")
    assert cache.contains(b"key")
    assert entry.key() == b"key"
    assert entry.value() == b"value"


def test_cache_get():
    count = 500
    cache = Cache(capacity=1000)
    for i in range(count):
        cache.insert(f"key_{i}".encode(), f"value_{i}".encode())
        assert cache.contains(f"key_{i}".encode())
        assert cache.usage() == i + 1
    assert cache.usage() == count
    for i in range(count):
        assert cache.get(f"key_{i}".encode()) == f"value_{i}".encode()


def test_cache_remove():
    cache = Cache(capacity=10)
    cache.insert(b"key", b"value")
    assert cache.contains(b"key")
    entry = cache.remove(b"key")
    assert entry.key() == b"key"
    assert entry.value() == b"value"
    assert not cache.contains(b"key")


def test_cache_clear():
    cache = Cache(capacity=10)
    cache.insert(b"key", b"value")
    assert cache.contains(b"key")
    cache.clear()
    assert not cache.contains(b"key")


def test_cache_usage():
    count = 1000
    cache = Cache(capacity=count * 2)
    for i in range(count):
        cache.insert(f"key_{i}".encode(), f"value_{i}".encode())

    assert cache.usage() == count


def test_cache_shards():
    count = 1000
    for i in range(1, count):
        cache = CacheBuilder(10).with_shards(i).build()
        assert cache.shards() == i


def test_cache_capacity():
    count = 1000
    for i in range(1, count):
        cache = CacheBuilder(i).build()
        assert cache.capacity() == i
