from pyfoyer import (
    CacheBuilder,
    EvictionConfig,
    FifoConfig,
    LfuConfig,
    LruConfig,
    S3FifoConfig,
)


def test_cache_builder_with_shards():
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


def test_cache_builder_with_eviction_config():
    configs = [
        EvictionConfig.fifo(FifoConfig()),
        EvictionConfig.s3fifo(
            S3FifoConfig(
                small_queue_capacity_ratio=0.5,
                ghost_queue_capacity_ratio=0.5,
                small_to_main_freq_threshold=5,
            )
        ),
        EvictionConfig.lru(LruConfig(high_priority_pool_ratio=0.5)),
        EvictionConfig.lfu(
            LfuConfig(
                window_capacity_ratio=0.2,
                protected_capacity_ratio=0.5,
                cmsketch_eps=0.0001,
                cmsketch_confidence=0.99,
            )
        ),
    ]
    capacity = 1000
    for config in configs:
        builder = CacheBuilder(capacity)
        builder = builder.with_name("test")
        builder = builder.with_eviction_config(config)
        cache = builder.build()
        assert cache is not None
        assert cache.capacity() == capacity
        assert cache.usage() == 0
