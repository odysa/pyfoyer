from pyfoyer import (
    FifoConfig,
    LfuConfig,
    LruConfig,
    S3FifoConfig,
)


def test_fifo_config():
    config = FifoConfig()
    assert config is not None


def test_s3fifo_config():
    config = S3FifoConfig(
        small_queue_capacity_ratio=0.13234,
        ghost_queue_capacity_ratio=0.435234,
        small_to_main_freq_threshold=10,
    )
    assert config.small_queue_capacity_ratio == 0.13234
    assert config.ghost_queue_capacity_ratio == 0.435234
    assert config.small_to_main_freq_threshold == 10


def test_lru_config():
    config = LruConfig(
        high_priority_pool_ratio=0.23432432,
    )
    assert config.high_priority_pool_ratio == 0.23432432


def test_lfu_config():
    config = LfuConfig(
        window_capacity_ratio=0.1243,
        protected_capacity_ratio=0.234234,
        cmsketch_eps=0.0032401,
        cmsketch_confidence=0.123143,
    )
    assert config.window_capacity_ratio == 0.1243
    assert config.protected_capacity_ratio == 0.234234
    assert config.cmsketch_eps == 0.0032401
    assert config.cmsketch_confidence == 0.123143
