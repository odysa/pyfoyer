use bytes::Bytes;
use foyer::{
    Cache, CacheBuilder, CacheEntry, DefaultHasher, EvictionConfig, FifoConfig, LfuConfig,
    LruConfig, S3FifoConfig,
};
use pyo3::{exceptions::PyValueError, prelude::*, types::PyBytes, Bound};

/// A Python module implemented in Rust.
#[pymodule]
fn pyfoyer(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyCache>()?;
    m.add_class::<PyCacheBuilder>()?;
    m.add_class::<PyCacheEntry>()?;
    m.add_class::<PyEvictionConfig>()?;
    m.add_class::<PyFifoConfig>()?;
    m.add_class::<PyS3FifoConfig>()?;
    m.add_class::<PyLruConfig>()?;
    m.add_class::<PyLfuConfig>()?;
    Ok(())
}

#[pyclass(name = "CacheEntry")]
pub struct PyCacheEntry {
    entry: CacheEntry<Bytes, Bytes>,
}

#[pymethods]
impl PyCacheEntry {
    fn key<'py>(&'py self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, self.entry.key())
    }

    fn value<'py>(&'py self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, self.entry.value())
    }
}

impl From<CacheEntry<Bytes, Bytes>> for PyCacheEntry {
    fn from(entry: CacheEntry<Bytes, Bytes>) -> Self {
        PyCacheEntry { entry }
    }
}

#[pyclass(name = "Cache")]
pub struct PyCache {
    cache: Cache<Bytes, Bytes>,
}

#[pymethods]
impl PyCache {
    #[new]
    fn new(capacity: usize) -> Self {
        PyCache {
            cache: Cache::builder(capacity).build(),
        }
    }

    fn insert<'py>(
        &'py self,
        _py: Python<'py>,
        key: Bound<'py, PyBytes>,
        value: Bound<'py, PyBytes>,
    ) -> PyResult<PyCacheEntry> {
        Ok(self
            .cache
            .insert(
                Bytes::copy_from_slice(key.as_bytes()),
                Bytes::copy_from_slice(value.as_bytes()),
            )
            .into())
    }

    fn contains<'py>(&'py self, _py: Python<'py>, key: Bound<'py, PyBytes>) -> bool {
        self.cache.contains(key.as_bytes())
    }

    fn get<'py>(
        &'py self,
        py: Python<'py>,
        key: Bound<'py, PyBytes>,
    ) -> Option<Bound<'py, PyBytes>> {
        self.cache
            .get(key.as_bytes())
            .map(|entry| PyBytes::new(py, entry.value()))
    }

    fn remove<'py>(
        &'py self,
        _py: Python<'py>,
        key: Bound<'py, PyBytes>,
    ) -> PyResult<PyCacheEntry> {
        match self.cache.remove(key.as_bytes()) {
            Some(entry) => PyResult::Ok(entry.into()),
            None => PyResult::Err(PyErr::new::<PyValueError, _>("Key not found in cache")),
        }
    }

    fn clear(&self) {
        self.cache.clear()
    }

    fn usage(&self) -> usize {
        self.cache.usage()
    }

    fn capacity(&self) -> usize {
        self.cache.capacity()
    }

    fn shards(&self) -> usize {
        self.cache.shards()
    }
}

#[pyclass(name = "EvictionConfig", frozen)]
#[derive(Clone)]
pub struct PyEvictionConfig {
    inner: EvictionConfig,
}

impl From<PyEvictionConfig> for EvictionConfig {
    fn from(cfg: PyEvictionConfig) -> Self {
        cfg.inner
    }
}

#[pymethods]
impl PyEvictionConfig {
    #[staticmethod]
    pub fn fifo(cfg: PyFifoConfig) -> Self {
        PyEvictionConfig {
            inner: EvictionConfig::Fifo(cfg.into()),
        }
    }

    #[staticmethod]
    pub fn s3fifo(cfg: PyS3FifoConfig) -> Self {
        PyEvictionConfig {
            inner: EvictionConfig::S3Fifo(cfg.into()),
        }
    }

    #[staticmethod]
    pub fn lru(cfg: PyLruConfig) -> Self {
        PyEvictionConfig {
            inner: EvictionConfig::Lru(cfg.into()),
        }
    }

    #[staticmethod]
    pub fn lfu(cfg: PyLfuConfig) -> Self {
        PyEvictionConfig {
            inner: EvictionConfig::Lfu(cfg.into()),
        }
    }

    pub fn __repr__(&self) -> String {
        format!("{:?}", self.inner)
    }
}

#[pyclass(name = "FifoConfig")]
#[derive(Debug, Clone)]
pub struct PyFifoConfig {
    inner: FifoConfig,
}

#[pymethods]
impl PyFifoConfig {
    #[new]
    fn new() -> Self {
        PyFifoConfig {
            inner: FifoConfig::default(),
        }
    }
}

impl From<PyFifoConfig> for FifoConfig {
    fn from(cfg: PyFifoConfig) -> Self {
        cfg.inner
    }
}

#[pyclass(name = "S3FifoConfig")]
#[derive(Debug, Clone)]
pub struct PyS3FifoConfig {
    inner: S3FifoConfig,
}

#[pymethods]
impl PyS3FifoConfig {
    #[new]
    fn new(
        small_queue_capacity_ratio: Option<f64>,
        ghost_queue_capacity_ratio: Option<f64>,
        small_to_main_freq_threshold: Option<u8>,
    ) -> Self {
        let mut inner = S3FifoConfig::default();
        if let Some(small_queue_capacity_ratio) = small_queue_capacity_ratio {
            inner.small_queue_capacity_ratio = small_queue_capacity_ratio;
        }
        if let Some(ghost_queue_capacity_ratio) = ghost_queue_capacity_ratio {
            inner.ghost_queue_capacity_ratio = ghost_queue_capacity_ratio;
        }
        if let Some(small_to_main_freq_threshold) = small_to_main_freq_threshold {
            inner.small_to_main_freq_threshold = small_to_main_freq_threshold;
        }
        PyS3FifoConfig { inner }
    }

    #[getter]
    fn small_queue_capacity_ratio(&self) -> f64 {
        self.inner.small_queue_capacity_ratio
    }

    #[getter]
    fn ghost_queue_capacity_ratio(&self) -> f64 {
        self.inner.ghost_queue_capacity_ratio
    }

    #[getter]
    fn small_to_main_freq_threshold(&self) -> u8 {
        self.inner.small_to_main_freq_threshold
    }
}

impl From<PyS3FifoConfig> for S3FifoConfig {
    fn from(cfg: PyS3FifoConfig) -> Self {
        cfg.inner
    }
}

#[pyclass(name = "LruConfig")]
#[derive(Debug, Clone)]
pub struct PyLruConfig {
    inner: LruConfig,
}

#[pymethods]
impl PyLruConfig {
    #[new]
    fn new(high_priority_pool_ratio: Option<f64>) -> Self {
        match high_priority_pool_ratio {
            Some(high_priority_pool_ratio) => PyLruConfig {
                inner: LruConfig {
                    high_priority_pool_ratio,
                },
            },
            None => PyLruConfig {
                inner: LruConfig::default(),
            },
        }
    }

    #[getter]
    fn high_priority_pool_ratio(&self) -> f64 {
        self.inner.high_priority_pool_ratio
    }
}

impl From<PyLruConfig> for LruConfig {
    fn from(cfg: PyLruConfig) -> Self {
        cfg.inner
    }
}

#[pyclass(name = "LfuConfig")]
#[derive(Debug, Clone)]
pub struct PyLfuConfig {
    inner: LfuConfig,
}

#[pymethods]
impl PyLfuConfig {
    #[new]
    fn new(
        window_capacity_ratio: Option<f64>,
        protected_capacity_ratio: Option<f64>,
        cmsketch_eps: Option<f64>,
        cmsketch_confidence: Option<f64>,
    ) -> Self {
        let mut inner = LfuConfig::default();
        if let Some(window_capacity_ratio) = window_capacity_ratio {
            inner.window_capacity_ratio = window_capacity_ratio;
        }
        if let Some(protected_capacity_ratio) = protected_capacity_ratio {
            inner.protected_capacity_ratio = protected_capacity_ratio;
        }
        if let Some(cmsketch_eps) = cmsketch_eps {
            inner.cmsketch_eps = cmsketch_eps;
        }
        if let Some(cmsketch_confidence) = cmsketch_confidence {
            inner.cmsketch_confidence = cmsketch_confidence;
        }
        PyLfuConfig { inner }
    }

    #[getter]
    fn window_capacity_ratio(&self) -> f64 {
        self.inner.window_capacity_ratio
    }

    #[getter]
    fn protected_capacity_ratio(&self) -> f64 {
        self.inner.protected_capacity_ratio
    }

    #[getter]
    fn cmsketch_eps(&self) -> f64 {
        self.inner.cmsketch_eps
    }

    #[getter]
    fn cmsketch_confidence(&self) -> f64 {
        self.inner.cmsketch_confidence
    }
}

impl From<PyLfuConfig> for LfuConfig {
    fn from(cfg: PyLfuConfig) -> Self {
        cfg.inner
    }
}

#[pyclass(name = "CacheBuilder")]
pub struct PyCacheBuilder {
    builder: Option<CacheBuilder<Bytes, Bytes, DefaultHasher>>,
}

#[pymethods]
impl PyCacheBuilder {
    #[new]
    fn new(capacity: usize) -> Self {
        PyCacheBuilder {
            builder: Some(CacheBuilder::new(capacity)),
        }
    }

    fn with_name<'py>(
        mut slf: PyRefMut<'py, Self>,
        _py: Python<'py>,
        name: String,
    ) -> PyRefMut<'py, Self> {
        slf.builder = Some(
            slf.builder
                .take()
                .expect("Builder not initialized")
                .with_name(name),
        );
        slf
    }

    fn with_shards<'py>(
        mut slf: PyRefMut<'py, Self>,
        _py: Python<'py>,
        shards: usize,
    ) -> PyRefMut<'py, Self> {
        slf.builder = Some(
            slf.builder
                .take()
                .expect("Builder not initialized")
                .with_shards(shards),
        );
        slf
    }

    fn with_eviction_config<'py>(
        mut slf: PyRefMut<'py, Self>,
        _py: Python<'py>,
        eviction_config: Bound<'py, PyEvictionConfig>,
    ) -> PyRefMut<'py, Self> {
        let eviction_config = eviction_config.get().inner.clone();

        let builder = slf
            .builder
            .take()
            .expect("Builder not initialized")
            .with_eviction_config(eviction_config);

        slf.builder = Some(builder);
        slf
    }

    fn build<'py>(mut slf: PyRefMut<'py, Self>, _py: Python<'py>) -> PyResult<Py<PyCache>> {
        Python::with_gil(|py| {
            Py::new(
                py,
                PyCache {
                    cache: slf.builder.take().expect("Builder not initialized").build(),
                },
            )
        })
    }
}
