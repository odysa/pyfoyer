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

#[pyclass]
#[derive(Debug, Clone)]
pub struct PyFifoConfig {
    inner: FifoConfig,
}

impl From<PyFifoConfig> for FifoConfig {
    fn from(cfg: PyFifoConfig) -> Self {
        cfg.inner
    }
}

#[pyclass]
#[derive(Debug, Clone)]
pub struct PyS3FifoConfig {
    inner: S3FifoConfig,
}

impl From<PyS3FifoConfig> for S3FifoConfig {
    fn from(cfg: PyS3FifoConfig) -> Self {
        cfg.inner
    }
}

#[pyclass]
#[derive(Debug, Clone)]
pub struct PyLruConfig {
    inner: LruConfig,
}

impl From<PyLruConfig> for LruConfig {
    fn from(cfg: PyLruConfig) -> Self {
        cfg.inner
    }
}

#[pyclass]
#[derive(Debug, Clone)]
pub struct PyLfuConfig {
    inner: LfuConfig,
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
