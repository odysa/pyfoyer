use bytes::Bytes;
use foyer::{Cache, CacheBuilder, CacheEntry, DefaultHasher};
use pyo3::{exceptions::PyValueError, prelude::*, types::PyBytes, Bound};

/// A Python module implemented in Rust.
#[pymodule]
fn pyfoyer(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyCache>()?;
    m.add_class::<PyCacheBuilder>()?;
    m.add_class::<PyCacheEntry>()?;
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

struct CacheBuilderInner(CacheBuilder<Bytes, Bytes, DefaultHasher>);

impl Default for CacheBuilderInner {
    fn default() -> Self {
        CacheBuilderInner(CacheBuilder::new(1000))
    }
}

#[pyclass(name = "CacheBuilder")]
pub struct PyCacheBuilder {
    builder: CacheBuilderInner,
}

#[pymethods]
impl PyCacheBuilder {
    #[new]
    fn new(capacity: usize) -> Self {
        PyCacheBuilder {
            builder: CacheBuilderInner(CacheBuilder::new(capacity)),
        }
    }

    fn with_name<'py>(
        mut slf: PyRefMut<'py, Self>,
        _py: Python<'py>,
        name: String,
    ) -> PyRefMut<'py, Self> {
        let inner = std::mem::take(&mut slf.builder);
        let inner = inner.0.with_name(name);
        slf.builder = CacheBuilderInner(inner);
        slf
    }

    fn with_shards<'py>(
        mut slf: PyRefMut<'py, Self>,
        _py: Python<'py>,
        shards: usize,
    ) -> PyRefMut<'py, Self> {
        let inner = std::mem::take(&mut slf.builder);
        let inner = inner.0.with_shards(shards);
        slf.builder = CacheBuilderInner(inner);
        slf
    }

    fn build<'py>(mut slf: PyRefMut<'py, Self>) -> PyResult<Py<PyCache>> {
        let builder = std::mem::take(&mut slf.builder);
        let cache = builder.0.build();
        let py_cache = PyCache { cache };
        Python::with_gil(|py| Py::new(py, py_cache))
    }
}
