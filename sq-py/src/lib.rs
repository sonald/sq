use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;

/// Formats the sum of two numbers as string.
#[pyfunction]
fn sum_as_string(a: usize, b: usize) -> PyResult<String> {
    Ok((a + b).to_string())
}

#[pyfunction]
pub fn sq_exec(sql: &str, output: Option<&str>) -> PyResult<String> {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut ds = rt.block_on(async { sq::execute(sql).await.unwrap() });
    match output {
        Some("csv") | None => Ok(ds.to_csv().unwrap()),
        Some(v) => Err(PyTypeError::new_err(format!("type {} not supported", v))),
    }
}

/// A Python module implemented in Rust.
#[pymodule]
fn sq_py(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(sq_exec, m)?)?;
    m.add_function(wrap_pyfunction!(sum_as_string, m)?)
}

