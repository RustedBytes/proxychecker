use std::time::{Duration, Instant};

use futures::stream::{self, StreamExt};
use pyo3::exceptions::{PyRuntimeError, PyStopAsyncIteration, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyDict;
use tokio::runtime::Builder as TokioRuntimeBuilder;
use wreq::header;

const DEFAULT_CHECK_URL: &str = "https://httpbin.org/post";
const DEFAULT_TIMEOUT_MS: u64 = 5_000;
const DEFAULT_CONCURRENCY: usize = 64;
const REQUEST_BODY: &str = "rsloop proxy checker";

#[derive(Clone)]
struct CheckerConfig {
    check_url: String,
    user_agent: String,
    timeout: Duration,
    concurrency: usize,
    return_response: bool,
}

struct ProxyOutcome {
    proxy: String,
    elapsed_ms: u128,
    status: Option<u16>,
    ok: bool,
    error: Option<String>,
    response_text: Option<String>,
}

#[pyclass(module = "rsloop_rust_proxychecker")]
struct PyProxyCheckStream {
    queue: Py<PyAny>,
}

#[pyfunction]
#[pyo3(signature=(proxies, *, user_agent, check_url=None, timeout_ms=DEFAULT_TIMEOUT_MS, concurrency=DEFAULT_CONCURRENCY, return_response=false))]
fn check_proxies(
    py: Python<'_>,
    proxies: Vec<String>,
    user_agent: String,
    check_url: Option<String>,
    timeout_ms: u64,
    concurrency: usize,
    return_response: bool,
) -> PyResult<Bound<'_, PyAny>> {
    if user_agent.trim().is_empty() {
        return Err(PyValueError::new_err("user_agent must not be empty"));
    }
    if timeout_ms == 0 {
        return Err(PyValueError::new_err("timeout_ms must be greater than 0"));
    }
    if concurrency == 0 {
        return Err(PyValueError::new_err("concurrency must be greater than 0"));
    }

    let check_url = check_url.unwrap_or_else(|| DEFAULT_CHECK_URL.to_string());
    wreq::Url::parse(&check_url)
        .map_err(|err| PyValueError::new_err(format!("invalid check_url: {err}")))?;

    let config = CheckerConfig {
        check_url,
        user_agent,
        timeout: Duration::from_millis(timeout_ms),
        concurrency,
        return_response,
    };
    let locals = rsloop::rust_async::get_current_locals(py)?;
    let queue = py.import("asyncio")?.getattr("Queue")?.call0()?.unbind();
    let stream = Py::new(
        py,
        PyProxyCheckStream {
            queue: queue.clone_ref(py),
        },
    )?
    .into_any();

    spawn_proxy_checks(
        proxies,
        config.clone(),
        locals.event_loop(py).unbind(),
        queue,
    );
    rsloop::rust_async::future_into_py(py, async move {
        let _ = config;
        Ok(stream)
    })
}

fn spawn_proxy_checks(
    proxies: Vec<String>,
    config: CheckerConfig,
    loop_obj: Py<PyAny>,
    queue: Py<PyAny>,
) {
    std::thread::spawn(move || {
        let result = run_proxy_checks_blocking(proxies, config.clone(), &loop_obj, &queue);
        if let Err(err) = result {
            let _ = emit_stream_error(&loop_obj, &queue, err);
        }
        let _ = emit_stream_end(&loop_obj, &queue);
    });
}

fn run_proxy_checks_blocking(
    proxies: Vec<String>,
    config: CheckerConfig,
    loop_obj: &Py<PyAny>,
    queue: &Py<PyAny>,
) -> Result<(), String> {
    let runtime = TokioRuntimeBuilder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|err| format!("failed to build tokio runtime for wreq: {err}"))?;

    runtime.block_on(run_proxy_checks_async(proxies, config, loop_obj, queue))
}

async fn run_proxy_checks_async(
    proxies: Vec<String>,
    config: CheckerConfig,
    loop_obj: &Py<PyAny>,
    queue: &Py<PyAny>,
) -> Result<(), String> {
    let client = build_client(&config)?;
    let concurrency = config.concurrency.min(proxies.len().max(1));

    let mut outcomes = stream::iter(proxies.into_iter().map(|proxy| {
        let client = client.clone();
        let config = config.clone();
        async move { check_one_proxy(client, config, proxy).await }
    }))
    .buffer_unordered(concurrency)
    .boxed();

    while let Some(outcome) = outcomes.next().await {
        emit_stream_result(&loop_obj, &queue, outcome)
            .map_err(|err| format!("failed to emit proxy result: {err}"))?;
    }

    Ok(())
}

fn build_client(config: &CheckerConfig) -> Result<wreq::Client, String> {
    let mut builder = wreq::Client::builder()
        .user_agent(config.user_agent.clone())
        .no_proxy()
        .no_keepalive()
        .timeout(config.timeout)
        .read_timeout(config.timeout)
        .connect_timeout(config.timeout)
        .pool_idle_timeout(Some(config.timeout))
        .tcp_keepalive(Some(config.timeout))
        .tcp_keepalive_interval(Some(config.timeout))
        .tcp_user_timeout(Some(config.timeout));

    builder = builder.tcp_nodelay(true);

    builder
        .build()
        .map_err(|err| format!("failed to build wreq client: {err}"))
}

async fn check_one_proxy(
    client: wreq::Client,
    config: CheckerConfig,
    proxy: String,
) -> ProxyOutcome {
    let started = Instant::now();
    let request = client
        .post(config.check_url.clone())
        .proxy(proxy.clone())
        .header(header::CONTENT_TYPE, "text/plain; charset=utf-8")
        .timeout(config.timeout)
        .read_timeout(config.timeout)
        .body(REQUEST_BODY);

    match request.send().await {
        Ok(mut response) => {
            let status = response.status();
            let status_code = status.as_u16();
            let body = if config.return_response {
                response
                    .bytes()
                    .await
                    .map(|bytes| Some(String::from_utf8_lossy(&bytes).into_owned()))
            } else {
                async {
                    while response.chunk().await?.is_some() {}
                    Ok(None)
                }
                .await
            };
            match body {
                Ok(response_text) if status.is_success() => ProxyOutcome {
                    proxy,
                    elapsed_ms: started.elapsed().as_millis(),
                    status: Some(status_code),
                    ok: true,
                    error: None,
                    response_text,
                },
                Ok(response_text) => ProxyOutcome {
                    proxy,
                    elapsed_ms: started.elapsed().as_millis(),
                    status: Some(status_code),
                    ok: false,
                    error: Some(format!("target returned HTTP {status_code}")),
                    response_text,
                },
                Err(err) => ProxyOutcome {
                    proxy,
                    elapsed_ms: started.elapsed().as_millis(),
                    status: Some(status_code),
                    ok: false,
                    error: Some(format!("response body read failed: {err}")),
                    response_text: None,
                },
            }
        }
        Err(err) => ProxyOutcome {
            proxy,
            elapsed_ms: started.elapsed().as_millis(),
            status: None,
            ok: false,
            error: Some(err.to_string()),
            response_text: None,
        },
    }
}

fn build_outcome_dict(py: Python<'_>, outcome: ProxyOutcome) -> PyResult<Py<PyAny>> {
    let item = PyDict::new(py);
    item.set_item("proxy", outcome.proxy)?;
    item.set_item("ok", outcome.ok)?;
    item.set_item(
        "elapsed_ms",
        outcome.elapsed_ms.min(u64::MAX as u128) as u64,
    )?;
    if let Some(status) = outcome.status {
        item.set_item("status", status)?;
    }
    if let Some(error) = outcome.error {
        item.set_item("error", error)?;
    }
    if let Some(response_text) = outcome.response_text {
        item.set_item("response_text", response_text)?;
    }
    Ok(item.unbind().into_any())
}

fn emit_stream_result(
    loop_obj: &Py<PyAny>,
    queue: &Py<PyAny>,
    outcome: ProxyOutcome,
) -> PyResult<()> {
    Python::attach(|py| {
        let value = build_outcome_dict(py, outcome)?;
        let message = PyDict::new(py);
        message.set_item("kind", "item")?;
        message.set_item("value", value)?;
        schedule_queue_put(py, loop_obj, queue, message.unbind().into_any())
    })
}

fn emit_stream_error(loop_obj: &Py<PyAny>, queue: &Py<PyAny>, error: String) -> PyResult<()> {
    Python::attach(|py| {
        let message = PyDict::new(py);
        message.set_item("kind", "error")?;
        message.set_item("error", error)?;
        schedule_queue_put(py, loop_obj, queue, message.unbind().into_any())
    })
}

fn emit_stream_end(loop_obj: &Py<PyAny>, queue: &Py<PyAny>) -> PyResult<()> {
    Python::attach(|py| {
        let message = PyDict::new(py);
        message.set_item("kind", "end")?;
        schedule_queue_put(py, loop_obj, queue, message.unbind().into_any())
    })
}

fn schedule_queue_put(
    py: Python<'_>,
    loop_obj: &Py<PyAny>,
    queue: &Py<PyAny>,
    message: Py<PyAny>,
) -> PyResult<()> {
    let queue_bound = queue.bind(py);
    let put_nowait = queue_bound.getattr("put_nowait")?;
    loop_obj
        .bind(py)
        .call_method1("call_soon_threadsafe", (put_nowait, message))?;
    Ok(())
}

#[pymethods]
impl PyProxyCheckStream {
    fn __aiter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    fn __anext__(slf: Py<Self>, py: Python<'_>) -> PyResult<Bound<'_, PyAny>> {
        let queue = slf.borrow(py).queue.clone_ref(py);
        let locals = rsloop::rust_async::get_current_locals(py)?;

        rsloop::rust_async::future_into_py_with_locals(py, locals.clone(), async move {
            let queued = Python::attach(|py| {
                let awaitable = queue.bind(py).call_method0("get")?;
                rsloop::rust_async::into_future_with_locals(&locals, awaitable)
            })?
            .await?;

            Python::attach(|py| {
                let message = queued.bind(py).cast::<PyDict>()?;
                let kind = message
                    .get_item("kind")?
                    .ok_or_else(|| PyRuntimeError::new_err("stream message missing kind"))?
                    .extract::<String>()?;

                match kind.as_str() {
                    "item" => {
                        let value = message
                            .get_item("value")?
                            .ok_or_else(|| PyRuntimeError::new_err("stream item missing value"))?;
                        Ok(value.unbind())
                    }
                    "error" => {
                        let error = message
                            .get_item("error")?
                            .ok_or_else(|| PyRuntimeError::new_err("stream error missing payload"))?
                            .extract::<String>()?;
                        Err(PyRuntimeError::new_err(error))
                    }
                    "end" => Err(PyStopAsyncIteration::new_err(())),
                    other => Err(PyRuntimeError::new_err(format!(
                        "unexpected stream message kind: {other}"
                    ))),
                }
            })
        })
    }
}

#[pymodule(gil_used = false)]
fn rsloop_rust_proxychecker(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyProxyCheckStream>()?;
    m.add_function(wrap_pyfunction!(check_proxies, m)?)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    async fn check_response(status: u16, return_response: bool, truncated: bool) -> ProxyOutcome {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut buffer = [0; 1024];
            loop {
                let count = socket.read(&mut buffer).await.unwrap();
                assert_ne!(count, 0, "client closed before sending request body");
                request.extend_from_slice(&buffer[..count]);
                if let Some(end) = request.windows(4).position(|part| part == b"\r\n\r\n") {
                    if request.len() >= end + 4 + REQUEST_BODY.len() {
                        break;
                    }
                }
            }
            assert!(request.starts_with(b"POST http://target.invalid/check HTTP/1.1\r\n"));
            if truncated {
                socket.write_all(format!(
                    "HTTP/1.1 {status} Test\r\nContent-Length: 100\r\nConnection: close\r\n\r\nshort"
                ).as_bytes()).await.unwrap();
            } else {
                socket.write_all(format!(
                    "HTTP/1.1 {status} Test\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n"
                ).as_bytes()).await.unwrap();
                socket.write_all(b"3\r\nhel\r\n").await.unwrap();
                socket.write_all(b"3\r\nlo\xff\r\n0\r\n\r\n").await.unwrap();
            }
            socket.shutdown().await.unwrap();
        });
        let config = CheckerConfig {
            check_url: "http://target.invalid/check".into(),
            user_agent: "proxychecker regression test".into(),
            timeout: Duration::from_secs(5),
            concurrency: 1,
            return_response,
        };
        let outcome = check_one_proxy(build_client(&config).unwrap(), config, proxy.clone()).await;
        server.await.unwrap();
        assert_eq!(outcome.proxy, proxy);
        assert_eq!(outcome.status, Some(status));
        outcome
    }

    #[tokio::test]
    async fn discards_body_for_success_and_non_success_status() {
        for status in [200, 503] {
            let outcome = check_response(status, false, false).await;
            assert_eq!(outcome.ok, status == 200);
            assert_eq!(outcome.response_text, None);
            assert_eq!(
                outcome.error,
                (status != 200).then(|| format!("target returned HTTP {status}"))
            );
        }
    }

    #[tokio::test]
    async fn body_read_errors_fail_for_both_modes_and_statuses() {
        for return_response in [false, true] {
            for status in [200, 503] {
                let outcome = check_response(status, return_response, true).await;
                assert!(!outcome.ok);
                assert_eq!(outcome.response_text, None);
                assert!(outcome
                    .error
                    .unwrap()
                    .starts_with("response body read failed: "));
            }
        }
    }

    #[tokio::test]
    async fn return_response_preserves_full_body_and_lossy_utf8() {
        for status in [200, 503] {
            let outcome = check_response(status, true, false).await;
            assert_eq!(outcome.ok, status == 200);
            assert_eq!(outcome.response_text.as_deref(), Some("hello\u{fffd}"));
            assert_eq!(
                outcome.error,
                (status != 200).then(|| format!("target returned HTTP {status}"))
            );
        }
    }
}
