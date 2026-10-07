use super::*;
use pyo3::ffi::c_str;

fn with_python(test: impl for<'py> FnOnce(Python<'py>)) {
    Python::initialize();
    Python::attach(test);
}

#[test]
fn outcome_dict_preserves_optional_fields_and_saturates_elapsed() {
    with_python(|py| {
        let full = ProxyOutcome {
            proxy: "http://proxy".into(),
            elapsed_ms: u128::MAX,
            status: Some(503),
            ok: false,
            error: Some("HTTP failure".into()),
            response_text: Some("body".into()),
        };
        let dict = build_outcome_dict(py, full).unwrap();
        let dict = dict.bind(py).cast::<PyDict>().unwrap();
        assert_eq!(dict.len(), 6);
        assert_eq!(
            dict.get_item("elapsed_ms")
                .unwrap()
                .unwrap()
                .extract::<u64>()
                .unwrap(),
            u64::MAX
        );
        assert_eq!(
            dict.get_item("status")
                .unwrap()
                .unwrap()
                .extract::<u16>()
                .unwrap(),
            503
        );
        assert_eq!(
            dict.get_item("error")
                .unwrap()
                .unwrap()
                .extract::<String>()
                .unwrap(),
            "HTTP failure"
        );
        assert_eq!(
            dict.get_item("response_text")
                .unwrap()
                .unwrap()
                .extract::<String>()
                .unwrap(),
            "body"
        );
        let empty = ProxyOutcome {
            proxy: "invalid://proxy".into(),
            elapsed_ms: 7,
            status: None,
            ok: false,
            error: None,
            response_text: None,
        };
        let dict = build_outcome_dict(py, empty).unwrap();
        let dict = dict.bind(py).cast::<PyDict>().unwrap();
        assert_eq!(dict.len(), 3);
        assert_eq!(
            dict.get_item("elapsed_ms")
                .unwrap()
                .unwrap()
                .extract::<u64>()
                .unwrap(),
            7
        );
    });
}

#[test]
fn queue_messages_deliver_items_errors_and_end() {
    with_python(|py| {
        let queue = py
            .eval(c_str!("__import__('asyncio').Queue()"), None, None)
            .unwrap()
            .unbind();
        let loop_obj = py
            .eval(
                c_str!(
                    "type('Loop', (), {'call_soon_threadsafe': lambda self, cb, msg: cb(msg)})()"
                ),
                None,
                None,
            )
            .unwrap()
            .unbind();
        let outcome = ProxyOutcome {
            proxy: "proxy".into(),
            elapsed_ms: 1,
            status: Some(200),
            ok: true,
            error: None,
            response_text: None,
        };
        emit_stream_result(&loop_obj, &queue, outcome).unwrap();
        let message = queue.bind(py).call_method0("get_nowait").unwrap();
        let item = decode_message(&message).unwrap();
        assert!(item
            .bind(py)
            .get_item("ok")
            .unwrap()
            .extract::<bool>()
            .unwrap());
        emit_stream_error(&loop_obj, &queue, "worker failed".into()).unwrap();
        let message = queue.bind(py).call_method0("get_nowait").unwrap();
        let err = decode_message(&message).unwrap_err();
        assert!(err.is_instance_of::<PyRuntimeError>(py));
        assert_eq!(err.value(py).to_string(), "worker failed");
        emit_stream_end(&loop_obj, &queue).unwrap();
        let message = queue.bind(py).call_method0("get_nowait").unwrap();
        assert!(decode_message(&message)
            .unwrap_err()
            .is_instance_of::<PyStopAsyncIteration>(py));
    });
}

#[test]
fn malformed_messages_raise_descriptive_errors() {
    with_python(|py| {
        for (kind, expected) in [
            (None, "stream message missing kind"),
            (Some("item"), "stream item missing value"),
            (Some("error"), "stream error missing payload"),
            (Some("other"), "unexpected stream message kind: other"),
        ] {
            let message = PyDict::new(py);
            if let Some(kind) = kind {
                message.set_item("kind", kind).unwrap();
            }
            let err = decode_message(message.as_any()).unwrap_err();
            assert!(err.is_instance_of::<PyRuntimeError>(py));
            assert_eq!(err.value(py).to_string(), expected);
        }
        assert!(decode_message(py.None().bind(py)).is_err());
    });
}

#[test]
fn queue_delivery_propagates_event_loop_errors() {
    with_python(|py| {
        let closed_loop = py.None();
        let queue = py
            .eval(c_str!("__import__('asyncio').Queue()"), None, None)
            .unwrap()
            .unbind();
        assert!(emit_stream_end(&closed_loop, &queue).is_err());
    });
}

#[test]
fn stream_drop_and_pending_iteration_cancel_but_completion_does_not() {
    let (cancel, cancelled) = tokio::sync::watch::channel(false);
    PendingIteration(Some(cancel.clone())).complete();
    assert!(!*cancelled.borrow());
    drop(PendingIteration(Some(cancel.clone())));
    assert!(*cancelled.borrow());
    cancel.send(false).unwrap();
    Python::initialize();
    Python::attach(|py| {
        let stream = PyProxyCheckStream {
            queue: py.None(),
            cancel,
            credits: std::sync::Arc::new(tokio::sync::Semaphore::new(1)),
        };
        drop(stream);
    });
    assert!(*cancelled.borrow());
}
