use super::*;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

#[test]
fn proxy_parser_rejects_unsupported_schemes() {
    for proxy in [
        "invalid://proxy",
        "ftp://127.0.0.1:8080",
        "file:///tmp/proxy",
    ] {
        assert!(parse_proxy(proxy).is_err());
    }
}

#[test]
fn proxy_parser_preserves_supported_schemes_and_bare_addresses() {
    for proxy in [
        "http://127.0.0.1:8080",
        "https://127.0.0.1:8080",
        "socks4://127.0.0.1:1080",
        "socks4a://127.0.0.1:1080",
        "socks5://user:pass@127.0.0.1:1080",
        "socks5h://127.0.0.1:1080",
        "127.0.0.1:8080",
    ] {
        assert!(parse_proxy(proxy).is_ok(), "failed to parse {proxy}");
    }
}

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
            socket
                .write_all(
                    format!(
                "HTTP/1.1 {status} Test\r\nContent-Length: 100\r\nConnection: close\r\n\r\nshort"
            )
                    .as_bytes(),
                )
                .await
                .unwrap();
        } else {
            socket
                .write_all(
                    format!(
                "HTTP/1.1 {status} Test\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n"
            )
                    .as_bytes(),
                )
                .await
                .unwrap();
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

fn config_for(check_url: &str) -> CheckerConfig {
    CheckerConfig {
        check_url: check_url.into(),
        user_agent: "test".into(),
        timeout: Duration::from_secs(1),
        concurrency: 2,
        return_response: false,
    }
}

#[test]
fn malformed_proxy_and_invalid_user_agent_are_rejected() {
    for proxy in ["http://", "http://[", "", "socks5://"] {
        assert!(
            parse_proxy(proxy).is_err(),
            "accepted malformed proxy {proxy}"
        );
    }
    let mut config = config_for("http://target.invalid/check");
    config.user_agent = "invalid\nheader".into();
    assert!(build_client(&config).is_err());
}

#[tokio::test]
async fn invalid_proxy_and_transport_errors_have_no_http_status() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let unavailable = format!("http://{}", listener.local_addr().unwrap());
    drop(listener);
    for proxy in ["invalid://proxy".to_string(), unavailable] {
        let config = config_for("http://target.invalid/check");
        let outcome = check_one_proxy(build_client(&config).unwrap(), config, proxy.clone()).await;
        assert!(!outcome.ok);
        assert_eq!(outcome.proxy, proxy);
        assert!(outcome.error.is_some());
        assert_eq!(outcome.status, None);
        assert_eq!(outcome.response_text, None);
    }
}
