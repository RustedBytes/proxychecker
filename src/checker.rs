//! HTTP client configuration, proxy validation, and single-proxy checks.

use std::time::Instant;

use futures::StreamExt;
use reqwest::header;

use crate::config::{CheckerConfig, REQUEST_BODY};

/// Result of one proxy check before conversion to a Python dictionary.
pub(crate) struct ProxyOutcome {
    pub(crate) proxy: String,
    pub(crate) elapsed_ms: u128,
    pub(crate) status: Option<u16>,
    pub(crate) ok: bool,
    pub(crate) error: Option<String>,
    pub(crate) response_text: Option<String>,
}

/// Build a Tokio HTTP client with the configured timeout and disabled idle pooling.
pub(crate) fn build_client(
    config: &CheckerConfig,
    proxy: Option<reqwest::Proxy>,
) -> Result<reqwest::Client, String> {
    let mut builder = reqwest::Client::builder()
        .tls_backend_rustls()
        .user_agent(config.user_agent.clone())
        .no_proxy()
        .pool_max_idle_per_host(0)
        .timeout(config.timeout)
        .read_timeout(config.timeout)
        .connect_timeout(config.timeout)
        .pool_idle_timeout(Some(config.timeout))
        .tcp_keepalive(Some(config.timeout))
        .tcp_keepalive_interval(Some(config.timeout))
        .tcp_user_timeout(Some(config.timeout));

    if let Some(proxy) = proxy {
        builder = builder.proxy(proxy);
    }
    builder = builder.tcp_nodelay(true);

    builder
        .build()
        .map_err(|err| format!("failed to build reqwest client: {err}"))
}

/// Parse HTTP/HTTPS/SOCKS proxies, rejecting schemes that would bypass the proxy.
fn parse_proxy(proxy: &str) -> Result<reqwest::Proxy, String> {
    // Preserve support for host:port inputs while rejecting schemes that reqwest's
    // matcher silently ignores (which would otherwise send the request directly).
    let uri = if proxy.contains("://") {
        url::Url::parse(proxy)
    } else {
        url::Url::parse(&format!("http://{proxy}"))
    }
    .map_err(|err| format!("invalid proxy URL: {err}"))?;
    if uri.host_str().is_none_or(str::is_empty) {
        return Err("invalid proxy URL: missing host".into());
    }
    if !matches!(
        uri.scheme(),
        "http" | "https" | "socks4" | "socks4a" | "socks5" | "socks5h"
    ) {
        return Err(format!("unsupported proxy scheme: {}", uri.scheme()));
    }
    reqwest::Proxy::all(uri.as_str()).map_err(|err| err.to_string())
}

/// POST through one proxy, drain its body, and record transport or HTTP failures.
///
/// Body-read failures override status-based success; text is collected only when requested.
pub(crate) async fn check_one_proxy(config: CheckerConfig, proxy: String) -> ProxyOutcome {
    let started = Instant::now();
    let request_proxy = match parse_proxy(&proxy) {
        Ok(request_proxy) => request_proxy,
        Err(err) => {
            return ProxyOutcome {
                proxy,
                elapsed_ms: started.elapsed().as_millis(),
                status: None,
                ok: false,
                error: Some(err),
                response_text: None,
            };
        }
    };
    let client = match build_client(&config, Some(request_proxy)) {
        Ok(client) => client,
        Err(err) => {
            return ProxyOutcome {
                proxy,
                elapsed_ms: started.elapsed().as_millis(),
                status: None,
                ok: false,
                error: Some(err),
                response_text: None,
            };
        }
    };
    let request = client
        .post(config.check_url.clone())
        .header(header::CONTENT_TYPE, "text/plain; charset=utf-8")
        .timeout(config.timeout)
        .body(REQUEST_BODY);

    match request.send().await {
        Ok(response) => {
            let status = response.status();
            let status_code = status.as_u16();
            let body = if config.return_response {
                response
                    .bytes()
                    .await
                    .map(|bytes| Some(String::from_utf8_lossy(&bytes).into_owned()))
            } else {
                async {
                    let mut chunks = response.bytes_stream().boxed();
                    while let Some(chunk) = chunks.next().await {
                        drop(chunk?);
                    }
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

#[cfg(test)]
#[path = "checker_tests.rs"]
mod tests;
