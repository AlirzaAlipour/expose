use bytes::Bytes;
use expose_common::protocol::Message;

/// Rewrite response headers (Location, Content-Location, Set-Cookie paths) so
/// redirected URLs remain within the path-based tunnel prefix.
pub fn rewrite_response_headers(message: Message, path_prefix: &str, tunnel_name: &str) -> Message {
    match message {
        Message::HttpResponse {
            id,
            status,
            headers,
            body,
        } => {
            let prefix = build_prefix(path_prefix, tunnel_name);
            let mut rewritten = Vec::with_capacity(headers.len());
            for (name, value) in headers {
                let lower = name.to_ascii_lowercase();
                if lower == "location" || lower == "content-location" {
                    let new_value = rewrite_location(&value, &prefix);
                    rewritten.push((name, new_value));
                } else if lower == "set-cookie" {
                    let new_value = rewrite_cookie_path(&value, &prefix);
                    rewritten.push((name, new_value));
                } else {
                    rewritten.push((name, value));
                }
            }
            Message::HttpResponse {
                id,
                status,
                headers: rewritten,
                body,
            }
        }
        other => other,
    }
}

/// Inject a `<base>` tag into HTML responses so browsers resolve relative URLs
/// against the tunnel prefix.
pub fn maybe_inject_base_tag(message: Message, path_prefix: &str, tunnel_name: &str) -> Message {
    let prefix = build_prefix(path_prefix, tunnel_name);
    match message {
        Message::HttpResponse {
            id,
            status,
            headers,
            body,
        } => {
            let is_html = headers.iter().any(|(name, value)| {
                name.eq_ignore_ascii_case("content-type")
                    && value.to_ascii_lowercase().contains("text/html")
            });
            if !is_html {
                return Message::HttpResponse {
                    id,
                    status,
                    headers,
                    body,
                };
            }
            let body_str = match std::str::from_utf8(&body) {
                Ok(text) => text,
                Err(_) => {
                    return Message::HttpResponse {
                        id,
                        status,
                        headers,
                        body,
                    }
                }
            };
            let base_tag = format!("<base href=\"{}/\">", prefix);
            let lower = body_str.to_ascii_lowercase();
            let mut injected = String::new();
            if let Some(head_index) = lower.find("<head") {
                if let Some(close_offset) = lower[head_index..].find('>') {
                    let insert_pos = head_index + close_offset + 1;
                    injected.push_str(&body_str[..insert_pos]);
                    injected.push_str(&base_tag);
                    injected.push('\n');
                    injected.push_str(&body_str[insert_pos..]);
                } else {
                    injected = format!("{}{}", base_tag, body_str);
                }
            } else {
                injected = format!("{}{}", base_tag, body_str);
            }
            let mut new_headers = Vec::with_capacity(headers.len());
            for (name, value) in headers.into_iter() {
                if name.eq_ignore_ascii_case("content-length") {
                    new_headers.push((name, injected.len().to_string()));
                } else {
                    new_headers.push((name, value));
                }
            }
            Message::HttpResponse {
                id,
                status,
                headers: new_headers,
                body: Bytes::from(injected),
            }
        }
        other => other,
    }
}

fn build_prefix(path_prefix: &str, tunnel_name: &str) -> String {
    let trimmed = path_prefix.trim_end_matches('/');
    if trimmed.is_empty() {
        format!("/{}", tunnel_name)
    } else {
        format!("{}/{}", trimmed, tunnel_name)
    }
}

/// Rewrite absolute Location header values with the provided prefix.
pub fn rewrite_location(value: &str, prefix: &str) -> String {
    if value.is_empty() {
        return value.to_string();
    }
    if value.starts_with("http://") || value.starts_with("https://") {
        return rewrite_absolute_url(value, prefix);
    }
    if value.starts_with('/') {
        return ensure_prefixed_path(value, prefix);
    }
    value.to_string()
}

fn rewrite_absolute_url(value: &str, prefix: &str) -> String {
    if let Some(scheme_idx) = value.find("://") {
        let (scheme, rest) = value.split_at(scheme_idx + 3);
        if let Some(path_idx) = rest.find('/') {
            let (host, path) = rest.split_at(path_idx);
            let rewritten_path = ensure_prefixed_path(path, prefix);
            format!("{}{}{}", scheme, host, rewritten_path)
        } else {
            let rewritten_path = ensure_prefixed_path("/", prefix);
            format!("{}{}{}", scheme, rest, rewritten_path)
        }
    } else {
        value.to_string()
    }
}

fn ensure_prefixed_path(path: &str, prefix: &str) -> String {
    if path.starts_with(prefix) {
        return path.to_string();
    }
    if path == "/" {
        return format!("{}/", prefix);
    }
    if path.starts_with('/') {
        format!("{}{}", prefix, path)
    } else {
        path.to_string()
    }
}

/// Rewrite cookie Path= attributes to include the tunnel prefix.
pub fn rewrite_cookie_path(value: &str, prefix: &str) -> String {
    let mut changed = false;
    let mut parts = Vec::new();
    for segment in value.split(';') {
        let leading_ws_len = segment.chars().take_while(|c| c.is_whitespace()).count();
        let (leading_ws, rest) = segment.split_at(leading_ws_len);
        if rest.len() >= 5 && rest[..5].eq_ignore_ascii_case("path=") {
            let path_value = &rest[5..];
            if path_value.starts_with('/') && !path_value.starts_with(prefix) {
                let new_segment = format!("{}Path={}{}", leading_ws, prefix, path_value);
                parts.push(new_segment);
                changed = true;
                continue;
            }
        }
        parts.push(format!("{}{}", leading_ws, rest));
    }
    if !changed {
        return value.to_string();
    }
    let mut rewritten = parts.join(";");
    if value.ends_with(';') {
        rewritten.push(';');
    }
    rewritten
}

#[cfg(test)]
mod tests {
    use super::*;
    use expose_common::protocol::Message;

    fn headers_with_location(value: &str) -> Vec<(String, String)> {
        vec![("Location".into(), value.into())]
    }

    fn sample_response(headers: Vec<(String, String)>) -> Message {
        Message::HttpResponse {
            id: uuid::Uuid::nil(),
            status: 302,
            headers,
            body: Bytes::new(),
        }
    }

    #[test]
    fn rewrites_absolute_path_location() {
        let msg = sample_response(headers_with_location("/admin/"));
        let rewritten = rewrite_response_headers(msg, "/t", "demo");
        if let Message::HttpResponse { headers, .. } = rewritten {
            assert_eq!(headers[0].1, "/t/demo/admin/");
        } else {
            panic!("unexpected message")
        }
    }

    #[test]
    fn rewrites_root_path_location() {
        let msg = sample_response(headers_with_location("/"));
        let rewritten = rewrite_response_headers(msg, "/t", "demo");
        if let Message::HttpResponse { headers, .. } = rewritten {
            assert_eq!(headers[0].1, "/t/demo/");
        }
    }

    #[test]
    fn leaves_prefixed_location() {
        let msg = sample_response(headers_with_location("/t/demo/admin"));
        let rewritten = rewrite_response_headers(msg, "/t", "demo");
        if let Message::HttpResponse { headers, .. } = rewritten {
            assert_eq!(headers[0].1, "/t/demo/admin");
        }
    }

    #[test]
    fn leaves_relative_location() {
        let msg = sample_response(headers_with_location("next"));
        let rewritten = rewrite_response_headers(msg, "/t", "demo");
        if let Message::HttpResponse { headers, .. } = rewritten {
            assert_eq!(headers[0].1, "next");
        }
    }

    #[test]
    fn rewrites_full_url_location() {
        let msg = sample_response(headers_with_location("http://host.com/admin"));
        let rewritten = rewrite_response_headers(msg, "/t", "demo");
        if let Message::HttpResponse { headers, .. } = rewritten {
            assert_eq!(headers[0].1, "http://host.com/t/demo/admin");
        }
    }

    #[test]
    fn preserves_query_string() {
        let msg = sample_response(headers_with_location("/login?next=/dash"));
        let rewritten = rewrite_response_headers(msg, "/t", "demo");
        if let Message::HttpResponse { headers, .. } = rewritten {
            assert_eq!(headers[0].1, "/t/demo/login?next=/dash");
        }
    }

    #[test]
    fn rewrites_cookie_path_attribute() {
        let cookie = "sid=abc; Path=/; HttpOnly";
        let rewritten = rewrite_cookie_path(cookie, "/t/demo");
        assert_eq!(rewritten, "sid=abc; Path=/t/demo/; HttpOnly");
    }

    #[test]
    fn rewrites_cookie_with_subpath() {
        let cookie = "sid=abc; Path=/api; HttpOnly";
        let rewritten = rewrite_cookie_path(cookie, "/t/demo");
        assert_eq!(rewritten, "sid=abc; Path=/t/demo/api; HttpOnly");
    }

    #[test]
    fn leaves_cookie_without_path() {
        let cookie = "sid=abc; HttpOnly";
        let rewritten = rewrite_cookie_path(cookie, "/t/demo");
        assert_eq!(rewritten, cookie);
    }

    #[test]
    fn leaves_cookie_with_existing_prefix() {
        let cookie = "sid=abc; Path=/t/demo/; HttpOnly";
        let rewritten = rewrite_cookie_path(cookie, "/t/demo");
        assert_eq!(rewritten, cookie);
    }

    #[test]
    fn rewrites_multiple_headers() {
        let msg = Message::HttpResponse {
            id: uuid::Uuid::nil(),
            status: 302,
            headers: vec![
                ("Location".into(), "/admin".into()),
                ("Content-Location".into(), "/config".into()),
                ("Set-Cookie".into(), "sid=1; Path=/".into()),
                ("Set-Cookie".into(), "auth=1; Path=/api".into()),
            ],
            body: Bytes::new(),
        };
        let rewritten = rewrite_response_headers(msg, "/path", "demo");
        if let Message::HttpResponse { headers, .. } = rewritten {
            assert_eq!(headers[0].1, "/path/demo/admin");
            assert_eq!(headers[1].1, "/path/demo/config");
            assert_eq!(headers[2].1, "sid=1; Path=/path/demo/");
            assert_eq!(headers[3].1, "auth=1; Path=/path/demo/api");
        }
    }

    #[test]
    fn injects_base_tag_updates_length() {
        let body = Bytes::from("<html><head><title>Test</title></head><body>ok</body></html>");
        let msg = Message::HttpResponse {
            id: uuid::Uuid::nil(),
            status: 200,
            headers: vec![
                ("Content-Type".into(), "text/html".into()),
                ("Content-Length".into(), body.len().to_string()),
            ],
            body,
        };
        let rewritten = maybe_inject_base_tag(msg, "/t", "demo");
        if let Message::HttpResponse { headers, body, .. } = rewritten {
            assert!(std::str::from_utf8(&body)
                .unwrap()
                .contains("<base href=\"/t/demo/\">"));
            let len_header = headers
                .iter()
                .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
                .unwrap();
            assert_eq!(len_header.1, body.len().to_string());
        }
    }

    #[test]
    fn base_tag_insertion_without_head() {
        let body = Bytes::from("<html><body>ok</body></html>");
        let msg = Message::HttpResponse {
            id: uuid::Uuid::nil(),
            status: 200,
            headers: vec![("Content-Type".into(), "text/html".into())],
            body,
        };
        let rewritten = maybe_inject_base_tag(msg, "/t", "demo");
        if let Message::HttpResponse { body, .. } = rewritten {
            let body_text = std::str::from_utf8(&body).unwrap();
            assert!(
                body_text.starts_with("<base href=\"/t/demo/\">")
                    || body_text.contains("<base href=\"/t/demo/\">")
            );
        }
    }

    #[test]
    fn html_detection_case_insensitive() {
        let body = Bytes::from("<html></html>");
        let msg = Message::HttpResponse {
            id: uuid::Uuid::nil(),
            status: 200,
            headers: vec![("Content-Type".into(), "Text/Html; charset=utf-8".into())],
            body,
        };
        let rewritten = maybe_inject_base_tag(msg, "/t", "demo");
        if let Message::HttpResponse { body, .. } = rewritten {
            assert!(std::str::from_utf8(&body)
                .unwrap()
                .contains("<base href=\"/t/demo/\">"));
        }
    }
}
