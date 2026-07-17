use bytes::Bytes;
use expose_common::protocol::Message;
use expose_common::types::TunnelProtocol;
use expose_common::utils;
use expose_common::{ConnectRequest, ConnectResponse, RequestLimits};
use uuid::Uuid;

#[test]
fn message_roundtrip_all_variants() {
    let id = Uuid::new_v4();
    let messages = vec![
        Message::Connect(ConnectRequest::new(None, None, TunnelProtocol::Http, "1.0")),
        Message::ConnectAck(ConnectResponse::build(
            id,
            "alpha".into(),
            "test.local".into(),
            TunnelProtocol::Http,
            true,
            Some(443),
            RequestLimits::default(),
        )),
        Message::HttpRequest {
            id,
            method: "GET".into(),
            path: "/".into(),
            headers: vec![("host".into(), "example".into())],
            body: Bytes::from_static(b"ping"),
        },
        Message::HttpResponse {
            id,
            status: 200,
            headers: vec![("content-type".into(), "text/plain".into())],
            body: Bytes::from_static(b"pong"),
        },
        Message::Disconnect { reason: None },
        Message::Error {
            code: expose_common::ErrorCode::InternalError,
            message: "boom".into(),
        },
    ];

    for message in messages {
        let buf = message.encode().expect("encode");
        let decoded = Message::decode(&buf).expect("decode");
        assert_eq!(
            std::mem::discriminant(&message),
            std::mem::discriminant(&decoded)
        );
        assert_eq!(message.encode().unwrap(), decoded.encode().unwrap());
    }
}

#[test]
fn large_payload_is_supported() {
    let id = Uuid::new_v4();
    let payload = vec![1u8; 10 * 1024 * 1024];
    let message = Message::HttpResponse {
        id,
        status: 200,
        headers: vec![],
        body: Bytes::from(payload.clone()),
    };

    let buf = message.encode().expect("encode large payload");
    let decoded = Message::decode(&buf).expect("decode large payload");
    if let Message::HttpResponse { body, .. } = decoded {
        assert_eq!(body.len(), payload.len());
    } else {
        panic!("unexpected variant");
    }
}

#[test]
fn invalid_message_returns_error() {
    let bytes = vec![0xFE, 0xED, 0xBE, 0xEF];
    assert!(Message::decode(&bytes).is_err());
}

#[test]
fn sanitize_subdomain_cases() {
    assert_eq!(utils::sanitize_subdomain(" My-App "), Some("my-app".into()));
    assert!(utils::sanitize_subdomain("--bad--").is_none());
    assert!(utils::sanitize_subdomain(&"a".repeat(80)).is_none());
    assert_eq!(utils::sanitize_subdomain("Ok123"), Some("ok123".into()));
}
