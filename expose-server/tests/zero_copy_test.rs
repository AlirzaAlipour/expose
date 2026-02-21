use bytes::Bytes;
use expose_common::protocol::Message;

#[cfg(all(target_os = "linux", feature = "io_uring"))]
#[tokio::test]
async fn test_io_uring_available() {
    let caps = expose_server::platform::detect_capabilities();
    println!("io_uring available: {}", caps.io_uring_available);
}

#[test]
fn test_bytes_serialization_compat() {
    let msg = Message::HttpRequest {
        id: uuid::Uuid::new_v4(),
        method: "GET".into(),
        path: "/".into(),
        headers: vec![],
        body: Bytes::from_static(b"test body"),
    };

    let encoded = msg.encode().unwrap();
    let decoded = Message::decode(&encoded).unwrap();
    assert_eq!(msg, decoded);
}
