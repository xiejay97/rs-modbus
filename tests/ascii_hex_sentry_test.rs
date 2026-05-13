//! ASCII HEX_DECODE sentry — defensive decoding (035dbd1)
//!
//! Even though the FSM already rejects non-hex characters at reception time,
//! `decode_payload` has a second line of defense (`hex_decode_byte` returning
//! `None` -> `InvalidHex`). This test verifies that defense-in-depth path.

use rs_modbus::error::ModbusError;
use rs_modbus::layers::application::{
    ApplicationLayer, ApplicationRole, AsciiApplicationLayer, AsciiApplicationLayerOptions,
};
use rs_modbus::layers::physical::{PhysicalLayer, TcpClientPhysicalLayer, TcpServerPhysicalLayer};
use std::time::Duration;
use tokio::time::{sleep, timeout};

#[tokio::test]
async fn decode_payload_rejects_non_hex_characters_with_invalid_hex() {
    let server = TcpServerPhysicalLayer::new();
    server.set_addr("127.0.0.1:0".to_string()).await;
    server.open().await.unwrap();
    let addr = server.get_addr().await.unwrap();

    let app = AsciiApplicationLayer::with_options(
        server.clone(),
        AsciiApplicationLayerOptions { lenient_hex: false },
    );
    app.set_role(ApplicationRole::Slave).unwrap();
    let mut err_rx = app.subscribe_framing_error();
    sleep(Duration::from_millis(30)).await;

    let client = TcpClientPhysicalLayer::new();
    client.set_addr(addr).await;
    client.open().await.unwrap();
    sleep(Duration::from_millis(30)).await;

    // Build an ASCII frame where one hex pair contains 'G' (0x47).
    // :01G300AA\r\n — 'G' in the third position is not valid hex.
    let bad = b":01G300AA\r\n".to_vec();
    client.write(&bad).await.unwrap();

    let err = timeout(Duration::from_secs(2), err_rx.recv())
        .await
        .expect("error within 2s")
        .expect("error channel open");

    assert!(
        matches!(err, ModbusError::InvalidHex),
        "expected InvalidHex, got {:?}",
        err
    );

    client.destroy().await;
    server.destroy().await;
    app.destroy().await;
}
