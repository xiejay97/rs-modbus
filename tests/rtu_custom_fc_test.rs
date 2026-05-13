//! RTU Custom Function Code framing tests (Item #10)

use rs_modbus::error::ModbusError;
use rs_modbus::layers::application::{
    ApplicationLayer, ApplicationRole, RtuApplicationLayer, RtuApplicationLayerOptions,
};
use rs_modbus::layers::physical::{
    ConnectionId, DataEvent, PhysicalLayer, PhysicalLayerType, ResponseFn,
};
use rs_modbus::types::{CustomFcPredict, CustomFunctionCode};
use rs_modbus::utils::crc;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;
use tokio::time::timeout;

struct FakeNetPhysicalLayer {
    data_tx: broadcast::Sender<DataEvent>,
}

impl FakeNetPhysicalLayer {
    fn new() -> Arc<Self> {
        let (data_tx, _) = broadcast::channel(16);
        Arc::new(Self { data_tx })
    }

    fn inject(&self, data: Vec<u8>, connection: ConnectionId) {
        let response: ResponseFn = Arc::new(|_| Box::pin(async { Ok(()) }));
        let _ = self.data_tx.send(DataEvent {
            data,
            response,
            connection,
        });
    }
}

#[async_trait::async_trait]
impl PhysicalLayer for FakeNetPhysicalLayer {
    fn layer_type(&self) -> PhysicalLayerType {
        PhysicalLayerType::Net
    }

    async fn open(&self) -> Result<(), ModbusError> {
        Ok(())
    }

    async fn write(&self, _data: &[u8]) -> Result<(), ModbusError> {
        Ok(())
    }

    async fn close(&self) -> Result<(), ModbusError> {
        Ok(())
    }

    async fn destroy(&self) {}

    fn is_open(&self) -> bool {
        true
    }

    fn is_destroyed(&self) -> bool {
        false
    }

    fn subscribe_data(&self) -> broadcast::Receiver<DataEvent> {
        self.data_tx.subscribe()
    }

    fn subscribe_write(&self) -> broadcast::Receiver<Vec<u8>> {
        let (_, rx) = broadcast::channel(1);
        rx
    }

    fn subscribe_error(&self) -> broadcast::Receiver<ModbusError> {
        let (_, rx) = broadcast::channel(1);
        rx
    }

    fn subscribe_connection_close(&self) -> broadcast::Receiver<ConnectionId> {
        let (_, rx) = broadcast::channel(1);
        rx
    }

    fn subscribe_close(&self) -> broadcast::Receiver<()> {
        let (_, rx) = broadcast::channel(1);
        rx
    }
}

fn rtu_frame(unit: u8, fc: u8, payload: &[u8]) -> Vec<u8> {
    let mut buf = vec![unit, fc];
    buf.extend_from_slice(payload);
    let c = crc(&buf);
    buf.extend_from_slice(&c.to_le_bytes());
    buf
}

#[tokio::test]
async fn custom_fc_fixed_length_predictor() {
    let physical = FakeNetPhysicalLayer::new();
    let app = RtuApplicationLayer::new(physical.clone(), RtuApplicationLayerOptions::default());
    app.set_role(ApplicationRole::Slave).unwrap();

    let cfc = CustomFunctionCode {
        fc: 0x65,
        predict_request_length: Arc::new(|_| CustomFcPredict::Length(8)),
        predict_response_length: Arc::new(|_| CustomFcPredict::Length(8)),
        handle: None,
    };
    app.add_custom_function_code(cfc);

    let mut framing_rx = app.subscribe_framing();
    let mut error_rx = app.subscribe_framing_error();

    let frame = rtu_frame(1, 0x65, &[0x11, 0x22, 0x33, 0x44]);
    physical.inject(frame, Arc::from("fake-conn"));

    let f = timeout(Duration::from_millis(100), framing_rx.recv())
        .await
        .expect("framing within 100ms")
        .expect("channel open");
    assert_eq!(f.adu.fc, 0x65);
    assert_eq!(f.adu.data, vec![0x11, 0x22, 0x33, 0x44]);

    assert!(
        timeout(Duration::from_millis(50), error_rx.recv())
            .await
            .is_err(),
        "no error expected"
    );

    app.destroy().await;
}

#[tokio::test]
async fn custom_fc_variable_length_predictor() {
    let physical = FakeNetPhysicalLayer::new();
    let app = RtuApplicationLayer::new(physical.clone(), RtuApplicationLayerOptions::default());
    app.set_role(ApplicationRole::Slave).unwrap();

    let cfc = CustomFunctionCode {
        fc: 0x66,
        predict_request_length: Arc::new(|buf| {
            if buf.len() < 3 {
                CustomFcPredict::NeedMore
            } else {
                CustomFcPredict::Length(5 + buf[2] as usize)
            }
        }),
        predict_response_length: Arc::new(|_| CustomFcPredict::NeedMore),
        handle: None,
    };
    app.add_custom_function_code(cfc);

    let mut framing_rx = app.subscribe_framing();

    let frame = rtu_frame(1, 0x66, &[0x03, 0xaa, 0xbb, 0xcc]);
    physical.inject(frame, Arc::from("fake-conn"));

    let f = timeout(Duration::from_millis(100), framing_rx.recv())
        .await
        .expect("framing within 100ms")
        .expect("channel open");
    assert_eq!(f.adu.fc, 0x66);
    assert_eq!(f.adu.data, vec![0x03, 0xaa, 0xbb, 0xcc]);

    app.destroy().await;
}
