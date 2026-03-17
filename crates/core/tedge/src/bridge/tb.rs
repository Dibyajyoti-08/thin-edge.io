use super::BridgeConfig;

/// ThingsBoard MQTT bridge configuration.
///
/// ThingsBoard uses standard MQTT topics:
/// - `v1/devices/me/telemetry` for telemetry data
/// - `v1/devices/me/attributes` for device attributes
/// - `v1/gateway/telemetry` for gateway (child device) telemetry
/// - `v1/gateway/attributes` for gateway (child device) attributes
pub fn tb_bridge_config(
    host: &str,
    port: u16,
    device_id: &str,
) -> BridgeConfig {
    BridgeConfig {
        cloud_name: "tb".to_string(),
        address: format!("{}:{}", host, port),
        remote_clientid: device_id.to_string(),
        bridge_root_cert_path: None,
        bridge_certfile: None,
        bridge_keyfile: None,
        topics: vec![
            // Outgoing: telemetry
            r#"v1/devices/me/telemetry out 1 "" """#.to_string(),
            // Outgoing: attributes
            r#"v1/devices/me/attributes out 1 "" """#.to_string(),
            // Outgoing: gateway telemetry
            r#"v1/gateway/telemetry out 1 "" """#.to_string(),
            // Outgoing: gateway attributes
            r#"v1/gateway/attributes out 1 "" """#.to_string(),
            // Incoming: RPC requests
            r#"v1/devices/me/rpc/request/+ in 1 "" """#.to_string(),
            // Incoming: attribute updates
            r#"v1/devices/me/attributes/response/+ in 1 "" """#.to_string(),
        ],
    }
}