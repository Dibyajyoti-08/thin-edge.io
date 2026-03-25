use super::command::bridge_health_topic;
use super::command::is_bridge_health_up_message;
use crate::cli::RESPONSE_TIMEOUT;
use crate::ConnectError;
use crate::DeviceStatus;
use anyhow::anyhow;
use certificate::parse_root_certificate::create_tls_config_without_client_cert;
use rumqttc::Event;
use rumqttc::Incoming;
use rumqttc::MqttOptions;
use rumqttc::Outgoing;
use rumqttc::Packet;
use rumqttc::QoS::AtLeastOnce;
use rumqttc::TlsConfiguration;
use rumqttc::Transport;
use std::sync::Arc;
use tedge_config::tedge_toml::mapper_config::TbMapperSpecificConfig;
use tedge_config::tedge_toml::ProfileName;
use tedge_config::TEdgeConfig;

pub async fn check_device_status_tb(
    tedge_config: &TEdgeConfig,
    profile: Option<&ProfileName>,
) -> Result<DeviceStatus, ConnectError> {
    let tb_config = tedge_config.mapper_config::<TbMapperSpecificConfig>(&profile)?;
    let topic_prefix = &tb_config.bridge.topic_prefix;
    let built_in_bridge_health = bridge_health_topic(topic_prefix, tedge_config).name;
    const CLIENT_ID: &str = "check_connection_tb";

    let mut mqtt_options = tedge_config
        .mqtt_config()?
        .with_session_prefix(CLIENT_ID)
        .rumqttc_options()?;
    mqtt_options.set_keep_alive(RESPONSE_TIMEOUT);

    let (client, mut event_loop) = rumqttc::AsyncClient::new(mqtt_options, 10);

    // Only subscribe to bridge health topic.
    // Mosquitto publishes {"status":"up"} here automatically when
    // the bridge connection to ThingsBoard is established.
    // No need to publish/subscribe to tb/test-connection or tb/connection-success.
    client
        .subscribe(&built_in_bridge_health, AtLeastOnce)
        .await?;

    let mut err = None;
    loop {
        match event_loop.poll().await {
            Ok(Event::Incoming(Packet::Publish(response))) => {
                eprintln!(
                    "Received on [{}]: {}",
                    response.topic,
                    String::from_utf8_lossy(&response.payload)
                );
                // is_bridge_health_up_message checks if payload contains "up"
                if is_bridge_health_up_message(
                    &response,
                    &built_in_bridge_health,
                    tedge_config.mqtt.bridge.built_in,
                ) {
                    eprintln!("Bridge is UP - ThingsBoard connected!");
                    break; // success
                }
            }
            Ok(Event::Outgoing(Outgoing::PingReq)) => {
                // Timeout — no bridge health message received
                err = Some(anyhow!("Didn't receive a response from ThingsBoard"));
                break;
            }
            Ok(Event::Incoming(Incoming::Disconnect)) => {
                err = Some(anyhow!(
                    "Client was disconnected from mosquitto during connection check"
                ));
                break;
            }
            Err(e) => {
                err = Some(
                    anyhow::Error::from(e)
                        .context("Failed to connect to mosquitto for connection check"),
                );
                break;
            }
            _ => {}
        }
    }

    // Cleanly disconnect client
    client.disconnect().await?;
    loop {
        match event_loop.poll().await {
            Ok(Event::Outgoing(Outgoing::Disconnect)) | Err(_) => break,
            _ => {}
        }
    }

    match err {
        None => Ok(DeviceStatus::AlreadyExists),
        Some(err) => Err(err
            .context("Failed to verify device is connected to ThingsBoard")
            .into()),
    }
}

/// Provision a device with ThingsBoard using X.509 certificate-based provisioning.
///
/// Reads `tb.device.provision_key`, `tb.device.provision_secret`, `tb.device.id`,
/// and `tb.device.cert_path` from the thin-edge config, then connects directly to
/// ThingsBoard's provisioning MQTT endpoint (port 8883) and exchanges credentials.
///
/// Returns `Ok(())` immediately if `tb.device.provision_key` is not set (provisioning
/// is skipped and the normal connect flow proceeds).
pub async fn provision_device_tb(
    tedge_config: &TEdgeConfig,
    profile: Option<&ProfileName>,
) -> anyhow::Result<()> {
    let tb_config = tedge_config.mapper_config::<TbMapperSpecificConfig>(&profile)?;

    // Skip silently if no provision_key is configured
    let provision_key = match &tb_config.cloud_specific.provision_key {
        Some(k) => k.clone(),
        None => return Ok(()),
    };

    let provision_secret = tb_config
        .cloud_specific
        .provision_secret
        .clone()
        .ok_or_else(|| {
            anyhow!("tb.device.provision_secret is not set. Run: tedge config set tb.device.provision_secret <secret>")
        })?;

    let device_name = tb_config.device.id()?;

    // Read device certificate (already created by `tedge cert create --cloud tb`)
    let cert_path = tb_config.device.cert_path.as_std_path();
    let cert_pem = std::fs::read_to_string(cert_path).map_err(|e| {
        anyhow!(
            "Failed to read device certificate at {}: {}",
            cert_path.display(),
            e
        )
    })?;

    // ThingsBoard expects the certificate without PEM headers/footers and newlines
    let cert_hash = cert_pem
        .replace("-----BEGIN CERTIFICATE-----\n", "")
        .replace("-----END CERTIFICATE-----\n", "")
        .replace('\n', "");

    let tb_host = tb_config
        .url()
        .or_config_not_set()
        .map_err(|_| anyhow!("tb.url is not configured. Run: tedge config set tb.url <host>"))?
        .as_str()
        .to_string();

    let payload = serde_json::json!({
        "provisionDeviceKey": provision_key,
        "provisionDeviceSecret": provision_secret,
        "credentialsType": "X509_CERTIFICATE",
        "deviceName": device_name,
        "hash": cert_hash,
    });

    let payload_bytes = serde_json::to_vec(&payload)
        .map_err(|e| anyhow!("Failed to serialize provisioning request: {}", e))?;

    // Connect directly to ThingsBoard (not via local bridge) using username "provision"
    const TB_PROVISION_PORT: u16 = 8883;
    let mut mqtt_options = MqttOptions::new("tedge_tb_provision", &tb_host, TB_PROVISION_PORT);
    mqtt_options.set_credentials("provision", "");
    mqtt_options.set_keep_alive(RESPONSE_TIMEOUT);

    // TLS: use the configured TB root certificate path (no client cert for provisioning)
    let tls_config = create_tls_config_without_client_cert(tb_config.root_cert_path.as_std_path())
        .map_err(|e| anyhow!("Failed to build TLS configuration for provisioning: {}", e))?;
    mqtt_options.set_transport(Transport::tls_with_config(
        TlsConfiguration::Rustls(Arc::new(tls_config)).into(),
    ));

    let (client, mut event_loop) = rumqttc::AsyncClient::new(mqtt_options, 10);

    const PROVISION_REQUEST_TOPIC: &str = "/provision/request";
    const PROVISION_RESPONSE_TOPIC: &str = "/provision/response";

    client
        .subscribe(PROVISION_RESPONSE_TOPIC, AtLeastOnce)
        .await?;

    eprintln!(
        "Connecting to ThingsBoard provisioning endpoint at {}:{}",
        tb_host, TB_PROVISION_PORT
    );

    let mut err: Option<anyhow::Error> = None;
    let mut provisioned = false;

    loop {
        match event_loop.poll().await {
            Ok(Event::Incoming(Packet::SubAck(_))) => {
                // Subscribed — now send the provisioning request
                client
                    .publish(
                        PROVISION_REQUEST_TOPIC,
                        AtLeastOnce,
                        false,
                        payload_bytes.clone(),
                    )
                    .await?;
                eprintln!("Provisioning request sent for device '{}'", device_name);
            }
            Ok(Event::Incoming(Packet::Publish(response))) => {
                if response.topic == PROVISION_RESPONSE_TOPIC {
                    match serde_json::from_slice::<serde_json::Value>(&response.payload) {
                        Ok(decoded) => {
                            let status = decoded["status"].as_str().unwrap_or("UNKNOWN");
                            if status == "SUCCESS" {
                                let returned_cert =
                                    decoded["credentialsValue"].as_str().unwrap_or("");
                                if returned_cert == cert_hash {
                                    eprintln!(
                                        "Device '{}' provisioned successfully in ThingsBoard.",
                                        device_name
                                    );
                                    provisioned = true;
                                } else {
                                    err = Some(anyhow!(
                                        "ThingsBoard returned a certificate that does not match the device certificate"
                                    ));
                                }
                            } else {
                                let msg = decoded["errorMsg"]
                                    .as_str()
                                    .unwrap_or("no error message returned");
                                err = Some(anyhow!(
                                    "ThingsBoard provisioning failed (status: '{}'): {}",
                                    status,
                                    msg
                                ));
                            }
                        }
                        Err(e) => {
                            err = Some(anyhow!(
                                "Failed to parse ThingsBoard provisioning response: {}",
                                e
                            ));
                        }
                    }
                    break;
                }
            }
            Ok(Event::Outgoing(Outgoing::PingReq)) => {
                err = Some(anyhow!(
                    "Timed out waiting for provisioning response from ThingsBoard"
                ));
                break;
            }
            Ok(Event::Incoming(Incoming::Disconnect)) => {
                err = Some(anyhow!("Disconnected from ThingsBoard during provisioning"));
                break;
            }
            Err(e) => {
                err = Some(
                    anyhow::Error::from(e)
                        .context("MQTT connection error during ThingsBoard provisioning"),
                );
                break;
            }
            _ => {}
        }
    }

    // Cleanly disconnect
    client.disconnect().await?;
    loop {
        match event_loop.poll().await {
            Ok(Event::Outgoing(Outgoing::Disconnect)) | Err(_) => break,
            _ => {}
        }
    }

    match (provisioned, err) {
        (true, _) => Ok(()),
        (false, Some(e)) => Err(e.context("ThingsBoard device provisioning failed")),
        (false, None) => Err(anyhow!(
            "Provisioning ended without a response from ThingsBoard"
        )),
    }
}
