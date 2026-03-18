use crate::bridge::config::BridgeConfig;
use crate::bridge::BridgeLocation;
use camino::Utf8PathBuf;
use std::borrow::Cow;
use tedge_api::mqtt_topics::MqttSchema;
use tedge_config::models::HostPort;
use tedge_config::models::TopicPrefix;
use tedge_config::tedge_toml::ProfileName;
use tedge_config::TEdgeConfig;

const MQTT_TLS_PORT: u16 = 8883;

#[derive(Debug)]
pub struct BridgeConfigTbParams {
    pub mqtt_host: HostPort<MQTT_TLS_PORT>,
    pub config_file: Cow<'static, str>,
    pub bridge_root_cert_path: Utf8PathBuf,
    pub remote_clientid: String,
    pub bridge_certfile: Utf8PathBuf,
    pub bridge_keyfile: Utf8PathBuf,
    pub bridge_location: BridgeLocation,
    pub topic_prefix: TopicPrefix,
    pub profile_name: Option<ProfileName>,
    pub mqtt_schema: MqttSchema,
    pub keepalive_interval: std::time::Duration,
    pub proxy: Option<rumqttc::Proxy>,
}

pub(crate) async fn check_device_status_tb(
    tedge_config: &TEdgeConfig,
    profile: Option<&ProfileName>,
) -> Result<DeviceStatus, Fancy<ConnectError>> {
    let tb_config = tedge_config.mapper_config::<TbMapperSpecificConfig>(&profile)?;

    let topic = bridge_health_topic(&tb_config.bridge.topic_prefix, tedge_config);
    let health_topic = topic.name.clone();

    let mqtt_config = tedge_config
        .mqtt_config()?
        .with_no_session()
        .with_subscriptions(topic.into());

    let client = mqtt_channel::Connection::new(&mqtt_config)
        .await
        .map_err(|err| Fancy::from(ConnectError::ConnectionCheckError(err.to_string())))?;

    let mut received = client.received;
    tokio::time::timeout(RESPONSE_TIMEOUT, async {
        while let Some(msg) = received.next().await {
            if is_bridge_health_up_message(
                &rumqttc::Publish::new(&msg.topic.name, rumqttc::QoS::AtLeastOnce, &msg.payload),
                &health_topic,
                tedge_config.mqtt.bridge.built_in,
            ) {
                return Ok(DeviceStatus::AlreadyExists);
            }
        }
        Ok(DeviceStatus::Unknown)
    })
    .await
    .unwrap_or(Ok(DeviceStatus::Unknown))
}

impl From<BridgeConfigTbParams> for BridgeConfig {
    fn from(params: BridgeConfigTbParams) -> Self {
        let BridgeConfigTbParams {
            mqtt_host,
            config_file,
            bridge_root_cert_path,
            remote_clientid,
            bridge_certfile,
            bridge_keyfile,
            bridge_location,
            topic_prefix,
            profile_name,
            mqtt_schema,
            keepalive_interval,
            proxy,
        } = params;

        let address = mqtt_host.clone();

        // ThingsBoard MQTT topics
        // Telemetry: v1/devices/me/telemetry
        // Attributes: v1/devices/me/attributes
        // RPC: v1/devices/me/rpc/request/+, v1/devices/me/rpc/response/+
        let mut topics: Vec<String> = vec![
            // Publish telemetry
            format!("v1/devices/me/telemetry out 1 {topic_prefix}/ \"\""),
            // Publish attributes
            format!("v1/devices/me/attributes out 1 {topic_prefix}/ \"\""),
            // Subscribe to attribute updates
            format!("v1/devices/me/attributes in 1 {topic_prefix}/ \"\""),
            // Subscribe to RPC requests
            format!("v1/devices/me/rpc/request/+ in 1 {topic_prefix}/ \"\""),
            // Publish RPC responses
            format!("v1/devices/me/rpc/response/+ out 1 {topic_prefix}/ \"\""),
        ];

        let health_topic = mqtt_schema.topic_for(
            &tedge_api::EntityTopicId::default_main_device(),
            &tedge_api::Channel::Health,
        );

        BridgeConfig {
            cloud_name: "ThingsBoard".into(),
            config_file,
            connection: format!(
                "edge_to_tb{}",
                profile_name
                    .as_ref()
                    .map(|p| format!("@{p}"))
                    .unwrap_or_default()
            ),
            address,
            remote_username: None,
            remote_password: None,
            bridge_root_cert_path,
            remote_clientid,
            local_clientid: format!(
                "ThingsBoard{}",
                profile_name
                    .as_ref()
                    .map(|p| format!("@{p}"))
                    .unwrap_or_default()
            ),
            bridge_certfile,
            bridge_keyfile,
            use_mapper: true,
            use_agent: false,
            try_private: false,
            start_type: "automatic".into(),
            clean_session: true,
            include_local_clean_session: false,
            local_clean_session: false,
            notifications: false,
            notifications_local_only: false,
            notification_topic: "".into(),
            bridge_attempt_unsubscribe: false,
            topics,
            bridge_location,
            connection_check_attempts: 2,
            auth_type: tedge_config::models::auth_method::AuthType::Certificate,
            mosquitto_version: None,
            keepalive_interval,
            proxy,
            health_topic: health_topic.name,
        }
    }
}
