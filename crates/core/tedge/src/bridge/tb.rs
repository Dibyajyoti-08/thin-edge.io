use super::config::BridgeLocation;
use super::config::ProxyWrapper;
use super::BridgeConfig;
use camino::Utf8PathBuf;
use std::borrow::Cow;
use std::time::Duration;
use tedge_api::mqtt_topics::Channel;
use tedge_api::mqtt_topics::EntityTopicId;
use tedge_api::mqtt_topics::MqttSchema;
use tedge_config::models::auth_method::AuthType;
use tedge_config::models::HostPort;
use tedge_config::models::TopicPrefix;
use tedge_config::models::MQTT_TLS_PORT;
use tedge_config::tedge_toml::ProfileName;

#[derive(Debug)]
pub struct BridgeConfigTbParams {
    pub mqtt_host: HostPort<MQTT_TLS_PORT>,
    pub config_file: Cow<'static, str>,
    pub remote_clientid: String,
    pub bridge_root_cert_path: Utf8PathBuf,
    pub bridge_certfile: Utf8PathBuf,
    pub bridge_keyfile: Utf8PathBuf,
    pub bridge_location: BridgeLocation,
    pub topic_prefix: TopicPrefix,
    pub profile_name: Option<ProfileName>,
    pub mqtt_schema: MqttSchema,
    pub keepalive_interval: Duration,
    pub proxy: Option<rumqttc::Proxy>,
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

        let user_name = remote_clientid.to_string();

        // telemtry/command topics for use by the user
        // let pub_msg_topic = format!("td/# out 1 {topic_prefix}/ thinedge/{remote_clientid}/");
        // let sub_msg_topic = format!("cmd/# in 1 {topic_prefix}/ thinedge/{remote_clientid}/");
        let telemetry_out = format!("telemetry out 1 {topic_prefix}/ v1/devices/me/");
        let attributes_out = format!("attributes out 1 {topic_prefix}/ v1/devices/me/");
        let attributes_in = format!("attributes in 1 {topic_prefix}/ v1/devices/me/");
        let rpc_request_in = format!("rpc/request/+ in 1 {topic_prefix}/ v1/devices/me/");
        let rpc_response_out = format!("rpc/response/+ out 1 {topic_prefix}/ v1/devices/me/");

        let service_name = format!("mosquitto-{topic_prefix}-bridge");
        let health = mqtt_schema.topic_for(
            &EntityTopicId::default_main_service(&service_name).unwrap(),
            &Channel::Health,
        );
        Self {
            cloud_name: "tb".into(),
            config_file,
            connection: if let Some(profile) = &profile_name {
                format!("edge_to_tb@{profile}")
            } else {
                "edge_to_tb".into()
            },
            address: mqtt_host,
            remote_username: Some(user_name),
            remote_password: None,
            bridge_root_cert_path,
            remote_clientid,
            local_clientid: if let Some(profile) = &profile_name {
                format!("edge_to_tb@{profile}")
            } else {
                "edge_to_tb".into()
            },
            bridge_certfile,
            bridge_keyfile,
            use_mapper: true,
            use_agent: false,
            try_private: false,
            start_type: "automatic".into(),
            clean_session: false,
            include_local_clean_session: false,
            local_clean_session: false,
            notifications: true,
            notifications_local_only: true,
            notification_topic: health.name,
            bridge_attempt_unsubscribe: false,
            topics: vec![
                telemetry_out,
                attributes_out,
                attributes_in,
                rpc_request_in,
                rpc_response_out,
            ],
            bridge_location,
            connection_check_attempts: 2,
            auth_type: AuthType::Certificate,
            mosquitto_version: None,
            keepalive_interval,
            proxy: proxy.map(ProxyWrapper),
        }
    }
}

#[test]
fn test_bridge_config_from_tb_params() -> anyhow::Result<()> {
    let params = BridgeConfigTbParams {
        mqtt_host: HostPort::<MQTT_TLS_PORT>::try_from("tb.example.com")?,
        config_file: "tb-bridge.conf".into(),
        remote_clientid: "alpha".into(),
        bridge_root_cert_path: "./test_root.pem".into(),
        bridge_certfile: "./test-certificate.pem".into(),
        bridge_keyfile: "./test-private-key.pem".into(),
        bridge_location: BridgeLocation::Mosquitto,
        topic_prefix: "tb".try_into().unwrap(),
        profile_name: Some("profile".parse().unwrap()),
        mqtt_schema: MqttSchema::with_root("te".into()),
        keepalive_interval: Duration::from_secs(60),
        proxy: None,
    };
    let bridge = BridgeConfig::from(params);

    let expected = BridgeConfig {
        cloud_name: "tb".into(),
        config_file: "tb-bridge.conf".into(),
        connection: "edge_to_tb@profile".into(),
        address: HostPort::<MQTT_TLS_PORT>::try_from("tb.example.com")?,
        remote_username: Some("alpha".into()),
        remote_password: None,
        bridge_root_cert_path: Utf8PathBuf::from("./test_root.pem"),
        remote_clientid: "alpha".into(),
        local_clientid: "edge_to_tb@profile".into(),
        bridge_certfile: "./test-certificate.pem".into(),
        bridge_keyfile: "./test-private-key.pem".into(),
        use_mapper: true,
        use_agent: false,
        try_private: false,
        start_type: "automatic".into(),
        clean_session: false,
        include_local_clean_session: false,
        local_clean_session: false,
        notifications: true,
        notifications_local_only: true,
        notification_topic: "te/device/main/service/mosquitto-tb-bridge/status/health".into(),
        bridge_attempt_unsubscribe: false,
        topics: vec![
            "telemetry out 1 tb/ v1/devices/me/".into(),
            "attributes out 1 tb/ v1/devices/me/".into(),
            "attributes in 1 tb/ v1/devices/me/".into(),
            "rpc/request/+ in 1 tb/ v1/devices/me/".into(),
            "rpc/response/+ out 1 tb/ v1/devices/me/".into(),
        ],
        bridge_location: BridgeLocation::Mosquitto,
        connection_check_attempts: 2,
        auth_type: AuthType::Certificate,
        mosquitto_version: None,
        keepalive_interval: Duration::from_secs(60),
        proxy: None,
    };

    assert_eq!(bridge, expected);

    Ok(())
}

#[test]
fn test_bridge_config_tb_custom_topic_prefix() -> anyhow::Result<()> {
    let params = BridgeConfigTbParams {
        mqtt_host: HostPort::<MQTT_TLS_PORT>::try_from("tb.example.com")?,
        config_file: "tb-bridge.conf".into(),
        remote_clientid: "alpha".into(),
        bridge_root_cert_path: "./test_root.pem".into(),
        bridge_certfile: "./test-certificate.pem".into(),
        bridge_keyfile: "./test-private-key.pem".into(),
        bridge_location: BridgeLocation::Mosquitto,
        topic_prefix: "tb-custom".try_into().unwrap(),
        profile_name: Some("profile".parse().unwrap()),
        mqtt_schema: MqttSchema::with_root("te".into()),
        keepalive_interval: Duration::from_secs(60),
        proxy: None,
    };
    let bridge = BridgeConfig::from(params);

    let expected = BridgeConfig {
        cloud_name: "tb".into(),
        config_file: "tb-bridge.conf".into(),
        connection: "edge_to_tb@profile".into(),
        address: HostPort::<MQTT_TLS_PORT>::try_from("tb.example.com")?,
        remote_username: Some("alpha".into()),
        remote_password: None,
        bridge_root_cert_path: Utf8PathBuf::from("./test_root.pem"),
        remote_clientid: "alpha".into(),
        local_clientid: "edge_to_tb@profile".into(),
        bridge_certfile: "./test-certificate.pem".into(),
        bridge_keyfile: "./test-private-key.pem".into(),
        use_mapper: true,
        use_agent: false,
        topics: vec![
            "telemetry out 1 tb-custom/ v1/devices/me/".into(),
            "attributes out 1 tb-custom/ v1/devices/me/".into(),
            "attributes in 1 tb-custom/ v1/devices/me/".into(),
            "rpc/request/+ in 1 tb-custom/ v1/devices/me/".into(),
            "rpc/response/+ out 1 tb-custom/ v1/devices/me/".into(),
        ],
        try_private: false,
        start_type: "automatic".into(),
        clean_session: false,
        include_local_clean_session: false,
        local_clean_session: false,
        notifications: true,
        notifications_local_only: true,
        notification_topic: "te/device/main/service/mosquitto-tb-custom-bridge/status/health"
            .into(),
        bridge_attempt_unsubscribe: false,
        bridge_location: BridgeLocation::Mosquitto,
        connection_check_attempts: 2,
        auth_type: AuthType::Certificate,
        mosquitto_version: None,
        keepalive_interval: Duration::from_secs(60),
        proxy: None,
    };

    assert_eq!(bridge, expected);

    Ok(())
}
