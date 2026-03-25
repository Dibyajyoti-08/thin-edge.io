use crate::core::component::TEdgeComponent;
use crate::core::mapper::start_basic_actors;
use crate::flows_config;
use async_trait::async_trait;
use mqtt_channel::Topic;
use tb_mapper_ext::TbConverter;
use tedge_api::mqtt_topics::MqttSchema;
use tedge_config::models::TopicPrefix;
use tedge_config::tedge_toml::mapper_config::TbMapperSpecificConfig;
use tedge_config::tedge_toml::ProfileName;
use tedge_config::TEdgeConfig;
use tedge_file_system_ext::FsWatchActorBuilder;
use tedge_flows::FlowsMapperBuilder;
use tedge_mqtt_bridge::BridgeConfig;
use tedge_mqtt_bridge::MqttBridgeActorBuilder;
use tedge_watch_ext::WatchActorBuilder;

pub struct TbMapper {
    pub profile: Option<ProfileName>,
}

#[async_trait]
impl TEdgeComponent for TbMapper {
    async fn start(
        &self,
        tedge_config: TEdgeConfig,
        config_dir: &tedge_config::Path,
    ) -> Result<(), anyhow::Error> {
        let tb_config = tedge_config.mapper_config::<TbMapperSpecificConfig>(&self.profile)?;
        let prefix = &tb_config.bridge.topic_prefix;
        let tb_mapper_name = format!("tedge-mapper-{prefix}");
        let (mut runtime, mut mqtt_actor) =
            start_basic_actors(&tb_mapper_name, &tedge_config).await?;
        let mqtt_schema = MqttSchema::with_root(tedge_config.mqtt.topic_root.clone());

        if tedge_config.mqtt.bridge.built_in {
            let device_id: String = tb_config.device.id()?;
            let device_topic_id = tedge_config.mqtt.device_topic_id.clone();
            let rules = built_in_bridge_rules(prefix)?;
            let mut cloud_config = tedge_mqtt_bridge::MqttOptions::new(
                device_id,
                tb_config.url().or_config_not_set()?.to_string(),
                8883,
            );
            cloud_config.set_clean_session(false);
            cloud_config.set_keep_alive(tb_config.bridge.keepalive_interval.duration());
            let bridge_name = format!("tb-bridge-{prefix}");
            let health_topic = tedge_api::health::service_health_topic(
                &mqtt_schema,
                &device_topic_id,
                &bridge_name,
            );
            let bridge_actor = MqttBridgeActorBuilder::new(
                &tedge_config,
                &bridge_name,
                &health_topic,
                rules,
                cloud_config,
                None,
            )
            .await;
            runtime.spawn(bridge_actor).await?;
        }

        let mqtt_schema = MqttSchema::with_root(tedge_config.mqtt.topic_root.clone());
        // let errors_topic = Topic::new_unchecked(&format!("te/errors/{tb_mapper_name}"));
        let input_topics = {
            let configured = tb_config.topics.to_string();
            if configured.is_empty() || configured == "[]" {
                r#"["te/+/+/+/+/m/+", "te/+/+/+/+/e/+", "te/+/+/+/+/a/+", "te/+/+/+/+/twin/+", "te/+/+/+/+/status/health"]"#.to_string()
            } else {
                configured
            }
        };
        let tb_converter = TbConverter::new(
            tb_config.cloud_specific.mapper.timestamp,
            &mqtt_schema,
            tb_config.cloud_specific.mapper.timestamp_format,
            prefix.value().clone(),
            tb_config.mapper.mqtt.max_payload_size.0,
            // tb_config.topics.to_string(),
            input_topics,
        );

        let flow_dir =
            tedge_flows::flows_dir(config_dir, "tb", self.profile.as_ref().map(|p| p.as_ref()));
        let flows = tb_converter.flow_registry(flow_dir).await?;
        let service_config = flows_config(&tedge_config, &tb_mapper_name)?;
        let mut fs_actor = FsWatchActorBuilder::new();
        let mut cmd_watcher_actor = WatchActorBuilder::new();
        let mut flows_mapper =
            tedge_flows::FlowsMapperBuilder::try_new(flows, service_config).await?;
        flows_mapper.connect(&mut mqtt_actor);
        flows_mapper.connect_fs(&mut fs_actor);
        flows_mapper.connect_cmd(&mut cmd_watcher_actor);

        runtime.spawn(flows_mapper).await?;
        runtime.spawn(fs_actor).await?;
        runtime.spawn(cmd_watcher_actor).await?;
        runtime.spawn(mqtt_actor).await?;
        runtime.run_to_completion().await?;
        Ok(())
    }
}

fn built_in_bridge_rules(topic_prefix: &TopicPrefix) -> Result<BridgeConfig, anyhow::Error> {
    let local_prefix = format!("{topic_prefix}/");
    let mut bridge = BridgeConfig::new();

    // Telemetry
    bridge.forward_from_local(
        "v1/devices/me/telemetry",
        local_prefix.clone(),
        String::new(),
    )?;
    // Attributes publish
    bridge.forward_from_local(
        "v1/devices/me/attributes",
        local_prefix.clone(),
        String::new(),
    )?;
    // Attributes subscribe
    bridge.forward_from_remote(
        "v1/devices/me/attributes",
        local_prefix.clone(),
        String::new(),
    )?;
    // RPC request (from cloud)
    bridge.forward_from_remote(
        "v1/devices/me/rpc/request/+",
        local_prefix.clone(),
        String::new(),
    )?;
    // RPC response (to cloud)
    bridge.forward_from_local(
        "v1/devices/me/rpc/response/+",
        local_prefix.clone(),
        String::new(),
    )?;
    // Gateway telemetry
    bridge.forward_from_local("v1/gateway/telemetry", local_prefix.clone(), String::new())?;
    // Gateway attributes
    bridge.forward_from_local("v1/gateway/attributes", local_prefix.clone(), String::new())?;
    // Gateway connect/disconnect
    bridge.forward_from_local("v1/gateway/connect", local_prefix.clone(), String::new())?;
    bridge.forward_from_local("v1/gateway/disconnect", local_prefix.clone(), String::new())?;

    Ok(bridge)
}

#[test]
fn bridge_rules_are_valid() {
    built_in_bridge_rules(&"tb".try_into().unwrap()).unwrap();
}
