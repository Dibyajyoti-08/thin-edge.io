use crate::core::component::TEdgeComponent;
use crate::core::mapper::start_basic_actors;
use crate::flows_config;
use async_trait::async_trait;
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
use tracing::warn;

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

        if tedge_config.mqtt.bridge.built_in {
            let rule = built_in_bridge_rules(prefix)?;
            let cloud_config = tb_config.cloud_client_config(&tedge_config)?;
            let mqtt_schema = MqttSchema::with_root(tedge_config.mqtt.topic_root.clone());
            let health_topic = tedge_api::service_health_topic(
                &mqtt_schema,
                &tedge_config.mqtt.device_topic_id,
            );
            let cloud_options = cloud_config.rumqttc_options()?;
            match MqttBridgeActorBuilder::new(
                &tedge_config,
                &tb_mapper_name,
                &health_topic,
                rule,
                cloud_options,
            )
            .await
            {
                Ok(bridge_actor) => {
                    runtime.spawn(bridge_actor).await?;
                }
                Err(err) => {
                    warn!("Could not start built-in bridge for ThingsBoard: {err}");
                }
            }
        }

        let tb_converter = TbConverter::from_config(&tb_config, &tedge_config)?;
        let flows_dir = tedge_flows::flows_dir(
            config_dir,
            "tb",
            self.profile.as_ref().map(|p| p.as_ref()),
        );
        let flows = tb_converter.flow_registry(flows_dir).await?;
        let service_config = flows_config(&tedge_config, &tb_mapper_name)?;

        let mut fs_actor = FsWatchActorBuilder::new();
        let mut cmd_watcher_actor = WatchActorBuilder::new();
        let mut flows_mapper = FlowsMapperBuilder::try_new(flows, service_config).await?;
        flows_mapper.connect(&mut mqtt_actor);
        flows_mapper.connect_fs(&mut fs_actor);
        flows_mapper.connect_cmd(&mut cmd_watcher_actor);

        runtime.spawn(flows_mapper).await?;
        runtime.spawn(mqtt_actor).await?;
        runtime.spawn(fs_actor).await?;
        runtime.spawn(cmd_watcher_actor).await?;
        runtime.run_to_completion().await?;
        Ok(())
    }
}

fn built_in_bridge_rules(
    topic_prefix: &TopicPrefix,
) -> Result<BridgeConfig, anyhow::Error> {
    let local_prefix = format!("{topic_prefix}/");
    let mut bridge = BridgeConfig::new();

    // Telemetry
    bridge.forward_from_local("v1/devices/me/telemetry", local_prefix.clone(), String::new())?;
    // Attributes publish
    bridge.forward_from_local("v1/devices/me/attributes", local_prefix.clone(), String::new())?;
    // Attributes subscribe
    bridge.forward_from_remote("v1/devices/me/attributes", local_prefix.clone(), String::new())?;
    // RPC request (from cloud)
    bridge.forward_from_remote("v1/devices/me/rpc/request/+", local_prefix.clone(), String::new())?;
    // RPC response (to cloud)
    bridge.forward_from_local("v1/devices/me/rpc/response/+", local_prefix.clone(), String::new())?;
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