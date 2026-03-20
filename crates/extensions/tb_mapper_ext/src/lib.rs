use camino::Utf8Path;
use std::time::SystemTime;
use tedge_api::mqtt_topics::MqttSchema;
use tedge_config::models::TopicPrefix;
use tedge_flows::ConfigError;
use tedge_flows::ConnectedFlowRegistry;
use tedge_flows::FlowContextHandle;
use tedge_flows::FlowError;
use tedge_flows::FlowRegistryExt;
use tedge_flows::JsonValue;
use tedge_flows::Message;
use tedge_flows::UpdateFlowRegistryError;
use tedge_mqtt_ext::Topic;
use tedge_utils::file::create_directory_with_defaults;
use tedge_utils::timestamp::TimeFormat;

/// Converter for ThingsBoard MQTT messages.
///
/// Transforms thin-edge.io internal MQTT messages into ThingsBoard's
/// expected format and topic structure.
pub struct TbConverter {
    input_topics: String,
    topic_prefix: TopicPrefix,
    errors_topic: Topic,
    size_threshold: usize,
    add_timestamp: bool,
    time_format: TimeFormat,
}

impl TbConverter {
    pub fn new(
        add_timestamp: bool,
        mqtt_schema: &MqttSchema,
        time_format: TimeFormat,
        topic_prefix: TopicPrefix,
        max_payload_size: u32,
        input_topics: String,
    ) -> Self {
        let errors_topic = mqtt_schema.error_topic();
        let size_threshold = max_payload_size as usize;
        TbConverter {
            input_topics,
            topic_prefix,
            errors_topic,
            size_threshold,
            add_timestamp,
            time_format,
        }
    }

    pub async fn flow_registry(
        &self,
        flows_dir: impl AsRef<Utf8Path>,
    ) -> Result<ConnectedFlowRegistry, UpdateFlowRegistryError> {
        create_directory_with_defaults(flows_dir.as_ref()).await?;
        let mut flows = ConnectedFlowRegistry::new(flows_dir);
        flows.register_builtin(SetTbTopic::default());
        self.persist_builtin_flow(&mut flows).await?;
        Ok(flows)
    }

    pub(crate) async fn persist_builtin_flow(
        &self,
        flows: &mut ConnectedFlowRegistry,
    ) -> Result<(), UpdateFlowRegistryError> {
        flows
            .persist_builtin_flow("tb_builtin", self.builtin_flow().as_str())
            .await
    }

    fn builtin_flow(&self) -> String {
        let timestamp_step = if self.add_timestamp {
            format!(
                r#"{{ builtin = "add-timestamp", config = {{ format = "{}" }} }},"#,
                self.time_format
            )
        } else {
            String::new()
        };

        format!(
            r#"
input.mqtt.topics = {input_topics}

steps = [
    {{ builtin = "skip-mosquitto-health-status" }},
    {timestamp_step}
    {{ builtin = "limit-payload-size", config = {{ max_size = {max_size} }} }},
    {{ builtin = "set-tb-topic", config = {{ prefix = "{topic_prefix}" }} }},
]

errors.mqtt.topic = "{errors_topic}"
"#,
            input_topics = self.input_topics,
            topic_prefix = self.topic_prefix,
            max_size = self.size_threshold,
            errors_topic = self.errors_topic,
        )
    }
}

/// ThingsBoard uses specific MQTT topics for telemetry and attributes:
///
/// - Telemetry: `v1/devices/me/telemetry`
/// - Attributes: `v1/devices/me/attributes`
/// - RPC: `v1/devices/me/rpc/request/{id}`
///
/// For gateway mode (multiple devices):
/// - Telemetry: `v1/gateway/telemetry`
/// - Attributes: `v1/gateway/attributes`
///
/// This transformer maps thin-edge.io topics to ThingsBoard topics.
fn map_to_tb_topic(source: &str) -> Option<String> {
    // Use topic segments to determine message type and device scope.
    // thin-edge.io topics follow: te/<device_type>/<device_id>/<service_type>/<service_id>/<channel>/<type>
    match source.split('/').collect::<Vec<_>>()[..] {
        // Main device measurements -> Telemetry
        [_, "device", "main", _, _, "m", _] => Some("v1/devices/me/telemetry".to_string()),
        // Child device measurements -> Gateway telemetry
        [_, "device", _, _, _, "m", _] => Some("v1/gateway/telemetry".to_string()),
        // Main device events -> Telemetry
        [_, "device", "main", _, _, "e", _] => Some("v1/devices/me/telemetry".to_string()),
        // Child device events -> Gateway telemetry
        [_, "device", _, _, _, "e", _] => Some("v1/gateway/telemetry".to_string()),
        // Main device alarms -> Telemetry
        [_, "device", "main", _, _, "a", _] => Some("v1/devices/me/telemetry".to_string()),
        // Child device alarms -> Gateway telemetry
        [_, "device", _, _, _, "a", _] => Some("v1/gateway/telemetry".to_string()),
        // Main device twin -> Attributes
        [_, "device", "main", _, _, "twin", _] => Some("v1/devices/me/attributes".to_string()),
        // Child device twin -> Gateway attributes
        [_, "device", _, _, _, "twin", _] => Some("v1/gateway/attributes".to_string()),
        // Main device health -> Telemetry
        [_, "device", "main", _, _, "status", "health"] => {
            Some("v1/devices/me/telemetry".to_string())
        }
        // Child device health -> Gateway telemetry
        [_, "device", _, _, _, "status", "health"] => Some("v1/gateway/telemetry".to_string()),
        _ => None,
    }
}

/// Extracts the device name from a thin-edge topic.
///
/// e.g. "te/device/child01///m/temperature" -> "child01"
fn extract_device_name(topic: &str) -> &str {
    topic.split('/').nth(2).unwrap_or("main")
}

#[derive(Clone, Default)]
pub struct SetTbTopic {
    prefix: String,
}

impl tedge_flows::Transformer for SetTbTopic {
    fn name(&self) -> &str {
        "set-tb-topic"
    }

    fn set_config(&mut self, config: JsonValue) -> Result<(), ConfigError> {
        let prefix = config.string_property("prefix").unwrap_or("tb");
        self.prefix = prefix.to_owned();
        Ok(())
    }

    fn on_message(
        &mut self,
        _timestamp: SystemTime,
        message: &Message,
        _context: &FlowContextHandle,
    ) -> Result<Vec<Message>, FlowError> {
        if let Some(tb_topic) = map_to_tb_topic(&message.topic) {
            Ok(vec![Message::new(tb_topic, message.payload.clone())])
        } else {
            // Unknown topic pattern — skip
            Ok(vec![])
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_main_device_measurement_topic() {
        let topic = map_to_tb_topic("te/device/main///m/temperature");
        assert_eq!(topic, Some("v1/devices/me/telemetry".to_string()));
    }

    #[test]
    fn test_child_device_measurement_topic() {
        let topic = map_to_tb_topic("te/device/child01///m/temperature");
        assert_eq!(topic, Some("v1/gateway/telemetry".to_string()));
    }

    #[test]
    fn test_main_device_event_topic() {
        let topic = map_to_tb_topic("te/device/main///e/login");
        assert_eq!(topic, Some("v1/devices/me/telemetry".to_string()));
    }

    #[test]
    fn test_main_device_alarm_topic() {
        let topic = map_to_tb_topic("te/device/main///a/high_temp");
        assert_eq!(topic, Some("v1/devices/me/telemetry".to_string()));
    }

    #[test]
    fn test_main_device_twin_topic() {
        let topic = map_to_tb_topic("te/device/main///twin/my_attribute");
        assert_eq!(topic, Some("v1/devices/me/attributes".to_string()));
    }

    #[test]
    fn test_child_device_twin_topic() {
        let topic = map_to_tb_topic("te/device/child01///twin/my_attribute");
        assert_eq!(topic, Some("v1/gateway/attributes".to_string()));
    }

    #[test]
    fn test_main_device_health_topic() {
        let topic = map_to_tb_topic("te/device/main///status/health");
        assert_eq!(topic, Some("v1/devices/me/telemetry".to_string()));
    }

    #[test]
    fn test_unknown_topic_returns_none() {
        let topic = map_to_tb_topic("te/device/main///cmd/restart");
        assert_eq!(topic, None);
    }

    #[test]
    fn test_extract_device_name() {
        assert_eq!(extract_device_name("te/device/child01///m/temp"), "child01");
        assert_eq!(extract_device_name("te/device/main///m/temp"), "main");
    }
}
