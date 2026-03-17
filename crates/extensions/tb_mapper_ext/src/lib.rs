use camino::Utf8Path;
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
        input_topics: String,
        topic_prefix: TopicPrefix,
        errors_topic: Topic,
        size_threshold: usize,
        add_timestamp: bool,
        time_format: TimeFormat,
    ) -> Self {
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
        create_directory_with_defaults(flows_dir.as_ref())
            .map_err(|e| UpdateFlowRegistryError::Other(e.into()))?;
        let mut flows = ConnectedFlowRegistry::new(flows_dir);
        flows.register_builtin(SetTbTopic::default());
        self.persist_builtin_flow(&mut flows).await?;
        Ok(flows)
    }

    pub(crate) async fn persist_builtin_flow(
        &self,
        flows: &mut ConnectedFlowRegistry,
    ) -> Result<(), UpdateFlowRegistryError> {
        let flow_definition = self.builtin_flow();
        flows
            .update_flow("tb_builtin", &flow_definition)
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
fn map_to_tb_topic(source: &str, prefix: &str) -> Option<String> {
    let schema = MqttSchema::with_root(prefix.to_string());

    // Parse the thin-edge topic to determine the entity and channel
    if let Ok((entity_id, channel)) = schema.entity_channel_of(&Topic::new_unchecked(source)) {
        let device_name = entity_id.default_device_name().unwrap_or("main");
        let is_main_device = device_name == "main";

        match channel.as_str() {
            // Measurements -> Telemetry
            c if c.contains("/m/") || c.ends_with("/m/") => {
                if is_main_device {
                    Some("v1/devices/me/telemetry".to_string())
                } else {
                    // Gateway mode: wrap payload with device name
                    Some("v1/gateway/telemetry".to_string())
                }
            }
            // Events and Alarms -> Telemetry (ThingsBoard treats these as telemetry)
            c if c.contains("/e/") || c.contains("/a/") => {
                if is_main_device {
                    Some("v1/devices/me/telemetry".to_string())
                } else {
                    Some("v1/gateway/telemetry".to_string())
                }
            }
            // Twin data -> Attributes
            c if c.contains("/twin/") => {
                if is_main_device {
                    Some("v1/devices/me/attributes".to_string())
                } else {
                    Some("v1/gateway/attributes".to_string())
                }
            }
            // Health status -> Telemetry
            c if c.contains("/status/health") => {
                if is_main_device {
                    Some("v1/devices/me/telemetry".to_string())
                } else {
                    Some("v1/gateway/telemetry".to_string())
                }
            }
            _ => None,
        }
    } else {
        None
    }
}

/// Wraps a payload for ThingsBoard gateway mode.
///
/// ThingsBoard gateway expects:
/// ```json
/// { "Device A": [{ "key": "value" }] }
/// ```
fn wrap_for_gateway(device_name: &str, payload: serde_json::Value) -> serde_json::Value {
    serde_json::json!({
        device_name: [payload]
    })
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
        if let Some(prefix) = config.get("prefix").and_then(|v| v.as_str()) {
            self.prefix = prefix.to_string();
        }
        Ok(())
    }

    fn on_message(
        &self,
        _context: &FlowContextHandle,
        message: Message,
    ) -> Result<Vec<Message>, FlowError> {
        let source_topic = message.topic.name.as_str();

        if let Some(tb_topic) = map_to_tb_topic(source_topic, &self.prefix) {
            let output = Message {
                topic: Topic::new_unchecked(&tb_topic),
                ..message
            };
            Ok(vec![output])
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
        let topic = map_to_tb_topic("te/device/main///m/temperature", "te");
        assert_eq!(topic, Some("v1/devices/me/telemetry".to_string()));
    }

    #[test]
    fn test_child_device_measurement_topic() {
        let topic = map_to_tb_topic("te/device/child01///m/temperature", "te");
        assert_eq!(topic, Some("v1/gateway/telemetry".to_string()));
    }

    #[test]
    fn test_main_device_twin_topic() {
        let topic = map_to_tb_topic("te/device/main///twin/my_attribute", "te");
        assert_eq!(topic, Some("v1/devices/me/attributes".to_string()));
    }

    #[test]
    fn test_child_device_twin_topic() {
        let topic = map_to_tb_topic("te/device/child01///twin/my_attribute", "te");
        assert_eq!(topic, Some("v1/gateway/attributes".to_string()));
    }

    #[test]
    fn test_unknown_topic_returns_none() {
        let topic = map_to_tb_topic("te/device/main///cmd/restart", "te");
        assert_eq!(topic, None);
    }

    #[test]
    fn test_wrap_for_gateway() {
        let payload = serde_json::json!({"temperature": 25.0});
        let wrapped = wrap_for_gateway("child01", payload);
        assert_eq!(
            wrapped,
            serde_json::json!({"child01": [{"temperature": 25.0}]})
        );
    }
}