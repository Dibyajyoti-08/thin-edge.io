# ThingsBoard Cloud Support in thin-edge.io

**Author:** Dibyajyoti-08 \<eng.djena075@gmail.com\>  
**Integration Period:** March 16, 2026 – March 26, 2026  
**Total Commits:** 45  
**Branch:** `main`

---

## Overview

This document describes the porting of ThingsBoard (TB) cloud support into thin-edge.io. The work adds ThingsBoard as a first-class cloud provider alongside the existing Cumulocity IoT (c8y), Azure (az), and AWS (aws) integrations.

ThingsBoard is an open-source IoT platform that uses MQTT with a specific topic schema (`v1/devices/me/...` for single devices and `v1/gateway/...` for gateways). This integration enables a thin-edge.io device to:

- Publish telemetry, events, alarms, and twin/attribute data to ThingsBoard
- Receive RPC requests from ThingsBoard and respond to them
- Provision itself with ThingsBoard using X.509 certificates
- Be managed through the standard `tedge connect tb` / `tedge disconnect tb` CLI

---

## Architecture

```
┌──────────────────────────────────────────────────────┐
│                 Device (thin-edge.io)                │
│                                                      │
│  te/device/main///m/temp  ──►  TbConverter           │
│  te/device/child01///m/temp     (tb_mapper_ext)      │
│            │                        │                │
│            │                        ▼                │
│            │              tb/telemetry               │
│            │              tb/v1/gateway/telemetry    │
│            │                        │                │
│            └──► mosquitto-tb-bridge ─────────────────► ThingsBoard Cloud
│                                                      │    v1/devices/me/telemetry
│  TbMapper (tedge-mapper-tb)                          │    v1/devices/me/attributes
│  - Runs as a systemd service                         │    v1/devices/me/rpc/...
│  - Subscribes to te/# topics                         │
│  - Converts to TB format via flows                   │
└──────────────────────────────────────────────────────┘
```

---

## Commit History & Change Analysis

### Phase 1 — Skeleton & Workspace Setup (Mar 16–17, 2026)

#### `960d4835` — Skeleton implementation for the ThingsBoard integration

The foundational commit that created all the empty module files and registered the new modules within the existing crate structure:

**New files created:**

- `crates/core/tedge/src/bridge/tb.rs` — Bridge configuration for TB
- `crates/core/tedge/src/cli/connect/tb.rs` — `tedge connect tb` command handler
- `crates/core/tedge_mapper/src/tb/mapper.rs` — TB mapper actor
- `crates/core/tedge_mapper/src/tb/mod.rs` — TB mapper module entry
- `crates/extensions/tb_mapper_ext/Cargo.toml` — New extension crate
- `crates/extensions/tb_mapper_ext/src/lib.rs` — TB converter extension

**Registered in:**

- `crates/core/tedge/src/bridge/mod.rs` — added `pub mod tb`
- `crates/core/tedge/src/cli/connect/mod.rs` — added `pub mod tb`

---

#### `44bea151` — Workspace dependency registration

Added `tb_mapper_ext` to the root `Cargo.toml` workspace members so the new crate is discoverable by cargo.

#### `fa7cae78` — ThingsBoard extension: initial TB data model

Created the `tb_mapper_ext` crate with:

- `TbConverter` struct — the core message converter
- `map_to_tb_topic()` function — maps thin-edge.io MQTT topics to TB topics
- Initial `SetTbTopic` flow transformer implementation
- `Cargo.toml` with dependencies (`tedge_flows`, `tedge_mqtt_ext`, `tedge_api`, `tedge_config`, `tedge_utils`, `camino`, `serde_json`)

#### `a7741e71` — `tedge_mapper` Cargo.toml: ThingsBoard features

Added `tb_mapper_ext` as a dependency in `crates/core/tedge_mapper/Cargo.toml` with appropriate features.

#### `9395cb79` — Bridge configuration for ThingsBoard

First implementation of `BridgeConfigTbParams` and its `From<BridgeConfigTbParams> for BridgeConfig` conversion in `crates/core/tedge/src/bridge/tb.rs`.

---

### Phase 2 — Core CLI & Config Integration (Mar 17–18, 2026)

#### `605166e1` — Refactor TbConverter and map_to_tb_topic

Improved the `map_to_tb_topic` function for correct topic pattern matching and better error handling in the flow pipeline.

#### `dd3fdec8` — Add TB MQTT payload limit constant

Added `TB_MQTT_PAYLOAD_LIMIT` constant to `tedge_config` to cap message sizes per ThingsBoard's constraints.

#### `c470dfdc` — TB MQTT payload limit constant in config

Wired the payload limit constant into the main `TEdgeConfig`.

#### `b6a02ddc` — Integrate ThingsBoard configuration into TEdgeConfig

Major config work in `crates/common/tedge_config/src/tedge_toml/tedge_config.rs` (+151 lines):

- Added `tb` section to the main `TEdgeConfig` struct
- Defined `TbConfig` with fields:
  - `url` / `mqtt.host` / `mqtt.port` — ThingsBoard server address
  - `device.id` — device identifier (used as MQTT client ID and username)
  - `auth.cert_file`, `auth.key_file`, `auth.ca_file` — X.509 credentials
  - `access_token` — alternative token-based auth
  - `topic_prefix` — namespace prefix (default: `tb`)

#### `125fdb33` — CloudType enum: ThingsBoard variant

Added `Tb` variant to the `CloudType` enum in `crates/common/tedge_config/src/tedge_toml/models/mod.rs` so ThingsBoard is recognized as a valid cloud type throughout the system.

#### `195f8886` — Refactor BridgeConfigTbParams and device status check

Significant work on `crates/core/tedge/src/bridge/tb.rs` (+149 lines):

- Complete `BridgeConfigTbParams` with all MQTT bridge configuration fields
- Initial skeleton for `check_device_status_tb()`
- Bridging topics defined:
  - `telemetry out 1 {prefix}/ v1/devices/me/`
  - `attributes out 1 {prefix}/ v1/devices/me/`
  - `attributes in 1 {prefix}/ v1/devices/me/`
  - `rpc/request/+ in 1 {prefix}/ v1/devices/me/`
  - `rpc/response/+ out 1 {prefix}/ v1/devices/me/`

#### `39a4371a` — CloudArg and ConnectCommand: ThingsBoard support

Updated CLI argument parsing in:

- `crates/core/tedge/src/cli/common.rs` — Added `Tb` and `Tb(ProfileName)` variants to `CloudArg` and `MaybeBorrowedCloud` enums
- `crates/core/tedge/src/cli/connect/command.rs` — Added TB branch in `ConnectCommand::run()` to invoke the TB connect flow

#### `cd782165` — MapperName: TbMapper support

Added `Tb` variant to the `MapperName` enum and updated `lookup_component()` in `crates/core/tedge_mapper/src/lib.rs` so `tedge-mapper tb` dispatches to the new `TbMapper`.

#### `defa1451` — TbMapper component and built-in bridge rules

First full implementation of `TbMapper` in `crates/core/tedge_mapper/src/tb/mapper.rs` (+117 lines):

- Actor-based mapper that subscribes to TB-relevant topics
- Calls `TbConverter` to transform messages
- Sets up built-in flow pipeline rules for message processing
- Implements the `tedge-mapper-tb` service entry point

#### `1ed09a51` — `possible_clouds()`: ThingsBoard entry

Added `tb` to the list returned by `possible_clouds()` in `crates/core/tedge/src/cli/refresh_bridges.rs` so `tedge reconnect` and bridge refresh commands include TB.

#### `d7382896` — SystemService enum: tedge-mapper-tb

Added `TedgeMapperTb` to the `SystemService` enum and its `Display` implementation in `crates/core/tedge/src/system_services/services.rs`, mapping it to the service name `tedge-mapper-tb`.

---

### Phase 3 — Mapper Config & Structural Refinement (Mar 18–20, 2026)

#### `8200e83a` — TbMapperSpecificConfig struct

Created `TbMapperSpecificConfig` in the mapper config module with:

- `add_timestamp: bool` — controls whether a timestamp is injected into outgoing messages

#### `486dc9c6` — Mapper configuration: TB cloud-specific functionality

Comprehensive mapper config additions (+190 lines across two files):

- `crates/common/tedge_config/src/tedge_toml/tedge_config/mapper_config/mod.rs` — `TbCloudMapperConfig` struct definition
- `crates/common/tedge_config/src/tedge_toml/tedge_config/mapper_config/compat.rs` — compatibility layer mapping config fields to TB mapper parameters

#### `8622ada9` — Rename TbMapperSpecificConfig → TbCloudMapperConfig

Renamed the struct for naming consistency with the other cloud mapper configs (e.g., `C8yCloudMapperConfig`, `AzCloudMapperConfig`).

#### `0434ac5e` — Improve config comments

Minor documentation improvement on config field comments.

#### `078e270b` — tb_mapper_ext package integration

Wired `tb_mapper_ext` into the mapper's module system:

- `crates/core/tedge_mapper/src/lib.rs` — registered `tb` module
- `crates/common/tedge_config/src/tedge_toml/tedge_config_location.rs` — added TB config file path registration
- Updated `Cargo.lock`

#### `1a87f26e` — TbConverter accepts MqttSchema; error topic handling

Refactored `TbConverter::new()` in `crates/extensions/tb_mapper_ext/src/lib.rs` to:

- Accept `&MqttSchema` instead of a raw topic string for the errors topic
- Derive the error topic from the schema (`mqtt_schema.error_topic()`)
- Accept `input_topics: String` as a parameter (dynamic subscription pattern)

Similarly updated `TbMapper` in `crates/core/tedge_mapper/src/tb/mapper.rs` to pass the schema through.

#### `970904af` — Refactor TbMapperSpecificConfig to use TbCloudMapperConfig

Updated the compatibility layer in `mapper_config/compat.rs` to use the renamed struct consistently.

---

### Phase 4 — Bridge CLI Commands & Inspection (Mar 23, 2026)

#### `e0883678` — Refactor BridgeConfigTbParams; complete topic handling

Major refactor of `crates/core/tedge/src/bridge/tb.rs` (+162 lines, -88 lines):

- Finalized the exact Mosquitto bridge topic directives
- Added support for profile-specific connection names (`edge_to_tb@{profile}`)
- Set `clean_session: false` to retain subscriptions across reconnects
- Added `use_mapper: true`, `use_agent: false`, `auth_type: Certificate`
- Health topic derived from `MqttSchema` for the bridge service

#### `2a05fa8d` — cloud_name(): ThingsBoard support

Added `"tb"` to `cloud_name()` in `crates/core/tedge/src/cli/bridge/common.rs`.

#### `0216b960` — run_inspect(): ThingsBoard support

Added TB branch to `run_inspect()` in `crates/core/tedge/src/cli/bridge/inspect.rs` so `tedge bridge inspect tb` works.

#### `6ffd6449` — CloudTopicArg: ThingsBoard topic testing

Added TB topics to `CloudTopicArg` in `crates/core/tedge/src/cli/bridge/test_command.rs` (+22 lines) so `tedge bridge test tb` can verify topic connectivity.

#### `934f006e` — extract_device_id_for_cloud(): ThingsBoard

Added `Tb` matching in `crates/core/tedge/src/cli/certificate/create_key.rs` so `tedge cert create --cloud tb` correctly extracts the device ID.

#### `252b6864` — Fix formatting: Tb variant in MaybeBorrowedCloud

Corrected the `Display` implementation for the `Tb(profile)` variant in `crates/core/tedge/src/cli/common.rs`.

---

### Phase 5 — Device Provisioning with X.509 (Mar 23–24, 2026)

#### `e3611bf3` — check_device_status_tb() function

Added `check_device_status_tb()` in `crates/core/tedge/src/cli/connect/tb.rs` (+129 lines):

- Connects to the local MQTT broker
- Subscribes to the TB bridge health topic
- Waits for the bridge to report a connected status
- Returns `Ok(DeviceStatus::Connected)` or appropriate error

#### `68209fae` — TbMapper: flows configuration integration

Updated `TbMapper` in `crates/core/tedge_mapper/src/tb/mapper.rs` to:

- Create a `ConnectedFlowRegistry` from `TbConverter`
- Reference the correct `max_payload_size` from config

#### `c143aa9b` — ThingsBoard config: access_token, provision_key, provision_secret

Added three new config fields to `TEdgeConfig` for the TB section:

- `tb.access_token` — pre-provisioned device access token
- `tb.provision_key` — provisioning device key
- `tb.provision_secret` — provisioning device secret

#### `569097afa` — provision_key and provision_secret in compat layer

Added `provision_key` and `provision_secret` fields to `TbMapperSpecificConfig` compatibility struct.

#### `7fe9cb74` — Optional provision_key/secret in TbCloudMapperConfig

Made `provision_key` and `provision_secret` fields `Option<String>` (not required) for X.509 provisioning.

#### `30e63b5a` — ConnectCommand: X.509 provisioning invocation

Updated `crates/core/tedge/src/cli/connect/command.rs` (+14 lines) to call `provision_device_tb()` when TB cloud is selected and a provision key is configured.

#### `673180d2` — X.509 certificate-based device provisioning

Full implementation of `provision_device_tb()` in `crates/core/tedge/src/cli/connect/tb.rs` (+193 lines):

- Connects directly to ThingsBoard's MQTT broker using the provision key/secret
- Publishes an X.509 provisioning request payload to `v1/devices/provision`
- Subscribes to the response topic and parses the assigned `access_token`
- Writes the token back to the thin-edge.io config for subsequent connections

#### `1dc9d444` — Refactor provisioning payload; improve error handling

Cleaned up error handling in `provision_device_tb()`:

- Better JSON payload construction
- Clear error messages for network/config failures
- Timeout on provisioning response wait

---

### Phase 6 — Systemd Integration & Connection Reliability (Mar 25, 2026)

#### `2c6b0175` — Systemd service and target files for tedge-mapper-tb

Created systemd unit files for the ThingsBoard mapper service:

- `configuration/init/systemd/tedge-mapper-tb.service` — main service unit
- `configuration/init/systemd/tedge-mapper-tb@.service` — profile-specific service unit (allows `tedge-mapper-tb@production.service`)
- `configuration/init/systemd/tedge-mapper-tb.target` — systemd target for TB services
- Added debug logging to `check_device_status_tb` for bridge health tracking

#### `fd54193b` — BridgeConfig: final telemetry and RPC topics

Finalized the bridge topic configuration in `crates/core/tedge/src/bridge/tb.rs`:

- Correct Mosquitto bridge directives for all 5 TB topics
- Adjusted `connection` identifier format
- Ensured `clean_session: false` for reliable reconnection

#### `2a93cf34` — Simplify MQTT connection checks in check_device_status_tb

Streamlined the connection verification logic:

- Removed unnecessary topic subscriptions
- Improved log messages for bridge health status reporting
- Simplified the wait loop

#### `d10c12c2` — Remove debug clutter from check_device_status_tb

Cleaned up `crates/core/tedge/src/cli/connect/tb.rs`: removed commented-out code and debug print statements (+130 lines removed).

---

### Phase 7 — Mapper Improvements & Topic Mapping (Mar 25, 2026)

#### `6610e7f1` — TbMapper: dynamic input topics and file system / command watchers

Major enhancement of `crates/core/tedge_mapper/src/tb/mapper.rs` (+24 lines):

- TbMapper now subscribes to dynamically computed input topics (from `TbConverter.input_topics`)
- Integrated `FileSystemWatcher` for flow config hot-reload
- Integrated `CommandWatcher` for handling TB RPC commands
- Improved flow lifecycle management (start/stop)

#### `4d5f1d0f` — Refactor map_to_tb_topic: parameterized prefix; child device routing

Significant improvement to `crates/extensions/tb_mapper_ext/src/lib.rs` (+49 lines, -26 lines):

- `map_to_tb_topic()` now accepts a `prefix: &str` parameter (used by `SetTbTopic` which reads it from flow config)
- Added routing for all thin-edge channel types:

| Thin-edge topic pattern          | ThingsBoard topic                |
| -------------------------------- | -------------------------------- |
| `te/device/main///m/{type}`      | `{prefix}/telemetry`             |
| `te/device/{child}///m/{type}`   | `{prefix}/v1/gateway/telemetry`  |
| `te/device/main///e/{type}`      | `{prefix}/telemetry`             |
| `te/device/{child}///e/{type}`   | `{prefix}/v1/gateway/telemetry`  |
| `te/device/main///a/{type}`      | `{prefix}/telemetry`             |
| `te/device/{child}///a/{type}`   | `{prefix}/v1/gateway/telemetry`  |
| `te/device/main///twin/{key}`    | `{prefix}/attributes`            |
| `te/device/{child}///twin/{key}` | `{prefix}/v1/gateway/attributes` |
| `te/device/…/status/health`      | _(dropped — not forwarded)_      |

- Health topics are explicitly excluded from forwarding to ThingsBoard

#### `9472477a` — Remove unnecessary commented lines

Removed ~35 lines of commented-out dead code from `tb_mapper_ext/src/lib.rs`.

---

### Phase 8 — Reconnection Resilience (Mar 26, 2026)

#### `cf6490ea` — Retaining data after reconnection is established

Final fix in `crates/core/tedge/src/bridge/tb.rs`:

- Ensured `clean_session: false` and `local_clean_session: false` are correctly set
- Ensures messages queued during broker outages are replayed once the bridge reconnects to ThingsBoard

---

## Files Changed Summary

| File                                                                             | Role                                                              |
| -------------------------------------------------------------------------------- | ----------------------------------------------------------------- |
| `crates/extensions/tb_mapper_ext/src/lib.rs`                                     | `TbConverter`, `SetTbTopic` flow transformer, `map_to_tb_topic()` |
| `crates/extensions/tb_mapper_ext/Cargo.toml`                                     | New extension crate manifest                                      |
| `crates/core/tedge/src/bridge/tb.rs`                                             | `BridgeConfigTbParams` → Mosquitto bridge config                  |
| `crates/core/tedge/src/cli/connect/tb.rs`                                        | `check_device_status_tb()`, `provision_device_tb()`               |
| `crates/core/tedge/src/cli/connect/command.rs`                                   | TB branch in `ConnectCommand::run()`                              |
| `crates/core/tedge/src/cli/common.rs`                                            | `CloudArg::Tb`, `MaybeBorrowedCloud::Tb` variants                 |
| `crates/core/tedge/src/cli/bridge/common.rs`                                     | `cloud_name()` TB support                                         |
| `crates/core/tedge/src/cli/bridge/inspect.rs`                                    | `run_inspect()` TB support                                        |
| `crates/core/tedge/src/cli/bridge/test_command.rs`                               | `CloudTopicArg` TB topics                                         |
| `crates/core/tedge/src/cli/certificate/create_key.rs`                            | `extract_device_id_for_cloud()` TB                                |
| `crates/core/tedge/src/cli/refresh_bridges.rs`                                   | `possible_clouds()` TB entry                                      |
| `crates/core/tedge/src/system_services/services.rs`                              | `SystemService::TedgeMapperTb`                                    |
| `crates/core/tedge/Cargo.toml`                                                   | `tb_mapper_ext` dependency                                        |
| `crates/core/tedge_mapper/src/lib.rs`                                            | `MapperName::Tb`, `lookup_component()` TB                         |
| `crates/core/tedge_mapper/src/tb/mapper.rs`                                      | `TbMapper` actor implementation                                   |
| `crates/core/tedge_mapper/src/tb/mod.rs`                                         | TB mapper module declaration                                      |
| `crates/core/tedge_mapper/Cargo.toml`                                            | TB features/dependencies                                          |
| `crates/common/tedge_config/src/tedge_toml/tedge_config.rs`                      | `TbConfig` in `TEdgeConfig`                                       |
| `crates/common/tedge_config/src/tedge_toml/models/mod.rs`                        | `CloudType::Tb`                                                   |
| `crates/common/tedge_config/src/tedge_toml/tedge_config_location.rs`             | TB config file path                                               |
| `crates/common/tedge_config/src/tedge_toml/tedge_config/mapper_config/mod.rs`    | `TbCloudMapperConfig`                                             |
| `crates/common/tedge_config/src/tedge_toml/tedge_config/mapper_config/compat.rs` | compatibility layer                                               |
| `configuration/init/systemd/tedge-mapper-tb.service`                             | systemd service unit                                              |
| `configuration/init/systemd/tedge-mapper-tb@.service`                            | profile-aware service unit                                        |
| `configuration/init/systemd/tedge-mapper-tb.target`                              | systemd target                                                    |
| `Cargo.toml`                                                                     | workspace member registration                                     |
| `Cargo.lock`                                                                     | dependency lock update                                            |

---

## Key Design Decisions

### 1. MQTT Bridge via Mosquitto

ThingsBoard acts as a standard MQTT broker. The integration uses thin-edge.io's existing Mosquitto bridge infrastructure (same as c8y/az/aws) rather than a custom TCP connection. The bridge maps local `{prefix}/...` topics to TB's `v1/devices/me/...` namespace.

### 2. Flow-based Message Transformation

Message conversion uses thin-edge.io's `tedge_flows` pipeline rather than hard-coded Rust logic:

```toml
steps = [
  { builtin = "skip-mosquitto-health-status" },
  { builtin = "add-timestamp", config = { format = "unix" } },
  { builtin = "limit-payload-size", config = { max_size = 65536 } },
  { builtin = "set-tb-topic", config = { prefix = "tb" } },
]
```

The `SetTbTopic` transformer is implemented as a `tedge_flows::Transformer` plugin.

### 3. X.509 Certificate Authentication

ThingsBoard supports X.509 mutual TLS. The integration:

- Uses the device's existing thin-edge.io certificate for authentication
- Supports automatic provisioning via ThingsBoard's MQTT provisioning API
- Falls back to access token auth if `tb.access_token` is configured

### 4. Gateway Mode for Child Devices

Measurements/events/alarms from child devices are routed to ThingsBoard's **gateway API** (`v1/gateway/telemetry`, `v1/gateway/attributes`) rather than the single-device API.

### 5. Session Persistence

`clean_session: false` ensures that MQTT subscriptions and queued messages survive broker restarts and network outages, providing reliable delivery.

---

## Usage

### Connect to ThingsBoard

```sh
# Configure ThingsBoard endpoint
tedge config set tb.url "mqtt.thingsboard.example.com"

# Connect using X.509 certificate (auto-provisions if provision key configured)
tedge connect tb

# Disconnect
tedge disconnect tb
```

### Verify connection

```sh
tedge bridge inspect tb
tedge bridge test tb
```

### Configuration reference

```toml
[tb]
url = "mqtt.thingsboard.example.com"
topic_prefix = "tb"
access_token = ""          # optional: pre-provisioned token
provision_key = ""         # optional: for auto-provisioning
provision_secret = ""      # optional: for auto-provisioning

[tb.device]
id = "my-device-001"

[tb.auth]
cert_file = "/etc/tedge/device-certs/tedge-certificate.pem"
key_file  = "/etc/tedge/device-certs/tedge-private-key.pem"
ca_file   = "/etc/tedge/device-certs/ca.pem"

[tb.mapper]
add_timestamp = true
```

### Service management

```sh
systemctl status tedge-mapper-tb
systemctl start  tedge-mapper-tb
systemctl stop   tedge-mapper-tb
# Profile-specific instance:
systemctl start  tedge-mapper-tb@production
```
