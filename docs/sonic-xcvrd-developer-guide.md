# SONiC XCVRD Developer Guide

A comprehensive technical reference for developers working on the SONiC Transceiver Daemon (xcvrd).

---

## Table of Contents

1. [Overview](#overview)
2. [Architecture & Threading Model](#architecture--threading-model)
3. [Directory Structure](#directory-structure)
4. [Core Components Deep Dive](#core-components-deep-dive)
5. [CMIS State Machine](#cmis-state-machine)
6. [Database Tables Reference](#database-tables-reference)
7. [Configuration & Settings](#configuration--settings)
8. [Event Flow & Data Flow](#event-flow--data-flow)
9. [Debugging Guide](#debugging-guide)
10. [Code Patterns & Best Practices](#code-patterns--best-practices)
11. [Testing](#testing)
12. [Common Issues & Troubleshooting](#common-issues--troubleshooting)

---

## Overview

### What is xcvrd?

`xcvrd` (Transceiver Daemon) is a SONiC platform daemon responsible for:

- **Transceiver Presence Detection**: Monitoring SFP/QSFP module insertion/removal
- **EEPROM Data Collection**: Reading and posting transceiver information to Redis databases
- **CMIS Module Management**: Managing CMIS-compliant module initialization and state transitions
- **DOM Monitoring**: Periodic collection of Diagnostic Optical Monitoring (DOM) sensor data
- **VDM Monitoring**: Vital Data Monitoring for advanced diagnostics
- **Link Bring-up Control**: Deterministic TX enable/disable for SFF modules

### Supported Module Types

| Module Type | Management Standard | Manager |
|-------------|---------------------|---------|
| SFP/SFP+ | SFF-8472 | SffManagerTask |
| QSFP+/QSFP28 | SFF-8636 | SffManagerTask |
| QSFP-DD | CMIS | CmisManagerTask |
| OSFP | CMIS | CmisManagerTask |
| OSFP-8X | CMIS | CmisManagerTask |

### Entry Point

```bash
xcvrd [--skip_cmis_mgr] [--enable_sff_mgr]
```

| Flag | Description | Default |
|------|-------------|---------|
| `--skip_cmis_mgr` | Disable CMIS manager thread | Enabled |
| `--enable_sff_mgr` | Enable SFF manager thread | Disabled |

---

## Architecture & Threading Model

### High-Level Architecture

```
                                    ┌─────────────────────────────────────────────┐
                                    │              DaemonXcvrd                     │
                                    │         (Main Thread / Daemon Base)          │
                                    │                                             │
                                    │  - Platform initialization                  │
                                    │  - Signal handling (SIGHUP, SIGINT, SIGTERM)│
                                    │  - Thread lifecycle management              │
                                    │  - Database table initialization            │
                                    └─────────────────┬───────────────────────────┘
                                                      │
                     ┌────────────────────────────────┼────────────────────────────────┐
                     │                                │                                │
                     ▼                                ▼                                ▼
    ┌────────────────────────────┐  ┌────────────────────────────┐  ┌────────────────────────────┐
    │    SfpStateUpdateTask      │  │     CmisManagerTask        │  │    DomInfoUpdateTask       │
    │                            │  │                            │  │                            │
    │  - SFP presence detection  │  │  - CMIS state machine      │  │  - Periodic DOM updates    │
    │  - EEPROM info posting     │  │  - Application selection   │  │  - VDM real values         │
    │  - Error status tracking   │  │  - Datapath initialization │  │  - Status flag updates     │
    │  - DOM threshold posting   │  │  - TX power control        │  │  - Firmware info           │
    └────────────────────────────┘  └────────────────────────────┘  └────────────────────────────┘
                                                      │
                                                      │ (Optional)
                                                      ▼
                                    ┌────────────────────────────┐
                                    │     SffManagerTask         │
                                    │                            │
                                    │  - SFF TX enable/disable   │
                                    │  - High power class enable │
                                    │  - Deterministic link up   │
                                    └────────────────────────────┘
```

### Thread Responsibilities

| Thread | Primary Responsibility | Update Frequency |
|--------|------------------------|------------------|
| `DaemonXcvrd` | Daemon lifecycle, platform init | Event-driven |
| `SfpStateUpdateTask` | Presence detection, initial EEPROM read | 1s polling |
| `CmisManagerTask` | CMIS module state management | 60s state machine loop |
| `DomInfoUpdateTask` | DOM/VDM sensor monitoring | 60s periodic |
| `SffManagerTask` | SFF TX control (optional) | Event-driven |

### Thread Synchronization

Threads communicate through:
1. **Shared `stop_event`**: Signals all threads to terminate
2. **Redis Pub/Sub**: Database change notifications via `SubscriberStateTable`
3. **Shared dictionaries**: `port_dict`, `sfp_obj_dict` (read-mostly, occasional writes)

---

## Directory Structure

```
sonic-xcvrd/
├── xcvrd/
│   ├── __init__.py
│   ├── xcvrd.py                    # Main daemon entry point (~2300 lines)
│   │                               # Contains: DaemonXcvrd, CmisManagerTask, SfpStateUpdateTask
│   ├── sff_mgr.py                  # SFF link bring-up manager
│   ├── dom/
│   │   ├── dom_mgr.py              # DomInfoUpdateTask - periodic DOM monitoring
│   │   └── utilities/
│   │       ├── db/
│   │       │   └── utils.py        # Base DBUtils class
│   │       ├── dom_sensor/
│   │       │   ├── utils.py        # DOMUtils - DOM sensor operations
│   │       │   └── db_utils.py     # DOMDBUtils - DOM database posting
│   │       ├── status/
│   │       │   ├── utils.py        # StatusUtils - transceiver status ops
│   │       │   └── db_utils.py     # StatusDBUtils - status database posting
│   │       └── vdm/
│   │           ├── utils.py        # VDMUtils - VDM operations
│   │           └── db_utils.py     # VDMDBUtils - VDM database posting
│   └── xcvrd_utilities/
│       ├── common.py               # Shared helper functions
│       ├── port_event_helper.py    # Port change event handling
│       ├── xcvr_table_helper.py    # Database table abstraction
│       ├── sfp_status_helper.py    # SFP error status handling
│       ├── media_settings_parser.py # SerDes SI settings parser
│       ├── optics_si_parser.py     # Optics SI settings parser
│       └── utils.py                # XCVRDUtils wrapper class
├── tests/
│   ├── test_xcvrd.py               # Unit tests
│   ├── mock_platform.py            # Platform API mocks
│   └── mock_swsscommon.py          # swsscommon mocks
├── setup.py                        # Package configuration
└── pytest.ini                      # Test configuration
```

---

## Core Components Deep Dive

### 1. DaemonXcvrd (xcvrd.py)

The main daemon class that orchestrates all transceiver operations.

**Key Methods:**

```python
class DaemonXcvrd(daemon_base.DaemonBase):
    def init(self):
        """
        Initialize platform chassis and sfputil objects.
        Load port configuration and create database tables.
        """

    def run(self):
        """
        Main daemon loop:
        1. Wait for PortConfigDone from APPL_DB
        2. Initialize SFP object dictionary
        3. Spawn worker threads (SfpStateUpdateTask, CmisManagerTask, DomInfoUpdateTask)
        4. Handle signals and monitor thread health
        """

    def signal_handler(self, sig, frame):
        """Handle SIGHUP, SIGINT, SIGTERM signals"""
```

**Initialization Sequence:**

```
1. DaemonXcvrd.init()
   ├── Load platform_chassis via sonic_platform
   ├── Load platform_sfputil (legacy fallback)
   ├── Create XcvrTableHelper for all namespaces
   └── Initialize PortMapping from CONFIG_DB

2. DaemonXcvrd.run()
   ├── wait_for_port_config_done()
   ├── initialize_sfp_obj_dict()
   ├── Spawn SfpStateUpdateTask
   ├── Spawn CmisManagerTask (if not skipped)
   ├── Spawn DomInfoUpdateTask
   └── Spawn SffManagerTask (if enabled)
```

### 2. SfpStateUpdateTask (xcvrd.py:1390-1979)

Monitors SFP presence and posts transceiver information to the database.

**Key Methods:**

```python
class SfpStateUpdateTask(threading.Thread):
    def task_worker(self):
        """
        Main loop:
        1. Wait for platform change events via _wrapper_get_transceiver_change_event()
        2. Map events to internal FSM events (NORMAL_EVENT, SYSTEM_BECOME_READY, etc.)
        3. Handle insertion: read EEPROM, post to TRANSCEIVER_INFO
        4. Handle removal: clean up database entries
        """

    def _post_port_sfp_info_and_dom_thr_to_db_once(self, port_dict, transceiver_dict):
        """Post initial transceiver info and DOM thresholds after insertion"""

    def _init_port_sfp_status_sw_tbl(self, port_dict):
        """Initialize TRANSCEIVER_STATUS_SW table with status/error fields"""
```

**Event Processing FSM:**

```
                    ┌─────────────────┐
                    │   STATE_INIT    │
                    └────────┬────────┘
                             │ SYSTEM_BECOME_READY
                             ▼
                    ┌─────────────────┐
          ┌─────────│  STATE_NORMAL   │◄────────┐
          │         └────────┬────────┘         │
          │                  │                  │
   SYSTEM_FAIL               │ NORMAL_EVENT     │ SYSTEM_BECOME_READY
          │                  │                  │
          ▼                  ▼                  │
    ┌─────────────────┐   Process              │
    │   STATE_EXIT    │   change event ────────┘
    └─────────────────┘
```

### 3. CmisManagerTask (xcvrd.py:319-1389)

Manages CMIS-compliant module initialization and configuration.

**Key Methods:**

```python
class CmisManagerTask(threading.Thread):
    CMIS_MODULE_TYPES = ['QSFP-DD', 'QSFP_DD', 'OSFP', 'OSFP-8X', 'QSFP+C']
    CMIS_MAX_HOST_LANES = 8
    CMIS_MAX_RETRIES = 3

    def task_worker(self):
        """
        CMIS state machine loop:
        1. Subscribe to CONFIG_DB PORT, STATE_DB TRANSCEIVER_INFO, STATE_DB PORT_TABLE
        2. For each port in port_dict, process state transitions
        3. Handle timeouts with retry logic
        """

    def force_cmis_reinit(self, lport, retries=0):
        """Force restart of CMIS state machine for a port"""

    def get_cmis_host_lanes_mask(self, api, appl, host_lane_count, subport):
        """Calculate active host lanes bitmask based on application and subport"""

    def is_cmis_application_update_required(self, api, app_new, host_lanes_mask):
        """Check if CMIS application code needs to be updated"""
```

### 4. DomInfoUpdateTask (dom/dom_mgr.py)

Periodic monitoring of DOM sensors, VDM values, and transceiver status.

**Key Methods:**

```python
class DomInfoUpdateTask(threading.Thread):
    DOM_INFO_UPDATE_PERIOD_SECS = 60

    def task_worker(self):
        """
        Periodic monitoring loop (every 60 seconds):
        1. Check port DOM monitoring enabled/disabled
        2. Skip ports in CMIS initialization process
        3. Post DOM sensor values (temp, voltage, bias, power)
        4. Post DOM flags (alarm/warning threshold violations)
        5. Post transceiver HW status
        6. Post VDM values if supported (with freeze/unfreeze)
        """

    def is_port_dom_monitoring_disabled(self, logical_port_name):
        """Check if DOM polling is disabled via CONFIG_DB or CMIS init in progress"""

    def on_port_update_event(self, port_change_event):
        """Handle link change events from APPL_DB flap_count field"""
```

### 5. SffManagerTask (sff_mgr.py)

Deterministic link bring-up for SFF-compliant (non-CMIS) modules.

**Key Methods:**

```python
class SffManagerTask(threading.Thread):
    DEFAULT_NUM_LANES_PER_PPORT = 4

    def task_worker(self):
        """
        TX control loop:
        1. Monitor CONFIG_DB admin_status, STATE_DB host_tx_ready
        2. Enable TX only when: admin_status='up' AND host_tx_ready='true'
        3. Handle per-lane TX disable for breakout configurations
        """

    def calculate_tx_disable_delta_array(self, cur_tx_disable_array, tx_disable_flag, active_lanes):
        """Calculate which lanes need TX state change"""

    def enable_high_power_class(self, xcvr_api, lport):
        """Enable high power class (5-8) for supported modules"""
```

---

## CMIS State Machine

### State Definitions

```python
# xcvrd.py:56-72
CMIS_STATE_UNKNOWN        = 'UNKNOWN'       # Initial state, xcvrd not yet processed
CMIS_STATE_INSERTED       = 'INSERTED'      # Module detected, starting initialization
CMIS_STATE_DP_PRE_INIT_CHECK = 'DP_PRE_INIT_CHECK'  # Checking pre-conditions for DataPath init
CMIS_STATE_DP_DEINIT      = 'DP_DEINIT'     # Deinitializing DataPath
CMIS_STATE_AP_CONF        = 'AP_CONFIGURED' # Configuring Application code
CMIS_STATE_DP_INIT        = 'DP_INIT'       # DataPath initialization in progress
CMIS_STATE_DP_ACTIVATE    = 'DP_ACTIVATION' # Activating DataPath
CMIS_STATE_DP_TXON        = 'DP_TXON'       # Turning on TX
CMIS_STATE_READY          = 'READY'         # Fully initialized (terminal state)
CMIS_STATE_REMOVED        = 'REMOVED'       # Module removed (terminal state)
CMIS_STATE_FAILED         = 'FAILED'        # Initialization failed (terminal state)

CMIS_TERMINAL_STATES = {CMIS_STATE_FAILED, CMIS_STATE_READY, CMIS_STATE_REMOVED}
```

### State Transition Diagram

```
                                   Module Insertion
                                         │
                                         ▼
                              ┌──────────────────┐
                              │     INSERTED     │
                              │                  │
                              │ - Get desired    │
                              │   application    │
                              │ - Calculate lane │
                              │   masks          │
                              │ - Check decomm   │
                              │   required       │
                              └────────┬─────────┘
                                       │
           ┌───────────────────────────┼───────────────────────────┐
           │                           │                           │
           │ host_tx_ready=false       │ host_tx_ready=true        │ decommission
           │ OR admin_status=down      │ AND admin_status=up       │ required
           ▼                           ▼                           ▼
    ┌─────────────┐          ┌─────────────────────┐       ┌──────────────┐
    │    READY    │          │  DP_PRE_INIT_CHECK  │       │   DP_DEINIT  │◄──┐
    │  (TX OFF)   │          │                     │       │  (decomm)    │   │
    └─────────────┘          │ - TX laser OFF if   │       └──────┬───────┘   │
                             │   forced_tx_disabled│              │           │
                             │ - Configure ZR      │              │           │
                             │   tx_power          │              │           │
                             │ - Check if app      │              │           │
                             │   update needed     │              │           │
                             └──────────┬──────────┘              │           │
                                        │                         │           │
                   need_update=false    │    need_update=true     │           │
                          │             │             │           │           │
                          ▼             │             ▼           │           │
                   ┌──────────┐         │      ┌──────────────┐   │           │
                   │  READY   │         │      │   DP_DEINIT  │◄──┘           │
                   └──────────┘         │      │              │               │
                                        │      │ - Set DP     │               │
                                        │      │   deinit     │               │
                                        │      │ - TX disable │               │
                                        │      │ - Set lpmode │               │
                                        │      │   false      │               │
                                        │      └──────┬───────┘               │
                                        │             │                       │
                                        │             ▼                       │
                                        │      ┌──────────────┐               │
                                        │      │   AP_CONF    │               │
                                        │      │              │               │
                                        │      │ - Wait for   │               │
                                        │      │   ModuleReady│               │
                                        │      │ - Wait for   │               │
                                        │      │   DP Deact'd │               │
                                        │      │ - Configure  │               │
                                        │      │   laser freq │               │
                                        │      │ - Stage SI   │               │
                                        │      │   settings   │               │
                                        │      │ - Set appl   │               │
                                        │      └──────┬───────┘               │
                                        │             │                       │
                                        │             ▼                       │
                                        │      ┌──────────────┐               │
                                        │      │   DP_INIT    │               │
                                        │      │              │               │
                                        │      │ - Wait for   │               │
                                        │      │   ConfigSucc │               │
                                        │      │ - Clear      │               │
                                        │      │   decomm     │───────────────┘
                                        │      │ - Check DP   │  (restart if decomm)
                                        │      │   init pend  │
                                        │      └──────┬───────┘
                                        │             │
                                        │             ▼
                                        │      ┌──────────────┐
                                        │      │ DP_ACTIVATE  │
                                        │      │              │
                                        │      │ - Apply DP   │
                                        │      │   init       │
                                        │      │ - Wait for   │
                                        │      │   DP Activ'd │
                                        │      └──────┬───────┘
                                        │             │
                                        │             ▼
                                        │      ┌──────────────┐
                                        │      │   DP_TXON    │
                                        │      │              │
                                        │      │ - Enable TX  │
                                        │      │ - Wait TX    │
                                        │      │   enabled    │
                                        │      └──────┬───────┘
                                        │             │
                                        └─────────────┼─────────────────────────┐
                                                      ▼                         │
                                               ┌──────────┐                     │
                                               │  READY   │                     │
                                               │          │                     │
                                               │ - Post   │                     │
                                               │   ApSel  │                     │
                                               │   to DB  │                     │
                                               └──────────┘                     │
                                                                                │
          ┌─────────────────────────────────────────────────────────────────────┘
          │ timeout OR error at any state
          ▼
    ┌──────────┐
    │  FAILED  │  (after CMIS_MAX_RETRIES=3 attempts)
    └──────────┘
```

### Key CMIS Timing Parameters

```python
# Retrieved from module EEPROM
get_datapath_init_duration()     # DP init time (ms)
get_datapath_deinit_duration()   # DP deinit time (ms)
get_datapath_tx_turnoff_duration() # TX turnoff time (ms)
get_module_pwr_up_duration()     # Module power up time (ms)
get_module_pwr_down_duration()   # Module power down time (ms)

# Fixed constants
MGMT_INIT_TIME_DELAY_SECS = 2    # Management interface init time
CMIS_DEF_EXPIRED = 60            # Default expiration time (seconds)
```

### Decommission Flow

When switching application codes (e.g., from 400G to 2x200G breakout), all DataPath lanes must be reset:

```python
def is_decommission_required(self, api, app_new):
    """
    Check if any lane has a different application code than desired.
    If so, decommission is required to reset all lanes to app=0.
    """
    for lane in range(CMIS_MAX_HOST_LANES):
        app_cur = api.get_application(lane)
        if app_cur != 0 and app_cur != app_new:
            return True
    return False
```

---

## Database Tables Reference

### STATE_DB Tables

| Table Name | Key | Description | Updated By |
|------------|-----|-------------|------------|
| `TRANSCEIVER_INFO` | `<port_name>` | Module identification info | SfpStateUpdateTask |
| `TRANSCEIVER_DOM_SENSOR` | `<port_name>` | DOM sensor values (temp, voltage, power) | DomInfoUpdateTask |
| `TRANSCEIVER_DOM_FLAG` | `<port_name>` | DOM alarm/warning flags | DomInfoUpdateTask |
| `TRANSCEIVER_DOM_THRESHOLD` | `<port_name>` | DOM alarm/warning thresholds | SfpStateUpdateTask |
| `TRANSCEIVER_STATUS` | `<port_name>` | Module hardware status | DomInfoUpdateTask |
| `TRANSCEIVER_STATUS_FLAG` | `<port_name>` | Status alarm/warning flags | DomInfoUpdateTask |
| `TRANSCEIVER_STATUS_SW` | `<port_name>` | Software status (status, error, cmis_state) | CmisManagerTask, SfpStateUpdateTask |
| `TRANSCEIVER_VDM_REAL_VALUE` | `<port_name>` | VDM real-time values | DomInfoUpdateTask |
| `TRANSCEIVER_VDM_*_FLAG` | `<port_name>` | VDM alarm/warning flags | DomInfoUpdateTask |
| `TRANSCEIVER_VDM_*_THRESHOLD` | `<port_name>` | VDM thresholds | SfpStateUpdateTask |
| `TRANSCEIVER_FIRMWARE_INFO` | `<port_name>` | Firmware versions | DomInfoUpdateTask |
| `TRANSCEIVER_PM` | `<port_name>` | Performance monitoring data | DomInfoUpdateTask |

### TRANSCEIVER_INFO Fields

```
type                          - Module type (e.g., "QSFP-DD")
vendor_rev                    - Vendor revision
serial                        - Serial number
manufacturer                  - Vendor name
model                         - Part number (PN)
vendor_oui                    - Vendor OUI
vendor_date                   - Manufacturing date
connector                     - Connector type
encoding                      - Encoding type
cable_type                    - Cable type
cable_length                  - Cable length
specification_compliance      - Compliance specification
application_advertisement     - CMIS application advertisement
cmis_rev                      - CMIS revision (if CMIS module)
active_apsel_hostlane1-8      - Active application per lane
host_lane_count               - Number of host lanes
media_lane_count              - Number of media lanes
is_replaceable                - Whether module is hot-swappable
```

### TRANSCEIVER_STATUS_SW Fields

```
status     - '1' (present) or '0' (not present)
error      - Error description string or 'N/A'
cmis_state - Current CMIS state (UNKNOWN, INSERTED, READY, FAILED, etc.)
```

### CONFIG_DB Tables

| Table Name | Key | Description |
|------------|-----|-------------|
| `PORT` | `<port_name>` | Port configuration |

### CONFIG_DB PORT Fields Used by xcvrd

```
index          - Physical port index
lanes          - Comma-separated lane list
speed          - Port speed (e.g., 400000)
admin_status   - 'up' or 'down'
subport        - Subport index for breakout (0=no breakout, 1-N for subports)
laser_freq     - Laser frequency for coherent modules (GHz)
tx_power       - TX output power for coherent modules (dBm)
dom_polling    - 'enabled' or 'disabled'
```

---

## Configuration & Settings

### Media Settings (media_settings.json)

Platform-specific SerDes Signal Integrity settings loaded from:
- `<platform_path>/<hwsku>/media_settings.json`

**Structure:**

```json
{
  "GLOBAL_MEDIA_SETTINGS": {
    "0-31": {
      "QSFP-DD-400G-DR4": {
        "idriver": "0x12",
        "idriver_post1": "0x00",
        "idriver_pre1": "0x02"
      }
    }
  },
  "PORT_MEDIA_SETTINGS": {
    "Ethernet0": {
      "QSFP-DD-400G-FR4": {
        "idriver": "0x14"
      }
    }
  }
}
```

**Lookup Priority:**
1. `PORT_MEDIA_SETTINGS[<port>][<speed>][<media_key>]`
2. `GLOBAL_MEDIA_SETTINGS[<range>][<media_key>]`
3. Default values

### Optics SI Settings (optics_si_settings.json)

Module-specific Signal Integrity settings:
- `<platform_path>/<hwsku>/optics_si_settings.json`

**Structure:**

```json
{
  "GLOBAL_OPTICS_SI_SETTINGS": {
    "0-31": {
      "VENDOR_NAME-MODEL-50G": {
        "OutputEqPreCursorTargetRx": {
          "OutputEqPreCursorTargetRx1": "0",
          "OutputEqPreCursorTargetRx2": "0"
        }
      }
    }
  }
}
```

---

## Event Flow & Data Flow

### Module Insertion Flow

```
Platform Hardware
       │
       │ SFP Interrupt / Polling
       ▼
┌──────────────────────────────────┐
│  platform_chassis.get_change_   │
│  event(timeout)                 │
│  Returns: (status, sfp_events,  │
│            sfp_errors)          │
└────────────────┬─────────────────┘
                 │
                 ▼
┌──────────────────────────────────┐
│    SfpStateUpdateTask           │
│                                 │
│ 1. Soak insert event (2 sec)    │
│ 2. Read EEPROM transceiver_info │
│ 3. Post to TRANSCEIVER_INFO     │
│ 4. Post DOM thresholds          │
│ 5. Init TRANSCEIVER_STATUS_SW   │
└────────────────┬─────────────────┘
                 │
                 │ STATE_DB TRANSCEIVER_INFO SET
                 ▼
┌──────────────────────────────────┐
│    CmisManagerTask              │
│                                 │
│ 1. Detect XCVR_TYPE change      │
│ 2. Check if CMIS module         │
│ 3. Start CMIS state machine     │
│ 4. Update TRANSCEIVER_STATUS_SW │
│    with cmis_state              │
└──────────────────────────────────┘
```

### Port Configuration Change Flow

```
CONFIG_DB PORT_TABLE
       │
       │ SET/DEL Operation
       ▼
┌──────────────────────────────────┐
│  PortChangeObserver             │
│                                 │
│  - SubscriberStateTable         │
│  - Filter fields (if specified) │
│  - Deduplicate events           │
└────────────────┬─────────────────┘
                 │
                 │ PortChangeEvent
                 ▼
┌──────────────────────────────────┐
│  on_port_update_event()         │
│  (CmisManagerTask /             │
│   SffManagerTask /              │
│   DomInfoUpdateTask)            │
│                                 │
│  Update internal port_dict      │
│  Trigger state machine if needed│
└──────────────────────────────────┘
```

### Periodic DOM Update Flow

```
┌─────────────────────────────────────────────────────┐
│              DomInfoUpdateTask                      │
│                                                     │
│  while not stop_event:                              │
│      if time >= next_periodic_update:               │
│          for each physical_port:                    │
│              ├─ Check dom_polling enabled           │
│              ├─ Check not in CMIS init              │
│              ├─ Check presence                      │
│              │                                      │
│              ├─► post_port_sfp_firmware_info_to_db()│
│              ├─► post_port_dom_sensor_info_to_db()  │
│              ├─► post_port_dom_flags_to_db()        │
│              ├─► post_transceiver_hw_status_to_db() │
│              │                                      │
│              │   if VDM supported:                  │
│              │   ├─► vdm_freeze()                   │
│              │   ├─► post_port_vdm_real_values()    │
│              │   ├─► post_port_vdm_flags()          │
│              │   ├─► post_port_pm_info()            │
│              │   └─► vdm_unfreeze()                 │
│                                                     │
│          next_periodic_update += 60 seconds         │
└─────────────────────────────────────────────────────┘
```

---

## Debugging Guide

### Syslog Identifiers

| Identifier | Source |
|------------|--------|
| `xcvrd` | Main daemon, SfpStateUpdateTask |
| `CmisManagerTask` | CMIS state machine |
| `DomInfoUpdateTask` | DOM periodic updates |
| `SffManagerTask` | SFF link bring-up |
| `SFF-MAIN:` | SffManagerTask main loop |
| `SFF-PORT-UPDATE:` | SffManagerTask port events |
| `CMIS:` | CmisManagerTask prefix |

### Key Log Messages to Monitor

**CMIS State Transitions:**
```bash
# Watch CMIS state changes
grep "CMIS:" /var/log/syslog | grep -E "(state=|READY|FAILED)"

# Example output:
# CMIS: Ethernet0: 400G, lanemask=0xff, CMIS state=INSERTED, Module state=ModuleReady, DP state=DataPathDeactivated
# CMIS: Ethernet0: no CMIS application update required...READY
```

**Module Insertion/Removal:**
```bash
grep -E "(SFP insert|SFP remove|Transceiver for)" /var/log/syslog
```

**Errors:**
```bash
grep -E "(Error|error|Failed|failed|Exception)" /var/log/syslog | grep xcvrd
```

### Redis DB Inspection

```bash
# Check transceiver info
redis-cli -n 6 hgetall "TRANSCEIVER_INFO|Ethernet0"

# Check CMIS state
redis-cli -n 6 hget "TRANSCEIVER_STATUS_SW|Ethernet0" "cmis_state"

# Check DOM sensor values
redis-cli -n 6 hgetall "TRANSCEIVER_DOM_SENSOR|Ethernet0"

# Check error status
redis-cli -n 6 hget "TRANSCEIVER_STATUS_SW|Ethernet0" "error"

# Monitor real-time DB changes
redis-cli -n 6 psubscribe "__keyspace@6__:TRANSCEIVER_*"
```

### Show Commands

```bash
# Transceiver presence
show interfaces transceiver presence

# Transceiver EEPROM info
show interfaces transceiver eeprom

# DOM sensor values
show interfaces transceiver dom

# Error status
show interfaces transceiver error-status
```

### Platform API Debugging

```python
# Interactive debugging
import sonic_platform
chassis = sonic_platform.platform.Platform().get_chassis()
sfp = chassis.get_sfp(0)  # Physical port 0

# Check presence
sfp.get_presence()

# Get XCVR API
api = sfp.get_xcvr_api()

# Get transceiver info
sfp.get_transceiver_info()

# Get DOM info
sfp.get_transceiver_bulk_status()

# CMIS-specific (if CMIS module)
api.get_module_state()
api.get_datapath_state()
api.get_application(lane=0)
api.get_config_datapath_hostlane_status()
```

---

## Code Patterns & Best Practices

### 1. Platform API Wrapper Pattern

Always use wrapper functions for platform API calls to handle both chassis API and legacy sfputil:

```python
# xcvrd_utilities/common.py
def _wrapper_get_presence(physical_port):
    if platform_chassis is not None:
        try:
            return platform_chassis.get_sfp(physical_port).get_presence()
        except NotImplementedError:
            pass
    if platform_sfputil is not None:
        try:
            return platform_sfputil.get_presence(physical_port)
        except NotImplementedError:
            pass
    return False
```

### 2. Multi-ASIC Awareness

All database operations must be ASIC-aware:

```python
# Get ASIC ID for a logical port
asic_id = port_mapping.get_asic_id_for_logical_port(lport)

# Get table for specific ASIC
table = xcvr_table_helper.get_intf_tbl(asic_id)
```

### 3. Graceful Error Handling

```python
try:
    dom_info = sfp.get_transceiver_dom_info()
except (KeyError, TypeError) as e:
    # Continue processing - could be transient error during port reset
    self.log_warning(f"Got exception {repr(e)} for port {lport}, ignored")
    continue
except NotImplementedError:
    self.log_error("Functionality not implemented")
    sys.exit(NOT_IMPLEMENTED_ERROR)
```

### 4. Stop Event Checking

Always check stop event in loops:

```python
while not self.task_stopping_event.is_set():
    for lport in self.port_dict:
        if self.task_stopping_event.is_set():
            break
        # Process port...
```

### 5. Thread-Safe Logging

Use thread-specific loggers:

```python
# xcvrd_utilities/common.py
def get_helper_logger():
    """Get a thread-specific logger"""
    thread_id = threading.current_thread().ident
    if thread_id not in thread_loggers:
        thread_loggers[thread_id] = syslogger.SysLogger(
            get_syslog_identifier_common(),
            enable_runtime_config=True
        )
    return thread_loggers[thread_id]
```

---

## Testing

### Running Unit Tests

```bash
cd sonic-xcvrd
pytest tests/test_xcvrd.py -v

# With coverage
pytest tests/test_xcvrd.py -v --cov=xcvrd --cov-report=html
```

### Key Test Areas

```python
# tests/test_xcvrd.py

# Media settings parsing
def test_load_media_settings()
def test_get_media_settings_key()
def test_get_media_settings_for_speed()

# Optics SI settings
def test_load_optics_si_settings()
def test_fetch_optics_si_setting()

# Port event handling
def test_port_change_event()
def test_port_mapping()

# DOM updates
def test_post_port_dom_info_to_db()

# Error handling
def test_sfp_error_status()
```

### Mock Objects

```python
# tests/mock_swsscommon.py
class Table:
    def __init__(self, db, table_name): ...
    def set(self, key, fvp): ...
    def get(self, key): ...
    def hget(self, key, field): ...

# tests/mock_platform.py
class MockSfp:
    def get_presence(self): ...
    def get_transceiver_info(self): ...
    def get_xcvr_api(self): ...
```

---

## Common Issues & Troubleshooting

### Issue: CMIS Module Stuck in INSERTED State

**Symptoms:**
- `cmis_state` remains `INSERTED` in `TRANSCEIVER_STATUS_SW`
- Module is physically present

**Debugging Steps:**
1. Check if `host_tx_ready` is `true` in STATE_DB PORT_TABLE
2. Check if `admin_status` is `up` in CONFIG_DB PORT
3. Verify module is CMIS type (QSFP-DD, OSFP)
4. Check for errors in syslog: `grep "CMIS:" /var/log/syslog`

**Common Causes:**
- NPU not ready (`host_tx_ready` is `false`)
- Port administratively down
- Invalid speed/lane configuration

### Issue: DOM Values Not Updating

**Symptoms:**
- `TRANSCEIVER_DOM_SENSOR` table has stale values

**Debugging Steps:**
1. Check `dom_polling` in CONFIG_DB PORT: `redis-cli -n 4 hget "PORT|Ethernet0" dom_polling`
2. Verify CMIS state is READY (DOM skipped during init)
3. Check module presence
4. Look for exceptions: `grep "DomInfoUpdateTask" /var/log/syslog`

### Issue: Module Shows Error Status

**Symptoms:**
- `error` field in `TRANSCEIVER_STATUS_SW` is not `N/A`

**Debugging Steps:**
1. Read error description: `redis-cli -n 6 hget "TRANSCEIVER_STATUS_SW|Ethernet0" error`
2. Check error bit mask interpretation in `sfp_status_helper.py`
3. Common errors:
   - `SFP_ERRORS_BLOCKING_MASK (0x02)`: Blocks EEPROM reading
   - Check platform-specific error handler

### Issue: SFF TX Not Enabling

**Symptoms:**
- Module inserted but TX remains disabled
- Link stays down

**Debugging Steps:**
1. Verify SffManagerTask is enabled (`--enable_sff_mgr`)
2. Check `host_tx_ready`: `redis-cli -n 6 hget "PORT_TABLE|Ethernet0" host_tx_ready`
3. Check `admin_status`: `redis-cli -n 4 hget "PORT|Ethernet0" admin_status`
4. Verify module type is QSFP28 or QSFP+ (SFF only handles these)
5. Check if module is copper (TX control skipped for copper)

### Issue: Application Code Mismatch

**Symptoms:**
- CMIS module not working at expected speed
- Different lanes show different applications

**Debugging Steps:**
```python
# Check application per lane
api = chassis.get_sfp(0).get_xcvr_api()
for lane in range(8):
    print(f"Lane {lane}: app={api.get_application(lane)}")

# Check desired application
api.get_application_advertisement()
```

---

## Quick Reference

### Key File Locations

| File | Purpose |
|------|---------|
| `xcvrd/xcvrd.py` | Main daemon, CmisManagerTask, SfpStateUpdateTask |
| `xcvrd/sff_mgr.py` | SffManagerTask |
| `xcvrd/dom/dom_mgr.py` | DomInfoUpdateTask |
| `xcvrd/xcvrd_utilities/common.py` | Shared utility functions |
| `xcvrd/xcvrd_utilities/xcvr_table_helper.py` | Database table abstraction |
| `xcvrd/xcvrd_utilities/port_event_helper.py` | Port change event handling |

### Important Constants

```python
# Timing
SFP_INSERT_EVENT_POLL_PERIOD_MSECS = 1000   # SFP polling interval
STATE_MACHINE_UPDATE_PERIOD_MSECS = 60000   # CMIS state machine interval
DOM_INFO_UPDATE_PERIOD_SECS = 60            # DOM update interval
MGMT_INIT_TIME_DELAY_SECS = 2               # SFP insert soak time

# Retries
CMIS_MAX_RETRIES = 3                        # Max CMIS init retries
RETRY_TIMES_FOR_SYSTEM_READY = 24           # System ready retries

# Lane masks
CMIS_MAX_HOST_LANES = 8
ALL_LANES_MASK = 0xff
```

### State Machine States Summary

| State | Description | Next State |
|-------|-------------|------------|
| UNKNOWN | Not processed yet | INSERTED |
| INSERTED | Module detected | DP_PRE_INIT_CHECK or READY |
| DP_PRE_INIT_CHECK | Pre-init validation | DP_DEINIT or READY |
| DP_DEINIT | Deinitializing DataPath | AP_CONF |
| AP_CONF | Configuring application | DP_INIT |
| DP_INIT | Initializing DataPath | DP_ACTIVATE |
| DP_ACTIVATE | Activating DataPath | DP_TXON |
| DP_TXON | Enabling TX | READY |
| READY | Fully initialized | (terminal) |
| FAILED | Initialization failed | (terminal) |
| REMOVED | Module removed | (terminal) |

---

## Contributing

When making changes to xcvrd:

1. **Unit Tests**: Add/update tests in `tests/test_xcvrd.py`
2. **Logging**: Use appropriate log levels (debug/info/notice/warning/error)
3. **Multi-ASIC**: Ensure changes work in multi-ASIC environments
4. **Backward Compatibility**: Support both chassis API and legacy sfputil
5. **Error Handling**: Handle NotImplementedError for optional platform APIs
6. **Documentation**: Update this guide for significant changes

---

*Last updated: January 2026*
