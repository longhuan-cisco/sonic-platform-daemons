# Why Traditional And Custom Media Settings Use Separate Lookup Paths V2

## Document Scope

This note describes the design choice to keep:

- `get_traditional_media_settings_value()` for traditional media settings
- `get_custom_media_settings_value()` for custom media settings
- `resolve_media_settings_for_db()` as the place where both are merged into the final APP_DB payload

instead of trying to make one single low-level getter cover all media settings handling.

## Code Version

- Repository: `https://github.com/longhuan-cisco/sonic-platform-daemons.git`
- Branch: `support_custom_attrs_after_base_refactor`
- Commit: `093bf91f0903a8f79a1f1aee77009b40208401ef`
- Component: `sonic-xcvrd/xcvrd/xcvrd_utilities/media_settings_parser.py`

## Current Main Flow

The current flow has three layers:

1. Raw traditional lookup
2. Raw custom lookup
3. Final APP_DB payload construction

In code, that looks like:

```python
traditional = get_traditional_media_settings_value(physical_port, key)
custom = get_custom_media_settings_value(physical_port, key)

payload = MediaSettingsParserBase.to_db_value(
    traditional, lane_count, subport_num
)

custom_db_value = CustomMediaSettingsParser.to_db_value(
    custom, lane_count, subport_num
)
if custom_db_value is not None:
    payload[CUSTOM_SERDES_ATTRS_KEY_IN_DB] = custom_db_value
```

`resolve_media_settings_for_db()` is the function that orchestrates this.

## Why The Flow Is Split This Way

Traditional and custom settings can both match the same port, but they are not the same kind of result.

Traditional settings return normal SerDes attributes:

```python
{
    "main": {"lane0": "0x11", "lane1": "0x12", "lane2": "0x13", "lane3": "0x14"},
    "pre1": {"lane0": "0x01", "lane1": "0x02", "lane2": "0x03", "lane3": "0x04"},
}
```

Custom settings return `CUSTOM:` attributes:

```python
{
    "CUSTOM:ABC": {"lane0": 1, "lane1": 2, "lane2": 3, "lane3": 4},
    "CUSTOM:XYZ": {"lane0": "ADAPTIVE", "lane1": "ADAPTIVE", "lane2": "ADAPTIVE", "lane3": "ADAPTIVE"},
}
```

These two kinds of data serialize differently in APP_DB:

```python
{
    "main": "0x13,0x14",
    "pre1": "0x03,0x04",
    "custom_serdes_attrs": "{\"attributes\":[{\"ABC\":{\"value\":[3,4]}},{\"XYZ\":{\"value\":[\"ADAPTIVE\",\"ADAPTIVE\"]}}]}",
}
```

That is why the design keeps:

- raw traditional lookup separate
- raw custom lookup separate
- final merge and serialization explicit

## Traditional Lookup

`get_traditional_media_settings_value()` resolves only:

- `GLOBAL_MEDIA_SETTINGS`
- `PORT_MEDIA_SETTINGS`

Its precedence is:

1. GLOBAL explicit match
2. PORT explicit match
3. PORT default
4. GLOBAL default

It returns the raw matched dictionary before lane slicing and before DB serialization.

Example:

```python
{
    "main": {"lane0": "0x11", "lane1": "0x12", "lane2": "0x13", "lane3": "0x14"}
}
```

## Custom Lookup

`get_custom_media_settings_value()` resolves only:

- `CUSTOM_MEDIA_SETTINGS`

It also returns the raw matched dictionary before DB serialization.

Example:

```python
{
    "CUSTOM:ABC": {"lane0": 1, "lane1": 2, "lane2": 3, "lane3": 4}
}
```

The custom parser owns the custom-only behavior:

- port selector matching
- custom default fallback
- custom APP_DB serialization

## Serialization Split

The serialization boundary is now also explicit:

- `MediaSettingsParserBase.to_db_value()` handles traditional settings
- `CustomMediaSettingsParser.to_db_value()` handles custom settings

This is useful because:

- the traditional serializer is shared and centralized
- the custom serializer stays near the custom parsing logic
- `resolve_media_settings_for_db()` mostly coordinates instead of open-coding conversions

## Why Not Collapse Everything Into One Getter

One single getter usually leads to one of two outcomes.

### Mixed Raw Dictionary

```python
{
    "main": {"lane0": "0x11", "lane1": "0x12"},
    "CUSTOM:ABC": {"lane0": 1, "lane1": 2},
}
```

This still forces the caller to branch on key type, so it does not actually simplify the model.

### Final DB Payload

```python
{
    "main": "0x11,0x12",
    "custom_serdes_attrs": "{\"attributes\":[...]}"
}
```

This is cleaner for the caller, but then the getter is no longer a raw lookup helper. It becomes a full payload builder that owns:

- config lookup
- precedence
- lane slicing
- traditional serialization
- custom aggregation

That makes the name and abstraction less clear.

## Recommended Mental Model

The cleanest way to read the current implementation is:

- `get_traditional_media_settings_value()` finds the traditional raw match
- `get_custom_media_settings_value()` finds the custom raw match
- `resolve_media_settings_for_db()` builds the final APP_DB payload

So the code is centralized at the flow level, but not flattened at the logic level.

That is the main design tradeoff preserved by the current implementation.
