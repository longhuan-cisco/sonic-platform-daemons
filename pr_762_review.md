## PR #762 Review: Refactor media-settings parser

### Summary
This PR by @prgeor refactors the media settings parser in `media_settings_parser.py` by introducing an ABC-based class hierarchy (`MediaSettingsParserBase`, `GlobalMediaSettingsParser`, `PortMediaSettingsParser`, `CustomMediaSettingsParser`). It also moves `is_copper()` from `media_settings_parser.py` to `common.py` and adds a new test fixture for port-overrides-global-default scenarios.

**Files changed** (against upstream master):
- `sonic-xcvrd/xcvrd/xcvrd_utilities/media_settings_parser.py` (+122, -88)
- `sonic-xcvrd/xcvrd/xcvrd_utilities/common.py` (+9)
- `sonic-xcvrd/tests/test_xcvrd.py` (+31, -4 substantive)

---

### Critical Issues

#### 1. `if result:` vs `is not None` — Behavioral Regression

**File:** `media_settings_parser.py`, `get_media_settings_value()` (lines ~309-331 in PR)

The refactored code uses truthiness checks (`if result:`) on the return value from `parse()`, but the original code used `is not None`. This is a meaningful behavioral change:

**Scenario:** Vendor key matches in `GLOBAL_MEDIA_SETTINGS`, but lane speed does NOT match. `get_media_settings_for_speed()` returns `{}`.

- **Old behavior:** `get_media_settings()` returns `{}`. The check `if media_settings is not None` evaluates True for `{}`, so `{}` is returned immediately — meaning "vendor matched, but no speed-specific settings exist; stop searching."
- **New behavior:** `GlobalMediaSettingsParser.parse()` internally checks `if media_settings is not None` (correct), returns `({}, {})`. But then `get_media_settings_value()` checks `if result:` — which is `False` for `{}` — so it **falls through** to `PORT_MEDIA_SETTINGS` and potentially returns different settings.

This changes the priority semantics: the old code treated "vendor matched with no speed data" as a definitive answer (`{}`), while the new code treats it as "no match, keep looking." This is the exact concern @longhuan-cisco raised.

**Recommendation:** Either:
- (a) Use an explicit match flag in the return tuple: `(matched: bool, result: dict, default: dict)`, or
- (b) Change `if result:` to `if result is not None:` and have `parse()` return `None` for "no match" vs `{}` for "matched but empty", or
- (c) Document this as an intentional change in priority semantics and add test coverage for the edge case.

#### 2. Missing Log Message

**File:** `media_settings_parser.py`

The original code logged:
```python
helper_logger.log_notice("No values for physical port '{}'".format(physical_port))
```
when a port was present in `PORT_MEDIA_SETTINGS` but had no entry and no global default existed. This diagnostic log was removed in the refactor. While not a correctness issue, operators may rely on this log for debugging missing SI settings.

**Recommendation:** Restore the log in `get_media_settings_value()` when all parsers return empty results.

---

### Positive Changes

#### 1. Stale `media_dict` Bug Fix
The old code did **not** reset `media_dict` per iteration in the `GLOBAL_MEDIA_SETTINGS` loop:
```python
# OLD — media_dict persists across iterations
for keys in g_dict[GLOBAL_MEDIA_SETTINGS_KEY]:
    # conditions set media_dict but never reset it
    media_settings = get_media_settings(key, media_dict)  # stale from prior iteration!
```
The new `GlobalMediaSettingsParser.parse()` correctly resets `media_dict = {}` at the top of each iteration. This is a real bug fix (also flagged by Copilot), though it should be called out explicitly in the PR description.

#### 2. Cleaner Priority Documentation
The new code clearly documents the intended resolution order:
```python
# Priority order:
#   1. GLOBAL explicit match
#   2. PORT explicit match
#   3. PORT Default
#   4. CUSTOM explicit match
#   5. GLOBAL Default (last-resort fallback)
```
This is much easier to reason about than the old interleaved logic.

#### 3. `is_copper()` Relocation
Moving `is_copper()` to `common.py` is appropriate — it's a platform utility, not parser-specific. The implementation is functionally equivalent (uses `common.platform_chassis` instead of `xcvrd.platform_chassis`, both initialized from the same chassis object).

---

### Minor Issues

#### 4. Parser Objects Instantiated Per Call
`get_media_settings_value()` creates `GlobalMediaSettingsParser()`, `PortMediaSettingsParser()`, and `CustomMediaSettingsParser()` on every invocation. These classes are stateless — consider making them module-level singletons:
```python
_global_parser = GlobalMediaSettingsParser()
_port_parser = PortMediaSettingsParser()
_custom_parser = CustomMediaSettingsParser()
```

#### 5. `CustomMediaSettingsParser` Is Dead Code
It always returns `({}, {})` and `CUSTOM_MEDIA_SETTINGS_KEY` doesn't exist in any known `media_settings.json`. Adding a placeholder parser with a TODO and wiring it into the resolution chain adds unreachable code paths. Consider adding it when the feature is actually implemented.

#### 6. Missing Test Coverage for the `{}` Edge Case
The new test fixture `media_settings_port_overrides_global_default` tests the happy path (vendor match in PORT overrides GLOBAL default) but doesn't test the case where vendor matches in GLOBAL with a **wrong** lane speed. This is exactly the scenario where the truthiness-vs-`is not None` change matters. A test like this would catch it:
```python
# GLOBAL has vendor match at speed:400GAUI-8, PORT has Default
# Key requests speed:100GAUI-2 — vendor matches in GLOBAL, speed doesn't
# Old behavior: return {} (stop). New behavior: fall through to PORT Default.
```

---

### Verdict

The refactoring direction (modular parsers with clear priority) is sound and the stale `media_dict` bug fix is valuable. However, **Issue #1 (truthiness vs `is not None`)** is a behavioral regression that should be resolved before merging, either by fixing the check semantics or by explicitly documenting and testing the new priority behavior. Issue #2 (missing log) is a minor regression that should also be addressed.
