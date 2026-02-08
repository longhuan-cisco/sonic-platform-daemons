# PR Review: CMIS Decommission Per-Logical-Port Refactor

**Files changed**: `xcvrd.py` (+257/-95), `port_event_helper.py` (+52/-40)

**Commits**:
1. `f7377f2` — Enhance decommission logic on per-logical-port basis
2. `1cb0285` — Add `check_active_application`
3. `53cdd5e` — Fix media lanes mask for decomm
4. `7683b91` — Fix del-set events matching and avoid unnecessary `force_cmis_reinit`

---

## Medium Severity

### 1. Mutable default argument in `get_decomm_pending_host_lanes_mask`

**File**: `xcvrd.py:801`

```python
def get_decomm_pending_host_lanes_mask(self, lport, exclude_lports=[]):
```

Python evaluates default argument expressions once at function definition time, not at
each call. The resulting `[]` object is stored on the function object itself
(`CmisManagerTask.get_decomm_pending_host_lanes_mask.__defaults__`). Every call that
omits `exclude_lports` receives the same shared list object.

If any code path mutates this list (e.g. `.append()`), the mutation persists across
calls:

```python
>>> def f(x=[]):
...     x.append(1)
...     return x
>>> f()
[1]
>>> f()
[1, 1]       # same list object, accumulated state
>>> f()
[1, 1, 1]
```

Currently safe because all callers pass `[lport]` explicitly, but this is a latent bug.

**Fix**:
```python
def get_decomm_pending_host_lanes_mask(self, lport, exclude_lports=None):
    if exclude_lports is None:
        exclude_lports = []
```

---

### 2. `get_data_path_mask` media lane calculation assumes sequential mapping

**File**: `xcvrd.py:853-890`

**What commit `53cdd5e` fixed**: Before this commit, `get_host_lanes_mask_requiring_decomm`
returned only a host lanes mask. The `set_decomm_pending` caller then did:

```python
# BEFORE (commit f7377f2):
self.port_dict[lport]['media_lanes_mask'] = lanes_mask_requiring_decomm
# BUG: using host mask as media mask
```

Host and media lane counts can differ. Commit `53cdd5e` introduced `get_data_path_mask`
to return both host and media masks separately.

**Remaining concern**: The media mask calculation still assumes media lanes are allocated
sequentially, `media_lane_count` lanes per data path, in order:

```python
media_start_lane_idx = data_path_idx * media_lane_count
return host_lanes_mask, ((1 << media_lane_count) - 1) << media_start_lane_idx
```

The code reads `media_lane_assignment_options` for the validity check (line 874) but does
not use it to calculate the actual media lane offset. Instead it derives the media lane
position from `data_path_idx * media_lane_count`.

**Example where it works** (QSFP-DD with two 4x100G data paths):

```
App 1: host_lane_count=4, media_lane_count=4,
       host_lane_assignment_options=0b00010001 (start at lane 0 or lane 4)
       media_lane_assignment_options=0b00010001

DP0: host lanes 0-3 (app 1), DP1: host lanes 4-7 (app 1)

get_data_path_mask(app_advt, app=1, lane_idx=5):
  start=0: host_lanes_mask=0x0F, no overlap with lane 5, data_path_idx -> 1
  start=4: host_lanes_mask=0xF0, overlaps lane 5
    media_start_lane_idx = 1 * 4 = 4
    media_mask = 0xF0
    Returns (0xF0, 0xF0)  -- Correct
```

**Example where it could be wrong** (hypothetical non-contiguous media lane assignment):

```
App 2: host_lane_count=2, media_lane_count=1,
       host_lane_assignment_options=0b01010101 (can start at 0, 2, 4, or 6)
       media_lane_assignment_options=0b00001010 (media lanes 1 and 3 only, NOT 0 and 1)

DP0: host lanes 0-1, should map to media lane 1
DP1: host lanes 2-3, should map to media lane 3

get_data_path_mask(app_advt, app=2, lane_idx=2):
  start=0: host_mask=0x03, no overlap with lane 2, data_path_idx=1
  start=2: host_mask=0x0C, overlaps lane 2
    media_start_lane_idx = 1 * 1 = 1    <- sequential: picks media lane 1
    media_mask = 0b0010
    Returns (0x0C, 0b0010)  -- Wrong: should be media lane 3 (0b1000)
```

To be fully correct, the code would need to iterate `media_lane_assignment_options` in
parallel to find the actual media start lane for each data path, rather than computing it
as `data_path_idx * media_lane_count`.

In practice, the sequential assumption holds for the vast majority of CMIS modules in the
field today, so this is a correctness vs. practicality tradeoff.

---

## Low Severity

### 3. `set_decomm_pending` name is misleading for its dual purpose

**File**: `xcvrd.py:722-788`

The method does three things:
1. Checks if decommission is needed (reads Active Control Set)
2. Sets up decomm state if needed (writes `decomm_pending_dict`, overwrites `port_dict`
   lane masks)
3. Returns whether the caller should skip further processing

The name `set_decomm_pending` implies it always sets something, but it often returns
`False` without modifying state (the "decomm not needed" path).

**Suggested name**: `configure_decomm_if_required`

- `configure` communicates it may set up state (not just check)
- `if_required` communicates it is conditional and may do nothing
- Parallels existing naming like `is_cmis_application_update_required`

---

### 4. `check_active_application` makes a redundant I2C call

**File**: `xcvrd.py:1099-1123`

`check_active_application` calls `api.get_active_apsel_hostlane()` again, even though
this was already called in `get_host_lanes_mask_requiring_decomm` earlier in the same
state machine iteration. For the non-decomm path (CMIS_STATE_DP_INIT -> check config ->
check active app), this is a new I2C read that was not there before.

Consider caching the result across the state machine iteration or passing the
`active_app_dict` as a parameter if I2C overhead is a concern.

---

### 5. `force_cmis_reinit` retry count preserved after decomm

**File**: `xcvrd.py:1660`

When decomm completes, the old code called `self.force_cmis_reinit(lport)` (retries=0,
a fresh start). The new code passes `self.force_cmis_reinit(lport, retries)`, preserving
the retry count from the decomm phase.

This means if decomm took several retries, the subsequent normal CMIS initialization
starts with an elevated retry count and may hit `CMIS_MAX_RETRIES` sooner. Verify this
is intentional.

---

### 6. Port event cache: 3+ events edge case

**File**: `port_event_helper.py:160-180`

The new `deque(maxlen=2)` approach correctly handles DEL->SET sequences. A subtle edge
case exists when 3+ events arrive in one select cycle for the same
`(port_name, db_name, table_name)`:

- DEL, SET, DEL -> deque keeps [SET, DEL] -> popleft fires (SET+DEL only keeps DEL),
  first DEL is lost.

In practice this is unlikely to matter (a rapid DEL->SET->DEL probably only needs the
final DEL), but worth noting.

---

## Trivial

### 7. Extra blank line in `get_active_application_list`

**File**: `xcvrd.py:1066`

```python
def get_active_application_list(self, active_app_dict):

    """
```

Extra blank line between the method definition and docstring.
