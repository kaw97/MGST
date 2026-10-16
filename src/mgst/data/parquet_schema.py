"""Dynamic schema discovery and flattening for Parquet conversion.

Provides bidirectional conversion between nested system/body/station dicts
(as stored in JSONL) and flat dicts suitable for Parquet tables.

The table structure (systems, bodies, stations) is fixed by design,
but columns within each table are discovered dynamically from the data.
"""

import json
import logging
from collections import defaultdict
from typing import Any, Dict, List, Optional, Set, Tuple

import pyarrow as pa

logger = logging.getLogger(__name__)

# Prefixes used for flattened nested fields, enabling deterministic unflattening
_MATERIAL_PREFIX = "mat_"
_ATMOSPHERE_PREFIX = "atm_"
_SOLID_PREFIX = "solid_"
_PARENT_PREFIX = "parent_"
_RING_PREFIX = "ring_"
_BELT_PREFIX = "belt_"
_SIGNAL_PREFIX = "sig_"
_COORD_FIELDS = ("x", "y", "z")

# Fields that are nested structures requiring special flattening
_SYSTEM_NESTED_FIELDS = {"coords", "bodies", "stations", "controllingFaction",
                         "factions", "timestamps", "powers",
                         "powerConflictProgress", "thargoidWar"}
_BODY_NESTED_FIELDS = {"materials", "atmosphereComposition", "solidComposition",
                       "parents", "rings", "belts", "signals", "stations"}
# timestamps is stored as JSON string on both systems and bodies

# Maximum slot counts for array-of-struct fields (from data analysis)
MAX_PARENTS = 7
MAX_RINGS = 3
MAX_BELTS = 3

# Ring/belt struct field names (core fields always present)
_RING_CORE_FIELDS = ("name", "type", "mass", "innerRadius", "outerRadius")
# Rings can also have id64 and signals (with mining commodities)
_RING_EXTRA_FIELDS = ("id64",)
# Ring signals are stored as a JSON string per ring slot
_RING_ALL_FIELDS = _RING_CORE_FIELDS + _RING_EXTRA_FIELDS


def _infer_pyarrow_type(value: Any) -> Optional[pa.DataType]:
    """Infer a PyArrow type from a Python value."""
    if value is None:
        return None
    if isinstance(value, bool):
        return pa.bool_()
    if isinstance(value, int):
        return pa.int64()
    if isinstance(value, float):
        return pa.float64()
    if isinstance(value, str):
        return pa.string()
    return pa.string()  # Fallback: serialize as string


def _sanitize_col_name(name: str) -> str:
    """Sanitize a string for use as a Parquet column name."""
    return name.replace(" ", "_").replace("-", "_")


class SchemaDiscoverer:
    """Discovers Parquet schema dynamically from system data.

    Tracks all encountered field names and infers PyArrow types.
    No hardcoded column lists - schema is built entirely from observed data.
    """

    def __init__(self) -> None:
        # Maps column_name -> pyarrow type
        self.system_columns: Dict[str, pa.DataType] = {}
        self.body_columns: Dict[str, pa.DataType] = {}
        self.station_columns: Dict[str, pa.DataType] = {}
        # Track discovered nested key sets
        self._material_keys: Set[str] = set()
        self._atmosphere_keys: Set[str] = set()
        self._solid_keys: Set[str] = set()
        self._signal_keys: Set[str] = set()
        self._systems_observed = 0

    def observe_system(self, system: dict) -> None:
        """Observe a system dict and update known columns."""
        self._systems_observed += 1

        # System-level fields
        for key, value in system.items():
            if key in _SYSTEM_NESTED_FIELDS:
                if key == "timestamps":
                    col = "timestamps_json"
                    if col not in self.system_columns:
                        self.system_columns[col] = pa.string()
                continue
            col_name = key
            if col_name not in self.system_columns:
                pa_type = _infer_pyarrow_type(value)
                if pa_type is not None:
                    self.system_columns[col_name] = pa_type

        # Coords
        coords = system.get("coords")
        if coords:
            for c in _COORD_FIELDS:
                if c not in self.system_columns:
                    self.system_columns[c] = pa.float64()

        # controllingFaction
        cf = system.get("controllingFaction")
        if isinstance(cf, dict):
            for cf_key, cf_val in cf.items():
                if isinstance(cf_val, (list, dict)):
                    col = f"controllingFaction_{cf_key}_json"
                    if col not in self.system_columns:
                        self.system_columns[col] = pa.string()
                else:
                    col = f"controllingFaction_{cf_key}"
                    if col not in self.system_columns:
                        pa_type = _infer_pyarrow_type(cf_val)
                        if pa_type is not None:
                            self.system_columns[col] = pa_type

        # Complex system fields stored as JSON strings
        for json_field in ("factions", "powers", "powerConflictProgress"):
            if system.get(json_field):
                col = f"{json_field}_json"
                if col not in self.system_columns:
                    self.system_columns[col] = pa.string()

        if isinstance(system.get("thargoidWar"), dict):
            col = "thargoidWar_json"
            if col not in self.system_columns:
                self.system_columns[col] = pa.string()

        # Bodies
        for body in system.get("bodies", []):
            self._observe_body(body)

        # Stations
        for station in system.get("stations", []):
            self._observe_station(station)

    def _observe_body(self, body: dict) -> None:
        """Observe a body dict and update body columns."""
        for key, value in body.items():
            if key in _BODY_NESTED_FIELDS:
                continue
            if key == "timestamps":
                col = "timestamps_json"
                if col not in self.body_columns:
                    self.body_columns[col] = pa.string()
                continue
            if key == "stations" and isinstance(value, list) and value:
                col = "body_stations_json"
                if col not in self.body_columns:
                    self.body_columns[col] = pa.string()
                continue
            if key not in self.body_columns:
                pa_type = _infer_pyarrow_type(value)
                if pa_type is not None:
                    self.body_columns[key] = pa_type

        # Ensure system_id64 FK is tracked
        if "system_id64" not in self.body_columns:
            self.body_columns["system_id64"] = pa.int64()

        # Materials
        materials = body.get("materials")
        if isinstance(materials, dict):
            for mat_key in materials:
                san = _sanitize_col_name(mat_key)
                col = f"{_MATERIAL_PREFIX}{san}"
                self._material_keys.add(mat_key)
                if col not in self.body_columns:
                    self.body_columns[col] = pa.float64()

        # Atmosphere composition
        atm = body.get("atmosphereComposition")
        if isinstance(atm, dict):
            for atm_key in atm:
                san = _sanitize_col_name(atm_key)
                col = f"{_ATMOSPHERE_PREFIX}{san}"
                self._atmosphere_keys.add(atm_key)
                if col not in self.body_columns:
                    self.body_columns[col] = pa.float64()

        # Solid composition
        solid = body.get("solidComposition")
        if isinstance(solid, dict):
            for sol_key in solid:
                san = _sanitize_col_name(sol_key)
                col = f"{_SOLID_PREFIX}{san}"
                self._solid_keys.add(sol_key)
                if col not in self.body_columns:
                    self.body_columns[col] = pa.float64()

        # Parents (slot columns)
        parents = body.get("parents")
        if isinstance(parents, list):
            for idx, parent in enumerate(parents[:MAX_PARENTS]):
                type_col = f"{_PARENT_PREFIX}{idx}_type"
                id_col = f"{_PARENT_PREFIX}{idx}_id"
                if type_col not in self.body_columns:
                    self.body_columns[type_col] = pa.string()
                if id_col not in self.body_columns:
                    self.body_columns[id_col] = pa.int64()

        # Rings (slot columns)
        rings = body.get("rings")
        if isinstance(rings, list):
            for idx, ring in enumerate(rings[:MAX_RINGS]):
                for field in _RING_ALL_FIELDS:
                    col = f"{_RING_PREFIX}{idx}_{field}"
                    if col not in self.body_columns:
                        self.body_columns[col] = _infer_pyarrow_type(
                            ring.get(field)
                        ) or pa.string()
                # Ring signals stored as JSON string
                if "signals" in ring:
                    col = f"{_RING_PREFIX}{idx}_signals_json"
                    if col not in self.body_columns:
                        self.body_columns[col] = pa.string()

        # Belts (slot columns)
        belts = body.get("belts")
        if isinstance(belts, list):
            for idx, belt in enumerate(belts[:MAX_BELTS]):
                for field in _RING_ALL_FIELDS:
                    col = f"{_BELT_PREFIX}{idx}_{field}"
                    if col not in self.body_columns:
                        self.body_columns[col] = _infer_pyarrow_type(
                            belt.get(field)
                        ) or pa.string()
                if "signals" in belt:
                    col = f"{_BELT_PREFIX}{idx}_signals_json"
                    if col not in self.body_columns:
                        self.body_columns[col] = pa.string()

        # Signals
        signals = body.get("signals")
        if isinstance(signals, dict):
            sig_data = signals.get("signals", {})
            if isinstance(sig_data, dict):
                for sig_key in sig_data:
                    san = _sanitize_col_name(sig_key)
                    col = f"{_SIGNAL_PREFIX}{san}"
                    self._signal_keys.add(sig_key)
                    if col not in self.body_columns:
                        self.body_columns[col] = pa.int64()
            # signals updateTime
            if "updateTime" in signals:
                col = "signals_updateTime"
                if col not in self.body_columns:
                    self.body_columns[col] = pa.string()
            # signals genuses
            if "genuses" in signals:
                col = "signals_genuses"
                if col not in self.body_columns:
                    self.body_columns[col] = pa.string()  # stored as JSON string

    def _observe_station(self, station: dict) -> None:
        """Observe a station dict and update station columns."""
        if "system_id64" not in self.station_columns:
            self.station_columns["system_id64"] = pa.int64()

        for key, value in station.items():
            if key in ("market", "economies", "services", "landingPads",
                       "controllingFaction", "outfitting", "shipyard"):
                continue
            col = key
            if col not in self.station_columns:
                pa_type = _infer_pyarrow_type(value)
                if pa_type is not None:
                    self.station_columns[col] = pa_type

        # Landing pads
        pads = station.get("landingPads")
        if isinstance(pads, dict):
            for pad_key in ("large", "medium", "small"):
                col = f"landingPads_{pad_key}"
                if col not in self.station_columns:
                    self.station_columns[col] = pa.int64()

        # Economies - store as JSON string
        if "economies" in station:
            col = "economies_json"
            if col not in self.station_columns:
                self.station_columns[col] = pa.string()

        # Services - store as JSON string
        if "services" in station:
            col = "services_json"
            if col not in self.station_columns:
                self.station_columns[col] = pa.string()

        # Market - store as JSON string (deeply nested, rarely queried)
        if "market" in station:
            col = "market_json"
            if col not in self.station_columns:
                self.station_columns[col] = pa.string()

        # Outfitting/Shipyard - store as JSON string
        for field in ("outfitting", "shipyard"):
            if field in station:
                col = f"{field}_json"
                if col not in self.station_columns:
                    self.station_columns[col] = pa.string()

    def get_system_schema(self) -> pa.Schema:
        """Build PyArrow schema for systems table."""
        fields = []
        for col_name, col_type in sorted(self.system_columns.items()):
            fields.append(pa.field(col_name, col_type, nullable=True))
        return pa.schema(fields)

    def get_body_schema(self) -> pa.Schema:
        """Build PyArrow schema for bodies table."""
        fields = []
        for col_name, col_type in sorted(self.body_columns.items()):
            fields.append(pa.field(col_name, col_type, nullable=True))
        return pa.schema(fields)

    def get_station_schema(self) -> pa.Schema:
        """Build PyArrow schema for stations table."""
        fields = []
        for col_name, col_type in sorted(self.station_columns.items()):
            fields.append(pa.field(col_name, col_type, nullable=True))
        return pa.schema(fields)

    @property
    def systems_observed(self) -> int:
        return self._systems_observed


# ---------------------------------------------------------------------------
# Flattening functions
# ---------------------------------------------------------------------------


def flatten_system(system: dict) -> dict:
    """Flatten a system dict to a flat row for systems.parquet.

    Extracts system-level fields, flattens coords, controllingFaction.
    Excludes bodies, stations, factions, timestamps arrays.
    """
    row: Dict[str, Any] = {}

    for key, value in system.items():
        if key in _SYSTEM_NESTED_FIELDS:
            if key == "timestamps":
                row["timestamps_json"] = json.dumps(value)
            elif key == "controllingFaction" and isinstance(value, dict):
                # Flatten controllingFaction into prefixed columns
                for cf_key, cf_val in value.items():
                    if isinstance(cf_val, (list, dict)):
                        # Complex sub-fields (activeStates, etc.) as JSON
                        row[f"controllingFaction_{cf_key}_json"] = json.dumps(cf_val)
                    else:
                        row[f"controllingFaction_{cf_key}"] = cf_val
            elif key == "factions" and value:
                row["factions_json"] = json.dumps(value)
            elif key == "powers" and value:
                row["powers_json"] = json.dumps(value)
            elif key == "powerConflictProgress" and value:
                row["powerConflictProgress_json"] = json.dumps(value)
            elif key == "thargoidWar" and isinstance(value, dict):
                row["thargoidWar_json"] = json.dumps(value)
            continue
        row[key] = value

    # Flatten coords
    coords = system.get("coords")
    if coords:
        row["x"] = float(coords.get("x", 0))
        row["y"] = float(coords.get("y", 0))
        row["z"] = float(coords.get("z", 0))

    return row


def flatten_body(body: dict, system_id64: int) -> dict:
    """Flatten a body dict to a flat row for bodies.parquet.

    Adds system_id64 FK. Flattens parents, rings, belts, materials,
    atmosphereComposition, solidComposition, signals into prefixed columns.
    """
    row: Dict[str, Any] = {"system_id64": system_id64}

    # Copy scalar fields
    for key, value in body.items():
        if key in _BODY_NESTED_FIELDS:
            if key == "timestamps":
                row["timestamps_json"] = json.dumps(value)
            elif key == "stations" and value:
                # Body-level stations (rare) stored as JSON string
                row["body_stations_json"] = json.dumps(value)
            continue
        row[key] = value

    # Flatten materials
    materials = body.get("materials")
    if isinstance(materials, dict):
        for mat_key, mat_val in materials.items():
            row[f"{_MATERIAL_PREFIX}{_sanitize_col_name(mat_key)}"] = mat_val

    # Flatten atmosphere composition
    atm = body.get("atmosphereComposition")
    if isinstance(atm, dict):
        for atm_key, atm_val in atm.items():
            row[f"{_ATMOSPHERE_PREFIX}{_sanitize_col_name(atm_key)}"] = atm_val

    # Flatten solid composition
    solid = body.get("solidComposition")
    if isinstance(solid, dict):
        for sol_key, sol_val in solid.items():
            row[f"{_SOLID_PREFIX}{_sanitize_col_name(sol_key)}"] = sol_val

    # Flatten parents into slots
    parents = body.get("parents")
    if isinstance(parents, list):
        for idx, parent in enumerate(parents[:MAX_PARENTS]):
            if isinstance(parent, dict):
                for p_type, p_id in parent.items():
                    row[f"{_PARENT_PREFIX}{idx}_type"] = p_type
                    row[f"{_PARENT_PREFIX}{idx}_id"] = p_id
                    break  # Each parent dict has exactly one key

    # Flatten rings into slots
    rings = body.get("rings")
    if isinstance(rings, list):
        for idx, ring in enumerate(rings[:MAX_RINGS]):
            for field in _RING_ALL_FIELDS:
                row[f"{_RING_PREFIX}{idx}_{field}"] = ring.get(field)
            # Ring signals as JSON string
            ring_signals = ring.get("signals")
            if ring_signals is not None:
                row[f"{_RING_PREFIX}{idx}_signals_json"] = json.dumps(ring_signals)

    # Flatten belts into slots
    belts = body.get("belts")
    if isinstance(belts, list):
        for idx, belt in enumerate(belts[:MAX_BELTS]):
            for field in _RING_ALL_FIELDS:
                row[f"{_BELT_PREFIX}{idx}_{field}"] = belt.get(field)
            belt_signals = belt.get("signals")
            if belt_signals is not None:
                row[f"{_BELT_PREFIX}{idx}_signals_json"] = json.dumps(belt_signals)

    # Flatten signals
    signals = body.get("signals")
    if isinstance(signals, dict):
        sig_data = signals.get("signals", {})
        if isinstance(sig_data, dict):
            for sig_key, sig_val in sig_data.items():
                row[f"{_SIGNAL_PREFIX}{_sanitize_col_name(sig_key)}"] = sig_val
        if "updateTime" in signals:
            row["signals_updateTime"] = signals["updateTime"]
        genuses = signals.get("genuses")
        if genuses is not None:
            row["signals_genuses"] = json.dumps(genuses)

    return row


def flatten_station(station: dict, system_id64: int) -> dict:
    """Flatten a station dict to a flat row for stations.parquet.

    Stores market, economies, services, outfitting, shipyard as JSON strings.
    """
    row: Dict[str, Any] = {"system_id64": system_id64}

    for key, value in station.items():
        if key in ("market", "economies", "services", "landingPads",
                   "controllingFaction", "outfitting", "shipyard"):
            continue
        row[key] = value

    # controllingFaction at station level is a simple string (faction name)
    cf = station.get("controllingFaction")
    if cf is not None:
        row["controllingFaction"] = cf

    # Flatten landing pads
    pads = station.get("landingPads")
    if isinstance(pads, dict):
        for pad_key in ("large", "medium", "small"):
            row[f"landingPads_{pad_key}"] = pads.get(pad_key)

    # Store complex nested structures as JSON strings
    economies = station.get("economies")
    if economies is not None:
        row["economies_json"] = json.dumps(economies)

    services = station.get("services")
    if services is not None:
        row["services_json"] = json.dumps(services)

    market = station.get("market")
    if market is not None:
        row["market_json"] = json.dumps(market)

    for field in ("outfitting", "shipyard"):
        val = station.get(field)
        if val is not None:
            row[f"{field}_json"] = json.dumps(val)

    return row


# ---------------------------------------------------------------------------
# Unflattening functions
# ---------------------------------------------------------------------------


def unflatten_body(flat: dict) -> dict:
    """Reconstruct a body dict from a flattened Parquet row.

    Reverses flatten_body(): reconstructs materials, atmosphereComposition,
    solidComposition, parents, rings, belts, signals from prefixed columns.
    """
    body: Dict[str, Any] = {}
    materials: Dict[str, float] = {}
    atmosphere: Dict[str, float] = {}
    solid: Dict[str, float] = {}
    parents_slots: Dict[int, Dict[str, Any]] = {}
    ring_slots: Dict[int, Dict[str, Any]] = {}
    ring_signals_json: Dict[int, str] = {}
    belt_slots: Dict[int, Dict[str, Any]] = {}
    belt_signals_json: Dict[int, str] = {}
    signal_data: Dict[str, int] = {}
    signals_update_time: Optional[str] = None
    signals_genuses: Optional[str] = None

    for key, value in flat.items():
        if key == "system_id64":
            continue  # FK, not part of the original body dict

        # For nested/prefixed fields, skip None (absent slot)
        if key.startswith(_MATERIAL_PREFIX):
            if value is not None:
                materials[_unsanitize_material_key(key[len(_MATERIAL_PREFIX):])] = value
        elif key.startswith(_ATMOSPHERE_PREFIX):
            if value is not None:
                atmosphere[_unsanitize_atm_key(key[len(_ATMOSPHERE_PREFIX):])] = value
        elif key.startswith(_SOLID_PREFIX):
            if value is not None:
                solid[_unsanitize_solid_key(key[len(_SOLID_PREFIX):])] = value
        elif key.startswith(_PARENT_PREFIX):
            if value is not None:
                rest = key[len(_PARENT_PREFIX):]
                idx_str, field = rest.split("_", 1)
                idx = int(idx_str)
                if idx not in parents_slots:
                    parents_slots[idx] = {}
                parents_slots[idx][field] = value
        elif key.startswith(_RING_PREFIX):
            if value is not None:
                rest = key[len(_RING_PREFIX):]
                idx_str, field = rest.split("_", 1)
                idx = int(idx_str)
                if field == "signals_json":
                    ring_signals_json[idx] = value
                else:
                    if idx not in ring_slots:
                        ring_slots[idx] = {}
                    ring_slots[idx][field] = value
        elif key.startswith(_BELT_PREFIX):
            if value is not None:
                rest = key[len(_BELT_PREFIX):]
                idx_str, field = rest.split("_", 1)
                idx = int(idx_str)
                if field == "signals_json":
                    belt_signals_json[idx] = value
                else:
                    if idx not in belt_slots:
                        belt_slots[idx] = {}
                    belt_slots[idx][field] = value
        elif key.startswith(_SIGNAL_PREFIX):
            if value is not None:
                sig_name = key[len(_SIGNAL_PREFIX):]
                signal_data[_unsanitize_signal_key(sig_name)] = value
        elif key == "signals_updateTime":
            if value is not None:
                signals_update_time = value
        elif key == "signals_genuses":
            if value is not None:
                signals_genuses = value
        elif key == "timestamps_json":
            if value is not None:
                body["timestamps"] = json.loads(value)
        elif key == "body_stations_json":
            if value is not None:
                body["stations"] = json.loads(value)
        else:
            # Scalar fields: preserve None values (they are legitimate nulls)
            body[key] = value

    # Reconstruct materials
    if materials:
        body["materials"] = materials

    # Reconstruct atmosphere composition
    if atmosphere:
        body["atmosphereComposition"] = atmosphere

    # Reconstruct solid composition
    if solid:
        body["solidComposition"] = solid

    # Reconstruct parents list
    if parents_slots:
        parents = []
        for idx in sorted(parents_slots.keys()):
            slot = parents_slots[idx]
            if "type" in slot and "id" in slot:
                parents.append({slot["type"]: slot["id"]})
        if parents:
            body["parents"] = parents

    # Reconstruct rings list
    all_ring_indices = set(ring_slots.keys()) | set(ring_signals_json.keys())
    if all_ring_indices:
        rings = []
        for idx in sorted(all_ring_indices):
            ring = ring_slots.get(idx, {})
            if idx in ring_signals_json:
                ring["signals"] = json.loads(ring_signals_json[idx])
            if ring:
                rings.append(ring)
        if rings:
            body["rings"] = rings

    # Reconstruct belts list
    all_belt_indices = set(belt_slots.keys()) | set(belt_signals_json.keys())
    if all_belt_indices:
        belts = []
        for idx in sorted(all_belt_indices):
            belt = belt_slots.get(idx, {})
            if idx in belt_signals_json:
                belt["signals"] = json.loads(belt_signals_json[idx])
            if belt:
                belts.append(belt)
        if belts:
            body["belts"] = belts

    # Reconstruct signals
    if signal_data or signals_update_time is not None:
        signals: Dict[str, Any] = {}
        if signal_data:
            signals["signals"] = signal_data
        if signals_update_time is not None:
            signals["updateTime"] = signals_update_time
        if signals_genuses is not None:
            signals["genuses"] = json.loads(signals_genuses)
        body["signals"] = signals

    # Reconstruct empty stations list (bodies always have this)
    if "stations" not in body:
        body["stations"] = []

    return body


def unflatten_system(flat: dict) -> dict:
    """Reconstruct system-level dict from a flattened Parquet row.

    Reverses flatten_system(): reconstructs coords, controllingFaction.
    Does NOT include bodies or stations (those come from separate tables).
    """
    system: Dict[str, Any] = {}
    cf_fields: Dict[str, Any] = {}

    for key, value in flat.items():
        if key in _COORD_FIELDS:
            continue  # handled below
        elif key.startswith("controllingFaction_"):
            if value is not None:
                suffix = key[len("controllingFaction_"):]
                if suffix.endswith("_json"):
                    # Complex sub-field stored as JSON
                    cf_fields[suffix[:-5]] = json.loads(value)
                else:
                    cf_fields[suffix] = value
        elif key == "timestamps_json":
            if value is not None:
                system["timestamps"] = json.loads(value)
        elif key == "factions_json":
            if value is not None:
                system["factions"] = json.loads(value)
        elif key == "powers_json":
            if value is not None:
                system["powers"] = json.loads(value)
        elif key == "powerConflictProgress_json":
            if value is not None:
                system["powerConflictProgress"] = json.loads(value)
        elif key == "thargoidWar_json":
            if value is not None:
                system["thargoidWar"] = json.loads(value)
        else:
            # Scalar fields: preserve None values (they are legitimate nulls)
            system[key] = value

    # Reconstruct coords
    if any(k in flat and flat[k] is not None for k in _COORD_FIELDS):
        system["coords"] = {
            "x": flat.get("x"),
            "y": flat.get("y"),
            "z": flat.get("z"),
        }

    # Reconstruct controllingFaction
    if cf_fields:
        system["controllingFaction"] = cf_fields

    return system


def unflatten_system_with_bodies(
    flat_system: dict,
    flat_bodies: List[dict],
    flat_stations: Optional[List[dict]] = None,
) -> dict:
    """Reconstruct a complete system dict from flat system + body + station rows.

    This produces a dict compatible with JSONPatternMatcher.matches().
    """
    system = unflatten_system(flat_system)
    system["bodies"] = [unflatten_body(fb) for fb in flat_bodies]
    if flat_stations:
        system["stations"] = [unflatten_station(fs) for fs in flat_stations]
    else:
        system["stations"] = []
    return system


def unflatten_station(flat: dict) -> dict:
    """Reconstruct a station dict from a flattened Parquet row."""
    station: Dict[str, Any] = {}
    pads: Dict[str, Any] = {}

    for key, value in flat.items():
        if key == "system_id64":
            continue
        elif key.startswith("landingPads_"):
            if value is not None:
                pad_key = key[len("landingPads_"):]
                pads[pad_key] = value
        elif key == "economies_json":
            if value is not None:
                station["economies"] = json.loads(value)
        elif key == "services_json":
            if value is not None:
                station["services"] = json.loads(value)
        elif key == "market_json":
            if value is not None:
                station["market"] = json.loads(value)
        elif key == "outfitting_json":
            if value is not None:
                station["outfitting"] = json.loads(value)
        elif key == "shipyard_json":
            if value is not None:
                station["shipyard"] = json.loads(value)
        else:
            # Scalar fields: preserve None values (they are legitimate nulls)
            station[key] = value

    if pads:
        station["landingPads"] = pads

    return station


# ---------------------------------------------------------------------------
# Key name sanitization/unsanitization
# ---------------------------------------------------------------------------

# Known material keys (spaces/hyphens to handle)
_KNOWN_MATERIALS = {
    "Antimony", "Arsenic", "Cadmium", "Carbon", "Chromium", "Germanium",
    "Iron", "Manganese", "Mercury", "Molybdenum", "Nickel", "Niobium",
    "Phosphorus", "Polonium", "Ruthenium", "Selenium", "Sulphur",
    "Technetium", "Tellurium", "Tin", "Tungsten", "Vanadium", "Yttrium",
    "Zinc", "Zirconium",
}
_SANITIZED_MATERIAL_MAP: Dict[str, str] = {
    _sanitize_col_name(k): k for k in _KNOWN_MATERIALS
}

_KNOWN_ATM_KEYS = {
    "Ammonia", "Argon", "Carbon dioxide", "Helium", "Hydrogen", "Iron",
    "Methane", "Neon", "Nitrogen", "Oxygen", "Silicates",
    "Sulphur dioxide", "Water",
}
_SANITIZED_ATM_MAP: Dict[str, str] = {
    _sanitize_col_name(k): k for k in _KNOWN_ATM_KEYS
}

_KNOWN_SOLID_KEYS = {"Ice", "Metal", "Rock"}
_SANITIZED_SOLID_MAP: Dict[str, str] = {
    _sanitize_col_name(k): k for k in _KNOWN_SOLID_KEYS
}

_KNOWN_SIGNAL_KEYS = {
    "$SAA_SignalType_Biological;",
    "$SAA_SignalType_Geological;",
    "$SAA_SignalType_Human;",
    "$SAA_SignalType_Thargoid;",
    "$SAA_SignalType_Guardian;",
    "$SAA_SignalType_Other;",
    "$SAA_SignalType_PlanetAnomaly;",
}
_SANITIZED_SIGNAL_MAP: Dict[str, str] = {
    _sanitize_col_name(k): k for k in _KNOWN_SIGNAL_KEYS
}


def _unsanitize_material_key(sanitized: str) -> str:
    """Convert sanitized material column suffix back to original key."""
    if sanitized in _SANITIZED_MATERIAL_MAP:
        return _SANITIZED_MATERIAL_MAP[sanitized]
    # Unknown material - reverse sanitization best-effort
    return sanitized.replace("_", " ")


def _unsanitize_atm_key(sanitized: str) -> str:
    """Convert sanitized atmosphere column suffix back to original key."""
    if sanitized in _SANITIZED_ATM_MAP:
        return _SANITIZED_ATM_MAP[sanitized]
    return sanitized.replace("_", " ")


def _unsanitize_solid_key(sanitized: str) -> str:
    """Convert sanitized solid composition column suffix back to original key."""
    if sanitized in _SANITIZED_SOLID_MAP:
        return _SANITIZED_SOLID_MAP[sanitized]
    return sanitized.replace("_", " ")


def _unsanitize_signal_key(sanitized: str) -> str:
    """Convert sanitized signal column suffix back to original key."""
    if sanitized in _SANITIZED_SIGNAL_MAP:
        return _SANITIZED_SIGNAL_MAP[sanitized]
    return sanitized.replace("_", " ")
