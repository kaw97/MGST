"""MGST Streamlit GUI - Search Builder & Results Browser."""

import json
import os
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import streamlit as st

from mgst.gui.constants import (
    ATMOSPHERE_TYPES,
    PARENT_TYPES,
    PLANET_SUBTYPES,
    RING_TYPES,
    STAR_SUBTYPES,
    VOLCANISM_TYPES,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[3]  # src/mgst/gui -> project root
PATTERNS_DIR = PROJECT_ROOT / "patterns"


def _default_body_state():
    """Return a fresh body state dict for the visual builder."""
    return {
        "body_type": "Any",
        "sub_types": [],
        "variable": "",
        # numeric ranges
        "temp_enabled": False,
        "temp_min": 0.0,
        "temp_max": 1000.0,
        "gravity_enabled": False,
        "gravity_min": 0.0,
        "gravity_max": 100.0,
        "pressure_enabled": False,
        "pressure_min": 0.0,
        "pressure_max": 100000.0,
        # parents
        "parent_type": "",
        "parent_value": "*",
        # rings
        "ring_mode": "No filter",
        "ring_types": [],
        # volcanism
        "volcanism_mode": "No filter",
        "volcanism_types": [],
        # atmosphere
        "atmosphere_mode": "No filter",
        "atmosphere_types": [],
    }


def _subtype_options(body_type):
    """Return subtype list filtered by body type selector."""
    if body_type == "Planet":
        return PLANET_SUBTYPES
    if body_type == "Star":
        return STAR_SUBTYPES
    return PLANET_SUBTYPES + STAR_SUBTYPES


def _list_pattern_files():
    """List *.json files in the patterns directory."""
    if PATTERNS_DIR.is_dir():
        return sorted(f.name for f in PATTERNS_DIR.glob("*.json"))
    return []


def _list_output_dirs():
    """List output subdirectories sorted newest-first."""
    out = PROJECT_ROOT / "output"
    if not out.is_dir():
        return []
    dirs = [d for d in out.iterdir() if d.is_dir()]
    dirs.sort(key=lambda d: d.stat().st_mtime, reverse=True)
    return dirs


def _load_builtin_configs():
    """Load built-in config names and descriptions. Returns dict name->desc."""
    try:
        from mgst.configs.config_loader import config_loader
        return config_loader.list_available_configs()
    except Exception:
        return {}


def _find_results_file(directory):
    """Find the main results file (tsv or jsonl) in a directory."""
    for ext in ("*.tsv", "*.jsonl"):
        files = list(directory.glob(ext))
        # skip metadata files
        files = [f for f in files if "metadata" not in f.name]
        if files:
            return files[0]
    return None


def _find_metadata_file(directory):
    """Find the search metadata JSON in a directory."""
    files = list(directory.glob("*_search_metadata.json"))
    if files:
        return files[0]
    return None


def _parse_stats_from_stdout(text):
    """Extract key stats from stdout log text."""
    stats = {}
    patterns = {
        "Files processed": r"Files processed:\s*(\d+)",
        "Systems processed": r"Systems processed:\s*([\d,]+)",
        "Matches found": r"Matches found:\s*([\d,]+)",
        "Match rate": r"Match rate:\s*([\d.]+%)",
        "Processing time": r"Processing time:\s*(.+)",
        "Speed": r"Speed:\s*(.+)",
    }
    for label, pat in patterns.items():
        m = re.search(pat, text)
        if m:
            stats[label] = m.group(1)
    return stats


# ---------------------------------------------------------------------------
# Session state initialization
# ---------------------------------------------------------------------------

def init_session_state():
    defaults = {
        # search mode
        "search_mode": "corridor",
        # database
        "database_path": "Databases/galaxy_sectors_compressed",
        "sector_index_path": "",
        # corridor
        "start_coords": "",
        "end_coords": "",
        "radius": 500.0,
        # sectors
        "sectors_text": "",
        # pattern source tracking
        "pattern_source": "visual",
        "pattern_json": '{\n  "description": "",\n  "name": "*",\n  "bodies": []\n}',
        "selected_pattern_file": "",
        "selected_builtin_config": "",
        # visual builder bodies
        "vb_description": "",
        "vb_name": "*",
        "vb_pop_mode": "Any",
        "vb_pop_min": 0,
        "vb_pop_max": 1000000,
        "vb_bodies": [],
        # output
        "output_dir_name": "",
        "output_format": "tsv",
        "workers": 8,
        "test_mode": False,
        "dry_run": False,
        # execution
        "search_running": False,
        "search_stdout": "",
        "search_returncode": None,
        "search_output_dir": "",
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v


# ---------------------------------------------------------------------------
# Pattern generation from visual builder
# ---------------------------------------------------------------------------

def build_pattern_from_visual_state():
    """Build a JSON pattern dict from the visual builder session state."""
    pattern = {}

    desc = st.session_state.get("vb_description", "")
    if desc:
        pattern["description"] = desc

    name = st.session_state.get("vb_name", "*")
    if name:
        pattern["name"] = name

    # population
    pop_mode = st.session_state.get("vb_pop_mode", "Any")
    if pop_mode == "Unpopulated":
        pattern["population"] = [0, None]
    elif pop_mode == "Range":
        pop_range = {}
        pmin = st.session_state.get("vb_pop_min", 0)
        pmax = st.session_state.get("vb_pop_max", 0)
        if pmin > 0:
            pop_range["min"] = pmin
        if pmax > 0:
            pop_range["max"] = pmax
        if pop_range:
            pattern["population"] = pop_range

    # bodies
    bodies_state = st.session_state.get("vb_bodies", [])
    if bodies_state:
        bodies = []
        for bs in bodies_state:
            body = {}

            # variable binding -> bodyId
            var = bs.get("variable", "").strip()
            if var:
                if not var.startswith("$"):
                    var = "$" + var
                body["bodyId"] = var

            # type
            bt = bs.get("body_type", "Any")
            if bt != "Any":
                body["type"] = bt

            # subType
            subs = bs.get("sub_types", [])
            if len(subs) == 1:
                body["subType"] = subs[0]
            elif len(subs) > 1:
                body["subType"] = list(subs)

            # numeric ranges
            if bs.get("temp_enabled"):
                body["surfaceTemperature"] = {
                    "min": bs.get("temp_min", 0),
                    "max": bs.get("temp_max", 1000),
                }
            if bs.get("gravity_enabled"):
                body["gravity"] = {
                    "min": bs.get("gravity_min", 0),
                    "max": bs.get("gravity_max", 100),
                }
            if bs.get("pressure_enabled"):
                body["surfacePressure"] = {
                    "min": bs.get("pressure_min", 0),
                    "max": bs.get("pressure_max", 100000),
                }

            # parents
            pt = bs.get("parent_type", "")
            if pt:
                pv = bs.get("parent_value", "*")
                body["parents"] = [{pt: pv}]

            # rings
            ring_mode = bs.get("ring_mode", "No filter")
            if ring_mode == "Must have":
                body["rings"] = ["*"]
            elif ring_mode == "Must NOT have":
                body["rings"] = []
            elif ring_mode == "Specific type":
                rtypes = bs.get("ring_types", [])
                if rtypes:
                    body["rings"] = [{"type": rt} for rt in rtypes]

            # volcanism
            volc_mode = bs.get("volcanism_mode", "No filter")
            if volc_mode == "Has any":
                body["volcanismType"] = "*"
            elif volc_mode == "Specific types":
                vtypes = bs.get("volcanism_types", [])
                if len(vtypes) == 1:
                    body["volcanismType"] = vtypes[0]
                elif len(vtypes) > 1:
                    body["volcanismType"] = list(vtypes)

            # atmosphere
            atmo_mode = bs.get("atmosphere_mode", "No filter")
            if atmo_mode == "Has any":
                body["atmosphereType"] = "*"
            elif atmo_mode == "Specific types":
                atypes = bs.get("atmosphere_types", [])
                if len(atypes) == 1:
                    body["atmosphereType"] = atypes[0]
                elif len(atypes) > 1:
                    body["atmosphereType"] = list(atypes)

            bodies.append(body)

        if bodies:
            pattern["bodies"] = bodies

    return pattern


# ---------------------------------------------------------------------------
# Search Builder page
# ---------------------------------------------------------------------------

def render_search_builder():
    st.header("Search Builder")

    # --- 1. Search mode ---
    mode = st.radio(
        "Search Mode",
        ["galaxy", "sectors", "corridor"],
        index=["galaxy", "sectors", "corridor"].index(
            st.session_state.get("search_mode", "corridor")
        ),
        horizontal=True,
        key="search_mode",
    )

    # --- 2. Database path ---
    st.text_input(
        "Database path",
        key="database_path",
        help="Path to the sector database directory",
    )

    # --- 3. Mode-specific inputs ---
    if mode == "sectors":
        st.text_area(
            "Sector names (comma or newline separated)",
            key="sectors_text",
            height=100,
        )
    elif mode == "corridor":
        c1, c2 = st.columns(2)
        with c1:
            st.text_input("Start coordinates (x,y,z)", key="start_coords")
        with c2:
            st.text_input("End coordinates (x,y,z)", key="end_coords")
        c3, c4 = st.columns(2)
        with c3:
            st.number_input(
                "Radius (ly)", min_value=1.0, step=50.0, key="radius"
            )
        with c4:
            st.text_input(
                "Sector index path (optional)",
                key="sector_index_path",
                help="Leave blank to use default (database/sector_index.json)",
            )

    # --- 4. Pattern configuration ---
    st.subheader("Pattern Configuration")
    tab_visual, tab_json, tab_file, tab_builtin = st.tabs(
        ["Visual Builder", "JSON Editor", "Load Pattern", "Built-in Configs"]
    )

    with tab_visual:
        _render_visual_builder()

    with tab_json:
        _render_json_editor()

    with tab_file:
        _render_pattern_file_loader()

    with tab_builtin:
        _render_builtin_configs()

    # --- 5. Output settings ---
    st.subheader("Output Settings")
    oc1, oc2, oc3 = st.columns(3)
    with oc1:
        st.text_input(
            "Output directory name",
            key="output_dir_name",
            help="Subdirectory name under output/. Leave blank for auto-generated.",
        )
    with oc2:
        st.selectbox("Output format", ["tsv", "jsonl"], key="output_format")
    with oc3:
        st.number_input("Workers", min_value=1, max_value=64, step=1, key="workers")

    oc4, oc5 = st.columns(2)
    with oc4:
        st.checkbox("Test mode (1000 systems/file)", key="test_mode")
    with oc5:
        st.checkbox("Dry run (preview only)", key="dry_run")

    # --- 6. Run button ---
    st.divider()
    if st.button("Run Search", type="primary", use_container_width=True):
        _run_search()

    # --- 7. Show previous result if exists ---
    if st.session_state.get("search_stdout"):
        _show_search_results()


def _render_visual_builder():
    """Render the visual pattern builder tab."""
    st.text_input("Description", key="vb_description", placeholder="What are you searching for?")
    st.text_input("System name pattern", key="vb_name", placeholder="* (wildcard)")

    pop_mode = st.selectbox(
        "Population filter",
        ["Any", "Unpopulated", "Range"],
        key="vb_pop_mode",
    )
    if pop_mode == "Range":
        pc1, pc2 = st.columns(2)
        with pc1:
            st.number_input("Min population", min_value=0, step=1000, key="vb_pop_min")
        with pc2:
            st.number_input("Max population", min_value=0, step=1000, key="vb_pop_max")

    # Bodies
    st.markdown("---")
    st.markdown("**Bodies**")

    if st.button("Add Body"):
        st.session_state["vb_bodies"].append(_default_body_state())
        st.rerun()

    bodies = st.session_state.get("vb_bodies", [])
    to_remove = None
    for i, bs in enumerate(bodies):
        with st.expander(f"Body {i + 1}", expanded=True):
            _render_body_editor(i, bs)
            if st.button(f"Remove body {i + 1}", key=f"rm_body_{i}"):
                to_remove = i

    if to_remove is not None:
        st.session_state["vb_bodies"].pop(to_remove)
        st.rerun()

    # Preview generated JSON
    pattern = build_pattern_from_visual_state()
    st.markdown("**Generated JSON**")
    st.code(json.dumps(pattern, indent=2), language="json")

    # Mark source as visual when user interacts here
    st.session_state["pattern_source"] = "visual"


def _render_body_editor(idx, bs):
    """Render editor widgets for a single body in the visual builder."""
    prefix = f"body_{idx}"

    # Type
    type_options = ["Any", "Planet", "Star"]
    current_type = bs.get("body_type", "Any")
    if current_type not in type_options:
        current_type = "Any"
    new_type = st.selectbox(
        "Type", type_options,
        index=type_options.index(current_type),
        key=f"{prefix}_type",
    )
    bs["body_type"] = new_type

    # SubType multiselect
    opts = _subtype_options(new_type)
    bs["sub_types"] = st.multiselect(
        "SubType(s)", opts,
        default=[s for s in bs.get("sub_types", []) if s in opts],
        key=f"{prefix}_subtypes",
    )

    # Variable binding
    bs["variable"] = st.text_input(
        "Variable binding (e.g. $planet)",
        value=bs.get("variable", ""),
        key=f"{prefix}_var",
    )

    # Numeric ranges
    with st.expander("Numeric ranges"):
        bs["temp_enabled"] = st.checkbox(
            "Surface temperature range", value=bs.get("temp_enabled", False),
            key=f"{prefix}_temp_en",
        )
        if bs["temp_enabled"]:
            tc1, tc2 = st.columns(2)
            with tc1:
                bs["temp_min"] = st.number_input(
                    "Temp min (K)", value=bs.get("temp_min", 0.0),
                    key=f"{prefix}_tmin",
                )
            with tc2:
                bs["temp_max"] = st.number_input(
                    "Temp max (K)", value=bs.get("temp_max", 1000.0),
                    key=f"{prefix}_tmax",
                )

        bs["gravity_enabled"] = st.checkbox(
            "Gravity range", value=bs.get("gravity_enabled", False),
            key=f"{prefix}_grav_en",
        )
        if bs["gravity_enabled"]:
            gc1, gc2 = st.columns(2)
            with gc1:
                bs["gravity_min"] = st.number_input(
                    "Gravity min (g)", value=bs.get("gravity_min", 0.0),
                    key=f"{prefix}_gmin",
                )
            with gc2:
                bs["gravity_max"] = st.number_input(
                    "Gravity max (g)", value=bs.get("gravity_max", 100.0),
                    key=f"{prefix}_gmax",
                )

        bs["pressure_enabled"] = st.checkbox(
            "Surface pressure range", value=bs.get("pressure_enabled", False),
            key=f"{prefix}_pres_en",
        )
        if bs["pressure_enabled"]:
            pc1, pc2 = st.columns(2)
            with pc1:
                bs["pressure_min"] = st.number_input(
                    "Pressure min (atm)", value=bs.get("pressure_min", 0.0),
                    key=f"{prefix}_pmin",
                )
            with pc2:
                bs["pressure_max"] = st.number_input(
                    "Pressure max (atm)", value=bs.get("pressure_max", 100000.0),
                    key=f"{prefix}_pmax",
                )

    # Parent config
    with st.expander("Parent body"):
        pt_options = [""] + PARENT_TYPES
        current_pt = bs.get("parent_type", "")
        if current_pt not in pt_options:
            current_pt = ""
        bs["parent_type"] = st.selectbox(
            "Parent type", pt_options,
            index=pt_options.index(current_pt),
            key=f"{prefix}_ptype",
        )
        if bs["parent_type"]:
            bs["parent_value"] = st.text_input(
                "Parent value (bodyId, $var, or *)",
                value=bs.get("parent_value", "*"),
                key=f"{prefix}_pval",
            )

    # Rings
    with st.expander("Rings"):
        ring_options = ["No filter", "Must have", "Must NOT have", "Specific type"]
        current_rm = bs.get("ring_mode", "No filter")
        if current_rm not in ring_options:
            current_rm = "No filter"
        bs["ring_mode"] = st.selectbox(
            "Ring filter", ring_options,
            index=ring_options.index(current_rm),
            key=f"{prefix}_ringmode",
        )
        if bs["ring_mode"] == "Specific type":
            bs["ring_types"] = st.multiselect(
                "Ring types", RING_TYPES,
                default=[r for r in bs.get("ring_types", []) if r in RING_TYPES],
                key=f"{prefix}_ringtypes",
            )

    # Volcanism
    with st.expander("Volcanism"):
        volc_options = ["No filter", "Has any", "Specific types"]
        current_vm = bs.get("volcanism_mode", "No filter")
        if current_vm not in volc_options:
            current_vm = "No filter"
        bs["volcanism_mode"] = st.selectbox(
            "Volcanism filter", volc_options,
            index=volc_options.index(current_vm),
            key=f"{prefix}_volcmode",
        )
        if bs["volcanism_mode"] == "Specific types":
            bs["volcanism_types"] = st.multiselect(
                "Volcanism types", VOLCANISM_TYPES,
                default=[v for v in bs.get("volcanism_types", []) if v in VOLCANISM_TYPES],
                key=f"{prefix}_volctypes",
            )

    # Atmosphere
    with st.expander("Atmosphere"):
        atmo_options = ["No filter", "Has any", "Specific types"]
        current_am = bs.get("atmosphere_mode", "No filter")
        if current_am not in atmo_options:
            current_am = "No filter"
        bs["atmosphere_mode"] = st.selectbox(
            "Atmosphere filter", atmo_options,
            index=atmo_options.index(current_am),
            key=f"{prefix}_atmomode",
        )
        if bs["atmosphere_mode"] == "Specific types":
            bs["atmosphere_types"] = st.multiselect(
                "Atmosphere types", ATMOSPHERE_TYPES,
                default=[a for a in bs.get("atmosphere_types", []) if a in ATMOSPHERE_TYPES],
                key=f"{prefix}_atmotypes",
            )


def _render_json_editor():
    """Render the raw JSON editor tab."""
    json_text = st.text_area(
        "Pattern JSON",
        value=st.session_state.get("pattern_json", "{}"),
        height=400,
        key="pattern_json",
    )
    # Validate
    try:
        parsed = json.loads(json_text)
        body_count = len(parsed.get("bodies", []))
        desc = parsed.get("description", "")
        st.success(f"Valid JSON — {body_count} body pattern(s). {desc}")
        st.session_state["pattern_source"] = "json_editor"
    except json.JSONDecodeError as e:
        st.error(f"Invalid JSON: {e}")


def _render_pattern_file_loader():
    """Render the Load Pattern tab."""
    files = _list_pattern_files()
    if not files:
        st.info("No pattern files found in patterns/ directory.")
        return

    selected = st.selectbox("Pattern file", files, key="_pfile_select")
    if selected:
        path = PATTERNS_DIR / selected
        try:
            content = path.read_text()
            st.code(content, language="json")
            if st.button("Use this pattern", key="use_pattern_file"):
                st.session_state["selected_pattern_file"] = str(path)
                st.session_state["pattern_source"] = "file"
                st.session_state["pattern_json"] = content
                st.success(f"Pattern loaded: {selected}")
        except Exception as e:
            st.error(f"Error reading file: {e}")


def _render_builtin_configs():
    """Render the Built-in Configs tab."""
    configs = _load_builtin_configs()
    if not configs:
        st.info("No built-in configs available (check mgst installation).")
        return

    names = list(configs.keys())
    selected = st.selectbox("Built-in config", names, key="_builtin_select")
    if selected:
        st.markdown(f"**Description:** {configs[selected]}")
        if st.button("Use this config", key="use_builtin_config"):
            st.session_state["selected_builtin_config"] = selected
            st.session_state["pattern_source"] = "builtin"
            st.success(f"Config selected: {selected}")


# ---------------------------------------------------------------------------
# Search execution
# ---------------------------------------------------------------------------

def _run_search():
    """Build and execute the mgst filter command as a subprocess."""
    mode = st.session_state["search_mode"]
    db_path = st.session_state["database_path"].strip()

    # Validate database path
    if not db_path:
        st.error("Database path is required.")
        return
    db_abs = Path(db_path)
    if not db_abs.is_absolute():
        db_abs = PROJECT_ROOT / db_path
    if not db_abs.is_dir():
        st.error(f"Database directory not found: {db_abs}")
        return

    # Determine output directory
    dir_name = st.session_state.get("output_dir_name", "").strip()
    if not dir_name:
        dir_name = f"gui_{mode}_{datetime.now():%Y%m%d_%H%M%S}"
    output_dir = PROJECT_ROOT / "output" / dir_name
    output_dir.mkdir(parents=True, exist_ok=True)

    fmt = st.session_state["output_format"]
    output_file = output_dir / f"results.{fmt}"

    # Build command
    cmd = [
        sys.executable, "-m", "mgst", "filter",
        "--mode", mode,
        "--database", str(db_abs),
        "--output", str(output_file),
        "--format", fmt,
        "--workers", str(st.session_state["workers"]),
    ]

    # Mode-specific args
    if mode == "sectors":
        sectors_raw = st.session_state.get("sectors_text", "").strip()
        if not sectors_raw:
            st.error("Sectors mode requires sector names.")
            return
        sectors_clean = ",".join(
            s.strip() for s in re.split(r"[,\n]+", sectors_raw) if s.strip()
        )
        cmd += ["--sectors", sectors_clean]
    elif mode == "corridor":
        start = st.session_state.get("start_coords", "").strip()
        end = st.session_state.get("end_coords", "").strip()
        radius = st.session_state.get("radius", 500.0)
        if not start or not end:
            st.error("Corridor mode requires start and end coordinates.")
            return
        cmd += ["--start", start, "--end", end, "--radius", str(radius)]
        si = st.session_state.get("sector_index_path", "").strip()
        if si:
            si_path = Path(si) if Path(si).is_absolute() else PROJECT_ROOT / si
            if si_path.exists():
                cmd += ["--sector-index", str(si_path)]

    # Pattern / config args
    source = st.session_state["pattern_source"]
    if source == "builtin":
        config_name = st.session_state.get("selected_builtin_config", "")
        if not config_name:
            st.error("No built-in config selected.")
            return
        cmd += ["--config", config_name]
    else:
        # Determine pattern JSON to use
        if source == "visual":
            pattern = build_pattern_from_visual_state()
        elif source == "json_editor":
            try:
                pattern = json.loads(st.session_state.get("pattern_json", "{}"))
            except json.JSONDecodeError as e:
                st.error(f"Invalid pattern JSON: {e}")
                return
        elif source == "file":
            pf = st.session_state.get("selected_pattern_file", "")
            if pf and Path(pf).exists():
                cmd += ["--pattern-file", pf]
                pattern = None
            else:
                st.error("Selected pattern file not found.")
                return
        else:
            pattern = build_pattern_from_visual_state()

        # Save pattern to file if not loading from existing file
        if pattern is not None:
            pattern_path = output_dir / "pattern.json"
            pattern_path.write_text(json.dumps(pattern, indent=2))
            cmd += ["--pattern-file", str(pattern_path)]

    if st.session_state.get("test_mode"):
        cmd.append("--test")
    if st.session_state.get("dry_run"):
        cmd.append("--dry-run")

    # Execute
    st.session_state["search_output_dir"] = str(output_dir)

    with st.status("Running search...", expanded=True) as status:
        st.code(" ".join(cmd), language="bash")
        output_placeholder = st.empty()

        try:
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                cwd=str(PROJECT_ROOT),
            )

            lines = []
            for line in proc.stdout:
                lines.append(line)
                # Show last 30 lines
                display = "".join(lines[-30:])
                output_placeholder.code(display, language="text")

            proc.wait()
            full_output = "".join(lines)
            st.session_state["search_stdout"] = full_output
            st.session_state["search_returncode"] = proc.returncode

            if proc.returncode == 0:
                status.update(label="Search completed!", state="complete")
            else:
                status.update(label="Search failed", state="error")

        except Exception as e:
            st.error(f"Failed to run search: {e}")
            st.session_state["search_returncode"] = -1
            st.session_state["search_stdout"] = str(e)


def _show_search_results():
    """Display results summary after a search run."""
    stdout = st.session_state.get("search_stdout", "")
    rc = st.session_state.get("search_returncode")

    if rc == 0:
        stats = _parse_stats_from_stdout(stdout)
        if stats:
            cols = st.columns(3)
            stat_items = list(stats.items())
            for i, (label, value) in enumerate(stat_items):
                cols[i % 3].metric(label, value)
    else:
        st.error(f"Search exited with code {rc}")

    out_dir = st.session_state.get("search_output_dir", "")
    if out_dir:
        st.info(f"Output directory: {out_dir}")


# ---------------------------------------------------------------------------
# Results Browser page
# ---------------------------------------------------------------------------

def render_results_browser():
    st.header("Results Browser")

    dirs = _list_output_dirs()
    if not dirs:
        st.info("No output directories found. Run a search first.")
        return

    dir_labels = [f"{d.name}  ({datetime.fromtimestamp(d.stat().st_mtime):%Y-%m-%d %H:%M})" for d in dirs]
    selected_idx = st.selectbox(
        "Output directory",
        range(len(dirs)),
        format_func=lambda i: dir_labels[i],
        key="results_dir_idx",
    )
    selected_dir = dirs[selected_idx]

    tab_data, tab_meta, tab_logs = st.tabs(["Results Data", "Search Metadata", "Logs"])

    with tab_data:
        _render_results_data(selected_dir)

    with tab_meta:
        _render_search_metadata(selected_dir)

    with tab_logs:
        _render_logs(selected_dir)


def _render_results_data(directory):
    """Render results data table."""
    results_file = _find_results_file(directory)
    if not results_file:
        st.info("No results file found in this directory.")
        return

    st.markdown(f"**File:** `{results_file.name}`")

    try:
        import pandas as pd

        if results_file.suffix == ".tsv":
            df = pd.read_csv(results_file, sep="\t", nrows=10000)
        else:
            df = pd.read_json(results_file, lines=True, nrows=10000)

        if df.empty:
            st.info("Results file is empty (no matches).")
            return

        st.markdown(f"Showing {len(df):,} rows")

        # Column selector
        all_cols = list(df.columns)
        selected_cols = st.multiselect(
            "Columns to display",
            all_cols,
            default=all_cols,
            key="results_columns",
        )

        # Text search filter
        search_text = st.text_input("Filter by name", key="results_filter")
        view_df = df[selected_cols] if selected_cols else df

        if search_text:
            # Search across string columns for the text
            name_cols = [c for c in view_df.columns if "name" in c.lower()]
            if name_cols:
                mask = view_df[name_cols[0]].astype(str).str.contains(
                    search_text, case=False, na=False
                )
                view_df = view_df[mask]
            st.markdown(f"Filtered to {len(view_df):,} rows")

        st.dataframe(view_df, use_container_width=True)

        # Download
        csv_data = view_df.to_csv(index=False).encode("utf-8")
        st.download_button(
            "Download as CSV",
            csv_data,
            file_name=f"{directory.name}_results.csv",
            mime="text/csv",
        )

    except Exception as e:
        st.error(f"Error loading results: {e}")


def _render_search_metadata(directory):
    """Render search metadata display."""
    meta_file = _find_metadata_file(directory)
    if not meta_file:
        st.info("No search metadata file found.")
        return

    try:
        meta = json.loads(meta_file.read_text())

        # Key fields
        st.markdown(f"**Mode:** {meta.get('search_mode', 'N/A')}")
        st.markdown(f"**Timestamp:** {meta.get('timestamp', 'N/A')}")
        st.markdown(f"**Sectors searched:** {meta.get('sector_files_searched', 'N/A')}")

        params = meta.get("search_parameters", {})
        if params.get("start_coords"):
            st.markdown(f"**Start:** {params['start_coords']}")
        if params.get("end_coords"):
            st.markdown(f"**End:** {params['end_coords']}")
        if params.get("radius"):
            st.markdown(f"**Radius:** {params['radius']} ly")
        if params.get("sectors"):
            st.markdown(f"**Sectors:** {params['sectors']}")

        # Full JSON
        with st.expander("Full metadata JSON"):
            st.code(json.dumps(meta, indent=2), language="json")

    except Exception as e:
        st.error(f"Error loading metadata: {e}")


def _render_logs(directory):
    """Render log file displays."""
    log_files = [
        ("stdin.txt", "Command Info"),
        ("stdout.txt", "Output Log"),
        ("stderr.txt", "Error Log"),
    ]

    for filename, label in log_files:
        path = directory / filename
        if path.exists():
            with st.expander(label):
                try:
                    content = path.read_text(errors="replace")
                    st.code(content[:50000], language="text")
                except Exception as e:
                    st.error(f"Error reading {filename}: {e}")
        else:
            with st.expander(f"{label} (not found)"):
                st.info(f"{filename} not found in this directory.")

    # Also show pattern.json if present
    pattern_path = directory / "pattern.json"
    if pattern_path.exists():
        with st.expander("Pattern JSON"):
            try:
                st.code(pattern_path.read_text(), language="json")
            except Exception as e:
                st.error(f"Error reading pattern.json: {e}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    st.set_page_config(
        page_title="MGST - Mikunn Galactic Search Tool",
        page_icon=None,
        layout="wide",
    )
    init_session_state()

    page = st.sidebar.radio("Navigation", ["Search Builder", "Results Browser"])

    if page == "Search Builder":
        render_search_builder()
    else:
        render_results_browser()


if __name__ == "__main__":
    main()
