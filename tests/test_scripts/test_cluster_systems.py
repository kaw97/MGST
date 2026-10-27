"""Tests for the predicted-signal and species filters in scripts/cluster_systems.py."""

import csv
import importlib.util
import subprocess
import sys
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "cluster_systems.py"
spec = importlib.util.spec_from_file_location("cluster_systems", SCRIPT)
cluster_systems = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cluster_systems)

FIELDS = ["name", "id64", "spansh_url", "x", "y", "z", "qualifying_bodies", "total_min_value", "body_details"]
SYSTEMS = [
    # one body with 2 species, one with 1
    ("Mixed", "0,0,0", "Mixed A 1(Stratum Tectonicas 19.0M, Osseus Discus 12.9M);Mixed A 2(Tussock Virgam 14.3M)"),
    # every body has 2+ species
    ("Rich", "10,0,0", "Rich 1(Aleoida Gravis 12.9M, Tubus Cavas 11.9M);Rich 2(Frutexa Flammasis 10.3M, Stratum Cucumisis 16.2M, Clypeus Speculumi 16.2M)"),
    # single-species bodies only
    ("Sparse", "20,0,0", "Sparse 1(Tubus Cavas 11.9M);Sparse 2(Tubus Cavas 11.9M)"),
]


def system(details):
    return {"_row": {"body_details": details}}


class TestBodySignalCounts:
    def test_counts_species_per_body(self):
        assert cluster_systems.body_signal_counts(SYSTEMS[0][2]) == [2, 1]
        assert cluster_systems.body_signal_counts(SYSTEMS[1][2]) == [2, 3]

    def test_empty_field(self):
        assert cluster_systems.body_signal_counts("") == []

    def test_entry_without_species_counts_zero(self):
        assert cluster_systems.body_signal_counts("Body 1;Body 2(Tubus Cavas 11.9M)") == [0, 1]


class TestBodySpecies:
    def test_names_and_species_without_values(self):
        assert cluster_systems.body_species(SYSTEMS[0][2]) == [
            ("Mixed A 1", ["Stratum Tectonicas", "Osseus Discus"]),
            ("Mixed A 2", ["Tussock Virgam"]),
        ]

    def test_entry_without_species(self):
        assert cluster_systems.body_species("Body 1;Body 2(Tubus Cavas 11.9M)") == [
            ("Body 1", []), ("Body 2", ["Tubus Cavas"])]


class TestFilterByBodySignals:
    SYS = [system(d) for _, _, d in SYSTEMS]

    def names(self, kept):
        return [SYSTEMS[self.SYS.index(s)][0] for s in kept]

    def test_any_body(self):
        assert self.names(cluster_systems.filter_by_body_signals(self.SYS, 2, "any")) == ["Mixed", "Rich"]

    def test_all_bodies(self):
        assert self.names(cluster_systems.filter_by_body_signals(self.SYS, 2, "all")) == ["Rich"]

    def test_threshold_one_keeps_everything(self):
        assert len(cluster_systems.filter_by_body_signals(self.SYS, 1, "all")) == 3

    def test_system_without_details_dropped(self):
        assert cluster_systems.filter_by_body_signals([system("")], 1, "any") == []


class TestResolveSpeciesTargets:
    def test_species_case_insensitive(self):
        assert cluster_systems.resolve_species_targets(["tubus cavas"], []) == [{"Tubus Cavas"}]

    def test_genus_expands_to_all_species(self):
        assert cluster_systems.resolve_species_targets([], ["recepta"]) == [
            {"Recepta Umbrux", "Recepta Deltahedronix", "Recepta Conditivus"}]

    def test_unknown_species_suggests(self):
        with pytest.raises(ValueError, match="Did you mean: Stratum Tectonicas"):
            cluster_systems.resolve_species_targets(["Stratum Tectonica"], [])

    def test_unknown_genus(self):
        with pytest.raises(ValueError, match="Unknown genus 'Bacterium'"):
            cluster_systems.resolve_species_targets([], ["Bacterium"])


class TestFilterBySpecies:
    def run(self, species=(), genera=(), match="any", scope="body"):
        systems = [{"name": n, "_row": {"body_details": d}} for n, _, d in SYSTEMS]
        groups = cluster_systems.resolve_species_targets(list(species), list(genera))
        return cluster_systems.filter_by_species(systems, groups, match, scope)

    def names(self, kept):
        return [s["name"] for s in kept]

    def test_any_species(self):
        assert self.names(self.run(["Tubus Cavas", "Tussock Virgam"])) == ["Mixed", "Rich", "Sparse"]
        assert self.names(self.run(["Osseus Discus"])) == ["Mixed"]

    def test_genus(self):
        assert self.names(self.run(genera=["Stratum"])) == ["Mixed", "Rich"]

    def test_all_on_one_body(self):
        # Mixed has Stratum Tectonicas and Tussock Virgam, but on different bodies
        assert self.names(self.run(["Stratum Tectonicas", "Tussock Virgam"], match="all")) == []
        assert self.names(self.run(["Stratum Tectonicas", "Osseus Discus"], match="all")) == ["Mixed"]

    def test_all_across_system(self):
        assert self.names(self.run(["Stratum Tectonicas", "Tussock Virgam"], match="all",
                                   scope="system")) == ["Mixed"]

    def test_all_mixes_species_and_genus(self):
        # Rich: Clypeus Speculumi and Stratum Cucumisis share body 2; Tubus Cavas is on body 1
        assert self.names(self.run(["Clypeus Speculumi"], ["Stratum"], match="all")) == ["Rich"]
        assert self.names(self.run(["Tubus Cavas"], ["Clypeus"], match="all")) == []
        assert self.names(self.run(["Tubus Cavas"], ["Clypeus"], match="all", scope="system")) == ["Rich"]

    def test_matched_species_column(self):
        kept = self.run(["Tubus Cavas"], ["Stratum"])
        assert {s["name"]: s["_row"]["matched_species"] for s in kept} == {
            "Mixed": "Mixed A 1(Stratum Tectonicas)",
            "Rich": "Rich 1(Tubus Cavas);Rich 2(Stratum Cucumisis)",
            "Sparse": "Sparse 1(Tubus Cavas);Sparse 2(Tubus Cavas)",
        }

    def test_system_without_details_dropped(self):
        groups = cluster_systems.resolve_species_targets(["Tubus Cavas"], [])
        assert cluster_systems.filter_by_species([{"_row": {}}], groups, "any", "body") == []


def run_script(tmp_path, *extra):
    src = tmp_path / "results.tsv"
    with open(src, "w", newline="") as f:
        w = csv.writer(f, delimiter="\t")
        w.writerow(FIELDS)
        for i, (name, xyz, details) in enumerate(SYSTEMS):
            x, y, z = xyz.split(",")
            w.writerow([name, i, "", x, y, z, details.count("(") , 0, details])
    out = tmp_path / "clusters.tsv"
    result = subprocess.run(
        [sys.executable, str(SCRIPT), "-i", str(src), "-d", "50", "-o", str(out), *extra],
        capture_output=True, text=True,
    )
    return result, out


def clustered_names(out):
    with open(out) as fh:
        return {name.strip() for row in csv.DictReader(fh, delimiter="\t") for name in row["systems"].split(";")}


class TestCommandLine:
    def test_all_mode_end_to_end(self, tmp_path):
        result, out = run_script(tmp_path, "--min-body-signals", "2", "--signal-filter", "all")
        assert result.returncode == 0, result.stderr
        assert "kept 1 of 3 systems" in result.stdout
        summary = (tmp_path / "clusters_summary.txt").read_text()
        assert "every predicted body with >= 2 predicted species" in summary
        assert clustered_names(out) == {"Rich"}

    def test_any_mode_end_to_end(self, tmp_path):
        result, out = run_script(tmp_path, "--min-body-signals", "2")
        assert result.returncode == 0, result.stderr
        assert "kept 2 of 3 systems" in result.stdout
        assert clustered_names(out) == {"Mixed", "Rich"}

    def test_no_filter_keeps_all(self, tmp_path):
        result, out = run_script(tmp_path)
        assert result.returncode == 0, result.stderr
        assert "Signal filter" not in result.stdout
        assert clustered_names(out) == {"Mixed", "Rich", "Sparse"}

    def test_rejects_zero(self, tmp_path):
        result, _ = run_script(tmp_path, "--min-body-signals", "0")
        assert result.returncode != 0

    def test_species_filter_end_to_end(self, tmp_path):
        result, out = run_script(tmp_path, "--genus", "stratum", "--species", "Osseus Discus")
        assert result.returncode == 0, result.stderr
        assert "kept 2 of 3 systems" in result.stdout
        summary = (tmp_path / "clusters_summary.txt").read_text()
        assert "Species filter: any of Osseus Discus, stratum (genus) on a single body" in summary
        assert clustered_names(out) == {"Mixed", "Rich"}
        route = next((tmp_path / "clusters").glob("cluster_0001_*.tsv"))
        with open(route) as fh:
            rows = list(csv.DictReader(fh, delimiter="\t"))
        assert {r["name"]: r["matched_species"] for r in rows} == {
            "Mixed": "Mixed A 1(Stratum Tectonicas, Osseus Discus)",
            "Rich": "Rich 2(Stratum Cucumisis)",
        }

    def test_species_and_signal_filters_combine(self, tmp_path):
        result, out = run_script(tmp_path, "--species", "Tubus Cavas", "--min-body-signals", "2")
        assert result.returncode == 0, result.stderr
        summary = (tmp_path / "clusters_summary.txt").read_text()
        assert "Signal filter:" in summary and "Species filter:" in summary
        assert clustered_names(out) == {"Rich"}

    def test_unknown_species_rejected(self, tmp_path):
        result, _ = run_script(tmp_path, "--species", "Tubus Cava")
        assert result.returncode != 0
        assert "Did you mean: Tubus Cavas" in result.stderr

    def test_no_match_writes_nothing(self, tmp_path):
        result, out = run_script(tmp_path, "--species", "Fumerola Extremus")
        assert result.returncode == 0, result.stderr
        assert "No systems pass the filters" in result.stdout
        assert not out.exists()
