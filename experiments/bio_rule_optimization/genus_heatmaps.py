"""Compile per-species temperature and gravity panels into one composite figure per genus.

Panels are the untitled species figures in panels/ of a temp_gravity_rules.py or temp_gravity_scatter.py run, in alphabetical order, with bold
letters A, B, C and so on and the full species name above each. The counts go in a caption under the figure.
Writes <Genus>.png and panels.tsv, which lists the species behind each letter.
"""

import argparse
import json
import string
import sys
from collections import defaultdict
from pathlib import Path

import matplotlib
from PIL import Image, ImageDraw, ImageFont

FONTS = Path(matplotlib.get_data_path()) / "fonts/ttf"
GUTTER = 56  # left of each panel, holds the letter
LABEL_SIZE = 38
NAME_SIZE = 24
HEADER = 46  # above each panel, holds the letter and full species name
CAPTION_SIZE = 21
CAPTION_MARGIN = 24
INK = "#000000"
HEATMAP_TEXT = ("Colour shows the share of bodies of known truth where the predicted species is right, on a 40 by 40 grid; "
           "cells with fewer than 5 known bodies are blank. Curves above and beside each map show the share along that "
           "axis, omitting bins with fewer than 20 known bodies. Dashed lines mark limits that apply to all of the "
           "species' bodies. Predictions are shown before the limits are applied, and axis ranges differ between panels.")


def panel_text(n):
    """What the caption says about one panel, from the numbers in panels.json."""
    text = f"{n['right']:,} right and {n['wrong']:,} wrong"
    if n["removed_right"] or n["removed_wrong"]:
        text += f"; dashed limits remove {n['removed_wrong']:,} wrong and {n['removed_right']:,} right"
    elif not n["any_limit"]:
        text += "; no limit applied"
    if n["scoped_limit"]:
        text += "; limits for one body type or atmosphere are not drawn"
    return text


SCATTER_TEXT = ("Each point is a body of known truth: blue where the predicted species is right, orange where it is wrong, "
                "drawn in random order so neither colour is always on top. Axes span the central 99.8% of known bodies on "
                "each axis, extended to any drawn limit, so a few extreme bodies fall outside. Dashed lines mark limits "
                "that apply to all of the species' bodies. Predictions are shown before the limits are applied, and axis "
                "ranges differ between panels.")
KINDS = {"heatmap": ("heatmaps", HEATMAP_TEXT), "scatter": ("scatter plots", SCATTER_TEXT)}


def draw_caption(runs, width, background):
    """Caption as an image: (bold?, text) runs wrapped by word to width."""
    regular = ImageFont.truetype(str(FONTS / "DejaVuSans.ttf"), CAPTION_SIZE)
    bold = ImageFont.truetype(str(FONTS / "DejaVuSans-Bold.ttf"), CAPTION_SIZE)
    words = [(bold if is_bold else regular, word) for is_bold, text in runs for word in text.split(" ")]
    space = regular.getlength(" ")
    usable = width - 2 * CAPTION_MARGIN
    lines, line, used = [], [], 0.0
    for font, word in words:
        w = font.getlength(word)
        if line and used + w > usable:
            lines.append(line)
            line, used = [], 0.0
        line.append((font, word, used))
        used += w + space
    lines.append(line)
    pitch = int(CAPTION_SIZE * 1.4)
    image = Image.new("RGB", (width, CAPTION_MARGIN + pitch * len(lines) + CAPTION_MARGIN // 2), background)
    draw = ImageDraw.Draw(image)
    for row, line in enumerate(lines):
        for font, word, x in line:
            draw.text((CAPTION_MARGIN + x, CAPTION_MARGIN // 2 + row * pitch), word, font=font, fill=INK)
    return image


def composite(kind, genus, paths, numbers):
    species = [p.stem.replace("_", " ") for p in paths]
    runs = [(True, f"Temperature and gravity {KINDS[kind][0]} for {genus} species."), (False, KINDS[kind][1])]
    for i, s in enumerate(species):
        end = "." if i == len(species) - 1 else ";"
        runs += [(True, f"({string.ascii_uppercase[i]})"), (False, f"{s.split(' ', 1)[1]}: {panel_text(numbers[s])}{end}")]
    return build_sheet(paths, species, runs, 2 if len(paths) <= 4 else 3)


def build_sheet(paths, names, runs, columns):
    """Panels in a grid, each with a bold letter and its name above, and the caption runs below."""
    panels = [Image.open(p).convert("RGB") for p in paths]
    width, height = panels[0].size
    if any(p.size != panels[0].size for p in panels):
        sys.exit(f"Panels differ in size: {paths}")
    rows = (len(panels) + columns - 1) // columns
    total = columns * (width + GUTTER)
    caption = draw_caption(runs, total, panels[0].getpixel((0, 0)))
    sheet = Image.new("RGB", (total, rows * (height + HEADER) + caption.height), panels[0].getpixel((0, 0)))
    draw = ImageDraw.Draw(sheet)
    font = ImageFont.truetype(str(FONTS / "DejaVuSans-Bold.ttf"), LABEL_SIZE)
    name_font = ImageFont.truetype(str(FONTS / "DejaVuSans.ttf"), NAME_SIZE)
    for i, panel in enumerate(panels):
        x, y = (i % columns) * (width + GUTTER), (i // columns) * (height + HEADER)
        sheet.paste(panel, (x + GUTTER, y + HEADER))
        draw.text((x + GUTTER + 16, y + 14), names[i], font=name_font, fill=INK)
        draw.text((x + 8, y + 6), string.ascii_uppercase[i], font=font, fill=INK)
    sheet.paste(caption, (0, rows * (height + HEADER)))
    return sheet


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--panels", type=Path, required=True, help="panels/ directory of a temp_gravity_rules.py or temp_gravity_scatter.py run")
    ap.add_argument("--kind", choices=KINDS, default="heatmap")
    ap.add_argument("--output", type=Path, required=True, help="New output directory (must not exist)")
    args = ap.parse_args()
    if args.output.exists():
        sys.exit(f"Output directory already exists: {args.output}")
    args.output.mkdir(parents=True)

    numbers = json.loads((args.panels / "panels.json").read_text())
    genera = defaultdict(list)
    for p in sorted(args.panels.glob("*.png")):
        genera[p.stem.split("_")[0]].append(p)
    key = ["genus\tpanel\tspecies"]
    for genus, paths in sorted(genera.items()):
        composite(args.kind, genus, paths, numbers).save(args.output / f"{genus}.png", optimize=True)
        key += [f"{genus}\t{string.ascii_uppercase[i]}\t{p.stem.replace('_', ' ')}" for i, p in enumerate(paths)]
    (args.output / "panels.tsv").write_text("\n".join(key) + "\n")
    print(f"{len(genera)} genera, {len(key) - 1} panels -> {args.output}")


if __name__ == "__main__":
    main()
