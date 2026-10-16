
import os
from collections import defaultdict

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

from celestrak.benchmark import QUERIES

DBMS = ["postgres", "mysql", "clickhouse"]
LABELS = {"postgres": "PostgreSQL", "mysql": "MySQL", "clickhouse": "ClickHouse"}
# Categorical slots 1-3 of the reference palette (validated for all pairs, light mode).
COLORS = {"postgres": "#2a78d6", "mysql": "#eb6834", "clickhouse": "#1baf7a"}
SURFACE, TEXT, MUTED, GRID = "#fcfcfb", "#0b0b0b", "#52514e", "#e4e3df"


def _style(ax, title, xlabel):
    ax.set_facecolor(SURFACE)
    ax.set_title(title, loc="left", color=TEXT, fontsize=12, pad=30)
    ax.set_xlabel(xlabel, color=MUTED)
    ax.tick_params(colors=MUTED, length=0)
    ax.grid(axis="x", color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)
    for side in ("top", "right", "left"):
        ax.spines[side].set_visible(False)
    ax.spines["bottom"].set_color(GRID)


def _grouped_barh(path, title, xlabel, groups, values, fmt, log=False):
    """groups: category labels; values[dbms] -> list aligned with groups."""
    n = len(groups)
    fig, ax = plt.subplots(figsize=(9, 0.9 * n + 1.6), facecolor=SURFACE)
    height = 0.26
    for i, dbms in enumerate(DBMS):
        ys = [g + (i - 1) * (height + 0.02) for g in range(n)]  # legend order top-down
        bars = ax.barh(ys, values[dbms], height=height, color=COLORS[dbms],
                       label=LABELS[dbms], edgecolor=SURFACE, linewidth=1)
        for bar, v in zip(bars, values[dbms]):
            ax.text(bar.get_width(), bar.get_y() + height / 2, " " + fmt(v),
                    va="center", fontsize=8, color=MUTED)
    ax.set_yticks(range(n), groups, color=TEXT)
    ax.invert_yaxis()
    if log:
        ax.set_xscale("log")
    ax.xaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:,.10g}"))
    _style(ax, title, xlabel)
    ax.legend(loc="lower left", frameon=False, labelcolor=TEXT, ncols=3,
              bbox_to_anchor=(0, 1.0), fontsize=9, borderaxespad=0.2)
    fig.tight_layout()
    fig.savefig(path, dpi=150, facecolor=SURFACE)
    plt.close(fig)


def render(run_id: str, results: list[tuple], extra: dict, out_dir: str) -> list[str]:
    """results: rows (dbms, test_group, test_name, metric, value, rows) from meta.benchmark_results."""
    os.makedirs(out_dir, exist_ok=True)
    data = defaultdict(dict)  # (group, metric) -> {(dbms, test): (value, rows)}
    for dbms, group, test, metric, value, rows in results:
        data[(group, metric)][(dbms, test)] = (value, rows)

    def series(group, metric, tests, scale=1.0):
        return {d: [data[(group, metric)].get((d, t), (0, 0))[0] * scale for t in tests]
                for d in DBMS}

    # Tables still empty in ODS (e.g. supgp before its first successful fetch) are left out.
    tables = sorted({t for (_, t), (_, rows) in data[("load", "seconds")].items() if rows})
    queries = list(QUERIES)
    files = []

    p = os.path.join(out_dir, "load_throughput.png")
    _grouped_barh(p, "Bulk load: строк в секунду (больше — лучше)", "rows / sec",
                  tables, series("load", "rows_per_sec", tables), lambda v: f"{v:,.0f}")
    files.append(p)

    p = os.path.join(out_dir, "query_latency.png")
    _grouped_barh(p, "Запросы: медиана времени, мс (меньше — лучше, лог. шкала)", "ms",
                  queries, series("query", "seconds", queries, 1000), lambda v: f"{v:,.1f}",
                  log=True)
    files.append(p)

    p = os.path.join(out_dir, "storage_size.png")
    _grouped_barh(p, "Размер на диске с индексами, МБ (меньше — лучше)", "MB",
                  tables, series("storage", "bytes", tables, 1 / 2**20), lambda v: f"{v:,.1f}")
    files.append(p)

    md = [f"# Результаты storage_benchmark\n\nRun: `{run_id}`\n"]
    md.append("## Объём данных\n\n| Таблица | Строк | CSV, МБ | Сырые файлы в S3, МБ |\n|---|---:|---:|---:|")
    for t in tables:
        rows = data[("load", "seconds")].get(("postgres", t), (0, 0))[1]
        md.append(f"| {t} | {rows:,} | {extra['csv_bytes'].get(t, 0) / 2**20:,.1f} | "
                  f"{extra['raw_bytes'].get(t, 0) / 2**20:,.1f} |")

    def table(title, group, metric, tests, fmt, scale=1.0, names=None):
        md.append(f"\n## {title}\n\n| | " + " | ".join(LABELS[d] for d in DBMS) + " |")
        md.append("|---|" + "---:|" * len(DBMS))
        for t in tests:
            cells = [fmt(data[(group, metric)].get((d, t), (0, 0))[0] * scale) for d in DBMS]
            label = f"{t} — {names[t]}" if names else t
            md.append(f"| {label} | " + " | ".join(cells) + " |")

    table("Загрузка, секунд", "load", "seconds", tables, lambda v: f"{v:.2f}")
    table("Загрузка, строк/с", "load", "rows_per_sec", tables, lambda v: f"{v:,.0f}")
    table("Запросы, медиана мс", "query", "seconds", queries, lambda v: f"{v:,.1f}", 1000,
          {q: QUERIES[q][0] for q in queries})
    table("Размер на диске, МБ", "storage", "bytes", tables, lambda v: f"{v:,.1f}", 1 / 2**20)
    md.append("\n![load](load_throughput.png)\n\n![queries](query_latency.png)\n\n"
              "![storage](storage_size.png)\n")

    p = os.path.join(out_dir, "results.md")
    with open(p, "w", encoding="utf-8") as f:
        f.write("\n".join(md))
    files.append(p)
    return files
