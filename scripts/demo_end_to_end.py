# scripts/demo_end_to_end.py
import argparse

from src.end_to_end import run_and_report, RunAndReportConfig, ReportItem


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="tests/fixtures/nrel_sample.sqlite")
    ap.add_argument("--out", default="reports/demo")
    ap.add_argument("--title", default="Mock Analytics Team Report")
    ap.add_argument("--max_attempts", type=int, default=3)
    ap.add_argument("--preview_rows", type=int, default=12)
    ap.add_argument("--q", action="append", help="Question (repeatable). If omitted, uses a default suite.")
    args = ap.parse_args()

    cfg = RunAndReportConfig(
        db_path=args.db,
        report_title=args.title,
        max_attempts=args.max_attempts,
        preview_rows=args.preview_rows,
    )

    if args.q:
        items = [ReportItem(id=f"q{i+1}", title=f"Query {i+1}", question=q) for i, q in enumerate(args.q)]
    else:
        items = [
            ReportItem("r1", "Stations by state", "How many stations are there by state?"),
            ReportItem("r2", "Top cities by station count", "Top 10 cities by station count"),
            ReportItem("r3", "Restricted access footprint", "How many stations have restricted access?"),
            ReportItem("r4", "Fuel type mix", "Count stations by fuel_type_code"),
            ReportItem("r5", "California station sample", "Show 50 stations in California with station_name and street_address"),
        ]

    out = run_and_report(items, cfg=cfg, out_dir=args.out)
    print(out)


if __name__ == "__main__":
    main()
