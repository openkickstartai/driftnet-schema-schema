#!/usr/bin/env python3
"""DriftNet CLI — extract implicit schema contracts, detect upstream drift."""
import argparse
import sys
from pathlib import Path
from driftnet import extract, compare, save_yaml, load_yaml


def cmd_extract(args):
    merged = {}
    for p in args.files:
        path = Path(p)
        if not path.exists():
            print(f"[SKIP] {p} not found", file=sys.stderr)
            continue
        schema = extract(path.read_text(), str(path))
        for src, spec in schema.items():
            if src in merged:
                old = merged[src]
                all_cols = set(old["columns"]) | set(spec["columns"])
                refs = dict(old["references"])
                for c, lines in spec["references"].items():
                    refs[c] = refs.get(c, []) + lines
                merged[src] = {"columns": sorted(all_cols), "references": refs}
            else:
                merged[src] = spec
        n_cols = sum(len(s["columns"]) for s in schema.values())
        print(f"[SCAN] {p}: {n_cols} columns from {len(schema)} sources")
    out = args.output or "driftnet-contract.yaml"
    save_yaml(merged, out)
    total = sum(len(s["columns"]) for s in merged.values())
    print(f"[OK] Contract saved: {out} ({total} columns, {len(merged)} sources)")


def cmd_check(args):
    contract = load_yaml(args.contract)
    actual = load_yaml(args.actual)
    drifts = compare(contract, actual)
    if not drifts:
        print("[OK] No schema drift detected.")
        return
    for d in drifts:
        icon = "\U0001f534" if d["type"] == "missing" else "\U0001f7e1"
        loc = f" (lines {d['lines']})" if d["lines"] else ""
        print(f"  {icon} [{d['type'].upper()}] {d['message']}{loc}")
    print(f"\n[DRIFT] {len(drifts)} issue(s) found.")
    if any(d["type"] == "missing" for d in drifts):
        sys.exit(1)


def main():
    p = argparse.ArgumentParser(prog="driftnet", description="Schema drift sentinel")
    sub = p.add_subparsers(dest="command")
    p_ext = sub.add_parser("extract", help="Extract implicit schema from Python files")
    p_ext.add_argument("files", nargs="+", help="Python files to scan")
    p_ext.add_argument("-o", "--output", help="Output YAML path")
    p_chk = sub.add_parser("check", help="Compare contract vs actual schema")
    p_chk.add_argument("contract", help="Contract YAML")
    p_chk.add_argument("actual", help="Actual upstream schema YAML")
    args = p.parse_args()
    if not args.command:
        p.print_help()
        sys.exit(1)
    {"extract": cmd_extract, "check": cmd_check}[args.command](args)


if __name__ == "__main__":
    main()
