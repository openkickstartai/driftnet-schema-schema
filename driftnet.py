"""DriftNet core — AST-based implicit schema extraction and drift comparison."""
import ast
import re
import yaml
from pathlib import Path

SQL_RE = re.compile(r"\bSELECT\s+(.+?)\s+FROM\s+(\w+)", re.I | re.S)
COL_METHODS = {"groupby", "merge", "sort_values", "drop", "fillna", "drop_duplicates"}
COL_KW = {"on", "left_on", "right_on", "by", "subset", "columns"}


class Extractor(ast.NodeVisitor):
    def __init__(self):
        self.schemas = {}

    def _add(self, src, col, line):
        self.schemas.setdefault(src, {}).setdefault(col, []).append(line)

    def _name(self, n):
        if isinstance(n, ast.Name):
            return n.id
        if isinstance(n, ast.Attribute):
            return f"{self._name(n.value)}.{n.attr}"
        if isinstance(n, ast.Subscript):
            return self._name(n.value)
        if isinstance(n, ast.Call):
            return self._name(n.func)
        return "_"

    def _strings(self, n):
        if isinstance(n, ast.Constant) and isinstance(n.value, str):
            return [n.value]
        if isinstance(n, ast.List):
            return [e.value for e in n.elts
                    if isinstance(e, ast.Constant) and isinstance(e.value, str)]
        return []

    def visit_Subscript(self, node):
        if isinstance(node.slice, ast.Constant) and isinstance(node.slice.value, str):
            self._add(self._name(node.value), node.slice.value, node.lineno)
        self.generic_visit(node)

    def visit_Call(self, node):
        if isinstance(node.func, ast.Attribute):
            method = node.func.attr
            src = self._name(node.func.value)
            if method in COL_METHODS:
                for a in node.args:
                    for c in self._strings(a):
                        self._add(src, c, node.lineno)
                for kw in node.keywords:
                    if kw.arg in COL_KW:
                        for c in self._strings(kw.value):
                            self._add(src, c, node.lineno)
            if method == "astype":
                for a in node.args:
                    if isinstance(a, ast.Dict):
                        for k in a.keys:
                            if isinstance(k, ast.Constant) and isinstance(k.value, str):
                                self._add(src, k.value, node.lineno)
        self.generic_visit(node)

    def visit_Constant(self, node):
        if isinstance(node.value, str):
            for m in SQL_RE.finditer(node.value):
                cols_raw, tbl = m.group(1).strip(), m.group(2)
                if cols_raw != "*":
                    for c in cols_raw.split(","):
                        c = c.strip().split()[-1].split(".")[-1]
                        if c.isidentifier():
                            self._add(tbl, c, node.lineno)
        self.generic_visit(node)


def extract(code, filename="<stdin>"):
    ext = Extractor()
    ext.visit(ast.parse(code, filename))
    return {
        src: {"columns": sorted(refs.keys()),
              "references": {c: lines for c, lines in sorted(refs.items())}}
        for src, refs in ext.schemas.items()
    }


def compare(contract, actual):
    drifts = []
    for src, spec in contract.items():
        if src not in actual:
            continue
        expected = set(spec.get("columns", []))
        real = set(actual[src].get("columns", []))
        refs = spec.get("references", {})
        for col in sorted(expected - real):
            drifts.append({"type": "missing", "source": src, "column": col,
                           "lines": refs.get(col, []),
                           "message": f"Column '{col}' used in code but missing upstream in '{src}'"})
        for col in sorted(real - expected):
            drifts.append({"type": "added", "source": src, "column": col,
                           "lines": [],
                           "message": f"New column '{col}' in '{src}' (unused)"})
    return drifts


def save_yaml(data, path):
    Path(path).write_text(yaml.dump(data, default_flow_style=False, sort_keys=True))


def load_yaml(path):
    return yaml.safe_load(Path(path).read_text()) or {}
