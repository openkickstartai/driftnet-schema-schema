# DriftNet

Schema drift sentinel. Extracts implicit schema contracts from your Python pipeline code via AST analysis, compares against upstream, catches drift before your pipeline catches fire.

**Zero dependencies on your pipeline. Zero config. Just point and scan.**

## Install

```bash
pip install -r requirements.txt
```

## Usage

### Extract schema contract from code

```bash
python cli.py extract pipeline.py etl/*.py -o contract.yaml
```

Scans Python files, finds every `df['col']`, `.groupby()`, `.merge(on=)`, `SELECT ... FROM`, `data['key']` — outputs a YAML contract with columns and line references.

### Check for drift

```bash
python cli.py check contract.yaml upstream-schema.yaml
```

Compares your code's expectations against the real upstream schema. Exit code 1 if columns your code uses are missing upstream.

Output example:
```
  🔴 [MISSING] Column 'email' used in code but missing upstream in 'users' (lines [45, 67])
  🟡 [ADDED] New column 'email_address' in 'users' (unused)
```

### CI Integration

Add to your GitHub Actions:

```yaml
- run: python cli.py extract src/*.py -o contract.yaml
- run: python cli.py check contract.yaml upstream.yaml
```

Fails the build if upstream drifted.

## What it detects

| Pattern | Example |
|---|---|
| Subscript access | `df['user_id']`, `data['key']` |
| Method column args | `.groupby('region')`, `.sort_values(by=['ts'])` |
| Merge keys | `.merge(other, on='id')`, `left_on=`/`right_on=` |
| Type casts | `.astype({'score': float})` |
| SQL in strings | `"SELECT id, name FROM users"` |
| Nested dict | `data['address']['zip']` |

## Performance

AST parsing is single-pass, O(n) on code size. No tree-sitter runtime needed. Scans 10k LOC in <50ms.

## License

MIT
