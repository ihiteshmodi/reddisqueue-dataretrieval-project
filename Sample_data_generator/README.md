# Sample Data Generator

This folder contains a zero-argument generator script.

## Run

From workspace root:

```bash
python Sample_data_generator/generate_sample_data.py
```

## Behavior

- Always truncates and recreates `master_clientdata.db` in the current working directory.
- Always runs full mode: schema + indexes + data generation.
- Uses hardcoded 6-month date range ending today.
- Uses deterministic seed and fixed hierarchy size with 25 advertisers.
- Adds only ID/date oriented indexes (no name lookup indexes).
