# Streamlit Community Cloud version

This version is designed for Streamlit Community Cloud:

- Website Watcher report upload
- candidate parsing
- tender vs irrelevant classification
- optional one-level HTML/PDF deep enrichment
- review from Postgres

## Required secret

Set one of these in Streamlit secrets:

```toml
DATABASE_URL = "postgresql://USER:PASSWORD@HOST:5432/DBNAME?sslmode=require"
```

or

```toml
[db]
url = "postgresql://USER:PASSWORD@HOST:5432/DBNAME?sslmode=require"
```

## Entrypoint

Use:

`ui/Home.py`

## Notes

- This version avoids SQLite and local persistent file storage.
- PDF enrichment is done in memory.
- Detections are stored in Postgres.
