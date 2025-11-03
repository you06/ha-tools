# HA Tools

Toolbox for Home Assistant.

## gps command

The `gps` subcommand exports latitude and longitude updates from Home Assistant's
`home-assistant_v2.db` SQLite recorder into a MySQL-compatible database. It
creates and maintains a `gps_points` table, keeping it in sync by upserting each
state record that includes GPS attributes.

```bash
./ha-tools gps --sqlite=/path/to/home-assistant_v2.db --dsn='user:pass@tcp(host:3306)/database'
```

- `--sqlite` (required): Path to Home Assistant's recorder SQLite database.
- `--dsn` (required): MySQL DSN, such as
  `user:pass@tcp(host:3306)/database?parseTime=true`. When connecting to TiDB
  Cloud with TLS, append `?tls=tidb` to the DSN; the command pre-registers that
  TLS profile.

If the MySQL connection is successful, the command will ensure the `gps_points`
table and supporting indexes exist, then upsert rows for every state entry that
contains latitude and longitude attributes.
