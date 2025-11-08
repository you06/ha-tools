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
  The tool automatically appends `parseTime=true` if it is not present.

If the MySQL connection is successful, the command will ensure the `gps_points`
table and supporting indexes exist, then upsert rows for every state entry that
contains latitude and longitude attributes.

## energy command

The `energy` subcommand exports all state updates emitted by the Home Assistant
entities that belong to the smart socket you specify via the `--entity` flag
(power, current, voltage, consumption, switches, etc.). Each matching state row
is stored in an `energy_points` table with both the raw value and parsed
metadata such as unit, device class, and friendly name.

```bash
./ha-tools energy --sqlite=/path/to/home-assistant_v2.db --dsn='user:pass@tcp(host:3306)/database' --entity=my_socket
```

- `--sqlite` (required): Path to Home Assistant's recorder SQLite database.
- `--dsn` (required): MySQL DSN (TiDB TLS is supported the same way as `gps`; `parseTime=true`
  is appended automatically if omitted).
- `--entity` (required): Entity slug (e.g., `smart_socket`); the exporter grabs every entity_id containing this substring.

The command mirrors the `gps` behavior: it will create the target table (if
needed), add an `entity_id`/`last_updated` index, and upsert each Home Assistant
state row so the external database always has the latest telemetry.
