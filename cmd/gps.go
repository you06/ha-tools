package cmd

import (
	"context"
	"crypto/tls"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net"
	"strconv"
	"strings"
	"time"

	_ "github.com/glebarez/sqlite"
	"github.com/go-sql-driver/mysql"
	"github.com/spf13/cobra"
)

var (
	gpsSQLitePath string
	gpsMySQLDSN   string
)

// gpsCmd migrates GPS state data from Home Assistant's recorder database into MySQL.
var gpsCmd = &cobra.Command{
	Use:   "gps",
	Short: "Export Home Assistant GPS entries into MySQL",
	Long:  "Reads latitude and longitude updates from the Home Assistant SQLite recorder database and upserts them into a MySQL table for external consumption.",
	RunE: func(cmd *cobra.Command, args []string) error {
		if gpsSQLitePath == "" {
			return errors.New("sqlite database path is required")
		}
		if gpsMySQLDSN == "" {
			return errors.New("mysql dsn is required")
		}

		ctx := cmd.Context()
		if ctx == nil {
			ctx = context.Background()
		}

		return transferGPSData(ctx, gpsSQLitePath, gpsMySQLDSN)
	},
}

func init() {
	gpsCmd.Flags().StringVar(&gpsSQLitePath, "sqlite", "", "Path to the Home Assistant SQLite recorder database")
	gpsCmd.Flags().StringVar(&gpsMySQLDSN, "dsn", "", "MySQL DSN, e.g. user:password@tcp(host:3306)/database")
	_ = gpsCmd.MarkFlagRequired("sqlite")
	_ = gpsCmd.MarkFlagRequired("dsn")

	rootCmd.AddCommand(gpsCmd)
}

func transferGPSData(ctx context.Context, sqlitePath, mysqlDSN string) error {
	// TLS config for tidb cloud.
	if strings.Contains(mysqlDSN, "tls=tidb") {
		dummyDSN := strings.ReplaceAll(mysqlDSN, "tls=tidb", "tls=")
		cfg, err := mysql.ParseDSN(dummyDSN)
		if err != nil {
			return fmt.Errorf("parse mysql dsn: %w", err)
		}

		serverName := cfg.Addr
		if host, _, splitErr := net.SplitHostPort(serverName); splitErr == nil {
			serverName = host
		}
		if len(serverName) == 0 {
			serverName = "localhost"
		}

		if err := mysql.RegisterTLSConfig("tidb", &tls.Config{
			MinVersion: tls.VersionTLS12,
			ServerName: serverName,
		}); err != nil && !strings.Contains(err.Error(), "already registered") {
			return fmt.Errorf("register tls config %q: %w", "tidb", err)
		}
	}

	sqliteDB, err := sql.Open("sqlite", sqlitePath)
	if err != nil {
		return fmt.Errorf("open sqlite database: %w", err)
	}
	defer sqliteDB.Close()
	sqliteDB.SetMaxOpenConns(1)

	if err := sqliteDB.PingContext(ctx); err != nil {
		return fmt.Errorf("ping sqlite database: %w", err)
	}

	mysqlDB, err := sql.Open("mysql", mysqlDSN)
	if err != nil {
		return fmt.Errorf("open mysql database: %w", err)
	}
	defer mysqlDB.Close()

	if err := mysqlDB.PingContext(ctx); err != nil {
		return fmt.Errorf("ping mysql database: %w", err)
	}

	if err := ensureGPSPointsTable(ctx, mysqlDB); err != nil {
		return fmt.Errorf("ensure gps_points table: %w", err)
	}

	const query = `
SELECT
    s.state_id,
    sm.entity_id,
    s.state,
    s.last_updated_ts,
    COALESCE(sa.shared_attrs, '')
FROM states s
JOIN state_attributes sa ON s.attributes_id = sa.attributes_id
JOIN states_meta sm ON s.metadata_id = sm.metadata_id
WHERE sa.shared_attrs LIKE '%"latitude"%'
  AND sa.shared_attrs LIKE '%"longitude"%'
`

	rows, err := sqliteDB.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("query sqlite database: %w", err)
	}
	defer rows.Close()

	const upsertPrefix = `
INSERT INTO gps_points(
    state_id, entity_id, state, latitude, longitude, gps_accuracy, last_updated
) VALUES`
	const upsertSuffix = `
ON DUPLICATE KEY UPDATE
    entity_id = VALUES(entity_id),
    state = VALUES(state),
    latitude = VALUES(latitude),
    longitude = VALUES(longitude),
    gps_accuracy = VALUES(gps_accuracy),
    last_updated = VALUES(last_updated)
`

	var (
		args          []any
		valueSegments strings.Builder
		rowCount      int
	)
	valueSegments.Grow(256)
	for rows.Next() {
		var (
			stateID        int64
			entityID       string
			state          string
			lastUpdatedVal sql.NullFloat64
			attributesJSON string
		)

		if err := rows.Scan(&stateID, &entityID, &state, &lastUpdatedVal, &attributesJSON); err != nil {
			return fmt.Errorf("scan sqlite row: %w", err)
		}

		latitude, longitude, accuracy, err := extractCoordinates(attributesJSON)
		if err != nil {
			return fmt.Errorf("parse attributes for state_id %d: %w", stateID, err)
		}
		if !latitude.Valid || !longitude.Valid {
			continue
		}

		lastUpdated, err := floatToNullTime(lastUpdatedVal)
		if err != nil {
			return fmt.Errorf("convert last_updated_ts for state_id %d: %w", stateID, err)
		}

		if rowCount > 0 {
			valueSegments.WriteString(",")
		}
		valueSegments.WriteString("\n    (?, ?, ?, ?, ?, ?, ?)")
		args = append(args,
			stateID,
			entityID,
			state,
			latitude,
			longitude,
			accuracy,
			lastUpdated,
		)
		rowCount++
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate sqlite rows: %w", err)
	}

	if rowCount == 0 {
		return nil
	}

	var queryBuilder strings.Builder
	queryBuilder.Grow(len(upsertPrefix) + valueSegments.Len() + len(upsertSuffix) + 1)
	queryBuilder.WriteString(upsertPrefix)
	queryBuilder.WriteString(valueSegments.String())
	queryBuilder.WriteByte('\n')
	queryBuilder.WriteString(upsertSuffix)

	if _, err := mysqlDB.ExecContext(ctx, queryBuilder.String(), args...); err != nil {
		return fmt.Errorf("upsert mysql rows: %w", err)
	}

	return nil
}

func ensureGPSPointsTable(ctx context.Context, db *sql.DB) error {
	const ddl = `
CREATE TABLE IF NOT EXISTS gps_points (
    state_id BIGINT PRIMARY KEY,
    entity_id VARCHAR(255) NOT NULL,
    state VARCHAR(255) NOT NULL,
    latitude DOUBLE NOT NULL,
    longitude DOUBLE NOT NULL,
    gps_accuracy DOUBLE NULL,
    last_updated DATETIME NULL
)
`

	if _, err := db.ExecContext(ctx, ddl); err != nil {
		return err
	}

	if err := ensureGPSPointsIndexes(ctx, db); err != nil {
		return fmt.Errorf("ensure gps_points indexes: %w", err)
	}

	return nil
}

type gpsIndexInfo struct {
	nonUnique bool
	columns   []string
}

func ensureGPSPointsIndexes(ctx context.Context, db *sql.DB) error {
	schema, err := currentMySQLDatabase(ctx, db)
	if err != nil {
		return err
	}

	query := `
SELECT INDEX_NAME, COLUMN_NAME, NON_UNIQUE, SEQ_IN_INDEX
FROM INFORMATION_SCHEMA.STATISTICS
WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'gps_points'
ORDER BY INDEX_NAME, SEQ_IN_INDEX
`
	rows, err := db.QueryContext(ctx, query, schema)
	if err != nil {
		return err
	}
	defer rows.Close()

	indexes := map[string]*gpsIndexInfo{}
	for rows.Next() {
		var (
			indexName string
			column    sql.NullString
			nonUnique int
			seq       int
		)
		if err := rows.Scan(&indexName, &column, &nonUnique, &seq); err != nil {
			return err
		}
		if !column.Valid {
			continue
		}
		info, ok := indexes[indexName]
		if !ok {
			info = &gpsIndexInfo{
				nonUnique: nonUnique == 1,
				columns:   []string{},
			}
			indexes[indexName] = info
		}
		if len(info.columns) < seq {
			info.columns = append(info.columns, make([]string, seq-len(info.columns))...)
		}
		info.columns[seq-1] = column.String
	}
	if err := rows.Err(); err != nil {
		return err
	}

	if err := ensurePrimaryKeyOnStateID(ctx, db, indexes); err != nil {
		return err
	}

	if err := dropConflictingEntityIndexes(ctx, db, indexes); err != nil {
		return err
	}

	if err := ensureSupportingEntityIndex(ctx, db); err != nil {
		return err
	}

	return nil
}

func ensurePrimaryKeyOnStateID(ctx context.Context, db *sql.DB, indexes map[string]*gpsIndexInfo) error {
	const (
		mysqlErrNoSuchKey = 1091
	)

	primary := indexes["PRIMARY"]
	if primary != nil && len(primary.columns) == 1 && primary.columns[0] == "state_id" {
		return nil
	}

	if _, err := db.ExecContext(ctx, "ALTER TABLE gps_points DROP PRIMARY KEY"); err != nil {
		if !isMySQLError(err, mysqlErrNoSuchKey) {
			return fmt.Errorf("drop existing primary key: %w", err)
		}
	}

	if _, err := db.ExecContext(ctx, "ALTER TABLE gps_points ADD PRIMARY KEY (state_id)"); err != nil {
		return fmt.Errorf("add primary key on state_id: %w", err)
	}

	return nil
}

func dropConflictingEntityIndexes(ctx context.Context, db *sql.DB, indexes map[string]*gpsIndexInfo) error {
	for name, info := range indexes {
		if name == "PRIMARY" || info.nonUnique {
			continue
		}
		if containsString(info.columns, "state_id") {
			continue
		}
		if containsString(info.columns, "entity_id") {
			stmt := fmt.Sprintf("ALTER TABLE gps_points DROP INDEX %s", quoteIdentifier(name))
			if _, err := db.ExecContext(ctx, stmt); err != nil {
				return fmt.Errorf("drop unique index %s: %w", name, err)
			}
		}
	}
	return nil
}

func ensureSupportingEntityIndex(ctx context.Context, db *sql.DB) error {
	const mysqlErrDuplicateKey = 1061

	stmt := `
ALTER TABLE gps_points
ADD INDEX idx_gps_points_entity_last_updated (entity_id, last_updated)
`
	if _, err := db.ExecContext(ctx, stmt); err != nil {
		if !isMySQLError(err, mysqlErrDuplicateKey) {
			return fmt.Errorf("add supporting index: %w", err)
		}
	}
	return nil
}

func currentMySQLDatabase(ctx context.Context, db *sql.DB) (string, error) {
	var schema sql.NullString
	if err := db.QueryRowContext(ctx, "SELECT DATABASE()").Scan(&schema); err != nil {
		return "", fmt.Errorf("detect current database: %w", err)
	}
	if !schema.Valid || schema.String == "" {
		return "", errors.New("mysql dsn must select a database; none detected")
	}
	return schema.String, nil
}

func isMySQLError(err error, code uint16) bool {
	var mysqlErr *mysql.MySQLError
	if errors.As(err, &mysqlErr) {
		return mysqlErr.Number == code
	}
	return false
}

func containsString(in []string, target string) bool {
	for _, v := range in {
		if v == target {
			return true
		}
	}
	return false
}

func quoteIdentifier(id string) string {
	return "`" + strings.ReplaceAll(id, "`", "``") + "`"
}

func extractCoordinates(raw string) (lat sql.NullFloat64, lon sql.NullFloat64, acc sql.NullFloat64, err error) {
	if raw == "" {
		return lat, lon, acc, nil
	}

	var attrs map[string]any
	if err := json.Unmarshal([]byte(raw), &attrs); err != nil {
		return lat, lon, acc, fmt.Errorf("unmarshal shared_attrs: %w", err)
	}

	if v, ok := pickFloat(attrs["latitude"]); ok {
		lat = sql.NullFloat64{Float64: v, Valid: true}
	}
	if v, ok := pickFloat(attrs["longitude"]); ok {
		lon = sql.NullFloat64{Float64: v, Valid: true}
	}
	if v, ok := pickFloat(attrs["gps_accuracy"]); ok {
		acc = sql.NullFloat64{Float64: v, Valid: true}
	}

	return lat, lon, acc, nil
}

func pickFloat(v any) (float64, bool) {
	switch val := v.(type) {
	case nil:
		return 0, false
	case float64:
		return val, true
	case float32:
		return float64(val), true
	case int:
		return float64(val), true
	case int64:
		return float64(val), true
	case json.Number:
		f, err := val.Float64()
		if err != nil {
			return 0, false
		}
		return f, true
	case string:
		if val == "" {
			return 0, false
		}
		f, err := strconv.ParseFloat(val, 64)
		if err != nil {
			return 0, false
		}
		return f, true
	default:
		return 0, false
	}
}

func floatToNullTime(v sql.NullFloat64) (sql.NullTime, error) {
	if !v.Valid {
		return sql.NullTime{}, nil
	}

	seconds, frac := math.Modf(v.Float64)
	if math.IsNaN(seconds) || math.IsNaN(frac) {
		return sql.NullTime{}, errors.New("invalid float for timestamp")
	}

	loc := time.Local
	if loc == nil {
		loc = time.UTC
	}
	t := time.Unix(int64(seconds), int64(frac*1e9)).In(loc)
	if t.IsZero() {
		return sql.NullTime{}, nil
	}

	return sql.NullTime{Time: t, Valid: true}, nil
}
