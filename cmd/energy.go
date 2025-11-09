package cmd

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

var (
	energySQLitePath string
	energyMySQLDSN   string
	energyEntity     string
)

// energyCmd migrates smart socket telemetry for the smart socket device.
var energyCmd = &cobra.Command{
	Use:   "energy",
	Short: "Export Home Assistant energy metrics into MySQL",
	Long:  "Reads smart socket telemetry (power, voltage, current, etc.) for the specified entity family and upserts it into a MySQL table.",
	RunE: func(cmd *cobra.Command, args []string) error {
		if energySQLitePath == "" {
			return errors.New("sqlite database path is required")
		}
		if energyMySQLDSN == "" {
			return errors.New("mysql dsn is required")
		}
		if energyEntity == "" {
			return errors.New("entity is required")
		}

		ctx := cmd.Context()
		if ctx == nil {
			ctx = context.Background()
		}

		return transferEnergyData(ctx, energySQLitePath, energyMySQLDSN, energyEntity)
	},
}

func init() {
	energyCmd.Flags().StringVar(&energySQLitePath, "sqlite", "", "Path to the Home Assistant SQLite recorder database")
	energyCmd.Flags().StringVar(&energyMySQLDSN, "dsn", "", "MySQL DSN, e.g. user:password@tcp(host:3306)/database")
	energyCmd.Flags().StringVar(&energyEntity, "entity", "", "Entity slug to export (match prefix for related sensors)")
	_ = energyCmd.MarkFlagRequired("sqlite")
	_ = energyCmd.MarkFlagRequired("dsn")
	_ = energyCmd.MarkFlagRequired("entity")

	rootCmd.AddCommand(energyCmd)
}

func transferEnergyData(ctx context.Context, sqlitePath, mysqlDSN, entitySlug string) error {
	mysqlDSN = ensureParseTimeEnabled(mysqlDSN)
	if err := maybeRegisterTiDBTLS(mysqlDSN); err != nil {
		return fmt.Errorf("configure mysql tls: %w", err)
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

	if err := ensureEnergyPointsTable(ctx, mysqlDB); err != nil {
		return fmt.Errorf("ensure energy_points table: %w", err)
	}

	entityWatermarks, err := loadEnergyEntityWatermarks(ctx, mysqlDB)
	if err != nil {
		return fmt.Errorf("load energy checkpoints: %w", err)
	}

	const queryPrefix = `
SELECT
    s.state_id,
    sm.entity_id,
    s.state,
    s.last_updated_ts,
    COALESCE(sa.shared_attrs, '')
FROM states s
JOIN states_meta sm ON s.metadata_id = sm.metadata_id
LEFT JOIN state_attributes sa ON s.attributes_id = sa.attributes_id
`

	query := queryPrefix + "WHERE sm.entity_id LIKE ? ORDER BY sm.entity_id, s.last_updated_ts"
	entityPattern := "%" + entitySlug + "%"

	rows, err := sqliteDB.QueryContext(ctx, query, entityPattern)
	if err != nil {
		return fmt.Errorf("query sqlite database: %w", err)
	}
	defer rows.Close()

	const upsertPrefix = `
INSERT INTO energy_points(
    entity_id,
    state,
    numeric_state,
    unit,
    device_class,
    state_class,
    friendly_name,
    last_updated
) VALUES`
	const upsertSuffix = `
ON DUPLICATE KEY UPDATE
    entity_id = VALUES(entity_id),
    state = VALUES(state),
    numeric_state = VALUES(numeric_state),
    unit = VALUES(unit),
    device_class = VALUES(device_class),
    state_class = VALUES(state_class),
    friendly_name = VALUES(friendly_name),
    last_updated = VALUES(last_updated)
`

	const energyBatchSize = 500

	var (
		args          []any
		valueSegments strings.Builder
		rowCount      int
	)
	valueSegments.Grow(256)

	flushBatch := func() error {
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

		valueSegments.Reset()
		args = args[:0]
		rowCount = 0
		return nil
	}

	appendRow := func(row energyRow) error {
		if rowCount > 0 {
			valueSegments.WriteString(",")
		}
		valueSegments.WriteString("\n    (?, ?, ?, ?, ?, ?, ?, ?)")

		args = append(args,
			row.entityID,
			row.state,
			row.numericState,
			row.meta.Unit,
			row.meta.DeviceClass,
			row.meta.StateClass,
			row.meta.FriendlyName,
			row.lastUpdated,
		)

		if row.lastUpdated.Valid {
			if current, ok := entityWatermarks[row.entityID]; !ok || row.lastUpdated.Time.After(current) {
				entityWatermarks[row.entityID] = row.lastUpdated.Time
			}
		}

		rowCount++

		if rowCount >= energyBatchSize {
			return flushBatch()
		}
		return nil
	}

	averager := newMinuteAverager(appendRow)

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

		lastUpdated, err := floatToNullTime(lastUpdatedVal)
		if err != nil {
			return fmt.Errorf("convert last_updated_ts for state_id %d: %w", stateID, err)
		}

		if lastUpdated.Valid {
			if watermark, ok := entityWatermarks[entityID]; ok {
				if !lastUpdated.Time.After(watermark) {
					continue
				}
			}
		}

		meta, err := extractEnergyMetadata(attributesJSON)
		if err != nil {
			return fmt.Errorf("parse attributes for state_id %d: %w", stateID, err)
		}

		numericState := parseNumericState(state)
		row := energyRow{
			stateID:      stateID,
			entityID:     entityID,
			state:        state,
			numericState: numericState,
			meta:         meta,
			lastUpdated:  lastUpdated,
		}

		if shouldAggregateRow(row) {
			if err := averager.Add(row); err != nil {
				return err
			}
			continue
		}

		if err := averager.Flush(); err != nil {
			return err
		}

		if err := appendRow(row); err != nil {
			return err
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate sqlite rows: %w", err)
	}

	if err := averager.Flush(); err != nil {
		return err
	}

	return flushBatch()
}

type energyMetadata struct {
	Unit         sql.NullString
	DeviceClass  sql.NullString
	StateClass   sql.NullString
	FriendlyName sql.NullString
}

func extractEnergyMetadata(raw string) (energyMetadata, error) {
	meta := energyMetadata{}
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return meta, nil
	}

	var attrs map[string]any
	if err := json.Unmarshal([]byte(trimmed), &attrs); err != nil {
		return meta, fmt.Errorf("unmarshal shared_attrs: %w", err)
	}

	if v, ok := pickString(attrs["unit_of_measurement"]); ok {
		meta.Unit = sql.NullString{String: v, Valid: true}
	}
	if v, ok := pickString(attrs["device_class"]); ok {
		meta.DeviceClass = sql.NullString{String: v, Valid: true}
	}
	if v, ok := pickString(attrs["state_class"]); ok {
		meta.StateClass = sql.NullString{String: v, Valid: true}
	}
	if v, ok := pickString(attrs["friendly_name"]); ok {
		meta.FriendlyName = sql.NullString{String: v, Valid: true}
	}

	return meta, nil
}

func parseNumericState(raw string) sql.NullFloat64 {
	if raw == "" {
		return sql.NullFloat64{}
	}
	f, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return sql.NullFloat64{}
	}
	return sql.NullFloat64{Float64: f, Valid: true}
}

func ensureEnergyPointsTable(ctx context.Context, db *sql.DB) error {
	const (
		mysqlErrDuplicateKey = 1061
		mysqlErrCantDrop     = 1091
	)

	const ddl = `
CREATE TABLE IF NOT EXISTS energy_points (
    state_id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    entity_id VARCHAR(255) NOT NULL,
    state VARCHAR(255) NOT NULL,
    numeric_state DOUBLE NULL,
    unit VARCHAR(64) NULL,
    device_class VARCHAR(64) NULL,
    state_class VARCHAR(64) NULL,
    friendly_name VARCHAR(255) NULL,
    last_updated DATETIME NULL
)
`

	if _, err := db.ExecContext(ctx, ddl); err != nil {
		return err
	}

	const modifyStmt = `
ALTER TABLE energy_points
MODIFY COLUMN state_id BIGINT NOT NULL AUTO_INCREMENT
`
	if _, err := db.ExecContext(ctx, modifyStmt); err != nil {
		return fmt.Errorf("ensure auto increment state_id: %w", err)
	}

	dropAttrStmt := `
ALTER TABLE energy_points
DROP COLUMN attributes
`
	if _, err := db.ExecContext(ctx, dropAttrStmt); err != nil {
		if !isMySQLError(err, mysqlErrCantDrop) {
			return fmt.Errorf("drop legacy attributes column: %w", err)
		}
	}

	stmt := `
ALTER TABLE energy_points
ADD INDEX idx_energy_points_entity_last_updated (entity_id, last_updated)
`
	if _, err := db.ExecContext(ctx, stmt); err != nil {
		if !isMySQLError(err, mysqlErrDuplicateKey) {
			return fmt.Errorf("add supporting index: %w", err)
		}
	}

	return nil
}

func loadEnergyEntityWatermarks(ctx context.Context, db *sql.DB) (map[string]time.Time, error) {
	const query = `
SELECT entity_id, MAX(last_updated)
FROM energy_points
GROUP BY entity_id
`
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	watermarks := make(map[string]time.Time)
	for rows.Next() {
		var (
			entityID string
			ts       sql.NullTime
		)
		if err := rows.Scan(&entityID, &ts); err != nil {
			return nil, err
		}
		if ts.Valid {
			watermarks[entityID] = ts.Time
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return watermarks, nil
}

type energyRow struct {
	stateID      int64
	entityID     string
	state        string
	numericState sql.NullFloat64
	meta         energyMetadata
	lastUpdated  sql.NullTime
}

var energyMinuteAverageTokens = []string{"_voltage", "_current", "_current_consumption"}

func shouldAggregateRow(row energyRow) bool {
	return row.lastUpdated.Valid && row.numericState.Valid && needsMinuteAverage(row.entityID)
}

func needsMinuteAverage(entityID string) bool {
	lowered := strings.ToLower(entityID)
	for _, token := range energyMinuteAverageTokens {
		if strings.Contains(lowered, token) {
			return true
		}
	}
	return false
}

type minuteAverager struct {
	emit func(energyRow) error

	active       bool
	entityID     string
	minute       time.Time
	sum          float64
	count        int
	maxTime      time.Time
	maxTimeValid bool
	stateID      int64
	meta         energyMetadata
}

func newMinuteAverager(emit func(energyRow) error) *minuteAverager {
	return &minuteAverager{emit: emit}
}

func (m *minuteAverager) Add(row energyRow) error {
	minute := row.lastUpdated.Time.Truncate(time.Minute)
	if m.active {
		if row.entityID != m.entityID || !minute.Equal(m.minute) {
			if err := m.Flush(); err != nil {
				return err
			}
		}
	}
	if !m.active {
		m.active = true
		m.entityID = row.entityID
		m.minute = minute
		m.sum = 0
		m.count = 0
		m.maxTime = time.Time{}
		m.maxTimeValid = false
	}

	m.sum += row.numericState.Float64
	m.count++

	if !m.maxTimeValid || row.lastUpdated.Time.After(m.maxTime) || (row.lastUpdated.Time.Equal(m.maxTime) && row.stateID > m.stateID) {
		m.maxTime = row.lastUpdated.Time
		m.maxTimeValid = true
		m.stateID = row.stateID
		m.meta = row.meta
	}

	return nil
}

func (m *minuteAverager) Flush() error {
	if !m.active {
		return nil
	}
	defer m.reset()
	if m.count == 0 || !m.maxTimeValid {
		return nil
	}

	avg := m.sum / float64(m.count)
	row := energyRow{
		stateID:      m.stateID,
		entityID:     m.entityID,
		state:        strconv.FormatFloat(avg, 'f', -1, 64),
		numericState: sql.NullFloat64{Float64: avg, Valid: true},
		meta:         m.meta,
		lastUpdated:  sql.NullTime{Time: m.maxTime, Valid: true},
	}

	return m.emit(row)
}

func (m *minuteAverager) reset() {
	m.active = false
	m.entityID = ""
	m.minute = time.Time{}
	m.sum = 0
	m.count = 0
	m.maxTime = time.Time{}
	m.maxTimeValid = false
	m.stateID = 0
	m.meta = energyMetadata{}
}
