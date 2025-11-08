package cmd

import (
	"crypto/tls"
	"fmt"
	"net"
	"strings"

	"github.com/go-sql-driver/mysql"
)

// maybeRegisterTiDBTLS registers the tidb TLS profile when requested in the DSN.
func maybeRegisterTiDBTLS(mysqlDSN string) error {
	if !strings.Contains(mysqlDSN, "tls=tidb") {
		return nil
	}

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
	return nil
}

// ensureParseTimeEnabled appends parseTime=true to the DSN when absent so DATETIME values scan as time.Time.
func ensureParseTimeEnabled(mysqlDSN string) string {
	if mysqlDSN == "" {
		return mysqlDSN
	}
	if strings.Contains(strings.ToLower(mysqlDSN), "parsetime=") {
		return mysqlDSN
	}
	if strings.Contains(mysqlDSN, "?") {
		return mysqlDSN + "&parseTime=true"
	}
	return mysqlDSN + "?parseTime=true"
}
