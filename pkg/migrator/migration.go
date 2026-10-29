package migrator

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

var migrationFileRegex = regexp.MustCompile(`^(\d+)_([a-zA-Z0-9_\-]+?)(?:\.(up|down))?\.sql$`)

// Migration represents a single parsed versioned SQL migration
type Migration struct {
	Version       int64
	Name          string
	Path          string
	Checksum      string
	RawSQL        string
	NoTransaction bool
}

// ComputeChecksum calculates SHA-256 over normalized migration SQL content
func ComputeChecksum(sqlContent string) string {
	// Normalize line endings to avoid OS-specific checksum discrepancies
	normalized := strings.ReplaceAll(sqlContent, "\r\n", "\n")
	hash := sha256.Sum256([]byte(normalized))
	return hex.EncodeToString(hash[:])
}

// LoadMigrationFile reads, parses, and computes the checksum of an individual SQL file
func LoadMigrationFile(filePath string) (*Migration, error) {
	fileName := filepath.Base(filePath)
	matches := migrationFileRegex.FindStringSubmatch(fileName)
	if len(matches) < 3 {
		return nil, fmt.Errorf("invalid migration filename '%s': expected format <version>_<name>.sql or <version>_<name>.up.sql", fileName)
	}

	version, err := strconv.ParseInt(matches[1], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("parse migration version in '%s': %w", fileName, err)
	}

	name := matches[2]
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("read migration file '%s': %w", filePath, err)
	}

	rawSQL := string(data)
	checksum := ComputeChecksum(rawSQL)

	// Check for non-transactional directive (e.g. for concurrent index builds in Postgres)
	noTx := strings.Contains(rawSQL, "-- +migrate no-transaction")

	return &Migration{
		Version:       version,
		Name:          name,
		Path:          filePath,
		Checksum:      checksum,
		RawSQL:        rawSQL,
		NoTransaction: noTx,
	}, nil
}

// LoadMigrationsFromDir discovers, validates, and sorts all up-migrations in a directory
func LoadMigrationsFromDir(dirPath string) ([]*Migration, error) {
	entries, err := os.ReadDir(dirPath)
	if err != nil {
		return nil, fmt.Errorf("read migrations directory '%s': %w", dirPath, err)
	}

	migrations := make([]*Migration, 0, len(entries))
	versionsSeen := make(map[int64]string)

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}

		// Skip down migrations for forward application
		if strings.HasSuffix(entry.Name(), ".down.sql") {
			continue
		}

		fullPath := filepath.Join(dirPath, entry.Name())
		m, err := LoadMigrationFile(fullPath)
		if err != nil {
			return nil, err
		}

		if existingFile, exists := versionsSeen[m.Version]; exists {
			return nil, fmt.Errorf("duplicate migration version %d detected: '%s' and '%s'", m.Version, existingFile, entry.Name())
		}
		versionsSeen[m.Version] = entry.Name()
		migrations = append(migrations, m)
	}

	// Sort deterministically by version ascending
	sort.Slice(migrations, func(i, j int) bool {
		return migrations[i].Version < migrations[j].Version
	})

	return migrations, nil
}
