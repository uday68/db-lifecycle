package migrator

import (
	"context"
	"testing"
)

func BenchmarkMigrator_ComputeChecksum(b *testing.B) {
	sqlContent := `
		CREATE TABLE accounts (
			id BIGSERIAL PRIMARY KEY,
			account_number VARCHAR(64) NOT NULL UNIQUE,
			organization_id UUID NOT NULL,
			currency VARCHAR(3) NOT NULL DEFAULT 'USD',
			balance_cents BIGINT NOT NULL DEFAULT 0,
			status VARCHAR(32) NOT NULL DEFAULT 'ACTIVE',
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		);
		CREATE INDEX idx_accounts_org ON accounts(organization_id);
		CREATE INDEX idx_accounts_status ON accounts(status);
	`

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = ComputeChecksum(sqlContent)
	}
}

func BenchmarkMigrator_PlanOverhead(b *testing.B) {
	dir := createTempMigrations(&testing.T{}, map[string]string{
		"001_create_accounts.sql": "CREATE TABLE accounts (id INT);",
		"002_add_index.sql":       "CREATE INDEX idx ON accounts(id);",
		"003_add_column.sql":      "ALTER TABLE accounts ADD COLUMN col TEXT;",
	})

	driver := NewMemoryDriver()
	migrator := NewMigrator(MigratorConfig{MigrationsDir: dir}, driver, driver)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := migrator.Plan(ctx)
		if err != nil {
			b.Fatalf("plan failed: %v", err)
		}
	}
}
