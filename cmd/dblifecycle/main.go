package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"text/tabwriter"
	"time"

	"github.com/uday68/db-lifecycle/pkg/migrator"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	command := os.Args[1]

	fs := flag.NewFlagSet(command, flag.ExitOnError)
	migrationsDir := fs.String("dir", "./migrations", "Path to migrations directory")
	databaseURL := fs.String("db", "", "PostgreSQL connection URL (or env DB_URL)")
	lockTimeout := fs.Duration("lock-timeout", 10*time.Second, "Advisory lock acquisition timeout")

	_ = fs.Parse(os.Args[2:])

	dbURL := *databaseURL
	if dbURL == "" {
		dbURL = os.Getenv("DB_URL")
	}

	var driver migrator.Driver
	var locker migrator.Locker

	if dbURL == "" {
		// Default to in-memory driver when running locally without active PostgreSQL instance
		memDriver := migrator.NewMemoryDriver()
		driver = memDriver
		locker = memDriver
	} else {
		pgDriver, err := migrator.NewPostgresDriver(dbURL)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error connecting to PostgreSQL: %v\n", err)
			os.Exit(1)
		}
		defer pgDriver.Close()
		driver = pgDriver
		locker = pgDriver
	}

	m := migrator.NewMigrator(migrator.MigratorConfig{
		MigrationsDir: *migrationsDir,
		LockTimeout:   *lockTimeout,
	}, driver, locker)

	ctx := context.Background()

	switch command {
	case "status":
		statuses, err := m.Status(ctx)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Status check failed: %v\n", err)
			os.Exit(1)
		}

		w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
		fmt.Fprintln(w, "VERSION\tNAME\tSTATUS\tEXEC_TIME\tAPPLIED_AT")
		for _, s := range statuses {
			execTime := "-"
			appliedAt := "-"
			if s.Status == "APPLIED" {
				execTime = fmt.Sprintf("%dms", s.ExecutionTimeMs)
				appliedAt = s.AppliedAt.Format("2006-01-02 15:04:05")
			}
			fmt.Fprintf(w, "%d\t%s\t%s\t%s\t%s\n", s.Version, s.Name, s.Status, execTime, appliedAt)
		}
		w.Flush()

	case "plan":
		pending, err := m.Plan(ctx)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Plan computation failed: %v\n", err)
			os.Exit(1)
		}

		if len(pending) == 0 {
			fmt.Println("No pending migrations. Database schema is up to date.")
			return
		}

		fmt.Printf("Dry-Run Migration Plan (%d pending migrations):\n", len(pending))
		for _, mig := range pending {
			txMode := "Transactional"
			if mig.NoTransaction {
				txMode = "Non-Transactional (-- +migrate no-transaction)"
			}
			fmt.Printf("  -> [%d] %s (%s, checksum: %.8s...)\n", mig.Version, mig.Name, txMode, mig.Checksum)
		}

	case "up":
		executed, err := m.Up(ctx)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Migration failed: %v\n", err)
			os.Exit(1)
		}

		if len(executed) == 0 {
			fmt.Println("No migrations applied. Schema is up to date.")
			return
		}

		fmt.Printf("Successfully applied %d migrations:\n", len(executed))
		for _, rec := range executed {
			fmt.Printf("  [OK] Version %d (%s) applied in %dms\n", rec.Version, rec.Name, rec.ExecutionTimeMs)
		}

	case "validate":
		if err := m.Validate(ctx); err != nil {
			fmt.Fprintf(os.Stderr, "Validation failed: %v\n", err)
			os.Exit(1)
		}
		fmt.Println("Validation passed: All migration files and database checksums match.")

	case "version":
		currVersion, err := m.Version(ctx)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Version retrieval failed: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("Current applied schema version: %d\n", currVersion)

	default:
		fmt.Fprintf(os.Stderr, "Unknown command '%s'\n", command)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println("Usage: dblifecycle <command> [options]")
	fmt.Println("Commands:")
	fmt.Println("  status     Display current schema version and migration history")
	fmt.Println("  plan       Compute and display pending migrations without applying changes (dry-run)")
	fmt.Println("  up         Apply eligible pending migrations in order with advisory lock")
	fmt.Println("  validate   Verify disk migration files and stored metadata checksums")
	fmt.Println("  version    Display the current highest applied schema version")
}
