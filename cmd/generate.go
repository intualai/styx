package cmd

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"os"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
	"github.com/golang-migrate/migrate"
	_ "github.com/golang-migrate/migrate/database/postgres"
	_ "github.com/golang-migrate/migrate/source/file"
	"github.com/lib/pq"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
)

const DOCKER_POSTGRES_IMAGE = "postgres:16-bookworm"

var (
	inputFile string
	outputDir string
)

var generateCommand = &cobra.Command{
	Use:   "generate",
	Short: "Create/update migrations with an input schema.sql file",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("Generating migrations from %s to %s\n", inputFile, outputDir)
		if err := generateMigrations(inputFile, outputDir); err != nil {
			log.Error().Err(err).Msgf("Failed to generate migrations")
			os.Exit(1)
		}
	},
}

func dumpDatabaseSchema(dsn string) ([]byte, error) {
	db, err := pq.NewConnector(dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	conn := sql.OpenDB(db)
	defer conn.Close()

	query := `
SELECT table_name, column_name, data_type, is_nullable, 
       column_default, character_maximum_length,
       numeric_precision, numeric_scale,
       udt_name, is_identity, identity_generation
FROM information_schema.columns
WHERE table_schema = 'public'
ORDER BY table_name, ordinal_position;`

	rows, err := conn.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to query database schema: %w", err)
	}
	defer rows.Close()

	var schema bytes.Buffer
	for rows.Next() {
		var tableName, columnName, dataType, isNullable string
		var columnDefault, charMaxLen, numPrecision, numScale, udtName, isIdentity, identityGen sql.NullString

		err := rows.Scan(&tableName, &columnName, &dataType, &isNullable,
			&columnDefault, &charMaxLen, &numPrecision, &numScale,
			&udtName, &isIdentity, &identityGen)
		if err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		// Build column definition
		def := fmt.Sprintf("%s.%s %s", tableName, columnName, dataType)
		if charMaxLen.Valid {
			def += fmt.Sprintf("(%s)", charMaxLen.String)
		} else if numPrecision.Valid {
			if numScale.Valid {
				def += fmt.Sprintf("(%s,%s)", numPrecision.String, numScale.String)
			} else {
				def += fmt.Sprintf("(%s)", numPrecision.String)
			}
		}

		if columnDefault.Valid {
			def += " DEFAULT " + columnDefault.String
		}
		if isNullable == "NO" {
			def += " NOT NULL"
		}
		if isIdentity.Valid && isIdentity.String == "YES" {
			def += " GENERATED " + identityGen.String + " AS IDENTITY"
		}

		schema.WriteString(def + "\n")
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	return schema.Bytes(), nil
}

func applyExistingMigrations(migrationsDir, dsn string) error {
	files, err := os.ReadDir(migrationsDir)
	if err != nil && !os.IsNotExist(err) {
		return nil
	}

	if len(files) == 0 {
		log.Info().Msg("No existing migrations found. Continuing")
		return nil
	}

	log.Info().Msg("Applying existing migrations...")

	migrationURL := fmt.Sprintf("file://%s", migrationsDir)
	m, err := migrate.New(migrationURL, dsn)
	if err != nil {
		return fmt.Errorf("failed to initialize `golang-migrate`: %w", err)
	}

	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		return fmt.Errorf("failed to apply migrations to sample container: %w", err)
	}

	return nil
}

// Entrypoint function for the command
func generateMigrations(schemaFile, migrationsDir string) error {
	// 1. Make sure the migrations output dir exists
	// 2. Create postgres:16-bookworm container
	// 3. Apply existing migrations to container. If no migrations in folder, skip this step
	// 4. Dump the current database schema
	// 5. Somehow compare the current database schema to each item in schema.sql
	// 6. Somehow generate a migration changeset

	if err := os.MkdirAll(migrationsDir, 0755); err != nil {
		return fmt.Errorf("failed to create directory %s: %w", migrationsDir, err)
	}

	ctx := context.Background()
	dockerClient, err := client.NewClientWithOpts(client.FromEnv)
	if err != nil {
		return fmt.Errorf("failed to create Docker client: %w", err)
	}

	// Pull the postgres:16-bookworm docker image + start the container
	log.Trace().Msgf("Pulling %s docker image", DOCKER_POSTGRES_IMAGE)
	_, err = dockerClient.ImagePull(ctx, DOCKER_POSTGRES_IMAGE, image.PullOptions{})
	if err != nil {
		return fmt.Errorf("failed to pull PostgreSQL docker image: %w", err)
	}

	log.Trace().Msg("Starting PostgreSQL docker container")
	port := "5432/tcp"
	hostConfig := &container.HostConfig{
		PortBindings: nat.PortMap{
			nat.Port(port): []nat.PortBinding{
				{
					HostIP:   "0.0.0.0",
					HostPort: "5433",
				},
			},
		},
	}

	resp, err := dockerClient.ContainerCreate(
		ctx,
		&container.Config{
			Image: DOCKER_POSTGRES_IMAGE,
			Env: []string{
				"POSTGRES_USER=postgres",
				"POSTGRES_PASSWORD=postgres",
				"POSTGRES_DB=styx",
			},
			ExposedPorts: nat.PortSet{
				nat.Port(port): struct{}{},
			},
		},
		hostConfig,
		nil,
		nil,
		"styx-postgres",
	)
	if err != nil {
		return fmt.Errorf("failed to create PostgreSQL container: %w", err)
	}

	if err := dockerClient.ContainerStart(ctx, resp.ID, container.StartOptions{}); err != nil {
		return fmt.Errorf("failed to start PostgreSQL container: %w", err)
	}

	defer func() {
		log.Info().Msgf("Stopping PostgreSQL container (id: %s)...", resp.ID)

		timeout := 10
		if err := dockerClient.ContainerStop(ctx, resp.ID, container.StopOptions{
			Timeout: &timeout,
		}); err != nil {
			log.Fatal().Msgf("Failed to stop container: %v\n", err)
		}

		log.Info().Msgf("Removing PostgreSQL container (id: %s)...", resp.ID)
		if err := dockerClient.ContainerRemove(ctx, resp.ID, container.RemoveOptions{
			Force: true,
		}); err != nil {
			log.Fatal().Msgf("Failed to remove container: %v\n", err)
		}
	}()

	log.Info().Msg("Waiting for PostgreSQL to start...")
	time.Sleep(3 * time.Second)

	postgresDsn := "postgres://postgres:postgres@localhost:5433/styx?sslmode=disable"
	if err := applyExistingMigrations(migrationsDir, postgresDsn); err != nil {
		return fmt.Errorf("failed to apply existing migrations: %w", err)
	}

	currentSchema, err := dumpDatabaseSchema(postgresDsn)
	if err != nil {
		return fmt.Errorf("failed to dump current database schema: %w", err)
	}
	log.Info().Msgf("Current schema:\n%s", currentSchema)

	return nil
}

func init() {
	generateCommand.Flags().StringVarP(&inputFile, "input", "i", "schema.sql", "Path to the input schema.sql file (required)")
	generateCommand.Flags().StringVarP(&outputDir, "output-dir", "o", "migrations", "Directory to output the generated migrations (required)")

	generateCommand.MarkFlagRequired("input")
	generateCommand.MarkFlagRequired("output-dir")

	rootCmd.AddCommand(generateCommand)
}
