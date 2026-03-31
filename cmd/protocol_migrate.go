package cmd

import (
	"context"
	"fmt"

	_ "github.com/lib/pq"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/support/config"
	"github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/wallet-backend/cmd/utils"
	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/services"
	internalutils "github.com/stellar/wallet-backend/internal/utils"
)

type protocolMigrateCmd struct{}

func (c *protocolMigrateCmd) Command() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "protocol-migrate",
		Short: "Data migration commands for protocol state",
		Long:  "Parent command for protocol data migrations. Use subcommands to run specific migration tasks.",
		Run: func(cmd *cobra.Command, args []string) {
			if err := cmd.Help(); err != nil {
				log.Fatalf("Error calling help command: %s", err.Error())
			}
		},
	}

	cmd.AddCommand(c.historyCommand())
	cmd.AddCommand(c.currentStateCommand())

	return cmd
}

// migrationCommandOpts captures the shared flags for migration subcommands.
type migrationCommandOpts struct {
	databaseURL            string
	rpcURL                 string
	networkPassphrase      string
	protocolIDs            []string
	logLevel               string
	latestLedgerCursorName string
}

// buildMigrationCommand creates a cobra.Command with shared migration flags and validation.
// addFlags adds strategy-specific flags. extraValidate runs strategy-specific validation.
// runE receives the shared opts and executes the strategy-specific logic.
func buildMigrationCommand(
	use, short, long string,
	addFlags func(cmd *cobra.Command, opts *migrationCommandOpts),
	extraValidate func() error,
	runE func(opts *migrationCommandOpts) error,
) *cobra.Command {
	var opts migrationCommandOpts

	cfgOpts := config.ConfigOptions{
		utils.DatabaseURLOption(&opts.databaseURL),
		utils.RPCURLOption(&opts.rpcURL),
		utils.NetworkPassphraseOption(&opts.networkPassphrase),
	}

	cmd := &cobra.Command{
		Use:   use,
		Short: short,
		Long:  long,
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			if err := cfgOpts.RequireE(); err != nil {
				return fmt.Errorf("requiring values of config options: %w", err)
			}
			if err := cfgOpts.SetValues(); err != nil {
				return fmt.Errorf("setting values of config options: %w", err)
			}

			if opts.logLevel != "" {
				ll, err := logrus.ParseLevel(opts.logLevel)
				if err != nil {
					return fmt.Errorf("invalid log level %q: %w", opts.logLevel, err)
				}
				log.DefaultLogger.SetLevel(ll)
			}

			if len(opts.protocolIDs) == 0 {
				return fmt.Errorf("at least one --protocol-id is required")
			}
			if extraValidate != nil {
				return extraValidate()
			}
			return nil
		},
		RunE: func(_ *cobra.Command, _ []string) error {
			return runE(&opts)
		},
	}

	if err := cfgOpts.Init(cmd); err != nil {
		log.Fatalf("Error initializing a config option: %s", err.Error())
	}

	cmd.Flags().StringSliceVar(&opts.protocolIDs, "protocol-id", nil, "Protocol ID(s) to migrate (required, repeatable)")
	cmd.Flags().StringVar(&opts.logLevel, "log-level", "", `Log level: "TRACE", "DEBUG", "INFO", "WARN", "ERROR", "FATAL", "PANIC"`)
	cmd.Flags().StringVar(&opts.latestLedgerCursorName, "latest-ledger-cursor-name", data.LatestLedgerCursorName, "Name of the latest ledger cursor in the ingest store. Must match the value used by the ingest service.")

	if addFlags != nil {
		addFlags(cmd, &opts)
	}

	return cmd
}

// runMigration handles the shared setup (processors, DB, models, ledger backend) and
// delegates to createAndRun for strategy-specific service creation and execution.
func runMigration(
	label string,
	opts *migrationCommandOpts,
	createAndRun func(
		ctx context.Context,
		dbPool db.ConnectionPool,
		ledgerBackend ledgerbackend.LedgerBackend,
		ledgerBackendFactory func() ledgerbackend.LedgerBackend,
		models *data.Models,
		processors []services.ProtocolProcessor,
	) error,
) error {
	ctx := context.Background()

	// Build processors from protocol IDs using the dynamic registry
	var processors []services.ProtocolProcessor
	for _, pid := range opts.protocolIDs {
		factory, ok := services.GetProcessor(pid)
		if !ok {
			return fmt.Errorf("unknown protocol ID %q — no processor registered", pid)
		}
		p := factory()
		if p == nil {
			return fmt.Errorf("processor factory for protocol %q returned nil", pid)
		}
		processors = append(processors, p)
	}

	// Open DB connection
	dbPool, err := db.OpenDBConnectionPool(opts.databaseURL)
	if err != nil {
		return fmt.Errorf("opening database connection: %w", err)
	}
	defer internalutils.DeferredClose(ctx, dbPool, fmt.Sprintf("closing dbPool in protocol migrate %s", label))

	// Create models
	sqlxDB, err := dbPool.SqlxDB(ctx)
	if err != nil {
		return fmt.Errorf("getting sqlx DB: %w", err)
	}
	metricsService := metrics.NewMetricsService(sqlxDB)
	models, err := data.NewModels(dbPool, metricsService)
	if err != nil {
		return fmt.Errorf("creating models: %w", err)
	}

	// Create ledger backend factory for re-creating backends between range preparations.
	// RPCLedgerBackend does not support calling PrepareRange more than once.
	newBackend := func() ledgerbackend.LedgerBackend {
		return ledgerbackend.NewRPCLedgerBackend(ledgerbackend.RPCLedgerBackendOptions{
			RPCServerURL: opts.rpcURL,
			BufferSize:   10,
		})
	}

	return createAndRun(ctx, dbPool, newBackend(), newBackend, models, processors)
}

func (c *protocolMigrateCmd) historyCommand() *cobra.Command {
	var oldestLedgerCursorName string

	return buildMigrationCommand(
		"history",
		"Backfill protocol history state from oldest to latest ingested ledger",
		"Processes historical ledgers from oldest_ingest_ledger to the tip, producing protocol state changes and converging with live ingestion via CAS-gated cursors.",
		func(cmd *cobra.Command, opts *migrationCommandOpts) {
			cmd.Flags().StringVar(&oldestLedgerCursorName, "oldest-ledger-cursor-name", data.OldestLedgerCursorName, "Name of the oldest ledger cursor in the ingest store. Must match the value used by the ingest service.")
		},
		nil,
		func(opts *migrationCommandOpts) error {
			return runMigration("history", opts, func(ctx context.Context, dbPool db.ConnectionPool, ledgerBackend ledgerbackend.LedgerBackend, ledgerBackendFactory func() ledgerbackend.LedgerBackend, models *data.Models, processors []services.ProtocolProcessor) error {
				service, err := services.NewProtocolMigrateHistoryService(services.ProtocolMigrateHistoryConfig{
					DB:                     dbPool,
					LedgerBackend:          ledgerBackend,
					LedgerBackendFactory:   ledgerBackendFactory,
					ProtocolsModel:         models.Protocols,
					ProtocolContractsModel: models.ProtocolContracts,
					IngestStore:            models.IngestStore,
					NetworkPassphrase:      opts.networkPassphrase,
					Processors:             processors,
					LatestLedgerCursorName: opts.latestLedgerCursorName,
					OldestLedgerCursorName: oldestLedgerCursorName,
				})
				if err != nil {
					return fmt.Errorf("creating protocol migrate history service: %w", err)
				}
				if err := service.Run(ctx, opts.protocolIDs); err != nil {
					return fmt.Errorf("running protocol migrate history: %w", err)
				}
				return nil
			})
		},
	)
}

func (c *protocolMigrateCmd) currentStateCommand() *cobra.Command {
	var startLedger uint32

	return buildMigrationCommand(
		"current-state",
		"Build protocol current state from a start ledger forward",
		"Processes ledgers from --start-ledger to the tip, building protocol current state and converging with live ingestion via CAS-gated cursors.",
		func(cmd *cobra.Command, opts *migrationCommandOpts) {
			cmd.Flags().Uint32Var(&startLedger, "start-ledger", 0, "Ledger sequence to begin current-state migration from (required)")
		},
		func() error {
			if startLedger == 0 {
				return fmt.Errorf("--start-ledger is required and must be > 0")
			}
			return nil
		},
		func(opts *migrationCommandOpts) error {
			return runMigration("current-state", opts, func(ctx context.Context, dbPool db.ConnectionPool, ledgerBackend ledgerbackend.LedgerBackend, ledgerBackendFactory func() ledgerbackend.LedgerBackend, models *data.Models, processors []services.ProtocolProcessor) error {
				service, err := services.NewProtocolMigrateCurrentStateService(services.ProtocolMigrateCurrentStateConfig{
					DB:                     dbPool,
					LedgerBackend:          ledgerBackend,
					LedgerBackendFactory:   ledgerBackendFactory,
					ProtocolsModel:         models.Protocols,
					ProtocolContractsModel: models.ProtocolContracts,
					IngestStore:            models.IngestStore,
					NetworkPassphrase:      opts.networkPassphrase,
					Processors:             processors,
					LatestLedgerCursorName: opts.latestLedgerCursorName,
					StartLedger:            startLedger,
				})
				if err != nil {
					return fmt.Errorf("creating protocol migrate current-state service: %w", err)
				}
				if err := service.Run(ctx, opts.protocolIDs); err != nil {
					return fmt.Errorf("running protocol migrate current-state: %w", err)
				}
				return nil
			})
		},
	)
}
