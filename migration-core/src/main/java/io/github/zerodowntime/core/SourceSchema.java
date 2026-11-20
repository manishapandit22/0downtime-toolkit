package io.github.zerodowntime.core;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import javax.sql.DataSource;

/** DDL for the Postgres side: the source table and the transactional outbox. */
public final class SourceSchema {

    static final String SOURCE_TABLE = "source_records";
    static final String OUTBOX_TABLE = "outbox";

    private static final String[] DDL = {
        """
        CREATE TABLE IF NOT EXISTS source_records (
            record_key text PRIMARY KEY,
            version    bigint  NOT NULL,
            deleted    boolean NOT NULL,
            attrs      jsonb   NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS outbox (
            id           bigserial PRIMARY KEY,
            record_key   text        NOT NULL,
            version      bigint      NOT NULL,
            deleted      boolean     NOT NULL,
            attrs        jsonb       NOT NULL,
            created_at   timestamptz NOT NULL DEFAULT now(),
            delivered_at timestamptz
        )
        """,
        "CREATE INDEX IF NOT EXISTS outbox_pending ON outbox (id) WHERE delivered_at IS NULL",
        """
        CREATE TABLE IF NOT EXISTS backfill_checkpoint (
            job_name   text PRIMARY KEY,
            last_key   text        NOT NULL,
            updated_at timestamptz NOT NULL DEFAULT now()
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS migration_state (
            id         int  PRIMARY KEY CHECK (id = 1),
            phase      text        NOT NULL,
            updated_at timestamptz NOT NULL DEFAULT now()
        )
        """,
        "INSERT INTO migration_state (id, phase) VALUES (1, 'DUAL_WRITE') ON CONFLICT DO NOTHING"
    };

    private SourceSchema() {}

    public static void create(DataSource dataSource) {
        try (Connection c = dataSource.getConnection(); Statement s = c.createStatement()) {
            for (String ddl : DDL) {
                s.execute(ddl);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("could not create source schema", e);
        }
    }
}
