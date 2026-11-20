package io.github.zerodowntime.core;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import javax.sql.DataSource;

/**
 * The migration phase, persisted in Postgres so it survives restarts.
 *
 * <p>This row is also the write fence for the source. Every source write takes a SHARED lock on
 * it inside its own transaction ({@link #writeGuard()}); a phase change is an UPDATE of the same
 * row, which needs an exclusive lock. So a phase change waits for in-flight source writes to
 * commit, and any write that starts afterwards sees the new phase. No write can slip through
 * "between the check and the commit".
 */
public class MigrationState {

    private final DataSource dataSource;

    public MigrationState(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public Phase current() {
        try (Connection c = dataSource.getConnection();
                PreparedStatement ps = c.prepareStatement("SELECT phase FROM migration_state WHERE id = 1");
                ResultSet rs = ps.executeQuery()) {
            rs.next();
            return Phase.valueOf(rs.getString(1));
        } catch (SQLException e) {
            throw new IllegalStateException("reading migration phase failed", e);
        }
    }

    /**
     * Compare-and-set: moves {@code from} to {@code to} only if the phase still is {@code from}.
     * Returns false if someone else moved it first. Throws if the transition is not in the diagram.
     */
    public boolean transition(Phase from, Phase to) {
        if (!from.canGo(to)) {
            throw new IllegalStateException("illegal transition " + from + " -> " + to);
        }
        try (Connection c = dataSource.getConnection();
                PreparedStatement ps = c.prepareStatement(
                        "UPDATE migration_state SET phase = ?, updated_at = now() WHERE id = 1 AND phase = ?")) {
            ps.setString(1, to.name());
            ps.setString(2, from.name());
            return ps.executeUpdate() == 1;
        } catch (SQLException e) {
            throw new IllegalStateException("phase transition failed", e);
        }
    }

    /** Guard for {@link SourceRepository}: share-lock the phase row, refuse if not writable. */
    public SourceRepository.TxGuard writeGuard() {
        return c -> {
            Phase p;
            try (PreparedStatement ps = c.prepareStatement("SELECT phase FROM migration_state WHERE id = 1 FOR SHARE");
                    ResultSet rs = ps.executeQuery()) {
                rs.next();
                p = Phase.valueOf(rs.getString(1));
            }
            if (!p.acceptsSourceWrites()) {
                throw new WritesPausedException("source is not accepting writes in phase " + p);
            }
        };
    }
}
