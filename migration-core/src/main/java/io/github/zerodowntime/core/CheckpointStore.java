package io.github.zerodowntime.core;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;
import javax.sql.DataSource;

/** Remembers how far a backfill job got. Not final so tests can inject crashes. */
public class CheckpointStore {

    private final DataSource dataSource;

    public CheckpointStore(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    /** The last key fully processed by this job, if any. */
    public Optional<String> load(String jobName) {
        try (Connection c = dataSource.getConnection();
                PreparedStatement ps = c.prepareStatement("SELECT last_key FROM backfill_checkpoint WHERE job_name = ?")) {
            ps.setString(1, jobName);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? Optional.of(rs.getString(1)) : Optional.empty();
            }
        } catch (SQLException e) {
            throw new IllegalStateException("loading checkpoint failed", e);
        }
    }

    public void save(String jobName, String lastKey) {
        try (Connection c = dataSource.getConnection();
                PreparedStatement ps = c.prepareStatement("""
                        INSERT INTO backfill_checkpoint (job_name, last_key) VALUES (?, ?)
                        ON CONFLICT (job_name) DO UPDATE SET last_key = EXCLUDED.last_key, updated_at = now()
                        """)) {
            ps.setString(1, jobName);
            ps.setString(2, lastKey);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("saving checkpoint failed", e);
        }
    }
}
