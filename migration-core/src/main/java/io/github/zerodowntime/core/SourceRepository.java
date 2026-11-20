package io.github.zerodowntime.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Optional;
import javax.sql.DataSource;

/**
 * The application's write path on the source (Postgres).
 *
 * <p>Every change does two things in ONE transaction: it updates {@code source_records} (bumping
 * the per-row version) and inserts the new full row state into {@code outbox}. Either both
 * commit or neither does, so a crash can never leave a source change without its event.
 */
public final class SourceRepository {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final TypeReference<Map<String, String>> ATTRS_TYPE = new TypeReference<>() {};

    private final DataSource dataSource;

    public SourceRepository(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    /**
     * Runs first inside a write transaction. The cutover fence uses it to take a shared lock on the
     * migration phase row and refuse the write if the source is no longer accepting writes.
     */
    @FunctionalInterface
    public interface TxGuard {
        void check(Connection c) throws SQLException;
    }

    /** Creates or updates a row. Returns the new version. */
    public long upsert(String key, Map<String, String> attributes) {
        return upsert(key, attributes, null);
    }

    /** Same as {@link #upsert(String, Map)} but the guard runs first, in the same transaction. */
    public long upsert(String key, Map<String, String> attributes, TxGuard guard) {
        String attrsJson = toJson(attributes);
        return inTransaction(c -> {
            if (guard != null) {
                guard.check(c);
            }
            long version;
            try (PreparedStatement ps = c.prepareStatement("""
                    INSERT INTO source_records (record_key, version, deleted, attrs)
                    VALUES (?, 1, false, ?::jsonb)
                    ON CONFLICT (record_key) DO UPDATE
                       SET version = source_records.version + 1, deleted = false, attrs = EXCLUDED.attrs
                    RETURNING version
                    """)) {
                ps.setString(1, key);
                ps.setString(2, attrsJson);
                version = single(ps);
            }
            insertOutbox(c, key, version, false, attrsJson);
            return version;
        });
    }

    /**
     * Deletes a row by turning it into a tombstone with a higher version. Returns the tombstone
     * version, or empty if the row does not exist or is already deleted.
     */
    public Optional<Long> delete(String key) {
        return delete(key, null);
    }

    /** Same as {@link #delete(String)} but the guard runs first, in the same transaction. */
    public Optional<Long> delete(String key, TxGuard guard) {
        return inTransaction(c -> {
            if (guard != null) {
                guard.check(c);
            }
            Long version = null;
            try (PreparedStatement ps = c.prepareStatement("""
                    UPDATE source_records SET version = version + 1, deleted = true, attrs = '{}'::jsonb
                    WHERE record_key = ? AND deleted = false
                    RETURNING version
                    """)) {
                ps.setString(1, key);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        version = rs.getLong(1);
                    }
                }
            }
            if (version == null) {
                return Optional.<Long>empty();
            }
            insertOutbox(c, key, version, true, "{}");
            return Optional.of(version);
        });
    }

    /**
     * Applies a row that was written on the TARGET while it was authoritative (rollback path).
     * Monotonic like the forward path: only a strictly higher version takes effect. Deliberately
     * writes NO outbox row, otherwise the change would bounce straight back to the target.
     */
    public boolean applyReplicated(VersionedRecord r) {
        try (Connection c = dataSource.getConnection();
                PreparedStatement ps = c.prepareStatement("""
                        INSERT INTO source_records (record_key, version, deleted, attrs) VALUES (?, ?, ?, ?::jsonb)
                        ON CONFLICT (record_key) DO UPDATE
                           SET version = EXCLUDED.version, deleted = EXCLUDED.deleted, attrs = EXCLUDED.attrs
                         WHERE source_records.version < EXCLUDED.version
                        """)) {
            ps.setString(1, r.key());
            ps.setLong(2, r.version());
            ps.setBoolean(3, r.deleted());
            ps.setString(4, toJson(r.attributes()));
            return ps.executeUpdate() == 1;
        } catch (SQLException e) {
            throw new IllegalStateException("applyReplicated failed for " + r.key(), e);
        }
    }

    public Optional<VersionedRecord> read(String key) {
        try (Connection c = dataSource.getConnection();
                PreparedStatement ps = c.prepareStatement(
                        "SELECT record_key, version, deleted, attrs FROM source_records WHERE record_key = ?")) {
            ps.setString(1, key);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? Optional.of(toRecord(rs)) : Optional.empty();
            }
        } catch (SQLException e) {
            throw new IllegalStateException("read failed for " + key, e);
        }
    }

    /**
     * Next chunk of rows (tombstones included) in key order, strictly after {@code afterKey}
     * (or from the start if null). Used by the backfill.
     */
    public java.util.List<VersionedRecord> readChunk(String afterKey, int limit) {
        String sql = afterKey == null
                ? "SELECT record_key, version, deleted, attrs FROM source_records ORDER BY record_key LIMIT ?"
                : "SELECT record_key, version, deleted, attrs FROM source_records WHERE record_key > ? ORDER BY record_key LIMIT ?";
        try (Connection c = dataSource.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            int i = 1;
            if (afterKey != null) {
                ps.setString(i++, afterKey);
            }
            ps.setInt(i, limit);
            java.util.List<VersionedRecord> rows = new java.util.ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    rows.add(toRecord(rs));
                }
            }
            return rows;
        } catch (SQLException e) {
            throw new IllegalStateException("reading chunk failed", e);
        }
    }

    private static void insertOutbox(Connection c, String key, long version, boolean deleted, String attrsJson)
            throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("""
                INSERT INTO outbox (record_key, version, deleted, attrs) VALUES (?, ?, ?, ?::jsonb)
                """)) {
            ps.setString(1, key);
            ps.setLong(2, version);
            ps.setBoolean(3, deleted);
            ps.setString(4, attrsJson);
            ps.executeUpdate();
        }
    }

    private static long single(PreparedStatement ps) throws SQLException {
        try (ResultSet rs = ps.executeQuery()) {
            rs.next();
            return rs.getLong(1);
        }
    }

    static VersionedRecord toRecord(ResultSet rs) throws SQLException {
        return new VersionedRecord(
                rs.getString("record_key"), rs.getLong("version"), rs.getBoolean("deleted"), fromJson(rs.getString("attrs")));
    }

    static String toJson(Map<String, String> attributes) {
        try {
            return JSON.writeValueAsString(attributes == null ? Map.of() : attributes);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("attributes not serializable", e);
        }
    }

    static Map<String, String> fromJson(String json) {
        try {
            return JSON.readValue(json, ATTRS_TYPE);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("corrupt attrs json: " + json, e);
        }
    }

    @FunctionalInterface
    private interface TxWork<T> {
        T run(Connection c) throws SQLException;
    }

    private <T> T inTransaction(TxWork<T> work) {
        try (Connection c = dataSource.getConnection()) {
            c.setAutoCommit(false);
            try {
                T result = work.run(c);
                c.commit();
                return result;
            } catch (SQLException | RuntimeException e) {
                c.rollback();
                throw e;
            }
        } catch (SQLException e) {
            throw new IllegalStateException("transaction failed", e);
        }
    }
}
