package io.github.zerodowntime.core;

import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import javax.sql.DataSource;

/** Reads pending outbox events and marks them delivered. Not final so tests can inject crashes. */
public class OutboxStore {

    /** One outbox row: its id plus the full row state to replicate. */
    public record Event(long id, VersionedRecord record) {}

    private final DataSource dataSource;

    public OutboxStore(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    /** Oldest undelivered events first. */
    public List<Event> fetchPending(int limit) {
        try (Connection c = dataSource.getConnection();
                PreparedStatement ps = c.prepareStatement("""
                        SELECT id, record_key, version, deleted, attrs FROM outbox
                        WHERE delivered_at IS NULL ORDER BY id LIMIT ?
                        """)) {
            ps.setInt(1, limit);
            List<Event> events = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    events.add(new Event(rs.getLong("id"), SourceRepository.toRecord(rs)));
                }
            }
            return events;
        } catch (SQLException e) {
            throw new IllegalStateException("fetching outbox failed", e);
        }
    }

    public void markDelivered(List<Long> ids) {
        if (ids.isEmpty()) {
            return;
        }
        try (Connection c = dataSource.getConnection();
                PreparedStatement ps = c.prepareStatement(
                        "UPDATE outbox SET delivered_at = now() WHERE id = ANY (?) AND delivered_at IS NULL")) {
            Array array = c.createArrayOf("bigint", ids.toArray());
            ps.setArray(1, array);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("marking outbox delivered failed", e);
        }
    }

    /** Highest outbox id so far (0 if none). A watermark: later changes have a larger id. */
    public long maxId() {
        try (Connection c = dataSource.getConnection();
                PreparedStatement ps = c.prepareStatement("SELECT coalesce(max(id), 0) FROM outbox");
                ResultSet rs = ps.executeQuery()) {
            rs.next();
            return rs.getLong(1);
        } catch (SQLException e) {
            throw new IllegalStateException("reading outbox watermark failed", e);
        }
    }

    /** Every key changed on the source after the watermark, delivered or not. */
    public java.util.Set<String> keysChangedAfter(long watermark) {
        try (Connection c = dataSource.getConnection();
                PreparedStatement ps = c.prepareStatement("SELECT DISTINCT record_key FROM outbox WHERE id > ?")) {
            ps.setLong(1, watermark);
            java.util.Set<String> keys = new java.util.TreeSet<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    keys.add(rs.getString(1));
                }
            }
            return keys;
        } catch (SQLException e) {
            throw new IllegalStateException("reading changed keys failed", e);
        }
    }

    public long pendingCount() {
        try (Connection c = dataSource.getConnection();
                PreparedStatement ps = c.prepareStatement("SELECT count(*) FROM outbox WHERE delivered_at IS NULL");
                ResultSet rs = ps.executeQuery()) {
            rs.next();
            return rs.getLong(1);
        } catch (SQLException e) {
            throw new IllegalStateException("counting outbox failed", e);
        }
    }
}
