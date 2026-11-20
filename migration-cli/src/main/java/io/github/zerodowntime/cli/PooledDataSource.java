package io.github.zerodowntime.cli;

import java.io.PrintWriter;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import javax.sql.DataSource;

/**
 * Minimal bounded connection pool. The engine opens a connection per operation, which is fine for
 * tests but under sustained load exhausts the OS's ephemeral TCP ports ("Address already in use")
 * long before Postgres is busy. Pooling fixes that without adding a dependency.
 *
 * <p>Connections handed out are proxies: {@code close()} returns the physical connection to the
 * pool after rolling back any open transaction and restoring auto-commit.
 */
public final class PooledDataSource implements DataSource, AutoCloseable {

    private final DataSource delegate;
    private final BlockingQueue<Connection> idle = new LinkedBlockingQueue<>();
    private final Semaphore permits;
    private final long waitSeconds;

    public PooledDataSource(DataSource delegate, int maxConnections) {
        this(delegate, maxConnections, 30);
    }

    public PooledDataSource(DataSource delegate, int maxConnections, long waitSeconds) {
        this.delegate = delegate;
        this.permits = new Semaphore(maxConnections);
        this.waitSeconds = waitSeconds;
    }

    @Override
    public Connection getConnection() throws SQLException {
        try {
            if (!permits.tryAcquire(waitSeconds, TimeUnit.SECONDS)) {
                throw new SQLException("connection pool exhausted (waited " + waitSeconds + "s)");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SQLException("interrupted waiting for a pooled connection", e);
        }
        Connection real = null;
        try {
            while (real == null) {
                Connection candidate = idle.poll();
                if (candidate == null) {
                    real = delegate.getConnection();
                } else if (!candidate.isClosed() && candidate.isValid(1)) {
                    real = candidate;
                } else {
                    closeQuietly(candidate);
                }
            }
            return wrap(real);
        } catch (SQLException | RuntimeException e) {
            permits.release();
            closeQuietly(real);
            throw e;
        }
    }

    private Connection wrap(Connection real) {
        boolean[] released = {false};
        InvocationHandler handler = (proxy, method, args) -> {
            switch (method.getName()) {
                case "close" -> {
                    if (!released[0]) {
                        released[0] = true;
                        release(real);
                    }
                    return null;
                }
                case "isClosed" -> {
                    return released[0] || real.isClosed();
                }
                default -> {
                    if (released[0]) {
                        throw new SQLException("connection already returned to the pool");
                    }
                    try {
                        return method.invoke(real, args);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                }
            }
        };
        return (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(), new Class<?>[] {Connection.class}, handler);
    }

    private void release(Connection real) {
        try {
            if (!real.isClosed()) {
                if (!real.getAutoCommit()) {
                    real.rollback();
                    real.setAutoCommit(true);
                }
                idle.offer(real);
            }
        } catch (SQLException broken) {
            closeQuietly(real);
        } finally {
            permits.release();
        }
    }

    private static void closeQuietly(Connection c) {
        if (c != null) {
            try {
                c.close();
            } catch (SQLException ignored) {
                // nothing useful to do
            }
        }
    }

    @Override
    public void close() {
        Connection c;
        while ((c = idle.poll()) != null) {
            closeQuietly(c);
        }
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        throw new SQLFeatureNotSupportedException("credentials are fixed on the underlying data source");
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return delegate.getLogWriter();
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
        delegate.setLogWriter(out);
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
        delegate.setLoginTimeout(seconds);
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return delegate.getLoginTimeout();
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new SQLException("not a wrapper");
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return false;
    }
}
