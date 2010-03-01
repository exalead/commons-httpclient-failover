package com.exalead.io.failover;

import java.io.IOException;

import org.apache.commons.httpclient.ConnectionPoolTimeoutException;
import org.apache.commons.httpclient.HostConfiguration;
import org.apache.commons.httpclient.HttpConnection;
import org.apache.commons.httpclient.HttpConnectionManager;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.params.HttpConnectionManagerParams;
import org.apache.log4j.Logger;

/**
 * @file
 * This class is largely derived of the MonitoredHttpConnectionManager.
 * Copyright the Apache Software Foundation.
 * Licensed under the Apache License 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

/**
 * Manages a set of HttpConnections with monitoring and failover.
 * All HostConfigurations contained in this connection manager are "equivalent" and form a pool
 */
public class MonitoredHttpConnectionManager implements HttpConnectionManager {
    private static final Logger LOG = Logger.getLogger("monitored");
    /** The default maximum number of connections allowed per host */
    public static final int DEFAULT_MAX_HOST_CONNECTIONS = 2;   // Per RFC 2616 sec 8.1.4
    /** The default maximum number of connections allowed overall */
    public static final int DEFAULT_MAX_TOTAL_CONNECTIONS = 20;


    /** Collection of parameters associated with this connection manager. */
    private HttpConnectionManagerParams params = new HttpConnectionManagerParams(); 
    /** Connection Pool */
    //private ConnectionPool connectionPool;
    private MultiHostConnectionPool connectionPool;

   volatile boolean shutdown = false;

    /* ****************** Global methods *********************** */ 

    public MonitoredHttpConnectionManager() {
        this.connectionPool = new MultiHostConnectionPool();
    }

    /**
     * Shuts down the connection manager and releases all resources.  All connections associated 
     * with this class will be closed and released. 
     * 
     * <p>The connection manager can no longer be used once shut down.  
     * 
     * <p>Calling this method more than once will have no effect.
     */
    public synchronized void shutdown() {
        synchronized (connectionPool) {
            if (!shutdown) {
                shutdown = true;
                connectionPool.shutdown();
            }
        }
    }

    /* ****************** Main entry point : get connection *********************** */ 

    /**
     * @see HttpConnectionManager#getConnection(HostConfiguration)
     */
    public HttpConnection getConnection(HostConfiguration hostConfiguration) {
        while (true) {
            try {
                return getConnectionWithTimeout(hostConfiguration, 0);
            } catch (ConnectionPoolTimeoutException e) {
                // we'll go ahead and log this, but it should never happen. HttpExceptions
                // are only thrown when the timeout occurs and since we have no timeout
                // it should never happen.
                LOG.debug(
                        "Unexpected exception while waiting for connection",
                        e
                );
            }
        }
    }

    /**
     * Gets a connection or waits if one is not available.  A connection is
     * available if one exists that is not being used or if fewer than
     * maxHostConnections have been created in the connectionPool, and fewer
     * than maxTotalConnections have been created in all connectionPools.
     *
     * @param hostConfiguration The host configuration specifying the connection
     *        details.
     * @param timeout the number of milliseconds to wait for a connection, 0 to
     * wait indefinitely
     *
     * @return HttpConnection an available connection
     *
     * @throws HttpException if a connection does not become available in
     * 'timeout' milliseconds
     * 
     * @since 3.0
     */
    public HttpConnection getConnectionWithTimeout(HostConfiguration hostConfiguration, 
            long timeout) throws ConnectionPoolTimeoutException {

        LOG.trace("enter HttpConnectionManager.getConnectionWithTimeout(HostConfiguration, long)");

        if (hostConfiguration == null) {
            throw new IllegalArgumentException("hostConfiguration is null");
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("HttpConnectionManager.getConnection:  config = "
                    + hostConfiguration + ", timeout = " + timeout);
        }

        final HttpConnection conn = doGetConnection(hostConfiguration, timeout);

        // wrap the connection in an adapter so we can ensure it is used 
        // only once
        return new HttpConnectionAdapter(conn);
    }

    /**
     * @see HttpConnectionManager#getConnection(HostConfiguration, long)
     * 
     * @deprecated Use #getConnectionWithTimeout(HostConfiguration, long)
     */
    public HttpConnection getConnection(HostConfiguration hostConfiguration, 
            long timeout) throws HttpException {

        LOG.trace("enter HttpConnectionManager.getConnection(HostConfiguration, long)");
        try {
            return getConnectionWithTimeout(hostConfiguration, timeout);
        } catch(ConnectionPoolTimeoutException e) {
            throw new HttpException(e.getMessage());
        }
    }

    private HttpConnection doGetConnectionCritical(HostConfiguration hostConfiguration, 
            long timeout, int maxHostConnections, int maxTotalConnections) throws ConnectionPoolTimeoutException {
        HttpConnection connection = null;

        // we clone the hostConfiguration
        // so that it cannot be changed once the connection has been retrieved
        hostConfiguration = new HostConfiguration(hostConfiguration);

        // TODO: timeout stuff 
        boolean useTimeout = (timeout > 0);
        long timeToWait = timeout;
        long startWait = 0;
        long endWait = 0;

        if (shutdown) {
            throw new IllegalStateException("Connection factory has been shutdown.");
        }
        
        try {
            connection = connectionPool.createConnection(hostConfiguration);
        } catch (IOException e) {
            throw new FailureFakeConnectionPoolTimeoutException(e);
        }
        return connection;
    }

    private HttpConnection doGetConnection(HostConfiguration hostConfiguration, 
            long timeout) throws ConnectionPoolTimeoutException {

        HttpConnection connection = null;

        int maxHostConnections = this.params.getMaxConnectionsPerHost(hostConfiguration);
        int maxTotalConnections = this.params.getMaxTotalConnections();

        synchronized (connectionPool) {
            connection = doGetConnectionCritical(hostConfiguration, timeout, maxHostConnections, maxTotalConnections);
        }
        return connection;
    }

    /* **************************** Inspection *************************** */

    /**
     * Gets the total number of pooled connections for the given host configuration.  This 
     * is the total number of connections that have been created and are still in use 
     * by this connection manager for the host configuration.  This value will
     * not exceed the {@link #getMaxConnectionsPerHost() maximum number of connections per
     * host}.
     * 
     * @param hostConfiguration The host configuration
     * @return The total number of pooled connections
     */
    public int getConnectionsInPool(HostConfiguration hostConfiguration) {
        // TODO
        return 0;
    }

    /**
     * Gets the total number of pooled connections.  This is the total number of 
     * connections that have been created and are still in use by this connection 
     * manager.  This value will not exceed the {@link #getMaxTotalConnections() 
     * maximum number of connections}.
     * 
     * @return the total number of pooled connections
     */
    public int getConnectionsInPool() {
        // TODO
        return 0;
    }

    /**
     * Deletes all closed connections.  Only connections currently owned by the connection
     * manager are processed.
     * 
     * @see HttpConnection#isOpen()
     * 
     * @since 3.0
     */
    public void deleteClosedConnections() {
        // TODO
    }

    /**
     * @since 3.0
     */
    public void closeIdleConnections(long idleTimeout) {
        // TODO
    }

    /**
     * Make the given HttpConnection available for use by other requests.
     * If another thread is blocked in getConnection() that could use this
     * connection, it will be woken up.
     *
     * @param conn the HttpConnection to make available.
     */
    public void releaseConnection(HttpConnection conn) {
        LOG.trace("enter HttpConnectionManager.releaseConnection(HttpConnection)");

        if (conn instanceof HttpConnectionAdapter) {
            // connections given out are wrapped in an HttpConnectionAdapter
            conn = ((HttpConnectionAdapter) conn).getWrappedConnection();
        } else {
            // this is okay, when an HttpConnectionAdapter is released
            // is releases the real connection
        }

        // make sure that the response has been read.
        // XXX FIXME TODO FIXME
        //SimpleHttpConnectionManager.finishLastResponse(conn);

        connectionPool.releaseConnection(conn);
    }

    /**
     * Gets the host configuration for a connection.
     * @param conn the connection to get the configuration of
     * @return a new HostConfiguration
     */
    HostConfiguration configurationForConnection(HttpConnection conn) {

        HostConfiguration connectionConfiguration = new HostConfiguration();

        connectionConfiguration.setHost(
                conn.getHost(), 
                conn.getPort(), 
                conn.getProtocol()
        );
        if (conn.getLocalAddress() != null) {
            connectionConfiguration.setLocalAddress(conn.getLocalAddress());
        }
        if (conn.getProxyHost() != null) {
            connectionConfiguration.setProxy(conn.getProxyHost(), conn.getProxyPort());
        }

        return connectionConfiguration;
    }

    /**
     * Returns {@link HttpConnectionManagerParams parameters} associated 
     * with this connection manager.
     * 
     * @since 3.0
     * 
     * @see HttpConnectionManagerParams
     */
    public HttpConnectionManagerParams getParams() {
        return this.params;
    }

    /**
     * Assigns {@link HttpConnectionManagerParams parameters} for this 
     * connection manager.
     * 
     * @since 3.0
     * 
     * @see HttpConnectionManagerParams
     */
    public void setParams(final HttpConnectionManagerParams params) {
        if (params == null) {
            throw new IllegalArgumentException("Parameters may not be null");
        }
        this.params = params;
    }
}