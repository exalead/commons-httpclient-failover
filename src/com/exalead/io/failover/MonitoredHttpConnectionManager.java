package com.exalead.io.failover;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.httpclient.ConnectionPoolTimeoutException;
import org.apache.commons.httpclient.HostConfiguration;
import org.apache.commons.httpclient.HttpConnection;
import org.apache.commons.httpclient.HttpConnectionManager;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpState;
import org.apache.commons.httpclient.methods.GetMethod;
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
    /* **************** Static helpers ******************** */

    private static final Logger LOG = Logger.getLogger("monitored");

    /**
     * Gets the host configuration for a connection.
     * @param conn the connection to get the configuration of
     * @return a new HostConfiguration
     */
    static HostConfiguration rebuildConfigurationFromConnection(HttpConnection conn) {
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

    /* ***************** Global stuff ****************** */

    /** The default maximum number of connections allowed per host */
    public static final int DEFAULT_MAX_HOST_CONNECTIONS = 2;   // Per RFC 2616 sec 8.1.4
    /** The default maximum number of connections allowed overall */
    public static final int DEFAULT_MAX_TOTAL_CONNECTIONS = 20;

    /** Collection of parameters associated with this connection manager. */
    private HttpConnectionManagerParams params = new HttpConnectionManagerParams(); 

    volatile boolean shutdown = false;

    public synchronized void addHost(String host, int port, int power) {
        HostState hs = new HostState();
        hs.configuration = new HostConfiguration();
        hs.configuration.setHost(host, port);
        hs.power = power;
        hosts.add(hs);
        hostsMap.put(hs.configuration, hs);
    }

    /** The list of all hosts in the pool */
    List<HostState> hosts;
    /** Fast access map by configuration */
    Map<HostConfiguration, HostState> hostsMap;

    /**
     * Shuts down the connection manager and releases all resources.  All connections associated 
     * with this class will be closed and released. 
     * Cleans up all connection pool resources.
     * <p>The connection manager can no longer be used once shut down.  
     * 
     * <p>Calling this method more than once will have no effect.
     */
    public synchronized void shutdown() {
        //        // close all free connections
        //        Iterator<HttpConnection> iter = freeConnections.iterator();
        //        while (iter.hasNext()) {
        //            HttpConnection conn = iter.next();
        //            iter.remove();
        //            conn.close();
        //        }
        //
        //        // interrupt all waiting threads
        //        Iterator<WaitingThread> itert = waitingThreads.iterator();
        //        while (itert.hasNext()) {
        //            WaitingThread waiter = itert.next();
        //            iter.remove();
        //            waiter.interruptedByConnectionPool = true;
        //            waiter.thread.interrupt();
        //        }
        //
        //        // clear out map hosts
        //        mapHosts.clear();
        //        // remove all references to connections
        //        idleConnectionHandler.removeAll();
    }

    /* *************************** Location helpers ************************* */

    /** Find a Host given its configuration. Must be called with the lock */
    private HostState getHostFromConfiguration(HostConfiguration config) {
        return hostsMap.get(config);
    }

    /* *************************** Hosts round-robin dispatch ************************* */

    /** Index of the current host in the list */
    private int currentHost;
    /** Number of dispatches already performed on current host */
    private int dispatchesOnCurrentHost;

    /** 
     * Get the host to use for next connection. 
     * It performs round-robin amongst currently alive hosts, respecting the power property.
     * This method must be called with the pool lock.
     * TODO: Improve this method 
     */
    private HostState getNextRoundRobinHost() throws IOException {
        if (dispatchesOnCurrentHost >= hosts.get(currentHost).power) {
            /* Power reached, goto next */
            dispatchesOnCurrentHost = 0;
            currentHost++;
        } else if (!hosts.get(currentHost).down) {
            /* Power not reached */
            dispatchesOnCurrentHost++;
            return hosts.get(currentHost);
        }

        boolean reachedEnd = false;
        while (true) {
            if (currentHost >= hosts.size()) {
                if (reachedEnd) {
                    /* Oops, all hosts are down ! */
                    throw new IOException("All hosts are down");
                }
                currentHost = 0;
                reachedEnd = true;
            }
            if (!hosts.get(currentHost).down) {
                break;
            } else {
                currentHost++;
            }
        }
        dispatchesOnCurrentHost = 1;
        return hosts.get(currentHost);
    }

    /**
     * Get the host that should be used next in the round-robin dispatch
     * @return a HostConfiguration that should be passed to the HttpClient
     */
    public HostConfiguration getHostToUse() throws IOException {
        synchronized(this) { 
            HostState hs = getNextRoundRobinHost();
            return hs.configuration;
        }
    }

    /** We synchronously check the connection if its check is more than this delay old */
    long maxCheckDelayWithoutSynchronousCheck = 1000;
    int connectionTimeout = 500;
    int isAliveTimeout = 500;
    int applicativeTimeout = 5000;

    /* *************************** isAlive monitoring ************************* */

    /** 
     * Returns "true" if host is up and alive.
     * Returns "false" if host is up but not alive
     * Throws an exception in case of connection error
     */
    boolean checkConnection(MonitoredConnection connection) throws IOException {
        /* TODO: HANDLE really state */
        HttpState hs = new HttpState();
        GetMethod gm = new GetMethod(connection.host.getURI() + "/isAlive");
        connection.conn.setSocketTimeout(isAliveTimeout);
        int statusCode = gm.execute(hs, connection.conn);
        return statusCode < 400;
    }

    /* *************************** Entry point: acquire ************************* */


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

    private HttpConnection doGetConnection(HostConfiguration hostConfiguration, 
            long timeout) throws ConnectionPoolTimeoutException {
        int maxHostConnections = this.params.getMaxConnectionsPerHost(hostConfiguration);
        int maxTotalConnections = this.params.getMaxTotalConnections();
        return doGetConnectionCritical(hostConfiguration, timeout, maxHostConnections, maxTotalConnections);
    }

    /**
     * Creates a new connection and returns it for use of the calling method.
     * This method should only be called by the HttpClient  
     *
     * @param hostConfiguration the configuration for the connection
     * @return a new connection or <code>null</code> if none are available
     */
    private HttpConnection doGetConnectionCritical(HostConfiguration hostConfiguration, 
            long timeout,
            int maxHostConnections, int maxTotalConnections)
    throws ConnectionPoolTimeoutException {
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
            connection = createConnection(hostConfiguration);
        } catch (IOException e) {
            throw new FailureFakeConnectionPoolTimeoutException(e);
        }
        return connection;
    }    

    /** The real work is done here */
    private  HttpConnection createConnection(HostConfiguration hostConfiguration) throws IOException {
        HostState host = null;
        synchronized(this) {
            host = getHostFromConfiguration(hostConfiguration);
        }

        /* We are now going to loop until:
         *  - We have noticed that host is down -> fail
         *  - We have found a suitable connection:
         *      - either a recently checked connection
         *      - or an old connection that we synchronously recheck 
         *  - There is no more waiting connection and:
         *      - we create one right now and it works
         *      - we create one right now and it fails -> host is down
         *  - The number of iterations is limited to avoid potential
         *    race condition between monitoring thread(s) and this loop 
         */

        MonitoredConnection c = null;
        for (int curLoop = 0; curLoop < 10; curLoop++) {
            boolean needSynchronousCheck = false;

            /* Try to select an existing connection */
            synchronized(this) {
                long now = System.currentTimeMillis();

                if (host.down) {
                    throw new IOException("Host is down (marked as down)");
                }

                long minDate = now - maxCheckDelayWithoutSynchronousCheck;
                List<MonitoredConnection> recentlyChecked = host.getRecentlyCheckedConnections(minDate);
                if (recentlyChecked.size() == 0) {
                    /* There is no recently checked connection */
                    needSynchronousCheck = true;
                    /* Let's help a bit the monitoring thread by taking a connection that was not checked recently */
                    c = host.getOldestCheckedConnection();
                } else {
                    /** TODO: better scheduling ? */
                    c = recentlyChecked.get(0);
                }

                if (c != null) {
                    host.removeFreeConnection(c);
                }
            }

            /* There was no free connection for this host, so let's connect now */
            if (c == null) {
                needSynchronousCheck = false;
                try {
                    c = host.connect(connectionTimeout);
                } catch (IOException e) {
                    /* In that case, we don't care if it's a fail or timeout:
                     * we can't connect to the host in time, so the host is down.
                     */
                    synchronized(this) {
                        host.down = true;
                        // If the host has connections, it means that they were 
                        // established while we were trying to connect. .. So maybe the host
                        // is not down, but to be sure, let's still consider as down and 
                        // kill everything
                        host.killAllConnections();
                        throw new IOException("Host is down (couldn't connect)");
                    }
                }
            }

            /* The connection we got was too old, perform a check */
            if (needSynchronousCheck) {
                try {
                    boolean ret = checkConnection(c);
                    /* Host is up but not alive: just kill all connections.
                     * It's useless to try another connection: host knows it's not alive
                     */

                    if (ret == false) {
                        synchronized(this) {
                            host.down = true;
                            host.killAllConnections();
                            throw new IOException("Host is down (not alive)");
                        }
                    } else {
                        // Great, we have a working connection !
                        break;
                    }
                } catch (IOException e) {
                    synchronized(this) {
                        if (e instanceof SocketTimeoutException) {
                            /* Timeout while trying to get isAlive -> host is hanged.
                             * Don't waste time checking connections, we would just timeout more.
                             * So, kill everything
                             */
                            host.down = true;
                            host.killAllConnections();
                            /* Don't forget to close this connection to avoid FD leak */
                            c.conn.close();
                            throw new IOException("Host is down (isAlive timeout)");
                        } else {
                            /* Connection failure. Server looks down (connection reset by peer). But it could
                             * be only that connection which failed (TCP timeout for example). In that case, 
                             * we try to fast-kill the stale connections and we retry. Either there are still
                             * some alive connections and we can retry with them, or the loop will try to 
                             * reconnect and success (for example, the host went down and up very fast) or fail 
                             */
                            host.killStaleConnections();
                            /* Don't forget to close this connection to avoid FD leak */
                            c.conn.close();
                            continue;
                        }
                    }
                }
            } else {
                /* We don't need a synchronous check -> Great, we have a working connection ! */
                break;
            }
        }

        /* End of the loop */
        if (c == null) {
            throw new Error("Unexpected null connection out of the loop. Bad escape path ?");
        }

        c.conn.setSocketTimeout(applicativeTimeout);

        host.inFlightConnections++;

        /* TODO: 
         *         connection.getParams().setDefaults(parent.getParams());
         * connection.setHttpConnectionManager(parent);
         * numConnections++;
         * hostPool.numConnections++;
         */

        /* We loose the association with the monitored connection, we'll recreate one at release time */
        return c.conn;
    }

    /* ************************ Entry point: release ******************** */

    public enum FailureType {
        OK,
        TIMEOUT,
        OTHER_ERROR
    }

    static ThreadLocal<FailureType> nextReleasedConnectionFailureType = new ThreadLocal<FailureType>();

    public void onHostFailure(HostConfiguration host, FailureType type) {
        nextReleasedConnectionFailureType.set(type);
    }

    /**
     * Marks the given connection as free and available for use by other requests.
     * TODO: WAKEUP
     * @param conn the HttpConnection to make available.
     */
    public void releaseConnection(HttpConnection conn) {
        LOG.trace("enter HttpConnectionManager.releaseConnection(HttpConnection)");

        if (conn instanceof HttpConnectionAdapter) {
            conn = ((HttpConnectionAdapter) conn).getWrappedConnection();
        }

        /* Find the host state for this connection */
        HostConfiguration config = rebuildConfigurationFromConnection(conn);
        HostState host = null;
        synchronized(this) {
            host = getHostFromConfiguration(config);
        }

        /* Rebuild the MonitoredConnection object */
        MonitoredConnection mc = new MonitoredConnection();
        mc.conn = conn;
        mc.host = host;

        if (nextReleasedConnectionFailureType.get() == null) {
            nextReleasedConnectionFailureType.set(FailureType.OK);
        }

        switch(nextReleasedConnectionFailureType.get()) {
        case TIMEOUT:
            /* This case is the most tricky.
             * But maybe it was because the remote host is down.
             * So let's take an average path: we mark all free connections for this
             * host as very old so that they get rechecked before any attempt
             * to use them.
             */
            synchronized(this) {
                host.markConnectionsAsUnchecked();
            }
            /* As a safety measure, we kill this connection and don't enqueue it */
            mc.conn.close();
            break;

        case OTHER_ERROR:
            /* Connection is dead, so we fast-kill the stale connections and we
             * schedule the server for monitoring ASAP.
             */
            synchronized(this) {
                host.killStaleConnections();
                setNextToMonitor(host);
            }
            /* And don't forget to kill this connection */
            mc.conn.close();
            break;

        case OK:
            long now = System.currentTimeMillis();
            synchronized(this) {
                mc.lastMonitoringTime = now;
                mc.lastUseTime = now;
                host.addFreeConnection(mc);
            }
            break;

        }
    }

    /* *************************** Hosts monitoring scheduler ************************* */

    LinkedList<HostState> nextToMonitorList = new LinkedList<HostState>();
    LinkedList<HostState> alreadyMonitored = new LinkedList<HostState>();
    void setNextToMonitor(HostState host) {

        /* Remove the host from the two lists */
        nextToMonitorList.remove(host);
        alreadyMonitored.remove(host);
        /* And put it at front of next */
        nextToMonitorList.addFirst(host);
    }

    HostState nextToMonitor() {
        if (nextToMonitorList.size() + alreadyMonitored.size() != hosts.size()){
            throw new Error("Inconsistent monitoring lists !!");
        }

        /* Swap buffers */
        if (nextToMonitorList.size() == 0) {
            LinkedList<HostState> tmp = nextToMonitorList;
            nextToMonitorList = alreadyMonitored;
            alreadyMonitored = tmp;
        }

        HostState next = nextToMonitorList.removeFirst();
        alreadyMonitored.addLast(next);
        return next;
    }

    /* **************************** Inspection and helpers *************************** */

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

    public void closeIdleConnections(long idleTimeout) {
        // TODO
    }

    /** Get the parameters of this connection manager */
    public HttpConnectionManagerParams getParams() {
        return this.params;
    }

    /** Assigns the parameters */
    public void setParams(final HttpConnectionManagerParams params) {
        if (params == null) {
            throw new IllegalArgumentException("Parameters may not be null");
        }
        this.params = params;
    }
}