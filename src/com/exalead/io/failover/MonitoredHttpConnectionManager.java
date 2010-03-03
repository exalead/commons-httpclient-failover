package com.exalead.io.failover;

import java.io.IOException;
import java.io.InputStream;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
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
import org.apache.commons.httpclient.params.HttpClientParams;
import org.apache.commons.httpclient.params.HttpConnectionManagerParams;
import org.apache.log4j.Logger;
import org.apache.log4j.NDC;

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

    private static final Logger logger = Logger.getLogger("httpclient.failover");

    /**
     * Gets the host configuration for a connection.
     * @param conn the connection to get the configuration of
     * @return a new HostConfiguration
     */
    static HostConfiguration rebuildConfigurationFromConnection(HttpConnection conn) {
        HostConfiguration connectionConfiguration = new HostConfiguration();
        connectionConfiguration.setHost(conn.getHost(), conn.getPort(), conn.getProtocol());
        if (conn.getLocalAddress() != null) {
            connectionConfiguration.setLocalAddress(conn.getLocalAddress());
        }
        if (conn.getProxyHost() != null) {
            connectionConfiguration.setProxy(conn.getProxyHost(), conn.getProxyPort());
        }
        return connectionConfiguration;
    }
    
    static void consumeLastResponse(HttpConnection conn) {
        InputStream lastResponse = conn.getLastResponseInputStream();
        if (lastResponse != null) {
            conn.setLastResponseInputStream(null);
            try {
                lastResponse.close();
            } catch (IOException ioe) {
                conn.close();
            }
        }
    }

    /* ***************** Global stuff ****************** */

    /** Collection of parameters associated with this connection manager. */
    private HttpConnectionManagerParams params = new HttpConnectionManagerParams(); 
    volatile boolean shutdown = false;

    public synchronized void addHost(String host, int port, int power) {
        HostState hs = new HostState();
        hs.configuration = new HostConfiguration();
        hs.configuration.setHost(host, port);
        hs.power = power;
        
        if (hostsMap.containsKey(hs.configuration)) {
            throw new IllegalArgumentException("Host: " + host + ":" + port + " already exists");
        }
         
        hosts.add(hs);
        
        for (int i = 0; i < power; i++) hostsForSelection.add(hs);
        
        hostsMap.put(hs.configuration, hs);
        nextToMonitorList.add(hs);
    }
    
    public synchronized void removeHost(String host, int port) {
        HostConfiguration hc = new HostConfiguration();
        hc.setHost(host, port);
        
        if (hosts.size() == 1) {
            throw new IllegalArgumentException("Can't remove last host of pool");
        }
        
        hostsMap.remove(hc);

        /* Clean-up the round robin structure */
        Iterator<HostState> iter = hostsForSelection.listIterator();
        while (iter.hasNext()) {
            HostState hs = iter.next();
            if (hs.configuration.equals(hc)) {
                iter.remove();
            }
        }
        currentHost = 0;
        
        /* Cleanup the monitoring structure and the main list */
        
        nextToMonitorList.clear();
        alreadyMonitored.clear();
        
        iter = hosts.listIterator();
        while (iter.hasNext()) {
            HostState hs = iter.next();
            if (hs.configuration.equals(hc)) {
                iter.remove();
            } else {
               nextToMonitorList.add(hs);
            }
        }
    }

    /** The list of all hosts in the pool */
    List<HostState> hosts = new ArrayList<HostState>();
    /** Fast access map by configuration */
    Map<HostConfiguration, HostState> hostsMap = new HashMap<HostConfiguration, HostState>();

    /**
     * Shuts down the connection manager and releases all resources.  All connections associated 
     * with this class will be closed and released. 
     * Cleans up all connection pool resources.
     * <p>The connection manager can no longer be used once shut down.  
     * 
     * <p>Calling this method more than once will have no effect.
     */
    public synchronized void shutdown() {
        if (shutdown) return;
        
        for (HostState hs : hosts) {
            for (MonitoredConnection mc : hs.freeConnections) {
                mc.conn.close();
            }
            hs.freeConnections.clear();
        }
    }

    /* *************************** Location helpers ************************* */

    /** Find a Host given its configuration. Must be called with the lock */
    private HostState getHostFromConfiguration(HostConfiguration config) {
        HostState hs = hostsMap.get(config);
        if (hs == null) {
            throw new Error("Host: " + config +  " not found !");
        }
        return hs;
    }

    /* *************************** Hosts round-robin dispatch ************************* */

    private List<HostState> hostsForSelection = new ArrayList<HostState>();
    /** Index of the current host in the list */
    private int currentHost;

    /** 
     * Get the host to use for next connection. 
     * It performs round-robin amongst currently alive hosts, respecting the power property.
     * This method must be called with the pool lock.
     * TODO: Improve this method 
     */
    private HostState getNextRoundRobinHost() throws IOException {
        boolean alreadyReachedEnd = false;
        while (true) {
            currentHost++;
            if (currentHost >= hostsForSelection.size()) {
                if (alreadyReachedEnd) {
                    currentHost = 0;
                    /* Oops, all hosts are down ! */
                    throw new IOException("All hosts are down");
                }
                currentHost = 0;
                alreadyReachedEnd = true;
            }
            if (!hostsForSelection.get(currentHost).down) {
                break;
            }
        }
        return hostsForSelection.get(currentHost);
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
    int failMaxTries = 2;
    long failTimeout = 200;
    int maxIdleConnectionsPerHost;
    /** Should we perform auto scaling-down of idle connections */
    boolean autoScaleIdleConnections;
    String isAlivePath = "isAlive";

    /* *************************** isAlive monitoring ************************* */

    /** Fake manager to which we "release" the isAlive connection */
    static class DummyManager implements HttpConnectionManager {
        public void closeIdleConnections(long idleTimeout) {
        }
        public HttpConnection getConnection(HostConfiguration hostConfiguration) {
            return null;
        }
        public HttpConnection getConnection(HostConfiguration hostConfiguration, long timeout) {
            return null;
        }
        public HttpConnection getConnectionWithTimeout(
                HostConfiguration hostConfiguration, long timeout)
        throws ConnectionPoolTimeoutException {
            return null;
        }
        public HttpConnectionManagerParams getParams() {
            return null;
        }
        public void releaseConnection(HttpConnection conn) {
        }
        public void setParams(HttpConnectionManagerParams params) {
        }
    }
    DummyManager dummyManager = new DummyManager();

    /** 
     * Returns "true" if host is up and alive.
     * Returns "false" if host is up but not alive
     * Throws an exception in case of connection error
     */
    boolean checkConnection(MonitoredConnection connection) throws IOException {
        /* TODO: HANDLE really state (for the case where isAlive is password-protected) */
        HttpState hs = new HttpState();
        GetMethod gm = new GetMethod(connection.host.getURI() + "/" + isAlivePath);
        
        gm.getParams().setDefaults(new HttpClientParams());

        connection.conn.setSocketTimeout(isAliveTimeout);
        int statusCode = gm.execute(hs, connection.conn);
        connection.conn.setHttpConnectionManager(dummyManager);
        consumeLastResponse(connection.conn);
        return statusCode < 400;
    }

    /* *************************** Entry point: acquire ************************* */

    /** @see HttpConnectionManager#getConnection(HostConfiguration) */
    @Deprecated
    public HttpConnection getConnection(HostConfiguration hostConfiguration) {
        while (true) {
            try {
                return getConnectionWithTimeout(hostConfiguration, 0);
            } catch (ConnectionPoolTimeoutException e) {
                // This should not happen as, without timeout, we loop until we have something
                logger.error("Unexpected exception while waiting for connection", e);
            }
        }
    }

    /**
     * @see HttpConnectionManager#getConnection(HostConfiguration, long)
     * @deprecated Use #getConnectionWithTimeout(HostConfiguration, long)
     */
    public HttpConnection getConnection(HostConfiguration hostConfiguration, 
            long timeout) throws HttpException {

        logger.trace("enter HttpConnectionManager.getConnection(HostConfiguration, long)");
        try {
            return getConnectionWithTimeout(hostConfiguration, timeout);
        } catch(ConnectionPoolTimeoutException e) {
            throw new HttpException(e.getMessage());
        }
    }

    /**
     * Gets a connection or waits if one is not available. A connection is available
     * if not all hosts of the cluster are down and if the maximum number of connections
     * is not reached.
     *
     * @param hostConfiguration This parameter is ignored
     * @param timeout the number of milliseconds to wait for a connection, 0 to
     * wait indefinitely
     * @throws ConnectionPoolTimeoutException if a connection does not become available in
     * 'timeout' milliseconds
     */
    public HttpConnection getConnectionWithTimeout(HostConfiguration hostConfiguration, 
            long timeout) throws ConnectionPoolTimeoutException {
        logger.debug("HttpConnectionManager.getConnection:timeout = " + timeout);
        // wrap the connection in an adapter so we can ensure it is used 
        // only once
        return new HttpConnectionAdapter(doGetConnection(timeout));
    }

    /** 
     * Try to acquire a connection from all hosts in the cluster. Loop if all hosts are down
     * until the timeout has expired.
     */
    private HttpConnection doGetConnection(long timeout) throws ConnectionPoolTimeoutException {
        // TODO: connections restriction + connections restriction timeout
        //int maxTotalConnections = this.params.getMaxTotalConnections();
        //int maxHostConnections = maxTotalConnections;

        long start = System.currentTimeMillis();
        boolean useTimeout = (timeout > 0);

        if (shutdown) {
            throw new IllegalStateException("Connection factory has been shutdown.");
        }

        NDC.push("acquire");
        try {
            HttpConnection connection = null;
            int loops = 0;
            while (true) {
                loops++;
                if (loops > failMaxTries) {
                    throw new ConnectionPoolTimeoutException("Could not acquire any connection (all hosts down)");
                }
                if (loops > 1) {
                    logger.info("Restart trying to acquire on any host (loop " + loops + ")");
                }
                
                try {
                    connection = acquireConnectionOnAnyHost();
                    break;
                } catch (PoolAcquireException e) {
                    logger.warn("All cluster hosts are down !");
                    if (useTimeout && (System.currentTimeMillis() - start > failTimeout)) {
                        logger.warn("Timeout -> ConnectionPoolTimeoutException");
                        throw new ConnectionPoolTimeoutException("Could not acquire any connection (all hosts down)");
                    }
                    try { Thread.sleep(50); } catch (InterruptedException e2) {}
                }
            }
            return connection;
        } finally {
            NDC.pop();
        }
    }

    /**
     * Try to create a connection by looping on all hosts of the cluster.
     * @throws PoolAcquireException if all hosts in the cluster are down
     */
    private HttpConnection acquireConnectionOnAnyHost() throws PoolAcquireException {
        HttpConnection connection = null;
        for (int i = 0; i < hosts.size(); i++) {
            HostState hs = null;
            try {
                synchronized(this) {
                    hs = getNextRoundRobinHost();
                }
            } catch (IOException e) {
                /* Crap, we already know that all hosts are down, break */
                break;
            }
            try {
                connection = acquireConnection(hs);
                break;
            } catch (PoolAcquireException e) {
                logger.info("This host (" + hs + ") is down, goto next");
                // This host is down, goto next
                continue;
            }
        }
        if (connection == null) {
            throw new PoolAcquireException("All hosts are down");
        }
        connection.setHttpConnectionManager(this);
        return connection;
    }


    /**
     * Try to acquire a connection on a specific host of the cluster.
     * @throws PoolAcquireException if this host is down
     */ 
    private  HttpConnection acquireConnection(HostState host) throws PoolAcquireException {
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
                if (logger.isDebugEnabled()) logger.debug("Acquire connection for " + host);
                long now = System.currentTimeMillis();

                if (host.down) {
                    logger.info("oups, host is down: " + host);
                    throw new PoolAcquireException("Host is down (marked as down)");
                }

                long minDate = now - maxCheckDelayWithoutSynchronousCheck;
                List<MonitoredConnection> recentlyChecked = host.getRecentlyCheckedConnections(minDate);
                if (recentlyChecked.size() == 0) {
                    logger.info("No recently checked connection for " + host);
                    /* There is no recently checked connection */
                    needSynchronousCheck = true;
                    /* Let's help a bit the monitoring thread by taking a connection that was not checked recently */
                    c = host.getOldestCheckedConnection();
                } else {
                    // TODO: better scheduling ?
                    logger.debug("Have a recently checked connection");
                    c = recentlyChecked.get(0);
                }

                if (c != null) {
                    host.removeFreeConnection(c);
                }
            }

            /* There was no free connection for this host, so let's connect now */
            if (c == null) {
                // Always check newly created creations in case the host is up but not alive
                needSynchronousCheck = true;
                try {
                    logger.info("No free connection, connect to: " + host);
                    c = host.connect(connectionTimeout);
                } catch (IOException e) {
                    /* In that case, we don't care if it's a fail or timeout:
                     * we can't connect to the host in time, so the host is down.
                     */
                    synchronized(this) {
                        logger.info("Connection failed: " + e.getMessage() +" --> host is down");
                        host.down = true;
                        // If the host has connections, it means that they were 
                        // established while we were trying to connect. .. So maybe the host
                        // is not down, but to be sure, let's still consider as down and 
                        // kill everything
                        host.killAllConnections();
                        throw new PoolAcquireException("Host is down (couldn't connect)", e);
                    }
                }
            }
            if (c == null) throw new Error("Error, null connection");

            /* The connection we got was too old, perform a check */
            if (needSynchronousCheck) {
                try {
                    if (logger.isDebugEnabled()) logger.debug("Check connection for " + host);
                    boolean ret = checkConnection(c);
                    /* Host is up but not alive: just kill all connections.
                     * It's useless to try another connection: host knows it's not alive
                     */

                    if (ret == false) {
                        synchronized(this) {
                            logger.info("Host is not alive:"  + host);
                            host.down = true;
                            host.killAllConnections();
                            throw new PoolAcquireException("Host is down (not alive)");
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
                            logger.info("Host isAlive check timeout: " + host);
                            host.down = true;
                            host.killAllConnections();
                            /* Don't forget to close this connection to avoid FD leak */
                            c.conn.close();
                            throw new PoolAcquireException("Host is down (isAlive timeout)", e);
                        } else {
                            /* Connection failure. Server looks down (connection reset by peer). But it could
                             * be only that connection which failed (TCP timeout for example). In that case, 
                             * we try to fast-kill the stale connections and we retry. Either there are still
                             * some alive connections and we can retry with them, or the loop will try to 
                             * reconnect and success (for example, the host went down and up very fast) or fail 
                             */
                            logger.info("Host isAlive check failed: " + host + ": " + e.getMessage());
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

        try {
            c.conn.setSocketTimeout(applicativeTimeout);
        } catch (Exception e) {
            throw new Error("Failed to set socket timeout", e);
        }

        synchronized(this) {
            host.usedConnections++;
            /* Keep track of the real maximum of connections that were allocated before the next
             * monitoring loop
             */
            if (host.usedConnections > host.usedConnectionsInPast[host.usedConnectionsInPastIdx]) {
                host.usedConnectionsInPast[host.usedConnectionsInPastIdx] = host.usedConnections;
            }
        }
        
        // We do stale checking ourselves, DO NOT do it !
        c.conn.getParams().setDefaults(this.getParams());
        c.conn.getParams().setStaleCheckingEnabled(false);
        c.conn.setHttpConnectionManager(this);
        
        /* We loose the association with the monitored connection, we'll recreate one at release time */
        return c.conn;
    }

    /* ************************ Entry point: release ******************** */

    /**
     * Marks the given connection as free and available for use by other requests.
     * @param conn the HttpConnection to make available.
     */
    public void releaseConnection(HttpConnection conn) {
        NDC.push("release");
        logger.trace("enter HttpConnectionManager.releaseConnection(HttpConnection)");

        if (conn instanceof HttpConnectionAdapter) {
            conn = ((HttpConnectionAdapter) conn).getWrappedConnection();
        }

        /* Find the host state for this connection */
        HostConfiguration config = rebuildConfigurationFromConnection(conn);

        /* Rebuild the MonitoredConnection object */
        MonitoredConnection mc = new MonitoredConnection();
        mc.conn = conn;

        synchronized(this) {
            HostState host = getHostFromConfiguration(config); 
            mc.host = host;
            host.usedConnections--;
            
            if (!mc.conn.isOpen()) {
                logger.info("Releasing a CLOSED connection !");
                // OK, this case is the most tricky.
                // What we know is that something went wrong with this connection, but we don't
                // know if it was reset or timeout, because HttpClient has already hidden the details :(
                // Maybe just the connection was broken or maybe the host is down
                //
                // So let's take an average path: we mark all free connections for this
                // host as very old so that they get rechecked before any attempt
                // to use them. Basically, we tell everyone "don't trust this host, check first"
                host.markConnectionsAsUnchecked();
            } else {
                if (maxIdleConnectionsPerHost != 0 && mc.host.freeConnections.size() >= maxIdleConnectionsPerHost) {
                    logger.info("Discarding returned connection (too many idle ones)");
                    mc.conn.close();
                } else {
                    long now = System.currentTimeMillis();
                    mc.lastMonitoringTime = now;
                    mc.lastUseTime = now;
                    host.addFreeConnection(mc);
                }
            }
        }
        NDC.pop();
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
     * Gets the total number of currently active (checked out) connections
     */
    public synchronized int getUsedConnections() {
        int total = 0;
        for (HostState hs : hosts) {
            total += hs.usedConnections;
        }
        return total;
    }

    /**
     * Gets the total number of free (in pool) connections.
     */
    public int getConnectionsInPool() {
        int total = 0;
        for (HostState hs : hosts) {
            total += hs.freeConnections.size();
        }
        return total;
    }

    /** Get the parameters of this connection manager */
    @Override
    public HttpConnectionManagerParams getParams() {
        return this.params;
    }

    /** Assigns the parameters */
    @Override
    public void setParams(final HttpConnectionManagerParams params) {
        if (params == null) {
            throw new IllegalArgumentException("Parameters may not be null");
        }
        this.params = params;
    }
    
    @Override
    public void closeIdleConnections(long idleTimeout) {
        // TODO ?
    }
}