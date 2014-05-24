/* Copyright 2010 Exalead S.A.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or 
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. 
 */

package com.exalead.io.failover;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.httpclient.URI;
import org.apache.commons.httpclient.UsernamePasswordCredentials;
import org.apache.commons.httpclient.DefaultHttpMethodRetryHandler;
import org.apache.commons.httpclient.HostConfiguration;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.auth.AuthScope;
import org.apache.commons.httpclient.params.HttpClientParams;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FailoverHttpClient {
    protected MonitoredHttpConnectionManager manager;
    protected HttpClient client;
    protected List<PoolMonitoringThread> threads = new ArrayList<PoolMonitoringThread>();
    
    public FailoverHttpClient() {
        manager = new MonitoredHttpConnectionManager();
        client = new HttpClient(manager);
        
        client.setParams(new HttpClientParams());
        manager.getParams().setStaleCheckingEnabled(false);
    }
 
     public void setCredentials(String login, String password) {
        manager.setCredentials(new UsernamePasswordCredentials(login, password));
    }
    
    /** 
     * Maximum number of tries to acquire a connection if all
     * hosts are down
     */
    public void setConnectionAcquireFailMaxTries(int tries) {
        manager.failMaxTries = tries;
    }

    /** 
     * Maximum time to acquire a connection if all hosts are down
     */
    public void setConnectionAcquireFailTimeout(long timeout) {
        manager.failTimeout = timeout;
    }
    
    /** 
     * Set the path on which the "isAlive" service is mounted.
     * isAlive must reply < 400 if host is alive, > 500 if host is down
     */
    public void setIsAlivePath(String isAlivePath) {
        manager.isAlivePath = isAlivePath;
    }
    
    /**
     * Set the timeout for establishing connection
     * @param timeout timeout in milliseconds
     */
    public void setConnectTimeout(int timeout) {
        manager.connectionTimeout = timeout;
    }
    
    /**
     * Set the timeout for the isAlive service to answer
     * @param timeout timeout in milliseconds
     */
    public void setIsAliveTimeout(int timeout) {
        manager.isAliveTimeout = timeout;
    }
    
    public MonitoredHttpConnectionManager getConnectionManager() {
        return manager;
    }

//    /** Maximum time to wait for a connection to become available
//     * (if all hosts are not down and max number of connections
//     * is exceeded)
//     */
//    public void setConnectionAcquireTimeout(long timeout) {
//        client.getParams().setConnectionManagerTimeout(timeout);
//    }
    
    public void startMonitoring(int nthreads) {
        int delay = 1000 / manager.hosts.size() * nthreads;
        for (int i = 0; i < nthreads; i++) {
            PoolMonitoringThread pmt = new PoolMonitoringThread();
            pmt.loopDelay = delay;
            pmt.pool = manager;
            pmt.start();
            threads.add(pmt);
        }
    }

    public void shutdown() {
        for (PoolMonitoringThread pmt : threads) {
            try { pmt.join();} catch (InterruptedException e) {}
        }
        manager.shutdown();
    }

    public void addHost(URI uri, int power) {
    	manager.addHost(uri, power);
    }
    
    public void addHost(String host, int port, int power) {
        manager.addHost(host, port, power);
    }

    public int executeMethod(HttpMethod method) throws HttpException, IOException {
        return executeMethod(method, 0, 1);
    }

    public int executeMethod(HttpMethod method, int timeout) throws HttpException, IOException {
        return executeMethod(method, timeout, 1);
    }

    public int executeMethod(HttpMethod method, int timeout, int retries) throws HttpException, IOException {
    	if (manager.hosts.size() == 0) {
    		logger.error("Could not execute method without any host.");
    		throw new HttpException("Trying to execute methods without host");
    	}
        // Fake config, the underlying manager manages all
        HostConfiguration config = new HostConfiguration();

        /* Set method parameters */
        method.getParams().setSoTimeout(timeout);
        /* DO NOT retry magically the method */
        method.getParams().setParameter(HttpMethodParams.RETRY_HANDLER,
                new DefaultHttpMethodRetryHandler(0, false));

        if (manager.getCredentials() != null) {
            client.getParams().setAuthenticationPreemptive(true);
            client.getState().setCredentials(new AuthScope(AuthScope.ANY), manager.getCredentials());
        }

        IOException fail = null;
        for (int i = 1; i <= retries; ++i) {
        	try {
        		return client.executeMethod(config, method);
        	} catch (IOException e) {
        		logger.warn("Failed to execute method - try " + i + "/" + retries);
        		fail = e;
        		continue;
        	}
        }
        logger.warn("exception in executeMethod: " + fail.getMessage());
        throw fail;
    }

    final Logger logger = LoggerFactory.getLogger("httpclient.failover");

}
