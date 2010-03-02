package com.exalead.io.failover;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.httpclient.DefaultHttpMethodRetryHandler;
import org.apache.commons.httpclient.HostConfiguration;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.params.HttpClientParams;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.log4j.Logger;

public class FailoverHttpClient {
    protected MonitoredHttpConnectionManager manager;
    protected HttpClient client;
    protected List<PoolMonitoringThread> threads = new ArrayList<PoolMonitoringThread>();
    
    public FailoverHttpClient() {
        manager = new MonitoredHttpConnectionManager();
        client = new HttpClient(manager);
        
        client.setParams(new HttpClientParams());
       // client.getParams().setSoTimeout(5000);
        manager.getParams().setStaleCheckingEnabled(false);
    }
    
    public void setConnectionAcquireTimeout(long timeout) {
        client.getParams().setConnectionManagerTimeout(timeout);
    }
    
    public void startMonitoring(int nthreads) {
        int delay = 1000 / manager.hosts.size() * nthreads;
        for (int i = 0; i < nthreads; i++) {
            PoolMonitoringThread pmt = new PoolMonitoringThread();
            pmt.loopDelay = delay;
            pmt.start();
            threads.add(pmt);
        }
    }
    
    public void shutdown() {
        for (PoolMonitoringThread pmt : threads) {
            pmt.stop = true;
            try { pmt.join();} catch (InterruptedException e) {}
        }
        manager.shutdown();
    }
    
    public void addHost(String host, int port, int power) {
        manager.addHost(host, port, power);
    }
    
    public int executeMethod(HttpMethod method) throws HttpException, IOException {
        return executeMethod(method, 0);
    }
    
    
    public int executeMethod(HttpMethod method, int timeout) throws HttpException, IOException {
        // Fake config, the underlying manager manages all
        HostConfiguration config = new HostConfiguration();
        
        /* Set method parameters */
        method.getParams().setSoTimeout(timeout);
        /* DO NOT retry magically the method */
        method.getParams().setParameter(HttpMethodParams.RETRY_HANDLER,
                new DefaultHttpMethodRetryHandler(0, false));
        
        try {
            return client.executeMethod(config, method);
        } catch (IOException e) {
            /* If we get an exception at that point, the underlying connection has
             * already been released, so we cannot set the failure type anymore */
            logger.warn("NoRetryHttpClient: exception in executeMethod: " + e.getMessage());
            throw e;
        }
    }

//    TBD: Would we really gain *anything* ?
//    /** 
//     * If you get an error after executeMethod, while reading the response stream, you can 
//     * inform the FailoverClient about its type:
//     *  - TIMEOUT if you got a SocketTimeoutException (could indicate that the host is hanged)
//     *  - OTHER_ERROR else
//     */
//    public void setThreadLocalFailureType(FailureType type) {
//        manager.onHostFailure(host, type)
//    }
    
    private static Logger logger = Logger.getLogger("httpclient.failover");
}