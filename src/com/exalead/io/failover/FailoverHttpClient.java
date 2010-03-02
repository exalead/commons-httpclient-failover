package com.exalead.io.failover;

import java.io.IOException;
import java.net.SocketTimeoutException;

import org.apache.commons.httpclient.DefaultHttpMethodRetryHandler;
import org.apache.commons.httpclient.HostConfiguration;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.params.HttpClientParams;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.log4j.Logger;

import com.exalead.io.failover.MonitoredHttpConnectionManager.FailureType;

public class FailoverHttpClient {
    protected MonitoredHttpConnectionManager manager;
    protected HttpClient client;
    
    public FailoverHttpClient() {
        manager = new MonitoredHttpConnectionManager();
        
        client = new HttpClient(manager);
        client.setParams(new HttpClientParams());
        client.getParams().setSoTimeout(5000);
        client.getParams().setConnectionManagerTimeout(50);
        manager.getParams().setStaleCheckingEnabled(false);
    }
    
    public void addHost(String host, int port, int power) {
        manager.addHost(host, port, power);
    }
    
    public int executeMethod(HttpMethod method) throws HttpException, IOException {
        // Fake config
        HostConfiguration config = new HostConfiguration();
        
        /* DO NOT retry magically the method */
        method.getParams().setParameter(HttpMethodParams.RETRY_HANDLER,
                new DefaultHttpMethodRetryHandler(0, false));
        
        try {
            return client.executeMethod(config, method);
        } catch (IOException e) {
            logger.warn("NoRetryHttpClient: exception is: ", e);
            if (e instanceof SocketTimeoutException) {
                manager.onHostFailure(config, FailureType.TIMEOUT);
            } else {
                manager.onHostFailure(config, FailureType.OTHER_ERROR);
            }
            throw e;
        }
    }
    
    private static Logger logger = Logger.getLogger("httpclient.failover");
}