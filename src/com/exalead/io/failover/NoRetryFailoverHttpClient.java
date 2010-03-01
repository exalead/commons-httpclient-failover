package com.exalead.io.failover;

import java.io.IOException;

import org.apache.commons.httpclient.HostConfiguration;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.log4j.Logger;

public class NoRetryFailoverHttpClient {
    protected MonitoredHttpConnectionManager manager;
    protected HttpClient client;
    
    public NoRetryFailoverHttpClient() {
        manager = new MonitoredHttpConnectionManager();

        client = new HttpClient(manager);
    }
    
    public void addHost(String host, int port, int power) {
        manager.addHost(host, port, power);
    }
    
    public int executeMethod(HttpMethod method) throws HttpException, IOException {
        HostConfiguration config = manager.getHostToUse();
        
        try {
            return client.executeMethod(config, method);
        } catch (IOException e) {
            logger.warn("Exception is", e);
            throw e;
        }
    }
    
    private static Logger logger = Logger.getLogger("monitored");
}