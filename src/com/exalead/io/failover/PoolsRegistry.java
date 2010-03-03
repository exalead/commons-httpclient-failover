package com.exalead.io.failover;
import java.util.*;

public class PoolsRegistry {
    static Map<String, MonitoredHttpConnectionManager> map = new HashMap<String, MonitoredHttpConnectionManager>();

    synchronized static List<String> poolNames() {
        ArrayList<String> list = new ArrayList<String>();
        list.addAll(map.keySet());
        return list;
    }

    synchronized static MonitoredHttpConnectionManager getPool(String name) {
        return map.get(name);
    }

    synchronized static void clear() {
        map.clear();
    }

    synchronized static void addPool(String name, MonitoredHttpConnectionManager manager){
        map.put(name, manager);
    }
}
