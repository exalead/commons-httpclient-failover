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
