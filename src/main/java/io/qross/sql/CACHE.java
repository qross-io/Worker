package io.qross.sql;

import io.qross.util.DateTime;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CACHE {

    private static Map<String, Map<String, Object>> ALL = new HashMap<>();
    private static Map<String, Map<String, Long>> EXPIRE = new HashMap<>();

    private static int lastHour = DateTime.now().getHour();

    public static void set(String apiName, Object value) {
        set(apiName, "", value);
    }

    public static synchronized void set(String apiName, String params, Object value) {
        if (!ALL.containsKey(apiName)) {
            ALL.put(apiName, new HashMap<>());
            EXPIRE.put(apiName, new HashMap<>());
        }
        ALL.get(apiName).put(params, value);
        EXPIRE.get(apiName).put(params, DateTime.now().toEpochSecond());
    }

    public static Object get(String apiName) {
        return get(apiName, "");
    }

    public static Object get(String apiName, String params) {
        if (ALL.containsKey(apiName) && ALL.get(apiName).containsKey(params)) {
            if (!EXPIRE.containsKey(apiName)) {
                EXPIRE.put(apiName, new HashMap<>());
            }
            EXPIRE.get(apiName).put(params, DateTime.now().toEpochSecond());
            return ALL.get(apiName).get(params);
        }
        else {
            return null;
        }
    }

    public static boolean contains(String apiName) {
        return contains(apiName, "");
    }

    public static boolean contains(String apiName, String params) {
        CACHE.clean();
        return ALL.containsKey(apiName) && ALL.get(apiName).containsKey(params);
    }

    public static void remove(String apiName) {
        ALL.remove(apiName);
    }

    public static void remove(String apiName, String params) {
        if (ALL.containsKey(apiName) && ALL.get(apiName).containsKey(params)) {
            ALL.get(apiName).remove(params);
        }
    }

    public static synchronized void clean() {
        DateTime now = DateTime.now();
        int hour = now.getHour();
        if (hour != lastHour) {
            long second = now.toEpochSecond();
            //待清除列表
            List<NameAndParams> nps = new ArrayList<>();
            for(String name : EXPIRE.keySet()) {
                for (String params : EXPIRE.get(name).keySet()) {
                    if (second - EXPIRE.get(name).get(params) >= 3600) {
                        nps.add(new NameAndParams(name, params));
                    }
                }
            }

            for (NameAndParams np : nps) {
                EXPIRE.get(np.name).remove(np.params);
                ALL.get(np.name).remove(np.params);
            }

            nps.clear();
            lastHour = hour;
        }
    }
}

class NameAndParams {

    public String name;
    public String params;

    public NameAndParams(String name, String params) {
        this.name = name;
        this.params = params;
    }
}
