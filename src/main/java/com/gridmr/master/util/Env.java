package com.gridmr.master.util;

public final class Env {
    private Env() {}

    public static String getEnvOrDefault(String key, String def) {
        String v = System.getenv(key);
        if (v == null || v.isEmpty()) return def;
        return v;
    }
}
