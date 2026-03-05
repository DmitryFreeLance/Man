package com.reviewsbot;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

public final class Payload {
    private Payload() {}

    public static Map<String, String> parse(String payload) {
        Map<String, String> map = new HashMap<>();
        if (payload == null || payload.isBlank()) {
            return map;
        }
        String[] parts = payload.split(";");
        for (String part : parts) {
            if (part.isBlank() || !part.contains("=")) continue;
            String[] kv = part.split("=", 2);
            String key = kv[0];
            String val = kv.length > 1 ? kv[1] : "";
            try {
                byte[] decoded = Base64.getUrlDecoder().decode(val);
                map.put(key, new String(decoded, StandardCharsets.UTF_8));
            } catch (IllegalArgumentException ex) {
                map.put(key, val);
            }
        }
        return map;
    }

    public static String build(Map<String, String> map) {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, String> e : map.entrySet()) {
            if (sb.length() > 0) sb.append(';');
            String encoded = Base64.getUrlEncoder().encodeToString(e.getValue().getBytes(StandardCharsets.UTF_8));
            sb.append(e.getKey()).append('=').append(encoded);
        }
        return sb.toString();
    }

    public static String put(String payload, String key, String value) {
        Map<String, String> map = parse(payload);
        map.put(key, value == null ? "" : value);
        return build(map);
    }

    public static String get(String payload, String key) {
        return parse(payload).get(key);
    }
}
