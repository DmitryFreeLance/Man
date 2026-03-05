package com.reviewsbot;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class BotConfig {
    public final String botToken;
    public final String botUsername;
    public final Set<Long> adminIds;
    public final String secretCode;
    public final String dbPath;
    public final String providerToken;

    public final int taxSystemCode;
    public final int vatCode;
    public final String paymentSubject;
    public final String paymentMode;

    public final int defaultPriceWeek;
    public final int defaultPriceMonth;
    public final int defaultPriceSingle;

    public BotConfig() {
        this.botToken = envOrThrow("BOT_TOKEN");
        this.botUsername = envOrDefault("BOT_USERNAME", "ReviewsBot");
        this.secretCode = envOrDefault("SECRET_CODE", "1234");
        this.dbPath = envOrDefault("DB_PATH", "data/reviewsbot.sqlite");
        this.providerToken = envOrThrow("PROVIDER_TOKEN");

        this.defaultPriceWeek = Integer.parseInt(envOrDefault("PRICE_WEEK", "200"));
        this.defaultPriceMonth = Integer.parseInt(envOrDefault("PRICE_MONTH", "500"));
        this.defaultPriceSingle = Integer.parseInt(envOrDefault("PRICE_SINGLE", "99"));

        this.taxSystemCode = Integer.parseInt(envOrDefault("TAX_SYSTEM_CODE", "1"));
        this.vatCode = Integer.parseInt(envOrDefault("VAT_CODE", "1"));
        this.paymentSubject = envOrDefault("PAYMENT_SUBJECT", "service");
        this.paymentMode = envOrDefault("PAYMENT_MODE", "full_payment");

        String admins = envOrDefault("ADMIN_IDS", "");
        if (admins.isBlank()) {
            this.adminIds = Collections.emptySet();
        } else {
            Set<Long> ids = new HashSet<>();
            Arrays.stream(admins.split(","))
                    .map(String::trim)
                    .filter(s -> !s.isBlank())
                    .forEach(s -> ids.add(Long.parseLong(s)));
            this.adminIds = ids;
        }
    }

    private static String envOrDefault(String key, String def) {
        String val = System.getenv(key);
        return (val == null || val.isBlank()) ? def : val;
    }

    private static String envOrThrow(String key) {
        String val = System.getenv(key);
        if (val == null || val.isBlank()) {
            throw new IllegalStateException("Missing env var: " + key);
        }
        return val;
    }
}
