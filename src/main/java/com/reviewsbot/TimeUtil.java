package com.reviewsbot;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public final class TimeUtil {
    private TimeUtil() {}

    public static String nowIso() {
        return OffsetDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
    }

    public static Instant parseIso(String iso) {
        if (iso == null || iso.isBlank()) {
            return null;
        }
        return OffsetDateTime.parse(iso).toInstant();
    }

    public static String addDaysIso(int days) {
        return OffsetDateTime.now(ZoneOffset.UTC).plusDays(days).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
    }

    public static String fromInstant(Instant instant) {
        if (instant == null) {
            return null;
        }
        return OffsetDateTime.ofInstant(instant, ZoneOffset.UTC).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
    }
}
