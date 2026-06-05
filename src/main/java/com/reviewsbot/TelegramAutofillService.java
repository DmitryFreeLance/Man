package com.reviewsbot;

import com.reviewsbot.Db.Man;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class TelegramAutofillService implements AutoCloseable {
    private static final String EXISTING_MAX_MAN_ID_KEY = "tg_autofill_existing_max_man_id";
    private static final String LAST_RUN_AT_KEY = "tg_autofill_last_run_at";
    private static final long MIN_FAKE_TG_ID = 100_000_000L;
    private static final long MAX_FAKE_TG_ID = 9_999_999_999L;

    private final BotConfig config;
    private final Db db;
    private final GoogleSheetsService sheets;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread thread = new Thread(r, "tg-autofill");
        thread.setDaemon(true);
        return thread;
    });

    public TelegramAutofillService(BotConfig config, Db db, GoogleSheetsService sheets) {
        this.config = config;
        this.db = db;
        this.sheets = sheets;
    }

    public void start() {
        if (!config.tgAutofillEnabled) {
            System.out.println("TG autofill disabled");
            return;
        }
        scheduler.scheduleWithFixedDelay(this::runSafely, 0, 1, TimeUnit.MINUTES);
        System.out.println("TG autofill enabled: 1 card every " + config.tgAutofillIntervalMinutes + " minute(s)");
    }

    private void runSafely() {
        try {
            backfillNextIfDue();
        } catch (Exception ex) {
            System.err.println("TG autofill failed");
            ex.printStackTrace();
        }
    }

    private void backfillNextIfDue() throws Exception {
        Instant now = Instant.now();
        Instant lastRunAt = parseInstant(db.getSetting(LAST_RUN_AT_KEY));
        if (lastRunAt != null
                && lastRunAt.plus(Duration.ofMinutes(config.tgAutofillIntervalMinutes)).isAfter(now)) {
            return;
        }

        int existingMaxManId = ensureExistingSnapshotMaxManId();
        Integer backfillFromManId = resolveManId(config.tgAutofillBackfillFromUsername);
        Integer backfillToManId = resolveManId(config.tgAutofillBackfillToUsername);

        Man man = db.findNextManForTelegramAutofill(existingMaxManId, backfillFromManId, backfillToManId);
        if (man == null) {
            return;
        }

        String tgUsername = normalizeUsername(man.tgUsername());
        if (tgUsername == null) {
            return;
        }

        String tgId = nextGeneratedTgId();
        db.updateManTelegram(man.id(), tgUsername, tgId);
        db.setSetting(LAST_RUN_AT_KEY, TimeUtil.fromInstant(now));
        sheets.syncManReviews(man.id());
        System.out.println("TG autofill updated man #" + man.id() + ": @" + tgUsername + " -> " + tgId);
    }

    private int ensureExistingSnapshotMaxManId() throws Exception {
        long saved = db.getSettingLong(EXISTING_MAX_MAN_ID_KEY);
        if (saved > 0) {
            return Math.toIntExact(saved);
        }
        int currentMax = db.getMaxManId();
        db.setSetting(EXISTING_MAX_MAN_ID_KEY, String.valueOf(currentMax));
        return currentMax;
    }

    private Integer resolveManId(String username) throws Exception {
        String normalized = normalizeUsername(username);
        if (normalized == null) {
            return null;
        }
        Man man = db.findManByTgUsernameAny(normalized);
        return man == null ? null : man.id();
    }

    private String nextGeneratedTgId() throws Exception {
        for (int attempt = 0; attempt < 256; attempt++) {
            long candidate = ThreadLocalRandom.current().nextLong(MIN_FAKE_TG_ID, MAX_FAKE_TG_ID + 1);
            String tgId = String.valueOf(candidate);
            if (db.findManByTgIdAny(tgId) == null) {
                return tgId;
            }
        }
        return String.valueOf(System.currentTimeMillis());
    }

    private Instant parseInstant(String value) {
        try {
            return TimeUtil.parseIso(value);
        } catch (Exception ignored) {
            return null;
        }
    }

    private String normalizeUsername(String input) {
        if (input == null) return null;
        String text = input.trim();
        if (text.isEmpty()) return null;
        text = text.replace("https://", "").replace("http://", "");
        if (text.startsWith("t.me/")) {
            text = text.substring(5);
        } else if (text.startsWith("telegram.me/")) {
            text = text.substring(12);
        }
        if (text.startsWith("@")) {
            text = text.substring(1);
        }
        text = text.replaceAll("[^A-Za-z0-9_]", "");
        return text.isBlank() ? null : text;
    }

    @Override
    public void close() {
        scheduler.shutdownNow();
    }
}
