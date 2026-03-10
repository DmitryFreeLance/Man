package com.reviewsbot;

import com.reviewsbot.Db.Man;
import com.reviewsbot.Db.Payment;
import com.reviewsbot.Db.Review;
import com.reviewsbot.Db.Stats;
import com.reviewsbot.Db.User;
import org.telegram.telegrambots.bots.TelegramLongPollingBot;
import org.telegram.telegrambots.meta.api.methods.AnswerCallbackQuery;
import org.telegram.telegrambots.meta.api.methods.AnswerPreCheckoutQuery;
import org.telegram.telegrambots.meta.api.methods.groupadministration.GetChat;
import org.telegram.telegrambots.meta.api.methods.send.SendMessage;
import org.telegram.telegrambots.meta.api.methods.send.SendPhoto;
import org.telegram.telegrambots.meta.api.methods.send.SendInvoice;
import org.telegram.telegrambots.meta.api.methods.send.SendDocument;
import org.telegram.telegrambots.meta.api.methods.updatingmessages.EditMessageText;
import org.telegram.telegrambots.meta.api.methods.updatingmessages.EditMessageMedia;
import org.telegram.telegrambots.meta.api.objects.CallbackQuery;
import org.telegram.telegrambots.meta.api.objects.Chat;
import org.telegram.telegrambots.meta.api.objects.InputFile;
import org.telegram.telegrambots.meta.api.objects.Message;
import org.telegram.telegrambots.meta.api.objects.PhotoSize;
import org.telegram.telegrambots.meta.api.objects.media.InputMediaPhoto;
import org.telegram.telegrambots.meta.api.objects.Update;
import org.telegram.telegrambots.meta.api.objects.payments.LabeledPrice;
import org.telegram.telegrambots.meta.api.objects.payments.PreCheckoutQuery;
import org.telegram.telegrambots.meta.api.objects.payments.SuccessfulPayment;
import org.telegram.telegrambots.meta.api.objects.replykeyboard.InlineKeyboardMarkup;
import org.telegram.telegrambots.meta.api.objects.replykeyboard.buttons.InlineKeyboardButton;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.io.File;
import java.io.FileOutputStream;

public class BotService extends TelegramLongPollingBot {
    private final BotConfig config;
    private final Db db;
    private final GoogleSheetsService sheets;
    private final DateTimeFormatter dateFmt = DateTimeFormatter.ofPattern("dd.MM.yyyy").withLocale(Locale.forLanguageTag("ru"));

    public BotService(BotConfig config, Db db) {
        this.config = config;
        this.db = db;
        this.sheets = new GoogleSheetsService(config, db);
    }

    @Override
    public String getBotToken() {
        return config.botToken;
    }

    @Override
    public String getBotUsername() {
        return config.botUsername;
    }

    @Override
    public void onUpdateReceived(Update update) {
        try {
            if (update.hasPreCheckoutQuery()) {
                handlePreCheckout(update.getPreCheckoutQuery());
                return;
            }
            if (update.hasMessage() && update.getMessage().hasSuccessfulPayment()) {
                handleSuccessfulPayment(update.getMessage());
                return;
            }
            if (update.hasCallbackQuery()) {
                handleCallback(update.getCallbackQuery());
            } else if (update.hasMessage()) {
                handleMessage(update.getMessage());
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private void handleMessage(Message message) throws Exception {
        if (!message.hasText() && !message.hasPhoto()) {
            return;
        }
        long tgId = message.getFrom().getId();
        boolean isAdmin = config.adminIds.contains(tgId);
        User user = db.upsertUser(tgId,
                message.getFrom().getUserName(),
                message.getFrom().getFirstName(),
                isAdmin);

        String text = message.hasText() ? message.getText().trim() : "";

        if ("/start".equalsIgnoreCase(text) || text.startsWith("/start ")) {
            handleStart(user, message);
            return;
        }

        if ("/admin".equalsIgnoreCase(text)) {
            if (user.isAdmin()) {
                sendAdminMenu(message.getChatId());
            } else {
                sendText(message.getChatId(), "Доступ запрещён.");
            }
            return;
        }

        if ("/add".equalsIgnoreCase(text)) {
            if (user.isAdmin()) {
                db.updateUserState(tgId, UserState.WAIT_ADMIN_ADD_PHOTO, Payload.put(null, "flow", "admin"));
                sendSkipPrompt(message.getChatId(), "Отправьте фото мужчины или нажмите «Пропустить».", "skip:admin_add_photo");
            } else {
                sendText(message.getChatId(), "Доступ запрещён.");
            }
            return;
        }

        if (user.state() == UserState.WAIT_PHONE_FOR_FIND) {
            String phone = normalizePhone(text);
            db.clearUserState(tgId);
            handleFindByPhone(user, message.getChatId(), phone, "find");
            return;
        }

        if (user.state() == UserState.WAIT_TG_FOR_FIND) {
            String[] tg = normalizeTelegram(text);
            db.clearUserState(tgId);
            handleFindByTelegram(user, message.getChatId(), tg[0], tg[1], "find");
            return;
        }

        if (user.state() == UserState.WAIT_NAME_FOR_FIND) {
            db.clearUserState(tgId);
            handleFindByName(user, message.getChatId(), text);
            return;
        }

        if (user.state() == UserState.WAIT_PHONE_FOR_REVIEW) {
            String phone = normalizePhone(text);
            db.clearUserState(tgId);
            handleFindByPhone(user, message.getChatId(), phone, "review");
            return;
        }

        if (user.state() == UserState.WAIT_TG_FOR_REVIEW) {
            String[] tg = normalizeTelegram(text);
            db.clearUserState(tgId);
            handleFindByTelegram(user, message.getChatId(), tg[0], tg[1], "review");
            return;
        }

        if (user.state() == UserState.WAIT_MAN_PHOTO) {
            handleManPhoto(user, message);
            return;
        }

        if (user.state() == UserState.WAIT_MAN_NAME) {
            handleManName(user, message);
            return;
        }

        if (user.state() == UserState.WAIT_MAN_DESC) {
            handleManDesc(user, message);
            return;
        }

        if (user.state() == UserState.WAIT_REVIEW_TEXT) {
            handleReviewText(user, message);
            return;
        }

        if (user.state() == UserState.WAIT_EDIT_REVIEW_TEXT) {
            handleEditReviewText(user, message);
            return;
        }

        if (user.state() == UserState.WAIT_ADMIN_ADD_PHOTO) {
            handleAdminAddPhoto(user, message);
            return;
        }

        if (user.state() == UserState.WAIT_ADMIN_ADD_NAME) {
            handleAdminAddName(user, message);
            return;
        }

        if (user.state() == UserState.WAIT_ADMIN_ADD_DESC) {
            handleAdminAddDesc(user, message);
            return;
        }

        if (user.state() == UserState.WAIT_ADMIN_ADD_CONTACT) {
            handleAdminAddContact(user, message);
            return;
        }

        if (user.state() == UserState.WAIT_ADMIN_SET_PHOTO) {
            handleAdminSetPhoto(user, message);
            return;
        }

        if (user.state() == UserState.WAIT_ADMIN_GRANT_PREMIUM_USER) {
            handleAdminGrantUser(user, message);
            return;
        }

        if (user.state() == UserState.WAIT_ADMIN_GRANT_PREMIUM_DAYS) {
            handleAdminGrantDays(user, message);
            return;
        }

        if (user.state() == UserState.WAIT_ADMIN_SET_PRICES) {
            handleAdminSetPrices(user, message);
            return;
        }
        if (user.state() == UserState.WAIT_ADMIN_ADD_ADMIN) {
            handleAdminAddAdmin(user, message);
            return;
        }

        sendMainMenu(message.getChatId(), user);
    }

    private void handleCallback(CallbackQuery cb) throws Exception {
        long tgId = cb.getFrom().getId();
        boolean isAdmin = config.adminIds.contains(tgId);
        User user = db.upsertUser(tgId, cb.getFrom().getUserName(), cb.getFrom().getFirstName(), isAdmin);
        String data = cb.getData();

        if (data.startsWith("menu:")) {
            handleMenuCallback(cb, user);
            return;
        }

        if ("noop".equals(data)) {
            answer(cb);
            return;
        }

        if (data.startsWith("find:")) {
            handleFindCallback(cb, user);
            return;
        }

        if (data.startsWith("review:")) {
            handleReviewCallback(cb, user);
            return;
        }

        if (data.startsWith("create:man")) {
            startCreateMan(user, cb.getMessage().getChatId());
            return;
        }

        if (data.startsWith("rate:")) {
            handleRateCallback(cb, user);
            return;
        }

        if (data.startsWith("editrate:")) {
            handleEditRateCallback(cb, user);
            return;
        }

        if (data.startsWith("reviews:")) {
            handleReviewsPagination(cb, user);
            return;
        }

        if (data.startsWith("skip:")) {
            handleSkipCallback(cb, user);
            return;
        }

        if (data.startsWith("menlist:")) {
            handleMenListPagination(cb, user);
            return;
        }

        if (data.startsWith("back:man:")) {
            int manId = Integer.parseInt(data.split(":")[2]);
            Man man = db.getManById(manId);
            if (man != null) {
                sendManCard(cb.getMessage().getChatId(), user, man, true);
            } else {
                sendText(cb.getMessage().getChatId(), "Карточка не найдена.");
            }
            answer(cb);
            return;
        }

        if (data.startsWith("search:")) {
            handleNameSearchPagination(cb, user);
            return;
        }

        if (data.startsWith("myreviews:")) {
            handleMyReviewsPagination(cb, user);
            return;
        }

        if (data.startsWith("pay:")) {
            handlePaymentCallback(cb, user);
            return;
        }

        if (data.startsWith("admin:")) {
            handleAdminCallback(cb, user);
            return;
        }
    }

    private void handleStart(User user, Message message) throws Exception {
        if (user.state() == UserState.WAIT_SECRET) {
            db.updateUserState(user.tgId(), UserState.NONE, null);
        }
        sendMainMenu(message.getChatId(), user, welcomeText());
    }

    private void handleMenuCallback(CallbackQuery cb, User user) throws Exception {
        String data = cb.getData();
        long chatId = cb.getMessage().getChatId();
        if (data.equals("menu:find")) {
            sendFindMethod(chatId, "find");
        } else if (data.equals("menu:main")) {
            sendMainMenu(chatId, user);
        } else if (data.equals("menu:review")) {
            sendFindMethod(chatId, "review");
        } else if (data.equals("menu:myreviews")) {
            sendMyReview(chatId, user, 0, null);
        } else if (data.equals("menu:admin")) {
            if (user.isAdmin()) {
                sendAdminMenu(chatId);
            } else {
                sendText(chatId, "Доступ запрещён.");
            }
        }
        answer(cb);
    }

    private void handleFindCallback(CallbackQuery cb, User user) throws Exception {
        long chatId = cb.getMessage().getChatId();
        switch (cb.getData()) {
            case "find:phone" -> {
                db.updateUserState(user.tgId(), UserState.WAIT_PHONE_FOR_FIND, null);
                sendText(chatId, "Введите номер телефона для поиска.");
            }
            case "find:tg" -> {
                db.updateUserState(user.tgId(), UserState.WAIT_TG_FOR_FIND, null);
                sendText(chatId, "Введите тег ТГ (например @user) или числовой Telegram ID — он не меняется.");
            }
            case "find:name" -> {
                db.updateUserState(user.tgId(), UserState.WAIT_NAME_FOR_FIND, null);
                sendText(chatId, "Введите имя для поиска.");
            }
        }
        answer(cb);
    }

    private void handleReviewCallback(CallbackQuery cb, User user) throws Exception {
        long chatId = cb.getMessage().getChatId();
        String data = cb.getData();
        if (data.equals("review:phone")) {
            db.updateUserState(user.tgId(), UserState.WAIT_PHONE_FOR_REVIEW, null);
            sendText(chatId, "Введите номер телефона для отзыва.");
        } else if (data.equals("review:tg")) {
            db.updateUserState(user.tgId(), UserState.WAIT_TG_FOR_REVIEW, null);
            sendText(chatId, "Введите тег ТГ (например @user) или числовой Telegram ID — он не меняется.");
        } else if (data.startsWith("review:start:")) {
            int manId = Integer.parseInt(data.split(":")[2]);
            sendRatingButtons(chatId, manId, false);
        }
        answer(cb);
    }

    private void handleFindByPhone(User user, long chatId, String phone, String flow) throws Exception {
        if (phone == null || phone.isBlank()) {
            sendText(chatId, "Не удалось распознать номер. Попробуйте ещё раз.");
            return;
        }
        Man man = db.findManByPhone(phone);
        if (man == null) {
            String payload = null;
            payload = Payload.put(payload, "flow", flow);
            payload = Payload.put(payload, "phone", phone);
            db.updateUserState(user.tgId(), UserState.NONE, payload);
            sendCreatePrompt(chatId);
        } else {
            if ("review".equals(flow)) {
                sendRatingButtons(chatId, man.id(), false);
            } else {
                sendManCard(chatId, user, man, true);
            }
        }
    }

    private void handleFindByTelegram(User user, long chatId, String username, String tgId, String flow) throws Exception {
        if (tgId == null && username != null) {
            tgId = resolveTelegramId(username);
        }
        Man man = null;
        if (tgId != null) man = db.findManByTgId(tgId);
        if (man == null && username != null) man = db.findManByTgUsername(username);
        if (man != null) {
            if (username != null && (man.tgUsername() == null || !man.tgUsername().equalsIgnoreCase(username))) {
                db.updateManTelegram(man.id(), username, null);
            }
            if (tgId != null && (man.tgId() == null || !man.tgId().equals(tgId))) {
                db.updateManTelegram(man.id(), null, tgId);
            }
        }
        if (man == null) {
            String payload = null;
            payload = Payload.put(payload, "flow", flow);
            if (username != null) payload = Payload.put(payload, "tg_username", username);
            if (tgId != null) payload = Payload.put(payload, "tg_id", tgId);
            db.updateUserState(user.tgId(), UserState.NONE, payload);
            sendCreatePrompt(chatId);
        } else {
            if ("review".equals(flow)) {
                sendRatingButtons(chatId, man.id(), false);
            } else {
                sendManCard(chatId, user, man, true);
            }
        }
    }

    private void handleFindByName(User user, long chatId, String name) throws Exception {
        if (name == null || name.isBlank()) {
            sendText(chatId, "Имя не распознано. Попробуйте ещё раз.");
            return;
        }
        db.updateUserState(user.tgId(), UserState.NONE, Payload.put(null, "search_name", name));
        sendNameSearchResult(chatId, user, name, 0, null);
    }

    private void handleManPhoto(User user, Message message) throws Exception {
        startCreateMan(user, message.getChatId());
    }

    private void handleManName(User user, Message message) throws Exception {
        startCreateMan(user, message.getChatId());
    }

    private void handleManDesc(User user, Message message) throws Exception {
        startCreateMan(user, message.getChatId());
    }

    private void handleReviewText(User user, Message message) throws Exception {
        String text = null;
        if (message.hasText()) {
            text = message.getText().trim();
            if (text.equalsIgnoreCase("пропустить")) text = null;
        }
        finalizeReviewText(user, message.getChatId(), text);
    }

    private void handleEditReviewText(User user, Message message) throws Exception {
        String text = null;
        if (message.hasText()) {
            text = message.getText().trim();
            if (text.equalsIgnoreCase("пропустить")) text = null;
        }
        finalizeEditReviewText(user, message.getChatId(), text);
    }

    private void finalizeReviewText(User user, long chatId, String text) throws Exception {
        Map<String, String> map = Payload.parse(user.statePayload());
        int manId = Integer.parseInt(map.get("man_id"));
        int rating = Integer.parseInt(map.get("rating"));
        Review existing = db.getReviewByAuthorAndMan(user.id(), manId);
        if (existing != null) {
            String newText = normalizeReviewText(text);
            String oldText = normalizeReviewText(existing.text());
            boolean same = existing.rating() == rating && Objects.equals(oldText, newText);
            if (same) {
                db.clearUserState(user.tgId());
                sendTextWithMenuButton(chatId, "У вас уже есть такой отзыв.");
            } else {
                db.updateReview(existing.id(), rating, text);
                db.clearUserState(user.tgId());
                sendTextWithMenuButton(chatId, "Отзыв обновлён.");
                Review updated = db.getReviewById(existing.id());
                Man man = db.getManById(manId);
                if (updated != null && man != null) {
                    sheets.syncReview(man, updated, user);
                }
            }
        } else {
            Review review = db.createReview(manId, user.id(), rating, text);
            db.clearUserState(user.tgId());
            sendTextWithMenuButton(chatId, "Отзыв опубликован.");
            if (review != null) {
                Man man = db.getManById(manId);
                if (man != null) {
                    sheets.syncReview(man, review, user);
                }
            }
        }
        Man man = db.getManById(manId);
        if (man != null) {
            sendManCard(chatId, user, man, true);
        }
    }

    private void finalizeEditReviewText(User user, long chatId, String text) throws Exception {
        Map<String, String> map = Payload.parse(user.statePayload());
        int reviewId = Integer.parseInt(map.get("review_id"));
        int rating = Integer.parseInt(map.get("rating"));
        if (text == null) {
            Review existing = db.getReviewById(reviewId);
            if (existing != null) text = existing.text();
        }
        Review existing = db.getReviewById(reviewId);
        String newText = normalizeReviewText(text);
        String oldText = existing == null ? null : normalizeReviewText(existing.text());
        boolean same = existing != null && existing.rating() == rating && Objects.equals(oldText, newText);
        if (same) {
            db.clearUserState(user.tgId());
            sendTextWithMenuButton(chatId, "Отзыв не изменился.");
            return;
        }
        db.updateReview(reviewId, rating, text);
        db.clearUserState(user.tgId());
        sendTextWithMenuButton(chatId, "Отзыв обновлён.");
        Review updated = db.getReviewById(reviewId);
        if (updated != null) {
            Man man = db.getManById(updated.manId());
            if (man != null) {
                sheets.syncReview(man, updated, user);
            }
        }
    }

    private void handleAdminAddPhoto(User user, Message message) throws Exception {
        String payload = user.statePayload();
        if (message.hasPhoto()) {
            String fileId = message.getPhoto().stream().max(Comparator.comparing(PhotoSize::getFileSize))
                    .map(PhotoSize::getFileId).orElse(null);
            payload = Payload.put(payload, "photo", fileId);
            db.updateUserState(user.tgId(), UserState.WAIT_ADMIN_ADD_NAME, payload);
            sendText(message.getChatId(), "Введите имя мужчины.");
        } else if (message.hasText() && message.getText().equalsIgnoreCase("пропустить")) {
            payload = Payload.put(payload, "photo", "");
            db.updateUserState(user.tgId(), UserState.WAIT_ADMIN_ADD_NAME, payload);
            sendText(message.getChatId(), "Введите имя мужчины.");
        } else {
            sendSkipPrompt(message.getChatId(), "Отправьте фото или нажмите «Пропустить».", "skip:admin_add_photo");
        }
    }

    private void handleAdminAddName(User user, Message message) throws Exception {
        if (!message.hasText() || message.getText().isBlank()) {
            sendText(message.getChatId(), "Имя не распознано. Попробуйте ещё раз.");
            return;
        }
        String payload = Payload.put(user.statePayload(), "name", message.getText().trim());
        db.updateUserState(user.tgId(), UserState.WAIT_ADMIN_ADD_DESC, payload);
        sendSkipPrompt(message.getChatId(), "Добавьте описание или нажмите «Пропустить».", "skip:admin_add_desc");
    }

    private void handleAdminAddDesc(User user, Message message) throws Exception {
        String desc = null;
        if (message.hasText()) {
            desc = message.getText().trim();
            if (desc.equalsIgnoreCase("пропустить")) desc = null;
        }
        String payload = Payload.put(user.statePayload(), "desc", desc == null ? "" : desc);
        db.updateUserState(user.tgId(), UserState.WAIT_ADMIN_ADD_CONTACT, payload);
        sendSkipPrompt(message.getChatId(), "Введите телефон или @username (или нажмите «Пропустить»).", "skip:admin_add_contact");
    }

    private void handleAdminSetPhoto(User user, Message message) throws Exception {
        String manIdStr = Payload.get(user.statePayload(), "man_id");
        if (manIdStr == null) {
            db.clearUserState(user.tgId());
            sendText(message.getChatId(), "Карточка не выбрана.");
            return;
        }
        if (message.hasPhoto()) {
            String fileId = message.getPhoto().stream().max(Comparator.comparing(PhotoSize::getFileSize))
                    .map(PhotoSize::getFileId).orElse(null);
            int manId = Integer.parseInt(manIdStr);
            db.updateManPhoto(manId, fileId);
            db.clearUserState(user.tgId());
            sendText(message.getChatId(), "Фото обновлено.");
        } else {
            sendAdminPhotoPrompt(message.getChatId());
        }
    }

    private void handleAdminAddContact(User user, Message message) throws Exception {
        String phone = null;
        String username = null;
        if (message.hasText()) {
            String text = message.getText().trim();
            if (!text.equalsIgnoreCase("пропустить")) {
                if (text.startsWith("@")) {
                    username = text.substring(1);
                } else if (text.matches("\\d{5,}")) {
                    phone = normalizePhone(text);
                }
            }
        }
        String payload = user.statePayload();
        payload = Payload.put(payload, "phone", phone == null ? "" : phone);
        payload = Payload.put(payload, "tg_username", username == null ? "" : username);
        db.updateUserState(user.tgId(), UserState.NONE, payload);
        createManFromPayload(user, message.getChatId(), payload);
    }

    private void handleAdminGrantUser(User user, Message message) throws Exception {
        if (!message.hasText()) return;
        String text = message.getText().trim();
        if (!text.matches("\\d+")) {
            sendText(message.getChatId(), "Введите числовой Telegram ID.");
            return;
        }
        String payload = Payload.put(null, "grant_tg", text);
        db.updateUserState(user.tgId(), UserState.WAIT_ADMIN_GRANT_PREMIUM_DAYS, payload);
        sendText(message.getChatId(), "Введите количество дней премиума.");
    }

    private void handleAdminGrantDays(User user, Message message) throws Exception {
        if (!message.hasText()) return;
        String text = message.getText().trim();
        if (!text.matches("\\d+")) {
            sendText(message.getChatId(), "Введите число дней.");
            return;
        }
        int days = Integer.parseInt(text);
        String tg = Payload.get(user.statePayload(), "grant_tg");
        User target = db.getUserByTgId(Long.parseLong(tg));
        if (target == null) {
            sendText(message.getChatId(), "Пользователь не найден.");
            db.clearUserState(user.tgId());
            return;
        }
        Instant now = Instant.now();
        Instant base = target.premiumUntil() != null && target.premiumUntil().isAfter(now)
                ? target.premiumUntil() : now;
        Instant until = base.plusSeconds(days * 86400L);
        db.setPremiumUntil(target.id(), until);
        db.clearUserState(user.tgId());
        sendText(message.getChatId(), "Премиум выдан до " + formatDate(until) + ".");
    }

    private void handleAdminSetPrices(User user, Message message) throws Exception {
        if (!message.hasText()) return;
        String text = message.getText().trim();
        String key = Payload.get(user.statePayload(), "price_key");
        if (key != null && !key.isBlank()) {
            if (!text.matches("\\d+")) {
                sendText(message.getChatId(), "Введите только число.");
                return;
            }
            db.setSetting(key, text);
            db.clearUserState(user.tgId());
            sendPriceMenu(message.getChatId());
            return;
        }
        // Резервный формат: week=200 month=500 ...
        String[] parts = text.split("\\s+");
        for (String part : parts) {
            if (!part.contains("=")) continue;
            String[] kv = part.split("=", 2);
            String k = kv[0];
            String value = kv[1];
            switch (k) {
                case "week" -> db.setSetting("price_week", value);
                case "month" -> db.setSetting("price_month", value);
                case "single" -> db.setSetting("price_single", value);
            }
        }
        db.clearUserState(user.tgId());
        sendPriceMenu(message.getChatId());
    }

    private void handleAdminAddAdmin(User user, Message message) throws Exception {
        if (!message.hasText()) return;
        String text = message.getText().trim();
        if (!text.matches("\\d+")) {
            sendText(message.getChatId(), "Введите числовой Telegram ID.");
            return;
        }
        long tgId = Long.parseLong(text);
        db.grantAdminByTgId(tgId);
        db.clearUserState(user.tgId());
        sendText(message.getChatId(), "Админ добавлен: " + tgId);
    }

    private void startCreateMan(User user, long chatId) throws Exception {
        if (user.statePayload() == null || user.statePayload().isBlank()) {
            sendText(chatId, "Нет данных для создания карточки.");
            return;
        }
        String payload = Payload.put(user.statePayload(), "flow", "review");
        createManFromPayload(user, chatId, payload);
    }

    private void createManFromPayload(User user, long chatId, String payloadRaw) throws Exception {
        Map<String, String> map = Payload.parse(payloadRaw);
        String flow = map.getOrDefault("flow", "find");
        boolean adminFlow = "admin".equals(flow);
        String phone = emptyToNull(map.get("phone"));
        String tgUsername = emptyToNull(map.get("tg_username"));
        String tgId = emptyToNull(map.get("tg_id"));
        String name = emptyToNull(map.get("name"));
        String desc = emptyToNull(map.get("desc"));
        String photo = emptyToNull(map.get("photo"));

        if (name == null || name.isBlank()) {
            name = "";
        }

        if (!adminFlow) {
            desc = null;
            photo = null;
        }

        Man man = db.createMan(phone, tgUsername, tgId, name, desc, photo, user.id());
        db.updateUserState(user.tgId(), UserState.NONE, null);

        if ("review".equals(flow)) {
            sendRatingButtons(chatId, man.id(), false);
        } else {
            sendManCard(chatId, user, man, true);
        }
    }

    private void handleRateCallback(CallbackQuery cb, User user) throws Exception {
        String[] parts = cb.getData().split(":");
        int manId = Integer.parseInt(parts[1]);
        int rating = Integer.parseInt(parts[2]);
        String payload = null;
        payload = Payload.put(payload, "man_id", String.valueOf(manId));
        payload = Payload.put(payload, "rating", String.valueOf(rating));
        db.updateUserState(user.tgId(), UserState.WAIT_REVIEW_TEXT, payload);
        sendSkipPrompt(cb.getMessage().getChatId(), "Напишите комментарий или нажмите «Пропустить».", "skip:review_text");
        answer(cb);
    }

    private void handleEditRateCallback(CallbackQuery cb, User user) throws Exception {
        String[] parts = cb.getData().split(":");
        int reviewId = Integer.parseInt(parts[1]);
        int rating = Integer.parseInt(parts[2]);
        String payload = null;
        payload = Payload.put(payload, "review_id", String.valueOf(reviewId));
        payload = Payload.put(payload, "rating", String.valueOf(rating));
        db.updateUserState(user.tgId(), UserState.WAIT_EDIT_REVIEW_TEXT, payload);
        sendSkipPrompt(cb.getMessage().getChatId(), "Отправьте новый текст или нажмите «Пропустить».", "skip:edit_review_text");
        answer(cb);
    }

    private void handleReviewsPagination(CallbackQuery cb, User user) throws Exception {
        String[] parts = cb.getData().split(":");
        int manId = Integer.parseInt(parts[1]);
        int offset = Integer.parseInt(parts[2]);
        sendReviewsPage(cb.getMessage().getChatId(), user, manId, offset, cb.getMessage().getMessageId());
        answer(cb);
    }

    private void handleMyReviewsPagination(CallbackQuery cb, User user) throws Exception {
        String[] parts = cb.getData().split(":");
        int offset = Integer.parseInt(parts[1]);
        String action = parts.length > 2 ? parts[2] : "";
        if (action.startsWith("edit")) {
            int reviewId = Integer.parseInt(action.split("-")[1]);
            sendRatingButtons(cb.getMessage().getChatId(), reviewId, true);
            answer(cb);
            return;
        }
        if (action.startsWith("del")) {
            int reviewId = Integer.parseInt(action.split("-")[1]);
            Review review = db.getReviewById(reviewId);
            if (review != null) {
                Man man = db.getManById(review.manId());
                if (man != null) {
                    sheets.markReviewDeleted(man, review, user);
                }
            }
            db.deleteReview(reviewId);
            sendText(cb.getMessage().getChatId(), "Отзыв удалён.");
            sendMyReview(cb.getMessage().getChatId(), user, offset, null);
            answer(cb);
            return;
        }
        sendMyReview(cb.getMessage().getChatId(), user, offset, cb.getMessage().getMessageId());
        answer(cb);
    }

    private void handleSkipCallback(CallbackQuery cb, User user) throws Exception {
        String action = cb.getData().split(":")[1];
        long chatId = cb.getMessage().getChatId();
        switch (action) {
            case "man_photo" -> {
                startCreateMan(user, chatId);
            }
            case "man_desc" -> {
                startCreateMan(user, chatId);
            }
            case "review_text" -> {
                if (user.state() == UserState.WAIT_REVIEW_TEXT) {
                    finalizeReviewText(user, chatId, null);
                }
            }
            case "edit_review_text" -> {
                if (user.state() == UserState.WAIT_EDIT_REVIEW_TEXT) {
                    finalizeEditReviewText(user, chatId, null);
                }
            }
            case "admin_add_photo" -> {
                if (user.state() == UserState.WAIT_ADMIN_ADD_PHOTO) {
                    String payload = Payload.put(user.statePayload(), "photo", "");
                    db.updateUserState(user.tgId(), UserState.WAIT_ADMIN_ADD_NAME, payload);
                    sendText(chatId, "Введите имя мужчины.");
                }
            }
            case "admin_add_desc" -> {
                if (user.state() == UserState.WAIT_ADMIN_ADD_DESC) {
                    String payload = Payload.put(user.statePayload(), "desc", "");
                    db.updateUserState(user.tgId(), UserState.WAIT_ADMIN_ADD_CONTACT, payload);
                    sendSkipPrompt(chatId, "Введите телефон или @username (или нажмите «Пропустить»).", "skip:admin_add_contact");
                }
            }
            case "admin_add_contact" -> {
                if (user.state() == UserState.WAIT_ADMIN_ADD_CONTACT) {
                    String payload = user.statePayload();
                    payload = Payload.put(payload, "phone", "");
                    payload = Payload.put(payload, "tg_username", "");
                    db.updateUserState(user.tgId(), UserState.NONE, payload);
                    createManFromPayload(user, chatId, payload);
                }
            }
        }
        answer(cb);
    }

    private void handleNameSearchPagination(CallbackQuery cb, User user) throws Exception {
        String[] parts = cb.getData().split(":");
        int offset = Integer.parseInt(parts[1]);
        String name = Payload.get(user.statePayload(), "search_name");
        if (name == null || name.isBlank()) {
            sendText(cb.getMessage().getChatId(), "Поиск не задан.");
            answer(cb);
            return;
        }
        sendNameSearchResult(cb.getMessage().getChatId(), user, name, offset, cb.getMessage().getMessageId());
        answer(cb);
    }

    private void handleMenListPagination(CallbackQuery cb, User user) throws Exception {
        String[] parts = cb.getData().split(":");
        String flow = parts[1];
        int offset = Integer.parseInt(parts[2]);
        sendMenList(cb.getMessage().getChatId(), user, flow, offset, cb.getMessage().getMessageId());
        answer(cb);
    }

    private void handlePaymentCallback(CallbackQuery cb, User user) throws Exception {
        long chatId = cb.getMessage().getChatId();
        sendText(chatId, "Оплата отключена. Доступ открыт всем.");
        answer(cb);
    }

    private void handleAdminCallback(CallbackQuery cb, User user) throws Exception {
        if (!user.isAdmin()) {
            sendText(cb.getMessage().getChatId(), "Доступ запрещён.");
            answer(cb);
            return;
        }
        String[] parts = cb.getData().split(":");
        String action = parts[1];
        long chatId = cb.getMessage().getChatId();
        switch (action) {
            case "users" -> {
                int offset = Integer.parseInt(parts[2]);
                sendAdminUsers(chatId, offset, cb.getMessage().getMessageId());
            }
            case "grant" -> {
                sendText(chatId, "Премиум отключён.");
            }
            case "stats" -> sendAdminStats(chatId);
            case "prices" -> {
                sendText(chatId, "Настройка цен отключена.");
            }
            case "addman" -> {
                db.updateUserState(user.tgId(), UserState.WAIT_ADMIN_ADD_PHOTO, Payload.put(null, "flow", "admin"));
                sendSkipPrompt(chatId, "Отправьте фото мужчины или нажмите «Пропустить».", "skip:admin_add_photo");
            }
            case "addadmin" -> {
                db.updateUserState(user.tgId(), UserState.WAIT_ADMIN_ADD_ADMIN, null);
                sendText(chatId, "Введите Telegram ID нового админа.");
            }
            case "closed" -> {
                int offset = Integer.parseInt(parts[2]);
                sendClosedMenList(chatId, offset, cb.getMessage().getMessageId());
            }
            case "close" -> {
                int manId = Integer.parseInt(parts[2]);
                db.closeMan(manId);
                sendText(chatId, "Карточка закрыта.");
            }
            case "restore" -> {
                int manId = Integer.parseInt(parts[2]);
                db.restoreMan(manId);
                sendText(chatId, "Карточка восстановлена.");
            }
            case "photo" -> {
                int offset = Integer.parseInt(parts[2]);
                sendAdminPhotoMenList(chatId, offset, cb.getMessage().getMessageId());
            }
            case "setphoto" -> {
                int manId = Integer.parseInt(parts[2]);
                String payload = Payload.put(null, "man_id", String.valueOf(manId));
                db.updateUserState(user.tgId(), UserState.WAIT_ADMIN_SET_PHOTO, payload);
                sendAdminPhotoPrompt(chatId);
            }
            case "photo_cancel" -> {
                db.clearUserState(user.tgId());
                sendAdminMenu(chatId);
            }
            case "price" -> {
                sendText(chatId, "Настройка цен отключена.");
            }
        }
        answer(cb);
    }

    private void sendMainMenu(long chatId, User user) throws Exception {
        sendMainMenu(chatId, user, "Главное меню:");
    }

    private void sendMainMenu(long chatId, User user, String text) throws Exception {
        String findLabel = "🔍 Найти мужчину";
        String reviewLabel = "📝 Оставить отзыв";

        List<List<InlineKeyboardButton>> rows = new ArrayList<>();
        rows.add(List.of(btn(findLabel, "menu:find")));
        rows.add(List.of(btn(reviewLabel, "menu:review")));
        rows.add(List.of(btn("📜 Мои отзывы", "menu:myreviews")));
        if (user.isAdmin()) {
            rows.add(List.of(btn("⚙️ Админ-панель", "menu:admin")));
        }
        SendMessage sm = new SendMessage(String.valueOf(chatId), text);
        sm.setReplyMarkup(new InlineKeyboardMarkup(rows));
        execute(sm);
    }
    private String welcomeText() {
        return "Добро пожаловать! Здесь ты можешь оставить отзыв о мужчине, " +
                "посмотреть отзывы других участниц и безопасно выбрать премиум-доступ. " +
                "Мы сохраняем отзывы и помогаем сделать информированный выбор.";
    }

    private void sendFindMethod(long chatId, String flow) throws Exception {
        List<List<InlineKeyboardButton>> rows = new ArrayList<>();
        if ("find".equals(flow)) {
            rows.add(List.of(btn("🔎 Поиск по номеру", "find:phone")));
            rows.add(List.of(btn("✍️ Ввести тег ТГ", "find:tg")));
            rows.add(List.of(btn("📋 Все мужчины", "menlist:find:0")));
        } else {
            rows.add(List.of(btn("🔎 Поиск по номеру", "review:phone")));
            rows.add(List.of(btn("✍️ Ввести тег ТГ", "review:tg")));
            rows.add(List.of(btn("📋 Все мужчины", "menlist:review:0")));
        }
        SendMessage sm = new SendMessage(String.valueOf(chatId), "Как будем искать?");
        sm.setReplyMarkup(new InlineKeyboardMarkup(rows));
        execute(sm);
    }

    private void sendCreatePrompt(long chatId) throws Exception {
        List<List<InlineKeyboardButton>> rows = new ArrayList<>();
        rows.add(List.of(btn("📝 Оставить первый отзыв", "create:man")));
        rows.add(List.of(btn("❌ Отмена", "menu:main")));
        SendMessage sm = new SendMessage(String.valueOf(chatId), "Мужчина не найден. Хотите оставить первый отзыв?");
        sm.setReplyMarkup(new InlineKeyboardMarkup(rows));
        execute(sm);
    }

    private void sendManCard(long chatId, User user, Man man, boolean includeReviews) throws Exception {
        if (man.isClosed() && !user.isAdmin()) {
            sendText(chatId, "Карточка недоступна.");
            return;
        }
        String textBody = buildManPreviewText(man, man.isClosed());

        List<List<InlineKeyboardButton>> rows = new ArrayList<>();
        rows.add(List.of(btn("📝 Оставить отзыв", "review:start:" + man.id())));
        rows.add(List.of(btn("📄 Посмотреть отзывы", "reviews:" + man.id() + ":0")));
        if (user.isAdmin()) {
            if (man.isClosed()) {
                rows.add(List.of(btn("♻️ Восстановить", "admin:restore:" + man.id())));
            } else {
                rows.add(List.of(btn("🔒 Закрыть", "admin:close:" + man.id())));
            }
        }

        if (man.photoFileId() != null && !man.photoFileId().isBlank()) {
            SendPhoto sp = new SendPhoto();
            sp.setChatId(String.valueOf(chatId));
            sp.setPhoto(new InputFile(man.photoFileId()));
            sp.setCaption(textBody);
            sp.setReplyMarkup(new InlineKeyboardMarkup(rows));
            execute(sp);
        } else {
            SendMessage sm = new SendMessage(String.valueOf(chatId), textBody);
            sm.setReplyMarkup(new InlineKeyboardMarkup(rows));
            execute(sm);
        }
    }

    private void sendPaywall(long chatId, Man man) throws Exception {
        int week = db.getSettingInt("price_week");
        int month = db.getSettingInt("price_month");
        int single = db.getSettingInt("price_single");
        String text = "Доступ к отзывам платный.\n" +
                "• Премиум на неделю — " + week + "р\n" +
                "• Премиум на месяц — " + month + "р\n" +
                "• Разовый доступ к одной карточке — " + single + "р (бессрочно)";
        List<List<InlineKeyboardButton>> rows = new ArrayList<>();
        rows.add(List.of(btn("💎 Премиум на неделю", "pay:week")));
        rows.add(List.of(btn("💎 Премиум на месяц", "pay:month")));
        rows.add(List.of(btn("🔓 Открыть разово", "pay:single:" + man.id())));
        rows.add(List.of(btn("📝 Оставить отзыв", "review:start:" + man.id())));
        SendMessage sm = new SendMessage(String.valueOf(chatId), text);
        sm.setReplyMarkup(new InlineKeyboardMarkup(rows));
        execute(sm);
    }

    private void sendReviewsPage(long chatId, User user, int manId, int offset, Integer messageId) throws Exception {
        Man man = db.getManById(manId);
        if (man == null) {
            sendText(chatId, "Карточка не найдена.");
            return;
        }
        List<Review> list = db.listReviewsForMan(manId, 1, offset);
        if (list.isEmpty()) {
            sendText(chatId, "Отзывов пока нет.");
            return;
        }
        Review r = list.get(0);
        String header = buildManHeaderText(man, user.isAdmin() && man.isClosed());
        String text = header + "\n\nОтзыв:\n" + formatReview(r);

        int total = db.reviewCount(manId);
        boolean hasPrev = offset > 0;
        boolean hasNext = offset + 1 < total;
        List<List<InlineKeyboardButton>> rows = new ArrayList<>();
        String prev = hasPrev ? ("reviews:" + manId + ":" + (offset - 1)) : null;
        String next = hasNext ? ("reviews:" + manId + ":" + (offset + 1)) : null;
        List<InlineKeyboardButton> nav = buildItemNavRow(offset, total, prev, next);
        if (!nav.isEmpty()) rows.add(nav);
        rows.add(List.of(btn("⬅️ Назад", "back:man:" + manId)));
        rows.add(List.of(btn("📝 Оставить отзыв", "review:start:" + manId)));
        InlineKeyboardMarkup markup = new InlineKeyboardMarkup(rows);

        if (messageId != null) {
            try {
                EditMessageText em = new EditMessageText();
                em.setChatId(String.valueOf(chatId));
                em.setMessageId(messageId);
                em.setText(text);
                em.setReplyMarkup(markup);
                execute(em);
            } catch (Exception ex) {
                org.telegram.telegrambots.meta.api.methods.updatingmessages.EditMessageCaption em =
                        new org.telegram.telegrambots.meta.api.methods.updatingmessages.EditMessageCaption();
                em.setChatId(String.valueOf(chatId));
                em.setMessageId(messageId);
                em.setCaption(text);
                em.setReplyMarkup(markup);
                execute(em);
            }
        } else {
            SendMessage sm = new SendMessage(String.valueOf(chatId), text);
            sm.setReplyMarkup(markup);
            execute(sm);
        }
    }

    private void sendMyReview(long chatId, User user, int offset, Integer messageId) throws Exception {
        List<Review> list = db.listReviewsByAuthor(user.id(), 1, offset);
        if (list.isEmpty()) {
            sendText(chatId, "У вас пока нет отзывов.");
            return;
        }
        Review r = list.get(0);
        Man man = db.getManById(r.manId());
        String manName = man != null ? man.name() : "(удалён)";
        String text = "Ваш отзыв о " + manName + "\n\n" + formatReview(r);

        int total = db.countReviewsByAuthor(user.id());
        boolean hasPrev = offset > 0;
        boolean hasNext = offset + 1 < total;
        List<List<InlineKeyboardButton>> rows = new ArrayList<>();
        String prev = hasPrev ? ("myreviews:" + (offset - 1)) : null;
        String next = hasNext ? ("myreviews:" + (offset + 1)) : null;
        List<InlineKeyboardButton> nav = buildItemNavRow(offset, total, prev, next);
        if (!nav.isEmpty()) rows.add(nav);
        rows.add(List.of(
                btn("✏️ Изменить", "myreviews:" + offset + ":edit-" + r.id()),
                btn("🗑 Удалить", "myreviews:" + offset + ":del-" + r.id())
        ));
        InlineKeyboardMarkup markup = new InlineKeyboardMarkup(rows);

        if (messageId != null) {
            EditMessageText em = new EditMessageText();
            em.setChatId(String.valueOf(chatId));
            em.setMessageId(messageId);
            em.setText(text);
            em.setReplyMarkup(markup);
            execute(em);
        } else {
            SendMessage sm = new SendMessage(String.valueOf(chatId), text);
            sm.setReplyMarkup(markup);
            execute(sm);
        }
    }

    private void sendPremium(long chatId, User user) throws Exception {
        int week = db.getSettingInt("price_week");
        int month = db.getSettingInt("price_month");
        int single = db.getSettingInt("price_single");
        String status = isPremium(user)
                ? "Активен до " + formatDate(user.premiumUntil())
                : "Не активен";
        String text = "Премиум статус: " + status + "\n" +
                "Неделя: " + week + "р\n" +
                "Месяц: " + month + "р\n" +
                "Разовый доступ: " + single + "р";
        List<List<InlineKeyboardButton>> rows = new ArrayList<>();
        rows.add(List.of(btn("💎 Премиум на неделю", "pay:week")));
        rows.add(List.of(btn("💎 Премиум на месяц", "pay:month")));
        SendMessage sm = new SendMessage(String.valueOf(chatId), text);
        sm.setReplyMarkup(new InlineKeyboardMarkup(rows));
        execute(sm);
    }

    private void sendAdminMenu(long chatId) throws Exception {
        List<List<InlineKeyboardButton>> rows = new ArrayList<>();
        rows.add(List.of(btn("👥 Участницы", "admin:users:0")));
        rows.add(List.of(btn("📊 Статистика", "admin:stats")));
        rows.add(List.of(btn("➕ Добавить мужчину", "admin:addman")));
        rows.add(List.of(btn("📷 Добавить фото", "admin:photo:0")));
        rows.add(List.of(btn("📁 Закрытые карточки", "admin:closed:0")));
        rows.add(List.of(btn("➕ Добавить админа", "admin:addadmin")));
        SendMessage sm = new SendMessage(String.valueOf(chatId), "Админ-панель:");
        sm.setReplyMarkup(new InlineKeyboardMarkup(rows));
        execute(sm);
    }

    private void sendPriceMenu(long chatId) throws Exception {
        int w = db.getSettingInt("price_week");
        int m = db.getSettingInt("price_month");
        int s = db.getSettingInt("price_single");
        String text = "Текущие значения:\n" +
                "• Неделя: " + w + "р\n" +
                "• Месяц: " + m + "р\n" +
                "• Разовый доступ: " + s + "р";
        List<List<InlineKeyboardButton>> rows = new ArrayList<>();
        rows.add(List.of(btn("Изменить неделю", "admin:price:price_week")));
        rows.add(List.of(btn("Изменить месяц", "admin:price:price_month")));
        rows.add(List.of(btn("Изменить разовый доступ", "admin:price:price_single")));
        rows.add(List.of(btn("⬅️ Назад", "menu:admin")));
        SendMessage sm = new SendMessage(String.valueOf(chatId), text);
        sm.setReplyMarkup(new InlineKeyboardMarkup(rows));
        execute(sm);
    }

    private void sendAdminUsers(long chatId, int offset, Integer messageId) throws Exception {
        int total = db.countUsers();
        List<User> users = db.listUsers(5, offset);
        StringBuilder sb = new StringBuilder();
        sb.append("Участницы (всего ").append(total).append("):\n\n");
        for (User u : users) {
            sb.append(u.firstName() != null ? u.firstName() : "").append(" ")
                    .append(u.username() != null ? "@" + u.username() : "")
                    .append(" | ID: ").append(u.tgId());
            if (u.premiumUntil() != null) sb.append(" | Premium до ").append(formatDate(u.premiumUntil()));
            sb.append("\n");
        }
        boolean hasPrev = offset > 0;
        boolean hasNext = offset + 5 < total;
        List<List<InlineKeyboardButton>> rows = new ArrayList<>();
        String prev = hasPrev ? ("admin:users:" + Math.max(0, offset - 5)) : null;
        String next = hasNext ? ("admin:users:" + (offset + 5)) : null;
        List<InlineKeyboardButton> nav = buildPageNavRow(offset, 5, total, prev, next);
        if (!nav.isEmpty()) rows.add(nav);
        rows.add(List.of(btn("⬅️ Назад", "menu:admin")));
        InlineKeyboardMarkup markup = new InlineKeyboardMarkup(rows);

        if (messageId != null) {
            EditMessageText em = new EditMessageText();
            em.setChatId(String.valueOf(chatId));
            em.setMessageId(messageId);
            em.setText(sb.toString());
            em.setReplyMarkup(markup);
            execute(em);
        } else {
            SendMessage sm = new SendMessage(String.valueOf(chatId), sb.toString());
            sm.setReplyMarkup(markup);
            execute(sm);
        }
    }

    private void sendAdminStats(long chatId) throws Exception {
        File file = File.createTempFile("stats_", ".xlsx");
        try (Workbook wb = new XSSFWorkbook()) {
            Sheet usersSheet = wb.createSheet("Участницы");
            Row h1 = usersSheet.createRow(0);
            h1.createCell(0).setCellValue("Telegram ID");
            h1.createCell(1).setCellValue("Имя");
            h1.createCell(2).setCellValue("Тег");
            h1.createCell(3).setCellValue("Статус подписки");

            List<User> users = db.listAllUsers();
            int rowIdx = 1;
            Instant now = Instant.now();
            for (User u : users) {
                Row r = usersSheet.createRow(rowIdx++);
                r.createCell(0).setCellValue(String.valueOf(u.tgId()));
                r.createCell(1).setCellValue(u.firstName() == null ? "" : u.firstName());
                r.createCell(2).setCellValue(u.username() == null ? "" : "@" + u.username());
                String status = (u.premiumUntil() != null && u.premiumUntil().isAfter(now))
                        ? ("Премиум до " + formatDate(u.premiumUntil()))
                        : "Без премиума";
                r.createCell(3).setCellValue(status);
            }

            Sheet menSheet = wb.createSheet("Мужчины");
            Row h2 = menSheet.createRow(0);
            h2.createCell(0).setCellValue("Имя");
            h2.createCell(1).setCellValue("Телефон");
            h2.createCell(2).setCellValue("Тег ТГ");
            h2.createCell(3).setCellValue("TG ID");
            h2.createCell(4).setCellValue("Отзывов");
            h2.createCell(5).setCellValue("Средняя оценка");

            List<Db.ManStats> men = db.listMenWithStats();
            rowIdx = 1;
            for (Db.ManStats m : men) {
                Row r = menSheet.createRow(rowIdx++);
                r.createCell(0).setCellValue(m.name());
                r.createCell(1).setCellValue(m.phone() == null ? "" : m.phone());
                r.createCell(2).setCellValue(m.tgUsername() == null ? "" : "@" + m.tgUsername());
                r.createCell(3).setCellValue(m.tgId() == null ? "" : m.tgId());
                r.createCell(4).setCellValue(m.reviewsCount());
                r.createCell(5).setCellValue(m.reviewsCount() == 0 ? 0 : m.avgRating());
            }

            try (FileOutputStream fos = new FileOutputStream(file)) {
                wb.write(fos);
            }
        }

        SendDocument sd = new SendDocument();
        sd.setChatId(String.valueOf(chatId));
        sd.setCaption("Статистика");
        sd.setDocument(new InputFile(file));
        execute(sd);
        // best-effort cleanup
        file.delete();
    }

    private void sendNameSearchResult(long chatId, User user, String name, int offset, Integer messageId) throws Exception {
        List<Man> list = db.searchMenByName(name, 1, offset);
        if (list.isEmpty()) {
            sendText(chatId, "Ничего не найдено.");
            return;
        }
        Man man = list.get(0);
        int total = db.countMenByName(name);
        boolean hasPrev = offset > 0;
        boolean hasNext = offset + 1 < total;

        String text = buildManPreviewText(man, false);
        List<List<InlineKeyboardButton>> rows = new ArrayList<>();
        String prev = hasPrev ? ("search:" + (offset - 1)) : null;
        String next = hasNext ? ("search:" + (offset + 1)) : null;
        List<InlineKeyboardButton> nav = buildItemNavRow(offset, total, prev, next);
        if (!nav.isEmpty()) rows.add(nav);
        rows.add(List.of(btn("Посмотреть отзывы", "reviews:" + man.id() + ":0")));
        InlineKeyboardMarkup markup = new InlineKeyboardMarkup(rows);

        if (messageId != null) {
            EditMessageText em = new EditMessageText();
            em.setChatId(String.valueOf(chatId));
            em.setMessageId(messageId);
            em.setText(text);
            em.setReplyMarkup(markup);
            execute(em);
        } else {
            SendMessage sm = new SendMessage(String.valueOf(chatId), text);
            sm.setReplyMarkup(markup);
            execute(sm);
        }
    }

    private void sendMenList(long chatId, User user, String flow, int offset, Integer messageId) throws Exception {
        List<Man> list = db.listMen(1, offset);
        if (list.isEmpty()) {
            sendText(chatId, "Список пуст.");
            return;
        }
        Man man = list.get(0);
        int total = db.countMen();
        boolean hasPrev = offset > 0;
        boolean hasNext = offset + 1 < total;

        String text = buildManPreviewText(man, false);

        List<List<InlineKeyboardButton>> rows = new ArrayList<>();
        String prev = hasPrev ? ("menlist:" + flow + ":" + (offset - 1)) : null;
        String next = hasNext ? ("menlist:" + flow + ":" + (offset + 1)) : null;
        List<InlineKeyboardButton> nav = buildItemNavRow(offset, total, prev, next);
        if (!nav.isEmpty()) rows.add(nav);
        rows.add(List.of(btn("Посмотреть отзывы", "reviews:" + man.id() + ":0")));
        rows.add(List.of(btn("📝 Оставить отзыв", "review:start:" + man.id())));
        rows.add(List.of(btn("⬅️ Назад", "menu:" + ("review".equals(flow) ? "review" : "find"))));
        InlineKeyboardMarkup markup = new InlineKeyboardMarkup(rows);

        if (messageId != null) {
            try {
                EditMessageText em = new EditMessageText();
                em.setChatId(String.valueOf(chatId));
                em.setMessageId(messageId);
                em.setText(text);
                em.setReplyMarkup(markup);
                execute(em);
            } catch (Exception ex) {
                SendMessage sm = new SendMessage(String.valueOf(chatId), text);
                sm.setReplyMarkup(markup);
                execute(sm);
            }
        } else {
            SendMessage sm = new SendMessage(String.valueOf(chatId), text);
            sm.setReplyMarkup(markup);
            execute(sm);
        }
    }

    private void sendClosedMenList(long chatId, int offset, Integer messageId) throws Exception {
        List<Man> list = db.listClosedMen(1, offset);
        if (list.isEmpty()) {
            sendText(chatId, "Закрытых карточек нет.");
            return;
        }
        Man man = list.get(0);
        int total = db.countClosedMen();
        boolean hasPrev = offset > 0;
        boolean hasNext = offset + 1 < total;

        String text = buildManPreviewText(man, true);

        List<List<InlineKeyboardButton>> rows = new ArrayList<>();
        String prev = hasPrev ? ("admin:closed:" + (offset - 1)) : null;
        String next = hasNext ? ("admin:closed:" + (offset + 1)) : null;
        List<InlineKeyboardButton> nav = buildItemNavRow(offset, total, prev, next);
        if (!nav.isEmpty()) rows.add(nav);
        rows.add(List.of(btn("Посмотреть отзывы", "reviews:" + man.id() + ":0")));
        rows.add(List.of(btn("♻️ Восстановить", "admin:restore:" + man.id())));
        rows.add(List.of(btn("⬅️ Назад", "menu:admin")));
        InlineKeyboardMarkup markup = new InlineKeyboardMarkup(rows);

        if (messageId != null) {
            try {
                EditMessageText em = new EditMessageText();
                em.setChatId(String.valueOf(chatId));
                em.setMessageId(messageId);
                em.setText(text);
                em.setReplyMarkup(markup);
                execute(em);
            } catch (Exception ex) {
                SendMessage sm = new SendMessage(String.valueOf(chatId), text);
                sm.setReplyMarkup(markup);
                execute(sm);
            }
        } else {
            SendMessage sm = new SendMessage(String.valueOf(chatId), text);
            sm.setReplyMarkup(markup);
            execute(sm);
        }
    }

    private void sendAdminPhotoPrompt(long chatId) throws Exception {
        List<List<InlineKeyboardButton>> rows = new ArrayList<>();
        rows.add(List.of(btn("❌ Отмена", "admin:photo_cancel")));
        SendMessage sm = new SendMessage(String.valueOf(chatId), "Отправьте фото мужчины.");
        sm.setReplyMarkup(new InlineKeyboardMarkup(rows));
        execute(sm);
    }

    private void sendAdminPhotoMenList(long chatId, int offset, Integer messageId) throws Exception {
        List<Man> list = db.listMen(1, offset);
        if (list.isEmpty()) {
            sendText(chatId, "Список пуст.");
            return;
        }
        Man man = list.get(0);
        int total = db.countMen();
        boolean hasPrev = offset > 0;
        boolean hasNext = offset + 1 < total;

        String text = buildManPreviewText(man, false);
        List<List<InlineKeyboardButton>> rows = new ArrayList<>();
        String prev = hasPrev ? ("admin:photo:" + (offset - 1)) : null;
        String next = hasNext ? ("admin:photo:" + (offset + 1)) : null;
        List<InlineKeyboardButton> nav = buildItemNavRow(offset, total, prev, next);
        if (!nav.isEmpty()) rows.add(nav);
        rows.add(List.of(btn("📷 Выбрать карточку", "admin:setphoto:" + man.id())));
        rows.add(List.of(btn("⬅️ Назад", "menu:admin")));
        InlineKeyboardMarkup markup = new InlineKeyboardMarkup(rows);

        if (messageId != null) {
            try {
                EditMessageText em = new EditMessageText();
                em.setChatId(String.valueOf(chatId));
                em.setMessageId(messageId);
                em.setText(text);
                em.setReplyMarkup(markup);
                execute(em);
            } catch (Exception ex) {
                SendMessage sm = new SendMessage(String.valueOf(chatId), text);
                sm.setReplyMarkup(markup);
                execute(sm);
            }
        } else {
            SendMessage sm = new SendMessage(String.valueOf(chatId), text);
            sm.setReplyMarkup(markup);
            execute(sm);
        }
    }

    private void sendInvoiceForPayment(long chatId, Payment payment, String title, String description) throws Exception {
        String payload = "pay:" + payment.id();
        int amountKop = payment.amount() * 100;

        LabeledPrice price = new LabeledPrice(title, amountKop);
        SendInvoice invoice = new SendInvoice();
        invoice.setChatId(String.valueOf(chatId));
        invoice.setTitle(title);
        invoice.setDescription(description);
        invoice.setPayload(payload);
        invoice.setProviderToken(config.providerToken);
        invoice.setCurrency("RUB");
        invoice.setPrices(List.of(price));
        invoice.setNeedEmail(true);
        invoice.setSendEmailToProvider(true);
        invoice.setProviderData(buildProviderData(title, payment.amount()));
        execute(invoice);
    }

    private void handlePreCheckout(PreCheckoutQuery query) throws Exception {
        boolean ok = false;
        String error = null;
        Integer paymentId = parsePaymentId(query.getInvoicePayload());
        if (paymentId != null) {
            Payment payment = db.getPayment(paymentId);
            if (payment != null && !"PAID".equals(payment.status())) {
                ok = true;
            } else {
                error = "Платёж не найден или уже обработан.";
            }
        } else {
            error = "Некорректный платёж.";
        }
        AnswerPreCheckoutQuery ans = new AnswerPreCheckoutQuery();
        ans.setPreCheckoutQueryId(query.getId());
        ans.setOk(ok);
        if (!ok && error != null) {
            ans.setErrorMessage(error);
        }
        execute(ans);
    }

    private void handleSuccessfulPayment(Message message) throws Exception {
        SuccessfulPayment sp = message.getSuccessfulPayment();
        if (sp == null) return;
        Integer paymentId = parsePaymentId(sp.getInvoicePayload());
        if (paymentId == null) return;
        Payment payment = db.getPayment(paymentId);
        if (payment == null) {
            sendText(message.getChatId(), "Платёж не найден.");
            return;
        }
        if ("PAID".equals(payment.status())) {
            return;
        }
        db.updatePaymentStatus(paymentId, "PAID");
        finalizePayment(payment);
    }

    private void finalizePayment(Payment payment) throws Exception {
        User user = db.getUserById(payment.userId());
        if (user == null) return;
        Instant now = Instant.now();
        if (payment.type().equals("WEEK") || payment.type().equals("MONTH")) {
            int days = payment.type().equals("WEEK") ? 7 : 30;
            Instant base = user.premiumUntil() != null && user.premiumUntil().isAfter(now)
                    ? user.premiumUntil() : now;
            Instant until = base.plusSeconds(days * 86400L);
            db.setPremiumUntil(user.id(), until);
            sendText(user.tgId(), "Оплата получена. Премиум до " + formatDate(until) + ".");
        } else if (payment.type().equals("SINGLE") && payment.manId() != null) {
            db.grantAccess(user.id(), payment.manId(), null);
            sendText(user.tgId(), "Оплата получена. Доступ открыт.");
        }
    }

    private boolean hasAccess(User user, Man man) throws Exception {
        if (user == null || man == null) return false;
        if (user.isAdmin()) return true;
        if (isPremium(user)) return true;
        return db.hasAccess(user.id(), man.id(), Instant.now());
    }

    private boolean isPremium(User user) {
        return user.premiumUntil() != null && user.premiumUntil().isAfter(Instant.now());
    }

    private void sendText(long chatId, String text) throws Exception {
        SendMessage sm = new SendMessage(String.valueOf(chatId), text);
        execute(sm);
    }

    private List<InlineKeyboardButton> buildNavRow(int current, int total, String prevData, String nextData) {
        List<InlineKeyboardButton> row = new ArrayList<>();
        if (total <= 0) return row;
        if (prevData != null) row.add(btn("⬅️", prevData));
        row.add(btn(current + "/" + total, "noop"));
        if (nextData != null) row.add(btn("➡️", nextData));
        return row;
    }

    private List<InlineKeyboardButton> buildItemNavRow(int offset, int total, String prevData, String nextData) {
        if (total <= 1 && prevData == null && nextData == null) return new ArrayList<>();
        int current = Math.min(total, offset + 1);
        return buildNavRow(current, Math.max(total, 1), prevData, nextData);
    }

    private List<InlineKeyboardButton> buildPageNavRow(int offset, int pageSize, int total, String prevData, String nextData) {
        int totalPages = Math.max(1, (int) Math.ceil(total / (double) pageSize));
        int current = Math.min(totalPages, (offset / pageSize) + 1);
        if (total <= pageSize && prevData == null && nextData == null) return new ArrayList<>();
        return buildNavRow(current, totalPages, prevData, nextData);
    }

    private void sendTextWithMenuButton(long chatId, String text) throws Exception {
        List<List<InlineKeyboardButton>> rows = new ArrayList<>();
        rows.add(List.of(btn("🏠 В меню", "menu:main")));
        SendMessage sm = new SendMessage(String.valueOf(chatId), text);
        sm.setReplyMarkup(new InlineKeyboardMarkup(rows));
        execute(sm);
    }

    private void sendSkipPrompt(long chatId, String text, String skipCallback) throws Exception {
        List<List<InlineKeyboardButton>> rows = new ArrayList<>();
        rows.add(List.of(btn("Пропустить", skipCallback)));
        SendMessage sm = new SendMessage(String.valueOf(chatId), text);
        sm.setReplyMarkup(new InlineKeyboardMarkup(rows));
        execute(sm);
    }

    private InlineKeyboardButton btn(String text, String data) {
        InlineKeyboardButton b = new InlineKeyboardButton();
        b.setText(text);
        b.setCallbackData(data);
        return b;
    }

    private void answer(CallbackQuery cb) throws Exception {
        AnswerCallbackQuery a = new AnswerCallbackQuery();
        a.setCallbackQueryId(cb.getId());
        execute(a);
    }

    private void sendRatingButtons(long chatId, int id, boolean isEdit) throws Exception {
        List<List<InlineKeyboardButton>> rows = new ArrayList<>();
        for (int i = 1; i <= 5; i++) {
            String data = (isEdit ? "editrate:" : "rate:") + id + ":" + i;
            rows.add(List.of(btn("⭐".repeat(i), data)));
        }
        SendMessage sm = new SendMessage(String.valueOf(chatId), "Выберите оценку (1–5):");
        sm.setReplyMarkup(new InlineKeyboardMarkup(rows));
        execute(sm);
    }

    private String formatReview(Review r) {
        String stars = "⭐".repeat(r.rating());
        String date = r.createdAt() == null ? "" : dateFmt.format(r.createdAt().atZone(ZoneId.systemDefault()));
        String text = r.text() == null ? "(без комментария)" : r.text();
        return stars + " | " + date + "\n" + text;
    }

    private String buildManHeaderText(Man man, boolean showClosedStatus) throws Exception {
        double avg = db.averageRating(man.id());
        int count = db.reviewCount(man.id());
        StringBuilder sb = new StringBuilder();
        if (showClosedStatus && man.isClosed()) sb.append("Статус: закрыта\n");
        if (man.phone() != null) sb.append("Телефон: ").append(man.phone()).append("\n");
        if (man.tgUsername() != null) sb.append("Telegram: @").append(man.tgUsername()).append("\n");
        if (man.tgId() != null) sb.append("Telegram ID: ").append(man.tgId()).append("\n");
        sb.append("Средний рейтинг: ")
                .append(String.format(Locale.forLanguageTag("ru"), "%.2f", avg))
                .append(" (отзывов: ").append(count).append(")");
        return sb.toString();
    }

    private String buildManPreviewText(Man man, boolean showClosedStatus) throws Exception {
        double avg = db.averageRating(man.id());
        int count = db.reviewCount(man.id());
        StringBuilder sb = new StringBuilder();
        if (showClosedStatus && man.isClosed()) sb.append("Статус: закрыта\n");
        if (man.phone() != null) sb.append("Телефон: ").append(man.phone()).append("\n");
        if (man.tgUsername() != null) sb.append("Telegram: @").append(man.tgUsername()).append("\n");
        if (man.tgId() != null) sb.append("Telegram ID: ").append(man.tgId()).append("\n");
        sb.append("Средний рейтинг: ")
                .append(String.format(Locale.forLanguageTag("ru"), "%.2f", avg))
                .append(" (отзывов: ").append(count).append(")\n");

        List<Review> reviews = db.listReviewsForMan(man.id(), 1, 0);
        if (reviews.isEmpty()) {
            sb.append("\nОтзывов пока нет.");
        } else {
            sb.append("\nПоследний отзыв:\n");
            sb.append(formatReview(reviews.get(0)));
        }
        return sb.toString();
    }

    private String formatDate(Instant instant) {
        if (instant == null) return "";
        return dateFmt.format(instant.atZone(ZoneId.systemDefault()));
    }

    private String buildProviderData(String title, int amountRub) {
        String value = String.format(Locale.US, "%.2f", (double) amountRub);
        String escapedTitle = jsonEscape(title);
        return "{"
                + "\"receipt\":{"
                + "\"tax_system_code\":" + config.taxSystemCode + ","
                + "\"items\":[{"
                + "\"description\":\"" + escapedTitle + "\","
                + "\"quantity\":\"1.00\","
                + "\"amount\":{\"value\":\"" + value + "\",\"currency\":\"RUB\"},"
                + "\"vat_code\":" + config.vatCode + ","
                + "\"payment_subject\":\"" + jsonEscape(config.paymentSubject) + "\","
                + "\"payment_mode\":\"" + jsonEscape(config.paymentMode) + "\""
                + "}]"
                + "}"
                + "}";
    }

    private String jsonEscape(String s) {
        if (s == null) return "";
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private Integer parsePaymentId(String payload) {
        if (payload == null || !payload.startsWith("pay:")) return null;
        String[] parts = payload.split(":");
        if (parts.length < 2) return null;
        try {
            return Integer.parseInt(parts[1]);
        } catch (NumberFormatException ex) {
            return null;
        }
    }

    private String normalizePhone(String input) {
        if (input == null) return null;
        String digits = input.replaceAll("\\D", "");
        return digits.length() < 5 ? null : digits;
    }

    private String[] normalizeTelegram(String input) {
        if (input == null) return new String[]{null, null};
        String text = input.trim();
        text = text.replace("https://", "").replace("http://", "");
        if (text.startsWith("t.me/")) {
            text = text.substring(5);
        } else if (text.startsWith("telegram.me/")) {
            text = text.substring(12);
        }
        if (text.startsWith("@")) {
            text = text.substring(1);
        }
        if (text.matches("\\d+")) {
            return new String[]{null, text};
        }
        return new String[]{text, null};
    }

    private String resolveTelegramId(String username) {
        if (username == null || username.isBlank()) return null;
        try {
            GetChat gc = new GetChat();
            gc.setChatId("@" + username);
            Chat chat = execute(gc);
            if (chat != null) return String.valueOf(chat.getId());
        } catch (Exception ignored) {
        }
        return null;
    }

    private String emptyToNull(String s) {
        return (s == null || s.isBlank()) ? null : s;
    }

    private String normalizeReviewText(String s) {
        if (s == null) return null;
        String t = s.trim();
        return t.isEmpty() ? null : t;
    }

    private List<Long> getAllAdminIds() throws Exception {
        List<Long> ids = new ArrayList<>(db.listAdminTgIds());
        for (long id : config.adminIds) {
            if (!ids.contains(id)) ids.add(id);
        }
        return ids;
    }

    private String safeName(String name) {
        return name == null ? "" : name;
    }

    private String safeUsername(String username) {
        return username == null || username.isBlank() ? "" : ("@" + username);
    }
}
