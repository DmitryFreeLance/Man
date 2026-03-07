package com.reviewsbot;

import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.SheetsScopes;
import com.google.api.services.sheets.v4.model.AddSheetRequest;
import com.google.api.services.sheets.v4.model.AppendValuesResponse;
import com.google.api.services.sheets.v4.model.BatchUpdateSpreadsheetRequest;
import com.google.api.services.sheets.v4.model.BatchUpdateSpreadsheetResponse;
import com.google.api.services.sheets.v4.model.ClearValuesRequest;
import com.google.api.services.sheets.v4.model.Request;
import com.google.api.services.sheets.v4.model.SheetProperties;
import com.google.api.services.sheets.v4.model.Spreadsheet;
import com.google.api.services.sheets.v4.model.ValueRange;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;

import java.io.FileInputStream;
import java.io.InputStream;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GoogleSheetsService {
    private static final String UNIFIED_SHEET_TITLE = "Отзывы";
    private final Db db;
    private final Sheets sheets;
    private final String spreadsheetId;
    private final boolean enabled;
    private SheetRef cachedSheet;

    private final DateTimeFormatter dateFmt = DateTimeFormatter.ofPattern("dd.MM.yyyy")
            .withLocale(Locale.forLanguageTag("ru"))
            .withZone(ZoneId.systemDefault());

    private record SheetRef(int sheetId, String sheetName) {}

    public GoogleSheetsService(BotConfig config, Db db) {
        this.db = db;
        if (config.sheetsCredentialsPath == null || config.sheetsCredentialsPath.isBlank()
                || config.sheetsSpreadsheetId == null || config.sheetsSpreadsheetId.isBlank()) {
            this.sheets = null;
            this.spreadsheetId = null;
            this.enabled = false;
            return;
        }
        Sheets client = null;
        String sheetId = config.sheetsSpreadsheetId;
        boolean ok = false;
        try (InputStream in = new FileInputStream(config.sheetsCredentialsPath)) {
            GoogleCredentials creds = GoogleCredentials.fromStream(in)
                    .createScoped(List.of(SheetsScopes.SPREADSHEETS));
            client = new Sheets.Builder(new NetHttpTransport(), JacksonFactory.getDefaultInstance(),
                    new HttpCredentialsAdapter(creds))
                    .setApplicationName("ReviewsBot")
                    .build();
            ok = true;
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        this.sheets = client;
        this.spreadsheetId = sheetId;
        this.enabled = ok;
        if (ok) {
            try {
                migrateUnifiedSheetIfNeeded();
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }

    public void syncReview(Db.Man man, Db.Review review, Db.User author) {
        if (!enabled) return;
        try {
            SheetRef sheet = getUnifiedSheet();
            int rowIndex = ensureReviewRow(sheet, man, review, author);
            updateRow(sheet.sheetName(), rowIndex, man, review, author, "OK");
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public void markReviewDeleted(Db.Man man, Db.Review review, Db.User author) {
        if (!enabled) return;
        try {
            Db.ReviewSheet rs = db.getReviewSheet(review.id());
            if (rs == null) return;
            SheetRef sheet = getUnifiedSheet();
            updateRow(sheet.sheetName(), rs.rowIndex(), man, review, author, "Удалён");
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private void migrateUnifiedSheetIfNeeded() throws Exception {
        String flag = db.getSetting("sheets_unified_migrated");
        if ("1".equals(flag)) return;
        SheetRef sheet = getUnifiedSheet();
        sheets.spreadsheets().values()
                .clear(spreadsheetId, sheet.sheetName() + "!A:Z", new ClearValuesRequest())
                .execute();

        List<List<Object>> values = new ArrayList<>();
        values.add(headerRow());

        List<Db.ReviewExport> exports = db.listAllReviewsForSheets();
        int rowIndex = 2;
        for (Db.ReviewExport r : exports) {
            values.add(buildRow(
                    r.manPhone(),
                    r.manTgUsername(),
                    r.manTgId(),
                    r.manId(),
                    r.reviewId(),
                    r.createdAt(),
                    r.rating(),
                    r.text(),
                    r.authorTgId(),
                    r.authorName(),
                    r.authorUsername(),
                    "OK"
            ));
            db.upsertReviewSheet(r.reviewId(), r.manId(), sheet.sheetId(), rowIndex++);
        }

        ValueRange body = new ValueRange().setValues(values);
        sheets.spreadsheets().values()
                .update(spreadsheetId, sheet.sheetName() + "!A1:I" + values.size(), body)
                .setValueInputOption("RAW")
                .execute();

        db.setSetting("sheets_unified_migrated", "1");
    }

    private SheetRef getUnifiedSheet() throws Exception {
        if (cachedSheet != null) return cachedSheet;
        Spreadsheet ss = sheets.spreadsheets().get(spreadsheetId)
                .setFields("sheets.properties.sheetId,sheets.properties.title")
                .execute();
        if (ss.getSheets() != null) {
            for (var s : ss.getSheets()) {
                if (UNIFIED_SHEET_TITLE.equals(s.getProperties().getTitle())) {
                    cachedSheet = new SheetRef(s.getProperties().getSheetId(), UNIFIED_SHEET_TITLE);
                    return cachedSheet;
                }
            }
        }
        AddSheetRequest addSheet = new AddSheetRequest()
                .setProperties(new SheetProperties().setTitle(UNIFIED_SHEET_TITLE));
        BatchUpdateSpreadsheetRequest req = new BatchUpdateSpreadsheetRequest()
                .setRequests(List.of(new Request().setAddSheet(addSheet)));
        BatchUpdateSpreadsheetResponse resp = sheets.spreadsheets().batchUpdate(spreadsheetId, req).execute();
        int sheetId = resp.getReplies().get(0).getAddSheet().getProperties().getSheetId();
        cachedSheet = new SheetRef(sheetId, UNIFIED_SHEET_TITLE);
        writeHeader(UNIFIED_SHEET_TITLE);
        return cachedSheet;
    }

    private void writeHeader(String sheetName) throws Exception {
        ValueRange body = new ValueRange().setValues(List.of(headerRow()));
        sheets.spreadsheets().values()
                .update(spreadsheetId, sheetName + "!A1:I1", body)
                .setValueInputOption("RAW")
                .execute();
    }

    private List<Object> headerRow() {
        return List.of("Review ID", "Мужчина", "Дата", "Оценка", "Текст", "Автор ID", "Автор", "Тег", "Статус");
    }

    private int ensureReviewRow(SheetRef sheet, Db.Man man, Db.Review review, Db.User author) throws Exception {
        Db.ReviewSheet existing = db.getReviewSheet(review.id());
        if (existing != null) return existing.rowIndex();
        List<Object> row = buildRow(man, review, author, "OK");
        ValueRange body = new ValueRange().setValues(List.of(row));
        AppendValuesResponse resp = sheets.spreadsheets().values()
                .append(spreadsheetId, sheet.sheetName() + "!A:I", body)
                .setValueInputOption("RAW")
                .setInsertDataOption("INSERT_ROWS")
                .setIncludeValuesInResponse(true)
                .execute();
        String range = resp.getUpdates() != null ? resp.getUpdates().getUpdatedRange() : null;
        int rowIndex = extractRowIndex(range);
        if (rowIndex <= 0) {
            int updatedRows = resp.getUpdates() != null ? resp.getUpdates().getUpdatedRows() : 0;
            rowIndex = Math.max(2, updatedRows + 1);
        }
        db.upsertReviewSheet(review.id(), man.id(), sheet.sheetId(), rowIndex);
        return rowIndex;
    }

    private void updateRow(String sheetName, int rowIndex, Db.Man man, Db.Review review, Db.User author, String status) throws Exception {
        List<Object> row = buildRow(man, review, author, status);
        ValueRange body = new ValueRange().setValues(List.of(row));
        String range = sheetName + "!A" + rowIndex + ":I" + rowIndex;
        sheets.spreadsheets().values()
                .update(spreadsheetId, range, body)
                .setValueInputOption("RAW")
                .execute();
    }

    private List<Object> buildRow(Db.Man man, Db.Review review, Db.User author, String status) {
        return buildRow(
                man.phone(),
                man.tgUsername(),
                man.tgId(),
                man.id(),
                review.id(),
                review.createdAt(),
                review.rating(),
                review.text(),
                author != null ? author.tgId() : 0,
                author != null ? author.firstName() : "",
                author != null ? author.username() : "",
                status
        );
    }

    private List<Object> buildRow(String manPhone, String manTgUsername, String manTgId, int manId,
                                  int reviewId, java.time.Instant createdAt, int rating, String text,
                                  long authorTgId, String authorName, String authorUsername, String status) {
        String date = createdAt == null ? "" : dateFmt.format(createdAt);
        String authorTag = authorUsername == null || authorUsername.isBlank() ? "" : ("@" + authorUsername);
        String manRef = formatManRef(manPhone, manTgUsername, manTgId, manId);
        String safeText = text == null ? "" : text;
        String safeName = authorName == null ? "" : authorName;
        return List.of(
                reviewId,
                manRef,
                date,
                rating,
                safeText,
                authorTgId == 0 ? "" : String.valueOf(authorTgId),
                safeName,
                authorTag,
                status
        );
    }

    private String formatManRef(String phone, String tgUsername, String tgId, int manId) {
        if (phone != null && !phone.isBlank()) return phone;
        if (tgUsername != null && !tgUsername.isBlank()) return "@" + tgUsername;
        if (tgId != null && !tgId.isBlank()) return "TGID_" + tgId;
        return "MAN_" + manId;
    }

    private int extractRowIndex(String range) {
        if (range == null) return -1;
        Matcher m = Pattern.compile("!A(\\d+):").matcher(range);
        if (m.find()) {
            return Integer.parseInt(m.group(1));
        }
        return -1;
    }
}
