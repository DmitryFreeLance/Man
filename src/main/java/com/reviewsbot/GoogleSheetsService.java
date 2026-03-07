package com.reviewsbot;

import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.SheetsScopes;
import com.google.api.services.sheets.v4.model.AddSheetRequest;
import com.google.api.services.sheets.v4.model.AppendValuesResponse;
import com.google.api.services.sheets.v4.model.BatchUpdateSpreadsheetRequest;
import com.google.api.services.sheets.v4.model.BatchUpdateSpreadsheetResponse;
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
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GoogleSheetsService {
    private final Db db;
    private final Sheets sheets;
    private final String spreadsheetId;
    private final boolean enabled;
    private final DateTimeFormatter dateFmt = DateTimeFormatter.ofPattern("dd.MM.yyyy")
            .withLocale(Locale.forLanguageTag("ru"))
            .withZone(ZoneId.systemDefault());

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
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void syncReview(Db.Man man, Db.Review review, Db.User author) {
        if (!enabled) return;
        try {
            Db.ManSheet sheet = ensureSheet(man);
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
            Db.ManSheet sheet = db.getManSheet(review.manId());
            if (sheet == null) return;
            updateRow(sheet.sheetName(), rs.rowIndex(), man, review, author, "Удалён");
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private Db.ManSheet ensureSheet(Db.Man man) throws Exception {
        Db.ManSheet existing = db.getManSheet(man.id());
        if (existing != null) return existing;
        String baseTitle = buildSheetTitle(man);
        String title = ensureUniqueTitle(baseTitle);
        AddSheetRequest addSheet = new AddSheetRequest()
                .setProperties(new SheetProperties().setTitle(title));
        BatchUpdateSpreadsheetRequest req = new BatchUpdateSpreadsheetRequest()
                .setRequests(List.of(new Request().setAddSheet(addSheet)));
        BatchUpdateSpreadsheetResponse resp = sheets.spreadsheets().batchUpdate(spreadsheetId, req).execute();
        int sheetId = resp.getReplies().get(0).getAddSheet().getProperties().getSheetId();
        db.upsertManSheet(man.id(), sheetId, title);
        writeHeader(title);
        return new Db.ManSheet(man.id(), sheetId, title);
    }

    private String buildSheetTitle(Db.Man man) {
        if (man.phone() != null && !man.phone().isBlank()) {
            return sanitizeSheetTitle(man.phone());
        }
        if (man.tgUsername() != null && !man.tgUsername().isBlank()) {
            return sanitizeSheetTitle("@" + man.tgUsername());
        }
        if (man.tgId() != null && !man.tgId().isBlank()) {
            return sanitizeSheetTitle("TGID_" + man.tgId());
        }
        return sanitizeSheetTitle(man.name());
    }

    private void writeHeader(String sheetName) throws Exception {
        List<Object> header = List.of("Review ID", "Дата", "Оценка", "Текст", "Автор ID", "Автор", "Тег", "Статус");
        ValueRange body = new ValueRange().setValues(List.of(header));
        sheets.spreadsheets().values()
                .update(spreadsheetId, sheetName + "!A1:H1", body)
                .setValueInputOption("RAW")
                .execute();
    }

    private int ensureReviewRow(Db.ManSheet sheet, Db.Man man, Db.Review review, Db.User author) throws Exception {
        Db.ReviewSheet existing = db.getReviewSheet(review.id());
        if (existing != null) return existing.rowIndex();
        List<Object> row = buildRow(man, review, author, "OK");
        ValueRange body = new ValueRange().setValues(List.of(row));
        AppendValuesResponse resp = sheets.spreadsheets().values()
                .append(spreadsheetId, sheet.sheetName() + "!A:H", body)
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
        String range = sheetName + "!A" + rowIndex + ":H" + rowIndex;
        sheets.spreadsheets().values()
                .update(spreadsheetId, range, body)
                .setValueInputOption("RAW")
                .execute();
    }

    private List<Object> buildRow(Db.Man man, Db.Review review, Db.User author, String status) {
        String date = review.createdAt() == null ? "" : dateFmt.format(review.createdAt());
        String authorName = author != null && author.firstName() != null ? author.firstName() : "";
        String authorTag = author != null && author.username() != null ? ("@" + author.username()) : "";
        String text = review.text() == null ? "" : review.text();
        return List.of(
                review.id(),
                date,
                review.rating(),
                text,
                author != null ? String.valueOf(author.tgId()) : "",
                authorName,
                authorTag,
                status
        );
    }

    private int extractRowIndex(String range) {
        if (range == null) return -1;
        Matcher m = Pattern.compile("!A(\\d+):").matcher(range);
        if (m.find()) {
            return Integer.parseInt(m.group(1));
        }
        return -1;
    }

    private String sanitizeSheetTitle(String title) {
        String t = title.replaceAll("[\\\\/?*\\[\\]]", " ").trim();
        if (t.length() > 100) t = t.substring(0, 100);
        if (t.isBlank()) t = "man_sheet";
        return t;
    }

    private String ensureUniqueTitle(String base) throws Exception {
        Set<String> titles = new HashSet<>();
        Spreadsheet ss = sheets.spreadsheets().get(spreadsheetId)
                .setFields("sheets.properties.title")
                .execute();
        if (ss.getSheets() != null) {
            ss.getSheets().forEach(s -> titles.add(s.getProperties().getTitle()));
        }
        if (!titles.contains(base)) return base;
        int i = 2;
        String candidate = base + "_" + i;
        while (titles.contains(candidate)) {
            i++;
            candidate = base + "_" + i;
        }
        return candidate;
    }
}
