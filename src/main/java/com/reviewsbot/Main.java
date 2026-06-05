package com.reviewsbot;

import org.telegram.telegrambots.meta.TelegramBotsApi;
import org.telegram.telegrambots.updatesreceivers.DefaultBotSession;

public class Main {
    public static void main(String[] args) throws Exception {
        BotConfig config = new BotConfig();
        Db db = new Db(config);
        GoogleSheetsService sheets = new GoogleSheetsService(config, db);
        TelegramAutofillService tgAutofill = new TelegramAutofillService(config, db, sheets);
        Runtime.getRuntime().addShutdownHook(new Thread(tgAutofill::close, "tg-autofill-shutdown"));
        TelegramBotsApi api = new TelegramBotsApi(DefaultBotSession.class);
        api.registerBot(new BotService(config, db, sheets));
        tgAutofill.start();
        System.out.println("Bot started");
    }
}
