package com.reviewsbot;

import org.telegram.telegrambots.meta.TelegramBotsApi;
import org.telegram.telegrambots.updatesreceivers.DefaultBotSession;

public class Main {
    public static void main(String[] args) throws Exception {
        BotConfig config = new BotConfig();
        Db db = new Db(config);
        TelegramBotsApi api = new TelegramBotsApi(DefaultBotSession.class);
        api.registerBot(new BotService(config, db));
        System.out.println("Bot started");
    }
}
