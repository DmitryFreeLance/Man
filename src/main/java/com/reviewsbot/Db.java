package com.reviewsbot;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class Db {
    private final BotConfig config;
    private final Connection conn;

    public record User(int id, long tgId, String username, String firstName, boolean isAdmin,
                       Instant premiumUntil, UserState state, String statePayload) {}

    public record Man(int id, String phone, String tgUsername, String tgId, String name,
                      String description, String photoFileId, boolean isClosed, Integer createdBy, Instant createdAt) {}

    public record Review(int id, int manId, int authorId, int rating, String text,
                         String status, Instant createdAt, Instant updatedAt) {}

    public record Payment(int id, int userId, Integer manId, String type, int amount,
                          String status, Instant createdAt, Instant updatedAt) {}

    public record Stats(int users, int men, int reviews) {}
    public record ManStats(int id, String name, String phone, String tgUsername, String tgId, int reviewsCount, double avgRating) {}

    public Db(BotConfig config) throws Exception {
        this.config = config;
        String dbUrl = "jdbc:sqlite:" + config.dbPath;
        this.conn = DriverManager.getConnection(dbUrl);
        try (Statement st = conn.createStatement()) {
            st.execute("PRAGMA journal_mode=WAL");
            st.execute("PRAGMA foreign_keys=ON");
        }
        initSchema();
        ensureReviewStatusColumn();
        ensureMenClosedColumn();
        ensureDefaultSettings();
    }

    private void initSchema() throws Exception {
        StringBuilder sb = new StringBuilder();
        try (BufferedReader br = new BufferedReader(new InputStreamReader(
                Objects.requireNonNull(Db.class.getResourceAsStream("/schema.sql")),
                StandardCharsets.UTF_8))) {
            String line;
            while ((line = br.readLine()) != null) {
                sb.append(line).append('\n');
            }
        }
        String[] statements = sb.toString().split(";");
        try (Statement st = conn.createStatement()) {
            for (String stmt : statements) {
                String sql = stmt.trim();
                if (sql.isEmpty()) continue;
                st.execute(sql);
            }
        }
    }

    private void ensureDefaultSettings() throws SQLException {
        ensureSetting("price_week", String.valueOf(config.defaultPriceWeek));
        ensureSetting("price_month", String.valueOf(config.defaultPriceMonth));
        ensureSetting("price_single", String.valueOf(config.defaultPriceSingle));
    }

    private void ensureReviewStatusColumn() throws SQLException {
        boolean hasStatus = false;
        try (PreparedStatement ps = conn.prepareStatement("PRAGMA table_info(reviews)")) {
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    if ("status".equalsIgnoreCase(rs.getString("name"))) {
                        hasStatus = true;
                        break;
                    }
                }
            }
        }
        if (!hasStatus) {
            try (Statement st = conn.createStatement()) {
                st.execute("ALTER TABLE reviews ADD COLUMN status TEXT NOT NULL DEFAULT 'PENDING'");
            }
        }
    }

    private void ensureMenClosedColumn() throws SQLException {
        boolean hasColumn = false;
        try (PreparedStatement ps = conn.prepareStatement("PRAGMA table_info(men)")) {
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    if ("is_closed".equalsIgnoreCase(rs.getString("name"))) {
                        hasColumn = true;
                        break;
                    }
                }
            }
        }
        if (!hasColumn) {
            try (Statement st = conn.createStatement()) {
                st.execute("ALTER TABLE men ADD COLUMN is_closed INTEGER NOT NULL DEFAULT 0");
            }
        }
    }

    private void ensureSetting(String key, String value) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT OR IGNORE INTO settings(key, value) VALUES(?, ?)");
             PreparedStatement ps2 = conn.prepareStatement(
                     "SELECT value FROM settings WHERE key=?")) {
            ps.setString(1, key);
            ps.setString(2, value);
            ps.executeUpdate();
            ps2.setString(1, key);
            try (ResultSet rs = ps2.executeQuery()) {
                if (!rs.next()) {
                    return;
                }
                String current = rs.getString(1);
                if (current == null || current.isBlank()) {
                    try (PreparedStatement ps3 = conn.prepareStatement(
                            "UPDATE settings SET value=? WHERE key=?")) {
                        ps3.setString(1, value);
                        ps3.setString(2, key);
                        ps3.executeUpdate();
                    }
                }
            }
        }
    }

    public synchronized User upsertUser(long tgId, String username, String firstName, boolean isAdmin) throws SQLException {
        User existing = getUserByTgId(tgId);
        if (existing == null) {
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO users(tg_id, tg_username, first_name, is_admin, registered_at, state) VALUES(?,?,?,?,?,?)")) {
                ps.setLong(1, tgId);
                ps.setString(2, username);
                ps.setString(3, firstName);
                ps.setInt(4, isAdmin ? 1 : 0);
                ps.setString(5, TimeUtil.nowIso());
                ps.setString(6, isAdmin ? UserState.NONE.name() : UserState.WAIT_SECRET.name());
                ps.executeUpdate();
            }
        } else {
            try (PreparedStatement ps = conn.prepareStatement(
                    "UPDATE users SET tg_username=?, first_name=?, is_admin=? WHERE tg_id=?")) {
                ps.setString(1, username);
                ps.setString(2, firstName);
                ps.setInt(3, (existing.isAdmin || isAdmin) ? 1 : 0);
                ps.setLong(4, tgId);
                ps.executeUpdate();
            }
        }
        return getUserByTgId(tgId);
    }

    public synchronized User getUserByTgId(long tgId) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT id,tg_id,tg_username,first_name,is_admin,premium_until,state,state_payload FROM users WHERE tg_id=?")) {
            ps.setLong(1, tgId);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return null;
                }
                return mapUser(rs);
            }
        }
    }

    public synchronized User getUserById(int id) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT id,tg_id,tg_username,first_name,is_admin,premium_until,state,state_payload FROM users WHERE id=?")) {
            ps.setInt(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return null;
                }
                return mapUser(rs);
            }
        }
    }

    public synchronized void updateUserState(long tgId, UserState state, String payload) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "UPDATE users SET state=?, state_payload=? WHERE tg_id=?")) {
            ps.setString(1, state == null ? null : state.name());
            ps.setString(2, payload);
            ps.setLong(3, tgId);
            ps.executeUpdate();
        }
    }

    public synchronized void setPremiumUntil(int userId, Instant premiumUntil) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "UPDATE users SET premium_until=? WHERE id=?")) {
            ps.setString(1, TimeUtil.fromInstant(premiumUntil));
            ps.setInt(2, userId);
            ps.executeUpdate();
        }
    }

    public synchronized void clearUserState(long tgId) throws SQLException {
        updateUserState(tgId, UserState.NONE, null);
    }

    public synchronized int getSettingInt(String key) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT value FROM settings WHERE key=?")) {
            ps.setString(1, key);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return Integer.parseInt(rs.getString(1));
                }
            }
        }
        return 0;
    }

    public synchronized void setSetting(String key, String value) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO settings(key, value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value")) {
            ps.setString(1, key);
            ps.setString(2, value);
            ps.executeUpdate();
        }
    }

    public synchronized Man findManByPhone(String phone) throws SQLException {
        if (phone == null) return null;
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT * FROM men WHERE phone=? AND is_closed=0")) {
            ps.setString(1, phone);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return mapMan(rs);
            }
        }
        return null;
    }

    public synchronized Man findManByTgUsername(String username) throws SQLException {
        if (username == null) return null;
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT * FROM men WHERE tg_username=? AND is_closed=0")) {
            ps.setString(1, username);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return mapMan(rs);
            }
        }
        return null;
    }

    public synchronized Man findManByTgId(String tgId) throws SQLException {
        if (tgId == null) return null;
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT * FROM men WHERE tg_id=? AND is_closed=0")) {
            ps.setString(1, tgId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return mapMan(rs);
            }
        }
        return null;
    }

    public synchronized Man getManById(int id) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT * FROM men WHERE id=?")) {
            ps.setInt(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return mapMan(rs);
            }
        }
        return null;
    }

    public synchronized Man createMan(String phone, String tgUsername, String tgId,
                                      String name, String description, String photoFileId, Integer createdBy) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO men(phone,tg_username,tg_id,name,description,photo_file_id,is_closed,created_by,created_at) VALUES(?,?,?,?,?,?,?,?,?)")) {
            ps.setString(1, phone);
            ps.setString(2, tgUsername);
            ps.setString(3, tgId);
            ps.setString(4, name);
            ps.setString(5, description);
            ps.setString(6, photoFileId);
            ps.setInt(7, 0);
            if (createdBy == null) {
                ps.setNull(8, Types.INTEGER);
            } else {
                ps.setInt(8, createdBy);
            }
            ps.setString(9, TimeUtil.nowIso());
            ps.executeUpdate();
        } catch (SQLException ex) {
            Man m = null;
            if (phone != null) m = findManByPhone(phone);
            if (m == null && tgUsername != null) m = findManByTgUsername(tgUsername);
            if (m == null && tgId != null) m = findManByTgId(tgId);
            if (m != null) return m;
            throw ex;
        }
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT * FROM men ORDER BY id DESC LIMIT 1")) {
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return mapMan(rs);
            }
        }
        return null;
    }

    public synchronized List<Man> searchMenByName(String name, int limit, int offset) throws SQLException {
        List<Man> list = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT * FROM men WHERE is_closed=0 AND lower(name) LIKE ? ORDER BY id DESC LIMIT ? OFFSET ?")) {
            ps.setString(1, "%" + name.toLowerCase() + "%");
            ps.setInt(2, limit);
            ps.setInt(3, offset);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) list.add(mapMan(rs));
            }
        }
        return list;
    }

    public synchronized List<Man> listMen(int limit, int offset) throws SQLException {
        List<Man> list = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT * FROM men WHERE is_closed=0 ORDER BY id DESC LIMIT ? OFFSET ?")) {
            ps.setInt(1, limit);
            ps.setInt(2, offset);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) list.add(mapMan(rs));
            }
        }
        return list;
    }

    public synchronized int countMen() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("SELECT COUNT(*) FROM men WHERE is_closed=0")) {
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return rs.getInt(1);
            }
        }
        return 0;
    }

    public synchronized List<Man> listClosedMen(int limit, int offset) throws SQLException {
        List<Man> list = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT * FROM men WHERE is_closed=1 ORDER BY id DESC LIMIT ? OFFSET ?")) {
            ps.setInt(1, limit);
            ps.setInt(2, offset);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) list.add(mapMan(rs));
            }
        }
        return list;
    }

    public synchronized int countClosedMen() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("SELECT COUNT(*) FROM men WHERE is_closed=1")) {
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return rs.getInt(1);
            }
        }
        return 0;
    }

    public synchronized void closeMan(int manId) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "UPDATE men SET is_closed=1 WHERE id=?")) {
            ps.setInt(1, manId);
            ps.executeUpdate();
        }
    }

    public synchronized void restoreMan(int manId) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "UPDATE men SET is_closed=0 WHERE id=?")) {
            ps.setInt(1, manId);
            ps.executeUpdate();
        }
    }

    public synchronized int countMenByName(String name) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT count(*) FROM men WHERE is_closed=0 AND lower(name) LIKE ?")) {
            ps.setString(1, "%" + name.toLowerCase() + "%");
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return rs.getInt(1);
            }
        }
        return 0;
    }

    public synchronized double averageRating(int manId) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT AVG(rating) FROM reviews WHERE man_id=? AND status='APPROVED'")) {
            ps.setInt(1, manId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return rs.getDouble(1);
            }
        }
        return 0.0;
    }

    public synchronized int reviewCount(int manId) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT COUNT(*) FROM reviews WHERE man_id=? AND status='APPROVED'")) {
            ps.setInt(1, manId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return rs.getInt(1);
            }
        }
        return 0;
    }

    public synchronized List<Review> listReviewsForMan(int manId, int limit, int offset) throws SQLException {
        List<Review> list = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT * FROM reviews WHERE man_id=? AND status='APPROVED' ORDER BY id DESC LIMIT ? OFFSET ?")) {
            ps.setInt(1, manId);
            ps.setInt(2, limit);
            ps.setInt(3, offset);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) list.add(mapReview(rs));
            }
        }
        return list;
    }

    public synchronized List<Review> listReviewsByAuthor(int authorId, int limit, int offset) throws SQLException {
        List<Review> list = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT * FROM reviews WHERE author_id=? ORDER BY id DESC LIMIT ? OFFSET ?")) {
            ps.setInt(1, authorId);
            ps.setInt(2, limit);
            ps.setInt(3, offset);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) list.add(mapReview(rs));
            }
        }
        return list;
    }

    public synchronized int countReviewsByAuthor(int authorId) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT COUNT(*) FROM reviews WHERE author_id=?")) {
            ps.setInt(1, authorId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return rs.getInt(1);
            }
        }
        return 0;
    }

    public synchronized Review getReviewById(int reviewId) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT * FROM reviews WHERE id=?")) {
            ps.setInt(1, reviewId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return mapReview(rs);
            }
        }
        return null;
    }

    public synchronized Review createReview(int manId, int authorId, int rating, String text) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO reviews(man_id, author_id, rating, text, status, created_at) VALUES(?,?,?,?,?,?)")) {
            ps.setInt(1, manId);
            ps.setInt(2, authorId);
            ps.setInt(3, rating);
            ps.setString(4, text);
            ps.setString(5, "PENDING");
            ps.setString(6, TimeUtil.nowIso());
            ps.executeUpdate();
        }
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT * FROM reviews ORDER BY id DESC LIMIT 1")) {
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return mapReview(rs);
            }
        }
        return null;
    }

    public synchronized void updateReview(int reviewId, int rating, String text) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "UPDATE reviews SET rating=?, text=?, updated_at=? WHERE id=?")) {
            ps.setInt(1, rating);
            ps.setString(2, text);
            ps.setString(3, TimeUtil.nowIso());
            ps.setInt(4, reviewId);
            ps.executeUpdate();
        }
    }

    public synchronized void updateReviewStatus(int reviewId, String status) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "UPDATE reviews SET status=?, updated_at=? WHERE id=?")) {
            ps.setString(1, status);
            ps.setString(2, TimeUtil.nowIso());
            ps.setInt(3, reviewId);
            ps.executeUpdate();
        }
    }

    public synchronized void deleteReview(int reviewId) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "DELETE FROM reviews WHERE id=?")) {
            ps.setInt(1, reviewId);
            ps.executeUpdate();
        }
    }

    public synchronized boolean hasAccess(int userId, int manId, Instant now) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT expires_at FROM access WHERE user_id=? AND man_id=?")) {
            ps.setInt(1, userId);
            ps.setInt(2, manId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    String expires = rs.getString(1);
                    if (expires == null || expires.isBlank()) return true;
                    Instant exp = TimeUtil.parseIso(expires);
                    return exp == null || exp.isAfter(now);
                }
            }
        }
        return false;
    }

    public synchronized void grantAccess(int userId, int manId, Instant expiresAt) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO access(user_id, man_id, granted_at, expires_at) VALUES(?,?,?,?) " +
                        "ON CONFLICT(user_id, man_id) DO UPDATE SET expires_at=excluded.expires_at")) {
            ps.setInt(1, userId);
            ps.setInt(2, manId);
            ps.setString(3, TimeUtil.nowIso());
            ps.setString(4, TimeUtil.fromInstant(expiresAt));
            ps.executeUpdate();
        }
    }

    public synchronized Payment createPayment(int userId, Integer manId, String type, int amount) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO payments(user_id, man_id, type, amount, status, created_at) VALUES(?,?,?,?,?,?)")) {
            ps.setInt(1, userId);
            if (manId == null) {
                ps.setNull(2, Types.INTEGER);
            } else {
                ps.setInt(2, manId);
            }
            ps.setString(3, type);
            ps.setInt(4, amount);
            ps.setString(5, "PENDING");
            ps.setString(6, TimeUtil.nowIso());
            ps.executeUpdate();
        }
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT * FROM payments ORDER BY id DESC LIMIT 1")) {
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return mapPayment(rs);
            }
        }
        return null;
    }

    public synchronized Payment getPayment(int paymentId) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT * FROM payments WHERE id=?")) {
            ps.setInt(1, paymentId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return mapPayment(rs);
            }
        }
        return null;
    }

    public synchronized void updatePaymentStatus(int paymentId, String status) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "UPDATE payments SET status=?, updated_at=? WHERE id=?")) {
            ps.setString(1, status);
            ps.setString(2, TimeUtil.nowIso());
            ps.setInt(3, paymentId);
            ps.executeUpdate();
        }
    }

    public synchronized int countUsers() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("SELECT COUNT(*) FROM users")) {
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return rs.getInt(1);
            }
        }
        return 0;
    }

    public synchronized List<User> listUsers(int limit, int offset) throws SQLException {
        List<User> list = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT id,tg_id,tg_username,first_name,is_admin,premium_until,state,state_payload FROM users ORDER BY id DESC LIMIT ? OFFSET ?")) {
            ps.setInt(1, limit);
            ps.setInt(2, offset);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) list.add(mapUser(rs));
            }
        }
        return list;
    }

    public synchronized List<User> listAllUsers() throws SQLException {
        List<User> list = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT id,tg_id,tg_username,first_name,is_admin,premium_until,state,state_payload FROM users ORDER BY id DESC")) {
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) list.add(mapUser(rs));
            }
        }
        return list;
    }

    public synchronized List<ManStats> listMenWithStats() throws SQLException {
        List<ManStats> list = new ArrayList<>();
        String sql = "SELECT m.id, m.name, m.phone, m.tg_username, m.tg_id, " +
                "COUNT(r.id) AS reviews_count, AVG(r.rating) AS avg_rating " +
                "FROM men m " +
                "LEFT JOIN reviews r ON r.man_id = m.id AND r.status='APPROVED' " +
                "GROUP BY m.id " +
                "ORDER BY m.id DESC";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    list.add(new ManStats(
                            rs.getInt("id"),
                            rs.getString("name"),
                            rs.getString("phone"),
                            rs.getString("tg_username"),
                            rs.getString("tg_id"),
                            rs.getInt("reviews_count"),
                            rs.getDouble("avg_rating")
                    ));
                }
            }
        }
        return list;
    }

    public synchronized Stats getStats() throws SQLException {
        int users = 0;
        int men = 0;
        int reviews = 0;
        try (PreparedStatement ps = conn.prepareStatement("SELECT COUNT(*) FROM users")) {
            try (ResultSet rs = ps.executeQuery()) { if (rs.next()) users = rs.getInt(1); }
        }
        try (PreparedStatement ps = conn.prepareStatement("SELECT COUNT(*) FROM men")) {
            try (ResultSet rs = ps.executeQuery()) { if (rs.next()) men = rs.getInt(1); }
        }
        try (PreparedStatement ps = conn.prepareStatement("SELECT COUNT(*) FROM reviews")) {
            try (ResultSet rs = ps.executeQuery()) { if (rs.next()) reviews = rs.getInt(1); }
        }
        return new Stats(users, men, reviews);
    }

    public synchronized void grantAdminByTgId(long tgId) throws SQLException {
        User existing = getUserByTgId(tgId);
        if (existing == null) {
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO users(tg_id, is_admin, registered_at, state) VALUES(?,?,?,?)")) {
                ps.setLong(1, tgId);
                ps.setInt(2, 1);
                ps.setString(3, TimeUtil.nowIso());
                ps.setString(4, UserState.NONE.name());
                ps.executeUpdate();
            }
        } else {
            try (PreparedStatement ps = conn.prepareStatement(
                    "UPDATE users SET is_admin=1 WHERE tg_id=?")) {
                ps.setLong(1, tgId);
                ps.executeUpdate();
            }
        }
    }

    public synchronized List<Long> listAdminTgIds() throws SQLException {
        List<Long> ids = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement("SELECT tg_id FROM users WHERE is_admin=1")) {
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) ids.add(rs.getLong(1));
            }
        }
        return ids;
    }

    private User mapUser(ResultSet rs) throws SQLException {
        return new User(
                rs.getInt("id"),
                rs.getLong("tg_id"),
                rs.getString("tg_username"),
                rs.getString("first_name"),
                rs.getInt("is_admin") == 1,
                TimeUtil.parseIso(rs.getString("premium_until")),
                rs.getString("state") == null ? UserState.NONE : UserState.valueOf(rs.getString("state")),
                rs.getString("state_payload")
        );
    }

    private Man mapMan(ResultSet rs) throws SQLException {
        return new Man(
                rs.getInt("id"),
                rs.getString("phone"),
                rs.getString("tg_username"),
                rs.getString("tg_id"),
                rs.getString("name"),
                rs.getString("description"),
                rs.getString("photo_file_id"),
                rs.getInt("is_closed") == 1,
                (Integer) rs.getObject("created_by"),
                TimeUtil.parseIso(rs.getString("created_at"))
        );
    }

    private Review mapReview(ResultSet rs) throws SQLException {
        return new Review(
                rs.getInt("id"),
                rs.getInt("man_id"),
                rs.getInt("author_id"),
                rs.getInt("rating"),
                rs.getString("text"),
                rs.getString("status"),
                TimeUtil.parseIso(rs.getString("created_at")),
                TimeUtil.parseIso(rs.getString("updated_at"))
        );
    }

    private Payment mapPayment(ResultSet rs) throws SQLException {
        Integer manId = (Integer) rs.getObject("man_id");
        return new Payment(
                rs.getInt("id"),
                rs.getInt("user_id"),
                manId,
                rs.getString("type"),
                rs.getInt("amount"),
                rs.getString("status"),
                TimeUtil.parseIso(rs.getString("created_at")),
                TimeUtil.parseIso(rs.getString("updated_at"))
        );
    }
}
