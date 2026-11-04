package ru.student7.integration;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.camel.builder.RouteBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
public class CmsSyncRoute extends RouteBuilder {

    private static final DateTimeFormatter DB_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final int PAGE_SIZE = 10;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Value("${api.username}")
    private String username;

    @Value("${api.password}")
    private String password;

    @Value("${cms.api.url}")
    private String cmsApiUrl;

    @Override
    public void configure() {

        onException(Exception.class)
            .handled(true)
            .log("CMS synchronization error: ${exception.message}")
            .logStackTrace(true);

        from("timer:cmsSync?period={{sync.timer.period}}")
            .routeId("cmsSyncRoute")

            .to("sql:CREATE TABLE IF NOT EXISTS spares_history (" +
                "spare_code TEXT PRIMARY KEY, " +
                "spare_name TEXT, " +
                "spare_description TEXT, " +
                "spare_type TEXT, " +
                "spare_status TEXT, " +
                "price TEXT, " +
                "quantity INTEGER, " +
                "updated_at TEXT, " +
                "first_seen_at TEXT, " +
                "last_seen_at TEXT, " +
                "is_archived INTEGER DEFAULT 0, " +
                "archived_at TEXT)")

            // фиксируем время начала синхронизации и стартовую страницу
            .process(exchange -> {
                exchange.setProperty("syncStartedAt",
                        LocalDateTime.now().format(DB_FORMAT));
                exchange.setProperty("page", 0);
                exchange.setProperty("hasMore", true);
            })

            // цикл по страницам
            .loopDoWhile(simple("${exchangeProperty.hasMore}"))

                // авторизация
                .process(exchange -> {
                    String credentials = username + ":" + password;
                    String encoded = Base64.getEncoder()
                            .encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
                    exchange.getIn().setHeader("Authorization", "Basic " + encoded);
                })

                // запрос текущей страницы
                .toD(cmsApiUrl + "?page=${exchangeProperty.page}&size=" + PAGE_SIZE)
                .convertBodyTo(String.class)

                // парсим страницу в список Map-ов для SQL
                .process(exchange -> {
                    String json = exchange.getIn().getBody(String.class);
                    JsonNode root = objectMapper.readTree(json);

                    if (root == null || !root.isArray()) {
                        exchange.setProperty("hasMore", false);
                        exchange.getIn().setBody(null);
                        return;
                    }

                    int count = root.size();
                    exchange.setProperty("hasMore", count == PAGE_SIZE);

                    if (count == 0) {
                        exchange.getIn().setBody(null);
                        return;
                    }

                    String now = LocalDateTime.now().format(DB_FORMAT);
                    List<Map<String, Object>> rows = new ArrayList<>();

                    for (JsonNode node : root) {
                        Map<String, Object> row = new HashMap<>();
                        row.put("spareCode", node.path("spareCode").asText());
                        row.put("spareName", node.path("spareName").asText());
                        row.put("spareDescription", node.path("spareDescription").asText());
                        row.put("spareType", node.path("spareType").asText());
                        row.put("spareStatus", node.path("spareStatus").asText());
                        row.put("price", node.path("price").asText());
                        row.put("quantity", node.path("quantity").asInt());

                        String updatedAt = node.path("updatedAt").asText();
                        row.put("updatedAt", updatedAt);
                        row.put("firstSeenAt", now);
                        row.put("lastSeenAt", now);

                        rows.add(row);
                    }

                    exchange.getIn().setBody(rows);
                })

                // upsert пачки записей в одной транзакции (обновляем или добавляем)
                .choice()
                    .when(simple("${body} != null"))
                        .split(body())
                            .to("sql:INSERT INTO spares_history " +
                                "(spare_code, spare_name, spare_description, spare_type, " +
                                "spare_status, price, quantity, updated_at, " +
                                "first_seen_at, last_seen_at, is_archived) " +
                                "VALUES (:#spareCode, :#spareName, :#spareDescription, :#spareType, " +
                                ":#spareStatus, :#price, :#quantity, :#updatedAt, " +
                                ":#firstSeenAt, :#lastSeenAt, 0) " +
                                "ON CONFLICT(spare_code) DO UPDATE SET " +
                                "spare_name = excluded.spare_name, " +
                                "spare_description = excluded.spare_description, " +
                                "spare_type = excluded.spare_type, " +
                                "spare_status = excluded.spare_status, " +
                                "price = excluded.price, " +
                                "quantity = excluded.quantity, " +
                                "updated_at = excluded.updated_at, " +
                                "last_seen_at = excluded.last_seen_at, " +
                                "is_archived = 0, " +
                                "archived_at = NULL")
                        .end()
                .end()

                // следующая страница
                .process(exchange -> {
                    Integer page = exchange.getProperty("page", Integer.class);
                    exchange.setProperty("page", page + 1);
                })

                .log("CMS: page ${exchangeProperty.page} processed.")
            .end()

            // архивация пропавших из cms записей
            .process(exchange -> {
                exchange.getIn().setHeader("syncStartedAt",
                        exchange.getProperty("syncStartedAt"));
                exchange.getIn().setHeader("now",
                        LocalDateTime.now().format(DB_FORMAT));
            })
            .to("sql:UPDATE spares_history " +
                "SET is_archived = 1, archived_at = :#now " +
                "WHERE last_seen_at < :#syncStartedAt AND is_archived = 0")
            .log("CMS synchronization completed. Start: ${exchangeProperty.syncStartedAt}");
    }
}