package ru.student7.integration;

import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.http.base.HttpOperationFailedException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

@Component
public class ReportRoute extends RouteBuilder {

    private final JdbcTemplate jdbcTemplate;
    private final TokenProvider tokenProvider;

    @Value("${report.api.url}")
    private String reportApiUrl;

    public ReportRoute(JdbcTemplate jdbcTemplate, TokenProvider tokenProvider) {
        this.jdbcTemplate = jdbcTemplate;
        this.tokenProvider = tokenProvider;
    }

    @Override
    public void configure() {
        // если токен протух, сбрасываем кэш и пробуем еще раз
        onException(HttpOperationFailedException.class)
            .onWhen(simple("${exception.statusCode} == 401"))
            .maximumRedeliveries(1)
            .process(exchange -> {
                log.warn("Token rejected by the server (401). Get a new and try again...");
                tokenProvider.invalidateToken();
                exchange.getIn().setHeader("Authorization", "Bearer " +
                tokenProvider.getToken());
            });

        // глобальный перехват ошибок для этого маршрута
        onException(Exception.class)
            .handled(true)
            .log("Error generating report: ${exception.message}")
            .logStackTrace(true);

        // запуск раз в сутки, ждем минуту после старта,
        // чтобы поток №1 успел отработать первым
        from("timer:reportTimer?period=86400000&delay=60000")
            .routeId("reportRoute")

            // получаем токен и кладем его в заголовок
            .process(exchange -> {
                String token = tokenProvider.getToken();
                exchange.getIn().setHeader("Authorization", "Bearer " + token);
            })

            // читаем данные из sqlite и формируем csv
            .process(exchange -> {
                // берем только актуальные (не архивные) детали
                List<Map<String, Object>> rows = jdbcTemplate.queryForList(
                    "SELECT spare_code, spare_name, spare_description, " +
                    "spare_type, spare_status, price, quantity, updated_at " +
                    "FROM spares_history WHERE is_archived = 0 ORDER BY spare_code"
                );

                StringBuilder csv = new StringBuilder();

                for (Map<String, Object> row : rows) {
                    String updatedAtStr = row.get("updated_at") != null ?
                    row.get("updated_at").toString() : "";
                                
                    csv.append(escapeCsv(row.get("spare_code"))).append(";")
                       .append(escapeCsv(row.get("spare_name"))).append(";")
                       .append(escapeCsv(row.get("spare_description"))).append(";")
                       .append(escapeCsv(row.get("spare_type"))).append(";")
                       .append(escapeCsv(row.get("spare_status"))).append(";")
                       .append(escapeCsv(row.get("price"))).append(";")
                       .append(escapeCsv(row.get("quantity"))).append(";")
                       .append(escapeCsv(updatedAtStr)).append("\n");
                }

                // переводим строку в байты utf-8
                byte[] csvBytes = csv.toString().getBytes(
                    StandardCharsets.UTF_8);
                exchange.getIn().setBody(csvBytes);
                
                log.info("CSV formed. Number of records: {}", rows.size());
            })

            // отправляем на report api
            .setHeader(Exchange.HTTP_METHOD, constant("POST"))
            .setHeader(Exchange.CONTENT_TYPE, constant("text/csv; charset=UTF-8"))
            .toD(reportApiUrl)
            .log("Report API response: ${header.CamelHttpResponseCode} - ${body}");   
    }
    
    // экранирование спецсимволов для csv -
    // если в тексте есть ';', кавычки или переносы строк, поле оборачивается в кавычки
    private String escapeCsv(Object value) {
        if (value == null) return "";
        String str = value.toString();
        if (str.contains(";") || str.contains("\"") || str.contains("\n") ||
        str.contains("\r")) {
            return "\"" + str.replace("\"", "\"\"") + "\"";
        }
        return str;
    }
}