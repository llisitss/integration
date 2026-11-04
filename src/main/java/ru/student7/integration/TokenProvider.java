package ru.student7.integration;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.time.Instant;

@Component
public class TokenProvider {

    @Value("${api.username}")
    private String username;

    @Value("${api.password}")
    private String password;

    @Value("${token.api.url}")
    private String tokenApiUrl;

    // по умолчанию считаем что токен живет 60 секунд
    @Value("${api.token.ttl.seconds:60}")
    private long ttlSeconds;

    private final RestTemplate restTemplate = new RestTemplate();
    private final ObjectMapper objectMapper = new ObjectMapper();

    private String cachedToken;
    private Instant cachedAt;

    public synchronized String getToken() {
        if (cachedToken != null && Instant.now().isBefore(
            cachedAt.plusSeconds(ttlSeconds))) {
            return cachedToken;
        }
        refreshToken();
        return cachedToken;
    }

    // сбрасывает кэш, нужно, если сервер вернул 401
    public synchronized void invalidateToken() {
        this.cachedToken = null;
        this.cachedAt = null;
    }

    private void refreshToken() {
        try {
            String body = String.format(
                "{\"username\": \"%s\", \"password\": \"%s\"}",
                username, password);
            
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            HttpEntity<String> request = new HttpEntity<>(body, headers);

            String response = restTemplate.postForObject(
                    tokenApiUrl, 
                    request, 
                    String.class
            );

            JsonNode json = objectMapper.readTree(response);
            cachedToken = json.get("token").asText();
            cachedAt = Instant.now();
            System.out.println("Token for Report API successfully updated");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}