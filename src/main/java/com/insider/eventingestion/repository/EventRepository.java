package com.insider.eventingestion.repository;

import com.insider.eventingestion.domain.EventRecord;
import com.insider.eventingestion.domain.MetricResponse;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Repository;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;

@Slf4j
@Repository
@RequiredArgsConstructor
public class EventRepository {

    private final DataSource dataSource;

    private static final Set<String> ALLOWED_GROUPS = Set.of(
            "channel",
            "campaign_id",
            "toStartOfHour(timestamp)",
            "toStartOfDay(timestamp)"
    );

    @PostConstruct
    public void initSchema() {
        String query = """
                CREATE TABLE IF NOT EXISTS events (
                    event_id String,
                    event_name String,
                    user_id String,
                    timestamp DateTime,
                    channel String,
                    campaign_id String,
                    tags Array(String),
                    metadata Map(String, String),
                    inserted_at DateTime DEFAULT now()
                ) ENGINE = ReplacingMergeTree(inserted_at)
                ORDER BY (event_name, timestamp, user_id, event_id);
                """;

        int maxRetries = 10;
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try (Connection conn = dataSource.getConnection();
                 Statement stmt = conn.createStatement()) {
                stmt.execute(query);
                log.info("ClickHouse schema initialized successfully.");
                return;
            } catch (SQLException e) {
                log.warn("ClickHouse connection attempt {}/{} failed: {}", attempt, maxRetries, e.getMessage());
                if (attempt == maxRetries) {
                    throw new RuntimeException("Failed to initialize ClickHouse schema after " + maxRetries + " retries", e);
                }
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted while waiting for ClickHouse", ie);
                }
            }
        }
    }

    public void insertBatch(List<EventRecord> events) {
        if (events == null || events.isEmpty()) return;

        StringBuilder sql = new StringBuilder(
                "INSERT INTO events (event_id, event_name, user_id, timestamp, channel, campaign_id, tags, metadata, inserted_at) VALUES ");

        for (int i = 0; i < events.size(); i++) {
            EventRecord e = events.get(i);

            // Escape single quotes to prevent SQL issues
            String eventId    = escape(e.getEventId());
            String eventName  = escape(e.getEventName());
            String userId     = escape(e.getUserId());
            String channel    = escape(e.getChannel() != null ? e.getChannel() : "");
            String campaignId = escape(e.getCampaignId() != null ? e.getCampaignId() : "");
            String tsStr      = e.getTimestamp().format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
            String insStr     = e.getInsertedAt().format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

            // Build Array(String) literal: ['a','b']
            String tagsLiteral = buildArrayLiteral(e.getTags());

            // Build Map(String,String) literal: {'k':'v'}
            String metaLiteral = buildMapLiteral(e.getMetadata());

            sql.append(String.format("('%s','%s','%s','%s','%s','%s',%s,%s,'%s')",
                    eventId, eventName, userId, tsStr, channel, campaignId,
                    tagsLiteral, metaLiteral, insStr));

            if (i < events.size() - 1) sql.append(",");
        }

        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute(sql.toString());
        } catch (SQLException e) {
            log.error("Failed to insert batch to ClickHouse: {}", e.getMessage());
            throw new RuntimeException("Failed to insert batch", e);
        }
    }

    private String escape(String s) {
        if (s == null) return "";
        return s.replace("\\", "\\\\").replace("'", "\\'");
    }

    private String buildArrayLiteral(List<String> tags) {
        if (tags == null || tags.isEmpty()) return "[]";
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < tags.size(); i++) {
            sb.append("'").append(escape(tags.get(i))).append("'");
            if (i < tags.size() - 1) sb.append(",");
        }
        sb.append("]");
        return sb.toString();
    }

    private String buildMapLiteral(Map<String, String> map) {
        if (map == null || map.isEmpty()) return "{}";
        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<String, String> entry : map.entrySet()) {
            if (!first) sb.append(",");
            sb.append("'").append(escape(entry.getKey())).append("':'")
              .append(escape(entry.getValue())).append("'");
            first = false;
        }
        sb.append("}");
        return sb.toString();
    }

    @Cacheable(cacheNames = "metrics", key = "#from + '|' + #to + '|' + #eventName + '|' + #groupBy", sync = true)
    public MetricResponse getMetrics(Long from, Long to, String eventName, String groupBy) {
        log.info("Cache miss for getMetrics({}, {}, {}, {})", from, to, eventName, groupBy);
        StringBuilder query = new StringBuilder("SELECT count(*) as total_event_count, uniqExact(user_id) as unique_event_count");

        if (groupBy != null && !groupBy.isEmpty()) {
            if (!ALLOWED_GROUPS.contains(groupBy)) {
                throw new IllegalArgumentException("invalid group by column");
            }
            query.append(", ").append(groupBy).append(" as grouped_by");
        }

        query.append(" FROM events FINAL WHERE event_name = ?");

        if (from != null && from > 0) {
            query.append(" AND timestamp >= toDateTime(?)");
        }
        if (to != null && to > 0) {
            query.append(" AND timestamp <= toDateTime(?)");
        }

        if (groupBy != null && !groupBy.isEmpty()) {
            query.append(" GROUP BY ").append(groupBy);
        }

        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(query.toString())) {

            int paramIdx = 1;
            pstmt.setString(paramIdx++, eventName);

            if (from != null && from > 0) {
                pstmt.setLong(paramIdx++, from);
            }
            if (to != null && to > 0) {
                pstmt.setLong(paramIdx++, to);
            }

            try (ResultSet rs = pstmt.executeQuery()) {
                MetricResponse response = MetricResponse.builder()
                        .groupedData(new ArrayList<>())
                        .build();

                long totalSum = 0;
                long uniqueSum = 0;

                while (rs.next()) {
                    long totalCount = rs.getLong("total_event_count");
                    long uniqueCount = rs.getLong("unique_event_count");

                    if (groupBy != null && !groupBy.isEmpty()) {
                        String groupVal = rs.getString("grouped_by");
                        Map<String, Object> groupMap = new HashMap<>();
                        groupMap.put("group", groupVal);
                        groupMap.put("total_event_count", totalCount);
                        groupMap.put("unique_event_count", uniqueCount);
                        response.getGroupedData().add(groupMap);

                        totalSum += totalCount;
                        uniqueSum += uniqueCount;
                    } else {
                        totalSum = totalCount;
                        uniqueSum = uniqueCount;
                    }
                }

                response.setTotalEventCount(totalSum);
                response.setUniqueEventCount(uniqueSum);

                return response;
            }
        } catch (SQLException e) {
            log.error("Failed to execute metrics query", e);
            throw new RuntimeException("Failed to get metrics", e);
        }
    }
}
