package com.insider.eventingestion.controller;

import com.insider.eventingestion.batcher.EventBatcher;
import com.insider.eventingestion.domain.Event;
import com.insider.eventingestion.domain.EventRecord;
import com.insider.eventingestion.domain.MetricResponse;
import com.insider.eventingestion.repository.EventRepository;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.HexFormat;
import java.util.List;
import java.util.stream.Collectors;

@RestController
@RequiredArgsConstructor
public class EventController {

    private final EventBatcher batcher;
    private final EventRepository repository;

    private String generateEventId(Event e) {
        try {
            String data = String.format("%s|%s|%d", e.getEventName(), e.getUserId(), e.getTimestamp());
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] hash = md.digest(data.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(hash);
        } catch (NoSuchAlgorithmException ex) {
            throw new RuntimeException("MD5 not available", ex);
        }
    }

    private void validateTimestamp(Long timestamp) {
        if (timestamp == null || timestamp <= 0) {
            throw new IllegalArgumentException("invalid timestamp");
        }
        long now = Instant.now().getEpochSecond();
        // Allow 3700s (3600 + 100s buffer) to account for clock skew between Docker container and host
        if (timestamp < now - 3700 || timestamp > now + 3700) {
            throw new IllegalArgumentException("timestamp must be within 1 hour of current time");
        }
    }

    private EventRecord mapToRecord(Event e) {
        return EventRecord.builder()
                .eventId(generateEventId(e))
                .eventName(e.getEventName())
                .userId(e.getUserId())
                .timestamp(LocalDateTime.ofEpochSecond(e.getTimestamp(), 0, ZoneOffset.UTC))
                .channel(e.getChannel())
                .campaignId(e.getCampaignId())
                .tags(e.getTags())
                .metadata(e.getMetadata() != null ? 
                        e.getMetadata().entrySet().stream().collect(Collectors.toMap(
                                java.util.Map.Entry::getKey,
                                entry -> String.valueOf(entry.getValue())
                        )) : null)
                .insertedAt(LocalDateTime.now(ZoneOffset.UTC))
                .build();
    }

    @PostMapping("/events")
    public ResponseEntity<String> ingestEvent(@Valid @RequestBody Event event) {
        try {
            validateTimestamp(event.getTimestamp());
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(e.getMessage());
        }

        EventRecord record = mapToRecord(event);
        if (!batcher.add(record)) {
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body("Server overloaded: queue full");
        }

        return ResponseEntity.accepted().body("{\"status\": \"accepted\"}");
    }

    @PostMapping("/events/bulk")
    public ResponseEntity<String> bulkIngestEvents(@Valid @RequestBody List<Event> events) {
        List<EventRecord> records;
        try {
            records = events.stream().map(e -> {
                validateTimestamp(e.getTimestamp());
                return mapToRecord(e);
            }).collect(Collectors.toList());
        } catch (IllegalArgumentException ex) {
            return ResponseEntity.badRequest().body("Validation failed: " + ex.getMessage());
        }

        if (!records.isEmpty()) {
            repository.insertBatch(records);
        }

        return ResponseEntity.status(HttpStatus.CREATED)
                .body(String.format("{\"status\": \"created\", \"count\": %d}", events.size()));
    }

    @GetMapping("/metrics")
    public ResponseEntity<?> getMetrics(
            @RequestParam(name = "event_name", required = true) String eventName,
            @RequestParam(name = "from", required = false) Long from,
            @RequestParam(name = "to", required = false) Long to,
            @RequestParam(name = "group_by", required = false) String groupBy) {

        if (eventName == null || eventName.isEmpty()) {
            return ResponseEntity.badRequest().body("event_name is required");
        }

        try {
            MetricResponse response = repository.getMetrics(from, to, eventName, groupBy);
            return ResponseEntity.ok(response);
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(e.getMessage());
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Failed to get metrics");
        }
    }
}
