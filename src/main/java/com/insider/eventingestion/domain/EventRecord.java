package com.insider.eventingestion.domain;

import lombok.Builder;
import lombok.Data;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

@Data
@Builder
public class EventRecord {
    private String eventId;
    private String eventName;
    private String userId;
    private LocalDateTime timestamp;
    private String channel;
    private String campaignId;
    private List<String> tags;
    private Map<String, String> metadata;
    private LocalDateTime insertedAt;
}
