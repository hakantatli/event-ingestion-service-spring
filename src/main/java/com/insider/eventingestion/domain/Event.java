package com.insider.eventingestion.domain;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public class Event {

    @NotBlank(message = "event_name is required")
    @JsonProperty("event_name")
    private String eventName;

    @NotBlank(message = "user_id is required")
    @JsonProperty("user_id")
    private String userId;

    @NotNull(message = "invalid timestamp")
    private Long timestamp;

    private String channel;

    @JsonProperty("campaign_id")
    private String campaignId;

    private List<String> tags;

    private Map<String, Object> metadata;
}
