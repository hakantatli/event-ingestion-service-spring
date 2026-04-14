package com.insider.eventingestion.domain;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class MetricResponse {
    
    @JsonProperty("total_event_count")
    private long totalEventCount;
    
    @JsonProperty("unique_event_count")
    private long uniqueEventCount;
    
    @JsonProperty("grouped_data")
    private List<Map<String, Object>> groupedData;
}
