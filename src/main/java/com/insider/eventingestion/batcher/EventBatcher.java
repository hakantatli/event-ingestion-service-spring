package com.insider.eventingestion.batcher;

import com.insider.eventingestion.domain.EventRecord;
import com.insider.eventingestion.repository.EventRepository;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

@Slf4j
@Component
public class EventBatcher {

    private final EventRepository repository;
    private final BlockingQueue<EventRecord> queue;
    private final int batchSize;
    private final ScheduledExecutorService scheduler;
    private final ExecutorService insertExecutor;

    public EventBatcher(
            EventRepository repository,
            @Value("${app.batcher.queue-capacity:1000000}") int queueCapacity,
            @Value("${app.batcher.batch-size:50000}") int batchSize,
            @Value("${app.batcher.flush-interval-ms:50}") long flushIntervalMs) {
        this.repository = repository;
        this.queue = new ArrayBlockingQueue<>(queueCapacity);
        this.batchSize = batchSize;
        
        this.scheduler = Executors.newSingleThreadScheduledExecutor();
        this.insertExecutor = Executors.newFixedThreadPool(4); // Adjust for parallel IO
        
        this.scheduler.scheduleAtFixedRate(this::flush, flushIntervalMs, flushIntervalMs, TimeUnit.MILLISECONDS);
    }

    public boolean add(EventRecord event) {
        return queue.offer(event);
    }

    private void flush() {
        int drainedSize = queue.size();
        if (drainedSize == 0) return;

        // Drain up to batch size
        List<EventRecord> batch = new ArrayList<>(Math.min(drainedSize, batchSize));
        queue.drainTo(batch, batchSize);

        if (!batch.isEmpty()) {
            insertExecutor.submit(() -> {
                try {
                    repository.insertBatch(batch);
                } catch (Exception e) {
                    log.error("Failed to insert batch of size {}: {}", batch.size(), e.getMessage());
                }
            });
        }
    }

    @PreDestroy
    public void shutdown() {
        log.info("Shutting down batcher...");
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(1, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }

        // Final flush
        flush();

        insertExecutor.shutdown();
        try {
            if (!insertExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                insertExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            insertExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
