# Task 5 Report: Console 模型扩展 ConsumerGroupStatus

## Status: Completed

## Changes Made

**File:** `flare-mq-console/src/main/java/com/flare/mq/console/model/ConsumerGroupStatus.java`

Rewrote the model class with the following changes:

### Fields Added (5 new)
- `activeConsumers` (int) -- count of alive consumers
- `totalLag` (long) -- aggregate lag across all queues
- `queues` (List<QueueInfo>) -- per-queue consumption details
- `consumers` (List<ConsumerInfo>) -- connected consumer instances

### Fields Renamed (1)
- `subscriptionTopic` renamed to `topic`

### Fields Removed (1)
- `lag` (replaced by `totalLag`)

### Fields Kept (6)
- `groupName`, `consumerCount`, `totalConsumed`, `consumeTps`, `status`, `lastUpdateTime`

### Constructor Removed
- The 2-arg constructor `ConsumerGroupStatus(groupName, subscriptionTopic)` was removed, leaving only the no-arg constructor.

### toString Removed
- The `toString()` override was removed.

### New Inner Classes

**QueueInfo** -- per-queue consumption state:
- `queueId` (int)
- `maxOffset` (long)
- `consumedOffset` (long)
- `lag` (long)

**ConsumerInfo** -- connected consumer metadata:
- `consumerId` (String)
- `lastHeartbeat` (long)
- `alive` (boolean)

## Verification
- `mvn compile -pl flare-mq-console -am -q` passed with no errors.
- Commit `7d23548` on `master`.
