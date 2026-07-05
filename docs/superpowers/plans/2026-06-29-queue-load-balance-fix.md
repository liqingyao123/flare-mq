# Queue 负载均衡修复 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix queue load balance — all messages route to queue 0 because `incrementMessageCount()` is never called after `putMessage`.

**Architecture:** One-line addition in `BrokerRequestHandler.handleSendMessage()`: call `queue.incrementMessageCount()` after successful `putMessage`. The method already exists on `QueueConfig`. Existing test `testSelectLeastLoadedQueue` in `QueueManagerTest` already validates the `incrementMessageCount` + `selectLeastLoadedQueue` contract.

**Tech Stack:** Java 8, JUnit 5

## Global Constraints

- Target file: `flare-mq-broker/src/main/java/com/flare/mq/broker/BrokerRequestHandler.java`
- `QueueConfig.incrementMessageCount()` already exists — no new methods needed
- Existing tests must continue to pass

---

### Task 1: Add incrementMessageCount call in handleSendMessage

**Files:**
- Modify: `flare-mq-broker/src/main/java/com/flare/mq/broker/BrokerRequestHandler.java:114-126`

**Interfaces:**
- Consumes: `QueueConfig.incrementMessageCount()` (existing, `QueueConfig.java:87`)
- Produces: N/A — internal behavior change

- [ ] **Step 1: Add the one-line fix**

In `BrokerRequestHandler.handleSendMessage()`, in the success branch, after `putMessage` succeeds, increment the counter on the selected queue. Change lines 114-118 from:

```java
        PutMessageResult putRes = messageStore.putMessage(storeMsg);
        if (putRes != null && putRes.isOk()) {
            return ProtocolMessage.createSuccessResponse(
                    MessageType.SEND_MESSAGE_RESPONSE,
                    request.getRequestId(),
                    "OK".getBytes(StandardCharsets.UTF_8));
```

to:

```java
        PutMessageResult putRes = messageStore.putMessage(storeMsg);
        if (putRes != null && putRes.isOk()) {
            queue.incrementMessageCount();
            return ProtocolMessage.createSuccessResponse(
                    MessageType.SEND_MESSAGE_RESPONSE,
                    request.getRequestId(),
                    "OK".getBytes(StandardCharsets.UTF_8));
```

- [ ] **Step 2: Run existing tests to verify no regression**

```bash
mvn test -pl flare-mq-broker -Dtest=QueueManagerTest
```

Expected: all tests pass, especially `testSelectLeastLoadedQueue`.

- [ ] **Step 3: Commit**

```bash
git add flare-mq-broker/src/main/java/com/flare/mq/broker/BrokerRequestHandler.java
git commit -m "fix: increment queue messageCount after successful putMessage to fix load balance"
```
