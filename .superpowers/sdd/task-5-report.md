# Task 5 Report: 前端 index.html — Vue 3 单页面

## Status: Complete

## Summary
Created the Vue 3 CDN single-page management UI at `ruyuan-mq-console/src/main/resources/static/index.html`.

- **概览 (Overview) tab**: Displays 8 metric cards (Broker nodes, health status, topic count, message count, uptime, CPU/memory/disk usage) plus TPS, version, and last refresh time. Polls `/api/overview` and `/api/health` every 5 seconds.
- **Broker tab**: Table with 7 columns (Broker, address, role, status, CPU, memory, disk). Clicking a row shows an expandable detail bar with connections, messages, TPS, and cluster name. Polls `/api/brokers` every 5 seconds.
- **Topic tab**: Table with search filter, "新建 Topic" button with modal dialog (name + queue count), and per-row delete button with confirmation. Polls `/api/topics` every 5 seconds.

## Commits
- `66328b9` - feat: add Vue 3 management UI with overview, broker, and topic tabs

## Build Verification
- `mvn package -pl ruyuan-mq-console -am -q -DskipTests` -- BUILD SUCCESS
- `jar tf ruyuan-mq-console/target/ruyuan-mq-console-1.0.0-SNAPSHOT.jar | grep index.html` -- confirms `static/index.html` is packaged inside the jar

## Concerns
- The jar contains `static/index.html` at the root level, not `BOOT-INF/classes/static/index.html`. This is correct for the non-Spring-Boot packaging. The static resource serving mechanism (Task 3's controller or a separate static file handler) must serve from the classpath root accordingly.
- All API endpoints (`/api/overview`, `/api/health`, `/api/brokers`, `/api/topics`, `POST /api/topics`, `DELETE /api/topics/{name}`) are assumed to be implemented by Tasks 3 and 4. No API mocking is included.
- Vue 3 is loaded via unpkg CDN -- requires network access at runtime.

---

## Fix Report: 5 Issues Resolved (2026-07-04)

### Issue 1: Polling timers not cleared for outgoing tab
**Root cause:** In `switchTab(t)`, `clearInterval(this.timers[t])` was called AFTER `this.tab=t`, so it cleared the NEW tab's timer instead of the old tab's. The outgoing tab's timer kept running indefinitely, leaking intervals.

**Fix:**
```javascript
switchTab(t) {
  if (this.timers[this.tab]) { clearInterval(this.timers[this.tab]); delete this.timers[this.tab]; }
  this.tab=t; this.selectedBrokerName=null; this.selectedTopicName=null;
  this.loadTab(t); this.timers[t]=setInterval(()=>this.loadTab(t),5000);
},
```
The fix reads `this.timers[this.tab]` BEFORE updating `this.tab`, so the old tab's interval is correctly cleared.

### Issue 2: selectedTopic index stale after search
**Root cause:** `selectedTopic` stored an array index into `filteredTopics`. When the user types in the search box, `filteredTopics` shrinks and indices shift, so the stored index may point to a different topic.

**Fix:** Replaced `selectedTopic` (integer index) with `selectedTopicName` (string). Added:
- `selectedTopicData` computed property that finds the topic by name in `filteredTopics`
- `watch: { topicSearch() { this.selectedTopicName = null; } }` to clear selection on search
- Template now uses `@click="selectedTopicName=t.topicName"` and `v-if="selectedTopicData"` with `selectedTopicData.queueCount`, etc.

### Issue 3: selectedBroker index stale on refresh
**Root cause:** Same pattern -- `selectedBroker` was an array index that became stale when the broker list refreshed (every 5 seconds).

**Fix:** Replaced `selectedBroker` (integer index) with `selectedBrokerName` (string). Added:
- `selectedBrokerData` computed property that finds the broker by name in the `brokers` array
- Template now uses `@click="selectedBrokerName=b.brokerName"` and `v-if="selectedBrokerData"` with `selectedBrokerData.activeConnections`, etc.

### Issue 4: Dead code -- /api/health fetched but never used
**Root cause:** `loadTab('overview')` fetched `/api/health` and stored it in `this.health`, but no template binding or computed property ever read `this.health`.

**Fix:** Removed the `fetch('/api/health')` line from `loadTab()` and removed the `health: {}` field from `data()`.

### Issue 5: No .catch() on fetch calls
**Root cause:** All 4 fetch calls (overview, brokers, topics, createTopic, deleteTopic) had no `.catch()` handler. Network errors or server errors would result in unhandled promise rejections.

**Fix:** Added `.catch(e => console.error('...', e))` to every fetch chain:
- `loadTab` overview: `Failed to load overview`
- `loadTab` brokers: `Failed to load brokers`
- `loadTab` topics: `Failed to load topics`
- `createTopic`: `Failed to create topic`
- `deleteTopic`: `Failed to delete topic`

## Build Verification
- `mvn package -pl ruyuan-mq-console -am -q -DskipTests` -- BUILD SUCCESS
- `jar tf ruyuan-mq-console/target/ruyuan-mq-console-1.0.0-SNAPSHOT.jar | grep index.html` -- confirms `static/index.html` is packaged
