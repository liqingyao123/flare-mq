package com.flare.mq.nameserver.cluster;

import com.flare.mq.nameserver.registry.BrokerData;
import com.flare.mq.nameserver.registry.ServiceRegistry;
import com.flare.mq.protocol.MessageType;
import com.flare.mq.protocol.ProtocolMessage;
import com.flare.mq.protocol.client.NettyClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Master 选举管理器（NameServer 单一决策权威）。
 * 选举规则：totalMessages 降序 → brokerId 升序；promote 把新 master 地址写入 brokerId 0 槽位，
 * 使客户端 getMasterAddr()（读 id0）零改动切换到新 master。epoch 单调递增，用作旧主栅栏。
 */
public class MasterElectionManager {

    private static final Logger logger = LoggerFactory.getLogger(MasterElectionManager.class);

    private final ServiceRegistry serviceRegistry;
    private final long leaseDurationMs;
    private final AtomicLong epoch = new AtomicLong(0);

    public MasterElectionManager(ServiceRegistry serviceRegistry, long leaseDurationMs) {
        this.serviceRegistry = serviceRegistry;
        this.leaseDurationMs = leaseDurationMs;
    }

    /** 确定性选主：offset 降序，平局 brokerId 升序。先过滤掉地址槽位被清空的幽灵节点（无法提升）。 */
    static BrokerData electNewMaster(List<BrokerData> candidates) {
        return candidates.stream()
                .filter(MasterElectionManager::hasUsableAddr)
                .sorted(Comparator
                        .comparingLong(BrokerData::getTotalMessages).reversed()
                        .thenComparingLong(MasterElectionManager::minBrokerId))
                .findFirst()
                .orElse(null);
    }

    private static boolean hasUsableAddr(BrokerData d) {
        return d.getBrokerAddrs().values().stream()
                .anyMatch(addr -> addr != null && !addr.isEmpty());
    }

    private static long minBrokerId(BrokerData d) {
        return d.getBrokerAddrs().keySet().stream()
                .mapToLong(Long::longValue)
                .min()
                .orElse(Long.MAX_VALUE);
    }

    public long getEpoch() {
        return epoch.get();
    }

    public long getLeaseDurationMs() {
        return leaseDurationMs;
    }

    /** 提升：把新 master 的地址写入 brokerId 0 槽位，epoch+1，返回新 epoch；无可写地址返回 -1（调用方跳过）。 */
    public long promoteToMaster(BrokerData newMaster) {
        long id = minBrokerId(newMaster);
        String addr = newMaster.getBrokerAddrs().get(id);
        if (addr == null || addr.isEmpty()) {
            logger.warn("Cannot promote broker {}: no usable address", newMaster.getBrokerName());
            return -1L;
        }
        newMaster.getBrokerAddrs().put(0L, addr);
        long e = epoch.incrementAndGet();
        logger.info("Promoted master: broker={}, addr={}, epoch={}", newMaster.getBrokerName(), addr, e);
        return e;
    }

    protected long now() { return System.currentTimeMillis(); }

    /** 定期调用：master 租约过期则选新主并下发 BECOME_MASTER。 */
    public void checkAndFailover() {
        try {
            BrokerData master = findAliveMaster();
            if (master != null) {
                return;   // 当前主仍存活
            }
            BrokerData staleMaster = findStaleMaster();      // id0 持有者但租约已过期（通常是死主）
            // 排除故障/被栅栏的旧主：否则其复活后以最高 offset 反复当选，并连带剥夺真赢家的 id0
            List<BrokerData> slaves = aliveSlaves(staleMaster);
            BrokerData winner = electNewMaster(slaves);
            if (winner == null) {
                logger.warn("No alive slave candidate for master failover");
                return;
            }
            sendStandDown(staleMaster);
            clearStaleId0Slots(winner);                      // 清所有非赢家节点的 id0 槽位
            long e = promoteToMaster(winner);
            if (e < 0) {
                logger.warn("Promotion skipped (no usable address): broker={}, will retry next scan",
                        winner.getBrokerName());
                return;                                      // 正常路径已被 hasUsableAddr 过滤；防御性兜底
            }
            if (staleMaster != null && !staleMaster.getBrokerName().equals(winner.getBrokerName())) {
                serviceRegistry.migrateTopicRoutes(staleMaster.getBrokerName(), winner.getBrokerName());
            }
            if (!sendBecomeMaster(winner, e)) {
                rollbackPromotion(winner);                   // I3：RPC 失败则回滚，下轮重扫重试
            }
        } catch (Exception ex) {
            logger.error("Error in checkAndFailover", ex);
        }
    }

    /** id0 槽位持有者但租约已过期的节点（死主/被隔离主）。 */
    private BrokerData findStaleMaster() {
        for (BrokerData d : serviceRegistry.getAllBrokerData().values()) {
            if (d.getBrokerAddrs().containsKey(0L) && !isAlive(d)) {
                return d;
            }
        }
        return null;
    }

    /** 移除除赢家外所有节点的 id0 槽位，防止残留/僵尸双主。 */
    private void clearStaleId0Slots(BrokerData winner) {
        for (BrokerData d : serviceRegistry.getAllBrokerData().values()) {
            if (d != winner && d.getBrokerAddrs().containsKey(0L)) {
                d.getBrokerAddrs().remove(0L);
                logger.info("Cleared stale master slot: broker={}", d.getBrokerName());
            }
        }
    }

    /** BECOME_MASTER 下发失败时回滚提升，避免留下"假只读 master"。 */
    private void rollbackPromotion(BrokerData winner) {
        winner.getBrokerAddrs().remove(0L);
        logger.warn("Rolled back master promotion (BECOME_MASTER failed): broker={}", winner.getBrokerName());
    }

    /** 当前 master = 拥有 brokerId 0 槽位且租约未过期的节点。 */
    private BrokerData findAliveMaster() {
        for (BrokerData d : serviceRegistry.getAllBrokerData().values()) {
            if (d.getBrokerAddrs().containsKey(0L) && isAlive(d)) {
                return d;
            }
        }
        return null;
    }

    /** 存活且非故障/被栅栏旧主的节点（选举候选）。 */
    private List<BrokerData> aliveSlaves(BrokerData staleMaster) {
        List<BrokerData> alive = new ArrayList<>();
        for (BrokerData d : serviceRegistry.getAllBrokerData().values()) {
            if (isAlive(d) && d != staleMaster) {
                alive.add(d);
            }
        }
        return alive;
    }

    private boolean isAlive(BrokerData d) {
        return (now() - d.getLastUpdateTimestamp()) <= leaseDurationMs;
    }

    /** 尽力通知旧主让位（连不上/失败不影响主流程）。 */
    private void sendStandDown(BrokerData master) {
        if (master == null) return;
        String addr = master.getBrokerAddrs().get(0L);
        sendToBroker(addr, new ProtocolMessage(MessageType.STAND_DOWN_REQUEST, null));
    }

    /** 通知新主上任（携带新 epoch）。成功返回 true，失败返回 false（调用方据此回滚）。 */
    private boolean sendBecomeMaster(BrokerData newMaster, long e) {
        String addr = newMaster.getBrokerAddrs().get(0L);
        if (addr == null) return false;
        String json = "{\"epoch\":" + e + "}";
        return sendToBroker(addr, new ProtocolMessage(MessageType.BECOME_MASTER_REQUEST,
                json.getBytes(StandardCharsets.UTF_8)));
    }

    /** 短超时、异常吞掉：RPC 是尽力而为的通知。返回是否收到成功响应。 */
    private boolean sendToBroker(String addr, ProtocolMessage msg) {
        if (addr == null) return false;
        NettyClient client = null;
        try {
            String[] parts = addr.split(":");
            client = new NettyClient(parts[0], Integer.parseInt(parts[1]));
            client.connect();
            ProtocolMessage resp = client.sendSync(msg, 1000);
            return resp != null && resp.isSuccess();
        } catch (Exception e) {
            logger.warn("RPC to broker {} failed (best-effort): {}", addr, e.getMessage());
            return false;
        } finally {
            if (client != null) client.shutdown();
        }
    }
}
