package com.flare.mq.nameserver.cluster;

import com.flare.mq.nameserver.registry.BrokerData;
import com.flare.mq.nameserver.registry.ServiceRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    /** 确定性选主：offset 降序，平局 brokerId 升序。 */
    static BrokerData electNewMaster(List<BrokerData> candidates) {
        return candidates.stream()
                .sorted(Comparator
                        .comparingLong(BrokerData::getTotalMessages).reversed()
                        .thenComparingLong(MasterElectionManager::minBrokerId))
                .findFirst()
                .orElse(null);
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

    /** 提升：把新 master 的地址写入 brokerId 0 槽位，epoch+1，返回新 epoch。 */
    public long promoteToMaster(BrokerData newMaster) {
        long id = minBrokerId(newMaster);
        String addr = newMaster.getBrokerAddrs().get(id);
        newMaster.getBrokerAddrs().put(0L, addr);
        long e = epoch.incrementAndGet();
        logger.info("Promoted master: broker={}, addr={}, epoch={}", newMaster.getBrokerName(), addr, e);
        return e;
    }
}
