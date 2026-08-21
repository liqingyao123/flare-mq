package com.flare.mq.broker.cluster;

/**
 * Broker 身份解析：brokerName 默认 host-port，brokerId 默认 port-10911。
 * 两者均可被显式配置覆盖，保证多 broker 默认启动即唯一。
 */
public class BrokerIdentity {

    public final String brokerName;
    public final long brokerId;

    private BrokerIdentity(String brokerName, long brokerId) {
        this.brokerName = brokerName;
        this.brokerId = brokerId;
    }

    public static BrokerIdentity resolve(String brokerAddr, String explicitName,
                                         boolean nameExplicit, Long explicitId, boolean idExplicit) {
        String host = brokerAddr.split(":")[0];
        int port = Integer.parseInt(brokerAddr.split(":")[1]);
        String name = nameExplicit ? explicitName : host + "-" + port;
        long id = idExplicit ? explicitId : (long) port - 10911;
        return new BrokerIdentity(name, id);
    }
}
