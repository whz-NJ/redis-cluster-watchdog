/*
 * Copyright 2016-2018 Leon Chen
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.moilioncircle.redis.cluster.watchdog.message.handler;

import com.moilioncircle.redis.cluster.watchdog.manager.ClusterManagers;
import com.moilioncircle.redis.cluster.watchdog.message.ClusterMessage;
import com.moilioncircle.redis.cluster.watchdog.state.ClusterLink;
import com.moilioncircle.redis.cluster.watchdog.state.ClusterNode;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Arrays;
import java.util.Objects;

import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.CLUSTERMSG_TYPE_PONG;
import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.CLUSTER_NODE_HANDSHAKE;
import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.CLUSTER_NODE_MASTER;
import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.CLUSTER_NODE_MIGRATE_TO;
import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.CLUSTER_NODE_NOADDR;
import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.CLUSTER_NODE_SLAVE;
import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.CLUSTER_SLOTS;
import static com.moilioncircle.redis.cluster.watchdog.manager.ClusterSlotManager.bitmapTestBit;
import static com.moilioncircle.redis.cluster.watchdog.state.NodeStates.nodeInHandshake;
import static com.moilioncircle.redis.cluster.watchdog.state.NodeStates.nodeIsMaster;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class ClusterMessagePingHandler extends AbstractClusterMessageHandler {
    
    private static final Log logger = LogFactory.getLog(ClusterMessagePingHandler.class);
    
    public ClusterMessagePingHandler(ClusterManagers managers) {
        super(managers);
    }
    
    @Override
    public boolean handle(ClusterNode sender, ClusterLink link, ClusterMessage hdr) {
        logger.debug("Ping packet received: node:" + (link.node == null ? "(nil)" : link.node.name));
        
        if (server.myself.ip == null && managers.configuration.getClusterAnnounceIp() == null) {
            String ip = link.fd.getLocalAddress(null);
            if (!Objects.equals(ip, server.myself.ip)) server.myself.ip = ip;
        }
        
        managers.messages.clusterSendPing(link, CLUSTERMSG_TYPE_PONG); // WHZ 根据配置的redis版本确定发送消息的格式
        
        if (link.node != null && nodeInHandshake(link.node)) {
            if (sender != null) {
                nodeUpdateAddressIfNeeded(sender, link, hdr);
                managers.nodes.clusterDelNode(link.node);
                return false;
            }
            
            managers.nodes.clusterRenameNode(link.node, hdr.name);
            link.node.flags &= ~CLUSTER_NODE_HANDSHAKE;
            link.node.flags |= hdr.flags & (CLUSTER_NODE_MASTER | CLUSTER_NODE_SLAVE);
        } else if (link.node != null && !link.node.name.equals(hdr.name)) {
            link.node.flags |= CLUSTER_NODE_NOADDR;
            link.node.ip = null;
            link.node.port = 0;
            link.node.busPort = 0;
            managers.connections.freeClusterLink(link);
            return false;
        }
        
        if (sender == null) return true;
        
        if (!nodeInHandshake(sender)) nodeUpdateAddressIfNeeded(sender, link, hdr);
        
        if (hdr.master == null) managers.nodes.clusterSetNodeAsMaster(sender); // WHZ sender 的 slaveof 为空，说明它是 master
        else {
            ClusterNode master = managers.nodes.clusterLookupNode(hdr.master); // WHZ hdr.master 不是消息带过来的，是自己赋值的 ???
            if (nodeIsMaster(sender)) { // WHZ 本地记录的 sender 信息为 Master，和收到的 PING 消息不符
                managers.slots.clusterDelNodeSlots(sender);
                sender.flags &= ~(CLUSTER_NODE_MASTER | CLUSTER_NODE_MIGRATE_TO); // WHZ 将 MASTER 和 MIGRATE_TO 置 0
                sender.flags |= CLUSTER_NODE_SLAVE; // WHZ 将 SLAVE 置 1
            }
            if (master != null && (sender.master == null || !Objects.equals(sender.master, master))) {
                if (sender.master != null) managers.nodes.clusterNodeRemoveSlave(sender.master, sender);
                managers.nodes.clusterNodeAddSlave(master, sender);
                sender.master = master;
            }
        }
        /* 1) If the sender of the message is a master, and we detected that
         *    the set of slots it claims changed, scan the slots to see if we
         *    need to update our configuration. */
        ClusterNode senderMaster = nodeIsMaster(sender) ? sender : sender.master;
        if (senderMaster != null && !Arrays.equals(senderMaster.slots, hdr.slots)) {
            if (nodeIsMaster(sender)) clusterUpdateSlotsConfigWith(sender, hdr.configEpoch, hdr.slots);

            /* 2) We also check for the reverse condition, that is, the sender
             *    claims to serve slots we know are served by a master with a
             *    greater configEpoch. If this happens we inform the sender.
             *
             * This is useful because sometimes after a partition heals, a
             * reappearing master may be the last one to claim a given set of
             * hash slots, but with a configuration that other instances know to
             * be deprecated. Example:
             *
             * A and B are master and slave for slots 1,2,3.
             * A is partitioned away, B gets promoted.
             * B is partitioned away, and A returns available.
             *
             * Usually B would PING A publishing its set of served slots and its
             * configEpoch, but because of the partition B can't inform A of the
             * new configuration, so other nodes that have an updated table must
             * do it. In this way A will stop to act as a master (or can try to
             * failover if there are the conditions to win the election). */
            for (int i = 0; i < CLUSTER_SLOTS; i++) {
                if (!bitmapTestBit(hdr.slots, i)) continue;
                if (server.cluster.slots[i] == null) continue;
                if (Objects.equals(server.cluster.slots[i], sender)) continue;
                if (server.cluster.slots[i].configEpoch <= hdr.configEpoch) continue; //WHZ Sender的某个Slot上的Master.ConfigEpoch比自己这里记录的小，
                managers.messages.clusterSendUpdate(sender.link, server.cluster.slots[i]); //WHZ 那么就会返回UPDATE告诉Sender更新Slots归属信息

                break;
            }
        }
        
        if (nodeIsMaster(server.myself) && nodeIsMaster(sender) && hdr.configEpoch == server.myself.configEpoch) // WHZ configEpoch 冲突的处理
            clusterHandleConfigEpochCollision(sender);
        clusterProcessGossipSection(hdr, link);
        return true;
    }
}
