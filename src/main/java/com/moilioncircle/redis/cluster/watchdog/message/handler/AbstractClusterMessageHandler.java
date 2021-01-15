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

import com.moilioncircle.redis.cluster.watchdog.ClusterConfiguration;
import com.moilioncircle.redis.cluster.watchdog.manager.ClusterManagers;
import com.moilioncircle.redis.cluster.watchdog.manager.ClusterSlotManager;
import com.moilioncircle.redis.cluster.watchdog.message.ClusterMessage;
import com.moilioncircle.redis.cluster.watchdog.message.ClusterMessageDataGossip;
import com.moilioncircle.redis.cluster.watchdog.state.ClusterLink;
import com.moilioncircle.redis.cluster.watchdog.state.ClusterNode;
import com.moilioncircle.redis.cluster.watchdog.state.ServerState;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.CLUSTERMSG_TYPE_COUNT;
import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.CLUSTER_NODE_FAIL;
import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.CLUSTER_NODE_NOADDR;
import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.CLUSTER_NODE_PFAIL;
import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.CLUSTER_SLOTS;
import static com.moilioncircle.redis.cluster.watchdog.ClusterNodeInfo.valueOf;
import static com.moilioncircle.redis.cluster.watchdog.Version.PROTOCOL_V1;
import static com.moilioncircle.redis.cluster.watchdog.manager.ClusterConfigManager.representClusterNodeFlags;
import static com.moilioncircle.redis.cluster.watchdog.manager.ClusterSlotManager.bitmapTestBit;
import static com.moilioncircle.redis.cluster.watchdog.state.NodeStates.nodeFailed;
import static com.moilioncircle.redis.cluster.watchdog.state.NodeStates.nodeHasAddr;
import static com.moilioncircle.redis.cluster.watchdog.state.NodeStates.nodeInHandshake;
import static com.moilioncircle.redis.cluster.watchdog.state.NodeStates.nodeIsMaster;
import static com.moilioncircle.redis.cluster.watchdog.state.NodeStates.nodeIsSlave;
import static com.moilioncircle.redis.cluster.watchdog.state.NodeStates.nodePFailed;
import static java.lang.Math.max;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public abstract class AbstractClusterMessageHandler implements ClusterMessageHandler {
    
    private static final Log logger = LogFactory.getLog(AbstractClusterMessageHandler.class);
    
    protected ServerState server;
    protected ClusterManagers managers;
    protected ClusterConfiguration configuration;
    
    public AbstractClusterMessageHandler(ClusterManagers managers) {
        this.managers = managers;
        this.server = managers.server;
        this.configuration = managers.configuration;
    }
    
    public abstract boolean handle(ClusterNode sender, ClusterLink link, ClusterMessage hdr);
    
    @Override
    public boolean handle(ClusterLink link, ClusterMessage hdr) {
        if (hdr.type < CLUSTERMSG_TYPE_COUNT)
            server.cluster.messagesReceived[hdr.type]++;
        if (hdr.version != configuration.getVersion()) return true; //如果redis版本号（配置不变的）不同丢弃
        ClusterNode sender = managers.nodes.clusterLookupNode(hdr.name);
        if (sender != null && !nodeInHandshake(sender)) {
            sender.configEpoch = max(hdr.configEpoch, sender.configEpoch);
            server.cluster.currentEpoch = max(hdr.currentEpoch, server.cluster.currentEpoch);
        }
        handle(sender, link, hdr);
        managers.states.clusterUpdateState();
        return true;
    }

    /* This function is called when we receive a master configuration via a
     * PING, PONG or UPDATE packet. What we receive is a node, a configEpoch of the
     * node, and the set of slots claimed under this configEpoch.
     *
     * What we do is to rebind the slots with newer configuration compared to our
     * local configuration, and if needed, we turn ourself into a replica of the
     * node (see the function comments for more info).
     *
     * The 'sender' is the node for which we received a configuration update.
     * Sometimes it is not actually the "Sender" of the information, like in the
     * case we receive the info via an UPDATE packet. */
    public void clusterUpdateSlotsConfigWith(ClusterNode sender, long senderConfigEpoch, byte[] slots) {
        ClusterNode myself = server.myself;
        ClusterNode previous = nodeIsMaster(myself) ? myself : myself.master;
        if (Objects.equals(sender, myself)) {
            logger.info("Discarding UPDATE message fail myself.");
            return;
        }
        
        ClusterNode next = null;
        List<Integer> dirties = new ArrayList<>();
        for (int i = 0; i < CLUSTER_SLOTS; i++) {
            ClusterNode n = server.cluster.slots[i];
            if (!bitmapTestBit(slots, i)) continue;
            if (Objects.equals(n, sender)) continue;
            if (server.cluster.importing[i] != null) continue;
            /* We rebind the slot to the new node claiming it if:
             * 1) The slot was unassigned or the new node claims it with a
             *    greater configEpoch.
             * 2) We are not currently importing the slot. */ // WHZ 这里比较版本，并更新 slot 信息
            if (n == null || n.configEpoch < senderConfigEpoch) {
                ClusterSlotManager sm = managers.slots;
                if (Objects.equals(n, previous)) next = sender;
                if (Objects.equals(n, myself) && sm.countKeysInSlot(i) > 0) dirties.add(i);
                managers.slots.clusterDelSlot(i);
                managers.slots.clusterAddSlot(sender, i);
            }
        }
        if (next != null && previous.assignedSlots == 0) managers.nodes.clusterSetMyMasterTo(sender);
        else if (!dirties.isEmpty()) dirties.stream().forEach(slot -> managers.slots.delKeysInSlot(slot));
    }

    /* Process the gossip section of PING or PONG packets.
     * Note that this function assumes that the packet is already sanity-checked
     * by the caller, not in the content of the gossip section, but in the
     * length. */
    public void clusterProcessGossipSection(ClusterMessage hdr, ClusterLink link) {
        List<ClusterMessageDataGossip> gossips = hdr.data.gossips;
        ClusterNode sender = link.node != null ? link.node : managers.nodes.clusterLookupNode(hdr.name);
        for (ClusterMessageDataGossip gossip : gossips) {
            if (logger.isDebugEnabled()) {
                logger.debug("GOSSIP " + gossip.name + " " + gossip.ip + ":" + gossip.port + "@" + gossip.busPort + " " + representClusterNodeFlags(gossip.flags));
            }
            
            ClusterNode node = managers.nodes.clusterLookupNode(gossip.name);
            
            if (node == null) {
                if (sender != null && nodeHasAddr(gossip.flags)
                        && !managers.blacklists.clusterBlacklistExists(gossip.name)) {
                    managers.nodes.clusterStartHandshake(gossip.ip, gossip.port, gossip.busPort);
                }
                continue;
            }
            /* Update our state accordingly to the gossip sections */
            /* We already know this node.
               Handle failure reports, only when the sender is a master. */
            if (sender != null && nodeIsMaster(sender) && !Objects.equals(node, server.myself)) {
                if (nodePFailed(gossip.flags) || nodeFailed(gossip.flags)) {
                    if (managers.nodes.clusterNodeAddFailureReport(node, sender) && configuration.isVerbose()) {
                        logger.info("Node " + sender.name + " reported node " + node.name + " as not reachable.");
                    }
                    markNodeAsFailingIfNeeded(node); // WHZ 更新状态为 FAIL
                } else if (managers.nodes.clusterNodeDelFailureReport(node, sender) && configuration.isVerbose()) {
                    logger.info("Node " + sender.name + " reported node " + node.name + " is back online.");
                }
            }
            
            if (configuration.getVersion() == PROTOCOL_V1
                    && !nodePFailed(gossip.flags) && !nodeFailed(gossip.flags)
                    && node.pingTime == 0 && managers.nodes.clusterNodeFailureReportsCount(node) == 0
                    && gossip.pongTime <= (System.currentTimeMillis() + 500) && gossip.pongTime > node.pongTime) {
                node.pongTime = gossip.pongTime;
            }

            /* If we already know this node, but it is not reachable, and
             * we see a different address in the gossip section of a node that
             * can talk with this other node, update the address, disconnect
             * the old link if any, so that we'll attempt to connect with the
             * new address. */
            if ((nodePFailed(node.flags) || nodeFailed(node.flags))
                    && nodeHasAddr(gossip.flags) && !nodePFailed(gossip.flags) && !nodeFailed(gossip.flags)
                    && (!node.ip.equalsIgnoreCase(gossip.ip) || node.port != gossip.port || node.busPort != gossip.busPort)) {
                
                if (node.link != null) managers.connections.freeClusterLink(node.link);
                node.ip = gossip.ip;
                node.port = gossip.port;
                node.busPort = gossip.busPort;
                node.flags &= ~CLUSTER_NODE_NOADDR; //WHZ 将 NOADDR 位置1
            }
        }
    }
    
    public boolean nodeUpdateAddressIfNeeded(ClusterNode node, ClusterLink link, ClusterMessage hdr) {
        if (link.equals(node.link)) return false;
        String ip = link.fd.getRemoteAddress(hdr.ip);
        if (node.port == hdr.port && node.busPort == hdr.busPort && ip.equalsIgnoreCase(node.ip)) return false;
        node.ip = ip;
        node.port = hdr.port;
        node.busPort = hdr.busPort;
        if (node.link != null) managers.connections.freeClusterLink(node.link);
        logger.info("Address updated for node " + node.name + ", now " + node.ip + ":" + node.port);
        if (nodeIsSlave(server.myself) && Objects.equals(server.myself.master, node)) {
            managers.replications.replicationSetMaster(node);
        }
        return true;
    }
    
    public void markNodeAsFailingIfNeeded(ClusterNode node) {
        int quorum = server.cluster.size / 2 + 1;
        if (!nodePFailed(node) || nodeFailed(node)) return;
        int failures = managers.nodes.clusterNodeFailureReportsCount(node);
        if (nodeIsMaster(server.myself)) failures++;  // WHZ 大部分 master 节点认为该节点挂了，才认为挂
        if (failures < quorum) return;
        logger.info("Marking node " + node.name + " as failing (quorum reached).");
        //
        long now = System.currentTimeMillis();
        node.flags &= ~CLUSTER_NODE_PFAIL;
        node.flags |= CLUSTER_NODE_FAIL;
        node.failTime = now;
        managers.notifyNodeFailed(valueOf(node, server.myself));
        if (nodeIsMaster(server.myself)) managers.messages.clusterSendFail(node.name);
    }
    
    public void clusterHandleConfigEpochCollision(ClusterNode sender) {
        ClusterNode myself = server.myself;
        long configEpoch = myself.configEpoch;
        if (sender.configEpoch != configEpoch) return;
        if (nodeIsSlave(sender) || nodeIsSlave(myself)) return;
        if (sender.name.compareTo(server.myself.name) <= 0) return;
        server.cluster.currentEpoch++;
        myself.configEpoch = configEpoch = server.cluster.currentEpoch;
        logger.info("WARNING: configEpoch collision with node " + sender.name + ". configEpoch set to " + configEpoch);
    }
}
