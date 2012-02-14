/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.dtxn;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.zookeeper_voltpatches.WatchedEvent;
import org.apache.zookeeper_voltpatches.Watcher;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.voltcore.agreement.ZKUtil;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.MiscUtils;

import org.voltdb.VoltZK;

public class MailboxTracker {
    private static final VoltLogger log = new VoltLogger("HOST");

    private final ZooKeeper m_zk;

    private final boolean m_isLeader;
    private volatile Map<Integer, ArrayList<Long>> m_hostsToSites =
            new HashMap<Integer, ArrayList<Long>>();
    private volatile Map<Integer, ArrayList<Long>> m_partitionsToSites =
            new HashMap<Integer, ArrayList<Long>>();
    private volatile Map<Long, Integer> m_sitesToPartitions =
            new HashMap<Long, Integer>();
    private volatile Map<Integer, Long> m_hostsToPlanners =
            new HashMap<Integer, Long>();
    private volatile Map<Integer, ArrayList<Long>> m_hostsToInitiators =
            new HashMap<Integer, ArrayList<Long>>();
    private volatile Map<Integer, Long> m_partitionToPrimaryInitiator =
            new HashMap<Integer, Long>();

    public MailboxTracker(ZooKeeper zk, int hostId) throws Exception {
        m_zk = zk;

        m_isLeader = (getAndWatchSites() == hostId);
        getAndWatchPlanners();
        getAndWatchInitiators();
    }

    /**
     * @return the host ID of the first host that registered the mailboxes
     * @throws Exception
     */
    private int getAndWatchSites() throws Exception {
        List<String> children = m_zk.getChildren(VoltZK.mailboxes_executionsites, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                try {
                    getAndWatchSites();
                } catch (Exception e) {
                    log.error(e.getMessage());
                }
            }
        });
        ZKUtil.sortSequentialNodes(children);

        log.info("Mailboxtracker getAndWatchSites() triggered.");

        int firstHostId = -1;
        Map<Integer, ArrayList<Long>> hostsToSites = new HashMap<Integer, ArrayList<Long>>();
        Map<Integer, ArrayList<Long>> partitionsToSites = new HashMap<Integer, ArrayList<Long>>();
        Map<Long, Integer> sitesToPartitions = new HashMap<Long, Integer>();
        for (String child : children) {
            byte[] data = m_zk.getData(VoltZK.mailboxes_executionsites + "/" + child, false, null);
            JSONObject jsObj = new JSONObject(new String(data, "UTF-8"));

            log.info("Mailboxtracker getAndWatchSites processing: " + jsObj.toString(2));

            try {
                long HSId = jsObj.getLong("HSId");
                int partitionId = jsObj.getInt("partitionId");
                int hostId = MiscUtils.getHostIdFromHSId(HSId);

                if (firstHostId == -1) {
                    firstHostId = hostId;
                }

                ArrayList<Long> hostSiteList = hostsToSites.get(hostId);
                if (hostSiteList == null)
                {
                    hostSiteList = new ArrayList<Long>();
                    hostsToSites.put(hostId, hostSiteList);
                }
                hostSiteList.add(HSId);

                ArrayList<Long> partSiteList = partitionsToSites.get(partitionId);
                if (partSiteList == null) {
                    partSiteList = new ArrayList<Long>();
                    partitionsToSites.put(partitionId, partSiteList);
                }
                partSiteList.add(HSId);

                sitesToPartitions.put(HSId, partitionId);
            } catch (JSONException e) {
                log.error(e.getMessage());
            }
        }

        m_hostsToSites = hostsToSites;
        m_partitionsToSites = partitionsToSites;
        m_sitesToPartitions = sitesToPartitions;

        return firstHostId;
    }

    private void getAndWatchPlanners() throws Exception {
        List<String> children = m_zk.getChildren(VoltZK.mailboxes_asyncplanners, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                try {
                    getAndWatchPlanners();
                } catch (Exception e) {
                    log.error(e.getMessage());
                }
            }
        });

        Map<Integer, Long> hostsToPlanners = new HashMap<Integer, Long>();
        for (String child : children) {
            byte[] data = m_zk.getData(VoltZK.mailboxes_asyncplanners + "/" + child, false, null);
            JSONObject jsObj = new JSONObject(new String(data, "UTF-8"));
            try {
                long HSId = jsObj.getLong("HSId");
                hostsToPlanners.put(MiscUtils.getHostIdFromHSId(HSId), HSId);
            } catch (JSONException e) {
                log.error(e.getMessage());
            }
        }

        m_hostsToPlanners = hostsToPlanners;
    }

    private void getAndWatchInitiators() throws Exception {
        List<String> children = m_zk.getChildren(VoltZK.mailboxes_initiators, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                try {
                    getAndWatchInitiators();
                } catch (Exception e) {
                    log.error(e.getMessage());
                }
            }
        });

        // First map the initiators to hosts
        Map<Integer, ArrayList<Long>> hostsToInitiators = new HashMap<Integer, ArrayList<Long>>();
        for (String child : children) {
            byte[] data = m_zk.getData(VoltZK.mailboxes_initiators + "/" + child, false, null);
            JSONObject jsObj = new JSONObject(new String(data, "UTF-8"));
            try {
                long HSId = jsObj.getLong("HSId");
                int hostId = MiscUtils.getHostIdFromHSId(HSId);

                ArrayList<Long> initiators = hostsToInitiators.get(hostId);
                if (initiators == null) {
                    initiators = new ArrayList<Long>();
                    hostsToInitiators.put(hostId, initiators);
                }
                initiators.add(HSId);
                // TODO: needs to determine if it's the master or replica
            } catch (JSONException e) {
                log.error(e.getMessage());
            }
        }

        m_hostsToInitiators = hostsToInitiators;


        // Then map the partitions to primary initiator. This probably
        // moves to a separate leader election zk node once k-safety
        // tests have to pass. Thus it is written independently from
        // the above loop.
        for (String child : children) {
            byte[] data = m_zk.getData(
                    VoltZK.path(VoltZK.mailboxes_initiators, child),
                    false,
                    null);
            JSONObject jsObj= new JSONObject(new String(data, "UTF-8"));
            try {
                long HSId = jsObj.getLong("HSId");
                int partitionId = jsObj.getInt("partitionId");
                m_partitionToPrimaryInitiator.put(partitionId, HSId);
            }
            catch (JSONException e) {
                log.error(e.getMessage());
            }
        }

    }

    public static int getHostForHSId(long HSId) {
        return MiscUtils.getHostIdFromHSId(HSId);
    }

    public List<Long> getSitesForHost(int hostId) {
        return m_hostsToSites.get(hostId);
    }

    public List<Long> getSitesForPartition(int partitionId) {
        return m_partitionsToSites.get(partitionId);
    }

    public Integer getPartitionForSite(long hsId) {
        return m_sitesToPartitions.get(hsId);
    }

    public Long getPlannerForHost(int hostId) {
        return m_hostsToPlanners.get(hostId);
    }

    public List<Long> getInitiatorForHost(int hostId) {
        return m_hostsToInitiators.get(hostId);
    }

    public Set<Integer> getAllHosts() {
        HashSet<Integer> hosts = new HashSet<Integer>();
        hosts.addAll(m_hostsToSites.keySet());
        return hosts;
    }

    public Set<Long> getAllSites() {
        HashSet<Long> sites = new HashSet<Long>();
        for (Collection<Long> values : m_hostsToSites.values()) {
            sites.addAll(values);
        }
        return sites;
    }

    public Set<Long> getAllInitiators() {
        HashSet<Long> initiators = new HashSet<Long>();
        for (Collection<Long> values : m_hostsToInitiators.values()) {
            initiators.addAll(values);
        }
        return initiators;
    }

    public boolean isLeader() {
        return m_isLeader;
    }
}
