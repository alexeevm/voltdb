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
package org.voltcore.agreement;

import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.zookeeper_voltpatches.CreateMode;
import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.WatchedEvent;
import org.apache.zookeeper_voltpatches.Watcher;
import org.apache.zookeeper_voltpatches.ZooDefs.Ids;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.voltcore.utils.MiscUtils;
import org.voltdb.VoltDB;

public class LeaderElector {
    private final ZooKeeper zk;
    private final String dir;
    private final String prefix;
    private final byte[] data;
    private final LeaderNoticeHandler cb;
    private String node = null;

    private volatile String leader = null;
    private volatile boolean isLeader = false;
    private final ExecutorService es;

    private final Runnable eventHandler = new Runnable() {
        @Override
        public void run() {
            try {
                leader = watchNextLowerNode();
            } catch (Exception e) {
                VoltDB.crashLocalVoltDB("Failed to get leader", false, e);
            }

            if (node.equals(leader)) {
                // become the leader
                isLeader = true;
                if (cb != null) {
                    cb.becomeLeader();
                }
            }
        }
    };

    private final Watcher watcher = new Watcher() {
        @Override
        public void process(WatchedEvent event) {
            es.submit(eventHandler);
        }
    };

    public LeaderElector(ZooKeeper zk, String dir, String prefix, byte[] data,
                         LeaderNoticeHandler cb) {
        this.zk = zk;
        this.dir = dir;
        this.prefix = prefix;
        this.data = data;
        this.cb = cb;
        es = Executors.newSingleThreadExecutor(MiscUtils.getThreadFactory("Leader elector-" + dir));
    }

    /**
     * Start leader election.
     *
     * Creates an ephemeral sequential node under the given directory and check
     * if we are the first one who created it.
     *
     * For details about the leader election algorithm, @see <a href=
     * "http://zookeeper.apache.org/doc/trunk/recipes.html#sc_leaderElection"
     * >Zookeeper Leader Election</a>
     *
     * @throws Exception
     */
    public void start() throws Exception {
        node = zk.create(ZKUtil.joinZKPath(dir, prefix + "_"), data,
                         Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        Future<?> task = es.submit(eventHandler);
        task.get();
    }

    public boolean isLeader() {
        return isLeader;
    }

    /**
     * Get the current leader node
     * @return
     */
    public String leader() {
        return leader;
    }

    public String getNode() {
        return node;
    }

    /**
     * Deletes the ephemeral node.
     *
     * @throws InterruptedException
     * @throws KeeperException
     */
    public void done() throws InterruptedException, KeeperException {
        zk.delete(node, -1);
    }

    /**
     * Set a watch on the node that comes before the specified node in the
     * directory.

     * @return The lowest sequential node
     * @throws Exception
     */
    private String watchNextLowerNode() throws Exception {
        /*
         * Iterate through the sorted list of children and find the given node,
         * then setup a watcher on the previous node if it exists, otherwise the
         * previous of the previous...until we reach the beginning, then we are
         * the lowest node.
         */
        List<String> children = zk.getChildren(dir, false);
        ZKUtil.sortSequentialNodes(children);
        String lowest = null;
        String previous = null;
        ListIterator<String> iter = children.listIterator();
        while (iter.hasNext()) {
            String child = ZKUtil.joinZKPath(dir, iter.next());
            if (lowest == null) {
                lowest = child;
                previous = child;
                continue;
            }

            if (child.equals(node)) {
                while (zk.exists(previous, watcher) == null) {
                    if (previous.equals(lowest)) {
                        /*
                         * If the leader disappeared, and we follow the leader, we
                         * become the leader now
                         */
                        lowest = child;
                        break;
                    } else {
                        // reverse the direction of iteration
                        previous = iter.previous();
                    }
                }
                break;
            }
            previous = child;
        }

        return lowest;
    }
}
