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

package org.voltdb;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;

import java.lang.Exception;
import java.lang.management.ManagementFactory;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.zookeeper_voltpatches.CreateMode;
import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.ZooDefs.Ids;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltcore.agreement.ZKUtil;

import org.voltcore.messaging.HostMessenger;
import org.voltcore.messaging.Mailbox;
import org.voltdb.iv2.InitiatorMailbox;
import org.voltdb.VoltDB.START_ACTION;
import org.voltdb.VoltZK.MailboxType;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Database;
import org.voltdb.compiler.AsyncCompilerAgent;
import org.voltdb.compiler.ClusterConfig;
import org.voltdb.compiler.deploymentfile.DeploymentType;
import org.voltdb.compiler.deploymentfile.HeartbeatType;
import org.voltdb.compiler.deploymentfile.UsersType;
import org.voltdb.dtxn.DtxnInitiatorMailbox;
import org.voltdb.dtxn.ExecutorTxnIdSafetyState;
import org.voltdb.dtxn.MailboxTracker;
import org.voltdb.dtxn.MailboxUpdateHandler;
import org.voltdb.dtxn.SimpleDtxnInitiator;
import org.voltdb.dtxn.SiteTracker;
import org.voltdb.dtxn.TransactionInitiator;
import org.voltdb.export.ExportManager;
import org.voltdb.fault.FaultDistributor;
import org.voltdb.fault.FaultDistributorInterface;
import org.voltdb.licensetool.LicenseApi;
import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;

import org.voltdb.messaging.VoltDbMessageFactory;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.HTTPAdminListener;
import org.voltdb.utils.LogKeys;
import org.voltdb.utils.MiscUtils;
import org.voltdb.utils.PlatformProperties;
import org.voltdb.utils.ResponseSampler;
import org.voltdb.utils.SystemStatsCollector;
import org.voltdb.utils.VoltSampler;

/**
 * RealVoltDB initializes global server components, like the messaging
 * layer, ExecutionSite(s), and ClientInterface. It provides accessors
 * or references to those global objects. It is basically the global
 * namespace. A lot of the global namespace is described by VoltDBInterface
 * to allow test mocking.
 */
public class RealVoltDB implements VoltDBInterface, RestoreAgent.Callback, MailboxUpdateHandler
{
    private static final VoltLogger log = new VoltLogger(VoltDB.class.getName());
    private static final VoltLogger hostLog = new VoltLogger("HOST");


    public VoltDB.Configuration m_config = new VoltDB.Configuration();
    CatalogContext m_catalogContext;
    volatile SiteTracker m_siteTracker;
    MailboxTracker m_mailboxTracker;
    private String m_buildString;
    private static final String m_defaultVersionString = "2.2.1";
    private String m_versionString = m_defaultVersionString;
    HostMessenger m_messenger = null;
    final ArrayList<ClientInterface> m_clientInterfaces = new ArrayList<ClientInterface>();
    final ArrayList<SimpleDtxnInitiator> m_dtxns = new ArrayList<SimpleDtxnInitiator>();
    private Map<Long, ExecutionSite> m_localSites;
    HTTPAdminListener m_adminListener;
    private Map<Long, Thread> m_siteThreads;
    private ArrayList<ExecutionSiteRunner> m_runners;
    private ExecutionSite m_currentThreadSite;
    private StatsAgent m_statsAgent = new StatsAgent();
    private AsyncCompilerAgent m_asyncCompilerAgent = new AsyncCompilerAgent();
    public AsyncCompilerAgent getAsyncCompilerAgent() { return m_asyncCompilerAgent; }
    FaultDistributor m_faultManager;
    private PartitionCountStats m_partitionCountStats = null;
    private IOStats m_ioStats = null;
    private MemoryStats m_memoryStats = null;
    private StatsManager m_statsManager = null;
    private SnapshotCompletionMonitor m_snapshotCompletionMonitor;
    int m_myHostId;
    long m_depCRC = -1;
    String m_serializedCatalog;
    String m_httpPortExtraLogMessage = null;
    boolean m_jsonEnabled;
    DeploymentType m_deployment;
    final HashSet<Integer> m_downHosts = new HashSet<Integer>();
    final Set<Integer> m_downNonExecSites = new HashSet<Integer>();
    //For command log only, will also mark self as faulted
    final Set<Long> m_downSites = new HashSet<Long>();

    // Should the execution sites be started in recovery mode
    // (used for joining a node to an existing cluster)
    // If CL is enabled this will be set to true
    // by the CL when the truncation snapshot completes
    // and this node is viable for replay
    volatile boolean m_recovering = false;
    boolean m_replicationActive = false;

    //Only restrict recovery completion during test
    static Semaphore m_testBlockRecoveryCompletion = new Semaphore(Integer.MAX_VALUE);
    private boolean m_executionSitesRecovered = false;
    private boolean m_agreementSiteRecovered = false;
    private long m_executionSiteRecoveryFinish;
    private long m_executionSiteRecoveryTransferred;

    // id of the leader, or the host restore planner says has the catalog
    int m_hostIdWithStartupCatalog;
    String m_pathToStartupCatalog;

    // Synchronize initialize and shutdown.
    private final Object m_startAndStopLock = new Object();

    // Synchronize updates of catalog contexts with context accessors.
    private final Object m_catalogUpdateLock = new Object();

    // add a random number to the sampler output to make it likely to be unique for this process.
    private final VoltSampler m_sampler = new VoltSampler(10, "sample" + String.valueOf(new Random().nextInt() % 10000) + ".txt");
    private final AtomicBoolean m_hasStartedSampler = new AtomicBoolean(false);

    final VoltDBNodeFailureFaultHandler m_faultHandler = new VoltDBNodeFailureFaultHandler(this);

    RestoreAgent m_restoreAgent = null;

    private volatile boolean m_isRunning = false;

    @Override
    public boolean recovering() { return m_recovering; }

    private final long m_recoveryStartTime = System.currentTimeMillis();

    CommandLog m_commandLog;

    private volatile OperationMode m_mode = OperationMode.INITIALIZING;
    private OperationMode m_startMode = null;
    private ReplicationRole m_replicationRole = null;

    volatile String m_localMetadata = "";
    final Map<Integer, String> m_clusterMetadata = Collections.synchronizedMap(new HashMap<Integer, String>());

    private ExecutorService m_computationService;

    // methods accessed via the singleton
    @Override
    public void startSampler() {
        if (m_hasStartedSampler.compareAndSet(false, true)) {
            m_sampler.start();
        }
    }

    HeartbeatThread heartbeatThread;
    private ScheduledThreadPoolExecutor m_periodicWorkThread;

    // The configured license api: use to decide enterprise/cvommunity edition feature enablement
    LicenseApi m_licenseApi;
    LicenseApi getLicenseApi() { return m_licenseApi; }


    /**
     * Initialize all the global components, then initialize all the m_sites.
     */
    @Override
    public void initialize(VoltDB.Configuration config) {
        synchronized(m_startAndStopLock) {
            hostLog.l7dlog( Level.INFO, LogKeys.host_VoltDB_StartupString.name(), null);

            // set the mode first thing
            m_mode = OperationMode.INITIALIZING;
            m_config = config;

            // set a bunch of things to null/empty/new for tests
            // which reusue the process
            m_clientInterfaces.clear();
            m_dtxns.clear();
            m_adminListener = null;
            m_commandLog = new DummyCommandLog();
            m_deployment = null;
            m_messenger = null;
            m_statsAgent = new StatsAgent();
            m_asyncCompilerAgent = new AsyncCompilerAgent();
            m_faultManager = null;
            m_snapshotCompletionMonitor = null;
            m_catalogContext = null;
            m_partitionCountStats = null;
            m_ioStats = null;
            m_memoryStats = null;
            m_statsManager = null;
            m_restoreAgent = null;
            m_hostIdWithStartupCatalog = 0;
            m_pathToStartupCatalog = m_config.m_pathToCatalog;

            m_computationService = Executors.newFixedThreadPool(
                    Runtime.getRuntime().availableProcessors(),
                    new ThreadFactory() {
                        private int threadIndex = 0;
                        @Override
                        public synchronized Thread  newThread(Runnable r) {
                            Thread t = new Thread(null, r, "Computation service thread - " + threadIndex++, 131072);
                            t.setDaemon(true);
                            return t;
                        }

                    });

            // determine if this is a rejoining node
            // (used for license check and later the actual rejoin)
            boolean isRejoin = config.m_rejoinToHostAndPort != null;

            // Set std-out/err to use the UTF-8 encoding and fail if UTF-8 isn't supported
            try {
                System.setOut(new PrintStream(System.out, true, "UTF-8"));
                System.setErr(new PrintStream(System.err, true, "UTF-8"));
            } catch (UnsupportedEncodingException e) {
                hostLog.fatal("Support for the UTF-8 encoding is required for VoltDB. This means you are likely running an unsupported JVM. Exiting.");
                System.exit(-1);
            }

            // check that this is a 64 bit VM
            if (System.getProperty("java.vm.name").contains("64") == false) {
                hostLog.fatal("You are running on an unsupported (probably 32 bit) JVM. Exiting.");
                System.exit(-1);
            }

            m_snapshotCompletionMonitor = new SnapshotCompletionMonitor();

            readBuildInfo(config.m_isEnterprise ? "Enterprise Edition" : "Community Edition");

            // start up the response sampler if asked to by setting the env var
            // VOLTDB_RESPONSE_SAMPLE_PATH to a valid path
            ResponseSampler.initializeIfEnabled();

            readDeploymentAndCreateStarterCatalogContext();

            // Create the thread pool here. It's needed by buildClusterMesh()
            final int availableProcessors = Runtime.getRuntime().availableProcessors();
            int poolSize = 1;
            if (availableProcessors > 4) {
                poolSize = 2;
            }
            m_periodicWorkThread = MiscUtils.getScheduledThreadPoolExecutor("Periodic Work", poolSize, 1024 * 128);
            buildClusterMesh(isRejoin);

            m_licenseApi = MiscUtils.licenseApiFactory(m_config.m_pathToLicense);

            JSONObject topoJson = createTopologyBlob();

            Deque<Mailbox> siteMailboxes = null;
            DtxnInitiatorMailbox initiatorMailbox = null;
            m_faultManager = new FaultDistributor(this);
            try {
                siteMailboxes = createMailboxesForSites(topoJson);
                m_mailboxTracker = new MailboxTracker(m_messenger.getZK(), this);
                m_mailboxTracker.start();
                createMailboxesForInitiators(topoJson);
            } catch (Exception e) {
                VoltDB.crashLocalVoltDB(e.getMessage(), true, e);
            }

            // do the many init tasks in the Inits class
            Inits inits = new Inits(this, 1);
            inits.doInitializationWork();

            // set up site structure
            m_localSites = Collections.synchronizedMap(new HashMap<Long, ExecutionSite>());
            m_siteThreads = Collections.synchronizedMap(new HashMap<Long, Thread>());
            m_runners = new ArrayList<ExecutionSiteRunner>();

            if (config.m_backend.isIPC) {
                int eeCount = m_siteTracker.getLocalSites().length;
                if (config.m_ipcPorts.size() != eeCount) {
                    hostLog.fatal("Specified an IPC backend but only supplied " + config.m_ipcPorts.size() +
                            " backend ports when " + eeCount + " are required");
                    System.exit(-1);
                }
            }

            collectLocalNetworkMetadata();
            m_clusterMetadata.put(m_messenger.getHostId(), getLocalMetadata());

            /*
             * Create execution sites runners (and threads) for all exec sites except the first one.
             * This allows the sites to be set up in the thread that will end up running them.
             * Cache the first Site from the catalog and only do the setup once the other threads have been started.
             */
            Mailbox localThreadMailbox = siteMailboxes.poll();
            m_currentThreadSite = null;
            for (Mailbox mailbox : siteMailboxes) {
                long site = mailbox.getHSId();
                int sitesHostId = SiteTracker.getHostForSite(site);

                // start a local site
                if (sitesHostId == m_myHostId) {
                    ExecutionSiteRunner runner =
                            new ExecutionSiteRunner(mailbox,
                                                    m_catalogContext,
                                                    m_serializedCatalog,
                                                    m_recovering,
                                                    m_replicationActive,
                                                    m_downHosts,
                                                    hostLog);
                    m_runners.add(runner);
                    Thread runnerThread = new Thread(runner, "Site " + site);
                    runnerThread.start();
                    log.l7dlog(Level.TRACE, LogKeys.org_voltdb_VoltDB_CreatingThreadForSite.name(), new Object[] { site }, null);
                    m_siteThreads.put(site, runnerThread);
                }
            }

            /*
             * Now that the runners have been started and are doing setup of the other sites in parallel
             * this thread can set up its own execution site.
             */
            try {
                ExecutionSite siteObj =
                        new ExecutionSite(VoltDB.instance(),
                                          localThreadMailbox,
                                          m_serializedCatalog,
                                          null,
                                          m_recovering,
                                          m_replicationActive,
                                          m_downHosts,
                                          m_catalogContext.m_transactionId);
                m_localSites.put(localThreadMailbox.getHSId(), siteObj);
                m_currentThreadSite = siteObj;
            } catch (Exception e) {
                VoltDB.crashLocalVoltDB(e.getMessage(), true, e);
            }

            /*
             * Stop and wait for the runners to finish setting up and then put
             * the constructed ExecutionSites in the local site map.
             */
            for (ExecutionSiteRunner runner : m_runners) {
                synchronized (runner) {
                    if (!runner.m_isSiteCreated) {
                        try {
                            runner.wait();
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    m_localSites.put(runner.m_siteId, runner.m_siteObj);
                }
            }

            // Create the client interface
            int portOffset = 0;
            // TODO: fix
            //for (long site : m_siteTracker.getMailboxTracker().getAllInitiators()) {
            for (int i = 0; i < 1; i++) {
                // create DTXN and CI for each local non-EE site
                SimpleDtxnInitiator initiator =
                        new SimpleDtxnInitiator(initiatorMailbox,
                                                m_catalogContext,
                                                m_messenger,
                                                m_myHostId,
                                                m_myHostId, // fake initiator ID
                                                m_config.m_timestampTestingSalt);

                try {
                    ClientInterface ci =
                            ClientInterface.create(m_messenger,
                                                   m_catalogContext,
                                                   m_replicationRole,
                                                   initiator,
                                                   m_catalogContext.numberOfNodes,
                                                   config.m_port + portOffset,
                                                   config.m_adminPort + portOffset,
                                                   m_config.m_timestampTestingSalt);
                    m_clientInterfaces.add(ci);
                } catch (Exception e) {
                    VoltDB.crashLocalVoltDB(e.getMessage(), true, e);
                }
                portOffset += 2;
                m_dtxns.add(initiator);
            }

            m_partitionCountStats = new PartitionCountStats( m_catalogContext.numberOfPartitions);
            m_statsAgent.registerStatsSource(SysProcSelector.PARTITIONCOUNT,
                                             0, m_partitionCountStats);
            m_ioStats = new IOStats();
            m_statsAgent.registerStatsSource(SysProcSelector.IOSTATS,
                                             0, m_ioStats);
            m_memoryStats = new MemoryStats();
            m_statsAgent.registerStatsSource(SysProcSelector.MEMORY,
                                             0, m_memoryStats);
            // Create the statistics manager and register it to JMX registry
            m_statsManager = null;
            try {
                final Class<?> statsManagerClass =
                    Class.forName("org.voltdb.management.JMXStatsManager");
                m_statsManager = (StatsManager)statsManagerClass.newInstance();
                m_statsManager.initialize(new ArrayList<Long>(m_localSites.keySet()));
            } catch (Exception e) {}

            try {
                m_snapshotCompletionMonitor.init(m_messenger.getZK());
            } catch (Exception e) {
                hostLog.fatal("Error initializing snapshot completion monitor", e);
                VoltDB.crashLocalVoltDB("Error initializing snapshot completion monitor", true, e);
            }

            if (m_commandLog != null && isRejoin) {
                m_commandLog.initForRejoin(
                        m_catalogContext, Long.MIN_VALUE,
                        0,
                        // m_messenger.getDiscoveredFaultSequenceNumber(),
                        m_downSites);
            }

            try {
                m_messenger.waitForAllHostsToBeReady(m_deployment.getCluster().getHostcount());
            } catch (Exception e) {
                hostLog.fatal("Failed to announce ready state.");
                VoltDB.crashLocalVoltDB("Failed to announce ready state.", false, null);
            }

            heartbeatThread = new HeartbeatThread(m_clientInterfaces);
            heartbeatThread.start();
            schedulePeriodicWorks();

            // print out a bunch of useful system info
            logDebuggingInfo(m_config.m_adminPort, m_config.m_httpPort, m_httpPortExtraLogMessage, m_jsonEnabled);

            int k = m_catalogContext.numberOfExecSites / m_catalogContext.numberOfPartitions;
            if (k == 1) {
                hostLog.warn("Running without redundancy (k=0) is not recommended for production use.");
            }

            assert(m_clientInterfaces.size() > 0);
            ClientInterface ci = m_clientInterfaces.get(0);
            ci.initializeSnapshotDaemon(m_messenger.getZK());

            // set additional restore agent stuff
            TransactionInitiator initiator = m_dtxns.get(0);
            if (m_restoreAgent != null) {
                m_restoreAgent.setCatalogContext(m_catalogContext);
                m_restoreAgent.setInitiator(initiator);
            }
        }
    }

    private JSONObject createTopologyBlob()
    {
        int sitesperhost = m_deployment.getCluster().getSitesperhost();
        int hostcount = m_deployment.getCluster().getHostcount();
        int kfactor = m_deployment.getCluster().getKfactor();
        ClusterConfig clusterConfig = new ClusterConfig(hostcount, sitesperhost, kfactor);
        JSONObject topo = registerClusterConfig(clusterConfig);
        return topo;
    }

    private JSONObject registerClusterConfig(ClusterConfig config)
    {
        JSONObject topo = null;
        try {
            topo = config.getTopology(m_messenger.getLiveHostIds());
            byte[] payload = topo.toString(4).getBytes("UTF-8");
            m_messenger.getZK().create(VoltZK.topology, payload,
                                       Ids.OPEN_ACL_UNSAFE,
                                       CreateMode.PERSISTENT);
            byte[] data = m_messenger.getZK().getData(VoltZK.topology, false, null);
            topo = new JSONObject(new String(data, "UTF-8"));
        }
        catch (KeeperException.NodeExistsException nee) {
        }
        catch (Exception e) {
            VoltDB.crashLocalVoltDB("Unable to write topology to ZK, dying",
                                    true, e);
        }
        return topo;
    }

    private ArrayDeque<Mailbox> createMailboxesForSites(JSONObject topo) throws Exception
    {
        ArrayDeque<Mailbox> mailboxes = new ArrayDeque<Mailbox>();
        List<Integer> partitions =
            ClusterConfig.partitionsForHost(topo, m_messenger.getHostId());
        for (Integer partition : partitions) {
            Mailbox mailbox = m_messenger.createMailbox();
            mailboxes.add(mailbox);
            registerExecutionSiteMailbox(mailbox.getHSId(), partition);
        }
        return mailboxes;
    }

    private void registerExecutionSiteMailbox(long HSId, int partitionId) throws Exception
    {
        JSONObject jsObj = new JSONObject();
        jsObj.put("HSId", HSId);
        jsObj.put("partitionId", partitionId);
        byte[] payload = jsObj.toString(4).getBytes("UTF-8");
        m_messenger.getZK().create(VoltZK.mailboxes_executionsites_site, payload,
                                   Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
    }

    private Deque<Mailbox> createMailboxesForInitiators(JSONObject topo) throws Exception
    {
        Deque<Mailbox> mailboxes = new ArrayDeque<Mailbox>();
        List<Integer> partitions =
            ClusterConfig.partitionsForHost(topo, m_messenger.getHostId());
        for (Integer partition : partitions) {
            Mailbox mailbox = new InitiatorMailbox(m_messenger, partition);
            m_messenger.createMailbox(null, mailbox);
            mailboxes.add(mailbox);
            registerInitiatorMailbox(mailbox.getHSId(), partition);
        }
        return mailboxes;
    }

    private void registerInitiatorMailbox(long HSId, int partition) throws Exception {
        JSONObject jsObj = new JSONObject();
        jsObj.put("HSId", HSId);
        jsObj.put("partitionId", partition);
        byte[] payload = jsObj.toString(4).getBytes("UTF-8");
        m_messenger.getZK().create(VoltZK.mailboxes_initiators_initiator, payload,
                                   Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
    }

    /**
     * Schedule all the periodic works
     */
    private void schedulePeriodicWorks() {
        // JMX stats broadcast
        scheduleWork(new Runnable() {
            @Override
            public void run() {
                m_statsManager.sendNotification();
            }
        }, 0, StatsManager.POLL_INTERVAL, TimeUnit.MILLISECONDS);

        // small stats samples
        scheduleWork(new Runnable() {
            @Override
            public void run() {
                SystemStatsCollector.asyncSampleSystemNow(false, false);
            }
        }, 0, 5, TimeUnit.SECONDS);

        // medium stats samples
        scheduleWork(new Runnable() {
            @Override
            public void run() {
                SystemStatsCollector.asyncSampleSystemNow(true, false);
            }
        }, 0, 1, TimeUnit.MINUTES);

        // large stats samples
        scheduleWork(new Runnable() {
            @Override
            public void run() {
                SystemStatsCollector.asyncSampleSystemNow(true, true);
            }
        }, 0, 6, TimeUnit.MINUTES);
    }

    void readDeploymentAndCreateStarterCatalogContext() {
        m_deployment = CatalogUtil.parseDeployment(m_config.m_pathToDeployment);
        // wasn't a valid xml deployment file
        if (m_deployment == null) {
            hostLog.error("Not a valid XML deployment file at URL: " + m_config.m_pathToDeployment);
            VoltDB.crashLocalVoltDB("Not a valid XML deployment file at URL: "
                    + m_config.m_pathToDeployment, false, null);
        }

        // note the heatbeats are specified in seconds in xml, but ms internally
        HeartbeatType hbt = m_deployment.getHeartbeat();
        if (hbt != null)
            m_config.m_deadHostTimeoutMS = hbt.getTimeout() * 1000;

        // create a dummy catalog to load deployment info into
        Catalog catalog = new Catalog();
        Cluster cluster = catalog.getClusters().add("cluster");
        Database db = cluster.getDatabases().add("database");

        // create groups as needed for users
        if (m_deployment.getUsers() != null) {
            for (UsersType.User user : m_deployment.getUsers().getUser()) {
                String groupsCSV = user.getGroups();
                String[] groups = groupsCSV.split(",");
                for (String group : groups) {
                    if (db.getGroups().get(group) == null) {
                        db.getGroups().add(group);
                    }
                }
            }
        }

        long depCRC = CatalogUtil.compileDeploymentAndGetCRC(catalog, m_deployment,
                                                             true, true);
        assert(depCRC != -1);
        m_catalogContext = new CatalogContext(0, catalog, null, depCRC, 0, -1);
    }

    void collectLocalNetworkMetadata() {
        boolean threw = false;
        JSONStringer stringer = new JSONStringer();
        try {
            stringer.object();
            stringer.key("interfaces").array();

            /*
             * If no interface was specified, do a ton of work
             * to identify all ipv4 or ipv6 interfaces and
             * marshal them into JSON. Always put the ipv4 address first
             * so that the export client will use it
             */
            if (m_config.m_externalInterface.equals("")) {
                LinkedList<NetworkInterface> interfaces = new LinkedList<NetworkInterface>();
                try {
                    Enumeration<NetworkInterface> intfEnum = NetworkInterface.getNetworkInterfaces();
                    while (intfEnum.hasMoreElements()) {
                        NetworkInterface intf = intfEnum.nextElement();
                        if (intf.isLoopback() || !intf.isUp()) {
                            continue;
                        }
                        interfaces.offer(intf);
                    }
                } catch (SocketException e) {
                    throw new RuntimeException(e);
                }

                if (interfaces.isEmpty()) {
                    stringer.value("localhost");
                } else {

                    boolean addedIp = false;
                    while (!interfaces.isEmpty()) {
                        NetworkInterface intf = interfaces.poll();
                        Enumeration<InetAddress> inetAddrs = intf.getInetAddresses();
                        Inet6Address inet6addr = null;
                        Inet4Address inet4addr = null;
                        while (inetAddrs.hasMoreElements()) {
                            InetAddress addr = inetAddrs.nextElement();
                            if (addr instanceof Inet6Address) {
                                inet6addr = (Inet6Address)addr;
                                if (inet6addr.isLinkLocalAddress()) {
                                    inet6addr = null;
                                }
                            } else if (addr instanceof Inet4Address) {
                                inet4addr = (Inet4Address)addr;
                            }
                        }
                        if (inet4addr != null) {
                            stringer.value(inet4addr.getHostAddress());
                            addedIp = true;
                        }
                        if (inet6addr != null) {
                            stringer.value(inet6addr.getHostAddress());
                            addedIp = true;
                        }
                    }
                    if (!addedIp) {
                        stringer.value("localhost");
                    }
                }
            } else {
                stringer.value(m_config.m_externalInterface);
            }
        } catch (Exception e) {
            threw = true;
            hostLog.warn("Error while collecting data about local network interfaces", e);
        }
        try {
            if (threw) {
                stringer = new JSONStringer();
                stringer.object();
                stringer.key("interfaces").array();
                stringer.value("localhost");
                stringer.endArray();
            } else {
                stringer.endArray();
            }
            stringer.key("clientPort").value(m_config.m_port);
            stringer.key("adminPort").value(m_config.m_adminPort);
            stringer.key("httpPort").value(m_config.m_httpPort);
            stringer.key("drPort").value(m_config.m_drAgentPortStart);
            stringer.endObject();
            JSONObject obj = new JSONObject(stringer.toString());
            // possibly atomic swap from null to realz
            m_localMetadata = obj.toString(4);
        } catch (Exception e) {
            hostLog.warn("Failed to collect data about lcoal network interfaces", e);
        }
    }

    /**
     * Start the voltcore HostMessenger. This joins the node
     * to the existing cluster. In the non rejoin case, this
     * function will return when the mesh is complete. If
     * rejoining, it will return when the node and agreement
     * site are synched to the existing cluster.
     */
    void buildClusterMesh(boolean isRejoin) {
        int numberOfNodes = m_deployment.getCluster().getHostcount();
        if (numberOfNodes <= 0) {
            hostLog.l7dlog( Level.FATAL, LogKeys.host_VoltDB_InvalidHostCount.name(),
                    new Object[] { numberOfNodes }, null);
            VoltDB.crashLocalVoltDB("Invalid cluster size: " + numberOfNodes, false, null);
        }

        String leaderAddress = m_config.m_leader;
        InetAddress leader = null;
        try {
            leader = InetAddress.getByName(leaderAddress);
        } catch (UnknownHostException ex) {
            hostLog.l7dlog( Level.FATAL, LogKeys.host_VoltDB_CouldNotRetrieveLeaderAddress.name(),
                    new Object[] { leaderAddress }, null);
            VoltDB.crashLocalVoltDB("Failed to resolve leader address.", false, null);
        }

        org.voltcore.messaging.HostMessenger.Config hmconfig =
            new org.voltcore.messaging.HostMessenger.Config(
                leader.getHostAddress(),
                m_config.m_leaderPort != null ? m_config.m_leaderPort : m_config.m_internalPort);
        hmconfig.internalPort = m_config.m_internalPort;
        hmconfig.internalInterface = m_config.m_internalInterface;
        hmconfig.zkInterface = m_config.m_zkInterface;
        hmconfig.deadHostTimeout = m_config.m_deadHostTimeoutMS;
        hmconfig.factory = new VoltDbMessageFactory();
        m_messenger =
            new org.voltcore.messaging.HostMessenger(hmconfig);

        hostLog.l7dlog( Level.TRACE, LogKeys.host_VoltDB_CreatingVoltDB.name(), new Object[] { numberOfNodes, leader }, null);
        hostLog.info(String.format("Beginning inter-node communication on port %d.", m_config.m_internalPort));

        try {
            m_messenger.start();
        } catch (Exception e) {
            VoltDB.crashLocalVoltDB(e.getMessage(), true, e);
        }

        VoltZK.createPersistentZKNodes(m_messenger.getZK());

        if (!isRejoin) {
            m_messenger.waitForGroupJoin(numberOfNodes);
        }

        // Use the host messenger's hostId.
        m_myHostId = m_messenger.getHostId();
    }

    HashSet<Integer> rejoinExistingMesh(int numberOfNodes, long deploymentCRC) {
        throw new UnsupportedOperationException();
//        // sensible defaults (sorta)
//        String rejoinHostCredentialString = null;
//        String rejoinHostAddressString = null;
//
//        //Client interface port of node that will receive @Rejoin invocation
//        int rejoinPort = m_config.m_port;
//        String rejoinHost = null;
//        String rejoinUser = null;
//        String rejoinPass = null;
//
//        // this will cause the ExecutionSites to start in recovering mode
//        m_recovering = true;
//
//        // split a "user:pass@host:port" string into "user:pass" and "host:port"
//        int atSignIndex = m_config.m_rejoinToHostAndPort.indexOf('@');
//        if (atSignIndex == -1) {
//            rejoinHostAddressString = m_config.m_rejoinToHostAndPort;
//        }
//        else {
//            rejoinHostCredentialString = m_config.m_rejoinToHostAndPort.substring(0, atSignIndex).trim();
//            rejoinHostAddressString = m_config.m_rejoinToHostAndPort.substring(atSignIndex + 1).trim();
//        }
//
//        int colonIndex = -1;
//        // split a "user:pass" string into "user" and "pass"
//        if (rejoinHostCredentialString != null) {
//            colonIndex = rejoinHostCredentialString.indexOf(':');
//            if (colonIndex == -1) {
//                rejoinUser = rejoinHostCredentialString.trim();
//                System.out.print("password: ");
//                BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
//                try {
//                    rejoinPass = br.readLine();
//                } catch (IOException e) {
//                    hostLog.error("Unable to read passord for rejoining credentials from console.");
//                    System.exit(-1);
//                }
//            }
//            else {
//                rejoinUser = rejoinHostCredentialString.substring(0, colonIndex).trim();
//                rejoinPass = rejoinHostCredentialString.substring(colonIndex + 1).trim();
//            }
//        }
//
//        // split a "host:port" string into "host" and "port"
//        colonIndex = rejoinHostAddressString.indexOf(':');
//        if (colonIndex == -1) {
//            rejoinHost = rejoinHostAddressString.trim();
//            // note rejoinPort has a default
//        }
//        else {
//            rejoinHost = rejoinHostAddressString.substring(0, colonIndex).trim();
//            rejoinPort = Integer.parseInt(rejoinHostAddressString.substring(colonIndex + 1).trim());
//        }
//
//        hostLog.info(String.format("Inter-node communication will use port %d.", m_config.m_internalPort));
//        ServerSocketChannel listener = null;
//        try {
//            listener = ServerSocketChannel.open();
//            listener.socket().bind(new InetSocketAddress(m_config.m_internalPort));
//        } catch (IOException e) {
//            hostLog.error("Problem opening listening rejoin socket: " + e.getMessage());
//            System.exit(-1);
//        }
//        m_messenger = new HostMessenger(m_network, listener, numberOfNodes, 0, deploymentCRC, hostLog);
//
//        // make empty strings null
//        if ((rejoinUser != null) && (rejoinUser.length() == 0)) rejoinUser = null;
//        if ((rejoinPass != null) && (rejoinPass.length() == 0)) rejoinPass = null;
//
//        // URL Decode so usernames/passwords can contain weird stuff
//        try {
//            if (rejoinUser != null) rejoinUser = URLDecoder.decode(rejoinUser, "UTF-8");
//            if (rejoinPass != null) rejoinPass = URLDecoder.decode(rejoinPass, "UTF-8");
//        } catch (UnsupportedEncodingException e) {
//            hostLog.error("Problem URL-decoding credentials for rejoin authentication: " + e.getMessage());
//            System.exit(-1);
//        }
//
//        ClientConfig clientConfig = new ClientConfig(rejoinUser, rejoinPass);
//        Client client = ClientFactory.createClient(clientConfig);
//        ClientResponse response = null;
//        RejoinCallback rcb = new RejoinCallback() {
//
//        };
//        try {
//            client.createConnection(rejoinHost, rejoinPort);
//            InetSocketAddress inetsockaddr = new InetSocketAddress(rejoinHost, rejoinPort);
//            SocketChannel socket = SocketChannel.open(inetsockaddr);
//            String ip_addr = socket.socket().getLocalAddress().getHostAddress();
//            socket.close();
//            m_config.m_selectedRejoinInterface =
//                m_config.m_internalInterface.isEmpty() ? ip_addr : m_config.m_internalInterface;
//            client.callProcedure(
//                    rcb,
//                    "@Rejoin",
//                    m_config.m_selectedRejoinInterface,
//                    m_config.m_internalPort);
//        }
//        catch (Exception e) {
//            hostLog.fatal("Problem connecting client to " + m_config.m_selectedRejoinInterface + ":" +
//                   m_config.m_internalPort + ". " + e.getMessage());
//            VoltDB.crashVoltDB();
//        }
//
//        Object retval[] = m_messenger.waitForGroupJoin(60 * 1000);
//
//        m_instanceId = new Object[] { retval[0], retval[1] };
//
//        @SuppressWarnings("unchecked")
//        HashSet<Integer> downHosts = (HashSet<Integer>)retval[2];
//        hostLog.info("Down hosts are " + downHosts.toString());
//
//        try {
//            //Callback validates response asynchronously. Just wait for the response before continuing.
//            //Timeout because a failure might result in the response not coming.
//            response = rcb.waitForResponse(3000);
//            if (response == null) {
//                hostLog.fatal("Recovering node timed out rejoining");
//                VoltDB.crashVoltDB();
//            }
//        }
//        catch (InterruptedException e) {
//            hostLog.fatal("Interrupted while attempting to rejoin cluster");
//            VoltDB.crashVoltDB();
//        }
//        return downHosts;
    }

    void logDebuggingInfo(int adminPort, int httpPort, String httpPortExtraLogMessage, boolean jsonEnabled) {
        String startAction = m_config.m_startAction.toString();
        String startActionLog = "Database start action is " + (startAction.substring(0, 1).toUpperCase() +
                                 startAction.substring(1).toLowerCase()) + ".";
        if (m_config.m_startAction == START_ACTION.START) {
            startActionLog += " Will create a new database if there is nothing to recover from.";
        }
        hostLog.info(startActionLog);

        // print out awesome network stuff
        hostLog.info(String.format("Listening for native wire protocol clients on port %d.", m_config.m_port));
        hostLog.info(String.format("Listening for admin wire protocol clients on port %d.", adminPort));

        if (m_startMode == OperationMode.PAUSED) {
            hostLog.info(String.format("Started in admin mode. Clients on port %d will be rejected in admin mode.", m_config.m_port));
        }

        if (m_replicationRole == ReplicationRole.MASTER) {
                hostLog.info("Started as " + m_replicationRole.toString().toLowerCase() + " cluster.");
        } else if (m_replicationRole == ReplicationRole.REPLICA) {
            hostLog.info("Started as " + m_replicationRole.toString().toLowerCase() + " cluster. " +
                             "Clients can only call read-only procedures.");
        }
        if (httpPortExtraLogMessage != null)
            hostLog.info(httpPortExtraLogMessage);
        if (httpPort != -1) {
            hostLog.info(String.format("Local machine HTTP monitoring is listening on port %d.", httpPort));
        }
        else {
            hostLog.info(String.format("Local machine HTTP monitoring is disabled."));
        }
        if (jsonEnabled) {
            hostLog.info(String.format("Json API over HTTP enabled at path /api/1.0/, listening on port %d.", httpPort));
        }
        else {
            hostLog.info("Json API disabled.");
        }

        // replay command line args that we can see
        List<String> iargs = ManagementFactory.getRuntimeMXBean().getInputArguments();
        StringBuilder sb = new StringBuilder("Available JVM arguments:");
        for (String iarg : iargs)
            sb.append(" ").append(iarg);
        if (iargs.size() > 0) hostLog.info(sb.toString());
        else hostLog.info("No JVM command line args known.");

        // java heap size
        long javamaxheapmem = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax();
        javamaxheapmem /= (1024 * 1024);
        hostLog.info(String.format("Maximum usable Java heap set to %d mb.", javamaxheapmem));

        m_catalogContext.logDebuggingInfoFromCatalog();

        // print out a bunch of useful system info
        PlatformProperties pp = PlatformProperties.getPlatformProperties();
        String[] lines = pp.toLogLines().split("\n");
        for (String line : lines) {
            hostLog.info(line.trim());
        }

        final ZooKeeper zk = m_messenger.getZK();

        // Publish this node's metadata and then retrieve the metadata
        try {
            zk.create(
                    VoltZK.cluster_metadata + m_messenger.getHostId(),
                    getLocalMetadata().getBytes("UTF-8"),
                    Ids.OPEN_ACL_UNSAFE,
                    CreateMode.EPHEMERAL);
        } catch (Exception e) {
            VoltDB.crashLocalVoltDB("Error creating \"/cluster_metadata\" node in ZK", true, e);
        }

        /*
         * Spin and attempt to retrieve cluster metadata for all nodes in the cluster.
         */
        HashSet<Integer> metadataToRetrieve = new HashSet<Integer>(m_siteTracker.getAllHosts());
        metadataToRetrieve.remove(m_messenger.getHostId());
        while (!metadataToRetrieve.isEmpty()) {
            Map<Integer, ZKUtil.ByteArrayCallback> callbacks = new HashMap<Integer, ZKUtil.ByteArrayCallback>();
            for (Integer hostId : metadataToRetrieve) {
                ZKUtil.ByteArrayCallback cb = new ZKUtil.ByteArrayCallback();
                zk.getData(VoltZK.cluster_metadata + hostId, false, cb, null);
                callbacks.put(hostId, cb);
            }

            for (Map.Entry<Integer, ZKUtil.ByteArrayCallback> entry : callbacks.entrySet()) {
                try {
                    ZKUtil.ByteArrayCallback cb = entry.getValue();
                    Integer hostId = entry.getKey();
                    m_clusterMetadata.put(hostId, new String(cb.getData(), "UTF-8"));
                    metadataToRetrieve.remove(hostId);
                } catch (KeeperException.NoNodeException e) {}
                  catch (Exception e) {
                      VoltDB.crashLocalVoltDB("Error retrieving cluster metadata", true, e);
                }
            }

        }

        // print out cluster membership
        hostLog.info("About to list cluster interfaces for all nodes with format [ip1 ip2 ... ipN] client-port:admin-port:http-port");
        for (int hostId : m_siteTracker.getAllHosts()) {
            if (hostId == m_messenger.getHostId()) {
                hostLog.info(
                        String.format(
                                "  Host id: %d with interfaces: %s [SELF]",
                                hostId,
                                MiscUtils.formatHostMetadataFromJSON(getLocalMetadata())));
            }
            else {
                String hostMeta = m_clusterMetadata.get(hostId);
                hostLog.info(
                        String.format(
                                "  Host id: %d with interfaces: %s [PEER]",
                                hostId,
                                MiscUtils.formatHostMetadataFromJSON(hostMeta)));
            }
        }
    }


    public static String[] extractBuildInfo() {
        StringBuilder sb = new StringBuilder(64);
        String buildString = "VoltDB";
        String versionString = m_defaultVersionString;
        byte b = -1;
        try {
            InputStream buildstringStream =
                ClassLoader.getSystemResourceAsStream("buildstring.txt");
            if (buildstringStream == null) {
                throw new RuntimeException("Unreadable or missing buildstring.txt file.");
            }
            while ((b = (byte) buildstringStream.read()) != -1) {
                sb.append((char)b);
            }
            sb.append("\n");
            String parts[] = sb.toString().split(" ", 2);
            if (parts.length != 2) {
                throw new RuntimeException("Invalid buildstring.txt file.");
            }
            versionString = parts[0].trim();
            buildString = parts[1].trim();
        } catch (Exception ignored) {
            try {
                InputStream buildstringStream = new FileInputStream("version.txt");
                while ((b = (byte) buildstringStream.read()) != -1) {
                    sb.append((char)b);
                }
                versionString = sb.toString().trim();
            }
            catch (Exception ignored2) {
                log.l7dlog( Level.ERROR, LogKeys.org_voltdb_VoltDB_FailedToRetrieveBuildString.name(), null);
            }
        }
        return new String[] { versionString, buildString };
    }

    @Override
    public void readBuildInfo(String editionTag) {
        String buildInfo[] = extractBuildInfo();
        m_versionString = buildInfo[0];
        m_buildString = buildInfo[1];
        hostLog.info(String.format("Build: %s %s %s", m_versionString, m_buildString, editionTag));
    }

    /**
     * Start all the site's event loops. That's it.
     */
    @Override
    public void run() {
        // start the separate EE threads
        for (ExecutionSiteRunner r : m_runners) {
            synchronized (r) {
                assert(r.m_isSiteCreated) : "Site should already have been created by ExecutionSiteRunner";
                r.notifyAll();
            }
        }

        if (m_restoreAgent != null) {
            // start restore process
            m_restoreAgent.restore();
        }
        else {
            onRestoreCompletion(Long.MIN_VALUE);
        }

        // start one site in the current thread
        Thread.currentThread().setName("ExecutionSiteAndVoltDB");
        m_isRunning = true;
        try
        {
            m_currentThreadSite.run();
        }
        catch (Throwable t)
        {
            String errmsg = "ExecutionSite: " + m_currentThreadSite.m_siteId +
            " encountered an " +
            "unexpected error and will die, taking this VoltDB node down.";
            System.err.println(errmsg);
            t.printStackTrace();
            VoltDB.crashLocalVoltDB(errmsg, true, t);
        }
    }

    /**
     * Try to shut everything down so they system is ready to call
     * initialize again.
     * @param mainSiteThread The thread that m_inititalized the VoltDB or
     * null if called from that thread.
     */
    @Override
    public void shutdown(Thread mainSiteThread) throws InterruptedException {
        synchronized(m_startAndStopLock) {
            m_mode = OperationMode.SHUTTINGDOWN;
            m_executionSitesRecovered = false;
            m_agreementSiteRecovered = false;
            m_snapshotCompletionMonitor.shutdown();
            m_periodicWorkThread.shutdown();
            heartbeatThread.interrupt();
            heartbeatThread.join();
            m_mailboxTracker.shutdown();
            // Things are going pear-shaped, tell the fault distributor to
            // shut its fat mouth
            m_faultManager.shutDown();

            if (m_hasStartedSampler.get()) {
                m_sampler.setShouldStop();
                m_sampler.join();
            }

            // shutdown the web monitoring / json
            if (m_adminListener != null)
                m_adminListener.stop();

            // shut down the client interface
            for (ClientInterface ci : m_clientInterfaces) {
                ci.shutdown();
            }

            // shut down Export and its connectors.
            ExportManager.instance().shutdown();

            // tell all m_sites to stop their runloops
            if (m_localSites != null) {
                for (ExecutionSite site : m_localSites.values())
                    site.startShutdown();
            }

            // try to join all threads but the main one
            // probably want to check if one of these is the current thread
            if (m_siteThreads != null) {
                for (Thread siteThread : m_siteThreads.values()) {
                    if (Thread.currentThread().equals(siteThread) == false) {
                        // don't interrupt here. the site will start shutdown when
                        // it sees the shutdown flag set.
                        siteThread.join();
                    }
                }
            }

            // try to join the main thread (possibly this one)
            if (mainSiteThread != null) {
                if (Thread.currentThread().equals(mainSiteThread) == false) {
                    // don't interrupt here. the site will start shutdown when
                    // it sees the shutdown flag set.
                    mainSiteThread.join();
                }
            }

            // help the gc along
            m_localSites = null;
            m_currentThreadSite = null;
            m_siteThreads = null;
            m_runners = null;

            // shut down the network/messaging stuff
            // Close the host messenger first, which should close down all of
            // the ForeignHost sockets cleanly
            if (m_messenger != null)
            {
                m_messenger.shutdown();
            }
            m_messenger = null;

            //Also for test code that expects a fresh stats agent
            if (m_statsAgent != null) {
                m_statsAgent.shutdown();
                m_statsAgent = null;
            }

            if (m_asyncCompilerAgent != null) {
                m_asyncCompilerAgent.shutdown();
                m_asyncCompilerAgent = null;
            }

            // The network iterates this list. Clear it after network's done.
            m_clientInterfaces.clear();

            ExportManager.instance().shutdown();
            m_computationService.shutdown();
            m_computationService.awaitTermination(1, TimeUnit.DAYS);
            m_computationService = null;

            // probably unnecessary
            System.gc();
            m_isRunning = false;
        }
    }

    /** Last transaction ID at which the rejoin commit took place.
     * Also, use the intrinsic lock to safeguard access from multiple
     * execution site threads */
    private static Long lastNodeRejoinPrepare_txnId = 0L;
    @Override
    public synchronized String doRejoinPrepare(
            long currentTxnId,
            int rejoinHostId,
            String rejoiningHostname,
            int portToConnect,
            Set<Integer> liveHosts)
    {
        return lastNodeRejoinPrepare_txnId.toString();
    }

    /** Last transaction ID at which the rejoin commit took place.
     * Also, use the intrinsic lock to safeguard access from multiple
     * execution site threads */
    // private static Long lastNodeRejoinFinish_txnId = 0L;
    @Override
    public synchronized String doRejoinCommitOrRollback(long currentTxnId, boolean commit) {
        throw new UnsupportedOperationException();
//        if (m_recovering) {
//            recoveryLog.fatal("Concurrent rejoins are not supported");
//            VoltDB.crashVoltDB();
//        }
//        // another site already did this work.
//        if (currentTxnId == lastNodeRejoinFinish_txnId) {
//            return null;
//        }
//        else if (currentTxnId < lastNodeRejoinFinish_txnId) {
//            throw new RuntimeException("Trying to rejoin (commit/rollback) with an old transaction.");
//        }
//        recoveryLog.info("Rejoining commit node with txnid: " + currentTxnId +
//                         " lastNodeRejoinFinish_txnId: " + lastNodeRejoinFinish_txnId);
//        HostMessenger messenger = getHostMessenger();
//        if (commit) {
//            // put the foreign host into the set of active ones
//            HostMessenger.JoiningNodeInfo joinNodeInfo = messenger.rejoinForeignHostCommit();
//            m_faultManager.reportFaultCleared(
//                    new NodeFailureFault(
//                            joinNodeInfo.hostId,
//                            m_siteTracker.getNonExecSitesForHost(joinNodeInfo.hostId),
//                            joinNodeInfo.hostName));
//            try {
//                m_faultHandler.m_waitForFaultClear.acquire();
//            } catch (InterruptedException e) {
//                VoltDB.crashVoltDB();//shouldn't happen
//            }
//
//            ArrayList<Integer> rejoiningSiteIds = new ArrayList<Integer>();
//            ArrayList<Integer> rejoiningExecSiteIds = new ArrayList<Integer>();
//            Cluster cluster = m_catalogContext.catalog.getClusters().get("cluster");
//            for (Site site : cluster.getSites()) {
//                int siteId = Integer.parseInt(site.getTypeName());
//                int hostId = Integer.parseInt(site.getHost().getTypeName());
//                if (hostId == joinNodeInfo.hostId) {
//                    assert(site.getIsup() == false);
//                    rejoiningSiteIds.add(siteId);
//                    if (site.getIsexec() == true) {
//                        rejoiningExecSiteIds.add(siteId);
//                    }
//                }
//            }
//            assert(rejoiningSiteIds.size() > 0);
//
//            // get a string list of all the new sites
//            StringBuilder newIds = new StringBuilder();
//            for (int siteId : rejoiningSiteIds) {
//                newIds.append(siteId).append(",");
//            }
//            // trim the last comma
//            newIds.setLength(newIds.length() - 1);
//
//            // change the catalog to reflect this change
//            hostLog.info("Host joined, host id: " + joinNodeInfo.hostId + " hostname: " + joinNodeInfo.hostName);
//            hostLog.info("  Adding sites to cluster: " + newIds);
//            StringBuilder sb = new StringBuilder();
//            for (int siteId : rejoiningSiteIds)
//            {
//                sb.append("set ");
//                String site_path = VoltDB.instance().getCatalogContext().catalog.
//                                   getClusters().get("cluster").getSites().
//                                   get(Integer.toString(siteId)).getPath();
//                sb.append(site_path).append(" ").append("isUp true");
//                sb.append("\n");
//            }
//            String catalogDiffCommands = sb.toString();
//            clusterUpdate(catalogDiffCommands);
//
//            // update the SafteyState in the initiators
//            for (SimpleDtxnInitiator dtxn : m_dtxns) {
//                dtxn.notifyExecutionSiteRejoin(rejoiningExecSiteIds);
//            }
//
//            //Notify the export manager the cluster topology has changed
//            ExportManager.instance().notifyOfClusterTopologyChange();
//        }
//        else {
//            // clean up any connections made
//            messenger.rejoinForeignHostRollback();
//        }
//        recoveryLog.info("Setting lastNodeRejoinFinish_txnId to: " + currentTxnId);
//        lastNodeRejoinFinish_txnId = currentTxnId;
//
//        return null;
    }

    /** Last transaction ID at which the logging config updated.
     * Also, use the intrinsic lock to safeguard access from multiple
     * execution site threads */
    private static Long lastLogUpdate_txnId = 0L;
    @Override
    public void logUpdate(String xmlConfig, long currentTxnId)
    {
        synchronized(lastLogUpdate_txnId)
        {
            // another site already did this work.
            if (currentTxnId == lastLogUpdate_txnId) {
                return;
            }
            else if (currentTxnId < lastLogUpdate_txnId) {
                throw new RuntimeException("Trying to update logging config with an old transaction.");
            }
            System.out.println("Updating RealVoltDB logging config from txnid: " +
                               lastLogUpdate_txnId + " to " + currentTxnId);
            lastLogUpdate_txnId = currentTxnId;
            VoltLogger.configure(xmlConfig);
        }
    }

    /** Struct to associate a context with a counter of served sites */
    private static class ContextTracker {
        ContextTracker(CatalogContext context) {
            m_dispensedSites = 1;
            m_context = context;
        }
        long m_dispensedSites;
        CatalogContext m_context;
    }

    /** Associate transaction ids to contexts */
    private final HashMap<Long, ContextTracker>m_txnIdToContextTracker =
        new HashMap<Long, ContextTracker>();

    @Override
    public CatalogContext catalogUpdate(
            String diffCommands,
            byte[] newCatalogBytes,
            int expectedCatalogVersion,
            long currentTxnId,
            long deploymentCRC)
    {
        synchronized(m_catalogUpdateLock) {
            // A site is catching up with catalog updates
            if (currentTxnId <= m_catalogContext.m_transactionId && !m_txnIdToContextTracker.isEmpty()) {
                ContextTracker contextTracker = m_txnIdToContextTracker.get(currentTxnId);
                // This 'dispensed' concept is a little crazy fragile. Maybe it would be better
                // to keep a rolling N catalogs? Or perhaps to keep catalogs for N minutes? Open
                // to opinions here.
                contextTracker.m_dispensedSites++;
                int ttlsites = m_siteTracker.getSitesForHost(m_messenger.getHostId()).size();
                if (contextTracker.m_dispensedSites == ttlsites) {
                    m_txnIdToContextTracker.remove(currentTxnId);
                }
                return contextTracker.m_context;
            }
            else if (m_catalogContext.catalogVersion != expectedCatalogVersion) {
                throw new RuntimeException("Trying to update main catalog context with diff " +
                "commands generated for an out-of date catalog. Expected catalog version: " +
                expectedCatalogVersion + " does not match actual version: " + m_catalogContext.catalogVersion);
            }

            // 0. A new catalog! Update the global context and the context tracker
            m_catalogContext =
                m_catalogContext.update(currentTxnId, newCatalogBytes, diffCommands, true, deploymentCRC);
            m_txnIdToContextTracker.put(currentTxnId, new ContextTracker(m_catalogContext));
            m_catalogContext.logDebuggingInfoFromCatalog();

            // 1. update the export manager.
            ExportManager.instance().updateCatalog(m_catalogContext);

            // 2. update client interface (asynchronously)
            //    CI in turn updates the planner thread.
            for (ClientInterface ci : m_clientInterfaces) {
                ci.notifyOfCatalogUpdate();
            }

            // 3. update HTTPClientInterface (asynchronously)
            // This purges cached connection state so that access with
            // stale auth info is prevented.
            if (m_adminListener != null)
            {
                m_adminListener.notifyOfCatalogUpdate();
            }

            return m_catalogContext;
        }
    }

    @Override
    public void clusterUpdate(String diffCommands)
    {
        synchronized(m_catalogUpdateLock)
        {
            //Reuse the txn id since this doesn't change schema/procs/export
            m_catalogContext = m_catalogContext.update(m_catalogContext.m_transactionId, null,
                                                       diffCommands, false, -1);
        }

        for (ClientInterface ci : m_clientInterfaces)
        {
            ci.notifyOfCatalogUpdate();
        }
    }

    @Override
    public VoltDB.Configuration getConfig() {
        return m_config;
    }

    @Override
    public String getBuildString() {
        return m_buildString;
    }

    @Override
    public String getVersionString() {
        return m_versionString;
    }

    @Override
    public HostMessenger getHostMessenger() {
        return m_messenger;
    }

    @Override
    public ArrayList<ClientInterface> getClientInterfaces() {
        return m_clientInterfaces;
    }

    @Override
    public Map<Long, ExecutionSite> getLocalSites() {
        return m_localSites;
    }

    @Override
    public StatsAgent getStatsAgent() {
        return m_statsAgent;
    }

    @Override
    public MemoryStats getMemoryStatsSource() {
        return m_memoryStats;
    }

    @Override
    public FaultDistributorInterface getFaultDistributor()
    {
        return m_faultManager;
    }

    @Override
    public CatalogContext getCatalogContext() {
        synchronized(m_catalogUpdateLock) {
            return m_catalogContext;
        }
    }

    /**
     * Tells if the VoltDB is running. m_isRunning needs to be set to true
     * when the run() method is called, and set to false when shutting down.
     *
     * @return true if the VoltDB is running.
     */
    @Override
    public boolean isRunning() {
        return m_isRunning;
    }

    /**
     * Debugging function - creates a record of the current state of the system.
     * @param out PrintStream to write report to.
     */
    public void createRuntimeReport(PrintStream out) {
        // This function may be running in its own thread.

        out.print("MIME-Version: 1.0\n");
        out.print("Content-type: multipart/mixed; boundary=\"reportsection\"");

        out.print("\n\n--reportsection\nContent-Type: text/plain\n\nClientInterface Report\n");
        for (ClientInterface ci : getClientInterfaces()) {
          out.print(ci.toString() + "\n");
        }

        out.print("\n\n--reportsection\nContent-Type: text/plain\n\nLocalSite Report\n");
        for(ExecutionSite es : getLocalSites().values()) {
            out.print(es.toString() + "\n");
        }

        out.print("\n\n--reportsection--");
    }

    @Override
    public boolean ignoreCrash() {
        return false;
    }

    @Override
    public BackendTarget getBackendTargetType() {
        return m_config.m_backend;
    }

    @Override
    public synchronized void onExecutionSiteRecoveryCompletion(long transferred) {
        m_executionSiteRecoveryFinish = System.currentTimeMillis();
        m_executionSiteRecoveryTransferred = transferred;
        m_executionSitesRecovered = true;
        onRecoveryCompletion();
    }

    private void onRecoveryCompletion() {
        if (!m_executionSitesRecovered || !m_agreementSiteRecovered) {
            return;
        }
        try {
            m_testBlockRecoveryCompletion.acquire();
        } catch (InterruptedException e) {}
        final long delta = ((m_executionSiteRecoveryFinish - m_recoveryStartTime) / 1000);
        final long megabytes = m_executionSiteRecoveryTransferred / (1024 * 1024);
        final double megabytesPerSecond = megabytes / ((m_executionSiteRecoveryFinish - m_recoveryStartTime) / 1000.0);
        for (ClientInterface intf : getClientInterfaces()) {
            intf.mayActivateSnapshotDaemon();
        }
        hostLog.info(
                "Node data recovery completed after " + delta + " seconds with " + megabytes +
                " megabytes transferred at a rate of " +
                megabytesPerSecond + " megabytes/sec");
        try {
            final ZooKeeper zk = m_messenger.getZK();
            boolean logRecoveryCompleted = false;
            try {
                zk.create(VoltZK.unfaulted_hosts, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            } catch (KeeperException.NodeExistsException e) {}
            if (getCommandLog().getClass().getName().equals("org.voltdb.CommandLogImpl")) {
                try {
                    zk.create(VoltZK.request_truncation_snapshot, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                } catch (KeeperException.NodeExistsException e) {}
            } else {
                logRecoveryCompleted = true;
            }
            ByteBuffer txnIdBuffer = ByteBuffer.allocate(8);
            txnIdBuffer.putLong(TransactionIdManager.makeIdFromComponents(System.currentTimeMillis(), 0, 1));
            zk.create(
                    VoltZK.unfaulted_hosts + m_messenger.getHostId(),
                    txnIdBuffer.array(),
                    Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            if (logRecoveryCompleted) {
                m_recovering = false;
                hostLog.info("Node recovery completed");
            }
        } catch (Exception e) {
            VoltDB.crashLocalVoltDB("Unable to log host recovery completion to ZK", true, e);
        }
        hostLog.info("Logging host recovery completion to ZK");
    }

    @Override
    public CommandLog getCommandLog() {
        return m_commandLog;
    }

    @Override
    public OperationMode getMode()
    {
        return m_mode;
    }

    @Override
    public void setMode(OperationMode mode)
    {
        if (m_mode != mode)
        {
            if (mode == OperationMode.PAUSED)
            {
                hostLog.info("Server is entering admin mode and pausing.");
            }
            else if (m_mode == OperationMode.PAUSED)
            {
                hostLog.info("Server is exiting admin mode and resuming operation.");
            }
        }
        m_mode = mode;
    }

    @Override
    public void setStartMode(OperationMode mode) {
        m_startMode = mode;
    }

    @Override
    public OperationMode getStartMode()
    {
        return m_startMode;
    }

    @Override
    public void setReplicationRole(ReplicationRole role)
    {
        if (m_replicationRole == null) {
            m_replicationRole = role;
        } else if (m_replicationRole == ReplicationRole.REPLICA) {
            if (role != ReplicationRole.MASTER) {
                return;
            }

            hostLog.info("Changing replication role from " + m_replicationRole +
                         " to " + role);
            m_replicationRole = role;
            for (ClientInterface ci : m_clientInterfaces) {
                ci.setReplicationRole(m_replicationRole);
            }
        }
    }

    @Override
    public ReplicationRole getReplicationRole()
    {
        return m_replicationRole;
    }

    /**
     * Get the metadata map for the wholes cluster.
     * Note: this may include failed nodes so check for live ones
     *  and filter this if needed.
     *
     * Metadata is currently a JSON object
     */
    @Override
    public Map<Integer, String> getClusterMetadataMap() {
        return m_clusterMetadata;
    }

    /**
     * Metadata is a JSON object
     */
    @Override
    public String getLocalMetadata() {
        return m_localMetadata;
    }

    @Override
    public void onRestoreCompletion(long txnId) {

        /*
         * Command log is already initialized if this is a rejoin
         */
        if ((m_commandLog != null) && (m_commandLog.needsInitialization())) {
            // Initialize command logger
            m_commandLog.init(m_catalogContext, txnId);
        }

        /*
         * Enable the initiator to send normal heartbeats and accept client
         * connections
         */
        for (SimpleDtxnInitiator dtxn : m_dtxns) {
            dtxn.setSendHeartbeats(true);
        }

        for (ClientInterface ci : m_clientInterfaces) {
            try {
                ci.startAcceptingConnections();
            } catch (IOException e) {
                hostLog.l7dlog(Level.FATAL,
                               LogKeys.host_VoltDB_ErrorStartAcceptingConnections.name(),
                               e);
                VoltDB.crashLocalVoltDB("Error starting client interface.", true, e);
            }
        }

        if (m_startMode != null) {
            m_mode = m_startMode;
        } else {
            // Shouldn't be here, but to be safe
            m_mode = OperationMode.RUNNING;
        }
        hostLog.l7dlog( Level.INFO, LogKeys.host_VoltDB_ServerCompletedInitialization.name(), null);
    }

    @Override
    public synchronized void onAgreementSiteRecoveryCompletion() {
        m_agreementSiteRecovered = true;
        onRecoveryCompletion();
    }

    @Override
    public SnapshotCompletionMonitor getSnapshotCompletionMonitor() {
        return m_snapshotCompletionMonitor;
    }

    @Override
    public synchronized void recoveryComplete() {
        m_recovering = false;
        hostLog.info("Node recovery completed");
    }

    @Override
    public ScheduledFuture<?> scheduleWork(Runnable work,
                             long initialDelay,
                             long delay,
                             TimeUnit unit) {
        if (delay > 0) {
            return m_periodicWorkThread.scheduleWithFixedDelay(work,
                                                        initialDelay, delay,
                                                        unit);
        } else {
            return m_periodicWorkThread.schedule(work, initialDelay, unit);
        }
    }

    @Override
    public ExecutorService getComputationService() {
        return m_computationService;
    }

    @Override
    public void setReplicationActive(boolean active)
    {
        if (m_replicationActive != active) {
            m_replicationActive = active;
            if (m_localSites != null) {
                for (ExecutionSite s : m_localSites.values()) {
                    s.getPartitionDRGateway().setActive(active);
                }
            }
        }
    }

    @Override
    public boolean getReplicationActive()
    {
        return m_replicationActive;
    }

    @Override
    public void handleMailboxUpdate(Map<MailboxType, List<MailboxNodeContent>> mailboxes) {
        m_siteTracker = new SiteTracker(m_myHostId, mailboxes);
    }

    @Override
    public SiteTracker getSiteTracker() {
        return m_siteTracker;
    }
}
