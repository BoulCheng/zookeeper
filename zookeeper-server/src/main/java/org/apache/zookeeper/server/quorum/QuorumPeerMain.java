/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper.server.quorum;

import java.io.IOException;
import java.util.Map;
import javax.management.JMException;
import javax.security.sasl.SaslException;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.audit.ZKAuditProvider;
import org.apache.zookeeper.jmx.ManagedUtil;
import org.apache.zookeeper.metrics.MetricsProvider;
import org.apache.zookeeper.metrics.MetricsProviderLifeCycleException;
import org.apache.zookeeper.metrics.impl.MetricsProviderBootstrap;
import org.apache.zookeeper.server.DatadirCleanupManager;
import org.apache.zookeeper.server.ExitCode;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ServerMetrics;
import org.apache.zookeeper.server.ZKDatabase;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.admin.AdminServer.AdminServerException;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog.DatadirException;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;
import org.apache.zookeeper.server.util.JvmPauseMonitor;
import org.apache.zookeeper.util.ServiceUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 处理配置文件 zoo.cfg 和 myid
 *
 * <h2>Configuration file</h2>
 *
 * When the main() method of this class is used to start the program, the first
 * argument is used as a path to the config file, which will be used to obtain
 * configuration information. This file is a Properties file, so keys and
 * values are separated by equals (=) and the key/value pairs are separated
 * by new lines. The following is a general summary of keys used in the
 * configuration file. For full details on this see the documentation in
 * docs/index.html
 * <ol>
 * <li>dataDir - The directory where the ZooKeeper data is stored.</li>
 * <li>dataLogDir - The directory where the ZooKeeper transaction log is stored.</li>
 * <li>clientPort - The port used to communicate with clients.</li>
 * <li>tickTime - The duration of a tick in milliseconds. This is the basic
 * unit of time in ZooKeeper.</li>
 * <li>initLimit - The maximum number of ticks that a follower will wait to
 * initially synchronize with a leader.</li>
 * <li>syncLimit - The maximum number of ticks that a follower will wait for a
 * message (including heartbeats) from the leader.</li>
 * <li>server.<i>id</i> - This is the host:port[:port] that the server with the
 * given id will use for the quorum protocol.</li>
 * </ol>
 * In addition to the config file. There is a file in the data directory called
 * "myid" that contains the server id as an ASCII decimal value.
 *
 */
@InterfaceAudience.Public
public class QuorumPeerMain {

    private static final Logger LOG = LoggerFactory.getLogger(QuorumPeerMain.class);

    private static final String USAGE = "Usage: QuorumPeerMain configfile";

    protected QuorumPeer quorumPeer;

    /**
     *
     nohup "$JAVA" $ZOO_DATADIR_AUTOCREATE "-Dzookeeper.log.dir=${ZOO_LOG_DIR}" \
     "-Dzookeeper.log.file=${ZOO_LOG_FILE}" "-Dzookeeper.root.logger=${ZOO_LOG4J_PROP}" \
     -XX:+HeapDumpOnOutOfMemoryError -XX:OnOutOfMemoryError='kill -9 %p' \
     -cp "$CLASSPATH" $JVMFLAGS $ZOOMAIN "$ZOOCFG" > "$_ZOO_DAEMON_OUT" 2>&1 < /dev/null &

     * @param args
     */
    private static String[] setSyspropAndArgs(String[] args) {

        System.setProperty("zookeeper.log.dir", "/Users/apple/IdeaProjects/private/zookeeper/bin/../logs");
        System.setProperty("zookeeper.log.file", "zookeeper-apple-server-appledeiMac.local.log");
        System.setProperty("zookeeper.root.logger", "INFO,CONSOLE");
        System.setProperty("com.sun.management.jmxremote", "");
        System.setProperty("com.sun.management.jmxremote.local.only", "false");

        System.out.println(args.length);
        return new String[]{"/Users/apple/IdeaProjects/private/zookeeper/bin/../conf/zoo.cfg"};

        /**
         * Starting zookeeper ...
         * JAVA: /Library/Java/JavaVirtualMachines/jdk1.8.0_221.jdk/Contents/Home/bin/java
         * ZOO_DATADIR_AUTOCREATE:
         * ZOO_LOG_DIR: /Users/apple/IdeaProjects/private/zookeeper/bin/../logs
         * ZOO_LOG_FILE: zookeeper-apple-server-appledeiMac.local.log
         * ZOO_LOG4J_PROP: INFO,CONSOLE
         * CLASSPATH: /Users/apple/IdeaProjects/private/zookeeper/bin/../zookeeper-server/target/classes:/Users/apple/IdeaProjects/private/zookeeper/bin/../build/classes:/Users/apple/IdeaProjects/private/zookeeper/bin/../zookeeper-server/target/lib/zookeeper-jute-3.6.1.jar:/Users/apple/IdeaProjects/private/zookeeper/bin/../zookeeper-server/target/lib/token-provider-2.0.0.jar:/Users/apple/IdeaProjects/private/zookeeper/bin/../zookeeper-server/target/lib/spotbugs-annotations-4.0.2.jar:/Users/apple/IdeaProjects/private/zookeeper/bin/../zookeeper-server/target/lib/snappy-java-1.1.7.jar:/Users/apple/IdeaProjects/private/zookeeper/bin/../zookeeper-server/target/lib/slf4j-log4j12-1.7.25.jar:/Users/apple/IdeaProjects/private/zookeeper/bin/../zookeeper-server/target/lib/slf4j-api-1.7.25.jar:/Users/apple/IdeaProjects/private/zookeeper/bin/../zookeeper-server/target/lib/objenesis-2.6.jar:/Users/apple/IdeaProjects/private/zookeeper/bin/../zookeeper-server/target/lib/nimbus-jose-jwt-4.41.2.jar:/Users/apple/IdeaProjects/private/zookeeper/bin/../zookeeper-server/target/lib/netty-transport-native-unix-common-4.1.48.Final.jar:/Users/apple/IdeaProjects/private/zookeeper/bin/../zookeeper-server/target/lib/netty-transport-native-epoll-4.1.48.Final.jar:/Users/apple/IdeaProjects/private/zookeeper/bin/../zookeeper-server/target/lib/netty-transport-4.1.48.Final.jar:/Users/apple/IdeaProjects/private/zookeeper/bin/../zookeeper-server/target/lib/netty-resolver-4.1.48.Final.jar:/Users/apple/IdeaProjects/private/zookeeper/bin/../zookeeper-server/target/lib/netty-handler-4.1.48.Final.jar:/Users/apple/IdeaProjects/private/zookeeper/bin/../zookeeper-server/target/lib/netty-common-4.1.48.Final.jar:/Users/apple/IdeaProjects/private/zookeeper/bin/../zookeeper-server/target/lib/netty-codec-4.1.48.Final.jar:/Users/apple/IdeaProjects/private/zookeeper/bin/../zookeeper-server/target/lib/netty-buffer-4.1.48.Final.jar:/Users/apple/IdeaProjects/private/zookeeper/bin/../zookeeper-server/target/lib/mockito-core-2.27.0.jar:/Users/apple/IdeaProjects/private/zookeeper/bin/../zookeeper-server/target/lib/metrics-core-3.2.5.jar:/Users/apple/IdeaProjects/private/zookeeper/bin/../zookeeper-server/target/lib/log4j-1.2.17.jar:/Users/apple/IdeaProjects/private/zookeeper/bin/../zookeeper-server/target/lib/kerby-xdr-2.0.0.jar:/Users/apple/IdeaProjects/private/zookeeper/bin/../zookeeper-server/target/lib/kerby-util-2.0.0.jar:/Users/apple/IdeaProjects/private/zookeeper/bin/../zookeeper-server/target/lib/kerby-pkix-2.0.0.jar:/Users/apple/IdeaProjects/private/zookeeper/bin/../zookeeper-server/target/lib/kerby-config-2.0.0.jar:/Users/apple/IdeaProjects/private/zookeeper/bin/../zookeeper-server/target/lib/kerby-asn1-2.0.0.jar:/Users/apple/IdeaProjects/private/zookeeper/bin/../zookeeper-server/target/lib/kerb-util-2.0.0.jar:/Users/apple/IdeaProjects/private/zookeeper/bin/../zookeeper-server/target/lib/kerb-simplekdc-2.0.0.jar:/Users/apple/IdeaProjects/private/zookeeper/bin/../zookeeper-server/target/lib/kerb-server-2.0.0.jar:/Users/apple/IdeaProjects/private/zookeeper/bin/../zookeeper-server/target/lib/kerb-identity-2.0.0.jar:/Users/apple/IdeaProjects/private/zookeeper/bin/../zookeeper-server/target/lib/kerb-crypto-2.0.0.jar:/Users/apple/IdeaProjects/private/zookeeper/bin/../zookeeper-server/target/lib/kerb-core-2.0.0.jar:/Users/apple/IdeaProjects/private/zookeeper/bin/../zookeeper-server/target/lib/kerb-common-2.0.0.jar:/Users/apple/IdeaProjects/private/zookeeper/bin/../zookeeper-server/target/lib/kerb-client-2.0.0.jar:/Users/apple/IdeaProjects/private/zookeeper/bin/../zookeeper-server/target/lib/kerb-admin-2.0.0.jar:/Users/apple/IdeaProjects/private/zookeeper/bin/../zookeeper-server/target/lib/junit-4.12.jar:/Users/apple/IdeaProjects/private/zookeeper/bin/../zookeeper-server/target/lib/jsr305-3.0.2.jar:/Users/apple/IdeaProjects/private/zookeeper/bin/../zookeeper-server/target/lib/json-smart-2.3.jar:/Users/apple/IdeaProjects/private/zookeeper/bin/../zookeeper-server/target/lib/json-simple-1.1.1.jar:/Users/apple/IdeaProjects/private/zookeeper/bin/../zookeeper-server/target/lib/jmockit-1.48.jar:/Users/apple/IdeaProjects/private/zookeeper/bin/../zookeeper-server/target/lib/jline-2.11.jar:/Users/apple/IdeaProjects/private/zookeeper/bin/../zookeeper-server/target/lib/jetty-util-9.4.24.v20191120.jar:/Users/apple/IdeaProjects/private/zookeeper/bin/../zookeeper-server/target/lib/jetty-servlet-9.4.24.v20191120.jar:/Users/apple/IdeaProjects/private/zookeeper/bin/../zookeeper-server/target/lib/jetty-server-9.4.24.v20191120.jar:/Users/apple/IdeaProjects/private/zookeeper/bin/../zookeeper-server/target/lib/jetty-security-9.4.24.v20191120.jar:/Users/apple/IdeaProjects/private/zookeeper/bin/../zookeeper-server/target/lib/jetty-io-9.4.24.v20191120.jar:/Users/apple/IdeaProjects/private/zookeeper/bin/../zookeeper-server/target/lib/jetty-http-9.4.24.v20191120.jar:/Users/apple/IdeaProjects/private/zookeeper/bin/../zookeeper-server/target/lib/jcip-annotations-1.0-1.jar:/Users/apple/IdeaProjects/private/zookeeper/bin/../zookeeper-server/target/lib/javax.servlet-api-3.1.0.jar:/Users/apple/IdeaProjects/private/zookeeper/bin/../zookeeper-server/target/lib/jackson-databind-2.10.3.jar:/Users/apple/IdeaProjects/private/zookeeper/bin/../zookeeper-server/target/lib/jackson-core-2.10.3.jar:/Users/apple/IdeaProjects/private/zookeeper/bin/../zookeeper-server/target/lib/jackson-annotations-2.10.3.jar:/Users/apple/IdeaProjects/private/zookeeper/bin/../zookeeper-server/target/lib/hamcrest-core-1.3.jar:/Users/apple/IdeaProjects/private/zookeeper/bin/../zookeeper-server/target/lib/hamcrest-all-1.3.jar:/Users/apple/IdeaProjects/private/zookeeper/bin/../zookeeper-server/target/lib/commons-lang-2.6.jar:/Users/apple/IdeaProjects/private/zookeeper/bin/../zookeeper-server/target/lib/commons-io-2.6.jar:/Users/apple/IdeaProjects/private/zookeeper/bin/../zookeeper-server/target/lib/commons-collections-3.2.2.jar:/Users/apple/IdeaProjects/private/zookeeper/bin/../zookeeper-server/target/lib/commons-cli-1.2.jar:/Users/apple/IdeaProjects/private/zookeeper/bin/../zookeeper-server/target/lib/byte-buddy-agent-1.9.10.jar:/Users/apple/IdeaProjects/private/zookeeper/bin/../zookeeper-server/target/lib/byte-buddy-1.9.10.jar:/Users/apple/IdeaProjects/private/zookeeper/bin/../zookeeper-server/target/lib/bcprov-jdk15on-1.60.jar:/Users/apple/IdeaProjects/private/zookeeper/bin/../zookeeper-server/target/lib/bcpkix-jdk15on-1.60.jar:/Users/apple/IdeaProjects/private/zookeeper/bin/../zookeeper-server/target/lib/audience-annotations-0.5.0.jar:/Users/apple/IdeaProjects/private/zookeeper/bin/../zookeeper-server/target/lib/asm-5.0.4.jar:/Users/apple/IdeaProjects/private/zookeeper/bin/../zookeeper-server/target/lib/accessors-smart-1.2.jar:/Users/apple/IdeaProjects/private/zookeeper/bin/../build/lib/*.jar:/Users/apple/IdeaProjects/private/zookeeper/bin/../lib/*.jar:/Users/apple/IdeaProjects/private/zookeeper/bin/../zookeeper-*.jar:/Users/apple/IdeaProjects/private/zookeeper/bin/../zookeeper-server/src/main/resources/lib/*.jar:/Users/apple/IdeaProjects/private/zookeeper/bin/../conf:
         * JVMFLAGS: -Xmx1000m
         * ZOOMAIN: -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.local.only=false org.apache.zookeeper.server.quorum.QuorumPeerMain
         * ZOOCFG: /Users/apple/IdeaProjects/private/zookeeper/bin/../conf/zoo.cfg
         * _ZOO_DAEMON_OUT: /Users/apple/IdeaProjects/private/zookeeper/bin/../logs/zookeeper-apple-server-appledeiMac.local.out
         * STARTED
         */

    }
    /**
     * zkServer.sh  mainclass
     * To start the replicated server specify the configuration file name on
     * the command line.
     * @param args path to the configfile (zoo.cfg)
     */
    public static void main(String[] args) {
        args = setSyspropAndArgs(args);

        LOG.info("==========args");
        for (int i = 0; i < args.length; i++) {
            System.out.println(i + ": " + args[i]);
        }
        for (Map.Entry entry : System.getProperties().entrySet()) {
            System.out.println(entry.getKey().toString() + ": " + entry.getValue().toString());
        }

        QuorumPeerMain main = new QuorumPeerMain();
        try {
            main.initializeAndRun(args);
        } catch (IllegalArgumentException e) {
            LOG.error("Invalid arguments, exiting abnormally", e);
            LOG.info(USAGE);
            System.err.println(USAGE);
            ZKAuditProvider.addServerStartFailureAuditLog();
            ServiceUtils.requestSystemExit(ExitCode.INVALID_INVOCATION.getValue());
        } catch (ConfigException e) {
            LOG.error("Invalid config, exiting abnormally", e);
            System.err.println("Invalid config, exiting abnormally");
            ZKAuditProvider.addServerStartFailureAuditLog();
            ServiceUtils.requestSystemExit(ExitCode.INVALID_INVOCATION.getValue());
        } catch (DatadirException e) {
            LOG.error("Unable to access datadir, exiting abnormally", e);
            System.err.println("Unable to access datadir, exiting abnormally");
            ZKAuditProvider.addServerStartFailureAuditLog();
            ServiceUtils.requestSystemExit(ExitCode.UNABLE_TO_ACCESS_DATADIR.getValue());
        } catch (AdminServerException e) {
            LOG.error("Unable to start AdminServer, exiting abnormally", e);
            System.err.println("Unable to start AdminServer, exiting abnormally");
            ZKAuditProvider.addServerStartFailureAuditLog();
            ServiceUtils.requestSystemExit(ExitCode.ERROR_STARTING_ADMIN_SERVER.getValue());
        } catch (Exception e) {
            LOG.error("Unexpected exception, exiting abnormally", e);
            ZKAuditProvider.addServerStartFailureAuditLog();
            ServiceUtils.requestSystemExit(ExitCode.UNEXPECTED_ERROR.getValue());
        }
        LOG.info("Exiting normally");
        ServiceUtils.requestSystemExit(ExitCode.EXECUTION_FINISHED.getValue());
    }

    protected void initializeAndRun(String[] args) throws ConfigException, IOException, AdminServerException {
        QuorumPeerConfig config = new QuorumPeerConfig();
        if (args.length == 1) {
            // 解析zoo.cfg 处理配置信息
            config.parse(args[0]);
        }

        // Start and schedule the the purge task
        DatadirCleanupManager purgeMgr = new DatadirCleanupManager(
            config.getDataDir(),
            config.getDataLogDir(),
            config.getSnapRetainCount(),
            config.getPurgeInterval());
        purgeMgr.start();

        if (args.length == 1 && config.isDistributed()) {
            // zkServer集群
            runFromConfig(config);
        } else {
            // zkServer单机
            LOG.warn("Either no config or no quorum defined in config, running in standalone mode");
            // there is only server in the quorum -- run as standalone
            ZooKeeperServerMain.main(args);
        }
    }

    public void runFromConfig(QuorumPeerConfig config) throws IOException, AdminServerException {
        try {
            ManagedUtil.registerLog4jMBeans();
        } catch (JMException e) {
            LOG.warn("Unable to register log4j JMX control", e);
        }

        LOG.info("Starting quorum peer");
        MetricsProvider metricsProvider;
        try {
            metricsProvider = MetricsProviderBootstrap.startMetricsProvider(
                config.getMetricsProviderClassName(),
                config.getMetricsProviderConfiguration());
        } catch (MetricsProviderLifeCycleException error) {
            throw new IOException("Cannot boot MetricsProvider " + config.getMetricsProviderClassName(), error);
        }
        try {
            ServerMetrics.metricsProviderInitialized(metricsProvider);
            ServerCnxnFactory cnxnFactory = null;
            ServerCnxnFactory secureCnxnFactory = null;

            if (config.getClientPortAddress() != null) {
                cnxnFactory = ServerCnxnFactory.createFactory();
                cnxnFactory.configure(config.getClientPortAddress(), config.getMaxClientCnxns(), config.getClientPortListenBacklog(), false);
            }

            if (config.getSecureClientPortAddress() != null) {
                secureCnxnFactory = ServerCnxnFactory.createFactory();
                secureCnxnFactory.configure(config.getSecureClientPortAddress(), config.getMaxClientCnxns(), config.getClientPortListenBacklog(), true);
            }

            quorumPeer = getQuorumPeer();
            quorumPeer.setTxnFactory(new FileTxnSnapLog(config.getDataLogDir(), config.getDataDir()));
            quorumPeer.enableLocalSessions(config.areLocalSessionsEnabled());
            quorumPeer.enableLocalSessionsUpgrading(config.isLocalSessionsUpgradingEnabled());
            //quorumPeer.setQuorumPeers(config.getAllMembers());
            quorumPeer.setElectionType(config.getElectionAlg());
            quorumPeer.setMyid(config.getServerId());
            quorumPeer.setTickTime(config.getTickTime());
            quorumPeer.setMinSessionTimeout(config.getMinSessionTimeout());
            quorumPeer.setMaxSessionTimeout(config.getMaxSessionTimeout());
            quorumPeer.setInitLimit(config.getInitLimit());
            quorumPeer.setSyncLimit(config.getSyncLimit());
            quorumPeer.setConnectToLearnerMasterLimit(config.getConnectToLearnerMasterLimit());
            quorumPeer.setObserverMasterPort(config.getObserverMasterPort());
            quorumPeer.setConfigFileName(config.getConfigFilename());
            quorumPeer.setClientPortListenBacklog(config.getClientPortListenBacklog());
            quorumPeer.setZKDatabase(new ZKDatabase(quorumPeer.getTxnFactory()));
            quorumPeer.setQuorumVerifier(config.getQuorumVerifier(), false);
            if (config.getLastSeenQuorumVerifier() != null) {
                quorumPeer.setLastSeenQuorumVerifier(config.getLastSeenQuorumVerifier(), false);
            }
            quorumPeer.initConfigInZKDatabase();
            quorumPeer.setCnxnFactory(cnxnFactory);
            quorumPeer.setSecureCnxnFactory(secureCnxnFactory);
            quorumPeer.setSslQuorum(config.isSslQuorum());
            quorumPeer.setUsePortUnification(config.shouldUsePortUnification());
            quorumPeer.setLearnerType(config.getPeerType());
            quorumPeer.setSyncEnabled(config.getSyncEnabled());
            quorumPeer.setQuorumListenOnAllIPs(config.getQuorumListenOnAllIPs());
            if (config.sslQuorumReloadCertFiles) {
                quorumPeer.getX509Util().enableCertFileReloading();
            }
            quorumPeer.setMultiAddressEnabled(config.isMultiAddressEnabled());
            quorumPeer.setMultiAddressReachabilityCheckEnabled(config.isMultiAddressReachabilityCheckEnabled());
            quorumPeer.setMultiAddressReachabilityCheckTimeoutMs(config.getMultiAddressReachabilityCheckTimeoutMs());

            // sets quorum sasl authentication configurations
            quorumPeer.setQuorumSaslEnabled(config.quorumEnableSasl);
            if (quorumPeer.isQuorumSaslAuthEnabled()) {
                quorumPeer.setQuorumServerSaslRequired(config.quorumServerRequireSasl);
                quorumPeer.setQuorumLearnerSaslRequired(config.quorumLearnerRequireSasl);
                quorumPeer.setQuorumServicePrincipal(config.quorumServicePrincipal);
                quorumPeer.setQuorumServerLoginContext(config.quorumServerLoginContext);
                quorumPeer.setQuorumLearnerLoginContext(config.quorumLearnerLoginContext);
            }
            quorumPeer.setQuorumCnxnThreadsSize(config.quorumCnxnThreadsSize);
            quorumPeer.initialize();

            if (config.jvmPauseMonitorToRun) {
                quorumPeer.setJvmPauseMonitor(new JvmPauseMonitor(config));
            }

            quorumPeer.start();
            ZKAuditProvider.addZKStartStopAuditLog();
            quorumPeer.join();
        } catch (InterruptedException e) {
            // warn, but generally this is ok
            LOG.warn("Quorum Peer interrupted", e);
        } finally {
            if (metricsProvider != null) {
                try {
                    metricsProvider.stop();
                } catch (Throwable error) {
                    LOG.warn("Error while stopping metrics", error);
                }
            }
        }
    }

    // @VisibleForTesting
    protected QuorumPeer getQuorumPeer() throws SaslException {
        return new QuorumPeer();
    }

}
