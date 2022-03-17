## 7c5cecc3b3c00886a5bc39a9a8cad6ca1088b095 ##
```
Cyclomatic Complexity	2
Assertions		0
Lines of Code		21
-  
  public void testSaveNamespaceWithRenamedLease() throws Exception {
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(new Configuration())
        .numDataNodes(1).build();
    cluster.waitActive();
    DistributedFileSystem fs = (DistributedFileSystem) cluster.getFileSystem();
    OutputStream out = null;
    try {
      fs.mkdirs(new Path("/test-target"));
      out = fs.create(new Path("/test-source/foo")); // don't close
      fs.rename(new Path("/test-source/"), new Path("/test-target/"));

      fs.setSafeMode(SafeModeAction.SAFEMODE_ENTER);
      cluster.getNameNodeRpc().saveNamespace();
      fs.setSafeMode(SafeModeAction.SAFEMODE_LEAVE);
    } finally {
      IOUtils.cleanup(LOG, out, fs);
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
```
```
Cyclomatic Complexity	 2
Assertions		 0
Lines of Code		8
-  (timeout=300000)
  public void testCheckpointSucceedsWithLegacyOIVException() throws Exception {
    // Delete the OIV image dir to cause an IOException while saving
    FileUtil.fullyDelete(tmpOivImgDir);

    doEdits(0, 10);
    HATestUtil.waitForStandbyToCatchUp(nns[0], nns[1]);
    // Once the standby catches up, it should notice that it needs to
    // do a checkpoint and save one to its local directories.
    HATestUtil.waitForCheckpoint(cluster, 1, ImmutableList.of(12));

    // It should also upload it back to the active.
    HATestUtil.waitForCheckpoint(cluster, 0, ImmutableList.of(12));
  }
```
```
Cyclomatic Complexity	 1
Assertions		 3
Lines of Code		16
-  
  public void testGetConfiguration() throws ServletException {
    AuthFilter filter = new AuthFilter();
    Map<String, String> m = new HashMap<String,String>();
    m.put(DFSConfigKeys.DFS_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL_KEY,
        "xyz/thehost@REALM");
    m.put(DFSConfigKeys.DFS_WEB_AUTHENTICATION_KERBEROS_KEYTAB_KEY,
        "thekeytab");
    FilterConfig config = new DummyFilterConfig(m);
    Properties p = filter.getConfiguration("random", config);
    Assert.assertEquals("xyz/thehost@REALM",
        p.getProperty("kerberos.principal"));
    Assert.assertEquals("thekeytab", p.getProperty("kerberos.keytab"));
    Assert.assertEquals("true",
        p.getProperty(PseudoAuthenticationHandler.ANONYMOUS_ALLOWED));
  }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		11
-  
  public void testGetSimpleAuthDisabledConfiguration() throws ServletException {
    AuthFilter filter = new AuthFilter();
    Map<String, String> m = new HashMap<String,String>();
    m.put(DFSConfigKeys.DFS_WEB_AUTHENTICATION_SIMPLE_ANONYMOUS_ALLOWED,
        "false");
    FilterConfig config = new DummyFilterConfig(m);
    Properties p = filter.getConfiguration("random", config);
    Assert.assertEquals("false",
        p.getProperty(PseudoAuthenticationHandler.ANONYMOUS_ALLOWED));
  }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		9
-  
  public void testGetSimpleAuthDefaultConfiguration() throws ServletException {
    AuthFilter filter = new AuthFilter();
    Map<String, String> m = new HashMap<String,String>();
    
    FilterConfig config = new DummyFilterConfig(m);
    Properties p = filter.getConfiguration("random", config);
    Assert.assertEquals("true",
        p.getProperty(PseudoAuthenticationHandler.ANONYMOUS_ALLOWED));
  }
```
```
Cyclomatic Complexity	 2
Assertions		 2
Lines of Code		18
-  
  public void testProxyPasswordFromCredentialProvider() throws Exception {
    ClientConfiguration awsConf = new ClientConfiguration();
    // set up conf to have a cred provider
    final Configuration conf2 = new Configuration();
    final File file = tempDir.newFile("test.jks");
    final URI jks = ProviderUtils.nestURIForLocalJavaKeyStoreProvider(
        file.toURI());
    conf2.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH,
        jks.toString());

    provisionProxyPassword(conf2, "password");

    // let's set the password in config and ensure that it uses the credential
    // provider provisioned value instead.
    conf2.set(Constants.PROXY_PASSWORD, "passwordLJM");
    char[] pwd = conf2.getPassword(Constants.PROXY_PASSWORD);
    assertNotNull("Proxy password should not retrun null.", pwd);
    if (pwd != null) {
      assertEquals("Proxy password override did NOT work.", "password",
          new String(pwd));
    }
  }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		16
-  
  public void testCredsFromUserInfo() throws Exception {
    // set up conf to have a cred provider
    final Configuration conf = new Configuration();
    final File file = tempDir.newFile("test.jks");
    final URI jks = ProviderUtils.nestURIForLocalJavaKeyStoreProvider(
        file.toURI());
    conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH,
        jks.toString());

    provisionAccessKeys(conf);

    conf.set(Constants.ACCESS_KEY, EXAMPLE_ID + "LJM");
    URI uriWithUserInfo = new URI("s3a://123:456@foobar");
    S3xLoginHelper.Login creds =
        S3AUtils.getAWSAccessKeys(uriWithUserInfo, conf);
    assertEquals("AccessKey incorrect.", "123", creds.getUser());
    assertEquals("SecretKey incorrect.", "456", creds.getPassword());
  }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		17
-  
  public void testIDFromUserInfoSecretFromCredentialProvider()
      throws Exception {
    // set up conf to have a cred provider
    final Configuration conf = new Configuration();
    final File file = tempDir.newFile("test.jks");
    final URI jks = ProviderUtils.nestURIForLocalJavaKeyStoreProvider(
        file.toURI());
    conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH,
        jks.toString());

    provisionAccessKeys(conf);

    conf.set(Constants.ACCESS_KEY, EXAMPLE_ID + "LJM");
    URI uriWithUserInfo = new URI("s3a://123@foobar");
    S3xLoginHelper.Login creds =
        S3AUtils.getAWSAccessKeys(uriWithUserInfo, conf);
    assertEquals("AccessKey incorrect.", "123", creds.getUser());
    assertEquals("SecretKey incorrect.", EXAMPLE_KEY, creds.getPassword());@@@   }
```
```
Cyclomatic Complexity	 -1
Assertions		 6
Lines of Code		51
-  
  public void testReadFileChanged() throws Throwable {
    describe("overwrite a file with a shorter one during a read, seek");
    final int fullLength = 8192;
    final byte[] fullDataset = dataset(fullLength, 'a', 32);
    final int shortLen = 4096;
    final byte[] shortDataset = dataset(shortLen, 'A', 32);
    final FileSystem fs = getFileSystem();
    final Path testpath = path("readFileToChange.txt");
    // initial write
    writeDataset(fs, testpath, fullDataset, fullDataset.length, 1024, false);
    try(FSDataInputStream instream = fs.open(testpath)) {
      instream.seek(fullLength - 16);
      assertTrue("no data to read", instream.read() >= 0);
      // overwrite
      writeDataset(fs, testpath, shortDataset, shortDataset.length, 1024, true);
      // here the file length is less. Probe the file to see if this is true,
      // with a spin and wait
      LambdaTestUtils.eventually(30 * 1000, 1000,
          new Callable<Void>() {
            @Override
            public Void call() throws Exception {
              assertEquals(shortLen, fs.getFileStatus(testpath).getLen());
              return null;
            }
          });

      // here length is shorter. Assuming it has propagated to all replicas,
      // the position of the input stream is now beyond the EOF.
      // An attempt to seek backwards to a position greater than the
      // short length will raise an exception from AWS S3, which must be
      // translated into an EOF

      instream.seek(shortLen + 1024);
      int c = instream.read();
      assertIsEOF("read()", c);

      byte[] buf = new byte[256];

      assertIsEOF("read(buffer)", instream.read(buf));
      assertIsEOF("read(offset)",
          instream.read(instream.getPos(), buf, 0, buf.length));

      // now do a block read fully, again, backwards from the current pos
      try {
        instream.readFully(shortLen + 512, buf);
        fail("Expected readFully to fail");
      } catch (EOFException expected) {
        LOG.debug("Expected EOF: ", expected);
      }

      assertIsEOF("read(offset)",
          instream.read(shortLen + 510, buf, 0, buf.length));

      // seek somewhere useful
      instream.seek(shortLen - 256);

      // delete the file. Reads must fail
      fs.delete(testpath, false);

      try {
        int r = instream.read();
        fail("Expected an exception, got " + r);
      } catch (FileNotFoundException e) {
        // expected
      }

      try {
        instream.readFully(2048, buf);
        fail("Expected readFully to fail");
      } catch (FileNotFoundException e) {
        // expected
      }

    }@@@   }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		6
-  
  public void testLoginWithUserAndPass() throws Throwable {
    S3xLoginHelper.Login login = assertMatchesLogin(USER, PASS,
        WITH_USER_AND_PASS);
    assertTrue("Login of " + login, login.hasLogin());
  }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		4
-  
  public void testLoginWithSlashInPass() throws Throwable {
    assertMatchesLogin(USER, "pa//", WITH_SLASH_IN_PASS);
  }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		4
-  
  public void testLoginWithPlusInPass() throws Throwable {
    assertMatchesLogin(USER, "pa+", WITH_PLUS_IN_PASS);
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		3
-  
  public void testPathURIFixup() throws Throwable {

  }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		10
-  
  public void testSetSSLConf() {
    final DistCpOptions options = new DistCpOptions(
        Collections.singletonList(new Path("hdfs://localhost:8020/source")),
        new Path("hdfs://localhost:8020/target/"));
    Assert.assertNull(options.getSslConfigurationFile());

    options.setSslConfigurationFile("/tmp/ssl-client.xml");
    Assert.assertEquals("/tmp/ssl-client.xml",
        options.getSslConfigurationFile());@@@   }
```
```
Cyclomatic Complexity	 1
Assertions		 4
Lines of Code		34
-  
  public void testParseCopyBufferSize() {
    DistCpOptions options =
        OptionsParser.parse(new String[] {
            "hdfs://localhost:8020/source/first",
            "hdfs://localhost:8020/target/" });
    Assert.assertEquals(options.getCopyBufferSize(),
        DistCpConstants.COPY_BUFFER_SIZE_DEFAULT);

    options =
        OptionsParser.parse(new String[] { "-copybuffersize", "0",
            "hdfs://localhost:8020/source/first",
            "hdfs://localhost:8020/target/" });
    Assert.assertEquals(options.getCopyBufferSize(),
        DistCpConstants.COPY_BUFFER_SIZE_DEFAULT);

    options =
        OptionsParser.parse(new String[] { "-copybuffersize", "-1",
            "hdfs://localhost:8020/source/first",
            "hdfs://localhost:8020/target/" });
    Assert.assertEquals(options.getCopyBufferSize(),
        DistCpConstants.COPY_BUFFER_SIZE_DEFAULT);

    options =
        OptionsParser.parse(new String[] { "-copybuffersize", "4194304",
            "hdfs://localhost:8020/source/first",
            "hdfs://localhost:8020/target/" });
    Assert.assertEquals(options.getCopyBufferSize(), 4194304);

    try {
      OptionsParser
          .parse(new String[] { "-copybuffersize", "hello",
              "hdfs://localhost:8020/source/first",
              "hdfs://localhost:8020/target/" });
      Assert.fail("Non numberic copybuffersize parsed successfully!");
    } catch (IllegalArgumentException ignore) {
    }
  }
```
```
Cyclomatic Complexity	 1
Assertions		 4
Lines of Code		21
-  
  public void testVerboseLog() {
    DistCpOptions options = OptionsParser
        .parse(new String[] {"hdfs://localhost:9820/source/first",
            "hdfs://localhost:9820/target/"});
    Assert.assertFalse(options.shouldVerboseLog());

    try {
      OptionsParser
          .parse(new String[] {"-v", "hdfs://localhost:8020/source/first",
              "hdfs://localhost:8020/target/"});
      Assert.fail("-v should fail if -log option is not specified");
    } catch (IllegalArgumentException e) {
      assertExceptionContains("-v is valid only with -log option", e);
    }

    options = OptionsParser
        .parse(new String[] {"-log", "hdfs://localhost:8020/logs", "-v",
            "hdfs://localhost:8020/source/first",
            "hdfs://localhost:8020/target/"});
    Assert.assertTrue(options.shouldVerboseLog());@@@+    assertThat(options.getFiltersFile()).isEqualTo("/tmp/filters.txt");@@@   }
```
```
Cyclomatic Complexity	 1
Assertions		 8
Lines of Code		30
-  
  public void testQueryRMNodes() throws Exception {
    RMContext rmContext = Mockito.mock(RMContext.class);
    NodeId node1 = NodeId.newInstance("node1", 1234);
    RMNode rmNode1 = Mockito.mock(RMNode.class);
    ConcurrentMap<NodeId, RMNode> inactiveList =
        new ConcurrentHashMap<NodeId, RMNode>();
    Mockito.when(rmNode1.getState()).thenReturn(NodeState.SHUTDOWN);
    inactiveList.put(node1, rmNode1);
    Mockito.when(rmContext.getInactiveRMNodes()).thenReturn(inactiveList);
    List<RMNode> result = RMServerUtils.queryRMNodes(rmContext,
        EnumSet.of(NodeState.SHUTDOWN));
    Assert.assertTrue(result.size() != 0);
    Assert.assertEquals(result.get(0), rmNode1);
    Mockito.when(rmNode1.getState()).thenReturn(NodeState.DECOMMISSIONED);
    result = RMServerUtils.queryRMNodes(rmContext,
        EnumSet.of(NodeState.DECOMMISSIONED));
    Assert.assertTrue(result.size() != 0);
    Assert.assertEquals(result.get(0), rmNode1);
    Mockito.when(rmNode1.getState()).thenReturn(NodeState.LOST);
    result = RMServerUtils.queryRMNodes(rmContext,
        EnumSet.of(NodeState.LOST));
    Assert.assertTrue(result.size() != 0);
    Assert.assertEquals(result.get(0), rmNode1);
    Mockito.when(rmNode1.getState()).thenReturn(NodeState.REBOOTED);
    result = RMServerUtils.queryRMNodes(rmContext,
        EnumSet.of(NodeState.REBOOTED));
    Assert.assertTrue(result.size() != 0);
    Assert.assertEquals(result.get(0), rmNode1);@@@   }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		24
-  
  public void testAssignToBadDefaultQueue() throws Exception {
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);

    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<queuePlacementPolicy>");
    out.println("<rule name=\"specified\" create=\"false\" />");
    out.println("<rule name=\"default\" create=\"false\" />");
    out.println("</queuePlacementPolicy>");
    out.println("</allocations>");
    out.close();
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    RMApp rmApp1 = new MockRMApp(0, 0, RMAppState.NEW);

    try {
      FSLeafQueue queue1 = scheduler.assignToQueue(rmApp1, "default",
          "asterix");
    } catch (IllegalStateException ise) {
      fail("Bad queue placement policy terminal rule should not throw " +
          "exception ");
    }
  }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		13
-  
  public void testAssignToNonLeafQueueReturnsNull() throws Exception {
    conf.set(FairSchedulerConfiguration.USER_AS_DEFAULT_QUEUE, "true");
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    scheduler.getQueueManager().getLeafQueue("root.child1.granchild", true);
    scheduler.getQueueManager().getLeafQueue("root.child2", true);

    RMApp rmApp1 = new MockRMApp(0, 0, RMAppState.NEW);
    RMApp rmApp2 = new MockRMApp(1, 1, RMAppState.NEW);

    // Trying to assign to non leaf queue would return null
    assertNull(scheduler.assignToQueue(rmApp1, "root.child1", "tintin"));
    assertNotNull(scheduler.assignToQueue(rmApp2, "root.child2", "snowy"));
  }
```
```
Cyclomatic Complexity	 1
Assertions		 9
Lines of Code		46
-  
  public void testQueuePlacementWithPolicy() throws Exception {
    conf.setClass(CommonConfigurationKeys.HADOOP_SECURITY_GROUP_MAPPING,
        SimpleGroupsMapping.class, GroupMappingServiceProvider.class);
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    ApplicationAttemptId appId;

    List<QueuePlacementRule> rules = new ArrayList<QueuePlacementRule>();
    rules.add(new QueuePlacementRule.Specified().initialize(true, null));
    rules.add(new QueuePlacementRule.User().initialize(false, null));
    rules.add(new QueuePlacementRule.PrimaryGroup().initialize(false, null));
    rules.add(new QueuePlacementRule.SecondaryGroupExistingQueue().initialize(false, null));
    rules.add(new QueuePlacementRule.Default().initialize(true, null));
    Set<String> queues = Sets.newHashSet("root.user1", "root.user3group",
        "root.user4subgroup1", "root.user4subgroup2" , "root.user5subgroup2");
    Map<FSQueueType, Set<String>> configuredQueues = new HashMap<FSQueueType, Set<String>>();
    configuredQueues.put(FSQueueType.LEAF, queues);
    configuredQueues.put(FSQueueType.PARENT, new HashSet<String>());
    scheduler.getAllocationConfiguration().placementPolicy =
        new QueuePlacementPolicy(rules, configuredQueues, conf);
    appId = createSchedulingRequest(1024, "somequeue", "user1");
    assertEquals("root.somequeue", scheduler.getSchedulerApp(appId).getQueueName());
    appId = createSchedulingRequest(1024, "default", "user1");
    assertEquals("root.user1", scheduler.getSchedulerApp(appId).getQueueName());
    appId = createSchedulingRequest(1024, "default", "user3");
    assertEquals("root.user3group", scheduler.getSchedulerApp(appId).getQueueName());
    appId = createSchedulingRequest(1024, "default", "user4");
    assertEquals("root.user4subgroup1", scheduler.getSchedulerApp(appId).getQueueName());
    appId = createSchedulingRequest(1024, "default", "user5");
    assertEquals("root.user5subgroup2", scheduler.getSchedulerApp(appId).getQueueName());
    appId = createSchedulingRequest(1024, "default", "otheruser");
    assertEquals("root.default", scheduler.getSchedulerApp(appId).getQueueName());
    
    // test without specified as first rule
    rules = new ArrayList<QueuePlacementRule>();
    rules.add(new QueuePlacementRule.User().initialize(false, null));
    rules.add(new QueuePlacementRule.Specified().initialize(true, null));
    rules.add(new QueuePlacementRule.Default().initialize(true, null));
    scheduler.getAllocationConfiguration().placementPolicy =
        new QueuePlacementPolicy(rules, configuredQueues, conf);
    appId = createSchedulingRequest(1024, "somequeue", "user1");
    assertEquals("root.user1", scheduler.getSchedulerApp(appId).getQueueName());
    appId = createSchedulingRequest(1024, "somequeue", "otheruser");
    assertEquals("root.somequeue", scheduler.getSchedulerApp(appId).getQueueName());
    appId = createSchedulingRequest(1024, "default", "otheruser");
    assertEquals("root.default", scheduler.getSchedulerApp(appId).getQueueName());
  }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		28
-  
  public void testNestedUserQueue() throws IOException {
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);
    conf.setClass(CommonConfigurationKeys.HADOOP_SECURITY_GROUP_MAPPING,
        SimpleGroupsMapping.class, GroupMappingServiceProvider.class);
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<queue name=\"user1group\" type=\"parent\">");
    out.println("<minResources>1024mb,0vcores</minResources>");
    out.println("</queue>");
    out.println("<queuePlacementPolicy>");
    out.println("<rule name=\"specified\" create=\"false\" />");
    out.println("<rule name=\"nestedUserQueue\">");
    out.println("     <rule name=\"primaryGroup\" create=\"false\" />");
    out.println("</rule>");
    out.println("<rule name=\"default\" />");
    out.println("</queuePlacementPolicy>");
    out.println("</allocations>");
    out.close();

    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());
    RMApp rmApp1 = new MockRMApp(0, 0, RMAppState.NEW);

    FSLeafQueue user1Leaf = scheduler.assignToQueue(rmApp1, "root.default",
        "user1");

    assertEquals("root.user1group.user1", user1Leaf.getName());
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		10
-  
  public void testAppAttempts() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    RMApp app1 = rm.submitApp(CONTAINER_MB, "testwordcount", "user1");
    amNodeManager.nodeHeartbeat(true);
    testAppAttemptsHelper(app1.getApplicationId().toString(), app1,
        MediaType.APPLICATION_JSON);
    rm.stop();
  }
```
```
Cyclomatic Complexity	 4
Assertions		 2
Lines of Code		28
-   (timeout = 20000)
  public void testMultipleAppAttempts() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 8192);
    RMApp app1 = rm.submitApp(CONTAINER_MB, "testwordcount", "user1");
    MockAM am = MockRM.launchAndRegisterAM(app1, rm, amNodeManager);
    int maxAppAttempts = rm.getConfig().getInt(
        YarnConfiguration.RM_AM_MAX_ATTEMPTS,
        YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS);
    assertTrue(maxAppAttempts > 1);
    int numAttempt = 1;
    while (true) {
      // fail the AM by sending CONTAINER_FINISHED event without registering.
      amNodeManager.nodeHeartbeat(am.getApplicationAttemptId(), 1, ContainerState.COMPLETE);
      rm.waitForState(am.getApplicationAttemptId(), RMAppAttemptState.FAILED);
      if (numAttempt == maxAppAttempts) {
        rm.waitForState(app1.getApplicationId(), RMAppState.FAILED);
        break;
      }
      // wait for app to start a new attempt.
      rm.waitForState(app1.getApplicationId(), RMAppState.ACCEPTED);
      am = MockRM.launchAndRegisterAM(app1, rm, amNodeManager);
      numAttempt++;
    }
    assertEquals("incorrect number of attempts", maxAppAttempts,
        app1.getAppAttempts().values().size());
    testAppAttemptsHelper(app1.getApplicationId().toString(), app1,
        MediaType.APPLICATION_JSON);
    rm.stop();
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		10
-  
  public void testAppAttemptsSlash() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    RMApp app1 = rm.submitApp(CONTAINER_MB);
    amNodeManager.nodeHeartbeat(true);
    testAppAttemptsHelper(app1.getApplicationId().toString() + "/", app1,
        MediaType.APPLICATION_JSON);
    rm.stop();
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		9
-  
  public void testAppAttemtpsDefault() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    RMApp app1 = rm.submitApp(CONTAINER_MB);
    amNodeManager.nodeHeartbeat(true);
    testAppAttemptsHelper(app1.getApplicationId().toString() + "/", app1, "");
    rm.stop();
  }
```
```
Cyclomatic Complexity	 1
Assertions		 3
Lines of Code		34
-  
  public void testInvalidAppIdGetAttempts() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    RMApp app = rm.submitApp(CONTAINER_MB);
    amNodeManager.nodeHeartbeat(true);
    WebResource r = resource();

    try {
      r.path("ws").path("v1").path("cluster").path("apps")
          .path("application_invalid_12").path("appattempts")
          .accept(MediaType.APPLICATION_JSON)
          .get(JSONObject.class);
      fail("should have thrown exception on invalid appAttempt");
    } catch (UniformInterfaceException ue) {
      ClientResponse response = ue.getResponse();

      assertEquals(Status.BAD_REQUEST, response.getClientResponseStatus());
      assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
      JSONObject msg = response.getEntity(JSONObject.class);
      JSONObject exception = msg.getJSONObject("RemoteException");
      assertEquals("incorrect number of elements", 3, exception.length());
      String message = exception.getString("message");
      String type = exception.getString("exception");
      String classname = exception.getString("javaClassName");
      WebServicesTestUtils.checkStringMatch("exception message",
          "java.lang.IllegalArgumentException: Invalid ApplicationId:"
              + " application_invalid_12",
          message);
      WebServicesTestUtils.checkStringMatch("exception type",
          "BadRequestException", type);
      WebServicesTestUtils.checkStringMatch("exception classname",
          "org.apache.hadoop.yarn.webapp.BadRequestException", classname);

    } finally {
      rm.stop();
    }
  }
```
```
Cyclomatic Complexity	 1
Assertions		 3
Lines of Code		34
-  
  public void testInvalidAppAttemptId() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    RMApp app = rm.submitApp(CONTAINER_MB);
    amNodeManager.nodeHeartbeat(true);
    WebResource r = resource();

    try {
      r.path("ws").path("v1").path("cluster").path("apps")
          .path(app.getApplicationId().toString()).path("appattempts")
          .path("appattempt_invalid_12_000001")
          .accept(MediaType.APPLICATION_JSON).get(JSONObject.class);
      fail("should have thrown exception on invalid appAttempt");
    } catch (UniformInterfaceException ue) {
      ClientResponse response = ue.getResponse();

      assertEquals(Status.BAD_REQUEST, response.getClientResponseStatus());
      assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
      JSONObject msg = response.getEntity(JSONObject.class);
      JSONObject exception = msg.getJSONObject("RemoteException");
      assertEquals("incorrect number of elements", 3, exception.length());
      String message = exception.getString("message");
      String type = exception.getString("exception");
      String classname = exception.getString("javaClassName");
      WebServicesTestUtils.checkStringMatch("exception message",
          "java.lang.IllegalArgumentException: Invalid AppAttemptId:"
              + " appattempt_invalid_12_000001",
          message);
      WebServicesTestUtils.checkStringMatch("exception type",
          "BadRequestException", type);
      WebServicesTestUtils.checkStringMatch("exception classname",
          "org.apache.hadoop.yarn.webapp.BadRequestException", classname);

    } finally {
      rm.stop();
    }
  }
```
```
Cyclomatic Complexity	 1
Assertions		 3
Lines of Code		33
-  
  public void testNonexistAppAttempts() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    rm.submitApp(CONTAINER_MB, "testwordcount", "user1");
    amNodeManager.nodeHeartbeat(true);
    WebResource r = resource();

    try {
      r.path("ws").path("v1").path("cluster").path("apps")
          .path("application_00000_0099").accept(MediaType.APPLICATION_JSON)
          .get(JSONObject.class);
      fail("should have thrown exception on invalid appid");
    } catch (UniformInterfaceException ue) {
      ClientResponse response = ue.getResponse();

      assertEquals(Status.NOT_FOUND, response.getClientResponseStatus());
      assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());

      JSONObject msg = response.getEntity(JSONObject.class);
      JSONObject exception = msg.getJSONObject("RemoteException");
      assertEquals("incorrect number of elements", 3, exception.length());
      String message = exception.getString("message");
      String type = exception.getString("exception");
      String classname = exception.getString("javaClassName");
      WebServicesTestUtils.checkStringMatch("exception message",
          "java.lang.Exception: app with id: application_00000_0099 not found",
          message);
      WebServicesTestUtils.checkStringMatch("exception type",
          "NotFoundException", type);
      WebServicesTestUtils.checkStringMatch("exception classname",
          "org.apache.hadoop.yarn.webapp.NotFoundException", classname);
    } finally {
      rm.stop();
    }
  }
```
```
Cyclomatic Complexity	 1
Assertions		 4
Lines of Code		26
-  
  public void testAppAttemptsXML() throws JSONException, Exception {
    rm.start();
    String user = "user1";
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    RMApp app1 = rm.submitApp(CONTAINER_MB, "testwordcount", user);
    amNodeManager.nodeHeartbeat(true);
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .path("apps").path(app1.getApplicationId().toString())
        .path("appattempts").accept(MediaType.APPLICATION_XML)
        .get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_XML_TYPE, response.getType());
    String xml = response.getEntity(String.class);

    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    DocumentBuilder db = dbf.newDocumentBuilder();
    InputSource is = new InputSource();
    is.setCharacterStream(new StringReader(xml));
    Document dom = db.parse(is);
    NodeList nodes = dom.getElementsByTagName("appAttempts");
    assertEquals("incorrect number of elements", 1, nodes.getLength());
    NodeList attempt = dom.getElementsByTagName("appAttempt");
    assertEquals("incorrect number of elements", 1, attempt.getLength());
    verifyAppAttemptsXML(attempt, app1.getCurrentAppAttempt(), user);
    rm.stop();
  }
```
## 3dc30bc24e50343efe1f514b923d27a0786d3ac1 ##
```
Cyclomatic Complexity	1
Assertions		5
Lines of Code		21
-   (timeout = 30000)
  public void testCopyMergeSingleDirectory() throws IOException {
    setupDirs();
    boolean copyMergeResult = copyMerge("partitioned", "tmp/merged");
    Assert.assertTrue("Expected successful copyMerge result.", copyMergeResult);
    File merged = new File(TEST_DIR, "tmp/merged");
    Assert.assertTrue("File tmp/merged must exist after copyMerge.",
        merged.exists());
    BufferedReader rdr = new BufferedReader(new FileReader(merged));

    try {
      Assert.assertEquals("Line 1 of merged file must contain \"foo\".",
          "foo", rdr.readLine());
      Assert.assertEquals("Line 2 of merged file must contain \"bar\".",
          "bar", rdr.readLine());
      Assert.assertNull("Expected end of file reading merged file.",
          rdr.readLine());
    }
    finally {
      rdr.close();
    }
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		13
-  
  public void testRetryUpToMaximumTimeWithFixedSleep() throws UnreliableException {
    UnreliableInterface unreliable = (UnreliableInterface)
      RetryProxy.create(UnreliableInterface.class, unreliableImpl,
                        retryUpToMaximumTimeWithFixedSleep(80, 10, TimeUnit.NANOSECONDS));
    unreliable.alwaysSucceeds();
    unreliable.failsOnceThenSucceeds();
    try {
      unreliable.failsTenTimesThenSucceeds();
      fail("Should fail");
    } catch (UnreliableException e) {
      // expected
    }
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		10
-  
  public void testStandaloneClient() throws IOException {
    try {
      TestProtocol proxy = RPC.waitForProxy(TestProtocol.class,
        TestProtocol.versionID, new InetSocketAddress(ADDRESS, 20), conf, 15000L);
      proxy.echo("");
      fail("We should not have reached here");
    } catch (ConnectException ioe) {
      //this is what we expected
    }
  }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		13
-  
  public void testServerAddress() throws IOException {
    Server server = new RPC.Builder(conf).setProtocol(TestProtocol.class)
        .setInstance(new TestImpl()).setBindAddress(ADDRESS).setPort(0)
        .setNumHandlers(5).setVerbose(true).build();
    InetSocketAddress bindAddr = null;
    try {
      bindAddr = NetUtils.getConnectAddress(server);
    } finally {
      server.stop();
    }
    assertEquals(InetAddress.getLocalHost(), bindAddr.getAddress());@@@   }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		15
-    // old client vs new server
  public void testVersion0ClientVersion1Server() throws Exception {
    // create a server with two handlers
    TestImpl1 impl = new TestImpl1();
    server = new RPC.Builder(conf).setProtocol(TestProtocol1.class)
        .setInstance(impl).setBindAddress(ADDRESS).setPort(0).setNumHandlers(2)
        .setVerbose(false).build();
    server.addProtocol(RPC.RpcKind.RPC_WRITABLE, TestProtocol0.class, impl);
    server.start();
    addr = NetUtils.getConnectAddress(server);@@@ 
    proxy = RPC.getProtocolProxy(
        TestProtocol0.class, TestProtocol0.versionID, addr, conf);

    TestProtocol0 proxy0 = (TestProtocol0)proxy.getProxy();
    proxy0.ping();
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		17
-    // old client vs new server
  public void testVersion1ClientVersion0Server() throws Exception {
    // create a server with two handlers
    server = new RPC.Builder(conf).setProtocol(TestProtocol0.class)
        .setInstance(new TestImpl0()).setBindAddress(ADDRESS).setPort(0)
        .setNumHandlers(2).setVerbose(false).build();
    server.start();
    addr = NetUtils.getConnectAddress(server);

    proxy = RPC.getProtocolProxy(
        TestProtocol1.class, TestProtocol1.versionID, addr, conf);

    TestProtocol1 proxy1 = (TestProtocol1)proxy.getProxy();
    proxy1.ping();
    try {
      proxy1.echo("hello");
      fail("Echo should fail");
    } catch(IOException e) {
    }
  }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		14
-   // Compatible new client & old server
  public void testVersion2ClientVersion1Server() throws Exception {
    // create a server with two handlers
    TestImpl1 impl = new TestImpl1();
    server = new RPC.Builder(conf).setProtocol(TestProtocol1.class)
        .setInstance(impl).setBindAddress(ADDRESS).setPort(0).setNumHandlers(2)
        .setVerbose(false).build();
    server.addProtocol(RPC.RpcKind.RPC_WRITABLE, TestProtocol0.class, impl);
    server.start();
    addr = NetUtils.getConnectAddress(server);


    Version2Client client = new Version2Client();
    client.ping();
    assertEquals("hello", client.echo("hello"));
    
    // echo(int) is not supported by server, so returning 3
    // This verifies that echo(int) and echo(String)'s hash codes are different
    assertEquals(3, client.echo(3));
  }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		14
-   // equal version client and server
  public void testVersion2ClientVersion2Server() throws Exception {
    // create a server with two handlers
    TestImpl2 impl = new TestImpl2();
    server = new RPC.Builder(conf).setProtocol(TestProtocol2.class)
        .setInstance(impl).setBindAddress(ADDRESS).setPort(0).setNumHandlers(2)
        .setVerbose(false).build();
    server.addProtocol(RPC.RpcKind.RPC_WRITABLE, TestProtocol0.class, impl);
    server.start();
    addr = NetUtils.getConnectAddress(server);

    Version2Client client = new Version2Client();

    client.ping();
    assertEquals("hello", client.echo("hello"));
    
    // now that echo(int) is supported by the server, echo(int) should return -3
    assertEquals(-3, client.echo(3));
  }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		21
-  
  public void testVersionMismatch() throws IOException {
    server = new RPC.Builder(conf).setProtocol(TestProtocol2.class)
        .setInstance(new TestImpl2()).setBindAddress(ADDRESS).setPort(0)
        .setNumHandlers(2).setVerbose(false).build();
    server.start();
    addr = NetUtils.getConnectAddress(server);

    TestProtocol4 proxy = RPC.getProxy(TestProtocol4.class,
        TestProtocol4.versionID, addr, conf);
    try {
      proxy.echo(21);
      fail("The call must throw VersionMismatch exception");
    } catch (RemoteException ex) {
      Assert.assertEquals(RPC.VersionMismatch.class.getName(), 
          ex.getClassName());
      Assert.assertTrue(ex.getErrorCode().equals(
          RpcErrorCodeProto.ERROR_RPC_VERSION_MISMATCH));
    }  catch (IOException ex) {
      fail("Expected version mismatch but got " + ex);
    }
  }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		18
-  
  public void testIsMethodSupported() throws IOException {
    server = new RPC.Builder(conf).setProtocol(TestProtocol2.class)
        .setInstance(new TestImpl2()).setBindAddress(ADDRESS).setPort(0)
        .setNumHandlers(2).setVerbose(false).build();
    server.start();
    addr = NetUtils.getConnectAddress(server);

    TestProtocol2 proxy = RPC.getProxy(TestProtocol2.class,
        TestProtocol2.versionID, addr, conf);
    boolean supported = RpcClientUtil.isMethodSupported(proxy,
        TestProtocol2.class, RPC.RpcKind.RPC_WRITABLE,
        RPC.getProtocolVersion(TestProtocol2.class), "echo");
    Assert.assertTrue(supported);
    supported = RpcClientUtil.isMethodSupported(proxy,
        TestProtocol2.class, RPC.RpcKind.RPC_PROTOCOL_BUFFER,
        RPC.getProtocolVersion(TestProtocol2.class), "echo");
    Assert.assertFalse(supported);
  }
```
```
Cyclomatic Complexity	 3
Assertions		 4
Lines of Code		33
-  
  public void testProtocolMetaInfoSSTranslatorPB() throws Exception {
    TestImpl1 impl = new TestImpl1();
    server = new RPC.Builder(conf).setProtocol(TestProtocol1.class)
        .setInstance(impl).setBindAddress(ADDRESS).setPort(0).setNumHandlers(2)
        .setVerbose(false).build();
    server.addProtocol(RPC.RpcKind.RPC_WRITABLE, TestProtocol0.class, impl);
    server.start();

    ProtocolMetaInfoServerSideTranslatorPB xlator = 
        new ProtocolMetaInfoServerSideTranslatorPB(server);

    GetProtocolSignatureResponseProto resp = xlator.getProtocolSignature(
        null,
        createGetProtocolSigRequestProto(TestProtocol1.class,
            RPC.RpcKind.RPC_PROTOCOL_BUFFER));
    //No signatures should be found
    Assert.assertEquals(0, resp.getProtocolSignatureCount());
    resp = xlator.getProtocolSignature(
        null,
        createGetProtocolSigRequestProto(TestProtocol1.class,
            RPC.RpcKind.RPC_WRITABLE));
    Assert.assertEquals(1, resp.getProtocolSignatureCount());
    ProtocolSignatureProto sig = resp.getProtocolSignatureList().get(0);
    Assert.assertEquals(TestProtocol1.versionID, sig.getVersion());
    boolean found = false;
    int expected = ProtocolSignature.getFingerprint(TestProtocol1.class
        .getMethod("echo", String.class));
    for (int m : sig.getMethodsList()) {
      if (expected == m) {
        found = true;
        break;
      }
    }
    Assert.assertTrue(found);
  }
```
```
Cyclomatic Complexity	 1
Assertions		 7
Lines of Code		26
-  
  public void testGetAllStoragePolicies() throws Exception {
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(0).build();
    cluster.waitActive();
    final DistributedFileSystem fs = cluster.getFileSystem();
    try {
      BlockStoragePolicy[] policies = fs.getStoragePolicies();
      Assert.assertEquals(6, policies.length);
      Assert.assertEquals(POLICY_SUITE.getPolicy(COLD).toString(),
          policies[0].toString());
      Assert.assertEquals(POLICY_SUITE.getPolicy(WARM).toString(),
          policies[1].toString());
      Assert.assertEquals(POLICY_SUITE.getPolicy(HOT).toString(),
          policies[2].toString());
      Assert.assertEquals(POLICY_SUITE.getPolicy(ONESSD).toString(),
          policies[3].toString());
      Assert.assertEquals(POLICY_SUITE.getPolicy(ALLSSD).toString(),
          policies[4].toString());
      Assert.assertEquals(POLICY_SUITE.getPolicy(LAZY_PERSIST).toString(),
          policies[5].toString());
    } finally {
      IOUtils.cleanup(null, fs);
      cluster.shutdown();
    }@@@   }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		6
-  (timeout=100000)
  public void testExitZeroOnSuccess() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    
    initConf(conf);
    
    oneNodeTest(conf, true);
  }
```
```
Cyclomatic Complexity	 2
Assertions		 1
Lines of Code		18
-  
  public void testAuditHftp() throws Exception {
    final Path file = new Path(fnames[0]);

    final String hftpUri =
      "hftp://" + conf.get(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY);

    HftpFileSystem hftpFs = null;

    setupAuditLogs();
    try {
      hftpFs = (HftpFileSystem) new Path(hftpUri).getFileSystem(conf);
      InputStream istream = hftpFs.open(file);
      @SuppressWarnings("unused")
      int val = istream.read();
      istream.close();

      verifyAuditLogs(true);
    } finally {
      if (hftpFs != null) hftpFs.close();
    }@@@   }
```
```
Cyclomatic Complexity	 5
Assertions		 4
Lines of Code		68
-  
  public void testTransitionWithUnreachableZK() throws Exception {
    final AtomicBoolean zkUnreachable = new AtomicBoolean(false);
    final AtomicBoolean threadHung = new AtomicBoolean(false);
    final Object hangLock = new Object();
    final Configuration conf = createHARMConf("rm1,rm2", "rm1", 1234);
    conf.setBoolean(YarnConfiguration.AUTO_FAILOVER_ENABLED, false);

    // Create a state store that can simulate losing contact with the ZK node
    TestZKRMStateStoreTester zkTester = new TestZKRMStateStoreTester() {
      @Override
      public RMStateStore getRMStateStore() throws Exception {
        YarnConfiguration storeConf = new YarnConfiguration(conf);
        storeConf.set(YarnConfiguration.RM_ZK_ADDRESS, hostPort);
        storeConf.set(YarnConfiguration.ZK_RM_STATE_STORE_PARENT_PATH,
            workingZnode);
        storeConf.setInt(YarnConfiguration.RM_ZK_TIMEOUT_MS, 500);
        this.client = createClient();
        this.store = new TestZKRMStateStoreInternal(storeConf, workingZnode) {
          @Override
          synchronized void doStoreMultiWithRetries(final List<Op> opList)
              throws Exception {
            if (zkUnreachable.get()) {
              // Let the test know that it can now proceed
              threadHung.set(true);

              synchronized (hangLock) {
                hangLock.notify();
              }

              // Take a long nap while holding the lock to simulate the ZK node
              // being unreachable. This behavior models what happens in
              // super.doStoreMultiWithRetries() when the ZK node it unreachble.
              // If that behavior changes, then this test should also change or
              // be phased out.
              Thread.sleep(60000);
            } else {
              // Business as usual
              super.doStoreMultiWithRetries(opList);
            }
          }
        };
        return this.store;
      }
    };

    // Start with a single RM in HA mode
    final RMStateStore store = zkTester.getRMStateStore();
    final MockRM rm = new MockRM(conf, store);
    rm.start();

    // Make the RM active
    final StateChangeRequestInfo req = new StateChangeRequestInfo(
        HAServiceProtocol.RequestSource.REQUEST_BY_USER);

    rm.getRMContext().getRMAdminService().transitionToActive(req);
    assertEquals("RM with ZKStore didn't start",
        Service.STATE.STARTED, rm.getServiceState());
    assertEquals("RM should be Active",
        HAServiceProtocol.HAServiceState.ACTIVE,
        rm.getRMContext().getRMAdminService().getServiceStatus().getState());

    // Simulate the ZK node going dark and wait for the
    // VerifyActiveStatusThread to hang
    zkUnreachable.set(true);

    synchronized (hangLock) {
      while (!threadHung.get()) {
        hangLock.wait();
      }
    }

    assertTrue("Unable to perform test because Verify Active Status Thread "
        + "did not run", threadHung.get());

    // Try to transition the RM to standby.  Give up after 1000ms.
    Thread standby = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          rm.getRMContext().getRMAdminService().transitionToStandby(req);
        } catch (IOException ex) {
          // OK to exit
        }
      }
    }, "Test Unreachable ZK Thread");

    standby.start();
    standby.join(1000);

    assertFalse("The thread initiating the transition to standby is hung",
        standby.isAlive());
    zkUnreachable.set(false);
  }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		17
-  
  public void testNoAuthExceptionInNonHAMode() throws Exception {
    TestZKRMStateStoreTester zkTester = new TestZKRMStateStoreTester();
    String appRoot = zkTester.getWorkingZNode() + "/ZKRMStateRoot/RMAppRoot" ;
    ZooKeeper zk = spy(createClient());
    doThrow(new KeeperException.NoAuthException()).when(zk).
        create(appRoot, null, RMZKUtils.getZKAcls(new Configuration()),
            CreateMode.PERSISTENT);
    try {
      zkTester.getRMStateStore(zk);
      fail("Expected exception to be thrown");
    } catch(ServiceStateException e) {
      assertNotNull(e.getCause());
      assertTrue("Expected NoAuthException",
          e.getCause() instanceof KeeperException.NoAuthException);
    }
  }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		10
-  
  public void testSimplePass2() throws IOException, PlanningException {
    // generate allocation from single tenant that exceed avg momentarily but
    // fit within
    // max instantanesou
    int[] f = generateData(3600, (int) Math.ceil(0.69 * totCont));
    assertTrue(plan.toString(),
        plan.addReservation(new InMemoryReservationAllocation(
            ReservationSystemTestUtil.getNewReservationId(), null, "u1",
            "dedicated", initTime, initTime + f.length,
            ReservationSystemTestUtil.generateAllocation(initTime, step, f),
            res, minAlloc)));
  }
```
```
Cyclomatic Complexity	 2
Assertions		 1
Lines of Code		12
-  
  public void testMultiTenantPass() throws IOException, PlanningException {
    // generate allocation from multiple tenants that barely fit in tot capacity
    int[] f = generateData(3600, (int) Math.ceil(0.25 * totCont));
    for (int i = 0; i < 4; i++) {
      assertTrue(plan.toString(),
          plan.addReservation(new InMemoryReservationAllocation(
              ReservationSystemTestUtil.getNewReservationId(), null, "u" + i,
              "dedicated", initTime, initTime + f.length,
              ReservationSystemTestUtil.generateAllocation(initTime, step, f),
              res, minAlloc)));
    }
  }
```
```
Cyclomatic Complexity	 2
Assertions		 1
Lines of Code		12
-  (expected = ResourceOverCommitException.class)
  public void testMultiTenantFail() throws IOException, PlanningException {
    // generate allocation from multiple tenants that exceed tot capacity
    int[] f = generateData(3600, (int) Math.ceil(0.25 * totCont));
    for (int i = 0; i < 5; i++) {
      assertTrue(plan.toString(),
          plan.addReservation(new InMemoryReservationAllocation(
              ReservationSystemTestUtil.getNewReservationId(), null, "u" + i,
              "dedicated", initTime, initTime + f.length,
              ReservationSystemTestUtil.generateAllocation(initTime, step, f),
              res, minAlloc)));
    }
  }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		11
-  (expected = PlanningQuotaException.class)
  public void testInstFail() throws IOException, PlanningException {
    // generate allocation that exceed the instantaneous cap single-show
    int[] f = generateData(3600, (int) Math.ceil(0.71 * totCont));
    assertTrue(plan.toString(),
        plan.addReservation(new InMemoryReservationAllocation(
            ReservationSystemTestUtil.getNewReservationId(), null, "u1",
            "dedicated", initTime, initTime + f.length,
            ReservationSystemTestUtil.generateAllocation(initTime, step, f),
            res, minAlloc)));
    Assert.fail("should not have accepted this");
  }
```
```
Cyclomatic Complexity	 1
Assertions		 3
Lines of Code		26
-  
  public void testInstFailBySum() throws IOException, PlanningException {
    // generate allocation that exceed the instantaneous cap by sum
    int[] f = generateData(3600, (int) Math.ceil(0.3 * totCont));

    assertTrue(plan.toString(),
        plan.addReservation(new InMemoryReservationAllocation(
            ReservationSystemTestUtil.getNewReservationId(), null, "u1",
            "dedicated", initTime, initTime + f.length,
            ReservationSystemTestUtil.generateAllocation(initTime, step, f),
            res, minAlloc)));
    assertTrue(plan.toString(),
        plan.addReservation(new InMemoryReservationAllocation(
            ReservationSystemTestUtil.getNewReservationId(), null, "u1",
            "dedicated", initTime, initTime + f.length,
            ReservationSystemTestUtil.generateAllocation(initTime, step, f),
            res, minAlloc)));
    try {
      assertTrue(plan.toString(),
          plan.addReservation(new InMemoryReservationAllocation(
              ReservationSystemTestUtil.getNewReservationId(), null, "u1",
              "dedicated", initTime, initTime + f.length,
              ReservationSystemTestUtil.generateAllocation(initTime, step, f),
              res, minAlloc)));
      Assert.fail();
    } catch (PlanningQuotaException p) {
      // expected
    }
  }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		13
-  (expected = PlanningQuotaException.class)
  public void testFailAvg() throws IOException, PlanningException {
    // generate an allocation which violates the 25% average single-shot
    Map<ReservationInterval, ReservationRequest> req =
        new TreeMap<ReservationInterval, ReservationRequest>();
    long win = timeWindow / 2 + 100;
    int cont = (int) Math.ceil(0.5 * totCont);
    req.put(new ReservationInterval(initTime, initTime + win),
        ReservationRequest.newInstance(Resource.newInstance(1024, 1), cont));

    assertTrue(plan.toString(),
        plan.addReservation(new InMemoryReservationAllocation(
            ReservationSystemTestUtil.getNewReservationId(), null, "u1",
            "dedicated", initTime, initTime + win, req, res, minAlloc)));
  }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		21
-  
  public void testFailAvgBySum() throws IOException, PlanningException {
    // generate an allocation which violates the 25% average by sum
    Map<ReservationInterval, ReservationRequest> req =
        new TreeMap<ReservationInterval, ReservationRequest>();
    long win = 86400000 / 4 + 1;
    int cont = (int) Math.ceil(0.5 * totCont);
    req.put(new ReservationInterval(initTime, initTime + win),
        ReservationRequest.newInstance(Resource.newInstance(1024, 1), cont));
    assertTrue(plan.toString(),
        plan.addReservation(new InMemoryReservationAllocation(
            ReservationSystemTestUtil.getNewReservationId(), null, "u1",
            "dedicated", initTime, initTime + win, req, res, minAlloc)));

    try {
      assertTrue(plan.toString(),
          plan.addReservation(new InMemoryReservationAllocation(
              ReservationSystemTestUtil.getNewReservationId(), null, "u1",
              "dedicated", initTime, initTime + win, req, res, minAlloc)));

      Assert.fail("should not have accepted this");
    } catch (PlanningQuotaException e) {
      // expected
    }
  }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		11
-  
  public void testSingleUserBarelyFitPass() throws IOException,
      PlanningException {
    // generate allocation from single tenant that barely fit
    int[] f = generateData(3600, totCont);
    assertTrue(plan.toString(),
        plan.addReservation(new InMemoryReservationAllocation(
            ReservationSystemTestUtil.getNewReservationId(), null, "u1",
            "dedicated", initTime, initTime + f.length,
            ReservationSystemTestUtil.generateAllocation(initTime, step, f),
            res, minAlloc)));
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		8
-  (expected = ResourceOverCommitException.class)
  public void testSingleFail() throws IOException, PlanningException {
    // generate allocation from single tenant that exceed capacity
    int[] f = generateData(3600, (int) (1.1 * totCont));
    plan.addReservation(new InMemoryReservationAllocation(
        ReservationSystemTestUtil.getNewReservationId(), null, "u1",
        "dedicated", initTime, initTime + f.length, ReservationSystemTestUtil
            .generateAllocation(initTime, step, f), res, minAlloc));
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		11
-  (expected = MismatchedUserException.class)
  public void testUserMismatch() throws IOException, PlanningException {
    // generate allocation from single tenant that exceed capacity
    int[] f = generateData(3600, (int) (0.5 * totCont));

    ReservationId rid = ReservationSystemTestUtil.getNewReservationId();
    plan.addReservation(new InMemoryReservationAllocation(rid, null, "u1",
        "dedicated", initTime, initTime + f.length, ReservationSystemTestUtil
            .generateAllocation(initTime, step, f), res, minAlloc));

    // trying to update a reservation with a mismatching user
    plan.updateReservation(new InMemoryReservationAllocation(rid, null, "u2",
        "dedicated", initTime, initTime + f.length, ReservationSystemTestUtil
            .generateAllocation(initTime, step, f), res, minAlloc));
  }
```
```
Cyclomatic Complexity	 2
Assertions		 1
Lines of Code		12
-  
  public void testMultiTenantPass() throws IOException, PlanningException {
    // generate allocation from multiple tenants that barely fit in tot capacity
    int[] f = generateData(3600, (int) Math.ceil(0.25 * totCont));
    for (int i = 0; i < 4; i++) {
      assertTrue(plan.toString(),
          plan.addReservation(new InMemoryReservationAllocation(
              ReservationSystemTestUtil.getNewReservationId(), null, "u" + i,
              "dedicated", initTime, initTime + f.length,
              ReservationSystemTestUtil.generateAllocation(initTime, step, f),
              res, minAlloc)));
    }
  }
```
```
Cyclomatic Complexity	 2
Assertions		 1
Lines of Code		12
-  (expected = ResourceOverCommitException.class)
  public void testMultiTenantFail() throws IOException, PlanningException {
    // generate allocation from multiple tenants that exceed tot capacity
    int[] f = generateData(3600, (int) Math.ceil(0.25 * totCont));
    for (int i = 0; i < 5; i++) {
      assertTrue(plan.toString(),
          plan.addReservation(new InMemoryReservationAllocation(
              ReservationSystemTestUtil.getNewReservationId(), null, "u" + i,
              "dedicated", initTime, initTime + f.length,
              ReservationSystemTestUtil.generateAllocation(initTime, step, f),
              res, minAlloc)));
    }
  }
```
## 4f3dfb7c1cce2543f7c6eba786779425c14dd7c4 ##
```
Cyclomatic Complexity	2
Assertions		0
Lines of Code		21
-  
  public void testSaveNamespaceWithRenamedLease() throws Exception {
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(new Configuration())
        .numDataNodes(1).build();
    cluster.waitActive();
    DistributedFileSystem fs = (DistributedFileSystem) cluster.getFileSystem();
    OutputStream out = null;
    try {
      fs.mkdirs(new Path("/test-target"));
      out = fs.create(new Path("/test-source/foo")); // don't close
      fs.rename(new Path("/test-source/"), new Path("/test-target/"));

      fs.setSafeMode(SafeModeAction.SAFEMODE_ENTER);
      cluster.getNameNodeRpc().saveNamespace();
      fs.setSafeMode(SafeModeAction.SAFEMODE_LEAVE);
    } finally {
      IOUtils.cleanup(LOG, out, fs);
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
```
```
Cyclomatic Complexity	 2
Assertions		 0
Lines of Code		8
-  (timeout=300000)
  public void testCheckpointSucceedsWithLegacyOIVException() throws Exception {
    // Delete the OIV image dir to cause an IOException while saving
    FileUtil.fullyDelete(tmpOivImgDir);

    doEdits(0, 10);
    HATestUtil.waitForStandbyToCatchUp(nns[0], nns[1]);
    // Once the standby catches up, it should notice that it needs to
    // do a checkpoint and save one to its local directories.
    HATestUtil.waitForCheckpoint(cluster, 1, ImmutableList.of(12));

    // It should also upload it back to the active.
    HATestUtil.waitForCheckpoint(cluster, 0, ImmutableList.of(12));
  }
```
```
Cyclomatic Complexity	 1
Assertions		 3
Lines of Code		16
-  
  public void testGetConfiguration() throws ServletException {
    AuthFilter filter = new AuthFilter();
    Map<String, String> m = new HashMap<String,String>();
    m.put(DFSConfigKeys.DFS_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL_KEY,
        "xyz/thehost@REALM");
    m.put(DFSConfigKeys.DFS_WEB_AUTHENTICATION_KERBEROS_KEYTAB_KEY,
        "thekeytab");
    FilterConfig config = new DummyFilterConfig(m);
    Properties p = filter.getConfiguration("random", config);
    Assert.assertEquals("xyz/thehost@REALM",
        p.getProperty("kerberos.principal"));
    Assert.assertEquals("thekeytab", p.getProperty("kerberos.keytab"));
    Assert.assertEquals("true",
        p.getProperty(PseudoAuthenticationHandler.ANONYMOUS_ALLOWED));
  }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		11
-  
  public void testGetSimpleAuthDisabledConfiguration() throws ServletException {
    AuthFilter filter = new AuthFilter();
    Map<String, String> m = new HashMap<String,String>();
    m.put(DFSConfigKeys.DFS_WEB_AUTHENTICATION_SIMPLE_ANONYMOUS_ALLOWED,
        "false");
    FilterConfig config = new DummyFilterConfig(m);
    Properties p = filter.getConfiguration("random", config);
    Assert.assertEquals("false",
        p.getProperty(PseudoAuthenticationHandler.ANONYMOUS_ALLOWED));
  }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		9
-  
  public void testGetSimpleAuthDefaultConfiguration() throws ServletException {
    AuthFilter filter = new AuthFilter();
    Map<String, String> m = new HashMap<String,String>();
    
    FilterConfig config = new DummyFilterConfig(m);
    Properties p = filter.getConfiguration("random", config);
    Assert.assertEquals("true",
        p.getProperty(PseudoAuthenticationHandler.ANONYMOUS_ALLOWED));
  }
```
```
Cyclomatic Complexity	 2
Assertions		 2
Lines of Code		18
-  
  public void testProxyPasswordFromCredentialProvider() throws Exception {
    ClientConfiguration awsConf = new ClientConfiguration();
    // set up conf to have a cred provider
    final Configuration conf2 = new Configuration();
    final File file = tempDir.newFile("test.jks");
    final URI jks = ProviderUtils.nestURIForLocalJavaKeyStoreProvider(
        file.toURI());
    conf2.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH,
        jks.toString());

    provisionProxyPassword(conf2, "password");

    // let's set the password in config and ensure that it uses the credential
    // provider provisioned value instead.
    conf2.set(Constants.PROXY_PASSWORD, "passwordLJM");
    char[] pwd = conf2.getPassword(Constants.PROXY_PASSWORD);
    assertNotNull("Proxy password should not retrun null.", pwd);
    if (pwd != null) {
      assertEquals("Proxy password override did NOT work.", "password",
          new String(pwd));
    }
  }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		16
-  
  public void testCredsFromUserInfo() throws Exception {
    // set up conf to have a cred provider
    final Configuration conf = new Configuration();
    final File file = tempDir.newFile("test.jks");
    final URI jks = ProviderUtils.nestURIForLocalJavaKeyStoreProvider(
        file.toURI());
    conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH,
        jks.toString());

    provisionAccessKeys(conf);

    conf.set(Constants.ACCESS_KEY, EXAMPLE_ID + "LJM");
    URI uriWithUserInfo = new URI("s3a://123:456@foobar");
    S3xLoginHelper.Login creds =
        S3AUtils.getAWSAccessKeys(uriWithUserInfo, conf);
    assertEquals("AccessKey incorrect.", "123", creds.getUser());
    assertEquals("SecretKey incorrect.", "456", creds.getPassword());
  }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		17
-  
  public void testIDFromUserInfoSecretFromCredentialProvider()
      throws Exception {
    // set up conf to have a cred provider
    final Configuration conf = new Configuration();
    final File file = tempDir.newFile("test.jks");
    final URI jks = ProviderUtils.nestURIForLocalJavaKeyStoreProvider(
        file.toURI());
    conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH,
        jks.toString());

    provisionAccessKeys(conf);

    conf.set(Constants.ACCESS_KEY, EXAMPLE_ID + "LJM");
    URI uriWithUserInfo = new URI("s3a://123@foobar");
    S3xLoginHelper.Login creds =
        S3AUtils.getAWSAccessKeys(uriWithUserInfo, conf);
    assertEquals("AccessKey incorrect.", "123", creds.getUser());
    assertEquals("SecretKey incorrect.", EXAMPLE_KEY, creds.getPassword());@@@   }
```
```
Cyclomatic Complexity	 1
Assertions		 3
Lines of Code		15
-  
  public void testBucketConfigurationPropagationResolution() throws Throwable {
    Configuration config = new Configuration(false);
    String basekey = "fs.s3a.base";
    String baseref = "fs.s3a.baseref";
    String baseref2 = "fs.s3a.baseref2";
    config.set(basekey, "orig");
    config.set(baseref2, "${fs.s3a.base}");
    setBucketOption(config, "b", basekey, "1024");
    setBucketOption(config, "b", baseref, "${fs.s3a.base}");
    Configuration updated = propagateBucketOptions(config, "b");
    assertOptionEquals(updated, basekey, "1024");
    assertOptionEquals(updated, baseref, "1024");
    assertOptionEquals(updated, baseref2, "1024");
  }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		9
-  
  public void testMultipleBucketConfigurations() throws Throwable {
    Configuration config = new Configuration(false);
    setBucketOption(config, "b", USER_AGENT_PREFIX, "UA-b");
    setBucketOption(config, "c", USER_AGENT_PREFIX, "UA-c");
    config.set(USER_AGENT_PREFIX, "UA-orig");
    Configuration updated = propagateBucketOptions(config, "c");
    assertOptionEquals(updated, USER_AGENT_PREFIX, "UA-c");
  }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		9
-  
  public void testClearBucketOption() throws Throwable {
    Configuration config = new Configuration();
    config.set(USER_AGENT_PREFIX, "base");
    setBucketOption(config, "bucket", USER_AGENT_PREFIX, "overridden");
    clearBucketOption(config, "bucket", USER_AGENT_PREFIX);
    Configuration updated = propagateBucketOptions(config, "c");
    assertOptionEquals(updated, USER_AGENT_PREFIX, "base");
  }
```
```
Cyclomatic Complexity	 1
Assertions		 5
Lines of Code		18
-  
  public void testBucketConfigurationSkipsUnmodifiable() throws Throwable {
    Configuration config = new Configuration(false);
    String impl = "fs.s3a.impl";
    config.set(impl, "orig");
    setBucketOption(config, "b", impl, "b");
    String metastoreImpl = "fs.s3a.metadatastore.impl";
    String ddb = "org.apache.hadoop.fs.s3a.s3guard.DynamoDBMetadataStore";
    setBucketOption(config, "b", metastoreImpl, ddb);
    setBucketOption(config, "b", "impl2", "b2");
    setBucketOption(config, "b", "bucket.b.loop", "b3");
    assertOptionEquals(config, "fs.s3a.bucket.b.impl", "b");

    Configuration updated = propagateBucketOptions(config, "b");
    assertOptionEquals(updated, impl, "orig");
    assertOptionEquals(updated, "fs.s3a.impl2", "b2");
    assertOptionEquals(updated, metastoreImpl, ddb);
    assertOptionEquals(updated, "fs.s3a.bucket.b.loop", null);
  }
```
```
Cyclomatic Complexity	 -1
Assertions		 6
Lines of Code		51
-  
  public void testReadFileChanged() throws Throwable {
    describe("overwrite a file with a shorter one during a read, seek");
    final int fullLength = 8192;
    final byte[] fullDataset = dataset(fullLength, 'a', 32);
    final int shortLen = 4096;
    final byte[] shortDataset = dataset(shortLen, 'A', 32);
    final FileSystem fs = getFileSystem();
    final Path testpath = path("readFileToChange.txt");
    // initial write
    writeDataset(fs, testpath, fullDataset, fullDataset.length, 1024, false);
    try(FSDataInputStream instream = fs.open(testpath)) {
      instream.seek(fullLength - 16);
      assertTrue("no data to read", instream.read() >= 0);
      // overwrite
      writeDataset(fs, testpath, shortDataset, shortDataset.length, 1024, true);
      // here the file length is less. Probe the file to see if this is true,
      // with a spin and wait
      LambdaTestUtils.eventually(30 * 1000, 1000,
          new Callable<Void>() {
            @Override
            public Void call() throws Exception {
              assertEquals(shortLen, fs.getFileStatus(testpath).getLen());
              return null;
            }
          });

      // here length is shorter. Assuming it has propagated to all replicas,
      // the position of the input stream is now beyond the EOF.
      // An attempt to seek backwards to a position greater than the
      // short length will raise an exception from AWS S3, which must be
      // translated into an EOF

      instream.seek(shortLen + 1024);
      int c = instream.read();
      assertIsEOF("read()", c);

      byte[] buf = new byte[256];

      assertIsEOF("read(buffer)", instream.read(buf));
      assertIsEOF("read(offset)",
          instream.read(instream.getPos(), buf, 0, buf.length));

      // now do a block read fully, again, backwards from the current pos
      try {
        instream.readFully(shortLen + 512, buf);
        fail("Expected readFully to fail");
      } catch (EOFException expected) {
        LOG.debug("Expected EOF: ", expected);
      }

      assertIsEOF("read(offset)",
          instream.read(shortLen + 510, buf, 0, buf.length));

      // seek somewhere useful
      instream.seek(shortLen - 256);

      // delete the file. Reads must fail
      fs.delete(testpath, false);

      try {
        int r = instream.read();
        fail("Expected an exception, got " + r);
      } catch (FileNotFoundException e) {
        // expected
      }

      try {
        instream.readFully(2048, buf);
        fail("Expected readFully to fail");
      } catch (FileNotFoundException e) {
        // expected
      }

    }@@@   }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		6
-  
  public void testLoginWithUserAndPass() throws Throwable {
    S3xLoginHelper.Login login = assertMatchesLogin(USER, PASS,
        WITH_USER_AND_PASS);
    assertTrue("Login of " + login, login.hasLogin());
  }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		4
-  
  public void testLoginWithSlashInPass() throws Throwable {
    assertMatchesLogin(USER, "pa//", WITH_SLASH_IN_PASS);
  }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		4
-  
  public void testLoginWithPlusInPass() throws Throwable {
    assertMatchesLogin(USER, "pa+", WITH_PLUS_IN_PASS);
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		3
-  
  public void testPathURIFixup() throws Throwable {

  }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		10
-  
  public void testSetSSLConf() {
    final DistCpOptions options = new DistCpOptions(
        Collections.singletonList(new Path("hdfs://localhost:8020/source")),
        new Path("hdfs://localhost:8020/target/"));
    Assert.assertNull(options.getSslConfigurationFile());

    options.setSslConfigurationFile("/tmp/ssl-client.xml");
    Assert.assertEquals("/tmp/ssl-client.xml",
        options.getSslConfigurationFile());@@@   }
```
```
Cyclomatic Complexity	 1
Assertions		 4
Lines of Code		34
-  
  public void testParseCopyBufferSize() {
    DistCpOptions options =
        OptionsParser.parse(new String[] {
            "hdfs://localhost:8020/source/first",
            "hdfs://localhost:8020/target/" });
    Assert.assertEquals(options.getCopyBufferSize(),
        DistCpConstants.COPY_BUFFER_SIZE_DEFAULT);

    options =
        OptionsParser.parse(new String[] { "-copybuffersize", "0",
            "hdfs://localhost:8020/source/first",
            "hdfs://localhost:8020/target/" });
    Assert.assertEquals(options.getCopyBufferSize(),
        DistCpConstants.COPY_BUFFER_SIZE_DEFAULT);

    options =
        OptionsParser.parse(new String[] { "-copybuffersize", "-1",
            "hdfs://localhost:8020/source/first",
            "hdfs://localhost:8020/target/" });
    Assert.assertEquals(options.getCopyBufferSize(),
        DistCpConstants.COPY_BUFFER_SIZE_DEFAULT);

    options =
        OptionsParser.parse(new String[] { "-copybuffersize", "4194304",
            "hdfs://localhost:8020/source/first",
            "hdfs://localhost:8020/target/" });
    Assert.assertEquals(options.getCopyBufferSize(), 4194304);

    try {
      OptionsParser
          .parse(new String[] { "-copybuffersize", "hello",
              "hdfs://localhost:8020/source/first",
              "hdfs://localhost:8020/target/" });
      Assert.fail("Non numberic copybuffersize parsed successfully!");
    } catch (IllegalArgumentException ignore) {
    }
  }
```
```
Cyclomatic Complexity	 1
Assertions		 4
Lines of Code		21
-  
  public void testVerboseLog() {
    DistCpOptions options = OptionsParser
        .parse(new String[] {"hdfs://localhost:9820/source/first",
            "hdfs://localhost:9820/target/"});
    Assert.assertFalse(options.shouldVerboseLog());

    try {
      OptionsParser
          .parse(new String[] {"-v", "hdfs://localhost:8020/source/first",
              "hdfs://localhost:8020/target/"});
      Assert.fail("-v should fail if -log option is not specified");
    } catch (IllegalArgumentException e) {
      assertExceptionContains("-v is valid only with -log option", e);
    }

    options = OptionsParser
        .parse(new String[] {"-log", "hdfs://localhost:8020/logs", "-v",
            "hdfs://localhost:8020/source/first",
            "hdfs://localhost:8020/target/"});
    Assert.assertTrue(options.shouldVerboseLog());@@@+    assertThat(options.getFiltersFile()).isEqualTo("/tmp/filters.txt");@@@   }
```
```
Cyclomatic Complexity	 1
Assertions		 8
Lines of Code		30
-  
  public void testQueryRMNodes() throws Exception {
    RMContext rmContext = Mockito.mock(RMContext.class);
    NodeId node1 = NodeId.newInstance("node1", 1234);
    RMNode rmNode1 = Mockito.mock(RMNode.class);
    ConcurrentMap<NodeId, RMNode> inactiveList =
        new ConcurrentHashMap<NodeId, RMNode>();
    Mockito.when(rmNode1.getState()).thenReturn(NodeState.SHUTDOWN);
    inactiveList.put(node1, rmNode1);
    Mockito.when(rmContext.getInactiveRMNodes()).thenReturn(inactiveList);
    List<RMNode> result = RMServerUtils.queryRMNodes(rmContext,
        EnumSet.of(NodeState.SHUTDOWN));
    Assert.assertTrue(result.size() != 0);
    Assert.assertEquals(result.get(0), rmNode1);
    Mockito.when(rmNode1.getState()).thenReturn(NodeState.DECOMMISSIONED);
    result = RMServerUtils.queryRMNodes(rmContext,
        EnumSet.of(NodeState.DECOMMISSIONED));
    Assert.assertTrue(result.size() != 0);
    Assert.assertEquals(result.get(0), rmNode1);
    Mockito.when(rmNode1.getState()).thenReturn(NodeState.LOST);
    result = RMServerUtils.queryRMNodes(rmContext,
        EnumSet.of(NodeState.LOST));
    Assert.assertTrue(result.size() != 0);
    Assert.assertEquals(result.get(0), rmNode1);
    Mockito.when(rmNode1.getState()).thenReturn(NodeState.REBOOTED);
    result = RMServerUtils.queryRMNodes(rmContext,
        EnumSet.of(NodeState.REBOOTED));
    Assert.assertTrue(result.size() != 0);
    Assert.assertEquals(result.get(0), rmNode1);@@@   }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		10
-  
  public void testAppAttempts() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    RMApp app1 = rm.submitApp(CONTAINER_MB, "testwordcount", "user1");
    amNodeManager.nodeHeartbeat(true);
    testAppAttemptsHelper(app1.getApplicationId().toString(), app1,
        MediaType.APPLICATION_JSON);
    rm.stop();
  }
```
```
Cyclomatic Complexity	 4
Assertions		 2
Lines of Code		28
-   (timeout = 20000)
  public void testMultipleAppAttempts() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 8192);
    RMApp app1 = rm.submitApp(CONTAINER_MB, "testwordcount", "user1");
    MockAM am = MockRM.launchAndRegisterAM(app1, rm, amNodeManager);
    int maxAppAttempts = rm.getConfig().getInt(
        YarnConfiguration.RM_AM_MAX_ATTEMPTS,
        YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS);
    assertTrue(maxAppAttempts > 1);
    int numAttempt = 1;
    while (true) {
      // fail the AM by sending CONTAINER_FINISHED event without registering.
      amNodeManager.nodeHeartbeat(am.getApplicationAttemptId(), 1, ContainerState.COMPLETE);
      rm.waitForState(am.getApplicationAttemptId(), RMAppAttemptState.FAILED);
      if (numAttempt == maxAppAttempts) {
        rm.waitForState(app1.getApplicationId(), RMAppState.FAILED);
        break;
      }
      // wait for app to start a new attempt.
      rm.waitForState(app1.getApplicationId(), RMAppState.ACCEPTED);
      am = MockRM.launchAndRegisterAM(app1, rm, amNodeManager);
      numAttempt++;
    }
    assertEquals("incorrect number of attempts", maxAppAttempts,
        app1.getAppAttempts().values().size());
    testAppAttemptsHelper(app1.getApplicationId().toString(), app1,
        MediaType.APPLICATION_JSON);
    rm.stop();
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		10
-  
  public void testAppAttemptsSlash() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    RMApp app1 = rm.submitApp(CONTAINER_MB);
    amNodeManager.nodeHeartbeat(true);
    testAppAttemptsHelper(app1.getApplicationId().toString() + "/", app1,
        MediaType.APPLICATION_JSON);
    rm.stop();
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		9
-  
  public void testAppAttemtpsDefault() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    RMApp app1 = rm.submitApp(CONTAINER_MB);
    amNodeManager.nodeHeartbeat(true);
    testAppAttemptsHelper(app1.getApplicationId().toString() + "/", app1, "");
    rm.stop();
  }
```
```
Cyclomatic Complexity	 1
Assertions		 3
Lines of Code		34
-  
  public void testInvalidAppIdGetAttempts() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    RMApp app = rm.submitApp(CONTAINER_MB);
    amNodeManager.nodeHeartbeat(true);
    WebResource r = resource();

    try {
      r.path("ws").path("v1").path("cluster").path("apps")
          .path("application_invalid_12").path("appattempts")
          .accept(MediaType.APPLICATION_JSON)
          .get(JSONObject.class);
      fail("should have thrown exception on invalid appAttempt");
    } catch (UniformInterfaceException ue) {
      ClientResponse response = ue.getResponse();

      assertEquals(Status.BAD_REQUEST, response.getClientResponseStatus());
      assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
      JSONObject msg = response.getEntity(JSONObject.class);
      JSONObject exception = msg.getJSONObject("RemoteException");
      assertEquals("incorrect number of elements", 3, exception.length());
      String message = exception.getString("message");
      String type = exception.getString("exception");
      String classname = exception.getString("javaClassName");
      WebServicesTestUtils.checkStringMatch("exception message",
          "java.lang.IllegalArgumentException: Invalid ApplicationId:"
              + " application_invalid_12",
          message);
      WebServicesTestUtils.checkStringMatch("exception type",
          "BadRequestException", type);
      WebServicesTestUtils.checkStringMatch("exception classname",
          "org.apache.hadoop.yarn.webapp.BadRequestException", classname);

    } finally {
      rm.stop();
    }
  }
```
```
Cyclomatic Complexity	 1
Assertions		 3
Lines of Code		34
-  
  public void testInvalidAppAttemptId() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    RMApp app = rm.submitApp(CONTAINER_MB);
    amNodeManager.nodeHeartbeat(true);
    WebResource r = resource();

    try {
      r.path("ws").path("v1").path("cluster").path("apps")
          .path(app.getApplicationId().toString()).path("appattempts")
          .path("appattempt_invalid_12_000001")
          .accept(MediaType.APPLICATION_JSON).get(JSONObject.class);
      fail("should have thrown exception on invalid appAttempt");
    } catch (UniformInterfaceException ue) {
      ClientResponse response = ue.getResponse();

      assertEquals(Status.BAD_REQUEST, response.getClientResponseStatus());
      assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
      JSONObject msg = response.getEntity(JSONObject.class);
      JSONObject exception = msg.getJSONObject("RemoteException");
      assertEquals("incorrect number of elements", 3, exception.length());
      String message = exception.getString("message");
      String type = exception.getString("exception");
      String classname = exception.getString("javaClassName");
      WebServicesTestUtils.checkStringMatch("exception message",
          "java.lang.IllegalArgumentException: Invalid AppAttemptId:"
              + " appattempt_invalid_12_000001",
          message);
      WebServicesTestUtils.checkStringMatch("exception type",
          "BadRequestException", type);
      WebServicesTestUtils.checkStringMatch("exception classname",
          "org.apache.hadoop.yarn.webapp.BadRequestException", classname);

    } finally {
      rm.stop();
    }
  }
```
```
Cyclomatic Complexity	 1
Assertions		 3
Lines of Code		33
-  
  public void testNonexistAppAttempts() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    rm.submitApp(CONTAINER_MB, "testwordcount", "user1");
    amNodeManager.nodeHeartbeat(true);
    WebResource r = resource();

    try {
      r.path("ws").path("v1").path("cluster").path("apps")
          .path("application_00000_0099").accept(MediaType.APPLICATION_JSON)
          .get(JSONObject.class);
      fail("should have thrown exception on invalid appid");
    } catch (UniformInterfaceException ue) {
      ClientResponse response = ue.getResponse();

      assertEquals(Status.NOT_FOUND, response.getClientResponseStatus());
      assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());

      JSONObject msg = response.getEntity(JSONObject.class);
      JSONObject exception = msg.getJSONObject("RemoteException");
      assertEquals("incorrect number of elements", 3, exception.length());
      String message = exception.getString("message");
      String type = exception.getString("exception");
      String classname = exception.getString("javaClassName");
      WebServicesTestUtils.checkStringMatch("exception message",
          "java.lang.Exception: app with id: application_00000_0099 not found",
          message);
      WebServicesTestUtils.checkStringMatch("exception type",
          "NotFoundException", type);
      WebServicesTestUtils.checkStringMatch("exception classname",
          "org.apache.hadoop.yarn.webapp.NotFoundException", classname);
    } finally {
      rm.stop();
    }
  }
```
```
Cyclomatic Complexity	 1
Assertions		 4
Lines of Code		26
-  
  public void testAppAttemptsXML() throws JSONException, Exception {
    rm.start();
    String user = "user1";
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    RMApp app1 = rm.submitApp(CONTAINER_MB, "testwordcount", user);
    amNodeManager.nodeHeartbeat(true);
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .path("apps").path(app1.getApplicationId().toString())
        .path("appattempts").accept(MediaType.APPLICATION_XML)
        .get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_XML_TYPE, response.getType());
    String xml = response.getEntity(String.class);

    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    DocumentBuilder db = dbf.newDocumentBuilder();
    InputSource is = new InputSource();
    is.setCharacterStream(new StringReader(xml));
    Document dom = db.parse(is);
    NodeList nodes = dom.getElementsByTagName("appAttempts");
    assertEquals("incorrect number of elements", 1, nodes.getLength());
    NodeList attempt = dom.getElementsByTagName("appAttempt");
    assertEquals("incorrect number of elements", 1, attempt.getLength());
    verifyAppAttemptsXML(attempt, app1.getCurrentAppAttempt(), user);
    rm.stop();
  }
```
## 85af77c75768416db24ca506fd1704ce664ca92f ##
```
Cyclomatic Complexity	2
Assertions		0
Lines of Code		21
-  
  public void testSaveNamespaceWithRenamedLease() throws Exception {
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(new Configuration())
        .numDataNodes(1).build();
    cluster.waitActive();
    DistributedFileSystem fs = (DistributedFileSystem) cluster.getFileSystem();
    OutputStream out = null;
    try {
      fs.mkdirs(new Path("/test-target"));
      out = fs.create(new Path("/test-source/foo")); // don't close
      fs.rename(new Path("/test-source/"), new Path("/test-target/"));

      fs.setSafeMode(SafeModeAction.SAFEMODE_ENTER);
      cluster.getNameNodeRpc().saveNamespace();
      fs.setSafeMode(SafeModeAction.SAFEMODE_LEAVE);
    } finally {
      IOUtils.cleanup(LOG, out, fs);
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
```
```
Cyclomatic Complexity	 2
Assertions		 0
Lines of Code		8
-  (timeout=300000)
  public void testCheckpointSucceedsWithLegacyOIVException() throws Exception {
    // Delete the OIV image dir to cause an IOException while saving
    FileUtil.fullyDelete(tmpOivImgDir);

    doEdits(0, 10);
    HATestUtil.waitForStandbyToCatchUp(nns[0], nns[1]);
    // Once the standby catches up, it should notice that it needs to
    // do a checkpoint and save one to its local directories.
    HATestUtil.waitForCheckpoint(cluster, 1, ImmutableList.of(12));

    // It should also upload it back to the active.
    HATestUtil.waitForCheckpoint(cluster, 0, ImmutableList.of(12));
  }
```
```
Cyclomatic Complexity	 1
Assertions		 3
Lines of Code		16
-  
  public void testGetConfiguration() throws ServletException {
    AuthFilter filter = new AuthFilter();
    Map<String, String> m = new HashMap<String,String>();
    m.put(DFSConfigKeys.DFS_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL_KEY,
        "xyz/thehost@REALM");
    m.put(DFSConfigKeys.DFS_WEB_AUTHENTICATION_KERBEROS_KEYTAB_KEY,
        "thekeytab");
    FilterConfig config = new DummyFilterConfig(m);
    Properties p = filter.getConfiguration("random", config);
    Assert.assertEquals("xyz/thehost@REALM",
        p.getProperty("kerberos.principal"));
    Assert.assertEquals("thekeytab", p.getProperty("kerberos.keytab"));
    Assert.assertEquals("true",
        p.getProperty(PseudoAuthenticationHandler.ANONYMOUS_ALLOWED));
  }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		11
-  
  public void testGetSimpleAuthDisabledConfiguration() throws ServletException {
    AuthFilter filter = new AuthFilter();
    Map<String, String> m = new HashMap<String,String>();
    m.put(DFSConfigKeys.DFS_WEB_AUTHENTICATION_SIMPLE_ANONYMOUS_ALLOWED,
        "false");
    FilterConfig config = new DummyFilterConfig(m);
    Properties p = filter.getConfiguration("random", config);
    Assert.assertEquals("false",
        p.getProperty(PseudoAuthenticationHandler.ANONYMOUS_ALLOWED));
  }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		9
-  
  public void testGetSimpleAuthDefaultConfiguration() throws ServletException {
    AuthFilter filter = new AuthFilter();
    Map<String, String> m = new HashMap<String,String>();
    
    FilterConfig config = new DummyFilterConfig(m);
    Properties p = filter.getConfiguration("random", config);
    Assert.assertEquals("true",
        p.getProperty(PseudoAuthenticationHandler.ANONYMOUS_ALLOWED));
  }
```
```
Cyclomatic Complexity	 2
Assertions		 2
Lines of Code		18
-  
  public void testProxyPasswordFromCredentialProvider() throws Exception {
    ClientConfiguration awsConf = new ClientConfiguration();
    // set up conf to have a cred provider
    final Configuration conf2 = new Configuration();
    final File file = tempDir.newFile("test.jks");
    final URI jks = ProviderUtils.nestURIForLocalJavaKeyStoreProvider(
        file.toURI());
    conf2.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH,
        jks.toString());

    provisionProxyPassword(conf2, "password");

    // let's set the password in config and ensure that it uses the credential
    // provider provisioned value instead.
    conf2.set(Constants.PROXY_PASSWORD, "passwordLJM");
    char[] pwd = conf2.getPassword(Constants.PROXY_PASSWORD);
    assertNotNull("Proxy password should not retrun null.", pwd);
    if (pwd != null) {
      assertEquals("Proxy password override did NOT work.", "password",
          new String(pwd));
    }
  }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		16
-  
  public void testCredsFromUserInfo() throws Exception {
    // set up conf to have a cred provider
    final Configuration conf = new Configuration();
    final File file = tempDir.newFile("test.jks");
    final URI jks = ProviderUtils.nestURIForLocalJavaKeyStoreProvider(
        file.toURI());
    conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH,
        jks.toString());

    provisionAccessKeys(conf);

    conf.set(Constants.ACCESS_KEY, EXAMPLE_ID + "LJM");
    URI uriWithUserInfo = new URI("s3a://123:456@foobar");
    S3xLoginHelper.Login creds =
        S3AUtils.getAWSAccessKeys(uriWithUserInfo, conf);
    assertEquals("AccessKey incorrect.", "123", creds.getUser());
    assertEquals("SecretKey incorrect.", "456", creds.getPassword());
  }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		17
-  
  public void testIDFromUserInfoSecretFromCredentialProvider()
      throws Exception {
    // set up conf to have a cred provider
    final Configuration conf = new Configuration();
    final File file = tempDir.newFile("test.jks");
    final URI jks = ProviderUtils.nestURIForLocalJavaKeyStoreProvider(
        file.toURI());
    conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH,
        jks.toString());

    provisionAccessKeys(conf);

    conf.set(Constants.ACCESS_KEY, EXAMPLE_ID + "LJM");
    URI uriWithUserInfo = new URI("s3a://123@foobar");
    S3xLoginHelper.Login creds =
        S3AUtils.getAWSAccessKeys(uriWithUserInfo, conf);
    assertEquals("AccessKey incorrect.", "123", creds.getUser());
    assertEquals("SecretKey incorrect.", EXAMPLE_KEY, creds.getPassword());@@@   }
```
```
Cyclomatic Complexity	 -1
Assertions		 6
Lines of Code		51
-  
  public void testReadFileChanged() throws Throwable {
    describe("overwrite a file with a shorter one during a read, seek");
    final int fullLength = 8192;
    final byte[] fullDataset = dataset(fullLength, 'a', 32);
    final int shortLen = 4096;
    final byte[] shortDataset = dataset(shortLen, 'A', 32);
    final FileSystem fs = getFileSystem();
    final Path testpath = path("readFileToChange.txt");
    // initial write
    writeDataset(fs, testpath, fullDataset, fullDataset.length, 1024, false);
    try(FSDataInputStream instream = fs.open(testpath)) {
      instream.seek(fullLength - 16);
      assertTrue("no data to read", instream.read() >= 0);
      // overwrite
      writeDataset(fs, testpath, shortDataset, shortDataset.length, 1024, true);
      // here the file length is less. Probe the file to see if this is true,
      // with a spin and wait
      LambdaTestUtils.eventually(30 * 1000, 1000,
          new Callable<Void>() {
            @Override
            public Void call() throws Exception {
              assertEquals(shortLen, fs.getFileStatus(testpath).getLen());
              return null;
            }
          });

      // here length is shorter. Assuming it has propagated to all replicas,
      // the position of the input stream is now beyond the EOF.
      // An attempt to seek backwards to a position greater than the
      // short length will raise an exception from AWS S3, which must be
      // translated into an EOF

      instream.seek(shortLen + 1024);
      int c = instream.read();
      assertIsEOF("read()", c);

      byte[] buf = new byte[256];

      assertIsEOF("read(buffer)", instream.read(buf));
      assertIsEOF("read(offset)",
          instream.read(instream.getPos(), buf, 0, buf.length));

      // now do a block read fully, again, backwards from the current pos
      try {
        instream.readFully(shortLen + 512, buf);
        fail("Expected readFully to fail");
      } catch (EOFException expected) {
        LOG.debug("Expected EOF: ", expected);
      }

      assertIsEOF("read(offset)",
          instream.read(shortLen + 510, buf, 0, buf.length));

      // seek somewhere useful
      instream.seek(shortLen - 256);

      // delete the file. Reads must fail
      fs.delete(testpath, false);

      try {
        int r = instream.read();
        fail("Expected an exception, got " + r);
      } catch (FileNotFoundException e) {
        // expected
      }

      try {
        instream.readFully(2048, buf);
        fail("Expected readFully to fail");
      } catch (FileNotFoundException e) {
        // expected
      }

    }@@@   }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		6
-  
  public void testLoginWithUserAndPass() throws Throwable {
    S3xLoginHelper.Login login = assertMatchesLogin(USER, PASS,
        WITH_USER_AND_PASS);
    assertTrue("Login of " + login, login.hasLogin());
  }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		4
-  
  public void testLoginWithSlashInPass() throws Throwable {
    assertMatchesLogin(USER, "pa//", WITH_SLASH_IN_PASS);
  }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		4
-  
  public void testLoginWithPlusInPass() throws Throwable {
    assertMatchesLogin(USER, "pa+", WITH_PLUS_IN_PASS);
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		3
-  
  public void testPathURIFixup() throws Throwable {

  }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		10
-  
  public void testSetSSLConf() {
    final DistCpOptions options = new DistCpOptions(
        Collections.singletonList(new Path("hdfs://localhost:8020/source")),
        new Path("hdfs://localhost:8020/target/"));
    Assert.assertNull(options.getSslConfigurationFile());

    options.setSslConfigurationFile("/tmp/ssl-client.xml");
    Assert.assertEquals("/tmp/ssl-client.xml",
        options.getSslConfigurationFile());@@@   }
```
```
Cyclomatic Complexity	 1
Assertions		 4
Lines of Code		34
-  
  public void testParseCopyBufferSize() {
    DistCpOptions options =
        OptionsParser.parse(new String[] {
            "hdfs://localhost:8020/source/first",
            "hdfs://localhost:8020/target/" });
    Assert.assertEquals(options.getCopyBufferSize(),
        DistCpConstants.COPY_BUFFER_SIZE_DEFAULT);

    options =
        OptionsParser.parse(new String[] { "-copybuffersize", "0",
            "hdfs://localhost:8020/source/first",
            "hdfs://localhost:8020/target/" });
    Assert.assertEquals(options.getCopyBufferSize(),
        DistCpConstants.COPY_BUFFER_SIZE_DEFAULT);

    options =
        OptionsParser.parse(new String[] { "-copybuffersize", "-1",
            "hdfs://localhost:8020/source/first",
            "hdfs://localhost:8020/target/" });
    Assert.assertEquals(options.getCopyBufferSize(),
        DistCpConstants.COPY_BUFFER_SIZE_DEFAULT);

    options =
        OptionsParser.parse(new String[] { "-copybuffersize", "4194304",
            "hdfs://localhost:8020/source/first",
            "hdfs://localhost:8020/target/" });
    Assert.assertEquals(options.getCopyBufferSize(), 4194304);

    try {
      OptionsParser
          .parse(new String[] { "-copybuffersize", "hello",
              "hdfs://localhost:8020/source/first",
              "hdfs://localhost:8020/target/" });
      Assert.fail("Non numberic copybuffersize parsed successfully!");
    } catch (IllegalArgumentException ignore) {
    }
  }
```
```
Cyclomatic Complexity	 1
Assertions		 4
Lines of Code		21
-  
  public void testVerboseLog() {
    DistCpOptions options = OptionsParser
        .parse(new String[] {"hdfs://localhost:9820/source/first",
            "hdfs://localhost:9820/target/"});
    Assert.assertFalse(options.shouldVerboseLog());

    try {
      OptionsParser
          .parse(new String[] {"-v", "hdfs://localhost:8020/source/first",
              "hdfs://localhost:8020/target/"});
      Assert.fail("-v should fail if -log option is not specified");
    } catch (IllegalArgumentException e) {
      assertExceptionContains("-v is valid only with -log option", e);
    }

    options = OptionsParser
        .parse(new String[] {"-log", "hdfs://localhost:8020/logs", "-v",
            "hdfs://localhost:8020/source/first",
            "hdfs://localhost:8020/target/"});
    Assert.assertTrue(options.shouldVerboseLog());@@@+    assertThat(options.getFiltersFile()).isEqualTo("/tmp/filters.txt");@@@   }
```
```
Cyclomatic Complexity	 1
Assertions		 8
Lines of Code		30
-  
  public void testQueryRMNodes() throws Exception {
    RMContext rmContext = Mockito.mock(RMContext.class);
    NodeId node1 = NodeId.newInstance("node1", 1234);
    RMNode rmNode1 = Mockito.mock(RMNode.class);
    ConcurrentMap<NodeId, RMNode> inactiveList =
        new ConcurrentHashMap<NodeId, RMNode>();
    Mockito.when(rmNode1.getState()).thenReturn(NodeState.SHUTDOWN);
    inactiveList.put(node1, rmNode1);
    Mockito.when(rmContext.getInactiveRMNodes()).thenReturn(inactiveList);
    List<RMNode> result = RMServerUtils.queryRMNodes(rmContext,
        EnumSet.of(NodeState.SHUTDOWN));
    Assert.assertTrue(result.size() != 0);
    Assert.assertEquals(result.get(0), rmNode1);
    Mockito.when(rmNode1.getState()).thenReturn(NodeState.DECOMMISSIONED);
    result = RMServerUtils.queryRMNodes(rmContext,
        EnumSet.of(NodeState.DECOMMISSIONED));
    Assert.assertTrue(result.size() != 0);
    Assert.assertEquals(result.get(0), rmNode1);
    Mockito.when(rmNode1.getState()).thenReturn(NodeState.LOST);
    result = RMServerUtils.queryRMNodes(rmContext,
        EnumSet.of(NodeState.LOST));
    Assert.assertTrue(result.size() != 0);
    Assert.assertEquals(result.get(0), rmNode1);
    Mockito.when(rmNode1.getState()).thenReturn(NodeState.REBOOTED);
    result = RMServerUtils.queryRMNodes(rmContext,
        EnumSet.of(NodeState.REBOOTED));
    Assert.assertTrue(result.size() != 0);
    Assert.assertEquals(result.get(0), rmNode1);@@@   }
```
```
Cyclomatic Complexity	 1
Assertions		 5
Lines of Code		15
-  
  public void testAssignToQueue() throws Exception {
    conf.set(FairSchedulerConfiguration.USER_AS_DEFAULT_QUEUE, "true");
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    RMApp rmApp1 = new MockRMApp(0, 0, RMAppState.NEW);
    RMApp rmApp2 = new MockRMApp(1, 1, RMAppState.NEW);
    
    FSLeafQueue queue1 = scheduler.assignToQueue(rmApp1, "default", "asterix");
    FSLeafQueue queue2 = scheduler.assignToQueue(rmApp2, "notdefault", "obelix");
    
    // assert FSLeafQueue's name is the correct name is the one set in the RMApp
    assertEquals(rmApp1.getQueue(), queue1.getName());
    assertEquals("root.asterix", rmApp1.getQueue());
    assertEquals(rmApp2.getQueue(), queue2.getName());
    assertEquals("root.notdefault", rmApp2.getQueue());
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		24
-  
  public void testAssignToBadDefaultQueue() throws Exception {
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);

    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<queuePlacementPolicy>");
    out.println("<rule name=\"specified\" create=\"false\" />");
    out.println("<rule name=\"default\" create=\"false\" />");
    out.println("</queuePlacementPolicy>");
    out.println("</allocations>");
    out.close();
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    RMApp rmApp1 = new MockRMApp(0, 0, RMAppState.NEW);

    try {
      FSLeafQueue queue1 = scheduler.assignToQueue(rmApp1, "default",
          "asterix");
    } catch (IllegalStateException ise) {
      fail("Bad queue placement policy terminal rule should not throw " +
          "exception ");
    }
  }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		13
-  
  public void testAssignToNonLeafQueueReturnsNull() throws Exception {
    conf.set(FairSchedulerConfiguration.USER_AS_DEFAULT_QUEUE, "true");
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    scheduler.getQueueManager().getLeafQueue("root.child1.granchild", true);
    scheduler.getQueueManager().getLeafQueue("root.child2", true);

    RMApp rmApp1 = new MockRMApp(0, 0, RMAppState.NEW);
    RMApp rmApp2 = new MockRMApp(1, 1, RMAppState.NEW);

    // Trying to assign to non leaf queue would return null
    assertNull(scheduler.assignToQueue(rmApp1, "root.child1", "tintin"));
    assertNotNull(scheduler.assignToQueue(rmApp2, "root.child2", "snowy"));
  }
```
```
Cyclomatic Complexity	 1
Assertions		 9
Lines of Code		46
-  
  public void testQueuePlacementWithPolicy() throws Exception {
    conf.setClass(CommonConfigurationKeys.HADOOP_SECURITY_GROUP_MAPPING,
        SimpleGroupsMapping.class, GroupMappingServiceProvider.class);
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    ApplicationAttemptId appId;

    List<QueuePlacementRule> rules = new ArrayList<QueuePlacementRule>();
    rules.add(new QueuePlacementRule.Specified().initialize(true, null));
    rules.add(new QueuePlacementRule.User().initialize(false, null));
    rules.add(new QueuePlacementRule.PrimaryGroup().initialize(false, null));
    rules.add(new QueuePlacementRule.SecondaryGroupExistingQueue().initialize(false, null));
    rules.add(new QueuePlacementRule.Default().initialize(true, null));
    Set<String> queues = Sets.newHashSet("root.user1", "root.user3group",
        "root.user4subgroup1", "root.user4subgroup2" , "root.user5subgroup2");
    Map<FSQueueType, Set<String>> configuredQueues = new HashMap<FSQueueType, Set<String>>();
    configuredQueues.put(FSQueueType.LEAF, queues);
    configuredQueues.put(FSQueueType.PARENT, new HashSet<String>());
    scheduler.getAllocationConfiguration().placementPolicy =
        new QueuePlacementPolicy(rules, configuredQueues, conf);
    appId = createSchedulingRequest(1024, "somequeue", "user1");
    assertEquals("root.somequeue", scheduler.getSchedulerApp(appId).getQueueName());
    appId = createSchedulingRequest(1024, "default", "user1");
    assertEquals("root.user1", scheduler.getSchedulerApp(appId).getQueueName());
    appId = createSchedulingRequest(1024, "default", "user3");
    assertEquals("root.user3group", scheduler.getSchedulerApp(appId).getQueueName());
    appId = createSchedulingRequest(1024, "default", "user4");
    assertEquals("root.user4subgroup1", scheduler.getSchedulerApp(appId).getQueueName());
    appId = createSchedulingRequest(1024, "default", "user5");
    assertEquals("root.user5subgroup2", scheduler.getSchedulerApp(appId).getQueueName());
    appId = createSchedulingRequest(1024, "default", "otheruser");
    assertEquals("root.default", scheduler.getSchedulerApp(appId).getQueueName());
    
    // test without specified as first rule
    rules = new ArrayList<QueuePlacementRule>();
    rules.add(new QueuePlacementRule.User().initialize(false, null));
    rules.add(new QueuePlacementRule.Specified().initialize(true, null));
    rules.add(new QueuePlacementRule.Default().initialize(true, null));
    scheduler.getAllocationConfiguration().placementPolicy =
        new QueuePlacementPolicy(rules, configuredQueues, conf);
    appId = createSchedulingRequest(1024, "somequeue", "user1");
    assertEquals("root.user1", scheduler.getSchedulerApp(appId).getQueueName());
    appId = createSchedulingRequest(1024, "somequeue", "otheruser");
    assertEquals("root.somequeue", scheduler.getSchedulerApp(appId).getQueueName());
    appId = createSchedulingRequest(1024, "default", "otheruser");
    assertEquals("root.default", scheduler.getSchedulerApp(appId).getQueueName());@@@   }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		28
-  
  public void testNestedUserQueue() throws IOException {
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);
    conf.setClass(CommonConfigurationKeys.HADOOP_SECURITY_GROUP_MAPPING,
        SimpleGroupsMapping.class, GroupMappingServiceProvider.class);
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<queue name=\"user1group\" type=\"parent\">");
    out.println("<minResources>1024mb,0vcores</minResources>");
    out.println("</queue>");
    out.println("<queuePlacementPolicy>");
    out.println("<rule name=\"specified\" create=\"false\" />");
    out.println("<rule name=\"nestedUserQueue\">");
    out.println("     <rule name=\"primaryGroup\" create=\"false\" />");
    out.println("</rule>");
    out.println("<rule name=\"default\" />");
    out.println("</queuePlacementPolicy>");
    out.println("</allocations>");
    out.close();

    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());
    RMApp rmApp1 = new MockRMApp(0, 0, RMAppState.NEW);

    FSLeafQueue user1Leaf = scheduler.assignToQueue(rmApp1, "root.default",
        "user1");

    assertEquals("root.user1group.user1", user1Leaf.getName());@@@   }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		10
-  
  public void testAppAttempts() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    RMApp app1 = rm.submitApp(CONTAINER_MB, "testwordcount", "user1");
    amNodeManager.nodeHeartbeat(true);
    testAppAttemptsHelper(app1.getApplicationId().toString(), app1,
        MediaType.APPLICATION_JSON);
    rm.stop();
  }
```
```
Cyclomatic Complexity	 4
Assertions		 2
Lines of Code		28
-   (timeout = 20000)
  public void testMultipleAppAttempts() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 8192);
    RMApp app1 = rm.submitApp(CONTAINER_MB, "testwordcount", "user1");
    MockAM am = MockRM.launchAndRegisterAM(app1, rm, amNodeManager);
    int maxAppAttempts = rm.getConfig().getInt(
        YarnConfiguration.RM_AM_MAX_ATTEMPTS,
        YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS);
    assertTrue(maxAppAttempts > 1);
    int numAttempt = 1;
    while (true) {
      // fail the AM by sending CONTAINER_FINISHED event without registering.
      amNodeManager.nodeHeartbeat(am.getApplicationAttemptId(), 1, ContainerState.COMPLETE);
      rm.waitForState(am.getApplicationAttemptId(), RMAppAttemptState.FAILED);
      if (numAttempt == maxAppAttempts) {
        rm.waitForState(app1.getApplicationId(), RMAppState.FAILED);
        break;
      }
      // wait for app to start a new attempt.
      rm.waitForState(app1.getApplicationId(), RMAppState.ACCEPTED);
      am = MockRM.launchAndRegisterAM(app1, rm, amNodeManager);
      numAttempt++;
    }
    assertEquals("incorrect number of attempts", maxAppAttempts,
        app1.getAppAttempts().values().size());
    testAppAttemptsHelper(app1.getApplicationId().toString(), app1,
        MediaType.APPLICATION_JSON);
    rm.stop();
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		10
-  
  public void testAppAttemptsSlash() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    RMApp app1 = rm.submitApp(CONTAINER_MB);
    amNodeManager.nodeHeartbeat(true);
    testAppAttemptsHelper(app1.getApplicationId().toString() + "/", app1,
        MediaType.APPLICATION_JSON);
    rm.stop();
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		9
-  
  public void testAppAttemtpsDefault() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    RMApp app1 = rm.submitApp(CONTAINER_MB);
    amNodeManager.nodeHeartbeat(true);
    testAppAttemptsHelper(app1.getApplicationId().toString() + "/", app1, "");
    rm.stop();
  }
```
```
Cyclomatic Complexity	 1
Assertions		 3
Lines of Code		34
-  
  public void testInvalidAppIdGetAttempts() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    RMApp app = rm.submitApp(CONTAINER_MB);
    amNodeManager.nodeHeartbeat(true);
    WebResource r = resource();

    try {
      r.path("ws").path("v1").path("cluster").path("apps")
          .path("application_invalid_12").path("appattempts")
          .accept(MediaType.APPLICATION_JSON)
          .get(JSONObject.class);
      fail("should have thrown exception on invalid appAttempt");
    } catch (UniformInterfaceException ue) {
      ClientResponse response = ue.getResponse();

      assertEquals(Status.BAD_REQUEST, response.getClientResponseStatus());
      assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
      JSONObject msg = response.getEntity(JSONObject.class);
      JSONObject exception = msg.getJSONObject("RemoteException");
      assertEquals("incorrect number of elements", 3, exception.length());
      String message = exception.getString("message");
      String type = exception.getString("exception");
      String classname = exception.getString("javaClassName");
      WebServicesTestUtils.checkStringMatch("exception message",
          "java.lang.IllegalArgumentException: Invalid ApplicationId:"
              + " application_invalid_12",
          message);
      WebServicesTestUtils.checkStringMatch("exception type",
          "BadRequestException", type);
      WebServicesTestUtils.checkStringMatch("exception classname",
          "org.apache.hadoop.yarn.webapp.BadRequestException", classname);

    } finally {
      rm.stop();
    }
  }
```
```
Cyclomatic Complexity	 1
Assertions		 3
Lines of Code		34
-  
  public void testInvalidAppAttemptId() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    RMApp app = rm.submitApp(CONTAINER_MB);
    amNodeManager.nodeHeartbeat(true);
    WebResource r = resource();

    try {
      r.path("ws").path("v1").path("cluster").path("apps")
          .path(app.getApplicationId().toString()).path("appattempts")
          .path("appattempt_invalid_12_000001")
          .accept(MediaType.APPLICATION_JSON).get(JSONObject.class);
      fail("should have thrown exception on invalid appAttempt");
    } catch (UniformInterfaceException ue) {
      ClientResponse response = ue.getResponse();

      assertEquals(Status.BAD_REQUEST, response.getClientResponseStatus());
      assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
      JSONObject msg = response.getEntity(JSONObject.class);
      JSONObject exception = msg.getJSONObject("RemoteException");
      assertEquals("incorrect number of elements", 3, exception.length());
      String message = exception.getString("message");
      String type = exception.getString("exception");
      String classname = exception.getString("javaClassName");
      WebServicesTestUtils.checkStringMatch("exception message",
          "java.lang.IllegalArgumentException: Invalid AppAttemptId:"
              + " appattempt_invalid_12_000001",
          message);
      WebServicesTestUtils.checkStringMatch("exception type",
          "BadRequestException", type);
      WebServicesTestUtils.checkStringMatch("exception classname",
          "org.apache.hadoop.yarn.webapp.BadRequestException", classname);

    } finally {
      rm.stop();
    }
  }
```
```
Cyclomatic Complexity	 1
Assertions		 3
Lines of Code		33
-  
  public void testNonexistAppAttempts() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    rm.submitApp(CONTAINER_MB, "testwordcount", "user1");
    amNodeManager.nodeHeartbeat(true);
    WebResource r = resource();

    try {
      r.path("ws").path("v1").path("cluster").path("apps")
          .path("application_00000_0099").accept(MediaType.APPLICATION_JSON)
          .get(JSONObject.class);
      fail("should have thrown exception on invalid appid");
    } catch (UniformInterfaceException ue) {
      ClientResponse response = ue.getResponse();

      assertEquals(Status.NOT_FOUND, response.getClientResponseStatus());
      assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());

      JSONObject msg = response.getEntity(JSONObject.class);
      JSONObject exception = msg.getJSONObject("RemoteException");
      assertEquals("incorrect number of elements", 3, exception.length());
      String message = exception.getString("message");
      String type = exception.getString("exception");
      String classname = exception.getString("javaClassName");
      WebServicesTestUtils.checkStringMatch("exception message",
          "java.lang.Exception: app with id: application_00000_0099 not found",
          message);
      WebServicesTestUtils.checkStringMatch("exception type",
          "NotFoundException", type);
      WebServicesTestUtils.checkStringMatch("exception classname",
          "org.apache.hadoop.yarn.webapp.NotFoundException", classname);
    } finally {
      rm.stop();
    }
  }
```
```
Cyclomatic Complexity	 1
Assertions		 4
Lines of Code		26
-  
  public void testAppAttemptsXML() throws JSONException, Exception {
    rm.start();
    String user = "user1";
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    RMApp app1 = rm.submitApp(CONTAINER_MB, "testwordcount", user);
    amNodeManager.nodeHeartbeat(true);
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .path("apps").path(app1.getApplicationId().toString())
        .path("appattempts").accept(MediaType.APPLICATION_XML)
        .get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_XML_TYPE, response.getType());
    String xml = response.getEntity(String.class);

    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    DocumentBuilder db = dbf.newDocumentBuilder();
    InputSource is = new InputSource();
    is.setCharacterStream(new StringReader(xml));
    Document dom = db.parse(is);
    NodeList nodes = dom.getElementsByTagName("appAttempts");
    assertEquals("incorrect number of elements", 1, nodes.getLength());
    NodeList attempt = dom.getElementsByTagName("appAttempt");
    assertEquals("incorrect number of elements", 1, attempt.getLength());
    verifyAppAttemptsXML(attempt, app1.getCurrentAppAttempt(), user);
    rm.stop();
  }
```
## b9b49ed956e6fa9b55758f3d2c1b92ae2597cdbb ##
```
Cyclomatic Complexity	1
Assertions		3
Lines of Code		16
-  
  public void testGetConfiguration() throws ServletException {
    AuthFilter filter = new AuthFilter();
    Map<String, String> m = new HashMap<String,String>();
    m.put(DFSConfigKeys.DFS_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL_KEY,
        "xyz/thehost@REALM");
    m.put(DFSConfigKeys.DFS_WEB_AUTHENTICATION_KERBEROS_KEYTAB_KEY,
        "thekeytab");
    FilterConfig config = new DummyFilterConfig(m);
    Properties p = filter.getConfiguration("random", config);
    Assert.assertEquals("xyz/thehost@REALM",
        p.getProperty("kerberos.principal"));
    Assert.assertEquals("thekeytab", p.getProperty("kerberos.keytab"));
    Assert.assertEquals("true",
        p.getProperty(PseudoAuthenticationHandler.ANONYMOUS_ALLOWED));
  }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		11
-  
  public void testGetSimpleAuthDisabledConfiguration() throws ServletException {
    AuthFilter filter = new AuthFilter();
    Map<String, String> m = new HashMap<String,String>();
    m.put(DFSConfigKeys.DFS_WEB_AUTHENTICATION_SIMPLE_ANONYMOUS_ALLOWED,
        "false");
    FilterConfig config = new DummyFilterConfig(m);
    Properties p = filter.getConfiguration("random", config);
    Assert.assertEquals("false",
        p.getProperty(PseudoAuthenticationHandler.ANONYMOUS_ALLOWED));
  }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		9
-  
  public void testGetSimpleAuthDefaultConfiguration() throws ServletException {
    AuthFilter filter = new AuthFilter();
    Map<String, String> m = new HashMap<String,String>();
    
    FilterConfig config = new DummyFilterConfig(m);
    Properties p = filter.getConfiguration("random", config);
    Assert.assertEquals("true",
        p.getProperty(PseudoAuthenticationHandler.ANONYMOUS_ALLOWED));
  }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		16
-  
  public void testCredsFromUserInfo() throws Exception {
    // set up conf to have a cred provider
    final Configuration conf = new Configuration();
    final File file = tempDir.newFile("test.jks");
    final URI jks = ProviderUtils.nestURIForLocalJavaKeyStoreProvider(
        file.toURI());
    conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH,
        jks.toString());

    provisionAccessKeys(conf);

    conf.set(Constants.ACCESS_KEY, EXAMPLE_ID + "LJM");
    URI uriWithUserInfo = new URI("s3a://123:456@foobar");
    S3xLoginHelper.Login creds =
        S3AUtils.getAWSAccessKeys(uriWithUserInfo, conf);
    assertEquals("AccessKey incorrect.", "123", creds.getUser());
    assertEquals("SecretKey incorrect.", "456", creds.getPassword());
  }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		17
-  
  public void testIDFromUserInfoSecretFromCredentialProvider()
      throws Exception {
    // set up conf to have a cred provider
    final Configuration conf = new Configuration();
    final File file = tempDir.newFile("test.jks");
    final URI jks = ProviderUtils.nestURIForLocalJavaKeyStoreProvider(
        file.toURI());
    conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH,
        jks.toString());

    provisionAccessKeys(conf);

    conf.set(Constants.ACCESS_KEY, EXAMPLE_ID + "LJM");
    URI uriWithUserInfo = new URI("s3a://123@foobar");
    S3xLoginHelper.Login creds =
        S3AUtils.getAWSAccessKeys(uriWithUserInfo, conf);
    assertEquals("AccessKey incorrect.", "123", creds.getUser());
    assertEquals("SecretKey incorrect.", EXAMPLE_KEY, creds.getPassword());@@@   }
```
```
Cyclomatic Complexity	 -1
Assertions		 6
Lines of Code		37
-  
  public void testReadFileChanged() throws Throwable {
    describe("overwrite a file with a shorter one during a read, seek");
    final int fullLength = 8192;
    final byte[] fullDataset = dataset(fullLength, 'a', 32);
    final int shortLen = 4096;
    final byte[] shortDataset = dataset(shortLen, 'A', 32);
    final FileSystem fs = getFileSystem();
    final Path testpath = path("readFileToChange.txt");
    // initial write
    writeDataset(fs, testpath, fullDataset, fullDataset.length, 1024, false);
    try(FSDataInputStream instream = fs.open(testpath)) {
      instream.seek(fullLength - 16);
      assertTrue("no data to read", instream.read() >= 0);
      // overwrite
      writeDataset(fs, testpath, shortDataset, shortDataset.length, 1024, true);
      // here the file length is less. Probe the file to see if this is true,
      // with a spin and wait
      eventually(30 * 1000, 1000,
          () -> {
            assertEquals(shortLen, fs.getFileStatus(testpath).getLen());
          });

      // here length is shorter. Assuming it has propagated to all replicas,
      // the position of the input stream is now beyond the EOF.
      // An attempt to seek backwards to a position greater than the
      // short length will raise an exception from AWS S3, which must be
      // translated into an EOF

      instream.seek(shortLen + 1024);
      int c = instream.read();
      assertIsEOF("read()", c);

      byte[] buf = new byte[256];

      assertIsEOF("read(buffer)", instream.read(buf));
      assertIsEOF("read(offset)",
          instream.read(instream.getPos(), buf, 0, buf.length));

      // now do a block read fully, again, backwards from the current pos
      intercept(EOFException.class, "", "readfully",
          () -> instream.readFully(shortLen + 512, buf));

      assertIsEOF("read(offset)",
          instream.read(shortLen + 510, buf, 0, buf.length));

      // seek somewhere useful
      instream.seek(shortLen - 256);

      // delete the file. Reads must fail
      fs.delete(testpath, false);

      intercept(FileNotFoundException.class, "", "read()",
          () -> instream.read());
      intercept(FileNotFoundException.class, "", "readfully",
          () -> instream.readFully(2048, buf));

    }@@@   }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		6
-  
  public void testLoginWithUserAndPass() throws Throwable {
    S3xLoginHelper.Login login = assertMatchesLogin(USER, PASS,
        WITH_USER_AND_PASS);
    assertTrue("Login of " + login, login.hasLogin());
  }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		4
-  
  public void testLoginWithSlashInPass() throws Throwable {
    assertMatchesLogin(USER, "pa//", WITH_SLASH_IN_PASS);
  }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		4
-  
  public void testLoginWithPlusInPass() throws Throwable {
    assertMatchesLogin(USER, "pa+", WITH_PLUS_IN_PASS);
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		3
-  
  public void testPathURIFixup() throws Throwable {

  }
```
```
Cyclomatic Complexity	 1
Assertions		 8
Lines of Code		30
-  
  public void testQueryRMNodes() throws Exception {
    RMContext rmContext = Mockito.mock(RMContext.class);
    NodeId node1 = NodeId.newInstance("node1", 1234);
    RMNode rmNode1 = Mockito.mock(RMNode.class);
    ConcurrentMap<NodeId, RMNode> inactiveList =
        new ConcurrentHashMap<NodeId, RMNode>();
    Mockito.when(rmNode1.getState()).thenReturn(NodeState.SHUTDOWN);
    inactiveList.put(node1, rmNode1);
    Mockito.when(rmContext.getInactiveRMNodes()).thenReturn(inactiveList);
    List<RMNode> result = RMServerUtils.queryRMNodes(rmContext,
        EnumSet.of(NodeState.SHUTDOWN));
    Assert.assertTrue(result.size() != 0);
    Assert.assertEquals(result.get(0), rmNode1);
    Mockito.when(rmNode1.getState()).thenReturn(NodeState.DECOMMISSIONED);
    result = RMServerUtils.queryRMNodes(rmContext,
        EnumSet.of(NodeState.DECOMMISSIONED));
    Assert.assertTrue(result.size() != 0);
    Assert.assertEquals(result.get(0), rmNode1);
    Mockito.when(rmNode1.getState()).thenReturn(NodeState.LOST);
    result = RMServerUtils.queryRMNodes(rmContext,
        EnumSet.of(NodeState.LOST));
    Assert.assertTrue(result.size() != 0);
    Assert.assertEquals(result.get(0), rmNode1);
    Mockito.when(rmNode1.getState()).thenReturn(NodeState.REBOOTED);
    result = RMServerUtils.queryRMNodes(rmContext,
        EnumSet.of(NodeState.REBOOTED));
    Assert.assertTrue(result.size() != 0);
    Assert.assertEquals(result.get(0), rmNode1);@@@   }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		28
-  
  public void testParentWithReservation() throws Exception {
    Configuration conf = new Configuration();
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);

    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<queue name=\"parent\">");
    out.println("<reservation>");
    out.println("</reservation>");
    out.println(" <queue name=\"child\">");
    out.println(" </queue>");
    out.println("</queue>");
    out.println("</allocations>");
    out.close();

    AllocationFileLoaderService allocLoader = new AllocationFileLoaderService();
    allocLoader.init(conf);
    ReloadListener confHolder = new ReloadListener();
    allocLoader.setReloadListener(confHolder);
    try {
      allocLoader.reloadAllocations();
    } catch (AllocationConfigurationException ex) {
      assertEquals(ex.getMessage(), "The configuration settings for root.parent"
          + " are invalid. A queue element that contains child queue elements"
          + " or that has the type='parent' attribute cannot also include a"
          + " reservation element.");
    }
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		24
-  
  public void testAssignToBadDefaultQueue() throws Exception {
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);

    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<queuePlacementPolicy>");
    out.println("<rule name=\"specified\" create=\"false\" />");
    out.println("<rule name=\"default\" create=\"false\" />");
    out.println("</queuePlacementPolicy>");
    out.println("</allocations>");
    out.close();
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    RMApp rmApp1 = new MockRMApp(0, 0, RMAppState.NEW);

    try {
      FSLeafQueue queue1 = scheduler.assignToQueue(rmApp1, "default",
          "asterix");
    } catch (IllegalStateException ise) {
      fail("Bad queue placement policy terminal rule should not throw " +
          "exception ");
    }
  }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		13
-  
  public void testAssignToNonLeafQueueReturnsNull() throws Exception {
    conf.set(FairSchedulerConfiguration.USER_AS_DEFAULT_QUEUE, "true");
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    scheduler.getQueueManager().getLeafQueue("root.child1.granchild", true);
    scheduler.getQueueManager().getLeafQueue("root.child2", true);

    RMApp rmApp1 = new MockRMApp(0, 0, RMAppState.NEW);
    RMApp rmApp2 = new MockRMApp(1, 1, RMAppState.NEW);

    // Trying to assign to non leaf queue would return null
    assertNull(scheduler.assignToQueue(rmApp1, "root.child1", "tintin"));
    assertNotNull(scheduler.assignToQueue(rmApp2, "root.child2", "snowy"));
  }
```
```
Cyclomatic Complexity	 1
Assertions		 9
Lines of Code		51
-  
  public void testQueuePlacementWithPolicy() throws Exception {
    conf.setClass(CommonConfigurationKeys.HADOOP_SECURITY_GROUP_MAPPING,
        SimpleGroupsMapping.class, GroupMappingServiceProvider.class);
    Groups.getUserToGroupsMappingServiceWithLoadedConfiguration(conf);
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    ApplicationAttemptId appId;

    List<QueuePlacementRule> rules = new ArrayList<QueuePlacementRule>();
    rules.add(new QueuePlacementRule.Specified().initialize(true, null));
    rules.add(new QueuePlacementRule.User().initialize(false, null));
    rules.add(new QueuePlacementRule.PrimaryGroup().initialize(false, null));
    rules.add(new QueuePlacementRule.SecondaryGroupExistingQueue().initialize(false, null));
    rules.add(new QueuePlacementRule.Default().initialize(true, null));
    Set<String> queues = Sets.newHashSet("root.user1", "root.user3group",
        "root.user4subgroup1", "root.user4subgroup2" , "root.user5subgroup2");
    Map<FSQueueType, Set<String>> configuredQueues = new HashMap<FSQueueType, Set<String>>();
    configuredQueues.put(FSQueueType.LEAF, queues);
    configuredQueues.put(FSQueueType.PARENT, new HashSet<String>());
    scheduler.getAllocationConfiguration().placementPolicy =
        new QueuePlacementPolicy(rules, configuredQueues, conf);
    appId = createSchedulingRequest(1024, "somequeue", "user1");
    assertEquals("root.somequeue", scheduler.getSchedulerApp(appId).getQueueName());
    appId = createSchedulingRequest(1024, "default", "user1");
    assertEquals("root.user1", scheduler.getSchedulerApp(appId).getQueueName());
    appId = createSchedulingRequest(1024, "default", "user3");
    assertEquals("root.user3group", scheduler.getSchedulerApp(appId).getQueueName());
    appId = createSchedulingRequest(1024, "default", "user4");
    assertEquals("root.user4subgroup1", scheduler.getSchedulerApp(appId).getQueueName());
    appId = createSchedulingRequest(1024, "default", "user5");
    assertEquals("root.user5subgroup2", scheduler.getSchedulerApp(appId).getQueueName());
    appId = createSchedulingRequest(1024, "default", "otheruser");
    assertEquals("root.default", scheduler.getSchedulerApp(appId).getQueueName());
    
    // test without specified as first rule
    rules = new ArrayList<QueuePlacementRule>();
    rules.add(new QueuePlacementRule.User().initialize(false, null));
    rules.add(new QueuePlacementRule.Specified().initialize(true, null));
    rules.add(new QueuePlacementRule.Default().initialize(true, null));
    scheduler.getAllocationConfiguration().placementPolicy =
        new QueuePlacementPolicy(rules, configuredQueues, conf);
    appId = createSchedulingRequest(1024, "somequeue", "user1");
    assertEquals("root.user1", scheduler.getSchedulerApp(appId).getQueueName());
    appId = createSchedulingRequest(1024, "somequeue", "otheruser");
    assertEquals("root.somequeue", scheduler.getSchedulerApp(appId).getQueueName());
    appId = createSchedulingRequest(1024, "default", "otheruser");
    assertEquals("root.default", scheduler.getSchedulerApp(appId).getQueueName());

    // undo the group change
    conf.setClass(CommonConfigurationKeys.HADOOP_SECURITY_GROUP_MAPPING,
        JniBasedUnixGroupsMappingWithFallback.class,
        GroupMappingServiceProvider.class);
    Groups.getUserToGroupsMappingServiceWithLoadedConfiguration(conf);
  }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		33
-  
  public void testNestedUserQueue() throws IOException {
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);
    conf.setClass(CommonConfigurationKeys.HADOOP_SECURITY_GROUP_MAPPING,
        SimpleGroupsMapping.class, GroupMappingServiceProvider.class);
    Groups.getUserToGroupsMappingServiceWithLoadedConfiguration(conf);
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<queue name=\"user1group\" type=\"parent\">");
    out.println("<minResources>1024mb,0vcores</minResources>");
    out.println("</queue>");
    out.println("<queuePlacementPolicy>");
    out.println("<rule name=\"specified\" create=\"false\" />");
    out.println("<rule name=\"nestedUserQueue\">");
    out.println("     <rule name=\"primaryGroup\" create=\"false\" />");
    out.println("</rule>");
    out.println("<rule name=\"default\" />");
    out.println("</queuePlacementPolicy>");
    out.println("</allocations>");
    out.close();

    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());
    RMApp rmApp1 = new MockRMApp(0, 0, RMAppState.NEW);

    FSLeafQueue user1Leaf = scheduler.assignToQueue(rmApp1, "root.default",
        "user1");

    assertEquals("root.user1group.user1", user1Leaf.getName());
    // undo the group change
    conf.setClass(CommonConfigurationKeys.HADOOP_SECURITY_GROUP_MAPPING,
        JniBasedUnixGroupsMappingWithFallback.class,
        GroupMappingServiceProvider.class);
    Groups.getUserToGroupsMappingServiceWithLoadedConfiguration(conf);
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		10
-  
  public void testAppAttempts() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    RMApp app1 = rm.submitApp(CONTAINER_MB, "testwordcount", "user1");
    amNodeManager.nodeHeartbeat(true);
    testAppAttemptsHelper(app1.getApplicationId().toString(), app1,
        MediaType.APPLICATION_JSON);
    rm.stop();
  }
```
```
Cyclomatic Complexity	 4
Assertions		 2
Lines of Code		28
-   (timeout = 20000)
  public void testMultipleAppAttempts() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 8192);
    RMApp app1 = rm.submitApp(CONTAINER_MB, "testwordcount", "user1");
    MockAM am = MockRM.launchAndRegisterAM(app1, rm, amNodeManager);
    int maxAppAttempts = rm.getConfig().getInt(
        YarnConfiguration.RM_AM_MAX_ATTEMPTS,
        YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS);
    assertTrue(maxAppAttempts > 1);
    int numAttempt = 1;
    while (true) {
      // fail the AM by sending CONTAINER_FINISHED event without registering.
      amNodeManager.nodeHeartbeat(am.getApplicationAttemptId(), 1, ContainerState.COMPLETE);
      rm.waitForState(am.getApplicationAttemptId(), RMAppAttemptState.FAILED);
      if (numAttempt == maxAppAttempts) {
        rm.waitForState(app1.getApplicationId(), RMAppState.FAILED);
        break;
      }
      // wait for app to start a new attempt.
      rm.waitForState(app1.getApplicationId(), RMAppState.ACCEPTED);
      am = MockRM.launchAndRegisterAM(app1, rm, amNodeManager);
      numAttempt++;
    }
    assertEquals("incorrect number of attempts", maxAppAttempts,
        app1.getAppAttempts().values().size());
    testAppAttemptsHelper(app1.getApplicationId().toString(), app1,
        MediaType.APPLICATION_JSON);
    rm.stop();
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		10
-  
  public void testAppAttemptsSlash() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    RMApp app1 = rm.submitApp(CONTAINER_MB);
    amNodeManager.nodeHeartbeat(true);
    testAppAttemptsHelper(app1.getApplicationId().toString() + "/", app1,
        MediaType.APPLICATION_JSON);
    rm.stop();
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		9
-  
  public void testAppAttemtpsDefault() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    RMApp app1 = rm.submitApp(CONTAINER_MB);
    amNodeManager.nodeHeartbeat(true);
    testAppAttemptsHelper(app1.getApplicationId().toString() + "/", app1, "");
    rm.stop();
  }
```
```
Cyclomatic Complexity	 1
Assertions		 3
Lines of Code		35
-  
  public void testInvalidAppIdGetAttempts() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    RMApp app = rm.submitApp(CONTAINER_MB);
    amNodeManager.nodeHeartbeat(true);
    WebResource r = resource();

    try {
      r.path("ws").path("v1").path("cluster").path("apps")
          .path("application_invalid_12").path("appattempts")
          .accept(MediaType.APPLICATION_JSON)
          .get(JSONObject.class);
      fail("should have thrown exception on invalid appAttempt");
    } catch (UniformInterfaceException ue) {
      ClientResponse response = ue.getResponse();

      assertResponseStatusCode(Status.BAD_REQUEST, response.getStatusInfo());
      assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
          response.getType().toString());
      JSONObject msg = response.getEntity(JSONObject.class);
      JSONObject exception = msg.getJSONObject("RemoteException");
      assertEquals("incorrect number of elements", 3, exception.length());
      String message = exception.getString("message");
      String type = exception.getString("exception");
      String classname = exception.getString("javaClassName");
      WebServicesTestUtils.checkStringMatch("exception message",
          "java.lang.IllegalArgumentException: Invalid ApplicationId:"
              + " application_invalid_12",
          message);
      WebServicesTestUtils.checkStringMatch("exception type",
          "BadRequestException", type);
      WebServicesTestUtils.checkStringMatch("exception classname",
          "org.apache.hadoop.yarn.webapp.BadRequestException", classname);

    } finally {
      rm.stop();
    }
  }
```
```
Cyclomatic Complexity	 1
Assertions		 3
Lines of Code		35
-  
  public void testInvalidAppAttemptId() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    RMApp app = rm.submitApp(CONTAINER_MB);
    amNodeManager.nodeHeartbeat(true);
    WebResource r = resource();

    try {
      r.path("ws").path("v1").path("cluster").path("apps")
          .path(app.getApplicationId().toString()).path("appattempts")
          .path("appattempt_invalid_12_000001")
          .accept(MediaType.APPLICATION_JSON).get(JSONObject.class);
      fail("should have thrown exception on invalid appAttempt");
    } catch (UniformInterfaceException ue) {
      ClientResponse response = ue.getResponse();

      assertResponseStatusCode(Status.BAD_REQUEST, response.getStatusInfo());
      assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
          response.getType().toString());
      JSONObject msg = response.getEntity(JSONObject.class);
      JSONObject exception = msg.getJSONObject("RemoteException");
      assertEquals("incorrect number of elements", 3, exception.length());
      String message = exception.getString("message");
      String type = exception.getString("exception");
      String classname = exception.getString("javaClassName");
      WebServicesTestUtils.checkStringMatch("exception message",
          "java.lang.IllegalArgumentException: Invalid AppAttemptId:"
              + " appattempt_invalid_12_000001",
          message);
      WebServicesTestUtils.checkStringMatch("exception type",
          "BadRequestException", type);
      WebServicesTestUtils.checkStringMatch("exception classname",
          "org.apache.hadoop.yarn.webapp.BadRequestException", classname);

    } finally {
      rm.stop();
    }
  }
```
```
Cyclomatic Complexity	 1
Assertions		 3
Lines of Code		34
-  
  public void testNonexistAppAttempts() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    rm.submitApp(CONTAINER_MB, "testwordcount", "user1");
    amNodeManager.nodeHeartbeat(true);
    WebResource r = resource();

    try {
      r.path("ws").path("v1").path("cluster").path("apps")
          .path("application_00000_0099").accept(MediaType.APPLICATION_JSON)
          .get(JSONObject.class);
      fail("should have thrown exception on invalid appid");
    } catch (UniformInterfaceException ue) {
      ClientResponse response = ue.getResponse();

      assertResponseStatusCode(Status.NOT_FOUND, response.getStatusInfo());
      assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
          response.getType().toString());

      JSONObject msg = response.getEntity(JSONObject.class);
      JSONObject exception = msg.getJSONObject("RemoteException");
      assertEquals("incorrect number of elements", 3, exception.length());
      String message = exception.getString("message");
      String type = exception.getString("exception");
      String classname = exception.getString("javaClassName");
      WebServicesTestUtils.checkStringMatch("exception message",
          "java.lang.Exception: app with id: application_00000_0099 not found",
          message);
      WebServicesTestUtils.checkStringMatch("exception type",
          "NotFoundException", type);
      WebServicesTestUtils.checkStringMatch("exception classname",
          "org.apache.hadoop.yarn.webapp.NotFoundException", classname);
    } finally {
      rm.stop();
    }
  }
```
```
Cyclomatic Complexity	 1
Assertions		 4
Lines of Code		27
-  
  public void testAppAttemptsXML() throws JSONException, Exception {
    rm.start();
    String user = "user1";
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    RMApp app1 = rm.submitApp(CONTAINER_MB, "testwordcount", user);
    amNodeManager.nodeHeartbeat(true);
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .path("apps").path(app1.getApplicationId().toString())
        .path("appattempts").accept(MediaType.APPLICATION_XML)
        .get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_XML_TYPE + "; " + JettyUtils.UTF_8,
        response.getType().toString());
    String xml = response.getEntity(String.class);

    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    DocumentBuilder db = dbf.newDocumentBuilder();
    InputSource is = new InputSource();
    is.setCharacterStream(new StringReader(xml));
    Document dom = db.parse(is);
    NodeList nodes = dom.getElementsByTagName("appAttempts");
    assertEquals("incorrect number of elements", 1, nodes.getLength());
    NodeList attempt = dom.getElementsByTagName("appAttempt");
    assertEquals("incorrect number of elements", 1, attempt.getLength());
    verifyAppAttemptsXML(attempt, app1.getCurrentAppAttempt(), user);
    rm.stop();
  }
```
