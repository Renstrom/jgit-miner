## 14785f4e390e3f2dcbfa223bd33dbc1907f280d3 ##
```
Commit	14785f4e390e3f2dcbfa223bd33dbc1907f280d3
Directory name		dolphinscheduler-api
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		14
    
    public void testGetReceiverCc() throws Exception {
        MultiValueMap<String, String> paramsMap = new LinkedMultiValueMap<>();
        paramsMap.add("processInstanceId","13");
        MvcResult mvcResult = mockMvc.perform(get("/projects/{projectName}/executors/get-receiver-cc","cxc_1113")
                .header(SESSION_ID, sessionId)
                .params(paramsMap))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON_UTF8))
                .andReturn();
        Result result = JSONUtils.parseObject(mvcResult.getResponse().getContentAsString(), Result.class);
        Assert.assertEquals(Status.SUCCESS.getCode(),result.getCode().intValue());
        logger.info(mvcResult.getResponse().getContentAsString());
    }
```
```
Previous Commit	14785f4e390e3f2dcbfa223bd33dbc1907f280d3
Directory name	dolphinscheduler-api
Cyclomatic Complexity	 1
Assertions		 3
Lines of Code		16
    
    public  void testDelAlertgroupById(){

        User user = new User();
        // no operate
        Map<String, Object>  result = alertGroupService.delAlertgroupById(user,1);
        logger.info(result.toString());
        Assert.assertEquals(Status.USER_NO_OPERATION_PERM,result.get(Constants.STATUS));
        user.setUserType(UserType.ADMIN_USER);
        // not exist
        result = alertGroupService.delAlertgroupById(user,2);
        logger.info(result.toString());
        Assert.assertEquals(Status.ALERT_GROUP_NOT_EXIST,result.get(Constants.STATUS));
        //success
        Mockito.when(alertGroupMapper.selectById(2)).thenReturn(getEntity());
        result = alertGroupService.delAlertgroupById(user,2);
        logger.info(result.toString());
        Assert.assertEquals(Status.SUCCESS,result.get(Constants.STATUS));

 
     }
```
```
Previous Commit	14785f4e390e3f2dcbfa223bd33dbc1907f280d3
Directory name	dolphinscheduler-dao
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		10
    
    public void testQueryByUserId() {
        Integer count = 4;
        Integer userId = 1;

        Map<Integer, AlertGroup> alertGroupMap =
                createAlertGroups(count, userId);

        List<AlertGroup> alertGroupList =
                alertGroupMapper.queryByUserId(userId);

        compareAlertGroups(alertGroupMap,alertGroupList);

    }
```
```
Previous Commit	14785f4e390e3f2dcbfa223bd33dbc1907f280d3
Directory name	dolphinscheduler-dao
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		7
    
    public void testQueryByAlertType() {
        Integer count = 4;

        Map<Integer, AlertGroup> alertGroupMap = createAlertGroups(count);
        List<AlertGroup> alertGroupList = alertGroupMapper.queryByAlertType(AlertType.EMAIL);

        compareAlertGroups(alertGroupMap,alertGroupList);

     }
```
```
Previous Commit	14785f4e390e3f2dcbfa223bd33dbc1907f280d3
Directory name	dolphinscheduler-dao
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		0
//    
//    public void testQueryByUserNameAccurately() {
//        //insertOne
//        User user = insertOne();
//        //queryByUserNameAccurately
//        User queryUser = userMapper.queryByUserNameAccurately(user.getUserName());
//        Assert.assertEquals(queryUser.getUserName(), user.getUserName());
//    }
```
```
Previous Commit	14785f4e390e3f2dcbfa223bd33dbc1907f280d3
Directory name	dolphinscheduler-dao
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		0
//    
//    public void testQueryUserByNamePassword() {
//        //insertOne
//        User user = insertOne();
//        //queryUserByNamePassword
//        User queryUser = userMapper.queryUserByNamePassword(user.getUserName(),user.getUserPassword());
//        Assert.assertEquals(queryUser.getUserName(),user.getUserName());
//        Assert.assertEquals(queryUser.getUserPassword(), user.getUserPassword());
//    }
```
```
Previous Commit	14785f4e390e3f2dcbfa223bd33dbc1907f280d3
Directory name	dolphinscheduler-dao
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		8
    
    public void testQueryUserListByAlertGroupId() {
        //insertOne
        User user = insertOne();
        //insertOneAlertGroup
        AlertGroup alertGroup = insertOneAlertGroup();
        //insertOneUserAlertGroup
        UserAlertGroup userAlertGroup = insertOneUserAlertGroup(user, alertGroup);
        //queryUserListByAlertGroupId
        List<User> userList = userMapper.queryUserListByAlertGroupId(userAlertGroup.getAlertgroupId());
        Assert.assertNotEquals(userList.size(), 0);

     }
```
## ca66cfec171be8cd7a7359e47350580943222c42 ##
```
Commit	ca66cfec171be8cd7a7359e47350580943222c42
Directory name		dolphinscheduler-api
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		14
    
    public void testVerifyProccessDefinitionName() throws Exception {
        MultiValueMap<String, String> paramsMap = new LinkedMultiValueMap<>();
        paramsMap.add("name","dag_test");

        MvcResult mvcResult = mockMvc.perform(get("/projects/{projectName}/process/verify-name","cxc_1113")
                .header(SESSION_ID, sessionId)
                .params(paramsMap))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON_UTF8))
                .andReturn();

        Result result = JSONUtils.parseObject(mvcResult.getResponse().getContentAsString(), Result.class);
        Assert.assertEquals(Status.PROCESS_INSTANCE_EXIST.getCode(),result.getCode().intValue());
        logger.info(mvcResult.getResponse().getContentAsString());
     }
```
```
Previous Commit	ca66cfec171be8cd7a7359e47350580943222c42
Directory name	dolphinscheduler-api
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		17
    
    public void testQueryProcessDefinitionListPaging() throws Exception {
        MultiValueMap<String, String> paramsMap = new LinkedMultiValueMap<>();
        paramsMap.add("pageNo","1");
        paramsMap.add("searchVal","test");
        paramsMap.add("userId","");
        paramsMap.add("pageSize", "1");

        MvcResult mvcResult = mockMvc.perform(get("/projects/{projectName}/process/list-paging","cxc_1113")
                .header(SESSION_ID, sessionId)
                .params(paramsMap))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON_UTF8))
                .andReturn();

        Result result = JSONUtils.parseObject(mvcResult.getResponse().getContentAsString(), Result.class);
        Assert.assertEquals(Status.SUCCESS.getCode(),result.getCode().intValue());
        logger.info(mvcResult.getResponse().getContentAsString());
    }
```
```
Previous Commit	ca66cfec171be8cd7a7359e47350580943222c42
Directory name	dolphinscheduler-api
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		15
    
    public void testViewTree() throws Exception {
        MultiValueMap<String, String> paramsMap = new LinkedMultiValueMap<>();
        paramsMap.add("processId","91");
        paramsMap.add("limit","30");

        MvcResult mvcResult = mockMvc.perform(get("/projects/{projectName}/process/view-tree","cxc_1113")
                .header(SESSION_ID, sessionId)
                .params(paramsMap))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON_UTF8))
                .andReturn();

        Result result = JSONUtils.parseObject(mvcResult.getResponse().getContentAsString(), Result.class);
        Assert.assertEquals(Status.SUCCESS.getCode(),result.getCode().intValue());
        logger.info(mvcResult.getResponse().getContentAsString());
    }
```
```
Previous Commit	ca66cfec171be8cd7a7359e47350580943222c42
Directory name	dolphinscheduler-api
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		12
    
    public void testExportProcessDefinitionById() throws Exception {
        MultiValueMap<String, String> paramsMap = new LinkedMultiValueMap<>();
        paramsMap.add("processDefinitionId","91");

        MvcResult mvcResult = mockMvc.perform(get("/projects/{projectName}/process/export","cxc_1113")
                .header(SESSION_ID, sessionId)
                .params(paramsMap))
//                .andExpect(status().isOk())
//                .andExpect(content().contentType(MediaType.APPLICATION_JSON_UTF8))
                .andReturn();

        Result result = JSONUtils.parseObject(mvcResult.getResponse().getContentAsString(), Result.class);
        Assert.assertEquals(Status.SUCCESS.getCode(),result.getCode().intValue());
        logger.info(mvcResult.getResponse().getContentAsString());
    }
```
```
Previous Commit	ca66cfec171be8cd7a7359e47350580943222c42
Directory name	dolphinscheduler-api
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		14
    
    public void testQueryProccessDefinitionAllByProjectId() throws Exception {
        MultiValueMap<String, String> paramsMap = new LinkedMultiValueMap<>();
        paramsMap.add("projectId","9");

        MvcResult mvcResult = mockMvc.perform(get("/projects/{projectName}/process/queryProccessDefinitionAllByProjectId","cxc_1113")
                .header(SESSION_ID, sessionId)
                .params(paramsMap))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON_UTF8))
                .andReturn();

        Result result = JSONUtils.parseObject(mvcResult.getResponse().getContentAsString(), Result.class);
        Assert.assertEquals(Status.SUCCESS.getCode(),result.getCode().intValue());
        logger.info(mvcResult.getResponse().getContentAsString());
    }
```
```
Previous Commit	ca66cfec171be8cd7a7359e47350580943222c42
Directory name	dolphinscheduler-api
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		14
    
    public void testDeleteProcessDefinitionById() throws Exception {
        MultiValueMap<String, String> paramsMap = new LinkedMultiValueMap<>();
        paramsMap.add("processDefinitionId","73");

        MvcResult mvcResult = mockMvc.perform(get("/projects/{projectName}/process/delete","cxc_1113")
                .header(SESSION_ID, sessionId)
                .params(paramsMap))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON_UTF8))
                .andReturn();

        Result result = JSONUtils.parseObject(mvcResult.getResponse().getContentAsString(), Result.class);
        Assert.assertEquals(Status.SUCCESS.getCode(),result.getCode().intValue());
        logger.info(mvcResult.getResponse().getContentAsString());
    }
```
```
Previous Commit	ca66cfec171be8cd7a7359e47350580943222c42
Directory name	dolphinscheduler-api
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		14
    
    public void testBatchDeleteProcessDefinitionByIds() throws Exception {
        MultiValueMap<String, String> paramsMap = new LinkedMultiValueMap<>();
        paramsMap.add("processDefinitionIds","54,62");

        MvcResult mvcResult = mockMvc.perform(get("/projects/{projectName}/process/batch-delete","cxc_1113")
                .header(SESSION_ID, sessionId)
                .params(paramsMap))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON_UTF8))
                .andReturn();

        Result result = JSONUtils.parseObject(mvcResult.getResponse().getContentAsString(), Result.class);
        Assert.assertEquals(Status.SUCCESS.getCode(),result.getCode().intValue());
        logger.info(mvcResult.getResponse().getContentAsString());
     }
```
## 0c8d08cbaa0636ef93cbcb01c43a34f002bb7fdb ##
```
Commit	0c8d08cbaa0636ef93cbcb01c43a34f002bb7fdb
Directory name		dolphinscheduler-alert
Cyclomatic Complexity	 -1
Assertions		 1
Lines of Code		17
    
    public void process() {
        AlertInfo alertInfo = new AlertInfo();
        AlertData alertData = new AlertData();
        alertData.setId(1)
                .setAlertGroupId(1)
                .setContent("[\"alarm time：2018-02-05\", \"service name：MYSQL_ALTER\", \"alarm name：MYSQL_ALTER_DUMP\", " +
                        "\"get the alarm exception.！，interface error，exception information：timed out\", \"request address：http://blog.csdn.net/dreamInTheWorld/article/details/78539286\"]")
                .setLog("test log")
                .setReceivers("xx@xx.com")
                .setReceiversCc("xx@xx.com")
                .setShowType(ShowType.TEXT.getDescp())
                .setTitle("test title");

        alertInfo.setAlertData(alertData);
        List<String> list = new ArrayList<String>(){{ add("xx@xx.com"); }};
        alertInfo.addProp("receivers", list);
//        Map<String, Object> ret = plugin.process(alertInfo);
//        assertFalse(Boolean.parseBoolean(String.valueOf(ret.get(Constants.STATUS))));
    }
```
```
Previous Commit	0c8d08cbaa0636ef93cbcb01c43a34f002bb7fdb
Directory name	dolphinscheduler-api
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		14
    
    public void testGetReceiverCc() throws Exception {
        MultiValueMap<String, String> paramsMap = new LinkedMultiValueMap<>();
        paramsMap.add("processInstanceId","13");
        MvcResult mvcResult = mockMvc.perform(get("/projects/{projectName}/executors/get-receiver-cc","cxc_1113")
                .header(SESSION_ID, sessionId)
                .params(paramsMap))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON_UTF8))
                .andReturn();
        Result result = JSONUtils.parseObject(mvcResult.getResponse().getContentAsString(), Result.class);
        Assert.assertEquals(Status.SUCCESS.getCode(),result.getCode().intValue());
        logger.info(mvcResult.getResponse().getContentAsString());
    }
```
```
Previous Commit	0c8d08cbaa0636ef93cbcb01c43a34f002bb7fdb
Directory name	dolphinscheduler-api
Cyclomatic Complexity	 1
Assertions		 3
Lines of Code		16
    
    public  void testDelAlertgroupById(){

        User user = new User();
        // no operate
        Map<String, Object>  result = alertGroupService.delAlertgroupById(user,1);
        logger.info(result.toString());
        Assert.assertEquals(Status.USER_NO_OPERATION_PERM,result.get(Constants.STATUS));
        user.setUserType(UserType.ADMIN_USER);
        // not exist
        result = alertGroupService.delAlertgroupById(user,2);
        logger.info(result.toString());
        Assert.assertEquals(Status.ALERT_GROUP_NOT_EXIST,result.get(Constants.STATUS));
        //success
        Mockito.when(alertGroupMapper.selectById(2)).thenReturn(getEntity());
        result = alertGroupService.delAlertgroupById(user,2);
        logger.info(result.toString());
        Assert.assertEquals(Status.SUCCESS,result.get(Constants.STATUS));

 
     }
```
```
Previous Commit	0c8d08cbaa0636ef93cbcb01c43a34f002bb7fdb
Directory name	dolphinscheduler-dao
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		10
    
    public void testQueryByUserId() {
        Integer count = 4;
        Integer userId = 1;

        Map<Integer, AlertGroup> alertGroupMap =
                createAlertGroups(count, userId);

        List<AlertGroup> alertGroupList =
                alertGroupMapper.queryByUserId(userId);

        compareAlertGroups(alertGroupMap,alertGroupList);

    }
```
```
Previous Commit	0c8d08cbaa0636ef93cbcb01c43a34f002bb7fdb
Directory name	dolphinscheduler-dao
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		7
    
    public void testQueryByAlertType() {
        Integer count = 4;

        Map<Integer, AlertGroup> alertGroupMap = createAlertGroups(count);
        List<AlertGroup> alertGroupList = alertGroupMapper.queryByAlertType(AlertType.EMAIL);

        compareAlertGroups(alertGroupMap,alertGroupList);

     }
```
```
Previous Commit	0c8d08cbaa0636ef93cbcb01c43a34f002bb7fdb
Directory name	dolphinscheduler-dao
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		0
//    
//    public void testQueryByUserNameAccurately() {
//        //insertOne
//        User user = insertOne();
//        //queryByUserNameAccurately
//        User queryUser = userMapper.queryByUserNameAccurately(user.getUserName());
//        Assert.assertEquals(queryUser.getUserName(), user.getUserName());
//    }
```
```
Previous Commit	0c8d08cbaa0636ef93cbcb01c43a34f002bb7fdb
Directory name	dolphinscheduler-dao
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		0
//    
//    public void testQueryUserByNamePassword() {
//        //insertOne
//        User user = insertOne();
//        //queryUserByNamePassword
//        User queryUser = userMapper.queryUserByNamePassword(user.getUserName(),user.getUserPassword());
//        Assert.assertEquals(queryUser.getUserName(),user.getUserName());
//        Assert.assertEquals(queryUser.getUserPassword(), user.getUserPassword());
//    }
```
```
Previous Commit	0c8d08cbaa0636ef93cbcb01c43a34f002bb7fdb
Directory name	dolphinscheduler-dao
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		8
    
    public void testQueryUserListByAlertGroupId() {
        //insertOne
        User user = insertOne();
        //insertOneAlertGroup
        AlertGroup alertGroup = insertOneAlertGroup();
        //insertOneUserAlertGroup
        UserAlertGroup userAlertGroup = insertOneUserAlertGroup(user, alertGroup);
        //queryUserListByAlertGroupId
        List<User> userList = userMapper.queryUserListByAlertGroupId(userAlertGroup.getAlertgroupId());
        Assert.assertNotEquals(userList.size(), 0);

     }
```
## eec92ed1fc1a35e22b8e38ac40681a1dd803399b ##
```
Commit	eec92ed1fc1a35e22b8e38ac40681a1dd803399b
Directory name		dolphinscheduler-common
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		4
    
    public void getHdfsResourceFileNameTest(){
        logger.info(HadoopUtils.getHdfsResourceFileName("test","/test"));
    }
```
```
Previous Commit	eec92ed1fc1a35e22b8e38ac40681a1dd803399b
Directory name	dolphinscheduler-common
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		4
    
    public void getHdfsUdfFileNameTest(){
        logger.info(HadoopUtils.getHdfsUdfFileName("test","/test.jar"));
    }
```
## 4d553603ee95d166811fc45edadd736aa1853af3 ##
```
Commit	4d553603ee95d166811fc45edadd736aa1853af3
Directory name		dolphinscheduler-common
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		17
    
    public void testSetTaskNodeLocalParams() {
        String taskJson = "{\"conditionResult\":\"{\\\"successNode\\\":[\\\"\\\"],\\\"failedNode\\\":[\\\"\\\"]}\","
            + "\"conditionsTask\":false,\"depList\":[],\"dependence\":\"{}\",\"forbidden\":false,\"id\":\"tasks-75298\",\"maxRetryTimes\":0,\"name\":\"a1\","
            + "\"params\":\"{\\\"rawScript\\\":\\\"print(\\\\\\\"this is python task \\\\\\\",${p0})\\\","
            + "\\\"localParams\\\":[{\\\"prop\\\":\\\"p1\\\",\\\"direct\\\":\\\"IN\\\",\\\"type\\\":\\\"VARCHAR\\\",\\\"value\\\":\\\"1\\\"}],"
            + "\\\"resourceList\\\":[]}\",\"preTasks\":\"[]\",\"retryInterval\":1,\"runFlag\":\"NORMAL\",\"taskInstancePriority\":\"MEDIUM\","
            + "\"taskTimeoutParameter\":{\"enable\":false,\"interval\":0},\"timeout\":\"{\\\"enable\\\":false,\\\"strategy\\\":\\\"\\\"}\","
            + "\"type\":\"PYTHON\",\"workerGroup\":\"default\"}";
        TaskNode taskNode = JSONUtils.parseObject(taskJson, TaskNode.class);
        
        VarPoolUtils.setTaskNodeLocalParams(taskNode, "p1", "test1");
        Assert.assertEquals(VarPoolUtils.getTaskNodeLocalParam(taskNode, "p1"), "test1");
        
        ConcurrentHashMap<String, Object> propToValue = new ConcurrentHashMap<String, Object>();
        propToValue.put("p1", "test2");
        
        VarPoolUtils.setTaskNodeLocalParams(taskNode, propToValue);
        Assert.assertEquals(VarPoolUtils.getTaskNodeLocalParam(taskNode, "p1"), "test2");
    }
```
```
Previous Commit	4d553603ee95d166811fc45edadd736aa1853af3
Directory name	dolphinscheduler-common
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		0
//    
//    public void replacePlaceholdersT() {
//        Assert.assertEquals("2017test12017:***2016-12-31,20170102,20170130,20161227,20161231", TimePlaceholderUtils.replacePlaceholders("$[yyyy]test1$[yyyy:***]$[yyyy-MM-dd-1],$[month_begin(yyyyMMdd, 1)],$[month_end(yyyyMMdd, -1)],$[week_begin(yyyyMMdd, 1)],$[week_end(yyyyMMdd, -1)]",
//                date, true));
//
//        Assert.assertEquals("1483200061,1483290061,1485709261,1482771661,1483113600,1483203661", TimePlaceholderUtils.replacePlaceholders("$[timestamp(yyyyMMdd00mmss)],"
//                        + "$[timestamp(month_begin(yyyyMMddHHmmss, 1))],"
//                        + "$[timestamp(month_end(yyyyMMddHHmmss, -1))],"
//                        + "$[timestamp(week_begin(yyyyMMddHHmmss, 1))],"
//                        + "$[timestamp(week_end(yyyyMMdd000000, -1))],"
//                        + "$[timestamp(yyyyMMddHHmmss)]",
//                date, true));
//    }
```
```
Previous Commit	4d553603ee95d166811fc45edadd736aa1853af3
Directory name	dolphinscheduler-common
Cyclomatic Complexity	 1
Assertions		 7
Lines of Code		0
//    
//    public void calcMinutesT() {
//        Assert.assertEquals("Sun Jan 01 01:01:01 CST 2017=yyyy", TimePlaceholderUtils.calcMinutes("yyyy", date).toString());
//        Assert.assertEquals("Sun Jan 08 01:01:01 CST 2017=yyyyMMdd", TimePlaceholderUtils.calcMinutes("yyyyMMdd+7*1", date).toString());
//        Assert.assertEquals("Sun Dec 25 01:01:01 CST 2016=yyyyMMdd", TimePlaceholderUtils.calcMinutes("yyyyMMdd-7*1", date).toString());
//        Assert.assertEquals("Mon Jan 02 01:01:01 CST 2017=yyyyMMdd", TimePlaceholderUtils.calcMinutes("yyyyMMdd+1", date).toString());
//        Assert.assertEquals("Sat Dec 31 01:01:01 CST 2016=yyyyMMdd", TimePlaceholderUtils.calcMinutes("yyyyMMdd-1", date).toString());
//        Assert.assertEquals("Sun Jan 01 02:01:01 CST 2017=yyyyMMddHH", TimePlaceholderUtils.calcMinutes("yyyyMMddHH+1/24", date).toString());
//        Assert.assertEquals("Sun Jan 01 00:01:01 CST 2017=yyyyMMddHH", TimePlaceholderUtils.calcMinutes("yyyyMMddHH-1/24", date).toString());
//    }
```
```
Previous Commit	4d553603ee95d166811fc45edadd736aa1853af3
Directory name	dolphinscheduler-common
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		0
//    
//    public void calcMonthsT() {
//        Assert.assertEquals("Mon Jan 01 01:01:01 CST 2018=yyyyMMdd", TimePlaceholderUtils.calcMonths("add_months(yyyyMMdd,12*1)", date).toString());
//        Assert.assertEquals("Fri Jan 01 01:01:01 CST 2016=yyyyMMdd", TimePlaceholderUtils.calcMonths("add_months(yyyyMMdd,-12*1)", date).toString());
//    }
```
```
Previous Commit	4d553603ee95d166811fc45edadd736aa1853af3
Directory name	dolphinscheduler-server
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		15
    
    public void testDagHelper(){

        ProcessDefinition processDefinition = processDefinitionMapper.selectById(19);

        try {
            ProcessDag processDag = DagHelper.generateFlowDag(processDefinition.getProcessDefinitionJson(),
                    new ArrayList<>(), new ArrayList<>(), TaskDependType.TASK_POST);

            DAG<String,TaskNode,TaskNodeRelation> dag = DagHelper.buildDagGraph(processDag);
            Collection<String> start = DagHelper.getStartVertex("1", dag, null);

            System.out.println(start.toString());

            Map<String, TaskNode> forbidden = DagHelper.getForbiddenTaskNodeMaps(processDefinition.getProcessDefinitionJson());
            System.out.println(forbidden);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
```
```
Previous Commit	4d553603ee95d166811fc45edadd736aa1853af3
Directory name	dolphinscheduler-server
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		12
    
    public void testAdd(){
        TaskResponseEvent taskResponseEvent = TaskResponseEvent.newAck(ExecutionStatus.RUNNING_EXECUTION, new Date(),
                "", "", "", 1);
        taskResponseService.addResponse(taskResponseEvent);
        Assert.assertTrue(taskResponseService.getEventQueue().size() == 1);
        try {
            Thread.sleep(10);
        } catch (InterruptedException ignore) {
        }
        //after sleep, inner worker will take the event
        Assert.assertTrue(taskResponseService.getEventQueue().size() == 0);
     }
```
```
Previous Commit	4d553603ee95d166811fc45edadd736aa1853af3
Directory name	dolphinscheduler-server
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		0
//    (expected = IllegalStateException.class)
//    public void testSendAckWithIllegalStateException2(){
//        masterRegistry.registry();
//        final NettyServerConfig serverConfig = new NettyServerConfig();
//        serverConfig.setListenPort(30000);
//        NettyRemotingServer nettyRemotingServer = new NettyRemotingServer(serverConfig);
//        nettyRemotingServer.registerProcessor(CommandType.TASK_EXECUTE_ACK, taskAckProcessor);
//        nettyRemotingServer.start();
//
//        final NettyClientConfig clientConfig = new NettyClientConfig();
//        NettyRemotingClient nettyRemotingClient = new NettyRemotingClient(clientConfig);
//        Channel channel = nettyRemotingClient.getChannel(Host.of("localhost:30000"));
//        taskCallbackService.addRemoteChannel(1, new NettyRemoteChannel(channel, 1));
//        channel.close();
//        TaskExecuteAckCommand ackCommand = new TaskExecuteAckCommand();
//        ackCommand.setTaskInstanceId(1);
//        ackCommand.setStartTime(new Date());
//
//        nettyRemotingServer.close();
//
//        taskCallbackService.sendAck(1, ackCommand.convert2Command());
//        try {
//            Thread.sleep(5000);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//
//        Stopper.stop();
//
//        try {
//            Thread.sleep(5000);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//    }
```
```
Previous Commit	4d553603ee95d166811fc45edadd736aa1853af3
Directory name	dolphinscheduler-server
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		9
    
    public void testShellTask()
            throws Exception {
        TaskProps props = new TaskProps();
        props.setTaskAppId(String.valueOf(System.currentTimeMillis()));
        props.setTenantCode("1");
        ShellTask shellTaskTest = new ShellTask(taskExecutionContext, logger);
        Assert.assertNotNull(shellTaskTest);
    }
```
```
Previous Commit	4d553603ee95d166811fc45edadd736aa1853af3
Directory name	dolphinscheduler-server
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		10
    
    public void testInitForUnix() {
        try {
            PowerMockito.when(OSUtils.isWindows()).thenReturn(false);
            shellTask.init();
            Assert.assertTrue(true);
        } catch (Error | Exception e) {
            logger.error(e.getMessage());
        }
     }
```
```
Previous Commit	4d553603ee95d166811fc45edadd736aa1853af3
Directory name	dolphinscheduler-server
Cyclomatic Complexity	 2
Assertions		 1
Lines of Code		24
    
    public void testHandleForUnix() throws Exception {
        try {
            PowerMockito.when(OSUtils.isWindows()).thenReturn(false);
            TaskProps props = new TaskProps();
            props.setTaskAppId(String.valueOf(System.currentTimeMillis()));
            props.setTenantCode("1");
            props.setEnvFile(".dolphinscheduler_env.sh");
            props.setTaskStartTime(new Date());
            props.setTaskTimeout(0);
            props.setScheduleTime(new Date());
            props.setCmdTypeIfComplement(CommandType.START_PROCESS);
            props.setTaskParams("{\"rawScript\": \" echo ${test}\", \"localParams\": [{\"prop\":\"test\", \"direct\":\"IN\", \"type\":\"VARCHAR\", \"value\":\"123\"}]}");
            ShellTask shellTask1 = new ShellTask(taskExecutionContext, logger);
            shellTask1.init();
            shellTask1.handle();
            Assert.assertTrue(true);
        } catch (Error | Exception e) {
            if (!e.getMessage().contains("process error . exitCode is :  -1")
                    && !System.getProperty("os.name").startsWith("Windows")) {
                logger.error(e.getMessage());
            }
        }
    }
```
```
Previous Commit	4d553603ee95d166811fc45edadd736aa1853af3
Directory name	dolphinscheduler-server
Cyclomatic Complexity	 2
Assertions		 1
Lines of Code		23
    
    public void testHandleForWindows() throws Exception {
        try {
            Assume.assumeTrue(OSUtils.isWindows());
            TaskProps props = new TaskProps();
            props.setTaskAppId(String.valueOf(System.currentTimeMillis()));
            props.setTenantCode("1");
            props.setEnvFile(".dolphinscheduler_env.sh");
            props.setTaskStartTime(new Date());
            props.setTaskTimeout(0);
            props.setScheduleTime(new Date());
            props.setCmdTypeIfComplement(CommandType.START_PROCESS);
            props.setTaskParams("{\"rawScript\": \" echo ${test}\", \"localParams\": [{\"prop\":\"test\", \"direct\":\"IN\", \"type\":\"VARCHAR\", \"value\":\"123\"}]}");
            ShellTask shellTask1 = new ShellTask(taskExecutionContext, logger);
            shellTask1.init();
            shellTask1.handle();
            Assert.assertTrue(true);
        } catch (Error | Exception e) {
            if (!e.getMessage().contains("process error . exitCode is :  -1")) {
                logger.error(e.getMessage());
            }
        }
    }
```
```
Previous Commit	4d553603ee95d166811fc45edadd736aa1853af3
Directory name	dolphinscheduler-server
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		9
    
    public void testCancelApplication() throws Exception {
        try {
            shellTask.cancelApplication(true);
            Assert.assertTrue(true);
        } catch (Error | Exception e) {
            logger.error(e.getMessage());
        }
    }
```
