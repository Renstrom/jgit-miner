## a3aff81e4677bb2c6c8952a4acce2ca994d90667 ##
```
Commit	a3aff81e4677bb2c6c8952a4acce2ca994d90667
Directory name		common
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		16
    
    public void testSubscriptionData() throws Exception {
        SubscriptionData subscriptionData =
            FilterAPI.buildSubscriptionData("ConsumerGroup1", "TestTopic", "TAG1 || Tag2 || tag3");
        subscriptionData.setFilterClassSource("java hello");
        String prettyJson = RemotingSerializable.toJson(subscriptionData, true);
        long subVersion = subscriptionData.getSubVersion();
        assertThat(prettyJson).isEqualTo("{\n" +
            "\t\"classFilterMode\":false,\n" +
            "\t\"codeSet\":[2567159,2598904,3552217],\n" +
            "\t\"subString\":\"TAG1 || Tag2 || tag3\",\n" +
            "\t\"subVersion\":" + subVersion + ",\n" +
            "\t\"tagsSet\":[\"TAG1\",\"Tag2\",\"tag3\"],\n" +
            "\t\"topic\":\"TestTopic\"\n" +
            "}");
    }
```
## 3c3c5ef4b62cc350c50990e5c04b2bf7ef59937b ##
```
Commit	3c3c5ef4b62cc350c50990e5c04b2bf7ef59937b
Directory name		broker
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		13
    
    public void testBrokerStartAclEnabled() throws Exception {
        BrokerConfig brokerConfigAclEnabled = new BrokerConfig();
        brokerConfigAclEnabled.setEnableAcl(true);
        
        BrokerController brokerController = new BrokerController(
            brokerConfigAclEnabled,
            new NettyServerConfig(),
            new NettyClientConfig(),
            new MessageStoreConfig());
        assertThat(brokerController.initialize());
        brokerController.start();
        brokerController.shutdown();
    }
```
