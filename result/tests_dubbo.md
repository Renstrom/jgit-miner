## e7c93e579fd07b2111617e1e4af7821e9970e4a6 ##
```
Commit	e7c93e579fd07b2111617e1e4af7821e9970e4a6
Directory name		dubbo-config/dubbo-config-api
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		6
    
    public void testCallback() {
        assertEquals(this.getClass(), dubboShutdownHook.getCallbacks().iterator().next().getClass());
        dubboShutdownHook.addCallback(this);
        assertEquals(2, dubboShutdownHook.getCallbacks().size());
    }
```
## 28266668d38424bf936e21d16e6f32c8750685b7 ##
```
Commit	28266668d38424bf936e21d16e6f32c8750685b7
Directory name		dubbo-cluster
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		21
    
    public void testListMerger() throws Exception {
        List<Object> list1 = new ArrayList<Object>();
        list1.add(null);
        list1.add("1");
        list1.add("2");
        List<Object> list2 = new ArrayList<Object>();
        list2.add("3");
        list2.add("4");

        List result = MergerFactory.getMerger(List.class).merge(list1, list2);
        Assert.assertEquals(5, result.size());
        Assert.assertEquals(new ArrayList<String>() {
            {
                add(null);
                add("1");
                add("2");
                add("3");
                add("4");
            }
        }, result);
    }
```
```
Previous Commit	28266668d38424bf936e21d16e6f32c8750685b7
Directory name	dubbo-cluster
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		20
    
    public void testSetMerger() throws Exception {
        Set<Object> set1 = new HashSet<Object>();
        set1.add(null);
        set1.add("1");
        set1.add("2");
        Set<Object> set2 = new HashSet<Object>();
        set2.add("2");
        set2.add("3");

        Set result = MergerFactory.getMerger(Set.class).merge(set1, set2);

        Assert.assertEquals(4, result.size());
        Assert.assertEquals(new HashSet<String>() {
            {
                add(null);
                add("1");
                add("2");
                add("3");
            }
        }, result);
    }
```
## e7ce16d9923326605d16677c24a2c480c7ee30c4 ##
```
Commit	e7ce16d9923326605d16677c24a2c480c7ee30c4
Directory name		dubbo-config/dubbo-config-spring
Cyclomatic Complexity	 2
Assertions		 2
Lines of Code		0
//    
//    public void testModuleInfo() {
//
//        ReferenceAnnotationBeanPostProcessor beanPostProcessor = context.getBean(BEAN_NAME,
//                ReferenceAnnotationBeanPostProcessor.class);
//
//
//        Map<InjectionMetadata.InjectedElement, ReferenceBean<?>> referenceBeanMap =
//                beanPostProcessor.getInjectedMethodReferenceBeanMap();
//
//        for (Map.Entry<InjectionMetadata.InjectedElement, ReferenceBean<?>> entry : referenceBeanMap.entrySet()) {
//            ReferenceBean<?> referenceBean = entry.getValue();
//
//            assertThat(referenceBean.getModule().getName(), is("defaultModule"));
//            assertThat(referenceBean.getMonitor(), not(nullValue()));
//        }
//    }
```
## 897b80de3840c0bc9d9928dc1fb3f28ca8d78cc7 ##
```
Commit	897b80de3840c0bc9d9928dc1fb3f28ca8d78cc7
Directory name		dubbo-common
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		4
    (expected = IllegalArgumentException.class)
    public void testDeserialize_Primitive0() throws Exception {
        JavaBeanDescriptor descriptor = new JavaBeanDescriptor(long.class.getName(), JavaBeanDescriptor.TYPE_BEAN + 1);
    }
```
```
Previous Commit	897b80de3840c0bc9d9928dc1fb3f28ca8d78cc7
Directory name	dubbo-common
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		4
    (expected = IllegalArgumentException.class)
    public void testDeserialize_Null() throws Exception {
        JavaBeanDescriptor descriptor = new JavaBeanDescriptor(null, JavaBeanDescriptor.TYPE_BEAN);
    }
```
```
Previous Commit	897b80de3840c0bc9d9928dc1fb3f28ca8d78cc7
Directory name	dubbo-common
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		5
    (expected = IllegalArgumentException.class)
    public void testDeserialize_containsProperty() throws Exception {
        JavaBeanDescriptor descriptor = new JavaBeanDescriptor(long.class.getName(), JavaBeanDescriptor.TYPE_PRIMITIVE);
        descriptor.containsProperty(null);
    }
```
```
Previous Commit	897b80de3840c0bc9d9928dc1fb3f28ca8d78cc7
Directory name	dubbo-common
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		5
    (expected = IllegalStateException.class)
    public void testSetEnumNameProperty() throws Exception {
        JavaBeanDescriptor descriptor = new JavaBeanDescriptor(long.class.getName(), JavaBeanDescriptor.TYPE_PRIMITIVE);
        descriptor.setEnumNameProperty(JavaBeanDescriptor.class.getName());
    }
```
```
Previous Commit	897b80de3840c0bc9d9928dc1fb3f28ca8d78cc7
Directory name	dubbo-common
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		5
    (expected = IllegalStateException.class)
    public void testGetEnumNameProperty() throws Exception {
        JavaBeanDescriptor descriptor = new JavaBeanDescriptor(long.class.getName(), JavaBeanDescriptor.TYPE_PRIMITIVE);
        descriptor.getEnumPropertyName();
    }
```
```
Previous Commit	897b80de3840c0bc9d9928dc1fb3f28ca8d78cc7
Directory name	dubbo-common
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		5
    (expected = IllegalStateException.class)
    public void testSetClassNameProperty() throws Exception {
        JavaBeanDescriptor descriptor = new JavaBeanDescriptor(long.class.getName(), JavaBeanDescriptor.TYPE_PRIMITIVE);
        descriptor.setClassNameProperty(JavaBeanDescriptor.class.getName());
    }
```
```
Previous Commit	897b80de3840c0bc9d9928dc1fb3f28ca8d78cc7
Directory name	dubbo-common
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		5
    (expected = IllegalStateException.class)
    public void testGetClassNameProperty() throws Exception {
        JavaBeanDescriptor descriptor = new JavaBeanDescriptor(long.class.getName(), JavaBeanDescriptor.TYPE_PRIMITIVE);
        descriptor.getClassNameProperty();
    }
```
```
Previous Commit	897b80de3840c0bc9d9928dc1fb3f28ca8d78cc7
Directory name	dubbo-common
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		5
    (expected = IllegalStateException.class)
    public void testSetPrimitiveProperty() throws Exception {
        JavaBeanDescriptor descriptor = new JavaBeanDescriptor(JavaBeanDescriptor.class.getName(), JavaBeanDescriptor.TYPE_BEAN);
        descriptor.setPrimitiveProperty(JavaBeanDescriptor.class.getName());
    }
```
```
Previous Commit	897b80de3840c0bc9d9928dc1fb3f28ca8d78cc7
Directory name	dubbo-config/dubbo-config-api
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		5
    (expected = IllegalStateException.class)
    public void checkInterfaceAndMethods1() throws Exception {
        InterfaceConfig interfaceConfig = new InterfaceConfig();
        interfaceConfig.checkInterfaceAndMethods(null, null);
    }
```
```
Previous Commit	897b80de3840c0bc9d9928dc1fb3f28ca8d78cc7
Directory name	dubbo-config/dubbo-config-api
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		5
    (expected = IllegalStateException.class)
    public void checkInterfaceAndMethods2() throws Exception {
        InterfaceConfig interfaceConfig = new InterfaceConfig();
        interfaceConfig.checkInterfaceAndMethods(AbstractInterfaceConfigTest.class, null);
    }
```
```
Previous Commit	897b80de3840c0bc9d9928dc1fb3f28ca8d78cc7
Directory name	dubbo-config/dubbo-config-api
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		6
    (expected = IllegalStateException.class)
    public void checkInterfaceAndMethod3() throws Exception {
        MethodConfig methodConfig = new MethodConfig();
        InterfaceConfig interfaceConfig = new InterfaceConfig();
        interfaceConfig.checkInterfaceAndMethods(Greeting.class, Collections.singletonList(methodConfig));
    }
```
```
Previous Commit	897b80de3840c0bc9d9928dc1fb3f28ca8d78cc7
Directory name	dubbo-config/dubbo-config-api
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		7
    (expected = IllegalStateException.class)
    public void checkStubAndMock1() throws Exception {
        InterfaceConfig interfaceConfig = new InterfaceConfig();
        interfaceConfig.setLocal(GreetingLocal1.class.getName());
        interfaceConfig.checkStubAndLocal(Greeting.class);
        interfaceConfig.checkMock(Greeting.class);
    }
```
```
Previous Commit	897b80de3840c0bc9d9928dc1fb3f28ca8d78cc7
Directory name	dubbo-config/dubbo-config-api
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		7
    (expected = IllegalStateException.class)
    public void checkStubAndMock4() throws Exception {
        InterfaceConfig interfaceConfig = new InterfaceConfig();
        interfaceConfig.setStub(GreetingLocal1.class.getName());
        interfaceConfig.checkStubAndLocal(Greeting.class);
        interfaceConfig.checkMock(Greeting.class);
    }
```
```
Previous Commit	897b80de3840c0bc9d9928dc1fb3f28ca8d78cc7
Directory name	dubbo-config/dubbo-config-api
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		7
    (expected = IllegalStateException.class)
    public void checkStubAndMock7() throws Exception {
        InterfaceConfig interfaceConfig = new InterfaceConfig();
        interfaceConfig.setMock("return {a, b}");
        interfaceConfig.checkStubAndLocal(Greeting.class);
        interfaceConfig.checkMock(Greeting.class);
    }
```
```
Previous Commit	897b80de3840c0bc9d9928dc1fb3f28ca8d78cc7
Directory name	dubbo-config/dubbo-config-api
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		7
    (expected = IllegalStateException.class)
    public void checkStubAndMock8() throws Exception {
        InterfaceConfig interfaceConfig = new InterfaceConfig();
        interfaceConfig.setMock(GreetingMock1.class.getName());
        interfaceConfig.checkStubAndLocal(Greeting.class);
        interfaceConfig.checkMock(Greeting.class);
    }
```
```
Previous Commit	897b80de3840c0bc9d9928dc1fb3f28ca8d78cc7
Directory name	dubbo-config/dubbo-config-spring
Cyclomatic Complexity	 2
Assertions		 2
Lines of Code		13
    
    public void testModuleInfo() {
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(TestBean.class);

        ReferenceAnnotationBeanPostProcessor beanPostProcessor = context.getBean(BEAN_NAME,
                ReferenceAnnotationBeanPostProcessor.class);


        Map<InjectionMetadata.InjectedElement, ReferenceBean<?>> referenceBeanMap =
                beanPostProcessor.getInjectedMethodReferenceBeanMap();

        for (Map.Entry<InjectionMetadata.InjectedElement, ReferenceBean<?>> entry : referenceBeanMap.entrySet()) {
            ReferenceBean<?> referenceBean = entry.getValue();

            assertThat(referenceBean.getModule().getName(),is("defaultModule"));
            assertThat(referenceBean.getMonitor(), not(nullValue()));
        }
    }
```
```
Previous Commit	897b80de3840c0bc9d9928dc1fb3f28ca8d78cc7
Directory name	dubbo-config/dubbo-config-spring
Cyclomatic Complexity	 1
Assertions		 7
Lines of Code		14
    
    public void testReferenceCache() throws Exception {

        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(TestBean.class);

        TestBean testBean = context.getBean(TestBean.class);

        Assert.assertNotNull(testBean.getDemoServiceFromAncestor());
        Assert.assertNotNull(testBean.getDemoServiceFromParent());
        Assert.assertNotNull(testBean.getDemoService());

        Assert.assertEquals(testBean.getDemoServiceFromAncestor(), testBean.getDemoServiceFromParent());
        Assert.assertEquals(testBean.getDemoService(), testBean.getDemoServiceFromParent());

        DemoService demoService = testBean.getDemoService();

        Assert.assertEquals(demoService, testBean.getDemoServiceShouldBeSame());
        Assert.assertNotEquals(demoService, testBean.getDemoServiceShouldNotBeSame());

        context.close();

    }
```
```
Previous Commit	897b80de3840c0bc9d9928dc1fb3f28ca8d78cc7
Directory name	dubbo-config/dubbo-config-spring
Cyclomatic Complexity	 1
Assertions		 6
Lines of Code		12
    
    public void testReferenceCacheWithArray() throws Exception {

        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(TestBean.class);

        TestBean testBean = context.getBean(TestBean.class);

        Assert.assertNotNull(testBean.getDemoServiceFromAncestor());
        Assert.assertNotNull(testBean.getDemoServiceFromParent());
        Assert.assertNotNull(testBean.getDemoService());

        Assert.assertEquals(testBean.getDemoServiceFromAncestor(), testBean.getDemoServiceFromParent());
        Assert.assertEquals(testBean.getDemoService(), testBean.getDemoServiceFromParent());

        Assert.assertEquals(testBean.getDemoServiceWithArray(), testBean.getDemoServiceWithArrayShouldBeSame());

        context.close();

    }
```
```
Previous Commit	897b80de3840c0bc9d9928dc1fb3f28ca8d78cc7
Directory name	dubbo-remoting/dubbo-remoting-api
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		12
    (expected = RemotingException.class)
    public void requestTest03() throws RemotingException{
        channel = new MockChannel() {
            @Override
            public void send(Object req) throws RemotingException {
                throw new RemotingException(channel.getLocalAddress(), channel.getRemoteAddress(), "throw error");
            }
        };
        header = new HeaderExchangeChannel(channel);
        Object requestob = new Object();
        header.request(requestob, 1000);
     }
```
```
Previous Commit	897b80de3840c0bc9d9928dc1fb3f28ca8d78cc7
Directory name	dubbo-serialization/dubbo-serialization-test
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		3
    
    public void test_LoopReference() throws Exception {
        // FIXME: cannot make this test pass on protostuff
    }
```
