## 3b05dc685b4bf7bafb8057b15c837fd5333789c5 ##
```
Cyclomatic Complexity	1
Assertions		0
Lines of Code		4
-    
    public void testAssignWithTopicUnavailable() {
        unavailableTopicTest(true, false, Collections.<String>emptySet());
    }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		9
-  
  def testDeleteCmdWithShortInitialization() {
    // run one consumer in the group
    addConsumerGroupExecutor(numConsumers = 1)
    val cgcArgs = Array("--bootstrap-server", brokerList, "--delete", "--group", group)
    val service = getConsumerGroupService(cgcArgs)

    val output = TestUtils.grabConsoleOutput(service.deleteGroups())
    assertTrue(s"The consumer group deletion did not work as expected",
      output.contains(s"Group '$group' could not be deleted due to"))
  }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		9
-  
  def testDeleteWithShortInitialization() {
    // run one consumer in the group
    addConsumerGroupExecutor(numConsumers = 1)
    val cgcArgs = Array("--bootstrap-server", brokerList, "--delete", "--group", group)
    val service = getConsumerGroupService(cgcArgs)

    val result = service.deleteGroups()
    assertTrue(s"The consumer group deletion did not work as expected",
      result.size == 1 && result.keySet.contains(group))
  }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		13
-  
  def testMaxOffset() {
    val baseOffset = 50
    val seg = createSegment(baseOffset)
    val ms = records(baseOffset, "hello", "there", "beautiful")
    seg.append(52, RecordBatch.NO_TIMESTAMP, -1L, ms)
    def validate(offset: Long) =
      assertEquals(ms.records.asScala.filter(_.offset == offset).toList,
                   seg.read(startOffset = offset, maxSize = 1024, maxOffset = Some(offset+1)).records.records.asScala.toList)
    validate(50)
    validate(51)
    validate(52)@@@   }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		6
-    (expected = KafkaException.class)
    public void testInvalidSocketSendBufferSize() {
        props.put(CommonClientConfigs.SEND_BUFFER_CONFIG, -2);
        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.close();
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		6
-    (expected = KafkaException.class)
    public void testInvalidSocketReceiveBufferSize() {
        props.put(CommonClientConfigs.RECEIVE_BUFFER_CONFIG, -2);
        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.close();
    }
```
```
Cyclomatic Complexity	 1
Assertions		 3
Lines of Code		14
-    
    public void testStateChanges() throws InterruptedException {
        final StateListenerStub stateListener = new StateListenerStub();
        globalStreams.setStateListener(stateListener);

        Assert.assertEquals(globalStreams.state(), KafkaStreams.State.CREATED);
        Assert.assertEquals(stateListener.numChanges, 0);

        globalStreams.start();
        TestUtils.waitForCondition(
            () -> globalStreams.state() == KafkaStreams.State.RUNNING,
            10 * 1000,
            "Streams never started.");

        globalStreams.close();

        Assert.assertEquals(globalStreams.state(), KafkaStreams.State.NOT_RUNNING);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		16
-    
    public void testLocalThreadCloseWithoutConnectingToBroker() {
        final Properties props = new Properties();
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "appId");
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:1");
        props.setProperty(StreamsConfig.METRIC_REPORTER_CLASSES_CONFIG, MockMetricsReporter.class.getName());
        props.setProperty(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, NUM_THREADS);

        // make sure we have the global state thread running too
        builder.table("anyTopic");
        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        try {
            streams.start();
        } finally {
            streams.close();
        }
        // There's nothing to assert... We're testing that this operation actually completes.
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		25
-    
    public void testValueGetter() {
        final StreamsBuilder builder = new StreamsBuilder();

        final String topic1 = "topic1";
        final String topic2 = "topic2";

        final KTableImpl<String, String, String> table1 =
                (KTableImpl<String, String, String>) builder.table(topic1, consumed);
        final KTableImpl<String, String, Integer> table2 = (KTableImpl<String, String, Integer>) table1.mapValues(
                new ValueMapper<String, Integer>() {
                    @Override
                    public Integer apply(final String value) {
                        return new Integer(value);
                    }
                });
        final KTableImpl<String, Integer, Integer> table3 = (KTableImpl<String, Integer, Integer>) table2.filter(
                new Predicate<String, Integer>() {
                    @Override
                    public boolean test(final String key, final Integer value) {
                        return (value % 2) == 0;
                    }
                });
        table1.toStream().to(topic2, produced);
        final KTableImpl<String, String, String> table4 = (KTableImpl<String, String, String>) builder.table(topic2, consumed);

        doTestValueGetter(builder, topic1, table1, table2, table3, table4);@@@     }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		6
-    (expected = ProcessorStateException.class)
    public void shouldThrowProcessorStateExceptionOnInitializeOffsetsWhenAuthorizationException() {
        final Consumer consumer = mockConsumer(new AuthorizationException("blah"));
        final AbstractTask task = createTask(consumer, Collections.<StateStore, String>emptyMap());
        task.updateOffsetLimits();
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		6
-    (expected = ProcessorStateException.class)
    public void shouldThrowProcessorStateExceptionOnInitializeOffsetsWhenKafkaException() {
        final Consumer consumer = mockConsumer(new KafkaException("blah"));
        final AbstractTask task = createTask(consumer, Collections.<StateStore, String>emptyMap());
        task.updateOffsetLimits();
    }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		10
-    
    public void shouldResetChangeLogReaderOnCreateTasks() {
        mockSingleActiveTask();
        changeLogReader.reset();
        EasyMock.expectLastCall();
        replay();

        taskManager.setAssignmentMetadata(taskId0Assignment, Collections.<TaskId, Set<TopicPartition>>emptyMap());
        taskManager.createTasks(taskId0Partitions);
        verify(changeLogReader);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		13
-    
    public void shouldPutAndFetch() {
        final String key = "a";
        bytesStore.put(serializeKey(new Windowed<>(key, windows[0])), serializeValue(10));
        bytesStore.put(serializeKey(new Windowed<>(key, windows[1])), serializeValue(50));
        bytesStore.put(serializeKey(new Windowed<>(key, windows[2])), serializeValue(100));

        final KeyValueIterator<Bytes, byte[]> values = bytesStore.fetch(Bytes.wrap(key.getBytes()), 0, 500);

        final List<KeyValue<Windowed<String>, Long>> expected = Arrays.asList(
            KeyValue.pair(new Windowed<>(key, windows[0]), 10L),
            KeyValue.pair(new Windowed<>(key, windows[1]), 50L)
        );

        assertEquals(expected, toList(values));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		13
-    
    public void shouldFindValuesWithinRange() {
        final String key = "a";
        bytesStore.put(serializeKey(new Windowed<>(key, windows[0])), serializeValue(10));
        bytesStore.put(serializeKey(new Windowed<>(key, windows[1])), serializeValue(50));
        bytesStore.put(serializeKey(new Windowed<>(key, windows[2])), serializeValue(100));
        final KeyValueIterator<Bytes, byte[]> results = bytesStore.fetch(Bytes.wrap(key.getBytes()), 1, 999);
        final List<KeyValue<Windowed<String>, Long>> expected = Arrays.asList(
            KeyValue.pair(new Windowed<>(key, windows[0]), 10L),
            KeyValue.pair(new Windowed<>(key, windows[1]), 50L)
        );

        assertEquals(expected, toList(results));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		8
-    
    public void shouldRemove() {
        bytesStore.put(serializeKey(new Windowed<>("a", windows[0])), serializeValue(30));
        bytesStore.put(serializeKey(new Windowed<>("a", windows[1])), serializeValue(50));

        bytesStore.remove(serializeKey(new Windowed<>("a", windows[0])));
        final KeyValueIterator<Bytes, byte[]> value = bytesStore.fetch(Bytes.wrap("a".getBytes()), 0, 100);
        assertFalse(value.hasNext());
    }
```
```
Cyclomatic Complexity	 1
Assertions		 3
Lines of Code		20
-    
    public void shouldRollSegments() {
        // just to validate directories
        final Segments segments = new Segments(storeName, retention, segmentInterval);
        final String key = "a";

        bytesStore.put(serializeKey(new Windowed<>(key, windows[0])), serializeValue(50));
        bytesStore.put(serializeKey(new Windowed<>(key, windows[1])), serializeValue(100));
        bytesStore.put(serializeKey(new Windowed<>(key, windows[2])), serializeValue(500));
        assertEquals(Collections.singleton(segments.segmentName(0)), segmentDirs());

        bytesStore.put(serializeKey(new Windowed<>(key, windows[3])), serializeValue(1000));
        assertEquals(Utils.mkSet(segments.segmentName(0), segments.segmentName(1)), segmentDirs());

        final List<KeyValue<Windowed<String>, Long>> results = toList(bytesStore.fetch(Bytes.wrap(key.getBytes()), 0, 1500));

        assertEquals(
            Arrays.asList(
                KeyValue.pair(new Windowed<>(key, windows[0]), 50L),
                KeyValue.pair(new Windowed<>(key, windows[1]), 100L),
                KeyValue.pair(new Windowed<>(key, windows[2]), 500L)
            ),
            results
        );

    }
```
```
Cyclomatic Complexity	 1
Assertions		 3
Lines of Code		23
-    
    public void shouldGetAllSegments() {
        // just to validate directories
        final Segments segments = new Segments(storeName, retention, segmentInterval);
        final String key = "a";

        bytesStore.put(serializeKey(new Windowed<>(key, windows[0])), serializeValue(50L));
        assertEquals(Collections.singleton(segments.segmentName(0)), segmentDirs());

        bytesStore.put(serializeKey(new Windowed<>(key, windows[3])), serializeValue(100L));
        assertEquals(
            Utils.mkSet(
                segments.segmentName(0),
                segments.segmentName(1)
            ),
            segmentDirs()
        );

        final List<KeyValue<Windowed<String>, Long>> results = toList(bytesStore.all());
        assertEquals(
            Arrays.asList(
                KeyValue.pair(new Windowed<>(key, windows[0]), 50L),
                KeyValue.pair(new Windowed<>(key, windows[3]), 100L)
            ),
            results
        );

    }
```
```
Cyclomatic Complexity	 1
Assertions		 3
Lines of Code		23
-    
    public void shouldFetchAllSegments() {
        // just to validate directories
        final Segments segments = new Segments(storeName, retention, segmentInterval);
        final String key = "a";

        bytesStore.put(serializeKey(new Windowed<>(key, windows[0])), serializeValue(50L));
        assertEquals(Collections.singleton(segments.segmentName(0)), segmentDirs());

        bytesStore.put(serializeKey(new Windowed<>(key, windows[3])), serializeValue(100L));
        assertEquals(
            Utils.mkSet(
                segments.segmentName(0),
                segments.segmentName(1)
            ),
            segmentDirs()
        );

        final List<KeyValue<Windowed<String>, Long>> results = toList(bytesStore.fetchAll(0L, 60_000L));
        assertEquals(
            Arrays.asList(
                KeyValue.pair(new Windowed<>(key, windows[0]), 50L),
                KeyValue.pair(new Windowed<>(key, windows[3]), 100L)
            ),
            results
        );

    }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		35
-    
    public void shouldLoadSegmentsWithOldStyleDateFormattedName() {
        final Segments segments = new Segments(storeName, retention, segmentInterval);
        final String key = "a";

        bytesStore.put(serializeKey(new Windowed<>(key, windows[0])), serializeValue(50L));
        bytesStore.put(serializeKey(new Windowed<>(key, windows[3])), serializeValue(100L));
        bytesStore.close();

        final String firstSegmentName = segments.segmentName(0);
        final String[] nameParts = firstSegmentName.split("\\.");
        final Long segmentId = Long.parseLong(nameParts[1]);
        final SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmm");
        formatter.setTimeZone(new SimpleTimeZone(0, "UTC"));
        final String formatted = formatter.format(new Date(segmentId * segmentInterval));
        final File parent = new File(stateDir, storeName);
        final File oldStyleName = new File(parent, nameParts[0] + "-" + formatted);
        assertTrue(new File(parent, firstSegmentName).renameTo(oldStyleName));

        bytesStore = new RocksDBSegmentedBytesStore(
            storeName,
            "metrics-scope",
            retention,
            segmentInterval,
            schema
        );

        bytesStore.init(context, bytesStore);
        final List<KeyValue<Windowed<String>, Long>> results = toList(bytesStore.fetch(Bytes.wrap(key.getBytes()), 0L, 60_000L));
        assertThat(
            results,
            equalTo(
                Arrays.asList(
                    KeyValue.pair(new Windowed<>(key, windows[0]), 50L),
                    KeyValue.pair(new Windowed<>(key, windows[3]), 100L)
                )
            )
        );
    }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		31
-    
    public void shouldLoadSegmentsWithOldStyleColonFormattedName() {
        final Segments segments = new Segments(storeName, retention, segmentInterval);
        final String key = "a";

        bytesStore.put(serializeKey(new Windowed<>(key, windows[0])), serializeValue(50L));
        bytesStore.put(serializeKey(new Windowed<>(key, windows[3])), serializeValue(100L));
        bytesStore.close();

        final String firstSegmentName = segments.segmentName(0);
        final String[] nameParts = firstSegmentName.split("\\.");
        final File parent = new File(stateDir, storeName);
        final File oldStyleName = new File(parent, nameParts[0] + ":" + Long.parseLong(nameParts[1]));
        assertTrue(new File(parent, firstSegmentName).renameTo(oldStyleName));

        bytesStore = new RocksDBSegmentedBytesStore(
            storeName,
            "metrics-scope",
            retention,
            segmentInterval,
            schema
        );

        bytesStore.init(context, bytesStore);
        final List<KeyValue<Windowed<String>, Long>> results = toList(bytesStore.fetch(Bytes.wrap(key.getBytes()), 0L, 60_000L));
        assertThat(
            results,
            equalTo(
                Arrays.asList(
                    KeyValue.pair(new Windowed<>(key, windows[0]), 50L),
                    KeyValue.pair(new Windowed<>(key, windows[3]), 100L)
                )
            )
        );
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		8
-    
    public void shouldBeAbleToWriteToReInitializedStore() {
        final String key = "a";
        // need to create a segment so we can attempt to write to it again.
        bytesStore.put(serializeKey(new Windowed<>(key, windows[0])), serializeValue(50));
        bytesStore.close();
        bytesStore.init(context, bytesStore);
        bytesStore.put(serializeKey(new Windowed<>(key, windows[1])), serializeValue(100));
    }
```
```
Cyclomatic Complexity	 2
Assertions		 2
Lines of Code		12
-    
    public void shouldCreateWriteBatches() {
        final String key = "a";
        final Collection<KeyValue<byte[], byte[]>> records = new ArrayList<>();
        records.add(new KeyValue<>(serializeKey(new Windowed<>(key, windows[0])).get(), serializeValue(50L)));
        records.add(new KeyValue<>(serializeKey(new Windowed<>(key, windows[3])).get(), serializeValue(100L)));
        final Map<Segment, WriteBatch> writeBatchMap = bytesStore.getWriteBatches(records);
        assertEquals(2, writeBatchMap.size());
        for (final WriteBatch batch : writeBatchMap.values()) {
            assertEquals(1, batch.count());
        }
    }
```
```
Cyclomatic Complexity	 2
Assertions		 4
Lines of Code		18
-    
    public void shouldRestoreToByteStore() {
        // 0 segments initially.
        assertEquals(0, bytesStore.getSegments().size());
        final String key = "a";
        final Collection<KeyValue<byte[], byte[]>> records = new ArrayList<>();
        records.add(new KeyValue<>(serializeKey(new Windowed<>(key, windows[0])).get(), serializeValue(50L)));
        records.add(new KeyValue<>(serializeKey(new Windowed<>(key, windows[3])).get(), serializeValue(100L)));
        bytesStore.restoreAllInternal(records);

        // 2 segments are created during restoration.
        assertEquals(2, bytesStore.getSegments().size());

        // Bulk loading is enabled during recovery.
        for (final Segment segment : bytesStore.getSegments()) {
            Assert.assertThat(segment.getOptions().level0FileNumCompactionTrigger(), equalTo(1 << 30));
        }

        final List<KeyValue<Windowed<String>, Long>> expected = new ArrayList<>();
        expected.add(new KeyValue<>(new Windowed<>(key, windows[0]), 50L));
        expected.add(new KeyValue<>(new Windowed<>(key, windows[3]), 100L));

        final List<KeyValue<Windowed<String>, Long>> results = toList(bytesStore.all());
        assertEquals(expected, results);
    }
```
```
Cyclomatic Complexity	 3
Assertions		 3
Lines of Code		17
-    
    public void shouldRespectBulkLoadOptionsDuringInit() {
        bytesStore.init(context, bytesStore);
        final String key = "a";
        bytesStore.put(serializeKey(new Windowed<>(key, windows[0])), serializeValue(50L));
        bytesStore.put(serializeKey(new Windowed<>(key, windows[3])), serializeValue(100L));
        assertEquals(2, bytesStore.getSegments().size());

        final StateRestoreListener restoreListener = context.getRestoreListener(bytesStore.name());

        restoreListener.onRestoreStart(null, bytesStore.name(), 0L, 0L);

        for (final Segment segment : bytesStore.getSegments()) {
            Assert.assertThat(segment.getOptions().level0FileNumCompactionTrigger(), equalTo(1 << 30));
        }

        restoreListener.onRestoreEnd(null, bytesStore.name(), 0L);
        for (final Segment segment : bytesStore.getSegments()) {
            Assert.assertThat(segment.getOptions().level0FileNumCompactionTrigger(), equalTo(4));
        }
    }
```
```
Cyclomatic Complexity	 1
Assertions		 3
Lines of Code		35
-    
    public void shouldLogAndMeasureExpiredRecords() {
        LogCaptureAppender.setClassLoggerToDebug(RocksDBSegmentedBytesStore.class);
        final LogCaptureAppender appender = LogCaptureAppender.createAndRegister();

        // write a record to advance stream time, with a high enough timestamp
        // that the subsequent record in windows[0] will already be expired.
        bytesStore.put(serializeKey(new Windowed<>("dummy", nextSegmentWindow)), serializeValue(0));

        final Bytes key = serializeKey(new Windowed<>("a", windows[0]));
        final byte[] value = serializeValue(5);
        bytesStore.put(key, value);

        LogCaptureAppender.unregister(appender);

        final Map<MetricName, ? extends Metric> metrics = context.metrics().metrics();

        final Metric dropTotal = metrics.get(new MetricName(
            "expired-window-record-drop-total",
            "stream-metrics-scope-metrics",
            "The total number of occurrence of expired-window-record-drop operations.",
            mkMap(
                mkEntry("client-id", "mock"),
                mkEntry("task-id", "0_0"),
                mkEntry("metrics-scope-id", "bytes-store")
            )
        ));

        final Metric dropRate = metrics.get(new MetricName(
            "expired-window-record-drop-rate",
            "stream-metrics-scope-metrics",
            "The average number of occurrence of expired-window-record-drop operation per second.",
            mkMap(
                mkEntry("client-id", "mock"),
                mkEntry("task-id", "0_0"),
                mkEntry("metrics-scope-id", "bytes-store")
            )
        ));

        assertEquals(1.0, dropTotal.metricValue());
        assertNotEquals(0.0, dropRate.metricValue());
        final List<String> messages = appender.getMessages();
        assertThat(messages, hasItem("Skipping record for expired segment."));
    }
```
```
Cyclomatic Complexity	 2
Assertions		 1
Lines of Code		14
-    
    public void shouldFetchAllSessionsWithSameRecordKey() {

        final List<KeyValue<Windowed<String>, Long>> expected = Arrays.asList(KeyValue.pair(new Windowed<>("a", new SessionWindow(0, 0)), 1L),
                                                                                    KeyValue.pair(new Windowed<>("a", new SessionWindow(10, 10)), 2L),
                                                                                    KeyValue.pair(new Windowed<>("a", new SessionWindow(100, 100)), 3L),
                                                                                    KeyValue.pair(new Windowed<>("a", new SessionWindow(1000, 1000)), 4L));
        for (final KeyValue<Windowed<String>, Long> kv : expected) {
            sessionStore.put(kv.key, kv.value);
        }

        // add one that shouldn't appear in the results
        sessionStore.put(new Windowed<>("aa", new SessionWindow(0, 0)), 5L);

        try (final KeyValueIterator<Windowed<String>, Long> values = sessionStore.fetch("a")) {
            assertEquals(expected, toList(values));
        }
    }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		13
-    
    public void shouldFindValuesWithinMergingSessionWindowRange() {
        final String key = "a";
        sessionStore.put(new Windowed<>(key, new SessionWindow(0L, 0L)), 1L);
        sessionStore.put(new Windowed<>(key, new SessionWindow(1000L, 1000L)), 2L);

        final List<KeyValue<Windowed<String>, Long>> expected = Arrays.asList(
            KeyValue.pair(new Windowed<>(key, new SessionWindow(0L, 0L)), 1L),
            KeyValue.pair(new Windowed<>(key, new SessionWindow(1000L, 1000L)), 2L));

        try (final KeyValueIterator<Windowed<String>, Long> results =
                 sessionStore.findSessions(key, -1, 1000L)) {
            assertEquals(expected, toList(results));
        }
    }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		14
-    
    public void shouldRemove() {
        sessionStore.put(new Windowed<>("a", new SessionWindow(0, 1000)), 1L);
        sessionStore.put(new Windowed<>("a", new SessionWindow(1500, 2500)), 2L);

        sessionStore.remove(new Windowed<>("a", new SessionWindow(0, 1000)));

        try (final KeyValueIterator<Windowed<String>, Long> results =
                 sessionStore.findSessions("a", 0L, 1000L)) {
            assertFalse(results.hasNext());
        }

        try (final KeyValueIterator<Windowed<String>, Long> results =
                 sessionStore.findSessions("a", 1500L, 2500L)) {
            assertTrue(results.hasNext());
        }
    }
```
```
Cyclomatic Complexity	 1
Assertions		 3
Lines of Code		20
-    
    public void shouldFindSessionsToMerge() {
        final Windowed<String> session1 = new Windowed<>("a", new SessionWindow(0, 100));
        final Windowed<String> session2 = new Windowed<>("a", new SessionWindow(101, 200));
        final Windowed<String> session3 = new Windowed<>("a", new SessionWindow(201, 300));
        final Windowed<String> session4 = new Windowed<>("a", new SessionWindow(301, 400));
        final Windowed<String> session5 = new Windowed<>("a", new SessionWindow(401, 500));
        sessionStore.put(session1, 1L);
        sessionStore.put(session2, 2L);
        sessionStore.put(session3, 3L);
        sessionStore.put(session4, 4L);
        sessionStore.put(session5, 5L);

        try (final KeyValueIterator<Windowed<String>, Long> results =
                 sessionStore.findSessions("a", 150, 300)
        ) {
            assertEquals(session2, results.next().key);
            assertEquals(session3, results.next().key);
            assertFalse(results.hasNext());
        }
    }
```
```
Cyclomatic Complexity	 1
Assertions		 40
Lines of Code		48
-    
    public void testRangeAndSinglePointFetch() {
        windowStore = createWindowStore(context, false);
        final long startTime = segmentInterval - 4L;

        putFirstBatch(windowStore, startTime, context);

        assertEquals("zero", windowStore.fetch(0, startTime));
        assertEquals("one", windowStore.fetch(1, startTime + 1L));
        assertEquals("two", windowStore.fetch(2, startTime + 2L));
        assertEquals("four", windowStore.fetch(4, startTime + 4L));
        assertEquals("five", windowStore.fetch(5, startTime + 5L));

        assertEquals(Utils.mkList("zero"), toList(windowStore.fetch(0, ofEpochMilli(startTime + 0L - windowSize), ofEpochMilli(startTime + 0L + windowSize))));
        assertEquals(Utils.mkList("one"), toList(windowStore.fetch(1, ofEpochMilli(startTime + 1L - windowSize), ofEpochMilli(startTime + 1L + windowSize))));
        assertEquals(Utils.mkList("two"), toList(windowStore.fetch(2, ofEpochMilli(startTime + 2L - windowSize), ofEpochMilli(startTime + 2L + windowSize))));
        assertEquals(Utils.mkList(), toList(windowStore.fetch(3, ofEpochMilli(startTime + 3L - windowSize), ofEpochMilli(startTime + 3L + windowSize))));
        assertEquals(Utils.mkList("four"), toList(windowStore.fetch(4, ofEpochMilli(startTime + 4L - windowSize), ofEpochMilli(startTime + 4L + windowSize))));
        assertEquals(Utils.mkList("five"), toList(windowStore.fetch(5, ofEpochMilli(startTime + 5L - windowSize), ofEpochMilli(startTime + 5L + windowSize))));

        putSecondBatch(windowStore, startTime, context);

        assertEquals("two+1", windowStore.fetch(2, startTime + 3L));
        assertEquals("two+2", windowStore.fetch(2, startTime + 4L));
        assertEquals("two+3", windowStore.fetch(2, startTime + 5L));
        assertEquals("two+4", windowStore.fetch(2, startTime + 6L));
        assertEquals("two+5", windowStore.fetch(2, startTime + 7L));
        assertEquals("two+6", windowStore.fetch(2, startTime + 8L));

        assertEquals(Utils.mkList(), toList(windowStore.fetch(2, ofEpochMilli(startTime - 2L - windowSize), ofEpochMilli(startTime - 2L + windowSize))));
        assertEquals(Utils.mkList("two"), toList(windowStore.fetch(2, ofEpochMilli(startTime - 1L - windowSize), ofEpochMilli(startTime - 1L + windowSize))));
        assertEquals(Utils.mkList("two", "two+1"), toList(windowStore.fetch(2, ofEpochMilli(startTime - windowSize), ofEpochMilli(startTime + windowSize))));
        assertEquals(Utils.mkList("two", "two+1", "two+2"), toList(windowStore.fetch(2, ofEpochMilli(startTime + 1L - windowSize), ofEpochMilli(startTime + 1L + windowSize))));
        assertEquals(Utils.mkList("two", "two+1", "two+2", "two+3"), toList(windowStore.fetch(2, ofEpochMilli(startTime + 2L - windowSize), ofEpochMilli(startTime + 2L + windowSize))));
        assertEquals(Utils.mkList("two", "two+1", "two+2", "two+3", "two+4"), toList(windowStore.fetch(2, ofEpochMilli(startTime + 3L - windowSize), ofEpochMilli(startTime + 3L + windowSize))));
        assertEquals(Utils.mkList("two", "two+1", "two+2", "two+3", "two+4", "two+5"), toList(windowStore.fetch(2, ofEpochMilli(startTime + 4L - windowSize), ofEpochMilli(startTime + 4L + windowSize))));
        assertEquals(Utils.mkList("two", "two+1", "two+2", "two+3", "two+4", "two+5", "two+6"), toList(windowStore.fetch(2, ofEpochMilli(startTime + 5L - windowSize), ofEpochMilli(startTime + 5L + windowSize))));
        assertEquals(Utils.mkList("two+1", "two+2", "two+3", "two+4", "two+5", "two+6"), toList(windowStore.fetch(2, ofEpochMilli(startTime + 6L - windowSize), ofEpochMilli(startTime + 6L + windowSize))));
        assertEquals(Utils.mkList("two+2", "two+3", "two+4", "two+5", "two+6"), toList(windowStore.fetch(2, ofEpochMilli(startTime + 7L - windowSize), ofEpochMilli(startTime + 7L + windowSize))));
        assertEquals(Utils.mkList("two+3", "two+4", "two+5", "two+6"), toList(windowStore.fetch(2, ofEpochMilli(startTime + 8L - windowSize), ofEpochMilli(startTime + 8L + windowSize))));
        assertEquals(Utils.mkList("two+4", "two+5", "two+6"), toList(windowStore.fetch(2, ofEpochMilli(startTime + 9L - windowSize), ofEpochMilli(startTime + 9L + windowSize))));
        assertEquals(Utils.mkList("two+5", "two+6"), toList(windowStore.fetch(2, ofEpochMilli(startTime + 10L - windowSize), ofEpochMilli(startTime + 10L + windowSize))));
        assertEquals(Utils.mkList("two+6"), toList(windowStore.fetch(2, ofEpochMilli(startTime + 11L - windowSize), ofEpochMilli(startTime + 11L + windowSize))));
        assertEquals(Utils.mkList(), toList(windowStore.fetch(2, ofEpochMilli(startTime + 12L - windowSize), ofEpochMilli(startTime + 12L + windowSize))));

        // Flush the store and verify all current entries were properly flushed ...
        windowStore.flush();

        final Map<Integer, Set<String>> entriesByKey = entriesByKey(changeLog, startTime);

        assertEquals(Utils.mkSet("zero@0"), entriesByKey.get(0));
        assertEquals(Utils.mkSet("one@1"), entriesByKey.get(1));
        assertEquals(Utils.mkSet("two@2", "two+1@3", "two+2@4", "two+3@5", "two+4@6", "two+5@7", "two+6@8"), entriesByKey.get(2));
        assertNull(entriesByKey.get(3));
        assertEquals(Utils.mkSet("four@4"), entriesByKey.get(4));
        assertEquals(Utils.mkSet("five@5"), entriesByKey.get(5));
        assertNull(entriesByKey.get(6));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		15
-    
    public void shouldGetAll() {
        windowStore = createWindowStore(context, false);
        final long startTime = segmentInterval - 4L;

        putFirstBatch(windowStore, startTime, context);

        final KeyValue<Windowed<Integer>, String> zero = windowedPair(0, "zero", startTime + 0);
        final KeyValue<Windowed<Integer>, String> one = windowedPair(1, "one", startTime + 1);
        final KeyValue<Windowed<Integer>, String> two = windowedPair(2, "two", startTime + 2);
        final KeyValue<Windowed<Integer>, String> four = windowedPair(4, "four", startTime + 4);
        final KeyValue<Windowed<Integer>, String> five = windowedPair(5, "five", startTime + 5);

        assertEquals(
            Utils.mkList(zero, one, two, four, five),
            StreamsTestUtils.toList(windowStore.all())
        );
    }
```
```
Cyclomatic Complexity	 1
Assertions		 3
Lines of Code		23
-    
    public void shouldFetchAllInTimeRange() {
        windowStore = createWindowStore(context, false);
        final long startTime = segmentInterval - 4L;

        putFirstBatch(windowStore, startTime, context);

        final KeyValue<Windowed<Integer>, String> zero = windowedPair(0, "zero", startTime + 0);
        final KeyValue<Windowed<Integer>, String> one = windowedPair(1, "one", startTime + 1);
        final KeyValue<Windowed<Integer>, String> two = windowedPair(2, "two", startTime + 2);
        final KeyValue<Windowed<Integer>, String> four = windowedPair(4, "four", startTime + 4);
        final KeyValue<Windowed<Integer>, String> five = windowedPair(5, "five", startTime + 5);

        assertEquals(
            Utils.mkList(one, two, four),
            StreamsTestUtils.toList(windowStore.fetchAll(ofEpochMilli(startTime + 1), ofEpochMilli(startTime + 4)))
        );

        assertEquals(
            Utils.mkList(zero, one, two),
            StreamsTestUtils.toList(windowStore.fetchAll(ofEpochMilli(startTime + 0), ofEpochMilli(startTime + 3)))
        );

        assertEquals(
            Utils.mkList(one, two, four, five),
            StreamsTestUtils.toList(windowStore.fetchAll(ofEpochMilli(startTime + 1), ofEpochMilli(startTime + 5)))
        );
    }
```
```
Cyclomatic Complexity	 1
Assertions		 8
Lines of Code		44
-    
    public void testFetchRange() {
        windowStore = createWindowStore(context, false);
        final long startTime = segmentInterval - 4L;

        putFirstBatch(windowStore, startTime, context);

        final KeyValue<Windowed<Integer>, String> zero = windowedPair(0, "zero", startTime + 0);
        final KeyValue<Windowed<Integer>, String> one = windowedPair(1, "one", startTime + 1);
        final KeyValue<Windowed<Integer>, String> two = windowedPair(2, "two", startTime + 2);
        final KeyValue<Windowed<Integer>, String> four = windowedPair(4, "four", startTime + 4);
        final KeyValue<Windowed<Integer>, String> five = windowedPair(5, "five", startTime + 5);

        assertEquals(
            Utils.mkList(zero, one),
            StreamsTestUtils.toList(windowStore.fetch(0, 1, ofEpochMilli(startTime + 0L - windowSize), ofEpochMilli(startTime + 0L + windowSize)))
        );
        assertEquals(
            Utils.mkList(one),
            StreamsTestUtils.toList(windowStore.fetch(1, 1, ofEpochMilli(startTime + 0L - windowSize), ofEpochMilli(startTime + 0L + windowSize)))
        );
        assertEquals(
            Utils.mkList(one, two),
            StreamsTestUtils.toList(windowStore.fetch(1, 3, ofEpochMilli(startTime + 0L - windowSize), ofEpochMilli(startTime + 0L + windowSize)))
        );
        assertEquals(
            Utils.mkList(zero, one, two),
            StreamsTestUtils.toList(windowStore.fetch(0, 5, ofEpochMilli(startTime + 0L - windowSize), ofEpochMilli(startTime + 0L + windowSize)))
        );
        assertEquals(
            Utils.mkList(zero, one, two,
                four, five),
            StreamsTestUtils.toList(windowStore.fetch(0, 5, ofEpochMilli(startTime + 0L - windowSize), ofEpochMilli(startTime + 0L + windowSize + 5L)))
        );
        assertEquals(
            Utils.mkList(two, four, five),
            StreamsTestUtils.toList(windowStore.fetch(0, 5, ofEpochMilli(startTime + 2L), ofEpochMilli(startTime + 0L + windowSize + 5L)))
        );
        assertEquals(
            Utils.mkList(),
            StreamsTestUtils.toList(windowStore.fetch(4, 5, ofEpochMilli(startTime + 2L), ofEpochMilli(startTime + windowSize)))
        );
        assertEquals(
            Utils.mkList(),
            StreamsTestUtils.toList(windowStore.fetch(0, 3, ofEpochMilli(startTime + 3L), ofEpochMilli(startTime + windowSize + 5)))
        );
    }
```
```
Cyclomatic Complexity	 1
Assertions		 29
Lines of Code		37
-    
    public void testPutAndFetchBefore() {
        windowStore = createWindowStore(context, false);
        final long startTime = segmentInterval - 4L;

        putFirstBatch(windowStore, startTime, context);

        assertEquals(Utils.mkList("zero"), toList(windowStore.fetch(0, ofEpochMilli(startTime + 0L - windowSize), ofEpochMilli(startTime + 0L))));
        assertEquals(Utils.mkList("one"), toList(windowStore.fetch(1, ofEpochMilli(startTime + 1L - windowSize), ofEpochMilli(startTime + 1L))));
        assertEquals(Utils.mkList("two"), toList(windowStore.fetch(2, ofEpochMilli(startTime + 2L - windowSize), ofEpochMilli(startTime + 2L))));
        assertEquals(Utils.mkList(), toList(windowStore.fetch(3, ofEpochMilli(startTime + 3L - windowSize), ofEpochMilli(startTime + 3L))));
        assertEquals(Utils.mkList("four"), toList(windowStore.fetch(4, ofEpochMilli(startTime + 4L - windowSize), ofEpochMilli(startTime + 4L))));
        assertEquals(Utils.mkList("five"), toList(windowStore.fetch(5, ofEpochMilli(startTime + 5L - windowSize), ofEpochMilli(startTime + 5L))));

        putSecondBatch(windowStore, startTime, context);

        assertEquals(Utils.mkList(), toList(windowStore.fetch(2, ofEpochMilli(startTime - 1L - windowSize), ofEpochMilli(startTime - 1L))));
        assertEquals(Utils.mkList(), toList(windowStore.fetch(2, ofEpochMilli(startTime + 0L - windowSize), ofEpochMilli(startTime + 0L))));
        assertEquals(Utils.mkList(), toList(windowStore.fetch(2, ofEpochMilli(startTime + 1L - windowSize), ofEpochMilli(startTime + 1L))));
        assertEquals(Utils.mkList("two"), toList(windowStore.fetch(2, ofEpochMilli(startTime + 2L - windowSize), ofEpochMilli(startTime + 2L))));
        assertEquals(Utils.mkList("two", "two+1"), toList(windowStore.fetch(2, ofEpochMilli(startTime + 3L - windowSize), ofEpochMilli(startTime + 3L))));
        assertEquals(Utils.mkList("two", "two+1", "two+2"), toList(windowStore.fetch(2, ofEpochMilli(startTime + 4L - windowSize), ofEpochMilli(startTime + 4L))));
        assertEquals(Utils.mkList("two", "two+1", "two+2", "two+3"), toList(windowStore.fetch(2, ofEpochMilli(startTime + 5L - windowSize), ofEpochMilli(startTime + 5L))));
        assertEquals(Utils.mkList("two+1", "two+2", "two+3", "two+4"), toList(windowStore.fetch(2, ofEpochMilli(startTime + 6L - windowSize), ofEpochMilli(startTime + 6L))));
        assertEquals(Utils.mkList("two+2", "two+3", "two+4", "two+5"), toList(windowStore.fetch(2, ofEpochMilli(startTime + 7L - windowSize), ofEpochMilli(startTime + 7L))));
        assertEquals(Utils.mkList("two+3", "two+4", "two+5", "two+6"), toList(windowStore.fetch(2, ofEpochMilli(startTime + 8L - windowSize), ofEpochMilli(startTime + 8L))));
        assertEquals(Utils.mkList("two+4", "two+5", "two+6"), toList(windowStore.fetch(2, ofEpochMilli(startTime + 9L - windowSize), ofEpochMilli(startTime + 9L))));
        assertEquals(Utils.mkList("two+5", "two+6"), toList(windowStore.fetch(2, ofEpochMilli(startTime + 10L - windowSize), ofEpochMilli(startTime + 10L))));
        assertEquals(Utils.mkList("two+6"), toList(windowStore.fetch(2, ofEpochMilli(startTime + 11L - windowSize), ofEpochMilli(startTime + 11L))));
        assertEquals(Utils.mkList(), toList(windowStore.fetch(2, ofEpochMilli(startTime + 12L - windowSize), ofEpochMilli(startTime + 12L))));
        assertEquals(Utils.mkList(), toList(windowStore.fetch(2, ofEpochMilli(startTime + 13L - windowSize), ofEpochMilli(startTime + 13L))));

        // Flush the store and verify all current entries were properly flushed ...
        windowStore.flush();

        final Map<Integer, Set<String>> entriesByKey = entriesByKey(changeLog, startTime);

        assertEquals(Utils.mkSet("zero@0"), entriesByKey.get(0));
        assertEquals(Utils.mkSet("one@1"), entriesByKey.get(1));
        assertEquals(Utils.mkSet("two@2", "two+1@3", "two+2@4", "two+3@5", "two+4@6", "two+5@7", "two+6@8"), entriesByKey.get(2));
        assertNull(entriesByKey.get(3));
        assertEquals(Utils.mkSet("four@4"), entriesByKey.get(4));
        assertEquals(Utils.mkSet("five@5"), entriesByKey.get(5));
        assertNull(entriesByKey.get(6));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 29
Lines of Code		37
-    
    public void testPutAndFetchAfter() {
        windowStore = createWindowStore(context, false);
        final long startTime = segmentInterval - 4L;

        putFirstBatch(windowStore, startTime, context);

        assertEquals(Utils.mkList("zero"), toList(windowStore.fetch(0, ofEpochMilli(startTime + 0L), ofEpochMilli(startTime + 0L + windowSize))));
        assertEquals(Utils.mkList("one"), toList(windowStore.fetch(1, ofEpochMilli(startTime + 1L), ofEpochMilli(startTime + 1L + windowSize))));
        assertEquals(Utils.mkList("two"), toList(windowStore.fetch(2, ofEpochMilli(startTime + 2L), ofEpochMilli(startTime + 2L + windowSize))));
        assertEquals(Utils.mkList(), toList(windowStore.fetch(3, ofEpochMilli(startTime + 3L), ofEpochMilli(startTime + 3L + windowSize))));
        assertEquals(Utils.mkList("four"), toList(windowStore.fetch(4, ofEpochMilli(startTime + 4L), ofEpochMilli(startTime + 4L + windowSize))));
        assertEquals(Utils.mkList("five"), toList(windowStore.fetch(5, ofEpochMilli(startTime + 5L), ofEpochMilli(startTime + 5L + windowSize))));

        putSecondBatch(windowStore, startTime, context);

        assertEquals(Utils.mkList(), toList(windowStore.fetch(2, ofEpochMilli(startTime - 2L), ofEpochMilli(startTime - 2L + windowSize))));
        assertEquals(Utils.mkList("two"), toList(windowStore.fetch(2, ofEpochMilli(startTime - 1L), ofEpochMilli(startTime - 1L + windowSize))));
        assertEquals(Utils.mkList("two", "two+1"), toList(windowStore.fetch(2, ofEpochMilli(startTime), ofEpochMilli(startTime + windowSize))));
        assertEquals(Utils.mkList("two", "two+1", "two+2"), toList(windowStore.fetch(2, ofEpochMilli(startTime + 1L), ofEpochMilli(startTime + 1L + windowSize))));
        assertEquals(Utils.mkList("two", "two+1", "two+2", "two+3"), toList(windowStore.fetch(2, ofEpochMilli(startTime + 2L), ofEpochMilli(startTime + 2L + windowSize))));
        assertEquals(Utils.mkList("two+1", "two+2", "two+3", "two+4"), toList(windowStore.fetch(2, ofEpochMilli(startTime + 3L), ofEpochMilli(startTime + 3L + windowSize))));
        assertEquals(Utils.mkList("two+2", "two+3", "two+4", "two+5"), toList(windowStore.fetch(2, ofEpochMilli(startTime + 4L), ofEpochMilli(startTime + 4L + windowSize))));
        assertEquals(Utils.mkList("two+3", "two+4", "two+5", "two+6"), toList(windowStore.fetch(2, ofEpochMilli(startTime + 5L), ofEpochMilli(startTime + 5L + windowSize))));
        assertEquals(Utils.mkList("two+4", "two+5", "two+6"), toList(windowStore.fetch(2, ofEpochMilli(startTime + 6L), ofEpochMilli(startTime + 6L + windowSize))));
        assertEquals(Utils.mkList("two+5", "two+6"), toList(windowStore.fetch(2, ofEpochMilli(startTime + 7L), ofEpochMilli(startTime + 7L + windowSize))));
        assertEquals(Utils.mkList("two+6"), toList(windowStore.fetch(2, ofEpochMilli(startTime + 8L), ofEpochMilli(startTime + 8L + windowSize))));
        assertEquals(Utils.mkList(), toList(windowStore.fetch(2, ofEpochMilli(startTime + 9L), ofEpochMilli(startTime + 9L + windowSize))));
        assertEquals(Utils.mkList(), toList(windowStore.fetch(2, ofEpochMilli(startTime + 10L), ofEpochMilli(startTime + 10L + windowSize))));
        assertEquals(Utils.mkList(), toList(windowStore.fetch(2, ofEpochMilli(startTime + 11L), ofEpochMilli(startTime + 11L + windowSize))));
        assertEquals(Utils.mkList(), toList(windowStore.fetch(2, ofEpochMilli(startTime + 12L), ofEpochMilli(startTime + 12L + windowSize))));

        // Flush the store and verify all current entries were properly flushed ...
        windowStore.flush();

        final Map<Integer, Set<String>> entriesByKey = entriesByKey(changeLog, startTime);

        assertEquals(Utils.mkSet("zero@0"), entriesByKey.get(0));
        assertEquals(Utils.mkSet("one@1"), entriesByKey.get(1));
        assertEquals(Utils.mkSet("two@2", "two+1@3", "two+2@4", "two+3@5", "two+4@6", "two+5@7", "two+6@8"), entriesByKey.get(2));
        assertNull(entriesByKey.get(3));
        assertEquals(Utils.mkSet("four@4"), entriesByKey.get(4));
        assertEquals(Utils.mkSet("five@5"), entriesByKey.get(5));
        assertNull(entriesByKey.get(6));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 8
Lines of Code		19
-    
    public void testPutSameKeyTimestamp() {
        windowStore = createWindowStore(context, true);
        final long startTime = segmentInterval - 4L;

        setCurrentTime(startTime);
        windowStore.put(0, "zero");

        assertEquals(Utils.mkList("zero"), toList(windowStore.fetch(0, ofEpochMilli(startTime - windowSize), ofEpochMilli(startTime + windowSize))));

        windowStore.put(0, "zero");
        windowStore.put(0, "zero+");
        windowStore.put(0, "zero++");

        assertEquals(Utils.mkList("zero", "zero", "zero+", "zero++"), toList(windowStore.fetch(0, ofEpochMilli(startTime - windowSize), ofEpochMilli(startTime + windowSize))));
        assertEquals(Utils.mkList("zero", "zero", "zero+", "zero++"), toList(windowStore.fetch(0, ofEpochMilli(startTime + 1L - windowSize), ofEpochMilli(startTime + 1L + windowSize))));
        assertEquals(Utils.mkList("zero", "zero", "zero+", "zero++"), toList(windowStore.fetch(0, ofEpochMilli(startTime + 2L - windowSize), ofEpochMilli(startTime + 2L + windowSize))));
        assertEquals(Utils.mkList("zero", "zero", "zero+", "zero++"), toList(windowStore.fetch(0, ofEpochMilli(startTime + 3L - windowSize), ofEpochMilli(startTime + 3L + windowSize))));
        assertEquals(Utils.mkList(), toList(windowStore.fetch(0, ofEpochMilli(startTime + 4L - windowSize), ofEpochMilli(startTime + 4L + windowSize))));

        // Flush the store and verify all current entries were properly flushed ...
        windowStore.flush();

        final Map<Integer, Set<String>> entriesByKey = entriesByKey(changeLog, startTime);

        assertEquals(Utils.mkSet("zero@0", "zero@0", "zero+@0", "zero++@0"), entriesByKey.get(0));@@@     }
```
```
Cyclomatic Complexity	 4
Assertions		 9
Lines of Code		63
-    
    public void testSegmentMaintenance() {
        windowStore = createWindowStore(context, true);
        context.setTime(0L);
        setCurrentTime(0);
        windowStore.put(0, "v");
        assertEquals(
            Utils.mkSet(segments.segmentName(0L)),
            segmentDirs(baseDir)
        );

        setCurrentTime(segmentInterval - 1);
        windowStore.put(0, "v");
        windowStore.put(0, "v");
        assertEquals(
            Utils.mkSet(segments.segmentName(0L)),
            segmentDirs(baseDir)
        );

        setCurrentTime(segmentInterval);
        windowStore.put(0, "v");
        assertEquals(
            Utils.mkSet(segments.segmentName(0L), segments.segmentName(1L)),
            segmentDirs(baseDir)
        );

        WindowStoreIterator iter;
        int fetchedCount;

        iter = windowStore.fetch(0, ofEpochMilli(0L), ofEpochMilli(segmentInterval * 4));
        fetchedCount = 0;
        while (iter.hasNext()) {
            iter.next();
            fetchedCount++;
        }
        assertEquals(4, fetchedCount);

        assertEquals(
            Utils.mkSet(segments.segmentName(0L), segments.segmentName(1L)),
            segmentDirs(baseDir)
        );

        setCurrentTime(segmentInterval * 3);
        windowStore.put(0, "v");

        iter = windowStore.fetch(0, ofEpochMilli(0L), ofEpochMilli(segmentInterval * 4));
        fetchedCount = 0;
        while (iter.hasNext()) {
            iter.next();
            fetchedCount++;
        }
        assertEquals(2, fetchedCount);

        assertEquals(
            Utils.mkSet(segments.segmentName(1L), segments.segmentName(3L)),
            segmentDirs(baseDir)
        );

        setCurrentTime(segmentInterval * 5);
        windowStore.put(0, "v");

        iter = windowStore.fetch(0, ofEpochMilli(segmentInterval * 4), ofEpochMilli(segmentInterval * 10));
        fetchedCount = 0;
        while (iter.hasNext()) {
            iter.next();
            fetchedCount++;
        }
        assertEquals(1, fetchedCount);

        assertEquals(
            Utils.mkSet(segments.segmentName(3L), segments.segmentName(5L)),
            segmentDirs(baseDir)
        );

    }
```
```
Cyclomatic Complexity	 2
Assertions		 2
Lines of Code		29
-    
    public void testInitialLoading() {
        final File storeDir = new File(baseDir, windowName);

        windowStore = createWindowStore(context, false);

        new File(storeDir, segments.segmentName(0L)).mkdir();
        new File(storeDir, segments.segmentName(1L)).mkdir();
        new File(storeDir, segments.segmentName(2L)).mkdir();
        new File(storeDir, segments.segmentName(3L)).mkdir();
        new File(storeDir, segments.segmentName(4L)).mkdir();
        new File(storeDir, segments.segmentName(5L)).mkdir();
        new File(storeDir, segments.segmentName(6L)).mkdir();
        windowStore.close();

        windowStore = createWindowStore(context, false);

        // put something in the store to advance its stream time and expire the old segments
        windowStore.put(1, "v", 6L * segmentInterval);

        final List<String> expected = Utils.mkList(segments.segmentName(4L), segments.segmentName(5L), segments.segmentName(6L));
        expected.sort(String::compareTo);

        final List<String> actual = Utils.toList(segmentDirs(baseDir).iterator());
        actual.sort(String::compareTo);

        assertEquals(expected, actual);

        try (final WindowStoreIterator iter = windowStore.fetch(0, ofEpochMilli(0L), ofEpochMilli(1000000L))) {
            while (iter.hasNext()) {
                iter.next();
            }
        }

        assertEquals(
            Utils.mkSet(segments.segmentName(4L), segments.segmentName(5L), segments.segmentName(6L)),
            segmentDirs(baseDir)
        );
    }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		12
-    
    public void shouldCloseOpenIteratorsWhenStoreIsClosedAndNotThrowInvalidStateStoreExceptionOnHasNext() {
        windowStore = createWindowStore(context, false);
        setCurrentTime(0);
        windowStore.put(1, "one", 1L);
        windowStore.put(1, "two", 2L);
        windowStore.put(1, "three", 3L);

        final WindowStoreIterator<String> iterator = windowStore.fetch(1, ofEpochMilli(1L), ofEpochMilli(3L));
        assertTrue(iterator.hasNext());
        windowStore.close();

        assertFalse(iterator.hasNext());

    }
```
```
Cyclomatic Complexity	 1
Assertions		 3
Lines of Code		29
-    
    public void shouldFetchAndIterateOverExactKeys() {
        final long windowSize = 0x7a00000000000000L;
        final long retentionPeriod = 0x7a00000000000000L;

        final WindowStore<String, String> windowStore = Stores.windowStoreBuilder(
            Stores.persistentWindowStore(windowName, ofMillis(retentionPeriod), ofMillis(windowSize), true),
            Serdes.String(),
            Serdes.String()).build();

        windowStore.init(context, windowStore);

        windowStore.put("a", "0001", 0);
        windowStore.put("aa", "0002", 0);
        windowStore.put("a", "0003", 1);
        windowStore.put("aa", "0004", 1);
        windowStore.put("a", "0005", 0x7a00000000000000L - 1);


        final List expected = Utils.mkList("0001", "0003", "0005");
        assertThat(toList(windowStore.fetch("a", ofEpochMilli(0), ofEpochMilli(Long.MAX_VALUE))), equalTo(expected));

        List<KeyValue<Windowed<String>, String>> list =
            StreamsTestUtils.toList(windowStore.fetch("a", "a", ofEpochMilli(0), ofEpochMilli(Long.MAX_VALUE)));
        assertThat(list, equalTo(Utils.mkList(
            windowedPair("a", "0001", 0, windowSize),
            windowedPair("a", "0003", 1, windowSize),
            windowedPair("a", "0005", 0x7a00000000000000L - 1, windowSize)
        )));

        list = StreamsTestUtils.toList(windowStore.fetch("aa", "aa", ofEpochMilli(0), ofEpochMilli(Long.MAX_VALUE)));
        assertThat(list, equalTo(Utils.mkList(
            windowedPair("aa", "0002", 0, windowSize),
            windowedPair("aa", "0004", 1, windowSize)
        )));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		5
-    (expected = NullPointerException.class)
    public void shouldThrowNullPointerExceptionOnPutNullKey() {
        windowStore = createWindowStore(context, false);
        windowStore.put(null, "anyValue");
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		5
-    
    public void shouldNotThrowNullPointerExceptionOnPutNullValue() {
        windowStore = createWindowStore(context, false);
        windowStore.put(1, null);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		5
-    (expected = NullPointerException.class)
    public void shouldThrowNullPointerExceptionOnGetNullKey() {
        windowStore = createWindowStore(context, false);
        windowStore.fetch(null, ofEpochMilli(1L), ofEpochMilli(2L));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		5
-    (expected = NullPointerException.class)
    public void shouldThrowNullPointerExceptionOnRangeNullFromKey() {
        windowStore = createWindowStore(context, false);
        windowStore.fetch(null, 2, ofEpochMilli(1L), ofEpochMilli(2L));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		5
-    (expected = NullPointerException.class)
    public void shouldThrowNullPointerExceptionOnRangeNullToKey() {
        windowStore = createWindowStore(context, false);
        windowStore.fetch(1, null, ofEpochMilli(1L), ofEpochMilli(2L));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		11
-    
    public void shouldNoNullPointerWhenSerdeDoesNotHandleNull() {
        windowStore = new RocksDBWindowStore<>(
            new RocksDBSegmentedBytesStore(windowName, "metrics-scope", retentionPeriod, segmentInterval, new WindowKeySchema()),
            Serdes.Integer(),
            new SerdeThatDoesntHandleNull(),
            false,
            windowSize);
        windowStore.init(context, windowStore);

        assertNull(windowStore.fetch(1, 0));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 3
Lines of Code		26
-    
    public void shouldFetchAndIterateOverExactBinaryKeys() {
        final WindowStore<Bytes, String> windowStore = Stores.windowStoreBuilder(
            Stores.persistentWindowStore(windowName, ofMillis(60_000L), ofMillis(60_000L), true),
            Serdes.Bytes(),
            Serdes.String()).build();

        windowStore.init(context, windowStore);

        final Bytes key1 = Bytes.wrap(new byte[] {0});
        final Bytes key2 = Bytes.wrap(new byte[] {0, 0});
        final Bytes key3 = Bytes.wrap(new byte[] {0, 0, 0});
        windowStore.put(key1, "1", 0);
        windowStore.put(key2, "2", 0);
        windowStore.put(key3, "3", 0);
        windowStore.put(key1, "4", 1);
        windowStore.put(key2, "5", 1);
        windowStore.put(key3, "6", 59999);
        windowStore.put(key1, "7", 59999);
        windowStore.put(key2, "8", 59999);
        windowStore.put(key3, "9", 59999);

        final List expectedKey1 = Utils.mkList("1", "4", "7");
        assertThat(toList(windowStore.fetch(key1, ofEpochMilli(0), ofEpochMilli(Long.MAX_VALUE))), equalTo(expectedKey1));
        final List expectedKey2 = Utils.mkList("2", "5", "8");
        assertThat(toList(windowStore.fetch(key2, ofEpochMilli(0), ofEpochMilli(Long.MAX_VALUE))), equalTo(expectedKey2));
        final List expectedKey3 = Utils.mkList("3", "6", "9");
        assertThat(toList(windowStore.fetch(key3, ofEpochMilli(0), ofEpochMilli(Long.MAX_VALUE))), equalTo(expectedKey3));
    }
```
## 742f9281d9a747c35dfa2c3cfbd0a6b19f326508 ##
```
Cyclomatic Complexity	1
Assertions		0
Lines of Code		4
-    
    public void testAssignWithTopicUnavailable() {
        unavailableTopicTest(true, false, Collections.<String>emptySet());
    }
```
```
Cyclomatic Complexity	 7
Assertions		 4
Lines of Code		62
-    
    public void testMetadataFetchOnStaleMetadata() throws Exception {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        KafkaProducer<String, String> producer = new KafkaProducer<>(props, new StringSerializer(), new StringSerializer());
        Metadata metadata = PowerMock.createNiceMock(Metadata.class);
        MemberModifier.field(KafkaProducer.class, "metadata").set(producer, metadata);

        String topic = "topic";
        ProducerRecord<String, String> initialRecord = new ProducerRecord<>(topic, "value");
        // Create a record with a partition higher than the initial (outdated) partition range
        ProducerRecord<String, String> extendedRecord = new ProducerRecord<>(topic, 2, null, "value");
        Collection<Node> nodes = Collections.singletonList(new Node(0, "host1", 1000));
        final Cluster emptyCluster = new Cluster(null, nodes,
                Collections.<PartitionInfo>emptySet(),
                Collections.<String>emptySet(),
                Collections.<String>emptySet());
        final Cluster initialCluster = new Cluster(
                "dummy",
                Collections.singletonList(new Node(0, "host1", 1000)),
                Arrays.asList(new PartitionInfo(topic, 0, null, null, null)),
                Collections.<String>emptySet(),
                Collections.<String>emptySet());
        final Cluster extendedCluster = new Cluster(
                "dummy",
                Collections.singletonList(new Node(0, "host1", 1000)),
                Arrays.asList(
                        new PartitionInfo(topic, 0, null, null, null),
                        new PartitionInfo(topic, 1, null, null, null),
                        new PartitionInfo(topic, 2, null, null, null)),
                Collections.<String>emptySet(),
                Collections.<String>emptySet());

        // Expect exactly one fetch for each attempt to refresh while topic metadata is not available
        final int refreshAttempts = 5;
        EasyMock.expect(metadata.fetch()).andReturn(emptyCluster).times(refreshAttempts - 1);
        EasyMock.expect(metadata.fetch()).andReturn(initialCluster).once();
        EasyMock.expect(metadata.fetch()).andThrow(new IllegalStateException("Unexpected call to metadata.fetch()")).anyTimes();
        PowerMock.replay(metadata);
        producer.send(initialRecord);
        PowerMock.verify(metadata);

        // Expect exactly one fetch if topic metadata is available and records are still within range
        PowerMock.reset(metadata);
        EasyMock.expect(metadata.fetch()).andReturn(initialCluster).once();
        EasyMock.expect(metadata.fetch()).andThrow(new IllegalStateException("Unexpected call to metadata.fetch()")).anyTimes();
        PowerMock.replay(metadata);
        producer.send(initialRecord, null);
        PowerMock.verify(metadata);

        // Expect exactly two fetches if topic metadata is available but metadata response still returns
        // the same partition size (either because metadata are still stale at the broker too or because
        // there weren't any partitions added in the first place).
        PowerMock.reset(metadata);
        EasyMock.expect(metadata.fetch()).andReturn(initialCluster).once();
        EasyMock.expect(metadata.fetch()).andReturn(initialCluster).once();
        EasyMock.expect(metadata.fetch()).andThrow(new IllegalStateException("Unexpected call to metadata.fetch()")).anyTimes();
        PowerMock.replay(metadata);
        try {
            producer.send(extendedRecord, null);
            fail("Expected KafkaException to be raised");
        } catch (KafkaException e) {
            // expected
        }
        PowerMock.verify(metadata);

        // Expect exactly two fetches if topic metadata is available but outdated for the given record
        PowerMock.reset(metadata);
        EasyMock.expect(metadata.fetch()).andReturn(initialCluster).once();
        EasyMock.expect(metadata.fetch()).andReturn(extendedCluster).once();
        EasyMock.expect(metadata.fetch()).andThrow(new IllegalStateException("Unexpected call to metadata.fetch()")).anyTimes();
        PowerMock.replay(metadata);
        producer.send(extendedRecord, null);
        PowerMock.verify(metadata);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 9
Lines of Code		21
-    
    public void produceResponseVersionTest() {
        Map<TopicPartition, ProduceResponse.PartitionResponse> responseData = new HashMap<>();
        responseData.put(new TopicPartition("test", 0), new ProduceResponse.PartitionResponse(Errors.NONE,
                10000, RecordBatch.NO_TIMESTAMP, 100));
        ProduceResponse v0Response = new ProduceResponse(responseData);
        ProduceResponse v1Response = new ProduceResponse(responseData, 10);
        ProduceResponse v2Response = new ProduceResponse(responseData, 10);
        assertEquals("Throttle time must be zero", 0, v0Response.getThrottleTime());
        assertEquals("Throttle time must be 10", 10, v1Response.getThrottleTime());
        assertEquals("Throttle time must be 10", 10, v2Response.getThrottleTime());
        assertEquals("Should use schema version 0", ApiKeys.PRODUCE.responseSchema((short) 0),
                v0Response.toStruct((short) 0).schema());
        assertEquals("Should use schema version 1", ApiKeys.PRODUCE.responseSchema((short) 1),
                v1Response.toStruct((short) 1).schema());
        assertEquals("Should use schema version 2", ApiKeys.PRODUCE.responseSchema((short) 2),
                v2Response.toStruct((short) 2).schema());
        assertEquals("Response data does not match", responseData, v0Response.responses());
        assertEquals("Response data does not match", responseData, v1Response.responses());
        assertEquals("Response data does not match", responseData, v2Response.responses());
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		10
-  
  def testConsumeWithTopicAndGroupRead() {
    addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Write)), topicResource)
    sendRecords(1, tp)
    removeAllAcls()

    addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Read)), topicResource)
    addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Read)), groupResource)
    this.consumers.head.assign(List(tp).asJava)
    consumeRecords(this.consumers.head)
  }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		10
-  
  def testPatternSubscriptionWithNoTopicAccess() {
    addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Write)), topicResource)
    sendRecords(1, tp)
    removeAllAcls()

    addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Read)), groupResource)
    this.consumers.head.subscribe(Pattern.compile(topicPattern), new NoOpConsumerRebalanceListener)
    this.consumers.head.poll(50)
    assertTrue(this.consumers.head.subscription.isEmpty)
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		16
-  
  def testPatternSubscriptionNotMatchingInternalTopic() {
    addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Write)), topicResource)
    sendRecords(1, tp)
    removeAllAcls()

    addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Read)), topicResource)
    addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Read)), groupResource)

    val consumerConfig = new Properties
    consumerConfig.put(ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG, "false")
    val consumer = TestUtils.createNewConsumer(TestUtils.getBrokerListStrFromServers(servers), groupId = group,
      securityProtocol = SecurityProtocol.PLAINTEXT, props = Some(consumerConfig))
    try {
      consumer.subscribe(Pattern.compile(topicPattern))
      consumeRecords(consumer)
    } finally consumer.close()
}
```
```
Cyclomatic Complexity	 2
Assertions		 1
Lines of Code		21
-  
  def testCreatePermissionNeededToReadFromNonExistentTopic() {
    val newTopic = "newTopic"
    val topicPartition = new TopicPartition(newTopic, 0)
    val newTopicResource = new Resource(Topic, newTopic)
    addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Read)), newTopicResource)
    addAndVerifyAcls(groupReadAcl(groupResource), groupResource)
    addAndVerifyAcls(clusterAcl(Resource.ClusterResource), Resource.ClusterResource)
    try {
      this.consumers.head.assign(List(topicPartition).asJava)
      consumeRecords(this.consumers.head)
      Assert.fail("should have thrown exception")
    } catch {
      case e: TopicAuthorizationException =>
        assertEquals(Collections.singleton(newTopic), e.unauthorizedTopics())@@@     }

    addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Write)), newTopicResource)
    addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Create)), Resource.ClusterResource)

    sendRecords(numRecords, topicPartition)
    consumeRecords(this.consumers.head, topic = newTopic, part = 0)
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		4
-  (expected = classOf[AuthorizationException])
  def testCommitWithNoAccess() {
    this.consumers.head.commitSync(Map(tp -> new OffsetAndMetadata(5)).asJava)
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		5
-  (expected = classOf[KafkaException])
  def testCommitWithNoTopicAccess() {
    addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Read)), groupResource)
    this.consumers.head.commitSync(Map(tp -> new OffsetAndMetadata(5)).asJava)
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		6
-  (expected = classOf[TopicAuthorizationException])
  def testCommitWithTopicWrite() {
    addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Read)), groupResource)
    addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Write)), topicResource)
    this.consumers.head.commitSync(Map(tp -> new OffsetAndMetadata(5)).asJava)
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		6
-  (expected = classOf[TopicAuthorizationException])
  def testCommitWithTopicDescribe() {
    addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Read)), groupResource)
    addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Describe)), topicResource)
    this.consumers.head.commitSync(Map(tp -> new OffsetAndMetadata(5)).asJava)
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		5
-  (expected = classOf[AuthorizationException])
  def testOffsetFetchWithNoAccess() {
    this.consumers.head.assign(List(tp).asJava)
    this.consumers.head.position(tp)
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		6
-  (expected = classOf[GroupAuthorizationException])
  def testOffsetFetchWithNoGroupAccess() {
    addAndVerifyAcls(Set(new Acl(KafkaPrincipal.ANONYMOUS, Allow, Acl.WildCardHost, Read)), topicResource)
    this.consumers.head.assign(List(tp).asJava)
    this.consumers.head.position(tp)
  }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		10
-  
  def testListGroupWithNoExistingGroup() {
    val opts = new ConsumerGroupCommandOptions(Array("--zookeeper", zkConnect))
    val consumerGroupCommand = new ZkConsumerGroupService(opts)
    try {
      assert(consumerGroupCommand.listGroups().isEmpty)
    } finally {
      consumerGroupCommand.close()
    }
  }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		13
-  
  def testMaxOffset() {
    val baseOffset = 50
    val seg = createSegment(baseOffset)
    val ms = records(baseOffset, "hello", "there", "beautiful")
    seg.append(baseOffset, 52, RecordBatch.NO_TIMESTAMP, -1L, ms)
    def validate(offset: Long) =
      assertEquals(ms.records.asScala.filter(_.offset == offset).toList,
                   seg.read(startOffset = offset, maxSize = 1024, maxOffset = Some(offset+1)).records.records.asScala.toList)
    validate(50)
    validate(51)
    validate(52)@@@   }
```
```
Cyclomatic Complexity	 1
Assertions		 12
Lines of Code		27
-  
  def testTruncateHead(): Unit = {
    val epoch = 0.toShort

    append(stateManager, producerId, epoch, 0, 0L)
    append(stateManager, producerId, epoch, 1, 1L)
    stateManager.takeSnapshot()

    val anotherPid = 2L
    append(stateManager, anotherPid, epoch, 0, 2L)
    append(stateManager, anotherPid, epoch, 1, 3L)
    stateManager.takeSnapshot()
    assertEquals(Set(2, 4), currentSnapshotOffsets)

    stateManager.truncateHead(2)
    assertEquals(Set(2, 4), currentSnapshotOffsets)
    assertEquals(Set(anotherPid), stateManager.activeProducers.keySet)
    assertEquals(None, stateManager.lastEntry(producerId))

    val maybeEntry = stateManager.lastEntry(anotherPid)
    assertTrue(maybeEntry.isDefined)
    assertEquals(3L, maybeEntry.get.lastDataOffset)

    stateManager.truncateHead(3)
    assertEquals(Set(anotherPid), stateManager.activeProducers.keySet)
    assertEquals(Set(4), currentSnapshotOffsets)
    assertEquals(4, stateManager.mapEndOffset)

    stateManager.truncateHead(5)
    assertEquals(Set(), stateManager.activeProducers.keySet)
    assertEquals(Set(), currentSnapshotOffsets)
    assertEquals(5, stateManager.mapEndOffset)
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		4
-  
  def testReadUncommittedConsumerListOffsetEarliestOffsetEqualsHighWatermark(): Unit = {
    testConsumerListOffsetEarliestOffsetEqualsLimit(IsolationLevel.READ_UNCOMMITTED)
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		4
-  
  def testReadCommittedConsumerListOffsetEarliestOffsetEqualsLastStableOffset(): Unit = {
    testConsumerListOffsetEarliestOffsetEqualsLimit(IsolationLevel.READ_COMMITTED)
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		4
-    
    public void shouldAllowNullStoreNameOnReduce() {
        groupedStream.reduce(MockReducer.STRING_ADDER, (String) null);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		4
-    (expected = NullPointerException.class)
    public void shouldNotHaveNullStoreSupplierOnReduce() {
        groupedStream.reduce(MockReducer.STRING_ADDER, (StateStoreSupplier<KeyValueStore>) null);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		4
-    (expected = NullPointerException.class)
    public void shouldNotHaveNullStoreSupplierOnCount() {
        groupedStream.count((StateStoreSupplier<KeyValueStore>) null);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		4
-    (expected = NullPointerException.class)
    public void shouldNotHaveNullStoreSupplierOnWindowedCount() {
        groupedStream.count(TimeWindows.of(10), (StateStoreSupplier<WindowStore>) null);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		4
-    
    public void shouldAllowNullStoreNameWithWindowedReduce() {
        groupedStream.reduce(MockReducer.STRING_ADDER, TimeWindows.of(10), (String) null);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		4
-    
    public void shouldAllowNullStoreNameOnAggregate() {
        groupedStream.aggregate(MockInitializer.STRING_INIT, MockAggregator.TOSTRING_ADDER, Serdes.String(), null);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		4
-    
    public void shouldAllowNullStoreNameOnWindowedAggregate() {
        groupedStream.aggregate(MockInitializer.STRING_INIT, MockAggregator.TOSTRING_ADDER, TimeWindows.of(10), Serdes.String(), null);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		4
-    
    public void shouldAllowNullStoreNameOnCount()  {
        groupedTable.count((String) null);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		4
-    
    public void shouldAllowNullStoreNameOnAggregate() {
        groupedTable.aggregate(MockInitializer.STRING_INIT, MockAggregator.TOSTRING_ADDER, MockAggregator.TOSTRING_REMOVER, (String) null);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		4
-    
    public void shouldAllowNullStoreNameOnReduce() {
        groupedTable.reduce(MockReducer.STRING_ADDER, MockReducer.STRING_REMOVER, (String) null);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		4
-    (expected = NullPointerException.class)
    public void shouldNotAllowNullPredicateOnFilterNot() {
        testStream.filterNot(null);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		4
-    (expected = NullPointerException.class)
    public void shouldNotAllowNullMapperOnSelectKey() {
        testStream.selectKey(null);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		4
-    (expected = NullPointerException.class)
    public void shouldNotAllowNullMapperOnMap() {
        testStream.map(null);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		4
-    (expected = NullPointerException.class)
    public void shouldNotAllowNullMapperOnMapValues() {
        testStream.mapValues(null);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		4
-    (expected = NullPointerException.class)
    public void shouldNotAllowNullFilePathOnWriteAsText() {
        testStream.writeAsText(null);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		4
-    (expected = TopologyException.class)
    public void shouldNotAllowEmptyFilePathOnWriteAsText() {
        testStream.writeAsText("\t    \t");
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		4
-    (expected = NullPointerException.class)
    public void shouldNotAllowNullMapperOnFlatMap() {
        testStream.flatMap(null);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		4
-    (expected = NullPointerException.class)
    public void shouldNotAllowNullMapperOnFlatMapValues() {
        testStream.flatMapValues(null);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		4
-    (expected = IllegalArgumentException.class)
    public void shouldHaveAtLeastOnPredicateWhenBranching() {
        testStream.branch();
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		4
-    (expected = NullPointerException.class)
    public void shouldCantHaveNullPredicate() {
        testStream.branch((Predicate) null);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		4
-    (expected = NullPointerException.class)
    public void shouldNotAllowNullTopicOnThrough() {
        testStream.through(null);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		4
-    (expected = NullPointerException.class)
    public void shouldNotAllowNullTopicOnTo() {
        testStream.to(null);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		4
-    (expected = NullPointerException.class)
    public void shouldNotAllowNullTransformSupplierOnTransform() {
        testStream.transform(null);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		4
-    (expected = NullPointerException.class)
    public void shouldNotAllowNullTransformSupplierOnTransformValues() {
        testStream.transformValues(null);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		4
-    (expected = NullPointerException.class)
    public void shouldNotAllowNullProcessSupplier() {
        testStream.process(null);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		4
-    (expected = NullPointerException.class)
    public void shouldNotAllowNullOtherStreamOnJoin() {
        testStream.join(null, MockValueJoiner.TOSTRING_JOINER, JoinWindows.of(10));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		4
-    (expected = NullPointerException.class)
    public void shouldNotAllowNullValueJoinerOnJoin() {
        testStream.join(testStream, null, JoinWindows.of(10));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		4
-    (expected = NullPointerException.class)
    public void shouldNotAllowNullJoinWindowsOnJoin() {
        testStream.join(testStream, MockValueJoiner.TOSTRING_JOINER, null);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		4
-    (expected = NullPointerException.class)
    public void shouldNotAllowNullTableOnTableJoin() {
        testStream.leftJoin((KTable) null, MockValueJoiner.TOSTRING_JOINER);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		4
-    (expected = NullPointerException.class)
    public void shouldNotAllowNullValueMapperOnTableJoin() {
        testStream.leftJoin(builder.table("topic", stringConsumed), null);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		4
-    (expected = NullPointerException.class)
    public void shouldNotAllowNullSelectorOnGroupBy() {
        testStream.groupBy(null);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		4
-    (expected = NullPointerException.class)
    public void shouldNotAllowNullActionOnForEach() {
        testStream.foreach(null);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		6
-    (expected = NullPointerException.class)
    public void shouldNotAllowNullTableOnJoinWithGlobalTable() {
        testStream.join((GlobalKTable) null,
                        MockMapper.<String, String>selectValueMapper(),
                        MockValueJoiner.TOSTRING_JOINER);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		6
-    (expected = NullPointerException.class)
    public void shouldNotAllowNullMapperOnJoinWithGlobalTable() {
        testStream.join(builder.globalTable("global", stringConsumed),
                        null,
                        MockValueJoiner.TOSTRING_JOINER);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		6
-    (expected = NullPointerException.class)
    public void shouldNotAllowNullJoinerOnJoinWithGlobalTable() {
        testStream.join(builder.globalTable("global", stringConsumed),
                        MockMapper.<String, String>selectValueMapper(),
                        null);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		6
-    (expected = NullPointerException.class)
    public void shouldNotAllowNullTableOnJLeftJoinWithGlobalTable() {
        testStream.leftJoin((GlobalKTable) null,
                        MockMapper.<String, String>selectValueMapper(),
                        MockValueJoiner.TOSTRING_JOINER);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		6
-    (expected = NullPointerException.class)
    public void shouldNotAllowNullMapperOnLeftJoinWithGlobalTable() {
        testStream.leftJoin(builder.globalTable("global", stringConsumed),
                        null,
                        MockValueJoiner.TOSTRING_JOINER);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		6
-    (expected = NullPointerException.class)
    public void shouldNotAllowNullJoinerOnLeftJoinWithGlobalTable() {
        testStream.leftJoin(builder.globalTable("global", stringConsumed),
                        MockMapper.<String, String>selectValueMapper(),
                        null);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		4
-    (expected = NullPointerException.class)
    public void shouldThrowNullPointerOnPrintIfPrintedIsNull() {
        testStream.print((Printed) null);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		4
-    (expected = NullPointerException.class)
    public void shouldThrowNullPointerOnThroughWhenProducedIsNull() {
        testStream.through("topic", null);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		4
-    (expected = NullPointerException.class)
    public void shouldThrowNullPointerOnToWhenProducedIsNull() {
        testStream.to("topic", null);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		22
-    
    public void testAggCoalesced() {
        final StreamsBuilder builder = new StreamsBuilder();
        final String topic1 = "topic1";
        final MockProcessorSupplier<String, String> proc = new MockProcessorSupplier<>();

        KTable<String, String> table1 = builder.table(topic1, consumed);
        KTable<String, String> table2 = table1.groupBy(MockMapper.<String, String>noOpKeyValueMapper(),
                                                       stringSerialzied
        ).aggregate(MockInitializer.STRING_INIT,
            MockAggregator.TOSTRING_ADDER,
            MockAggregator.TOSTRING_REMOVER,
            stringSerde,
            "topic1-Canonized");

        table2.toStream().process(proc);

        driver.setUp(builder, stateDir);

        driver.process(topic1, "A", "1");
        driver.process(topic1, "A", "3");
        driver.process(topic1, "A", "4");
        driver.flushState();
        assertEquals(Utils.mkList(
            "A:0+4"), proc.processed);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		22
-    
    public void testCountCoalesced() {
        final StreamsBuilder builder = new StreamsBuilder();
        final String input = "count-test-input";
        final MockProcessorSupplier<String, Long> proc = new MockProcessorSupplier<>();

        builder.table(input, consumed)
            .groupBy(MockMapper.<String, String>selectValueKeyValueMapper(), stringSerialzied)
            .count("count")
            .toStream()
            .process(proc);

        driver.setUp(builder, stateDir);

        driver.process(input, "A", "green");
        driver.process(input, "B", "green");
        driver.process(input, "A", "blue");
        driver.process(input, "C", "yellow");
        driver.process(input, "D", "green");
        driver.flushState();


        assertEquals(Utils.mkList(
            "blue:1",
            "yellow:1",
            "green:2"
            ), proc.processed);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		53
-    
    public void shouldForwardToCorrectProcessorNodeWhenMultiCacheEvictions() {
        final String tableOne = "tableOne";
        final String tableTwo = "tableTwo";
        final StreamsBuilder builder = new StreamsBuilder();
        final String reduceTopic = "TestDriver-reducer-store-repartition";
        final Map<String, Long> reduceResults = new HashMap<>();

        final KTable<String, String> one = builder.table(tableOne, consumed);
        final KTable<Long, String> two = builder.table(tableTwo, Consumed.with(Serdes.Long(), Serdes.String()));


        final KTable<String, Long> reduce = two.groupBy(new KeyValueMapper<Long, String, KeyValue<String, Long>>() {
            @Override
            public KeyValue<String, Long> apply(final Long key, final String value) {
                return new KeyValue<>(value, key);
            }
        }, Serialized.with(Serdes.String(), Serdes.Long()))
                .reduce(new Reducer<Long>() {
                    @Override
                    public Long apply(final Long value1, final Long value2) {
                        return value1 + value2;
                    }
                }, new Reducer<Long>() {
                    @Override
                    public Long apply(final Long value1, final Long value2) {
                        return value1 - value2;
                    }
                }, "reducer-store");

        reduce.toStream().foreach(new ForeachAction<String, Long>() {
            @Override
            public void apply(final String key, final Long value) {
                reduceResults.put(key, value);
            }
        });

        one.leftJoin(reduce, new ValueJoiner<String, Long, String>() {
            @Override
            public String apply(final String value1, final Long value2) {
                return value1 + ":" + value2;
            }
        })
                .mapValues(new ValueMapper<String, String>() {
                    @Override
                    public String apply(final String value) {
                        return value;
                    }
                });

        driver.setUp(builder, stateDir, 111);
        driver.process(reduceTopic, "1", new Change<>(1L, null));
        driver.process("tableOne", "2", "2");
        // this should trigger eviction on the reducer-store topic
        driver.process(reduceTopic, "2", new Change<>(2L, null));
        // this wont as it is the same value
        driver.process(reduceTopic, "2", new Change<>(2L, null));
        assertEquals(Long.valueOf(2L), reduceResults.get("2"));

        // this will trigger eviction on the tableOne topic
        // that in turn will cause an eviction on reducer-topic. It will flush
        // key 2 as it is the only dirty entry in the cache
        driver.process("tableOne", "1", "5");
        assertEquals(Long.valueOf(4L), reduceResults.get("2"));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		10
-    
    public void shouldCreateInMemoryStoreSupplierNotLogged() {
        final StateStoreSupplier supplier = Stores.create("store")
                .withKeys(Serdes.String())
                .withValues(Serdes.String())
                .inMemory()
                .disableLogging()
                .build();

        assertFalse(supplier.loggingEnabled());
    }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		12
-    
    public void shouldCreatePersistenStoreSupplierWithLoggedConfig() {
        final StateStoreSupplier supplier = Stores.create("store")
                .withKeys(Serdes.String())
                .withValues(Serdes.String())
                .persistent()
                .enableLogging(Collections.singletonMap("retention.ms", "1000"))
                .build();

        final Map<String, String> config = supplier.logConfig();
        assertTrue(supplier.loggingEnabled());
        assertEquals("1000", config.get("retention.ms"));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		12
-    
    public void shouldPutAndFetch() {
        final String key = "a";
        bytesStore.put(serializeKey(new Windowed<>(key, new SessionWindow(10, 10L))), serializeValue(10L));
        bytesStore.put(serializeKey(new Windowed<>(key, new SessionWindow(500L, 1000L))), serializeValue(50L));
        bytesStore.put(serializeKey(new Windowed<>(key, new SessionWindow(1500L, 2000L))), serializeValue(100L));
        bytesStore.put(serializeKey(new Windowed<>(key, new SessionWindow(2500L, 3000L))), serializeValue(200L));

        final List<KeyValue<Windowed<String>, Long>> expected = Arrays.asList(KeyValue.pair(new Windowed<>(key, new SessionWindow(10, 10)), 10L),
                                                                                    KeyValue.pair(new Windowed<>(key, new SessionWindow(500, 1000)), 50L));

        final KeyValueIterator<Bytes, byte[]> values = bytesStore.fetch(Bytes.wrap(key.getBytes()), 0, 1000L);
        assertEquals(expected, toList(values));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		8
-    
    public void shouldFindValuesWithinRange() {
        final String key = "a";
        bytesStore.put(serializeKey(new Windowed<>(key, new SessionWindow(0L, 0L))), serializeValue(50L));
        bytesStore.put(serializeKey(new Windowed<>(key, new SessionWindow(1000L, 1000L))), serializeValue(10L));
        final KeyValueIterator<Bytes, byte[]> results = bytesStore.fetch(Bytes.wrap(key.getBytes()), 1L, 1999L);
        assertEquals(Collections.singletonList(KeyValue.pair(new Windowed<>(key, new SessionWindow(1000L, 1000L)), 10L)), toList(results));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		8
-    
    public void shouldRemove() {
        bytesStore.put(serializeKey(new Windowed<>("a", new SessionWindow(0, 1000))), serializeValue(30L));
        bytesStore.put(serializeKey(new Windowed<>("a", new SessionWindow(1500, 2500))), serializeValue(50L));

        bytesStore.remove(serializeKey(new Windowed<>("a", new SessionWindow(0, 1000))));
        final KeyValueIterator<Bytes, byte[]> value = bytesStore.fetch(Bytes.wrap("a".getBytes()), 0, 1000L);
        assertFalse(value.hasNext());
    }
```
```
Cyclomatic Complexity	 1
Assertions		 6
Lines of Code		27
-    
    public void shouldRollSegments() {
        // just to validate directories
        final Segments segments = new Segments(storeName, retention, numSegments);
        final String key = "a";
        bytesStore.put(serializeKey(new Windowed<>(key, new SessionWindow(0L, 0L))), serializeValue(50L));
        assertEquals(Collections.singleton(segments.segmentName(0)), segmentDirs());

        bytesStore.put(serializeKey(new Windowed<>(key, new SessionWindow(30000L, 60000L))), serializeValue(100L));
        assertEquals(Utils.mkSet(segments.segmentName(0),
                                 segments.segmentName(1)), segmentDirs());

        bytesStore.put(serializeKey(new Windowed<>(key, new SessionWindow(61000L, 120000L))), serializeValue(200L));
        assertEquals(Utils.mkSet(segments.segmentName(0),
                                 segments.segmentName(1),
                                 segments.segmentName(2)), segmentDirs());

        bytesStore.put(serializeKey(new Windowed<>(key, new SessionWindow(121000L, 180000L))), serializeValue(300L));
        assertEquals(Utils.mkSet(segments.segmentName(1),
                                 segments.segmentName(2),
                                 segments.segmentName(3)), segmentDirs());

        bytesStore.put(serializeKey(new Windowed<>(key, new SessionWindow(181000L, 240000L))), serializeValue(400L));
        assertEquals(Utils.mkSet(segments.segmentName(2),
                                 segments.segmentName(3),
                                 segments.segmentName(4)), segmentDirs());

        final List<KeyValue<Windowed<String>, Long>> results = toList(bytesStore.fetch(Bytes.wrap(key.getBytes()), 0, 240000));
        assertEquals(Arrays.asList(KeyValue.pair(new Windowed<>(key, new SessionWindow(61000L, 120000L)), 200L),
                                   KeyValue.pair(new Windowed<>(key, new SessionWindow(121000L, 180000L)), 300L),
                                   KeyValue.pair(new Windowed<>(key, new SessionWindow(181000L, 240000L)), 400L)
                                                 ), results);

    }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		25
-    
    public void shouldLoadSegementsWithOldStyleDateFormattedName() {
        final Segments segments = new Segments(storeName, retention, numSegments);
        final String key = "a";
        bytesStore.put(serializeKey(new Windowed<>(key, new SessionWindow(0L, 0L))), serializeValue(50L));
        bytesStore.put(serializeKey(new Windowed<>(key, new SessionWindow(30000L, 60000L))), serializeValue(100L));
        bytesStore.close();

        final String firstSegmentName = segments.segmentName(0);
        final String[] nameParts = firstSegmentName.split("\\.");
        final Long segmentId = Long.parseLong(nameParts[1]);
        final SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmm");
        formatter.setTimeZone(new SimpleTimeZone(0, "UTC"));
        final String formatted = formatter.format(new Date(segmentId * segmentInterval(retention, numSegments)));
        final File parent = new File(stateDir, storeName);
        final File oldStyleName = new File(parent, nameParts[0] + "-" + formatted);
        assertTrue(new File(parent, firstSegmentName).renameTo(oldStyleName));

        bytesStore = new RocksDBSegmentedBytesStore(storeName,
                                                    retention,
                                                    numSegments,
                                                    schema);

        bytesStore.init(context, bytesStore);
        final List<KeyValue<Windowed<String>, Long>> results = toList(bytesStore.fetch(Bytes.wrap(key.getBytes()), 0L, 60000L));
        assertThat(results, equalTo(Arrays.asList(KeyValue.pair(new Windowed<>(key, new SessionWindow(0L, 0L)), 50L),
                                                  KeyValue.pair(new Windowed<>(key, new SessionWindow(30000L, 60000L)), 100L))));
    }
```
```
Cyclomatic Complexity	 2
Assertions		 1
Lines of Code		13
-    
    public void shouldFetchAllSessionsWithSameRecordKey() {

        final List<KeyValue<Windowed<String>, Long>> expected = Arrays.asList(KeyValue.pair(new Windowed<>("a", new SessionWindow(0, 0)), 1L),
                                                                                    KeyValue.pair(new Windowed<>("a", new SessionWindow(10, 10)), 2L),
                                                                                    KeyValue.pair(new Windowed<>("a", new SessionWindow(100, 100)), 3L),
                                                                                    KeyValue.pair(new Windowed<>("a", new SessionWindow(1000, 1000)), 4L));
        for (KeyValue<Windowed<String>, Long> kv : expected) {
            sessionStore.put(kv.key, kv.value);
        }

        // add one that shouldn't appear in the results
        sessionStore.put(new Windowed<>("aa", new SessionWindow(0, 0)), 5L);

        final List<KeyValue<Windowed<String>, Long>> results = toList(sessionStore.fetch("a"));
        assertEquals(expected, results);

    }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		11
-    
    public void shouldFindValuesWithinMergingSessionWindowRange() {
        final String key = "a";
        sessionStore.put(new Windowed<>(key, new SessionWindow(0L, 0L)), 1L);
        sessionStore.put(new Windowed<>(key, new SessionWindow(1000L, 1000L)), 2L);
        final KeyValueIterator<Windowed<String>, Long> results = sessionStore.findSessions(key, -1, 1000L);

        final List<KeyValue<Windowed<String>, Long>> expected = Arrays.asList(
                KeyValue.pair(new Windowed<>(key, new SessionWindow(0L, 0L)), 1L),
                KeyValue.pair(new Windowed<>(key, new SessionWindow(1000L, 1000L)), 2L));
        assertEquals(expected, toList(results));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		8
-    
    public void shouldRemove() {
        sessionStore.put(new Windowed<>("a", new SessionWindow(0, 1000)), 1L);
        sessionStore.put(new Windowed<>("a", new SessionWindow(1500, 2500)), 2L);

        sessionStore.remove(new Windowed<>("a", new SessionWindow(0, 1000)));
        assertFalse(sessionStore.findSessions("a", 0, 1000L).hasNext());

        assertTrue(sessionStore.findSessions("a", 1500, 2500).hasNext());
    }
```
```
Cyclomatic Complexity	 1
Assertions		 3
Lines of Code		17
-    
    public void shouldFindSessionsToMerge() {
        final Windowed<String> session1 = new Windowed<>("a", new SessionWindow(0, 100));
        final Windowed<String> session2 = new Windowed<>("a", new SessionWindow(101, 200));
        final Windowed<String> session3 = new Windowed<>("a", new SessionWindow(201, 300));
        final Windowed<String> session4 = new Windowed<>("a", new SessionWindow(301, 400));
        final Windowed<String> session5 = new Windowed<>("a", new SessionWindow(401, 500));
        sessionStore.put(session1, 1L);
        sessionStore.put(session2, 2L);
        sessionStore.put(session3, 3L);
        sessionStore.put(session4, 4L);
        sessionStore.put(session5, 5L);
        final KeyValueIterator<Windowed<String>, Long> results = sessionStore.findSessions("a", 150, 300);
        assertEquals(session2, results.next().key);
        assertEquals(session3, results.next().key);
        assertFalse(results.hasNext());
    }
```
## 75a68341da423d8b041ac8824fbebe99f9bd15ff ##
```
Cyclomatic Complexity	3
Assertions		7
Lines of Code		28
-    
    public void testMetadata() throws Exception {
        long time = 0;
        metadata.update(emptyMetadataResponse(), time);
        assertFalse("No update needed.", metadata.timeToNextUpdate(time) == 0);
        metadata.requestUpdate();
        assertFalse("Still no updated needed due to backoff", metadata.timeToNextUpdate(time) == 0);
        time += refreshBackoffMs;
        assertTrue("Update needed now that backoff time expired", metadata.timeToNextUpdate(time) == 0);
        String topic = "my-topic";
        Thread t1 = asyncFetch(topic, 500);
        Thread t2 = asyncFetch(topic, 500);
        assertTrue("Awaiting update", t1.isAlive());
        assertTrue("Awaiting update", t2.isAlive());
        // Perform metadata update when an update is requested on the async fetch thread
        // This simulates the metadata update sequence in KafkaProducer
        while (t1.isAlive() || t2.isAlive()) {
            if (metadata.timeToNextUpdate(time) == 0) {
                MetadataResponse response = TestUtils.metadataUpdateWith(1, Collections.singletonMap(topic, 1));
                metadata.update(response, time);
                time += refreshBackoffMs;
            }
            Thread.sleep(1);
        }
        t1.join();
        t2.join();
        assertFalse("No update needed.", metadata.timeToNextUpdate(time) == 0);
        time += metadataExpireMs;
        assertTrue("Update needed due to stale metadata.", metadata.timeToNextUpdate(time) == 0);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 5
Lines of Code		17
-    
    public void testMetadataAwaitAfterClose() throws InterruptedException {
        long time = 0;
        metadata.update(emptyMetadataResponse(), time);
        assertFalse("No update needed.", metadata.timeToNextUpdate(time) == 0);
        metadata.requestUpdate();
        assertFalse("Still no updated needed due to backoff", metadata.timeToNextUpdate(time) == 0);
        time += refreshBackoffMs;
        assertTrue("Update needed now that backoff time expired", metadata.timeToNextUpdate(time) == 0);
        String topic = "my-topic";
        metadata.close();
        Thread t1 = asyncFetch(topic, 500);
        t1.join();
        assertTrue(backgroundError.get().getClass() == KafkaException.class);
        assertTrue(backgroundError.get().toString().contains("Requested metadata update after close"));
        clearBackgroundError();@@@     }
```
```
Cyclomatic Complexity	 4
Assertions		 5
Lines of Code		18
-    
    public void testTimeToNextUpdate_OverwriteBackoff() {
        long now = 10000;

        // New topic added to fetch set and update requested. It should allow immediate update.
        metadata.update(emptyMetadataResponse(), now);
        metadata.add("new-topic");
        assertEquals(0, metadata.timeToNextUpdate(now));

        // Even though setTopics called, immediate update isn't necessary if the new topic set isn't
        // containing a new topic,
        metadata.update(emptyMetadataResponse(), now);
        metadata.setTopics(metadata.topics());
        assertEquals(metadataExpireMs, metadata.timeToNextUpdate(now));

        // If the new set of topics containing a new topic then it should allow immediate update.
        metadata.setTopics(Collections.singletonList("another-new-topic"));
        assertEquals(0, metadata.timeToNextUpdate(now));

        // If metadata requested for all topics it should allow immediate update.
        metadata.update(emptyMetadataResponse(), now);
        metadata.needMetadataForAllTopics(true);
        assertEquals(0, metadata.timeToNextUpdate(now));

        // However if metadata is already capable to serve all topics it shouldn't override backoff.
        metadata.update(emptyMetadataResponse(), now);
        metadata.needMetadataForAllTopics(true);
        assertEquals(metadataExpireMs, metadata.timeToNextUpdate(now));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		20
-    
    public void testListenerGetsNotifiedOfUpdate() {
        long time = 0;
        final Set<String> topics = new HashSet<>();
        metadata.update(emptyMetadataResponse(), time);
        metadata.addListener(new Metadata.Listener() {
            @Override
            public void onMetadataUpdate(Cluster cluster, Set<String> unavailableTopics) {
                topics.clear();
                topics.addAll(cluster.topics());
            }
        });

        Map<String, Integer> partitionCounts = new HashMap<>();
        partitionCounts.put("topic", 1);
        partitionCounts.put("topic1", 1);
        MetadataResponse metadataResponse = TestUtils.metadataUpdateWith("dummy", 1, partitionCounts);
        metadata.update(metadataResponse, 100);

        assertEquals("Listener did not update topics list correctly",
            new HashSet<>(Arrays.asList("topic", "topic1")), topics);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		27
-    
    public void testListenerCanUnregister() {
        long time = 0;
        final Set<String> topics = new HashSet<>();
        metadata.update(emptyMetadataResponse(), time);
        final Metadata.Listener listener = new Metadata.Listener() {
            @Override
            public void onMetadataUpdate(Cluster cluster, Set<String> unavailableTopics) {
                topics.clear();
                topics.addAll(cluster.topics());
            }
        };
        metadata.addListener(listener);

        Map<String, Integer> partitionCounts = new HashMap<>();
        partitionCounts.put("topic", 1);
        partitionCounts.put("topic1", 1);
        MetadataResponse metadataResponse = TestUtils.metadataUpdateWith("dummy", 1, partitionCounts);
        metadata.update(metadataResponse, 100);

        metadata.removeListener(listener);

        partitionCounts.clear();
        partitionCounts.put("topic2", 1);
        partitionCounts.put("topic3", 1);
        metadataResponse = TestUtils.metadataUpdateWith("dummy", 1, partitionCounts);
        metadata.update(metadataResponse, 100);

        assertEquals("Listener did not update topics list correctly",
            new HashSet<>(Arrays.asList("topic", "topic1")), topics);
    }
```
```
Cyclomatic Complexity	 4
Assertions		 3
Lines of Code		25
-    
    public void testTopicExpiry() throws Exception {
        metadata = new Metadata(refreshBackoffMs, metadataExpireMs, true, true, new ClusterResourceListeners());

        // Test that topic is expired if not used within the expiry interval
        long time = 0;
        metadata.add("topic1");
        metadata.update(emptyMetadataResponse(), time);
        time += Metadata.TOPIC_EXPIRY_MS;
        metadata.update(emptyMetadataResponse(), time);
        assertFalse("Unused topic not expired", metadata.containsTopic("topic1"));

        // Test that topic is not expired if used within the expiry interval
        metadata.add("topic2");
        metadata.update(emptyMetadataResponse(), time);
        for (int i = 0; i < 3; i++) {
            time += Metadata.TOPIC_EXPIRY_MS / 2;
            metadata.update(emptyMetadataResponse(), time);
            assertTrue("Topic expired even though in use", metadata.containsTopic("topic2"));
            metadata.add("topic2");
        }

        // Test that topics added using setTopics expire
        HashSet<String> topics = new HashSet<>();
        topics.add("topic4");
        metadata.setTopics(topics);
        metadata.update(emptyMetadataResponse(), time);
        time += Metadata.TOPIC_EXPIRY_MS;
        metadata.update(emptyMetadataResponse(), time);
        assertFalse("Unused topic not expired", metadata.containsTopic("topic4"));
    }
```
```
Cyclomatic Complexity	 4
Assertions		 3
Lines of Code		24
-    
    public void testNonExpiringMetadata() throws Exception {
        metadata = new Metadata(refreshBackoffMs, metadataExpireMs, true, false, new ClusterResourceListeners());

        // Test that topic is not expired if not used within the expiry interval
        long time = 0;
        metadata.add("topic1");
        metadata.update(emptyMetadataResponse(), time);
        time += Metadata.TOPIC_EXPIRY_MS;
        metadata.update(emptyMetadataResponse(), time);
        assertTrue("Unused topic expired when expiry disabled", metadata.containsTopic("topic1"));

        // Test that topic is not expired if used within the expiry interval
        metadata.add("topic2");
        metadata.update(emptyMetadataResponse(), time);
        for (int i = 0; i < 3; i++) {
            time += Metadata.TOPIC_EXPIRY_MS / 2;
            metadata.update(emptyMetadataResponse(), time);
            assertTrue("Topic expired even though in use", metadata.containsTopic("topic2"));
            metadata.add("topic2");
        }

        // Test that topics added using setTopics don't expire
        HashSet<String> topics = new HashSet<>();
        topics.add("topic4");
        metadata.setTopics(topics);
        time += metadataExpireMs * 2;
        metadata.update(emptyMetadataResponse(), time);
        assertTrue("Unused topic expired when expiry disabled", metadata.containsTopic("topic4"));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		4
-    
    public void testAssignWithTopicUnavailable() {
        unavailableTopicTest(true, Collections.emptySet());
    }
```
```
Cyclomatic Complexity	 -1
Assertions		 0
Lines of Code		6
-    
    public void testMultibyteUtf8Lengths() {
        validateUtf8Length("A\u00ea\u00f1\u00fcC");
        validateUtf8Length("\ud801\udc00");
        validateUtf8Length("M\u00fcO");
    }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		13
-  
  def testMaxOffset() {
    val baseOffset = 50
    val seg = createSegment(baseOffset)
    val ms = records(baseOffset, "hello", "there", "beautiful")
    seg.append(52, RecordBatch.NO_TIMESTAMP, -1L, ms)
    def validate(offset: Long) =
      assertEquals(ms.records.asScala.filter(_.offset == offset).toList,
                   seg.read(startOffset = offset, maxSize = 1024, maxOffset = Some(offset+1)).records.records.asScala.toList)
    validate(50)
    validate(51)
    validate(52)@@@   }
```
```
Cyclomatic Complexity	 1
Assertions		 12
Lines of Code		27
-  
  def testTruncateHead(): Unit = {
    val epoch = 0.toShort

    append(stateManager, producerId, epoch, 0, 0L)
    append(stateManager, producerId, epoch, 1, 1L)
    stateManager.takeSnapshot()

    val anotherPid = 2L
    append(stateManager, anotherPid, epoch, 0, 2L)
    append(stateManager, anotherPid, epoch, 1, 3L)
    stateManager.takeSnapshot()
    assertEquals(Set(2, 4), currentSnapshotOffsets)

    stateManager.truncateHead(2)
    assertEquals(Set(2, 4), currentSnapshotOffsets)
    assertEquals(Set(anotherPid), stateManager.activeProducers.keySet)
    assertEquals(None, stateManager.lastEntry(producerId))

    val maybeEntry = stateManager.lastEntry(anotherPid)
    assertTrue(maybeEntry.isDefined)
    assertEquals(3L, maybeEntry.get.lastDataOffset)

    stateManager.truncateHead(3)
    assertEquals(Set(anotherPid), stateManager.activeProducers.keySet)
    assertEquals(Set(4), currentSnapshotOffsets)
    assertEquals(4, stateManager.mapEndOffset)

    stateManager.truncateHead(5)
    assertEquals(Set(), stateManager.activeProducers.keySet)
    assertEquals(Set(), currentSnapshotOffsets)
    assertEquals(5, stateManager.mapEndOffset)
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		7
-    
    public void testOsDefaultSocketBufferSizes() {
        props.put(CommonClientConfigs.SEND_BUFFER_CONFIG, Selectable.USE_DEFAULT_BUFFER_SIZE);
        props.put(CommonClientConfigs.RECEIVE_BUFFER_CONFIG, Selectable.USE_DEFAULT_BUFFER_SIZE);
        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.close();
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		6
-    (expected = KafkaException.class)
    public void testInvalidSocketSendBufferSize() {
        props.put(CommonClientConfigs.SEND_BUFFER_CONFIG, -2);
        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.close();
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		6
-    (expected = KafkaException.class)
    public void testInvalidSocketReceiveBufferSize() {
        props.put(CommonClientConfigs.RECEIVE_BUFFER_CONFIG, -2);
        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.close();
    }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		5
-    
    public void stateShouldTransitToNotRunningIfCloseRightAfterCreated() {
        globalStreams.close();

        Assert.assertEquals(KafkaStreams.State.NOT_RUNNING, globalStreams.state());
    }
```
```
Cyclomatic Complexity	 5
Assertions		 12
Lines of Code		53
-    
    public void stateShouldTransitToRunningIfNonDeadThreadsBackToRunning() throws InterruptedException {
        final StateListenerStub stateListener = new StateListenerStub();
        globalStreams.setStateListener(stateListener);

        Assert.assertEquals(0, stateListener.numChanges);
        Assert.assertEquals(KafkaStreams.State.CREATED, globalStreams.state());

        globalStreams.start();

        TestUtils.waitForCondition(
            () -> stateListener.numChanges == 2,
            "Streams never started.");
        Assert.assertEquals(KafkaStreams.State.RUNNING, globalStreams.state());

        for (final StreamThread thread: globalStreams.threads) {
            thread.stateListener().onChange(
                thread,
                StreamThread.State.PARTITIONS_REVOKED,
                StreamThread.State.RUNNING);
        }

        Assert.assertEquals(3, stateListener.numChanges);
        Assert.assertEquals(KafkaStreams.State.REBALANCING, globalStreams.state());

        for (final StreamThread thread : globalStreams.threads) {
            thread.stateListener().onChange(
                thread,
                StreamThread.State.PARTITIONS_ASSIGNED,
                StreamThread.State.PARTITIONS_REVOKED);
        }

        Assert.assertEquals(3, stateListener.numChanges);
        Assert.assertEquals(KafkaStreams.State.REBALANCING, globalStreams.state());

        globalStreams.threads[NUM_THREADS - 1].stateListener().onChange(
            globalStreams.threads[NUM_THREADS - 1],
            StreamThread.State.PENDING_SHUTDOWN,
            StreamThread.State.PARTITIONS_ASSIGNED);

        globalStreams.threads[NUM_THREADS - 1].stateListener().onChange(
            globalStreams.threads[NUM_THREADS - 1],
            StreamThread.State.DEAD,
            StreamThread.State.PENDING_SHUTDOWN);

        Assert.assertEquals(3, stateListener.numChanges);
        Assert.assertEquals(KafkaStreams.State.REBALANCING, globalStreams.state());

        for (final StreamThread thread : globalStreams.threads) {
            if (thread != globalStreams.threads[NUM_THREADS - 1]) {
                thread.stateListener().onChange(
                    thread,
                    StreamThread.State.RUNNING,
                    StreamThread.State.PARTITIONS_ASSIGNED);
            }
        }

        Assert.assertEquals(4, stateListener.numChanges);
        Assert.assertEquals(KafkaStreams.State.RUNNING, globalStreams.state());

        globalStreams.close();

        TestUtils.waitForCondition(
            () -> stateListener.numChanges == 6,
            "Streams never closed.");
        Assert.assertEquals(KafkaStreams.State.NOT_RUNNING, globalStreams.state());
    }
```
```
Cyclomatic Complexity	 4
Assertions		 10
Lines of Code		49
-    
    public void stateShouldTransitToErrorIfAllThreadsDead() throws InterruptedException {
        final StateListenerStub stateListener = new StateListenerStub();
        globalStreams.setStateListener(stateListener);

        Assert.assertEquals(0, stateListener.numChanges);
        Assert.assertEquals(KafkaStreams.State.CREATED, globalStreams.state());

        globalStreams.start();

        TestUtils.waitForCondition(
            () -> stateListener.numChanges == 2,
            "Streams never started.");
        Assert.assertEquals(KafkaStreams.State.RUNNING, globalStreams.state());

        for (final StreamThread thread : globalStreams.threads) {
            thread.stateListener().onChange(
                thread,
                StreamThread.State.PARTITIONS_REVOKED,
                StreamThread.State.RUNNING);
        }

        Assert.assertEquals(3, stateListener.numChanges);
        Assert.assertEquals(KafkaStreams.State.REBALANCING, globalStreams.state());

        globalStreams.threads[NUM_THREADS - 1].stateListener().onChange(
            globalStreams.threads[NUM_THREADS - 1],
            StreamThread.State.PENDING_SHUTDOWN,
            StreamThread.State.PARTITIONS_REVOKED);

        globalStreams.threads[NUM_THREADS - 1].stateListener().onChange(
            globalStreams.threads[NUM_THREADS - 1],
            StreamThread.State.DEAD,
            StreamThread.State.PENDING_SHUTDOWN);

        Assert.assertEquals(3, stateListener.numChanges);
        Assert.assertEquals(KafkaStreams.State.REBALANCING, globalStreams.state());

        for (final StreamThread thread : globalStreams.threads) {
            if (thread != globalStreams.threads[NUM_THREADS - 1]) {
                thread.stateListener().onChange(
                    thread,
                    StreamThread.State.PENDING_SHUTDOWN,
                    StreamThread.State.PARTITIONS_REVOKED);

                thread.stateListener().onChange(
                    thread,
                    StreamThread.State.DEAD,
                    StreamThread.State.PENDING_SHUTDOWN);
            }
        }

        Assert.assertEquals(4, stateListener.numChanges);
        Assert.assertEquals(KafkaStreams.State.ERROR, globalStreams.state());

        globalStreams.close();

        // the state should not stuck with ERROR, but transit to NOT_RUNNING in the end
        TestUtils.waitForCondition(
            () -> stateListener.numChanges == 6,
            "Streams never closed.");
        Assert.assertEquals(KafkaStreams.State.NOT_RUNNING, globalStreams.state());
    }
```
```
Cyclomatic Complexity	 2
Assertions		 3
Lines of Code		20
-    
    public void shouldCleanupResourcesOnCloseWithoutPreviousStart() throws Exception {
        builder.globalTable("anyTopic");
        final List<Node> nodes = Collections.singletonList(new Node(0, "localhost", 8121));
        final Cluster cluster = new Cluster("mockClusterId", nodes,
                                            Collections.emptySet(), Collections.emptySet(),
                                            Collections.emptySet(), nodes.get(0));
        final MockClientSupplier clientSupplier = new MockClientSupplier();
        clientSupplier.setClusterForAdminClient(cluster);
        final KafkaStreams streams = new KafkaStreams(builder.build(), props, clientSupplier);
        streams.close();
        TestUtils.waitForCondition(
            () -> streams.state() == KafkaStreams.State.NOT_RUNNING,
            "Streams never stopped.");

        // Ensure that any created clients are closed
        assertTrue(clientSupplier.consumer.closed());
        assertTrue(clientSupplier.restoreConsumer.closed());
        for (final MockProducer p : clientSupplier.producers) {
            assertTrue(p.closed());
        }
    }
```
```
Cyclomatic Complexity	 2
Assertions		 3
Lines of Code		36
-    
    public void testStateThreadClose() throws Exception {
        // make sure we have the global state thread running too
        builder.globalTable("anyTopic");
        final KafkaStreams streams = new KafkaStreams(builder.build(), props);

        try {
            final java.lang.reflect.Field threadsField = streams.getClass().getDeclaredField("threads");
            threadsField.setAccessible(true);
            final StreamThread[] threads = (StreamThread[]) threadsField.get(streams);

            assertEquals(NUM_THREADS, threads.length);
            assertEquals(streams.state(), KafkaStreams.State.CREATED);

            streams.start();
            TestUtils.waitForCondition(
                () -> streams.state() == KafkaStreams.State.RUNNING,
                "Streams never started.");

            for (int i = 0; i < NUM_THREADS; i++) {
                final StreamThread tmpThread = threads[i];
                tmpThread.shutdown();
                TestUtils.waitForCondition(
                    () -> tmpThread.state() == StreamThread.State.DEAD,
                    "Thread never stopped.");
                threads[i].join();
            }
            TestUtils.waitForCondition(
                () -> streams.state() == KafkaStreams.State.ERROR,
                "Streams never stopped.");
        } finally {
            streams.close();
        }

        TestUtils.waitForCondition(
            () -> streams.state() == KafkaStreams.State.NOT_RUNNING,
            "Streams never stopped.");

        final java.lang.reflect.Field globalThreadField = streams.getClass().getDeclaredField("globalStreamThread");
        globalThreadField.setAccessible(true);
        final GlobalStreamThread globalStreamThread = (GlobalStreamThread) globalThreadField.get(streams);
        assertNull(globalStreamThread);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		23
-    
    public void testStateGlobalThreadClose() throws Exception {
        // make sure we have the global state thread running too
        builder.globalTable("anyTopic");
        final KafkaStreams streams = new KafkaStreams(builder.build(), props);

        try {
            streams.start();
            TestUtils.waitForCondition(
                () -> streams.state() == KafkaStreams.State.RUNNING,
                "Streams never started.");
            final java.lang.reflect.Field globalThreadField = streams.getClass().getDeclaredField("globalStreamThread");
            globalThreadField.setAccessible(true);
            final GlobalStreamThread globalStreamThread = (GlobalStreamThread) globalThreadField.get(streams);
            globalStreamThread.shutdown();
            TestUtils.waitForCondition(
                () -> globalStreamThread.state() == GlobalStreamThread.State.DEAD,
                "Thread never stopped.");
            globalStreamThread.join();
            assertEquals(streams.state(), KafkaStreams.State.ERROR);
        } finally {
            streams.close();
        }

        assertEquals(streams.state(), KafkaStreams.State.NOT_RUNNING);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		16
-    
    public void globalThreadShouldTimeoutWhenBrokerConnectionCannotBeEstablished() {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "appId");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:1");
        props.put(StreamsConfig.METRIC_REPORTER_CLASSES_CONFIG, MockMetricsReporter.class.getName());
        props.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, NUM_THREADS);

        props.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 200);

        // make sure we have the global state thread running too
        builder.globalTable("anyTopic");
        try (final KafkaStreams streams = new KafkaStreams(builder.build(), props)) {
            streams.start();
            fail("expected start() to time out and throw an exception.");
        } catch (final StreamsException expected) {
            // This is a result of not being able to connect to the broker.
        }
        // There's nothing to assert... We're testing that this operation actually completes.
    }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		13
-    
    public void testLocalThreadCloseWithoutConnectingToBroker() {
        final Properties props = new Properties();
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "appId");
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:1");
        props.setProperty(StreamsConfig.METRIC_REPORTER_CLASSES_CONFIG, MockMetricsReporter.class.getName());
        props.setProperty(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, NUM_THREADS);

        // make sure we have the global state thread running too
        builder.table("anyTopic");
        try (final KafkaStreams streams = new KafkaStreams(builder.build(), props)) {
            streams.start();
        }
        // There's nothing to assert... We're testing that this operation actually completes.
    }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		13
-    
    public void testInitializesAndDestroysMetricsReporters() {
        final int oldInitCount = MockMetricsReporter.INIT_COUNT.get();

        try (final KafkaStreams streams = new KafkaStreams(builder.build(), props)) {
            final int newInitCount = MockMetricsReporter.INIT_COUNT.get();
            final int initDiff = newInitCount - oldInitCount;
            assertTrue("some reporters should be initialized by calling on construction", initDiff > 0);

            streams.start();
            final int oldCloseCount = MockMetricsReporter.CLOSE_COUNT.get();
            streams.close();
            assertEquals(oldCloseCount + initDiff, MockMetricsReporter.CLOSE_COUNT.get());
        }
    }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		8
-    
    public void testCloseIsIdempotent() {
        globalStreams.close();
        final int closeCount = MockMetricsReporter.CLOSE_COUNT.get();

        globalStreams.close();
        Assert.assertEquals("subsequent close() calls should do nothing",
            closeCount, MockMetricsReporter.CLOSE_COUNT.get());
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		12
-    
    public void testCannotStartOnceClosed() {
        globalStreams.start();
        globalStreams.close();
        try {
            globalStreams.start();
            fail("Should have throw IllegalStateException");
        } catch (final IllegalStateException expected) {
            // this is ok
        } finally {
            globalStreams.close();
        }
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		11
-    
    public void testCannotStartTwice() {
        globalStreams.start();

        try {
            globalStreams.start();
            fail("Should throw an IllegalStateException");
        } catch (final IllegalStateException e) {
            // this is ok
        } finally {
            globalStreams.close();
        }
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		11
-    
    public void shouldNotSetGlobalRestoreListenerAfterStarting() {
        globalStreams.start();
        try {
            globalStreams.setGlobalStateRestoreListener(new MockStateRestoreListener());
            fail("Should throw an IllegalStateException");
        } catch (final IllegalStateException e) {
            // expected
        } finally {
            globalStreams.close();
        }
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		9
-    
    public void shouldThrowExceptionSettingUncaughtExceptionHandlerNotInCreateState() {
        globalStreams.start();
        try {
            globalStreams.setUncaughtExceptionHandler(null);
            fail("Should throw IllegalStateException");
        } catch (final IllegalStateException e) {
            // expected
        }
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		9
-    
    public void shouldThrowExceptionSettingStateListenerNotInCreateState() {
        globalStreams.start();
        try {
            globalStreams.setStateListener(null);
            fail("Should throw IllegalStateException");
        } catch (final IllegalStateException e) {
            // expected
        }
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		8
-    
    public void testIllegalMetricsConfig() {
        props.setProperty(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG, "illegalConfig");

        try {
            new KafkaStreams(builder.build(), props);
            fail("Should have throw ConfigException");
        } catch (final ConfigException expected) { /* expected */ }
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		7
-    
    public void testLegalMetricsConfig() {
        props.setProperty(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG, Sensor.RecordingLevel.INFO.toString());
        new KafkaStreams(builder.build(), props).close();

        props.setProperty(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG, Sensor.RecordingLevel.DEBUG.toString());
        new KafkaStreams(builder.build(), props).close();
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		4
-    (expected = IllegalStateException.class)
    public void shouldNotGetAllTasksWhenNotRunning() {
        globalStreams.allMetadata();
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		4
-    (expected = IllegalStateException.class)
    public void shouldNotGetAllTasksWithStoreWhenNotRunning() {
        globalStreams.allMetadataForStore("store");
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		4
-    (expected = IllegalStateException.class)
    public void shouldNotGetTaskWithKeyAndSerializerWhenNotRunning() {
        globalStreams.metadataForKey("store", "key", Serdes.String().serializer());
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		4
-    (expected = IllegalStateException.class)
    public void shouldNotGetTaskWithKeyAndPartitionerWhenNotRunning() {
        globalStreams.metadataForKey("store", "key", (topic, key, value, numPartitions) -> 0);
    }
```
```
Cyclomatic Complexity	 3
Assertions		 2
Lines of Code		38
-    
    public void shouldReturnFalseOnCloseWhenThreadsHaventTerminated() throws Exception {
        final AtomicBoolean keepRunning = new AtomicBoolean(true);
        KafkaStreams streams = null;
        try {
            final StreamsBuilder builder = new StreamsBuilder();
            final CountDownLatch latch = new CountDownLatch(1);
            final String topic = "input";
            CLUSTER.createTopics(topic);

            builder.stream(topic, Consumed.with(Serdes.String(), Serdes.String()))
                    .foreach((key, value) -> {
                        try {
                            latch.countDown();
                            while (keepRunning.get()) {
                                Thread.sleep(10);
                            }
                        } catch (final InterruptedException e) {
                            // no-op
                        }
                    });
            streams = new KafkaStreams(builder.build(), props);
            streams.start();
            IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(topic,
                Collections.singletonList(new KeyValue<>("A", "A")),
                TestUtils.producerConfig(
                    CLUSTER.bootstrapServers(),
                    StringSerializer.class,
                    StringSerializer.class,
                    new Properties()),
                System.currentTimeMillis());

            assertTrue("Timed out waiting to receive single message", latch.await(30, TimeUnit.SECONDS));
            assertFalse(streams.close(Duration.ofMillis(10)));
        } finally {
            // stop the thread so we don't interfere with other tests etc
            keepRunning.set(false);
            if (streams != null) {
                streams.close();
            }
        }
    }
```
```
Cyclomatic Complexity	 2
Assertions		 10
Lines of Code		19
-    
    public void shouldReturnThreadMetadata() {
        globalStreams.start();
        final Set<ThreadMetadata> threadMetadata = globalStreams.localThreadsMetadata();
        assertNotNull(threadMetadata);
        assertEquals(2, threadMetadata.size());
        for (final ThreadMetadata metadata : threadMetadata) {
            assertTrue("#threadState() was: " + metadata.threadState() + "; expected either RUNNING, STARTING, PARTITIONS_REVOKED, PARTITIONS_ASSIGNED, or CREATED",
                asList("RUNNING", "STARTING", "PARTITIONS_REVOKED", "PARTITIONS_ASSIGNED", "CREATED").contains(metadata.threadState()));
            assertEquals(0, metadata.standbyTasks().size());
            assertEquals(0, metadata.activeTasks().size());
            final String threadName = metadata.threadName();
            assertTrue(threadName.startsWith("clientId-StreamThread-"));
            assertEquals(threadName + "-consumer", metadata.consumerClientId());
            assertEquals(threadName + "-restore-consumer", metadata.restoreConsumerClientId());
            assertEquals(Collections.singleton(threadName + "-producer"), metadata.producerClientIds());
            assertEquals("clientId-admin", metadata.adminClientId());
        }
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		10
-    
    public void shouldAllowCleanupBeforeStartAndAfterClose() {
        try {
            globalStreams.cleanUp();
            globalStreams.start();
        } finally {
            globalStreams.close();
        }
        globalStreams.cleanUp();
    }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		13
-    
    public void shouldThrowOnCleanupWhileRunning() throws InterruptedException {
        globalStreams.start();
        TestUtils.waitForCondition(
            () -> globalStreams.state() == KafkaStreams.State.RUNNING,
            "Streams never started.");

        try {
            globalStreams.cleanUp();
            fail("Should have thrown IllegalStateException");
        } catch (final IllegalStateException expected) {
            assertEquals("Cannot clean up while running.", expected.getMessage());
        }
    }
```
```
Cyclomatic Complexity	 2
Assertions		 4
Lines of Code		24
-    
    public void shouldCleanupOldStateDirs() throws InterruptedException {
        props.setProperty(StreamsConfig.STATE_CLEANUP_DELAY_MS_CONFIG, "1");

        final String topic = "topic";
        CLUSTER.createTopic(topic);
        final StreamsBuilder builder = new StreamsBuilder();

        builder.table(topic, Materialized.as("store"));

        try (final KafkaStreams streams = new KafkaStreams(builder.build(), props)) {
            final CountDownLatch latch = new CountDownLatch(1);
            streams.setStateListener((newState, oldState) -> {
                if (newState == KafkaStreams.State.RUNNING && oldState == KafkaStreams.State.REBALANCING) {
                    latch.countDown();
                }
            });
            final String appDir = props.getProperty(StreamsConfig.STATE_DIR_CONFIG) + File.separator + props.getProperty(StreamsConfig.APPLICATION_ID_CONFIG);
            final File oldTaskDir = new File(appDir, "10_1");
            assertTrue(oldTaskDir.mkdirs());

            streams.start();
            latch.await(30, TimeUnit.SECONDS);
            verifyCleanupStateDir(appDir, oldTaskDir);
            assertTrue(oldTaskDir.mkdirs());
            verifyCleanupStateDir(appDir, oldTaskDir);
        }
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		8
-    
    public void shouldThrowOnNegativeTimeoutForClose() {
        try (final KafkaStreams streams = new KafkaStreams(builder.build(), props)) {
            streams.close(Duration.ofMillis(-1L));
            fail("should not accept negative close parameter");
        } catch (final IllegalArgumentException e) {
            // expected
        }
    }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		12
-    
    public void shouldNotBlockInCloseForZeroDuration() throws InterruptedException {
        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        final Thread th = new Thread(() -> streams.close(Duration.ofMillis(0L)));

        th.start();

        try {
            th.join(30_000L);
            assertFalse(th.isAlive());
        } finally {
            streams.close();
        }
    }
```
```
Cyclomatic Complexity	 2
Assertions		 0
Lines of Code		18
-    
    public void statelessTopologyShouldNotCreateStateDirectory() throws Exception {
        final String inputTopic = testName.getMethodName() + "-input";
        final String outputTopic = testName.getMethodName() + "-output";
        CLUSTER.createTopics(inputTopic, outputTopic);

        final Topology topology = new Topology();
        topology.addSource("source", Serdes.String().deserializer(), Serdes.String().deserializer(), inputTopic)
                .addProcessor("process", () -> new AbstractProcessor<String, String>() {
                    @Override
                    public void process(final String key, final String value) {
                        if (value.length() % 2 == 0) {
                            context().forward(key, key + value);
                        }
                    }
                }, "source")
                .addSink("sink", outputTopic, new StringSerializer(), new StringSerializer(), "process");
        startStreamsAndCheckDirExists(topology, Collections.singleton(inputTopic), outputTopic, false);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		10
-    
    public void inMemoryStatefulTopologyShouldNotCreateStateDirectory() throws Exception {
        final String inputTopic = testName.getMethodName() + "-input";
        final String outputTopic = testName.getMethodName() + "-output";
        final String globalTopicName = testName.getMethodName() + "-global";
        final String storeName = testName.getMethodName() + "-counts";
        final String globalStoreName = testName.getMethodName() + "-globalStore";
        final Topology topology = getStatefulTopology(inputTopic, outputTopic, globalTopicName, storeName, globalStoreName, false);
        startStreamsAndCheckDirExists(topology, asList(inputTopic, globalTopicName), outputTopic, false);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		10
-    
    public void statefulTopologyShouldCreateStateDirectory() throws Exception {
        final String inputTopic = testName.getMethodName() + "-input";
        final String outputTopic = testName.getMethodName() + "-output";
        final String globalTopicName = testName.getMethodName() + "-global";
        final String storeName = testName.getMethodName() + "-counts";
        final String globalStoreName = testName.getMethodName() + "-globalStore";
        final Topology topology = getStatefulTopology(inputTopic, outputTopic, globalTopicName, storeName, globalStoreName, true);
        startStreamsAndCheckDirExists(topology, asList(inputTopic, globalTopicName), outputTopic, true);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		13
-    
    public void shouldPutAndFetch() {
        final String key = "a";
        bytesStore.put(serializeKey(new Windowed<>(key, windows[0])), serializeValue(10));
        bytesStore.put(serializeKey(new Windowed<>(key, windows[1])), serializeValue(50));
        bytesStore.put(serializeKey(new Windowed<>(key, windows[2])), serializeValue(100));

        final KeyValueIterator<Bytes, byte[]> values = bytesStore.fetch(Bytes.wrap(key.getBytes()), 0, 500);

        final List<KeyValue<Windowed<String>, Long>> expected = Arrays.asList(
            KeyValue.pair(new Windowed<>(key, windows[0]), 10L),
            KeyValue.pair(new Windowed<>(key, windows[1]), 50L)
        );

        assertEquals(expected, toList(values));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		13
-    
    public void shouldFindValuesWithinRange() {
        final String key = "a";
        bytesStore.put(serializeKey(new Windowed<>(key, windows[0])), serializeValue(10));
        bytesStore.put(serializeKey(new Windowed<>(key, windows[1])), serializeValue(50));
        bytesStore.put(serializeKey(new Windowed<>(key, windows[2])), serializeValue(100));
        final KeyValueIterator<Bytes, byte[]> results = bytesStore.fetch(Bytes.wrap(key.getBytes()), 1, 999);
        final List<KeyValue<Windowed<String>, Long>> expected = Arrays.asList(
            KeyValue.pair(new Windowed<>(key, windows[0]), 10L),
            KeyValue.pair(new Windowed<>(key, windows[1]), 50L)
        );

        assertEquals(expected, toList(results));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		8
-    
    public void shouldRemove() {
        bytesStore.put(serializeKey(new Windowed<>("a", windows[0])), serializeValue(30));
        bytesStore.put(serializeKey(new Windowed<>("a", windows[1])), serializeValue(50));

        bytesStore.remove(serializeKey(new Windowed<>("a", windows[0])));
        final KeyValueIterator<Bytes, byte[]> value = bytesStore.fetch(Bytes.wrap("a".getBytes()), 0, 100);
        assertFalse(value.hasNext());
    }
```
```
Cyclomatic Complexity	 1
Assertions		 3
Lines of Code		20
-    
    public void shouldRollSegments() {
        // just to validate directories
        final KeyValueSegments segments = new KeyValueSegments(storeName, retention, segmentInterval);
        final String key = "a";

        bytesStore.put(serializeKey(new Windowed<>(key, windows[0])), serializeValue(50));
        bytesStore.put(serializeKey(new Windowed<>(key, windows[1])), serializeValue(100));
        bytesStore.put(serializeKey(new Windowed<>(key, windows[2])), serializeValue(500));
        assertEquals(Collections.singleton(segments.segmentName(0)), segmentDirs());

        bytesStore.put(serializeKey(new Windowed<>(key, windows[3])), serializeValue(1000));
        assertEquals(Utils.mkSet(segments.segmentName(0), segments.segmentName(1)), segmentDirs());

        final List<KeyValue<Windowed<String>, Long>> results = toList(bytesStore.fetch(Bytes.wrap(key.getBytes()), 0, 1500));

        assertEquals(
            Arrays.asList(
                KeyValue.pair(new Windowed<>(key, windows[0]), 50L),
                KeyValue.pair(new Windowed<>(key, windows[1]), 100L),
                KeyValue.pair(new Windowed<>(key, windows[2]), 500L)
            ),
            results
        );

    }
```
```
Cyclomatic Complexity	 1
Assertions		 3
Lines of Code		23
-    
    public void shouldGetAllSegments() {
        // just to validate directories
        final KeyValueSegments segments = new KeyValueSegments(storeName, retention, segmentInterval);
        final String key = "a";

        bytesStore.put(serializeKey(new Windowed<>(key, windows[0])), serializeValue(50L));
        assertEquals(Collections.singleton(segments.segmentName(0)), segmentDirs());

        bytesStore.put(serializeKey(new Windowed<>(key, windows[3])), serializeValue(100L));
        assertEquals(
            Utils.mkSet(
                segments.segmentName(0),
                segments.segmentName(1)
            ),
            segmentDirs()
        );

        final List<KeyValue<Windowed<String>, Long>> results = toList(bytesStore.all());
        assertEquals(
            Arrays.asList(
                KeyValue.pair(new Windowed<>(key, windows[0]), 50L),
                KeyValue.pair(new Windowed<>(key, windows[3]), 100L)
            ),
            results
        );

    }
```
```
Cyclomatic Complexity	 1
Assertions		 3
Lines of Code		23
-    
    public void shouldFetchAllSegments() {
        // just to validate directories
        final KeyValueSegments segments = new KeyValueSegments(storeName, retention, segmentInterval);
        final String key = "a";

        bytesStore.put(serializeKey(new Windowed<>(key, windows[0])), serializeValue(50L));
        assertEquals(Collections.singleton(segments.segmentName(0)), segmentDirs());

        bytesStore.put(serializeKey(new Windowed<>(key, windows[3])), serializeValue(100L));
        assertEquals(
            Utils.mkSet(
                segments.segmentName(0),
                segments.segmentName(1)
            ),
            segmentDirs()
        );

        final List<KeyValue<Windowed<String>, Long>> results = toList(bytesStore.fetchAll(0L, 60_000L));
        assertEquals(
            Arrays.asList(
                KeyValue.pair(new Windowed<>(key, windows[0]), 50L),
                KeyValue.pair(new Windowed<>(key, windows[3]), 100L)
            ),
            results
        );

    }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		35
-    
    public void shouldLoadSegmentsWithOldStyleDateFormattedName() {
        final KeyValueSegments segments = new KeyValueSegments(storeName, retention, segmentInterval);
        final String key = "a";

        bytesStore.put(serializeKey(new Windowed<>(key, windows[0])), serializeValue(50L));
        bytesStore.put(serializeKey(new Windowed<>(key, windows[3])), serializeValue(100L));
        bytesStore.close();

        final String firstSegmentName = segments.segmentName(0);
        final String[] nameParts = firstSegmentName.split("\\.");
        final Long segmentId = Long.parseLong(nameParts[1]);
        final SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmm");
        formatter.setTimeZone(new SimpleTimeZone(0, "UTC"));
        final String formatted = formatter.format(new Date(segmentId * segmentInterval));
        final File parent = new File(stateDir, storeName);
        final File oldStyleName = new File(parent, nameParts[0] + "-" + formatted);
        assertTrue(new File(parent, firstSegmentName).renameTo(oldStyleName));

        bytesStore = new RocksDBSegmentedBytesStore(
            storeName,
            "metrics-scope",
            retention,
            segmentInterval,
            schema
        );

        bytesStore.init(context, bytesStore);
        final List<KeyValue<Windowed<String>, Long>> results = toList(bytesStore.fetch(Bytes.wrap(key.getBytes()), 0L, 60_000L));
        assertThat(
            results,
            equalTo(
                Arrays.asList(
                    KeyValue.pair(new Windowed<>(key, windows[0]), 50L),
                    KeyValue.pair(new Windowed<>(key, windows[3]), 100L)
                )
            )
        );
    }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		31
-    
    public void shouldLoadSegmentsWithOldStyleColonFormattedName() {
        final KeyValueSegments segments = new KeyValueSegments(storeName, retention, segmentInterval);
        final String key = "a";

        bytesStore.put(serializeKey(new Windowed<>(key, windows[0])), serializeValue(50L));
        bytesStore.put(serializeKey(new Windowed<>(key, windows[3])), serializeValue(100L));
        bytesStore.close();

        final String firstSegmentName = segments.segmentName(0);
        final String[] nameParts = firstSegmentName.split("\\.");
        final File parent = new File(stateDir, storeName);
        final File oldStyleName = new File(parent, nameParts[0] + ":" + Long.parseLong(nameParts[1]));
        assertTrue(new File(parent, firstSegmentName).renameTo(oldStyleName));

        bytesStore = new RocksDBSegmentedBytesStore(
            storeName,
            "metrics-scope",
            retention,
            segmentInterval,
            schema
        );

        bytesStore.init(context, bytesStore);
        final List<KeyValue<Windowed<String>, Long>> results = toList(bytesStore.fetch(Bytes.wrap(key.getBytes()), 0L, 60_000L));
        assertThat(
            results,
            equalTo(
                Arrays.asList(
                    KeyValue.pair(new Windowed<>(key, windows[0]), 50L),
                    KeyValue.pair(new Windowed<>(key, windows[3]), 100L)
                )
            )
        );
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		8
-    
    public void shouldBeAbleToWriteToReInitializedStore() {
        final String key = "a";
        // need to create a segment so we can attempt to write to it again.
        bytesStore.put(serializeKey(new Windowed<>(key, windows[0])), serializeValue(50));
        bytesStore.close();
        bytesStore.init(context, bytesStore);
        bytesStore.put(serializeKey(new Windowed<>(key, windows[1])), serializeValue(100));
    }
```
```
Cyclomatic Complexity	 2
Assertions		 2
Lines of Code		12
-    
    public void shouldCreateWriteBatches() {
        final String key = "a";
        final Collection<KeyValue<byte[], byte[]>> records = new ArrayList<>();
        records.add(new KeyValue<>(serializeKey(new Windowed<>(key, windows[0])).get(), serializeValue(50L)));
        records.add(new KeyValue<>(serializeKey(new Windowed<>(key, windows[3])).get(), serializeValue(100L)));
        final Map<KeyValueSegment, WriteBatch> writeBatchMap = bytesStore.getWriteBatches(records);
        assertEquals(2, writeBatchMap.size());
        for (final WriteBatch batch : writeBatchMap.values()) {
            assertEquals(1, batch.count());
        }
    }
```
```
Cyclomatic Complexity	 2
Assertions		 4
Lines of Code		18
-    
    public void shouldRestoreToByteStore() {
        // 0 segments initially.
        assertEquals(0, bytesStore.getSegments().size());
        final String key = "a";
        final Collection<KeyValue<byte[], byte[]>> records = new ArrayList<>();
        records.add(new KeyValue<>(serializeKey(new Windowed<>(key, windows[0])).get(), serializeValue(50L)));
        records.add(new KeyValue<>(serializeKey(new Windowed<>(key, windows[3])).get(), serializeValue(100L)));
        bytesStore.restoreAllInternal(records);

        // 2 segments are created during restoration.
        assertEquals(2, bytesStore.getSegments().size());

        // Bulk loading is enabled during recovery.
        for (final KeyValueSegment segment : bytesStore.getSegments()) {
            Assert.assertThat(segment.getOptions().level0FileNumCompactionTrigger(), equalTo(1 << 30));
        }

        final List<KeyValue<Windowed<String>, Long>> expected = new ArrayList<>();
        expected.add(new KeyValue<>(new Windowed<>(key, windows[0]), 50L));
        expected.add(new KeyValue<>(new Windowed<>(key, windows[3]), 100L));

        final List<KeyValue<Windowed<String>, Long>> results = toList(bytesStore.all());
        assertEquals(expected, results);
    }
```
```
Cyclomatic Complexity	 3
Assertions		 3
Lines of Code		17
-    
    public void shouldRespectBulkLoadOptionsDuringInit() {
        bytesStore.init(context, bytesStore);
        final String key = "a";
        bytesStore.put(serializeKey(new Windowed<>(key, windows[0])), serializeValue(50L));
        bytesStore.put(serializeKey(new Windowed<>(key, windows[3])), serializeValue(100L));
        assertEquals(2, bytesStore.getSegments().size());

        final StateRestoreListener restoreListener = context.getRestoreListener(bytesStore.name());

        restoreListener.onRestoreStart(null, bytesStore.name(), 0L, 0L);

        for (final KeyValueSegment segment : bytesStore.getSegments()) {
            Assert.assertThat(segment.getOptions().level0FileNumCompactionTrigger(), equalTo(1 << 30));
        }

        restoreListener.onRestoreEnd(null, bytesStore.name(), 0L);
        for (final KeyValueSegment segment : bytesStore.getSegments()) {
            Assert.assertThat(segment.getOptions().level0FileNumCompactionTrigger(), equalTo(4));
        }
    }
```
```
Cyclomatic Complexity	 1
Assertions		 3
Lines of Code		35
-    
    public void shouldLogAndMeasureExpiredRecords() {
        LogCaptureAppender.setClassLoggerToDebug(RocksDBSegmentedBytesStore.class);
        final LogCaptureAppender appender = LogCaptureAppender.createAndRegister();

        // write a record to advance stream time, with a high enough timestamp
        // that the subsequent record in windows[0] will already be expired.
        bytesStore.put(serializeKey(new Windowed<>("dummy", nextSegmentWindow)), serializeValue(0));

        final Bytes key = serializeKey(new Windowed<>("a", windows[0]));
        final byte[] value = serializeValue(5);
        bytesStore.put(key, value);

        LogCaptureAppender.unregister(appender);

        final Map<MetricName, ? extends Metric> metrics = context.metrics().metrics();

        final Metric dropTotal = metrics.get(new MetricName(
            "expired-window-record-drop-total",
            "stream-metrics-scope-metrics",
            "The total number of occurrence of expired-window-record-drop operations.",
            mkMap(
                mkEntry("client-id", "mock"),
                mkEntry("task-id", "0_0"),
                mkEntry("metrics-scope-id", "bytes-store")
            )
        ));

        final Metric dropRate = metrics.get(new MetricName(
            "expired-window-record-drop-rate",
            "stream-metrics-scope-metrics",
            "The average number of occurrence of expired-window-record-drop operation per second.",
            mkMap(
                mkEntry("client-id", "mock"),
                mkEntry("task-id", "0_0"),
                mkEntry("metrics-scope-id", "bytes-store")
            )
        ));

        assertEquals(1.0, dropTotal.metricValue());
        assertNotEquals(0.0, dropRate.metricValue());
        final List<String> messages = appender.getMessages();
        assertThat(messages, hasItem("Skipping record for expired segment."));
    }
```
```
Cyclomatic Complexity	 2
Assertions		 1
Lines of Code		15
-    
    public void shouldFetchAllSessionsWithSameRecordKey() {
        final List<KeyValue<Windowed<String>, Long>> expected = Arrays.asList(
            KeyValue.pair(new Windowed<>("a", new SessionWindow(0, 0)), 1L),
            KeyValue.pair(new Windowed<>("a", new SessionWindow(10, 10)), 2L),
            KeyValue.pair(new Windowed<>("a", new SessionWindow(100, 100)), 3L),
            KeyValue.pair(new Windowed<>("a", new SessionWindow(1000, 1000)), 4L));

        for (final KeyValue<Windowed<String>, Long> kv : expected) {
            sessionStore.put(kv.key, kv.value);
        }

        // add one that shouldn't appear in the results
        sessionStore.put(new Windowed<>("aa", new SessionWindow(0, 0)), 5L);

        try (final KeyValueIterator<Windowed<String>, Long> values = sessionStore.fetch("a")) {
            assertEquals(expected, toList(values));
        }
    }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		13
-    
    public void shouldFindValuesWithinMergingSessionWindowRange() {
        final String key = "a";
        sessionStore.put(new Windowed<>(key, new SessionWindow(0L, 0L)), 1L);
        sessionStore.put(new Windowed<>(key, new SessionWindow(1000L, 1000L)), 2L);

        final List<KeyValue<Windowed<String>, Long>> expected = Arrays.asList(
            KeyValue.pair(new Windowed<>(key, new SessionWindow(0L, 0L)), 1L),
            KeyValue.pair(new Windowed<>(key, new SessionWindow(1000L, 1000L)), 2L));

        try (final KeyValueIterator<Windowed<String>, Long> results =
                 sessionStore.findSessions(key, -1, 1000L)) {
            assertEquals(expected, toList(results));
        }
    }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		14
-    
    public void shouldRemove() {
        sessionStore.put(new Windowed<>("a", new SessionWindow(0, 1000)), 1L);
        sessionStore.put(new Windowed<>("a", new SessionWindow(1500, 2500)), 2L);

        sessionStore.remove(new Windowed<>("a", new SessionWindow(0, 1000)));

        try (final KeyValueIterator<Windowed<String>, Long> results =
                 sessionStore.findSessions("a", 0L, 1000L)) {
            assertFalse(results.hasNext());
        }

        try (final KeyValueIterator<Windowed<String>, Long> results =
                 sessionStore.findSessions("a", 1500L, 2500L)) {
            assertTrue(results.hasNext());
        }
    }
```
```
Cyclomatic Complexity	 1
Assertions		 3
Lines of Code		20
-    
    public void shouldFindSessionsToMerge() {
        final Windowed<String> session1 = new Windowed<>("a", new SessionWindow(0, 100));
        final Windowed<String> session2 = new Windowed<>("a", new SessionWindow(101, 200));
        final Windowed<String> session3 = new Windowed<>("a", new SessionWindow(201, 300));
        final Windowed<String> session4 = new Windowed<>("a", new SessionWindow(301, 400));
        final Windowed<String> session5 = new Windowed<>("a", new SessionWindow(401, 500));
        sessionStore.put(session1, 1L);
        sessionStore.put(session2, 2L);
        sessionStore.put(session3, 3L);
        sessionStore.put(session4, 4L);
        sessionStore.put(session5, 5L);

        try (final KeyValueIterator<Windowed<String>, Long> results =
                 sessionStore.findSessions("a", 150, 300)
        ) {
            assertEquals(session2, results.next().key);
            assertEquals(session3, results.next().key);
            assertFalse(results.hasNext());
        }
    }
```
## 78ac5a4003851b6910c44143ab3a8342f6ab31fb ##
```
Cyclomatic Complexity	1
Assertions		0
Lines of Code		4
-    
    public void testAssignWithTopicUnavailable() {
        unavailableTopicTest(true, false, Collections.<String>emptySet());
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		5
-  (expected = classOf[AuthorizationException])
  def testCommitWithNoAccess() {
    val consumer = createConsumer()
    consumer.commitSync(Map(tp -> new OffsetAndMetadata(5)).asJava)
  }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		9
-  
  def testDeleteCmdWithShortInitialization() {
    // run one consumer in the group
    addConsumerGroupExecutor(numConsumers = 1)
    val cgcArgs = Array("--bootstrap-server", brokerList, "--delete", "--group", group)
    val service = getConsumerGroupService(cgcArgs)

    val output = TestUtils.grabConsoleOutput(service.deleteGroups())
    assertTrue(s"The consumer group deletion did not work as expected",
      output.contains(s"Group '$group' could not be deleted due to"))
  }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		9
-  
  def testDeleteWithShortInitialization() {
    // run one consumer in the group
    addConsumerGroupExecutor(numConsumers = 1)
    val cgcArgs = Array("--bootstrap-server", brokerList, "--delete", "--group", group)
    val service = getConsumerGroupService(cgcArgs)

    val result = service.deleteGroups()
    assertTrue(s"The consumer group deletion did not work as expected",
      result.size == 1 && result.keySet.contains(group))
  }
```
```
Cyclomatic Complexity	 3
Assertions		 0
Lines of Code		21
-  
  def testDateTimeFormats() {
    //check valid formats
    invokeGetDateTimeMethod(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS"))
    invokeGetDateTimeMethod(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ"))
    invokeGetDateTimeMethod(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX"))
    invokeGetDateTimeMethod(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXX"))
    invokeGetDateTimeMethod(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX"))@@@ 
    //check some invalid formats
    try {
      invokeGetDateTimeMethod(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss"))
      fail("Call to getDateTime should fail")
    } catch {
      case _: ParseException =>
    }

    try {
      invokeGetDateTimeMethod(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.X"))
      fail("Call to getDateTime should fail")
    } catch {
      case _: ParseException =>
    }
  }
```
```
Cyclomatic Complexity	 1
Assertions		 12
Lines of Code		27
-  
  def testTruncateHead(): Unit = {
    val epoch = 0.toShort

    append(stateManager, producerId, epoch, 0, 0L)
    append(stateManager, producerId, epoch, 1, 1L)
    stateManager.takeSnapshot()

    val anotherPid = 2L
    append(stateManager, anotherPid, epoch, 0, 2L)
    append(stateManager, anotherPid, epoch, 1, 3L)
    stateManager.takeSnapshot()
    assertEquals(Set(2, 4), currentSnapshotOffsets)

    stateManager.truncateHead(2)
    assertEquals(Set(2, 4), currentSnapshotOffsets)
    assertEquals(Set(anotherPid), stateManager.activeProducers.keySet)
    assertEquals(None, stateManager.lastEntry(producerId))

    val maybeEntry = stateManager.lastEntry(anotherPid)
    assertTrue(maybeEntry.isDefined)
    assertEquals(3L, maybeEntry.get.lastDataOffset)

    stateManager.truncateHead(3)
    assertEquals(Set(anotherPid), stateManager.activeProducers.keySet)
    assertEquals(Set(4), currentSnapshotOffsets)
    assertEquals(4, stateManager.mapEndOffset)

    stateManager.truncateHead(5)
    assertEquals(Set(), stateManager.activeProducers.keySet)
    assertEquals(Set(), currentSnapshotOffsets)
    assertEquals(5, stateManager.mapEndOffset)
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		25
-    
    public void testValueGetter() {
        final StreamsBuilder builder = new StreamsBuilder();

        final String topic1 = "topic1";
        final String topic2 = "topic2";

        final KTableImpl<String, String, String> table1 =
                (KTableImpl<String, String, String>) builder.table(topic1, consumed);
        final KTableImpl<String, String, Integer> table2 = (KTableImpl<String, String, Integer>) table1.mapValues(
                new ValueMapper<String, Integer>() {
                    @Override
                    public Integer apply(final String value) {
                        return new Integer(value);
                    }
                });
        final KTableImpl<String, Integer, Integer> table3 = (KTableImpl<String, Integer, Integer>) table2.filter(
                new Predicate<String, Integer>() {
                    @Override
                    public boolean test(final String key, final Integer value) {
                        return (value % 2) == 0;
                    }
                });
        table1.toStream().to(topic2, produced);
        final KTableImpl<String, String, String> table4 = (KTableImpl<String, String, String>) builder.table(topic2, consumed);

        doTestValueGetter(builder, topic1, table1, table2, table3, table4);@@@     }
```
```
Cyclomatic Complexity	 2
Assertions		 6
Lines of Code		48
-    
    public void shouldRestoreToKTable() throws IOException {
        consumer.assign(mkList(globalTopicPartition));
        consumer.commitSync(mkMap(mkEntry(globalTopicPartition, new OffsetAndMetadata(0L))));

        final StandbyTask task = new StandbyTask(@@@+        return new StandbyTask(@@@             taskId,
            ktablePartitions,
            ktableTopology,
            consumer,
            changelogReader,
            createConfig(baseDir),
            null,
            stateDirectory
        );
        task.initializeStateStores();

        // The commit offset is at 0L. Records should not be processed
        List<ConsumerRecord<byte[], byte[]>> remaining = task.update(
            globalTopicPartition,
            Arrays.asList(
                makeConsumerRecord(globalTopicPartition, 10, 1),
                makeConsumerRecord(globalTopicPartition, 20, 2),
                makeConsumerRecord(globalTopicPartition, 30, 3),
                makeConsumerRecord(globalTopicPartition, 40, 4),
                makeConsumerRecord(globalTopicPartition, 50, 5)
            )
        );
        assertEquals(5, remaining.size());

        consumer.commitSync(mkMap(mkEntry(globalTopicPartition, new OffsetAndMetadata(10L))));
        task.commit(); // update offset limits

        // The commit offset has not reached, yet.
        remaining = task.update(globalTopicPartition, remaining);
        assertEquals(5, remaining.size());

        consumer.commitSync(mkMap(mkEntry(globalTopicPartition, new OffsetAndMetadata(11L))));
        task.commit(); // update offset limits

        // one record should be processed.
        remaining = task.update(globalTopicPartition, remaining);
        assertEquals(4, remaining.size());

        consumer.commitSync(mkMap(mkEntry(globalTopicPartition, new OffsetAndMetadata(45L))));
        task.commit(); // update offset limits

        // The commit offset is now 45. All record except for the last one should be processed.
        remaining = task.update(globalTopicPartition, remaining);
        assertEquals(1, remaining.size());

        consumer.commitSync(mkMap(mkEntry(globalTopicPartition, new OffsetAndMetadata(50L))));
        task.commit(); // update offset limits

        // The commit offset is now 50. Still the last record remains.
        remaining = task.update(globalTopicPartition, remaining);
        assertEquals(1, remaining.size());

        consumer.commitSync(mkMap(mkEntry(globalTopicPartition, new OffsetAndMetadata(60L))));
        task.commit(); // update offset limits

        // The commit offset is now 60. No record should be left.
        remaining = task.update(globalTopicPartition, remaining);
        assertEquals(emptyList(), remaining);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		6
-    
    public void shouldInitializeStateStoreWithoutException() throws IOException {
        final InternalStreamsBuilder builder = new InternalStreamsBuilder(new InternalTopologyBuilder());
        builder.stream(Collections.singleton("topic"), new ConsumedInternal<>()).groupByKey().count();

        initializeStandbyStores(builder);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		6
-    
    public void shouldInitializeWindowStoreWithoutException() throws IOException {
        final InternalStreamsBuilder builder = new InternalStreamsBuilder(new InternalTopologyBuilder());
        builder.stream(Collections.singleton("topic"), new ConsumedInternal<>()).groupByKey().windowedBy(TimeWindows.of(ofMillis(100))).count();

        initializeStandbyStores(builder);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		11
-    
    public void shouldThrowKafkaExceptionAssignmentErrorCodeNotConfigured() {
        final Map<String, Object> config = configProps();
        config.remove(StreamsConfig.InternalConfig.ASSIGNMENT_ERROR_CODE);

        try {
            partitionAssignor.configure(config);
            fail("Should have thrown KafkaException");
        } catch (final KafkaException expected) {
            assertThat(expected.getMessage(), equalTo("assignmentErrorCode is not specified"));
        }
    }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		13
-    
    public void shouldPutAndFetch() {
        final String key = "a";
        bytesStore.put(serializeKey(new Windowed<>(key, windows[0])), serializeValue(10));
        bytesStore.put(serializeKey(new Windowed<>(key, windows[1])), serializeValue(50));
        bytesStore.put(serializeKey(new Windowed<>(key, windows[2])), serializeValue(100));

        final KeyValueIterator<Bytes, byte[]> values = bytesStore.fetch(Bytes.wrap(key.getBytes()), 0, 500);

        final List<KeyValue<Windowed<String>, Long>> expected = Arrays.asList(
            KeyValue.pair(new Windowed<>(key, windows[0]), 10L),
            KeyValue.pair(new Windowed<>(key, windows[1]), 50L)
        );

        assertEquals(expected, toList(values));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		13
-    
    public void shouldFindValuesWithinRange() {
        final String key = "a";
        bytesStore.put(serializeKey(new Windowed<>(key, windows[0])), serializeValue(10));
        bytesStore.put(serializeKey(new Windowed<>(key, windows[1])), serializeValue(50));
        bytesStore.put(serializeKey(new Windowed<>(key, windows[2])), serializeValue(100));
        final KeyValueIterator<Bytes, byte[]> results = bytesStore.fetch(Bytes.wrap(key.getBytes()), 1, 999);
        final List<KeyValue<Windowed<String>, Long>> expected = Arrays.asList(
            KeyValue.pair(new Windowed<>(key, windows[0]), 10L),
            KeyValue.pair(new Windowed<>(key, windows[1]), 50L)
        );

        assertEquals(expected, toList(results));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		8
-    
    public void shouldRemove() {
        bytesStore.put(serializeKey(new Windowed<>("a", windows[0])), serializeValue(30));
        bytesStore.put(serializeKey(new Windowed<>("a", windows[1])), serializeValue(50));

        bytesStore.remove(serializeKey(new Windowed<>("a", windows[0])));
        final KeyValueIterator<Bytes, byte[]> value = bytesStore.fetch(Bytes.wrap("a".getBytes()), 0, 100);
        assertFalse(value.hasNext());
    }
```
```
Cyclomatic Complexity	 1
Assertions		 3
Lines of Code		20
-    
    public void shouldRollSegments() {
        // just to validate directories
        final Segments segments = new Segments(storeName, retention, segmentInterval);
        final String key = "a";

        bytesStore.put(serializeKey(new Windowed<>(key, windows[0])), serializeValue(50));
        bytesStore.put(serializeKey(new Windowed<>(key, windows[1])), serializeValue(100));
        bytesStore.put(serializeKey(new Windowed<>(key, windows[2])), serializeValue(500));
        assertEquals(Collections.singleton(segments.segmentName(0)), segmentDirs());

        bytesStore.put(serializeKey(new Windowed<>(key, windows[3])), serializeValue(1000));
        assertEquals(Utils.mkSet(segments.segmentName(0), segments.segmentName(1)), segmentDirs());

        final List<KeyValue<Windowed<String>, Long>> results = toList(bytesStore.fetch(Bytes.wrap(key.getBytes()), 0, 1500));

        assertEquals(
            Arrays.asList(
                KeyValue.pair(new Windowed<>(key, windows[0]), 50L),
                KeyValue.pair(new Windowed<>(key, windows[1]), 100L),
                KeyValue.pair(new Windowed<>(key, windows[2]), 500L)
            ),
            results
        );

    }
```
```
Cyclomatic Complexity	 1
Assertions		 3
Lines of Code		23
-    
    public void shouldGetAllSegments() {
        // just to validate directories
        final Segments segments = new Segments(storeName, retention, segmentInterval);
        final String key = "a";

        bytesStore.put(serializeKey(new Windowed<>(key, windows[0])), serializeValue(50L));
        assertEquals(Collections.singleton(segments.segmentName(0)), segmentDirs());

        bytesStore.put(serializeKey(new Windowed<>(key, windows[3])), serializeValue(100L));
        assertEquals(
            Utils.mkSet(
                segments.segmentName(0),
                segments.segmentName(1)
            ),
            segmentDirs()
        );

        final List<KeyValue<Windowed<String>, Long>> results = toList(bytesStore.all());
        assertEquals(
            Arrays.asList(
                KeyValue.pair(new Windowed<>(key, windows[0]), 50L),
                KeyValue.pair(new Windowed<>(key, windows[3]), 100L)
            ),
            results
        );

    }
```
```
Cyclomatic Complexity	 1
Assertions		 3
Lines of Code		23
-    
    public void shouldFetchAllSegments() {
        // just to validate directories
        final Segments segments = new Segments(storeName, retention, segmentInterval);
        final String key = "a";

        bytesStore.put(serializeKey(new Windowed<>(key, windows[0])), serializeValue(50L));
        assertEquals(Collections.singleton(segments.segmentName(0)), segmentDirs());

        bytesStore.put(serializeKey(new Windowed<>(key, windows[3])), serializeValue(100L));
        assertEquals(
            Utils.mkSet(
                segments.segmentName(0),
                segments.segmentName(1)
            ),
            segmentDirs()
        );

        final List<KeyValue<Windowed<String>, Long>> results = toList(bytesStore.fetchAll(0L, 60_000L));
        assertEquals(
            Arrays.asList(
                KeyValue.pair(new Windowed<>(key, windows[0]), 50L),
                KeyValue.pair(new Windowed<>(key, windows[3]), 100L)
            ),
            results
        );

    }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		35
-    
    public void shouldLoadSegmentsWithOldStyleDateFormattedName() {
        final Segments segments = new Segments(storeName, retention, segmentInterval);
        final String key = "a";

        bytesStore.put(serializeKey(new Windowed<>(key, windows[0])), serializeValue(50L));
        bytesStore.put(serializeKey(new Windowed<>(key, windows[3])), serializeValue(100L));
        bytesStore.close();

        final String firstSegmentName = segments.segmentName(0);
        final String[] nameParts = firstSegmentName.split("\\.");
        final Long segmentId = Long.parseLong(nameParts[1]);
        final SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmm");
        formatter.setTimeZone(new SimpleTimeZone(0, "UTC"));
        final String formatted = formatter.format(new Date(segmentId * segmentInterval));
        final File parent = new File(stateDir, storeName);
        final File oldStyleName = new File(parent, nameParts[0] + "-" + formatted);
        assertTrue(new File(parent, firstSegmentName).renameTo(oldStyleName));

        bytesStore = new RocksDBSegmentedBytesStore(
            storeName,
            "metrics-scope",
            retention,
            segmentInterval,
            schema
        );

        bytesStore.init(context, bytesStore);
        final List<KeyValue<Windowed<String>, Long>> results = toList(bytesStore.fetch(Bytes.wrap(key.getBytes()), 0L, 60_000L));
        assertThat(
            results,
            equalTo(
                Arrays.asList(
                    KeyValue.pair(new Windowed<>(key, windows[0]), 50L),
                    KeyValue.pair(new Windowed<>(key, windows[3]), 100L)
                )
            )
        );
    }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		31
-    
    public void shouldLoadSegmentsWithOldStyleColonFormattedName() {
        final Segments segments = new Segments(storeName, retention, segmentInterval);
        final String key = "a";

        bytesStore.put(serializeKey(new Windowed<>(key, windows[0])), serializeValue(50L));
        bytesStore.put(serializeKey(new Windowed<>(key, windows[3])), serializeValue(100L));
        bytesStore.close();

        final String firstSegmentName = segments.segmentName(0);
        final String[] nameParts = firstSegmentName.split("\\.");
        final File parent = new File(stateDir, storeName);
        final File oldStyleName = new File(parent, nameParts[0] + ":" + Long.parseLong(nameParts[1]));
        assertTrue(new File(parent, firstSegmentName).renameTo(oldStyleName));

        bytesStore = new RocksDBSegmentedBytesStore(
            storeName,
            "metrics-scope",
            retention,
            segmentInterval,
            schema
        );

        bytesStore.init(context, bytesStore);
        final List<KeyValue<Windowed<String>, Long>> results = toList(bytesStore.fetch(Bytes.wrap(key.getBytes()), 0L, 60_000L));
        assertThat(
            results,
            equalTo(
                Arrays.asList(
                    KeyValue.pair(new Windowed<>(key, windows[0]), 50L),
                    KeyValue.pair(new Windowed<>(key, windows[3]), 100L)
                )
            )
        );
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		8
-    
    public void shouldBeAbleToWriteToReInitializedStore() {
        final String key = "a";
        // need to create a segment so we can attempt to write to it again.
        bytesStore.put(serializeKey(new Windowed<>(key, windows[0])), serializeValue(50));
        bytesStore.close();
        bytesStore.init(context, bytesStore);
        bytesStore.put(serializeKey(new Windowed<>(key, windows[1])), serializeValue(100));
    }
```
```
Cyclomatic Complexity	 2
Assertions		 2
Lines of Code		12
-    
    public void shouldCreateWriteBatches() {
        final String key = "a";
        final Collection<KeyValue<byte[], byte[]>> records = new ArrayList<>();
        records.add(new KeyValue<>(serializeKey(new Windowed<>(key, windows[0])).get(), serializeValue(50L)));
        records.add(new KeyValue<>(serializeKey(new Windowed<>(key, windows[3])).get(), serializeValue(100L)));
        final Map<Segment, WriteBatch> writeBatchMap = bytesStore.getWriteBatches(records);
        assertEquals(2, writeBatchMap.size());
        for (final WriteBatch batch : writeBatchMap.values()) {
            assertEquals(1, batch.count());
        }
    }
```
```
Cyclomatic Complexity	 2
Assertions		 4
Lines of Code		18
-    
    public void shouldRestoreToByteStore() {
        // 0 segments initially.
        assertEquals(0, bytesStore.getSegments().size());
        final String key = "a";
        final Collection<KeyValue<byte[], byte[]>> records = new ArrayList<>();
        records.add(new KeyValue<>(serializeKey(new Windowed<>(key, windows[0])).get(), serializeValue(50L)));
        records.add(new KeyValue<>(serializeKey(new Windowed<>(key, windows[3])).get(), serializeValue(100L)));
        bytesStore.restoreAllInternal(records);

        // 2 segments are created during restoration.
        assertEquals(2, bytesStore.getSegments().size());

        // Bulk loading is enabled during recovery.
        for (final Segment segment : bytesStore.getSegments()) {
            Assert.assertThat(segment.getOptions().level0FileNumCompactionTrigger(), equalTo(1 << 30));
        }

        final List<KeyValue<Windowed<String>, Long>> expected = new ArrayList<>();
        expected.add(new KeyValue<>(new Windowed<>(key, windows[0]), 50L));
        expected.add(new KeyValue<>(new Windowed<>(key, windows[3]), 100L));

        final List<KeyValue<Windowed<String>, Long>> results = toList(bytesStore.all());
        assertEquals(expected, results);
    }
```
```
Cyclomatic Complexity	 3
Assertions		 3
Lines of Code		17
-    
    public void shouldRespectBulkLoadOptionsDuringInit() {
        bytesStore.init(context, bytesStore);
        final String key = "a";
        bytesStore.put(serializeKey(new Windowed<>(key, windows[0])), serializeValue(50L));
        bytesStore.put(serializeKey(new Windowed<>(key, windows[3])), serializeValue(100L));
        assertEquals(2, bytesStore.getSegments().size());

        final StateRestoreListener restoreListener = context.getRestoreListener(bytesStore.name());

        restoreListener.onRestoreStart(null, bytesStore.name(), 0L, 0L);

        for (final Segment segment : bytesStore.getSegments()) {
            Assert.assertThat(segment.getOptions().level0FileNumCompactionTrigger(), equalTo(1 << 30));
        }

        restoreListener.onRestoreEnd(null, bytesStore.name(), 0L);
        for (final Segment segment : bytesStore.getSegments()) {
            Assert.assertThat(segment.getOptions().level0FileNumCompactionTrigger(), equalTo(4));
        }
    }
```
```
Cyclomatic Complexity	 1
Assertions		 3
Lines of Code		35
-    
    public void shouldLogAndMeasureExpiredRecords() {
        LogCaptureAppender.setClassLoggerToDebug(RocksDBSegmentedBytesStore.class);
        final LogCaptureAppender appender = LogCaptureAppender.createAndRegister();

        // write a record to advance stream time, with a high enough timestamp
        // that the subsequent record in windows[0] will already be expired.
        bytesStore.put(serializeKey(new Windowed<>("dummy", nextSegmentWindow)), serializeValue(0));

        final Bytes key = serializeKey(new Windowed<>("a", windows[0]));
        final byte[] value = serializeValue(5);
        bytesStore.put(key, value);

        LogCaptureAppender.unregister(appender);

        final Map<MetricName, ? extends Metric> metrics = context.metrics().metrics();

        final Metric dropTotal = metrics.get(new MetricName(
            "expired-window-record-drop-total",
            "stream-metrics-scope-metrics",
            "The total number of occurrence of expired-window-record-drop operations.",
            mkMap(
                mkEntry("client-id", "mock"),
                mkEntry("task-id", "0_0"),
                mkEntry("metrics-scope-id", "bytes-store")
            )
        ));

        final Metric dropRate = metrics.get(new MetricName(
            "expired-window-record-drop-rate",
            "stream-metrics-scope-metrics",
            "The average number of occurrence of expired-window-record-drop operation per second.",
            mkMap(
                mkEntry("client-id", "mock"),
                mkEntry("task-id", "0_0"),
                mkEntry("metrics-scope-id", "bytes-store")
            )
        ));

        assertEquals(1.0, dropTotal.metricValue());
        assertNotEquals(0.0, dropRate.metricValue());
        final List<String> messages = appender.getMessages();
        assertThat(messages, hasItem("Skipping record for expired segment."));
    }
```
```
Cyclomatic Complexity	 2
Assertions		 1
Lines of Code		14
-    
    public void shouldFetchAllSessionsWithSameRecordKey() {

        final List<KeyValue<Windowed<String>, Long>> expected = Arrays.asList(KeyValue.pair(new Windowed<>("a", new SessionWindow(0, 0)), 1L),
                                                                                    KeyValue.pair(new Windowed<>("a", new SessionWindow(10, 10)), 2L),
                                                                                    KeyValue.pair(new Windowed<>("a", new SessionWindow(100, 100)), 3L),
                                                                                    KeyValue.pair(new Windowed<>("a", new SessionWindow(1000, 1000)), 4L));
        for (final KeyValue<Windowed<String>, Long> kv : expected) {
            sessionStore.put(kv.key, kv.value);
        }

        // add one that shouldn't appear in the results
        sessionStore.put(new Windowed<>("aa", new SessionWindow(0, 0)), 5L);

        try (final KeyValueIterator<Windowed<String>, Long> values = sessionStore.fetch("a")) {
            assertEquals(expected, toList(values));
        }
    }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		13
-    
    public void shouldFindValuesWithinMergingSessionWindowRange() {
        final String key = "a";
        sessionStore.put(new Windowed<>(key, new SessionWindow(0L, 0L)), 1L);
        sessionStore.put(new Windowed<>(key, new SessionWindow(1000L, 1000L)), 2L);

        final List<KeyValue<Windowed<String>, Long>> expected = Arrays.asList(
            KeyValue.pair(new Windowed<>(key, new SessionWindow(0L, 0L)), 1L),
            KeyValue.pair(new Windowed<>(key, new SessionWindow(1000L, 1000L)), 2L));

        try (final KeyValueIterator<Windowed<String>, Long> results =
                 sessionStore.findSessions(key, -1, 1000L)) {
            assertEquals(expected, toList(results));
        }
    }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		14
-    
    public void shouldRemove() {
        sessionStore.put(new Windowed<>("a", new SessionWindow(0, 1000)), 1L);
        sessionStore.put(new Windowed<>("a", new SessionWindow(1500, 2500)), 2L);

        sessionStore.remove(new Windowed<>("a", new SessionWindow(0, 1000)));

        try (final KeyValueIterator<Windowed<String>, Long> results =
                 sessionStore.findSessions("a", 0L, 1000L)) {
            assertFalse(results.hasNext());
        }

        try (final KeyValueIterator<Windowed<String>, Long> results =
                 sessionStore.findSessions("a", 1500L, 2500L)) {
            assertTrue(results.hasNext());
        }
    }
```
```
Cyclomatic Complexity	 1
Assertions		 3
Lines of Code		20
-    
    public void shouldFindSessionsToMerge() {
        final Windowed<String> session1 = new Windowed<>("a", new SessionWindow(0, 100));
        final Windowed<String> session2 = new Windowed<>("a", new SessionWindow(101, 200));
        final Windowed<String> session3 = new Windowed<>("a", new SessionWindow(201, 300));
        final Windowed<String> session4 = new Windowed<>("a", new SessionWindow(301, 400));
        final Windowed<String> session5 = new Windowed<>("a", new SessionWindow(401, 500));
        sessionStore.put(session1, 1L);
        sessionStore.put(session2, 2L);
        sessionStore.put(session3, 3L);
        sessionStore.put(session4, 4L);
        sessionStore.put(session5, 5L);

        try (final KeyValueIterator<Windowed<String>, Long> results =
                 sessionStore.findSessions("a", 150, 300)
        ) {
            assertEquals(session2, results.next().key);
            assertEquals(session3, results.next().key);
            assertFalse(results.hasNext());
        }
    }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		15
-    
    public void shouldGetAll() {
        windowStore = createWindowStore(context, false);
        final long startTime = segmentInterval - 4L;

        putFirstBatch(windowStore, startTime, context);

        final KeyValue<Windowed<Integer>, String> zero = windowedPair(0, "zero", startTime + 0);
        final KeyValue<Windowed<Integer>, String> one = windowedPair(1, "one", startTime + 1);
        final KeyValue<Windowed<Integer>, String> two = windowedPair(2, "two", startTime + 2);
        final KeyValue<Windowed<Integer>, String> four = windowedPair(4, "four", startTime + 4);
        final KeyValue<Windowed<Integer>, String> five = windowedPair(5, "five", startTime + 5);

        assertEquals(
            Utils.mkList(zero, one, two, four, five),
            StreamsTestUtils.toList(windowStore.all())
        );
    }
```
```
Cyclomatic Complexity	 1
Assertions		 3
Lines of Code		23
-    
    public void shouldFetchAllInTimeRange() {
        windowStore = createWindowStore(context, false);
        final long startTime = segmentInterval - 4L;

        putFirstBatch(windowStore, startTime, context);

        final KeyValue<Windowed<Integer>, String> zero = windowedPair(0, "zero", startTime + 0);
        final KeyValue<Windowed<Integer>, String> one = windowedPair(1, "one", startTime + 1);
        final KeyValue<Windowed<Integer>, String> two = windowedPair(2, "two", startTime + 2);
        final KeyValue<Windowed<Integer>, String> four = windowedPair(4, "four", startTime + 4);
        final KeyValue<Windowed<Integer>, String> five = windowedPair(5, "five", startTime + 5);

        assertEquals(
            Utils.mkList(one, two, four),
            StreamsTestUtils.toList(windowStore.fetchAll(ofEpochMilli(startTime + 1), ofEpochMilli(startTime + 4)))
        );

        assertEquals(
            Utils.mkList(zero, one, two),
            StreamsTestUtils.toList(windowStore.fetchAll(ofEpochMilli(startTime + 0), ofEpochMilli(startTime + 3)))
        );

        assertEquals(
            Utils.mkList(one, two, four, five),
            StreamsTestUtils.toList(windowStore.fetchAll(ofEpochMilli(startTime + 1), ofEpochMilli(startTime + 5)))
        );
    }
```
```
Cyclomatic Complexity	 1
Assertions		 8
Lines of Code		44
-    
    public void testFetchRange() {
        windowStore = createWindowStore(context, false);
        final long startTime = segmentInterval - 4L;

        putFirstBatch(windowStore, startTime, context);

        final KeyValue<Windowed<Integer>, String> zero = windowedPair(0, "zero", startTime + 0);
        final KeyValue<Windowed<Integer>, String> one = windowedPair(1, "one", startTime + 1);
        final KeyValue<Windowed<Integer>, String> two = windowedPair(2, "two", startTime + 2);
        final KeyValue<Windowed<Integer>, String> four = windowedPair(4, "four", startTime + 4);
        final KeyValue<Windowed<Integer>, String> five = windowedPair(5, "five", startTime + 5);

        assertEquals(
            Utils.mkList(zero, one),
            StreamsTestUtils.toList(windowStore.fetch(0, 1, ofEpochMilli(startTime + 0L - windowSize), ofEpochMilli(startTime + 0L + windowSize)))
        );
        assertEquals(
            Utils.mkList(one),
            StreamsTestUtils.toList(windowStore.fetch(1, 1, ofEpochMilli(startTime + 0L - windowSize), ofEpochMilli(startTime + 0L + windowSize)))
        );
        assertEquals(
            Utils.mkList(one, two),
            StreamsTestUtils.toList(windowStore.fetch(1, 3, ofEpochMilli(startTime + 0L - windowSize), ofEpochMilli(startTime + 0L + windowSize)))
        );
        assertEquals(
            Utils.mkList(zero, one, two),
            StreamsTestUtils.toList(windowStore.fetch(0, 5, ofEpochMilli(startTime + 0L - windowSize), ofEpochMilli(startTime + 0L + windowSize)))
        );
        assertEquals(
            Utils.mkList(zero, one, two,
                four, five),
            StreamsTestUtils.toList(windowStore.fetch(0, 5, ofEpochMilli(startTime + 0L - windowSize), ofEpochMilli(startTime + 0L + windowSize + 5L)))
        );
        assertEquals(
            Utils.mkList(two, four, five),
            StreamsTestUtils.toList(windowStore.fetch(0, 5, ofEpochMilli(startTime + 2L), ofEpochMilli(startTime + 0L + windowSize + 5L)))
        );
        assertEquals(
            Utils.mkList(),
            StreamsTestUtils.toList(windowStore.fetch(4, 5, ofEpochMilli(startTime + 2L), ofEpochMilli(startTime + windowSize)))
        );
        assertEquals(
            Utils.mkList(),
            StreamsTestUtils.toList(windowStore.fetch(0, 3, ofEpochMilli(startTime + 3L), ofEpochMilli(startTime + windowSize + 5)))
        );
    }
```
```
Cyclomatic Complexity	 1
Assertions		 29
Lines of Code		37
-    
    public void testPutAndFetchBefore() {
        windowStore = createWindowStore(context, false);
        final long startTime = segmentInterval - 4L;

        putFirstBatch(windowStore, startTime, context);

        assertEquals(Utils.mkList("zero"), toList(windowStore.fetch(0, ofEpochMilli(startTime + 0L - windowSize), ofEpochMilli(startTime + 0L))));
        assertEquals(Utils.mkList("one"), toList(windowStore.fetch(1, ofEpochMilli(startTime + 1L - windowSize), ofEpochMilli(startTime + 1L))));
        assertEquals(Utils.mkList("two"), toList(windowStore.fetch(2, ofEpochMilli(startTime + 2L - windowSize), ofEpochMilli(startTime + 2L))));
        assertEquals(Utils.mkList(), toList(windowStore.fetch(3, ofEpochMilli(startTime + 3L - windowSize), ofEpochMilli(startTime + 3L))));
        assertEquals(Utils.mkList("four"), toList(windowStore.fetch(4, ofEpochMilli(startTime + 4L - windowSize), ofEpochMilli(startTime + 4L))));
        assertEquals(Utils.mkList("five"), toList(windowStore.fetch(5, ofEpochMilli(startTime + 5L - windowSize), ofEpochMilli(startTime + 5L))));

        putSecondBatch(windowStore, startTime, context);

        assertEquals(Utils.mkList(), toList(windowStore.fetch(2, ofEpochMilli(startTime - 1L - windowSize), ofEpochMilli(startTime - 1L))));
        assertEquals(Utils.mkList(), toList(windowStore.fetch(2, ofEpochMilli(startTime + 0L - windowSize), ofEpochMilli(startTime + 0L))));
        assertEquals(Utils.mkList(), toList(windowStore.fetch(2, ofEpochMilli(startTime + 1L - windowSize), ofEpochMilli(startTime + 1L))));
        assertEquals(Utils.mkList("two"), toList(windowStore.fetch(2, ofEpochMilli(startTime + 2L - windowSize), ofEpochMilli(startTime + 2L))));
        assertEquals(Utils.mkList("two", "two+1"), toList(windowStore.fetch(2, ofEpochMilli(startTime + 3L - windowSize), ofEpochMilli(startTime + 3L))));
        assertEquals(Utils.mkList("two", "two+1", "two+2"), toList(windowStore.fetch(2, ofEpochMilli(startTime + 4L - windowSize), ofEpochMilli(startTime + 4L))));
        assertEquals(Utils.mkList("two", "two+1", "two+2", "two+3"), toList(windowStore.fetch(2, ofEpochMilli(startTime + 5L - windowSize), ofEpochMilli(startTime + 5L))));
        assertEquals(Utils.mkList("two+1", "two+2", "two+3", "two+4"), toList(windowStore.fetch(2, ofEpochMilli(startTime + 6L - windowSize), ofEpochMilli(startTime + 6L))));
        assertEquals(Utils.mkList("two+2", "two+3", "two+4", "two+5"), toList(windowStore.fetch(2, ofEpochMilli(startTime + 7L - windowSize), ofEpochMilli(startTime + 7L))));
        assertEquals(Utils.mkList("two+3", "two+4", "two+5", "two+6"), toList(windowStore.fetch(2, ofEpochMilli(startTime + 8L - windowSize), ofEpochMilli(startTime + 8L))));
        assertEquals(Utils.mkList("two+4", "two+5", "two+6"), toList(windowStore.fetch(2, ofEpochMilli(startTime + 9L - windowSize), ofEpochMilli(startTime + 9L))));
        assertEquals(Utils.mkList("two+5", "two+6"), toList(windowStore.fetch(2, ofEpochMilli(startTime + 10L - windowSize), ofEpochMilli(startTime + 10L))));
        assertEquals(Utils.mkList("two+6"), toList(windowStore.fetch(2, ofEpochMilli(startTime + 11L - windowSize), ofEpochMilli(startTime + 11L))));
        assertEquals(Utils.mkList(), toList(windowStore.fetch(2, ofEpochMilli(startTime + 12L - windowSize), ofEpochMilli(startTime + 12L))));
        assertEquals(Utils.mkList(), toList(windowStore.fetch(2, ofEpochMilli(startTime + 13L - windowSize), ofEpochMilli(startTime + 13L))));

        // Flush the store and verify all current entries were properly flushed ...
        windowStore.flush();

        final Map<Integer, Set<String>> entriesByKey = entriesByKey(changeLog, startTime);

        assertEquals(Utils.mkSet("zero@0"), entriesByKey.get(0));
        assertEquals(Utils.mkSet("one@1"), entriesByKey.get(1));
        assertEquals(Utils.mkSet("two@2", "two+1@3", "two+2@4", "two+3@5", "two+4@6", "two+5@7", "two+6@8"), entriesByKey.get(2));
        assertNull(entriesByKey.get(3));
        assertEquals(Utils.mkSet("four@4"), entriesByKey.get(4));
        assertEquals(Utils.mkSet("five@5"), entriesByKey.get(5));
        assertNull(entriesByKey.get(6));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 29
Lines of Code		37
-    
    public void testPutAndFetchAfter() {
        windowStore = createWindowStore(context, false);
        final long startTime = segmentInterval - 4L;

        putFirstBatch(windowStore, startTime, context);

        assertEquals(Utils.mkList("zero"), toList(windowStore.fetch(0, ofEpochMilli(startTime + 0L), ofEpochMilli(startTime + 0L + windowSize))));
        assertEquals(Utils.mkList("one"), toList(windowStore.fetch(1, ofEpochMilli(startTime + 1L), ofEpochMilli(startTime + 1L + windowSize))));
        assertEquals(Utils.mkList("two"), toList(windowStore.fetch(2, ofEpochMilli(startTime + 2L), ofEpochMilli(startTime + 2L + windowSize))));
        assertEquals(Utils.mkList(), toList(windowStore.fetch(3, ofEpochMilli(startTime + 3L), ofEpochMilli(startTime + 3L + windowSize))));
        assertEquals(Utils.mkList("four"), toList(windowStore.fetch(4, ofEpochMilli(startTime + 4L), ofEpochMilli(startTime + 4L + windowSize))));
        assertEquals(Utils.mkList("five"), toList(windowStore.fetch(5, ofEpochMilli(startTime + 5L), ofEpochMilli(startTime + 5L + windowSize))));

        putSecondBatch(windowStore, startTime, context);

        assertEquals(Utils.mkList(), toList(windowStore.fetch(2, ofEpochMilli(startTime - 2L), ofEpochMilli(startTime - 2L + windowSize))));
        assertEquals(Utils.mkList("two"), toList(windowStore.fetch(2, ofEpochMilli(startTime - 1L), ofEpochMilli(startTime - 1L + windowSize))));
        assertEquals(Utils.mkList("two", "two+1"), toList(windowStore.fetch(2, ofEpochMilli(startTime), ofEpochMilli(startTime + windowSize))));
        assertEquals(Utils.mkList("two", "two+1", "two+2"), toList(windowStore.fetch(2, ofEpochMilli(startTime + 1L), ofEpochMilli(startTime + 1L + windowSize))));
        assertEquals(Utils.mkList("two", "two+1", "two+2", "two+3"), toList(windowStore.fetch(2, ofEpochMilli(startTime + 2L), ofEpochMilli(startTime + 2L + windowSize))));
        assertEquals(Utils.mkList("two+1", "two+2", "two+3", "two+4"), toList(windowStore.fetch(2, ofEpochMilli(startTime + 3L), ofEpochMilli(startTime + 3L + windowSize))));
        assertEquals(Utils.mkList("two+2", "two+3", "two+4", "two+5"), toList(windowStore.fetch(2, ofEpochMilli(startTime + 4L), ofEpochMilli(startTime + 4L + windowSize))));
        assertEquals(Utils.mkList("two+3", "two+4", "two+5", "two+6"), toList(windowStore.fetch(2, ofEpochMilli(startTime + 5L), ofEpochMilli(startTime + 5L + windowSize))));
        assertEquals(Utils.mkList("two+4", "two+5", "two+6"), toList(windowStore.fetch(2, ofEpochMilli(startTime + 6L), ofEpochMilli(startTime + 6L + windowSize))));
        assertEquals(Utils.mkList("two+5", "two+6"), toList(windowStore.fetch(2, ofEpochMilli(startTime + 7L), ofEpochMilli(startTime + 7L + windowSize))));
        assertEquals(Utils.mkList("two+6"), toList(windowStore.fetch(2, ofEpochMilli(startTime + 8L), ofEpochMilli(startTime + 8L + windowSize))));
        assertEquals(Utils.mkList(), toList(windowStore.fetch(2, ofEpochMilli(startTime + 9L), ofEpochMilli(startTime + 9L + windowSize))));
        assertEquals(Utils.mkList(), toList(windowStore.fetch(2, ofEpochMilli(startTime + 10L), ofEpochMilli(startTime + 10L + windowSize))));
        assertEquals(Utils.mkList(), toList(windowStore.fetch(2, ofEpochMilli(startTime + 11L), ofEpochMilli(startTime + 11L + windowSize))));
        assertEquals(Utils.mkList(), toList(windowStore.fetch(2, ofEpochMilli(startTime + 12L), ofEpochMilli(startTime + 12L + windowSize))));

        // Flush the store and verify all current entries were properly flushed ...
        windowStore.flush();

        final Map<Integer, Set<String>> entriesByKey = entriesByKey(changeLog, startTime);

        assertEquals(Utils.mkSet("zero@0"), entriesByKey.get(0));
        assertEquals(Utils.mkSet("one@1"), entriesByKey.get(1));
        assertEquals(Utils.mkSet("two@2", "two+1@3", "two+2@4", "two+3@5", "two+4@6", "two+5@7", "two+6@8"), entriesByKey.get(2));
        assertNull(entriesByKey.get(3));
        assertEquals(Utils.mkSet("four@4"), entriesByKey.get(4));
        assertEquals(Utils.mkSet("five@5"), entriesByKey.get(5));
        assertNull(entriesByKey.get(6));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 8
Lines of Code		19
-    
    public void testPutSameKeyTimestamp() {
        windowStore = createWindowStore(context, true);
        final long startTime = segmentInterval - 4L;

        setCurrentTime(startTime);
        windowStore.put(0, "zero");

        assertEquals(Utils.mkList("zero"), toList(windowStore.fetch(0, ofEpochMilli(startTime - windowSize), ofEpochMilli(startTime + windowSize))));

        windowStore.put(0, "zero");
        windowStore.put(0, "zero+");
        windowStore.put(0, "zero++");

        assertEquals(Utils.mkList("zero", "zero", "zero+", "zero++"), toList(windowStore.fetch(0, ofEpochMilli(startTime - windowSize), ofEpochMilli(startTime + windowSize))));
        assertEquals(Utils.mkList("zero", "zero", "zero+", "zero++"), toList(windowStore.fetch(0, ofEpochMilli(startTime + 1L - windowSize), ofEpochMilli(startTime + 1L + windowSize))));
        assertEquals(Utils.mkList("zero", "zero", "zero+", "zero++"), toList(windowStore.fetch(0, ofEpochMilli(startTime + 2L - windowSize), ofEpochMilli(startTime + 2L + windowSize))));
        assertEquals(Utils.mkList("zero", "zero", "zero+", "zero++"), toList(windowStore.fetch(0, ofEpochMilli(startTime + 3L - windowSize), ofEpochMilli(startTime + 3L + windowSize))));
        assertEquals(Utils.mkList(), toList(windowStore.fetch(0, ofEpochMilli(startTime + 4L - windowSize), ofEpochMilli(startTime + 4L + windowSize))));

        // Flush the store and verify all current entries were properly flushed ...
        windowStore.flush();

        final Map<Integer, Set<String>> entriesByKey = entriesByKey(changeLog, startTime);

        assertEquals(Utils.mkSet("zero@0", "zero@0", "zero+@0", "zero++@0"), entriesByKey.get(0));
    }
```
```
Cyclomatic Complexity	 4
Assertions		 9
Lines of Code		63
-    
    public void testSegmentMaintenance() {
        windowStore = createWindowStore(context, true);
        context.setTime(0L);
        setCurrentTime(0);
        windowStore.put(0, "v");
        assertEquals(
            Utils.mkSet(segments.segmentName(0L)),
            segmentDirs(baseDir)
        );

        setCurrentTime(segmentInterval - 1);
        windowStore.put(0, "v");
        windowStore.put(0, "v");
        assertEquals(
            Utils.mkSet(segments.segmentName(0L)),
            segmentDirs(baseDir)
        );

        setCurrentTime(segmentInterval);
        windowStore.put(0, "v");
        assertEquals(
            Utils.mkSet(segments.segmentName(0L), segments.segmentName(1L)),
            segmentDirs(baseDir)
        );

        WindowStoreIterator iter;
        int fetchedCount;

        iter = windowStore.fetch(0, ofEpochMilli(0L), ofEpochMilli(segmentInterval * 4));
        fetchedCount = 0;
        while (iter.hasNext()) {
            iter.next();
            fetchedCount++;
        }
        assertEquals(4, fetchedCount);

        assertEquals(
            Utils.mkSet(segments.segmentName(0L), segments.segmentName(1L)),
            segmentDirs(baseDir)
        );

        setCurrentTime(segmentInterval * 3);
        windowStore.put(0, "v");

        iter = windowStore.fetch(0, ofEpochMilli(0L), ofEpochMilli(segmentInterval * 4));
        fetchedCount = 0;
        while (iter.hasNext()) {
            iter.next();
            fetchedCount++;
        }
        assertEquals(2, fetchedCount);

        assertEquals(
            Utils.mkSet(segments.segmentName(1L), segments.segmentName(3L)),
            segmentDirs(baseDir)
        );

        setCurrentTime(segmentInterval * 5);
        windowStore.put(0, "v");

        iter = windowStore.fetch(0, ofEpochMilli(segmentInterval * 4), ofEpochMilli(segmentInterval * 10));
        fetchedCount = 0;
        while (iter.hasNext()) {
            iter.next();
            fetchedCount++;
        }
        assertEquals(1, fetchedCount);

        assertEquals(
            Utils.mkSet(segments.segmentName(3L), segments.segmentName(5L)),
            segmentDirs(baseDir)
        );

    }
```
```
Cyclomatic Complexity	 2
Assertions		 2
Lines of Code		29
-    
    public void testInitialLoading() {
        final File storeDir = new File(baseDir, windowName);

        windowStore = createWindowStore(context, false);

        new File(storeDir, segments.segmentName(0L)).mkdir();
        new File(storeDir, segments.segmentName(1L)).mkdir();
        new File(storeDir, segments.segmentName(2L)).mkdir();
        new File(storeDir, segments.segmentName(3L)).mkdir();
        new File(storeDir, segments.segmentName(4L)).mkdir();
        new File(storeDir, segments.segmentName(5L)).mkdir();
        new File(storeDir, segments.segmentName(6L)).mkdir();
        windowStore.close();

        windowStore = createWindowStore(context, false);

        // put something in the store to advance its stream time and expire the old segments
        windowStore.put(1, "v", 6L * segmentInterval);

        final List<String> expected = Utils.mkList(segments.segmentName(4L), segments.segmentName(5L), segments.segmentName(6L));
        expected.sort(String::compareTo);

        final List<String> actual = Utils.toList(segmentDirs(baseDir).iterator());
        actual.sort(String::compareTo);

        assertEquals(expected, actual);

        try (final WindowStoreIterator iter = windowStore.fetch(0, ofEpochMilli(0L), ofEpochMilli(1000000L))) {
            while (iter.hasNext()) {
                iter.next();
            }
        }

        assertEquals(
            Utils.mkSet(segments.segmentName(4L), segments.segmentName(5L), segments.segmentName(6L)),
            segmentDirs(baseDir)
        );
    }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		12
-    
    public void shouldCloseOpenIteratorsWhenStoreIsClosedAndNotThrowInvalidStateStoreExceptionOnHasNext() {
        windowStore = createWindowStore(context, false);
        setCurrentTime(0);
        windowStore.put(1, "one", 1L);
        windowStore.put(1, "two", 2L);
        windowStore.put(1, "three", 3L);

        final WindowStoreIterator<String> iterator = windowStore.fetch(1, ofEpochMilli(1L), ofEpochMilli(3L));
        assertTrue(iterator.hasNext());
        windowStore.close();

        assertFalse(iterator.hasNext());

    }
```
```
Cyclomatic Complexity	 1
Assertions		 3
Lines of Code		29
-    
    public void shouldFetchAndIterateOverExactKeys() {
        final long windowSize = 0x7a00000000000000L;
        final long retentionPeriod = 0x7a00000000000000L;

        final WindowStore<String, String> windowStore = Stores.windowStoreBuilder(
            Stores.persistentWindowStore(windowName, ofMillis(retentionPeriod), ofMillis(windowSize), true),
            Serdes.String(),
            Serdes.String()).build();

        windowStore.init(context, windowStore);

        windowStore.put("a", "0001", 0);
        windowStore.put("aa", "0002", 0);
        windowStore.put("a", "0003", 1);
        windowStore.put("aa", "0004", 1);
        windowStore.put("a", "0005", 0x7a00000000000000L - 1);


        final List expected = Utils.mkList("0001", "0003", "0005");
        assertThat(toList(windowStore.fetch("a", ofEpochMilli(0), ofEpochMilli(Long.MAX_VALUE))), equalTo(expected));

        List<KeyValue<Windowed<String>, String>> list =
            StreamsTestUtils.toList(windowStore.fetch("a", "a", ofEpochMilli(0), ofEpochMilli(Long.MAX_VALUE)));
        assertThat(list, equalTo(Utils.mkList(
            windowedPair("a", "0001", 0, windowSize),
            windowedPair("a", "0003", 1, windowSize),
            windowedPair("a", "0005", 0x7a00000000000000L - 1, windowSize)
        )));

        list = StreamsTestUtils.toList(windowStore.fetch("aa", "aa", ofEpochMilli(0), ofEpochMilli(Long.MAX_VALUE)));
        assertThat(list, equalTo(Utils.mkList(
            windowedPair("aa", "0002", 0, windowSize),
            windowedPair("aa", "0004", 1, windowSize)
        )));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		5
-    (expected = NullPointerException.class)
    public void shouldThrowNullPointerExceptionOnPutNullKey() {
        windowStore = createWindowStore(context, false);
        windowStore.put(null, "anyValue");
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		5
-    
    public void shouldNotThrowNullPointerExceptionOnPutNullValue() {
        windowStore = createWindowStore(context, false);
        windowStore.put(1, null);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		5
-    (expected = NullPointerException.class)
    public void shouldThrowNullPointerExceptionOnGetNullKey() {
        windowStore = createWindowStore(context, false);
        windowStore.fetch(null, ofEpochMilli(1L), ofEpochMilli(2L));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		5
-    (expected = NullPointerException.class)
    public void shouldThrowNullPointerExceptionOnRangeNullFromKey() {
        windowStore = createWindowStore(context, false);
        windowStore.fetch(null, 2, ofEpochMilli(1L), ofEpochMilli(2L));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		5
-    (expected = NullPointerException.class)
    public void shouldThrowNullPointerExceptionOnRangeNullToKey() {
        windowStore = createWindowStore(context, false);
        windowStore.fetch(1, null, ofEpochMilli(1L), ofEpochMilli(2L));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		11
-    
    public void shouldNoNullPointerWhenSerdeDoesNotHandleNull() {
        windowStore = new RocksDBWindowStore<>(
            new RocksDBSegmentedBytesStore(windowName, "metrics-scope", retentionPeriod, segmentInterval, new WindowKeySchema()),
            Serdes.Integer(),
            new SerdeThatDoesntHandleNull(),
            false,
            windowSize);
        windowStore.init(context, windowStore);

        assertNull(windowStore.fetch(1, 0));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 3
Lines of Code		26
-    
    public void shouldFetchAndIterateOverExactBinaryKeys() {
        final WindowStore<Bytes, String> windowStore = Stores.windowStoreBuilder(
            Stores.persistentWindowStore(windowName, ofMillis(60_000L), ofMillis(60_000L), true),
            Serdes.Bytes(),
            Serdes.String()).build();

        windowStore.init(context, windowStore);

        final Bytes key1 = Bytes.wrap(new byte[] {0});
        final Bytes key2 = Bytes.wrap(new byte[] {0, 0});
        final Bytes key3 = Bytes.wrap(new byte[] {0, 0, 0});
        windowStore.put(key1, "1", 0);
        windowStore.put(key2, "2", 0);
        windowStore.put(key3, "3", 0);
        windowStore.put(key1, "4", 1);
        windowStore.put(key2, "5", 1);
        windowStore.put(key3, "6", 59999);
        windowStore.put(key1, "7", 59999);
        windowStore.put(key2, "8", 59999);
        windowStore.put(key3, "9", 59999);

        final List expectedKey1 = Utils.mkList("1", "4", "7");
        assertThat(toList(windowStore.fetch(key1, ofEpochMilli(0), ofEpochMilli(Long.MAX_VALUE))), equalTo(expectedKey1));
        final List expectedKey2 = Utils.mkList("2", "5", "8");
        assertThat(toList(windowStore.fetch(key2, ofEpochMilli(0), ofEpochMilli(Long.MAX_VALUE))), equalTo(expectedKey2));
        final List expectedKey3 = Utils.mkList("3", "6", "9");
        assertThat(toList(windowStore.fetch(key3, ofEpochMilli(0), ofEpochMilli(Long.MAX_VALUE))), equalTo(expectedKey3));
    }
```
## 83ec80988a1f9418ec1296e990115017c732594b ##
```
Cyclomatic Complexity	3
Assertions		2
Lines of Code		14
-    
    public void testRoundRobinWithUnavailablePartitions() {
        // When there are some unavailable partitions, we want to make sure that (1) we always pick an available partition,
        // and (2) the available partitions are selected in a round robin way.
        int countForPart0 = 0;
        int countForPart2 = 0;
        for (int i = 1; i <= 100; i++) {
            int part = partitioner.partition("test", null, null, null, null, cluster);
            assertTrue("We should never choose a leader-less node in round robin", part == 0 || part == 2);
            if (part == 0)
                countForPart0++;
            else
                countForPart2++;
        }
        assertEquals("The distribution between two available partitions should be even", countForPart0, countForPart2);
    }
```
```
Cyclomatic Complexity	 3
Assertions		 9
Lines of Code		19
-    
    public void testCloseConnectionInClosingState() throws Exception {
        KafkaChannel channel = createConnectionWithStagedReceives(5);
        String id = channel.id();
        selector.mute(id); // Mute to allow channel to be expired even if more data is available for read
        time.sleep(6000);  // The max idle time is 5000ms
        selector.poll(0);
        assertNull("Channel not expired", selector.channel(id));
        assertEquals(channel, selector.closingChannel(id));
        assertEquals(ChannelState.EXPIRED, channel.state());
        selector.close(id);
        assertNull("Channel not removed from channels", selector.channel(id));
        assertNull("Channel not removed from closingChannels", selector.closingChannel(id));
        assertTrue("Unexpected disconnect notification", selector.disconnected().isEmpty());
        assertEquals(ChannelState.EXPIRED, channel.state());
        assertNull(channel.selectionKey().attachment());
        selector.poll(0);
        assertTrue("Unexpected disconnect notification", selector.disconnected().isEmpty());@@@     }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		4
-    
    public void testCloseOldestConnectionWithMultipleStagedReceives() throws Exception {
        verifyCloseOldestConnectionWithStagedReceives(5);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		9
-  
  def testDeleteWithShortInitialization() {
    // run one consumer in the group
    addConsumerGroupExecutor(numConsumers = 1)
    val cgcArgs = Array("--bootstrap-server", brokerList, "--delete", "--group", group)
    val service = getConsumerGroupService(cgcArgs)

    val result = service.deleteGroups()
    assertTrue(s"The consumer group deletion did not work as expected",
      result.size == 1 && result.keySet.contains(group))
  }
```
```
Cyclomatic Complexity	 3
Assertions		 0
Lines of Code		21
-  
  def testDateTimeFormats() {
    //check valid formats
    invokeGetDateTimeMethod(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS"))
    invokeGetDateTimeMethod(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ"))
    invokeGetDateTimeMethod(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX"))
    invokeGetDateTimeMethod(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXX"))
    invokeGetDateTimeMethod(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX"))@@@ 
    //check some invalid formats
    try {
      invokeGetDateTimeMethod(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss"))
      fail("Call to getDateTime should fail")
    } catch {
      case _: ParseException =>
    }

    try {
      invokeGetDateTimeMethod(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.X"))
      fail("Call to getDateTime should fail")
    } catch {
      case _: ParseException =>
    }
  }
```
```
Cyclomatic Complexity	 3
Assertions		 3
Lines of Code		79
-  
  def testDelayedFetchAfterAppendRecords(): Unit = {
    val replicaManager: ReplicaManager = EasyMock.mock(classOf[ReplicaManager])
    val zkClient: KafkaZkClient = EasyMock.mock(classOf[KafkaZkClient])
    val controllerId = 0
    val controllerEpoch = 0
    val leaderEpoch = 5
    val replicaIds = List[Integer](brokerId, brokerId + 1).asJava
    val isr = replicaIds
    val logConfig = LogConfig(new Properties)

    val topicPartitions = (0 until 5).map { i => new TopicPartition("test-topic", i) }
    val logs = topicPartitions.map { tp => logManager.getOrCreateLog(tp, logConfig) }
    val replicas = logs.map { log => new Replica(brokerId, log.topicPartition, time, log = Some(log)) }
    val partitions = replicas.map { replica =>
      val tp = replica.topicPartition
      val partition = new Partition(tp,
        isOffline = false,
        replicaLagTimeMaxMs = Defaults.ReplicaLagTimeMaxMs,
        interBrokerProtocolVersion = ApiVersion.latestVersion,
        localBrokerId = brokerId,
        time,
        replicaManager,
        logManager,
        zkClient)
      partition.addReplicaIfNotExists(replica)
      partition.makeLeader(controllerId, new LeaderAndIsrRequest.PartitionState(controllerEpoch, brokerId,
        leaderEpoch, isr, 1, replicaIds, true), 0)
      partition
    }

    // Acquire leaderIsrUpdate read lock of a different partition when completing delayed fetch
    val tpKey: Capture[TopicPartitionOperationKey] = EasyMock.newCapture()
    EasyMock.expect(replicaManager.tryCompleteDelayedFetch(EasyMock.capture(tpKey)))
      .andAnswer(new IAnswer[Unit] {
        override def answer(): Unit = {
          val anotherPartition = (tpKey.getValue.partition + 1) % topicPartitions.size
          val partition = partitions(anotherPartition)
          partition.fetchOffsetSnapshot(Optional.of(leaderEpoch), fetchOnlyFromLeader = true)
        }
      }).anyTimes()
    EasyMock.replay(replicaManager, zkClient)

    def createRecords(baseOffset: Long): MemoryRecords = {
      val records = List(
        new SimpleRecord("k1".getBytes, "v1".getBytes),
        new SimpleRecord("k2".getBytes, "v2".getBytes))
      val buf = ByteBuffer.allocate(DefaultRecordBatch.sizeInBytes(records.asJava))
      val builder = MemoryRecords.builder(
        buf, RecordBatch.CURRENT_MAGIC_VALUE, CompressionType.NONE, TimestampType.CREATE_TIME,
        baseOffset, time.milliseconds, 0)
      records.foreach(builder.append)
      builder.build()
    }

    val done = new AtomicBoolean()
    val executor = Executors.newFixedThreadPool(topicPartitions.size + 1)
    try {
      // Invoke some operation that acquires leaderIsrUpdate write lock on one thread
      executor.submit(CoreUtils.runnable {
        while (!done.get) {
          partitions.foreach(_.maybeShrinkIsr(10000))
        }
      })
      // Append records to partitions, one partition-per-thread
      val futures = partitions.map { partition =>
        executor.submit(CoreUtils.runnable {
          (1 to 10000).foreach { _ => partition.appendRecordsToLeader(createRecords(baseOffset = 0), isFromClient = true) }
        })
      }
      futures.foreach(_.get(10, TimeUnit.SECONDS))
      done.set(true)
    } catch {
      case e: TimeoutException =>
        val allThreads = TestUtils.allThreadStackTraces()
        fail(s"Test timed out with exception $e, thread stack traces: $allThreads")
    } finally {
      executor.shutdownNow()
      executor.awaitTermination(5, TimeUnit.SECONDS)
    }@@@+    // Expansion does not affect the ISR@@@+    assertEquals(Set[Integer](leader, follower2), partition.isrState.isr, "ISR")@@@+    assertEquals(Set[Integer](leader, follower1, follower2), partition.isrState.maximalIsr, "ISR")@@@+    assertEquals(alterIsrManager.isrUpdates.head.leaderAndIsr.isr.toSet,@@@+      Set(leader, follower1, follower2), "AlterIsr")@@@   }
```
```
Cyclomatic Complexity	 2
Assertions		 2
Lines of Code		31
-  
  def testTryCompleteLockContention(): Unit = {
    executorService = Executors.newSingleThreadExecutor()
    val completionAttemptsRemaining = new AtomicInteger(Int.MaxValue)
    val tryCompleteSemaphore = new Semaphore(1)
    val key = "key"

    val op = new MockDelayedOperation(100000L, None, None) {
      override def tryComplete() = {
        val shouldComplete = completionAttemptsRemaining.decrementAndGet <= 0
        tryCompleteSemaphore.acquire()
        try {
          if (shouldComplete)
            forceComplete()
          else
            false
        } finally {
          tryCompleteSemaphore.release()
        }
      }
    }

    purgatory.tryCompleteElseWatch(op, Seq(key))
    completionAttemptsRemaining.set(2)
    tryCompleteSemaphore.acquire()
    val future = runOnAnotherThread(purgatory.checkAndComplete(key), shouldComplete = false)
    TestUtils.waitUntilTrue(() => tryCompleteSemaphore.hasQueuedThreads, "Not attempting to complete")
    purgatory.checkAndComplete(key) // this should not block even though lock is not free
    assertFalse("Operation should not have completed", op.isCompleted)
    tryCompleteSemaphore.release()
    future.get(10, TimeUnit.SECONDS)
    assertTrue("Operation should have completed", op.isCompleted)@@@   }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		4
-  
  def testDelayedOperationLock() {
    verifyDelayedOperationLock(new MockDelayedOperation(100000L), mismatchedLocks = false)
  }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		10
-  
  def testDelayedOperationLockOverride() {
    def newMockOperation = {
      val lock = new ReentrantLock
      new MockDelayedOperation(100000L, Some(lock), Some(lock))
    }
    verifyDelayedOperationLock(newMockOperation, mismatchedLocks = false)

    verifyDelayedOperationLock(new MockDelayedOperation(100000L, None, Some(new ReentrantLock)),
        mismatchedLocks = true)
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		25
-    
    public void testValueGetter() {
        final StreamsBuilder builder = new StreamsBuilder();

        final String topic1 = "topic1";
        final String topic2 = "topic2";

        final KTableImpl<String, String, String> table1 =
                (KTableImpl<String, String, String>) builder.table(topic1, consumed);
        final KTableImpl<String, String, Integer> table2 = (KTableImpl<String, String, Integer>) table1.mapValues(
                new ValueMapper<String, Integer>() {
                    @Override
                    public Integer apply(final String value) {
                        return new Integer(value);
                    }
                });
        final KTableImpl<String, Integer, Integer> table3 = (KTableImpl<String, Integer, Integer>) table2.filter(
                new Predicate<String, Integer>() {
                    @Override
                    public boolean test(final String key, final Integer value) {
                        return (value % 2) == 0;
                    }
                });
        table1.toStream().to(topic2, produced);
        final KTableImpl<String, String, String> table4 = (KTableImpl<String, String, String>) builder.table(topic2, consumed);

        doTestValueGetter(builder, topic1, table1, table2, table3, table4);@@@     }
```
```
Cyclomatic Complexity	 2
Assertions		 6
Lines of Code		48
-    
    public void shouldRestoreToKTable() throws IOException {
        consumer.assign(mkList(globalTopicPartition));
        consumer.commitSync(mkMap(mkEntry(globalTopicPartition, new OffsetAndMetadata(0L))));

        final StandbyTask task = new StandbyTask(@@@+        return new StandbyTask(@@@             taskId,
            ktablePartitions,
            ktableTopology,
            consumer,
            changelogReader,
            createConfig(baseDir),
            null,
            stateDirectory
        );
        task.initializeStateStores();

        // The commit offset is at 0L. Records should not be processed
        List<ConsumerRecord<byte[], byte[]>> remaining = task.update(
            globalTopicPartition,
            Arrays.asList(
                makeConsumerRecord(globalTopicPartition, 10, 1),
                makeConsumerRecord(globalTopicPartition, 20, 2),
                makeConsumerRecord(globalTopicPartition, 30, 3),
                makeConsumerRecord(globalTopicPartition, 40, 4),
                makeConsumerRecord(globalTopicPartition, 50, 5)
            )
        );
        assertEquals(5, remaining.size());

        consumer.commitSync(mkMap(mkEntry(globalTopicPartition, new OffsetAndMetadata(10L))));
        task.commit(); // update offset limits

        // The commit offset has not reached, yet.
        remaining = task.update(globalTopicPartition, remaining);
        assertEquals(5, remaining.size());

        consumer.commitSync(mkMap(mkEntry(globalTopicPartition, new OffsetAndMetadata(11L))));
        task.commit(); // update offset limits

        // one record should be processed.
        remaining = task.update(globalTopicPartition, remaining);
        assertEquals(4, remaining.size());

        consumer.commitSync(mkMap(mkEntry(globalTopicPartition, new OffsetAndMetadata(45L))));
        task.commit(); // update offset limits

        // The commit offset is now 45. All record except for the last one should be processed.
        remaining = task.update(globalTopicPartition, remaining);
        assertEquals(1, remaining.size());

        consumer.commitSync(mkMap(mkEntry(globalTopicPartition, new OffsetAndMetadata(50L))));
        task.commit(); // update offset limits

        // The commit offset is now 50. Still the last record remains.
        remaining = task.update(globalTopicPartition, remaining);
        assertEquals(1, remaining.size());

        consumer.commitSync(mkMap(mkEntry(globalTopicPartition, new OffsetAndMetadata(60L))));
        task.commit(); // update offset limits

        // The commit offset is now 60. No record should be left.
        remaining = task.update(globalTopicPartition, remaining);
        assertEquals(emptyList(), remaining);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		6
-    
    public void shouldInitializeStateStoreWithoutException() throws IOException {
        final InternalStreamsBuilder builder = new InternalStreamsBuilder(new InternalTopologyBuilder());
        builder.stream(Collections.singleton("topic"), new ConsumedInternal<>()).groupByKey().count();

        initializeStandbyStores(builder);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		6
-    
    public void shouldInitializeWindowStoreWithoutException() throws IOException {
        final InternalStreamsBuilder builder = new InternalStreamsBuilder(new InternalTopologyBuilder());
        builder.stream(Collections.singleton("topic"), new ConsumedInternal<>()).groupByKey().windowedBy(TimeWindows.of(ofMillis(100))).count();

        initializeStandbyStores(builder);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		11
-    
    public void shouldThrowKafkaExceptionAssignmentErrorCodeNotConfigured() {
        final Map<String, Object> config = configProps();
        config.remove(StreamsConfig.InternalConfig.ASSIGNMENT_ERROR_CODE);

        try {
            partitionAssignor.configure(config);
            fail("Should have thrown KafkaException");
        } catch (final KafkaException expected) {
            assertThat(expected.getMessage(), equalTo("assignmentErrorCode is not specified"));
        }
    }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		13
-    
    public void shouldPutAndFetch() {
        final String key = "a";
        bytesStore.put(serializeKey(new Windowed<>(key, windows[0])), serializeValue(10));
        bytesStore.put(serializeKey(new Windowed<>(key, windows[1])), serializeValue(50));
        bytesStore.put(serializeKey(new Windowed<>(key, windows[2])), serializeValue(100));

        final KeyValueIterator<Bytes, byte[]> values = bytesStore.fetch(Bytes.wrap(key.getBytes()), 0, 500);

        final List<KeyValue<Windowed<String>, Long>> expected = Arrays.asList(
            KeyValue.pair(new Windowed<>(key, windows[0]), 10L),
            KeyValue.pair(new Windowed<>(key, windows[1]), 50L)
        );

        assertEquals(expected, toList(values));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		13
-    
    public void shouldFindValuesWithinRange() {
        final String key = "a";
        bytesStore.put(serializeKey(new Windowed<>(key, windows[0])), serializeValue(10));
        bytesStore.put(serializeKey(new Windowed<>(key, windows[1])), serializeValue(50));
        bytesStore.put(serializeKey(new Windowed<>(key, windows[2])), serializeValue(100));
        final KeyValueIterator<Bytes, byte[]> results = bytesStore.fetch(Bytes.wrap(key.getBytes()), 1, 999);
        final List<KeyValue<Windowed<String>, Long>> expected = Arrays.asList(
            KeyValue.pair(new Windowed<>(key, windows[0]), 10L),
            KeyValue.pair(new Windowed<>(key, windows[1]), 50L)
        );

        assertEquals(expected, toList(results));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		8
-    
    public void shouldRemove() {
        bytesStore.put(serializeKey(new Windowed<>("a", windows[0])), serializeValue(30));
        bytesStore.put(serializeKey(new Windowed<>("a", windows[1])), serializeValue(50));

        bytesStore.remove(serializeKey(new Windowed<>("a", windows[0])));
        final KeyValueIterator<Bytes, byte[]> value = bytesStore.fetch(Bytes.wrap("a".getBytes()), 0, 100);
        assertFalse(value.hasNext());
    }
```
```
Cyclomatic Complexity	 1
Assertions		 3
Lines of Code		20
-    
    public void shouldRollSegments() {
        // just to validate directories
        final Segments segments = new Segments(storeName, retention, segmentInterval);
        final String key = "a";

        bytesStore.put(serializeKey(new Windowed<>(key, windows[0])), serializeValue(50));
        bytesStore.put(serializeKey(new Windowed<>(key, windows[1])), serializeValue(100));
        bytesStore.put(serializeKey(new Windowed<>(key, windows[2])), serializeValue(500));
        assertEquals(Collections.singleton(segments.segmentName(0)), segmentDirs());

        bytesStore.put(serializeKey(new Windowed<>(key, windows[3])), serializeValue(1000));
        assertEquals(Utils.mkSet(segments.segmentName(0), segments.segmentName(1)), segmentDirs());

        final List<KeyValue<Windowed<String>, Long>> results = toList(bytesStore.fetch(Bytes.wrap(key.getBytes()), 0, 1500));

        assertEquals(
            Arrays.asList(
                KeyValue.pair(new Windowed<>(key, windows[0]), 50L),
                KeyValue.pair(new Windowed<>(key, windows[1]), 100L),
                KeyValue.pair(new Windowed<>(key, windows[2]), 500L)
            ),
            results
        );

    }
```
```
Cyclomatic Complexity	 1
Assertions		 3
Lines of Code		23
-    
    public void shouldGetAllSegments() {
        // just to validate directories
        final Segments segments = new Segments(storeName, retention, segmentInterval);
        final String key = "a";

        bytesStore.put(serializeKey(new Windowed<>(key, windows[0])), serializeValue(50L));
        assertEquals(Collections.singleton(segments.segmentName(0)), segmentDirs());

        bytesStore.put(serializeKey(new Windowed<>(key, windows[3])), serializeValue(100L));
        assertEquals(
            Utils.mkSet(
                segments.segmentName(0),
                segments.segmentName(1)
            ),
            segmentDirs()
        );

        final List<KeyValue<Windowed<String>, Long>> results = toList(bytesStore.all());
        assertEquals(
            Arrays.asList(
                KeyValue.pair(new Windowed<>(key, windows[0]), 50L),
                KeyValue.pair(new Windowed<>(key, windows[3]), 100L)
            ),
            results
        );

    }
```
```
Cyclomatic Complexity	 1
Assertions		 3
Lines of Code		23
-    
    public void shouldFetchAllSegments() {
        // just to validate directories
        final Segments segments = new Segments(storeName, retention, segmentInterval);
        final String key = "a";

        bytesStore.put(serializeKey(new Windowed<>(key, windows[0])), serializeValue(50L));
        assertEquals(Collections.singleton(segments.segmentName(0)), segmentDirs());

        bytesStore.put(serializeKey(new Windowed<>(key, windows[3])), serializeValue(100L));
        assertEquals(
            Utils.mkSet(
                segments.segmentName(0),
                segments.segmentName(1)
            ),
            segmentDirs()
        );

        final List<KeyValue<Windowed<String>, Long>> results = toList(bytesStore.fetchAll(0L, 60_000L));
        assertEquals(
            Arrays.asList(
                KeyValue.pair(new Windowed<>(key, windows[0]), 50L),
                KeyValue.pair(new Windowed<>(key, windows[3]), 100L)
            ),
            results
        );

    }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		35
-    
    public void shouldLoadSegmentsWithOldStyleDateFormattedName() {
        final Segments segments = new Segments(storeName, retention, segmentInterval);
        final String key = "a";

        bytesStore.put(serializeKey(new Windowed<>(key, windows[0])), serializeValue(50L));
        bytesStore.put(serializeKey(new Windowed<>(key, windows[3])), serializeValue(100L));
        bytesStore.close();

        final String firstSegmentName = segments.segmentName(0);
        final String[] nameParts = firstSegmentName.split("\\.");
        final Long segmentId = Long.parseLong(nameParts[1]);
        final SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmm");
        formatter.setTimeZone(new SimpleTimeZone(0, "UTC"));
        final String formatted = formatter.format(new Date(segmentId * segmentInterval));
        final File parent = new File(stateDir, storeName);
        final File oldStyleName = new File(parent, nameParts[0] + "-" + formatted);
        assertTrue(new File(parent, firstSegmentName).renameTo(oldStyleName));

        bytesStore = new RocksDBSegmentedBytesStore(
            storeName,
            "metrics-scope",
            retention,
            segmentInterval,
            schema
        );

        bytesStore.init(context, bytesStore);
        final List<KeyValue<Windowed<String>, Long>> results = toList(bytesStore.fetch(Bytes.wrap(key.getBytes()), 0L, 60_000L));
        assertThat(
            results,
            equalTo(
                Arrays.asList(
                    KeyValue.pair(new Windowed<>(key, windows[0]), 50L),
                    KeyValue.pair(new Windowed<>(key, windows[3]), 100L)
                )
            )
        );
    }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		31
-    
    public void shouldLoadSegmentsWithOldStyleColonFormattedName() {
        final Segments segments = new Segments(storeName, retention, segmentInterval);
        final String key = "a";

        bytesStore.put(serializeKey(new Windowed<>(key, windows[0])), serializeValue(50L));
        bytesStore.put(serializeKey(new Windowed<>(key, windows[3])), serializeValue(100L));
        bytesStore.close();

        final String firstSegmentName = segments.segmentName(0);
        final String[] nameParts = firstSegmentName.split("\\.");
        final File parent = new File(stateDir, storeName);
        final File oldStyleName = new File(parent, nameParts[0] + ":" + Long.parseLong(nameParts[1]));
        assertTrue(new File(parent, firstSegmentName).renameTo(oldStyleName));

        bytesStore = new RocksDBSegmentedBytesStore(
            storeName,
            "metrics-scope",
            retention,
            segmentInterval,
            schema
        );

        bytesStore.init(context, bytesStore);
        final List<KeyValue<Windowed<String>, Long>> results = toList(bytesStore.fetch(Bytes.wrap(key.getBytes()), 0L, 60_000L));
        assertThat(
            results,
            equalTo(
                Arrays.asList(
                    KeyValue.pair(new Windowed<>(key, windows[0]), 50L),
                    KeyValue.pair(new Windowed<>(key, windows[3]), 100L)
                )
            )
        );
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		8
-    
    public void shouldBeAbleToWriteToReInitializedStore() {
        final String key = "a";
        // need to create a segment so we can attempt to write to it again.
        bytesStore.put(serializeKey(new Windowed<>(key, windows[0])), serializeValue(50));
        bytesStore.close();
        bytesStore.init(context, bytesStore);
        bytesStore.put(serializeKey(new Windowed<>(key, windows[1])), serializeValue(100));
    }
```
```
Cyclomatic Complexity	 2
Assertions		 2
Lines of Code		12
-    
    public void shouldCreateWriteBatches() {
        final String key = "a";
        final Collection<KeyValue<byte[], byte[]>> records = new ArrayList<>();
        records.add(new KeyValue<>(serializeKey(new Windowed<>(key, windows[0])).get(), serializeValue(50L)));
        records.add(new KeyValue<>(serializeKey(new Windowed<>(key, windows[3])).get(), serializeValue(100L)));
        final Map<Segment, WriteBatch> writeBatchMap = bytesStore.getWriteBatches(records);
        assertEquals(2, writeBatchMap.size());
        for (final WriteBatch batch : writeBatchMap.values()) {
            assertEquals(1, batch.count());
        }
    }
```
```
Cyclomatic Complexity	 2
Assertions		 4
Lines of Code		18
-    
    public void shouldRestoreToByteStore() {
        // 0 segments initially.
        assertEquals(0, bytesStore.getSegments().size());
        final String key = "a";
        final Collection<KeyValue<byte[], byte[]>> records = new ArrayList<>();
        records.add(new KeyValue<>(serializeKey(new Windowed<>(key, windows[0])).get(), serializeValue(50L)));
        records.add(new KeyValue<>(serializeKey(new Windowed<>(key, windows[3])).get(), serializeValue(100L)));
        bytesStore.restoreAllInternal(records);

        // 2 segments are created during restoration.
        assertEquals(2, bytesStore.getSegments().size());

        // Bulk loading is enabled during recovery.
        for (final Segment segment : bytesStore.getSegments()) {
            Assert.assertThat(segment.getOptions().level0FileNumCompactionTrigger(), equalTo(1 << 30));
        }

        final List<KeyValue<Windowed<String>, Long>> expected = new ArrayList<>();
        expected.add(new KeyValue<>(new Windowed<>(key, windows[0]), 50L));
        expected.add(new KeyValue<>(new Windowed<>(key, windows[3]), 100L));

        final List<KeyValue<Windowed<String>, Long>> results = toList(bytesStore.all());
        assertEquals(expected, results);
    }
```
```
Cyclomatic Complexity	 3
Assertions		 3
Lines of Code		17
-    
    public void shouldRespectBulkLoadOptionsDuringInit() {
        bytesStore.init(context, bytesStore);
        final String key = "a";
        bytesStore.put(serializeKey(new Windowed<>(key, windows[0])), serializeValue(50L));
        bytesStore.put(serializeKey(new Windowed<>(key, windows[3])), serializeValue(100L));
        assertEquals(2, bytesStore.getSegments().size());

        final StateRestoreListener restoreListener = context.getRestoreListener(bytesStore.name());

        restoreListener.onRestoreStart(null, bytesStore.name(), 0L, 0L);

        for (final Segment segment : bytesStore.getSegments()) {
            Assert.assertThat(segment.getOptions().level0FileNumCompactionTrigger(), equalTo(1 << 30));
        }

        restoreListener.onRestoreEnd(null, bytesStore.name(), 0L);
        for (final Segment segment : bytesStore.getSegments()) {
            Assert.assertThat(segment.getOptions().level0FileNumCompactionTrigger(), equalTo(4));
        }
    }
```
```
Cyclomatic Complexity	 1
Assertions		 3
Lines of Code		35
-    
    public void shouldLogAndMeasureExpiredRecords() {
        LogCaptureAppender.setClassLoggerToDebug(RocksDBSegmentedBytesStore.class);
        final LogCaptureAppender appender = LogCaptureAppender.createAndRegister();

        // write a record to advance stream time, with a high enough timestamp
        // that the subsequent record in windows[0] will already be expired.
        bytesStore.put(serializeKey(new Windowed<>("dummy", nextSegmentWindow)), serializeValue(0));

        final Bytes key = serializeKey(new Windowed<>("a", windows[0]));
        final byte[] value = serializeValue(5);
        bytesStore.put(key, value);

        LogCaptureAppender.unregister(appender);

        final Map<MetricName, ? extends Metric> metrics = context.metrics().metrics();

        final Metric dropTotal = metrics.get(new MetricName(
            "expired-window-record-drop-total",
            "stream-metrics-scope-metrics",
            "The total number of occurrence of expired-window-record-drop operations.",
            mkMap(
                mkEntry("client-id", "mock"),
                mkEntry("task-id", "0_0"),
                mkEntry("metrics-scope-id", "bytes-store")
            )
        ));

        final Metric dropRate = metrics.get(new MetricName(
            "expired-window-record-drop-rate",
            "stream-metrics-scope-metrics",
            "The average number of occurrence of expired-window-record-drop operation per second.",
            mkMap(
                mkEntry("client-id", "mock"),
                mkEntry("task-id", "0_0"),
                mkEntry("metrics-scope-id", "bytes-store")
            )
        ));

        assertEquals(1.0, dropTotal.metricValue());
        assertNotEquals(0.0, dropRate.metricValue());
        final List<String> messages = appender.getMessages();
        assertThat(messages, hasItem("Skipping record for expired segment."));
    }
```
```
Cyclomatic Complexity	 2
Assertions		 1
Lines of Code		14
-    
    public void shouldFetchAllSessionsWithSameRecordKey() {

        final List<KeyValue<Windowed<String>, Long>> expected = Arrays.asList(KeyValue.pair(new Windowed<>("a", new SessionWindow(0, 0)), 1L),
                                                                                    KeyValue.pair(new Windowed<>("a", new SessionWindow(10, 10)), 2L),
                                                                                    KeyValue.pair(new Windowed<>("a", new SessionWindow(100, 100)), 3L),
                                                                                    KeyValue.pair(new Windowed<>("a", new SessionWindow(1000, 1000)), 4L));
        for (final KeyValue<Windowed<String>, Long> kv : expected) {
            sessionStore.put(kv.key, kv.value);
        }

        // add one that shouldn't appear in the results
        sessionStore.put(new Windowed<>("aa", new SessionWindow(0, 0)), 5L);

        try (final KeyValueIterator<Windowed<String>, Long> values = sessionStore.fetch("a")) {
            assertEquals(expected, toList(values));
        }
    }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		13
-    
    public void shouldFindValuesWithinMergingSessionWindowRange() {
        final String key = "a";
        sessionStore.put(new Windowed<>(key, new SessionWindow(0L, 0L)), 1L);
        sessionStore.put(new Windowed<>(key, new SessionWindow(1000L, 1000L)), 2L);

        final List<KeyValue<Windowed<String>, Long>> expected = Arrays.asList(
            KeyValue.pair(new Windowed<>(key, new SessionWindow(0L, 0L)), 1L),
            KeyValue.pair(new Windowed<>(key, new SessionWindow(1000L, 1000L)), 2L));

        try (final KeyValueIterator<Windowed<String>, Long> results =
                 sessionStore.findSessions(key, -1, 1000L)) {
            assertEquals(expected, toList(results));
        }
    }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		14
-    
    public void shouldRemove() {
        sessionStore.put(new Windowed<>("a", new SessionWindow(0, 1000)), 1L);
        sessionStore.put(new Windowed<>("a", new SessionWindow(1500, 2500)), 2L);

        sessionStore.remove(new Windowed<>("a", new SessionWindow(0, 1000)));

        try (final KeyValueIterator<Windowed<String>, Long> results =
                 sessionStore.findSessions("a", 0L, 1000L)) {
            assertFalse(results.hasNext());
        }

        try (final KeyValueIterator<Windowed<String>, Long> results =
                 sessionStore.findSessions("a", 1500L, 2500L)) {
            assertTrue(results.hasNext());
        }
    }
```
```
Cyclomatic Complexity	 1
Assertions		 3
Lines of Code		20
-    
    public void shouldFindSessionsToMerge() {
        final Windowed<String> session1 = new Windowed<>("a", new SessionWindow(0, 100));
        final Windowed<String> session2 = new Windowed<>("a", new SessionWindow(101, 200));
        final Windowed<String> session3 = new Windowed<>("a", new SessionWindow(201, 300));
        final Windowed<String> session4 = new Windowed<>("a", new SessionWindow(301, 400));
        final Windowed<String> session5 = new Windowed<>("a", new SessionWindow(401, 500));
        sessionStore.put(session1, 1L);
        sessionStore.put(session2, 2L);
        sessionStore.put(session3, 3L);
        sessionStore.put(session4, 4L);
        sessionStore.put(session5, 5L);

        try (final KeyValueIterator<Windowed<String>, Long> results =
                 sessionStore.findSessions("a", 150, 300)
        ) {
            assertEquals(session2, results.next().key);
            assertEquals(session3, results.next().key);
            assertFalse(results.hasNext());
        }
    }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		15
-    
    public void shouldGetAll() {
        windowStore = createWindowStore(context, false);
        final long startTime = segmentInterval - 4L;

        putFirstBatch(windowStore, startTime, context);

        final KeyValue<Windowed<Integer>, String> zero = windowedPair(0, "zero", startTime + 0);
        final KeyValue<Windowed<Integer>, String> one = windowedPair(1, "one", startTime + 1);
        final KeyValue<Windowed<Integer>, String> two = windowedPair(2, "two", startTime + 2);
        final KeyValue<Windowed<Integer>, String> four = windowedPair(4, "four", startTime + 4);
        final KeyValue<Windowed<Integer>, String> five = windowedPair(5, "five", startTime + 5);

        assertEquals(
            Utils.mkList(zero, one, two, four, five),
            StreamsTestUtils.toList(windowStore.all())
        );
    }
```
```
Cyclomatic Complexity	 1
Assertions		 3
Lines of Code		23
-    
    public void shouldFetchAllInTimeRange() {
        windowStore = createWindowStore(context, false);
        final long startTime = segmentInterval - 4L;

        putFirstBatch(windowStore, startTime, context);

        final KeyValue<Windowed<Integer>, String> zero = windowedPair(0, "zero", startTime + 0);
        final KeyValue<Windowed<Integer>, String> one = windowedPair(1, "one", startTime + 1);
        final KeyValue<Windowed<Integer>, String> two = windowedPair(2, "two", startTime + 2);
        final KeyValue<Windowed<Integer>, String> four = windowedPair(4, "four", startTime + 4);
        final KeyValue<Windowed<Integer>, String> five = windowedPair(5, "five", startTime + 5);

        assertEquals(
            Utils.mkList(one, two, four),
            StreamsTestUtils.toList(windowStore.fetchAll(ofEpochMilli(startTime + 1), ofEpochMilli(startTime + 4)))
        );

        assertEquals(
            Utils.mkList(zero, one, two),
            StreamsTestUtils.toList(windowStore.fetchAll(ofEpochMilli(startTime + 0), ofEpochMilli(startTime + 3)))
        );

        assertEquals(
            Utils.mkList(one, two, four, five),
            StreamsTestUtils.toList(windowStore.fetchAll(ofEpochMilli(startTime + 1), ofEpochMilli(startTime + 5)))
        );
    }
```
```
Cyclomatic Complexity	 1
Assertions		 8
Lines of Code		44
-    
    public void testFetchRange() {
        windowStore = createWindowStore(context, false);
        final long startTime = segmentInterval - 4L;

        putFirstBatch(windowStore, startTime, context);

        final KeyValue<Windowed<Integer>, String> zero = windowedPair(0, "zero", startTime + 0);
        final KeyValue<Windowed<Integer>, String> one = windowedPair(1, "one", startTime + 1);
        final KeyValue<Windowed<Integer>, String> two = windowedPair(2, "two", startTime + 2);
        final KeyValue<Windowed<Integer>, String> four = windowedPair(4, "four", startTime + 4);
        final KeyValue<Windowed<Integer>, String> five = windowedPair(5, "five", startTime + 5);

        assertEquals(
            Utils.mkList(zero, one),
            StreamsTestUtils.toList(windowStore.fetch(0, 1, ofEpochMilli(startTime + 0L - windowSize), ofEpochMilli(startTime + 0L + windowSize)))
        );
        assertEquals(
            Utils.mkList(one),
            StreamsTestUtils.toList(windowStore.fetch(1, 1, ofEpochMilli(startTime + 0L - windowSize), ofEpochMilli(startTime + 0L + windowSize)))
        );
        assertEquals(
            Utils.mkList(one, two),
            StreamsTestUtils.toList(windowStore.fetch(1, 3, ofEpochMilli(startTime + 0L - windowSize), ofEpochMilli(startTime + 0L + windowSize)))
        );
        assertEquals(
            Utils.mkList(zero, one, two),
            StreamsTestUtils.toList(windowStore.fetch(0, 5, ofEpochMilli(startTime + 0L - windowSize), ofEpochMilli(startTime + 0L + windowSize)))
        );
        assertEquals(
            Utils.mkList(zero, one, two,
                four, five),
            StreamsTestUtils.toList(windowStore.fetch(0, 5, ofEpochMilli(startTime + 0L - windowSize), ofEpochMilli(startTime + 0L + windowSize + 5L)))
        );
        assertEquals(
            Utils.mkList(two, four, five),
            StreamsTestUtils.toList(windowStore.fetch(0, 5, ofEpochMilli(startTime + 2L), ofEpochMilli(startTime + 0L + windowSize + 5L)))
        );
        assertEquals(
            Utils.mkList(),
            StreamsTestUtils.toList(windowStore.fetch(4, 5, ofEpochMilli(startTime + 2L), ofEpochMilli(startTime + windowSize)))
        );
        assertEquals(
            Utils.mkList(),
            StreamsTestUtils.toList(windowStore.fetch(0, 3, ofEpochMilli(startTime + 3L), ofEpochMilli(startTime + windowSize + 5)))
        );
    }
```
```
Cyclomatic Complexity	 1
Assertions		 29
Lines of Code		37
-    
    public void testPutAndFetchBefore() {
        windowStore = createWindowStore(context, false);
        final long startTime = segmentInterval - 4L;

        putFirstBatch(windowStore, startTime, context);

        assertEquals(Utils.mkList("zero"), toList(windowStore.fetch(0, ofEpochMilli(startTime + 0L - windowSize), ofEpochMilli(startTime + 0L))));
        assertEquals(Utils.mkList("one"), toList(windowStore.fetch(1, ofEpochMilli(startTime + 1L - windowSize), ofEpochMilli(startTime + 1L))));
        assertEquals(Utils.mkList("two"), toList(windowStore.fetch(2, ofEpochMilli(startTime + 2L - windowSize), ofEpochMilli(startTime + 2L))));
        assertEquals(Utils.mkList(), toList(windowStore.fetch(3, ofEpochMilli(startTime + 3L - windowSize), ofEpochMilli(startTime + 3L))));
        assertEquals(Utils.mkList("four"), toList(windowStore.fetch(4, ofEpochMilli(startTime + 4L - windowSize), ofEpochMilli(startTime + 4L))));
        assertEquals(Utils.mkList("five"), toList(windowStore.fetch(5, ofEpochMilli(startTime + 5L - windowSize), ofEpochMilli(startTime + 5L))));

        putSecondBatch(windowStore, startTime, context);

        assertEquals(Utils.mkList(), toList(windowStore.fetch(2, ofEpochMilli(startTime - 1L - windowSize), ofEpochMilli(startTime - 1L))));
        assertEquals(Utils.mkList(), toList(windowStore.fetch(2, ofEpochMilli(startTime + 0L - windowSize), ofEpochMilli(startTime + 0L))));
        assertEquals(Utils.mkList(), toList(windowStore.fetch(2, ofEpochMilli(startTime + 1L - windowSize), ofEpochMilli(startTime + 1L))));
        assertEquals(Utils.mkList("two"), toList(windowStore.fetch(2, ofEpochMilli(startTime + 2L - windowSize), ofEpochMilli(startTime + 2L))));
        assertEquals(Utils.mkList("two", "two+1"), toList(windowStore.fetch(2, ofEpochMilli(startTime + 3L - windowSize), ofEpochMilli(startTime + 3L))));
        assertEquals(Utils.mkList("two", "two+1", "two+2"), toList(windowStore.fetch(2, ofEpochMilli(startTime + 4L - windowSize), ofEpochMilli(startTime + 4L))));
        assertEquals(Utils.mkList("two", "two+1", "two+2", "two+3"), toList(windowStore.fetch(2, ofEpochMilli(startTime + 5L - windowSize), ofEpochMilli(startTime + 5L))));
        assertEquals(Utils.mkList("two+1", "two+2", "two+3", "two+4"), toList(windowStore.fetch(2, ofEpochMilli(startTime + 6L - windowSize), ofEpochMilli(startTime + 6L))));
        assertEquals(Utils.mkList("two+2", "two+3", "two+4", "two+5"), toList(windowStore.fetch(2, ofEpochMilli(startTime + 7L - windowSize), ofEpochMilli(startTime + 7L))));
        assertEquals(Utils.mkList("two+3", "two+4", "two+5", "two+6"), toList(windowStore.fetch(2, ofEpochMilli(startTime + 8L - windowSize), ofEpochMilli(startTime + 8L))));
        assertEquals(Utils.mkList("two+4", "two+5", "two+6"), toList(windowStore.fetch(2, ofEpochMilli(startTime + 9L - windowSize), ofEpochMilli(startTime + 9L))));
        assertEquals(Utils.mkList("two+5", "two+6"), toList(windowStore.fetch(2, ofEpochMilli(startTime + 10L - windowSize), ofEpochMilli(startTime + 10L))));
        assertEquals(Utils.mkList("two+6"), toList(windowStore.fetch(2, ofEpochMilli(startTime + 11L - windowSize), ofEpochMilli(startTime + 11L))));
        assertEquals(Utils.mkList(), toList(windowStore.fetch(2, ofEpochMilli(startTime + 12L - windowSize), ofEpochMilli(startTime + 12L))));
        assertEquals(Utils.mkList(), toList(windowStore.fetch(2, ofEpochMilli(startTime + 13L - windowSize), ofEpochMilli(startTime + 13L))));

        // Flush the store and verify all current entries were properly flushed ...
        windowStore.flush();

        final Map<Integer, Set<String>> entriesByKey = entriesByKey(changeLog, startTime);

        assertEquals(Utils.mkSet("zero@0"), entriesByKey.get(0));
        assertEquals(Utils.mkSet("one@1"), entriesByKey.get(1));
        assertEquals(Utils.mkSet("two@2", "two+1@3", "two+2@4", "two+3@5", "two+4@6", "two+5@7", "two+6@8"), entriesByKey.get(2));
        assertNull(entriesByKey.get(3));
        assertEquals(Utils.mkSet("four@4"), entriesByKey.get(4));
        assertEquals(Utils.mkSet("five@5"), entriesByKey.get(5));
        assertNull(entriesByKey.get(6));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 29
Lines of Code		37
-    
    public void testPutAndFetchAfter() {
        windowStore = createWindowStore(context, false);
        final long startTime = segmentInterval - 4L;

        putFirstBatch(windowStore, startTime, context);

        assertEquals(Utils.mkList("zero"), toList(windowStore.fetch(0, ofEpochMilli(startTime + 0L), ofEpochMilli(startTime + 0L + windowSize))));
        assertEquals(Utils.mkList("one"), toList(windowStore.fetch(1, ofEpochMilli(startTime + 1L), ofEpochMilli(startTime + 1L + windowSize))));
        assertEquals(Utils.mkList("two"), toList(windowStore.fetch(2, ofEpochMilli(startTime + 2L), ofEpochMilli(startTime + 2L + windowSize))));
        assertEquals(Utils.mkList(), toList(windowStore.fetch(3, ofEpochMilli(startTime + 3L), ofEpochMilli(startTime + 3L + windowSize))));
        assertEquals(Utils.mkList("four"), toList(windowStore.fetch(4, ofEpochMilli(startTime + 4L), ofEpochMilli(startTime + 4L + windowSize))));
        assertEquals(Utils.mkList("five"), toList(windowStore.fetch(5, ofEpochMilli(startTime + 5L), ofEpochMilli(startTime + 5L + windowSize))));

        putSecondBatch(windowStore, startTime, context);

        assertEquals(Utils.mkList(), toList(windowStore.fetch(2, ofEpochMilli(startTime - 2L), ofEpochMilli(startTime - 2L + windowSize))));
        assertEquals(Utils.mkList("two"), toList(windowStore.fetch(2, ofEpochMilli(startTime - 1L), ofEpochMilli(startTime - 1L + windowSize))));
        assertEquals(Utils.mkList("two", "two+1"), toList(windowStore.fetch(2, ofEpochMilli(startTime), ofEpochMilli(startTime + windowSize))));
        assertEquals(Utils.mkList("two", "two+1", "two+2"), toList(windowStore.fetch(2, ofEpochMilli(startTime + 1L), ofEpochMilli(startTime + 1L + windowSize))));
        assertEquals(Utils.mkList("two", "two+1", "two+2", "two+3"), toList(windowStore.fetch(2, ofEpochMilli(startTime + 2L), ofEpochMilli(startTime + 2L + windowSize))));
        assertEquals(Utils.mkList("two+1", "two+2", "two+3", "two+4"), toList(windowStore.fetch(2, ofEpochMilli(startTime + 3L), ofEpochMilli(startTime + 3L + windowSize))));
        assertEquals(Utils.mkList("two+2", "two+3", "two+4", "two+5"), toList(windowStore.fetch(2, ofEpochMilli(startTime + 4L), ofEpochMilli(startTime + 4L + windowSize))));
        assertEquals(Utils.mkList("two+3", "two+4", "two+5", "two+6"), toList(windowStore.fetch(2, ofEpochMilli(startTime + 5L), ofEpochMilli(startTime + 5L + windowSize))));
        assertEquals(Utils.mkList("two+4", "two+5", "two+6"), toList(windowStore.fetch(2, ofEpochMilli(startTime + 6L), ofEpochMilli(startTime + 6L + windowSize))));
        assertEquals(Utils.mkList("two+5", "two+6"), toList(windowStore.fetch(2, ofEpochMilli(startTime + 7L), ofEpochMilli(startTime + 7L + windowSize))));
        assertEquals(Utils.mkList("two+6"), toList(windowStore.fetch(2, ofEpochMilli(startTime + 8L), ofEpochMilli(startTime + 8L + windowSize))));
        assertEquals(Utils.mkList(), toList(windowStore.fetch(2, ofEpochMilli(startTime + 9L), ofEpochMilli(startTime + 9L + windowSize))));
        assertEquals(Utils.mkList(), toList(windowStore.fetch(2, ofEpochMilli(startTime + 10L), ofEpochMilli(startTime + 10L + windowSize))));
        assertEquals(Utils.mkList(), toList(windowStore.fetch(2, ofEpochMilli(startTime + 11L), ofEpochMilli(startTime + 11L + windowSize))));
        assertEquals(Utils.mkList(), toList(windowStore.fetch(2, ofEpochMilli(startTime + 12L), ofEpochMilli(startTime + 12L + windowSize))));

        // Flush the store and verify all current entries were properly flushed ...
        windowStore.flush();

        final Map<Integer, Set<String>> entriesByKey = entriesByKey(changeLog, startTime);

        assertEquals(Utils.mkSet("zero@0"), entriesByKey.get(0));
        assertEquals(Utils.mkSet("one@1"), entriesByKey.get(1));
        assertEquals(Utils.mkSet("two@2", "two+1@3", "two+2@4", "two+3@5", "two+4@6", "two+5@7", "two+6@8"), entriesByKey.get(2));
        assertNull(entriesByKey.get(3));
        assertEquals(Utils.mkSet("four@4"), entriesByKey.get(4));
        assertEquals(Utils.mkSet("five@5"), entriesByKey.get(5));
        assertNull(entriesByKey.get(6));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 8
Lines of Code		19
-    
    public void testPutSameKeyTimestamp() {
        windowStore = createWindowStore(context, true);
        final long startTime = segmentInterval - 4L;

        setCurrentTime(startTime);
        windowStore.put(0, "zero");

        assertEquals(Utils.mkList("zero"), toList(windowStore.fetch(0, ofEpochMilli(startTime - windowSize), ofEpochMilli(startTime + windowSize))));

        windowStore.put(0, "zero");
        windowStore.put(0, "zero+");
        windowStore.put(0, "zero++");

        assertEquals(Utils.mkList("zero", "zero", "zero+", "zero++"), toList(windowStore.fetch(0, ofEpochMilli(startTime - windowSize), ofEpochMilli(startTime + windowSize))));
        assertEquals(Utils.mkList("zero", "zero", "zero+", "zero++"), toList(windowStore.fetch(0, ofEpochMilli(startTime + 1L - windowSize), ofEpochMilli(startTime + 1L + windowSize))));
        assertEquals(Utils.mkList("zero", "zero", "zero+", "zero++"), toList(windowStore.fetch(0, ofEpochMilli(startTime + 2L - windowSize), ofEpochMilli(startTime + 2L + windowSize))));
        assertEquals(Utils.mkList("zero", "zero", "zero+", "zero++"), toList(windowStore.fetch(0, ofEpochMilli(startTime + 3L - windowSize), ofEpochMilli(startTime + 3L + windowSize))));
        assertEquals(Utils.mkList(), toList(windowStore.fetch(0, ofEpochMilli(startTime + 4L - windowSize), ofEpochMilli(startTime + 4L + windowSize))));

        // Flush the store and verify all current entries were properly flushed ...
        windowStore.flush();

        final Map<Integer, Set<String>> entriesByKey = entriesByKey(changeLog, startTime);

        assertEquals(Utils.mkSet("zero@0", "zero@0", "zero+@0", "zero++@0"), entriesByKey.get(0));
    }
```
```
Cyclomatic Complexity	 4
Assertions		 9
Lines of Code		63
-    
    public void testSegmentMaintenance() {
        windowStore = createWindowStore(context, true);
        context.setTime(0L);
        setCurrentTime(0);
        windowStore.put(0, "v");
        assertEquals(
            Utils.mkSet(segments.segmentName(0L)),
            segmentDirs(baseDir)
        );

        setCurrentTime(segmentInterval - 1);
        windowStore.put(0, "v");
        windowStore.put(0, "v");
        assertEquals(
            Utils.mkSet(segments.segmentName(0L)),
            segmentDirs(baseDir)
        );

        setCurrentTime(segmentInterval);
        windowStore.put(0, "v");
        assertEquals(
            Utils.mkSet(segments.segmentName(0L), segments.segmentName(1L)),
            segmentDirs(baseDir)
        );

        WindowStoreIterator iter;
        int fetchedCount;

        iter = windowStore.fetch(0, ofEpochMilli(0L), ofEpochMilli(segmentInterval * 4));
        fetchedCount = 0;
        while (iter.hasNext()) {
            iter.next();
            fetchedCount++;
        }
        assertEquals(4, fetchedCount);

        assertEquals(
            Utils.mkSet(segments.segmentName(0L), segments.segmentName(1L)),
            segmentDirs(baseDir)
        );

        setCurrentTime(segmentInterval * 3);
        windowStore.put(0, "v");

        iter = windowStore.fetch(0, ofEpochMilli(0L), ofEpochMilli(segmentInterval * 4));
        fetchedCount = 0;
        while (iter.hasNext()) {
            iter.next();
            fetchedCount++;
        }
        assertEquals(2, fetchedCount);

        assertEquals(
            Utils.mkSet(segments.segmentName(1L), segments.segmentName(3L)),
            segmentDirs(baseDir)
        );

        setCurrentTime(segmentInterval * 5);
        windowStore.put(0, "v");

        iter = windowStore.fetch(0, ofEpochMilli(segmentInterval * 4), ofEpochMilli(segmentInterval * 10));
        fetchedCount = 0;
        while (iter.hasNext()) {
            iter.next();
            fetchedCount++;
        }
        assertEquals(1, fetchedCount);

        assertEquals(
            Utils.mkSet(segments.segmentName(3L), segments.segmentName(5L)),
            segmentDirs(baseDir)
        );

    }
```
```
Cyclomatic Complexity	 2
Assertions		 2
Lines of Code		29
-    
    public void testInitialLoading() {
        final File storeDir = new File(baseDir, windowName);

        windowStore = createWindowStore(context, false);

        new File(storeDir, segments.segmentName(0L)).mkdir();
        new File(storeDir, segments.segmentName(1L)).mkdir();
        new File(storeDir, segments.segmentName(2L)).mkdir();
        new File(storeDir, segments.segmentName(3L)).mkdir();
        new File(storeDir, segments.segmentName(4L)).mkdir();
        new File(storeDir, segments.segmentName(5L)).mkdir();
        new File(storeDir, segments.segmentName(6L)).mkdir();
        windowStore.close();

        windowStore = createWindowStore(context, false);

        // put something in the store to advance its stream time and expire the old segments
        windowStore.put(1, "v", 6L * segmentInterval);

        final List<String> expected = Utils.mkList(segments.segmentName(4L), segments.segmentName(5L), segments.segmentName(6L));
        expected.sort(String::compareTo);

        final List<String> actual = Utils.toList(segmentDirs(baseDir).iterator());
        actual.sort(String::compareTo);

        assertEquals(expected, actual);

        try (final WindowStoreIterator iter = windowStore.fetch(0, ofEpochMilli(0L), ofEpochMilli(1000000L))) {
            while (iter.hasNext()) {
                iter.next();
            }
        }

        assertEquals(
            Utils.mkSet(segments.segmentName(4L), segments.segmentName(5L), segments.segmentName(6L)),
            segmentDirs(baseDir)
        );
    }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		12
-    
    public void shouldCloseOpenIteratorsWhenStoreIsClosedAndNotThrowInvalidStateStoreExceptionOnHasNext() {
        windowStore = createWindowStore(context, false);
        setCurrentTime(0);
        windowStore.put(1, "one", 1L);
        windowStore.put(1, "two", 2L);
        windowStore.put(1, "three", 3L);

        final WindowStoreIterator<String> iterator = windowStore.fetch(1, ofEpochMilli(1L), ofEpochMilli(3L));
        assertTrue(iterator.hasNext());
        windowStore.close();

        assertFalse(iterator.hasNext());

    }
```
```
Cyclomatic Complexity	 1
Assertions		 3
Lines of Code		29
-    
    public void shouldFetchAndIterateOverExactKeys() {
        final long windowSize = 0x7a00000000000000L;
        final long retentionPeriod = 0x7a00000000000000L;

        final WindowStore<String, String> windowStore = Stores.windowStoreBuilder(
            Stores.persistentWindowStore(windowName, ofMillis(retentionPeriod), ofMillis(windowSize), true),
            Serdes.String(),
            Serdes.String()).build();

        windowStore.init(context, windowStore);

        windowStore.put("a", "0001", 0);
        windowStore.put("aa", "0002", 0);
        windowStore.put("a", "0003", 1);
        windowStore.put("aa", "0004", 1);
        windowStore.put("a", "0005", 0x7a00000000000000L - 1);


        final List expected = Utils.mkList("0001", "0003", "0005");
        assertThat(toList(windowStore.fetch("a", ofEpochMilli(0), ofEpochMilli(Long.MAX_VALUE))), equalTo(expected));

        List<KeyValue<Windowed<String>, String>> list =
            StreamsTestUtils.toList(windowStore.fetch("a", "a", ofEpochMilli(0), ofEpochMilli(Long.MAX_VALUE)));
        assertThat(list, equalTo(Utils.mkList(
            windowedPair("a", "0001", 0, windowSize),
            windowedPair("a", "0003", 1, windowSize),
            windowedPair("a", "0005", 0x7a00000000000000L - 1, windowSize)
        )));

        list = StreamsTestUtils.toList(windowStore.fetch("aa", "aa", ofEpochMilli(0), ofEpochMilli(Long.MAX_VALUE)));
        assertThat(list, equalTo(Utils.mkList(
            windowedPair("aa", "0002", 0, windowSize),
            windowedPair("aa", "0004", 1, windowSize)
        )));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		5
-    (expected = NullPointerException.class)
    public void shouldThrowNullPointerExceptionOnPutNullKey() {
        windowStore = createWindowStore(context, false);
        windowStore.put(null, "anyValue");
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		5
-    
    public void shouldNotThrowNullPointerExceptionOnPutNullValue() {
        windowStore = createWindowStore(context, false);
        windowStore.put(1, null);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		5
-    (expected = NullPointerException.class)
    public void shouldThrowNullPointerExceptionOnGetNullKey() {
        windowStore = createWindowStore(context, false);
        windowStore.fetch(null, ofEpochMilli(1L), ofEpochMilli(2L));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		5
-    (expected = NullPointerException.class)
    public void shouldThrowNullPointerExceptionOnRangeNullFromKey() {
        windowStore = createWindowStore(context, false);
        windowStore.fetch(null, 2, ofEpochMilli(1L), ofEpochMilli(2L));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		5
-    (expected = NullPointerException.class)
    public void shouldThrowNullPointerExceptionOnRangeNullToKey() {
        windowStore = createWindowStore(context, false);
        windowStore.fetch(1, null, ofEpochMilli(1L), ofEpochMilli(2L));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		11
-    
    public void shouldNoNullPointerWhenSerdeDoesNotHandleNull() {
        windowStore = new RocksDBWindowStore<>(
            new RocksDBSegmentedBytesStore(windowName, "metrics-scope", retentionPeriod, segmentInterval, new WindowKeySchema()),
            Serdes.Integer(),
            new SerdeThatDoesntHandleNull(),
            false,
            windowSize);
        windowStore.init(context, windowStore);

        assertNull(windowStore.fetch(1, 0));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 3
Lines of Code		26
-    
    public void shouldFetchAndIterateOverExactBinaryKeys() {
        final WindowStore<Bytes, String> windowStore = Stores.windowStoreBuilder(
            Stores.persistentWindowStore(windowName, ofMillis(60_000L), ofMillis(60_000L), true),
            Serdes.Bytes(),
            Serdes.String()).build();

        windowStore.init(context, windowStore);

        final Bytes key1 = Bytes.wrap(new byte[] {0});
        final Bytes key2 = Bytes.wrap(new byte[] {0, 0});
        final Bytes key3 = Bytes.wrap(new byte[] {0, 0, 0});
        windowStore.put(key1, "1", 0);
        windowStore.put(key2, "2", 0);
        windowStore.put(key3, "3", 0);
        windowStore.put(key1, "4", 1);
        windowStore.put(key2, "5", 1);
        windowStore.put(key3, "6", 59999);
        windowStore.put(key1, "7", 59999);
        windowStore.put(key2, "8", 59999);
        windowStore.put(key3, "9", 59999);

        final List expectedKey1 = Utils.mkList("1", "4", "7");
        assertThat(toList(windowStore.fetch(key1, ofEpochMilli(0), ofEpochMilli(Long.MAX_VALUE))), equalTo(expectedKey1));
        final List expectedKey2 = Utils.mkList("2", "5", "8");
        assertThat(toList(windowStore.fetch(key2, ofEpochMilli(0), ofEpochMilli(Long.MAX_VALUE))), equalTo(expectedKey2));
        final List expectedKey3 = Utils.mkList("3", "6", "9");
        assertThat(toList(windowStore.fetch(key3, ofEpochMilli(0), ofEpochMilli(Long.MAX_VALUE))), equalTo(expectedKey3));
    }
```
