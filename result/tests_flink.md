## c11df3370b6d0d48baa93eb2d13c1785fb90652a ##
```
Cyclomatic Complexity	1
Assertions		2
Lines of Code		40
-	
	public void testCorrectNumOfCollectedRecordsAndUpdatedStateWithUnexpectedExpiredIterator() {
		StreamShardHandle fakeToBeConsumedShard = getMockStreamShard("fakeStream", 0);

		LinkedList<KinesisStreamShardState> subscribedShardsStateUnderTest = new LinkedList<>();
		subscribedShardsStateUnderTest.add(
			new KinesisStreamShardState(KinesisDataFetcher.convertToStreamShardMetadata(fakeToBeConsumedShard),
				fakeToBeConsumedShard, new SequenceNumber("fakeStartingState")));

		TestSourceContext<String> sourceContext = new TestSourceContext<>();

		KinesisDeserializationSchemaWrapper<String> deserializationSchema = new KinesisDeserializationSchemaWrapper<>(
			new SimpleStringSchema());
		TestableKinesisDataFetcher<String> fetcher =
			new TestableKinesisDataFetcher<>(
				Collections.singletonList("fakeStream"),
				sourceContext,
				new Properties(),
				deserializationSchema,
				10,
				2,
				new AtomicReference<>(),
				subscribedShardsStateUnderTest,
				KinesisDataFetcher.createInitialSubscribedStreamsToLastDiscoveredShardsState(Collections.singletonList("fakeStream")),
				Mockito.mock(KinesisProxyInterface.class));

		int shardIndex = fetcher.registerNewSubscribedShardState(subscribedShardsStateUnderTest.get(0));
		new ShardConsumer<>(
			fetcher,
			shardIndex,
			subscribedShardsStateUnderTest.get(0).getStreamShardHandle(),
			subscribedShardsStateUnderTest.get(0).getLastProcessedSequenceNum(),
			// Get a total of 1000 records with 9 getRecords() calls,
			// and the 7th getRecords() call will encounter an unexpected expired shard iterator
			FakeKinesisBehavioursFactory.totalNumOfRecordsAfterNumOfGetRecordsCallsWithUnexpectedExpiredIterator(
				1000, 9, 7, 500L),
			new ShardMetricsReporter(),
			deserializationSchema)
			.run();

		assertEquals(1000, sourceContext.getCollectedOutputs().size());
		assertEquals(
			SentinelSequenceNumber.SENTINEL_SHARD_ENDING_SEQUENCE_NUM.get(),
			subscribedShardsStateUnderTest.get(0).getLastProcessedSequenceNum());
	}
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		13
-	
	public void testEmptyLineDelimiter() throws Exception {
		DataType dataType = ROW(
			FIELD("f0", STRING()),
			FIELD("f1", INT()),
			FIELD("f2", STRING()));
		RowType rowType = (RowType) dataType.getLogicalType();
		CsvRowDataSerializationSchema.Builder serSchemaBuilder =
			new CsvRowDataSerializationSchema.Builder(rowType).setLineDelimiter("");

		assertArrayEquals(
			"Test,12,Hello".getBytes(),
			serialize(serSchemaBuilder, rowData("Test", 12, "Hello")));@@@ 	}
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		4
-	
	public void testConcurrentReadStateAndProcess() throws Exception {
		testConcurrentReadStateAndProcess(isRemote);
	}
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		4
-	
	public void testConcurrentReadStateAndRelease() throws Exception {
		testConcurrentReadStateAndRelease(isRemote);
	}
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		4
-	
	public void testConcurrentReadStateAndProcessAndRelease() throws Exception {
		testConcurrentReadStateAndProcessAndRelease(isRemote);
	}
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		4
-	
	public void testReadEmptyState() throws Exception {
		testReadEmptyStateOrThrowException(isRemote, ChannelStateReader.NO_OP);
	}
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		4
-	(expected = IOException.class)
	public void testReadStateWithException() throws Exception {
		testReadEmptyStateOrThrowException(isRemote, new ChannelStateReaderWithException());
	}
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		10
-	
	public void testSetScheduleMode() {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		StreamGraph streamGraph = new StreamGraphGenerator(Collections.emptyList(),
			env.getConfig(), env.getCheckpointConfig())
			.setScheduleMode(ScheduleMode.LAZY_FROM_SOURCES)
			.generate();
		JobGraph jobGraph = StreamingJobGraphGenerator.createJobGraph(streamGraph);
		assertEquals(ScheduleMode.LAZY_FROM_SOURCES, jobGraph.getScheduleMode());@@@ 	}
```
```
Cyclomatic Complexity	 1
Assertions		 4
Lines of Code		18
-  
  def testFoldEventTimeWindows(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)

    val source = env.fromElements(("hello", 1), ("hello", 2))

    val window1 = source
      .keyBy(0)
      .timeWindow(Time.of(1, TimeUnit.SECONDS), Time.of(100, TimeUnit.MILLISECONDS))
      .fold(("", "", 1), new DummyFolder())

    val transform1 = window1.javaStream.getTransformation
      .asInstanceOf[OneInputTransformation[(String, Int), (String, Int)]]

    val operator1 = transform1.getOperator

    assertTrue(operator1.isInstanceOf[WindowOperator[_, _, _, _, _]])

    val winOperator1 = operator1.asInstanceOf[WindowOperator[_, _, _, _, _]]

    assertTrue(winOperator1.getTrigger.isInstanceOf[EventTimeTrigger])
    assertTrue(winOperator1.getWindowAssigner.isInstanceOf[SlidingEventTimeWindows])
    assertTrue(winOperator1.getStateDescriptor.isInstanceOf[FoldingStateDescriptor[_, _]])@@@   }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		13
-  (expected = classOf[UnsupportedOperationException])
  def testFoldWithRichFolderFails() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val source = env.fromElements(("hello", 1), ("hello", 2))

    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    source
      .keyBy(0)
      .window(SlidingEventTimeWindows.of(Time.seconds(1), Time.milliseconds(100)))
      .fold(("", 0), new RichFoldFunction[(String, Int), (String, Int)] {
        override def fold(accumulator: (String, Int), value: (String, Int)) = null
      })

    fail("exception was not thrown")
  }
```
```
Cyclomatic Complexity	 2
Assertions		 1
Lines of Code		17
-  
  def testSessionWithFoldFails() {
    // verify that fold does not work with merging windows
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val windowedStream = env.fromElements("Hello", "Ciao")
      .keyBy(x => x)
      .window(EventTimeSessionWindows.withGap(Time.seconds(5)))

    try
      windowedStream.fold("", new FoldFunction[String, String]() {
        @throws[Exception]
        def fold(accumulator: String, value: String): String = accumulator
      })

    catch {
      case _: UnsupportedOperationException =>
        // expected
        // use a catch to ensure that the exception is thrown by the fold
        return
    }

    fail("The fold call should fail.")
  }
```
```
Cyclomatic Complexity	 1
Assertions		 4
Lines of Code		26
-  
  def testFoldEventTime() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)

    val source = env.fromElements(("hello", 1), ("hello", 2))

    val window1 = source
      .keyBy(_._1)
      .window(SlidingEventTimeWindows.of(Time.seconds(1), Time.milliseconds(100)))
      .fold(("", "", 1), new DummyFolder)

    val transform = window1
      .javaStream
      .getTransformation
      .asInstanceOf[OneInputTransformation[(String, Int), (String, Int)]]

    val operator = transform.getOperator
    assertTrue(operator.isInstanceOf[WindowOperator[_, _, _, _, _ <: Window]])

    val winOperator = operator
      .asInstanceOf[WindowOperator[String, (String, Int), _, (String, Int), _ <: Window]]

    assertTrue(winOperator.getTrigger.isInstanceOf[EventTimeTrigger])
    assertTrue(winOperator.getWindowAssigner.isInstanceOf[SlidingEventTimeWindows])
    assertTrue(winOperator.getStateDescriptor.isInstanceOf[FoldingStateDescriptor[_, _]])

    processElementAndEnsureOutput[String, (String, Int), (String, Int)](
      winOperator,
      winOperator.getKeySelector,
      BasicTypeInfo.STRING_TYPE_INFO,
      ("hello", 1))
  }
```
```
Cyclomatic Complexity	 1
Assertions		 4
Lines of Code		26
-  
  def testFoldProcessingTime() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    val source = env.fromElements(("hello", 1), ("hello", 2))

    val window1 = source
      .keyBy(_._1)
      .window(SlidingProcessingTimeWindows.of(Time.seconds(1), Time.milliseconds(100)))
      .fold(("", "", 1), new DummyFolder)

    val transform = window1
      .javaStream
      .getTransformation
      .asInstanceOf[OneInputTransformation[(String, Int), (String, Int)]]

    val operator = transform.getOperator
    assertTrue(operator.isInstanceOf[WindowOperator[_, _, _, _, _ <: Window]])

    val winOperator = operator
      .asInstanceOf[WindowOperator[String, (String, Int), _, (String, Int), _ <: Window]]

    assertTrue(winOperator.getTrigger.isInstanceOf[ProcessingTimeTrigger])
    assertTrue(winOperator.getWindowAssigner.isInstanceOf[SlidingProcessingTimeWindows])
    assertTrue(winOperator.getStateDescriptor.isInstanceOf[FoldingStateDescriptor[_, _]])

    processElementAndEnsureOutput[String, (String, Int), (String, Int)](
      winOperator,
      winOperator.getKeySelector,
      BasicTypeInfo.STRING_TYPE_INFO,
      ("hello", 1))
  }
```
```
Cyclomatic Complexity	 1
Assertions		 4
Lines of Code		26
-  
  def testFoldEventTimeWithScalaFunction() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)

    val source = env.fromElements(("hello", 1), ("hello", 2))

    val window1 = source
      .keyBy(_._1)
      .window(SlidingEventTimeWindows.of(Time.seconds(1), Time.milliseconds(100)))
      .fold(("", "", 1)) { (acc, _) => acc }

    val transform = window1
      .javaStream
      .getTransformation
      .asInstanceOf[OneInputTransformation[(String, Int), (String, Int)]]

    val operator = transform.getOperator
    assertTrue(operator.isInstanceOf[WindowOperator[_, _, _, _, _ <: Window]])

    val winOperator = operator
      .asInstanceOf[WindowOperator[String, (String, Int), _, (String, Int), _ <: Window]]

    assertTrue(winOperator.getTrigger.isInstanceOf[EventTimeTrigger])
    assertTrue(winOperator.getWindowAssigner.isInstanceOf[SlidingEventTimeWindows])
    assertTrue(winOperator.getStateDescriptor.isInstanceOf[FoldingStateDescriptor[_, _]])

    processElementAndEnsureOutput[String, (String, Int), (String, Int)](
      winOperator,
      winOperator.getKeySelector,
      BasicTypeInfo.STRING_TYPE_INFO,
      ("hello", 1))
  }
```
```
Cyclomatic Complexity	 1
Assertions		 4
Lines of Code		35
-  
  def testFoldWithWindowFunctionEventTime() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)

    val source = env.fromElements(("hello", 1), ("hello", 2))

    val window1 = source
      .keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(1), Time.milliseconds(100)))
      .fold(
        ("", "", 1),
        new DummyFolder,
        new WindowFunction[(String, String, Int), (String, Int), String, TimeWindow] {
          override def apply(
              key: String,
              window: TimeWindow,
              input: Iterable[(String, String, Int)],
              out: Collector[(String, Int)]): Unit = input foreach {x => out.collect((x._1, x._3))}
        })

    val transform = window1
      .javaStream
      .getTransformation
      .asInstanceOf[OneInputTransformation[(String, Int), (String, Int)]]

    val operator = transform.getOperator
    assertTrue(operator.isInstanceOf[WindowOperator[_, _, _, _, _ <: Window]])

    val winOperator = operator
      .asInstanceOf[WindowOperator[String, (String, Int), _, (String, Int), _ <: Window]]

    assertTrue(winOperator.getTrigger.isInstanceOf[EventTimeTrigger])
    assertTrue(winOperator.getWindowAssigner.isInstanceOf[TumblingEventTimeWindows])
    assertTrue(winOperator.getStateDescriptor.isInstanceOf[FoldingStateDescriptor[_, _]])

    processElementAndEnsureOutput[String, (String, Int), (String, Int)](
      winOperator,
      winOperator.getKeySelector,
      BasicTypeInfo.STRING_TYPE_INFO,
      ("hello", 1))
  }
```
```
Cyclomatic Complexity	 1
Assertions		 4
Lines of Code		35
-  
  def testFoldWithWindowFunctionProcessingTime() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    val source = env.fromElements(("hello", 1), ("hello", 2))

    val window1 = source
      .keyBy(_._1)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(1), Time.milliseconds(100)))
      .fold(
        ("", "", 1),
        new DummyFolder,
        new WindowFunction[(String, String, Int), (String, Int), String, TimeWindow] {
          override def apply(
              key: String,
              window: TimeWindow,
              input: Iterable[(String, String, Int)],
              out: Collector[(String, Int)]): Unit = input foreach {x => out.collect((x._1, x._3))}
        })

    val transform = window1
      .javaStream
      .getTransformation
      .asInstanceOf[OneInputTransformation[(String, Int), (String, Int)]]

    val operator = transform.getOperator
    assertTrue(operator.isInstanceOf[WindowOperator[_, _, _, _, _ <: Window]])

    val winOperator = operator
      .asInstanceOf[WindowOperator[String, (String, Int), _, (String, Int), _ <: Window]]

    assertTrue(winOperator.getTrigger.isInstanceOf[ProcessingTimeTrigger])
    assertTrue(winOperator.getWindowAssigner.isInstanceOf[TumblingProcessingTimeWindows])
    assertTrue(winOperator.getStateDescriptor.isInstanceOf[FoldingStateDescriptor[_, _]])

    processElementAndEnsureOutput[String, (String, Int), (String, Int)](
      winOperator,
      winOperator.getKeySelector,
      BasicTypeInfo.STRING_TYPE_INFO,
      ("hello", 1))
  }
```
```
Cyclomatic Complexity	 1
Assertions		 4
Lines of Code		35
-  
  def testFoldWithProcessWindowFunctionEventTime() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)

    val source = env.fromElements(("hello", 1), ("hello", 2))

    val window1 = source
      .keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(1), Time.milliseconds(100)))
      .fold(
        ("", "", 1),
        new DummyFolder,
        new ProcessWindowFunction[(String, String, Int), (String, Int), String, TimeWindow] {
          override def process(
              key: String,
              window: Context,
              input: Iterable[(String, String, Int)],
              out: Collector[(String, Int)]): Unit = input foreach {x => out.collect((x._1, x._3))}
        })

    val transform = window1
      .javaStream
      .getTransformation
      .asInstanceOf[OneInputTransformation[(String, Int), (String, Int)]]

    val operator = transform.getOperator
    assertTrue(operator.isInstanceOf[WindowOperator[_, _, _, _, _ <: Window]])

    val winOperator = operator
      .asInstanceOf[WindowOperator[String, (String, Int), _, (String, Int), _ <: Window]]

    assertTrue(winOperator.getTrigger.isInstanceOf[EventTimeTrigger])
    assertTrue(winOperator.getWindowAssigner.isInstanceOf[TumblingEventTimeWindows])
    assertTrue(winOperator.getStateDescriptor.isInstanceOf[FoldingStateDescriptor[_, _]])

    processElementAndEnsureOutput[String, (String, Int), (String, Int)](
      winOperator,
      winOperator.getKeySelector,
      BasicTypeInfo.STRING_TYPE_INFO,
      ("hello", 1))
  }
```
```
Cyclomatic Complexity	 1
Assertions		 4
Lines of Code		35
-  
  def testFoldWithProcessWindowFunctionProcessingTime() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    val source = env.fromElements(("hello", 1), ("hello", 2))

    val window1 = source
      .keyBy(_._1)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(1), Time.milliseconds(100)))
      .fold(
        ("", "", 1),
        new DummyFolder,
        new ProcessWindowFunction[(String, String, Int), (String, Int), String, TimeWindow] {
          override def process(
              key: String,
              window: Context,
              input: Iterable[(String, String, Int)],
              out: Collector[(String, Int)]): Unit = input foreach {x => out.collect((x._1, x._3))}
        })

    val transform = window1
      .javaStream
      .getTransformation
      .asInstanceOf[OneInputTransformation[(String, Int), (String, Int)]]

    val operator = transform.getOperator
    assertTrue(operator.isInstanceOf[WindowOperator[_, _, _, _, _ <: Window]])

    val winOperator = operator
      .asInstanceOf[WindowOperator[String, (String, Int), _, (String, Int), _ <: Window]]

    assertTrue(winOperator.getTrigger.isInstanceOf[ProcessingTimeTrigger])
    assertTrue(winOperator.getWindowAssigner.isInstanceOf[TumblingProcessingTimeWindows])
    assertTrue(winOperator.getStateDescriptor.isInstanceOf[FoldingStateDescriptor[_, _]])

    processElementAndEnsureOutput[String, (String, Int), (String, Int)](
      winOperator,
      winOperator.getKeySelector,
      BasicTypeInfo.STRING_TYPE_INFO,
      ("hello", 1))
  }
```
```
Cyclomatic Complexity	 1
Assertions		 4
Lines of Code		36
-  
  def testApplyWithPreFolderEventTime() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)

    val source = env.fromElements(("hello", 1), ("hello", 2))

    val window1 = source
      .keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(1), Time.milliseconds(100)))
      .apply(
        ("", "", 1),
        new DummyFolder,
        new WindowFunction[(String, String, Int), (String, String, Int), String, TimeWindow] {
          override def apply(
              key: String,
              window: TimeWindow,
              input: Iterable[(String, String, Int)],
              out: Collector[(String, String, Int)]): Unit =
            input foreach {x => out.collect((x._1, x._2, x._3))}
        })

    val transform = window1
      .javaStream
      .getTransformation
      .asInstanceOf[OneInputTransformation[(String, Int), (String, Int)]]

    val operator = transform.getOperator
    assertTrue(operator.isInstanceOf[WindowOperator[_, _, _, _, _ <: Window]])

    val winOperator = operator
      .asInstanceOf[WindowOperator[String, (String, Int), _, (String, Int), _ <: Window]]

    assertTrue(winOperator.getTrigger.isInstanceOf[EventTimeTrigger])
    assertTrue(winOperator.getWindowAssigner.isInstanceOf[TumblingEventTimeWindows])
    assertTrue(winOperator.getStateDescriptor.isInstanceOf[FoldingStateDescriptor[_, _]])

    processElementAndEnsureOutput[String, (String, Int), (String, Int)](
      winOperator,
      winOperator.getKeySelector,
      BasicTypeInfo.STRING_TYPE_INFO,
      ("hello", 1))
  }
```
```
Cyclomatic Complexity	 1
Assertions		 4
Lines of Code		37
-  
  def testApplyWithPreFolderAndEvictor() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)

    val source = env.fromElements(("hello", 1), ("hello", 2))

    val window1 = source
      .keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(1), Time.milliseconds(100)))
      .evictor(CountEvictor.of(100))
      .apply(
        ("", "", 1),
        new DummyFolder,
        new WindowFunction[(String, String, Int), (String, String, Int), String, TimeWindow] {
          override def apply(
                              key: String,
                              window: TimeWindow,
                              input: Iterable[(String, String, Int)],
                              out: Collector[(String, String, Int)]): Unit =
            input foreach {x => out.collect((x._1, x._2, x._3))}
        })

    val transform = window1
      .javaStream
      .getTransformation
      .asInstanceOf[OneInputTransformation[(String, Int), (String, Int)]]

    val operator = transform.getOperator
    assertTrue(operator.isInstanceOf[WindowOperator[_, _, _, _, _ <: Window]])

    val winOperator = operator
      .asInstanceOf[WindowOperator[String, (String, Int), _, (String, Int), _ <: Window]]

    assertTrue(winOperator.getTrigger.isInstanceOf[EventTimeTrigger])
    assertTrue(winOperator.getWindowAssigner.isInstanceOf[TumblingEventTimeWindows])
    assertTrue(winOperator.getStateDescriptor.isInstanceOf[ListStateDescriptor[_]])

    processElementAndEnsureOutput[String, (String, Int), (String, Int)](
      winOperator,
      winOperator.getKeySelector,
      BasicTypeInfo.STRING_TYPE_INFO,
      ("hello", 1))
  }
```
```
Cyclomatic Complexity	 1
Assertions		 4
Lines of Code		35
-  
  def testFoldWithWindowFunctionEventTimeWithScalaFunction() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)

    val source = env.fromElements(("hello", 1), ("hello", 2))

    val window1 = source
      .keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(1), Time.milliseconds(100)))
      .fold(
        ("", "", 1),
        { (acc: (String, String, Int), _) => acc },
        { (
            _: String,
            _: TimeWindow,
            in: Iterable[(String, String, Int)],
            out: Collector[(String, Int)]) =>
              in foreach { x => out.collect((x._1, x._3)) }
        })

    val transform = window1
      .javaStream
      .getTransformation
      .asInstanceOf[OneInputTransformation[(String, Int), (String, Int)]]

    val operator = transform.getOperator
    assertTrue(operator.isInstanceOf[WindowOperator[_, _, _, _, _ <: Window]])

    val winOperator = operator
      .asInstanceOf[WindowOperator[String, (String, Int), _, (String, Int), _ <: Window]]

    assertTrue(winOperator.getTrigger.isInstanceOf[EventTimeTrigger])
    assertTrue(winOperator.getWindowAssigner.isInstanceOf[TumblingEventTimeWindows])
    assertTrue(winOperator.getStateDescriptor.isInstanceOf[FoldingStateDescriptor[_, _]])

    processElementAndEnsureOutput[String, (String, Int), (String, Int)](
      winOperator,
      winOperator.getKeySelector,
      BasicTypeInfo.STRING_TYPE_INFO,
      ("hello", 1))
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		6
-  
  def testOffsetAndFetch(): Unit = {
    checkSize(
      "SELECT * FROM Table3 OFFSET 2 ROWS FETCH NEXT 5 ROWS ONLY",
      5)
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		6
-  
  def testOffsetAndLimit(): Unit = {
    checkSize(
      "SELECT * FROM Table3 LIMIT 10 OFFSET 2",
      10)
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		6
-  
  def testFetch(): Unit = {
    checkSize(
      "SELECT * FROM Table3 FETCH NEXT 10 ROWS ONLY",
      10)
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		6
-  
  def testFetchWithLimitTable(): Unit = {
    checkSize(
      "SELECT * FROM LimitTable FETCH NEXT 10 ROWS ONLY",
      10)
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		6
-  
  def testFetchFirst(): Unit = {
    checkSize(
      "SELECT * FROM Table3 FETCH FIRST 10 ROWS ONLY",
      10)
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		6
-  
  def testFetchFirstWithLimitTable(): Unit = {
    checkSize(
      "SELECT * FROM LimitTable FETCH FIRST 10 ROWS ONLY",
      10)
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		6
-  
  def testLimit(): Unit = {
    checkSize(
      "SELECT * FROM Table3 LIMIT 5",
      5)
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		6
-  
  def testLimit0WithLimitTable(): Unit = {
    checkSize(
      "SELECT * FROM LimitTable LIMIT 0",
      0)
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		6
-  
  def testLimitWithLimitTable(): Unit = {
    checkSize(
      "SELECT * FROM LimitTable LIMIT 5",
      5)
  }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		6
-  (expected = classOf[ValidationException])
  def testTableLimitWithLimitTable(): Unit = {
    Assert.assertEquals(
      executeQuery(tEnv.from("LimitTable").fetch(5)).size,
      5)
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		6
-  
  def testLessThanOffset(): Unit = {
    checkSize(
      "SELECT * FROM Table3 OFFSET 2 ROWS FETCH NEXT 50 ROWS ONLY",
      19)
  }
```
## c89a622449cf0f08fa1b8944c67c5cabe7aa1cc6 ##
```
Cyclomatic Complexity	1
Assertions		0
Lines of Code		6
-    
    public void testSSSPExample() throws Exception {
        SingleSourceShortestPaths.main(new String[]{SingleSourceShortestPathsData.SRC_VERTEX_ID + "",
                edgesPath, resultPath, 10 + ""});
        expected = SingleSourceShortestPathsData.RESULTED_SINGLE_SOURCE_SHORTEST_PATHS;
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		6
-    
    public void testGSASSSPExample() throws Exception {
        GSASingleSourceShortestPaths.main(new String[]{SingleSourceShortestPathsData.SRC_VERTEX_ID + "",
                edgesPath, resultPath, 10 + ""});
        expected = SingleSourceShortestPathsData.RESULTED_SINGLE_SOURCE_SHORTEST_PATHS;
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		6
-    
    public void testPregelSSSPExample() throws Exception {
        PregelSSSP.main(new String[]{SingleSourceShortestPathsData.SRC_VERTEX_ID + "",
                edgesPath, resultPath, 10 + ""});
        expected = SingleSourceShortestPathsData.RESULTED_SINGLE_SOURCE_SHORTEST_PATHS;
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		7
-  (expected = classOf[ValidationException])
  def testAggregationOnNonExistingField(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val t = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv)
      // Must fail. Field 'foo does not exist.
      .select('foo.avg)@@@   }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		8
-  (expected = classOf[ValidationException])
  def testNonWorkingAggregationDataTypes(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val t = env.fromElements(("Hello", 1)).toTable(tEnv)
      // Must fail. Field '_1 is not a numeric type.
      .select('_1.sum)

    t.collect()
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		7
-  (expected = classOf[ValidationException])
  def testNoNestedAggregations(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val t = env.fromElements(("Hello", 1)).toTable(tEnv)
      // Must fail. Sum aggregation can not be chained.
      .select('_2.sum.sum)
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		8
-  (expected = classOf[ValidationException])
  def testGroupingOnNonExistentField(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val t = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
      // must fail. '_foo not a valid field
      .groupBy('_foo)
      .select('a.avg)
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		8
-  (expected = classOf[ValidationException])
  def testGroupingInvalidSelection(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val t = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
      .groupBy('a, 'b)
      // must fail. 'c is not a grouping key or aggregation
      .select('c)@@@   }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		7
-  (expected = classOf[ValidationException])
  def testSelectInvalidFieldFields(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    CollectionDataSets.get3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
      // must fail. Field 'foo does not exist
      .select('a, 'foo)
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		7
-  (expected = classOf[ValidationException])
  def testSelectAmbiguousRenaming(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    CollectionDataSets.get3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
      // must fail. 'a and 'b are both renamed to 'foo
      .select('a + 1 as 'foo, 'b + 2 as 'foo).toDataSet[Row].print()
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		7
-  (expected = classOf[ValidationException])
  def testSelectAmbiguousRenaming2(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    CollectionDataSets.get3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
      // must fail. 'a and 'b are both renamed to 'a
      .select('a, 'b as 'a).toDataSet[Row].print()
  }
```
```
Cyclomatic Complexity	 5
Assertions		 0
Lines of Code		30
-  
  def testAliasStarException(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    try {
      CollectionDataSets.get3TupleDataSet(env).toTable(tEnv, '*, 'b, 'c)
      fail("TableException expected")
    } catch {
      case _: TableException => //ignore
    }

    try {
      CollectionDataSets.get3TupleDataSet(env).toTable(tEnv)
        .select('_1 as '*, '_2 as 'b, '_1 as 'c)
      fail("ValidationException expected")
    } catch {
      case _: ValidationException => //ignore
    }

    try {
      CollectionDataSets.get3TupleDataSet(env).toTable(tEnv).as('*, 'b, 'c)
      fail("ValidationException expected")
    } catch {
      case _: ValidationException => //ignore
    }

    try {
      CollectionDataSets.get3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c).select('*, 'b)
      fail("ValidationException expected")
    } catch {
      case _: ValidationException => //ignore
    }@@@   }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		7
-  (expected = classOf[ValidationException])
  def testFilterInvalidFieldName(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val ds = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)

    // must fail. Field 'foo does not exist
    ds.filter( 'foo === 2 )
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		10
-  (expected = classOf[ValidationException])
  def testJoinNonExistingKey(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = CollectionDataSets.get5TupleDataSet(env).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)

    ds1.join(ds2)
      // must fail. Field 'foo does not exist
      .where('foo === 'e)
      .select('c, 'g)
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		10
-  (expected = classOf[ValidationException])
  def testJoinWithNonMatchingKeyTypes(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = CollectionDataSets.get5TupleDataSet(env).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)

    ds1.join(ds2)
      // must fail. Field 'a is Int, and 'g is String
      .where('a === 'g)
      .select('c, 'g).collect()
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		10
-  (expected = classOf[ValidationException])
  def testJoinWithAmbiguousFields(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = CollectionDataSets.get5TupleDataSet(env).toTable(tEnv, 'd, 'e, 'f, 'g, 'c)

    ds1.join(ds2)
      // must fail. Both inputs share the same field 'c
      .where('a === 'd)
      .select('c, 'g)
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		10
-  (expected = classOf[TableException])
  def testNoEqualityJoinPredicate1(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = CollectionDataSets.get5TupleDataSet(env).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)

    ds1.join(ds2)
      // must fail. No equality join predicate
      .where('d === 'f)
      .select('c, 'g).collect()
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		10
-  (expected = classOf[TableException])
  def testNoEqualityJoinPredicate2(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = CollectionDataSets.get5TupleDataSet(env).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)

    ds1.join(ds2)
      // must fail. No equality join predicate
      .where('a < 'd)
      .select('c, 'g).collect()
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		9
-  (expected = classOf[ValidationException])
  def testJoinTablesFromDifferentEnvs(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tEnv1 = TableEnvironment.getTableEnvironment(env, config)
    val tEnv2 = TableEnvironment.getTableEnvironment(env, config)

    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv1, 'a, 'b, 'c)
    val ds2 = CollectionDataSets.get5TupleDataSet(env).toTable(tEnv2, 'd, 'e, 'f, 'g, 'h)

    // Must fail. Tables are bound to different TableEnvironments.
    ds1.join(ds2).where('b === 'e).select('c, 'g)
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		9
-  (expected = classOf[ValidationException])
  def testNoJoinCondition(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)
    tEnv.getConfig.setNullCheck(true)

    val ds1 = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = CollectionDataSets.get5TupleDataSet(env).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)

    val joinT = ds2.leftOuterJoin(ds1, 'b === 'd && 'b < 3).select('c, 'g)
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		9
-  (expected = classOf[ValidationException])
  def testNoEquiJoin(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)
    tEnv.getConfig.setNullCheck(true)

    val ds1 = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = CollectionDataSets.get5TupleDataSet(env).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)

    val joinT = ds2.leftOuterJoin(ds1, 'b < 'd).select('c, 'g)
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		8
-  (expected = classOf[ValidationException])
  def testUnionDifferentColumnSize(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = CollectionDataSets.get5TupleDataSet(env).toTable(tEnv, 'a, 'b, 'd, 'c, 'e)

    // must fail. Union inputs have different column size.
    ds1.unionAll(ds2)
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		9
-  (expected = classOf[ValidationException])
  def testUnionDifferentFieldTypes(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = CollectionDataSets.get5TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c, 'd, 'e)
      .select('a, 'b, 'c)

    // must fail. Union inputs have different field types.
    ds1.unionAll(ds2)
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		9
-  (expected = classOf[ValidationException])
  def testUnionTablesFromDifferentEnvs(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tEnv1 = TableEnvironment.getTableEnvironment(env, config)
    val tEnv2 = TableEnvironment.getTableEnvironment(env, config)

    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv1, 'a, 'b, 'c)
    val ds2 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv2, 'a, 'b, 'c)

    // Must fail. Tables are bound to different TableEnvironments.
    ds1.unionAll(ds2).select('c)
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		9
-  (expected = classOf[ValidationException])
  def testMinusDifferentFieldTypes(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = CollectionDataSets.get5TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c, 'd, 'e)
      .select('a, 'b, 'c)

    // must fail. Minus inputs have different field types.
    ds1.minus(ds2)
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		9
-  (expected = classOf[ValidationException])
  def testMinusAllTablesFromDifferentEnvs(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tEnv1 = TableEnvironment.getTableEnvironment(env, config)
    val tEnv2 = TableEnvironment.getTableEnvironment(env, config)

    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv1, 'a, 'b, 'c)
    val ds2 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv2, 'a, 'b, 'c)

    // Must fail. Tables are bound to different TableEnvironments.
    ds1.minusAll(ds2).select('c)@@@   }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		9
-  (expected = classOf[ValidationException])
  def testIntersectWithDifferentFieldTypes(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = CollectionDataSets.get5TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c, 'd, 'e)
      .select('a, 'b, 'c)

    // must fail. Intersect inputs have different field types.
    ds1.intersect(ds2)
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		9
-  (expected = classOf[ValidationException])
  def testIntersectTablesFromDifferentEnvs(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tEnv1 = TableEnvironment.getTableEnvironment(env, config)
    val tEnv2 = TableEnvironment.getTableEnvironment(env, config)

    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv1, 'a, 'b, 'c)
    val ds2 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv2, 'a, 'b, 'c)

    // Must fail. Tables are bound to different TableEnvironments.
    ds1.intersect(ds2).select('c)@@@   }
```
## 603c120956989fdb004ff824ba3b62b6b62daa3c ##
```
Cyclomatic Complexity	2
Assertions		3
Lines of Code		31
-	
	public void testNodeClient() throws Exception{

		File dataDir = tempFolder.newFolder();

		Node node = nodeBuilder()
				.settings(ImmutableSettings.settingsBuilder()
						.put("http.enabled", false)
						.put("path.data", dataDir.getAbsolutePath()))
				// set a custom cluster name to verify that user config works correctly
				.clusterName("my-node-client-cluster")
				.local(true)
				.node();

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStreamSource<Tuple2<Integer, String>> source = env.addSource(new TestSourceFunction());

		Map<String, String> config = Maps.newHashMap();
		// This instructs the sink to emit after every element, otherwise they would be buffered
		config.put(ElasticsearchSink.CONFIG_KEY_BULK_FLUSH_MAX_ACTIONS, "1");
		config.put("cluster.name", "my-node-client-cluster");

		// connect to our local node
		config.put("node.local", "true");

		source.addSink(new ElasticsearchSink<>(config, new TestIndexRequestBuilder()));

		env.execute("Elasticsearch Node Client Test");


		// verify the results
		Client client = node.client();
		for (int i = 0; i < NUM_ELEMENTS; i++) {
			GetResponse response = client.get(new GetRequest("my-index",
					"my-type",
					Integer.toString(i))).actionGet();
			Assert.assertEquals("message #" + i, response.getSource().get("data"));
		}

		node.close();
	}
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		6
-    
    public void testSSSPExample() throws Exception {
        SingleSourceShortestPaths.main(new String[]{SingleSourceShortestPathsData.SRC_VERTEX_ID + "",
                edgesPath, resultPath, 10 + ""});
        expected = SingleSourceShortestPathsData.RESULTED_SINGLE_SOURCE_SHORTEST_PATHS;
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		6
-    
    public void testGSASSSPExample() throws Exception {
        GSASingleSourceShortestPaths.main(new String[]{SingleSourceShortestPathsData.SRC_VERTEX_ID + "",
                edgesPath, resultPath, 10 + ""});
        expected = SingleSourceShortestPathsData.RESULTED_SINGLE_SOURCE_SHORTEST_PATHS;
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		6
-    
    public void testPregelSSSPExample() throws Exception {
        PregelSSSP.main(new String[]{SingleSourceShortestPathsData.SRC_VERTEX_ID + "",
                edgesPath, resultPath, 10 + ""});
        expected = SingleSourceShortestPathsData.RESULTED_SINGLE_SOURCE_SHORTEST_PATHS;
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		7
-  (expected = classOf[ValidationException])
  def testAggregationOnNonExistingField(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val t = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv)
      // Must fail. Field 'foo does not exist.
      .select('foo.avg)@@@   }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		8
-  (expected = classOf[ValidationException])
  def testNonWorkingAggregationDataTypes(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val t = env.fromElements(("Hello", 1)).toTable(tEnv)
      // Must fail. Field '_1 is not a numeric type.
      .select('_1.sum)

    t.collect()
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		7
-  (expected = classOf[ValidationException])
  def testNoNestedAggregations(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val t = env.fromElements(("Hello", 1)).toTable(tEnv)
      // Must fail. Sum aggregation can not be chained.
      .select('_2.sum.sum)
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		8
-  (expected = classOf[ValidationException])
  def testGroupingOnNonExistentField(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val t = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
      // must fail. '_foo not a valid field
      .groupBy('_foo)
      .select('a.avg)
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		8
-  (expected = classOf[ValidationException])
  def testGroupingInvalidSelection(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val t = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
      .groupBy('a, 'b)
      // must fail. 'c is not a grouping key or aggregation
      .select('c)@@@   }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		10
-  (expected = classOf[ValidationException])
  def testSelectFromBatchWindow1(): Unit = {
    val sourceTable = util.addTable[(Int, Long, String, Double)]("MyTable", 'a, 'b, 'c, 'd)

    // time field is selected
    val resultTable = sourceTable
      .window(Tumble over 5.millis on 'a as 'w)
      .groupBy('w)
      .select('a.sum, 'c.count)

    val expected = "TODO"

    util.verifyTable(resultTable, expected)
  }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		10
-  (expected = classOf[ValidationException])
  def testSelectFromBatchWindow2(): Unit = {
    val sourceTable = util.addTable[(Int, Long, String, Double)]("MyTable", 'a, 'b, 'c, 'd)

    // time field is not selected
    val resultTable = sourceTable
      .window(Tumble over 5.millis on 'a as 'w)
      .groupBy('w)
      .select('c.count)

    val expected = "TODO"

    util.verifyTable(resultTable, expected)
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		10
-  (expected = classOf[ValidationException])
  def testJoinNonExistingKey(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = CollectionDataSets.get5TupleDataSet(env).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)

    ds1.join(ds2)
      // must fail. Field 'foo does not exist
      .where('foo === 'e)
      .select('c, 'g)
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		10
-  (expected = classOf[ValidationException])
  def testJoinWithNonMatchingKeyTypes(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = CollectionDataSets.get5TupleDataSet(env).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)

    ds1.join(ds2)
      // must fail. Field 'a is Int, and 'g is String
      .where('a === 'g)
      .select('c, 'g).collect()
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		10
-  (expected = classOf[ValidationException])
  def testJoinWithAmbiguousFields(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = CollectionDataSets.get5TupleDataSet(env).toTable(tEnv, 'd, 'e, 'f, 'g, 'c)

    ds1.join(ds2)
      // must fail. Both inputs share the same field 'c
      .where('a === 'd)
      .select('c, 'g)
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		10
-  (expected = classOf[TableException])
  def testNoEqualityJoinPredicate1(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = CollectionDataSets.get5TupleDataSet(env).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)

    ds1.join(ds2)
      // must fail. No equality join predicate
      .where('d === 'f)
      .select('c, 'g).collect()
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		10
-  (expected = classOf[TableException])
  def testNoEqualityJoinPredicate2(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = CollectionDataSets.get5TupleDataSet(env).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)

    ds1.join(ds2)
      // must fail. No equality join predicate
      .where('a < 'd)
      .select('c, 'g).collect()
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		9
-  (expected = classOf[ValidationException])
  def testJoinTablesFromDifferentEnvs(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tEnv1 = TableEnvironment.getTableEnvironment(env, config)
    val tEnv2 = TableEnvironment.getTableEnvironment(env, config)

    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv1, 'a, 'b, 'c)
    val ds2 = CollectionDataSets.get5TupleDataSet(env).toTable(tEnv2, 'd, 'e, 'f, 'g, 'h)

    // Must fail. Tables are bound to different TableEnvironments.
    ds1.join(ds2).where('b === 'e).select('c, 'g)
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		9
-  (expected = classOf[ValidationException])
  def testNoJoinCondition(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)
    tEnv.getConfig.setNullCheck(true)

    val ds1 = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = CollectionDataSets.get5TupleDataSet(env).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)

    val joinT = ds2.leftOuterJoin(ds1, 'b === 'd && 'b < 3).select('c, 'g)
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		9
-  (expected = classOf[ValidationException])
  def testNoEquiJoin(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)
    tEnv.getConfig.setNullCheck(true)

    val ds1 = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = CollectionDataSets.get5TupleDataSet(env).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)

    val joinT = ds2.leftOuterJoin(ds1, 'b < 'd).select('c, 'g)
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		8
-  (expected = classOf[ValidationException])
  def testUnionDifferentColumnSize(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = CollectionDataSets.get5TupleDataSet(env).toTable(tEnv, 'a, 'b, 'd, 'c, 'e)

    // must fail. Union inputs have different column size.
    ds1.unionAll(ds2)
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		9
-  (expected = classOf[ValidationException])
  def testUnionDifferentFieldTypes(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = CollectionDataSets.get5TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c, 'd, 'e)
      .select('a, 'b, 'c)

    // must fail. Union inputs have different field types.
    ds1.unionAll(ds2)
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		9
-  (expected = classOf[ValidationException])
  def testUnionTablesFromDifferentEnvs(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tEnv1 = TableEnvironment.getTableEnvironment(env, config)
    val tEnv2 = TableEnvironment.getTableEnvironment(env, config)

    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv1, 'a, 'b, 'c)
    val ds2 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv2, 'a, 'b, 'c)

    // Must fail. Tables are bound to different TableEnvironments.
    ds1.unionAll(ds2).select('c)
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		9
-  (expected = classOf[ValidationException])
  def testMinusDifferentFieldTypes(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = CollectionDataSets.get5TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c, 'd, 'e)
      .select('a, 'b, 'c)

    // must fail. Minus inputs have different field types.
    ds1.minus(ds2)
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		9
-  (expected = classOf[ValidationException])
  def testMinusAllTablesFromDifferentEnvs(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tEnv1 = TableEnvironment.getTableEnvironment(env, config)
    val tEnv2 = TableEnvironment.getTableEnvironment(env, config)

    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv1, 'a, 'b, 'c)
    val ds2 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv2, 'a, 'b, 'c)

    // Must fail. Tables are bound to different TableEnvironments.
    ds1.minusAll(ds2).select('c)@@@   }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		9
-  (expected = classOf[ValidationException])
  def testIntersectWithDifferentFieldTypes(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = CollectionDataSets.get5TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c, 'd, 'e)
      .select('a, 'b, 'c)

    // must fail. Intersect inputs have different field types.
    ds1.intersect(ds2)
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		9
-  (expected = classOf[ValidationException])
  def testIntersectTablesFromDifferentEnvs(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tEnv1 = TableEnvironment.getTableEnvironment(env, config)
    val tEnv2 = TableEnvironment.getTableEnvironment(env, config)

    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv1, 'a, 'b, 'c)
    val ds2 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv2, 'a, 'b, 'c)

    // Must fail. Tables are bound to different TableEnvironments.
    ds1.intersect(ds2).select('c)@@@   }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		18
-	
	public void testOverflow() {
		
		int buildSize = 1000;
		int probeSize = 1000;
		try {
			TestData.TupleGenerator bgen = new TestData.TupleGenerator(SEED1, 200, 1024, KeyMode.RANDOM, ValueMode.FIX_LENGTH);
			TestData.TupleGenerator pgen = new TestData.TupleGenerator(SEED2, 0, 1024, KeyMode.SORTED, ValueMode.FIX_LENGTH);
			
			final TestData.TupleGeneratorIterator buildInput = new TestData.TupleGeneratorIterator(bgen, buildSize);
			final TestData.TupleGeneratorIterator probeInput = new TestData.TupleGeneratorIterator(pgen, probeSize);
			doTest(buildInput,probeInput, bgen, pgen);
		}
		catch (Exception e) {
			e.printStackTrace();
			Assert.fail("An exception occurred during the test: " + e.getMessage());
		}
	}
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		18
-	
	public void testDoubleProbeSpilling() {
		
		int buildSize = 1000;
		int probeSize = 1000;
		try {
			TestData.TupleGenerator bgen = new TestData.TupleGenerator(SEED1, 0, 1024, KeyMode.SORTED, ValueMode.FIX_LENGTH);
			TestData.TupleGenerator pgen = new TestData.TupleGenerator(SEED2, 0, 1024, KeyMode.SORTED, ValueMode.FIX_LENGTH);
			
			final TestData.TupleGeneratorIterator buildInput = new TestData.TupleGeneratorIterator(bgen, buildSize);
			final TestData.TupleGeneratorIterator probeInput = new TestData.TupleGeneratorIterator(pgen, probeSize);
			doTest(buildInput,probeInput, bgen, pgen);
		}
		catch (Exception e) {
			e.printStackTrace();
			Assert.fail("An exception occurred during the test: " + e.getMessage());
		}
	}
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		19
-	
	public void testDoubleProbeInMemory() {
		
		int buildSize = 1000;
		int probeSize = 1000;
		try {
			TestData.TupleGenerator bgen = new TestData.TupleGenerator(SEED1, 0, 28, KeyMode.SORTED, ValueMode.FIX_LENGTH);
			TestData.TupleGenerator pgen = new TestData.TupleGenerator(SEED2, 0, 28, KeyMode.SORTED, ValueMode.FIX_LENGTH);
			
			final TestData.TupleGeneratorIterator buildInput = new TestData.TupleGeneratorIterator(bgen, buildSize);
			final TestData.TupleGeneratorIterator probeInput = new TestData.TupleGeneratorIterator(pgen, probeSize);
			
			doTest(buildInput,probeInput, bgen, pgen);
		}
		catch (Exception e) {
			e.printStackTrace();
			Assert.fail("An exception occurred during the test: " + e.getMessage());
		}
	}
```
```
Cyclomatic Complexity	 11
Assertions		 5
Lines of Code		105
-	
	public void testSpillingHashJoinWithMassiveCollisions() throws IOException {
		// the following two values are known to have a hash-code collision on the initial level.
		// we use them to make sure one partition grows over-proportionally large
		final int REPEATED_VALUE_1 = 40559;
		final int REPEATED_VALUE_2 = 92882;
		final int REPEATED_VALUE_COUNT_BUILD = 200000;
		final int REPEATED_VALUE_COUNT_PROBE = 5;
		
		final int NUM_KEYS = 1000000;
		final int BUILD_VALS_PER_KEY = 3;
		final int PROBE_VALS_PER_KEY = 10;
		
		// create a build input that gives 3 million pairs with 3 values sharing the same key, plus 400k pairs with two colliding keys
		MutableObjectIterator<Tuple2<Integer, Integer>> build1 = new UniformIntTupleGenerator(NUM_KEYS, BUILD_VALS_PER_KEY, false);
		MutableObjectIterator<Tuple2<Integer, Integer>> build2 = new TestData.ConstantIntIntTuplesIterator(REPEATED_VALUE_1, 17, REPEATED_VALUE_COUNT_BUILD);
		MutableObjectIterator<Tuple2<Integer, Integer>> build3 = new TestData.ConstantIntIntTuplesIterator(REPEATED_VALUE_2, 23, REPEATED_VALUE_COUNT_BUILD);
		List<MutableObjectIterator<Tuple2<Integer, Integer>>> builds = new ArrayList<>();
		builds.add(build1);
		builds.add(build2);
		builds.add(build3);
		MutableObjectIterator<Tuple2<Integer, Integer>> buildInput = new UnionIterator<>(builds);

		// allocate the memory for the HashTable
		List<MemorySegment> memSegments;
		try {
			memSegments = this.memoryManager.allocatePages(MEM_OWNER, 896);
		}
		catch (MemoryAllocationException maex) {
			fail("Memory for the Join could not be provided.");
			return;
		}
		
		// create the map for validating the results
		HashMap<Integer, Long> map = new HashMap<Integer, Long>(NUM_KEYS);
		
		// ----------------------------------------------------------------------------------------
		
		final ReOpenableMutableHashTable<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> join = new ReOpenableMutableHashTable<>(
				this.recordBuildSideAccesssor, this.recordProbeSideAccesssor, 
				this.recordBuildSideComparator, this.recordProbeSideComparator, this.pactRecordComparator,
				memSegments, ioManager, true);
		
		for(int probe = 0; probe < NUM_PROBES; probe++) {
			// create a probe input that gives 10 million pairs with 10 values sharing a key
			MutableObjectIterator<Tuple2<Integer, Integer>> probeInput = getProbeInput(NUM_KEYS, PROBE_VALS_PER_KEY, REPEATED_VALUE_1, REPEATED_VALUE_2);
			if(probe == 0) {
				join.open(buildInput, probeInput);
			} else {
				join.reopenProbe(probeInput);
			}
		
			Tuple2<Integer, Integer> record;
			final Tuple2<Integer, Integer> recordReuse = new Tuple2<>();

			while (join.nextRecord()) {
				long numBuildValues = 0;
		
				final Tuple2<Integer, Integer> probeRec = join.getCurrentProbeRecord();
				Integer key = probeRec.f0;
				
				MutableObjectIterator<Tuple2<Integer, Integer>> buildSide = join.getBuildSideIterator();
				if ((record = buildSide.next(recordReuse)) != null) {
					numBuildValues = 1;
					Assert.assertEquals("Probe-side key was different than build-side key.", key, record.f0); 
				}
				else {
					fail("No build side values found for a probe key.");
				}
				while ((record = buildSide.next(record)) != null) {
					numBuildValues++;
					Assert.assertEquals("Probe-side key was different than build-side key.", key, record.f0);
				}
				
				Long contained = map.get(key);
				if (contained == null) {
					contained = numBuildValues;
				}
				else {
					contained = contained + numBuildValues;
				}
				
				map.put(key, contained);
			}
		}
		
		join.close();
		
		Assert.assertEquals("Wrong number of keys", NUM_KEYS, map.size());
		for (Map.Entry<Integer, Long> entry : map.entrySet()) {
			long val = entry.getValue();
			int key = entry.getKey();
	
			if( key == REPEATED_VALUE_1 || key == REPEATED_VALUE_2) {
				Assert.assertEquals("Wrong number of values in per-key cross product for key " + key, 
							(PROBE_VALS_PER_KEY + REPEATED_VALUE_COUNT_PROBE) * (BUILD_VALS_PER_KEY + REPEATED_VALUE_COUNT_BUILD) * NUM_PROBES, val);
			} else {
				Assert.assertEquals("Wrong number of values in per-key cross product for key " + key, 
							PROBE_VALS_PER_KEY * BUILD_VALS_PER_KEY * NUM_PROBES, val);
			}
		}
		
		
		// ----------------------------------------------------------------------------------------
		
		this.memoryManager.release(join.getFreedMemory());
	}
```
```
Cyclomatic Complexity	 11
Assertions		 5
Lines of Code		106
-	
	public void testSpillingHashJoinWithTwoRecursions() throws IOException
	{
		// the following two values are known to have a hash-code collision on the first recursion level.
		// we use them to make sure one partition grows over-proportionally large
		final int REPEATED_VALUE_1 = 40559;
		final int REPEATED_VALUE_2 = 92882;
		final int REPEATED_VALUE_COUNT_BUILD = 200000;
		final int REPEATED_VALUE_COUNT_PROBE = 5;
		
		final int NUM_KEYS = 1000000;
		final int BUILD_VALS_PER_KEY = 3;
		final int PROBE_VALS_PER_KEY = 10;
		
		// create a build input that gives 3 million pairs with 3 values sharing the same key, plus 400k pairs with two colliding keys
		MutableObjectIterator<Tuple2<Integer, Integer>> build1 = new UniformIntTupleGenerator(NUM_KEYS, BUILD_VALS_PER_KEY, false);
		MutableObjectIterator<Tuple2<Integer, Integer>> build2 = new TestData.ConstantIntIntTuplesIterator(REPEATED_VALUE_1, 17, REPEATED_VALUE_COUNT_BUILD);
		MutableObjectIterator<Tuple2<Integer, Integer>> build3 = new TestData.ConstantIntIntTuplesIterator(REPEATED_VALUE_2, 23, REPEATED_VALUE_COUNT_BUILD);
		List<MutableObjectIterator<Tuple2<Integer, Integer>>> builds = new ArrayList<>();
		builds.add(build1);
		builds.add(build2);
		builds.add(build3);
		MutableObjectIterator<Tuple2<Integer, Integer>> buildInput = new UnionIterator<>(builds);
	

		// allocate the memory for the HashTable
		List<MemorySegment> memSegments;
		try {
			memSegments = this.memoryManager.allocatePages(MEM_OWNER, 896);
		}
		catch (MemoryAllocationException maex) {
			fail("Memory for the Join could not be provided.");
			return;
		}
		
		// create the map for validating the results
		HashMap<Integer, Long> map = new HashMap<Integer, Long>(NUM_KEYS);
		
		// ----------------------------------------------------------------------------------------
		
		final ReOpenableMutableHashTable<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> join = new ReOpenableMutableHashTable<>(
				this.recordBuildSideAccesssor, this.recordProbeSideAccesssor, 
				this.recordBuildSideComparator, this.recordProbeSideComparator, this.pactRecordComparator,
				memSegments, ioManager, true);
		
		for (int probe = 0; probe < NUM_PROBES; probe++) {
			// create a probe input that gives 10 million pairs with 10 values sharing a key
			MutableObjectIterator<Tuple2<Integer, Integer>> probeInput = getProbeInput(NUM_KEYS, PROBE_VALS_PER_KEY, REPEATED_VALUE_1, REPEATED_VALUE_2);
			if(probe == 0) {
				join.open(buildInput, probeInput);
			} else {
				join.reopenProbe(probeInput);
			}
			Tuple2<Integer, Integer> record;
			final Tuple2<Integer, Integer> recordReuse = new Tuple2<>();

			while (join.nextRecord())
			{	
				long numBuildValues = 0;
				
				final Tuple2<Integer, Integer> probeRec = join.getCurrentProbeRecord();
				Integer key = probeRec.f0;
				
				MutableObjectIterator<Tuple2<Integer, Integer>> buildSide = join.getBuildSideIterator();
				if ((record = buildSide.next(recordReuse)) != null) {
					numBuildValues = 1;
					Assert.assertEquals("Probe-side key was different than build-side key.", key, record.f0); 
				}
				else {
					fail("No build side values found for a probe key.");
				}
				while ((record = buildSide.next(recordReuse)) != null) {
					numBuildValues++;
					Assert.assertEquals("Probe-side key was different than build-side key.", key, record.f0);
				}
				
				Long contained = map.get(key);
				if (contained == null) {
					contained = numBuildValues;
				}
				else {
					contained = contained + numBuildValues;
				}
				
				map.put(key, contained);
			}
		}
		
		join.close();
		Assert.assertEquals("Wrong number of keys", NUM_KEYS, map.size());
		for (Map.Entry<Integer, Long> entry : map.entrySet()) {
			long val = entry.getValue();
			int key = entry.getKey();
	
			if( key == REPEATED_VALUE_1 || key == REPEATED_VALUE_2) {
				Assert.assertEquals("Wrong number of values in per-key cross product for key " + key, 
							(PROBE_VALS_PER_KEY + REPEATED_VALUE_COUNT_PROBE) * (BUILD_VALS_PER_KEY + REPEATED_VALUE_COUNT_BUILD) * NUM_PROBES, val);
			} else {
				Assert.assertEquals("Wrong number of values in per-key cross product for key " + key, 
							PROBE_VALS_PER_KEY * BUILD_VALS_PER_KEY * NUM_PROBES, val);
			}
		}
		
		
		// ----------------------------------------------------------------------------------------
		
		this.memoryManager.release(join.getFreedMemory());
	}
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		2
-	
	public void testValueStateRestoreWithWrongSerializers() {}
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		2
-	
	public void testListStateRestoreWithWrongSerializers() {}
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		2
-	
	public void testReducingStateRestoreWithWrongSerializers() {}
```
```
Cyclomatic Complexity	 1
Assertions		 3
Lines of Code		68
-	
	public void testStateOutputStream() throws IOException {
		File basePath = tempFolder.newFolder().getAbsoluteFile();

		try {
			// the state backend has a very low in-mem state threshold (15 bytes)
			FsStateBackend backend = CommonTestUtils.createCopySerializable(new FsStateBackend(basePath.toURI(), 15));
			JobID jobId = new JobID();

			// we know how FsCheckpointStreamFactory is implemented so we know where it
			// will store checkpoints
			File checkpointPath = new File(basePath.getAbsolutePath(), jobId.toString());

			CheckpointStreamFactory streamFactory = backend.createStreamFactory(jobId, "test_op");

			byte[] state1 = new byte[1274673];
			byte[] state2 = new byte[1];
			byte[] state3 = new byte[0];
			byte[] state4 = new byte[177];

			Random rnd = new Random();
			rnd.nextBytes(state1);
			rnd.nextBytes(state2);
			rnd.nextBytes(state3);
			rnd.nextBytes(state4);

			long checkpointId = 97231523452L;

			CheckpointStreamFactory.CheckpointStateOutputStream stream1 =
					streamFactory.createCheckpointStateOutputStream(checkpointId, System.currentTimeMillis());

			CheckpointStreamFactory.CheckpointStateOutputStream stream2 =
					streamFactory.createCheckpointStateOutputStream(checkpointId, System.currentTimeMillis());

			CheckpointStreamFactory.CheckpointStateOutputStream stream3 =
					streamFactory.createCheckpointStateOutputStream(checkpointId, System.currentTimeMillis());

			stream1.write(state1);
			stream2.write(state2);
			stream3.write(state3);

			FileStateHandle handle1 = (FileStateHandle) stream1.closeAndGetHandle();
			ByteStreamStateHandle handle2 = (ByteStreamStateHandle) stream2.closeAndGetHandle();
			ByteStreamStateHandle handle3 = (ByteStreamStateHandle) stream3.closeAndGetHandle();

			// use with try-with-resources
			StreamStateHandle handle4;
			try (CheckpointStreamFactory.CheckpointStateOutputStream stream4 =
					streamFactory.createCheckpointStateOutputStream(checkpointId, System.currentTimeMillis())) {
				stream4.write(state4);
				handle4 = stream4.closeAndGetHandle();
			}

			// close before accessing handle
			CheckpointStreamFactory.CheckpointStateOutputStream stream5 =
					streamFactory.createCheckpointStateOutputStream(checkpointId, System.currentTimeMillis());
			stream5.write(state4);
			stream5.close();
			try {
				stream5.closeAndGetHandle();
				fail();
			} catch (IOException e) {
				// uh-huh
			}

			validateBytesInStream(handle1.openInputStream(), state1);
			handle1.discardState();
			assertFalse(isDirectoryEmpty(basePath));
			ensureLocalFileDeleted(handle1.getFilePath());

			validateBytesInStream(handle2.openInputStream(), state2);
			handle2.discardState();

			// nothing was written to the stream, so it will return nothing
			assertNull(handle3);

			validateBytesInStream(handle4.openInputStream(), state4);
			handle4.discardState();
			assertTrue(isDirectoryEmpty(checkpointPath));
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		4
-	
	public void testConcurrentMapIfQueryable() throws Exception {
		//unsupported
	}
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		2
-	
	public void testValueStateRestoreWithWrongSerializers() {}
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		2
-	
	public void testListStateRestoreWithWrongSerializers() {}
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		2
-	
	public void testReducingStateRestoreWithWrongSerializers() {}
```
```
Cyclomatic Complexity	 1
Assertions		 5
Lines of Code		24
-	
	@SuppressWarnings("unchecked, deprecation")
	public void testNumStateEntries() throws Exception {
		KeyedStateBackend<Integer> backend = createKeyedBackend(IntSerializer.INSTANCE);

		ValueStateDescriptor<String> kvId = new ValueStateDescriptor<>("id", String.class, null);
		kvId.initializeSerializerUnlessSet(new ExecutionConfig());

		AsyncHeapKeyedStateBackend<Integer> heapBackend = (AsyncHeapKeyedStateBackend<Integer>) backend;

		assertEquals(0, heapBackend.numStateEntries());

		ValueState<String> state = backend.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);

		backend.setCurrentKey(0);
		state.update("hello");
		state.update("ciao");

		assertEquals(1, heapBackend.numStateEntries());

		backend.setCurrentKey(42);
		state.update("foo");

		assertEquals(2, heapBackend.numStateEntries());

		backend.setCurrentKey(0);
		state.clear();

		assertEquals(1, heapBackend.numStateEntries());

		backend.setCurrentKey(42);
		state.clear();

		assertEquals(0, heapBackend.numStateEntries());

		backend.dispose();
	}
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		26
-	
	public void testOversizedState() {
		try {
			MemoryStateBackend backend = new MemoryStateBackend(10);
			CheckpointStreamFactory streamFactory = backend.createStreamFactory(new JobID(), "test_op");

			HashMap<String, Integer> state = new HashMap<>();
			state.put("hey there", 2);
			state.put("the crazy brown fox stumbles over a sentence that does not contain every letter", 77);

			try {
				CheckpointStreamFactory.CheckpointStateOutputStream outStream =
						streamFactory.createCheckpointStateOutputStream(12, 459);

				ObjectOutputStream oos = new ObjectOutputStream(outStream);
				oos.writeObject(state);

				oos.flush();

				outStream.closeAndGetHandle();

				fail("this should cause an exception");
			}
			catch (IOException e) {
				// now darling, isn't that exactly what we wanted?
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
```
```
Cyclomatic Complexity	 1
Assertions		 3
Lines of Code		24
-	
	public void testStateStream() {
		try {
			MemoryStateBackend backend = new MemoryStateBackend();
			CheckpointStreamFactory streamFactory = backend.createStreamFactory(new JobID(), "test_op");

			HashMap<String, Integer> state = new HashMap<>();
			state.put("hey there", 2);
			state.put("the crazy brown fox stumbles over a sentence that does not contain every letter", 77);

			CheckpointStreamFactory.CheckpointStateOutputStream os = streamFactory.createCheckpointStateOutputStream(1, 2);
			ObjectOutputStream oos = new ObjectOutputStream(os);
			oos.writeObject(state);
			oos.flush();
			StreamStateHandle handle = os.closeAndGetHandle();

			assertNotNull(handle);

			try (ObjectInputStream ois = new ObjectInputStream(handle.openInputStream())) {
				assertEquals(state, ois.readObject());
				assertTrue(ois.available() <= 0);
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		25
-	
	public void testOversizedStateStream() {
		try {
			MemoryStateBackend backend = new MemoryStateBackend(10);
			CheckpointStreamFactory streamFactory = backend.createStreamFactory(new JobID(), "test_op");

			HashMap<String, Integer> state = new HashMap<>();
			state.put("hey there", 2);
			state.put("the crazy brown fox stumbles over a sentence that does not contain every letter", 77);

			CheckpointStreamFactory.CheckpointStateOutputStream os = streamFactory.createCheckpointStateOutputStream(1, 2);
			ObjectOutputStream oos = new ObjectOutputStream(os);

			try {
				oos.writeObject(state);
				oos.flush();
				os.closeAndGetHandle();
				fail("this should cause an exception");
			}
			catch (IOException e) {
				// oh boy! what an exception!
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		4
-	
	public void testConcurrentMapIfQueryable() throws Exception {
		//unsupported
	}
```
```
Cyclomatic Complexity	 6
Assertions		 7
Lines of Code		58
-	
	public void testExecutionConfigSerialization() throws IOException, ClassNotFoundException {
		final long seed = System.currentTimeMillis();
		final Random r = new Random(seed);@@@ 
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		StreamGraph streamingJob = new StreamGraph(env);
		StreamingJobGraphGenerator compiler = new StreamingJobGraphGenerator(streamingJob);
		
		boolean closureCleanerEnabled = r.nextBoolean(), forceAvroEnabled = r.nextBoolean(), forceKryoEnabled = r.nextBoolean(), objectReuseEnabled = r.nextBoolean(), sysoutLoggingEnabled = r.nextBoolean();
		int dop = 1 + r.nextInt(10);
		
		ExecutionConfig config = streamingJob.getExecutionConfig();
		if(closureCleanerEnabled) {
			config.enableClosureCleaner();
		} else {
			config.disableClosureCleaner();
		}
		if(forceAvroEnabled) {
			config.enableForceAvro();
		} else {
			config.disableForceAvro();
		}
		if(forceKryoEnabled) {
			config.enableForceKryo();
		} else {
			config.disableForceKryo();
		}
		if(objectReuseEnabled) {
			config.enableObjectReuse();
		} else {
			config.disableObjectReuse();
		}
		if(sysoutLoggingEnabled) {
			config.enableSysoutLogging();
		} else {
			config.disableSysoutLogging();
		}
		config.setParallelism(dop);
		
		JobGraph jobGraph = compiler.createJobGraph();

		final String EXEC_CONFIG_KEY = "runtime.config";

		InstantiationUtil.writeObjectToConfig(jobGraph.getSerializedExecutionConfig(),
			jobGraph.getJobConfiguration(),
			EXEC_CONFIG_KEY);

		SerializedValue<ExecutionConfig> serializedExecutionConfig = InstantiationUtil.readObjectFromConfig(
				jobGraph.getJobConfiguration(),
				EXEC_CONFIG_KEY,
				Thread.currentThread().getContextClassLoader());

		assertNotNull(serializedExecutionConfig);

		ExecutionConfig executionConfig = serializedExecutionConfig.deserializeValue(getClass().getClassLoader());

		assertEquals(closureCleanerEnabled, executionConfig.isClosureCleanerEnabled());
		assertEquals(forceAvroEnabled, executionConfig.isForceAvroEnabled());
		assertEquals(forceKryoEnabled, executionConfig.isForceKryoEnabled());
		assertEquals(objectReuseEnabled, executionConfig.isObjectReuseEnabled());
		assertEquals(sysoutLoggingEnabled, executionConfig.isSysoutLoggingEnabled());
		assertEquals(dop, executionConfig.getParallelism());
	}
```
## 2d741d0caabc325d0b0e96ffd47eae5609883c1d ##
```
Cyclomatic Complexity	2
Assertions		3
Lines of Code		31
-	
	public void testNodeClient() throws Exception{

		File dataDir = tempFolder.newFolder();

		Node node = nodeBuilder()
				.settings(ImmutableSettings.settingsBuilder()
						.put("http.enabled", false)
						.put("path.data", dataDir.getAbsolutePath()))
				// set a custom cluster name to verify that user config works correctly
				.clusterName("my-node-client-cluster")
				.local(true)
				.node();

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStreamSource<Tuple2<Integer, String>> source = env.addSource(new TestSourceFunction());

		Map<String, String> config = Maps.newHashMap();
		// This instructs the sink to emit after every element, otherwise they would be buffered
		config.put(ElasticsearchSink.CONFIG_KEY_BULK_FLUSH_MAX_ACTIONS, "1");
		config.put("cluster.name", "my-node-client-cluster");

		// connect to our local node
		config.put("node.local", "true");

		source.addSink(new ElasticsearchSink<>(config, new TestIndexRequestBuilder()));

		env.execute("Elasticsearch Node Client Test");


		// verify the results
		Client client = node.client();
		for (int i = 0; i < NUM_ELEMENTS; i++) {
			GetResponse response = client.get(new GetRequest("my-index",
					"my-type",
					Integer.toString(i))).actionGet();
			Assert.assertEquals("message #" + i, response.getSource().get("data"));
		}

		node.close();
	}
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		7
-	
	public void testWritableTypeInfoEquality() {
		WritableTypeInfo<TestClass> tpeInfo1 = new WritableTypeInfo<>(TestClass.class);
		WritableTypeInfo<TestClass> tpeInfo2 = new WritableTypeInfo<>(TestClass.class);

		assertEquals(tpeInfo1, tpeInfo2);
		assertEquals(tpeInfo1.hashCode(), tpeInfo2.hashCode());
	}
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		7
-	
	public void testEnumTypeEquality() {
		EnumTypeInfo<TestEnum> enumTypeInfo1 = new EnumTypeInfo<TestEnum>(TestEnum.class);
		EnumTypeInfo<TestEnum> enumTypeInfo2 = new EnumTypeInfo<TestEnum>(TestEnum.class);

		assertEquals(enumTypeInfo1, enumTypeInfo2);
		assertEquals(enumTypeInfo1.hashCode(), enumTypeInfo2.hashCode());
	}
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		7
-	
	public void testGenericTypeInfoEquality() {
		GenericTypeInfo<TestClass> tpeInfo1 = new GenericTypeInfo<>(TestClass.class);
		GenericTypeInfo<TestClass> tpeInfo2 = new GenericTypeInfo<>(TestClass.class);

		assertEquals(tpeInfo1, tpeInfo2);
		assertEquals(tpeInfo1.hashCode(), tpeInfo2.hashCode());
	}
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		6
-	
	public void testGenericTypeInfoInequality() {
		GenericTypeInfo<TestClass> tpeInfo1 = new GenericTypeInfo<>(TestClass.class);
		GenericTypeInfo<AlternativeClass> tpeInfo2 = new GenericTypeInfo<>(AlternativeClass.class);

		assertNotEquals(tpeInfo1, tpeInfo2);
	}
```
```
Cyclomatic Complexity	 1
Assertions		 4
Lines of Code		17
-	
	public void testPojoTypeInfoEquality() {
		try {
			TypeInformation<TestPojo> info1 = TypeExtractor.getForClass(TestPojo.class);
			TypeInformation<TestPojo> info2 = TypeExtractor.getForClass(TestPojo.class);
			
			assertTrue(info1 instanceof PojoTypeInfo);
			assertTrue(info2 instanceof PojoTypeInfo);
			
			assertTrue(info1.equals(info2));
			assertTrue(info1.hashCode() == info2.hashCode());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
```
```
Cyclomatic Complexity	 1
Assertions		 3
Lines of Code		14
-	
	public void testPojoTypeInfoInequality() {
		try {
			TypeInformation<TestPojo> info1 = TypeExtractor.getForClass(TestPojo.class);
			TypeInformation<AlternatePojo> info2 = TypeExtractor.getForClass(AlternatePojo.class);

			assertTrue(info1 instanceof PojoTypeInfo);
			assertTrue(info2 instanceof PojoTypeInfo);

			assertFalse(info1.equals(info2));
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		9
-	
	public void testSerializabilityOfPojoTypeInfo() throws IOException, ClassNotFoundException {
		PojoTypeInfo<TestPojo> pojoTypeInfo = (PojoTypeInfo<TestPojo>)TypeExtractor.getForClass(TestPojo.class);

		byte[] serializedPojoTypeInfo = InstantiationUtil.serializeObject(pojoTypeInfo);
		PojoTypeInfo<TestPojo> deserializedPojoTypeInfo = (PojoTypeInfo<TestPojo>)InstantiationUtil.deserializeObject(
			serializedPojoTypeInfo,
			getClass().getClassLoader());

		assertEquals(pojoTypeInfo, deserializedPojoTypeInfo);
	}
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		5
-	
	public void testPrimitivePojo() {
		TypeInformation<PrimitivePojo> info1 = TypeExtractor.getForClass(PrimitivePojo.class);

		assertTrue(info1 instanceof PojoTypeInfo);
	}
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		10
-  (expected = classOf[ValidationException])
  def testJoinNonExistingKey(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = CollectionDataSets.get5TupleDataSet(env).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)

    ds1.join(ds2)
      // must fail. Field 'foo does not exist
      .where('foo === 'e)
      .select('c, 'g)
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		10
-  (expected = classOf[ValidationException])
  def testJoinWithNonMatchingKeyTypes(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = CollectionDataSets.get5TupleDataSet(env).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)

    ds1.join(ds2)
      // must fail. Field 'a is Int, and 'g is String
      .where('a === 'g)
      .select('c, 'g).collect()
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		10
-  (expected = classOf[ValidationException])
  def testJoinWithAmbiguousFields(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = CollectionDataSets.get5TupleDataSet(env).toTable(tEnv, 'd, 'e, 'f, 'g, 'c)

    ds1.join(ds2)
      // must fail. Both inputs share the same field 'c
      .where('a === 'd)
      .select('c, 'g)
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		10
-  (expected = classOf[TableException])
  def testNoEqualityJoinPredicate1(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = CollectionDataSets.get5TupleDataSet(env).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)

    ds1.join(ds2)
      // must fail. No equality join predicate
      .where('d === 'f)
      .select('c, 'g).collect()
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		10
-  (expected = classOf[TableException])
  def testNoEqualityJoinPredicate2(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = CollectionDataSets.get5TupleDataSet(env).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)

    ds1.join(ds2)
      // must fail. No equality join predicate
      .where('a < 'd)
      .select('c, 'g).collect()
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		9
-  (expected = classOf[ValidationException])
  def testJoinTablesFromDifferentEnvs(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tEnv1 = TableEnvironment.getTableEnvironment(env, config)
    val tEnv2 = TableEnvironment.getTableEnvironment(env, config)

    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv1, 'a, 'b, 'c)
    val ds2 = CollectionDataSets.get5TupleDataSet(env).toTable(tEnv2, 'd, 'e, 'f, 'g, 'h)

    // Must fail. Tables are bound to different TableEnvironments.
    ds1.join(ds2).where('b === 'e).select('c, 'g)
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		9
-  (expected = classOf[ValidationException])
  def testNoJoinCondition(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)
    tEnv.getConfig.setNullCheck(true)

    val ds1 = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = CollectionDataSets.get5TupleDataSet(env).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)

    val joinT = ds2.leftOuterJoin(ds1, 'b === 'd && 'b < 3).select('c, 'g)
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		9
-  (expected = classOf[ValidationException])
  def testNoEquiJoin(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)
    tEnv.getConfig.setNullCheck(true)

    val ds1 = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = CollectionDataSets.get5TupleDataSet(env).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)

    val joinT = ds2.leftOuterJoin(ds1, 'b < 'd).select('c, 'g)
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		8
-  (expected = classOf[ValidationException])
  def testUnionDifferentColumnSize(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = CollectionDataSets.get5TupleDataSet(env).toTable(tEnv, 'a, 'b, 'd, 'c, 'e)

    // must fail. Union inputs have different column size.
    ds1.unionAll(ds2)
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		9
-  (expected = classOf[ValidationException])
  def testUnionDifferentFieldTypes(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = CollectionDataSets.get5TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c, 'd, 'e)
      .select('a, 'b, 'c)

    // must fail. Union inputs have different field types.
    ds1.unionAll(ds2)
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		9
-  (expected = classOf[ValidationException])
  def testUnionTablesFromDifferentEnvs(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tEnv1 = TableEnvironment.getTableEnvironment(env, config)
    val tEnv2 = TableEnvironment.getTableEnvironment(env, config)

    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv1, 'a, 'b, 'c)
    val ds2 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv2, 'a, 'b, 'c)

    // Must fail. Tables are bound to different TableEnvironments.
    ds1.unionAll(ds2).select('c)
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		9
-  (expected = classOf[ValidationException])
  def testMinusDifferentFieldTypes(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = CollectionDataSets.get5TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c, 'd, 'e)
      .select('a, 'b, 'c)

    // must fail. Minus inputs have different field types.
    ds1.minus(ds2)
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		9
-  (expected = classOf[ValidationException])
  def testMinusAllTablesFromDifferentEnvs(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tEnv1 = TableEnvironment.getTableEnvironment(env, config)
    val tEnv2 = TableEnvironment.getTableEnvironment(env, config)

    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv1, 'a, 'b, 'c)
    val ds2 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv2, 'a, 'b, 'c)

    // Must fail. Tables are bound to different TableEnvironments.
    ds1.minusAll(ds2).select('c)@@@   }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		9
-  (expected = classOf[ValidationException])
  def testIntersectWithDifferentFieldTypes(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = CollectionDataSets.get5TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c, 'd, 'e)
      .select('a, 'b, 'c)

    // must fail. Intersect inputs have different field types.
    ds1.intersect(ds2)
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		9
-  (expected = classOf[ValidationException])
  def testIntersectTablesFromDifferentEnvs(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tEnv1 = TableEnvironment.getTableEnvironment(env, config)
    val tEnv2 = TableEnvironment.getTableEnvironment(env, config)

    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv1, 'a, 'b, 'c)
    val ds2 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv2, 'a, 'b, 'c)

    // Must fail. Tables are bound to different TableEnvironments.
    ds1.intersect(ds2).select('c)@@@   }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		18
-	
	public void testOverflow() {
		
		int buildSize = 1000;
		int probeSize = 1000;
		try {
			TestData.TupleGenerator bgen = new TestData.TupleGenerator(SEED1, 200, 1024, KeyMode.RANDOM, ValueMode.FIX_LENGTH);
			TestData.TupleGenerator pgen = new TestData.TupleGenerator(SEED2, 0, 1024, KeyMode.SORTED, ValueMode.FIX_LENGTH);
			
			final TestData.TupleGeneratorIterator buildInput = new TestData.TupleGeneratorIterator(bgen, buildSize);
			final TestData.TupleGeneratorIterator probeInput = new TestData.TupleGeneratorIterator(pgen, probeSize);
			doTest(buildInput,probeInput, bgen, pgen);
		}
		catch (Exception e) {
			e.printStackTrace();
			Assert.fail("An exception occurred during the test: " + e.getMessage());
		}
	}
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		18
-	
	public void testDoubleProbeSpilling() {
		
		int buildSize = 1000;
		int probeSize = 1000;
		try {
			TestData.TupleGenerator bgen = new TestData.TupleGenerator(SEED1, 0, 1024, KeyMode.SORTED, ValueMode.FIX_LENGTH);
			TestData.TupleGenerator pgen = new TestData.TupleGenerator(SEED2, 0, 1024, KeyMode.SORTED, ValueMode.FIX_LENGTH);
			
			final TestData.TupleGeneratorIterator buildInput = new TestData.TupleGeneratorIterator(bgen, buildSize);
			final TestData.TupleGeneratorIterator probeInput = new TestData.TupleGeneratorIterator(pgen, probeSize);
			doTest(buildInput,probeInput, bgen, pgen);
		}
		catch (Exception e) {
			e.printStackTrace();
			Assert.fail("An exception occurred during the test: " + e.getMessage());
		}
	}
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		19
-	
	public void testDoubleProbeInMemory() {
		
		int buildSize = 1000;
		int probeSize = 1000;
		try {
			TestData.TupleGenerator bgen = new TestData.TupleGenerator(SEED1, 0, 28, KeyMode.SORTED, ValueMode.FIX_LENGTH);
			TestData.TupleGenerator pgen = new TestData.TupleGenerator(SEED2, 0, 28, KeyMode.SORTED, ValueMode.FIX_LENGTH);
			
			final TestData.TupleGeneratorIterator buildInput = new TestData.TupleGeneratorIterator(bgen, buildSize);
			final TestData.TupleGeneratorIterator probeInput = new TestData.TupleGeneratorIterator(pgen, probeSize);
			
			doTest(buildInput,probeInput, bgen, pgen);
		}
		catch (Exception e) {
			e.printStackTrace();
			Assert.fail("An exception occurred during the test: " + e.getMessage());
		}
	}
```
```
Cyclomatic Complexity	 11
Assertions		 5
Lines of Code		105
-	
	public void testSpillingHashJoinWithMassiveCollisions() throws IOException {
		// the following two values are known to have a hash-code collision on the initial level.
		// we use them to make sure one partition grows over-proportionally large
		final int REPEATED_VALUE_1 = 40559;
		final int REPEATED_VALUE_2 = 92882;
		final int REPEATED_VALUE_COUNT_BUILD = 200000;
		final int REPEATED_VALUE_COUNT_PROBE = 5;
		
		final int NUM_KEYS = 1000000;
		final int BUILD_VALS_PER_KEY = 3;
		final int PROBE_VALS_PER_KEY = 10;
		
		// create a build input that gives 3 million pairs with 3 values sharing the same key, plus 400k pairs with two colliding keys
		MutableObjectIterator<Tuple2<Integer, Integer>> build1 = new UniformIntTupleGenerator(NUM_KEYS, BUILD_VALS_PER_KEY, false);
		MutableObjectIterator<Tuple2<Integer, Integer>> build2 = new TestData.ConstantIntIntTuplesIterator(REPEATED_VALUE_1, 17, REPEATED_VALUE_COUNT_BUILD);
		MutableObjectIterator<Tuple2<Integer, Integer>> build3 = new TestData.ConstantIntIntTuplesIterator(REPEATED_VALUE_2, 23, REPEATED_VALUE_COUNT_BUILD);
		List<MutableObjectIterator<Tuple2<Integer, Integer>>> builds = new ArrayList<>();
		builds.add(build1);
		builds.add(build2);
		builds.add(build3);
		MutableObjectIterator<Tuple2<Integer, Integer>> buildInput = new UnionIterator<>(builds);

		// allocate the memory for the HashTable
		List<MemorySegment> memSegments;
		try {
			memSegments = this.memoryManager.allocatePages(MEM_OWNER, 896);
		}
		catch (MemoryAllocationException maex) {
			fail("Memory for the Join could not be provided.");
			return;
		}
		
		// create the map for validating the results
		HashMap<Integer, Long> map = new HashMap<Integer, Long>(NUM_KEYS);
		
		// ----------------------------------------------------------------------------------------
		
		final ReOpenableMutableHashTable<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> join = new ReOpenableMutableHashTable<>(
				this.recordBuildSideAccesssor, this.recordProbeSideAccesssor, 
				this.recordBuildSideComparator, this.recordProbeSideComparator, this.pactRecordComparator,
				memSegments, ioManager, true);
		
		for(int probe = 0; probe < NUM_PROBES; probe++) {
			// create a probe input that gives 10 million pairs with 10 values sharing a key
			MutableObjectIterator<Tuple2<Integer, Integer>> probeInput = getProbeInput(NUM_KEYS, PROBE_VALS_PER_KEY, REPEATED_VALUE_1, REPEATED_VALUE_2);
			if(probe == 0) {
				join.open(buildInput, probeInput);
			} else {
				join.reopenProbe(probeInput);
			}
		
			Tuple2<Integer, Integer> record;
			final Tuple2<Integer, Integer> recordReuse = new Tuple2<>();

			while (join.nextRecord()) {
				long numBuildValues = 0;
		
				final Tuple2<Integer, Integer> probeRec = join.getCurrentProbeRecord();
				Integer key = probeRec.f0;
				
				MutableObjectIterator<Tuple2<Integer, Integer>> buildSide = join.getBuildSideIterator();
				if ((record = buildSide.next(recordReuse)) != null) {
					numBuildValues = 1;
					Assert.assertEquals("Probe-side key was different than build-side key.", key, record.f0); 
				}
				else {
					fail("No build side values found for a probe key.");
				}
				while ((record = buildSide.next(record)) != null) {
					numBuildValues++;
					Assert.assertEquals("Probe-side key was different than build-side key.", key, record.f0);
				}
				
				Long contained = map.get(key);
				if (contained == null) {
					contained = numBuildValues;
				}
				else {
					contained = contained + numBuildValues;
				}
				
				map.put(key, contained);
			}
		}
		
		join.close();
		
		Assert.assertEquals("Wrong number of keys", NUM_KEYS, map.size());
		for (Map.Entry<Integer, Long> entry : map.entrySet()) {
			long val = entry.getValue();
			int key = entry.getKey();
	
			if( key == REPEATED_VALUE_1 || key == REPEATED_VALUE_2) {
				Assert.assertEquals("Wrong number of values in per-key cross product for key " + key, 
							(PROBE_VALS_PER_KEY + REPEATED_VALUE_COUNT_PROBE) * (BUILD_VALS_PER_KEY + REPEATED_VALUE_COUNT_BUILD) * NUM_PROBES, val);
			} else {
				Assert.assertEquals("Wrong number of values in per-key cross product for key " + key, 
							PROBE_VALS_PER_KEY * BUILD_VALS_PER_KEY * NUM_PROBES, val);
			}
		}
		
		
		// ----------------------------------------------------------------------------------------
		
		this.memoryManager.release(join.getFreedMemory());
	}
```
```
Cyclomatic Complexity	 11
Assertions		 5
Lines of Code		106
-	
	public void testSpillingHashJoinWithTwoRecursions() throws IOException
	{
		// the following two values are known to have a hash-code collision on the first recursion level.
		// we use them to make sure one partition grows over-proportionally large
		final int REPEATED_VALUE_1 = 40559;
		final int REPEATED_VALUE_2 = 92882;
		final int REPEATED_VALUE_COUNT_BUILD = 200000;
		final int REPEATED_VALUE_COUNT_PROBE = 5;
		
		final int NUM_KEYS = 1000000;
		final int BUILD_VALS_PER_KEY = 3;
		final int PROBE_VALS_PER_KEY = 10;
		
		// create a build input that gives 3 million pairs with 3 values sharing the same key, plus 400k pairs with two colliding keys
		MutableObjectIterator<Tuple2<Integer, Integer>> build1 = new UniformIntTupleGenerator(NUM_KEYS, BUILD_VALS_PER_KEY, false);
		MutableObjectIterator<Tuple2<Integer, Integer>> build2 = new TestData.ConstantIntIntTuplesIterator(REPEATED_VALUE_1, 17, REPEATED_VALUE_COUNT_BUILD);
		MutableObjectIterator<Tuple2<Integer, Integer>> build3 = new TestData.ConstantIntIntTuplesIterator(REPEATED_VALUE_2, 23, REPEATED_VALUE_COUNT_BUILD);
		List<MutableObjectIterator<Tuple2<Integer, Integer>>> builds = new ArrayList<>();
		builds.add(build1);
		builds.add(build2);
		builds.add(build3);
		MutableObjectIterator<Tuple2<Integer, Integer>> buildInput = new UnionIterator<>(builds);
	

		// allocate the memory for the HashTable
		List<MemorySegment> memSegments;
		try {
			memSegments = this.memoryManager.allocatePages(MEM_OWNER, 896);
		}
		catch (MemoryAllocationException maex) {
			fail("Memory for the Join could not be provided.");
			return;
		}
		
		// create the map for validating the results
		HashMap<Integer, Long> map = new HashMap<Integer, Long>(NUM_KEYS);
		
		// ----------------------------------------------------------------------------------------
		
		final ReOpenableMutableHashTable<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> join = new ReOpenableMutableHashTable<>(
				this.recordBuildSideAccesssor, this.recordProbeSideAccesssor, 
				this.recordBuildSideComparator, this.recordProbeSideComparator, this.pactRecordComparator,
				memSegments, ioManager, true);
		
		for (int probe = 0; probe < NUM_PROBES; probe++) {
			// create a probe input that gives 10 million pairs with 10 values sharing a key
			MutableObjectIterator<Tuple2<Integer, Integer>> probeInput = getProbeInput(NUM_KEYS, PROBE_VALS_PER_KEY, REPEATED_VALUE_1, REPEATED_VALUE_2);
			if(probe == 0) {
				join.open(buildInput, probeInput);
			} else {
				join.reopenProbe(probeInput);
			}
			Tuple2<Integer, Integer> record;
			final Tuple2<Integer, Integer> recordReuse = new Tuple2<>();

			while (join.nextRecord())
			{	
				long numBuildValues = 0;
				
				final Tuple2<Integer, Integer> probeRec = join.getCurrentProbeRecord();
				Integer key = probeRec.f0;
				
				MutableObjectIterator<Tuple2<Integer, Integer>> buildSide = join.getBuildSideIterator();
				if ((record = buildSide.next(recordReuse)) != null) {
					numBuildValues = 1;
					Assert.assertEquals("Probe-side key was different than build-side key.", key, record.f0); 
				}
				else {
					fail("No build side values found for a probe key.");
				}
				while ((record = buildSide.next(recordReuse)) != null) {
					numBuildValues++;
					Assert.assertEquals("Probe-side key was different than build-side key.", key, record.f0);
				}
				
				Long contained = map.get(key);
				if (contained == null) {
					contained = numBuildValues;
				}
				else {
					contained = contained + numBuildValues;
				}
				
				map.put(key, contained);
			}
		}
		
		join.close();
		Assert.assertEquals("Wrong number of keys", NUM_KEYS, map.size());
		for (Map.Entry<Integer, Long> entry : map.entrySet()) {
			long val = entry.getValue();
			int key = entry.getKey();
	
			if( key == REPEATED_VALUE_1 || key == REPEATED_VALUE_2) {
				Assert.assertEquals("Wrong number of values in per-key cross product for key " + key, 
							(PROBE_VALS_PER_KEY + REPEATED_VALUE_COUNT_PROBE) * (BUILD_VALS_PER_KEY + REPEATED_VALUE_COUNT_BUILD) * NUM_PROBES, val);
			} else {
				Assert.assertEquals("Wrong number of values in per-key cross product for key " + key, 
							PROBE_VALS_PER_KEY * BUILD_VALS_PER_KEY * NUM_PROBES, val);
			}
		}
		
		
		// ----------------------------------------------------------------------------------------
		
		this.memoryManager.release(join.getFreedMemory());
	}
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		2
-	
	public void testValueStateRestoreWithWrongSerializers() {}
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		2
-	
	public void testListStateRestoreWithWrongSerializers() {}
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		2
-	
	public void testReducingStateRestoreWithWrongSerializers() {}
```
```
Cyclomatic Complexity	 1
Assertions		 3
Lines of Code		68
-	
	public void testStateOutputStream() throws IOException {
		File basePath = tempFolder.newFolder().getAbsoluteFile();

		try {
			// the state backend has a very low in-mem state threshold (15 bytes)
			FsStateBackend backend = CommonTestUtils.createCopySerializable(new FsStateBackend(basePath.toURI(), 15));
			JobID jobId = new JobID();

			// we know how FsCheckpointStreamFactory is implemented so we know where it
			// will store checkpoints
			File checkpointPath = new File(basePath.getAbsolutePath(), jobId.toString());

			CheckpointStreamFactory streamFactory = backend.createStreamFactory(jobId, "test_op");

			byte[] state1 = new byte[1274673];
			byte[] state2 = new byte[1];
			byte[] state3 = new byte[0];
			byte[] state4 = new byte[177];

			Random rnd = new Random();
			rnd.nextBytes(state1);
			rnd.nextBytes(state2);
			rnd.nextBytes(state3);
			rnd.nextBytes(state4);

			long checkpointId = 97231523452L;

			CheckpointStreamFactory.CheckpointStateOutputStream stream1 =
					streamFactory.createCheckpointStateOutputStream(checkpointId, System.currentTimeMillis());

			CheckpointStreamFactory.CheckpointStateOutputStream stream2 =
					streamFactory.createCheckpointStateOutputStream(checkpointId, System.currentTimeMillis());

			CheckpointStreamFactory.CheckpointStateOutputStream stream3 =
					streamFactory.createCheckpointStateOutputStream(checkpointId, System.currentTimeMillis());

			stream1.write(state1);
			stream2.write(state2);
			stream3.write(state3);

			FileStateHandle handle1 = (FileStateHandle) stream1.closeAndGetHandle();
			ByteStreamStateHandle handle2 = (ByteStreamStateHandle) stream2.closeAndGetHandle();
			ByteStreamStateHandle handle3 = (ByteStreamStateHandle) stream3.closeAndGetHandle();

			// use with try-with-resources
			StreamStateHandle handle4;
			try (CheckpointStreamFactory.CheckpointStateOutputStream stream4 =
					streamFactory.createCheckpointStateOutputStream(checkpointId, System.currentTimeMillis())) {
				stream4.write(state4);
				handle4 = stream4.closeAndGetHandle();
			}

			// close before accessing handle
			CheckpointStreamFactory.CheckpointStateOutputStream stream5 =
					streamFactory.createCheckpointStateOutputStream(checkpointId, System.currentTimeMillis());
			stream5.write(state4);
			stream5.close();
			try {
				stream5.closeAndGetHandle();
				fail();
			} catch (IOException e) {
				// uh-huh
			}

			validateBytesInStream(handle1.openInputStream(), state1);
			handle1.discardState();
			assertFalse(isDirectoryEmpty(basePath));
			ensureLocalFileDeleted(handle1.getFilePath());

			validateBytesInStream(handle2.openInputStream(), state2);
			handle2.discardState();

			// nothing was written to the stream, so it will return nothing
			assertNull(handle3);

			validateBytesInStream(handle4.openInputStream(), state4);
			handle4.discardState();
			assertTrue(isDirectoryEmpty(checkpointPath));
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		4
-	
	public void testConcurrentMapIfQueryable() throws Exception {
		//unsupported
	}
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		2
-	
	public void testValueStateRestoreWithWrongSerializers() {}
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		2
-	
	public void testListStateRestoreWithWrongSerializers() {}
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		2
-	
	public void testReducingStateRestoreWithWrongSerializers() {}
```
```
Cyclomatic Complexity	 1
Assertions		 5
Lines of Code		24
-	
	@SuppressWarnings("unchecked, deprecation")
	public void testNumStateEntries() throws Exception {
		KeyedStateBackend<Integer> backend = createKeyedBackend(IntSerializer.INSTANCE);

		ValueStateDescriptor<String> kvId = new ValueStateDescriptor<>("id", String.class, null);
		kvId.initializeSerializerUnlessSet(new ExecutionConfig());

		AsyncHeapKeyedStateBackend<Integer> heapBackend = (AsyncHeapKeyedStateBackend<Integer>) backend;

		assertEquals(0, heapBackend.numStateEntries());

		ValueState<String> state = backend.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);

		backend.setCurrentKey(0);
		state.update("hello");
		state.update("ciao");

		assertEquals(1, heapBackend.numStateEntries());

		backend.setCurrentKey(42);
		state.update("foo");

		assertEquals(2, heapBackend.numStateEntries());

		backend.setCurrentKey(0);
		state.clear();

		assertEquals(1, heapBackend.numStateEntries());

		backend.setCurrentKey(42);
		state.clear();

		assertEquals(0, heapBackend.numStateEntries());

		backend.dispose();
	}
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		26
-	
	public void testOversizedState() {
		try {
			MemoryStateBackend backend = new MemoryStateBackend(10);
			CheckpointStreamFactory streamFactory = backend.createStreamFactory(new JobID(), "test_op");

			HashMap<String, Integer> state = new HashMap<>();
			state.put("hey there", 2);
			state.put("the crazy brown fox stumbles over a sentence that does not contain every letter", 77);

			try {
				CheckpointStreamFactory.CheckpointStateOutputStream outStream =
						streamFactory.createCheckpointStateOutputStream(12, 459);

				ObjectOutputStream oos = new ObjectOutputStream(outStream);
				oos.writeObject(state);

				oos.flush();

				outStream.closeAndGetHandle();

				fail("this should cause an exception");
			}
			catch (IOException e) {
				// now darling, isn't that exactly what we wanted?
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
```
```
Cyclomatic Complexity	 1
Assertions		 3
Lines of Code		24
-	
	public void testStateStream() {
		try {
			MemoryStateBackend backend = new MemoryStateBackend();
			CheckpointStreamFactory streamFactory = backend.createStreamFactory(new JobID(), "test_op");

			HashMap<String, Integer> state = new HashMap<>();
			state.put("hey there", 2);
			state.put("the crazy brown fox stumbles over a sentence that does not contain every letter", 77);

			CheckpointStreamFactory.CheckpointStateOutputStream os = streamFactory.createCheckpointStateOutputStream(1, 2);
			ObjectOutputStream oos = new ObjectOutputStream(os);
			oos.writeObject(state);
			oos.flush();
			StreamStateHandle handle = os.closeAndGetHandle();

			assertNotNull(handle);

			try (ObjectInputStream ois = new ObjectInputStream(handle.openInputStream())) {
				assertEquals(state, ois.readObject());
				assertTrue(ois.available() <= 0);
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		25
-	
	public void testOversizedStateStream() {
		try {
			MemoryStateBackend backend = new MemoryStateBackend(10);
			CheckpointStreamFactory streamFactory = backend.createStreamFactory(new JobID(), "test_op");

			HashMap<String, Integer> state = new HashMap<>();
			state.put("hey there", 2);
			state.put("the crazy brown fox stumbles over a sentence that does not contain every letter", 77);

			CheckpointStreamFactory.CheckpointStateOutputStream os = streamFactory.createCheckpointStateOutputStream(1, 2);
			ObjectOutputStream oos = new ObjectOutputStream(os);

			try {
				oos.writeObject(state);
				oos.flush();
				os.closeAndGetHandle();
				fail("this should cause an exception");
			}
			catch (IOException e) {
				// oh boy! what an exception!
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		4
-	
	public void testConcurrentMapIfQueryable() throws Exception {
		//unsupported
	}
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		12
-  
  def testCaseClassTypeInfoInequality(): Unit = {
    val tpeInfo1 = new CaseClassTypeInfo[Tuple2[Int, String]](
      classOf[Tuple2[Int, String]],
      Array(),
      Array(BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO),
      Array("_1", "_2")) {
      override def createSerializer(config: ExecutionConfig): TypeSerializer[(Int, String)] = ???
    }

    val tpeInfo2 = new CaseClassTypeInfo[Tuple2[Int, Boolean]](
      classOf[Tuple2[Int, Boolean]],@@@+    }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		15
-  
  def testEitherTypeEquality(): Unit = {
    val eitherTypeInfo1 = new EitherTypeInfo[Integer, String, Either[Integer, String]](
      classOf[Either[Integer,String]],
      BasicTypeInfo.INT_TYPE_INFO,
      BasicTypeInfo.STRING_TYPE_INFO
    )
    val eitherTypeInfo2 = new EitherTypeInfo[Integer, String, Either[Integer, String]](
      classOf[Either[Integer,String]],
      BasicTypeInfo.INT_TYPE_INFO,
      BasicTypeInfo.STRING_TYPE_INFO
    )

    assert(eitherTypeInfo1.equals(eitherTypeInfo2))
    assert(eitherTypeInfo1.hashCode() == eitherTypeInfo2.hashCode())
  }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		7
-  
  def testOptionTypeEquality: Unit = {
    val optionTypeInfo1 = new OptionTypeInfo[Integer, Option[Integer]](BasicTypeInfo.INT_TYPE_INFO)
    val optionTypeInfo2 = new OptionTypeInfo[Integer, Option[Integer]](BasicTypeInfo.INT_TYPE_INFO)

    assert(optionTypeInfo1.equals(optionTypeInfo2))
    assert(optionTypeInfo1.hashCode == optionTypeInfo2.hashCode)
  }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		6
-  
  def testOptionTypeInequality: Unit = {
    val optionTypeInfo1 = new OptionTypeInfo[Integer, Option[Integer]](BasicTypeInfo.INT_TYPE_INFO)
    val optionTypeInfo2 = new OptionTypeInfo[String, Option[String]](BasicTypeInfo.STRING_TYPE_INFO)

    assert(!optionTypeInfo1.equals(optionTypeInfo2))
  }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		6
-  
  def testOptionTypeInequalityWithDifferentType: Unit = {
    val optionTypeInfo = new OptionTypeInfo[Integer, Option[Integer]](BasicTypeInfo.INT_TYPE_INFO)
    val genericTypeInfo = new GenericTypeInfo[Double](Double.getClass.asInstanceOf[Class[Double]])

    assert(!optionTypeInfo.equals(genericTypeInfo))
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		10
-  
  def testTraversableTypeInfoInequality(): Unit = {
    val tpeInfo1 = new TraversableTypeInfo[Seq[Int], Int](
      classOf[Seq[Int]],
      BasicTypeInfo.INT_TYPE_INFO.asInstanceOf[TypeInformation[Int]]) {
      override def createSerializer(executionConfig: ExecutionConfig): TypeSerializer[Seq[Int]] =
        ???
    }

    val tpeInfo2 = new TraversableTypeInfo[List[Int], Int](@@@+    }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		7
-  
  def testTryTypeEquality(): Unit = {
    val TryTypeInfo1 = new TryTypeInfo[Integer, Try[Integer]](BasicTypeInfo.INT_TYPE_INFO)
    val TryTypeInfo2 = new TryTypeInfo[Integer, Try[Integer]](BasicTypeInfo.INT_TYPE_INFO)

    assert(TryTypeInfo1.equals(TryTypeInfo2))
    assert(TryTypeInfo1.hashCode == TryTypeInfo2.hashCode)
  }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		6
-  
  def testTryTypeInequality(): Unit = {
    val TryTypeInfo1 = new TryTypeInfo[Integer, Try[Integer]](BasicTypeInfo.INT_TYPE_INFO)
    val TryTypeInfo2 = new TryTypeInfo[String, Try[String]](BasicTypeInfo.STRING_TYPE_INFO)

    //noinspection ComparingUnrelatedTypes
    assert(!TryTypeInfo1.equals(TryTypeInfo2))
  }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		6
-  
  def testTryTypeInequalityWithDifferentType(): Unit = {
    val TryTypeInfo = new TryTypeInfo[Integer, Try[Integer]](BasicTypeInfo.INT_TYPE_INFO)
    val genericTypeInfo = new GenericTypeInfo[Double](Double.getClass.asInstanceOf[Class[Double]])

    //noinspection ComparingUnrelatedTypes
    assert(!TryTypeInfo.equals(genericTypeInfo))
  }
```
```
Cyclomatic Complexity	 6
Assertions		 7
Lines of Code		58
-	
	public void testExecutionConfigSerialization() throws IOException, ClassNotFoundException {
		final long seed = System.currentTimeMillis();
		final Random r = new Random(seed);@@@ 
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		StreamGraph streamingJob = new StreamGraph(env);
		StreamingJobGraphGenerator compiler = new StreamingJobGraphGenerator(streamingJob);
		
		boolean closureCleanerEnabled = r.nextBoolean(), forceAvroEnabled = r.nextBoolean(), forceKryoEnabled = r.nextBoolean(), objectReuseEnabled = r.nextBoolean(), sysoutLoggingEnabled = r.nextBoolean();
		int dop = 1 + r.nextInt(10);
		
		ExecutionConfig config = streamingJob.getExecutionConfig();
		if(closureCleanerEnabled) {
			config.enableClosureCleaner();
		} else {
			config.disableClosureCleaner();
		}
		if(forceAvroEnabled) {
			config.enableForceAvro();
		} else {
			config.disableForceAvro();
		}
		if(forceKryoEnabled) {
			config.enableForceKryo();
		} else {
			config.disableForceKryo();
		}
		if(objectReuseEnabled) {
			config.enableObjectReuse();
		} else {
			config.disableObjectReuse();
		}
		if(sysoutLoggingEnabled) {
			config.enableSysoutLogging();
		} else {
			config.disableSysoutLogging();
		}
		config.setParallelism(dop);
		
		JobGraph jobGraph = compiler.createJobGraph();

		final String EXEC_CONFIG_KEY = "runtime.config";

		InstantiationUtil.writeObjectToConfig(jobGraph.getSerializedExecutionConfig(),
			jobGraph.getJobConfiguration(),
			EXEC_CONFIG_KEY);

		SerializedValue<ExecutionConfig> serializedExecutionConfig = InstantiationUtil.readObjectFromConfig(
				jobGraph.getJobConfiguration(),
				EXEC_CONFIG_KEY,
				Thread.currentThread().getContextClassLoader());

		assertNotNull(serializedExecutionConfig);

		ExecutionConfig executionConfig = serializedExecutionConfig.deserializeValue(getClass().getClassLoader());

		assertEquals(closureCleanerEnabled, executionConfig.isClosureCleanerEnabled());
		assertEquals(forceAvroEnabled, executionConfig.isForceAvroEnabled());
		assertEquals(forceKryoEnabled, executionConfig.isForceKryoEnabled());
		assertEquals(objectReuseEnabled, executionConfig.isObjectReuseEnabled());
		assertEquals(sysoutLoggingEnabled, executionConfig.isSysoutLoggingEnabled());
		assertEquals(dop, executionConfig.getParallelism());
	}
```
## 6c078c0e53346fd2ac178b133b352d0ee9a39e24 ##
```
Cyclomatic Complexity	8
Assertions		3
Lines of Code		37
-	
	public void testHashWithLargeUndirectedRMatGraph() throws Exception {
		// computation is too large for collection mode
		Assume.assumeFalse(mode == TestExecutionMode.COLLECTION);

		long directed_checksum;
		long undirected_checksum;
		switch (idType) {
			case "byte":
				return;

			case "short":
			case "char":
			case "integer":
				directed_checksum = 0x00000681fad1587eL;
				undirected_checksum = 0x0000068713b3b7f1L;
				break;

			case "long":
				directed_checksum = 0x000006928a6301b1L;
				undirected_checksum = 0x000006a399edf0e6L;
				break;

			case "string":
				directed_checksum = 0x000006749670a2f7L;
				undirected_checksum = 0x0000067f19c6c4d5L;
				break;

			default:
				throw new IllegalArgumentException("Unknown type: " + idType);
		}

		String expected = "\n" +
			"triplet count: 9276207, triangle count: 1439454, global clustering coefficient: 0.15517700[0-9]+\n" +
			"vertex count: 3349, average clustering coefficient: 0.33029442[0-9]+\n";

		expectedOutput(parameters(12, "directed", "undirected", "hash"),
			"\n" + new Checksum(3349, directed_checksum) + expected);
		expectedOutput(parameters(12, "undirected", "undirected", "hash"),
			"\n" + new Checksum(3349, undirected_checksum) + expected);@@@+		TestUtils.verifyParallelism(parameters(8, "directed", "directed", "print"), largeOperators);@@@+		TestUtils.verifyParallelism(parameters(8, "directed", "undirected", "print"), largeOperators);@@@+		TestUtils.verifyParallelism(parameters(8, "undirected", "undirected", "print"), largeOperators);@@@ 	}
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		4
-  (expected = classOf[ValidationException])
  def testObviousInvalidIndexTableApi(): Unit = {
    testTableApi('f2.at(0), "FAIL", "FAIL")
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		4
-  (expected = classOf[ValidationException])
  def testEmptyArraySql(): Unit = {
    testSqlApi("ARRAY[]", "FAIL")
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		4
-  (expected = classOf[ValidationException])
  def testEmptyArrayTableApi(): Unit = {
    testTableApi("FAIL", "array()", "FAIL")
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		4
-  (expected = classOf[ValidationException])
  def testNullArraySql(): Unit = {
    testSqlApi("ARRAY[NULL]", "FAIL")
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		4
-  (expected = classOf[ValidationException])
  def testDifferentTypesArraySql(): Unit = {
    testSqlApi("ARRAY[1, TRUE]", "FAIL")
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		4
-  (expected = classOf[ValidationException])
  def testDifferentTypesArrayTableApi(): Unit = {
    testTableApi("FAIL", "array(1, true)", "FAIL")
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		8
-  (expected = classOf[ValidationException])
  def testUnsupportedComparison(): Unit = {
    testAllApis(
      'f2 <= 'f5.at(1),
      "f2 <= f5.at(1)",
      "f2 <= f5[1]",
      "FAIL")
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		7
-  (expected = classOf[ValidationException])
  def testElementNonArray(): Unit = {
    testTableApi(
      'f0.element(),
      "FAIL",
      "FAIL")
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		6
-  (expected = classOf[ValidationException])
  def testElementNonArraySql(): Unit = {
    testSqlApi(
      "ELEMENT(f0)",
      "FAIL")
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		4
-  (expected = classOf[ValidationException])
  def testCardinalityOnNonArray(): Unit = {
    testTableApi('f0.cardinality(), "FAIL", "FAIL")
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		4
-  (expected = classOf[ValidationException])
  def testCardinalityOnNonArraySql(): Unit = {
    testSqlApi("CARDINALITY(f0)", "FAIL")
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		4
-  (expected = classOf[ValidationException])
  def testInvalidSubstring1(): Unit = {
    // Must fail. Parameter of substring must be an Integer not a Double.
    testTableApi("test".substring(2.0.toExpr), "FAIL", "FAIL")
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		4
-  (expected = classOf[ValidationException])
  def testIfInvalidTypesScala(): Unit = {
    testTableApi(('f6 && true).?(5, "false"), "FAIL", "FAIL")
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		4
-  (expected = classOf[ValidationException])
  def testIfInvalidTypesJava(): Unit = {
    testTableApi("FAIL", "(f8 && true).?(5, 'false')", "FAIL")
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		4
-  (expected = classOf[ValidationException])
  def testInvalidStringComparison1(): Unit = {
    testTableApi("w" === 4, "FAIL", "FAIL")
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		4
-  (expected = classOf[ValidationException])
  def testInvalidStringComparison2(): Unit = {
    testTableApi("w" > 4.toExpr, "FAIL", "FAIL")
  }
```
```
Cyclomatic Complexity	 3
Assertions		 1
Lines of Code		37
-	
	public void testContentAddressableStream() {

		BlobClient client = null;
		InputStream is = null;

		try {
			File testFile = File.createTempFile("testfile", ".dat");
			testFile.deleteOnExit();

			BlobKey origKey = prepareTestFile(testFile);

			InetSocketAddress serverAddress = new InetSocketAddress("localhost", BLOB_SSL_SERVER.getPort());
			client = new BlobClient(serverAddress, sslClientConfig);

			// Store the data
			is = new FileInputStream(testFile);
			BlobKey receivedKey = client.put(is);
			assertEquals(origKey, receivedKey);

			is.close();
			is = null;

			// Retrieve the data
			is = client.get(receivedKey);
			validateGet(is, testFile);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		finally {
			if (is != null) {
				try {
					is.close();
				} catch (Throwable t) {}
			}
			if (client != null) {
				try {
					client.close();
				} catch (Throwable t) {}
			}
		}
	}
```
```
Cyclomatic Complexity	 3
Assertions		 0
Lines of Code		36
-	
	public void testRegularStream() {

		final JobID jobID = JobID.generate();
		final String key = "testkey3";

		try {
			final File testFile = File.createTempFile("testfile", ".dat");
			testFile.deleteOnExit();
			prepareTestFile(testFile);

			BlobClient client = null;
			InputStream is = null;
			try {

				final InetSocketAddress serverAddress = new InetSocketAddress("localhost", BLOB_SSL_SERVER.getPort());
				client = new BlobClient(serverAddress, sslClientConfig);

				// Store the data
				is = new FileInputStream(testFile);
				client.put(jobID, key, is);

				is.close();
				is = null;

				// Retrieve the data
				is = client.get(jobID, key);
				validateGet(is, testFile);

			}
			finally {
				if (is != null) {
					is.close();
				}
				if (client != null) {
					client.close();
				}
			}

		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
```
```
Cyclomatic Complexity	 1
Assertions		 16
Lines of Code		39
-	
	public void testConjunctFutureCompletion() throws Exception {
		// some futures that we combine
		CompletableFuture<Object> future1 = new FlinkCompletableFuture<>();
		CompletableFuture<Object> future2 = new FlinkCompletableFuture<>();
		CompletableFuture<Object> future3 = new FlinkCompletableFuture<>();
		CompletableFuture<Object> future4 = new FlinkCompletableFuture<>();

		// some future is initially completed
		future2.complete(new Object());

		// build the conjunct future
		ConjunctFuture<?> result = futureFactory.createFuture(Arrays.asList(future1, future2, future3, future4));

		Future<?> resultMapped = result.thenAccept(new AcceptFunction<Object>() {
			@Override
			public void accept(Object value) {}
		});

		assertEquals(4, result.getNumFuturesTotal());
		assertEquals(1, result.getNumFuturesCompleted());
		assertFalse(result.isDone());
		assertFalse(resultMapped.isDone());

		// complete two more futures
		future4.complete(new Object());
		assertEquals(2, result.getNumFuturesCompleted());
		assertFalse(result.isDone());
		assertFalse(resultMapped.isDone());

		future1.complete(new Object());
		assertEquals(3, result.getNumFuturesCompleted());
		assertFalse(result.isDone());
		assertFalse(resultMapped.isDone());

		// complete one future again
		future1.complete(new Object());
		assertEquals(3, result.getNumFuturesCompleted());
		assertFalse(result.isDone());
		assertFalse(resultMapped.isDone());

		// complete the final future
		future3.complete(new Object());
		assertEquals(4, result.getNumFuturesCompleted());
		assertTrue(result.isDone());
		assertTrue(resultMapped.isDone());
	}
```
```
Cyclomatic Complexity	 1
Assertions		 9
Lines of Code		33
-	
	public void testConjunctFutureFailureOnFirst() throws Exception {

		CompletableFuture<Object> future1 = new FlinkCompletableFuture<>();
		CompletableFuture<Object> future2 = new FlinkCompletableFuture<>();
		CompletableFuture<Object> future3 = new FlinkCompletableFuture<>();
		CompletableFuture<Object> future4 = new FlinkCompletableFuture<>();

		// build the conjunct future
		ConjunctFuture<?> result = futureFactory.createFuture(Arrays.asList(future1, future2, future3, future4));

		Future<?> resultMapped = result.thenAccept(new AcceptFunction<Object>() {
			@Override
			public void accept(Object value) {}
		});

		assertEquals(4, result.getNumFuturesTotal());
		assertEquals(0, result.getNumFuturesCompleted());
		assertFalse(result.isDone());
		assertFalse(resultMapped.isDone());

		future2.completeExceptionally(new IOException());

		assertEquals(0, result.getNumFuturesCompleted());
		assertTrue(result.isDone());
		assertTrue(resultMapped.isDone());

		try {
			result.get();
			fail();
		} catch (ExecutionException e) {
			assertTrue(e.getCause() instanceof IOException);
		}

		try {
			resultMapped.get();
			fail();
		} catch (ExecutionException e) {
			assertTrue(e.getCause() instanceof IOException);
		}
	}
```
```
Cyclomatic Complexity	 1
Assertions		 3
Lines of Code		23
-	
	public void calculateNetworkBufFromHeapSize() throws Exception {
		PowerMockito.mockStatic(EnvironmentInformation.class);
		// some defaults:
		when(EnvironmentInformation.getSizeOfFreeHeapMemoryWithDefrag()).thenReturn(1000L << 20); // 1000MB
		when(EnvironmentInformation.getMaxJvmHeapMemory()).thenReturn(1000L << 20); // 1000MB

		TaskManagerServicesConfiguration tmConfig;

		tmConfig = getTmConfig(TaskManagerOptions.MANAGED_MEMORY_SIZE.defaultValue(),
			TaskManagerOptions.MANAGED_MEMORY_FRACTION.defaultValue(),
			0.1f, 60L << 20, 1L << 30, MemoryType.HEAP);
		when(EnvironmentInformation.getSizeOfFreeHeapMemoryWithDefrag()).thenReturn(1000L << 20); // 1000MB
		assertEquals(100L << 20, TaskManagerServices.calculateNetworkBufferMemory(tmConfig));

		tmConfig = getTmConfig(10, TaskManagerOptions.MANAGED_MEMORY_FRACTION.defaultValue(),
			0.1f, 60L << 20, 1L << 30, MemoryType.OFF_HEAP);
		when(EnvironmentInformation.getMaxJvmHeapMemory()).thenReturn(890L << 20); // 890MB
		assertEquals((100L << 20) + 1 /* one too many due to floating point imprecision */,
			TaskManagerServices.calculateNetworkBufferMemory(tmConfig));

		tmConfig = getTmConfig(-1, 0.1f,
			0.1f, 60L << 20, 1L << 30, MemoryType.OFF_HEAP);
		when(EnvironmentInformation.getMaxJvmHeapMemory()).thenReturn(810L << 20); // 810MB
		assertEquals((100L << 20) + 1 /* one too many due to floating point imprecision */,
			TaskManagerServices.calculateNetworkBufferMemory(tmConfig));
	}
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		4
-	
	public void testRestoreWithInterruptLegacy() throws Exception {
		testRestoreWithInterrupt(LEGACY);
	}
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		13
-  
  def testReduceAlignedTimeWindows(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val source = env.fromElements(("hello", 1), ("hello", 2))
    
    val window1 = source
      .keyBy(0)
      .window(SlidingAlignedProcessingTimeWindows.of(Time.seconds(1), Time.milliseconds(100)))
      .reduce(new DummyReducer())

    val transform1 = window1.javaStream.getTransformation
        .asInstanceOf[OneInputTransformation[(String, Int), (String, Int)]]
    
    val operator1 = transform1.getOperator

    assertTrue(operator1.isInstanceOf[AggregatingProcessingTimeWindowOperator[_, _]])
  }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		20
-  
  def testApplyAlignedTimeWindows(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    val source = env.fromElements(("hello", 1), ("hello", 2))

    val window1 = source
      .keyBy(0)
      .window(TumblingAlignedProcessingTimeWindows.of(Time.minutes(1)))
      .apply(new WindowFunction[(String, Int), (String, Int), Tuple, TimeWindow]() {
        def apply(
                   key: Tuple,
                   window: TimeWindow,
                   values: Iterable[(String, Int)],
                   out: Collector[(String, Int)]) { }
      })

    val transform1 = window1.javaStream.getTransformation
      .asInstanceOf[OneInputTransformation[(String, Int), (String, Int)]]

    val operator1 = transform1.getOperator

    assertTrue(operator1.isInstanceOf[AccumulatingProcessingTimeWindowOperator[_, _, _]])@@@   }
```
```
Cyclomatic Complexity	 2
Assertions		 1
Lines of Code		47
-	
	public void testJobWithoutObjectReuse() throws Exception {
		isCollectionExecution = false;

		startCluster();
		try {
			// pre-submit
			try {
				preSubmit();
			}
			catch (Exception e) {
				System.err.println(e.getMessage());
				e.printStackTrace();
				Assert.fail("Pre-submit work caused an error: " + e.getMessage());
			}

			// prepare the test environment
			TestEnvironment env = new TestEnvironment(this.executor, this.parallelism, false);
			env.getConfig().disableObjectReuse();
			env.setAsContext();

			// Possibly run the test multiple times
			for (int i = 0; i < numberOfTestRepetitions; i++) {
				// call the test program
				try {
					testProgram();
					this.latestExecutionResult = env.getLastJobExecutionResult();
				}
				catch (Exception e) {
					System.err.println(e.getMessage());
					e.printStackTrace();
					Assert.fail("Error while calling the test program: " + e.getMessage());
				}

				Assert.assertNotNull("The test program never triggered an execution.",
						this.latestExecutionResult);
			}

			// post-submit
			try {
				postSubmit();
			}
			catch (Exception e) {
				System.err.println(e.getMessage());
				e.printStackTrace();
				Assert.fail("Post-submit work caused an error: " + e.getMessage());
			}
		} finally {
			stopCluster();
			TestEnvironment.unsetAsContext();
		}
	}
```
