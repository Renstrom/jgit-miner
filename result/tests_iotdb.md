## 22fe7d31729838eafe9c56573cbd062a906af714 ##
```
Commit	22fe7d31729838eafe9c56573cbd062a906af714
Directory name		iotdb
Cyclomatic Complexity	 3
Assertions		 23
Lines of Code		69
	
	public void testflushIndexTrees() throws IOException {
		OverflowSupport support = new OverflowSupport();
		OverflowTestUtils.produceUpdateData(support);
		List<OFRowGroupListMetadata> rowGroupListMetadatas = io.flush(support.getOverflowSeriesMap());
		assertEquals(2, rowGroupListMetadatas.size());
		OFRowGroupListMetadata d1 = null;
		OFRowGroupListMetadata d2 = null;
		if (rowGroupListMetadatas.get(0).getDeltaObjectId().equals(deltaObjectId1)) {
			d1 = rowGroupListMetadatas.get(0);
			d2 = rowGroupListMetadatas.get(1);
		} else {
			d1 = rowGroupListMetadatas.get(1);
			d2 = rowGroupListMetadatas.get(0);
		}
		assertEquals(2, d1.getMetaDatas().size());
		TimeSeriesChunkMetaData d1s1metadata = null;
		TimeSeriesChunkMetaData d1s2metadata = null;
		TimeSeriesChunkMetaData d2s1metadata = null;
		TimeSeriesChunkMetaData d2s2metadata = null;
		if (d1.getMetaDatas().get(0).getMeasurementId().equals(measurementId1)) {
			d1s1metadata = d1.getMetaDatas().get(0).getMetaDatas().get(0);
			d1s2metadata = d1.getMetaDatas().get(1).getMetaDatas().get(0);

			d2s1metadata = d2.getMetaDatas().get(0).getMetaDatas().get(0);
			d2s2metadata = d2.getMetaDatas().get(1).getMetaDatas().get(0);
		} else {
			d1s1metadata = d1.getMetaDatas().get(1).getMetaDatas().get(0);
			d1s2metadata = d1.getMetaDatas().get(0).getMetaDatas().get(0);

			d2s1metadata = d2.getMetaDatas().get(1).getMetaDatas().get(0);
			d2s2metadata = d2.getMetaDatas().get(0).getMetaDatas().get(0);
		}

		// d1 s1
		IntervalTreeOperation index = new IntervalTreeOperation(dataType1);
		DynamicOneColumnData d1s1 = index.queryFileBlock(null, null, null,
				OverflowIO.readOneTimeSeriesChunk(d1s1metadata, reader), new DynamicOneColumnData());
		assertEquals(2, d1s1.getTime(0));
		assertEquals(10, d1s1.getTime(1));
		assertEquals(20, d1s1.getTime(2));
		assertEquals(30, d1s1.getTime(3));

		assertEquals(10, d1s1.getInt(0));
		assertEquals(20, d1s1.getInt(1));
		// d1 s2
		index = new IntervalTreeOperation(dataType1);
		DynamicOneColumnData d1s2 = index.queryFileBlock(null, null, null,
				OverflowIO.readOneTimeSeriesChunk(d1s2metadata, reader), new DynamicOneColumnData());
		assertEquals(0, d1s2.getTime(0));
		assertEquals(-10, d1s2.getTime(1));
		assertEquals(20, d1s2.getTime(2));
		assertEquals(30, d1s2.getTime(3));

		assertEquals(0, d1s2.getInt(0));
		assertEquals(20, d1s2.getInt(1));
		// d2 s1
		index = new IntervalTreeOperation(dataType2);
		DynamicOneColumnData d2s1 = index.queryFileBlock(null, null, null,
				OverflowIO.readOneTimeSeriesChunk(d2s1metadata, reader), new DynamicOneColumnData());
		assertEquals(10, d2s1.getTime(0));
		assertEquals(14, d2s1.getTime(1));
		assertEquals(15, d2s1.getTime(2));
		assertEquals(40, d2s1.getTime(3));

		assertEquals(10.5f, d2s1.getFloat(0), error);
		assertEquals(20.5f, d2s1.getFloat(1), error);
		// d2 s2
		index = new IntervalTreeOperation(dataType2);
		DynamicOneColumnData d2s2 = index.queryFileBlock(null, null, null,
				OverflowIO.readOneTimeSeriesChunk(d2s2metadata, reader), new DynamicOneColumnData());

		assertEquals(0, d2s2.getTime(0));
		assertEquals(-20, d2s2.getTime(1));

		assertEquals(0, d2s2.getFloat(0), error);
	}
```
```
Previous Commit	22fe7d31729838eafe9c56573cbd062a906af714
Directory name	iotdb
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		15
    
    public void testSlimit1() throws QueryProcessorException, ArgsErrorException {
        String sqlStr =
                "select * from root.vehicle.d1 where s1 < 20 and time <= now() slimit 10";
        ASTNode astTree;
        try {
            astTree = ParseGenerator.generateAST(sqlStr); // parse string to ASTTree
        } catch (ParseException e) {
            //e.printStackTrace();
            throw new IllegalASTFormatException("parsing error,statement: " + sqlStr + " .message:" + e.getMessage());
        }
        ASTNode astNode = ParseUtils.findRootNonNullToken(astTree);
        RootOperator operator = generator.getLogicalPlan(astNode);
        Assert.assertEquals(operator.getClass(), QueryOperator.class);
        Assert.assertEquals(((QueryOperator) operator).getSeriesLimit(), 10);
     }
```
```
Previous Commit	22fe7d31729838eafe9c56573cbd062a906af714
Directory name	iotdb
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		13
    (expected = LogicalOperatorException.class)
    public void testSlimit2() throws QueryProcessorException, ArgsErrorException {
        String sqlStr =
                "select * from root.vehicle.d1 where s1 < 20 and time <= now() slimit 1111111111111111111111";
        ASTNode astTree;
        try {
            astTree = ParseGenerator.generateAST(sqlStr); // parse string to ASTTree
        } catch (ParseException e) {
            //e.printStackTrace();
            throw new IllegalASTFormatException("parsing error,statement: " + sqlStr + " .message:" + e.getMessage());
        }
        ASTNode astNode = ParseUtils.findRootNonNullToken(astTree);
        RootOperator operator = generator.getLogicalPlan(astNode);
        // expected to throw LogicalOperatorException: SLIMIT <SN>: SN should be Int32.
     }
```
```
Previous Commit	22fe7d31729838eafe9c56573cbd062a906af714
Directory name	iotdb
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		13
    (expected = LogicalOperatorException.class)
    public void testSlimit3() throws QueryProcessorException, ArgsErrorException {
        String sqlStr =
                "select * from root.vehicle.d1 where s1 < 20 and time <= now() slimit 0";
        ASTNode astTree;
        try {
            astTree = ParseGenerator.generateAST(sqlStr); // parse string to ASTTree
        } catch (ParseException e) {
            //e.printStackTrace();
            throw new IllegalASTFormatException("parsing error,statement: " + sqlStr + " .message:" + e.getMessage());
        }
        ASTNode astNode = ParseUtils.findRootNonNullToken(astTree);
        RootOperator operator = generator.getLogicalPlan(astNode);
        // expected to throw LogicalOperatorException: SLIMIT <SN>: SN must be a positive integer and can not be zero.
     }
```
```
Previous Commit	22fe7d31729838eafe9c56573cbd062a906af714
Directory name	iotdb
Cyclomatic Complexity	 1
Assertions		 3
Lines of Code		16
    
    public void testSoffset() throws QueryProcessorException, ArgsErrorException {
        String sqlStr =
                "select * from root.vehicle.d1 where s1 < 20 and time <= now() slimit 10 soffset 1";
        ASTNode astTree;
        try {
            astTree = ParseGenerator.generateAST(sqlStr); // parse string to ASTTree
        } catch (ParseException e) {
            //e.printStackTrace();
            throw new IllegalASTFormatException("parsing error,statement: " + sqlStr + " .message:" + e.getMessage());
        }
        ASTNode astNode = ParseUtils.findRootNonNullToken(astTree);
        RootOperator operator = generator.getLogicalPlan(astNode);
        Assert.assertEquals(operator.getClass(), QueryOperator.class);
        Assert.assertEquals(((QueryOperator) operator).getSeriesLimit(), 10);
        Assert.assertEquals(((QueryOperator) operator).getSeriesOffset(), 1);
     }
```
```
Previous Commit	22fe7d31729838eafe9c56573cbd062a906af714
Directory name	iotdb
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		11
    
    public void testMetadata() throws QueryProcessorException, ArgsErrorException, ProcessorException {
        String metadata = "create timeseries root.vehicle.d1.s1 with datatype=INT32,encoding=RLE";
        QueryProcessor processor = new QueryProcessor(new MemIntQpExecutor());
        MetadataPlan plan = (MetadataPlan)processor.parseSQLToPhysicalPlan(metadata);
        assertEquals("path: root.vehicle.d1.s1\n" +
                "dataType: INT32\n" +
                "encoding: RLE\n" +
                "namespace type: ADD_PATH\n" +
                "args: " , plan.toString());
    }
```
```
Previous Commit	22fe7d31729838eafe9c56573cbd062a906af714
Directory name	iotdb
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		13
    
    public void testAuthor() throws QueryProcessorException, ArgsErrorException, ProcessorException {
        String sql = "grant role xm privileges 'SET_STORAGE_GROUP','DELETE_TIMESERIES' on root.vehicle.d1.s1";
        QueryProcessor processor = new QueryProcessor(new MemIntQpExecutor());
        AuthorPlan plan = (AuthorPlan) processor.parseSQLToPhysicalPlan(sql);
        assertEquals("userName: null\n" +
                "roleName: xm\n" +
                "password: null\n" +
                "newPassword: null\n" +
                "permissions: [0, 4]\n" +
                "nodeName: root.vehicle.d1.s1\n" +
                "authorType: GRANT_ROLE", plan.toString());
    }
```
```
Previous Commit	22fe7d31729838eafe9c56573cbd062a906af714
Directory name	iotdb
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		9
    
    public void testProperty() throws QueryProcessorException, ArgsErrorException, ProcessorException {
        String sql = "add label label1021 to property propropro";
        QueryProcessor processor = new QueryProcessor(new MemIntQpExecutor());
        PropertyPlan plan = (PropertyPlan) processor.parseSQLToPhysicalPlan(sql);
        assertEquals("propertyPath: propropro.label1021\n" +
                "metadataPath: null\n" +
                "propertyType: ADD_PROPERTY_LABEL", plan.toString());
    }
```
```
Previous Commit	22fe7d31729838eafe9c56573cbd062a906af714
Directory name	iotdb
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		9
    
    public void testQuery1() throws QueryProcessorException, ArgsErrorException, ProcessorException {
        String sqlStr =
                "SELECT s1 FROM root.vehicle.d1 WHERE time > 5000";
        PhysicalPlan plan = processor.parseSQLToPhysicalPlan(sqlStr);
        QueryFilter queryFilter = ((QueryPlan) plan).getQueryFilter();
        QueryFilter expect = new GlobalTimeFilter(TimeFilter.gt(5000L));
        assertEquals(expect.toString(), queryFilter.toString());
    }
```
```
Previous Commit	22fe7d31729838eafe9c56573cbd062a906af714
Directory name	iotdb
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		10
    
    public void testQuery3() throws QueryProcessorException, ArgsErrorException, ProcessorException {
        String sqlStr =
                "SELECT s1 FROM root.vehicle.d1 WHERE time > 50 and time <= 100 or s1 < 10";
        PhysicalPlan plan = processor.parseSQLToPhysicalPlan(sqlStr);
        QueryFilter queryFilter = ((QueryPlan) plan).getQueryFilter();
        QueryFilter expect = new GlobalTimeFilter(FilterFactory.and(TimeFilter.gt(50L), TimeFilter.ltEq(100L)));
        expect = QueryFilterFactory.or(expect, new SeriesFilter<>(new Path("root.vehicle.d1.s1"), ValueFilter.lt(10)));
        assertEquals(expect.toString(), queryFilter.toString());
    }
```
```
Previous Commit	22fe7d31729838eafe9c56573cbd062a906af714
Directory name	iotdb
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		6
    
    public void testConcat1() throws QueryProcessorException, RecognitionException, ArgsErrorException, ProcessorException {
        String inputSQL = "select s1 from root.laptop.d1";
        PhysicalPlan plan = processor.parseSQLToPhysicalPlan(inputSQL);
        assertEquals("root.laptop.d1.s1", plan.getPaths().get(0).toString());
    }
```
```
Previous Commit	22fe7d31729838eafe9c56573cbd062a906af714
Directory name	iotdb
Cyclomatic Complexity	 1
Assertions		 3
Lines of Code		8
    
    public void testConcat2() throws QueryProcessorException, RecognitionException, ArgsErrorException, ProcessorException {
        String inputSQL = "select s1 from root.laptop.*";
        PhysicalPlan plan = processor.parseSQLToPhysicalPlan(inputSQL);
        assertEquals("root.laptop.d1.s1", plan.getPaths().get(0).toString());
        assertEquals("root.laptop.d2.s1", plan.getPaths().get(1).toString());
        assertEquals("root.laptop.d3.s1", plan.getPaths().get(2).toString());
    }
```
```
Previous Commit	22fe7d31729838eafe9c56573cbd062a906af714
Directory name	iotdb
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		7
    
    public void testConcat3() throws QueryProcessorException, RecognitionException, ArgsErrorException, ProcessorException {
        String inputSQL = "select s1 from root.laptop.d1 where s1 < 10";
        PhysicalPlan plan = processor.parseSQLToPhysicalPlan(inputSQL);
        SeriesFilter seriesFilter = new SeriesFilter(new Path("root.laptop.d1.s1"), ValueFilter.lt(10));
        assertEquals(seriesFilter.toString(), ((QueryPlan)plan).getQueryFilter().toString());
    }
```
## 8e118f4095fdb802c8c7b3f802e54087372b8120 ##
```
Commit	8e118f4095fdb802c8c7b3f802e54087372b8120
Directory name		server
Cyclomatic Complexity	 2
Assertions		 1
Lines of Code		15
  
  public void watermarkDetect() throws ParseException {
    ArrayList<String> ans = new ArrayList<>(Arrays.asList("TOK_WATERMARK_DETECT",
        "TOK_QUERY", "TOK_SELECT", "TOK_PATH", "*", "TOK_FROM", "TOK_PATH", "TOK_ROOT",
        "TOK_FLOAT_COMB", "0", ".", "99"));
    ArrayList<String> rec = new ArrayList<>();
    AstNode astTree = ParseGenerator.generateAST("watermark_detect(select * from root, 0.99)");
    astTree = ParseUtils.findRootNonNullToken(astTree);
    recursivePrintSon(astTree, rec);

    int i = 0;
    while (i <= rec.size() - 1) {
      assertEquals(rec.get(i), ans.get(i));
      i++;
    }
  }
```
## e0ce236efa433763c1e3f79f8d7149c8d04a09c8 ##
```
Commit	e0ce236efa433763c1e3f79f8d7149c8d04a09c8
Directory name		testcontainer
Cyclomatic Complexity	 -1
Assertions		 8
Lines of Code		0
  //  
  //  public void vectorCountTest() throws IoTDBConnectionException, StatementExecutionException {
  //    List<List<String>> measurementList = new ArrayList<>();
  //    List<String> schemaNames = new ArrayList<>();
  //    List<List<TSEncoding>> encodingList = new ArrayList<>();
  //    List<List<TSDataType>> dataTypeList = new ArrayList<>();
  //    List<CompressionType> compressionTypes = new ArrayList<>();
  //    List<TSDataType> dataTypes = new ArrayList<>();
  //    List<TSEncoding> encodings = new ArrayList<>();
  //    String[] vectorMeasurements = new String[10];
  //
  //    Stream.iterate(0, i -> i + 1)
  //        .limit(10)
  //        .forEach(
  //            i -> {
  //              dataTypes.add(TSDataType.DOUBLE);
  //              vectorMeasurements[i] = "vm" + i;
  //              encodings.add(TSEncoding.RLE);
  //              compressionTypes.add(CompressionType.SNAPPY);
  //            });
  //    schemaNames.add("schema");
  //    encodingList.add(encodings);
  //    dataTypeList.add(dataTypes);
  //    measurementList.add(Arrays.asList(vectorMeasurements));
  //
  //    session.createSchemaTemplate(
  //        "testcontainer",
  //        schemaNames,
  //        measurementList,
  //        dataTypeList,
  //        encodingList,
  //        compressionTypes);
  //    session.setStorageGroup("root.template");
  //    session.setSchemaTemplate("testcontainer", "root.template");
  //
  //    VectorMeasurementSchema vectorMeasurementSchema =
  //        new VectorMeasurementSchema(
  //            "vector", vectorMeasurements, dataTypes.toArray(new TSDataType[0]));
  //
  //    Tablet tablet = new Tablet("root.template.device1.vector",
  //    Arrays.asList(vectorMeasurementSchema));
  //    tablet.setAligned(true);
  //    for (int i = 0; i < 10; i++) {
  //      tablet.addTimestamp(i, i);
  //      for (int j = 0; j < 10; j++) {
  //        tablet.addValue("vm" + j, i, (double) i);
  //        tablet.rowSize++;
  //      }
  //    }
  //    session.insertTablet(tablet);
  //
  //    SessionDataSet sessionDataSet =
  //        session.executeQueryStatement("select count(*) from root.template.device1");
  //    Assert.assertTrue(sessionDataSet.hasNext());
  //    RowRecord next = sessionDataSet.next();
  //    Assert.assertEquals(10, next.getFields().get(0).getLongV());
  //
  //    sessionDataSet = session.executeQueryStatement("select count(vm1) from
  // root.template.device1");
  //    Assert.assertTrue(sessionDataSet.hasNext());
  //    next = sessionDataSet.next();
  //    Assert.assertEquals(10, next.getFields().get(0).getLongV());
  //
  //    sessionDataSet =
  //        session.executeQueryStatement("select count(vm1),count(vm2) from
  // root.template.device1");
  //    Assert.assertTrue(sessionDataSet.hasNext());
  //    next = sessionDataSet.next();
  //    Assert.assertEquals(2, next.getFields().size());
  //    Assert.assertEquals(10, next.getFields().get(0).getLongV());
  //    Assert.assertEquals(10, next.getFields().get(1).getLongV());
  //  }
```
```
Previous Commit	e0ce236efa433763c1e3f79f8d7149c8d04a09c8
Directory name	testcontainer
Cyclomatic Complexity	 2
Assertions		 1
Lines of Code		10
  
  public void testApplyMerge() throws InterruptedException {
    String sql = "MERGE";
    try {
      // Wait for 3S so that the leader can be elected
      Thread.sleep(3000);
      writeStatement.execute(sql);
    } catch (SQLException e) {
      Assert.assertNull(e);
    }
  }
```
```
Previous Commit	e0ce236efa433763c1e3f79f8d7149c8d04a09c8
Directory name	testcontainer
Cyclomatic Complexity	 2
Assertions		 1
Lines of Code		10
  
  public void testCreateSnapshot() throws InterruptedException {
    String sql = "CREATE SNAPSHOT FOR SCHEMA";
    try {
      // Wait for 3S so that the leader can be elected
      Thread.sleep(3000);
      writeStatement.execute(sql);
    } catch (SQLException e) {
      Assert.assertNull(e);
    }
  }
```
## 1c5840769bfb58fec555294c0e78cea37d1cd744 ##
```
Commit	1c5840769bfb58fec555294c0e78cea37d1cd744
Directory name		integration
Cyclomatic Complexity	 2
Assertions		 2
Lines of Code		49
  
  public void testRestartCompaction()
      throws SQLException, ClassNotFoundException, IOException, StorageEngineException {
    EnvironmentUtils.envSetUp();
    Class.forName(Config.JDBC_DRIVER_NAME);

    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("insert into root.turbine.d1(timestamp,s1) values(2,1.0)");
      statement.execute("flush");
      statement.execute("insert into root.turbine.d1(timestamp,s1) values(3,1.0)");
      statement.execute("flush");
      statement.execute("insert into root.turbine.d1(timestamp,s1) values(4,1.0)");
      statement.execute("flush");
      statement.execute("insert into root.turbine.d1(timestamp,s1) values(5,1.0)");
      statement.execute("flush");
      statement.execute("insert into root.turbine.d1(timestamp,s1) values(6,1.0)");
      statement.execute("flush");
      statement.execute("insert into root.turbine.d1(timestamp,s1) values(1,1.0)");
      statement.execute("flush");
      statement.execute("insert into root.turbine.d1(timestamp,s1) values(7,1.0)");
      statement.execute("flush");
    }

    try {
      CompactionTaskManager.getInstance().waitAllCompactionFinish();
      Thread.sleep(10000);
      EnvironmentUtils.restartDaemon();
    } catch (Exception e) {
      Assert.fail();
    }

    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet = statement.execute("SELECT s1 FROM root.turbine.d1");
      assertTrue(hasResultSet);
      String[] exp = new String[] {"1,1.0", "2,1.0", "3,1.0", "4,1.0", "5,1.0", "6,1.0", "7,1.0"};
      int cnt = 0;
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          String result = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(2);
          assertEquals(exp[cnt], result);
          cnt++;
        }
      }
    }

    EnvironmentUtils.cleanEnv();
  }
```
