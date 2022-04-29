## 64848c7ebfd5b95c528c0890ef47990de4e0a8f3 ##
```
Commit	64848c7ebfd5b95c528c0890ef47990de4e0a8f3
Directory name		extensions-core/hdfs-storage
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		12
  (expected = SegmentLoadingException.class)
  public void testFindSegmentsFail2() throws SegmentLoadingException
  {
    // will fail to desierialize descriptor.json because DefaultObjectMapper doesn't recognize NumberedShardSpec
    final HdfsDataSegmentFinder hdfsDataSegmentFinder = new HdfsDataSegmentFinder(conf, new DefaultObjectMapper());
    try {
      hdfsDataSegmentFinder.findSegments(dataSourceDir.toString(), false);
    }
    catch (SegmentLoadingException e) {
      Assert.assertTrue(e.getCause() instanceof IOException);
      throw e;
    }
  }
```
```
Previous Commit	64848c7ebfd5b95c528c0890ef47990de4e0a8f3
Directory name	extensions-core/s3-extensions
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		13
  (expected = SegmentLoadingException.class)
  public void testFindSegmentsFail2() throws SegmentLoadingException
  {
    final S3DataSegmentFinder s3DataSegmentFinder = new S3DataSegmentFinder(
        mockS3Client, config, new DefaultObjectMapper());

    try {
      s3DataSegmentFinder.findSegments("", false);
    }
    catch (SegmentLoadingException e) {
      Assert.assertTrue(e.getCause() instanceof IOException);
      throw e;
    }
  }
```
```
Previous Commit	64848c7ebfd5b95c528c0890ef47990de4e0a8f3
Directory name	server
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		12
  (expected = SegmentLoadingException.class)
  public void testFindSegmentsFail2() throws SegmentLoadingException
  {
    // will fail to desierialize descriptor.json because DefaultObjectMapper doesn't recognize NumberedShardSpec
    final LocalDataSegmentFinder localDataSegmentFinder = new LocalDataSegmentFinder(new DefaultObjectMapper());
    try {
      localDataSegmentFinder.findSegments(dataSourceDir.getAbsolutePath(), false);
    }
    catch (SegmentLoadingException e) {
      Assert.assertTrue(e.getCause() instanceof IOException);
      throw e;
    }
  }
```
## a2538d264d7dad829bcc48968100043694b55475 ##
```
Commit	a2538d264d7dad829bcc48968100043694b55475
Directory name		extensions-core/avro-extensions
Cyclomatic Complexity	 1
Assertions		 11
Lines of Code		37
  
  public void jsonPathExtractorExtractUnionsByType()
  {
    final AvroFlattenerMaker flattener = new AvroFlattenerMaker(false, false, true);

    // Unmamed types are accessed by type

    // int
    Assert.assertEquals(1, flattener.makeJsonPathExtractor("$.someMultiMemberUnion.int").apply(
        AvroStreamInputRowParserTest.buildSomeAvroDatumWithUnionValue(1)));

    // long
    Assert.assertEquals(1L, flattener.makeJsonPathExtractor("$.someMultiMemberUnion.long").apply(
        AvroStreamInputRowParserTest.buildSomeAvroDatumWithUnionValue(1L)));

    // float
    Assert.assertEquals((float) 1.0, flattener.makeJsonPathExtractor("$.someMultiMemberUnion.float").apply(
        AvroStreamInputRowParserTest.buildSomeAvroDatumWithUnionValue((float) 1.0)));

    // double
    Assert.assertEquals(1.0, flattener.makeJsonPathExtractor("$.someMultiMemberUnion.double").apply(
        AvroStreamInputRowParserTest.buildSomeAvroDatumWithUnionValue(1.0)));

    // string
    Assert.assertEquals("string", flattener.makeJsonPathExtractor("$.someMultiMemberUnion.string").apply(
        AvroStreamInputRowParserTest.buildSomeAvroDatumWithUnionValue(new Utf8("string"))));

    // bytes
    Assert.assertArrayEquals(new byte[] {1}, (byte[]) flattener.makeJsonPathExtractor("$.someMultiMemberUnion.bytes").apply(
        AvroStreamInputRowParserTest.buildSomeAvroDatumWithUnionValue(ByteBuffer.wrap(new byte[] {1}))));

    // map
    Assert.assertEquals(2, flattener.makeJsonPathExtractor("$.someMultiMemberUnion.map.two").apply(
        AvroStreamInputRowParserTest.buildSomeAvroDatumWithUnionValue(new HashMap<String, Integer>() {{
            put("one", 1);
            put("two", 2);
            put("three", 3);
          }
        }
        )));

    // array
    Assert.assertEquals(3, flattener.makeJsonPathExtractor("$.someMultiMemberUnion.array[2]").apply(
        AvroStreamInputRowParserTest.buildSomeAvroDatumWithUnionValue(Arrays.asList(1, 2, 3))));

    // Named types are accessed by name

    // record
    Assert.assertEquals("subRecordString", flattener.makeJsonPathExtractor("$.someMultiMemberUnion.UnionSubRecord.subString").apply(
        AvroStreamInputRowParserTest.buildSomeAvroDatumWithUnionValue(
            UnionSubRecord.newBuilder()
                          .setSubString("subRecordString")
                          .build())));

    // fixed
    final byte[] fixedBytes = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    Assert.assertEquals(fixedBytes, flattener.makeJsonPathExtractor("$.someMultiMemberUnion.UnionSubFixed").apply(
        AvroStreamInputRowParserTest.buildSomeAvroDatumWithUnionValue(new UnionSubFixed(fixedBytes))));

    // enum
    Assert.assertEquals(String.valueOf(UnionSubEnum.ENUM1), flattener.makeJsonPathExtractor("$.someMultiMemberUnion.UnionSubEnum").apply(
        AvroStreamInputRowParserTest.buildSomeAvroDatumWithUnionValue(UnionSubEnum.ENUM1)));
  }
```
```
Previous Commit	a2538d264d7dad829bcc48968100043694b55475
Directory name	extensions-core/avro-extensions
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		10
  (expected = UnsupportedOperationException.class)
  public void makeJsonQueryExtractor()
  {
    final SomeAvroDatum record = AvroStreamInputRowParserTest.buildSomeAvroDatum();
    final AvroFlattenerMaker flattener = new AvroFlattenerMaker(false, false, false);

    Assert.assertEquals(
        record.getTimestamp(),
        flattener.makeJsonQueryExtractor("$.timestamp").apply(record)
    );
  }
```
## b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c ##
```
Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name		sql
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		33
  
  public void testSelectConstantExpression() throws Exception
  {
    // Test with a Druid-specific function, to make sure they are hooked up correctly even when not selecting
    // from a table.
    testQuery(
        "SELECT REGEXP_EXTRACT('foo', '^(.)')",
        ImmutableList.of(
            Druids.newScanQueryBuilder()
                  .dataSource(
                      InlineDataSource.fromIterable(
                          ImmutableList.of(new Object[]{0L}),
                          RowSignature.builder().add("ZERO", ColumnType.LONG).build()
                      )
                  )
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .virtualColumns(
                      new ExpressionVirtualColumn(
                          "v0",
                          "'f'",
                          ColumnType.STRING,
                          ExprMacroTable.nil()
                      )
                  )
                  .columns("v0")
                  .resultFormat(ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                  .legacy(false)
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{"f"}
        )
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		31
  
  public void testExpressionContainingNull() throws Exception
  {
    testQuery(
        "SELECT ARRAY ['Hello', NULL]",
        ImmutableList.of(
            Druids.newScanQueryBuilder()
                  .dataSource(
                      InlineDataSource.fromIterable(
                          ImmutableList.of(new Object[]{0L}),
                          RowSignature.builder().add("ZERO", ColumnType.LONG).build()
                      )
                  )
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .virtualColumns(
                      new ExpressionVirtualColumn(
                          "v0",
                          "array('Hello',null)",
                          ColumnType.STRING,
                          ExprMacroTable.nil()
                      )
                  )
                  .columns("v0")
                  .resultFormat(ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                  .legacy(false)
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(new Object[]{"[\"Hello\",null]"})
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		42
  
  public void testSelectNonNumericNumberLiterals() throws Exception
  {
    // Tests to convert NaN, positive infinity and negative infinity as literals.
    testQuery(
        "SELECT"
        + " CAST(1 / 0.0 AS BIGINT),"
        + " CAST(1 / -0.0 AS BIGINT),"
        + " CAST(-1 / 0.0 AS BIGINT),"
        + " CAST(-1 / -0.0 AS BIGINT),"
        + " CAST(0/ 0.0 AS BIGINT)",
        ImmutableList.of(
            Druids.newScanQueryBuilder()
                  .dataSource(
                      InlineDataSource.fromIterable(
                          ImmutableList.of(
                              new Object[]{Long.MAX_VALUE, Long.MAX_VALUE, Long.MIN_VALUE, Long.MIN_VALUE, 0L}
                          ),
                          RowSignature.builder()
                                      .add("EXPR$0", ColumnType.LONG)
                                      .add("EXPR$1", ColumnType.LONG)
                                      .add("EXPR$2", ColumnType.LONG)
                                      .add("EXPR$3", ColumnType.LONG)
                                      .add("EXPR$4", ColumnType.LONG)
                                      .build()
                      )
                  )
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .columns("EXPR$0", "EXPR$1", "EXPR$2", "EXPR$3", "EXPR$4")
                  .resultFormat(ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                  .legacy(false)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{
                Long.MAX_VALUE,
                Long.MAX_VALUE,
                Long.MIN_VALUE,
                Long.MIN_VALUE,
                0L
            }
        )
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		21
  
  public void testSelectConstantExpressionFromTable() throws Exception
  {
    testQuery(
        "SELECT 1 + 1, dim1 FROM foo LIMIT 1",
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(expressionVirtualColumn("v0", "2", ColumnType.LONG))
                .columns("dim1", "v0")
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .limit(1)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{2, ""}
        )
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		11
  
  public void testSelectConstantExpressionEquivalentToNaN() throws Exception
  {
    expectedException.expectMessage(
        "'(log10(0) - log10(0))' evaluates to 'NaN' that is not supported in SQL. You can either cast the expression as bigint ('cast((log10(0) - log10(0)) as bigint)') or char ('cast((log10(0) - log10(0)) as char)') or change the expression itself");
    testQuery(
        "SELECT log10(0) - log10(0), dim1 FROM foo LIMIT 1",
        ImmutableList.of(),
        ImmutableList.of()
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		11
  
  public void testSelectConstantExpressionEquivalentToInfinity() throws Exception
  {
    expectedException.expectMessage(
        "'log10(0)' evaluates to '-Infinity' that is not supported in SQL. You can either cast the expression as bigint ('cast(log10(0) as bigint)') or char ('cast(log10(0) as char)') or change the expression itself");
    testQuery(
        "SELECT log10(0), dim1 FROM foo LIMIT 1",
        ImmutableList.of(),
        ImmutableList.of()
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 2
Assertions		 0
Lines of Code		46
  
  public void testExactTopNOnInnerJoinWithLimit() throws Exception
  {
    // Adjust topN threshold, so that the topN engine keeps only 1 slot for aggregates, which should be enough
    // to compute the query with limit 1.
    minTopNThreshold = 1;
    Map<String, Object> context = new HashMap<>(QUERY_CONTEXT_DEFAULT);
    context.put(PlannerConfig.CTX_KEY_USE_APPROXIMATE_TOPN, false);
    testQuery(
        "select f1.\"dim4\", sum(\"m1\") from numfoo f1 inner join (\n"
        + "  select \"dim4\" from numfoo where dim4 <> 'a' group by 1\n"
        + ") f2 on f1.\"dim4\" = f2.\"dim4\" group by 1 limit 1",
        context, // turn on exact topN
        ImmutableList.of(
            new TopNQueryBuilder()
                .intervals(querySegmentSpec(Filtration.eternity()))
                .granularity(Granularities.ALL)
                .dimension(new DefaultDimensionSpec("dim4", "_d0"))
                .aggregators(new DoubleSumAggregatorFactory("a0", "m1"))
                .metric(new DimensionTopNMetricSpec(null, StringComparators.LEXICOGRAPHIC))
                .threshold(1)
                .dataSource(
                    JoinDataSource.create(
                        new TableDataSource("numfoo"),
                        new QueryDataSource(
                            GroupByQuery.builder()
                                        .setInterval(querySegmentSpec(Filtration.eternity()))
                                        .setGranularity(Granularities.ALL)
                                        .setDimFilter(new NotDimFilter(new SelectorDimFilter("dim4", "a", null)))
                                        .setDataSource(new TableDataSource("numfoo"))
                                        .setDimensions(new DefaultDimensionSpec("dim4", "_d0"))
                                        .setContext(context)
                                        .build()
                        ),
                        "j0.",
                        "(\"dim4\" == \"j0._d0\")",
                        JoinType.INNER,
                        null,
                        ExprMacroTable.nil()
                    )
                )
                .context(context)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"b", 15.0}
        )
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		82
  
  public void testJoinOuterGroupByAndSubqueryHasLimit() throws Exception
  {
    // Cannot vectorize JOIN operator.
    cannotVectorize();

    testQuery(
        "SELECT dim2, AVG(m2) FROM (SELECT * FROM foo AS t1 INNER JOIN foo AS t2 ON t1.m1 = t2.m1 LIMIT 10) AS t3 GROUP BY dim2",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            newScanQueryBuilder()
                                .dataSource(
                                    join(
                                        new TableDataSource(CalciteTests.DATASOURCE1),
                                        new QueryDataSource(
                                            newScanQueryBuilder()
                                                .dataSource(CalciteTests.DATASOURCE1)
                                                .intervals(querySegmentSpec(Filtration.eternity()))
                                                .columns(ImmutableList.of("m1"))
                                                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                                .context(QUERY_CONTEXT_DEFAULT)
                                                .build()
                                        ),
                                        "j0.",
                                        equalsCondition(
                                            DruidExpression.fromColumn("m1"),
                                            DruidExpression.fromColumn("j0.m1")
                                        ),
                                        JoinType.INNER
                                    )
                                )
                                .intervals(querySegmentSpec(Filtration.eternity()))
                                .limit(10)
                                .columns("dim2", "m2")
                                .context(QUERY_CONTEXT_DEFAULT)
                                .build()
                        )
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setDimensions(new DefaultDimensionSpec("dim2", "d0", ColumnType.STRING))
                        .setGranularity(Granularities.ALL)
                        .setAggregatorSpecs(
                            useDefault
                            ? aggregators(
                                new DoubleSumAggregatorFactory("a0:sum", "m2"),
                                new CountAggregatorFactory("a0:count")
                            )
                            : aggregators(
                                new DoubleSumAggregatorFactory("a0:sum", "m2"),
                                new FilteredAggregatorFactory(
                                    new CountAggregatorFactory("a0:count"),
                                    not(selector("m2", null, null))
                                )
                            )
                        )
                        .setPostAggregatorSpecs(
                            ImmutableList.of(
                                new ArithmeticPostAggregator(
                                    "a0",
                                    "quotient",
                                    ImmutableList.of(
                                        new FieldAccessPostAggregator(null, "a0:sum"),
                                        new FieldAccessPostAggregator(null, "a0:count")
                                    )
                                )

                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        NullHandling.sqlCompatible()
        ? ImmutableList.of(
            new Object[]{null, 4.0},
            new Object[]{"", 3.0},
            new Object[]{"a", 2.5},
            new Object[]{"abc", 5.0}
        )
        : ImmutableList.of(
            new Object[]{"", 3.6666666666666665},
            new Object[]{"a", 2.5},
            new Object[]{"abc", 5.0}
        )
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 2
Assertions		 0
Lines of Code		80
  
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testJoinOuterGroupByAndSubqueryNoLimit(Map<String, Object> queryContext) throws Exception
  {
    // Fully removing the join allows this query to vectorize.
    if (!isRewriteJoinToFilter(queryContext)) {
      cannotVectorize();
    }

    testQuery(
        "SELECT dim2, AVG(m2) FROM (SELECT * FROM foo AS t1 INNER JOIN foo AS t2 ON t1.m1 = t2.m1) AS t3 GROUP BY dim2",
        queryContext,
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            join(
                                new TableDataSource(CalciteTests.DATASOURCE1),
                                new QueryDataSource(
                                    newScanQueryBuilder()
                                        .dataSource(CalciteTests.DATASOURCE1)
                                        .intervals(querySegmentSpec(Filtration.eternity()))
                                        .columns(ImmutableList.of("m1"))
                                        .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                        .context(QUERY_CONTEXT_DEFAULT)
                                        .build()
                                        .withOverriddenContext(queryContext)
                                ),
                                "j0.",
                                equalsCondition(
                                    DruidExpression.fromColumn("m1"),
                                    DruidExpression.fromColumn("j0.m1")
                                ),
                                JoinType.INNER
                            )
                        )
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setDimensions(new DefaultDimensionSpec("dim2", "d0", ColumnType.STRING))
                        .setGranularity(Granularities.ALL)
                        .setAggregatorSpecs(
                            useDefault
                            ? aggregators(
                                new DoubleSumAggregatorFactory("a0:sum", "m2"),
                                new CountAggregatorFactory("a0:count")
                            )
                            : aggregators(
                                new DoubleSumAggregatorFactory("a0:sum", "m2"),
                                new FilteredAggregatorFactory(
                                    new CountAggregatorFactory("a0:count"),
                                    not(selector("m2", null, null))
                                )
                            )
                        )
                        .setPostAggregatorSpecs(
                            ImmutableList.of(
                                new ArithmeticPostAggregator(
                                    "a0",
                                    "quotient",
                                    ImmutableList.of(
                                        new FieldAccessPostAggregator(null, "a0:sum"),
                                        new FieldAccessPostAggregator(null, "a0:count")
                                    )
                                )

                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
                        .withOverriddenContext(queryContext)
        ),
        NullHandling.sqlCompatible()
        ? ImmutableList.of(
            new Object[]{null, 4.0},
            new Object[]{"", 3.0},
            new Object[]{"a", 2.5},
            new Object[]{"abc", 5.0}
        )
        : ImmutableList.of(
            new Object[]{"", 3.6666666666666665},
            new Object[]{"a", 2.5},
            new Object[]{"abc", 5.0}
        )
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		82
  
  public void testJoinWithLimitBeforeJoining() throws Exception
  {
    // Cannot vectorize JOIN operator.
    cannotVectorize();

    testQuery(
        "SELECT t1.dim2, AVG(t1.m2) FROM (SELECT * FROM foo LIMIT 10) AS t1 INNER JOIN foo AS t2 ON t1.m1 = t2.m1 GROUP BY t1.dim2",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            join(
                                new QueryDataSource(
                                    newScanQueryBuilder()
                                        .dataSource(CalciteTests.DATASOURCE1)
                                        .intervals(querySegmentSpec(Filtration.eternity()))
                                        .columns("dim2", "m1", "m2")
                                        .context(QUERY_CONTEXT_DEFAULT)
                                        .limit(10)
                                        .build()
                                ),
                                new QueryDataSource(
                                    newScanQueryBuilder()
                                        .dataSource(CalciteTests.DATASOURCE1)
                                        .intervals(querySegmentSpec(Filtration.eternity()))
                                        .columns(ImmutableList.of("m1"))
                                        .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                        .context(QUERY_CONTEXT_DEFAULT)
                                        .build()
                                ),
                                "j0.",
                                equalsCondition(
                                    DruidExpression.fromColumn("m1"),
                                    DruidExpression.fromColumn("j0.m1")
                                ),
                                JoinType.INNER
                            )
                        )
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setDimensions(new DefaultDimensionSpec("dim2", "d0", ColumnType.STRING))
                        .setGranularity(Granularities.ALL)
                        .setAggregatorSpecs(
                            useDefault
                            ? aggregators(
                                new DoubleSumAggregatorFactory("a0:sum", "m2"),
                                new CountAggregatorFactory("a0:count")
                            )
                            : aggregators(
                                new DoubleSumAggregatorFactory("a0:sum", "m2"),
                                new FilteredAggregatorFactory(
                                    new CountAggregatorFactory("a0:count"),
                                    not(selector("m2", null, null))
                                )
                            )
                        )
                        .setPostAggregatorSpecs(
                            ImmutableList.of(
                                new ArithmeticPostAggregator(
                                    "a0",
                                    "quotient",
                                    ImmutableList.of(
                                        new FieldAccessPostAggregator(null, "a0:sum"),
                                        new FieldAccessPostAggregator(null, "a0:count")
                                    )
                                )

                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        NullHandling.sqlCompatible()
        ? ImmutableList.of(
            new Object[]{null, 4.0},
            new Object[]{"", 3.0},
            new Object[]{"a", 2.5},
            new Object[]{"abc", 5.0}
        )
        : ImmutableList.of(
            new Object[]{"", 3.6666666666666665},
            new Object[]{"a", 2.5},
            new Object[]{"abc", 5.0}
        )
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		49
  
  public void testJoinOnTimeseriesWithFloorOnTime() throws Exception
  {
    // Cannot vectorize JOIN operator.
    cannotVectorize();

    testQuery(
        "SELECT CAST(__time AS BIGINT), m1, ANY_VALUE(dim3, 100) FROM foo WHERE (TIME_FLOOR(__time, 'PT1H'), m1) IN\n"
        + "   (\n"
        + "     SELECT TIME_FLOOR(__time, 'PT1H') AS t1, MIN(m1) AS t2 FROM foo WHERE dim3 = 'b'\n"
        + "         AND __time BETWEEN '1994-04-29 00:00:00' AND '2020-01-11 00:00:00' GROUP BY 1\n"
        + "    )\n"
        + "GROUP BY 1, 2\n",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            join(
                                new TableDataSource(CalciteTests.DATASOURCE1),
                                new QueryDataSource(
                                    Druids.newTimeseriesQueryBuilder()
                                          .dataSource(CalciteTests.DATASOURCE1)
                                          .intervals(querySegmentSpec(Intervals.of("1994-04-29/2020-01-11T00:00:00.001Z")))
                                          .filters(selector("dim3", "b", null))
                                          .granularity(new PeriodGranularity(Period.hours(1), null, DateTimeZone.UTC))
                                          .aggregators(aggregators(
                                              new FloatMinAggregatorFactory("a0", "m1")
                                          ))
                                          .context(getTimeseriesContextWithFloorTime(TIMESERIES_CONTEXT_BY_GRAN, "d0"))
                                          .build()),
                                "j0.",
                                "((timestamp_floor(\"__time\",'PT1H',null,'UTC') == \"j0.d0\") && (\"m1\" == \"j0.a0\"))",
                                JoinType.INNER
                            )
                        )
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setDimensions(
                            new DefaultDimensionSpec("__time", "d0", ColumnType.LONG),
                            new DefaultDimensionSpec("m1", "d1", ColumnType.FLOAT)

                        )
                        .setGranularity(Granularities.ALL)
                        .setAggregatorSpecs(aggregators(
                            new StringAnyAggregatorFactory("a0", "dim3", 100)
                        ))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{946684800000L, 1.0f, "[a, b]"},
            new Object[]{946771200000L, 2.0f, "[b, c]"}
        )
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		62
  
  public void testJoinOnGroupByInsteadOfTimeseriesWithFloorOnTime() throws Exception
  {
    // Cannot vectorize JOIN operator.
    cannotVectorize();

    testQuery(
        "SELECT CAST(__time AS BIGINT), m1, ANY_VALUE(dim3, 100) FROM foo WHERE (CAST(TIME_FLOOR(__time, 'PT1H') AS BIGINT), m1) IN\n"
        + "   (\n"
        + "     SELECT CAST(TIME_FLOOR(__time, 'PT1H') AS BIGINT) + 0 AS t1, MIN(m1) AS t2 FROM foo WHERE dim3 = 'b'\n"
        + "         AND __time BETWEEN '1994-04-29 00:00:00' AND '2020-01-11 00:00:00' GROUP BY 1\n"
        + "    )\n"
        + "GROUP BY 1, 2\n",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            join(
                                new TableDataSource(CalciteTests.DATASOURCE1),
                                new QueryDataSource(
                                    GroupByQuery.builder()
                                                .setDataSource(CalciteTests.DATASOURCE1)
                                                .setInterval(querySegmentSpec(Intervals.of(
                                                    "1994-04-29/2020-01-11T00:00:00.001Z")))
                                                .setVirtualColumns(
                                                    expressionVirtualColumn(
                                                        "v0",
                                                        "(timestamp_floor(\"__time\",'PT1H',null,'UTC') + 0)",
                                                        ColumnType.LONG
                                                    )
                                                )
                                                .setDimFilter(selector("dim3", "b", null))
                                                .setGranularity(Granularities.ALL)
                                                .setDimensions(dimensions(new DefaultDimensionSpec(
                                                    "v0",
                                                    "d0",
                                                    ColumnType.LONG
                                                )))
                                                .setAggregatorSpecs(aggregators(
                                                    new FloatMinAggregatorFactory("a0", "m1")
                                                ))
                                                .setContext(QUERY_CONTEXT_DEFAULT)
                                                .build()),
                                "j0.",
                                "((timestamp_floor(\"__time\",'PT1H',null,'UTC') == \"j0.d0\") && (\"m1\" == \"j0.a0\"))",
                                JoinType.INNER
                            )
                        )
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setDimensions(
                            new DefaultDimensionSpec("__time", "d0", ColumnType.LONG),
                            new DefaultDimensionSpec("m1", "d1", ColumnType.FLOAT)

                        )
                        .setGranularity(Granularities.ALL)
                        .setAggregatorSpecs(aggregators(
                            new StringAnyAggregatorFactory("a0", "dim3", 100)
                        ))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{946684800000L, 1.0f, "[a, b]"},
            new Object[]{946771200000L, 2.0f, "[b, c]"}
        )
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 3
Assertions		 0
Lines of Code		82
  
  public void testSelectCountStar() throws Exception
  {
    // timeseries with all granularity have a single group, so should return default results for given aggregators
    // which for count is 0 and sum is null in sql compatible mode or 0.0 in default mode.
    testQuery(
        PLANNER_CONFIG_DEFAULT,
        QUERY_CONTEXT_DEFAULT,
        "SELECT exp(count(*)) + 10, sum(m2)  FROM druid.foo WHERE  dim2 = 0",
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(Druids.newTimeseriesQueryBuilder()
                               .dataSource(CalciteTests.DATASOURCE1)
                               .intervals(querySegmentSpec(Filtration.eternity()))
                               .filters(bound("dim2", "0", "0", false, false, null, StringComparators.NUMERIC))
                               .granularity(Granularities.ALL)
                               .aggregators(aggregators(
                                   new CountAggregatorFactory("a0"),
                                   new DoubleSumAggregatorFactory("a1", "m2")
                               ))
                               .postAggregators(
                                   expressionPostAgg("p0", "(exp(\"a0\") + 10)")
                               )
                               .context(QUERY_CONTEXT_DEFAULT)
                               .build()),
        ImmutableList.of(
            new Object[]{11.0, NullHandling.defaultDoubleValue()}
        )
    );

    testQuery(
        PLANNER_CONFIG_DEFAULT,
        QUERY_CONTEXT_DEFAULT,
        "SELECT exp(count(*)) + 10, sum(m2)  FROM druid.foo WHERE  __time >= TIMESTAMP '2999-01-01 00:00:00'",
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(Druids.newTimeseriesQueryBuilder()
                               .dataSource(CalciteTests.DATASOURCE1)
                               .intervals(querySegmentSpec(Intervals.of(
                                   "2999-01-01T00:00:00.000Z/146140482-04-24T15:36:27.903Z"))
                               )
                               .granularity(Granularities.ALL)
                               .aggregators(aggregators(
                                   new CountAggregatorFactory("a0"),
                                   new DoubleSumAggregatorFactory("a1", "m2")
                               ))
                               .postAggregators(
                                   expressionPostAgg("p0", "(exp(\"a0\") + 10)")
                               )
                               .context(QUERY_CONTEXT_DEFAULT)
                               .build()),
        ImmutableList.of(
            new Object[]{11.0, NullHandling.defaultDoubleValue()}
        )
    );

    // this behavior was not always correct, so make sure legacy behavior can be retained by skipping empty buckets
    // explicitly in the context which causes these timeseries queries to return no results
    testQuery(
        PLANNER_CONFIG_DEFAULT,
        TIMESERIES_CONTEXT_BY_GRAN,
        "SELECT COUNT(*) FROM foo WHERE dim1 = 'nonexistent'",
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(Druids.newTimeseriesQueryBuilder()
                               .dataSource(CalciteTests.DATASOURCE1)
                               .intervals(querySegmentSpec(Filtration.eternity()))
                               .filters(selector("dim1", "nonexistent", null))
                               .granularity(Granularities.ALL)
                               .aggregators(aggregators(
                                   new CountAggregatorFactory("a0")
                               ))
                               .context(TIMESERIES_CONTEXT_BY_GRAN)
                               .build()),
        ImmutableList.of()
    );

    // timeseries with a granularity is grouping by the time expression, so matching nothing returns no results
    testQuery(
        "SELECT COUNT(*) FROM foo WHERE dim1 = 'nonexistent' GROUP BY FLOOR(__time TO DAY)",
        ImmutableList.of(Druids.newTimeseriesQueryBuilder()
                               .dataSource(CalciteTests.DATASOURCE1)
                               .intervals(querySegmentSpec(Filtration.eternity()))
                               .filters(selector("dim1", "nonexistent", null))
                               .granularity(Granularities.DAY)
                               .aggregators(aggregators(
                                   new CountAggregatorFactory("a0")
                               ))
                               .context(getTimeseriesContextWithFloorTime(TIMESERIES_CONTEXT_BY_GRAN, "d0"))
                               .build()),
        ImmutableList.of()
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		44
  
  public void testSelectTrimFamily() throws Exception
  {
    // TRIM has some whacky parsing. Make sure the different forms work.

    testQuery(
        "SELECT\n"
        + "TRIM(BOTH 'x' FROM 'xfoox'),\n"
        + "TRIM(TRAILING 'x' FROM 'xfoox'),\n"
        + "TRIM(' ' FROM ' foo '),\n"
        + "TRIM(TRAILING FROM ' foo '),\n"
        + "TRIM(' foo '),\n"
        + "BTRIM(' foo '),\n"
        + "BTRIM('xfoox', 'x'),\n"
        + "LTRIM(' foo '),\n"
        + "LTRIM('xfoox', 'x'),\n"
        + "RTRIM(' foo '),\n"
        + "RTRIM('xfoox', 'x'),\n"
        + "COUNT(*)\n"
        + "FROM foo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .postAggregators(
                      expressionPostAgg("p0", "'foo'"),
                      expressionPostAgg("p1", "'xfoo'"),
                      expressionPostAgg("p2", "'foo'"),
                      expressionPostAgg("p3", "' foo'"),
                      expressionPostAgg("p4", "'foo'"),
                      expressionPostAgg("p5", "'foo'"),
                      expressionPostAgg("p6", "'foo'"),
                      expressionPostAgg("p7", "'foo '"),
                      expressionPostAgg("p8", "'foox'"),
                      expressionPostAgg("p9", "' foo'"),
                      expressionPostAgg("p10", "'xfoo'")
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{"foo", "xfoo", "foo", " foo", "foo", "foo", "foo", "foo ", "foox", " foo", "xfoo", 6L}
        )
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		34
  
  public void testSelectPadFamily() throws Exception
  {
    testQuery(
        "SELECT\n"
        + "LPAD('foo', 5, 'x'),\n"
        + "LPAD('foo', 2, 'x'),\n"
        + "LPAD('foo', 5),\n"
        + "RPAD('foo', 5, 'x'),\n"
        + "RPAD('foo', 2, 'x'),\n"
        + "RPAD('foo', 5),\n"
        + "COUNT(*)\n"
        + "FROM foo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .postAggregators(
                      expressionPostAgg("p0", "'xxfoo'"),
                      expressionPostAgg("p1", "'fo'"),
                      expressionPostAgg("p2", "'  foo'"),
                      expressionPostAgg("p3", "'fooxx'"),
                      expressionPostAgg("p4", "'fo'"),
                      expressionPostAgg("p5", "'foo  '")
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{"xxfoo", "fo", "  foo", "fooxx", "fo", "foo  ", 6L}
        )
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 2
Assertions		 0
Lines of Code		56
  
  public void testBitwiseExpressions() throws Exception
  {
    List<Object[]> expected;
    if (useDefault) {
      expected = ImmutableList.of(
          new Object[]{0L, 7L, 7L, -8L, 28L, 1L, 4607182418800017408L, 3.5E-323},
          new Object[]{325323L, 325323L, 0L, -325324L, 1301292L, 81330L, 4610334938539176755L, 1.60731E-318},
          new Object[]{0L, 0L, 0L, -1L, 0L, 0L, 0L, 0.0},
          new Object[]{0L, 0L, 0L, -1L, 0L, 0L, 0L, 0.0},
          new Object[]{0L, 0L, 0L, -1L, 0L, 0L, 0L, 0.0},
          new Object[]{0L, 0L, 0L, -1L, 0L, 0L, 0L, 0.0}
      );
    } else {
      expected = ImmutableList.of(
          new Object[]{null, null, null, -8L, 28L, 1L, 4607182418800017408L, 3.5E-323},
          new Object[]{325323L, 325323L, 0L, -325324L, 1301292L, 81330L, 4610334938539176755L, 1.60731E-318},
          new Object[]{0L, 0L, 0L, -1L, 0L, 0L, 0L, 0.0},
          new Object[]{null, null, null, null, null, null, null, null},
          new Object[]{null, null, null, null, null, null, null, null},
          new Object[]{null, null, null, null, null, null, null, null}
      );
    }
    testQuery(
        "SELECT\n"
        + "BITWISE_AND(l1, l2),\n"
        + "BITWISE_OR(l1, l2),\n"
        + "BITWISE_XOR(l1, l2),\n"
        + "BITWISE_COMPLEMENT(l1),\n"
        + "BITWISE_SHIFT_LEFT(l1, 2),\n"
        + "BITWISE_SHIFT_RIGHT(l1, 2),\n"
        + "BITWISE_CONVERT_DOUBLE_TO_LONG_BITS(d1),\n"
        + "BITWISE_CONVERT_LONG_BITS_TO_DOUBLE(l1)\n"
        + "FROM numfoo",
        ImmutableList.of(
            Druids.newScanQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE3)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .columns("v0", "v1", "v2", "v3", "v4", "v5", "v6", "v7")
                  .virtualColumns(
                      expressionVirtualColumn("v0", "bitwiseAnd(\"l1\",\"l2\")", ColumnType.LONG),
                      expressionVirtualColumn("v1", "bitwiseOr(\"l1\",\"l2\")", ColumnType.LONG),
                      expressionVirtualColumn("v2", "bitwiseXor(\"l1\",\"l2\")", ColumnType.LONG),
                      expressionVirtualColumn("v3", "bitwiseComplement(\"l1\")", ColumnType.LONG),
                      expressionVirtualColumn("v4", "bitwiseShiftLeft(\"l1\",2)", ColumnType.LONG),
                      expressionVirtualColumn("v5", "bitwiseShiftRight(\"l1\",2)", ColumnType.LONG),
                      expressionVirtualColumn("v6", "bitwiseConvertDoubleToLongBits(\"d1\")", ColumnType.LONG),
                      expressionVirtualColumn("v7", "bitwiseConvertLongBitsToDouble(\"l1\")", ColumnType.DOUBLE)
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                  .legacy(false)
                  .build()
        ),
        expected
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 2
Assertions		 0
Lines of Code		48
  
  public void testSafeDivideExpressions() throws Exception
  {
    List<Object[]> expected;
    if (useDefault) {
      expected = ImmutableList.of(
          new Object[]{0.0F, 0L, 0.0, 7.0F},
          new Object[]{1.0F, 1L, 1.0, 3253230.0F},
          new Object[]{0.0F, 0L, 0.0, 0.0F},
          new Object[]{0.0F, 0L, 0.0, 0.0F},
          new Object[]{0.0F, 0L, 0.0, 0.0F},
          new Object[]{0.0F, 0L, 0.0, 0.0F}
      );
    } else {
      expected = ImmutableList.of(
          new Object[]{null, null, null, 7.0F},
          new Object[]{1.0F, 1L, 1.0, 3253230.0F},
          new Object[]{0.0F, 0L, 0.0, 0.0F},
          new Object[]{null, null, null, null},
          new Object[]{null, null, null, null},
          new Object[]{null, null, null, null}
      );
    }
    testQuery(
        "SELECT\n"
        + "SAFE_DIVIDE(f1, f2),\n"
        + "SAFE_DIVIDE(l1, l2),\n"
        + "SAFE_DIVIDE(d2, d1),\n"
        + "SAFE_DIVIDE(l1, f1)\n"
        + "FROM numfoo",
        ImmutableList.of(
            Druids.newScanQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE3)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .columns("v0", "v1", "v2", "v3")
                  .virtualColumns(
                      expressionVirtualColumn("v0", "safe_divide(\"f1\",\"f2\")", ColumnType.FLOAT),
                      expressionVirtualColumn("v1", "safe_divide(\"l1\",\"l2\")", ColumnType.LONG),
                      expressionVirtualColumn("v2", "safe_divide(\"d2\",\"d1\")", ColumnType.DOUBLE),
                      expressionVirtualColumn("v3", "safe_divide(\"l1\",\"f1\")", ColumnType.FLOAT)
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                  .legacy(false)
                  .build()
        ),
        expected
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		27
  
  public void testSelectStar() throws Exception
  {
    testQuery(
        PLANNER_CONFIG_DEFAULT_NO_COMPLEX_SERDE,
        QUERY_CONTEXT_DEFAULT,
        "SELECT * FROM druid.foo",
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("__time", "cnt", "dim1", "dim2", "dim3", "m1", "m2", "unique_dim1")
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{timestamp("2000-01-01"), 1L, "", "a", "[\"a\",\"b\"]", 1f, 1.0, HLLC_STRING},
            new Object[]{timestamp("2000-01-02"), 1L, "10.1", NULL_STRING, "[\"b\",\"c\"]", 2f, 2.0, HLLC_STRING},
            new Object[]{timestamp("2000-01-03"), 1L, "2", "", "d", 3f, 3.0, HLLC_STRING},
            new Object[]{timestamp("2001-01-01"), 1L, "1", "a", "", 4f, 4.0, HLLC_STRING},
            new Object[]{timestamp("2001-01-02"), 1L, "def", "abc", NULL_STRING, 5f, 5.0, HLLC_STRING},
            new Object[]{timestamp("2001-01-03"), 1L, "abc", NULL_STRING, NULL_STRING, 6f, 6.0, HLLC_STRING}
        )
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		40
  
  public void testSelectStarOnForbiddenTable() throws Exception
  {
    assertQueryIsForbidden(
        "SELECT * FROM druid.forbiddenDatasource",
        CalciteTests.REGULAR_USER_AUTH_RESULT
    );

    testQuery(
        PLANNER_CONFIG_DEFAULT,
        "SELECT * FROM druid.forbiddenDatasource",
        CalciteTests.SUPER_USER_AUTH_RESULT,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.FORBIDDEN_DATASOURCE)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("__time", "cnt", "dim1", "dim2", "m1", "m2", "unique_dim1")
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{
                timestamp("2000-01-01"),
                1L,
                "forbidden",
                "abcd",
                9999.0f,
                NullHandling.defaultDoubleValue(),
                "\"AQAAAQAAAALFBA==\""
            },
            new Object[]{
                timestamp("2000-01-02"),
                1L,
                "forbidden",
                "a",
                1234.0f,
                NullHandling.defaultDoubleValue(),
                "\"AQAAAQAAAALFBA==\""
            }
        )
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		37
  
  public void testSelectStarOnForbiddenView() throws Exception
  {
    assertQueryIsForbidden(
        "SELECT * FROM view.forbiddenView",
        CalciteTests.REGULAR_USER_AUTH_RESULT
    );

    testQuery(
        PLANNER_CONFIG_DEFAULT,
        "SELECT * FROM view.forbiddenView",
        CalciteTests.SUPER_USER_AUTH_RESULT,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(
                    expressionVirtualColumn("v0", "substring(\"dim1\", 0, 1)", ColumnType.STRING),
                    expressionVirtualColumn("v1", "'a'", ColumnType.STRING)
                )
                .filters(selector("dim2", "a", null))
                .columns("__time", "v0", "v1")
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{
                timestamp("2000-01-01"),
                NullHandling.defaultStringValue(),
                "a"
            },
            new Object[]{
                timestamp("2001-01-01"),
                "1",
                "a"
            }
        )
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		50
  
  public void testSelectStarOnRestrictedView() throws Exception
  {
    testQuery(
        PLANNER_CONFIG_DEFAULT,
        "SELECT * FROM view.restrictedView",
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.FORBIDDEN_DATASOURCE)
                .filters(selector("dim2", "a", null))
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("__time", "dim1", "dim2", "m1")
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{
                timestamp("2000-01-02"),
                "forbidden",
                "a",
                1234.0f
            }
        )
    );

    testQuery(
        PLANNER_CONFIG_DEFAULT,
        "SELECT * FROM view.restrictedView",
        CalciteTests.SUPER_USER_AUTH_RESULT,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.FORBIDDEN_DATASOURCE)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .filters(selector("dim2", "a", null))
                .columns("__time", "dim1", "dim2", "m1")
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{
                timestamp("2000-01-02"),
                "forbidden",
                "a",
                1234.0f
            }
        )
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		19
  
  public void testUnqualifiedTableName() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM foo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{6L}
        )
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 2
Assertions		 0
Lines of Code		14
  
  public void testExplainSelectStar() throws Exception
  {
    // Skip vectorization since otherwise the "context" will change for each subtest.
    skipVectorize();

    testQuery(
        "EXPLAIN PLAN FOR SELECT * FROM druid.foo",
        ImmutableList.of(),
        ImmutableList.of(
            new Object[]{
                "DruidQueryRel(query=[{\"queryType\":\"scan\",\"dataSource\":{\"type\":\"table\",\"name\":\"foo\"},\"intervals\":{\"type\":\"intervals\",\"intervals\":[\"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\"]},\"virtualColumns\":[],\"resultFormat\":\"compactedList\",\"batchSize\":20480,\"filter\":null,\"columns\":[\"__time\",\"cnt\",\"dim1\",\"dim2\",\"dim3\",\"m1\",\"m2\",\"unique_dim1\"],\"legacy\":false,\"context\":{\"defaultTimeout\":300000,\"maxScatterGatherBytes\":9223372036854775807,\"sqlCurrentTimestamp\":\"2000-01-01T00:00:00Z\",\"sqlQueryId\":\"dummy\",\"vectorize\":\"false\",\"vectorizeVirtualColumns\":\"false\"},\"descending\":false,\"granularity\":{\"type\":\"all\"}}], signature=[{__time:LONG, cnt:LONG, dim1:STRING, dim2:STRING, dim3:STRING, m1:FLOAT, m2:DOUBLE, unique_dim1:COMPLEX<hyperUnique>}])\n",
                "[{\"name\":\"foo\",\"type\":\"DATASOURCE\"}]"
            }
        )
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		24
  
  public void testSelectStarWithLimit() throws Exception
  {
    testQuery(
        PLANNER_CONFIG_DEFAULT_NO_COMPLEX_SERDE,
        QUERY_CONTEXT_DEFAULT,
        "SELECT * FROM druid.foo LIMIT 2",
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("__time", "cnt", "dim1", "dim2", "dim3", "m1", "m2", "unique_dim1")
                .limit(2)
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{timestamp("2000-01-01"), 1L, "", "a", "[\"a\",\"b\"]", 1.0f, 1.0, HLLC_STRING},
            new Object[]{timestamp("2000-01-02"), 1L, "10.1", NULL_STRING, "[\"b\",\"c\"]", 2.0f, 2.0, HLLC_STRING}
        )
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		25
  
  public void testSelectStarWithLimitAndOffset() throws Exception
  {
    testQuery(
        PLANNER_CONFIG_DEFAULT_NO_COMPLEX_SERDE,
        QUERY_CONTEXT_DEFAULT,
        "SELECT * FROM druid.foo LIMIT 2 OFFSET 1",
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("__time", "cnt", "dim1", "dim2", "dim3", "m1", "m2", "unique_dim1")
                .offset(1)
                .limit(2)
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{timestamp("2000-01-02"), 1L, "10.1", NULL_STRING, "[\"b\",\"c\"]", 2.0f, 2.0, HLLC_STRING},
            new Object[]{timestamp("2000-01-03"), 1L, "2", "", "d", 3f, 3.0, HLLC_STRING}
        )
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		24
  
  public void testSelectWithProjection() throws Exception
  {
    testQuery(
        "SELECT SUBSTRING(dim2, 1, 1) FROM druid.foo LIMIT 2",
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(
                    expressionVirtualColumn("v0", "substring(\"dim2\", 0, 1)", ColumnType.STRING)
                )
                .columns("v0")
                .limit(2)
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"a"},
            new Object[]{NULL_STRING}
        )
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		23
  
  public void testSelectWithExpressionFilter() throws Exception
  {
    testQuery(
        "SELECT dim1 FROM druid.foo WHERE m1 + 1 = 7",
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(
                    expressionVirtualColumn("v0", "(\"m1\" + 1)", ColumnType.FLOAT)
                )
                .filters(selector("v0", "7", null))
                .columns("dim1")
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"abc"}
        )
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		25
  
  public void testSelectStarWithLimitTimeDescending() throws Exception
  {
    testQuery(
        PLANNER_CONFIG_DEFAULT_NO_COMPLEX_SERDE,
        QUERY_CONTEXT_DEFAULT,
        "SELECT * FROM druid.foo ORDER BY __time DESC LIMIT 2",
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns(ImmutableList.of("__time", "cnt", "dim1", "dim2", "dim3", "m1", "m2", "unique_dim1"))
                .limit(2)
                .order(ScanQuery.Order.DESCENDING)
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{timestamp("2001-01-03"), 1L, "abc", NULL_STRING, NULL_STRING, 6f, 6d, HLLC_STRING},
            new Object[]{timestamp("2001-01-02"), 1L, "def", "abc", NULL_STRING, 5f, 5d, HLLC_STRING}
        )
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		29
  
  public void testSelectStarWithoutLimitTimeAscending() throws Exception
  {
    testQuery(
        PLANNER_CONFIG_DEFAULT_NO_COMPLEX_SERDE,
        QUERY_CONTEXT_DEFAULT,
        "SELECT * FROM druid.foo ORDER BY __time",
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns(ImmutableList.of("__time", "cnt", "dim1", "dim2", "dim3", "m1", "m2", "unique_dim1"))
                .limit(Long.MAX_VALUE)
                .order(ScanQuery.Order.ASCENDING)
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{timestamp("2000-01-01"), 1L, "", "a", "[\"a\",\"b\"]", 1f, 1.0, HLLC_STRING},
            new Object[]{timestamp("2000-01-02"), 1L, "10.1", NULL_STRING, "[\"b\",\"c\"]", 2f, 2.0, HLLC_STRING},
            new Object[]{timestamp("2000-01-03"), 1L, "2", "", "d", 3f, 3.0, HLLC_STRING},
            new Object[]{timestamp("2001-01-01"), 1L, "1", "a", "", 4f, 4.0, HLLC_STRING},
            new Object[]{timestamp("2001-01-02"), 1L, "def", "abc", NULL_STRING, 5f, 5.0, HLLC_STRING},
            new Object[]{timestamp("2001-01-03"), 1L, "abc", NULL_STRING, NULL_STRING, 6f, 6.0, HLLC_STRING}
        )
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		21
  
  public void testSelectSingleColumnTwice() throws Exception
  {
    testQuery(
        "SELECT dim2 x, dim2 y FROM druid.foo LIMIT 2",
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("dim2")
                .limit(2)
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"a", "a"},
            new Object[]{NULL_STRING, NULL_STRING}
        )
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		22
  
  public void testSelectSingleColumnWithLimitDescending() throws Exception
  {
    testQuery(
        "SELECT dim1 FROM druid.foo ORDER BY __time DESC LIMIT 2",
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns(ImmutableList.of("__time", "dim1"))
                .limit(2)
                .order(ScanQuery.Order.DESCENDING)
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"abc"},
            new Object[]{"def"}
        )
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		22
  
  public void testSelectStarFromSelectSingleColumnWithLimitDescending() throws Exception
  {
    // After upgrading to Calcite 1.21, Calcite no longer respects the ORDER BY __time DESC
    // in the inner query. This is valid, as the SQL standard considers the subquery results to be an unordered
    // set of rows.
    testQuery(
        "SELECT * FROM (SELECT dim1 FROM druid.foo ORDER BY __time DESC) LIMIT 2",
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns(ImmutableList.of("dim1"))
                .limit(2)
                .order(ScanQuery.Order.NONE)
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{""},
            new Object[]{"10.1"}
        )
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		23
  
  public void testSelectLimitWrapping() throws Exception
  {
    testQuery(
        "SELECT dim1 FROM druid.foo ORDER BY __time DESC",
        OUTER_LIMIT_CONTEXT,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns(ImmutableList.of("__time", "dim1"))
                .limit(2)
                .order(ScanQuery.Order.DESCENDING)
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .context(OUTER_LIMIT_CONTEXT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"abc"},
            new Object[]{"def"}
        )
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		24
  
  public void testSelectLimitWrappingOnTopOfOffset() throws Exception
  {
    testQuery(
        "SELECT dim1 FROM druid.foo ORDER BY __time DESC OFFSET 1",
        OUTER_LIMIT_CONTEXT,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns(ImmutableList.of("__time", "dim1"))
                .offset(1)
                .limit(2)
                .order(ScanQuery.Order.DESCENDING)
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .context(OUTER_LIMIT_CONTEXT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"def"},
            new Object[]{"1"}
        )
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		23
  
  public void testSelectLimitWrappingOnTopOfOffsetAndLowLimit() throws Exception
  {
    testQuery(
        "SELECT dim1 FROM druid.foo ORDER BY __time DESC LIMIT 1 OFFSET 1",
        OUTER_LIMIT_CONTEXT,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns(ImmutableList.of("__time", "dim1"))
                .offset(1)
                .limit(1)
                .order(ScanQuery.Order.DESCENDING)
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .context(OUTER_LIMIT_CONTEXT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"def"}
        )
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		24
  
  public void testSelectLimitWrappingOnTopOfOffsetAndHighLimit() throws Exception
  {
    testQuery(
        "SELECT dim1 FROM druid.foo ORDER BY __time DESC LIMIT 10 OFFSET 1",
        OUTER_LIMIT_CONTEXT,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns(ImmutableList.of("__time", "dim1"))
                .offset(1)
                .limit(2)
                .order(ScanQuery.Order.DESCENDING)
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .context(OUTER_LIMIT_CONTEXT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"def"},
            new Object[]{"1"}
        )
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		23
  
  public void testSelectProjectionFromSelectSingleColumnWithInnerLimitDescending() throws Exception
  {
    testQuery(
        "SELECT 'beep ' || dim1 FROM (SELECT dim1 FROM druid.foo ORDER BY __time DESC LIMIT 2)",
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(expressionVirtualColumn("v0", "concat('beep ',\"dim1\")", ColumnType.STRING))
                .columns(ImmutableList.of("__time", "v0"))
                .limit(2)
                .order(ScanQuery.Order.DESCENDING)
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"beep abc"},
            new Object[]{"beep def"}
        )
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 2
Assertions		 0
Lines of Code		26
  
  public void testSelectProjectionFromSelectSingleColumnDescending() throws Exception
  {
    // Regression test for https://github.com/apache/druid/issues/7768.

    // After upgrading to Calcite 1.21, Calcite no longer respects the ORDER BY __time DESC
    // in the inner query. This is valid, as the SQL standard considers the subquery results to be an unordered
    // set of rows. This test now validates that the inner ordering is not applied.
    testQuery(
        "SELECT 'beep ' || dim1 FROM (SELECT dim1 FROM druid.foo ORDER BY __time DESC)",
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(expressionVirtualColumn("v0", "concat('beep ',\"dim1\")", ColumnType.STRING))
                .columns(ImmutableList.of("v0"))
                .order(ScanQuery.Order.NONE)
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"beep "},
            new Object[]{"beep 10.1"},
            new Object[]{"beep 2"},
            new Object[]{"beep 1"},
            new Object[]{"beep def"},
            new Object[]{"beep abc"}
        )
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		23
  
  public void testSelectProjectionFromSelectSingleColumnWithInnerAndOuterLimitDescending() throws Exception
  {
    testQuery(
        "SELECT 'beep ' || dim1 FROM (SELECT dim1 FROM druid.foo ORDER BY __time DESC LIMIT 4) LIMIT 2",
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(expressionVirtualColumn("v0", "concat('beep ',\"dim1\")", ColumnType.STRING))
                .columns(ImmutableList.of("__time", "v0"))
                .limit(2)
                .order(ScanQuery.Order.DESCENDING)
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"beep abc"},
            new Object[]{"beep def"}
        )
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		34
  
  public void testOrderThenLimitThenFilter() throws Exception
  {
    testQuery(
        "SELECT dim1 FROM "
        + "(SELECT __time, dim1 FROM druid.foo ORDER BY __time DESC LIMIT 4) "
        + "WHERE dim1 IN ('abc', 'def')",
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(
                    new QueryDataSource(
                        newScanQueryBuilder()
                            .dataSource(CalciteTests.DATASOURCE1)
                            .intervals(querySegmentSpec(Filtration.eternity()))
                            .columns(ImmutableList.of("__time", "dim1"))
                            .limit(4)
                            .order(ScanQuery.Order.DESCENDING)
                            .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                            .context(QUERY_CONTEXT_DEFAULT)
                            .build()
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns(ImmutableList.of("dim1"))
                .filters(in("dim1", Arrays.asList("abc", "def"), null))
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"abc"},
            new Object[]{"def"}
         )
     );
   }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 2
Assertions		 0
Lines of Code		38
  
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testUnionAllTwoQueriesLeftQueryIsJoin(Map<String, Object> queryContext) throws Exception
  {
    // Fully removing the join allows this query to vectorize.
    if (!isRewriteJoinToFilter(queryContext)) {
      cannotVectorize();
    }

    testQuery(
        "(SELECT COUNT(*) FROM foo INNER JOIN lookup.lookyloo ON foo.dim1 = lookyloo.k)  UNION ALL SELECT SUM(cnt) FROM foo",
        queryContext,
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(
                      join(
                          new TableDataSource(CalciteTests.DATASOURCE1),
                          new LookupDataSource("lookyloo"),
                          "j0.",
                          equalsCondition(DruidExpression.fromColumn("dim1"), DruidExpression.fromColumn("j0.k")),
                          JoinType.INNER
                      ))
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
                  .withOverriddenContext(queryContext),
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
                  .withOverriddenContext(queryContext)
        ),
        ImmutableList.of(new Object[]{1L}, new Object[]{6L})
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 2
Assertions		 0
Lines of Code		38
  
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testUnionAllTwoQueriesRightQueryIsJoin(Map<String, Object> queryContext) throws Exception
  {
    // Fully removing the join allows this query to vectorize.
    if (!isRewriteJoinToFilter(queryContext)) {
      cannotVectorize();
    }

    testQuery(
        "(SELECT SUM(cnt) FROM foo UNION ALL SELECT COUNT(*) FROM foo INNER JOIN lookup.lookyloo ON foo.dim1 = lookyloo.k) ",
        queryContext,
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
                  .withOverriddenContext(queryContext),
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(
                      join(
                          new TableDataSource(CalciteTests.DATASOURCE1),
                          new LookupDataSource("lookyloo"),
                          "j0.",
                          equalsCondition(DruidExpression.fromColumn("dim1"), DruidExpression.fromColumn("j0.k")),
                          JoinType.INNER
                      ))
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
                  .withOverriddenContext(queryContext)
        ),
        ImmutableList.of(new Object[]{6L}, new Object[]{1L})
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		43
  
  public void testUnionAllTwoQueriesBothQueriesAreJoin() throws Exception
  {
    cannotVectorize();

    testQuery(
        "("
        + "SELECT COUNT(*) FROM foo LEFT JOIN lookup.lookyloo ON foo.dim1 = lookyloo.k "
        + "                               UNION ALL                                       "
        + "SELECT COUNT(*) FROM foo INNER JOIN lookup.lookyloo ON foo.dim1 = lookyloo.k"
        + ") ",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(
                      join(
                          new TableDataSource(CalciteTests.DATASOURCE1),
                          new LookupDataSource("lookyloo"),
                          "j0.",
                          equalsCondition(DruidExpression.fromColumn("dim1"), DruidExpression.fromColumn("j0.k")),
                          JoinType.LEFT
                      )
                  )
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build(),
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(
                      join(
                          new TableDataSource(CalciteTests.DATASOURCE1),
                          new LookupDataSource("lookyloo"),
                          "j0.",
                          equalsCondition(DruidExpression.fromColumn("dim1"), DruidExpression.fromColumn("j0.k")),
                          JoinType.INNER
                      ))
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(new Object[]{6L}, new Object[]{1L})
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		30
  
  public void testSelectStarWithDimFilter() throws Exception
  {
    testQuery(
        PLANNER_CONFIG_DEFAULT_NO_COMPLEX_SERDE,
        QUERY_CONTEXT_DEFAULT,
        "SELECT * FROM druid.foo WHERE dim1 > 'd' OR dim2 = 'a'",
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .filters(
                    or(
                        bound("dim1", "d", null, true, false, null, StringComparators.LEXICOGRAPHIC),
                        selector("dim2", "a", null)
                    )
                )
                .columns("__time", "cnt", "dim1", "dim2", "dim3", "m1", "m2", "unique_dim1")
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{timestamp("2000-01-01"), 1L, "", "a", "[\"a\",\"b\"]", 1.0f, 1.0d, HLLC_STRING},
            new Object[]{timestamp("2001-01-01"), 1L, "1", "a", "", 4.0f, 4.0d, HLLC_STRING},
            new Object[]{timestamp("2001-01-02"), 1L, "def", "abc", NULL_STRING, 5.0f, 5.0d, HLLC_STRING}
        )
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		35
  
  public void testSelectDistinctWithStrlenFilter() throws Exception
  {
    // Cannot vectorize due to usage of expressions.
    cannotVectorize();

    testQuery(
        "SELECT distinct dim1 FROM druid.foo "
        + "WHERE CHARACTER_LENGTH(dim1) = 3 OR CAST(CHARACTER_LENGTH(dim1) AS varchar) = 3",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            expressionVirtualColumn("v0", "strlen(\"dim1\")", ColumnType.LONG),
                            // The two layers of CASTs here are unusual, they should really be collapsed into one
                            expressionVirtualColumn(
                                "v1",
                                "CAST(CAST(strlen(\"dim1\"), 'STRING'), 'LONG')",
                                ColumnType.LONG
                            )
                        )
                        .setDimensions(dimensions(new DefaultDimensionSpec("dim1", "d0")))
                        .setDimFilter(
                            or(
                                selector("v0", "3", null),
                                selector("v1", "3", null)
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"abc"},
            new Object[]{"def"}
        )
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 2
Assertions		 0
Lines of Code		30
  
  public void testSelectDistinctWithLimit() throws Exception
  {
    // Should use topN even if approximate topNs are off, because this query is exact.

    testQuery(
        "SELECT DISTINCT dim2 FROM druid.foo LIMIT 10",
        ImmutableList.of(
            new TopNQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .granularity(Granularities.ALL)
                .dimension(new DefaultDimensionSpec("dim2", "d0"))
                .metric(new DimensionTopNMetricSpec(null, StringComparators.LEXICOGRAPHIC))
                .threshold(10)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        NullHandling.replaceWithDefault() ?
        ImmutableList.of(
            new Object[]{""},
            new Object[]{"a"},
            new Object[]{"abc"}
        ) :
        ImmutableList.of(
            new Object[]{null},
            new Object[]{""},
            new Object[]{"a"},
            new Object[]{"abc"}
        )
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		30
  
  public void testSelectDistinctWithSortAsOuterQuery() throws Exception
  {
    testQuery(
        "SELECT * FROM (SELECT DISTINCT dim2 FROM druid.foo ORDER BY dim2) LIMIT 10",
        ImmutableList.of(
            new TopNQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .granularity(Granularities.ALL)
                .dimension(new DefaultDimensionSpec("dim2", "d0"))
                .metric(new DimensionTopNMetricSpec(null, StringComparators.LEXICOGRAPHIC))
                .threshold(10)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        NullHandling.replaceWithDefault() ?
        ImmutableList.of(
            new Object[]{""},
            new Object[]{"a"},
            new Object[]{"abc"}
        ) :
        ImmutableList.of(
            new Object[]{null},
            new Object[]{""},
            new Object[]{"a"},
            new Object[]{"abc"}
        )
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		30
  
  public void testSelectDistinctWithSortAsOuterQuery2() throws Exception
  {
    testQuery(
        "SELECT * FROM (SELECT DISTINCT dim2 FROM druid.foo ORDER BY dim2 LIMIT 5) LIMIT 10",
        ImmutableList.of(
            new TopNQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .granularity(Granularities.ALL)
                .dimension(new DefaultDimensionSpec("dim2", "d0"))
                .metric(new DimensionTopNMetricSpec(null, StringComparators.LEXICOGRAPHIC))
                .threshold(5)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        NullHandling.replaceWithDefault() ?
        ImmutableList.of(
            new Object[]{""},
            new Object[]{"a"},
            new Object[]{"abc"}
        ) :
        ImmutableList.of(
            new Object[]{null},
            new Object[]{""},
            new Object[]{"a"},
            new Object[]{"abc"}
        )
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		30
  
  public void testSelectDistinctWithSortAsOuterQuery3() throws Exception
  {
    testQuery(
        "SELECT * FROM (SELECT DISTINCT dim2 FROM druid.foo ORDER BY dim2 DESC LIMIT 5) LIMIT 10",
        ImmutableList.of(
            new TopNQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .granularity(Granularities.ALL)
                .dimension(new DefaultDimensionSpec("dim2", "d0"))
                .metric(new InvertedTopNMetricSpec(new DimensionTopNMetricSpec(null, StringComparators.LEXICOGRAPHIC)))
                .threshold(5)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        NullHandling.replaceWithDefault() ?
        ImmutableList.of(
            new Object[]{""},
            new Object[]{"abc"},
            new Object[]{"a"}
        ) :
        ImmutableList.of(
            new Object[]{null},
            new Object[]{"abc"},
            new Object[]{"a"},
            new Object[]{""}
        )
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		23
  
  public void testSelectNonAggregatingWithLimitLiterallyZero() throws Exception
  {
    // Query reduces to LIMIT 0.

    testQuery(
        "SELECT dim2 FROM druid.foo ORDER BY dim2 LIMIT 0",
        ImmutableList.of(
            Druids.newScanQueryBuilder()
                  .dataSource(
                      InlineDataSource.fromIterable(
                          ImmutableList.of(),
                          RowSignature.builder().add("dim2", ColumnType.STRING).build()
                      )
                  )
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .columns("dim2")
                  .resultFormat(ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                  .legacy(false)
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of()
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		22
  
  public void testSelectNonAggregatingWithLimitReducedToZero() throws Exception
  {
    // Query reduces to LIMIT 0.

    testQuery(
        "SELECT * FROM (SELECT dim2 FROM druid.foo ORDER BY dim2 LIMIT 2 OFFSET 5) OFFSET 2",
        ImmutableList.of(
            Druids.newScanQueryBuilder()
                  .dataSource(
                      InlineDataSource.fromIterable(
                          ImmutableList.of(),
                          RowSignature.builder().add("dim2", ColumnType.STRING).build()
                      )
                  )
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .columns("dim2")
                  .resultFormat(ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                  .legacy(false)
                  .build()
        ),
        ImmutableList.of()
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		22
  
  public void testSelectAggregatingWithLimitReducedToZero() throws Exception
  {
    // Query reduces to LIMIT 0.

    testQuery(
        "SELECT * FROM (SELECT DISTINCT dim2 FROM druid.foo ORDER BY dim2 LIMIT 2 OFFSET 5) OFFSET 2",
        ImmutableList.of(
            Druids.newScanQueryBuilder()
                  .dataSource(
                      InlineDataSource.fromIterable(
                          ImmutableList.of(),
                          RowSignature.builder().add("dim2", ColumnType.STRING).build()
                      )
                  )
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .columns("dim2")
                  .resultFormat(ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                  .legacy(false)
                  .build()
        ),
        ImmutableList.of()
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 2
Assertions		 0
Lines of Code		71
  
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testTopNFilterJoin(Map<String, Object> queryContext) throws Exception
  {
    // Fully removing the join allows this query to vectorize.
    if (!isRewriteJoinToFilter(queryContext)) {
      cannotVectorize();
    }

    // Filters on top N values of some dimension by using an inner join.
    testQuery(
        "SELECT t1.dim1, SUM(t1.cnt)\n"
        + "FROM druid.foo t1\n"
        + "  INNER JOIN (\n"
        + "  SELECT\n"
        + "    SUM(cnt) AS sum_cnt,\n"
        + "    dim2\n"
        + "  FROM druid.foo\n"
        + "  GROUP BY dim2\n"
        + "  ORDER BY 1 DESC\n"
        + "  LIMIT 2\n"
        + ") t2 ON (t1.dim2 = t2.dim2)\n"
        + "GROUP BY t1.dim1\n"
        + "ORDER BY 1\n",
        queryContext,
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            join(
                                new TableDataSource(CalciteTests.DATASOURCE1),
                                new QueryDataSource(
                                    new TopNQueryBuilder()
                                        .dataSource(CalciteTests.DATASOURCE1)
                                        .intervals(querySegmentSpec(Filtration.eternity()))
                                        .granularity(Granularities.ALL)
                                        .dimension(new DefaultDimensionSpec("dim2", "d0"))
                                        .aggregators(new LongSumAggregatorFactory("a0", "cnt"))
                                        .metric("a0")
                                        .threshold(2)
                                        .context(QUERY_CONTEXT_DEFAULT)
                                        .build()
                                ),
                                "j0.",
                                equalsCondition(
                                    DruidExpression.fromColumn("dim2"),
                                    DruidExpression.fromColumn("j0.d0")
                                ),
                                JoinType.INNER
                            )
                        )
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(dimensions(new DefaultDimensionSpec("dim1", "d0")))
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setLimitSpec(
                            new DefaultLimitSpec(
                                ImmutableList.of(
                                    new OrderByColumnSpec(
                                        "d0",
                                        OrderByColumnSpec.Direction.ASCENDING,
                                        StringComparators.LEXICOGRAPHIC
                                    )
                                ),
                                Integer.MAX_VALUE
                            )
                        )
                        .setContext(queryContext)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"", 1L},
            new Object[]{"1", 1L}
        )
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		65
  
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testTopNFilterJoinWithProjection(Map<String, Object> queryContext) throws Exception
  {
    // Cannot vectorize JOIN operator.
    cannotVectorize();

    // Filters on top N values of some dimension by using an inner join. Also projects the outer dimension.

    testQuery(
        "SELECT SUBSTRING(t1.dim1, 1, 10), SUM(t1.cnt)\n"
        + "FROM druid.foo t1\n"
        + "  INNER JOIN (\n"
        + "  SELECT\n"
        + "    SUM(cnt) AS sum_cnt,\n"
        + "    dim2\n"
        + "  FROM druid.foo\n"
        + "  GROUP BY dim2\n"
        + "  ORDER BY 1 DESC\n"
        + "  LIMIT 2\n"
        + ") t2 ON (t1.dim2 = t2.dim2)\n"
        + "GROUP BY SUBSTRING(t1.dim1, 1, 10)",
        queryContext,
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            join(
                                new TableDataSource(CalciteTests.DATASOURCE1),
                                new QueryDataSource(
                                    new TopNQueryBuilder()
                                        .dataSource(CalciteTests.DATASOURCE1)
                                        .intervals(querySegmentSpec(Filtration.eternity()))
                                        .granularity(Granularities.ALL)
                                        .dimension(new DefaultDimensionSpec("dim2", "d0"))
                                        .aggregators(new LongSumAggregatorFactory("a0", "cnt"))
                                        .metric("a0")
                                        .threshold(2)
                                        .context(QUERY_CONTEXT_DEFAULT)
                                        .build()
                                ),
                                "j0.",
                                equalsCondition(
                                    DruidExpression.fromColumn("dim2"),
                                    DruidExpression.fromColumn("j0.d0")
                                ),
                                JoinType.INNER
                            )
                        )
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(
                            dimensions(
                                new ExtractionDimensionSpec(
                                    "dim1",
                                    "d0",
                                    ColumnType.STRING,
                                    new SubstringDimExtractionFn(0, 10)
                                )
                            )
                        )
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setContext(queryContext)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{NULL_STRING, 1L},
            new Object[]{"1", 1L}
        )
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		51
  
  @Parameters(source = QueryContextForJoinProvider.class)
  @Ignore("Stopped working after the ability to join on subqueries was added to DruidJoinRule")
  public void testRemovableLeftJoin(Map<String, Object> queryContext) throws Exception
  {
    // LEFT JOIN where the right-hand side can be ignored.

    testQuery(
        "SELECT t1.dim1, SUM(t1.cnt)\n"
        + "FROM druid.foo t1\n"
        + "  LEFT JOIN (\n"
        + "  SELECT\n"
        + "    SUM(cnt) AS sum_cnt,\n"
        + "    dim2\n"
        + "  FROM druid.foo\n"
        + "  GROUP BY dim2\n"
        + "  ORDER BY 1 DESC\n"
        + "  LIMIT 2\n"
        + ") t2 ON (t1.dim2 = t2.dim2)\n"
        + "GROUP BY t1.dim1\n"
        + "ORDER BY 1\n",
        queryContext,
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(dimensions(new DefaultDimensionSpec("dim1", "d0")))
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setLimitSpec(
                            new DefaultLimitSpec(
                                ImmutableList.of(
                                    new OrderByColumnSpec(
                                        "d0",
                                        OrderByColumnSpec.Direction.ASCENDING,
                                        StringComparators.LEXICOGRAPHIC
                                    )
                                ),
                                Integer.MAX_VALUE
                            )
                        )
                        .setContext(queryContext)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"", 1L},
            new Object[]{"1", 1L},
            new Object[]{"10.1", 1L},
            new Object[]{"2", 1L},
            new Object[]{"abc", 1L},
            new Object[]{"def", 1L}
        )
     );
   }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 2
Assertions		 0
Lines of Code		39
  
  public void testSelectCurrentTimeAndDateLosAngeles() throws Exception
  {
    DateTimeZone timeZone = DateTimes.inferTzFromString(LOS_ANGELES);
    testQuery(
        PLANNER_CONFIG_DEFAULT,
        QUERY_CONTEXT_LOS_ANGELES,
        "SELECT CURRENT_TIMESTAMP, CURRENT_DATE, CURRENT_DATE + INTERVAL '1' DAY",
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(
            Druids.newScanQueryBuilder()
                  .dataSource(
                      InlineDataSource.fromIterable(
                          ImmutableList.of(
                              new Object[]{
                                  // milliseconds of timestamps as if they were in UTC. This looks strange
                                  // but intentional because they are what Calcite gives us.
                                  // See DruidLogicalValuesRule.getValueFromLiteral()
                                  // and Calcites.calciteDateTimeLiteralToJoda.
                                  new DateTime("2000-01-01T00Z", timeZone).withZone(DateTimeZone.UTC).getMillis(),
                                  new DateTime("1999-12-31", timeZone).withZone(DateTimeZone.UTC).getMillis(),
                                  new DateTime("2000-01-01", timeZone).withZone(DateTimeZone.UTC).getMillis()
                              }
                          ),
                          RowSignature.builder()
                                      .add("CURRENT_TIMESTAMP", ColumnType.LONG)
                                      .add("CURRENT_DATE", ColumnType.LONG)
                                      .add("EXPR$2", ColumnType.LONG)
                                      .build()
                      )
                  )
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .columns("CURRENT_DATE", "CURRENT_TIMESTAMP", "EXPR$2")
                  .resultFormat(ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                  .legacy(false)
                  .context(QUERY_CONTEXT_LOS_ANGELES)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{timestamp("2000-01-01T00Z", LOS_ANGELES), day("1999-12-31"), day("2000-01-01")}
        )
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		35
  
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testFilterAndGroupByLookupUsingJoinOperatorAllowNulls(Map<String, Object> queryContext) throws Exception
  {
    // Cannot vectorize JOIN operator.
    cannotVectorize();

    testQuery(
        "SELECT lookyloo.v, COUNT(*)\n"
        + "FROM foo LEFT JOIN lookup.lookyloo ON foo.dim2 = lookyloo.k\n"
        + "WHERE lookyloo.v <> 'xa' OR lookyloo.v IS NULL\n"
        + "GROUP BY lookyloo.v",
        queryContext,
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            join(
                                new TableDataSource(CalciteTests.DATASOURCE1),
                                new LookupDataSource("lookyloo"),
                                "j0.",
                                equalsCondition(DruidExpression.fromColumn("dim2"), DruidExpression.fromColumn("j0.k")),
                                JoinType.LEFT
                            )
                        )
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimFilter(or(not(selector("j0.v", "xa", null)), selector("j0.v", null, null)))
                        .setDimensions(dimensions(new DefaultDimensionSpec("j0.v", "d0")))
                        .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                        .setContext(queryContext)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{NULL_STRING, 3L},
            new Object[]{"xabc", 1L}
        )
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		42
  
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testFilterAndGroupByLookupUsingJoinOperatorBackwards(Map<String, Object> queryContext) throws Exception
  {
    // Like "testFilterAndGroupByLookupUsingJoinOperator", but with the table and lookup reversed.

    // Cannot vectorize JOIN operator.
    cannotVectorize();

    testQuery(
        "SELECT lookyloo.v, COUNT(*)\n"
        + "FROM lookup.lookyloo RIGHT JOIN foo ON foo.dim2 = lookyloo.k\n"
        + "WHERE lookyloo.v <> 'xa'\n"
        + "GROUP BY lookyloo.v",
        queryContext,
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            join(
                                new LookupDataSource("lookyloo"),
                                new QueryDataSource(
                                    newScanQueryBuilder()
                                        .dataSource(CalciteTests.DATASOURCE1)
                                        .intervals(querySegmentSpec(Filtration.eternity()))
                                        .columns("dim2")
                                        .context(QUERY_CONTEXT_DEFAULT)
                                        .build()
                                ),
                                "j0.",
                                equalsCondition(DruidExpression.fromColumn("k"), DruidExpression.fromColumn("j0.dim2")),
                                JoinType.RIGHT
                            )
                        )
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setDimFilter(not(selector("v", "xa", null)))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(dimensions(new DefaultDimensionSpec("v", "d0")))
                        .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                        .setContext(queryContext)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{NULL_STRING, 3L},
            new Object[]{"xabc", 1L}
        )
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		36
  
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testFilterAndGroupByLookupUsingJoinOperatorWithNotFilter(Map<String, Object> queryContext)
      throws Exception
  {
    // Cannot vectorize JOIN operator.
    cannotVectorize();

    testQuery(
        "SELECT lookyloo.v, COUNT(*)\n"
        + "FROM foo LEFT JOIN lookup.lookyloo ON foo.dim2 = lookyloo.k\n"
        + "WHERE lookyloo.v <> 'xa'\n"
        + "GROUP BY lookyloo.v",
        queryContext,
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            join(
                                new TableDataSource(CalciteTests.DATASOURCE1),
                                new LookupDataSource("lookyloo"),
                                "j0.",
                                equalsCondition(DruidExpression.fromColumn("dim2"), DruidExpression.fromColumn("j0.k")),
                                JoinType.LEFT
                            )
                        )
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setDimFilter(not(selector("j0.v", "xa", null)))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(dimensions(new DefaultDimensionSpec("j0.v", "d0")))
                        .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                        .setContext(queryContext)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{NULL_STRING, 3L},
            new Object[]{"xabc", 1L}
        )
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		42
  
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testJoinUnionTablesOnLookup(Map<String, Object> queryContext) throws Exception
  {
    // Cannot vectorize JOIN operator.
    cannotVectorize();

    testQuery(
        "SELECT lookyloo.v, COUNT(*)\n"
        + "FROM\n"
        + "  (SELECT dim2 FROM foo UNION ALL SELECT dim2 FROM numfoo) u\n"
        + "  LEFT JOIN lookup.lookyloo ON u.dim2 = lookyloo.k\n"
        + "WHERE lookyloo.v <> 'xa'\n"
        + "GROUP BY lookyloo.v",
        queryContext,
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            join(
                                new UnionDataSource(
                                    ImmutableList.of(
                                        new TableDataSource(CalciteTests.DATASOURCE1),
                                        new TableDataSource(CalciteTests.DATASOURCE3)
                                    )
                                ),
                                new LookupDataSource("lookyloo"),
                                "j0.",
                                equalsCondition(DruidExpression.fromColumn("dim2"), DruidExpression.fromColumn("j0.k")),
                                JoinType.LEFT
                            )
                        )
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setDimFilter(not(selector("j0.v", "xa", null)))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(dimensions(new DefaultDimensionSpec("j0.v", "d0")))
                        .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                        .setContext(queryContext)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{NULL_STRING, 6L},
            new Object[]{"xabc", 2L}
        )
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		34
  
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testFilterAndGroupByLookupUsingJoinOperator(Map<String, Object> queryContext) throws Exception
  {
    // Cannot vectorize JOIN operator.
    cannotVectorize();

    testQuery(
        "SELECT lookyloo.k, COUNT(*)\n"
        + "FROM foo LEFT JOIN lookup.lookyloo ON foo.dim2 = lookyloo.k\n"
        + "WHERE lookyloo.v = 'xa'\n"
        + "GROUP BY lookyloo.k",
        queryContext,
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            join(
                                new TableDataSource(CalciteTests.DATASOURCE1),
                                new LookupDataSource("lookyloo"),
                                "j0.",
                                equalsCondition(DruidExpression.fromColumn("dim2"), DruidExpression.fromColumn("j0.k")),
                                JoinType.LEFT
                            )
                        )
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setDimFilter(selector("j0.v", "xa", null))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(dimensions(new DefaultDimensionSpec("j0.k", "d0")))
                        .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                        .setContext(queryContext)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"a", 2L}
        )
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		49
  
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testFilterAndGroupByLookupUsingPostAggregationJoinOperator(Map<String, Object> queryContext)
      throws Exception
  {
    testQuery(
        "SELECT base.dim2, lookyloo.v, base.cnt FROM (\n"
        + "  SELECT dim2, COUNT(*) cnt FROM foo GROUP BY dim2\n"
        + ") base\n"
        + "LEFT JOIN lookup.lookyloo ON base.dim2 = lookyloo.k\n"
        + "WHERE lookyloo.v <> 'xa' OR lookyloo.v IS NULL",
        queryContext,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(
                    join(
                        new QueryDataSource(
                            GroupByQuery
                                .builder()
                                .setDataSource(CalciteTests.DATASOURCE1)
                                .setInterval(querySegmentSpec(Filtration.eternity()))
                                .setGranularity(Granularities.ALL)
                                .setDimensions(dimensions(new DefaultDimensionSpec("dim2", "d0")))
                                .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                                .setContext(queryContext)
                                .build()
                        ),
                        new LookupDataSource("lookyloo"),
                        "j0.",
                        equalsCondition(DruidExpression.fromColumn("d0"), DruidExpression.fromColumn("j0.k")),
                        JoinType.LEFT
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .filters(or(not(selector("j0.v", "xa", null)), selector("j0.v", null, null)))
                .columns("a0", "d0", "j0.v")
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        NullHandling.sqlCompatible()
        ? ImmutableList.of(
            new Object[]{NULL_STRING, NULL_STRING, 2L},
            new Object[]{"", NULL_STRING, 1L},
            new Object[]{"abc", "xabc", 1L}
        ) : ImmutableList.of(
            new Object[]{NULL_STRING, NULL_STRING, 3L},
            new Object[]{"abc", "xabc", 1L}
        )
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		32
  
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testGroupByInnerJoinOnLookupUsingJoinOperator(Map<String, Object> queryContext) throws Exception
  {
    // Cannot vectorize JOIN operator.
    cannotVectorize();

    testQuery(
        "SELECT lookyloo.v, COUNT(*)\n"
        + "FROM foo INNER JOIN lookup.lookyloo ON foo.dim1 = lookyloo.k\n"
        + "GROUP BY lookyloo.v",
        queryContext,
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            join(
                                new TableDataSource(CalciteTests.DATASOURCE1),
                                new LookupDataSource("lookyloo"),
                                "j0.",
                                equalsCondition(DruidExpression.fromColumn("dim1"), DruidExpression.fromColumn("j0.k")),
                                JoinType.INNER
                            )
                        )
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(dimensions(new DefaultDimensionSpec("j0.v", "d0")))
                        .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                        .setContext(queryContext)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"xabc", 1L}
        )
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		30
  
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testSelectOnLookupUsingInnerJoinOperator(Map<String, Object> queryContext) throws Exception
  {
    testQuery(
        "SELECT dim2, lookyloo.*\n"
        + "FROM foo INNER JOIN lookup.lookyloo ON foo.dim2 = lookyloo.k\n",
        queryContext,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(
                    join(
                        new TableDataSource(CalciteTests.DATASOURCE1),
                        new LookupDataSource("lookyloo"),
                        "j0.",
                        equalsCondition(DruidExpression.fromColumn("dim2"), DruidExpression.fromColumn("j0.k")),
                        JoinType.INNER
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("dim2", "j0.k", "j0.v")
                .context(queryContext)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"a", "a", "xa"},
            new Object[]{"a", "a", "xa"},
            new Object[]{"abc", "abc", "xabc"}
        )
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		41
  
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testLeftJoinTwoLookupsUsingJoinOperator(Map<String, Object> queryContext) throws Exception
  {
    testQuery(
        "SELECT dim1, dim2, l1.v, l2.v\n"
        + "FROM foo\n"
        + "LEFT JOIN lookup.lookyloo l1 ON foo.dim1 = l1.k\n"
        + "LEFT JOIN lookup.lookyloo l2 ON foo.dim2 = l2.k\n",
        queryContext,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(
                    join(
                        join(
                            new TableDataSource(CalciteTests.DATASOURCE1),
                            new LookupDataSource("lookyloo"),
                            "j0.",
                            equalsCondition(DruidExpression.fromColumn("dim1"), DruidExpression.fromColumn("j0.k")),
                            JoinType.LEFT
                        ),
                        new LookupDataSource("lookyloo"),
                        "_j0.",
                        equalsCondition(DruidExpression.fromColumn("dim2"), DruidExpression.fromColumn("_j0.k")),
                        JoinType.LEFT
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("_j0.v", "dim1", "dim2", "j0.v")
                .context(queryContext)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"", "a", NULL_STRING, "xa"},
            new Object[]{"10.1", NULL_STRING, NULL_STRING, NULL_STRING},
            new Object[]{"2", "", NULL_STRING, NULL_STRING},
            new Object[]{"1", "a", NULL_STRING, "xa"},
            new Object[]{"def", "abc", NULL_STRING, "xabc"},
            new Object[]{"abc", NULL_STRING, "xabc", NULL_STRING}
        )
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		41
  
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testInnerJoinTableLookupLookupWithFilterWithOuterLimit(Map<String, Object> queryContext) throws Exception
  {
    testQuery(
        "SELECT dim1\n"
        + "FROM foo\n"
        + "INNER JOIN lookup.lookyloo l ON foo.dim2 = l.k\n"
        + "INNER JOIN lookup.lookyloo l2 ON foo.dim2 = l2.k\n"
        + "WHERE l.v = 'xa'\n"
        + "LIMIT 100\n",
        queryContext,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(
                    join(
                        join(
                            new TableDataSource(CalciteTests.DATASOURCE1),
                            new LookupDataSource("lookyloo"),
                            "j0.",
                            equalsCondition(DruidExpression.fromColumn("dim2"), DruidExpression.fromColumn("j0.k")),
                            JoinType.INNER
                        ),
                        new LookupDataSource("lookyloo"),
                        "_j0.",
                        equalsCondition(DruidExpression.fromColumn("dim2"), DruidExpression.fromColumn("_j0.k")),
                        JoinType.INNER
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .limit(100)
                .filters(selector("j0.v", "xa", null))
                .columns("dim1")
                .context(queryContext)
                .build()
        ),
        ImmutableList.of(
            new Object[]{""},
            new Object[]{"1"}
        )
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		39
  
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testInnerJoinTableLookupLookupWithFilterWithoutLimit(Map<String, Object> queryContext) throws Exception
  {
    testQuery(
        "SELECT dim1\n"
        + "FROM foo\n"
        + "INNER JOIN lookup.lookyloo l ON foo.dim2 = l.k\n"
        + "INNER JOIN lookup.lookyloo l2 ON foo.dim2 = l2.k\n"
        + "WHERE l.v = 'xa'\n",
        queryContext,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(
                    join(
                        join(
                            new TableDataSource(CalciteTests.DATASOURCE1),
                            new LookupDataSource("lookyloo"),
                            "j0.",
                            equalsCondition(DruidExpression.fromColumn("dim2"), DruidExpression.fromColumn("j0.k")),
                            JoinType.INNER
                        ),
                        new LookupDataSource("lookyloo"),
                        "_j0.",
                        equalsCondition(DruidExpression.fromColumn("dim2"), DruidExpression.fromColumn("_j0.k")),
                        JoinType.INNER
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .filters(selector("j0.v", "xa", null))
                .columns("dim1")
                .context(queryContext)
                .build()
        ),
        ImmutableList.of(
            new Object[]{""},
            new Object[]{"1"}
        )
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		42
  
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testInnerJoinTableLookupLookupWithFilterWithOuterLimitWithAllColumns(Map<String, Object> queryContext)
      throws Exception
  {
    testQuery(
        "SELECT __time, cnt, dim1, dim2, dim3, m1, m2, unique_dim1\n"
        + "FROM foo\n"
        + "INNER JOIN lookup.lookyloo l ON foo.dim2 = l.k\n"
        + "INNER JOIN lookup.lookyloo l2 ON foo.dim2 = l2.k\n"
        + "WHERE l.v = 'xa'\n"
        + "LIMIT 100\n",
        queryContext,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(
                    join(
                        join(
                            new TableDataSource(CalciteTests.DATASOURCE1),
                            new LookupDataSource("lookyloo"),
                            "j0.",
                            equalsCondition(DruidExpression.fromColumn("dim2"), DruidExpression.fromColumn("j0.k")),
                            JoinType.INNER
                        ),
                        new LookupDataSource("lookyloo"),
                        "_j0.",
                        equalsCondition(DruidExpression.fromColumn("dim2"), DruidExpression.fromColumn("_j0.k")),
                        JoinType.INNER
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .limit(100)
                .filters(selector("j0.v", "xa", null))
                .columns("__time", "cnt", "dim1", "dim2", "dim3", "m1", "m2", "unique_dim1")
                .context(queryContext)
                .build()
        ),
        ImmutableList.of(
            new Object[]{946684800000L, 1L, "", "a", "[\"a\",\"b\"]", 1.0F, 1.0, "\"AQAAAEAAAA==\""},
            new Object[]{978307200000L, 1L, "1", "a", "", 4.0F, 4.0, "\"AQAAAQAAAAFREA==\""}
        )
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		40
  
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testInnerJoinTableLookupLookupWithFilterWithoutLimitWithAllColumns(Map<String, Object> queryContext)
      throws Exception
  {
    testQuery(
        "SELECT __time, cnt, dim1, dim2, dim3, m1, m2, unique_dim1\n"
        + "FROM foo\n"
        + "INNER JOIN lookup.lookyloo l ON foo.dim2 = l.k\n"
        + "INNER JOIN lookup.lookyloo l2 ON foo.dim2 = l2.k\n"
        + "WHERE l.v = 'xa'\n",
        queryContext,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(
                    join(
                        join(
                            new TableDataSource(CalciteTests.DATASOURCE1),
                            new LookupDataSource("lookyloo"),
                            "j0.",
                            equalsCondition(DruidExpression.fromColumn("dim2"), DruidExpression.fromColumn("j0.k")),
                            JoinType.INNER
                        ),
                        new LookupDataSource("lookyloo"),
                        "_j0.",
                        equalsCondition(DruidExpression.fromColumn("dim2"), DruidExpression.fromColumn("_j0.k")),
                        JoinType.INNER
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .filters(selector("j0.v", "xa", null))
                .columns("__time", "cnt", "dim1", "dim2", "dim3", "m1", "m2", "unique_dim1")
                .context(queryContext)
                .build()
        ),
        ImmutableList.of(
            new Object[]{946684800000L, 1L, "", "a", "[\"a\",\"b\"]", 1.0F, 1.0, "\"AQAAAEAAAA==\""},
            new Object[]{978307200000L, 1L, "1", "a", "", 4.0F, 4.0, "\"AQAAAQAAAAFREA==\""}
        )
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		48
  
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testInnerJoinQueryOfLookup(Map<String, Object> queryContext) throws Exception
  {
    // Cannot vectorize the subquery.
    cannotVectorize();

    testQuery(
        "SELECT dim1, dim2, t1.v, t1.v\n"
        + "FROM foo\n"
        + "INNER JOIN \n"
        + "  (SELECT SUBSTRING(k, 1, 1) k, LATEST(v, 10) v FROM lookup.lookyloo GROUP BY 1) t1\n"
        + "  ON foo.dim2 = t1.k",
        queryContext,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(
                    join(
                        new TableDataSource(CalciteTests.DATASOURCE1),
                        new QueryDataSource(
                            GroupByQuery
                                .builder()
                                .setDataSource(new LookupDataSource("lookyloo"))
                                .setInterval(querySegmentSpec(Filtration.eternity()))
                                .setGranularity(Granularities.ALL)
                                .setDimensions(
                                    new ExtractionDimensionSpec(
                                        "k",
                                        "d0",
                                        new SubstringDimExtractionFn(0, 1)
                                    )
                                )
                                .setAggregatorSpecs(new StringLastAggregatorFactory("a0", "v", 10))
                                .build()
                        ),
                        "j0.",
                        equalsCondition(DruidExpression.fromColumn("dim2"), DruidExpression.fromColumn("j0.d0")),
                        JoinType.INNER
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("dim1", "dim2", "j0.a0")
                .context(queryContext)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"", "a", "xabc", "xabc"},
            new Object[]{"1", "a", "xabc", "xabc"}
        )
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		34
  
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testInnerJoinQueryOfLookupRemovable(Map<String, Object> queryContext) throws Exception
  {
    // Like "testInnerJoinQueryOfLookup", but the subquery is removable.

    testQuery(
        "SELECT dim1, dim2, t1.sk\n"
        + "FROM foo\n"
        + "INNER JOIN \n"
        + "  (SELECT k, SUBSTRING(v, 1, 3) sk FROM lookup.lookyloo) t1\n"
        + "  ON foo.dim2 = t1.k",
        queryContext,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(
                    join(
                        new TableDataSource(CalciteTests.DATASOURCE1),
                        new LookupDataSource("lookyloo"),
                        "j0.",
                        equalsCondition(DruidExpression.fromColumn("dim2"), DruidExpression.fromColumn("j0.k")),
                        JoinType.INNER
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(expressionVirtualColumn("v0", "substring(\"j0.v\", 0, 3)", ColumnType.STRING))
                .columns("dim1", "dim2", "v0")
                .context(queryContext)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"", "a", "xa"},
            new Object[]{"1", "a", "xa"},
            new Object[]{"def", "abc", "xab"}
        )
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 2
Assertions		 0
Lines of Code		55
  
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testInnerJoinTwoLookupsToTableUsingNumericColumn(Map<String, Object> queryContext) throws Exception
  {
    // Regression test for https://github.com/apache/druid/issues/9646.

    // Cannot vectorize JOIN operator.
    cannotVectorize();

    testQuery(
        "SELECT COUNT(*)\n"
        + "FROM foo\n"
        + "INNER JOIN lookup.lookyloo l1 ON l1.k = foo.m1\n"
        + "INNER JOIN lookup.lookyloo l2 ON l2.k = l1.k",
        queryContext,
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(
                      join(
                          join(
                              new TableDataSource(CalciteTests.DATASOURCE1),
                              new QueryDataSource(
                                  newScanQueryBuilder()
                                      .dataSource(new LookupDataSource("lookyloo"))
                                      .intervals(querySegmentSpec(Filtration.eternity()))
                                      .virtualColumns(
                                          expressionVirtualColumn(
                                              "v0",
                                              "CAST(\"k\", 'DOUBLE')",
                                              ColumnType.FLOAT
                                          )
                                      )
                                      .columns("k", "v0")
                                      .context(QUERY_CONTEXT_DEFAULT)
                                      .build()
                              ),
                              "j0.",
                              equalsCondition(
                                  DruidExpression.fromColumn("m1"),
                                  DruidExpression.fromColumn("j0.v0")
                              ),
                              JoinType.INNER
                          ),
                          new LookupDataSource("lookyloo"),
                          "_j0.",
                          equalsCondition(DruidExpression.fromColumn("j0.k"), DruidExpression.fromColumn("_j0.k")),
                          JoinType.INNER
                      )
                  )
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(new CountAggregatorFactory("a0"))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{1L}
        )
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		52
  
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testInnerJoinTwoLookupsToTableUsingNumericColumnInReverse(Map<String, Object> queryContext)
      throws Exception
  {
    // Like "testInnerJoinTwoLookupsToTableUsingNumericColumn", but the tables are specified backwards.

    cannotVectorize();

    testQuery(
        "SELECT COUNT(*)\n"
        + "FROM lookup.lookyloo l1\n"
        + "INNER JOIN lookup.lookyloo l2 ON l1.k = l2.k\n"
        + "INNER JOIN foo on l2.k = foo.m1",
        queryContext,
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(
                      join(
                          join(
                              new LookupDataSource("lookyloo"),
                              new LookupDataSource("lookyloo"),
                              "j0.",
                              equalsCondition(
                                  DruidExpression.fromColumn("k"),
                                  DruidExpression.fromColumn("j0.k")
                              ),
                              JoinType.INNER
                          ),
                          new QueryDataSource(
                              newScanQueryBuilder()
                                  .dataSource(CalciteTests.DATASOURCE1)
                                  .intervals(querySegmentSpec(Filtration.eternity()))
                                  .columns("m1")
                                  .context(QUERY_CONTEXT_DEFAULT)
                                  .build()
                          ),
                          "_j0.",
                          equalsCondition(
                              DruidExpression.fromExpression("CAST(\"j0.k\", 'DOUBLE')"),
                              DruidExpression.fromColumn("_j0.m1")
                          ),
                          JoinType.INNER
                      )
                  )
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(new CountAggregatorFactory("a0"))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{1L}
        )
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 2
Assertions		 0
Lines of Code		76
  
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testInnerJoinLookupTableTable(Map<String, Object> queryContext) throws Exception
  {
    // Regression test for https://github.com/apache/druid/issues/9646.

    // Cannot vectorize JOIN operator.
    cannotVectorize();

    testQuery(
        "SELECT l.k, l.v, SUM(f.m1), SUM(nf.m1)\n"
        + "FROM lookup.lookyloo l\n"
        + "INNER JOIN druid.foo f on f.dim1 = l.k\n"
        + "INNER JOIN druid.numfoo nf on nf.dim1 = l.k\n"
        + "GROUP BY 1, 2 ORDER BY 2",
        queryContext,
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            join(
                                join(
                                    new LookupDataSource("lookyloo"),
                                    new QueryDataSource(
                                        newScanQueryBuilder()
                                            .dataSource(CalciteTests.DATASOURCE1)
                                            .intervals(querySegmentSpec(Filtration.eternity()))
                                            .columns("dim1", "m1")
                                            .context(QUERY_CONTEXT_DEFAULT)
                                            .build()
                                    ),
                                    "j0.",
                                    equalsCondition(
                                        DruidExpression.fromColumn("k"),
                                        DruidExpression.fromColumn("j0.dim1")
                                    ),
                                    JoinType.INNER
                                ),
                                new QueryDataSource(
                                    newScanQueryBuilder()
                                        .dataSource(CalciteTests.DATASOURCE3)
                                        .intervals(querySegmentSpec(Filtration.eternity()))
                                        .columns("dim1", "m1")
                                        .context(QUERY_CONTEXT_DEFAULT)
                                        .build()
                                ),
                                "_j0.",
                                equalsCondition(
                                    DruidExpression.fromColumn("k"),
                                    DruidExpression.fromColumn("_j0.dim1")
                                ),
                                JoinType.INNER
                            )
                        )
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("k", "d0"),
                                new DefaultDimensionSpec("v", "d1")
                            )
                        )
                        .setAggregatorSpecs(
                            aggregators(
                                new DoubleSumAggregatorFactory("a0", "j0.m1"),
                                new DoubleSumAggregatorFactory("a1", "_j0.m1")
                            )
                        )
                        .setLimitSpec(
                            new DefaultLimitSpec(
                                ImmutableList.of(new OrderByColumnSpec("d1", Direction.ASCENDING)),
                                null
                            )
                        )
                        .setContext(queryContext)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"abc", "xabc", 6d, 6d}
        )
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		76
  
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testInnerJoinLookupTableTableChained(Map<String, Object> queryContext) throws Exception
  {
    // Cannot vectorize JOIN operator.
    cannotVectorize();

    testQuery(
        "SELECT l.k, l.v, SUM(f.m1), SUM(nf.m1)\n"
        + "FROM lookup.lookyloo l\n"
        + "INNER JOIN druid.foo f on f.dim1 = l.k\n"
        + "INNER JOIN druid.numfoo nf on nf.dim1 = f.dim1\n"
        + "GROUP BY 1, 2 ORDER BY 2",
        queryContext,
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            join(
                                join(
                                    new LookupDataSource("lookyloo"),
                                    new QueryDataSource(
                                        newScanQueryBuilder()
                                            .dataSource(CalciteTests.DATASOURCE1)
                                            .intervals(querySegmentSpec(Filtration.eternity()))
                                            .columns("dim1", "m1")
                                            .context(QUERY_CONTEXT_DEFAULT)
                                            .build()
                                    ),
                                    "j0.",
                                    equalsCondition(
                                        DruidExpression.fromColumn("k"),
                                        DruidExpression.fromColumn("j0.dim1")
                                    ),
                                    JoinType.INNER
                                ),
                                new QueryDataSource(
                                    newScanQueryBuilder()
                                        .dataSource(CalciteTests.DATASOURCE3)
                                        .intervals(querySegmentSpec(Filtration.eternity()))
                                        .columns("dim1", "m1")
                                        .context(QUERY_CONTEXT_DEFAULT)
                                        .build()
                                ),
                                "_j0.",
                                equalsCondition(
                                    DruidExpression.fromColumn("j0.dim1"),
                                    DruidExpression.fromColumn("_j0.dim1")
                                ),
                                JoinType.INNER
                            )
                        )
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("k", "d0"),
                                new DefaultDimensionSpec("v", "d1")
                            )
                        )
                        .setAggregatorSpecs(
                            aggregators(
                                new DoubleSumAggregatorFactory("a0", "j0.m1"),
                                new DoubleSumAggregatorFactory("a1", "_j0.m1")
                            )
                        )
                        .setLimitSpec(
                            new DefaultLimitSpec(
                                ImmutableList.of(new OrderByColumnSpec("d1", Direction.ASCENDING)),
                                null
                            )
                        )
                        .setContext(queryContext)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"abc", "xabc", 6d, 6d}
        )
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 2
Assertions		 0
Lines of Code		39
  
  public void testWhereInSelectNullFromLookup() throws Exception
  {
    // Regression test for https://github.com/apache/druid/issues/9646.
    cannotVectorize();

    testQuery(
        "SELECT * FROM foo where dim1 IN (SELECT NULL FROM lookup.lookyloo)",
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(
                    join(
                        new TableDataSource(CalciteTests.DATASOURCE1),
                        new QueryDataSource(
                            GroupByQuery.builder()
                                        .setDataSource(new LookupDataSource("lookyloo"))
                                        .setInterval(querySegmentSpec(Filtration.eternity()))
                                        .setVirtualColumns(
                                            expressionVirtualColumn("v0", "null", ColumnType.STRING)
                                        )
                                        .setGranularity(Granularities.ALL)
                                        .setDimensions(dimensions(new DefaultDimensionSpec("v0", "d0")))
                                        .setContext(QUERY_CONTEXT_DEFAULT)
                                        .build()
                        ),
                        "j0.",
                        equalsCondition(DruidExpression.fromColumn("dim1"), DruidExpression.fromColumn("j0.d0")),
                        JoinType.INNER
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(
                    expressionVirtualColumn("v0", "null", ColumnType.STRING)
                )
                .columns("__time", "cnt", "dim2", "dim3", "m1", "m2", "unique_dim1", "v0")
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of()
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		32
  
  public void testCommaJoinLeftFunction() throws Exception
  {
    testQuery(
        "SELECT foo.dim1, foo.dim2, l.k, l.v\n"
        + "FROM foo, lookup.lookyloo l\n"
        + "WHERE SUBSTRING(foo.dim2, 1, 1) = l.k\n",
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(
                    join(
                        new TableDataSource(CalciteTests.DATASOURCE1),
                        new LookupDataSource("lookyloo"),
                        "j0.",
                        equalsCondition(
                            DruidExpression.fromExpression("substring(\"dim2\", 0, 1)"),
                            DruidExpression.fromColumn("j0.k")
                        ),
                        JoinType.INNER
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("dim1", "dim2", "j0.k", "j0.v")
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"", "a", "a", "xa"},
            new Object[]{"1", "a", "a", "xa"},
            new Object[]{"def", "abc", "a", "xa"}
        )
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 2
Assertions		 0
Lines of Code		49
  
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testCommaJoinTableLookupTableMismatchedTypes(Map<String, Object> queryContext) throws Exception
  {
    // Regression test for https://github.com/apache/druid/issues/9646.

    // Cannot vectorize JOIN operator.
    cannotVectorize();

    testQuery(
        "SELECT COUNT(*)\n"
        + "FROM foo, lookup.lookyloo l, numfoo\n"
        + "WHERE foo.cnt = l.k AND l.k = numfoo.cnt\n",
        queryContext,
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(
                      join(
                          join(
                              new TableDataSource(CalciteTests.DATASOURCE1),
                              new LookupDataSource("lookyloo"),
                              "j0.",
                              "1",
                              JoinType.INNER
                          ),
                          new QueryDataSource(
                              newScanQueryBuilder()
                                  .dataSource(CalciteTests.DATASOURCE3)
                                  .intervals(querySegmentSpec(Filtration.eternity()))
                                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                  .columns("cnt")
                                  .context(QUERY_CONTEXT_DEFAULT)
                                  .build()
                          ),
                          "_j0.",
                          "1",
                          JoinType.INNER
                      )
                  )
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(new CountAggregatorFactory("a0"))
                  .filters(and(
                      expressionFilter("(\"cnt\" == CAST(\"j0.k\", 'LONG'))"),
                      expressionFilter("(CAST(\"j0.k\", 'LONG') == \"_j0.cnt\")")
                  ))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{0L}
        )
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		63
  
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testJoinTableLookupTableMismatchedTypesWithoutComma(Map<String, Object> queryContext) throws Exception
  {
    // Cannot vectorize JOIN operator.
    cannotVectorize();

    testQuery(
        "SELECT COUNT(*)\n"
        + "FROM foo\n"
        + "INNER JOIN lookup.lookyloo l ON foo.cnt = l.k\n"
        + "INNER JOIN numfoo ON l.k = numfoo.cnt\n",
        queryContext,
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(
                      join(
                          join(
                              new TableDataSource(CalciteTests.DATASOURCE1),
                              new QueryDataSource(
                                  newScanQueryBuilder()
                                      .dataSource(new LookupDataSource("lookyloo"))
                                      .intervals(querySegmentSpec(Filtration.eternity()))
                                      .virtualColumns(
                                          expressionVirtualColumn("v0", "CAST(\"k\", 'LONG')", ColumnType.LONG)
                                      )
                                      .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                      .columns("k", "v0")
                                      .context(queryContext)
                                      .build()
                              ),
                              "j0.",
                              equalsCondition(
                                  DruidExpression.fromColumn("cnt"),
                                  DruidExpression.fromColumn("j0.v0")
                              ),
                              JoinType.INNER
                          ),
                          new QueryDataSource(
                              newScanQueryBuilder()
                                  .dataSource(CalciteTests.DATASOURCE3)
                                  .intervals(querySegmentSpec(Filtration.eternity()))
                                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                  .columns("cnt")
                                  .context(queryContext)
                                  .build()
                          ),
                          "_j0.",
                          equalsCondition(
                              DruidExpression.fromExpression("CAST(\"j0.k\", 'LONG')"),
                              DruidExpression.fromColumn("_j0.cnt")
                          ),
                          JoinType.INNER
                      )
                  )
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(new CountAggregatorFactory("a0"))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{0L}
        )
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		30
  
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testInnerJoinCastLeft(Map<String, Object> queryContext) throws Exception
  {
    // foo.m1 is FLOAT, l.k is STRING.

    testQuery(
        "SELECT foo.m1, l.k, l.v\n"
        + "FROM foo\n"
        + "INNER JOIN lookup.lookyloo l ON CAST(foo.m1 AS VARCHAR) = l.k\n",
        queryContext,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(
                    join(
                        new TableDataSource(CalciteTests.DATASOURCE1),
                        new LookupDataSource("lookyloo"),
                        "j0.",
                        equalsCondition(
                            DruidExpression.fromExpression("CAST(\"m1\", 'STRING')"),
                            DruidExpression.fromColumn("j0.k")
                        ),
                        JoinType.INNER
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("j0.k", "j0.v", "m1")
                .context(queryContext)
                .build()
        ),
        ImmutableList.of()
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		40
  
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testInnerJoinCastRight(Map<String, Object> queryContext) throws Exception
  {
    // foo.m1 is FLOAT, l.k is STRING.

    testQuery(
        "SELECT foo.m1, l.k, l.v\n"
        + "FROM foo\n"
        + "INNER JOIN lookup.lookyloo l ON foo.m1 = CAST(l.k AS FLOAT)\n",
        queryContext,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(
                    join(
                        new TableDataSource(CalciteTests.DATASOURCE1),
                        new QueryDataSource(
                            newScanQueryBuilder()
                                .dataSource(new LookupDataSource("lookyloo"))
                                .intervals(querySegmentSpec(Filtration.eternity()))
                                .virtualColumns(
                                    expressionVirtualColumn("v0", "CAST(\"k\", 'DOUBLE')", ColumnType.FLOAT)
                                )
                                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                .columns("k", "v", "v0")
                                .context(queryContext)
                                .build()
                        ),
                        "j0.",
                        equalsCondition(DruidExpression.fromColumn("m1"), DruidExpression.fromColumn("j0.v0")),
                        JoinType.INNER
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("j0.k", "j0.v", "m1")
                .context(queryContext)
                .build()
        ),
        ImmutableList.of(
            new Object[]{6f, "6", "x6"}
        )
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		40
  
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testInnerJoinMismatchedTypes(Map<String, Object> queryContext) throws Exception
  {
    // foo.m1 is FLOAT, l.k is STRING. Comparing them generates a CAST.

    testQuery(
        "SELECT foo.m1, l.k, l.v\n"
        + "FROM foo\n"
        + "INNER JOIN lookup.lookyloo l ON foo.m1 = l.k\n",
        queryContext,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(
                    join(
                        new TableDataSource(CalciteTests.DATASOURCE1),
                        new QueryDataSource(
                            newScanQueryBuilder()
                                .dataSource(new LookupDataSource("lookyloo"))
                                .intervals(querySegmentSpec(Filtration.eternity()))
                                .virtualColumns(
                                    expressionVirtualColumn("v0", "CAST(\"k\", 'DOUBLE')", ColumnType.FLOAT)
                                )
                                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                .columns("k", "v", "v0")
                                .context(queryContext)
                                .build()
                        ),
                        "j0.",
                        equalsCondition(DruidExpression.fromColumn("m1"), DruidExpression.fromColumn("j0.v0")),
                        JoinType.INNER
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("j0.k", "j0.v", "m1")
                .context(queryContext)
                .build()
        ),
        ImmutableList.of(
            new Object[]{6f, "6", "x6"}
        )
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		34
  
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testInnerJoinLeftFunction(Map<String, Object> queryContext) throws Exception
  {
    testQuery(
        "SELECT foo.dim1, foo.dim2, l.k, l.v\n"
        + "FROM foo\n"
        + "INNER JOIN lookup.lookyloo l ON SUBSTRING(foo.dim2, 1, 1) = l.k\n",
        queryContext,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(
                    join(
                        new TableDataSource(CalciteTests.DATASOURCE1),
                        new LookupDataSource("lookyloo"),
                        "j0.",
                        equalsCondition(
                            DruidExpression.fromExpression("substring(\"dim2\", 0, 1)"),
                            DruidExpression.fromColumn("j0.k")
                        ),
                        JoinType.INNER
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("dim1", "dim2", "j0.k", "j0.v")
                .context(queryContext)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"", "a", "a", "xa"},
            new Object[]{"1", "a", "a", "xa"},
            new Object[]{"def", "abc", "a", "xa"}
        )
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		41
  
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testInnerJoinRightFunction(Map<String, Object> queryContext) throws Exception
  {
    testQuery(
        "SELECT foo.dim1, foo.dim2, l.k, l.v\n"
        + "FROM foo\n"
        + "INNER JOIN lookup.lookyloo l ON foo.dim2 = SUBSTRING(l.k, 1, 2)\n",
        queryContext,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(
                    join(
                        new TableDataSource(CalciteTests.DATASOURCE1),
                        new QueryDataSource(
                            newScanQueryBuilder()
                                .dataSource(new LookupDataSource("lookyloo"))
                                .intervals(querySegmentSpec(Filtration.eternity()))
                                .virtualColumns(
                                    expressionVirtualColumn("v0", "substring(\"k\", 0, 2)", ColumnType.STRING)
                                )
                                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                .columns("k", "v", "v0")
                                .context(queryContext)
                                .build()
                        ),
                        "j0.",
                        equalsCondition(DruidExpression.fromColumn("dim2"), DruidExpression.fromColumn("j0.v0")),
                        JoinType.INNER
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("dim1", "dim2", "j0.k", "j0.v")
                .context(queryContext)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"", "a", "a", "xa"},
            new Object[]{"1", "a", "a", "xa"}
        )
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		41
  
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testLeftJoinLookupOntoLookupUsingJoinOperator(Map<String, Object> queryContext) throws Exception
  {
    testQuery(
        "SELECT dim2, l1.v, l2.v\n"
        + "FROM foo\n"
        + "LEFT JOIN lookup.lookyloo l1 ON foo.dim2 = l1.k\n"
        + "LEFT JOIN lookup.lookyloo l2 ON l1.k = l2.k",
        queryContext,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(
                    join(
                        join(
                            new TableDataSource(CalciteTests.DATASOURCE1),
                            new LookupDataSource("lookyloo"),
                            "j0.",
                            equalsCondition(DruidExpression.fromColumn("dim2"), DruidExpression.fromColumn("j0.k")),
                            JoinType.LEFT
                        ),
                        new LookupDataSource("lookyloo"),
                        "_j0.",
                        equalsCondition(DruidExpression.fromColumn("j0.k"), DruidExpression.fromColumn("_j0.k")),
                        JoinType.LEFT
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("_j0.v", "dim2", "j0.v")
                .context(queryContext)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"a", "xa", "xa"},
            new Object[]{NULL_STRING, NULL_STRING, NULL_STRING},
            new Object[]{"", NULL_STRING, NULL_STRING},
            new Object[]{"a", "xa", "xa"},
            new Object[]{"abc", "xabc", "xabc"},
            new Object[]{NULL_STRING, NULL_STRING, NULL_STRING}
        )
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		48
  
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testLeftJoinThreeLookupsUsingJoinOperator(Map<String, Object> queryContext) throws Exception
  {
    testQuery(
        "SELECT dim1, dim2, l1.v, l2.v, l3.v\n"
        + "FROM foo\n"
        + "LEFT JOIN lookup.lookyloo l1 ON foo.dim1 = l1.k\n"
        + "LEFT JOIN lookup.lookyloo l2 ON foo.dim2 = l2.k\n"
        + "LEFT JOIN lookup.lookyloo l3 ON l2.k = l3.k",
        queryContext,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(
                    join(
                        join(
                            join(
                                new TableDataSource(CalciteTests.DATASOURCE1),
                                new LookupDataSource("lookyloo"),
                                "j0.",
                                equalsCondition(DruidExpression.fromColumn("dim1"), DruidExpression.fromColumn("j0.k")),
                                JoinType.LEFT
                            ),
                            new LookupDataSource("lookyloo"),
                            "_j0.",
                            equalsCondition(DruidExpression.fromColumn("dim2"), DruidExpression.fromColumn("_j0.k")),
                            JoinType.LEFT
                        ),
                        new LookupDataSource("lookyloo"),
                        "__j0.",
                        equalsCondition(DruidExpression.fromColumn("_j0.k"), DruidExpression.fromColumn("__j0.k")),
                        JoinType.LEFT
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("__j0.v", "_j0.v", "dim1", "dim2", "j0.v")
                .context(queryContext)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"", "a", NULL_STRING, "xa", "xa"},
            new Object[]{"10.1", NULL_STRING, NULL_STRING, NULL_STRING, NULL_STRING},
            new Object[]{"2", "", NULL_STRING, NULL_STRING, NULL_STRING},
            new Object[]{"1", "a", NULL_STRING, "xa", "xa"},
            new Object[]{"def", "abc", NULL_STRING, "xabc", "xabc"},
            new Object[]{"abc", NULL_STRING, "xabc", NULL_STRING, NULL_STRING}
        )
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		35
  
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testSelectOnLookupUsingLeftJoinOperator(Map<String, Object> queryContext) throws Exception
  {
    testQuery(
        "SELECT dim1, lookyloo.*\n"
        + "FROM foo LEFT JOIN lookup.lookyloo ON foo.dim1 = lookyloo.k\n"
        + "WHERE lookyloo.v <> 'xxx' OR lookyloo.v IS NULL",
        queryContext,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(
                    join(
                        new TableDataSource(CalciteTests.DATASOURCE1),
                        new LookupDataSource("lookyloo"),
                        "j0.",
                        equalsCondition(DruidExpression.fromColumn("dim1"), DruidExpression.fromColumn("j0.k")),
                        JoinType.LEFT
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .filters(or(not(selector("j0.v", "xxx", null)), selector("j0.v", null, null)))
                .columns("dim1", "j0.k", "j0.v")
                .context(queryContext)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"", NULL_STRING, NULL_STRING},
            new Object[]{"10.1", NULL_STRING, NULL_STRING},
            new Object[]{"2", NULL_STRING, NULL_STRING},
            new Object[]{"1", NULL_STRING, NULL_STRING},
            new Object[]{"def", NULL_STRING, NULL_STRING},
            new Object[]{"abc", "abc", "xabc"}
        )
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		33
  
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testSelectOnLookupUsingRightJoinOperator(Map<String, Object> queryContext) throws Exception
  {
    testQuery(
        "SELECT dim1, lookyloo.*\n"
        + "FROM foo RIGHT JOIN lookup.lookyloo ON foo.dim1 = lookyloo.k\n"
        + "WHERE lookyloo.v <> 'xxx' OR lookyloo.v IS NULL",
        queryContext,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(
                    join(
                        new TableDataSource(CalciteTests.DATASOURCE1),
                        new LookupDataSource("lookyloo"),
                        "j0.",
                        equalsCondition(DruidExpression.fromColumn("dim1"), DruidExpression.fromColumn("j0.k")),
                        JoinType.RIGHT
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .filters(or(not(selector("j0.v", "xxx", null)), selector("j0.v", null, null)))
                .columns("dim1", "j0.k", "j0.v")
                .context(queryContext)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"abc", "abc", "xabc"},
            new Object[]{NULL_STRING, "a", "xa"},
            new Object[]{NULL_STRING, "nosuchkey", "mysteryvalue"},
            new Object[]{NULL_STRING, "6", "x6"}
        )
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		38
  
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testSelectOnLookupUsingFullJoinOperator(Map<String, Object> queryContext) throws Exception
  {
    testQuery(
        "SELECT dim1, m1, cnt, lookyloo.*\n"
        + "FROM foo FULL JOIN lookup.lookyloo ON foo.dim1 = lookyloo.k\n"
        + "WHERE lookyloo.v <> 'xxx' OR lookyloo.v IS NULL",
        queryContext,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(
                    join(
                        new TableDataSource(CalciteTests.DATASOURCE1),
                        new LookupDataSource("lookyloo"),
                        "j0.",
                        equalsCondition(DruidExpression.fromColumn("dim1"), DruidExpression.fromColumn("j0.k")),
                        JoinType.FULL
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .filters(or(not(selector("j0.v", "xxx", null)), selector("j0.v", null, null)))
                .columns("cnt", "dim1", "j0.k", "j0.v", "m1")
                .context(queryContext)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"", 1f, 1L, NULL_STRING, NULL_STRING},
            new Object[]{"10.1", 2f, 1L, NULL_STRING, NULL_STRING},
            new Object[]{"2", 3f, 1L, NULL_STRING, NULL_STRING},
            new Object[]{"1", 4f, 1L, NULL_STRING, NULL_STRING},
            new Object[]{"def", 5f, 1L, NULL_STRING, NULL_STRING},
            new Object[]{"abc", 6f, 1L, "abc", "xabc"},
            new Object[]{NULL_STRING, NULL_FLOAT, NULL_LONG, "a", "xa"},
            new Object[]{NULL_STRING, NULL_FLOAT, NULL_LONG, "nosuchkey", "mysteryvalue"},
            new Object[]{NULL_STRING, NULL_FLOAT, NULL_LONG, "6", "x6"}
        )
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		38
  
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testCountDistinctOfLookupUsingJoinOperator(Map<String, Object> queryContext) throws Exception
  {
    // Cannot yet vectorize the JOIN operator.
    cannotVectorize();

    testQuery(
        "SELECT COUNT(DISTINCT lookyloo.v)\n"
        + "FROM foo LEFT JOIN lookup.lookyloo ON foo.dim1 = lookyloo.k",
        queryContext,
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(
                      join(
                          new TableDataSource(CalciteTests.DATASOURCE1),
                          new LookupDataSource("lookyloo"),
                          "j0.",
                          equalsCondition(DruidExpression.fromColumn("dim1"), DruidExpression.fromColumn("j0.k")),
                          JoinType.LEFT
                      )
                  )
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(
                      new CardinalityAggregatorFactory(
                          "a0",
                          null,
                          ImmutableList.of(DefaultDimensionSpec.of("j0.v")),
                          false,
                          true
                      )
                  ))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{NullHandling.replaceWithDefault() ? 2L : 1L}
        )
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 2
Assertions		 0
Lines of Code		59
  
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testUsingSubqueryAsPartOfAndFilter(Map<String, Object> queryContext) throws Exception
  {
    // Fully removing the join allows this query to vectorize.
    if (!isRewriteJoinToFilter(queryContext)) {
      cannotVectorize();
    }

    testQuery(
        "SELECT dim1, dim2, COUNT(*) FROM druid.foo\n"
        + "WHERE dim2 IN (SELECT dim1 FROM druid.foo WHERE dim1 <> '')\n"
        + "AND dim1 <> 'xxx'\n"
        + "group by dim1, dim2 ORDER BY dim2",
        queryContext,
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            join(
                                new TableDataSource(CalciteTests.DATASOURCE1),
                                new QueryDataSource(
                                    GroupByQuery.builder()
                                                .setDataSource(CalciteTests.DATASOURCE1)
                                                .setInterval(querySegmentSpec(Filtration.eternity()))
                                                .setGranularity(Granularities.ALL)
                                                .setDimFilter(not(selector("dim1", "", null)))
                                                .setDimensions(dimensions(new DefaultDimensionSpec("dim1", "d0")))
                                                .setContext(QUERY_CONTEXT_DEFAULT)
                                                .build()
                                ),
                                "j0.",
                                equalsCondition(
                                    DruidExpression.fromColumn("dim2"),
                                    DruidExpression.fromColumn("j0.d0")
                                ),
                                JoinType.INNER
                            )
                        )
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimFilter(not(selector("dim1", "xxx", null)))
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("dim1", "d0"),
                                new DefaultDimensionSpec("dim2", "d1")
                            )
                        )
                        .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                        .setLimitSpec(
                            new DefaultLimitSpec(
                                ImmutableList.of(new OrderByColumnSpec("d1", OrderByColumnSpec.Direction.ASCENDING)),
                                Integer.MAX_VALUE
                            )
                        )
                        .setContext(queryContext)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"def", "abc", 1L}
        )
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		86
  
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testUsingSubqueryAsPartOfOrFilter(Map<String, Object> queryContext) throws Exception
  {
    // Cannot vectorize JOIN operator.
    cannotVectorize();

    testQuery(
        "SELECT dim1, dim2, COUNT(*) FROM druid.foo\n"
        + "WHERE dim1 = 'xxx' OR dim2 IN (SELECT dim1 FROM druid.foo WHERE dim1 LIKE '%bc')\n"
        + "group by dim1, dim2 ORDER BY dim2",
        queryContext,
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            join(
                                join(
                                    new TableDataSource(CalciteTests.DATASOURCE1),
                                    new QueryDataSource(
                                        Druids.newTimeseriesQueryBuilder()
                                              .dataSource(CalciteTests.DATASOURCE1)
                                              .intervals(querySegmentSpec(Filtration.eternity()))
                                              .filters(new LikeDimFilter("dim1", "%bc", null, null))
                                              .granularity(Granularities.ALL)
                                              .aggregators(new CountAggregatorFactory("a0"))
                                              .context(QUERY_CONTEXT_DEFAULT)
                                              .build()
                                    ),
                                    "j0.",
                                    "1",
                                    JoinType.INNER
                                ),
                                new QueryDataSource(
                                    GroupByQuery.builder()
                                                .setDataSource(CalciteTests.DATASOURCE1)
                                                .setInterval(querySegmentSpec(Filtration.eternity()))
                                                .setGranularity(Granularities.ALL)
                                                .setVirtualColumns(expressionVirtualColumn("v0", "1", ColumnType.LONG))
                                                .setDimFilter(new LikeDimFilter("dim1", "%bc", null, null))
                                                .setDimensions(
                                                    dimensions(
                                                        new DefaultDimensionSpec("dim1", "d0"),
                                                        new DefaultDimensionSpec("v0", "d1", ColumnType.LONG)
                                                    )
                                                )
                                                .setContext(queryContext)
                                                .build()
                                ),
                                "_j0.",
                                equalsCondition(
                                    DruidExpression.fromColumn("dim2"),
                                    DruidExpression.fromColumn("_j0.d0")
                                ),
                                JoinType.LEFT
                            )
                        )
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimFilter(
                            or(
                                selector("dim1", "xxx", null),
                                and(
                                    not(selector("j0.a0", "0", null)),
                                    not(selector("_j0.d1", null, null)),
                                    not(selector("dim2", null, null))
                                )
                            )
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("dim1", "d0"),
                                new DefaultDimensionSpec("dim2", "d1")
                            )
                        )
                        .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                        .setLimitSpec(
                            new DefaultLimitSpec(
                                ImmutableList.of(new OrderByColumnSpec("d1", OrderByColumnSpec.Direction.ASCENDING)),
                                Integer.MAX_VALUE
                            )
                        )
                        .setContext(queryContext)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"def", "abc", 1L}
        )
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		73
  
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testNestedGroupByOnInlineDataSourceWithFilter(Map<String, Object> queryContext) throws Exception
  {
    // Cannot vectorize due to virtual columns.
    cannotVectorize();

    testQuery(
        "with abc as"
        + "("
        + "  SELECT dim1, m2 from druid.foo where \"__time\" >= '2001-01-02'"
        + ")"
        + ", def as"
        + "("
        + "  SELECT t1.dim1, SUM(t2.m2) as \"metricSum\" "
        + "  from abc as t1 inner join abc as t2 on t1.dim1 = t2.dim1"
        + "  where t1.dim1='def'"
        + "  group by 1"
        + ")"
        + "SELECT count(*) from def",
        queryContext,
        ImmutableList.of(
            GroupByQuery
                .builder()
                .setDataSource(
                    GroupByQuery
                        .builder()
                        .setDataSource(
                            join(
                                new QueryDataSource(
                                    newScanQueryBuilder()
                                        .dataSource(CalciteTests.DATASOURCE1)
                                        .intervals(querySegmentSpec(Intervals.of(
                                            "2001-01-02T00:00:00.000Z/146140482-04-24T15:36:27.903Z")))
                                        .columns("dim1")
                                        .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                        .context(queryContext)
                                        .build()
                                ),
                                new QueryDataSource(
                                    newScanQueryBuilder()
                                        .dataSource(CalciteTests.DATASOURCE1)
                                        .intervals(querySegmentSpec(Intervals.of(
                                            "2001-01-02T00:00:00.000Z/146140482-04-24T15:36:27.903Z")))
                                        .columns("dim1", "m2")
                                        .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                        .context(queryContext)
                                        .build()
                                ),
                                "j0.",
                                equalsCondition(
                                    DruidExpression.fromColumn("dim1"),
                                    DruidExpression.fromColumn("j0.dim1")
                                ),
                                JoinType.INNER
                            )
                        )
                        .setGranularity(Granularities.ALL)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setDimFilter(selector("dim1", "def", null))
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v0", "d0")
                            )
                        )
                        .setVirtualColumns(expressionVirtualColumn("v0", "'def'", ColumnType.STRING))
                        .build()
                )
                .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                .setGranularity(Granularities.ALL)
                .setInterval(querySegmentSpec(Filtration.eternity()))
                .build()
        ),
        ImmutableList.of(new Object[]{1L})
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 2
Assertions		 1
Lines of Code		55
  
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testGroupByJoinAsNativeQueryWithUnoptimizedFilter(Map<String, Object> queryContext)
  {
    // The query below is the same as the inner groupBy on a join datasource from the test
    // testNestedGroupByOnInlineDataSourceWithFilter, except that the selector filter
    // dim1=def has been rewritten into an unoptimized filter, dim1 IN (def).
    //
    // The unoptimized filter will be optimized into dim1=def by the query toolchests in their
    // pre-merge decoration function, when it calls DimFilter.optimize().
    //
    // This test's goal is to ensure that the join filter rewrites function correctly when there are
    // unoptimized filters in the join query. The rewrite logic must apply to the optimized form of the filters,
    // as this is what will be passed to HashJoinSegmentAdapter.makeCursors(), where the result of the join
    // filter pre-analysis is used.
    //
    // A native query is used because the filter types where we support optimization are the AND/OR/NOT and
    // IN filters. However, when expressed in a SQL query, our SQL planning layer is smart enough to already apply
    // these optimizations in the native query it generates, making it impossible to test the unoptimized filter forms
    // using SQL queries.
    //
    // The test method is placed here for convenience as this class provides the necessary setup.
    Query query = GroupByQuery
        .builder()
        .setDataSource(
            join(
                new QueryDataSource(
                    newScanQueryBuilder()
                        .dataSource(CalciteTests.DATASOURCE1)
                        .intervals(querySegmentSpec(Intervals.of(
                            "2001-01-02T00:00:00.000Z/146140482-04-24T15:36:27.903Z")))
                        .columns("dim1")
                        .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                        .context(queryContext)
                        .build()
                ),
                new QueryDataSource(
                    newScanQueryBuilder()
                        .dataSource(CalciteTests.DATASOURCE1)
                        .intervals(querySegmentSpec(Intervals.of(
                            "2001-01-02T00:00:00.000Z/146140482-04-24T15:36:27.903Z")))
                        .columns("dim1", "m2")
                        .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                        .context(queryContext)
                        .build()
                ),
                "j0.",
                equalsCondition(
                    DruidExpression.fromColumn("dim1"),
                    DruidExpression.fromColumn("j0.dim1")
                ),
                JoinType.INNER
            )
        )
        .setGranularity(Granularities.ALL)
        .setInterval(querySegmentSpec(Filtration.eternity()))
        .setDimFilter(in("dim1", Collections.singletonList("def"), null))  // provide an unoptimized IN filter
        .setDimensions(
            dimensions(
                new DefaultDimensionSpec("v0", "d0")
            )
        )
        .setVirtualColumns(expressionVirtualColumn("v0", "'def'", ColumnType.STRING))
        .build();

    QueryLifecycleFactory qlf = CalciteTests.createMockQueryLifecycleFactory(walker, conglomerate);
    QueryLifecycle ql = qlf.factorize();
    Sequence seq = ql.runSimple(query, CalciteTests.SUPER_USER_AUTH_RESULT, Access.OK);
    List<Object> results = seq.toList();
    Assert.assertEquals(
        ImmutableList.of(ResultRow.of("def")),
        results
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		43
  
  public void testSemiJoinWithOuterTimeExtractScan() throws Exception
  {
    testQuery(
        "SELECT dim1, EXTRACT(MONTH FROM __time) FROM druid.foo\n"
        + " WHERE dim2 IN (\n"
        + "   SELECT dim2\n"
        + "   FROM druid.foo\n"
        + "   WHERE dim1 = 'def'\n"
        + " ) AND dim1 <> ''",
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(
                    join(
                        new TableDataSource(CalciteTests.DATASOURCE1),
                        new QueryDataSource(
                            GroupByQuery.builder()
                                        .setDataSource(CalciteTests.DATASOURCE1)
                                        .setInterval(querySegmentSpec(Filtration.eternity()))
                                        .setGranularity(Granularities.ALL)
                                        .setDimensions(dimensions(new DefaultDimensionSpec("dim2", "d0")))
                                        .setDimFilter(selector("dim1", "def", null))
                                        .setContext(QUERY_CONTEXT_DEFAULT)
                                        .build()
                        ),
                        "j0.",
                        equalsCondition(DruidExpression.fromColumn("dim2"), DruidExpression.fromColumn("j0.d0")),
                        JoinType.INNER
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(
                    expressionVirtualColumn("v0", "timestamp_extract(\"__time\",'MONTH','UTC')", ColumnType.LONG)
                )
                .filters(not(selector("dim1", "", null)))
                .columns("dim1", "v0")
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"def", 1L}
        )
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		101
  
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testSemiAndAntiJoinSimultaneouslyUsingWhereInSubquery(Map<String, Object> queryContext) throws Exception
  {
    cannotVectorize();

    testQuery(
        "SELECT dim1, COUNT(*) FROM foo\n"
        + "WHERE dim1 IN ('abc', 'def')\n"
        + "AND __time IN (SELECT MAX(__time) FROM foo)\n"
        + "AND __time NOT IN (SELECT MIN(__time) FROM foo)\n"
        + "GROUP BY 1",
        queryContext,
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            join(
                                join(
                                    join(
                                        new TableDataSource(CalciteTests.DATASOURCE1),
                                        new QueryDataSource(
                                            Druids.newTimeseriesQueryBuilder()
                                                  .dataSource(CalciteTests.DATASOURCE1)
                                                  .intervals(querySegmentSpec(Filtration.eternity()))
                                                  .granularity(Granularities.ALL)
                                                  .aggregators(new LongMaxAggregatorFactory("a0", "__time"))
                                                  .context(QUERY_CONTEXT_DEFAULT)
                                                  .build()
                                        ),
                                        "j0.",
                                        "(\"__time\" == \"j0.a0\")",
                                        JoinType.INNER
                                    ),
                                    new QueryDataSource(
                                        GroupByQuery.builder()
                                                    .setDataSource(
                                                        new QueryDataSource(
                                                            Druids.newTimeseriesQueryBuilder()
                                                                  .dataSource(CalciteTests.DATASOURCE1)
                                                                  .intervals(querySegmentSpec(Filtration.eternity()))
                                                                  .granularity(Granularities.ALL)
                                                                  .aggregators(
                                                                      new LongMinAggregatorFactory("a0", "__time")
                                                                  )
                                                                  .context(QUERY_CONTEXT_DEFAULT)
                                                                  .build()
                                                        )
                                                    )
                                                    .setInterval(querySegmentSpec(Filtration.eternity()))
                                                    .setGranularity(Granularities.ALL)
                                                    .setAggregatorSpecs(
                                                        new CountAggregatorFactory("_a0"),
                                                        NullHandling.sqlCompatible()
                                                        ? new FilteredAggregatorFactory(
                                                            new CountAggregatorFactory("_a1"),
                                                            not(selector("a0", null, null))
                                                        )
                                                        : new CountAggregatorFactory("_a1")
                                                    )
                                                    .setContext(QUERY_CONTEXT_DEFAULT)
                                                    .build()
                                    ),
                                    "_j0.",
                                    "1",
                                    JoinType.INNER
                                ),
                                new QueryDataSource(
                                    Druids.newTimeseriesQueryBuilder()
                                          .dataSource(CalciteTests.DATASOURCE1)
                                          .intervals(querySegmentSpec(Filtration.eternity()))
                                          .granularity(Granularities.ALL)
                                          .aggregators(new LongMinAggregatorFactory("a0", "__time"))
                                          .postAggregators(expressionPostAgg("p0", "1"))
                                          .context(QUERY_CONTEXT_DEFAULT)
                                          .build()
                                ),
                                "__j0.",
                                "(\"__time\" == \"__j0.a0\")",
                                JoinType.LEFT
                            )
                        )
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimFilter(
                            and(
                                in("dim1", ImmutableList.of("abc", "def"), null),
                                or(
                                    selector("_j0._a0", "0", null),
                                    and(
                                        selector("__j0.p0", null, null),
                                        expressionFilter("(\"_j0._a1\" >= \"_j0._a0\")")
                                    )
                                )
                            )
                        )
                        .setDimensions(dimensions(new DefaultDimensionSpec("dim1", "d0", ColumnType.STRING)))
                        .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                        .setContext(queryContext)
                        .build()
        ),
        ImmutableList.of(new Object[]{"abc", 1L})
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		61
  
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testSemiAndAntiJoinSimultaneouslyUsingExplicitJoins(Map<String, Object> queryContext) throws Exception
  {
    cannotVectorize();

    testQuery(
        "SELECT dim1, COUNT(*) FROM\n"
        + "foo\n"
        + "INNER JOIN (SELECT MAX(__time) t FROM foo) t0 on t0.t = foo.__time\n"
        + "LEFT JOIN (SELECT MIN(__time) t FROM foo) t1 on t1.t = foo.__time\n"
        + "WHERE dim1 IN ('abc', 'def') AND t1.t is null\n"
        + "GROUP BY 1",
        queryContext,
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            join(
                                join(
                                    new TableDataSource(CalciteTests.DATASOURCE1),
                                    new QueryDataSource(
                                        Druids.newTimeseriesQueryBuilder()
                                              .dataSource(CalciteTests.DATASOURCE1)
                                              .intervals(querySegmentSpec(Filtration.eternity()))
                                              .granularity(Granularities.ALL)
                                              .aggregators(new LongMaxAggregatorFactory("a0", "__time"))
                                              .context(QUERY_CONTEXT_DEFAULT)
                                              .build()
                                    ),
                                    "j0.",
                                    "(\"__time\" == \"j0.a0\")",
                                    JoinType.INNER
                                ),
                                new QueryDataSource(
                                    Druids.newTimeseriesQueryBuilder()
                                          .dataSource(CalciteTests.DATASOURCE1)
                                          .intervals(querySegmentSpec(Filtration.eternity()))
                                          .granularity(Granularities.ALL)
                                          .aggregators(new LongMinAggregatorFactory("a0", "__time"))
                                          .context(QUERY_CONTEXT_DEFAULT)
                                          .build()
                                ),
                                "_j0.",
                                "(\"__time\" == \"_j0.a0\")",
                                JoinType.LEFT
                            )
                        )
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimFilter(
                            and(
                                in("dim1", ImmutableList.of("abc", "def"), null),
                                selector("_j0.a0", null, null)
                            )
                        )
                        .setDimensions(dimensions(new DefaultDimensionSpec("dim1", "d0", ColumnType.STRING)))
                        .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                        .setContext(queryContext)
                        .build()
        ),
        ImmutableList.of(new Object[]{"abc", 1L})
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		74
  
  public void testSemiJoinWithOuterTimeExtractAggregateWithOrderBy() throws Exception
  {
    // Cannot vectorize due to virtual columns.
    cannotVectorize();

    testQuery(
        "SELECT COUNT(DISTINCT dim1), EXTRACT(MONTH FROM __time) FROM druid.foo\n"
        + " WHERE dim2 IN (\n"
        + "   SELECT dim2\n"
        + "   FROM druid.foo\n"
        + "   WHERE dim1 = 'def'\n"
        + " ) AND dim1 <> ''"
        + "GROUP BY EXTRACT(MONTH FROM __time)\n"
        + "ORDER BY EXTRACT(MONTH FROM __time)",
        ImmutableList.of(
            GroupByQuery
                .builder()
                .setDataSource(
                    join(
                        new TableDataSource(CalciteTests.DATASOURCE1),
                        new QueryDataSource(
                            GroupByQuery
                                .builder()
                                .setDataSource(CalciteTests.DATASOURCE1)
                                .setInterval(querySegmentSpec(Filtration.eternity()))
                                .setGranularity(Granularities.ALL)
                                .setDimensions(dimensions(new DefaultDimensionSpec("dim2", "d0")))
                                .setDimFilter(selector("dim1", "def", null))
                                .setContext(QUERY_CONTEXT_DEFAULT)
                                .build()
                        ),
                        "j0.",
                        equalsCondition(DruidExpression.fromColumn("dim2"), DruidExpression.fromColumn("j0.d0")),
                        JoinType.INNER
                    )
                )
                .setVirtualColumns(
                    expressionVirtualColumn("v0", "timestamp_extract(\"__time\",'MONTH','UTC')", ColumnType.LONG)
                )
                .setDimFilter(not(selector("dim1", "", null)))
                .setDimensions(dimensions(new DefaultDimensionSpec("v0", "d0", ColumnType.LONG)))
                .setInterval(querySegmentSpec(Filtration.eternity()))
                .setGranularity(Granularities.ALL)
                .setAggregatorSpecs(
                    aggregators(
                        new CardinalityAggregatorFactory(
                            "a0",
                            null,
                            ImmutableList.of(
                                new DefaultDimensionSpec("dim1", "dim1", ColumnType.STRING)
                            ),
                            false,
                            true
                        )
                    )
                )
                .setLimitSpec(
                    new DefaultLimitSpec(
                        ImmutableList.of(
                            new OrderByColumnSpec(
                                "d0",
                                OrderByColumnSpec.Direction.ASCENDING,
                                StringComparators.NUMERIC
                            )
                        ),
                        Integer.MAX_VALUE
                    )
                )
                .setContext(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{1L, 1L}
        )
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 2
Assertions		 0
Lines of Code		45
  
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testInAggregationSubquery(Map<String, Object> queryContext) throws Exception
  {
    // Fully removing the join allows this query to vectorize.
    if (!isRewriteJoinToFilter(queryContext)) {
      cannotVectorize();
    }

    testQuery(
        "SELECT DISTINCT __time FROM druid.foo WHERE __time IN (SELECT MAX(__time) FROM druid.foo)",
        queryContext,
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            join(
                                new TableDataSource(CalciteTests.DATASOURCE1),
                                new QueryDataSource(
                                    Druids.newTimeseriesQueryBuilder()
                                          .dataSource(CalciteTests.DATASOURCE1)
                                          .intervals(querySegmentSpec(Filtration.eternity()))
                                          .granularity(Granularities.ALL)
                                          .aggregators(new LongMaxAggregatorFactory("a0", "__time"))
                                          .context(QUERY_CONTEXT_DEFAULT)
                                          .build()
                                          .withOverriddenContext(queryContext)
                                ),
                                "j0.",
                                equalsCondition(
                                    DruidExpression.fromColumn("__time"),
                                    DruidExpression.fromColumn("j0.a0")
                                ),
                                JoinType.INNER
                            )
                        )
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(dimensions(new DefaultDimensionSpec("__time", "d0", ColumnType.LONG)))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
                        .withOverriddenContext(queryContext)
        ),
        ImmutableList.of(
            new Object[]{timestamp("2001-01-03")}
        )
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		80
  
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testNotInAggregationSubquery(Map<String, Object> queryContext) throws Exception
  {
    // Cannot vectorize JOIN operator.
    cannotVectorize();

    testQuery(
        "SELECT DISTINCT __time FROM druid.foo WHERE __time NOT IN (SELECT MAX(__time) FROM druid.foo)",
        queryContext,
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            join(
                                join(
                                    new TableDataSource(CalciteTests.DATASOURCE1),
                                    new QueryDataSource(
                                        GroupByQuery
                                            .builder()
                                            .setDataSource(
                                                Druids.newTimeseriesQueryBuilder()
                                                      .dataSource(CalciteTests.DATASOURCE1)
                                                      .intervals(querySegmentSpec(Filtration.eternity()))
                                                      .granularity(Granularities.ALL)
                                                      .aggregators(new LongMaxAggregatorFactory("a0", "__time"))
                                                      .context(QUERY_CONTEXT_DEFAULT)
                                                      .build()
                                            )
                                            .setInterval(querySegmentSpec(Filtration.eternity()))
                                            .setGranularity(Granularities.ALL)
                                            .setAggregatorSpecs(
                                                new CountAggregatorFactory("_a0"),
                                                NullHandling.sqlCompatible()
                                                ? new FilteredAggregatorFactory(
                                                    new CountAggregatorFactory("_a1"),
                                                    not(selector("a0", null, null))
                                                )
                                                : new CountAggregatorFactory("_a1")
                                            )
                                            .setContext(queryContext)
                                            .build()
                                    ),
                                    "j0.",
                                    "1",
                                    JoinType.INNER
                                ),
                                new QueryDataSource(
                                    Druids.newTimeseriesQueryBuilder()
                                          .dataSource(CalciteTests.DATASOURCE1)
                                          .intervals(querySegmentSpec(Filtration.eternity()))
                                          .granularity(Granularities.ALL)
                                          .aggregators(new LongMaxAggregatorFactory("a0", "__time"))
                                          .postAggregators(expressionPostAgg("p0", "1"))
                                          .context(QUERY_CONTEXT_DEFAULT)
                                          .build()
                                ),
                                "_j0.",
                                "(\"__time\" == \"_j0.a0\")",
                                JoinType.LEFT
                            )
                        )
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimFilter(
                            or(
                                selector("j0._a0", "0", null),
                                and(selector("_j0.p0", null, null), expressionFilter("(\"j0._a1\" >= \"j0._a0\")"))
                            )
                        )
                        .setDimensions(dimensions(new DefaultDimensionSpec("__time", "d0", ColumnType.LONG)))
                        .setContext(queryContext)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{timestamp("2000-01-01")},
            new Object[]{timestamp("2000-01-02")},
            new Object[]{timestamp("2000-01-03")},
            new Object[]{timestamp("2001-01-01")},
            new Object[]{timestamp("2001-01-02")}
        )
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		54
  
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testUsingSubqueryWithExtractionFns(Map<String, Object> queryContext) throws Exception
  {
    // Cannot vectorize JOIN operator.
    cannotVectorize();

    testQuery(
        "SELECT dim2, COUNT(*) FROM druid.foo "
        + "WHERE substring(dim2, 1, 1) IN (SELECT substring(dim1, 1, 1) FROM druid.foo WHERE dim1 <> '')"
        + "group by dim2",
        queryContext,
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            join(
                                new TableDataSource(CalciteTests.DATASOURCE1),
                                new QueryDataSource(
                                    GroupByQuery.builder()
                                                .setDataSource(CalciteTests.DATASOURCE1)
                                                .setInterval(querySegmentSpec(Filtration.eternity()))
                                                .setGranularity(Granularities.ALL)
                                                .setDimFilter(not(selector("dim1", "", null)))
                                                .setDimensions(
                                                    dimensions(new ExtractionDimensionSpec(
                                                        "dim1",
                                                        "d0",
                                                        new SubstringDimExtractionFn(
                                                            0,
                                                            1
                                                        )
                                                    ))
                                                )
                                                .setContext(QUERY_CONTEXT_DEFAULT)
                                                .build()
                                ),
                                "j0.",
                                equalsCondition(
                                    DruidExpression.fromExpression("substring(\"dim2\", 0, 1)"),
                                    DruidExpression.fromColumn("j0.d0")
                                ),
                                JoinType.INNER
                            )
                        )
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(dimensions(new DefaultDimensionSpec("dim2", "d0")))
                        .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                        .setContext(queryContext)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"a", 2L},
            new Object[]{"abc", 1L}
        )
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		31
  
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testInnerJoinWithIsNullFilter(Map<String, Object> queryContext) throws Exception
  {
    testQuery(
        "SELECT dim1, l.v from druid.foo f inner join lookup.lookyloo l on f.dim1 = l.k where f.dim2 is null",
        queryContext,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(
                    join(
                        new TableDataSource(CalciteTests.DATASOURCE1),
                        new LookupDataSource("lookyloo"),
                        "j0.",
                        equalsCondition(
                            DruidExpression.fromColumn("dim1"),
                            DruidExpression.fromColumn("j0.k")
                        ),
                        JoinType.INNER
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .filters(selector("dim2", null, null))
                .columns("dim1", "j0.v")
                .build()
        ),
        ImmutableList.of(
            new Object[]{"abc", "xabc"}
        )
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 2
Assertions		 0
Lines of Code		39
  
  @Parameters(source = QueryContextForJoinProvider.class)
  @Ignore // regression test for https://github.com/apache/druid/issues/9924
  public void testInnerJoinOnMultiValueColumn(Map<String, Object> queryContext) throws Exception
  {
    cannotVectorize();
    testQuery(
        "SELECT dim3, l.v, count(*) from druid.foo f inner join lookup.lookyloo l on f.dim3 = l.k "
        + "group by 1, 2",
        queryContext,
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            join(
                                new TableDataSource(CalciteTests.DATASOURCE1),
                                new LookupDataSource("lookyloo"),
                                "j0.",
                                equalsCondition(
                                    DruidExpression.fromColumn("dim3"),
                                    DruidExpression.fromColumn("j0.k")
                                ),
                                JoinType.INNER
                            )
                        )
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("dim3", "d0"),
                                new DefaultDimensionSpec("j0.v", "d1")
                            )
                        )
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"2", "x2", 1L}
         )
     );
   }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		40
  
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testCountOnSemiJoinSingleColumn(Map<String, Object> queryContext) throws Exception
  {
    testQuery(
        "SELECT dim1 FROM foo WHERE dim1 IN (SELECT dim1 FROM foo WHERE dim1 = '10.1')\n",
        queryContext,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(
                    join(
                        new TableDataSource(CalciteTests.DATASOURCE1),
                        new QueryDataSource(
                            GroupByQuery.builder()
                                        .setDataSource(CalciteTests.DATASOURCE1)
                                        .setInterval(querySegmentSpec(Filtration.eternity()))
                                        .setDimFilter(
                                            selector("dim1", "10.1", null)
                                        )
                                        .setGranularity(Granularities.ALL)
                                        .setDimensions(dimensions(new DefaultDimensionSpec("dim1", "d0")))
                                        .setContext(queryContext)
                                        .build()
                        ),
                        "j0.",
                        equalsCondition(DruidExpression.fromColumn("dim1"), DruidExpression.fromColumn("j0.d0")),
                        JoinType.INNER
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(expressionVirtualColumn("v0", "\'10.1\'", ColumnType.STRING))
                .columns("v0")
                .context(queryContext)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"10.1"}
        )
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		67
  
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testLeftJoinOnTwoInlineDataSourcesWithTimeFilter(Map<String, Object> queryContext) throws Exception
  {
    testQuery(
        "with abc as\n"
        + "(\n"
        + "  SELECT dim1, \"__time\", m1 from foo WHERE \"dim1\" = '10.1' AND \"__time\" >= '1999'\n"
        + ")\n"
        + "SELECT t1.dim1, t1.\"__time\" from abc as t1 LEFT JOIN abc as t2 on t1.dim1 = t2.dim1 WHERE t1.dim1 = '10.1'\n",
        queryContext,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(
                    join(
                        new QueryDataSource(
                            newScanQueryBuilder()
                                .dataSource(CalciteTests.DATASOURCE1)
                                .intervals(
                                    querySegmentSpec(
                                        Intervals.utc(
                                            DateTimes.of("1999-01-01").getMillis(),
                                            JodaUtils.MAX_INSTANT
                                        )
                                    )
                                )
                                .filters(new SelectorDimFilter("dim1", "10.1", null))
                                .virtualColumns(expressionVirtualColumn("v0", "\'10.1\'", ColumnType.STRING))
                                .columns(ImmutableList.of("__time", "v0"))
                                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                .context(queryContext)
                                .build()
                        ),
                        new QueryDataSource(
                            newScanQueryBuilder()
                                .dataSource(CalciteTests.DATASOURCE1)
                                .intervals(
                                    querySegmentSpec(
                                        Intervals.utc(
                                            DateTimes.of("1999-01-01").getMillis(),
                                            JodaUtils.MAX_INSTANT
                                        )
                                    )
                                )
                                .filters(new SelectorDimFilter("dim1", "10.1", null))
                                .virtualColumns(expressionVirtualColumn("v0", "\'10.1\'", ColumnType.STRING))
                                .columns(ImmutableList.of("v0"))
                                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                .context(queryContext)
                                .build()
                        ),
                        "j0.",
                        equalsCondition(DruidExpression.fromColumn("v0"), DruidExpression.fromColumn("j0.v0")),
                        JoinType.LEFT
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(expressionVirtualColumn("_v0", "\'10.1\'", ColumnType.STRING))
                .columns("__time", "_v0")
                .filters(new SelectorDimFilter("v0", "10.1", null))
                .context(queryContext)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"10.1", 946771200000L}
        )
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		57
  
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testLeftJoinOnTwoInlineDataSourcesWithTimeFilter_withLeftDirectAccess(Map<String, Object> queryContext)
      throws Exception
  {
    queryContext = withLeftDirectAccessEnabled(queryContext);
    testQuery(
        "with abc as\n"
        + "(\n"
        + "  SELECT dim1, \"__time\", m1 from foo WHERE \"dim1\" = '10.1' AND \"__time\" >= '1999'\n"
        + ")\n"
        + "SELECT t1.dim1, t1.\"__time\" from abc as t1 LEFT JOIN abc as t2 on t1.dim1 = t2.dim1 WHERE t1.dim1 = '10.1'\n",
        queryContext,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(
                    join(
                        new TableDataSource(CalciteTests.DATASOURCE1),
                        new QueryDataSource(
                            newScanQueryBuilder()
                                .dataSource(CalciteTests.DATASOURCE1)
                                .intervals(
                                    querySegmentSpec(
                                        Intervals.utc(
                                            DateTimes.of("1999-01-01").getMillis(),
                                            JodaUtils.MAX_INSTANT
                                        )
                                    )
                                )
                                .filters(new SelectorDimFilter("dim1", "10.1", null))
                                .virtualColumns(expressionVirtualColumn("v0", "\'10.1\'", ColumnType.STRING))
                                .columns(ImmutableList.of("v0"))
                                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                .context(queryContext)
                                .build()
                        ),
                        "j0.",
                        equalsCondition(DruidExpression.fromExpression("'10.1'"), DruidExpression.fromColumn("j0.v0")),
                        JoinType.LEFT,
                        selector("dim1", "10.1", null)
                    )
                )
                .intervals(querySegmentSpec(
                    Intervals.utc(
                        DateTimes.of("1999-01-01").getMillis(),
                        JodaUtils.MAX_INSTANT
                    )
                ))
                .virtualColumns(expressionVirtualColumn("v0", "\'10.1\'", ColumnType.STRING))
                .columns("__time", "v0")
                .context(queryContext)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"10.1", 946771200000L}
        )
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		52
  
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testLeftJoinOnTwoInlineDataSourcesWithOuterWhere(Map<String, Object> queryContext) throws Exception
  {
    testQuery(
        "with abc as\n"
        + "(\n"
        + "  SELECT dim1, \"__time\", m1 from foo WHERE \"dim1\" = '10.1'\n"
        + ")\n"
        + "SELECT t1.dim1, t1.\"__time\" from abc as t1 LEFT JOIN abc as t2 on t1.dim1 = t2.dim1 WHERE t1.dim1 = '10.1'\n",
        queryContext,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(
                    join(
                        new QueryDataSource(
                            newScanQueryBuilder()
                                .dataSource(CalciteTests.DATASOURCE1)
                                .intervals(querySegmentSpec(Filtration.eternity()))
                                .filters(new SelectorDimFilter("dim1", "10.1", null))
                                .virtualColumns(expressionVirtualColumn("v0", "\'10.1\'", ColumnType.STRING))
                                .columns(ImmutableList.of("__time", "v0"))
                                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                .context(queryContext)
                                .build()
                        ),
                        new QueryDataSource(
                            newScanQueryBuilder()
                                .dataSource(CalciteTests.DATASOURCE1)
                                .intervals(querySegmentSpec(Filtration.eternity()))
                                .filters(new SelectorDimFilter("dim1", "10.1", null))
                                .columns(ImmutableList.of("dim1"))
                                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                .context(queryContext)
                                .build()
                        ),
                        "j0.",
                        equalsCondition(DruidExpression.fromColumn("v0"), DruidExpression.fromColumn("j0.dim1")),
                        JoinType.LEFT
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(expressionVirtualColumn("_v0", "\'10.1\'", ColumnType.STRING))
                .columns("__time", "_v0")
                .filters(new SelectorDimFilter("v0", "10.1", null))
                .context(queryContext)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"10.1", 946771200000L}
        )
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		47
  
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testLeftJoinOnTwoInlineDataSourcesWithOuterWhere_withLeftDirectAccess(Map<String, Object> queryContext)
      throws Exception
  {
    queryContext = withLeftDirectAccessEnabled(queryContext);
    testQuery(
        "with abc as\n"
        + "(\n"
        + "  SELECT dim1, \"__time\", m1 from foo WHERE \"dim1\" = '10.1'\n"
        + ")\n"
        + "SELECT t1.dim1, t1.\"__time\" from abc as t1 LEFT JOIN abc as t2 on t1.dim1 = t2.dim1 WHERE t1.dim1 = '10.1'\n",
        queryContext,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(
                    join(
                        new TableDataSource(CalciteTests.DATASOURCE1),
                        new QueryDataSource(
                            newScanQueryBuilder()
                                .dataSource(CalciteTests.DATASOURCE1)
                                .intervals(querySegmentSpec(Filtration.eternity()))
                                .filters(new SelectorDimFilter("dim1", "10.1", null))
                                .columns(ImmutableList.of("dim1"))
                                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                .context(queryContext)
                                .build()
                        ),
                        "j0.",
                        equalsCondition(
                            DruidExpression.fromExpression("'10.1'"),
                            DruidExpression.fromColumn("j0.dim1")
                        ),
                        JoinType.LEFT,
                        selector("dim1", "10.1", null)
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(expressionVirtualColumn("v0", "\'10.1\'", ColumnType.STRING))
                .columns("__time", "v0")
                .context(queryContext)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"10.1", 946771200000L}
        )
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		51
  
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testLeftJoinOnTwoInlineDataSources(Map<String, Object> queryContext) throws Exception
  {
    testQuery(
        "with abc as\n"
        + "(\n"
        + "  SELECT dim1, \"__time\", m1 from foo WHERE \"dim1\" = '10.1'\n"
        + ")\n"
        + "SELECT t1.dim1, t1.\"__time\" from abc as t1 LEFT JOIN abc as t2 on t1.dim1 = t2.dim1\n",
        queryContext,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(
                    join(
                        new QueryDataSource(
                            newScanQueryBuilder()
                                .dataSource(CalciteTests.DATASOURCE1)
                                .intervals(querySegmentSpec(Filtration.eternity()))
                                .filters(new SelectorDimFilter("dim1", "10.1", null))
                                .virtualColumns(expressionVirtualColumn("v0", "\'10.1\'", ColumnType.STRING))
                                .columns(ImmutableList.of("__time", "v0"))
                                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                .context(queryContext)
                                .build()
                        ),
                        new QueryDataSource(
                            newScanQueryBuilder()
                                .dataSource(CalciteTests.DATASOURCE1)
                                .intervals(querySegmentSpec(Filtration.eternity()))
                                .filters(new SelectorDimFilter("dim1", "10.1", null))
                                .columns(ImmutableList.of("dim1"))
                                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                .context(queryContext)
                                .build()
                        ),
                        "j0.",
                        equalsCondition(DruidExpression.fromColumn("v0"), DruidExpression.fromColumn("j0.dim1")),
                        JoinType.LEFT
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(expressionVirtualColumn("_v0", "\'10.1\'", ColumnType.STRING))
                .columns("__time", "_v0")
                .context(queryContext)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"10.1", 946771200000L}
        )
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		46
  
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testLeftJoinOnTwoInlineDataSources_withLeftDirectAccess(Map<String, Object> queryContext) throws Exception
  {
    queryContext = withLeftDirectAccessEnabled(queryContext);
    testQuery(
        "with abc as\n"
        + "(\n"
        + "  SELECT dim1, \"__time\", m1 from foo WHERE \"dim1\" = '10.1'\n"
        + ")\n"
        + "SELECT t1.dim1, t1.\"__time\" from abc as t1 LEFT JOIN abc as t2 on t1.dim1 = t2.dim1\n",
        queryContext,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(
                    join(
                        new TableDataSource(CalciteTests.DATASOURCE1),
                        new QueryDataSource(
                            newScanQueryBuilder()
                                .dataSource(CalciteTests.DATASOURCE1)
                                .intervals(querySegmentSpec(Filtration.eternity()))
                                .filters(new SelectorDimFilter("dim1", "10.1", null))
                                .columns(ImmutableList.of("dim1"))
                                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                .context(queryContext)
                                .build()
                        ),
                        "j0.",
                        equalsCondition(
                            DruidExpression.fromExpression("'10.1'"),
                            DruidExpression.fromColumn("j0.dim1")
                        ),
                        JoinType.LEFT,
                        selector("dim1", "10.1", null)
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(expressionVirtualColumn("v0", "\'10.1\'", ColumnType.STRING))
                .columns("__time", "v0")
                .context(queryContext)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"10.1", 946771200000L}
        )
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		51
  
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testInnerJoinOnTwoInlineDataSourcesWithOuterWhere(Map<String, Object> queryContext) throws Exception
  {
    Druids.ScanQueryBuilder baseScanBuilder = newScanQueryBuilder()
        .dataSource(
            join(
                new QueryDataSource(
                    newScanQueryBuilder()
                        .dataSource(CalciteTests.DATASOURCE1)
                        .intervals(querySegmentSpec(Filtration.eternity()))
                        .filters(new SelectorDimFilter("dim1", "10.1", null))
                        .virtualColumns(expressionVirtualColumn("v0", "\'10.1\'", ColumnType.STRING))
                        .columns(ImmutableList.of("__time", "v0"))
                        .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                        .context(queryContext)
                        .build()
                ),
                new QueryDataSource(
                    newScanQueryBuilder()
                        .dataSource(CalciteTests.DATASOURCE1)
                        .intervals(querySegmentSpec(Filtration.eternity()))
                        .filters(new SelectorDimFilter("dim1", "10.1", null))
                        .columns(ImmutableList.of("dim1"))
                        .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                        .context(queryContext)
                        .build()
                ),
                "j0.",
                equalsCondition(DruidExpression.fromColumn("v0"), DruidExpression.fromColumn("j0.dim1")),
                JoinType.INNER
            )
        )
        .intervals(querySegmentSpec(Filtration.eternity()))
        .virtualColumns(expressionVirtualColumn("_v0", "\'10.1\'", ColumnType.STRING))
        .columns("__time", "_v0")
        .context(queryContext);

    testQuery(
        "with abc as\n"
        + "(\n"
        + "  SELECT dim1, \"__time\", m1 from foo WHERE \"dim1\" = '10.1'\n"
        + ")\n"
        + "SELECT t1.dim1, t1.\"__time\" from abc as t1 INNER JOIN abc as t2 on t1.dim1 = t2.dim1 WHERE t1.dim1 = '10.1'\n",
        queryContext,
        ImmutableList.of(
            NullHandling.sqlCompatible() ? baseScanBuilder.build() :
            baseScanBuilder.filters(new NotDimFilter(new SelectorDimFilter("v0", null, null))).build()),
        ImmutableList.of(
            new Object[]{"10.1", 946771200000L}
        )
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		47
  
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testInnerJoinOnTwoInlineDataSourcesWithOuterWhere_withLeftDirectAccess(Map<String, Object> queryContext)
      throws Exception
  {
    queryContext = withLeftDirectAccessEnabled(queryContext);
    testQuery(
        "with abc as\n"
        + "(\n"
        + "  SELECT dim1, \"__time\", m1 from foo WHERE \"dim1\" = '10.1'\n"
        + ")\n"
        + "SELECT t1.dim1, t1.\"__time\" from abc as t1 INNER JOIN abc as t2 on t1.dim1 = t2.dim1 WHERE t1.dim1 = '10.1'\n",
        queryContext,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(
                    join(
                        new TableDataSource(CalciteTests.DATASOURCE1),
                        new QueryDataSource(
                            newScanQueryBuilder()
                                .dataSource(CalciteTests.DATASOURCE1)
                                .intervals(querySegmentSpec(Filtration.eternity()))
                                .filters(new SelectorDimFilter("dim1", "10.1", null))
                                .columns(ImmutableList.of("dim1"))
                                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                .context(queryContext)
                                .build()
                        ),
                        "j0.",
                        equalsCondition(
                            DruidExpression.fromExpression("'10.1'"),
                            DruidExpression.fromColumn("j0.dim1")
                        ),
                        JoinType.INNER,
                        selector("dim1", "10.1", null)
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(expressionVirtualColumn("v0", "\'10.1\'", ColumnType.STRING))
                .columns("__time", "v0")
                .context(queryContext)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"10.1", 946771200000L}
        )
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		51
  
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testInnerJoinOnTwoInlineDataSources(Map<String, Object> queryContext) throws Exception
  {
    testQuery(
        "with abc as\n"
        + "(\n"
        + "  SELECT dim1, \"__time\", m1 from foo WHERE \"dim1\" = '10.1'\n"
        + ")\n"
        + "SELECT t1.dim1, t1.\"__time\" from abc as t1 INNER JOIN abc as t2 on t1.dim1 = t2.dim1\n",
        queryContext,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(
                    join(
                        new QueryDataSource(
                            newScanQueryBuilder()
                                .dataSource(CalciteTests.DATASOURCE1)
                                .intervals(querySegmentSpec(Filtration.eternity()))
                                .filters(new SelectorDimFilter("dim1", "10.1", null))
                                .virtualColumns(expressionVirtualColumn("v0", "\'10.1\'", ColumnType.STRING))
                                .columns(ImmutableList.of("__time", "v0"))
                                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                .context(queryContext)
                                .build()
                        ),
                        new QueryDataSource(
                            newScanQueryBuilder()
                                .dataSource(CalciteTests.DATASOURCE1)
                                .intervals(querySegmentSpec(Filtration.eternity()))
                                .filters(new SelectorDimFilter("dim1", "10.1", null))
                                .columns(ImmutableList.of("dim1"))
                                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                .context(queryContext)
                                .build()
                        ),
                        "j0.",
                        equalsCondition(DruidExpression.fromColumn("v0"), DruidExpression.fromColumn("j0.dim1")),
                        JoinType.INNER
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(expressionVirtualColumn("_v0", "\'10.1\'", ColumnType.STRING))
                .columns("__time", "_v0")
                .context(queryContext)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"10.1", 946771200000L}
        )
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		47
  
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testInnerJoinOnTwoInlineDataSources_withLeftDirectAccess(Map<String, Object> queryContext)
      throws Exception
  {
    queryContext = withLeftDirectAccessEnabled(queryContext);
    testQuery(
        "with abc as\n"
        + "(\n"
        + "  SELECT dim1, \"__time\", m1 from foo WHERE \"dim1\" = '10.1'\n"
        + ")\n"
        + "SELECT t1.dim1, t1.\"__time\" from abc as t1 INNER JOIN abc as t2 on t1.dim1 = t2.dim1\n",
        queryContext,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(
                    join(
                        new TableDataSource(CalciteTests.DATASOURCE1),
                        new QueryDataSource(
                            newScanQueryBuilder()
                                .dataSource(CalciteTests.DATASOURCE1)
                                .intervals(querySegmentSpec(Filtration.eternity()))
                                .filters(new SelectorDimFilter("dim1", "10.1", null))
                                .columns(ImmutableList.of("dim1"))
                                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                .context(queryContext)
                                .build()
                        ),
                        "j0.",
                        equalsCondition(
                            DruidExpression.fromExpression("'10.1'"),
                            DruidExpression.fromColumn("j0.dim1")
                        ),
                        JoinType.INNER,
                        selector("dim1", "10.1", null)
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(expressionVirtualColumn("v0", "\'10.1\'", ColumnType.STRING))
                .columns("__time", "v0")
                .context(queryContext)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"10.1", 946771200000L}
        )
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		13
  (expected = RelOptPlanner.CannotPlanException.class)
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testJoinOnConstantShouldFail(Map<String, Object> queryContext) throws Exception
  {
    cannotVectorize();

    final String query = "SELECT t1.dim1 from foo as t1 LEFT JOIN foo as t2 on t1.dim1 = '10.1'";

    testQuery(
        query,
        queryContext,
        ImmutableList.of(),
        ImmutableList.of()
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		37
  
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testTopNOnStringWithNonSortedOrUniqueDictionary(Map<String, Object> queryContext) throws Exception
  {
    testQuery(
        "SELECT druid.broadcast.dim4, COUNT(*)\n"
        + "FROM druid.numfoo\n"
        + "INNER JOIN druid.broadcast ON numfoo.dim4 = broadcast.dim4\n"
        + "GROUP BY 1 ORDER BY 2 LIMIT 4",
        queryContext,
        ImmutableList.of(
            new TopNQueryBuilder()
                .dataSource(
                    join(
                        new TableDataSource(CalciteTests.DATASOURCE3),
                        new GlobalTableDataSource(CalciteTests.BROADCAST_DATASOURCE),
                        "j0.",
                        equalsCondition(
                            DruidExpression.fromColumn("dim4"),
                            DruidExpression.fromColumn("j0.dim4")
                        ),
                        JoinType.INNER

                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .dimension(new DefaultDimensionSpec("j0.dim4", "_d0", ColumnType.STRING))
                .threshold(4)
                .aggregators(aggregators(new CountAggregatorFactory("a0")))
                .context(queryContext)
                .metric(new InvertedTopNMetricSpec(new NumericTopNMetricSpec("a0")))
                .build()
        ),
        ImmutableList.of(
            new Object[]{"a", 9L},
            new Object[]{"b", 9L}
        )
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		38
  
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testTopNOnStringWithNonSortedOrUniqueDictionaryOrderByDim(Map<String, Object> queryContext)
      throws Exception
  {
    testQuery(
        "SELECT druid.broadcast.dim4, COUNT(*)\n"
        + "FROM druid.numfoo\n"
        + "INNER JOIN druid.broadcast ON numfoo.dim4 = broadcast.dim4\n"
        + "GROUP BY 1 ORDER BY 1 DESC LIMIT 4",
        queryContext,
        ImmutableList.of(
            new TopNQueryBuilder()
                .dataSource(
                    join(
                        new TableDataSource(CalciteTests.DATASOURCE3),
                        new GlobalTableDataSource(CalciteTests.BROADCAST_DATASOURCE),
                        "j0.",
                        equalsCondition(
                            DruidExpression.fromColumn("dim4"),
                            DruidExpression.fromColumn("j0.dim4")
                        ),
                        JoinType.INNER
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .dimension(new DefaultDimensionSpec("j0.dim4", "_d0", ColumnType.STRING))
                .threshold(4)
                .aggregators(aggregators(new CountAggregatorFactory("a0")))
                .context(queryContext)
                .metric(new InvertedTopNMetricSpec(new DimensionTopNMetricSpec(null, StringComparators.LEXICOGRAPHIC)))
                .build()
        ),
        ImmutableList.of(
            new Object[]{"b", 9L},
            new Object[]{"a", 9L}
        )
     );
   }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 2
Assertions		 0
Lines of Code		83
  
  public void testLeftJoinRightTableCanBeEmpty() throws Exception
  {
    // HashJoinSegmentStorageAdapter is not vectorizable
    cannotVectorize();
 
    final DataSource rightTable;
    if (useDefault) {
      rightTable = InlineDataSource.fromIterable(
          ImmutableList.of(),
          RowSignature.builder().add("dim2", ColumnType.STRING).add("m2", ColumnType.DOUBLE).build()
      );
    } else {
      rightTable = new QueryDataSource(
          Druids.newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .resultFormat(ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .filters(new SelectorDimFilter("m2", null, null))
                .columns("dim2")
                .legacy(false)
                .build()
      );
    }

    testQuery(
        "SELECT v1.dim2, count(1) "
        + "FROM (SELECT * FROM foo where m1 > 2) v1 "
        + "LEFT OUTER JOIN ("
        + "  select dim2 from (select * from foo where m2 is null)"
        + ") sm ON v1.dim2 = sm.dim2 "
        + "group by 1",
        ImmutableList.of(
            new Builder()
                .setDataSource(
                    JoinDataSource.create(
                        new QueryDataSource(
                            Druids.newScanQueryBuilder()
                                  .dataSource(CalciteTests.DATASOURCE1)
                                  .intervals(querySegmentSpec(Filtration.eternity()))
                                  .resultFormat(ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                  .filters(new BoundDimFilter(
                                      "m1",
                                      "2",
                                      null,
                                      true,
                                      false,
                                      null,
                                      null,
                                      StringComparators.NUMERIC
                                  ))
                                  .columns("dim2")
                                  .legacy(false)
                                  .build()
                        ),
                        rightTable,
                        "j0.",
                        "(\"dim2\" == \"j0.dim2\")",
                        JoinType.LEFT,
                        null,
                        ExprMacroTable.nil()
                    )
                )
                .setInterval(querySegmentSpec(Filtration.eternity()))
                .setGranularity(Granularities.ALL)
                .setDimensions(
                    new DefaultDimensionSpec("dim2", "d0", ColumnType.STRING)
                )
                .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                .setContext(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        useDefault
        ? ImmutableList.of(
            new Object[]{"", 2L},
            new Object[]{"a", 1L},
            new Object[]{"abc", 1L}
        )
        : ImmutableList.of(
            new Object[]{null, 1L},
            new Object[]{"", 1L},
            new Object[]{"a", 1L},
            new Object[]{"abc", 1L}
        )
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		77
  
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testLeftJoinSubqueryWithNullKeyFilter(Map<String, Object> queryContext) throws Exception
  {
    // Cannot vectorize due to 'concat' expression.
    cannotVectorize();
 
    ScanQuery nullCompatibleModePlan = newScanQueryBuilder()
        .dataSource(
            join(
                new TableDataSource(CalciteTests.DATASOURCE1),
                new QueryDataSource(
                    GroupByQuery
                        .builder()
                        .setDataSource(new LookupDataSource("lookyloo"))
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            expressionVirtualColumn("v0", "concat(\"k\",'')", ColumnType.STRING)
                        )
                        .setDimensions(new DefaultDimensionSpec("v0", "d0"))
                        .build()
                ),
                "j0.",
                equalsCondition(DruidExpression.fromColumn("dim1"), DruidExpression.fromColumn("j0.d0")),
                JoinType.INNER
            )
        )
        .intervals(querySegmentSpec(Filtration.eternity()))
        .columns("dim1", "j0.d0")
        .context(queryContext)
        .build();

    ScanQuery nonNullCompatibleModePlan = newScanQueryBuilder()
        .dataSource(
            join(
                new TableDataSource(CalciteTests.DATASOURCE1),
                new QueryDataSource(
                    GroupByQuery
                        .builder()
                        .setDataSource(new LookupDataSource("lookyloo"))
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            expressionVirtualColumn("v0", "concat(\"k\",'')", ColumnType.STRING)
                        )
                        .setDimensions(new DefaultDimensionSpec("v0", "d0"))
                        .build()
                ),
                "j0.",
                equalsCondition(DruidExpression.fromColumn("dim1"), DruidExpression.fromColumn("j0.d0")),
                JoinType.LEFT
            )
        )
        .intervals(querySegmentSpec(Filtration.eternity()))
        .columns("dim1", "j0.d0")
        .filters(new NotDimFilter(new SelectorDimFilter("j0.d0", null, null)))
        .context(queryContext)
        .build();

    boolean isJoinFilterRewriteEnabled = queryContext.getOrDefault(JOIN_FILTER_REWRITE_ENABLE_KEY, true)
                                                     .toString()
                                                     .equals("true");
    testQuery(
        "SELECT dim1, l1.k\n"
        + "FROM foo\n"
        + "LEFT JOIN (select k || '' as k from lookup.lookyloo group by 1) l1 ON foo.dim1 = l1.k\n"
        + "WHERE l1.k IS NOT NULL\n",
        queryContext,
        ImmutableList.of(NullHandling.sqlCompatible() ? nullCompatibleModePlan : nonNullCompatibleModePlan),
        NullHandling.sqlCompatible() || !isJoinFilterRewriteEnabled
        ? ImmutableList.of(new Object[]{"abc", "abc"})
        : ImmutableList.of(
            new Object[]{"10.1", ""},
            // this result is incorrect. TODO : fix this result when the JoinFilterAnalyzer bug is fixed
            new Object[]{"2", ""},
            new Object[]{"1", ""},
            new Object[]{"def", ""},
            new Object[]{"abc", "abc"}
        )
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 2
Assertions		 0
Lines of Code		47
  
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testLeftJoinSubqueryWithSelectorFilter(Map<String, Object> queryContext) throws Exception
  {
    // Cannot vectorize due to 'concat' expression.
    cannotVectorize();

    // disable the cost model where inner join is treated like a filter
    // this leads to cost(left join) < cost(converted inner join) for the below query
    queryContext = QueryContextForJoinProvider.withOverrides(
        queryContext,
        ImmutableMap.of("computeInnerJoinCostAsFilter", "false")
    );
    testQuery(
        "SELECT dim1, l1.k\n"
        + "FROM foo\n"
        + "LEFT JOIN (select k || '' as k from lookup.lookyloo group by 1) l1 ON foo.dim1 = l1.k\n"
        + "WHERE l1.k = 'abc'\n",
        queryContext,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(
                    join(
                        new TableDataSource(CalciteTests.DATASOURCE1),
                        new QueryDataSource(
                            GroupByQuery
                                .builder()
                                .setDataSource(new LookupDataSource("lookyloo"))
                                .setInterval(querySegmentSpec(Filtration.eternity()))
                                .setGranularity(Granularities.ALL)
                                .setVirtualColumns(
                                    expressionVirtualColumn("v0", "concat(\"k\",'')", ColumnType.STRING)
                                )
                                .setDimensions(new DefaultDimensionSpec("v0", "d0"))
                                .build()
                        ),
                        "j0.",
                        equalsCondition(DruidExpression.fromColumn("dim1"), DruidExpression.fromColumn("j0.d0")),
                        JoinType.LEFT
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("dim1", "j0.d0")
                .filters(selector("j0.d0", "abc", null))
                .context(queryContext)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"abc", "abc"}
        )
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		42
  
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testLeftJoinWithNotNullFilter(Map<String, Object> queryContext) throws Exception
  {
    testQuery(
        "SELECT s.dim1, t.dim1\n"
        + "FROM foo as s\n"
        + "LEFT JOIN foo as t "
        + "ON s.dim1 = t.dim1 "
        + "and s.dim1 IS NOT NULL\n",
        queryContext,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(
                    join(
                        new TableDataSource(CalciteTests.DATASOURCE1),
                        new QueryDataSource(newScanQueryBuilder()
                                                .dataSource(CalciteTests.DATASOURCE1)
                                                .intervals(querySegmentSpec(Filtration.eternity()))
                                                .columns(ImmutableList.of("dim1"))
                                                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                                .context(QUERY_CONTEXT_DEFAULT)
                                                .build()),
                        "j0.",
                        equalsCondition(DruidExpression.fromColumn("dim1"), DruidExpression.fromColumn("j0.dim1")),
                        JoinType.LEFT
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("dim1", "j0.dim1")
                .context(queryContext)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"", ""},
            new Object[]{"10.1", "10.1"},
            new Object[]{"2", "2"},
            new Object[]{"1", "1"},
            new Object[]{"def", "def"},
            new Object[]{"abc", "abc"}
        )
    );
  }
```
```
Previous Commit	b6a0fbc8b6d677b391115c4ff7467774b0a5fb7c
Directory name	sql
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		48
  
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testInnerJoinSubqueryWithSelectorFilter(Map<String, Object> queryContext) throws Exception
  {
    // Cannot vectorize due to 'concat' expression.
    cannotVectorize();

    testQuery(
        "SELECT dim1, l1.k "
        + "FROM foo INNER JOIN (select k || '' as k from lookup.lookyloo group by 1) l1 "
        + "ON foo.dim1 = l1.k and l1.k = 'abc'",
        queryContext,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(
                    join(
                        new TableDataSource(CalciteTests.DATASOURCE1),
                        new QueryDataSource(
                            GroupByQuery
                                .builder()
                                .setDataSource(new LookupDataSource("lookyloo"))
                                .setInterval(querySegmentSpec(Filtration.eternity()))
                                .setGranularity(Granularities.ALL)
                                .setVirtualColumns(
                                    expressionVirtualColumn("v0", "concat(\"k\",'')", ColumnType.STRING)
                                )
                                .setDimensions(new DefaultDimensionSpec("v0", "d0"))
                                .build()
                        ),
                        "j0.",
                        StringUtils.format(
                            "(%s && %s)",
                            equalsCondition(DruidExpression.fromColumn("dim1"), DruidExpression.fromColumn("j0.d0")),
                            equalsCondition(
                                DruidExpression.fromExpression("'abc'"),
                                DruidExpression.fromColumn("j0.d0")
                            )
                        ),
                        JoinType.INNER
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("dim1", "j0.d0")
                .context(queryContext)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"abc", "abc"}
        )
    );
  }
```
## 4046c86d62192c812cea87188dd17e745fb83b04 ##
```
Commit	4046c86d62192c812cea87188dd17e745fb83b04
Directory name		core
Cyclomatic Complexity	 2
Assertions		 1
Lines of Code		25
  
  public void testBucketMonthComparator()
  {
    DataSegment[] sortedOrder = {
        makeDataSegment("test1", "2011-01-01/2011-01-02", "a"),
        makeDataSegment("test1", "2011-01-02/2011-01-03", "a"),
        makeDataSegment("test1", "2011-01-02/2011-01-03", "b"),
        makeDataSegment("test2", "2011-01-01/2011-01-02", "a"),
        makeDataSegment("test2", "2011-01-02/2011-01-03", "a"),
        makeDataSegment("test1", "2011-02-01/2011-02-02", "a"),
        makeDataSegment("test1", "2011-02-02/2011-02-03", "a"),
        makeDataSegment("test1", "2011-02-02/2011-02-03", "b"),
        makeDataSegment("test2", "2011-02-01/2011-02-02", "a"),
        makeDataSegment("test2", "2011-02-02/2011-02-03", "a"),
    };

    List<DataSegment> shuffled = new ArrayList<>(Arrays.asList(sortedOrder));
    Collections.shuffle(shuffled);

    Set<DataSegment> theSet = new TreeSet<>(DataSegment.bucketMonthComparator());
    theSet.addAll(shuffled);

    int index = 0;
    for (DataSegment dataSegment : theSet) {
      Assert.assertEquals(sortedOrder[index], dataSegment);
      ++index;
    }
  }
```
```
Previous Commit	4046c86d62192c812cea87188dd17e745fb83b04
Directory name	indexing-service
Cyclomatic Complexity	 2
Assertions		 0
Lines of Code		18
  
  public void testHugeTargetCompactionSize()
  {
    final PartitionConfigurationManager manager = new PartitionConfigurationManager(Long.MAX_VALUE, TUNING_CONFIG);
    final TestIndexIO indexIO = (TestIndexIO) toolbox.getIndexIO();
    final Map<File, QueryableIndex> queryableIndexMap = indexIO.getQueryableIndexMap();
    final List<Pair<QueryableIndex, DataSegment>> segments = new ArrayList<>();

    for (Entry<DataSegment, File> entry : SEGMENT_MAP.entrySet()) {
      final DataSegment segment = entry.getKey();
      final File file = entry.getValue();
      segments.add(Pair.of(Preconditions.checkNotNull(queryableIndexMap.get(file)), segment));
    }

    expectedException.expect(ArithmeticException.class);
    expectedException.expectMessage(
        CoreMatchers.startsWith("Estimated maxRowsPerSegment[922337203685477632] is out of integer value range.")
    );
    manager.computeTuningConfig(segments);
   }
```
```
Previous Commit	4046c86d62192c812cea87188dd17e745fb83b04
Directory name	server
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		17
  
  public void testSerdeTargetCompactionSizeBytesWithMaxRowsPerSegment()
  {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage(
        "targetCompactionSizeBytes[10000] cannot be used with maxRowsPerSegment[1000] and maxTotalRows[null]"
    );
    final DataSourceCompactionConfig config = new DataSourceCompactionConfig(
        "dataSource",
        null,
        500L,
        10000L,
        1000,
        new Period(3600),
        null,
        ImmutableMap.of("key", "val")
    );
  }
```
```
Previous Commit	4046c86d62192c812cea87188dd17e745fb83b04
Directory name	server
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		24
  
  public void testSerdeTargetCompactionSizeBytesWithMaxTotalRows()
  {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage(
        "targetCompactionSizeBytes[10000] cannot be used with maxRowsPerSegment[null] and maxTotalRows[10000]"
    );
    final DataSourceCompactionConfig config = new DataSourceCompactionConfig(
        "dataSource",
        null,
        500L,
        10000L,
        null,
        new Period(3600),
        new UserCompactTuningConfig(
            null,
            null,
            10000L,
            null,
            null,
            null
        ),
        ImmutableMap.of("key", "val")
    );
   }
```
## 05d58689ad4617b4b8008299be0876ee60f30df7 ##
```
Commit	05d58689ad4617b4b8008299be0876ee60f30df7
Directory name		processing
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		13
  
  public void testSimpleAppend() throws IOException
  {
    final IndexableAdapter adapter = new QueryableIndexIndexableAdapter(
        closer.closeLater(
            INDEX_IO.loadIndex(
                persistTmpDir
            )
        )
    );
    Assert.assertEquals(events.size(), adapter.getNumRows());
    appendAndValidate(persistTmpDir, new File(tmpDir, "reprocessed"));
  }
```
```
Previous Commit	05d58689ad4617b4b8008299be0876ee60f30df7
Directory name	processing
Cyclomatic Complexity	 1
Assertions		 3
Lines of Code		22
  
  public void testIdempotentAppend() throws IOException
  {
    final IndexableAdapter adapter = new QueryableIndexIndexableAdapter(
        closer.closeLater(
            INDEX_IO.loadIndex(
                persistTmpDir
            )
        )
    );
    Assert.assertEquals(events.size(), adapter.getNumRows());
    final File tmpDir1 = new File(tmpDir, "reprocessed1");
    appendAndValidate(persistTmpDir, tmpDir1);

    final File tmpDir2 = new File(tmpDir, "reprocessed2");
    final IndexableAdapter adapter2 = new QueryableIndexIndexableAdapter(closer.closeLater(INDEX_IO.loadIndex(tmpDir1)));
    Assert.assertEquals(events.size(), adapter2.getNumRows());
    appendAndValidate(tmpDir1, tmpDir2);

    final File tmpDir3 = new File(tmpDir, "reprocessed3");
    final IndexableAdapter adapter3 = new QueryableIndexIndexableAdapter(closer.closeLater(INDEX_IO.loadIndex(tmpDir2)));
    Assert.assertEquals(events.size(), adapter3.getNumRows());
    appendAndValidate(tmpDir2, tmpDir3);
  }
```
