## f06248ef84f01831b1709842a1067a1386532940 ##
```
Cyclomatic Complexity	1
Assertions		4
Lines of Code		14
-  
  public void testErrorNotGzipped() throws Exception {
    Header[] headers = new Header[2];
    headers[0] = new Header("Accept", Constants.MIMETYPE_BINARY);
    headers[1] = new Header("Accept-Encoding", "gzip");
    Response response = client.get("/" + TABLE + "/" + ROW_1 + "/" + COLUMN_2, headers);
    assertEquals(404, response.getCode());
    String contentEncoding = response.getHeader("Content-Encoding");
    assertTrue(contentEncoding == null || !contentEncoding.contains("gzip"));
    response = client.get("/" + TABLE, headers);
    assertEquals(405, response.getCode());
    contentEncoding = response.getHeader("Content-Encoding");
    assertTrue(contentEncoding == null || !contentEncoding.contains("gzip"));
  }
```
```
Cyclomatic Complexity	 1
Assertions		 6
Lines of Code		11
-  
  public void testValueOfNamespaceAndQualifier() {
    TableName name0 = TableName.valueOf("table");
    TableName name1 = TableName.valueOf("table", "table");
    assertEquals(NamespaceDescriptor.DEFAULT_NAMESPACE_NAME_STR, name0.getNamespaceAsString());
    assertEquals("table", name0.getQualifierAsString());
    assertEquals("table", name0.getNameAsString());
    assertEquals("table", name1.getNamespaceAsString());
    assertEquals("table", name1.getQualifierAsString());
    assertEquals("table:table", name1.getNameAsString());
  }
```
```
Cyclomatic Complexity	 1
Assertions		 12
Lines of Code		186
-  
  public void testVersions() throws Exception {
    byte [] TABLE = Bytes.toBytes("testVersions");@@@+      // Verify we can get each one properly@@@+      getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);@@@+      getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);@@@+      getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);@@@+      getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[7], VALUES[7]);@@@+      scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);@@@+      scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);@@@+      scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);@@@+      scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[7], VALUES[7]);@@@ 
    long [] STAMPS = makeStamps(20);
    byte [][] VALUES = makeNAscii(VALUE, 20);@@@+      // Verify we don't accidentally get others@@@+      getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[0]);@@@+      getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[9]);@@@+      scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[0]);@@@+      scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[9]);@@@ 
    Table ht = TEST_UTIL.createTable(TABLE, FAMILY, 10);
    TEST_UTIL.waitTableAvailable(TABLE, 10000);
    // Insert 4 versions of same column
    Put put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    put.add(FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    put.add(FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    put.add(FAMILY, QUALIFIER, STAMPS[5], VALUES[5]);
    ht.put(put);@@@+      // Ensure maxVersions of table is respected@@@ 
    // Verify we can get each one properly
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[5], VALUES[5]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[5], VALUES[5]);@@@+      TEST_UTIL.flush();@@@ 
    // Verify we don't accidentally get others
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[0]);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[3]);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[6]);
    scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[0]);
    scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[3]);
    scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[6]);

    // Ensure maxVersions in query is respected
    Get get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIER);
    get.setMaxVersions(2);
    Result result = ht.get(get);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[4], STAMPS[5]},
        new byte[][] {VALUES[4], VALUES[5]},
        0, 1);

    Scan scan = new Scan(ROW);
    scan.addColumn(FAMILY, QUALIFIER);
    scan.setMaxVersions(2);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[4], STAMPS[5]},
        new byte[][] {VALUES[4], VALUES[5]},
        0, 1);

    // Flush and redo

    TEST_UTIL.flush();

    // Verify we can get each one properly
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[5], VALUES[5]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[5], VALUES[5]);

    // Verify we don't accidentally get others
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[0]);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[3]);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[6]);
    scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[0]);
    scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[3]);
    scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[6]);

    // Ensure maxVersions in query is respected
    get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIER);
    get.setMaxVersions(2);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[4], STAMPS[5]},
        new byte[][] {VALUES[4], VALUES[5]},
        0, 1);

    scan = new Scan(ROW);
    scan.addColumn(FAMILY, QUALIFIER);
    scan.setMaxVersions(2);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[4], STAMPS[5]},
        new byte[][] {VALUES[4], VALUES[5]},
        0, 1);


    // Add some memstore and retest

    // Insert 4 more versions of same column and a dupe
    put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, STAMPS[3], VALUES[3]);
    put.add(FAMILY, QUALIFIER, STAMPS[6], VALUES[6]);
    put.add(FAMILY, QUALIFIER, STAMPS[7], VALUES[7]);
    put.add(FAMILY, QUALIFIER, STAMPS[8], VALUES[8]);
    ht.put(put);

    // Ensure maxVersions in query is respected
    get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIER);
    get.setMaxVersions();
    result = ht.get(get);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[1], STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7], STAMPS[8]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3], VALUES[4], VALUES[5], VALUES[6], VALUES[7], VALUES[8]},
        0, 7);

    scan = new Scan(ROW);
    scan.addColumn(FAMILY, QUALIFIER);
    scan.setMaxVersions();
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[1], STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7], STAMPS[8]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3], VALUES[4], VALUES[5], VALUES[6], VALUES[7], VALUES[8]},
        0, 7);

    get = new Get(ROW);
    get.setMaxVersions();
    result = ht.get(get);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[1], STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7], STAMPS[8]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3], VALUES[4], VALUES[5], VALUES[6], VALUES[7], VALUES[8]},
        0, 7);

    scan = new Scan(ROW);
    scan.setMaxVersions();
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[1], STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7], STAMPS[8]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3], VALUES[4], VALUES[5], VALUES[6], VALUES[7], VALUES[8]},
        0, 7);

    // Verify we can get each one properly
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[7], VALUES[7]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[7], VALUES[7]);

    // Verify we don't accidentally get others
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[0]);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[9]);
    scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[0]);
    scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[9]);

    // Ensure maxVersions of table is respected

    TEST_UTIL.flush();

    // Insert 4 more versions of same column and a dupe
    put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, STAMPS[9], VALUES[9]);
    put.add(FAMILY, QUALIFIER, STAMPS[11], VALUES[11]);
    put.add(FAMILY, QUALIFIER, STAMPS[13], VALUES[13]);
    put.add(FAMILY, QUALIFIER, STAMPS[15], VALUES[15]);
    ht.put(put);

    get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIER);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7], STAMPS[8], STAMPS[9], STAMPS[11], STAMPS[13], STAMPS[15]},
        new byte[][] {VALUES[3], VALUES[4], VALUES[5], VALUES[6], VALUES[7], VALUES[8], VALUES[9], VALUES[11], VALUES[13], VALUES[15]},
        0, 9);

    scan = new Scan(ROW);
    scan.addColumn(FAMILY, QUALIFIER);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7], STAMPS[8], STAMPS[9], STAMPS[11], STAMPS[13], STAMPS[15]},
        new byte[][] {VALUES[3], VALUES[4], VALUES[5], VALUES[6], VALUES[7], VALUES[8], VALUES[9], VALUES[11], VALUES[13], VALUES[15]},
        0, 9);

    // Delete a version in the memstore and a version in a storefile
    Delete delete = new Delete(ROW);
    delete.deleteColumn(FAMILY, QUALIFIER, STAMPS[11]);
    delete.deleteColumn(FAMILY, QUALIFIER, STAMPS[7]);
    ht.delete(delete);

    // Test that it's gone
    get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIER);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[1], STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[8], STAMPS[9], STAMPS[13], STAMPS[15]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3], VALUES[4], VALUES[5], VALUES[6], VALUES[8], VALUES[9], VALUES[13], VALUES[15]},
        0, 9);

    scan = new Scan(ROW);
    scan.addColumn(FAMILY, QUALIFIER);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[1], STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[8], STAMPS[9], STAMPS[13], STAMPS[15]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3], VALUES[4], VALUES[5], VALUES[6], VALUES[8], VALUES[9], VALUES[13], VALUES[15]},
        0, 9);

  }
```
```
Cyclomatic Complexity	 1
Assertions		 18
Lines of Code		163
-  
  public void testVersionLimits() throws Exception {
    byte [] TABLE = Bytes.toBytes("testVersionLimits");
    byte [][] FAMILIES = makeNAscii(FAMILY, 3);
    int [] LIMITS = {1,3,5};
    long [] STAMPS = makeStamps(10);
    byte [][] VALUES = makeNAscii(VALUE, 10);
    Table ht = TEST_UTIL.createTable(TABLE, FAMILIES, LIMITS);
    TEST_UTIL.waitTableAvailable(TABLE, 10000);
    // Insert limit + 1 on each family
    Put put = new Put(ROW);
    put.add(FAMILIES[0], QUALIFIER, STAMPS[0], VALUES[0]);
    put.add(FAMILIES[0], QUALIFIER, STAMPS[1], VALUES[1]);
    put.add(FAMILIES[1], QUALIFIER, STAMPS[0], VALUES[0]);
    put.add(FAMILIES[1], QUALIFIER, STAMPS[1], VALUES[1]);
    put.add(FAMILIES[1], QUALIFIER, STAMPS[2], VALUES[2]);
    put.add(FAMILIES[1], QUALIFIER, STAMPS[3], VALUES[3]);
    put.add(FAMILIES[2], QUALIFIER, STAMPS[0], VALUES[0]);
    put.add(FAMILIES[2], QUALIFIER, STAMPS[1], VALUES[1]);
    put.add(FAMILIES[2], QUALIFIER, STAMPS[2], VALUES[2]);
    put.add(FAMILIES[2], QUALIFIER, STAMPS[3], VALUES[3]);
    put.add(FAMILIES[2], QUALIFIER, STAMPS[4], VALUES[4]);
    put.add(FAMILIES[2], QUALIFIER, STAMPS[5], VALUES[5]);
    put.add(FAMILIES[2], QUALIFIER, STAMPS[6], VALUES[6]);
    ht.put(put);

    // Verify we only get the right number out of each

    // Family0

    Get get = new Get(ROW);
    get.addColumn(FAMILIES[0], QUALIFIER);
    get.setMaxVersions(Integer.MAX_VALUE);
    Result result = ht.get(get);
    assertNResult(result, ROW, FAMILIES[0], QUALIFIER,
        new long [] {STAMPS[1]},
        new byte[][] {VALUES[1]},
        0, 0);

    get = new Get(ROW);
    get.addFamily(FAMILIES[0]);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILIES[0], QUALIFIER,
        new long [] {STAMPS[1]},
        new byte[][] {VALUES[1]},
        0, 0);

    Scan scan = new Scan(ROW);
    scan.addColumn(FAMILIES[0], QUALIFIER);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILIES[0], QUALIFIER,
        new long [] {STAMPS[1]},
        new byte[][] {VALUES[1]},
        0, 0);

    scan = new Scan(ROW);
    scan.addFamily(FAMILIES[0]);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILIES[0], QUALIFIER,
        new long [] {STAMPS[1]},
        new byte[][] {VALUES[1]},
        0, 0);

    // Family1

    get = new Get(ROW);
    get.addColumn(FAMILIES[1], QUALIFIER);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILIES[1], QUALIFIER,
        new long [] {STAMPS[1], STAMPS[2], STAMPS[3]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3]},
        0, 2);

    get = new Get(ROW);
    get.addFamily(FAMILIES[1]);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILIES[1], QUALIFIER,
        new long [] {STAMPS[1], STAMPS[2], STAMPS[3]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3]},
        0, 2);

    scan = new Scan(ROW);
    scan.addColumn(FAMILIES[1], QUALIFIER);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILIES[1], QUALIFIER,
        new long [] {STAMPS[1], STAMPS[2], STAMPS[3]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3]},
        0, 2);

    scan = new Scan(ROW);
    scan.addFamily(FAMILIES[1]);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILIES[1], QUALIFIER,
        new long [] {STAMPS[1], STAMPS[2], STAMPS[3]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3]},
        0, 2);

    // Family2

    get = new Get(ROW);
    get.addColumn(FAMILIES[2], QUALIFIER);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILIES[2], QUALIFIER,
        new long [] {STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6]},
        new byte[][] {VALUES[2], VALUES[3], VALUES[4], VALUES[5], VALUES[6]},
        0, 4);

    get = new Get(ROW);
    get.addFamily(FAMILIES[2]);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILIES[2], QUALIFIER,
        new long [] {STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6]},
        new byte[][] {VALUES[2], VALUES[3], VALUES[4], VALUES[5], VALUES[6]},
        0, 4);

    scan = new Scan(ROW);
    scan.addColumn(FAMILIES[2], QUALIFIER);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILIES[2], QUALIFIER,
        new long [] {STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6]},
        new byte[][] {VALUES[2], VALUES[3], VALUES[4], VALUES[5], VALUES[6]},
        0, 4);

    scan = new Scan(ROW);
    scan.addFamily(FAMILIES[2]);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILIES[2], QUALIFIER,
        new long [] {STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6]},
        new byte[][] {VALUES[2], VALUES[3], VALUES[4], VALUES[5], VALUES[6]},
        0, 4);

    // Try all families

    get = new Get(ROW);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertTrue("Expected 9 keys but received " + result.size(),
        result.size() == 9);

    get = new Get(ROW);
    get.addFamily(FAMILIES[0]);
    get.addFamily(FAMILIES[1]);
    get.addFamily(FAMILIES[2]);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertTrue("Expected 9 keys but received " + result.size(),
        result.size() == 9);

    get = new Get(ROW);
    get.addColumn(FAMILIES[0], QUALIFIER);
    get.addColumn(FAMILIES[1], QUALIFIER);
    get.addColumn(FAMILIES[2], QUALIFIER);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertTrue("Expected 9 keys but received " + result.size(),
        result.size() == 9);

    scan = new Scan(ROW);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertTrue("Expected 9 keys but received " + result.size(),
        result.size() == 9);

    scan = new Scan(ROW);
    scan.setMaxVersions(Integer.MAX_VALUE);
    scan.addFamily(FAMILIES[0]);
    scan.addFamily(FAMILIES[1]);
    scan.addFamily(FAMILIES[2]);
    result = getSingleScanResult(ht, scan);
    assertTrue("Expected 9 keys but received " + result.size(),
        result.size() == 9);

    scan = new Scan(ROW);
    scan.setMaxVersions(Integer.MAX_VALUE);
    scan.addColumn(FAMILIES[0], QUALIFIER);
    scan.addColumn(FAMILIES[1], QUALIFIER);
    scan.addColumn(FAMILIES[2], QUALIFIER);
    result = getSingleScanResult(ht, scan);
    assertTrue("Expected 9 keys but received " + result.size(),
        result.size() == 9);

  }
```
```
Cyclomatic Complexity	 5
Assertions		 2
Lines of Code		33
-  
  public void testDeleteFamilyVersion() throws Exception {
    HBaseAdmin admin = new HBaseAdmin(TEST_UTIL.getConfiguration());
    byte [] TABLE = Bytes.toBytes("testDeleteFamilyVersion");

    byte [][] QUALIFIERS = makeNAscii(QUALIFIER, 1);
    byte [][] VALUES = makeN(VALUE, 5);
    long [] ts = {1000, 2000, 3000, 4000, 5000};

    Table ht = TEST_UTIL.createTable(TABLE, FAMILY, 5);
    TEST_UTIL.waitTableAvailable(TABLE, 10000);

    Put put = new Put(ROW);
    for (int q = 0; q < 1; q++)
      for (int t = 0; t < 5; t++)
        put.add(FAMILY, QUALIFIERS[q], ts[t], VALUES[t]);
    ht.put(put);
    admin.flush(TABLE);

    Delete delete = new Delete(ROW);
    delete.deleteFamilyVersion(FAMILY, ts[1]);  // delete version '2000'
    delete.deleteFamilyVersion(FAMILY, ts[3]);  // delete version '4000'
    ht.delete(delete);
    admin.flush(TABLE);

    for (int i = 0; i < 1; i++) {
      Get get = new Get(ROW);
      get.addColumn(FAMILY, QUALIFIERS[i]);
      get.setMaxVersions(Integer.MAX_VALUE);
      Result result = ht.get(get);
      // verify version '1000'/'3000'/'5000' remains for all columns
      assertNResult(result, ROW, FAMILY, QUALIFIERS[i],
          new long [] {ts[0], ts[2], ts[4]},
          new byte[][] {VALUES[0], VALUES[2], VALUES[4]},
          0, 2);
    }
    ht.close();
    admin.close();
  }
```
```
Cyclomatic Complexity	 7
Assertions		 7
Lines of Code		89
-  
  public void testDeleteFamilyVersionWithOtherDeletes() throws Exception {
    byte [] TABLE = Bytes.toBytes("testDeleteFamilyVersionWithOtherDeletes");

    byte [][] QUALIFIERS = makeNAscii(QUALIFIER, 5);
    byte [][] VALUES = makeN(VALUE, 5);
    long [] ts = {1000, 2000, 3000, 4000, 5000};

    HBaseAdmin admin = new HBaseAdmin(TEST_UTIL.getConfiguration());
    Table ht = TEST_UTIL.createTable(TABLE, FAMILY, 5);
    TEST_UTIL.waitTableAvailable(TABLE, 10000);

    Put put = null;
    Result result = null;
    Get get = null;
    Delete delete = null;

    // 1. put on ROW
    put = new Put(ROW);
    for (int q = 0; q < 5; q++)
      for (int t = 0; t < 5; t++)
        put.add(FAMILY, QUALIFIERS[q], ts[t], VALUES[t]);
    ht.put(put);
    admin.flush(TABLE);

    // 2. put on ROWS[0]
    byte [] ROW2 = Bytes.toBytes("myRowForTest");
    put = new Put(ROW2);
    for (int q = 0; q < 5; q++)
      for (int t = 0; t < 5; t++)
        put.add(FAMILY, QUALIFIERS[q], ts[t], VALUES[t]);
    ht.put(put);
    admin.flush(TABLE);

    // 3. delete on ROW
    delete = new Delete(ROW);
    // delete version <= 2000 of all columns
    // note: deleteFamily must be the first since it will mask
    // the subsequent other type deletes!
    delete.deleteFamily(FAMILY, ts[1]);
    // delete version '4000' of all columns
    delete.deleteFamilyVersion(FAMILY, ts[3]);
   // delete version <= 3000 of column 0
    delete.deleteColumns(FAMILY, QUALIFIERS[0], ts[2]);
    // delete version <= 5000 of column 2
    delete.deleteColumns(FAMILY, QUALIFIERS[2], ts[4]);
    // delete version 5000 of column 4
    delete.deleteColumn(FAMILY, QUALIFIERS[4], ts[4]);
    ht.delete(delete);
    admin.flush(TABLE);

     // 4. delete on ROWS[0]
    delete = new Delete(ROW2);
    delete.deleteFamilyVersion(FAMILY, ts[1]);  // delete version '2000'
    delete.deleteFamilyVersion(FAMILY, ts[3]);  // delete version '4000'
    ht.delete(delete);
    admin.flush(TABLE);

    // 5. check ROW
    get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIERS[0]);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILY, QUALIFIERS[0],
        new long [] {ts[4]},
        new byte[][] {VALUES[4]},
        0, 0);

    get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIERS[1]);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILY, QUALIFIERS[1],
        new long [] {ts[2], ts[4]},
        new byte[][] {VALUES[2], VALUES[4]},
        0, 1);

    get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIERS[2]);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertEquals(0, result.size());

    get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIERS[3]);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILY, QUALIFIERS[3],
        new long [] {ts[2], ts[4]},
        new byte[][] {VALUES[2], VALUES[4]},
        0, 1);

    get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIERS[4]);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILY, QUALIFIERS[4],
        new long [] {ts[2]},
        new byte[][] {VALUES[2]},
        0, 0);

    // 6. check ROWS[0]
    for (int i = 0; i < 5; i++) {
      get = new Get(ROW2);
      get.addColumn(FAMILY, QUALIFIERS[i]);
      get.setMaxVersions(Integer.MAX_VALUE);
      result = ht.get(get);
      // verify version '1000'/'3000'/'5000' remains for all columns
      assertNResult(result, ROW2, FAMILY, QUALIFIERS[i],
          new long [] {ts[0], ts[2], ts[4]},
          new byte[][] {VALUES[0], VALUES[2], VALUES[4]},
          0, 2);
    }
    ht.close();
    admin.close();
  }
```
```
Cyclomatic Complexity	 10
Assertions		 29
Lines of Code		259
-  
  public void testDeletes() throws Exception {
    byte [] TABLE = Bytes.toBytes("testDeletes");

    byte [][] ROWS = makeNAscii(ROW, 6);
    byte [][] FAMILIES = makeNAscii(FAMILY, 3);
    byte [][] VALUES = makeN(VALUE, 5);
    long [] ts = {1000, 2000, 3000, 4000, 5000};

    Table ht = TEST_UTIL.createTable(TABLE, FAMILIES, 3);
    TEST_UTIL.waitTableAvailable(TABLE, 10000);

    Put put = new Put(ROW);
    put.add(FAMILIES[0], QUALIFIER, ts[0], VALUES[0]);
    put.add(FAMILIES[0], QUALIFIER, ts[1], VALUES[1]);
    ht.put(put);

    Delete delete = new Delete(ROW);
    delete.deleteFamily(FAMILIES[0], ts[0]);
    ht.delete(delete);

    Get get = new Get(ROW);
    get.addFamily(FAMILIES[0]);
    get.setMaxVersions(Integer.MAX_VALUE);
    Result result = ht.get(get);
    assertNResult(result, ROW, FAMILIES[0], QUALIFIER,
        new long [] {ts[1]},
        new byte[][] {VALUES[1]},
        0, 0);

    Scan scan = new Scan(ROW);
    scan.addFamily(FAMILIES[0]);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILIES[0], QUALIFIER,
        new long [] {ts[1]},
        new byte[][] {VALUES[1]},
        0, 0);

    // Test delete latest version
    put = new Put(ROW);
    put.add(FAMILIES[0], QUALIFIER, ts[4], VALUES[4]);
    put.add(FAMILIES[0], QUALIFIER, ts[2], VALUES[2]);
    put.add(FAMILIES[0], QUALIFIER, ts[3], VALUES[3]);
    put.add(FAMILIES[0], null, ts[4], VALUES[4]);
    put.add(FAMILIES[0], null, ts[2], VALUES[2]);
    put.add(FAMILIES[0], null, ts[3], VALUES[3]);
    ht.put(put);

    delete = new Delete(ROW);
    delete.deleteColumn(FAMILIES[0], QUALIFIER); // ts[4]
    ht.delete(delete);

    get = new Get(ROW);
    get.addColumn(FAMILIES[0], QUALIFIER);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILIES[0], QUALIFIER,
        new long [] {ts[1], ts[2], ts[3]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3]},
        0, 2);

    scan = new Scan(ROW);
    scan.addColumn(FAMILIES[0], QUALIFIER);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILIES[0], QUALIFIER,
        new long [] {ts[1], ts[2], ts[3]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3]},
        0, 2);

    // Test for HBASE-1847
    delete = new Delete(ROW);
    delete.deleteColumn(FAMILIES[0], null);
    ht.delete(delete);

    // Cleanup null qualifier
    delete = new Delete(ROW);
    delete.deleteColumns(FAMILIES[0], null);
    ht.delete(delete);

    // Expected client behavior might be that you can re-put deleted values
    // But alas, this is not to be.  We can't put them back in either case.

    put = new Put(ROW);
    put.add(FAMILIES[0], QUALIFIER, ts[0], VALUES[0]); // 1000
    put.add(FAMILIES[0], QUALIFIER, ts[4], VALUES[4]); // 5000
    ht.put(put);


    // It used to be due to the internal implementation of Get, that
    // the Get() call would return ts[4] UNLIKE the Scan below. With
    // the switch to using Scan for Get this is no longer the case.
    get = new Get(ROW);
    get.addFamily(FAMILIES[0]);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILIES[0], QUALIFIER,
        new long [] {ts[1], ts[2], ts[3]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3]},
        0, 2);

    // The Scanner returns the previous values, the expected-naive-unexpected behavior

    scan = new Scan(ROW);
    scan.addFamily(FAMILIES[0]);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILIES[0], QUALIFIER,
        new long [] {ts[1], ts[2], ts[3]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3]},
        0, 2);

    // Test deleting an entire family from one row but not the other various ways

    put = new Put(ROWS[0]);
    put.add(FAMILIES[1], QUALIFIER, ts[0], VALUES[0]);
    put.add(FAMILIES[1], QUALIFIER, ts[1], VALUES[1]);
    put.add(FAMILIES[2], QUALIFIER, ts[2], VALUES[2]);
    put.add(FAMILIES[2], QUALIFIER, ts[3], VALUES[3]);
    ht.put(put);

    put = new Put(ROWS[1]);
    put.add(FAMILIES[1], QUALIFIER, ts[0], VALUES[0]);
    put.add(FAMILIES[1], QUALIFIER, ts[1], VALUES[1]);
    put.add(FAMILIES[2], QUALIFIER, ts[2], VALUES[2]);
    put.add(FAMILIES[2], QUALIFIER, ts[3], VALUES[3]);
    ht.put(put);

    put = new Put(ROWS[2]);
    put.add(FAMILIES[1], QUALIFIER, ts[0], VALUES[0]);
    put.add(FAMILIES[1], QUALIFIER, ts[1], VALUES[1]);
    put.add(FAMILIES[2], QUALIFIER, ts[2], VALUES[2]);
    put.add(FAMILIES[2], QUALIFIER, ts[3], VALUES[3]);
    ht.put(put);

    // Assert that above went in.
    get = new Get(ROWS[2]);
    get.addFamily(FAMILIES[1]);
    get.addFamily(FAMILIES[2]);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertTrue("Expected 4 key but received " + result.size() + ": " + result,
        result.size() == 4);

    delete = new Delete(ROWS[0]);
    delete.deleteFamily(FAMILIES[2]);
    ht.delete(delete);

    delete = new Delete(ROWS[1]);
    delete.deleteColumns(FAMILIES[1], QUALIFIER);
    ht.delete(delete);

    delete = new Delete(ROWS[2]);
    delete.deleteColumn(FAMILIES[1], QUALIFIER);
    delete.deleteColumn(FAMILIES[1], QUALIFIER);
    delete.deleteColumn(FAMILIES[2], QUALIFIER);
    ht.delete(delete);

    get = new Get(ROWS[0]);
    get.addFamily(FAMILIES[1]);
    get.addFamily(FAMILIES[2]);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertTrue("Expected 2 keys but received " + result.size(),
        result.size() == 2);
    assertNResult(result, ROWS[0], FAMILIES[1], QUALIFIER,
        new long [] {ts[0], ts[1]},
        new byte[][] {VALUES[0], VALUES[1]},
        0, 1);

    scan = new Scan(ROWS[0]);
    scan.addFamily(FAMILIES[1]);
    scan.addFamily(FAMILIES[2]);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertTrue("Expected 2 keys but received " + result.size(),
        result.size() == 2);
    assertNResult(result, ROWS[0], FAMILIES[1], QUALIFIER,
        new long [] {ts[0], ts[1]},
        new byte[][] {VALUES[0], VALUES[1]},
        0, 1);

    get = new Get(ROWS[1]);
    get.addFamily(FAMILIES[1]);
    get.addFamily(FAMILIES[2]);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertTrue("Expected 2 keys but received " + result.size(),
        result.size() == 2);

    scan = new Scan(ROWS[1]);
    scan.addFamily(FAMILIES[1]);
    scan.addFamily(FAMILIES[2]);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertTrue("Expected 2 keys but received " + result.size(),
        result.size() == 2);

    get = new Get(ROWS[2]);
    get.addFamily(FAMILIES[1]);
    get.addFamily(FAMILIES[2]);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertEquals(1, result.size());
    assertNResult(result, ROWS[2], FAMILIES[2], QUALIFIER,
        new long [] {ts[2]},
        new byte[][] {VALUES[2]},
        0, 0);

    scan = new Scan(ROWS[2]);
    scan.addFamily(FAMILIES[1]);
    scan.addFamily(FAMILIES[2]);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertEquals(1, result.size());
    assertNResult(result, ROWS[2], FAMILIES[2], QUALIFIER,
        new long [] {ts[2]},
        new byte[][] {VALUES[2]},
        0, 0);

    // Test if we delete the family first in one row (HBASE-1541)

    delete = new Delete(ROWS[3]);
    delete.deleteFamily(FAMILIES[1]);
    ht.delete(delete);

    put = new Put(ROWS[3]);
    put.add(FAMILIES[2], QUALIFIER, VALUES[0]);
    ht.put(put);

    put = new Put(ROWS[4]);
    put.add(FAMILIES[1], QUALIFIER, VALUES[1]);
    put.add(FAMILIES[2], QUALIFIER, VALUES[2]);
    ht.put(put);

    get = new Get(ROWS[3]);
    get.addFamily(FAMILIES[1]);
    get.addFamily(FAMILIES[2]);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertTrue("Expected 1 key but received " + result.size(),
        result.size() == 1);

    get = new Get(ROWS[4]);
    get.addFamily(FAMILIES[1]);
    get.addFamily(FAMILIES[2]);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertTrue("Expected 2 keys but received " + result.size(),
        result.size() == 2);

    scan = new Scan(ROWS[3]);
    scan.addFamily(FAMILIES[1]);
    scan.addFamily(FAMILIES[2]);
    scan.setMaxVersions(Integer.MAX_VALUE);
    ResultScanner scanner = ht.getScanner(scan);
    result = scanner.next();
    assertTrue("Expected 1 key but received " + result.size(),
        result.size() == 1);
    assertTrue(Bytes.equals(CellUtil.cloneRow(result.rawCells()[0]), ROWS[3]));
    assertTrue(Bytes.equals(CellUtil.cloneValue(result.rawCells()[0]), VALUES[0]));
    result = scanner.next();
    assertTrue("Expected 2 keys but received " + result.size(),
        result.size() == 2);
    assertTrue(Bytes.equals(CellUtil.cloneRow(result.rawCells()[0]), ROWS[4]));
    assertTrue(Bytes.equals(CellUtil.cloneRow(result.rawCells()[1]), ROWS[4]));
    assertTrue(Bytes.equals(CellUtil.cloneValue(result.rawCells()[0]), VALUES[1]));
    assertTrue(Bytes.equals(CellUtil.cloneValue(result.rawCells()[1]), VALUES[2]));
    scanner.close();

    // Add test of bulk deleting.
    for (int i = 0; i < 10; i++) {
      byte [] bytes = Bytes.toBytes(i);
      put = new Put(bytes);
      put.setDurability(Durability.SKIP_WAL);
      put.add(FAMILIES[0], QUALIFIER, bytes);@@@+      // Insert 4 more versions of same column and a dupe@@@+      put = new Put(ROW);@@@+      put.addColumn(FAMILY, QUALIFIER, STAMPS[9], VALUES[9]);@@@+      put.addColumn(FAMILY, QUALIFIER, STAMPS[11], VALUES[11]);@@@+      put.addColumn(FAMILY, QUALIFIER, STAMPS[13], VALUES[13]);@@@+      put.addColumn(FAMILY, QUALIFIER, STAMPS[15], VALUES[15]);@@@       ht.put(put);
    }
    for (int i = 0; i < 10; i++) {
      byte [] bytes = Bytes.toBytes(i);
      get = new Get(bytes);
      get.addFamily(FAMILIES[0]);@@@+@@@+      get = new Get(ROW);@@@+      get.addColumn(FAMILY, QUALIFIER);@@@+      get.readVersions(Integer.MAX_VALUE);@@@       result = ht.get(get);
      assertTrue(result.size() == 1);
    }
    ArrayList<Delete> deletes = new ArrayList<Delete>();
    for (int i = 0; i < 10; i++) {
      byte [] bytes = Bytes.toBytes(i);
      delete = new Delete(bytes);
      delete.deleteFamily(FAMILIES[0]);
      deletes.add(delete);
    }
    ht.delete(deletes);
    for (int i = 0; i < 10; i++) {
      byte [] bytes = Bytes.toBytes(i);
      get = new Get(bytes);
      get.addFamily(FAMILIES[0]);
      result = ht.get(get);
      assertTrue(result.size() == 0);
    }
  }
```
```
Cyclomatic Complexity	 9
Assertions		 11
Lines of Code		63
-  
  public void testJiraTest867() throws Exception {
    int numRows = 10;
    int numColsPerRow = 2000;

    byte [] TABLE = Bytes.toBytes("testJiraTest867");

    byte [][] ROWS = makeN(ROW, numRows);
    byte [][] QUALIFIERS = makeN(QUALIFIER, numColsPerRow);

    Table ht = TEST_UTIL.createTable(TABLE, FAMILY);
    TEST_UTIL.waitTableAvailable(TABLE, 10000);

    // Insert rows

    for(int i=0;i<numRows;i++) {
      Put put = new Put(ROWS[i]);
      put.setDurability(Durability.SKIP_WAL);
      for(int j=0;j<numColsPerRow;j++) {
        put.add(FAMILY, QUALIFIERS[j], QUALIFIERS[j]);
      }
      assertTrue("Put expected to contain " + numColsPerRow + " columns but " +
          "only contains " + put.size(), put.size() == numColsPerRow);
      ht.put(put);
    }

    // Get a row
    Get get = new Get(ROWS[numRows-1]);
    Result result = ht.get(get);
    assertNumKeys(result, numColsPerRow);
    Cell [] keys = result.rawCells();
    for(int i=0;i<result.size();i++) {
      assertKey(keys[i], ROWS[numRows-1], FAMILY, QUALIFIERS[i], QUALIFIERS[i]);
    }

    // Scan the rows
    Scan scan = new Scan();
    ResultScanner scanner = ht.getScanner(scan);
    int rowCount = 0;
    while((result = scanner.next()) != null) {
      assertNumKeys(result, numColsPerRow);
      Cell [] kvs = result.rawCells();
      for(int i=0;i<numColsPerRow;i++) {
        assertKey(kvs[i], ROWS[rowCount], FAMILY, QUALIFIERS[i], QUALIFIERS[i]);
      }
      rowCount++;
    }
    scanner.close();
    assertTrue("Expected to scan " + numRows + " rows but actually scanned "
        + rowCount + " rows", rowCount == numRows);

    // flush and try again

    TEST_UTIL.flush();

    // Get a row
    get = new Get(ROWS[numRows-1]);
    result = ht.get(get);
    assertNumKeys(result, numColsPerRow);
    keys = result.rawCells();
    for(int i=0;i<result.size();i++) {
      assertKey(keys[i], ROWS[numRows-1], FAMILY, QUALIFIERS[i], QUALIFIERS[i]);
    }

    // Scan the rows
    scan = new Scan();
    scanner = ht.getScanner(scan);
    rowCount = 0;
    while((result = scanner.next()) != null) {
      assertNumKeys(result, numColsPerRow);
      Cell [] kvs = result.rawCells();
      for(int i=0;i<numColsPerRow;i++) {
        assertKey(kvs[i], ROWS[rowCount], FAMILY, QUALIFIERS[i], QUALIFIERS[i]);
      }
      rowCount++;
    }
    scanner.close();
    assertTrue("Expected to scan " + numRows + " rows but actually scanned "
        + rowCount + " rows", rowCount == numRows);

  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		39
-  
  public void testJiraTest861() throws Exception {

    byte [] TABLE = Bytes.toBytes("testJiraTest861");
    byte [][] VALUES = makeNAscii(VALUE, 7);
    long [] STAMPS = makeStamps(7);

    Table ht = TEST_UTIL.createTable(TABLE, FAMILY, 10);
    TEST_UTIL.waitTableAvailable(TABLE, 10000);

    // Insert three versions

    Put put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, STAMPS[3], VALUES[3]);
    put.add(FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    put.add(FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    ht.put(put);

    // Get the middle value
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);

    // Try to get one version before (expect fail)
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[1]);

    // Try to get one version after (expect fail)
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[5]);

    // Try same from storefile
    TEST_UTIL.flush();
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[1]);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[5]);

    // Insert two more versions surrounding others, into memstore
    put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, STAMPS[0], VALUES[0]);
    put.add(FAMILY, QUALIFIER, STAMPS[6], VALUES[6]);
    ht.put(put);

    // Check we can get everything we should and can't get what we shouldn't
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[0], VALUES[0]);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[1]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[3], VALUES[3]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[5]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[6], VALUES[6]);

    // Try same from two storefiles
    TEST_UTIL.flush();
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[0], VALUES[0]);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[1]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[3], VALUES[3]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[5]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[6], VALUES[6]);

  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		33
-  
  public void testJiraTest33() throws Exception {

    byte [] TABLE = Bytes.toBytes("testJiraTest33");
    byte [][] VALUES = makeNAscii(VALUE, 7);
    long [] STAMPS = makeStamps(7);

    Table ht = TEST_UTIL.createTable(TABLE, FAMILY, 10);
    TEST_UTIL.waitTableAvailable(TABLE, 10000);

    // Insert lots versions

    Put put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, STAMPS[0], VALUES[0]);
    put.add(FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    put.add(FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    put.add(FAMILY, QUALIFIER, STAMPS[3], VALUES[3]);
    put.add(FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    put.add(FAMILY, QUALIFIER, STAMPS[5], VALUES[5]);
    ht.put(put);

    getVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 5);
    getVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 2);
    getVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 4, 5);
    getVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 2, 3);

    scanVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 5);
    scanVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 2);
    scanVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 4, 5);
    scanVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 2, 3);

    // Try same from storefile
    TEST_UTIL.flush();

    getVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 5);
    getVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 2);
    getVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 4, 5);
    getVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 2, 3);

    scanVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 5);
    scanVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 2);
    scanVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 4, 5);
    scanVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 2, 3);

  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		13
-  
  public void testJiraTest1014() throws Exception {

    byte [] TABLE = Bytes.toBytes("testJiraTest1014");

    Table ht = TEST_UTIL.createTable(TABLE, FAMILY, 10);
    TEST_UTIL.waitTableAvailable(TABLE, 10000);

    long manualStamp = 12345;

    // Insert lots versions

    Put put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, manualStamp, VALUE);
    ht.put(put);

    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, manualStamp, VALUE);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, manualStamp-1);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, manualStamp+1);

  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		29
-  
  public void testJiraTest1182() throws Exception {

    byte [] TABLE = Bytes.toBytes("testJiraTest1182");
    byte [][] VALUES = makeNAscii(VALUE, 7);
    long [] STAMPS = makeStamps(7);

    Table ht = TEST_UTIL.createTable(TABLE, FAMILY, 10);
    TEST_UTIL.waitTableAvailable(TABLE, 10000);

    // Insert lots versions

    Put put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, STAMPS[0], VALUES[0]);
    put.add(FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    put.add(FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    put.add(FAMILY, QUALIFIER, STAMPS[3], VALUES[3]);
    put.add(FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    put.add(FAMILY, QUALIFIER, STAMPS[5], VALUES[5]);
    ht.put(put);

    getVersionRangeAndVerifyGreaterThan(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 5);
    getVersionRangeAndVerifyGreaterThan(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 2, 5);
    getVersionRangeAndVerifyGreaterThan(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 4, 5);

    scanVersionRangeAndVerifyGreaterThan(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 5);
    scanVersionRangeAndVerifyGreaterThan(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 2, 5);
    scanVersionRangeAndVerifyGreaterThan(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 4, 5);

    // Try same from storefile
    TEST_UTIL.flush();

    getVersionRangeAndVerifyGreaterThan(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 5);
    getVersionRangeAndVerifyGreaterThan(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 2, 5);
    getVersionRangeAndVerifyGreaterThan(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 4, 5);

    scanVersionRangeAndVerifyGreaterThan(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 5);
    scanVersionRangeAndVerifyGreaterThan(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 2, 5);
    scanVersionRangeAndVerifyGreaterThan(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 4, 5);
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		21
-  
  public void testJiraTest52() throws Exception {
    byte [] TABLE = Bytes.toBytes("testJiraTest52");
    byte [][] VALUES = makeNAscii(VALUE, 7);
    long [] STAMPS = makeStamps(7);

    Table ht = TEST_UTIL.createTable(TABLE, FAMILY, 10);
    TEST_UTIL.waitTableAvailable(TABLE, 10000);

    // Insert lots versions

    Put put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, STAMPS[0], VALUES[0]);
    put.add(FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    put.add(FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    put.add(FAMILY, QUALIFIER, STAMPS[3], VALUES[3]);
    put.add(FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    put.add(FAMILY, QUALIFIER, STAMPS[5], VALUES[5]);
    ht.put(put);

    getAllVersionsAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 5);

    scanAllVersionsAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 5);

    // Try same from storefile
    TEST_UTIL.flush();

    getAllVersionsAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 5);

    scanAllVersionsAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 5);
  }
```
```
Cyclomatic Complexity	 1
Assertions		 12
Lines of Code		167
-  
  public void testDuplicateVersions() throws Exception {
    byte [] TABLE = Bytes.toBytes("testDuplicateVersions");

    long [] STAMPS = makeStamps(20);
    byte [][] VALUES = makeNAscii(VALUE, 20);

    Table ht = TEST_UTIL.createTable(TABLE, FAMILY, 10);
    TEST_UTIL.waitTableAvailable(TABLE, 10000);

    // Insert 4 versions of same column
    Put put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    put.add(FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    put.add(FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    put.add(FAMILY, QUALIFIER, STAMPS[5], VALUES[5]);
    ht.put(put);

    // Verify we can get each one properly
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[5], VALUES[5]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[5], VALUES[5]);

    // Verify we don't accidentally get others
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[0]);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[3]);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[6]);
    scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[0]);
    scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[3]);
    scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[6]);

    // Ensure maxVersions in query is respected
    Get get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIER);
    get.setMaxVersions(2);
    Result result = ht.get(get);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[4], STAMPS[5]},
        new byte[][] {VALUES[4], VALUES[5]},
        0, 1);

    Scan scan = new Scan(ROW);
    scan.addColumn(FAMILY, QUALIFIER);
    scan.setMaxVersions(2);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[4], STAMPS[5]},
        new byte[][] {VALUES[4], VALUES[5]},
        0, 1);

    // Flush and redo

    TEST_UTIL.flush();

    // Verify we can get each one properly
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[5], VALUES[5]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[5], VALUES[5]);

    // Verify we don't accidentally get others
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[0]);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[3]);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[6]);
    scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[0]);
    scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[3]);
    scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[6]);

    // Ensure maxVersions in query is respected
    get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIER);
    get.setMaxVersions(2);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[4], STAMPS[5]},
        new byte[][] {VALUES[4], VALUES[5]},
        0, 1);

    scan = new Scan(ROW);
    scan.addColumn(FAMILY, QUALIFIER);
    scan.setMaxVersions(2);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[4], STAMPS[5]},
        new byte[][] {VALUES[4], VALUES[5]},
        0, 1);


    // Add some memstore and retest

    // Insert 4 more versions of same column and a dupe
    put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, STAMPS[3], VALUES[3]);
    put.add(FAMILY, QUALIFIER, STAMPS[4], VALUES[14]);
    put.add(FAMILY, QUALIFIER, STAMPS[6], VALUES[6]);
    put.add(FAMILY, QUALIFIER, STAMPS[7], VALUES[7]);
    put.add(FAMILY, QUALIFIER, STAMPS[8], VALUES[8]);
    ht.put(put);

    // Ensure maxVersions in query is respected
    get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIER);
    get.setMaxVersions(7);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7], STAMPS[8]},
        new byte[][] {VALUES[2], VALUES[3], VALUES[14], VALUES[5], VALUES[6], VALUES[7], VALUES[8]},
        0, 6);

    scan = new Scan(ROW);
    scan.addColumn(FAMILY, QUALIFIER);
    scan.setMaxVersions(7);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7], STAMPS[8]},
        new byte[][] {VALUES[2], VALUES[3], VALUES[14], VALUES[5], VALUES[6], VALUES[7], VALUES[8]},
        0, 6);

    get = new Get(ROW);
    get.setMaxVersions(7);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7], STAMPS[8]},
        new byte[][] {VALUES[2], VALUES[3], VALUES[14], VALUES[5], VALUES[6], VALUES[7], VALUES[8]},
        0, 6);

    scan = new Scan(ROW);
    scan.setMaxVersions(7);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7], STAMPS[8]},
        new byte[][] {VALUES[2], VALUES[3], VALUES[14], VALUES[5], VALUES[6], VALUES[7], VALUES[8]},
        0, 6);

    // Verify we can get each one properly
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[14]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[7], VALUES[7]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[14]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[7], VALUES[7]);

    // Verify we don't accidentally get others
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[0]);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[9]);
    scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[0]);
    scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[9]);

    // Ensure maxVersions of table is respected

    TEST_UTIL.flush();

    // Insert 4 more versions of same column and a dupe
    put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, STAMPS[9], VALUES[9]);
    put.add(FAMILY, QUALIFIER, STAMPS[11], VALUES[11]);
    put.add(FAMILY, QUALIFIER, STAMPS[13], VALUES[13]);
    put.add(FAMILY, QUALIFIER, STAMPS[15], VALUES[15]);
    ht.put(put);

    get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIER);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7], STAMPS[8], STAMPS[9], STAMPS[11], STAMPS[13], STAMPS[15]},
        new byte[][] {VALUES[3], VALUES[14], VALUES[5], VALUES[6], VALUES[7], VALUES[8], VALUES[9], VALUES[11], VALUES[13], VALUES[15]},
        0, 9);

    scan = new Scan(ROW);
    scan.addColumn(FAMILY, QUALIFIER);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7], STAMPS[8], STAMPS[9], STAMPS[11], STAMPS[13], STAMPS[15]},
        new byte[][] {VALUES[3], VALUES[14], VALUES[5], VALUES[6], VALUES[7], VALUES[8], VALUES[9], VALUES[11], VALUES[13], VALUES[15]},
        0, 9);

    // Delete a version in the memstore and a version in a storefile
    Delete delete = new Delete(ROW);
    delete.deleteColumn(FAMILY, QUALIFIER, STAMPS[11]);
    delete.deleteColumn(FAMILY, QUALIFIER, STAMPS[7]);
    ht.delete(delete);

    // Test that it's gone
    get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIER);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[1], STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[8], STAMPS[9], STAMPS[13], STAMPS[15]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3], VALUES[14], VALUES[5], VALUES[6], VALUES[8], VALUES[9], VALUES[13], VALUES[15]},
        0, 9);

    scan = new Scan(ROW);
    scan.addColumn(FAMILY, QUALIFIER);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[1], STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[8], STAMPS[9], STAMPS[13], STAMPS[15]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3], VALUES[14], VALUES[5], VALUES[6], VALUES[8], VALUES[9], VALUES[13], VALUES[15]},
        0, 9);
  }
```
```
Cyclomatic Complexity	 1
Assertions		 4
Lines of Code		35
-  
  public void testUpdates() throws Exception {

    byte [] TABLE = Bytes.toBytes("testUpdates");
    Table hTable = TEST_UTIL.createTable(TABLE, FAMILY, 10);
    TEST_UTIL.waitTableAvailable(TABLE, 10000);

    // Write a column with values at timestamp 1, 2 and 3
    byte[] row = Bytes.toBytes("row1");
    byte[] qualifier = Bytes.toBytes("myCol");
    Put put = new Put(row);
    put.add(FAMILY, qualifier, 1L, Bytes.toBytes("AAA"));
    hTable.put(put);

    put = new Put(row);
    put.add(FAMILY, qualifier, 2L, Bytes.toBytes("BBB"));
    hTable.put(put);

    put = new Put(row);
    put.add(FAMILY, qualifier, 3L, Bytes.toBytes("EEE"));
    hTable.put(put);

    Get get = new Get(row);
    get.addColumn(FAMILY, qualifier);
    get.setMaxVersions();

    // Check that the column indeed has the right values at timestamps 1 and
    // 2
    Result result = hTable.get(get);
    NavigableMap<Long, byte[]> navigableMap =
        result.getMap().get(FAMILY).get(qualifier);
    assertEquals("AAA", Bytes.toString(navigableMap.get(1L)));
    assertEquals("BBB", Bytes.toString(navigableMap.get(2L)));

    // Update the value at timestamp 1
    put = new Put(row);
    put.add(FAMILY, qualifier, 1L, Bytes.toBytes("CCC"));
    hTable.put(put);

    // Update the value at timestamp 2
    put = new Put(row);
    put.add(FAMILY, qualifier, 2L, Bytes.toBytes("DDD"));
    hTable.put(put);

    // Check that the values at timestamp 2 and 1 got updated
    result = hTable.get(get);
    navigableMap = result.getMap().get(FAMILY).get(qualifier);
    assertEquals("CCC", Bytes.toString(navigableMap.get(1L)));
    assertEquals("DDD", Bytes.toString(navigableMap.get(2L)));
  }
```
```
Cyclomatic Complexity	 1
Assertions		 4
Lines of Code		42
-  
  public void testUpdatesWithMajorCompaction() throws Exception {

    TableName TABLE = TableName.valueOf("testUpdatesWithMajorCompaction");
    Table hTable = TEST_UTIL.createTable(TABLE, FAMILY, 10);
    TEST_UTIL.waitTableAvailable(TABLE, 10000);

    HBaseAdmin admin = new HBaseAdmin(TEST_UTIL.getConfiguration());

    // Write a column with values at timestamp 1, 2 and 3
    byte[] row = Bytes.toBytes("row2");
    byte[] qualifier = Bytes.toBytes("myCol");
    Put put = new Put(row);
    put.add(FAMILY, qualifier, 1L, Bytes.toBytes("AAA"));
    hTable.put(put);

    put = new Put(row);
    put.add(FAMILY, qualifier, 2L, Bytes.toBytes("BBB"));
    hTable.put(put);

    put = new Put(row);
    put.add(FAMILY, qualifier, 3L, Bytes.toBytes("EEE"));
    hTable.put(put);

    Get get = new Get(row);
    get.addColumn(FAMILY, qualifier);
    get.setMaxVersions();

    // Check that the column indeed has the right values at timestamps 1 and
    // 2
    Result result = hTable.get(get);
    NavigableMap<Long, byte[]> navigableMap =
        result.getMap().get(FAMILY).get(qualifier);
    assertEquals("AAA", Bytes.toString(navigableMap.get(1L)));
    assertEquals("BBB", Bytes.toString(navigableMap.get(2L)));

    // Trigger a major compaction
    admin.flush(TABLE);
    admin.majorCompact(TABLE);
    Thread.sleep(6000);

    // Update the value at timestamp 1
    put = new Put(row);
    put.add(FAMILY, qualifier, 1L, Bytes.toBytes("CCC"));
    hTable.put(put);

    // Update the value at timestamp 2
    put = new Put(row);
    put.add(FAMILY, qualifier, 2L, Bytes.toBytes("DDD"));
    hTable.put(put);

    // Trigger a major compaction
    admin.flush(TABLE);
    admin.majorCompact(TABLE);
    Thread.sleep(6000);

    // Check that the values at timestamp 2 and 1 got updated
    result = hTable.get(get);
    navigableMap = result.getMap().get(FAMILY).get(qualifier);
    assertEquals("CCC", Bytes.toString(navigableMap.get(1L)));
    assertEquals("DDD", Bytes.toString(navigableMap.get(2L)));
  }
```
```
Cyclomatic Complexity	 1
Assertions		 4
Lines of Code		46
-  
  public void testMajorCompactionBetweenTwoUpdates() throws Exception {

    String tableName = "testMajorCompactionBetweenTwoUpdates";
    byte [] TABLE = Bytes.toBytes(tableName);
    Table hTable = TEST_UTIL.createTable(TABLE, FAMILY, 10);
    TEST_UTIL.waitTableAvailable(TABLE, 10000);
    HBaseAdmin admin = new HBaseAdmin(TEST_UTIL.getConfiguration());

    // Write a column with values at timestamp 1, 2 and 3
    byte[] row = Bytes.toBytes("row3");
    byte[] qualifier = Bytes.toBytes("myCol");
    Put put = new Put(row);
    put.add(FAMILY, qualifier, 1L, Bytes.toBytes("AAA"));
    hTable.put(put);

    put = new Put(row);
    put.add(FAMILY, qualifier, 2L, Bytes.toBytes("BBB"));
    hTable.put(put);

    put = new Put(row);
    put.add(FAMILY, qualifier, 3L, Bytes.toBytes("EEE"));
    hTable.put(put);

    Get get = new Get(row);
    get.addColumn(FAMILY, qualifier);
    get.setMaxVersions();

    // Check that the column indeed has the right values at timestamps 1 and
    // 2
    Result result = hTable.get(get);
    NavigableMap<Long, byte[]> navigableMap =
        result.getMap().get(FAMILY).get(qualifier);
    assertEquals("AAA", Bytes.toString(navigableMap.get(1L)));
    assertEquals("BBB", Bytes.toString(navigableMap.get(2L)));

    // Trigger a major compaction
    admin.flush(tableName);
    admin.majorCompact(tableName);
    Thread.sleep(6000);

    // Update the value at timestamp 1
    put = new Put(row);
    put.add(FAMILY, qualifier, 1L, Bytes.toBytes("CCC"));
    hTable.put(put);

    // Trigger a major compaction
    admin.flush(tableName);
    admin.majorCompact(tableName);
    Thread.sleep(6000);

    // Update the value at timestamp 2
    put = new Put(row);
    put.add(FAMILY, qualifier, 2L, Bytes.toBytes("DDD"));
    hTable.put(put);

    // Trigger a major compaction
    admin.flush(tableName);
    admin.majorCompact(tableName);
    Thread.sleep(6000);

    // Check that the values at timestamp 2 and 1 got updated
    result = hTable.get(get);
    navigableMap = result.getMap().get(FAMILY).get(qualifier);

    assertEquals("CCC", Bytes.toString(navigableMap.get(1L)));
    assertEquals("DDD", Bytes.toString(navigableMap.get(2L)));
  }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		9
-  
  public void testGet_EmptyTable() throws IOException, InterruptedException {
    Table table = TEST_UTIL.createTable(TableName.valueOf("testGet_EmptyTable"), FAMILY);
    TEST_UTIL.waitTableAvailable(TableName.valueOf("testGet_EmptyTable"), 10000);
    Get get = new Get(ROW);
    get.addFamily(FAMILY);
    Result r = table.get(get);
    assertTrue(r.isEmpty());
  }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		20
-  
  public void testGet_NullQualifier() throws IOException, InterruptedException {
    Table table = TEST_UTIL.createTable(TableName.valueOf("testGet_NullQualifier"), FAMILY);
    TEST_UTIL.waitTableAvailable(TableName.valueOf("testGet_NullQualifier"), 10000);
    Put put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, VALUE);
    table.put(put);

    put = new Put(ROW);
    put.add(FAMILY, null, VALUE);
    table.put(put);
    LOG.info("Row put");

    Get get = new Get(ROW);
    get.addColumn(FAMILY, null);
    Result r = table.get(get);
    assertEquals(1, r.size());

    get = new Get(ROW);
    get.addFamily(FAMILY);
    r = table.get(get);
    assertEquals(2, r.size());
  }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		20
-  
  public void testGet_NonExistentRow() throws IOException, InterruptedException {
    Table table = TEST_UTIL.createTable(TableName.valueOf("testGet_NonExistentRow"), FAMILY);
    TEST_UTIL.waitTableAvailable(TableName.valueOf("testGet_NonExistentRow"), 10000);
    Put put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, VALUE);
    table.put(put);
    LOG.info("Row put");

    Get get = new Get(ROW);
    get.addFamily(FAMILY);
    Result r = table.get(get);
    assertFalse(r.isEmpty());
    System.out.println("Row retrieved successfully");

    byte [] missingrow = Bytes.toBytes("missingrow");
    get = new Get(missingrow);
    get.addFamily(FAMILY);
    r = table.get(get);
    assertTrue(r.isEmpty());
    LOG.info("Row missing as it should be");
  }
```
```
Cyclomatic Complexity	 4
Assertions		 5
Lines of Code		31
-  
  public void testPut() throws IOException, InterruptedException {
    final byte [] CONTENTS_FAMILY = Bytes.toBytes("contents");
    final byte [] SMALL_FAMILY = Bytes.toBytes("smallfam");
    final byte [] row1 = Bytes.toBytes("row1");
    final byte [] row2 = Bytes.toBytes("row2");
    final byte [] value = Bytes.toBytes("abcd");
    Table table = TEST_UTIL.createTable(Bytes.toBytes("testPut"),
      new byte [][] {CONTENTS_FAMILY, SMALL_FAMILY});
    TEST_UTIL.waitTableAvailable(Bytes.toBytes("testPut"), 10000);
    Put put = new Put(row1);
    put.add(CONTENTS_FAMILY, null, value);
    table.put(put);

    put = new Put(row2);
    put.add(CONTENTS_FAMILY, null, value);

    assertEquals(put.size(), 1);
    assertEquals(put.getFamilyCellMap().get(CONTENTS_FAMILY).size(), 1);

    // KeyValue v1 expectation.  Cast for now until we go all Cell all the time. TODO
    KeyValue kv = (KeyValue)put.getFamilyCellMap().get(CONTENTS_FAMILY).get(0);

    assertTrue(Bytes.equals(kv.getFamily(), CONTENTS_FAMILY));
    // will it return null or an empty byte array?
    assertTrue(Bytes.equals(kv.getQualifier(), new byte[0]));

    assertTrue(Bytes.equals(kv.getValue(), value));

    table.put(put);

    Scan scan = new Scan();
    scan.addColumn(CONTENTS_FAMILY, null);
    ResultScanner scanner = table.getScanner(scan);
    for (Result r : scanner) {
      for(Cell key : r.rawCells()) {
        System.out.println(Bytes.toString(r.getRow()) + ": " + key.toString());
      }
    }
  }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		16
-  
  public void testPutNoCF() throws IOException, InterruptedException {
    final byte[] BAD_FAM = Bytes.toBytes("BAD_CF");
    final byte[] VAL = Bytes.toBytes(100);
    Table table = TEST_UTIL.createTable(Bytes.toBytes("testPutNoCF"), FAMILY);
    TEST_UTIL.waitTableAvailable(Bytes.toBytes("testPutNoCF"), 10000);

    boolean caughtNSCFE = false;

    try {
      Put p = new Put(ROW);
      p.add(BAD_FAM, QUALIFIER, VAL);
      table.put(p);
    } catch (RetriesExhaustedWithDetailsException e) {
      caughtNSCFE = e.getCause(0) instanceof NoSuchColumnFamilyException;
    }
    assertTrue("Should throw NoSuchColumnFamilyException", caughtNSCFE);

  }
```
```
Cyclomatic Complexity	 3
Assertions		 1
Lines of Code		27
-  
  public void testRowsPut() throws IOException, InterruptedException {
    final byte[] CONTENTS_FAMILY = Bytes.toBytes("contents");
    final byte[] SMALL_FAMILY = Bytes.toBytes("smallfam");
    final int NB_BATCH_ROWS = 10;
    final byte[] value = Bytes.toBytes("abcd");
    Table table = TEST_UTIL.createTable(Bytes.toBytes("testRowsPut"),
      new byte[][] {CONTENTS_FAMILY, SMALL_FAMILY });
    TEST_UTIL.waitTableAvailable(Bytes.toBytes("testRowsPut"), 10000);
    ArrayList<Put> rowsUpdate = new ArrayList<Put>();
    for (int i = 0; i < NB_BATCH_ROWS; i++) {
      byte[] row = Bytes.toBytes("row" + i);
      Put put = new Put(row);
      put.setDurability(Durability.SKIP_WAL);
      put.add(CONTENTS_FAMILY, null, value);
      rowsUpdate.add(put);
    }
    table.put(rowsUpdate);
    Scan scan = new Scan();
    scan.addFamily(CONTENTS_FAMILY);
    ResultScanner scanner = table.getScanner(scan);
    int nbRows = 0;
    for (@SuppressWarnings("unused")
    Result row : scanner)
      nbRows++;
    assertEquals(NB_BATCH_ROWS, nbRows);
  }
```
```
Cyclomatic Complexity	 4
Assertions		 2
Lines of Code		40
-  
  public void testRowsPutBufferedOneFlush() throws IOException, InterruptedException {
    final byte [] CONTENTS_FAMILY = Bytes.toBytes("contents");
    final byte [] SMALL_FAMILY = Bytes.toBytes("smallfam");
    final byte [] value = Bytes.toBytes("abcd");
    final int NB_BATCH_ROWS = 10;
    HTable table = TEST_UTIL.createTable(Bytes.toBytes("testRowsPutBufferedOneFlush"),
      new byte [][] {CONTENTS_FAMILY, SMALL_FAMILY});
    TEST_UTIL.waitTableAvailable(Bytes.toBytes("testRowsPutBufferedOneFlush"),
        10000);
    table.setAutoFlush(false);
    ArrayList<Put> rowsUpdate = new ArrayList<Put>();
    for (int i = 0; i < NB_BATCH_ROWS * 10; i++) {
      byte[] row = Bytes.toBytes("row" + i);
      Put put = new Put(row);
      put.setDurability(Durability.SKIP_WAL);
      put.add(CONTENTS_FAMILY, null, value);
      rowsUpdate.add(put);
    }
    table.put(rowsUpdate);

    Scan scan = new Scan();
    scan.addFamily(CONTENTS_FAMILY);
    ResultScanner scanner = table.getScanner(scan);
    int nbRows = 0;
    for (@SuppressWarnings("unused")
    Result row : scanner)
      nbRows++;
    assertEquals(0, nbRows);
    scanner.close();

    table.flushCommits();

    scan = new Scan();
    scan.addFamily(CONTENTS_FAMILY);
    scanner = table.getScanner(scan);
    nbRows = 0;
    for (@SuppressWarnings("unused")
    Result row : scanner)
      nbRows++;
    assertEquals(NB_BATCH_ROWS * 10, nbRows);
    table.close();
  }
```
```
Cyclomatic Complexity	 3
Assertions		 1
Lines of Code		29
-  
  public void testRowsPutBufferedManyManyFlushes() throws IOException, InterruptedException {
    final byte[] CONTENTS_FAMILY = Bytes.toBytes("contents");
    final byte[] SMALL_FAMILY = Bytes.toBytes("smallfam");
    final byte[] value = Bytes.toBytes("abcd");
    final int NB_BATCH_ROWS = 10;
    HTable table = TEST_UTIL.createTable(Bytes.toBytes("testRowsPutBufferedManyManyFlushes"),
      new byte[][] {CONTENTS_FAMILY, SMALL_FAMILY });
    TEST_UTIL.waitTableAvailable(Bytes.toBytes("testRowsPutBufferedManyManyFlushes"),
        10000);
    table.setWriteBufferSize(10);
    ArrayList<Put> rowsUpdate = new ArrayList<Put>();
    for (int i = 0; i < NB_BATCH_ROWS * 10; i++) {
      byte[] row = Bytes.toBytes("row" + i);
      Put put = new Put(row);
      put.setDurability(Durability.SKIP_WAL);
      put.add(CONTENTS_FAMILY, null, value);
      rowsUpdate.add(put);
    }
    table.put(rowsUpdate);

    Scan scan = new Scan();
    scan.addFamily(CONTENTS_FAMILY);
    ResultScanner scanner = table.getScanner(scan);
    int nbRows = 0;
    for (@SuppressWarnings("unused")
    Result row : scanner)
      nbRows++;
    assertEquals(NB_BATCH_ROWS * 10, nbRows);
  }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		25
-  
  public void testAddKeyValue() throws IOException {
    final byte[] CONTENTS_FAMILY = Bytes.toBytes("contents");
    final byte[] value = Bytes.toBytes("abcd");
    final byte[] row1 = Bytes.toBytes("row1");
    final byte[] row2 = Bytes.toBytes("row2");
    byte[] qualifier = Bytes.toBytes("qf1");
    Put put = new Put(row1);

    // Adding KeyValue with the same row
    KeyValue kv = new KeyValue(row1, CONTENTS_FAMILY, qualifier, value);
    boolean ok = true;
    try {
      put.add(kv);
    } catch (IOException e) {
      ok = false;
    }
    assertEquals(true, ok);

    // Adding KeyValue with the different row
    kv = new KeyValue(row2, CONTENTS_FAMILY, qualifier, value);
    ok = false;
    try {
      put.add(kv);
    } catch (IOException e) {
      ok = true;
    }
    assertEquals(true, ok);
  }
```
```
Cyclomatic Complexity	 3
Assertions		 6
Lines of Code		19
-  
  public void testAppendWithoutWAL() throws Exception {
    List<Result> resultsWithWal = doAppend(true);
    List<Result> resultsWithoutWal = doAppend(false);
    assertEquals(resultsWithWal.size(), resultsWithoutWal.size());
    for (int i = 0; i != resultsWithWal.size(); ++i) {
      Result resultWithWal = resultsWithWal.get(i);
      Result resultWithoutWal = resultsWithoutWal.get(i);
      assertEquals(resultWithWal.rawCells().length, resultWithoutWal.rawCells().length);
      for (int j = 0; j != resultWithWal.rawCells().length; ++j) {
        Cell cellWithWal = resultWithWal.rawCells()[j];
        Cell cellWithoutWal = resultWithoutWal.rawCells()[j];
        assertTrue(Bytes.equals(CellUtil.cloneRow(cellWithWal), CellUtil.cloneRow(cellWithoutWal)));
        assertTrue(Bytes.equals(CellUtil.cloneFamily(cellWithWal), CellUtil.cloneFamily(cellWithoutWal)));
        assertTrue(Bytes.equals(CellUtil.cloneQualifier(cellWithWal), CellUtil.cloneQualifier(cellWithoutWal)));
        assertTrue(Bytes.equals(CellUtil.cloneValue(cellWithWal), CellUtil.cloneValue(cellWithoutWal)));
      }
    }
  }
```
```
Cyclomatic Complexity	 10
Assertions		 2
Lines of Code		74
-  
  public void testHBase737() throws IOException, InterruptedException {
    final byte [] FAM1 = Bytes.toBytes("fam1");
    final byte [] FAM2 = Bytes.toBytes("fam2");
    // Open table
    Table table = TEST_UTIL.createTable(Bytes.toBytes("testHBase737"),
      new byte [][] {FAM1, FAM2});
    TEST_UTIL.waitTableAvailable(Bytes.toBytes("testHBase737"), 10000);
    // Insert some values
    Put put = new Put(ROW);
    put.add(FAM1, Bytes.toBytes("letters"), Bytes.toBytes("abcdefg"));
    table.put(put);
    try {
      Thread.sleep(1000);
    } catch (InterruptedException i) {
      //ignore
    }

    put = new Put(ROW);
    put.add(FAM1, Bytes.toBytes("numbers"), Bytes.toBytes("123456"));
    table.put(put);

    try {
      Thread.sleep(1000);
    } catch (InterruptedException i) {
      //ignore
    }

    put = new Put(ROW);
    put.add(FAM2, Bytes.toBytes("letters"), Bytes.toBytes("hijklmnop"));
    table.put(put);

    long times[] = new long[3];

    // First scan the memstore

    Scan scan = new Scan();
    scan.addFamily(FAM1);
    scan.addFamily(FAM2);
    ResultScanner s = table.getScanner(scan);
    try {
      int index = 0;
      Result r = null;
      while ((r = s.next()) != null) {
        for(Cell key : r.rawCells()) {
          times[index++] = key.getTimestamp();
        }
      }
    } finally {
      s.close();
    }
    for (int i = 0; i < times.length - 1; i++) {
      for (int j = i + 1; j < times.length; j++) {
        assertTrue(times[j] > times[i]);
      }
    }

    // Flush data to disk and try again
    TEST_UTIL.flush();

    // Reset times
    for(int i=0;i<times.length;i++) {
      times[i] = 0;
    }

    try {
      Thread.sleep(1000);
    } catch (InterruptedException i) {
      //ignore
    }
    scan = new Scan();
    scan.addFamily(FAM1);
    scan.addFamily(FAM2);
    s = table.getScanner(scan);
    try {
      int index = 0;
      Result r = null;
      while ((r = s.next()) != null) {
        for(Cell key : r.rawCells()) {
          times[index++] = key.getTimestamp();
        }
      }
    } finally {
      s.close();
    }
    for (int i = 0; i < times.length - 1; i++) {
      for (int j = i + 1; j < times.length; j++) {
        assertTrue(times[j] > times[i]);
      }
    }
  }
```
```
Cyclomatic Complexity	 5
Assertions		 2
Lines of Code		27
-  
  public void testListTables() throws IOException, InterruptedException {
    TableName t1 = TableName.valueOf("testListTables1");
    TableName t2 = TableName.valueOf("testListTables2");
    TableName t3 = TableName.valueOf("testListTables3");
    TableName [] tables = new TableName[] { t1, t2, t3 };
    for (int i = 0; i < tables.length; i++) {
      TEST_UTIL.createTable(tables[i], FAMILY);
      TEST_UTIL.waitTableAvailable(tables[i], 10000);
    }
    Admin admin = new HBaseAdmin(TEST_UTIL.getConfiguration());
    HTableDescriptor[] ts = admin.listTables();
    HashSet<HTableDescriptor> result = new HashSet<HTableDescriptor>(ts.length);
    Collections.addAll(result, ts);
    int size = result.size();
    assertTrue(size >= tables.length);
    for (int i = 0; i < tables.length && i < size; i++) {
      boolean found = false;
      for (int j = 0; j < ts.length; j++) {
        if (ts[j].getTableName().equals(tables[i])) {
          found = true;
          break;
        }
      }
      assertTrue("Not found: " + tables[i], found);
    }
  }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		9
-  
  public void testUnmanagedHConnection() throws IOException, InterruptedException {
    final TableName tableName = TableName.valueOf("testUnmanagedHConnection");
    HTable t = createUnmangedHConnectionHTable(tableName);
    HBaseAdmin ha = new HBaseAdmin(t.getConnection());
    assertTrue(ha.tableExists(tableName));
    assertTrue(t.get(new Get(ROW)).isEmpty());
    ha.close();
  }
```
```
Cyclomatic Complexity	 1
Assertions		 5
Lines of Code		19
-  
  public void testUnmanagedHConnectionReconnect() throws Exception {
    final TableName tableName = TableName.valueOf("testUnmanagedHConnectionReconnect");
    HTable t = createUnmangedHConnectionHTable(tableName);
    Connection conn = t.getConnection();
    try (HBaseAdmin ha = new HBaseAdmin(conn)) {
      assertTrue(ha.tableExists(tableName));
      assertTrue(t.get(new Get(ROW)).isEmpty());
    }

    // stop the master
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    cluster.stopMaster(0, false);
    cluster.waitOnMaster(0);

    // start up a new master
    cluster.startMaster();
    assertTrue(cluster.waitForActiveAndReadyMaster());

    // test that the same unmanaged connection works with a new
    // HBaseAdmin and can connect to the new master;
    try (HBaseAdmin newAdmin = new HBaseAdmin(conn)) {
      assertTrue(newAdmin.tableExists(tableName));
      assertTrue(newAdmin.getClusterStatus().getServersSize() == SLAVES);
    }
  }
```
```
Cyclomatic Complexity	 7
Assertions		 5
Lines of Code		55
-  
  public void testMiscHTableStuff() throws IOException, InterruptedException {
    final TableName tableAname = TableName.valueOf("testMiscHTableStuffA");
    final TableName tableBname = TableName.valueOf("testMiscHTableStuffB");
    final byte[] attrName = Bytes.toBytes("TESTATTR");
    final byte[] attrValue = Bytes.toBytes("somevalue");
    byte[] value = Bytes.toBytes("value");

    Table a = TEST_UTIL.createTable(tableAname, HConstants.CATALOG_FAMILY);
    TEST_UTIL.waitTableAvailable(tableAname, 10000);
    Table b = TEST_UTIL.createTable(tableBname, HConstants.CATALOG_FAMILY);
    TEST_UTIL.waitTableAvailable(tableBname, 10000);
    Put put = new Put(ROW);
    put.add(HConstants.CATALOG_FAMILY, null, value);
    a.put(put);

    // open a new connection to A and a connection to b
    Table newA = new HTable(TEST_UTIL.getConfiguration(), tableAname);

    // copy data from A to B
    Scan scan = new Scan();
    scan.addFamily(HConstants.CATALOG_FAMILY);
    ResultScanner s = newA.getScanner(scan);
    try {
      for (Result r : s) {
        put = new Put(r.getRow());
        put.setDurability(Durability.SKIP_WAL);
        for (Cell kv : r.rawCells()) {
          put.add(kv);
        }
        b.put(put);
      }
    } finally {
      s.close();
    }

    // Opening a new connection to A will cause the tables to be reloaded
    Table anotherA = new HTable(TEST_UTIL.getConfiguration(), tableAname);
    Get get = new Get(ROW);
    get.addFamily(HConstants.CATALOG_FAMILY);
    anotherA.get(get);

    // We can still access A through newA because it has the table information
    // cached. And if it needs to recalibrate, that will cause the information
    // to be reloaded.

    // Test user metadata
    Admin admin = new HBaseAdmin(TEST_UTIL.getConfiguration());
    // make a modifiable descriptor
    HTableDescriptor desc = new HTableDescriptor(a.getTableDescriptor());
    // offline the table
    admin.disableTable(tableAname);
    // add a user attribute to HTD
    desc.setValue(attrName, attrValue);
    // add a user attribute to HCD
    for (HColumnDescriptor c : desc.getFamilies())
      c.setValue(attrName, attrValue);
    // update metadata for all regions of this table
    admin.modifyTable(tableAname, desc);
    // enable the table
    admin.enableTable(tableAname);

    // Test that attribute changes were applied
    desc = a.getTableDescriptor();
    assertEquals("wrong table descriptor returned", desc.getTableName(), tableAname);
    // check HTD attribute
    value = desc.getValue(attrName);
    assertFalse("missing HTD attribute value", value == null);
    assertFalse("HTD attribute value is incorrect",
      Bytes.compareTo(value, attrValue) != 0);
    // check HCD attribute
    for (HColumnDescriptor c : desc.getFamilies()) {
      value = c.getValue(attrName);
      assertFalse("missing HCD attribute value", value == null);
      assertFalse("HCD attribute value is incorrect",
        Bytes.compareTo(value, attrValue) != 0);
    }
  }
```
```
Cyclomatic Complexity	 1
Assertions		 25
Lines of Code		73
-  
  public void testGetClosestRowBefore() throws IOException, InterruptedException {
    final TableName tableAname = TableName.valueOf("testGetClosestRowBefore");
    final byte[] firstRow = Bytes.toBytes("row111");
    final byte[] secondRow = Bytes.toBytes("row222");
    final byte[] thirdRow = Bytes.toBytes("row333");
    final byte[] forthRow = Bytes.toBytes("row444");
    final byte[] beforeFirstRow = Bytes.toBytes("row");
    final byte[] beforeSecondRow = Bytes.toBytes("row22");
    final byte[] beforeThirdRow = Bytes.toBytes("row33");
    final byte[] beforeForthRow = Bytes.toBytes("row44");

    HTable table =
        TEST_UTIL.createTable(tableAname,
          new byte[][] { HConstants.CATALOG_FAMILY, Bytes.toBytes("info2") }, 1,
            1024);
    TEST_UTIL.waitTableAvailable(tableAname, 10000);
    // set block size to 64 to making 2 kvs into one block, bypassing the walkForwardInSingleRow
    // in Store.rowAtOrBeforeFromStoreFile
    String regionName = table.getRegionLocations().firstKey().getEncodedName();
    Region region =
        TEST_UTIL.getRSForFirstRegionInTable(tableAname).getFromOnlineRegions(regionName);
    Put put1 = new Put(firstRow);
    Put put2 = new Put(secondRow);
    Put put3 = new Put(thirdRow);
    Put put4 = new Put(forthRow);
    byte[] one = new byte[] { 1 };
    byte[] two = new byte[] { 2 };
    byte[] three = new byte[] { 3 };
    byte[] four = new byte[] { 4 };

    put1.add(HConstants.CATALOG_FAMILY, null, one);
    put2.add(HConstants.CATALOG_FAMILY, null, two);
    put3.add(HConstants.CATALOG_FAMILY, null, three);
    put4.add(HConstants.CATALOG_FAMILY, null, four);
    table.put(put1);
    table.put(put2);
    table.put(put3);
    table.put(put4);
    region.flush(true);
    Result result = null;

    // Test before first that null is returned
    result = table.getRowOrBefore(beforeFirstRow, HConstants.CATALOG_FAMILY);
    assertTrue(result == null);

    // Test at first that first is returned
    result = table.getRowOrBefore(firstRow, HConstants.CATALOG_FAMILY);
    assertTrue(result.containsColumn(HConstants.CATALOG_FAMILY, null));
    assertTrue(Bytes.equals(result.getRow(), firstRow));
    assertTrue(Bytes.equals(result.getValue(HConstants.CATALOG_FAMILY, null), one));

    // Test in between first and second that first is returned
    result = table.getRowOrBefore(beforeSecondRow, HConstants.CATALOG_FAMILY);
    assertTrue(result.containsColumn(HConstants.CATALOG_FAMILY, null));
    assertTrue(Bytes.equals(result.getRow(), firstRow));
    assertTrue(Bytes.equals(result.getValue(HConstants.CATALOG_FAMILY, null), one));

    // Test at second make sure second is returned
    result = table.getRowOrBefore(secondRow, HConstants.CATALOG_FAMILY);
    assertTrue(result.containsColumn(HConstants.CATALOG_FAMILY, null));
    assertTrue(Bytes.equals(result.getRow(), secondRow));
    assertTrue(Bytes.equals(result.getValue(HConstants.CATALOG_FAMILY, null), two));

    // Test in second and third, make sure second is returned
    result = table.getRowOrBefore(beforeThirdRow, HConstants.CATALOG_FAMILY);
    assertTrue(result.containsColumn(HConstants.CATALOG_FAMILY, null));
    assertTrue(Bytes.equals(result.getRow(), secondRow));
    assertTrue(Bytes.equals(result.getValue(HConstants.CATALOG_FAMILY, null), two));

    // Test at third make sure third is returned
    result = table.getRowOrBefore(thirdRow, HConstants.CATALOG_FAMILY);
    assertTrue(result.containsColumn(HConstants.CATALOG_FAMILY, null));
    assertTrue(Bytes.equals(result.getRow(), thirdRow));
    assertTrue(Bytes.equals(result.getValue(HConstants.CATALOG_FAMILY, null), three));

    // Test in third and forth, make sure third is returned
    result = table.getRowOrBefore(beforeForthRow, HConstants.CATALOG_FAMILY);
    assertTrue(result.containsColumn(HConstants.CATALOG_FAMILY, null));
    assertTrue(Bytes.equals(result.getRow(), thirdRow));
    assertTrue(Bytes.equals(result.getValue(HConstants.CATALOG_FAMILY, null), three));

    // Test at forth make sure forth is returned
    result = table.getRowOrBefore(forthRow, HConstants.CATALOG_FAMILY);
    assertTrue(result.containsColumn(HConstants.CATALOG_FAMILY, null));
    assertTrue(Bytes.equals(result.getRow(), forthRow));
    assertTrue(Bytes.equals(result.getValue(HConstants.CATALOG_FAMILY, null), four));

    // Test after forth make sure forth is returned
    result = table.getRowOrBefore(Bytes.add(forthRow, one), HConstants.CATALOG_FAMILY);
    assertTrue(result.containsColumn(HConstants.CATALOG_FAMILY, null));
    assertTrue(Bytes.equals(result.getRow(), forthRow));
    assertTrue(Bytes.equals(result.getValue(HConstants.CATALOG_FAMILY, null), four));

    table.close();
  }
```
```
Cyclomatic Complexity	 1
Assertions		 3
Lines of Code		11
-  
  public void testScanVariableReuse() throws Exception {
    Scan scan = new Scan();
    scan.addFamily(FAMILY);
    scan.addColumn(FAMILY, ROW);

    assertTrue(scan.getFamilyMap().get(FAMILY).size() == 1);

    scan = new Scan();
    scan.addFamily(FAMILY);

    assertTrue(scan.getFamilyMap().get(FAMILY) == null);
    assertTrue(scan.getFamilyMap().containsKey(FAMILY));
  }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		28
-  
  public void testMultiRowMutation() throws Exception {
    LOG.info("Starting testMultiRowMutation");
    final TableName TABLENAME = TableName.valueOf("testMultiRowMutation");
    final byte [] ROW1 = Bytes.toBytes("testRow1");

    Table t = TEST_UTIL.createTable(TABLENAME, FAMILY);
    TEST_UTIL.waitTableAvailable(TABLENAME, 10000);
    Put p = new Put(ROW);
    p.add(FAMILY, QUALIFIER, VALUE);
    MutationProto m1 = ProtobufUtil.toMutation(MutationType.PUT, p);

    p = new Put(ROW1);
    p.add(FAMILY, QUALIFIER, VALUE);
    MutationProto m2 = ProtobufUtil.toMutation(MutationType.PUT, p);

    MutateRowsRequest.Builder mrmBuilder = MutateRowsRequest.newBuilder();
    mrmBuilder.addMutationRequest(m1);
    mrmBuilder.addMutationRequest(m2);
    MutateRowsRequest mrm = mrmBuilder.build();
    CoprocessorRpcChannel channel = t.coprocessorService(ROW);
    MultiRowMutationService.BlockingInterface service =
       MultiRowMutationService.newBlockingStub(channel);
    service.mutateRows(null, mrm);
    Get g = new Get(ROW);
    Result r = t.get(g);
    assertEquals(0, Bytes.compareTo(VALUE, r.getValue(FAMILY, QUALIFIER)));
    g = new Get(ROW1);
    r = t.get(g);
    assertEquals(0, Bytes.compareTo(VALUE, r.getValue(FAMILY, QUALIFIER)));
  }
```
```
Cyclomatic Complexity	 3
Assertions		 3
Lines of Code		44
-  
  public void testRowMutation() throws Exception {
    LOG.info("Starting testRowMutation");
    final TableName TABLENAME = TableName.valueOf("testRowMutation");
    Table t = TEST_UTIL.createTable(TABLENAME, FAMILY);
    TEST_UTIL.waitTableAvailable(TABLENAME, 10000);
    byte [][] QUALIFIERS = new byte [][] {
        Bytes.toBytes("a"), Bytes.toBytes("b")
    };
    RowMutations arm = new RowMutations(ROW);
    Put p = new Put(ROW);
    p.add(FAMILY, QUALIFIERS[0], VALUE);
    arm.add(p);
    t.mutateRow(arm);

    Get g = new Get(ROW);
    Result r = t.get(g);
    assertEquals(0, Bytes.compareTo(VALUE, r.getValue(FAMILY, QUALIFIERS[0])));

    arm = new RowMutations(ROW);
    p = new Put(ROW);
    p.add(FAMILY, QUALIFIERS[1], VALUE);
    arm.add(p);
    Delete d = new Delete(ROW);
    d.deleteColumns(FAMILY, QUALIFIERS[0]);
    arm.add(d);
    // TODO: Trying mutateRow again.  The batch was failing with a one try only.
    t.mutateRow(arm);
    r = t.get(g);
    assertEquals(0, Bytes.compareTo(VALUE, r.getValue(FAMILY, QUALIFIERS[1])));
    assertNull(r.getValue(FAMILY, QUALIFIERS[0]));

    //Test that we get a region level exception
    try {
      arm = new RowMutations(ROW);
      p = new Put(ROW);
      p.add(new byte[]{'b', 'o', 'g', 'u', 's'}, QUALIFIERS[0], VALUE);
      arm.add(p);
      t.mutateRow(arm);
      fail("Expected NoSuchColumnFamilyException");
    } catch(RetriesExhaustedWithDetailsException e) {
      for(Throwable rootCause: e.getCauses()){
        if(rootCause instanceof NoSuchColumnFamilyException){
          return;
        }
      }
      throw e;
    }
  }
```
```
Cyclomatic Complexity	 1
Assertions		 6
Lines of Code		27
-  
  public void testAppend() throws Exception {
    LOG.info("Starting testAppend");
    final TableName TABLENAME = TableName.valueOf("testAppend");
    Table t = TEST_UTIL.createTable(TABLENAME, FAMILY);
    TEST_UTIL.waitTableAvailable(TABLENAME, 10000);
    byte[] v1 = Bytes.toBytes("42");
    byte[] v2 = Bytes.toBytes("23");
    byte [][] QUALIFIERS = new byte [][] {
        Bytes.toBytes("b"), Bytes.toBytes("a"), Bytes.toBytes("c")
    };
    Append a = new Append(ROW);
    a.add(FAMILY, QUALIFIERS[0], v1);
    a.add(FAMILY, QUALIFIERS[1], v2);
    a.setReturnResults(false);
    assertNullResult(t.append(a));

    a = new Append(ROW);
    a.add(FAMILY, QUALIFIERS[0], v2);
    a.add(FAMILY, QUALIFIERS[1], v1);
    a.add(FAMILY, QUALIFIERS[2], v2);
    Result r = t.append(a);
    assertEquals(0, Bytes.compareTo(Bytes.add(v1,v2), r.getValue(FAMILY, QUALIFIERS[0])));
    assertEquals(0, Bytes.compareTo(Bytes.add(v2,v1), r.getValue(FAMILY, QUALIFIERS[1])));
    // QUALIFIERS[2] previously not exist, verify both value and timestamp are correct
    assertEquals(0, Bytes.compareTo(v2, r.getValue(FAMILY, QUALIFIERS[2])));
    assertEquals(r.getColumnLatest(FAMILY, QUALIFIERS[0]).getTimestamp(),
        r.getColumnLatest(FAMILY, QUALIFIERS[2]).getTimestamp());
  }
```
```
Cyclomatic Complexity	 3
Assertions		 2
Lines of Code		30
-  
  public void testClientPoolRoundRobin() throws IOException, InterruptedException {
    final TableName tableName = TableName.valueOf("testClientPoolRoundRobin");

    int poolSize = 3;
    int numVersions = poolSize * 2;
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.set(HConstants.HBASE_CLIENT_IPC_POOL_TYPE, "round-robin");
    conf.setInt(HConstants.HBASE_CLIENT_IPC_POOL_SIZE, poolSize);

    Table table = TEST_UTIL.createTable(tableName, new byte[][] { FAMILY }, conf, Integer.MAX_VALUE);
    TEST_UTIL.waitTableAvailable(tableName, 10000);

    final long ts = EnvironmentEdgeManager.currentTime();
    Get get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIER);
    get.setMaxVersions();

    for (int versions = 1; versions <= numVersions; versions++) {
      Put put = new Put(ROW);
      put.add(FAMILY, QUALIFIER, ts + versions, VALUE);
      table.put(put);

      Result result = table.get(get);
      NavigableMap<Long, byte[]> navigableMap = result.getMap().get(FAMILY)
          .get(QUALIFIER);

      assertEquals("The number of versions of '" + Bytes.toString(FAMILY) + ":"
          + Bytes.toString(QUALIFIER) + " did not match", versions, navigableMap.size());
      for (Map.Entry<Long, byte[]> entry : navigableMap.entrySet()) {
        assertTrue("The value at time " + entry.getKey()
            + " did not match what was put",
            Bytes.equals(VALUE, entry.getValue()));
      }
    }
  }
```
```
Cyclomatic Complexity	 -1
Assertions		 5
Lines of Code		71
-8989") 
  public void testClientPoolThreadLocal() throws IOException, InterruptedException {
    final TableName tableName = TableName.valueOf("testClientPoolThreadLocal");

    int poolSize = Integer.MAX_VALUE;
    int numVersions = 3;
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.set(HConstants.HBASE_CLIENT_IPC_POOL_TYPE, "thread-local");
    conf.setInt(HConstants.HBASE_CLIENT_IPC_POOL_SIZE, poolSize);

    final Table table = TEST_UTIL.createTable(tableName,
        new byte[][] { FAMILY }, conf, 3);
    TEST_UTIL.waitTableAvailable(tableName, 10000);
    final long ts = EnvironmentEdgeManager.currentTime();
    final Get get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIER);
    get.setMaxVersions();

    for (int versions = 1; versions <= numVersions; versions++) {
      Put put = new Put(ROW);
      put.add(FAMILY, QUALIFIER, ts + versions, VALUE);
      table.put(put);

      Result result = table.get(get);
      NavigableMap<Long, byte[]> navigableMap = result.getMap().get(FAMILY)
          .get(QUALIFIER);

      assertEquals("The number of versions of '" + Bytes.toString(FAMILY) + ":"
          + Bytes.toString(QUALIFIER) + " did not match", versions, navigableMap.size());
      for (Map.Entry<Long, byte[]> entry : navigableMap.entrySet()) {
        assertTrue("The value at time " + entry.getKey()
            + " did not match what was put",
            Bytes.equals(VALUE, entry.getValue()));
      }
    }

    final Object waitLock = new Object();
    ExecutorService executorService = Executors.newFixedThreadPool(numVersions);
    final AtomicReference<AssertionError> error = new AtomicReference<AssertionError>(null);
    for (int versions = numVersions; versions < numVersions * 2; versions++) {
      final int versionsCopy = versions;
      executorService.submit(new Callable<Void>() {
        @Override
        public Void call() {
          try {
            Put put = new Put(ROW);
            put.add(FAMILY, QUALIFIER, ts + versionsCopy, VALUE);
            table.put(put);

            Result result = table.get(get);
            NavigableMap<Long, byte[]> navigableMap = result.getMap()
                .get(FAMILY).get(QUALIFIER);

            assertEquals("The number of versions of '" + Bytes.toString(FAMILY) + ":"
                + Bytes.toString(QUALIFIER) + " did not match " + versionsCopy, versionsCopy,
                navigableMap.size());
            for (Map.Entry<Long, byte[]> entry : navigableMap.entrySet()) {
              assertTrue("The value at time " + entry.getKey()
                  + " did not match what was put",
                  Bytes.equals(VALUE, entry.getValue()));
            }
            synchronized (waitLock) {
              waitLock.wait();
            }
          } catch (Exception e) {
          } catch (AssertionError e) {
            // the error happens in a thread, it won't fail the test,
            // need to pass it to the caller for proper handling.
            error.set(e);
            LOG.error(e);
          }

          return null;
        }
      });
    }
    synchronized (waitLock) {
      waitLock.notifyAll();
    }
    executorService.shutdownNow();
    assertNull(error.get());
  }
```
```
Cyclomatic Complexity	 3
Assertions		 4
Lines of Code		25
-  
  public void testCheckAndPut() throws IOException, InterruptedException {
    final byte [] anotherrow = Bytes.toBytes("anotherrow");
    final byte [] value2 = Bytes.toBytes("abcd");

    Table table = TEST_UTIL.createTable(TableName.valueOf("testCheckAndPut"), FAMILY);
    TEST_UTIL.waitTableAvailable(TableName.valueOf("testCheckAndPut"), 10000);
    Put put1 = new Put(ROW);
    put1.add(FAMILY, QUALIFIER, VALUE);

    // row doesn't exist, so using non-null value should be considered "not match".
    boolean ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, VALUE, put1);
    assertEquals(ok, false);

    // row doesn't exist, so using "null" to check for existence should be considered "match".
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, null, put1);
    assertEquals(ok, true);

    // row now exists, so using "null" to check for existence should be considered "not match".
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, null, put1);
    assertEquals(ok, false);

    Put put2 = new Put(ROW);
    put2.add(FAMILY, QUALIFIER, value2);

    // row now exists, use the matching value to check
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, VALUE, put2);
    assertEquals(ok, true);

    Put put3 = new Put(anotherrow);
    put3.add(FAMILY, QUALIFIER, VALUE);

    // try to do CheckAndPut on different rows
    try {
        ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, value2, put3);
        fail("trying to check and modify different rows should have failed.");
    } catch(Exception e) {}
  }
```
```
Cyclomatic Complexity	 2
Assertions		 19
Lines of Code		51
-  
  public void testCheckAndPutWithCompareOp() throws IOException, InterruptedException {
    final byte [] value1 = Bytes.toBytes("aaaa");
    final byte [] value2 = Bytes.toBytes("bbbb");
    final byte [] value3 = Bytes.toBytes("cccc");
    final byte [] value4 = Bytes.toBytes("dddd");

    Table table = TEST_UTIL.createTable(TableName.valueOf("testCheckAndPutWithCompareOp"), FAMILY);
    TEST_UTIL.waitTableAvailable(TableName.valueOf("testCheckAndPutWithCompareOp"), 10000);

    Put put2 = new Put(ROW);
    put2.add(FAMILY, QUALIFIER, value2);

    Put put3 = new Put(ROW);
    put3.add(FAMILY, QUALIFIER, value3);

    // row doesn't exist, so using "null" to check for existence should be considered "match".
    boolean ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, null, put2);
    assertEquals(ok, true);

    // cell = "bbbb", using "aaaa" to compare only LESS/LESS_OR_EQUAL/NOT_EQUAL
    // turns out "match"
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, CompareOp.GREATER, value1, put2);
    assertEquals(ok, false);
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, CompareOp.EQUAL, value1, put2);
    assertEquals(ok, false);
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, CompareOp.GREATER_OR_EQUAL, value1, put2);
    assertEquals(ok, false);
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, CompareOp.LESS, value1, put2);
    assertEquals(ok, true);
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, CompareOp.LESS_OR_EQUAL, value1, put2);
    assertEquals(ok, true);
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, CompareOp.NOT_EQUAL, value1, put3);
    assertEquals(ok, true);

    // cell = "cccc", using "dddd" to compare only LARGER/LARGER_OR_EQUAL/NOT_EQUAL
    // turns out "match"
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, CompareOp.LESS, value4, put3);
    assertEquals(ok, false);
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, CompareOp.LESS_OR_EQUAL, value4, put3);
    assertEquals(ok, false);
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, CompareOp.EQUAL, value4, put3);
    assertEquals(ok, false);
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, CompareOp.GREATER, value4, put3);
    assertEquals(ok, true);
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, CompareOp.GREATER_OR_EQUAL, value4, put3);
    assertEquals(ok, true);
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, CompareOp.NOT_EQUAL, value4, put2);
    assertEquals(ok, true);

    // cell = "bbbb", using "bbbb" to compare only GREATER_OR_EQUAL/LESS_OR_EQUAL/EQUAL
    // turns out "match"
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, CompareOp.GREATER, value2, put2);
    assertEquals(ok, false);
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, CompareOp.NOT_EQUAL, value2, put2);
    assertEquals(ok, false);
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, CompareOp.LESS, value2, put2);
    assertEquals(ok, false);
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, CompareOp.GREATER_OR_EQUAL, value2, put2);
    assertEquals(ok, true);
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, CompareOp.LESS_OR_EQUAL, value2, put2);
    assertEquals(ok, true);
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, CompareOp.EQUAL, value2, put3);
    assertEquals(ok, true);
  }
```
```
Cyclomatic Complexity	 1
Assertions		 18
Lines of Code		60
-  
  public void testCheckAndDeleteWithCompareOp() throws IOException, InterruptedException {
    final byte [] value1 = Bytes.toBytes("aaaa");
    final byte [] value2 = Bytes.toBytes("bbbb");
    final byte [] value3 = Bytes.toBytes("cccc");
    final byte [] value4 = Bytes.toBytes("dddd");
    Table table = TEST_UTIL.createTable(TableName.valueOf("testCheckAndDeleteWithCompareOp"),
        FAMILY);
    TEST_UTIL.waitTableAvailable(TableName.valueOf("testCheckAndDeleteWithCompareOp"), 10000);

    Put put2 = new Put(ROW);
    put2.add(FAMILY, QUALIFIER, value2);
    table.put(put2);

    Put put3 = new Put(ROW);
    put3.add(FAMILY, QUALIFIER, value3);

    Delete delete = new Delete(ROW);
    delete.deleteColumns(FAMILY, QUALIFIER);

    // cell = "bbbb", using "aaaa" to compare only LESS/LESS_OR_EQUAL/NOT_EQUAL
    // turns out "match"
    boolean ok = table.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareOp.GREATER, value1, delete);
    assertEquals(ok, false);
    ok = table.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareOp.EQUAL, value1, delete);
    assertEquals(ok, false);
    ok = table.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareOp.GREATER_OR_EQUAL, value1, delete);
    assertEquals(ok, false);
    ok = table.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareOp.LESS, value1, delete);
    assertEquals(ok, true);
    table.put(put2);
    ok = table.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareOp.LESS_OR_EQUAL, value1, delete);
    assertEquals(ok, true);
    table.put(put2);

    assertEquals(ok, true);

    // cell = "cccc", using "dddd" to compare only LARGER/LARGER_OR_EQUAL/NOT_EQUAL
    // turns out "match"
    table.put(put3);
    ok = table.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareOp.LESS, value4, delete);

    assertEquals(ok, false);
    ok = table.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareOp.LESS_OR_EQUAL, value4, delete);

    assertEquals(ok, false);
    ok = table.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareOp.EQUAL, value4, delete);

    assertEquals(ok, false);
    ok = table.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareOp.GREATER, value4, delete);

    assertEquals(ok, true);
    table.put(put3);
    ok = table.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareOp.GREATER_OR_EQUAL, value4, delete);
    assertEquals(ok, true);
    table.put(put3);
    ok = table.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareOp.NOT_EQUAL, value4, delete);

    assertEquals(ok, true);

    // cell = "bbbb", using "bbbb" to compare only GREATER_OR_EQUAL/LESS_OR_EQUAL/EQUAL
    // turns out "match"
    table.put(put2);
    ok = table.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareOp.GREATER, value2, delete);
    assertEquals(ok, false);
    ok = table.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareOp.NOT_EQUAL, value2, delete);
    assertEquals(ok, false);
    ok = table.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareOp.LESS, value2, delete);
    assertEquals(ok, false);
    ok = table.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareOp.GREATER_OR_EQUAL, value2, delete);
    assertEquals(ok, true);
    table.put(put2);
    ok = table.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareOp.LESS_OR_EQUAL, value2, delete);
    assertEquals(ok, true);
    table.put(put2);
    ok = table.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareOp.EQUAL, value2, delete);
    assertEquals(ok, true);
  }
```
```
Cyclomatic Complexity	 5
Assertions		 29
Lines of Code		83
-  
  public void testCacheOnWriteEvictOnClose() throws Exception {
    TableName tableName = TableName.valueOf("testCOWEOCfromClient");
    byte [] data = Bytes.toBytes("data");
    HTable table = TEST_UTIL.createTable(tableName, FAMILY);
    TEST_UTIL.waitTableAvailable(tableName, 10000);
    // get the block cache and region
    String regionName = table.getRegionLocations().firstKey().getEncodedName();
    Region region = TEST_UTIL.getRSForFirstRegionInTable(tableName)
      .getFromOnlineRegions(regionName);
    Store store = region.getStores().iterator().next();
    CacheConfig cacheConf = store.getCacheConfig();
    cacheConf.setCacheDataOnWrite(true);
    cacheConf.setEvictOnClose(true);
    BlockCache cache = cacheConf.getBlockCache();

    // establish baseline stats
    long startBlockCount = cache.getBlockCount();
    long startBlockHits = cache.getStats().getHitCount();
    long startBlockMiss = cache.getStats().getMissCount();

    // wait till baseline is stable, (minimal 500 ms)
    for (int i = 0; i < 5; i++) {
      Thread.sleep(100);
      if (startBlockCount != cache.getBlockCount()
          || startBlockHits != cache.getStats().getHitCount()
          || startBlockMiss != cache.getStats().getMissCount()) {
        startBlockCount = cache.getBlockCount();
        startBlockHits = cache.getStats().getHitCount();
        startBlockMiss = cache.getStats().getMissCount();
        i = -1;
      }
    }

    // insert data
    Put put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, data);
    table.put(put);
    assertTrue(Bytes.equals(table.get(new Get(ROW)).value(), data));
    // data was in memstore so don't expect any changes
    assertEquals(startBlockCount, cache.getBlockCount());
    assertEquals(startBlockHits, cache.getStats().getHitCount());
    assertEquals(startBlockMiss, cache.getStats().getMissCount());
    // flush the data
    LOG.debug("Flushing cache");
    region.flush(true);
    // expect two more blocks in cache - DATA and ROOT_INDEX
    // , no change in hits/misses
    long expectedBlockCount = startBlockCount + 2;
    long expectedBlockHits = startBlockHits;
    long expectedBlockMiss = startBlockMiss;
    assertEquals(expectedBlockCount, cache.getBlockCount());
    assertEquals(expectedBlockHits, cache.getStats().getHitCount());
    assertEquals(expectedBlockMiss, cache.getStats().getMissCount());
    // read the data and expect same blocks, one new hit, no misses
    assertTrue(Bytes.equals(table.get(new Get(ROW)).value(), data));
    assertEquals(expectedBlockCount, cache.getBlockCount());
    assertEquals(++expectedBlockHits, cache.getStats().getHitCount());
    assertEquals(expectedBlockMiss, cache.getStats().getMissCount());
    // insert a second column, read the row, no new blocks, one new hit
    byte [] QUALIFIER2 = Bytes.add(QUALIFIER, QUALIFIER);
    byte [] data2 = Bytes.add(data, data);
    put = new Put(ROW);
    put.add(FAMILY, QUALIFIER2, data2);
    table.put(put);
    Result r = table.get(new Get(ROW));
    assertTrue(Bytes.equals(r.getValue(FAMILY, QUALIFIER), data));
    assertTrue(Bytes.equals(r.getValue(FAMILY, QUALIFIER2), data2));
    assertEquals(expectedBlockCount, cache.getBlockCount());
    assertEquals(++expectedBlockHits, cache.getStats().getHitCount());
    assertEquals(expectedBlockMiss, cache.getStats().getMissCount());
    // flush, one new block
    System.out.println("Flushing cache");
    region.flush(true);
    // + 1 for Index Block, +1 for data block
    expectedBlockCount += 2;
    assertEquals(expectedBlockCount, cache.getBlockCount());
    assertEquals(expectedBlockHits, cache.getStats().getHitCount());
    assertEquals(expectedBlockMiss, cache.getStats().getMissCount());
    // compact, net minus two blocks, two hits, no misses
    System.out.println("Compacting");
    assertEquals(2, store.getStorefilesCount());
    store.triggerMajorCompaction();
    region.compact(true);
    waitForStoreFileCount(store, 1, 10000); // wait 10 seconds max
    assertEquals(1, store.getStorefilesCount());
    // evicted two data blocks and two index blocks and compaction does not cache new blocks
    expectedBlockCount = 0;
    assertEquals(expectedBlockCount, cache.getBlockCount());
    expectedBlockHits += 2;
    assertEquals(expectedBlockMiss, cache.getStats().getMissCount());
    assertEquals(expectedBlockHits, cache.getStats().getHitCount());
    // read the row, this should be a cache miss because we don't cache data
    // blocks on compaction
    r = table.get(new Get(ROW));
    assertTrue(Bytes.equals(r.getValue(FAMILY, QUALIFIER), data));
    assertTrue(Bytes.equals(r.getValue(FAMILY, QUALIFIER2), data2));
    expectedBlockCount += 1; // cached one data block
    assertEquals(expectedBlockCount, cache.getBlockCount());
    assertEquals(expectedBlockHits, cache.getStats().getHitCount());
    assertEquals(++expectedBlockMiss, cache.getStats().getMissCount());
  }
```
```
Cyclomatic Complexity	 4
Assertions		 6
Lines of Code		35
-  
  /**
   * Tests the non cached version of getRegionLocator by moving a region.
   */
  public void testNonCachedGetRegionLocation() throws Exception {
    // Test Initialization.
    TableName TABLE = TableName.valueOf("testNonCachedGetRegionLocation");
    byte [] family1 = Bytes.toBytes("f1");
    byte [] family2 = Bytes.toBytes("f2");
    try (HTable table = TEST_UTIL.createTable(TABLE, new byte[][] {family1, family2}, 10);
        Admin admin = new HBaseAdmin(TEST_UTIL.getConfiguration())) {
      TEST_UTIL.waitTableAvailable(TABLE, 10000);
      Map <HRegionInfo, ServerName> regionsMap = table.getRegionLocations();
      assertEquals(1, regionsMap.size());
      HRegionInfo regionInfo = regionsMap.keySet().iterator().next();
      ServerName addrBefore = regionsMap.get(regionInfo);
      // Verify region location before move.
      HRegionLocation addrCache = table.getRegionLocation(regionInfo.getStartKey(), false);
      HRegionLocation addrNoCache = table.getRegionLocation(regionInfo.getStartKey(),  true);

      assertEquals(addrBefore.getPort(), addrCache.getPort());
      assertEquals(addrBefore.getPort(), addrNoCache.getPort());

      ServerName addrAfter = null;
      // Now move the region to a different server.
      for (int i = 0; i < SLAVES; i++) {
        HRegionServer regionServer = TEST_UTIL.getHBaseCluster().getRegionServer(i);
        ServerName addr = regionServer.getServerName();
        if (addr.getPort() != addrBefore.getPort()) {
          admin.move(regionInfo.getEncodedNameAsBytes(),
              Bytes.toBytes(addr.toString()));
          // Wait for the region to move.
          Thread.sleep(5000);
          addrAfter = addr;
          break;
        }
      }

      // Verify the region was moved.
      addrCache = table.getRegionLocation(regionInfo.getStartKey(), false);
      addrNoCache = table.getRegionLocation(regionInfo.getStartKey(), true);
      assertNotNull(addrAfter);
      assertTrue(addrAfter.getPort() != addrCache.getPort());
      assertEquals(addrAfter.getPort(), addrNoCache.getPort());
    }
  }
```
```
Cyclomatic Complexity	 1
Assertions		 10
Lines of Code		37
-  
  /**
   * Tests getRegionsInRange by creating some regions over which a range of
   * keys spans; then changing the key range.
   */
  public void testGetRegionsInRange() throws Exception {
    // Test Initialization.
    byte [] startKey = Bytes.toBytes("ddc");
    byte [] endKey = Bytes.toBytes("mmm");
    TableName TABLE = TableName.valueOf("testGetRegionsInRange");
    HTable table = TEST_UTIL.createMultiRegionTable(TABLE, new byte[][] { FAMILY }, 10);
    int numOfRegions = -1;
    try (RegionLocator r = table.getRegionLocator()) {
      numOfRegions = r.getStartKeys().length;
    }
    assertEquals(26, numOfRegions);

    // Get the regions in this range
    List<HRegionLocation> regionsList = table.getRegionsInRange(startKey,
      endKey);
    assertEquals(10, regionsList.size());

    // Change the start key
    startKey = Bytes.toBytes("fff");
    regionsList = table.getRegionsInRange(startKey, endKey);
    assertEquals(7, regionsList.size());

    // Change the end key
    endKey = Bytes.toBytes("nnn");
    regionsList = table.getRegionsInRange(startKey, endKey);
    assertEquals(8, regionsList.size());

    // Empty start key
    regionsList = table.getRegionsInRange(HConstants.EMPTY_START_ROW, endKey);
    assertEquals(13, regionsList.size());

    // Empty end key
    regionsList = table.getRegionsInRange(startKey, HConstants.EMPTY_END_ROW);
    assertEquals(21, regionsList.size());

    // Both start and end keys empty
    regionsList = table.getRegionsInRange(HConstants.EMPTY_START_ROW,
      HConstants.EMPTY_END_ROW);
    assertEquals(26, regionsList.size());

    // Change the end key to somewhere in the last block
    endKey = Bytes.toBytes("zzz1");
    regionsList = table.getRegionsInRange(startKey, endKey);
    assertEquals(21, regionsList.size());

    // Change the start key to somewhere in the first block
    startKey = Bytes.toBytes("aac");
    regionsList = table.getRegionsInRange(startKey, endKey);
    assertEquals(26, regionsList.size());

    // Make start and end key the same
    startKey = endKey = Bytes.toBytes("ccc");
    regionsList = table.getRegionsInRange(startKey, endKey);
    assertEquals(1, regionsList.size());
  }
```
```
Cyclomatic Complexity	 2
Assertions		 1
Lines of Code		22
-  
  public void testJira6912() throws Exception {
    TableName TABLE = TableName.valueOf("testJira6912");
    Table foo = TEST_UTIL.createTable(TABLE, new byte[][] {FAMILY}, 10);
    TEST_UTIL.waitTableAvailable(TABLE, 10000);
    List<Put> puts = new ArrayList<Put>();
    for (int i=0;i !=100; i++){
      Put put = new Put(Bytes.toBytes(i));
      put.add(FAMILY, FAMILY, Bytes.toBytes(i));
      puts.add(put);
    }
    foo.put(puts);
    // If i comment this out it works
    TEST_UTIL.flush();

    Scan scan = new Scan();
    scan.setStartRow(Bytes.toBytes(1));
    scan.setStopRow(Bytes.toBytes(3));
    scan.addColumn(FAMILY, FAMILY);
    scan.setFilter(new RowFilter(CompareFilter.CompareOp.NOT_EQUAL, new BinaryComparator(Bytes.toBytes(1))));

    ResultScanner scanner = foo.getScanner(scan);
    Result[] bar = scanner.next(100);
    assertEquals(1, bar.length);
  }
```
```
Cyclomatic Complexity	 1
Assertions		 4
Lines of Code		24
-  
  public void testScan_NullQualifier() throws IOException, InterruptedException {
    Table table = TEST_UTIL.createTable(TableName.valueOf("testScan_NullQualifier"), FAMILY);
    TEST_UTIL.waitTableAvailable(TableName.valueOf("testScan_NullQualifier"), 10000);

    Put put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, VALUE);
    table.put(put);

    put = new Put(ROW);
    put.add(FAMILY, null, VALUE);
    table.put(put);
    LOG.info("Row put");

    Scan scan = new Scan();
    scan.addColumn(FAMILY, null);

    ResultScanner scanner = table.getScanner(scan);
    Result[] bar = scanner.next(100);
    assertEquals(1, bar.length);
    assertEquals(1, bar[0].size());

    scan = new Scan();
    scan.addFamily(FAMILY);

    scanner = table.getScanner(scan);
    bar = scanner.next(100);
    assertEquals(1, bar.length);
    assertEquals(2, bar[0].size());
  }
```
```
Cyclomatic Complexity	 2
Assertions		 2
Lines of Code		81
-  
  public void testIllegalTableDescriptor() throws Exception {
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf("testIllegalTableDescriptor"));
    HColumnDescriptor hcd = new HColumnDescriptor(FAMILY);

    // create table with 0 families
    checkTableIsIllegal(htd);
    htd.addFamily(hcd);
    checkTableIsLegal(htd);

    htd.setMaxFileSize(1024); // 1K
    checkTableIsIllegal(htd);
    htd.setMaxFileSize(0);
    checkTableIsIllegal(htd);
    htd.setMaxFileSize(1024 * 1024 * 1024); // 1G
    checkTableIsLegal(htd);

    htd.setMemStoreFlushSize(1024);
    checkTableIsIllegal(htd);
    htd.setMemStoreFlushSize(0);
    checkTableIsIllegal(htd);
    htd.setMemStoreFlushSize(128 * 1024 * 1024); // 128M
    checkTableIsLegal(htd);

    htd.setRegionSplitPolicyClassName("nonexisting.foo.class");
    checkTableIsIllegal(htd);
    htd.setRegionSplitPolicyClassName(null);
    checkTableIsLegal(htd);

    hcd.setBlocksize(0);
    checkTableIsIllegal(htd);
    hcd.setBlocksize(1024 * 1024 * 128); // 128M
    checkTableIsIllegal(htd);
    hcd.setBlocksize(1024);
    checkTableIsLegal(htd);

    hcd.setTimeToLive(0);
    checkTableIsIllegal(htd);
    hcd.setTimeToLive(-1);
    checkTableIsIllegal(htd);
    hcd.setTimeToLive(1);
    checkTableIsLegal(htd);

    hcd.setMinVersions(-1);
    checkTableIsIllegal(htd);
    hcd.setMinVersions(3);
    try {
      hcd.setMaxVersions(2);
      fail();
    } catch (IllegalArgumentException ex) {
      // expected
      hcd.setMaxVersions(10);
    }
    checkTableIsLegal(htd);

    // HBASE-13776 Setting illegal versions for HColumnDescriptor
    //  does not throw IllegalArgumentException
    // finally, minVersions must be less than or equal to maxVersions
    hcd.setMaxVersions(4);
    hcd.setMinVersions(5);
    checkTableIsIllegal(htd);
    hcd.setMinVersions(3);

    try {
      hcd.setScope(-1);
      fail("Illegal value for setScope did not throw");
    } catch (IllegalArgumentException e) {
      // expected
      hcd.setScope(0);
    }
    checkTableIsLegal(htd);

    try {
      hcd.setDFSReplication((short) -1);
      fail("Illegal value for setDFSReplication did not throw");
    } catch (IllegalArgumentException e) {
      // pass
    }
    // set an illegal DFS replication value by hand
    hcd.setValue(HColumnDescriptor.DFS_REPLICATION, "-1");
    checkTableIsIllegal(htd);
    try {
      hcd.setDFSReplication((short) -1);
      fail("Should throw exception if an illegal value is explicitly being set");
    } catch (IllegalArgumentException e) {
      // pass
    }

    // check the conf settings to disable sanity checks
    htd.setMemStoreFlushSize(0);

    // Check that logs warn on invalid table but allow it.
    ListAppender listAppender = new ListAppender();
    Logger log = Logger.getLogger(HMaster.class);
    log.addAppender(listAppender);
    log.setLevel(Level.WARN);

    htd.setConfiguration("hbase.table.sanity.checks", Boolean.FALSE.toString());
    checkTableIsLegal(htd);

    assertFalse(listAppender.getMessages().isEmpty());
    assertTrue(listAppender.getMessages().get(0).startsWith("MEMSTORE_FLUSHSIZE for table "
        + "descriptor or \"hbase.hregion.memstore.flush.size\" (0) is too small, which might "
        + "cause very frequent flushing."));

    log.removeAppender(listAppender);
  }
```
```
Cyclomatic Complexity	 2
Assertions		 0
Lines of Code		21
-  
  public void testGetRegionLocationFromPrimaryMetaRegion() throws IOException, InterruptedException {
    HTU.getHBaseAdmin().setBalancerRunning(false, true);

    ((ConnectionManager.HConnectionImplementation) HTU.getHBaseAdmin().getConnection()).
        setUseMetaReplicas(true);

    // Create table then get the single region for our new table.
    HTableDescriptor hdt = HTU.createTableDescriptor("testGetRegionLocationFromPrimaryMetaRegion");
    hdt.setRegionReplication(2);
    try {

      HTU.createTable(hdt, new byte[][] { f }, null);

      RegionServerHostingPrimayMetaRegionSlowOrStopCopro.slowDownPrimaryMetaScan = true;

      // Get user table location, always get it from the primary meta replica
      RegionLocations url = ((ClusterConnection) HTU.getConnection())
          .locateRegion(hdt.getTableName(), row, false, false);

    } finally {
      RegionServerHostingPrimayMetaRegionSlowOrStopCopro.slowDownPrimaryMetaScan = false;
      ((ConnectionManager.HConnectionImplementation) HTU.getHBaseAdmin().getConnection()).
          setUseMetaReplicas(false);
      HTU.getHBaseAdmin().setBalancerRunning(true, true);
      HTU.getHBaseAdmin().disableTable(hdt.getTableName());
      HTU.deleteTable(hdt.getTableName());
    }
  }
```
```
Cyclomatic Complexity	 7
Assertions		 2
Lines of Code		66
-  
  public void testReplicaGetWithPrimaryAndMetaDown() throws IOException, InterruptedException {
    HTU.getHBaseAdmin().setBalancerRunning(false, true);

    ((ConnectionManager.HConnectionImplementation)HTU.getHBaseAdmin().getConnection()).
        setUseMetaReplicas(true);

    // Create table then get the single region for our new table.
    HTableDescriptor hdt = HTU.createTableDescriptor("testReplicaGetWithPrimaryAndMetaDown");
    hdt.setRegionReplication(2);
    try {

      Table table = HTU.createTable(hdt, new byte[][] { f }, null);

      // Get Meta location
      RegionLocations mrl = ((ClusterConnection) HTU.getConnection())
          .locateRegion(TableName.META_TABLE_NAME,
              HConstants.EMPTY_START_ROW, false, false);

      // Get user table location
      RegionLocations url = ((ClusterConnection) HTU.getConnection())
          .locateRegion(hdt.getTableName(), row, false, false);

      // Make sure that user primary region is co-hosted with the meta region
      if (!url.getDefaultRegionLocation().getServerName().equals(
          mrl.getDefaultRegionLocation().getServerName())) {
        HTU.moveRegionAndWait(url.getDefaultRegionLocation().getRegionInfo(),
            mrl.getDefaultRegionLocation().getServerName());
      }

      // Make sure that the user replica region is not hosted by the same region server with
      // primary
      if (url.getRegionLocation(1).getServerName().equals(mrl.getDefaultRegionLocation()
          .getServerName())) {
        HTU.moveRegionAndWait(url.getRegionLocation(1).getRegionInfo(),
            url.getDefaultRegionLocation().getServerName());
      }

      // Wait until the meta table is updated with new location info
      while (true) {
        mrl = ((ClusterConnection) HTU.getConnection())
            .locateRegion(TableName.META_TABLE_NAME, HConstants.EMPTY_START_ROW, false, false);

        // Get user table location
        url = ((ClusterConnection) HTU.getConnection())
            .locateRegion(hdt.getTableName(), row, false, true);

        LOG.info("meta locations " + mrl);
        LOG.info("table locations " + url);
        ServerName a = url.getDefaultRegionLocation().getServerName();
        ServerName b = mrl.getDefaultRegionLocation().getServerName();
        if(a.equals(b)) {
          break;
        } else {
          LOG.info("Waiting for new region info to be updated in meta table");
          Thread.sleep(100);
        }
      }

      Put p = new Put(row);
      p.addColumn(f, row, row);
      table.put(p);

      // Flush so it can be picked by the replica refresher thread
      HTU.flush(table.getName());

      // Sleep for some time until data is picked up by replicas
      try {
        Thread.sleep(2 * REFRESH_PERIOD);
      } catch (InterruptedException e1) {
        LOG.error(e1);
      }

      // Simulating the RS down
      RegionServerHostingPrimayMetaRegionSlowOrStopCopro.throwException = true;

      // The first Get is supposed to succeed
      Get g = new Get(row);
      g.setConsistency(Consistency.TIMELINE);
      Result r = table.get(g);
      Assert.assertTrue(r.isStale());

      // The second Get will succeed as well
      r = table.get(g);
      Assert.assertTrue(r.isStale());

    } finally {
      ((ConnectionManager.HConnectionImplementation)HTU.getHBaseAdmin().getConnection()).
          setUseMetaReplicas(false);
      RegionServerHostingPrimayMetaRegionSlowOrStopCopro.throwException = false;
      HTU.getHBaseAdmin().setBalancerRunning(true, true);
      HTU.getHBaseAdmin().disableTable(hdt.getTableName());@@@+      HTU.getAdmin().disableTable(hdt.getTableName());@@@       HTU.deleteTable(hdt.getTableName());@@@     }@@@   }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		5
-  
  public void testLegacyWALObserverWriteToWAL() throws Exception {
    final WAL log = wals.getWAL(UNSPECIFIED_REGION, null);
    verifyWritesSeen(log, getCoprocessor(log, SampleRegionWALObserver.Legacy.class), true);
  }
```
```
Cyclomatic Complexity	 2
Assertions		 24
Lines of Code		64
-  
  public void testNonLegacyWALKeysDoNotExplode() throws Exception {
    TableName tableName = TableName.valueOf(TEST_TABLE);
    final HTableDescriptor htd = createBasic3FamilyHTD(Bytes
        .toString(TEST_TABLE));
    final HRegionInfo hri = new HRegionInfo(tableName, null, null);
    MultiVersionConcurrencyControl mvcc = new MultiVersionConcurrencyControl();

    fs.mkdirs(new Path(FSUtils.getTableDir(hbaseRootDir, tableName), hri.getEncodedName()));

    final Configuration newConf = HBaseConfiguration.create(this.conf);

    final WAL wal = wals.getWAL(UNSPECIFIED_REGION, null);
    final SampleRegionWALObserver newApi = getCoprocessor(wal, SampleRegionWALObserver.class);
    newApi.setTestValues(TEST_TABLE, TEST_ROW, null, null, null, null, null, null);
    final SampleRegionWALObserver oldApi = getCoprocessor(wal,
        SampleRegionWALObserver.Legacy.class);
    oldApi.setTestValues(TEST_TABLE, TEST_ROW, null, null, null, null, null, null);

    LOG.debug("ensuring wal entries haven't happened before we start");
    assertFalse(newApi.isPreWALWriteCalled());
    assertFalse(newApi.isPostWALWriteCalled());
    assertFalse(newApi.isPreWALWriteDeprecatedCalled());
    assertFalse(newApi.isPostWALWriteDeprecatedCalled());
    assertFalse(oldApi.isPreWALWriteCalled());
    assertFalse(oldApi.isPostWALWriteCalled());
    assertFalse(oldApi.isPreWALWriteDeprecatedCalled());
    assertFalse(oldApi.isPostWALWriteDeprecatedCalled());

    LOG.debug("writing to WAL with non-legacy keys.");
    final int countPerFamily = 5;
    for (HColumnDescriptor hcd : htd.getFamilies()) {
      addWALEdits(tableName, hri, TEST_ROW, hcd.getName(), countPerFamily,
          EnvironmentEdgeManager.getDelegate(), wal, htd, mvcc);
    }

    LOG.debug("Verify that only the non-legacy CP saw edits.");
    assertTrue(newApi.isPreWALWriteCalled());
    assertTrue(newApi.isPostWALWriteCalled());
    assertFalse(newApi.isPreWALWriteDeprecatedCalled());
    assertFalse(newApi.isPostWALWriteDeprecatedCalled());
    // wish we could test that the log message happened :/
    assertFalse(oldApi.isPreWALWriteCalled());
    assertFalse(oldApi.isPostWALWriteCalled());
    assertFalse(oldApi.isPreWALWriteDeprecatedCalled());
    assertFalse(oldApi.isPostWALWriteDeprecatedCalled());

    LOG.debug("reseting cp state.");
    newApi.setTestValues(TEST_TABLE, TEST_ROW, null, null, null, null, null, null);
    oldApi.setTestValues(TEST_TABLE, TEST_ROW, null, null, null, null, null, null);

    LOG.debug("write a log edit that supports legacy cps.");
    final long now = EnvironmentEdgeManager.currentTime();
    final WALKey legacyKey = new HLogKey(hri.getEncodedNameAsBytes(), hri.getTable(), now, mvcc);
    final WALEdit edit = new WALEdit();
    final byte[] nonce = Bytes.toBytes("1772");
    edit.add(new KeyValue(TEST_ROW, TEST_FAMILY[0], nonce, now, nonce));
    final long txid = wal.append(htd, hri, legacyKey, edit, true);
    wal.sync(txid);

    LOG.debug("Make sure legacy cps can see supported edits after having been skipped.");
    assertTrue("non-legacy WALObserver didn't see pre-write.", newApi.isPreWALWriteCalled());
    assertTrue("non-legacy WALObserver didn't see post-write.", newApi.isPostWALWriteCalled());
    assertFalse("non-legacy WALObserver shouldn't have seen legacy pre-write.",
        newApi.isPreWALWriteDeprecatedCalled());
    assertFalse("non-legacy WALObserver shouldn't have seen legacy post-write.",
        newApi.isPostWALWriteDeprecatedCalled());
    assertTrue("legacy WALObserver didn't see pre-write.", oldApi.isPreWALWriteCalled());
    assertTrue("legacy WALObserver didn't see post-write.", oldApi.isPostWALWriteCalled());
    assertTrue("legacy WALObserver didn't see legacy pre-write.",
        oldApi.isPreWALWriteDeprecatedCalled());
    assertTrue("legacy WALObserver didn't see legacy post-write.",
        oldApi.isPostWALWriteDeprecatedCalled());@@@   }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		22
-  
  public void testRowIndexWithTagsButNoTagsInCell() throws IOException {
    List<KeyValue> kvList = new ArrayList<>();
    byte[] row = new byte[0];
    byte[] family = new byte[0];
    byte[] qualifier = new byte[0];
    byte[] value = new byte[0];
    KeyValue expectedKV = new KeyValue(row, family, qualifier, 1L, Type.Put, value);
    kvList.add(expectedKV);
    DataBlockEncoding encoding = DataBlockEncoding.ROW_INDEX_V1;
    DataBlockEncoder encoder = encoding.getEncoder();
    ByteBuffer encodedBuffer =
        encodeKeyValues(encoding, kvList, getEncodingContext(Algorithm.NONE, encoding));
    HFileContext meta =
        new HFileContextBuilder().withHBaseCheckSum(false).withIncludesMvcc(includesMemstoreTS)
            .withIncludesTags(includesTags).withCompression(Compression.Algorithm.NONE).build();
    DataBlockEncoder.EncodedSeeker seeker =
        encoder.createSeeker(KeyValue.COMPARATOR, encoder.newDataBlockDecodingContext(meta));
    seeker.setCurrentBuffer(encodedBuffer);
    Cell cell = seeker.getKeyValue();
    Assert.assertEquals(expectedKV.getLength(), ((KeyValue) cell).getLength());
  }
```
```
Cyclomatic Complexity	 2
Assertions		 4
Lines of Code		27
-  
  public void compatibilityTest() throws Exception {
    HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
    Path testDir = TEST_UTIL.getDataTestDir();
    TEST_UTIL.getTestFileSystem().mkdirs(testDir);
    String persistencePath = testDir + "/bucket.persistence";
    BucketCache bucketCache =
      new BucketCache("file:" + testDir + "/bucket.cache", capacitySize, constructedBlockSize,
        null, writeThreads, writerQLen, persistencePath);
    long usedSize = bucketCache.getAllocator().getUsedSize();
    assertTrue(usedSize == 0);

    CacheTestUtils.HFileBlockPair[] blocks =
      CacheTestUtils.generateHFileBlocks(constructedBlockSize, 1);
    // Add blocks
    for (CacheTestUtils.HFileBlockPair block : blocks) {
      cacheAndWaitUntilFlushedToBucket(bucketCache, block.getBlockName(), block.getBlock());
    }
    usedSize = bucketCache.getAllocator().getUsedSize();
    assertTrue(usedSize != 0);
    // persistence backingMap using old way
    persistToFileInOldWay(persistencePath + ".old", bucketCache.getMaxSize(),
      bucketCache.backingMap, bucketCache.getDeserialiserMap());
    bucketCache.shutdown();

    // restore cache from file which skip check checksum
    bucketCache =
      new BucketCache("file:" + testDir + "/bucket.cache", capacitySize, constructedBlockSize,
        null, writeThreads, writerQLen, persistencePath + ".old");
    assertEquals(usedSize, bucketCache.getAllocator().getUsedSize());
    assertEquals(blocks.length, bucketCache.backingMap.size());
  }
```
```
Cyclomatic Complexity	 2
Assertions		 2
Lines of Code		13
-  
  public void testReplicaCost() {
    Configuration conf = HBaseConfiguration.create();
    StochasticLoadBalancer.CostFunction
        costFunction = new StochasticLoadBalancer.RegionReplicaHostCostFunction(conf);
    for (int[] mockCluster : clusterStateMocks) {
      BaseLoadBalancer.Cluster cluster = mockCluster(mockCluster);
      costFunction.init(cluster);
      double cost = costFunction.cost();
      assertTrue(cost >= 0);
      assertTrue(cost <= 1.01);
    }
  }
```
```
Cyclomatic Complexity	 2
Assertions		 4
Lines of Code		54
-  
  public void testReplicaCostForReplicas() {
    Configuration conf = HBaseConfiguration.create();
    StochasticLoadBalancer.CostFunction
        costFunction = new StochasticLoadBalancer.RegionReplicaHostCostFunction(conf);

    int [] servers = new int[] {3,3,3,3,3};
    TreeMap<ServerName, List<HRegionInfo>> clusterState = mockClusterServers(servers);

    BaseLoadBalancer.Cluster cluster;

    cluster = new BaseLoadBalancer.Cluster(clusterState, null, null, null);
    costFunction.init(cluster);
    double costWithoutReplicas = costFunction.cost();
    assertEquals(0, costWithoutReplicas, 0);

    // replicate the region from first server to the last server
    HRegionInfo replica1 = RegionReplicaUtil.getRegionInfoForReplica(
      clusterState.firstEntry().getValue().get(0),1);
    clusterState.lastEntry().getValue().add(replica1);

    cluster = new BaseLoadBalancer.Cluster(clusterState, null, null, null);
    costFunction.init(cluster);
    double costWith1ReplicaDifferentServer = costFunction.cost();

    assertEquals(0, costWith1ReplicaDifferentServer, 0);

    // add a third replica to the last server
    HRegionInfo replica2 = RegionReplicaUtil.getRegionInfoForReplica(replica1, 2);
    clusterState.lastEntry().getValue().add(replica2);

    cluster = new BaseLoadBalancer.Cluster(clusterState, null, null, null);
    costFunction.init(cluster);
    double costWith1ReplicaSameServer = costFunction.cost();

    assertTrue(costWith1ReplicaDifferentServer < costWith1ReplicaSameServer);

    // test with replication = 4 for following:

    HRegionInfo replica3;
    Iterator<Entry<ServerName, List<HRegionInfo>>> it;
    Entry<ServerName, List<HRegionInfo>> entry;

    clusterState = mockClusterServers(servers);
    it = clusterState.entrySet().iterator();
    entry = it.next(); //first server
    HRegionInfo hri = entry.getValue().get(0);
    replica1 = RegionReplicaUtil.getRegionInfoForReplica(hri, 1);
    replica2 = RegionReplicaUtil.getRegionInfoForReplica(hri, 2);
    replica3 = RegionReplicaUtil.getRegionInfoForReplica(hri, 3);
    entry.getValue().add(replica1);
    entry.getValue().add(replica2);
    it.next().getValue().add(replica3); //2nd server

    cluster = new BaseLoadBalancer.Cluster(clusterState, null, null, null);
    costFunction.init(cluster);
    double costWith3ReplicasSameServer = costFunction.cost();

    clusterState = mockClusterServers(servers);
    hri = clusterState.firstEntry().getValue().get(0);
    replica1 = RegionReplicaUtil.getRegionInfoForReplica(hri, 1);
    replica2 = RegionReplicaUtil.getRegionInfoForReplica(hri, 2);
    replica3 = RegionReplicaUtil.getRegionInfoForReplica(hri, 3);

    clusterState.firstEntry().getValue().add(replica1);
    clusterState.lastEntry().getValue().add(replica2);
    clusterState.lastEntry().getValue().add(replica3);

    cluster = new BaseLoadBalancer.Cluster(clusterState, null, null, null);
    costFunction.init(cluster);
    double costWith2ReplicasOnTwoServers = costFunction.cost();

    assertTrue(costWith2ReplicasOnTwoServers < costWith3ReplicasSameServer);
  }
```
```
Cyclomatic Complexity	 5
Assertions		 2
Lines of Code		21
-  
  public void testNeedsBalanceForColocatedReplicas() {
    // check for the case where there are two hosts and with one rack, and where
    // both the replicas are hosted on the same server
    List<HRegionInfo> regions = randomRegions(1);
    ServerName s1 = ServerName.valueOf("host1", 1000, 11111);
    ServerName s2 = ServerName.valueOf("host11", 1000, 11111);
    Map<ServerName, List<HRegionInfo>> map = new HashMap<ServerName, List<HRegionInfo>>();
    map.put(s1, regions);
    regions.add(RegionReplicaUtil.getRegionInfoForReplica(regions.get(0), 1));
    // until the step above s1 holds two replicas of a region
    regions = randomRegions(1);
    map.put(s2, regions);
    assertTrue(loadBalancer.needsBalance(new Cluster(map, null, null, null)));
    // check for the case where there are two hosts on the same rack and there are two racks
    // and both the replicas are on the same rack
    map.clear();
    regions = randomRegions(1);
    List<HRegionInfo> regionsOnS2 = new ArrayList<HRegionInfo>(1);
    regionsOnS2.add(RegionReplicaUtil.getRegionInfoForReplica(regions.get(0), 1));
    map.put(s1, regions);
    map.put(s2, regionsOnS2);
    // add another server so that the cluster has some host on another rack
    map.put(ServerName.valueOf("host2", 1000, 11111), randomRegions(1));
    assertTrue(loadBalancer.needsBalance(new Cluster(map, null, null,
        new ForTestRackManagerOne())));
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		9
-   (timeout = 60000)
  public void testSmallCluster() {
    int numNodes = 10;
    int numRegions = 1000;
    int numRegionsPerServer = 40; //all servers except one
    int replication = 1;
    int numTables = 10;
    testWithCluster(numNodes, numRegions, numRegionsPerServer, replication, numTables, true, true);
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		9
-   (timeout = 60000)
  public void testSmallCluster2() {
    int numNodes = 20;
    int numRegions = 2000;
    int numRegionsPerServer = 40; //all servers except one
    int replication = 1;
    int numTables = 10;
    testWithCluster(numNodes, numRegions, numRegionsPerServer, replication, numTables, true, true);
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		9
-   (timeout = 60000)
  public void testSmallCluster3() {
    int numNodes = 20;
    int numRegions = 2000;
    int numRegionsPerServer = 1; // all servers except one
    int replication = 1;
    int numTables = 10;
    /* fails because of max moves */
    testWithCluster(numNodes, numRegions, numRegionsPerServer, replication, numTables, false, false);
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		9
-   (timeout = 800000)
  public void testMidCluster() {
    int numNodes = 100;
    int numRegions = 10000;
    int numRegionsPerServer = 60; // all servers except one
    int replication = 1;
    int numTables = 40;
    testWithCluster(numNodes, numRegions, numRegionsPerServer, replication, numTables, true, true);
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		15
-   (timeout = 800000)
  public void testMidCluster2() {
    int numNodes = 200;
    int numRegions = 100000;
    int numRegionsPerServer = 40; // all servers except one
    int replication = 1;
    int numTables = 400;
    testWithCluster(numNodes,
        numRegions,
        numRegionsPerServer,
        replication,
        numTables,
        false, /* num large num regions means may not always get to best balance with one run */
        false);
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		9
-   (timeout = 800000)
  public void testMidCluster3() {
    int numNodes = 100;
    int numRegions = 2000;
    int numRegionsPerServer = 9; // all servers except one
    int replication = 1;
    int numTables = 110;
    testWithCluster(numNodes, numRegions, numRegionsPerServer, replication, numTables, true, true);
    // TODO(eclark): Make sure that the tables are well distributed.
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		9
-  
  public void testLargeCluster() {
    int numNodes = 1000;
    int numRegions = 100000; //100 regions per RS
    int numRegionsPerServer = 80; //all servers except one
    int numTables = 100;
    int replication = 1;
    testWithCluster(numNodes, numRegions, numRegionsPerServer, replication, numTables, true, true);
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		9
-   (timeout = 800000)
  public void testRegionReplicasOnSmallCluster() {
    int numNodes = 10;
    int numRegions = 1000;
    int replication = 3; // 3 replicas per region
    int numRegionsPerServer = 80; //all regions are mostly balanced
    int numTables = 10;
    testWithCluster(numNodes, numRegions, numRegionsPerServer, replication, numTables, true, true);
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		17
-   (timeout = 800000)
  public void testRegionReplicationOnMidClusterWithRacks() {
    conf.setLong(StochasticLoadBalancer.MAX_STEPS_KEY, 10000000L);
    conf.setFloat("hbase.master.balancer.stochastic.maxMovePercent", 1.0f);
    conf.setLong("hbase.master.balancer.stochastic.maxRunningTime", 120 * 1000); // 120 sec
    loadBalancer.setConf(conf);
    int numNodes = 30;
    int numRegions = numNodes * 30;
    int replication = 3; // 3 replicas per region
    int numRegionsPerServer = 28;
    int numTables = 10;
    int numRacks = 4; // all replicas should be on a different rack
    Map<ServerName, List<HRegionInfo>> serverMap =
        createServerMap(numNodes, numRegions, numRegionsPerServer, replication, numTables);
    RackManager rm = new ForTestRackManager(numRacks);

    testWithCluster(serverMap, rm, false, true);@@@   }
```
```
Cyclomatic Complexity	 2
Assertions		 1
Lines of Code		15
-  (timeout = 15000)
  public void testForDifferntHFileRefsZnodeVersion() throws Exception {
    // 1. Create a file
    Path file = new Path(root, "testForDifferntHFileRefsZnodeVersion");
    fs.createNewFile(file);
    // 2. Assert file is successfully created
    assertTrue("Test file not created!", fs.exists(file));
    ReplicationHFileCleaner cleaner = new ReplicationHFileCleaner();
    cleaner.setConf(conf);

    ReplicationQueuesClient replicationQueuesClient = Mockito.mock(ReplicationQueuesClient.class);
    //Return different znode version for each call
    Mockito.when(replicationQueuesClient.getHFileRefsNodeChangeVersion()).thenReturn(1, 2);

    Class<? extends ReplicationHFileCleaner> cleanerClass = cleaner.getClass();
    Field rqc = cleanerClass.getDeclaredField("rqc");
    rqc.setAccessible(true);
    rqc.set(cleaner, replicationQueuesClient);

    cleaner.isFileDeletable(fs.getFileStatus(file));@@@   }
```
```
Cyclomatic Complexity	 4
Assertions		 10
Lines of Code		49
-  
  public void testMasterOpsWhileSplitting() throws Exception {
    TableName tableName = TableName.valueOf("TestSplit");
    byte[] familyName = Bytes.toBytes("fam");
    try (HTable ht = TEST_UTIL.createTable(tableName, familyName)) {
      TEST_UTIL.loadTable(ht, familyName, false);
    }
    HRegionServer server = TEST_UTIL.getHBaseCluster().getRegionServer(0);
    byte[] firstRow = Bytes.toBytes("aaa");
    byte[] splitRow = Bytes.toBytes("lll");
    byte[] lastRow = Bytes.toBytes("zzz");
    try (Connection conn = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration())) {
      // this will also cache the region
      byte[] regionName = conn.getRegionLocator(tableName).getRegionLocation(splitRow)
          .getRegionInfo().getRegionName();
      Region region = server.getRegion(regionName);
      SplitTransactionImpl split = new SplitTransactionImpl((HRegion) region, splitRow);
      split.prepare();

      // 1. phase I
      PairOfSameType<Region> regions = split.createDaughters(server, server, null);
      assertFalse(test(conn, tableName, firstRow, server));
      assertFalse(test(conn, tableName, lastRow, server));

      // passing null as services prevents final step
      // 2, most of phase II
      split.openDaughters(server, null, regions.getFirst(), regions.getSecond());
      assertFalse(test(conn, tableName, firstRow, server));
      assertFalse(test(conn, tableName, lastRow, server));

      // 3. finish phase II
      // note that this replicates some code from SplitTransaction
      // 2nd daughter first
      if (split.useZKForAssignment) {
        server.postOpenDeployTasks(regions.getSecond());
      } else {
        server.reportRegionStateTransition(
          RegionServerStatusProtos.RegionStateTransition.TransitionCode.SPLIT,
          region.getRegionInfo(), regions.getFirst().getRegionInfo(),
          regions.getSecond().getRegionInfo());
      }

      // first daughter second
      if (split.useZKForAssignment) {
        server.postOpenDeployTasks(regions.getFirst());
      }

      // Add to online regions
      server.addToOnlineRegions(regions.getSecond());
      // THIS is the crucial point:
      // the 2nd daughter was added, so querying before the split key should fail.
      assertFalse(test(conn, tableName, firstRow, server));
      // past splitkey is ok.
      assertTrue(test(conn, tableName, lastRow, server));

      // Add to online regions
      server.addToOnlineRegions(regions.getFirst());
      assertTrue(test(conn, tableName, firstRow, server));
      assertTrue(test(conn, tableName, lastRow, server));

      if (split.useZKForAssignment) {
        // 4. phase III
        ((BaseCoordinatedStateManager) server.getCoordinatedStateManager())
            .getSplitTransactionCoordination().completeSplitTransaction(server, regions.getFirst(),
              regions.getSecond(), split.std, region);
      }

      assertTrue(test(conn, tableName, firstRow, server));
      assertTrue(test(conn, tableName, lastRow, server));
    }
  }
```
```
Cyclomatic Complexity	 4
Assertions		 5
Lines of Code		42
-  
  public void testTableAvailableWhileSplitting() throws Exception {
    TableName tableName = TableName.valueOf("TestTableAvailableWhileSplitting");
    byte[] familyName = Bytes.toBytes("fam");
    try (HTable ht = TEST_UTIL.createTable(tableName, familyName)) {
      TEST_UTIL.loadTable(ht, familyName, false);
    }
    Admin admin = TEST_UTIL.getHBaseAdmin();
    HRegionServer server = TEST_UTIL.getHBaseCluster().getRegionServer(0);
    byte[] splitRow = Bytes.toBytes("lll");
    try (Connection conn = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration())) {
      byte[] regionName = conn.getRegionLocator(tableName).getRegionLocation(splitRow)
          .getRegionInfo().getRegionName();
      Region region = server.getRegion(regionName);
      SplitTransactionImpl split = new SplitTransactionImpl((HRegion) region, splitRow);
      split.prepare();
      assertTrue(admin.isTableAvailable(tableName));

      // 1. phase I
      PairOfSameType<Region> regions = split.createDaughters(server, server, null);
      // Parent should be offline at this stage and daughters not yet open
      assertFalse(admin.isTableAvailable(tableName));

      // passing null as services prevents final step of postOpenDeployTasks
      // 2, most of phase II
      split.openDaughters(server, null, regions.getFirst(), regions.getSecond());
      assertFalse(admin.isTableAvailable(tableName));

      // Finish openeing daughters
      // 2nd daughter first
      if (split.useZKForAssignment) {
        server.postOpenDeployTasks(regions.getSecond());
      } else {
        server.reportRegionStateTransition(
          RegionServerStatusProtos.RegionStateTransition.TransitionCode.SPLIT,
          region.getRegionInfo(), regions.getFirst().getRegionInfo(),
          regions.getSecond().getRegionInfo());
      }

      // first daughter second
      if (split.useZKForAssignment) {
        server.postOpenDeployTasks(regions.getFirst());
      }

      // After postOpenDeploy daughters should have location in meta
      assertTrue(admin.isTableAvailable(tableName));

      server.addToOnlineRegions(regions.getSecond());
      server.addToOnlineRegions(regions.getFirst());
      assertTrue(admin.isTableAvailable(tableName));
    } finally {
      if (admin != null) {
        admin.close();
      }
    }
  }
```
```
Cyclomatic Complexity	 3
Assertions		 7
Lines of Code		45
-  (timeout = 60000)
  public void testSplitFailedCompactionAndSplit() throws Exception {
    final TableName tableName = TableName.valueOf("testSplitFailedCompactionAndSplit");
    Configuration conf = TESTING_UTIL.getConfiguration();
    try {
      HBaseAdmin admin = new HBaseAdmin(conf);
      // Create table then get the single region for our new table.
      HTableDescriptor htd = new HTableDescriptor(tableName);
      byte[] cf = Bytes.toBytes("cf");
      htd.addFamily(new HColumnDescriptor(cf));
      admin.createTable(htd);

      for (int i = 0; cluster.getRegions(tableName).size() == 0 && i < 100; i++) {
        Thread.sleep(100);
      }
      assertEquals(1, cluster.getRegions(tableName).size());

      HRegion region = cluster.getRegions(tableName).get(0);
      Store store = region.getStore(cf);
      int regionServerIndex = cluster.getServerWith(region.getRegionInfo().getRegionName());
      HRegionServer regionServer = cluster.getRegionServer(regionServerIndex);

      Table t = new HTable(conf, tableName);
      // insert data
      insertData(tableName, admin, t);
      insertData(tableName, admin, t);

      int fileNum = store.getStorefiles().size();
      // 0, Compaction Request
      store.triggerMajorCompaction();
      CompactionContext cc = store.requestCompaction();
      assertNotNull(cc);
      // 1, A timeout split
      // 1.1 close region
      assertEquals(2, region.close(false).get(cf).size());
      // 1.2 rollback and Region initialize again
      region.initialize();

      // 2, Run Compaction cc
      assertFalse(region.compact(cc, store, NoLimitThroughputController.INSTANCE));
      assertTrue(fileNum > store.getStorefiles().size());

      // 3, Split
      SplitTransaction st = new SplitTransactionImpl(region, Bytes.toBytes("row3"));
      assertTrue(st.prepare());
      st.execute(regionServer, regionServer);
      LOG.info("Waiting for region to come out of RIT");
      TESTING_UTIL.waitFor(60000, 1000, new Waiter.Predicate<Exception>() {
        @Override
        public boolean evaluate() throws Exception {
          RegionStates regionStates = cluster.getMaster().getAssignmentManager().getRegionStates();
          return !regionStates.isRegionsInTransition();
        }
      });
      assertEquals(2, cluster.getRegions(tableName).size());
    } finally {
      TESTING_UTIL.deleteTable(tableName);
    }
  }
```
```
Cyclomatic Complexity	 1
Assertions		 4
Lines of Code		39
-  (timeout = 120000)
  public void testFailedSplit() throws Exception {
    TableName tableName = TableName.valueOf("testFailedSplit");
    byte[] colFamily = Bytes.toBytes("info");
    TESTING_UTIL.createTable(tableName, colFamily);
    Connection connection = ConnectionFactory.createConnection(TESTING_UTIL.getConfiguration());
    HTable table = (HTable) connection.getTable(tableName);
    try {
      TESTING_UTIL.loadTable(table, colFamily);
      List<HRegionInfo> regions = TESTING_UTIL.getHBaseAdmin().getTableRegions(tableName);
      assertTrue(regions.size() == 1);
      final HRegion actualRegion = cluster.getRegions(tableName).get(0);
      actualRegion.getCoprocessorHost().load(FailingSplitRegionObserver.class,
        Coprocessor.PRIORITY_USER, actualRegion.getBaseConf());

      // The following split would fail.
      admin.split(tableName);
      FailingSplitRegionObserver observer = (FailingSplitRegionObserver) actualRegion
          .getCoprocessorHost().findCoprocessor(FailingSplitRegionObserver.class.getName());
      assertNotNull(observer);
      observer.latch.await();
      observer.postSplit.await();
      LOG.info("Waiting for region to come out of RIT: " + actualRegion);
      TESTING_UTIL.waitFor(60000, 1000, new Waiter.Predicate<Exception>() {
        @Override
        public boolean evaluate() throws Exception {
          RegionStates regionStates = cluster.getMaster().getAssignmentManager().getRegionStates();
          return !regionStates.isRegionsInTransition();
        }
      });
      regions = TESTING_UTIL.getHBaseAdmin().getTableRegions(tableName);
      assertTrue(regions.size() == 1);
      RegionStates regionStates = cluster.getMaster().getAssignmentManager().getRegionStates();
      Set<RegionState> rit = regionStates.getRegionsInTransition();
      assertTrue(rit.size() == 0);
    } finally {
      table.close();
      connection.close();
      TESTING_UTIL.deleteTable(tableName);
    }
  }
```
```
Cyclomatic Complexity	 2
Assertions		 2
Lines of Code		51
-   (timeout=300000)
  public void testSSHCleanupDaugtherRegionsOfAbortedSplit() throws Exception {
    TableName table = TableName.valueOf("testSSHCleanupDaugtherRegionsOfAbortedSplit");
    try {
      HTableDescriptor desc = new HTableDescriptor(table);
      desc.addFamily(new HColumnDescriptor(Bytes.toBytes("f")));
      admin.createTable(desc);
      HTable hTable = new HTable(cluster.getConfiguration(), desc.getTableName());
      for(int i = 1; i < 5; i++) {
        Put p1 = new Put(("r"+i).getBytes());
        p1.add(Bytes.toBytes("f"), "q1".getBytes(), "v".getBytes());
        hTable.put(p1);
      }
      admin.flush(desc.getTableName());
      List<HRegion> regions = cluster.getRegions(desc.getTableName());
      int serverWith = cluster.getServerWith(regions.get(0).getRegionInfo().getRegionName());
      HRegionServer regionServer = cluster.getRegionServer(serverWith);
      cluster.getServerWith(regions.get(0).getRegionInfo().getRegionName());
      SplitTransactionImpl st = new SplitTransactionImpl(regions.get(0), Bytes.toBytes("r3"));
      st.prepare();
      st.stepsBeforePONR(regionServer, regionServer, false);
      Path tableDir =
          FSUtils.getTableDir(cluster.getMaster().getMasterFileSystem().getRootDir(),
            desc.getTableName());
      tableDir.getFileSystem(cluster.getConfiguration());
      List<Path> regionDirs =
          FSUtils.getRegionDirs(tableDir.getFileSystem(cluster.getConfiguration()), tableDir);
      assertEquals(3,regionDirs.size());
      cluster.startRegionServer();
      regionServer.kill();
      cluster.getRegionServerThreads().get(serverWith).join();
      // Wait until finish processing of shutdown
      TESTING_UTIL.waitFor(60000, 1000, new Waiter.Predicate<Exception>() {
        @Override
        public boolean evaluate() throws Exception {
          return !cluster.getMaster().getServerManager().areDeadServersInProgress();
        }
      });
      // Wait until there are no more regions in transition
      TESTING_UTIL.waitFor(60000, 1000, new Waiter.Predicate<Exception>() {
        @Override
        public boolean evaluate() throws Exception {
          return !cluster.getMaster().getAssignmentManager().
              getRegionStates().isRegionsInTransition();
        }
      });
      regionDirs =
          FSUtils.getRegionDirs(tableDir.getFileSystem(cluster.getConfiguration()), tableDir);
      assertEquals(1,regionDirs.size());
    } finally {
      TESTING_UTIL.deleteTable(table);
    }
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		17
-  
  public void testNoEdits() throws Exception {
    TableName tableName = TableName.valueOf("TestLogRollPeriodNoEdits");
    TEST_UTIL.createTable(tableName, "cf");
    try {
      Table table = new HTable(TEST_UTIL.getConfiguration(), tableName);
      try {
        HRegionServer server = TEST_UTIL.getRSForFirstRegionInTable(tableName);
        WAL log = server.getWAL(null);
        checkMinLogRolls(log, 5);
      } finally {
        table.close();
      }
    } finally {
      TEST_UTIL.deleteTable(tableName);
    }
  }
```
```
Cyclomatic Complexity	 2
Assertions		 0
Lines of Code		38
-  (timeout=60000)
  public void testWithEdits() throws Exception {
    final TableName tableName = TableName.valueOf("TestLogRollPeriodWithEdits");
    final String family = "cf";

    TEST_UTIL.createTable(tableName, family);
    try {
      HRegionServer server = TEST_UTIL.getRSForFirstRegionInTable(tableName);
      WAL log = server.getWAL(null);
      final Table table = new HTable(TEST_UTIL.getConfiguration(), tableName);

      Thread writerThread = new Thread("writer") {
        @Override
        public void run() {
          try {
            long row = 0;
            while (!interrupted()) {
              Put p = new Put(Bytes.toBytes(String.format("row%d", row)));
              p.add(Bytes.toBytes(family), Bytes.toBytes("col"), Bytes.toBytes(row));
              table.put(p);
              row++;

              Thread.sleep(LOG_ROLL_PERIOD / 16);
            }
          } catch (Exception e) {
            LOG.warn(e);
          } 
        }
      };

      try {
        writerThread.start();
        checkMinLogRolls(log, 5);
      } finally {
        writerThread.interrupt();
        writerThread.join();
        table.close();
      }  
    } finally {
      TEST_UTIL.deleteTable(tableName);
    }
  }
```
```
Cyclomatic Complexity	 1
Assertions		 5
Lines of Code		30
-  
  public void testZKLockCleaner() throws Exception {
    MiniHBaseCluster cluster = utility1.startMiniCluster(1, 2);
    HTableDescriptor table = new HTableDescriptor(TableName.valueOf(Bytes.toBytes("zk")));
    HColumnDescriptor fam = new HColumnDescriptor(famName);
    fam.setScope(HConstants.REPLICATION_SCOPE_GLOBAL);
    table.addFamily(fam);
    new HBaseAdmin(conf1).createTable(table);
    ReplicationAdmin replicationAdmin = new ReplicationAdmin(conf1);
    ReplicationPeerConfig rpc = new ReplicationPeerConfig();
    rpc.setClusterKey(utility2.getClusterKey());
    replicationAdmin.addPeer("cluster2", rpc, null);
    HRegionServer rs = cluster.getRegionServer(0);
    ReplicationQueuesZKImpl zk = new ReplicationQueuesZKImpl(rs.getZooKeeper(), conf1, rs);
    zk.init(rs.getServerName().toString());
    List<String> replicators = zk.getListOfReplicators();
    assertEquals(2, replicators.size());
    String zNode = cluster.getRegionServer(1).getServerName().toString();

    assertTrue(zk.lockOtherRS(zNode));
    assertTrue(zk.checkLockExists(zNode));
    Thread.sleep(10000);
    assertTrue(zk.checkLockExists(zNode));
    cluster.abortRegionServer(0);
    Thread.sleep(10000);
    HRegionServer rs1 = cluster.getRegionServer(1);
    zk = new ReplicationQueuesZKImpl(rs1.getZooKeeper(), conf1, rs1);
    zk.init(rs1.getServerName().toString());
    assertFalse(zk.checkLockExists(zNode));

    utility1.shutdownMiniCluster();
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		4
-  (timeout=300000)
  public void killOneMasterRS() throws Exception {
    loadTableAndKillRS(utility1);
  }
```
```
Cyclomatic Complexity	 2
Assertions		 4
Lines of Code		19
-  
  public void testWALKeySerialization() throws Exception {
    Map<String, byte[]> attributes = new HashMap<String, byte[]>();
    attributes.put("foo", Bytes.toBytes("foo-value"));
    attributes.put("bar", Bytes.toBytes("bar-value"));
    WALKey key = new WALKey(info.getEncodedNameAsBytes(), tableName,
      System.currentTimeMillis(), 0L, 0L, mvcc, attributes);
    assertEquals(attributes, key.getExtendedAttributes());

    WALProtos.WALKey.Builder builder = key.getBuilder(null);
    WALProtos.WALKey serializedKey = builder.build();

    WALKey deserializedKey = new WALKey();
    deserializedKey.readFieldsFromPb(serializedKey, null);

    //equals() only checks region name, sequence id and write time
    assertEquals(key, deserializedKey);
    //can't use Map.equals() because byte arrays use reference equality
    assertEquals(key.getExtendedAttributes().keySet(),
      deserializedKey.getExtendedAttributes().keySet());
    for (Map.Entry<String, byte[]> entry : deserializedKey.getExtendedAttributes().entrySet()) {
      assertArrayEquals(key.getExtendedAttribute(entry.getKey()), entry.getValue());
    }
  }
```
```
Cyclomatic Complexity	 2
Assertions		 5
Lines of Code		29
-  
  public void testReplicationSourceWALReaderThreadWithFilter() throws Exception {
    final byte[] notReplicatedCf = Bytes.toBytes("notReplicated");
    final Map<TableName, List<String>> tableCfs = new HashMap<>();
    tableCfs.put(tableName, Collections.singletonList(Bytes.toString(family)));
    ReplicationPeer peer = mock(ReplicationPeer.class);
    when(peer.getTableCFs()).thenReturn(tableCfs);
    WALEntryFilter filter = new ChainWALEntryFilter(new TableCfWALEntryFilter(peer));

    // add filterable entries
    appendToLogPlus(3, notReplicatedCf);
    appendToLogPlus(3, notReplicatedCf);
    appendToLogPlus(3, notReplicatedCf);

    // add non filterable entries
    appendEntriesToLog(2);

    ReplicationSourceManager mockSourceManager = mock(ReplicationSourceManager.class);
    when(mockSourceManager.getTotalBufferUsed()).thenReturn(new AtomicLong(0));
    final ReplicationSourceWALReaderThread reader =
            new ReplicationSourceWALReaderThread(mockSourceManager, getQueueInfo(), walQueue,
                    0, fs, conf, filter, new MetricsSource("1"));
    reader.start();

    WALEntryBatch entryBatch = reader.take();

    assertNotNull(entryBatch);
    assertFalse(entryBatch.isEmpty());
    List<Entry> walEntries = entryBatch.getWalEntries();
    assertEquals(2, walEntries.size());
    for (Entry entry : walEntries) {
      ArrayList<Cell> cells = entry.getEdit().getCells();
      assertTrue(cells.size() == 1);
      assertTrue(CellUtil.matchingFamily(cells.get(0), family));
    }
  }
```
```
Cyclomatic Complexity	 -1
Assertions		 7
Lines of Code		34
-  
  public void testReplicationSourceWALReaderThreadWithFilterWhenLogRolled() throws Exception {
    final byte[] notReplicatedCf = Bytes.toBytes("notReplicated");
    final Map<TableName, List<String>> tableCfs = new HashMap<>();
    tableCfs.put(tableName, Collections.singletonList(Bytes.toString(family)));
    ReplicationPeer peer = mock(ReplicationPeer.class);
    when(peer.getTableCFs()).thenReturn(tableCfs);
    WALEntryFilter filter = new ChainWALEntryFilter(new TableCfWALEntryFilter(peer));

    appendToLogPlus(3, notReplicatedCf);

    Path firstWAL = walQueue.peek();
    final long eof = getPosition(firstWAL);

    ReplicationSourceManager mockSourceManager = mock(ReplicationSourceManager.class);
    when(mockSourceManager.getTotalBufferUsed()).thenReturn(new AtomicLong(0));
    final ReplicationSourceWALReaderThread reader =
            new ReplicationSourceWALReaderThread(mockSourceManager, getQueueInfo(), walQueue,
                    0, fs, conf, filter, new MetricsSource("1"));
    reader.start();

    // reader won't put any batch, even if EOF reached.
    Waiter.waitFor(conf, 20000, new Waiter.Predicate<Exception>() {
      @Override public boolean evaluate() {
        return reader.getLastReadPosition() >= eof;
      }
    });
    assertNull(reader.poll(0));

    log.rollWriter();

    // should get empty batch with current wal position, after wal rolled
    WALEntryBatch entryBatch = reader.take();

    Path lastWAL= walQueue.peek();
    long positionToBeLogged = getPosition(lastWAL);

    assertNotNull(entryBatch);
    assertTrue(entryBatch.isEmpty());
    assertEquals(1, walQueue.size());
    assertNotEquals(firstWAL, entryBatch.getLastWalPath());
    assertEquals(lastWAL, entryBatch.getLastWalPath());
    assertEquals(positionToBeLogged, entryBatch.getLastWalPosition());
  }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		13
-   (timeout=180000)
  public void testSplit() throws Exception {
    AccessTestAction action = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preSplit(ObserverContext.createAndPrepare(RCP_ENV, null));
        return null;
      }
    };

    verifyAllowed(action, SUPERUSER, USER_ADMIN, USER_OWNER, USER_GROUP_ADMIN);
    verifyDenied(action, USER_CREATE, USER_RW, USER_RO, USER_NONE, USER_GROUP_READ,
      USER_GROUP_WRITE, USER_GROUP_CREATE);
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		7
-  
  public void testMigration() throws DeserializationException {
    Configuration conf = UTIL.getConfiguration();
    ListMultimap<String,TablePermission> permissions = createPermissions();
    byte [] bytes = writePermissionsAsBytes(permissions, conf);
    AccessControlLists.readPermissions(bytes, conf);
  }
```
```
Cyclomatic Complexity	 -1
Assertions		 3
Lines of Code		37
-  
  public void testTokenAuthentication() throws Exception {
    UserGroupInformation testuser =
        UserGroupInformation.createUserForTesting("testuser", new String[]{"testgroup"});

    testuser.setAuthenticationMethod(
        UserGroupInformation.AuthenticationMethod.TOKEN);
    final Configuration conf = TEST_UTIL.getConfiguration();
    UserGroupInformation.setConfiguration(conf);
    Token<AuthenticationTokenIdentifier> token =
        secretManager.generateToken("testuser");
    LOG.debug("Got token: " + token.toString());
    testuser.addToken(token);

    // verify the server authenticates us as this token user
    testuser.doAs(new PrivilegedExceptionAction<Object>() {
      public Object run() throws Exception {
        Configuration c = server.getConfiguration();
        RpcClient rpcClient = RpcClientFactory.createClient(c, clusterId.toString());
        ServerName sn =
            ServerName.valueOf(server.getAddress().getHostName(), server.getAddress().getPort(),
                System.currentTimeMillis());
        try {
          BlockingRpcChannel channel = rpcClient.createBlockingRpcChannel(sn,
              User.getCurrent(), HConstants.DEFAULT_HBASE_RPC_TIMEOUT);
          AuthenticationProtos.AuthenticationService.BlockingInterface stub =
              AuthenticationProtos.AuthenticationService.newBlockingStub(channel);
          AuthenticationProtos.WhoAmIResponse response =
              stub.whoAmI(null, AuthenticationProtos.WhoAmIRequest.getDefaultInstance());
          String myname = response.getUsername();
          assertEquals("testuser", myname);
          String authMethod = response.getAuthMethod();
          assertEquals("TOKEN", authMethod);
        } finally {
          rpcClient.close();
        }
        return null;
      }
    });
  }
```
```
Cyclomatic Complexity	 2
Assertions		 0
Lines of Code		4
-  
  public void testThriftServerHttpTraceForbiddenWhenOptionsDisabled() throws Exception {
    // HTTP TRACE method should be disabled for security
    // See https://www.owasp.org/index.php/Cross_Site_Tracing
    checkHttpMethods("TRACE", HttpURLConnection.HTTP_FORBIDDEN);
  }
```
```
Cyclomatic Complexity	 2
Assertions		 0
Lines of Code		6
-  
  public void testThriftServerHttpTraceForbiddenWhenOptionsEnabled() throws Exception {
    // HTTP TRACE method should be disabled for security
    // See https://www.owasp.org/index.php/Cross_Site_Tracing
    TEST_UTIL.getConfiguration().setBoolean(ThriftServerRunner.THRIFT_HTTP_ALLOW_OPTIONS_METHOD,
        true);
    checkHttpMethods("TRACE", HttpURLConnection.HTTP_FORBIDDEN);
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		5
-  
  public void testThriftServerHttpOptionsForbiddenWhenOptionsDisabled() throws Exception {
    // HTTP OPTIONS method should be disabled by default, so we make sure
    // hbase.thrift.http.allow.options.method is not set anywhere in the config
    TEST_UTIL.getConfiguration().unset(ThriftServerRunner.THRIFT_HTTP_ALLOW_OPTIONS_METHOD);
    checkHttpMethods("OPTIONS", HttpURLConnection.HTTP_FORBIDDEN);
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		6
-  
  public void testThriftServerHttpOptionsOkWhenOptionsEnabled() throws Exception {
    TEST_UTIL.getConfiguration().setBoolean(ThriftServerRunner.THRIFT_HTTP_ALLOW_OPTIONS_METHOD,
        true);
    checkHttpMethods("OPTIONS", HttpURLConnection.HTTP_OK);
  }
```
## 679dd7b8f906b58d709200a48c38c41873fa5fc2 ##
```
Cyclomatic Complexity	1
Assertions		4
Lines of Code		14
-  
  public void testErrorNotGzipped() throws Exception {
    Header[] headers = new Header[2];
    headers[0] = new Header("Accept", Constants.MIMETYPE_BINARY);
    headers[1] = new Header("Accept-Encoding", "gzip");
    Response response = client.get("/" + TABLE + "/" + ROW_1 + "/" + COLUMN_2, headers);
    assertEquals(404, response.getCode());
    String contentEncoding = response.getHeader("Content-Encoding");
    assertTrue(contentEncoding == null || !contentEncoding.contains("gzip"));
    response = client.get("/" + TABLE, headers);
    assertEquals(405, response.getCode());
    contentEncoding = response.getHeader("Content-Encoding");
    assertTrue(contentEncoding == null || !contentEncoding.contains("gzip"));
  }
```
```
Cyclomatic Complexity	 1
Assertions		 6
Lines of Code		11
-  
  public void testValueOfNamespaceAndQualifier() {
    TableName name0 = TableName.valueOf("table");
    TableName name1 = TableName.valueOf("table", "table");
    assertEquals(NamespaceDescriptor.DEFAULT_NAMESPACE_NAME_STR, name0.getNamespaceAsString());
    assertEquals("table", name0.getQualifierAsString());
    assertEquals("table", name0.getNameAsString());
    assertEquals("table", name1.getNamespaceAsString());
    assertEquals("table", name1.getQualifierAsString());
    assertEquals("table:table", name1.getNameAsString());
  }
```
```
Cyclomatic Complexity	 1
Assertions		 12
Lines of Code		186
-  
  public void testVersions() throws Exception {
    byte [] TABLE = Bytes.toBytes("testVersions");@@@+      // Verify we can get each one properly@@@+      getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);@@@+      getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);@@@+      getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);@@@+      getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[7], VALUES[7]);@@@+      scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);@@@+      scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);@@@+      scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);@@@+      scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[7], VALUES[7]);@@@ 
    long [] STAMPS = makeStamps(20);
    byte [][] VALUES = makeNAscii(VALUE, 20);@@@+      // Verify we don't accidentally get others@@@+      getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[0]);@@@+      getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[9]);@@@+      scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[0]);@@@+      scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[9]);@@@ 
    Table ht = TEST_UTIL.createTable(TABLE, FAMILY, 10);
    TEST_UTIL.waitTableAvailable(TABLE, 10000);
    // Insert 4 versions of same column
    Put put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    put.add(FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    put.add(FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    put.add(FAMILY, QUALIFIER, STAMPS[5], VALUES[5]);
    ht.put(put);@@@+      // Ensure maxVersions of table is respected@@@ 
    // Verify we can get each one properly
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[5], VALUES[5]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[5], VALUES[5]);@@@+      TEST_UTIL.flush();@@@ 
    // Verify we don't accidentally get others
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[0]);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[3]);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[6]);
    scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[0]);
    scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[3]);
    scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[6]);

    // Ensure maxVersions in query is respected
    Get get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIER);
    get.setMaxVersions(2);
    Result result = ht.get(get);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[4], STAMPS[5]},
        new byte[][] {VALUES[4], VALUES[5]},
        0, 1);

    Scan scan = new Scan(ROW);
    scan.addColumn(FAMILY, QUALIFIER);
    scan.setMaxVersions(2);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[4], STAMPS[5]},
        new byte[][] {VALUES[4], VALUES[5]},
        0, 1);

    // Flush and redo

    TEST_UTIL.flush();

    // Verify we can get each one properly
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[5], VALUES[5]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[5], VALUES[5]);

    // Verify we don't accidentally get others
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[0]);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[3]);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[6]);
    scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[0]);
    scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[3]);
    scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[6]);

    // Ensure maxVersions in query is respected
    get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIER);
    get.setMaxVersions(2);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[4], STAMPS[5]},
        new byte[][] {VALUES[4], VALUES[5]},
        0, 1);

    scan = new Scan(ROW);
    scan.addColumn(FAMILY, QUALIFIER);
    scan.setMaxVersions(2);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[4], STAMPS[5]},
        new byte[][] {VALUES[4], VALUES[5]},
        0, 1);


    // Add some memstore and retest

    // Insert 4 more versions of same column and a dupe
    put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, STAMPS[3], VALUES[3]);
    put.add(FAMILY, QUALIFIER, STAMPS[6], VALUES[6]);
    put.add(FAMILY, QUALIFIER, STAMPS[7], VALUES[7]);
    put.add(FAMILY, QUALIFIER, STAMPS[8], VALUES[8]);
    ht.put(put);

    // Ensure maxVersions in query is respected
    get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIER);
    get.setMaxVersions();
    result = ht.get(get);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[1], STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7], STAMPS[8]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3], VALUES[4], VALUES[5], VALUES[6], VALUES[7], VALUES[8]},
        0, 7);

    scan = new Scan(ROW);
    scan.addColumn(FAMILY, QUALIFIER);
    scan.setMaxVersions();
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[1], STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7], STAMPS[8]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3], VALUES[4], VALUES[5], VALUES[6], VALUES[7], VALUES[8]},
        0, 7);

    get = new Get(ROW);
    get.setMaxVersions();
    result = ht.get(get);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[1], STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7], STAMPS[8]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3], VALUES[4], VALUES[5], VALUES[6], VALUES[7], VALUES[8]},
        0, 7);

    scan = new Scan(ROW);
    scan.setMaxVersions();
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[1], STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7], STAMPS[8]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3], VALUES[4], VALUES[5], VALUES[6], VALUES[7], VALUES[8]},
        0, 7);

    // Verify we can get each one properly
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[7], VALUES[7]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[7], VALUES[7]);

    // Verify we don't accidentally get others
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[0]);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[9]);
    scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[0]);
    scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[9]);

    // Ensure maxVersions of table is respected

    TEST_UTIL.flush();

    // Insert 4 more versions of same column and a dupe
    put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, STAMPS[9], VALUES[9]);
    put.add(FAMILY, QUALIFIER, STAMPS[11], VALUES[11]);
    put.add(FAMILY, QUALIFIER, STAMPS[13], VALUES[13]);
    put.add(FAMILY, QUALIFIER, STAMPS[15], VALUES[15]);
    ht.put(put);

    get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIER);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7], STAMPS[8], STAMPS[9], STAMPS[11], STAMPS[13], STAMPS[15]},
        new byte[][] {VALUES[3], VALUES[4], VALUES[5], VALUES[6], VALUES[7], VALUES[8], VALUES[9], VALUES[11], VALUES[13], VALUES[15]},
        0, 9);

    scan = new Scan(ROW);
    scan.addColumn(FAMILY, QUALIFIER);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7], STAMPS[8], STAMPS[9], STAMPS[11], STAMPS[13], STAMPS[15]},
        new byte[][] {VALUES[3], VALUES[4], VALUES[5], VALUES[6], VALUES[7], VALUES[8], VALUES[9], VALUES[11], VALUES[13], VALUES[15]},
        0, 9);

    // Delete a version in the memstore and a version in a storefile
    Delete delete = new Delete(ROW);
    delete.deleteColumn(FAMILY, QUALIFIER, STAMPS[11]);
    delete.deleteColumn(FAMILY, QUALIFIER, STAMPS[7]);
    ht.delete(delete);

    // Test that it's gone
    get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIER);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[1], STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[8], STAMPS[9], STAMPS[13], STAMPS[15]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3], VALUES[4], VALUES[5], VALUES[6], VALUES[8], VALUES[9], VALUES[13], VALUES[15]},
        0, 9);

    scan = new Scan(ROW);
    scan.addColumn(FAMILY, QUALIFIER);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[1], STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[8], STAMPS[9], STAMPS[13], STAMPS[15]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3], VALUES[4], VALUES[5], VALUES[6], VALUES[8], VALUES[9], VALUES[13], VALUES[15]},
        0, 9);

  }
```
```
Cyclomatic Complexity	 1
Assertions		 18
Lines of Code		163
-  
  public void testVersionLimits() throws Exception {
    byte [] TABLE = Bytes.toBytes("testVersionLimits");
    byte [][] FAMILIES = makeNAscii(FAMILY, 3);
    int [] LIMITS = {1,3,5};
    long [] STAMPS = makeStamps(10);
    byte [][] VALUES = makeNAscii(VALUE, 10);
    Table ht = TEST_UTIL.createTable(TABLE, FAMILIES, LIMITS);
    TEST_UTIL.waitTableAvailable(TABLE, 10000);
    // Insert limit + 1 on each family
    Put put = new Put(ROW);
    put.add(FAMILIES[0], QUALIFIER, STAMPS[0], VALUES[0]);
    put.add(FAMILIES[0], QUALIFIER, STAMPS[1], VALUES[1]);
    put.add(FAMILIES[1], QUALIFIER, STAMPS[0], VALUES[0]);
    put.add(FAMILIES[1], QUALIFIER, STAMPS[1], VALUES[1]);
    put.add(FAMILIES[1], QUALIFIER, STAMPS[2], VALUES[2]);
    put.add(FAMILIES[1], QUALIFIER, STAMPS[3], VALUES[3]);
    put.add(FAMILIES[2], QUALIFIER, STAMPS[0], VALUES[0]);
    put.add(FAMILIES[2], QUALIFIER, STAMPS[1], VALUES[1]);
    put.add(FAMILIES[2], QUALIFIER, STAMPS[2], VALUES[2]);
    put.add(FAMILIES[2], QUALIFIER, STAMPS[3], VALUES[3]);
    put.add(FAMILIES[2], QUALIFIER, STAMPS[4], VALUES[4]);
    put.add(FAMILIES[2], QUALIFIER, STAMPS[5], VALUES[5]);
    put.add(FAMILIES[2], QUALIFIER, STAMPS[6], VALUES[6]);
    ht.put(put);

    // Verify we only get the right number out of each

    // Family0

    Get get = new Get(ROW);
    get.addColumn(FAMILIES[0], QUALIFIER);
    get.setMaxVersions(Integer.MAX_VALUE);
    Result result = ht.get(get);
    assertNResult(result, ROW, FAMILIES[0], QUALIFIER,
        new long [] {STAMPS[1]},
        new byte[][] {VALUES[1]},
        0, 0);

    get = new Get(ROW);
    get.addFamily(FAMILIES[0]);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILIES[0], QUALIFIER,
        new long [] {STAMPS[1]},
        new byte[][] {VALUES[1]},
        0, 0);

    Scan scan = new Scan(ROW);
    scan.addColumn(FAMILIES[0], QUALIFIER);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILIES[0], QUALIFIER,
        new long [] {STAMPS[1]},
        new byte[][] {VALUES[1]},
        0, 0);

    scan = new Scan(ROW);
    scan.addFamily(FAMILIES[0]);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILIES[0], QUALIFIER,
        new long [] {STAMPS[1]},
        new byte[][] {VALUES[1]},
        0, 0);

    // Family1

    get = new Get(ROW);
    get.addColumn(FAMILIES[1], QUALIFIER);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILIES[1], QUALIFIER,
        new long [] {STAMPS[1], STAMPS[2], STAMPS[3]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3]},
        0, 2);

    get = new Get(ROW);
    get.addFamily(FAMILIES[1]);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILIES[1], QUALIFIER,
        new long [] {STAMPS[1], STAMPS[2], STAMPS[3]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3]},
        0, 2);

    scan = new Scan(ROW);
    scan.addColumn(FAMILIES[1], QUALIFIER);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILIES[1], QUALIFIER,
        new long [] {STAMPS[1], STAMPS[2], STAMPS[3]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3]},
        0, 2);

    scan = new Scan(ROW);
    scan.addFamily(FAMILIES[1]);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILIES[1], QUALIFIER,
        new long [] {STAMPS[1], STAMPS[2], STAMPS[3]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3]},
        0, 2);

    // Family2

    get = new Get(ROW);
    get.addColumn(FAMILIES[2], QUALIFIER);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILIES[2], QUALIFIER,
        new long [] {STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6]},
        new byte[][] {VALUES[2], VALUES[3], VALUES[4], VALUES[5], VALUES[6]},
        0, 4);

    get = new Get(ROW);
    get.addFamily(FAMILIES[2]);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILIES[2], QUALIFIER,
        new long [] {STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6]},
        new byte[][] {VALUES[2], VALUES[3], VALUES[4], VALUES[5], VALUES[6]},
        0, 4);

    scan = new Scan(ROW);
    scan.addColumn(FAMILIES[2], QUALIFIER);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILIES[2], QUALIFIER,
        new long [] {STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6]},
        new byte[][] {VALUES[2], VALUES[3], VALUES[4], VALUES[5], VALUES[6]},
        0, 4);

    scan = new Scan(ROW);
    scan.addFamily(FAMILIES[2]);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILIES[2], QUALIFIER,
        new long [] {STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6]},
        new byte[][] {VALUES[2], VALUES[3], VALUES[4], VALUES[5], VALUES[6]},
        0, 4);

    // Try all families

    get = new Get(ROW);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertTrue("Expected 9 keys but received " + result.size(),
        result.size() == 9);

    get = new Get(ROW);
    get.addFamily(FAMILIES[0]);
    get.addFamily(FAMILIES[1]);
    get.addFamily(FAMILIES[2]);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertTrue("Expected 9 keys but received " + result.size(),
        result.size() == 9);

    get = new Get(ROW);
    get.addColumn(FAMILIES[0], QUALIFIER);
    get.addColumn(FAMILIES[1], QUALIFIER);
    get.addColumn(FAMILIES[2], QUALIFIER);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertTrue("Expected 9 keys but received " + result.size(),
        result.size() == 9);

    scan = new Scan(ROW);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertTrue("Expected 9 keys but received " + result.size(),
        result.size() == 9);

    scan = new Scan(ROW);
    scan.setMaxVersions(Integer.MAX_VALUE);
    scan.addFamily(FAMILIES[0]);
    scan.addFamily(FAMILIES[1]);
    scan.addFamily(FAMILIES[2]);
    result = getSingleScanResult(ht, scan);
    assertTrue("Expected 9 keys but received " + result.size(),
        result.size() == 9);

    scan = new Scan(ROW);
    scan.setMaxVersions(Integer.MAX_VALUE);
    scan.addColumn(FAMILIES[0], QUALIFIER);
    scan.addColumn(FAMILIES[1], QUALIFIER);
    scan.addColumn(FAMILIES[2], QUALIFIER);
    result = getSingleScanResult(ht, scan);
    assertTrue("Expected 9 keys but received " + result.size(),
        result.size() == 9);

  }
```
```
Cyclomatic Complexity	 5
Assertions		 2
Lines of Code		33
-  
  public void testDeleteFamilyVersion() throws Exception {
    HBaseAdmin admin = new HBaseAdmin(TEST_UTIL.getConfiguration());
    byte [] TABLE = Bytes.toBytes("testDeleteFamilyVersion");

    byte [][] QUALIFIERS = makeNAscii(QUALIFIER, 1);
    byte [][] VALUES = makeN(VALUE, 5);
    long [] ts = {1000, 2000, 3000, 4000, 5000};

    Table ht = TEST_UTIL.createTable(TABLE, FAMILY, 5);
    TEST_UTIL.waitTableAvailable(TABLE, 10000);

    Put put = new Put(ROW);
    for (int q = 0; q < 1; q++)
      for (int t = 0; t < 5; t++)
        put.add(FAMILY, QUALIFIERS[q], ts[t], VALUES[t]);
    ht.put(put);
    admin.flush(TABLE);

    Delete delete = new Delete(ROW);
    delete.deleteFamilyVersion(FAMILY, ts[1]);  // delete version '2000'
    delete.deleteFamilyVersion(FAMILY, ts[3]);  // delete version '4000'
    ht.delete(delete);
    admin.flush(TABLE);

    for (int i = 0; i < 1; i++) {
      Get get = new Get(ROW);
      get.addColumn(FAMILY, QUALIFIERS[i]);
      get.setMaxVersions(Integer.MAX_VALUE);
      Result result = ht.get(get);
      // verify version '1000'/'3000'/'5000' remains for all columns
      assertNResult(result, ROW, FAMILY, QUALIFIERS[i],
          new long [] {ts[0], ts[2], ts[4]},
          new byte[][] {VALUES[0], VALUES[2], VALUES[4]},
          0, 2);
    }
    ht.close();
    admin.close();
  }
```
```
Cyclomatic Complexity	 7
Assertions		 7
Lines of Code		89
-  
  public void testDeleteFamilyVersionWithOtherDeletes() throws Exception {
    byte [] TABLE = Bytes.toBytes("testDeleteFamilyVersionWithOtherDeletes");

    byte [][] QUALIFIERS = makeNAscii(QUALIFIER, 5);
    byte [][] VALUES = makeN(VALUE, 5);
    long [] ts = {1000, 2000, 3000, 4000, 5000};

    HBaseAdmin admin = new HBaseAdmin(TEST_UTIL.getConfiguration());
    Table ht = TEST_UTIL.createTable(TABLE, FAMILY, 5);
    TEST_UTIL.waitTableAvailable(TABLE, 10000);

    Put put = null;
    Result result = null;
    Get get = null;
    Delete delete = null;

    // 1. put on ROW
    put = new Put(ROW);
    for (int q = 0; q < 5; q++)
      for (int t = 0; t < 5; t++)
        put.add(FAMILY, QUALIFIERS[q], ts[t], VALUES[t]);
    ht.put(put);
    admin.flush(TABLE);

    // 2. put on ROWS[0]
    byte [] ROW2 = Bytes.toBytes("myRowForTest");
    put = new Put(ROW2);
    for (int q = 0; q < 5; q++)
      for (int t = 0; t < 5; t++)
        put.add(FAMILY, QUALIFIERS[q], ts[t], VALUES[t]);
    ht.put(put);
    admin.flush(TABLE);

    // 3. delete on ROW
    delete = new Delete(ROW);
    // delete version <= 2000 of all columns
    // note: deleteFamily must be the first since it will mask
    // the subsequent other type deletes!
    delete.deleteFamily(FAMILY, ts[1]);
    // delete version '4000' of all columns
    delete.deleteFamilyVersion(FAMILY, ts[3]);
   // delete version <= 3000 of column 0
    delete.deleteColumns(FAMILY, QUALIFIERS[0], ts[2]);
    // delete version <= 5000 of column 2
    delete.deleteColumns(FAMILY, QUALIFIERS[2], ts[4]);
    // delete version 5000 of column 4
    delete.deleteColumn(FAMILY, QUALIFIERS[4], ts[4]);
    ht.delete(delete);
    admin.flush(TABLE);

     // 4. delete on ROWS[0]
    delete = new Delete(ROW2);
    delete.deleteFamilyVersion(FAMILY, ts[1]);  // delete version '2000'
    delete.deleteFamilyVersion(FAMILY, ts[3]);  // delete version '4000'
    ht.delete(delete);
    admin.flush(TABLE);

    // 5. check ROW
    get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIERS[0]);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILY, QUALIFIERS[0],
        new long [] {ts[4]},
        new byte[][] {VALUES[4]},
        0, 0);

    get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIERS[1]);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILY, QUALIFIERS[1],
        new long [] {ts[2], ts[4]},
        new byte[][] {VALUES[2], VALUES[4]},
        0, 1);

    get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIERS[2]);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertEquals(0, result.size());

    get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIERS[3]);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILY, QUALIFIERS[3],
        new long [] {ts[2], ts[4]},
        new byte[][] {VALUES[2], VALUES[4]},
        0, 1);

    get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIERS[4]);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILY, QUALIFIERS[4],
        new long [] {ts[2]},
        new byte[][] {VALUES[2]},
        0, 0);

    // 6. check ROWS[0]
    for (int i = 0; i < 5; i++) {
      get = new Get(ROW2);
      get.addColumn(FAMILY, QUALIFIERS[i]);
      get.setMaxVersions(Integer.MAX_VALUE);
      result = ht.get(get);
      // verify version '1000'/'3000'/'5000' remains for all columns
      assertNResult(result, ROW2, FAMILY, QUALIFIERS[i],
          new long [] {ts[0], ts[2], ts[4]},
          new byte[][] {VALUES[0], VALUES[2], VALUES[4]},
          0, 2);
    }
    ht.close();
    admin.close();
  }
```
```
Cyclomatic Complexity	 10
Assertions		 29
Lines of Code		259
-  
  public void testDeletes() throws Exception {
    byte [] TABLE = Bytes.toBytes("testDeletes");

    byte [][] ROWS = makeNAscii(ROW, 6);
    byte [][] FAMILIES = makeNAscii(FAMILY, 3);
    byte [][] VALUES = makeN(VALUE, 5);
    long [] ts = {1000, 2000, 3000, 4000, 5000};

    Table ht = TEST_UTIL.createTable(TABLE, FAMILIES, 3);
    TEST_UTIL.waitTableAvailable(TABLE, 10000);

    Put put = new Put(ROW);
    put.add(FAMILIES[0], QUALIFIER, ts[0], VALUES[0]);
    put.add(FAMILIES[0], QUALIFIER, ts[1], VALUES[1]);
    ht.put(put);

    Delete delete = new Delete(ROW);
    delete.deleteFamily(FAMILIES[0], ts[0]);
    ht.delete(delete);

    Get get = new Get(ROW);
    get.addFamily(FAMILIES[0]);
    get.setMaxVersions(Integer.MAX_VALUE);
    Result result = ht.get(get);
    assertNResult(result, ROW, FAMILIES[0], QUALIFIER,
        new long [] {ts[1]},
        new byte[][] {VALUES[1]},
        0, 0);

    Scan scan = new Scan(ROW);
    scan.addFamily(FAMILIES[0]);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILIES[0], QUALIFIER,
        new long [] {ts[1]},
        new byte[][] {VALUES[1]},
        0, 0);

    // Test delete latest version
    put = new Put(ROW);
    put.add(FAMILIES[0], QUALIFIER, ts[4], VALUES[4]);
    put.add(FAMILIES[0], QUALIFIER, ts[2], VALUES[2]);
    put.add(FAMILIES[0], QUALIFIER, ts[3], VALUES[3]);
    put.add(FAMILIES[0], null, ts[4], VALUES[4]);
    put.add(FAMILIES[0], null, ts[2], VALUES[2]);
    put.add(FAMILIES[0], null, ts[3], VALUES[3]);
    ht.put(put);

    delete = new Delete(ROW);
    delete.deleteColumn(FAMILIES[0], QUALIFIER); // ts[4]
    ht.delete(delete);

    get = new Get(ROW);
    get.addColumn(FAMILIES[0], QUALIFIER);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILIES[0], QUALIFIER,
        new long [] {ts[1], ts[2], ts[3]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3]},
        0, 2);

    scan = new Scan(ROW);
    scan.addColumn(FAMILIES[0], QUALIFIER);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILIES[0], QUALIFIER,
        new long [] {ts[1], ts[2], ts[3]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3]},
        0, 2);

    // Test for HBASE-1847
    delete = new Delete(ROW);
    delete.deleteColumn(FAMILIES[0], null);
    ht.delete(delete);

    // Cleanup null qualifier
    delete = new Delete(ROW);
    delete.deleteColumns(FAMILIES[0], null);
    ht.delete(delete);

    // Expected client behavior might be that you can re-put deleted values
    // But alas, this is not to be.  We can't put them back in either case.

    put = new Put(ROW);
    put.add(FAMILIES[0], QUALIFIER, ts[0], VALUES[0]); // 1000
    put.add(FAMILIES[0], QUALIFIER, ts[4], VALUES[4]); // 5000
    ht.put(put);


    // It used to be due to the internal implementation of Get, that
    // the Get() call would return ts[4] UNLIKE the Scan below. With
    // the switch to using Scan for Get this is no longer the case.
    get = new Get(ROW);
    get.addFamily(FAMILIES[0]);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILIES[0], QUALIFIER,
        new long [] {ts[1], ts[2], ts[3]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3]},
        0, 2);

    // The Scanner returns the previous values, the expected-naive-unexpected behavior

    scan = new Scan(ROW);
    scan.addFamily(FAMILIES[0]);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILIES[0], QUALIFIER,
        new long [] {ts[1], ts[2], ts[3]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3]},
        0, 2);

    // Test deleting an entire family from one row but not the other various ways

    put = new Put(ROWS[0]);
    put.add(FAMILIES[1], QUALIFIER, ts[0], VALUES[0]);
    put.add(FAMILIES[1], QUALIFIER, ts[1], VALUES[1]);
    put.add(FAMILIES[2], QUALIFIER, ts[2], VALUES[2]);
    put.add(FAMILIES[2], QUALIFIER, ts[3], VALUES[3]);
    ht.put(put);

    put = new Put(ROWS[1]);
    put.add(FAMILIES[1], QUALIFIER, ts[0], VALUES[0]);
    put.add(FAMILIES[1], QUALIFIER, ts[1], VALUES[1]);
    put.add(FAMILIES[2], QUALIFIER, ts[2], VALUES[2]);
    put.add(FAMILIES[2], QUALIFIER, ts[3], VALUES[3]);
    ht.put(put);

    put = new Put(ROWS[2]);
    put.add(FAMILIES[1], QUALIFIER, ts[0], VALUES[0]);
    put.add(FAMILIES[1], QUALIFIER, ts[1], VALUES[1]);
    put.add(FAMILIES[2], QUALIFIER, ts[2], VALUES[2]);
    put.add(FAMILIES[2], QUALIFIER, ts[3], VALUES[3]);
    ht.put(put);

    // Assert that above went in.
    get = new Get(ROWS[2]);
    get.addFamily(FAMILIES[1]);
    get.addFamily(FAMILIES[2]);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertTrue("Expected 4 key but received " + result.size() + ": " + result,
        result.size() == 4);

    delete = new Delete(ROWS[0]);
    delete.deleteFamily(FAMILIES[2]);
    ht.delete(delete);

    delete = new Delete(ROWS[1]);
    delete.deleteColumns(FAMILIES[1], QUALIFIER);
    ht.delete(delete);

    delete = new Delete(ROWS[2]);
    delete.deleteColumn(FAMILIES[1], QUALIFIER);
    delete.deleteColumn(FAMILIES[1], QUALIFIER);
    delete.deleteColumn(FAMILIES[2], QUALIFIER);
    ht.delete(delete);

    get = new Get(ROWS[0]);
    get.addFamily(FAMILIES[1]);
    get.addFamily(FAMILIES[2]);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertTrue("Expected 2 keys but received " + result.size(),
        result.size() == 2);
    assertNResult(result, ROWS[0], FAMILIES[1], QUALIFIER,
        new long [] {ts[0], ts[1]},
        new byte[][] {VALUES[0], VALUES[1]},
        0, 1);

    scan = new Scan(ROWS[0]);
    scan.addFamily(FAMILIES[1]);
    scan.addFamily(FAMILIES[2]);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertTrue("Expected 2 keys but received " + result.size(),
        result.size() == 2);
    assertNResult(result, ROWS[0], FAMILIES[1], QUALIFIER,
        new long [] {ts[0], ts[1]},
        new byte[][] {VALUES[0], VALUES[1]},
        0, 1);

    get = new Get(ROWS[1]);
    get.addFamily(FAMILIES[1]);
    get.addFamily(FAMILIES[2]);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertTrue("Expected 2 keys but received " + result.size(),
        result.size() == 2);

    scan = new Scan(ROWS[1]);
    scan.addFamily(FAMILIES[1]);
    scan.addFamily(FAMILIES[2]);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertTrue("Expected 2 keys but received " + result.size(),
        result.size() == 2);

    get = new Get(ROWS[2]);
    get.addFamily(FAMILIES[1]);
    get.addFamily(FAMILIES[2]);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertEquals(1, result.size());
    assertNResult(result, ROWS[2], FAMILIES[2], QUALIFIER,
        new long [] {ts[2]},
        new byte[][] {VALUES[2]},
        0, 0);

    scan = new Scan(ROWS[2]);
    scan.addFamily(FAMILIES[1]);
    scan.addFamily(FAMILIES[2]);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertEquals(1, result.size());
    assertNResult(result, ROWS[2], FAMILIES[2], QUALIFIER,
        new long [] {ts[2]},
        new byte[][] {VALUES[2]},
        0, 0);

    // Test if we delete the family first in one row (HBASE-1541)

    delete = new Delete(ROWS[3]);
    delete.deleteFamily(FAMILIES[1]);
    ht.delete(delete);

    put = new Put(ROWS[3]);
    put.add(FAMILIES[2], QUALIFIER, VALUES[0]);
    ht.put(put);

    put = new Put(ROWS[4]);
    put.add(FAMILIES[1], QUALIFIER, VALUES[1]);
    put.add(FAMILIES[2], QUALIFIER, VALUES[2]);
    ht.put(put);

    get = new Get(ROWS[3]);
    get.addFamily(FAMILIES[1]);
    get.addFamily(FAMILIES[2]);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertTrue("Expected 1 key but received " + result.size(),
        result.size() == 1);

    get = new Get(ROWS[4]);
    get.addFamily(FAMILIES[1]);
    get.addFamily(FAMILIES[2]);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertTrue("Expected 2 keys but received " + result.size(),
        result.size() == 2);

    scan = new Scan(ROWS[3]);
    scan.addFamily(FAMILIES[1]);
    scan.addFamily(FAMILIES[2]);
    scan.setMaxVersions(Integer.MAX_VALUE);
    ResultScanner scanner = ht.getScanner(scan);
    result = scanner.next();
    assertTrue("Expected 1 key but received " + result.size(),
        result.size() == 1);
    assertTrue(Bytes.equals(CellUtil.cloneRow(result.rawCells()[0]), ROWS[3]));
    assertTrue(Bytes.equals(CellUtil.cloneValue(result.rawCells()[0]), VALUES[0]));
    result = scanner.next();
    assertTrue("Expected 2 keys but received " + result.size(),
        result.size() == 2);
    assertTrue(Bytes.equals(CellUtil.cloneRow(result.rawCells()[0]), ROWS[4]));
    assertTrue(Bytes.equals(CellUtil.cloneRow(result.rawCells()[1]), ROWS[4]));
    assertTrue(Bytes.equals(CellUtil.cloneValue(result.rawCells()[0]), VALUES[1]));
    assertTrue(Bytes.equals(CellUtil.cloneValue(result.rawCells()[1]), VALUES[2]));
    scanner.close();

    // Add test of bulk deleting.
    for (int i = 0; i < 10; i++) {
      byte [] bytes = Bytes.toBytes(i);
      put = new Put(bytes);
      put.setDurability(Durability.SKIP_WAL);
      put.add(FAMILIES[0], QUALIFIER, bytes);@@@+      // Insert 4 more versions of same column and a dupe@@@+      put = new Put(ROW);@@@+      put.addColumn(FAMILY, QUALIFIER, STAMPS[9], VALUES[9]);@@@+      put.addColumn(FAMILY, QUALIFIER, STAMPS[11], VALUES[11]);@@@+      put.addColumn(FAMILY, QUALIFIER, STAMPS[13], VALUES[13]);@@@+      put.addColumn(FAMILY, QUALIFIER, STAMPS[15], VALUES[15]);@@@       ht.put(put);
    }
    for (int i = 0; i < 10; i++) {
      byte [] bytes = Bytes.toBytes(i);
      get = new Get(bytes);
      get.addFamily(FAMILIES[0]);@@@+@@@+      get = new Get(ROW);@@@+      get.addColumn(FAMILY, QUALIFIER);@@@+      get.readVersions(Integer.MAX_VALUE);@@@       result = ht.get(get);
      assertTrue(result.size() == 1);
    }
    ArrayList<Delete> deletes = new ArrayList<Delete>();
    for (int i = 0; i < 10; i++) {
      byte [] bytes = Bytes.toBytes(i);
      delete = new Delete(bytes);
      delete.deleteFamily(FAMILIES[0]);
      deletes.add(delete);
    }
    ht.delete(deletes);
    for (int i = 0; i < 10; i++) {
      byte [] bytes = Bytes.toBytes(i);
      get = new Get(bytes);
      get.addFamily(FAMILIES[0]);
      result = ht.get(get);
      assertTrue(result.size() == 0);
    }
  }
```
```
Cyclomatic Complexity	 9
Assertions		 11
Lines of Code		63
-  
  public void testJiraTest867() throws Exception {
    int numRows = 10;
    int numColsPerRow = 2000;

    byte [] TABLE = Bytes.toBytes("testJiraTest867");

    byte [][] ROWS = makeN(ROW, numRows);
    byte [][] QUALIFIERS = makeN(QUALIFIER, numColsPerRow);

    Table ht = TEST_UTIL.createTable(TABLE, FAMILY);
    TEST_UTIL.waitTableAvailable(TABLE, 10000);

    // Insert rows

    for(int i=0;i<numRows;i++) {
      Put put = new Put(ROWS[i]);
      put.setDurability(Durability.SKIP_WAL);
      for(int j=0;j<numColsPerRow;j++) {
        put.add(FAMILY, QUALIFIERS[j], QUALIFIERS[j]);
      }
      assertTrue("Put expected to contain " + numColsPerRow + " columns but " +
          "only contains " + put.size(), put.size() == numColsPerRow);
      ht.put(put);
    }

    // Get a row
    Get get = new Get(ROWS[numRows-1]);
    Result result = ht.get(get);
    assertNumKeys(result, numColsPerRow);
    Cell [] keys = result.rawCells();
    for(int i=0;i<result.size();i++) {
      assertKey(keys[i], ROWS[numRows-1], FAMILY, QUALIFIERS[i], QUALIFIERS[i]);
    }

    // Scan the rows
    Scan scan = new Scan();
    ResultScanner scanner = ht.getScanner(scan);
    int rowCount = 0;
    while((result = scanner.next()) != null) {
      assertNumKeys(result, numColsPerRow);
      Cell [] kvs = result.rawCells();
      for(int i=0;i<numColsPerRow;i++) {
        assertKey(kvs[i], ROWS[rowCount], FAMILY, QUALIFIERS[i], QUALIFIERS[i]);
      }
      rowCount++;
    }
    scanner.close();
    assertTrue("Expected to scan " + numRows + " rows but actually scanned "
        + rowCount + " rows", rowCount == numRows);

    // flush and try again

    TEST_UTIL.flush();

    // Get a row
    get = new Get(ROWS[numRows-1]);
    result = ht.get(get);
    assertNumKeys(result, numColsPerRow);
    keys = result.rawCells();
    for(int i=0;i<result.size();i++) {
      assertKey(keys[i], ROWS[numRows-1], FAMILY, QUALIFIERS[i], QUALIFIERS[i]);
    }

    // Scan the rows
    scan = new Scan();
    scanner = ht.getScanner(scan);
    rowCount = 0;
    while((result = scanner.next()) != null) {
      assertNumKeys(result, numColsPerRow);
      Cell [] kvs = result.rawCells();
      for(int i=0;i<numColsPerRow;i++) {
        assertKey(kvs[i], ROWS[rowCount], FAMILY, QUALIFIERS[i], QUALIFIERS[i]);
      }
      rowCount++;
    }
    scanner.close();
    assertTrue("Expected to scan " + numRows + " rows but actually scanned "
        + rowCount + " rows", rowCount == numRows);

  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		39
-  
  public void testJiraTest861() throws Exception {

    byte [] TABLE = Bytes.toBytes("testJiraTest861");
    byte [][] VALUES = makeNAscii(VALUE, 7);
    long [] STAMPS = makeStamps(7);

    Table ht = TEST_UTIL.createTable(TABLE, FAMILY, 10);
    TEST_UTIL.waitTableAvailable(TABLE, 10000);

    // Insert three versions

    Put put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, STAMPS[3], VALUES[3]);
    put.add(FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    put.add(FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    ht.put(put);

    // Get the middle value
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);

    // Try to get one version before (expect fail)
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[1]);

    // Try to get one version after (expect fail)
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[5]);

    // Try same from storefile
    TEST_UTIL.flush();
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[1]);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[5]);

    // Insert two more versions surrounding others, into memstore
    put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, STAMPS[0], VALUES[0]);
    put.add(FAMILY, QUALIFIER, STAMPS[6], VALUES[6]);
    ht.put(put);

    // Check we can get everything we should and can't get what we shouldn't
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[0], VALUES[0]);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[1]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[3], VALUES[3]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[5]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[6], VALUES[6]);

    // Try same from two storefiles
    TEST_UTIL.flush();
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[0], VALUES[0]);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[1]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[3], VALUES[3]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[5]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[6], VALUES[6]);

  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		33
-  
  public void testJiraTest33() throws Exception {

    byte [] TABLE = Bytes.toBytes("testJiraTest33");
    byte [][] VALUES = makeNAscii(VALUE, 7);
    long [] STAMPS = makeStamps(7);

    Table ht = TEST_UTIL.createTable(TABLE, FAMILY, 10);
    TEST_UTIL.waitTableAvailable(TABLE, 10000);

    // Insert lots versions

    Put put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, STAMPS[0], VALUES[0]);
    put.add(FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    put.add(FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    put.add(FAMILY, QUALIFIER, STAMPS[3], VALUES[3]);
    put.add(FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    put.add(FAMILY, QUALIFIER, STAMPS[5], VALUES[5]);
    ht.put(put);

    getVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 5);
    getVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 2);
    getVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 4, 5);
    getVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 2, 3);

    scanVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 5);
    scanVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 2);
    scanVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 4, 5);
    scanVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 2, 3);

    // Try same from storefile
    TEST_UTIL.flush();

    getVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 5);
    getVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 2);
    getVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 4, 5);
    getVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 2, 3);

    scanVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 5);
    scanVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 2);
    scanVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 4, 5);
    scanVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 2, 3);

  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		13
-  
  public void testJiraTest1014() throws Exception {

    byte [] TABLE = Bytes.toBytes("testJiraTest1014");

    Table ht = TEST_UTIL.createTable(TABLE, FAMILY, 10);
    TEST_UTIL.waitTableAvailable(TABLE, 10000);

    long manualStamp = 12345;

    // Insert lots versions

    Put put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, manualStamp, VALUE);
    ht.put(put);

    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, manualStamp, VALUE);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, manualStamp-1);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, manualStamp+1);

  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		29
-  
  public void testJiraTest1182() throws Exception {

    byte [] TABLE = Bytes.toBytes("testJiraTest1182");
    byte [][] VALUES = makeNAscii(VALUE, 7);
    long [] STAMPS = makeStamps(7);

    Table ht = TEST_UTIL.createTable(TABLE, FAMILY, 10);
    TEST_UTIL.waitTableAvailable(TABLE, 10000);

    // Insert lots versions

    Put put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, STAMPS[0], VALUES[0]);
    put.add(FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    put.add(FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    put.add(FAMILY, QUALIFIER, STAMPS[3], VALUES[3]);
    put.add(FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    put.add(FAMILY, QUALIFIER, STAMPS[5], VALUES[5]);
    ht.put(put);

    getVersionRangeAndVerifyGreaterThan(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 5);
    getVersionRangeAndVerifyGreaterThan(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 2, 5);
    getVersionRangeAndVerifyGreaterThan(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 4, 5);

    scanVersionRangeAndVerifyGreaterThan(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 5);
    scanVersionRangeAndVerifyGreaterThan(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 2, 5);
    scanVersionRangeAndVerifyGreaterThan(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 4, 5);

    // Try same from storefile
    TEST_UTIL.flush();

    getVersionRangeAndVerifyGreaterThan(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 5);
    getVersionRangeAndVerifyGreaterThan(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 2, 5);
    getVersionRangeAndVerifyGreaterThan(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 4, 5);

    scanVersionRangeAndVerifyGreaterThan(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 5);
    scanVersionRangeAndVerifyGreaterThan(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 2, 5);
    scanVersionRangeAndVerifyGreaterThan(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 4, 5);
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		21
-  
  public void testJiraTest52() throws Exception {
    byte [] TABLE = Bytes.toBytes("testJiraTest52");
    byte [][] VALUES = makeNAscii(VALUE, 7);
    long [] STAMPS = makeStamps(7);

    Table ht = TEST_UTIL.createTable(TABLE, FAMILY, 10);
    TEST_UTIL.waitTableAvailable(TABLE, 10000);

    // Insert lots versions

    Put put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, STAMPS[0], VALUES[0]);
    put.add(FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    put.add(FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    put.add(FAMILY, QUALIFIER, STAMPS[3], VALUES[3]);
    put.add(FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    put.add(FAMILY, QUALIFIER, STAMPS[5], VALUES[5]);
    ht.put(put);

    getAllVersionsAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 5);

    scanAllVersionsAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 5);

    // Try same from storefile
    TEST_UTIL.flush();

    getAllVersionsAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 5);

    scanAllVersionsAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 5);
  }
```
```
Cyclomatic Complexity	 1
Assertions		 12
Lines of Code		167
-  
  public void testDuplicateVersions() throws Exception {
    byte [] TABLE = Bytes.toBytes("testDuplicateVersions");

    long [] STAMPS = makeStamps(20);
    byte [][] VALUES = makeNAscii(VALUE, 20);

    Table ht = TEST_UTIL.createTable(TABLE, FAMILY, 10);
    TEST_UTIL.waitTableAvailable(TABLE, 10000);

    // Insert 4 versions of same column
    Put put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    put.add(FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    put.add(FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    put.add(FAMILY, QUALIFIER, STAMPS[5], VALUES[5]);
    ht.put(put);

    // Verify we can get each one properly
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[5], VALUES[5]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[5], VALUES[5]);

    // Verify we don't accidentally get others
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[0]);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[3]);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[6]);
    scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[0]);
    scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[3]);
    scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[6]);

    // Ensure maxVersions in query is respected
    Get get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIER);
    get.setMaxVersions(2);
    Result result = ht.get(get);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[4], STAMPS[5]},
        new byte[][] {VALUES[4], VALUES[5]},
        0, 1);

    Scan scan = new Scan(ROW);
    scan.addColumn(FAMILY, QUALIFIER);
    scan.setMaxVersions(2);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[4], STAMPS[5]},
        new byte[][] {VALUES[4], VALUES[5]},
        0, 1);

    // Flush and redo

    TEST_UTIL.flush();

    // Verify we can get each one properly
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[5], VALUES[5]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[5], VALUES[5]);

    // Verify we don't accidentally get others
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[0]);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[3]);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[6]);
    scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[0]);
    scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[3]);
    scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[6]);

    // Ensure maxVersions in query is respected
    get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIER);
    get.setMaxVersions(2);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[4], STAMPS[5]},
        new byte[][] {VALUES[4], VALUES[5]},
        0, 1);

    scan = new Scan(ROW);
    scan.addColumn(FAMILY, QUALIFIER);
    scan.setMaxVersions(2);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[4], STAMPS[5]},
        new byte[][] {VALUES[4], VALUES[5]},
        0, 1);


    // Add some memstore and retest

    // Insert 4 more versions of same column and a dupe
    put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, STAMPS[3], VALUES[3]);
    put.add(FAMILY, QUALIFIER, STAMPS[4], VALUES[14]);
    put.add(FAMILY, QUALIFIER, STAMPS[6], VALUES[6]);
    put.add(FAMILY, QUALIFIER, STAMPS[7], VALUES[7]);
    put.add(FAMILY, QUALIFIER, STAMPS[8], VALUES[8]);
    ht.put(put);

    // Ensure maxVersions in query is respected
    get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIER);
    get.setMaxVersions(7);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7], STAMPS[8]},
        new byte[][] {VALUES[2], VALUES[3], VALUES[14], VALUES[5], VALUES[6], VALUES[7], VALUES[8]},
        0, 6);

    scan = new Scan(ROW);
    scan.addColumn(FAMILY, QUALIFIER);
    scan.setMaxVersions(7);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7], STAMPS[8]},
        new byte[][] {VALUES[2], VALUES[3], VALUES[14], VALUES[5], VALUES[6], VALUES[7], VALUES[8]},
        0, 6);

    get = new Get(ROW);
    get.setMaxVersions(7);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7], STAMPS[8]},
        new byte[][] {VALUES[2], VALUES[3], VALUES[14], VALUES[5], VALUES[6], VALUES[7], VALUES[8]},
        0, 6);

    scan = new Scan(ROW);
    scan.setMaxVersions(7);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7], STAMPS[8]},
        new byte[][] {VALUES[2], VALUES[3], VALUES[14], VALUES[5], VALUES[6], VALUES[7], VALUES[8]},
        0, 6);

    // Verify we can get each one properly
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[14]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[7], VALUES[7]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[14]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[7], VALUES[7]);

    // Verify we don't accidentally get others
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[0]);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[9]);
    scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[0]);
    scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[9]);

    // Ensure maxVersions of table is respected

    TEST_UTIL.flush();

    // Insert 4 more versions of same column and a dupe
    put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, STAMPS[9], VALUES[9]);
    put.add(FAMILY, QUALIFIER, STAMPS[11], VALUES[11]);
    put.add(FAMILY, QUALIFIER, STAMPS[13], VALUES[13]);
    put.add(FAMILY, QUALIFIER, STAMPS[15], VALUES[15]);
    ht.put(put);

    get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIER);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7], STAMPS[8], STAMPS[9], STAMPS[11], STAMPS[13], STAMPS[15]},
        new byte[][] {VALUES[3], VALUES[14], VALUES[5], VALUES[6], VALUES[7], VALUES[8], VALUES[9], VALUES[11], VALUES[13], VALUES[15]},
        0, 9);

    scan = new Scan(ROW);
    scan.addColumn(FAMILY, QUALIFIER);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7], STAMPS[8], STAMPS[9], STAMPS[11], STAMPS[13], STAMPS[15]},
        new byte[][] {VALUES[3], VALUES[14], VALUES[5], VALUES[6], VALUES[7], VALUES[8], VALUES[9], VALUES[11], VALUES[13], VALUES[15]},
        0, 9);

    // Delete a version in the memstore and a version in a storefile
    Delete delete = new Delete(ROW);
    delete.deleteColumn(FAMILY, QUALIFIER, STAMPS[11]);
    delete.deleteColumn(FAMILY, QUALIFIER, STAMPS[7]);
    ht.delete(delete);

    // Test that it's gone
    get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIER);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[1], STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[8], STAMPS[9], STAMPS[13], STAMPS[15]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3], VALUES[14], VALUES[5], VALUES[6], VALUES[8], VALUES[9], VALUES[13], VALUES[15]},
        0, 9);

    scan = new Scan(ROW);
    scan.addColumn(FAMILY, QUALIFIER);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[1], STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[8], STAMPS[9], STAMPS[13], STAMPS[15]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3], VALUES[14], VALUES[5], VALUES[6], VALUES[8], VALUES[9], VALUES[13], VALUES[15]},
        0, 9);
  }
```
```
Cyclomatic Complexity	 1
Assertions		 4
Lines of Code		35
-  
  public void testUpdates() throws Exception {

    byte [] TABLE = Bytes.toBytes("testUpdates");
    Table hTable = TEST_UTIL.createTable(TABLE, FAMILY, 10);
    TEST_UTIL.waitTableAvailable(TABLE, 10000);

    // Write a column with values at timestamp 1, 2 and 3
    byte[] row = Bytes.toBytes("row1");
    byte[] qualifier = Bytes.toBytes("myCol");
    Put put = new Put(row);
    put.add(FAMILY, qualifier, 1L, Bytes.toBytes("AAA"));
    hTable.put(put);

    put = new Put(row);
    put.add(FAMILY, qualifier, 2L, Bytes.toBytes("BBB"));
    hTable.put(put);

    put = new Put(row);
    put.add(FAMILY, qualifier, 3L, Bytes.toBytes("EEE"));
    hTable.put(put);

    Get get = new Get(row);
    get.addColumn(FAMILY, qualifier);
    get.setMaxVersions();

    // Check that the column indeed has the right values at timestamps 1 and
    // 2
    Result result = hTable.get(get);
    NavigableMap<Long, byte[]> navigableMap =
        result.getMap().get(FAMILY).get(qualifier);
    assertEquals("AAA", Bytes.toString(navigableMap.get(1L)));
    assertEquals("BBB", Bytes.toString(navigableMap.get(2L)));

    // Update the value at timestamp 1
    put = new Put(row);
    put.add(FAMILY, qualifier, 1L, Bytes.toBytes("CCC"));
    hTable.put(put);

    // Update the value at timestamp 2
    put = new Put(row);
    put.add(FAMILY, qualifier, 2L, Bytes.toBytes("DDD"));
    hTable.put(put);

    // Check that the values at timestamp 2 and 1 got updated
    result = hTable.get(get);
    navigableMap = result.getMap().get(FAMILY).get(qualifier);
    assertEquals("CCC", Bytes.toString(navigableMap.get(1L)));
    assertEquals("DDD", Bytes.toString(navigableMap.get(2L)));
  }
```
```
Cyclomatic Complexity	 1
Assertions		 4
Lines of Code		42
-  
  public void testUpdatesWithMajorCompaction() throws Exception {

    TableName TABLE = TableName.valueOf("testUpdatesWithMajorCompaction");
    Table hTable = TEST_UTIL.createTable(TABLE, FAMILY, 10);
    TEST_UTIL.waitTableAvailable(TABLE, 10000);

    HBaseAdmin admin = new HBaseAdmin(TEST_UTIL.getConfiguration());

    // Write a column with values at timestamp 1, 2 and 3
    byte[] row = Bytes.toBytes("row2");
    byte[] qualifier = Bytes.toBytes("myCol");
    Put put = new Put(row);
    put.add(FAMILY, qualifier, 1L, Bytes.toBytes("AAA"));
    hTable.put(put);

    put = new Put(row);
    put.add(FAMILY, qualifier, 2L, Bytes.toBytes("BBB"));
    hTable.put(put);

    put = new Put(row);
    put.add(FAMILY, qualifier, 3L, Bytes.toBytes("EEE"));
    hTable.put(put);

    Get get = new Get(row);
    get.addColumn(FAMILY, qualifier);
    get.setMaxVersions();

    // Check that the column indeed has the right values at timestamps 1 and
    // 2
    Result result = hTable.get(get);
    NavigableMap<Long, byte[]> navigableMap =
        result.getMap().get(FAMILY).get(qualifier);
    assertEquals("AAA", Bytes.toString(navigableMap.get(1L)));
    assertEquals("BBB", Bytes.toString(navigableMap.get(2L)));

    // Trigger a major compaction
    admin.flush(TABLE);
    admin.majorCompact(TABLE);
    Thread.sleep(6000);

    // Update the value at timestamp 1
    put = new Put(row);
    put.add(FAMILY, qualifier, 1L, Bytes.toBytes("CCC"));
    hTable.put(put);

    // Update the value at timestamp 2
    put = new Put(row);
    put.add(FAMILY, qualifier, 2L, Bytes.toBytes("DDD"));
    hTable.put(put);

    // Trigger a major compaction
    admin.flush(TABLE);
    admin.majorCompact(TABLE);
    Thread.sleep(6000);

    // Check that the values at timestamp 2 and 1 got updated
    result = hTable.get(get);
    navigableMap = result.getMap().get(FAMILY).get(qualifier);
    assertEquals("CCC", Bytes.toString(navigableMap.get(1L)));
    assertEquals("DDD", Bytes.toString(navigableMap.get(2L)));
  }
```
```
Cyclomatic Complexity	 1
Assertions		 4
Lines of Code		46
-  
  public void testMajorCompactionBetweenTwoUpdates() throws Exception {

    String tableName = "testMajorCompactionBetweenTwoUpdates";
    byte [] TABLE = Bytes.toBytes(tableName);
    Table hTable = TEST_UTIL.createTable(TABLE, FAMILY, 10);
    TEST_UTIL.waitTableAvailable(TABLE, 10000);
    HBaseAdmin admin = new HBaseAdmin(TEST_UTIL.getConfiguration());

    // Write a column with values at timestamp 1, 2 and 3
    byte[] row = Bytes.toBytes("row3");
    byte[] qualifier = Bytes.toBytes("myCol");
    Put put = new Put(row);
    put.add(FAMILY, qualifier, 1L, Bytes.toBytes("AAA"));
    hTable.put(put);

    put = new Put(row);
    put.add(FAMILY, qualifier, 2L, Bytes.toBytes("BBB"));
    hTable.put(put);

    put = new Put(row);
    put.add(FAMILY, qualifier, 3L, Bytes.toBytes("EEE"));
    hTable.put(put);

    Get get = new Get(row);
    get.addColumn(FAMILY, qualifier);
    get.setMaxVersions();

    // Check that the column indeed has the right values at timestamps 1 and
    // 2
    Result result = hTable.get(get);
    NavigableMap<Long, byte[]> navigableMap =
        result.getMap().get(FAMILY).get(qualifier);
    assertEquals("AAA", Bytes.toString(navigableMap.get(1L)));
    assertEquals("BBB", Bytes.toString(navigableMap.get(2L)));

    // Trigger a major compaction
    admin.flush(tableName);
    admin.majorCompact(tableName);
    Thread.sleep(6000);

    // Update the value at timestamp 1
    put = new Put(row);
    put.add(FAMILY, qualifier, 1L, Bytes.toBytes("CCC"));
    hTable.put(put);

    // Trigger a major compaction
    admin.flush(tableName);
    admin.majorCompact(tableName);
    Thread.sleep(6000);

    // Update the value at timestamp 2
    put = new Put(row);
    put.add(FAMILY, qualifier, 2L, Bytes.toBytes("DDD"));
    hTable.put(put);

    // Trigger a major compaction
    admin.flush(tableName);
    admin.majorCompact(tableName);
    Thread.sleep(6000);

    // Check that the values at timestamp 2 and 1 got updated
    result = hTable.get(get);
    navigableMap = result.getMap().get(FAMILY).get(qualifier);

    assertEquals("CCC", Bytes.toString(navigableMap.get(1L)));
    assertEquals("DDD", Bytes.toString(navigableMap.get(2L)));
  }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		9
-  
  public void testGet_EmptyTable() throws IOException, InterruptedException {
    Table table = TEST_UTIL.createTable(TableName.valueOf("testGet_EmptyTable"), FAMILY);
    TEST_UTIL.waitTableAvailable(TableName.valueOf("testGet_EmptyTable"), 10000);
    Get get = new Get(ROW);
    get.addFamily(FAMILY);
    Result r = table.get(get);
    assertTrue(r.isEmpty());
  }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		20
-  
  public void testGet_NullQualifier() throws IOException, InterruptedException {
    Table table = TEST_UTIL.createTable(TableName.valueOf("testGet_NullQualifier"), FAMILY);
    TEST_UTIL.waitTableAvailable(TableName.valueOf("testGet_NullQualifier"), 10000);
    Put put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, VALUE);
    table.put(put);

    put = new Put(ROW);
    put.add(FAMILY, null, VALUE);
    table.put(put);
    LOG.info("Row put");

    Get get = new Get(ROW);
    get.addColumn(FAMILY, null);
    Result r = table.get(get);
    assertEquals(1, r.size());

    get = new Get(ROW);
    get.addFamily(FAMILY);
    r = table.get(get);
    assertEquals(2, r.size());
  }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		20
-  
  public void testGet_NonExistentRow() throws IOException, InterruptedException {
    Table table = TEST_UTIL.createTable(TableName.valueOf("testGet_NonExistentRow"), FAMILY);
    TEST_UTIL.waitTableAvailable(TableName.valueOf("testGet_NonExistentRow"), 10000);
    Put put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, VALUE);
    table.put(put);
    LOG.info("Row put");

    Get get = new Get(ROW);
    get.addFamily(FAMILY);
    Result r = table.get(get);
    assertFalse(r.isEmpty());
    System.out.println("Row retrieved successfully");

    byte [] missingrow = Bytes.toBytes("missingrow");
    get = new Get(missingrow);
    get.addFamily(FAMILY);
    r = table.get(get);
    assertTrue(r.isEmpty());
    LOG.info("Row missing as it should be");
  }
```
```
Cyclomatic Complexity	 4
Assertions		 5
Lines of Code		31
-  
  public void testPut() throws IOException, InterruptedException {
    final byte [] CONTENTS_FAMILY = Bytes.toBytes("contents");
    final byte [] SMALL_FAMILY = Bytes.toBytes("smallfam");
    final byte [] row1 = Bytes.toBytes("row1");
    final byte [] row2 = Bytes.toBytes("row2");
    final byte [] value = Bytes.toBytes("abcd");
    Table table = TEST_UTIL.createTable(Bytes.toBytes("testPut"),
      new byte [][] {CONTENTS_FAMILY, SMALL_FAMILY});
    TEST_UTIL.waitTableAvailable(Bytes.toBytes("testPut"), 10000);
    Put put = new Put(row1);
    put.add(CONTENTS_FAMILY, null, value);
    table.put(put);

    put = new Put(row2);
    put.add(CONTENTS_FAMILY, null, value);

    assertEquals(put.size(), 1);
    assertEquals(put.getFamilyCellMap().get(CONTENTS_FAMILY).size(), 1);

    // KeyValue v1 expectation.  Cast for now until we go all Cell all the time. TODO
    KeyValue kv = (KeyValue)put.getFamilyCellMap().get(CONTENTS_FAMILY).get(0);

    assertTrue(Bytes.equals(kv.getFamily(), CONTENTS_FAMILY));
    // will it return null or an empty byte array?
    assertTrue(Bytes.equals(kv.getQualifier(), new byte[0]));

    assertTrue(Bytes.equals(kv.getValue(), value));

    table.put(put);

    Scan scan = new Scan();
    scan.addColumn(CONTENTS_FAMILY, null);
    ResultScanner scanner = table.getScanner(scan);
    for (Result r : scanner) {
      for(Cell key : r.rawCells()) {
        System.out.println(Bytes.toString(r.getRow()) + ": " + key.toString());
      }
    }
  }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		16
-  
  public void testPutNoCF() throws IOException, InterruptedException {
    final byte[] BAD_FAM = Bytes.toBytes("BAD_CF");
    final byte[] VAL = Bytes.toBytes(100);
    Table table = TEST_UTIL.createTable(Bytes.toBytes("testPutNoCF"), FAMILY);
    TEST_UTIL.waitTableAvailable(Bytes.toBytes("testPutNoCF"), 10000);

    boolean caughtNSCFE = false;

    try {
      Put p = new Put(ROW);
      p.add(BAD_FAM, QUALIFIER, VAL);
      table.put(p);
    } catch (RetriesExhaustedWithDetailsException e) {
      caughtNSCFE = e.getCause(0) instanceof NoSuchColumnFamilyException;
    }
    assertTrue("Should throw NoSuchColumnFamilyException", caughtNSCFE);

  }
```
```
Cyclomatic Complexity	 3
Assertions		 1
Lines of Code		27
-  
  public void testRowsPut() throws IOException, InterruptedException {
    final byte[] CONTENTS_FAMILY = Bytes.toBytes("contents");
    final byte[] SMALL_FAMILY = Bytes.toBytes("smallfam");
    final int NB_BATCH_ROWS = 10;
    final byte[] value = Bytes.toBytes("abcd");
    Table table = TEST_UTIL.createTable(Bytes.toBytes("testRowsPut"),
      new byte[][] {CONTENTS_FAMILY, SMALL_FAMILY });
    TEST_UTIL.waitTableAvailable(Bytes.toBytes("testRowsPut"), 10000);
    ArrayList<Put> rowsUpdate = new ArrayList<Put>();
    for (int i = 0; i < NB_BATCH_ROWS; i++) {
      byte[] row = Bytes.toBytes("row" + i);
      Put put = new Put(row);
      put.setDurability(Durability.SKIP_WAL);
      put.add(CONTENTS_FAMILY, null, value);
      rowsUpdate.add(put);
    }
    table.put(rowsUpdate);
    Scan scan = new Scan();
    scan.addFamily(CONTENTS_FAMILY);
    ResultScanner scanner = table.getScanner(scan);
    int nbRows = 0;
    for (@SuppressWarnings("unused")
    Result row : scanner)
      nbRows++;
    assertEquals(NB_BATCH_ROWS, nbRows);
  }
```
```
Cyclomatic Complexity	 4
Assertions		 2
Lines of Code		40
-  
  public void testRowsPutBufferedOneFlush() throws IOException, InterruptedException {
    final byte [] CONTENTS_FAMILY = Bytes.toBytes("contents");
    final byte [] SMALL_FAMILY = Bytes.toBytes("smallfam");
    final byte [] value = Bytes.toBytes("abcd");
    final int NB_BATCH_ROWS = 10;
    HTable table = TEST_UTIL.createTable(Bytes.toBytes("testRowsPutBufferedOneFlush"),
      new byte [][] {CONTENTS_FAMILY, SMALL_FAMILY});
    TEST_UTIL.waitTableAvailable(Bytes.toBytes("testRowsPutBufferedOneFlush"),
        10000);
    table.setAutoFlush(false);
    ArrayList<Put> rowsUpdate = new ArrayList<Put>();
    for (int i = 0; i < NB_BATCH_ROWS * 10; i++) {
      byte[] row = Bytes.toBytes("row" + i);
      Put put = new Put(row);
      put.setDurability(Durability.SKIP_WAL);
      put.add(CONTENTS_FAMILY, null, value);
      rowsUpdate.add(put);
    }
    table.put(rowsUpdate);

    Scan scan = new Scan();
    scan.addFamily(CONTENTS_FAMILY);
    ResultScanner scanner = table.getScanner(scan);
    int nbRows = 0;
    for (@SuppressWarnings("unused")
    Result row : scanner)
      nbRows++;
    assertEquals(0, nbRows);
    scanner.close();

    table.flushCommits();

    scan = new Scan();
    scan.addFamily(CONTENTS_FAMILY);
    scanner = table.getScanner(scan);
    nbRows = 0;
    for (@SuppressWarnings("unused")
    Result row : scanner)
      nbRows++;
    assertEquals(NB_BATCH_ROWS * 10, nbRows);
    table.close();
  }
```
```
Cyclomatic Complexity	 3
Assertions		 1
Lines of Code		29
-  
  public void testRowsPutBufferedManyManyFlushes() throws IOException, InterruptedException {
    final byte[] CONTENTS_FAMILY = Bytes.toBytes("contents");
    final byte[] SMALL_FAMILY = Bytes.toBytes("smallfam");
    final byte[] value = Bytes.toBytes("abcd");
    final int NB_BATCH_ROWS = 10;
    HTable table = TEST_UTIL.createTable(Bytes.toBytes("testRowsPutBufferedManyManyFlushes"),
      new byte[][] {CONTENTS_FAMILY, SMALL_FAMILY });
    TEST_UTIL.waitTableAvailable(Bytes.toBytes("testRowsPutBufferedManyManyFlushes"),
        10000);
    table.setWriteBufferSize(10);
    ArrayList<Put> rowsUpdate = new ArrayList<Put>();
    for (int i = 0; i < NB_BATCH_ROWS * 10; i++) {
      byte[] row = Bytes.toBytes("row" + i);
      Put put = new Put(row);
      put.setDurability(Durability.SKIP_WAL);
      put.add(CONTENTS_FAMILY, null, value);
      rowsUpdate.add(put);
    }
    table.put(rowsUpdate);

    Scan scan = new Scan();
    scan.addFamily(CONTENTS_FAMILY);
    ResultScanner scanner = table.getScanner(scan);
    int nbRows = 0;
    for (@SuppressWarnings("unused")
    Result row : scanner)
      nbRows++;
    assertEquals(NB_BATCH_ROWS * 10, nbRows);
  }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		25
-  
  public void testAddKeyValue() throws IOException {
    final byte[] CONTENTS_FAMILY = Bytes.toBytes("contents");
    final byte[] value = Bytes.toBytes("abcd");
    final byte[] row1 = Bytes.toBytes("row1");
    final byte[] row2 = Bytes.toBytes("row2");
    byte[] qualifier = Bytes.toBytes("qf1");
    Put put = new Put(row1);

    // Adding KeyValue with the same row
    KeyValue kv = new KeyValue(row1, CONTENTS_FAMILY, qualifier, value);
    boolean ok = true;
    try {
      put.add(kv);
    } catch (IOException e) {
      ok = false;
    }
    assertEquals(true, ok);

    // Adding KeyValue with the different row
    kv = new KeyValue(row2, CONTENTS_FAMILY, qualifier, value);
    ok = false;
    try {
      put.add(kv);
    } catch (IOException e) {
      ok = true;
    }
    assertEquals(true, ok);
  }
```
```
Cyclomatic Complexity	 3
Assertions		 6
Lines of Code		19
-  
  public void testAppendWithoutWAL() throws Exception {
    List<Result> resultsWithWal = doAppend(true);
    List<Result> resultsWithoutWal = doAppend(false);
    assertEquals(resultsWithWal.size(), resultsWithoutWal.size());
    for (int i = 0; i != resultsWithWal.size(); ++i) {
      Result resultWithWal = resultsWithWal.get(i);
      Result resultWithoutWal = resultsWithoutWal.get(i);
      assertEquals(resultWithWal.rawCells().length, resultWithoutWal.rawCells().length);
      for (int j = 0; j != resultWithWal.rawCells().length; ++j) {
        Cell cellWithWal = resultWithWal.rawCells()[j];
        Cell cellWithoutWal = resultWithoutWal.rawCells()[j];
        assertTrue(Bytes.equals(CellUtil.cloneRow(cellWithWal), CellUtil.cloneRow(cellWithoutWal)));
        assertTrue(Bytes.equals(CellUtil.cloneFamily(cellWithWal), CellUtil.cloneFamily(cellWithoutWal)));
        assertTrue(Bytes.equals(CellUtil.cloneQualifier(cellWithWal), CellUtil.cloneQualifier(cellWithoutWal)));
        assertTrue(Bytes.equals(CellUtil.cloneValue(cellWithWal), CellUtil.cloneValue(cellWithoutWal)));
      }
    }
  }
```
```
Cyclomatic Complexity	 10
Assertions		 2
Lines of Code		74
-  
  public void testHBase737() throws IOException, InterruptedException {
    final byte [] FAM1 = Bytes.toBytes("fam1");
    final byte [] FAM2 = Bytes.toBytes("fam2");
    // Open table
    Table table = TEST_UTIL.createTable(Bytes.toBytes("testHBase737"),
      new byte [][] {FAM1, FAM2});
    TEST_UTIL.waitTableAvailable(Bytes.toBytes("testHBase737"), 10000);
    // Insert some values
    Put put = new Put(ROW);
    put.add(FAM1, Bytes.toBytes("letters"), Bytes.toBytes("abcdefg"));
    table.put(put);
    try {
      Thread.sleep(1000);
    } catch (InterruptedException i) {
      //ignore
    }

    put = new Put(ROW);
    put.add(FAM1, Bytes.toBytes("numbers"), Bytes.toBytes("123456"));
    table.put(put);

    try {
      Thread.sleep(1000);
    } catch (InterruptedException i) {
      //ignore
    }

    put = new Put(ROW);
    put.add(FAM2, Bytes.toBytes("letters"), Bytes.toBytes("hijklmnop"));
    table.put(put);

    long times[] = new long[3];

    // First scan the memstore

    Scan scan = new Scan();
    scan.addFamily(FAM1);
    scan.addFamily(FAM2);
    ResultScanner s = table.getScanner(scan);
    try {
      int index = 0;
      Result r = null;
      while ((r = s.next()) != null) {
        for(Cell key : r.rawCells()) {
          times[index++] = key.getTimestamp();
        }
      }
    } finally {
      s.close();
    }
    for (int i = 0; i < times.length - 1; i++) {
      for (int j = i + 1; j < times.length; j++) {
        assertTrue(times[j] > times[i]);
      }
    }

    // Flush data to disk and try again
    TEST_UTIL.flush();

    // Reset times
    for(int i=0;i<times.length;i++) {
      times[i] = 0;
    }

    try {
      Thread.sleep(1000);
    } catch (InterruptedException i) {
      //ignore
    }
    scan = new Scan();
    scan.addFamily(FAM1);
    scan.addFamily(FAM2);
    s = table.getScanner(scan);
    try {
      int index = 0;
      Result r = null;
      while ((r = s.next()) != null) {
        for(Cell key : r.rawCells()) {
          times[index++] = key.getTimestamp();
        }
      }
    } finally {
      s.close();
    }
    for (int i = 0; i < times.length - 1; i++) {
      for (int j = i + 1; j < times.length; j++) {
        assertTrue(times[j] > times[i]);
      }
    }
  }
```
```
Cyclomatic Complexity	 5
Assertions		 2
Lines of Code		27
-  
  public void testListTables() throws IOException, InterruptedException {
    TableName t1 = TableName.valueOf("testListTables1");
    TableName t2 = TableName.valueOf("testListTables2");
    TableName t3 = TableName.valueOf("testListTables3");
    TableName [] tables = new TableName[] { t1, t2, t3 };
    for (int i = 0; i < tables.length; i++) {
      TEST_UTIL.createTable(tables[i], FAMILY);
      TEST_UTIL.waitTableAvailable(tables[i], 10000);
    }
    Admin admin = new HBaseAdmin(TEST_UTIL.getConfiguration());
    HTableDescriptor[] ts = admin.listTables();
    HashSet<HTableDescriptor> result = new HashSet<HTableDescriptor>(ts.length);
    Collections.addAll(result, ts);
    int size = result.size();
    assertTrue(size >= tables.length);
    for (int i = 0; i < tables.length && i < size; i++) {
      boolean found = false;
      for (int j = 0; j < ts.length; j++) {
        if (ts[j].getTableName().equals(tables[i])) {
          found = true;
          break;
        }
      }
      assertTrue("Not found: " + tables[i], found);
    }
  }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		9
-  
  public void testUnmanagedHConnection() throws IOException, InterruptedException {
    final TableName tableName = TableName.valueOf("testUnmanagedHConnection");
    HTable t = createUnmangedHConnectionHTable(tableName);
    HBaseAdmin ha = new HBaseAdmin(t.getConnection());
    assertTrue(ha.tableExists(tableName));
    assertTrue(t.get(new Get(ROW)).isEmpty());
    ha.close();
  }
```
```
Cyclomatic Complexity	 3
Assertions		 5
Lines of Code		23
-  
  public void testUnmanagedHConnectionReconnect() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    Class registryImpl = conf.getClass(
        HConstants.REGISTRY_IMPL_CONF_KEY, ZKConnectionRegistry.class);
    // This test does not make sense for MasterRegistry since it stops the only master in the
    // cluster and starts a new master without populating the underlying config for the connection.
    Assume.assumeFalse(registryImpl.equals(MasterRegistry.class));
    final TableName tableName = TableName.valueOf("testUnmanagedHConnectionReconnect");
    HTable t = createUnmangedHConnectionHTable(tableName);
    Connection conn = t.getConnection();
    try (HBaseAdmin ha = new HBaseAdmin(conn)) {
      assertTrue(ha.tableExists(tableName));
      assertTrue(t.get(new Get(ROW)).isEmpty());
    }

    // stop the master
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    cluster.stopMaster(0, false);
    cluster.waitOnMaster(0);

    // start up a new master
    cluster.startMaster();
    assertTrue(cluster.waitForActiveAndReadyMaster());

    // test that the same unmanaged connection works with a new
    // HBaseAdmin and can connect to the new master;
    try (HBaseAdmin newAdmin = new HBaseAdmin(conn)) {
      assertTrue(newAdmin.tableExists(tableName));
      assertTrue(newAdmin.getClusterStatus().getServersSize() == SLAVES);
    }
  }
```
```
Cyclomatic Complexity	 7
Assertions		 5
Lines of Code		55
-  
  public void testMiscHTableStuff() throws IOException, InterruptedException {
    final TableName tableAname = TableName.valueOf("testMiscHTableStuffA");
    final TableName tableBname = TableName.valueOf("testMiscHTableStuffB");
    final byte[] attrName = Bytes.toBytes("TESTATTR");
    final byte[] attrValue = Bytes.toBytes("somevalue");
    byte[] value = Bytes.toBytes("value");

    Table a = TEST_UTIL.createTable(tableAname, HConstants.CATALOG_FAMILY);
    TEST_UTIL.waitTableAvailable(tableAname, 10000);
    Table b = TEST_UTIL.createTable(tableBname, HConstants.CATALOG_FAMILY);
    TEST_UTIL.waitTableAvailable(tableBname, 10000);
    Put put = new Put(ROW);
    put.add(HConstants.CATALOG_FAMILY, null, value);
    a.put(put);

    // open a new connection to A and a connection to b
    Table newA = new HTable(TEST_UTIL.getConfiguration(), tableAname);

    // copy data from A to B
    Scan scan = new Scan();
    scan.addFamily(HConstants.CATALOG_FAMILY);
    ResultScanner s = newA.getScanner(scan);
    try {
      for (Result r : s) {
        put = new Put(r.getRow());
        put.setDurability(Durability.SKIP_WAL);
        for (Cell kv : r.rawCells()) {
          put.add(kv);
        }
        b.put(put);
      }
    } finally {
      s.close();
    }

    // Opening a new connection to A will cause the tables to be reloaded
    Table anotherA = new HTable(TEST_UTIL.getConfiguration(), tableAname);
    Get get = new Get(ROW);
    get.addFamily(HConstants.CATALOG_FAMILY);
    anotherA.get(get);

    // We can still access A through newA because it has the table information
    // cached. And if it needs to recalibrate, that will cause the information
    // to be reloaded.

    // Test user metadata
    Admin admin = new HBaseAdmin(TEST_UTIL.getConfiguration());
    // make a modifiable descriptor
    HTableDescriptor desc = new HTableDescriptor(a.getTableDescriptor());
    // offline the table
    admin.disableTable(tableAname);
    // add a user attribute to HTD
    desc.setValue(attrName, attrValue);
    // add a user attribute to HCD
    for (HColumnDescriptor c : desc.getFamilies())
      c.setValue(attrName, attrValue);
    // update metadata for all regions of this table
    admin.modifyTable(tableAname, desc);
    // enable the table
    admin.enableTable(tableAname);

    // Test that attribute changes were applied
    desc = a.getTableDescriptor();
    assertEquals("wrong table descriptor returned", desc.getTableName(), tableAname);
    // check HTD attribute
    value = desc.getValue(attrName);
    assertFalse("missing HTD attribute value", value == null);
    assertFalse("HTD attribute value is incorrect",
      Bytes.compareTo(value, attrValue) != 0);
    // check HCD attribute
    for (HColumnDescriptor c : desc.getFamilies()) {
      value = c.getValue(attrName);
      assertFalse("missing HCD attribute value", value == null);
      assertFalse("HCD attribute value is incorrect",
        Bytes.compareTo(value, attrValue) != 0);
    }
  }
```
```
Cyclomatic Complexity	 1
Assertions		 25
Lines of Code		73
-  
  public void testGetClosestRowBefore() throws IOException, InterruptedException {
    final TableName tableAname = TableName.valueOf("testGetClosestRowBefore");
    final byte[] firstRow = Bytes.toBytes("row111");
    final byte[] secondRow = Bytes.toBytes("row222");
    final byte[] thirdRow = Bytes.toBytes("row333");
    final byte[] forthRow = Bytes.toBytes("row444");
    final byte[] beforeFirstRow = Bytes.toBytes("row");
    final byte[] beforeSecondRow = Bytes.toBytes("row22");
    final byte[] beforeThirdRow = Bytes.toBytes("row33");
    final byte[] beforeForthRow = Bytes.toBytes("row44");

    HTable table =
        TEST_UTIL.createTable(tableAname,
          new byte[][] { HConstants.CATALOG_FAMILY, Bytes.toBytes("info2") }, 1,
            1024);
    TEST_UTIL.waitTableAvailable(tableAname, 10000);
    // set block size to 64 to making 2 kvs into one block, bypassing the walkForwardInSingleRow
    // in Store.rowAtOrBeforeFromStoreFile
    String regionName = table.getRegionLocations().firstKey().getEncodedName();
    Region region =
        TEST_UTIL.getRSForFirstRegionInTable(tableAname).getFromOnlineRegions(regionName);
    Put put1 = new Put(firstRow);
    Put put2 = new Put(secondRow);
    Put put3 = new Put(thirdRow);
    Put put4 = new Put(forthRow);
    byte[] one = new byte[] { 1 };
    byte[] two = new byte[] { 2 };
    byte[] three = new byte[] { 3 };
    byte[] four = new byte[] { 4 };

    put1.add(HConstants.CATALOG_FAMILY, null, one);
    put2.add(HConstants.CATALOG_FAMILY, null, two);
    put3.add(HConstants.CATALOG_FAMILY, null, three);
    put4.add(HConstants.CATALOG_FAMILY, null, four);
    table.put(put1);
    table.put(put2);
    table.put(put3);
    table.put(put4);
    region.flush(true);
    Result result = null;

    // Test before first that null is returned
    result = table.getRowOrBefore(beforeFirstRow, HConstants.CATALOG_FAMILY);
    assertTrue(result == null);

    // Test at first that first is returned
    result = table.getRowOrBefore(firstRow, HConstants.CATALOG_FAMILY);
    assertTrue(result.containsColumn(HConstants.CATALOG_FAMILY, null));
    assertTrue(Bytes.equals(result.getRow(), firstRow));
    assertTrue(Bytes.equals(result.getValue(HConstants.CATALOG_FAMILY, null), one));

    // Test in between first and second that first is returned
    result = table.getRowOrBefore(beforeSecondRow, HConstants.CATALOG_FAMILY);
    assertTrue(result.containsColumn(HConstants.CATALOG_FAMILY, null));
    assertTrue(Bytes.equals(result.getRow(), firstRow));
    assertTrue(Bytes.equals(result.getValue(HConstants.CATALOG_FAMILY, null), one));

    // Test at second make sure second is returned
    result = table.getRowOrBefore(secondRow, HConstants.CATALOG_FAMILY);
    assertTrue(result.containsColumn(HConstants.CATALOG_FAMILY, null));
    assertTrue(Bytes.equals(result.getRow(), secondRow));
    assertTrue(Bytes.equals(result.getValue(HConstants.CATALOG_FAMILY, null), two));

    // Test in second and third, make sure second is returned
    result = table.getRowOrBefore(beforeThirdRow, HConstants.CATALOG_FAMILY);
    assertTrue(result.containsColumn(HConstants.CATALOG_FAMILY, null));
    assertTrue(Bytes.equals(result.getRow(), secondRow));
    assertTrue(Bytes.equals(result.getValue(HConstants.CATALOG_FAMILY, null), two));

    // Test at third make sure third is returned
    result = table.getRowOrBefore(thirdRow, HConstants.CATALOG_FAMILY);
    assertTrue(result.containsColumn(HConstants.CATALOG_FAMILY, null));
    assertTrue(Bytes.equals(result.getRow(), thirdRow));
    assertTrue(Bytes.equals(result.getValue(HConstants.CATALOG_FAMILY, null), three));

    // Test in third and forth, make sure third is returned
    result = table.getRowOrBefore(beforeForthRow, HConstants.CATALOG_FAMILY);
    assertTrue(result.containsColumn(HConstants.CATALOG_FAMILY, null));
    assertTrue(Bytes.equals(result.getRow(), thirdRow));
    assertTrue(Bytes.equals(result.getValue(HConstants.CATALOG_FAMILY, null), three));

    // Test at forth make sure forth is returned
    result = table.getRowOrBefore(forthRow, HConstants.CATALOG_FAMILY);
    assertTrue(result.containsColumn(HConstants.CATALOG_FAMILY, null));
    assertTrue(Bytes.equals(result.getRow(), forthRow));
    assertTrue(Bytes.equals(result.getValue(HConstants.CATALOG_FAMILY, null), four));

    // Test after forth make sure forth is returned
    result = table.getRowOrBefore(Bytes.add(forthRow, one), HConstants.CATALOG_FAMILY);
    assertTrue(result.containsColumn(HConstants.CATALOG_FAMILY, null));
    assertTrue(Bytes.equals(result.getRow(), forthRow));
    assertTrue(Bytes.equals(result.getValue(HConstants.CATALOG_FAMILY, null), four));

    table.close();
  }
```
```
Cyclomatic Complexity	 1
Assertions		 3
Lines of Code		11
-  
  public void testScanVariableReuse() throws Exception {
    Scan scan = new Scan();
    scan.addFamily(FAMILY);
    scan.addColumn(FAMILY, ROW);

    assertTrue(scan.getFamilyMap().get(FAMILY).size() == 1);

    scan = new Scan();
    scan.addFamily(FAMILY);

    assertTrue(scan.getFamilyMap().get(FAMILY) == null);
    assertTrue(scan.getFamilyMap().containsKey(FAMILY));
  }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		28
-  
  public void testMultiRowMutation() throws Exception {
    LOG.info("Starting testMultiRowMutation");
    final TableName TABLENAME = TableName.valueOf("testMultiRowMutation");
    final byte [] ROW1 = Bytes.toBytes("testRow1");

    Table t = TEST_UTIL.createTable(TABLENAME, FAMILY);
    TEST_UTIL.waitTableAvailable(TABLENAME, 10000);
    Put p = new Put(ROW);
    p.add(FAMILY, QUALIFIER, VALUE);
    MutationProto m1 = ProtobufUtil.toMutation(MutationType.PUT, p);

    p = new Put(ROW1);
    p.add(FAMILY, QUALIFIER, VALUE);
    MutationProto m2 = ProtobufUtil.toMutation(MutationType.PUT, p);

    MutateRowsRequest.Builder mrmBuilder = MutateRowsRequest.newBuilder();
    mrmBuilder.addMutationRequest(m1);
    mrmBuilder.addMutationRequest(m2);
    MutateRowsRequest mrm = mrmBuilder.build();
    CoprocessorRpcChannel channel = t.coprocessorService(ROW);
    MultiRowMutationService.BlockingInterface service =
       MultiRowMutationService.newBlockingStub(channel);
    service.mutateRows(null, mrm);
    Get g = new Get(ROW);
    Result r = t.get(g);
    assertEquals(0, Bytes.compareTo(VALUE, r.getValue(FAMILY, QUALIFIER)));
    g = new Get(ROW1);
    r = t.get(g);
    assertEquals(0, Bytes.compareTo(VALUE, r.getValue(FAMILY, QUALIFIER)));
  }
```
```
Cyclomatic Complexity	 3
Assertions		 3
Lines of Code		44
-  
  public void testRowMutation() throws Exception {
    LOG.info("Starting testRowMutation");
    final TableName TABLENAME = TableName.valueOf("testRowMutation");
    Table t = TEST_UTIL.createTable(TABLENAME, FAMILY);
    TEST_UTIL.waitTableAvailable(TABLENAME, 10000);
    byte [][] QUALIFIERS = new byte [][] {
        Bytes.toBytes("a"), Bytes.toBytes("b")
    };
    RowMutations arm = new RowMutations(ROW);
    Put p = new Put(ROW);
    p.add(FAMILY, QUALIFIERS[0], VALUE);
    arm.add(p);
    t.mutateRow(arm);

    Get g = new Get(ROW);
    Result r = t.get(g);
    assertEquals(0, Bytes.compareTo(VALUE, r.getValue(FAMILY, QUALIFIERS[0])));

    arm = new RowMutations(ROW);
    p = new Put(ROW);
    p.add(FAMILY, QUALIFIERS[1], VALUE);
    arm.add(p);
    Delete d = new Delete(ROW);
    d.deleteColumns(FAMILY, QUALIFIERS[0]);
    arm.add(d);
    // TODO: Trying mutateRow again.  The batch was failing with a one try only.
    t.mutateRow(arm);
    r = t.get(g);
    assertEquals(0, Bytes.compareTo(VALUE, r.getValue(FAMILY, QUALIFIERS[1])));
    assertNull(r.getValue(FAMILY, QUALIFIERS[0]));

    //Test that we get a region level exception
    try {
      arm = new RowMutations(ROW);
      p = new Put(ROW);
      p.add(new byte[]{'b', 'o', 'g', 'u', 's'}, QUALIFIERS[0], VALUE);
      arm.add(p);
      t.mutateRow(arm);
      fail("Expected NoSuchColumnFamilyException");
    } catch(RetriesExhaustedWithDetailsException e) {
      for(Throwable rootCause: e.getCauses()){
        if(rootCause instanceof NoSuchColumnFamilyException){
          return;
        }
      }
      throw e;
    }
  }
```
```
Cyclomatic Complexity	 1
Assertions		 6
Lines of Code		27
-  
  public void testAppend() throws Exception {
    LOG.info("Starting testAppend");
    final TableName TABLENAME = TableName.valueOf("testAppend");
    Table t = TEST_UTIL.createTable(TABLENAME, FAMILY);
    TEST_UTIL.waitTableAvailable(TABLENAME, 10000);
    byte[] v1 = Bytes.toBytes("42");
    byte[] v2 = Bytes.toBytes("23");
    byte [][] QUALIFIERS = new byte [][] {
        Bytes.toBytes("b"), Bytes.toBytes("a"), Bytes.toBytes("c")
    };
    Append a = new Append(ROW);
    a.add(FAMILY, QUALIFIERS[0], v1);
    a.add(FAMILY, QUALIFIERS[1], v2);
    a.setReturnResults(false);
    assertNullResult(t.append(a));

    a = new Append(ROW);
    a.add(FAMILY, QUALIFIERS[0], v2);
    a.add(FAMILY, QUALIFIERS[1], v1);
    a.add(FAMILY, QUALIFIERS[2], v2);
    Result r = t.append(a);
    assertEquals(0, Bytes.compareTo(Bytes.add(v1,v2), r.getValue(FAMILY, QUALIFIERS[0])));
    assertEquals(0, Bytes.compareTo(Bytes.add(v2,v1), r.getValue(FAMILY, QUALIFIERS[1])));
    // QUALIFIERS[2] previously not exist, verify both value and timestamp are correct
    assertEquals(0, Bytes.compareTo(v2, r.getValue(FAMILY, QUALIFIERS[2])));
    assertEquals(r.getColumnLatest(FAMILY, QUALIFIERS[0]).getTimestamp(),
        r.getColumnLatest(FAMILY, QUALIFIERS[2]).getTimestamp());
  }
```
```
Cyclomatic Complexity	 3
Assertions		 2
Lines of Code		30
-  
  public void testClientPoolRoundRobin() throws IOException, InterruptedException {
    final TableName tableName = TableName.valueOf("testClientPoolRoundRobin");

    int poolSize = 3;
    int numVersions = poolSize * 2;
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.set(HConstants.HBASE_CLIENT_IPC_POOL_TYPE, "round-robin");
    conf.setInt(HConstants.HBASE_CLIENT_IPC_POOL_SIZE, poolSize);

    Table table = TEST_UTIL.createTable(tableName, new byte[][] { FAMILY }, conf, Integer.MAX_VALUE);
    TEST_UTIL.waitTableAvailable(tableName, 10000);

    final long ts = EnvironmentEdgeManager.currentTime();
    Get get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIER);
    get.setMaxVersions();

    for (int versions = 1; versions <= numVersions; versions++) {
      Put put = new Put(ROW);
      put.add(FAMILY, QUALIFIER, ts + versions, VALUE);
      table.put(put);

      Result result = table.get(get);
      NavigableMap<Long, byte[]> navigableMap = result.getMap().get(FAMILY)
          .get(QUALIFIER);

      assertEquals("The number of versions of '" + Bytes.toString(FAMILY) + ":"
          + Bytes.toString(QUALIFIER) + " did not match", versions, navigableMap.size());
      for (Map.Entry<Long, byte[]> entry : navigableMap.entrySet()) {
        assertTrue("The value at time " + entry.getKey()
            + " did not match what was put",
            Bytes.equals(VALUE, entry.getValue()));
      }
    }
  }
```
```
Cyclomatic Complexity	 -1
Assertions		 5
Lines of Code		71
-8989") 
  public void testClientPoolThreadLocal() throws IOException, InterruptedException {
    final TableName tableName = TableName.valueOf("testClientPoolThreadLocal");

    int poolSize = Integer.MAX_VALUE;
    int numVersions = 3;
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.set(HConstants.HBASE_CLIENT_IPC_POOL_TYPE, "thread-local");
    conf.setInt(HConstants.HBASE_CLIENT_IPC_POOL_SIZE, poolSize);

    final Table table = TEST_UTIL.createTable(tableName,
        new byte[][] { FAMILY }, conf, 3);
    TEST_UTIL.waitTableAvailable(tableName, 10000);
    final long ts = EnvironmentEdgeManager.currentTime();
    final Get get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIER);
    get.setMaxVersions();

    for (int versions = 1; versions <= numVersions; versions++) {
      Put put = new Put(ROW);
      put.add(FAMILY, QUALIFIER, ts + versions, VALUE);
      table.put(put);

      Result result = table.get(get);
      NavigableMap<Long, byte[]> navigableMap = result.getMap().get(FAMILY)
          .get(QUALIFIER);

      assertEquals("The number of versions of '" + Bytes.toString(FAMILY) + ":"
          + Bytes.toString(QUALIFIER) + " did not match", versions, navigableMap.size());
      for (Map.Entry<Long, byte[]> entry : navigableMap.entrySet()) {
        assertTrue("The value at time " + entry.getKey()
            + " did not match what was put",
            Bytes.equals(VALUE, entry.getValue()));
      }
    }

    final Object waitLock = new Object();
    ExecutorService executorService = Executors.newFixedThreadPool(numVersions);
    final AtomicReference<AssertionError> error = new AtomicReference<AssertionError>(null);
    for (int versions = numVersions; versions < numVersions * 2; versions++) {
      final int versionsCopy = versions;
      executorService.submit(new Callable<Void>() {
        @Override
        public Void call() {
          try {
            Put put = new Put(ROW);
            put.add(FAMILY, QUALIFIER, ts + versionsCopy, VALUE);
            table.put(put);

            Result result = table.get(get);
            NavigableMap<Long, byte[]> navigableMap = result.getMap()
                .get(FAMILY).get(QUALIFIER);

            assertEquals("The number of versions of '" + Bytes.toString(FAMILY) + ":"
                + Bytes.toString(QUALIFIER) + " did not match " + versionsCopy, versionsCopy,
                navigableMap.size());
            for (Map.Entry<Long, byte[]> entry : navigableMap.entrySet()) {
              assertTrue("The value at time " + entry.getKey()
                  + " did not match what was put",
                  Bytes.equals(VALUE, entry.getValue()));
            }
            synchronized (waitLock) {
              waitLock.wait();
            }
          } catch (Exception e) {
          } catch (AssertionError e) {
            // the error happens in a thread, it won't fail the test,
            // need to pass it to the caller for proper handling.
            error.set(e);
            LOG.error(e);
          }

          return null;
        }
      });
    }
    synchronized (waitLock) {
      waitLock.notifyAll();
    }
    executorService.shutdownNow();
    assertNull(error.get());
  }
```
```
Cyclomatic Complexity	 3
Assertions		 4
Lines of Code		25
-  
  public void testCheckAndPut() throws IOException, InterruptedException {
    final byte [] anotherrow = Bytes.toBytes("anotherrow");
    final byte [] value2 = Bytes.toBytes("abcd");

    Table table = TEST_UTIL.createTable(TableName.valueOf("testCheckAndPut"), FAMILY);
    TEST_UTIL.waitTableAvailable(TableName.valueOf("testCheckAndPut"), 10000);
    Put put1 = new Put(ROW);
    put1.add(FAMILY, QUALIFIER, VALUE);

    // row doesn't exist, so using non-null value should be considered "not match".
    boolean ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, VALUE, put1);
    assertEquals(ok, false);

    // row doesn't exist, so using "null" to check for existence should be considered "match".
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, null, put1);
    assertEquals(ok, true);

    // row now exists, so using "null" to check for existence should be considered "not match".
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, null, put1);
    assertEquals(ok, false);

    Put put2 = new Put(ROW);
    put2.add(FAMILY, QUALIFIER, value2);

    // row now exists, use the matching value to check
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, VALUE, put2);
    assertEquals(ok, true);

    Put put3 = new Put(anotherrow);
    put3.add(FAMILY, QUALIFIER, VALUE);

    // try to do CheckAndPut on different rows
    try {
        ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, value2, put3);
        fail("trying to check and modify different rows should have failed.");
    } catch(Exception e) {}
  }
```
```
Cyclomatic Complexity	 2
Assertions		 19
Lines of Code		51
-  
  public void testCheckAndPutWithCompareOp() throws IOException, InterruptedException {
    final byte [] value1 = Bytes.toBytes("aaaa");
    final byte [] value2 = Bytes.toBytes("bbbb");
    final byte [] value3 = Bytes.toBytes("cccc");
    final byte [] value4 = Bytes.toBytes("dddd");

    Table table = TEST_UTIL.createTable(TableName.valueOf("testCheckAndPutWithCompareOp"), FAMILY);
    TEST_UTIL.waitTableAvailable(TableName.valueOf("testCheckAndPutWithCompareOp"), 10000);

    Put put2 = new Put(ROW);
    put2.add(FAMILY, QUALIFIER, value2);

    Put put3 = new Put(ROW);
    put3.add(FAMILY, QUALIFIER, value3);

    // row doesn't exist, so using "null" to check for existence should be considered "match".
    boolean ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, null, put2);
    assertEquals(ok, true);

    // cell = "bbbb", using "aaaa" to compare only LESS/LESS_OR_EQUAL/NOT_EQUAL
    // turns out "match"
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, CompareOp.GREATER, value1, put2);
    assertEquals(ok, false);
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, CompareOp.EQUAL, value1, put2);
    assertEquals(ok, false);
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, CompareOp.GREATER_OR_EQUAL, value1, put2);
    assertEquals(ok, false);
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, CompareOp.LESS, value1, put2);
    assertEquals(ok, true);
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, CompareOp.LESS_OR_EQUAL, value1, put2);
    assertEquals(ok, true);
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, CompareOp.NOT_EQUAL, value1, put3);
    assertEquals(ok, true);

    // cell = "cccc", using "dddd" to compare only LARGER/LARGER_OR_EQUAL/NOT_EQUAL
    // turns out "match"
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, CompareOp.LESS, value4, put3);
    assertEquals(ok, false);
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, CompareOp.LESS_OR_EQUAL, value4, put3);
    assertEquals(ok, false);
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, CompareOp.EQUAL, value4, put3);
    assertEquals(ok, false);
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, CompareOp.GREATER, value4, put3);
    assertEquals(ok, true);
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, CompareOp.GREATER_OR_EQUAL, value4, put3);
    assertEquals(ok, true);
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, CompareOp.NOT_EQUAL, value4, put2);
    assertEquals(ok, true);

    // cell = "bbbb", using "bbbb" to compare only GREATER_OR_EQUAL/LESS_OR_EQUAL/EQUAL
    // turns out "match"
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, CompareOp.GREATER, value2, put2);
    assertEquals(ok, false);
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, CompareOp.NOT_EQUAL, value2, put2);
    assertEquals(ok, false);
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, CompareOp.LESS, value2, put2);
    assertEquals(ok, false);
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, CompareOp.GREATER_OR_EQUAL, value2, put2);
    assertEquals(ok, true);
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, CompareOp.LESS_OR_EQUAL, value2, put2);
    assertEquals(ok, true);
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, CompareOp.EQUAL, value2, put3);
    assertEquals(ok, true);
  }
```
```
Cyclomatic Complexity	 1
Assertions		 33
Lines of Code		66
-  
  public void testCheckAndDeleteWithCompareOp() throws IOException, InterruptedException {
    final byte [] value1 = Bytes.toBytes("aaaa");
    final byte [] value2 = Bytes.toBytes("bbbb");
    final byte [] value3 = Bytes.toBytes("cccc");
    final byte [] value4 = Bytes.toBytes("dddd");
    Table table = TEST_UTIL.createTable(TableName.valueOf("testCheckAndDeleteWithCompareOp"),
        FAMILY);
    TEST_UTIL.waitTableAvailable(TableName.valueOf("testCheckAndDeleteWithCompareOp"), 10000);

    Put bbbb = new Put(ROW);
    bbbb.add(FAMILY, QUALIFIER, value2);

    Put cccc = new Put(ROW);
    cccc.add(FAMILY, QUALIFIER, value3);

    Delete delete = new Delete(ROW);
    delete.deleteColumns(FAMILY, QUALIFIER);

    // cell = "bbbb", using "aaaa" to compare only LESS/LESS_OR_EQUAL/NOT_EQUAL
    // turns out "match"
    table.put(bbbb);
    assertTrue(verifyPut(table, bbbb, value2));
    boolean ok = table.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareOp.GREATER, value1, delete);
    // aaaa is less than bbbb, > || >= should be false
    assertFalse(ok);
    ok = table.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareOp.EQUAL, value1, delete);
    assertFalse(ok);
    ok = table.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareOp.GREATER_OR_EQUAL, value1, delete);
    assertFalse(ok);
    // aaaa is less than bbbb, < || <= should be true
    ok = table.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareOp.LESS, value1, delete);
    assertTrue(ok);
    table.put(bbbb);
    assertTrue(verifyPut(table, bbbb, value2));
    ok = table.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareOp.LESS_OR_EQUAL, value1, delete);
    assertTrue(ok);

    // cell = "cccc", using "dddd" to compare only LARGER/LARGER_OR_EQUAL/NOT_EQUAL
    // turns out "match"
    table.put(cccc);
    assertTrue(verifyPut(table, cccc, value3));
    // dddd is larger than cccc,  < || <= shoule be false
    ok = table.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareOp.LESS, value4, delete);
    assertFalse(ok);
    ok = table.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareOp.LESS_OR_EQUAL, value4, delete);
    assertFalse(ok);
    ok = table.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareOp.EQUAL, value4, delete);
    assertFalse(ok);
    // dddd is larger than cccc, (> || >= || !=) shoule be true
    ok = table.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareOp.GREATER, value4, delete);
    assertTrue(ok);
    table.put(cccc);
    assertTrue(verifyPut(table, cccc, value3));
    ok = table.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareOp.GREATER_OR_EQUAL, value4, delete);
    assertTrue(ok);
    table.put(cccc);
    assertTrue(verifyPut(table, cccc, value3));
    ok = table.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareOp.NOT_EQUAL, value4, delete);
    assertTrue(ok);

    // cell = "bbbb", using "bbbb" to compare only GREATER_OR_EQUAL/LESS_OR_EQUAL/EQUAL
    // turns out "match"
    // bbbb equals to bbbb, != shoule be all false
    table.put(bbbb);
    assertTrue(verifyPut(table, bbbb, value2));
    ok = table.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareOp.GREATER, value2, delete);
    assertFalse(ok);
    ok = table.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareOp.NOT_EQUAL, value2, delete);
    assertFalse(ok);
    ok = table.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareOp.LESS, value2, delete);
    assertFalse(ok);
    ok = table.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareOp.GREATER_OR_EQUAL, value2, delete);
    assertTrue(ok);
    table.put(bbbb);
    assertTrue(verifyPut(table, bbbb, value2));
    ok = table.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareOp.LESS_OR_EQUAL, value2, delete);
    assertTrue(ok);
    table.put(bbbb);
    assertTrue(verifyPut(table, bbbb, value2));
    ok = table.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareOp.EQUAL, value2, delete);
    assertTrue(ok);
  }
```
```
Cyclomatic Complexity	 5
Assertions		 29
Lines of Code		83
-  
  public void testCacheOnWriteEvictOnClose() throws Exception {
    TableName tableName = TableName.valueOf("testCOWEOCfromClient");
    byte [] data = Bytes.toBytes("data");
    HTable table = TEST_UTIL.createTable(tableName, FAMILY);
    TEST_UTIL.waitTableAvailable(tableName, 10000);
    // get the block cache and region
    String regionName = table.getRegionLocations().firstKey().getEncodedName();
    Region region = TEST_UTIL.getRSForFirstRegionInTable(tableName)
      .getFromOnlineRegions(regionName);
    Store store = region.getStores().iterator().next();
    CacheConfig cacheConf = store.getCacheConfig();
    cacheConf.setCacheDataOnWrite(true);
    cacheConf.setEvictOnClose(true);
    BlockCache cache = cacheConf.getBlockCache();

    // establish baseline stats
    long startBlockCount = cache.getBlockCount();
    long startBlockHits = cache.getStats().getHitCount();
    long startBlockMiss = cache.getStats().getMissCount();

    // wait till baseline is stable, (minimal 500 ms)
    for (int i = 0; i < 5; i++) {
      Thread.sleep(100);
      if (startBlockCount != cache.getBlockCount()
          || startBlockHits != cache.getStats().getHitCount()
          || startBlockMiss != cache.getStats().getMissCount()) {
        startBlockCount = cache.getBlockCount();
        startBlockHits = cache.getStats().getHitCount();
        startBlockMiss = cache.getStats().getMissCount();
        i = -1;
      }
    }

    // insert data
    Put put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, data);
    table.put(put);
    assertTrue(Bytes.equals(table.get(new Get(ROW)).value(), data));
    // data was in memstore so don't expect any changes
    assertEquals(startBlockCount, cache.getBlockCount());
    assertEquals(startBlockHits, cache.getStats().getHitCount());
    assertEquals(startBlockMiss, cache.getStats().getMissCount());
    // flush the data
    LOG.debug("Flushing cache");
    region.flush(true);
    // expect two more blocks in cache - DATA and ROOT_INDEX
    // , no change in hits/misses
    long expectedBlockCount = startBlockCount + 2;
    long expectedBlockHits = startBlockHits;
    long expectedBlockMiss = startBlockMiss;
    assertEquals(expectedBlockCount, cache.getBlockCount());
    assertEquals(expectedBlockHits, cache.getStats().getHitCount());
    assertEquals(expectedBlockMiss, cache.getStats().getMissCount());
    // read the data and expect same blocks, one new hit, no misses
    assertTrue(Bytes.equals(table.get(new Get(ROW)).value(), data));
    assertEquals(expectedBlockCount, cache.getBlockCount());
    assertEquals(++expectedBlockHits, cache.getStats().getHitCount());
    assertEquals(expectedBlockMiss, cache.getStats().getMissCount());
    // insert a second column, read the row, no new blocks, one new hit
    byte [] QUALIFIER2 = Bytes.add(QUALIFIER, QUALIFIER);
    byte [] data2 = Bytes.add(data, data);
    put = new Put(ROW);
    put.add(FAMILY, QUALIFIER2, data2);
    table.put(put);
    Result r = table.get(new Get(ROW));
    assertTrue(Bytes.equals(r.getValue(FAMILY, QUALIFIER), data));
    assertTrue(Bytes.equals(r.getValue(FAMILY, QUALIFIER2), data2));
    assertEquals(expectedBlockCount, cache.getBlockCount());
    assertEquals(++expectedBlockHits, cache.getStats().getHitCount());
    assertEquals(expectedBlockMiss, cache.getStats().getMissCount());
    // flush, one new block
    System.out.println("Flushing cache");
    region.flush(true);
    // + 1 for Index Block, +1 for data block
    expectedBlockCount += 2;
    assertEquals(expectedBlockCount, cache.getBlockCount());
    assertEquals(expectedBlockHits, cache.getStats().getHitCount());
    assertEquals(expectedBlockMiss, cache.getStats().getMissCount());
    // compact, net minus two blocks, two hits, no misses
    System.out.println("Compacting");
    assertEquals(2, store.getStorefilesCount());
    store.triggerMajorCompaction();
    region.compact(true);
    waitForStoreFileCount(store, 1, 10000); // wait 10 seconds max
    assertEquals(1, store.getStorefilesCount());
    // evicted two data blocks and two index blocks and compaction does not cache new blocks
    expectedBlockCount -= 4;
    assertEquals(expectedBlockCount, cache.getBlockCount());
    expectedBlockHits += 2;
    assertEquals(expectedBlockMiss, cache.getStats().getMissCount());
    assertEquals(expectedBlockHits, cache.getStats().getHitCount());
    // read the row, this should be a cache miss because we don't cache data
    // blocks on compaction
    r = table.get(new Get(ROW));
    assertTrue(Bytes.equals(r.getValue(FAMILY, QUALIFIER), data));
    assertTrue(Bytes.equals(r.getValue(FAMILY, QUALIFIER2), data2));
    expectedBlockCount += 1; // cached one data block
    assertEquals(expectedBlockCount, cache.getBlockCount());
    assertEquals(expectedBlockHits, cache.getStats().getHitCount());
    assertEquals(++expectedBlockMiss, cache.getStats().getMissCount());
  }
```
```
Cyclomatic Complexity	 4
Assertions		 6
Lines of Code		35
-  
  /**
   * Tests the non cached version of getRegionLocator by moving a region.
   */
  public void testNonCachedGetRegionLocation() throws Exception {
    // Test Initialization.
    TableName TABLE = TableName.valueOf("testNonCachedGetRegionLocation");
    byte [] family1 = Bytes.toBytes("f1");
    byte [] family2 = Bytes.toBytes("f2");
    try (HTable table = TEST_UTIL.createTable(TABLE, new byte[][] {family1, family2}, 10);
        Admin admin = new HBaseAdmin(TEST_UTIL.getConfiguration())) {
      TEST_UTIL.waitTableAvailable(TABLE, 10000);
      Map <HRegionInfo, ServerName> regionsMap = table.getRegionLocations();
      assertEquals(1, regionsMap.size());
      HRegionInfo regionInfo = regionsMap.keySet().iterator().next();
      ServerName addrBefore = regionsMap.get(regionInfo);
      // Verify region location before move.
      HRegionLocation addrCache = table.getRegionLocation(regionInfo.getStartKey(), false);
      HRegionLocation addrNoCache = table.getRegionLocation(regionInfo.getStartKey(),  true);

      assertEquals(addrBefore.getPort(), addrCache.getPort());
      assertEquals(addrBefore.getPort(), addrNoCache.getPort());

      ServerName addrAfter = null;
      // Now move the region to a different server.
      for (int i = 0; i < SLAVES; i++) {
        HRegionServer regionServer = TEST_UTIL.getHBaseCluster().getRegionServer(i);
        ServerName addr = regionServer.getServerName();
        if (addr.getPort() != addrBefore.getPort()) {
          admin.move(regionInfo.getEncodedNameAsBytes(),
              Bytes.toBytes(addr.toString()));
          // Wait for the region to move.
          Thread.sleep(5000);
          addrAfter = addr;
          break;
        }
      }

      // Verify the region was moved.
      addrCache = table.getRegionLocation(regionInfo.getStartKey(), false);
      addrNoCache = table.getRegionLocation(regionInfo.getStartKey(), true);
      assertNotNull(addrAfter);
      assertTrue(addrAfter.getPort() != addrCache.getPort());
      assertEquals(addrAfter.getPort(), addrNoCache.getPort());
    }
  }
```
```
Cyclomatic Complexity	 1
Assertions		 10
Lines of Code		37
-  
  /**
   * Tests getRegionsInRange by creating some regions over which a range of
   * keys spans; then changing the key range.
   */
  public void testGetRegionsInRange() throws Exception {
    // Test Initialization.
    byte [] startKey = Bytes.toBytes("ddc");
    byte [] endKey = Bytes.toBytes("mmm");
    TableName TABLE = TableName.valueOf("testGetRegionsInRange");
    HTable table = TEST_UTIL.createMultiRegionTable(TABLE, new byte[][] { FAMILY }, 10);
    int numOfRegions = -1;
    try (RegionLocator r = table.getRegionLocator()) {
      numOfRegions = r.getStartKeys().length;
    }
    assertEquals(26, numOfRegions);

    // Get the regions in this range
    List<HRegionLocation> regionsList = table.getRegionsInRange(startKey,
      endKey);
    assertEquals(10, regionsList.size());

    // Change the start key
    startKey = Bytes.toBytes("fff");
    regionsList = table.getRegionsInRange(startKey, endKey);
    assertEquals(7, regionsList.size());

    // Change the end key
    endKey = Bytes.toBytes("nnn");
    regionsList = table.getRegionsInRange(startKey, endKey);
    assertEquals(8, regionsList.size());

    // Empty start key
    regionsList = table.getRegionsInRange(HConstants.EMPTY_START_ROW, endKey);
    assertEquals(13, regionsList.size());

    // Empty end key
    regionsList = table.getRegionsInRange(startKey, HConstants.EMPTY_END_ROW);
    assertEquals(21, regionsList.size());

    // Both start and end keys empty
    regionsList = table.getRegionsInRange(HConstants.EMPTY_START_ROW,
      HConstants.EMPTY_END_ROW);
    assertEquals(26, regionsList.size());

    // Change the end key to somewhere in the last block
    endKey = Bytes.toBytes("zzz1");
    regionsList = table.getRegionsInRange(startKey, endKey);
    assertEquals(21, regionsList.size());

    // Change the start key to somewhere in the first block
    startKey = Bytes.toBytes("aac");
    regionsList = table.getRegionsInRange(startKey, endKey);
    assertEquals(26, regionsList.size());

    // Make start and end key the same
    startKey = endKey = Bytes.toBytes("ccc");
    regionsList = table.getRegionsInRange(startKey, endKey);
    assertEquals(1, regionsList.size());
  }
```
```
Cyclomatic Complexity	 2
Assertions		 1
Lines of Code		22
-  
  public void testJira6912() throws Exception {
    TableName TABLE = TableName.valueOf("testJira6912");
    Table foo = TEST_UTIL.createTable(TABLE, new byte[][] {FAMILY}, 10);
    TEST_UTIL.waitTableAvailable(TABLE, 10000);
    List<Put> puts = new ArrayList<Put>();
    for (int i=0;i !=100; i++){
      Put put = new Put(Bytes.toBytes(i));
      put.add(FAMILY, FAMILY, Bytes.toBytes(i));
      puts.add(put);
    }
    foo.put(puts);
    // If i comment this out it works
    TEST_UTIL.flush();

    Scan scan = new Scan();
    scan.setStartRow(Bytes.toBytes(1));
    scan.setStopRow(Bytes.toBytes(3));
    scan.addColumn(FAMILY, FAMILY);
    scan.setFilter(new RowFilter(CompareFilter.CompareOp.NOT_EQUAL, new BinaryComparator(Bytes.toBytes(1))));

    ResultScanner scanner = foo.getScanner(scan);
    Result[] bar = scanner.next(100);
    assertEquals(1, bar.length);
  }
```
```
Cyclomatic Complexity	 1
Assertions		 4
Lines of Code		24
-  
  public void testScan_NullQualifier() throws IOException, InterruptedException {
    Table table = TEST_UTIL.createTable(TableName.valueOf("testScan_NullQualifier"), FAMILY);
    TEST_UTIL.waitTableAvailable(TableName.valueOf("testScan_NullQualifier"), 10000);

    Put put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, VALUE);
    table.put(put);

    put = new Put(ROW);
    put.add(FAMILY, null, VALUE);
    table.put(put);
    LOG.info("Row put");

    Scan scan = new Scan();
    scan.addColumn(FAMILY, null);

    ResultScanner scanner = table.getScanner(scan);
    Result[] bar = scanner.next(100);
    assertEquals(1, bar.length);
    assertEquals(1, bar[0].size());

    scan = new Scan();
    scan.addFamily(FAMILY);

    scanner = table.getScanner(scan);
    bar = scanner.next(100);
    assertEquals(1, bar.length);
    assertEquals(2, bar[0].size());
  }
```
```
Cyclomatic Complexity	 2
Assertions		 2
Lines of Code		81
-  
  public void testIllegalTableDescriptor() throws Exception {
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf("testIllegalTableDescriptor"));
    HColumnDescriptor hcd = new HColumnDescriptor(FAMILY);

    // create table with 0 families
    checkTableIsIllegal(htd);
    htd.addFamily(hcd);
    checkTableIsLegal(htd);

    htd.setMaxFileSize(1024); // 1K
    checkTableIsIllegal(htd);
    htd.setMaxFileSize(0);
    checkTableIsIllegal(htd);
    htd.setMaxFileSize(1024 * 1024 * 1024); // 1G
    checkTableIsLegal(htd);

    htd.setMemStoreFlushSize(1024);
    checkTableIsIllegal(htd);
    htd.setMemStoreFlushSize(0);
    checkTableIsIllegal(htd);
    htd.setMemStoreFlushSize(128 * 1024 * 1024); // 128M
    checkTableIsLegal(htd);

    htd.setRegionSplitPolicyClassName("nonexisting.foo.class");
    checkTableIsIllegal(htd);
    htd.setRegionSplitPolicyClassName(null);
    checkTableIsLegal(htd);

    hcd.setBlocksize(0);
    checkTableIsIllegal(htd);
    hcd.setBlocksize(1024 * 1024 * 128); // 128M
    checkTableIsIllegal(htd);
    hcd.setBlocksize(1024);
    checkTableIsLegal(htd);

    hcd.setTimeToLive(0);
    checkTableIsIllegal(htd);
    hcd.setTimeToLive(-1);
    checkTableIsIllegal(htd);
    hcd.setTimeToLive(1);
    checkTableIsLegal(htd);

    hcd.setMinVersions(-1);
    checkTableIsIllegal(htd);
    hcd.setMinVersions(3);
    try {
      hcd.setMaxVersions(2);
      fail();
    } catch (IllegalArgumentException ex) {
      // expected
      hcd.setMaxVersions(10);
    }
    checkTableIsLegal(htd);

    // HBASE-13776 Setting illegal versions for HColumnDescriptor
    //  does not throw IllegalArgumentException
    // finally, minVersions must be less than or equal to maxVersions
    hcd.setMaxVersions(4);
    hcd.setMinVersions(5);
    checkTableIsIllegal(htd);
    hcd.setMinVersions(3);

    try {
      hcd.setScope(-1);
      fail("Illegal value for setScope did not throw");
    } catch (IllegalArgumentException e) {
      // expected
      hcd.setScope(0);
    }
    checkTableIsLegal(htd);

    try {
      hcd.setDFSReplication((short) -1);
      fail("Illegal value for setDFSReplication did not throw");
    } catch (IllegalArgumentException e) {
      // pass
    }
    // set an illegal DFS replication value by hand
    hcd.setValue(HColumnDescriptor.DFS_REPLICATION, "-1");
    checkTableIsIllegal(htd);
    try {
      hcd.setDFSReplication((short) -1);
      fail("Should throw exception if an illegal value is explicitly being set");
    } catch (IllegalArgumentException e) {
      // pass
    }

    // check the conf settings to disable sanity checks
    htd.setMemStoreFlushSize(0);

    // Check that logs warn on invalid table but allow it.
    ListAppender listAppender = new ListAppender();
    Logger log = Logger.getLogger(HMaster.class);
    log.addAppender(listAppender);
    log.setLevel(Level.WARN);

    htd.setConfiguration("hbase.table.sanity.checks", Boolean.FALSE.toString());
    checkTableIsLegal(htd);

    assertFalse(listAppender.getMessages().isEmpty());
    assertTrue(listAppender.getMessages().get(0).startsWith("MEMSTORE_FLUSHSIZE for table "
        + "descriptor or \"hbase.hregion.memstore.flush.size\" (0) is too small, which might "
        + "cause very frequent flushing."));

    log.removeAppender(listAppender);
  }
```
```
Cyclomatic Complexity	 2
Assertions		 3
Lines of Code		21
-  
  public void testUserRegionLockThrowsException() throws IOException, InterruptedException {
    ((FakeRSRpcServices)badRS.getRSRpcServices()).setExceptionInjector(new LockSleepInjector());
    Configuration conf = new Configuration(TEST_UTIL.getConfiguration());
    conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 1);
    conf.setLong(HConstants.HBASE_CLIENT_META_OPERATION_TIMEOUT, 2000);
    conf.setLong(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 2000);

    try (ConnectionManager.HConnectionImplementation conn =
           (ConnectionManager.HConnectionImplementation) ConnectionFactory.createConnection(conf)) {
      ClientThread client1 = new ClientThread(conn);
      ClientThread client2 = new ClientThread(conn);
      client1.start();
      client2.start();
      client1.join();
      client2.join();
      // One thread will get the lock but will sleep in  LockExceptionInjector#throwOnScan and
      // eventually fail since the sleep time is more than hbase client scanner timeout period.
      // Other thread will wait to acquire userRegionLock.
      // Have no idea which thread will be scheduled first. So need to check both threads.

      // Both the threads will throw exception. One thread will throw exception since after
      // acquiring user region lock, it is sleeping for 5 seconds when the scanner time out period
      // is 2 seconds.
      // Other thread will throw exception since it was not able to get hold of user region lock
      // within meta operation timeout period.
      assertNotNull(client1.getException());
      assertNotNull(client2.getException());

      assertTrue(client1.getException() instanceof LockTimeoutException
        ^ client2.getException() instanceof LockTimeoutException);
    }
  }
```
```
Cyclomatic Complexity	 2
Assertions		 0
Lines of Code		21
-  
  public void testGetRegionLocationFromPrimaryMetaRegion() throws IOException, InterruptedException {
    HTU.getHBaseAdmin().setBalancerRunning(false, true);

    ((ConnectionManager.HConnectionImplementation) HTU.getHBaseAdmin().getConnection()).
        setUseMetaReplicas(true);

    // Create table then get the single region for our new table.
    HTableDescriptor hdt = HTU.createTableDescriptor("testGetRegionLocationFromPrimaryMetaRegion");
    hdt.setRegionReplication(2);
    try {

      HTU.createTable(hdt, new byte[][] { f }, null);

      RegionServerHostingPrimayMetaRegionSlowOrStopCopro.slowDownPrimaryMetaScan = true;

      // Get user table location, always get it from the primary meta replica
      RegionLocations url = ((ClusterConnection) HTU.getConnection())
          .locateRegion(hdt.getTableName(), row, false, false);

    } finally {
      RegionServerHostingPrimayMetaRegionSlowOrStopCopro.slowDownPrimaryMetaScan = false;
      ((ConnectionManager.HConnectionImplementation) HTU.getHBaseAdmin().getConnection()).
          setUseMetaReplicas(false);
      HTU.getHBaseAdmin().setBalancerRunning(true, true);
      HTU.getHBaseAdmin().disableTable(hdt.getTableName());
      HTU.deleteTable(hdt.getTableName());
    }
  }
```
```
Cyclomatic Complexity	 7
Assertions		 2
Lines of Code		66
-  
  public void testReplicaGetWithPrimaryAndMetaDown() throws IOException, InterruptedException {
    HTU.getHBaseAdmin().setBalancerRunning(false, true);

    ((ConnectionManager.HConnectionImplementation)HTU.getHBaseAdmin().getConnection()).
        setUseMetaReplicas(true);

    // Create table then get the single region for our new table.
    HTableDescriptor hdt = HTU.createTableDescriptor("testReplicaGetWithPrimaryAndMetaDown");
    hdt.setRegionReplication(2);
    try {

      Table table = HTU.createTable(hdt, new byte[][] { f }, null);

      // Get Meta location
      RegionLocations mrl = ((ClusterConnection) HTU.getConnection())
          .locateRegion(TableName.META_TABLE_NAME,
              HConstants.EMPTY_START_ROW, false, false);

      // Get user table location
      RegionLocations url = ((ClusterConnection) HTU.getConnection())
          .locateRegion(hdt.getTableName(), row, false, false);

      // Make sure that user primary region is co-hosted with the meta region
      if (!url.getDefaultRegionLocation().getServerName().equals(
          mrl.getDefaultRegionLocation().getServerName())) {
        HTU.moveRegionAndWait(url.getDefaultRegionLocation().getRegionInfo(),
            mrl.getDefaultRegionLocation().getServerName());
      }

      // Make sure that the user replica region is not hosted by the same region server with
      // primary
      if (url.getRegionLocation(1).getServerName().equals(mrl.getDefaultRegionLocation()
          .getServerName())) {
        HTU.moveRegionAndWait(url.getRegionLocation(1).getRegionInfo(),
            url.getDefaultRegionLocation().getServerName());
      }

      // Wait until the meta table is updated with new location info
      while (true) {
        mrl = ((ClusterConnection) HTU.getConnection())
            .locateRegion(TableName.META_TABLE_NAME, HConstants.EMPTY_START_ROW, false, false);

        // Get user table location
        url = ((ClusterConnection) HTU.getConnection())
            .locateRegion(hdt.getTableName(), row, false, true);

        LOG.info("meta locations " + mrl);
        LOG.info("table locations " + url);
        ServerName a = url.getDefaultRegionLocation().getServerName();
        ServerName b = mrl.getDefaultRegionLocation().getServerName();
        if(a.equals(b)) {
          break;
        } else {
          LOG.info("Waiting for new region info to be updated in meta table");
          Thread.sleep(100);
        }
      }

      Put p = new Put(row);
      p.addColumn(f, row, row);
      table.put(p);

      // Flush so it can be picked by the replica refresher thread
      HTU.flush(table.getName());

      // Sleep for some time until data is picked up by replicas
      try {
        Thread.sleep(2 * REFRESH_PERIOD);
      } catch (InterruptedException e1) {
        LOG.error(e1);
      }

      // Simulating the RS down
      RegionServerHostingPrimayMetaRegionSlowOrStopCopro.throwException = true;

      // The first Get is supposed to succeed
      Get g = new Get(row);
      g.setConsistency(Consistency.TIMELINE);
      Result r = table.get(g);
      Assert.assertTrue(r.isStale());

      // The second Get will succeed as well
      r = table.get(g);
      Assert.assertTrue(r.isStale());

    } finally {
      ((ConnectionManager.HConnectionImplementation)HTU.getHBaseAdmin().getConnection()).
          setUseMetaReplicas(false);
      RegionServerHostingPrimayMetaRegionSlowOrStopCopro.throwException = false;
      HTU.getHBaseAdmin().setBalancerRunning(true, true);
      HTU.getHBaseAdmin().disableTable(hdt.getTableName());@@@+      HTU.getAdmin().disableTable(hdt.getTableName());@@@       HTU.deleteTable(hdt.getTableName());@@@     }@@@   }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		5
-  
  public void testLegacyWALObserverWriteToWAL() throws Exception {
    final WAL log = wals.getWAL(UNSPECIFIED_REGION, null);
    verifyWritesSeen(log, getCoprocessor(log, SampleRegionWALObserver.Legacy.class), true);
  }
```
```
Cyclomatic Complexity	 2
Assertions		 24
Lines of Code		64
-  
  public void testNonLegacyWALKeysDoNotExplode() throws Exception {
    TableName tableName = TableName.valueOf(TEST_TABLE);
    final HTableDescriptor htd = createBasic3FamilyHTD(Bytes
        .toString(TEST_TABLE));
    final HRegionInfo hri = new HRegionInfo(tableName, null, null);
    MultiVersionConcurrencyControl mvcc = new MultiVersionConcurrencyControl();

    fs.mkdirs(new Path(FSUtils.getTableDir(hbaseRootDir, tableName), hri.getEncodedName()));

    final Configuration newConf = HBaseConfiguration.create(this.conf);

    final WAL wal = wals.getWAL(UNSPECIFIED_REGION, null);
    final SampleRegionWALObserver newApi = getCoprocessor(wal, SampleRegionWALObserver.class);
    newApi.setTestValues(TEST_TABLE, TEST_ROW, null, null, null, null, null, null);
    final SampleRegionWALObserver oldApi = getCoprocessor(wal,
        SampleRegionWALObserver.Legacy.class);
    oldApi.setTestValues(TEST_TABLE, TEST_ROW, null, null, null, null, null, null);

    LOG.debug("ensuring wal entries haven't happened before we start");
    assertFalse(newApi.isPreWALWriteCalled());
    assertFalse(newApi.isPostWALWriteCalled());
    assertFalse(newApi.isPreWALWriteDeprecatedCalled());
    assertFalse(newApi.isPostWALWriteDeprecatedCalled());
    assertFalse(oldApi.isPreWALWriteCalled());
    assertFalse(oldApi.isPostWALWriteCalled());
    assertFalse(oldApi.isPreWALWriteDeprecatedCalled());
    assertFalse(oldApi.isPostWALWriteDeprecatedCalled());

    LOG.debug("writing to WAL with non-legacy keys.");
    final int countPerFamily = 5;
    for (HColumnDescriptor hcd : htd.getFamilies()) {
      addWALEdits(tableName, hri, TEST_ROW, hcd.getName(), countPerFamily,
          EnvironmentEdgeManager.getDelegate(), wal, htd, mvcc);
    }

    LOG.debug("Verify that only the non-legacy CP saw edits.");
    assertTrue(newApi.isPreWALWriteCalled());
    assertTrue(newApi.isPostWALWriteCalled());
    assertFalse(newApi.isPreWALWriteDeprecatedCalled());
    assertFalse(newApi.isPostWALWriteDeprecatedCalled());
    // wish we could test that the log message happened :/
    assertFalse(oldApi.isPreWALWriteCalled());
    assertFalse(oldApi.isPostWALWriteCalled());
    assertFalse(oldApi.isPreWALWriteDeprecatedCalled());
    assertFalse(oldApi.isPostWALWriteDeprecatedCalled());

    LOG.debug("reseting cp state.");
    newApi.setTestValues(TEST_TABLE, TEST_ROW, null, null, null, null, null, null);
    oldApi.setTestValues(TEST_TABLE, TEST_ROW, null, null, null, null, null, null);

    LOG.debug("write a log edit that supports legacy cps.");
    final long now = EnvironmentEdgeManager.currentTime();
    final WALKey legacyKey = new HLogKey(hri.getEncodedNameAsBytes(), hri.getTable(), now, mvcc);
    final WALEdit edit = new WALEdit();
    final byte[] nonce = Bytes.toBytes("1772");
    edit.add(new KeyValue(TEST_ROW, TEST_FAMILY[0], nonce, now, nonce));
    final long txid = wal.append(htd, hri, legacyKey, edit, true);
    wal.sync(txid);

    LOG.debug("Make sure legacy cps can see supported edits after having been skipped.");
    assertTrue("non-legacy WALObserver didn't see pre-write.", newApi.isPreWALWriteCalled());
    assertTrue("non-legacy WALObserver didn't see post-write.", newApi.isPostWALWriteCalled());
    assertFalse("non-legacy WALObserver shouldn't have seen legacy pre-write.",
        newApi.isPreWALWriteDeprecatedCalled());
    assertFalse("non-legacy WALObserver shouldn't have seen legacy post-write.",
        newApi.isPostWALWriteDeprecatedCalled());
    assertTrue("legacy WALObserver didn't see pre-write.", oldApi.isPreWALWriteCalled());
    assertTrue("legacy WALObserver didn't see post-write.", oldApi.isPostWALWriteCalled());
    assertTrue("legacy WALObserver didn't see legacy pre-write.",
        oldApi.isPreWALWriteDeprecatedCalled());
    assertTrue("legacy WALObserver didn't see legacy post-write.",
        oldApi.isPostWALWriteDeprecatedCalled());@@@   }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		22
-  
  public void testRowIndexWithTagsButNoTagsInCell() throws IOException {
    List<KeyValue> kvList = new ArrayList<>();
    byte[] row = new byte[0];
    byte[] family = new byte[0];
    byte[] qualifier = new byte[0];
    byte[] value = new byte[0];
    KeyValue expectedKV = new KeyValue(row, family, qualifier, 1L, Type.Put, value);
    kvList.add(expectedKV);
    DataBlockEncoding encoding = DataBlockEncoding.ROW_INDEX_V1;
    DataBlockEncoder encoder = encoding.getEncoder();
    ByteBuffer encodedBuffer =
        encodeKeyValues(encoding, kvList, getEncodingContext(Algorithm.NONE, encoding));
    HFileContext meta =
        new HFileContextBuilder().withHBaseCheckSum(false).withIncludesMvcc(includesMemstoreTS)
            .withIncludesTags(includesTags).withCompression(Compression.Algorithm.NONE).build();
    DataBlockEncoder.EncodedSeeker seeker =
        encoder.createSeeker(KeyValue.COMPARATOR, encoder.newDataBlockDecodingContext(meta));
    seeker.setCurrentBuffer(encodedBuffer);
    Cell cell = seeker.getKeyValue();
    Assert.assertEquals(expectedKV.getLength(), ((KeyValue) cell).getLength());
  }
```
```
Cyclomatic Complexity	 2
Assertions		 4
Lines of Code		27
-  
  public void compatibilityTest() throws Exception {
    HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
    Path testDir = TEST_UTIL.getDataTestDir();
    TEST_UTIL.getTestFileSystem().mkdirs(testDir);
    String persistencePath = testDir + "/bucket.persistence";
    BucketCache bucketCache =
      new BucketCache("file:" + testDir + "/bucket.cache", capacitySize, constructedBlockSize,
        null, writeThreads, writerQLen, persistencePath);
    long usedSize = bucketCache.getAllocator().getUsedSize();
    assertTrue(usedSize == 0);

    CacheTestUtils.HFileBlockPair[] blocks =
      CacheTestUtils.generateHFileBlocks(constructedBlockSize, 1);
    // Add blocks
    for (CacheTestUtils.HFileBlockPair block : blocks) {
      cacheAndWaitUntilFlushedToBucket(bucketCache, block.getBlockName(), block.getBlock());
    }
    usedSize = bucketCache.getAllocator().getUsedSize();
    assertTrue(usedSize != 0);
    // persistence backingMap using old way
    persistToFileInOldWay(persistencePath + ".old", bucketCache.getMaxSize(),
      bucketCache.backingMap, bucketCache.getDeserialiserMap());
    bucketCache.shutdown();

    // restore cache from file which skip check checksum
    bucketCache =
      new BucketCache("file:" + testDir + "/bucket.cache", capacitySize, constructedBlockSize,
        null, writeThreads, writerQLen, persistencePath + ".old");
    assertEquals(usedSize, bucketCache.getAllocator().getUsedSize());
    assertEquals(blocks.length, bucketCache.backingMap.size());
  }
```
```
Cyclomatic Complexity	 21
Assertions		 12
Lines of Code		165
-   (timeout=240000)
  public void testMasterFailoverWithMockedRIT() throws Exception {

    final int NUM_MASTERS = 1;
    final int NUM_RS = 3;

    // Create config to use for this cluster
    Configuration conf = HBaseConfiguration.create();
    conf.setBoolean("hbase.assignment.usezk", true);

    // Start the cluster
    HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility(conf);
    TEST_UTIL.startMiniCluster(NUM_MASTERS, NUM_RS);
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    log("Cluster started");

    // Create a ZKW to use in the test
    ZooKeeperWatcher zkw = HBaseTestingUtility.getZooKeeperWatcher(TEST_UTIL);

    // get all the master threads
    List<MasterThread> masterThreads = cluster.getMasterThreads();
    assertEquals(1, masterThreads.size());

    // only one master thread, let's wait for it to be initialized
    assertTrue(cluster.waitForActiveAndReadyMaster());
    HMaster master = masterThreads.get(0).getMaster();
    assertTrue(master.isActiveMaster());
    assertTrue(master.isInitialized());

    // disable load balancing on this master
    master.balanceSwitch(false);

    // create two tables in META, each with 10 regions
    byte [] FAMILY = Bytes.toBytes("family");
    byte [][] SPLIT_KEYS = new byte [][] {
        new byte[0], Bytes.toBytes("aaa"), Bytes.toBytes("bbb"),
        Bytes.toBytes("ccc"), Bytes.toBytes("ddd"), Bytes.toBytes("eee"),
        Bytes.toBytes("fff"), Bytes.toBytes("ggg"), Bytes.toBytes("hhh"),
        Bytes.toBytes("iii"), Bytes.toBytes("jjj")
    };

    byte [] enabledTable = Bytes.toBytes("enabledTable");
    HTableDescriptor htdEnabled = new HTableDescriptor(TableName.valueOf(enabledTable));
    htdEnabled.addFamily(new HColumnDescriptor(FAMILY));

    FileSystem filesystem = FileSystem.get(conf);
    Path rootdir = FSUtils.getRootDir(conf);
    FSTableDescriptors fstd = new FSTableDescriptors(conf, filesystem, rootdir);
    // Write the .tableinfo
    fstd.createTableDescriptor(htdEnabled);

    HRegionInfo hriEnabled = new HRegionInfo(htdEnabled.getTableName(), null, null);
    createRegion(hriEnabled, rootdir, conf, htdEnabled);

    List<HRegionInfo> enabledRegions = TEST_UTIL.createMultiRegionsInMeta(
        TEST_UTIL.getConfiguration(), htdEnabled, SPLIT_KEYS);

    TableName disabledTable = TableName.valueOf("disabledTable");
    HTableDescriptor htdDisabled = new HTableDescriptor(disabledTable);
    htdDisabled.addFamily(new HColumnDescriptor(FAMILY));
    // Write the .tableinfo
    fstd.createTableDescriptor(htdDisabled);
    HRegionInfo hriDisabled = new HRegionInfo(htdDisabled.getTableName(), null, null);
    createRegion(hriDisabled, rootdir, conf, htdDisabled);
    List<HRegionInfo> disabledRegions = TEST_UTIL.createMultiRegionsInMeta(
        TEST_UTIL.getConfiguration(), htdDisabled, SPLIT_KEYS);

    TableName tableWithMergingRegions = TableName.valueOf("tableWithMergingRegions");
    TEST_UTIL.createTable(tableWithMergingRegions, FAMILY, new byte [][] {Bytes.toBytes("m")});

    log("Regions in hbase:meta and namespace have been created");

    // at this point we only expect 4 regions to be assigned out
    // (catalogs and namespace, + 2 merging regions)
    assertEquals(4, cluster.countServedRegions());

    // Move merging regions to the same region server
    AssignmentManager am = master.getAssignmentManager();
    RegionStates regionStates = am.getRegionStates();
    List<HRegionInfo> mergingRegions = regionStates.getRegionsOfTable(tableWithMergingRegions);
    assertEquals(2, mergingRegions.size());
    HRegionInfo a = mergingRegions.get(0);
    HRegionInfo b = mergingRegions.get(1);
    HRegionInfo newRegion = RegionMergeTransactionImpl.getMergedRegionInfo(a, b);
    ServerName mergingServer = regionStates.getRegionServerOfRegion(a);
    ServerName serverB = regionStates.getRegionServerOfRegion(b);
    if (!serverB.equals(mergingServer)) {
      RegionPlan plan = new RegionPlan(b, serverB, mergingServer);
      am.balance(plan);
      assertTrue(am.waitForAssignment(b));
    }

    // Let's just assign everything to first RS
    HRegionServer hrs = cluster.getRegionServer(0);
    ServerName serverName = hrs.getServerName();
    HRegionInfo closingRegion = enabledRegions.remove(0);
    // we'll need some regions to already be assigned out properly on live RS
    List<HRegionInfo> enabledAndAssignedRegions = new ArrayList<HRegionInfo>();
    enabledAndAssignedRegions.add(enabledRegions.remove(0));
    enabledAndAssignedRegions.add(enabledRegions.remove(0));
    enabledAndAssignedRegions.add(closingRegion);

    List<HRegionInfo> disabledAndAssignedRegions = new ArrayList<HRegionInfo>();
    disabledAndAssignedRegions.add(disabledRegions.remove(0));
    disabledAndAssignedRegions.add(disabledRegions.remove(0));

    // now actually assign them
    for (HRegionInfo hri : enabledAndAssignedRegions) {
      master.assignmentManager.addPlan(hri.getEncodedName(),
          new RegionPlan(hri, null, serverName));
      master.assignmentManager.assign(hri, true);
    }

    for (HRegionInfo hri : disabledAndAssignedRegions) {
      master.assignmentManager.addPlan(hri.getEncodedName(),
          new RegionPlan(hri, null, serverName));
      master.assignmentManager.assign(hri, true);
    }

    // wait for no more RIT
    log("Waiting for assignment to finish");
    ZKAssign.blockUntilNoRIT(zkw);
    log("Assignment completed");

    // Stop the master
    log("Aborting master");
    cluster.abortMaster(0);
    cluster.waitOnMaster(0);
    log("Master has aborted");

    /*
     * Now, let's start mocking up some weird states as described in the method
     * javadoc.
     */

    List<HRegionInfo> regionsThatShouldBeOnline = new ArrayList<HRegionInfo>();
    List<HRegionInfo> regionsThatShouldBeOffline = new ArrayList<HRegionInfo>();

    log("Beginning to mock scenarios");

    // Disable the disabledTable in ZK
    TableStateManager tsm = master.getTableStateManager();
    tsm.setTableState(disabledTable, TableState.State.DISABLED);

    /*
     *  ZK = OFFLINE
     */

    // Region that should be assigned but is not and is in ZK as OFFLINE
    // Cause: This can happen if the master crashed after creating the znode but before sending the
    //  request to the region server
    HRegionInfo region = enabledRegions.remove(0);
    regionsThatShouldBeOnline.add(region);
    ZKAssign.createNodeOffline(zkw, region, serverName);

    /*
     * ZK = CLOSING
     */
    // Cause: Same as offline.
    regionsThatShouldBeOnline.add(closingRegion);
    ZKAssign.createNodeClosing(zkw, closingRegion, serverName);

    /*
     * ZK = CLOSED
     */

    // Region of enabled table closed but not ack
    //Cause: Master was down while the region server updated the ZK status.
    region = enabledRegions.remove(0);
    regionsThatShouldBeOnline.add(region);
    int version = ZKAssign.createNodeClosing(zkw, region, serverName);
    ZKAssign.transitionNodeClosed(zkw, region, serverName, version);

    // Region of disabled table closed but not ack
    region = disabledRegions.remove(0);
    regionsThatShouldBeOffline.add(region);
    version = ZKAssign.createNodeClosing(zkw, region, serverName);
    ZKAssign.transitionNodeClosed(zkw, region, serverName, version);

    /*
     * ZK = OPENED
     */

    // Region of enabled table was opened on RS
    // Cause: as offline
    region = enabledRegions.remove(0);
    regionsThatShouldBeOnline.add(region);
    ZKAssign.createNodeOffline(zkw, region, serverName);
    ProtobufUtil.openRegion(null, hrs.getRSRpcServices(), hrs.getServerName(), region);
    while (true) {
      byte [] bytes = ZKAssign.getData(zkw, region.getEncodedName());
      RegionTransition rt = RegionTransition.parseFrom(bytes);
      if (rt != null && rt.getEventType().equals(EventType.RS_ZK_REGION_OPENED)) {
        break;
      }
      Thread.sleep(100);
    }

    // Region of disable table was opened on RS
    // Cause: Master failed while updating the status for this region server.
    region = disabledRegions.remove(0);
    regionsThatShouldBeOffline.add(region);
    ZKAssign.createNodeOffline(zkw, region, serverName);
    ProtobufUtil.openRegion(null, hrs.getRSRpcServices(), hrs.getServerName(), region);
    while (true) {
      byte [] bytes = ZKAssign.getData(zkw, region.getEncodedName());
      RegionTransition rt = RegionTransition.parseFrom(bytes);
      if (rt != null && rt.getEventType().equals(EventType.RS_ZK_REGION_OPENED)) {
        break;
      }
      Thread.sleep(100);
    }

    /*
     * ZK = MERGING
     */

    // Regions of table of merging regions
    // Cause: Master was down while merging was going on
    hrs.getCoordinatedStateManager().
      getRegionMergeCoordination().startRegionMergeTransaction(newRegion, mergingServer, a, b);

    /*
     * ZK = NONE
     */

    /*
     * DONE MOCKING
     */

    log("Done mocking data up in ZK");

    // Start up a new master
    log("Starting up a new master");
    master = cluster.startMaster().getMaster();
    log("Waiting for master to be ready");
    cluster.waitForActiveAndReadyMaster();
    log("Master is ready");

    // Get new region states since master restarted
    regionStates = master.getAssignmentManager().getRegionStates();
    // Merging region should remain merging
    assertTrue(regionStates.isRegionInState(a, State.MERGING));
    assertTrue(regionStates.isRegionInState(b, State.MERGING));
    assertTrue(regionStates.isRegionInState(newRegion, State.MERGING_NEW));
    // Now remove the faked merging znode, merging regions should be
    // offlined automatically, otherwise it is a bug in AM.
    ZKAssign.deleteNodeFailSilent(zkw, newRegion);

    // Failover should be completed, now wait for no RIT
    log("Waiting for no more RIT");
    ZKAssign.blockUntilNoRIT(zkw);
    log("No more RIT in ZK, now doing final test verification");

    // Grab all the regions that are online across RSs
    Set<HRegionInfo> onlineRegions = new TreeSet<HRegionInfo>();
    for (JVMClusterUtil.RegionServerThread rst :
      cluster.getRegionServerThreads()) {
      onlineRegions.addAll(ProtobufUtil.getOnlineRegions(
        rst.getRegionServer().getRSRpcServices()));
    }

    // Now, everything that should be online should be online
    for (HRegionInfo hri : regionsThatShouldBeOnline) {
      assertTrue(onlineRegions.contains(hri));
    }

    // Everything that should be offline should not be online
    for (HRegionInfo hri : regionsThatShouldBeOffline) {
      if (onlineRegions.contains(hri)) {
       LOG.debug(hri);
      }
      assertFalse(onlineRegions.contains(hri));
    }

    log("Done with verification, all passed, shutting down cluster");

    // Done, shutdown the cluster
    TEST_UTIL.shutdownMiniCluster();
  }
```
```
Cyclomatic Complexity	 25
Assertions		 21
Lines of Code		283
-   (timeout=180000)
  public void testMasterFailoverWithMockedRITOnDeadRS() throws Exception {

    final int NUM_MASTERS = 1;
    final int NUM_RS = 2;

    // Create and start the cluster
    HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setBoolean("hbase.assignment.usezk", true);

    conf.setInt(ServerManager.WAIT_ON_REGIONSERVERS_MINTOSTART, 1);
    conf.setInt(ServerManager.WAIT_ON_REGIONSERVERS_MAXTOSTART, 2);
    TEST_UTIL.startMiniCluster(NUM_MASTERS, NUM_RS);
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    log("Cluster started");

    // Create a ZKW to use in the test
    ZooKeeperWatcher zkw = new ZooKeeperWatcher(TEST_UTIL.getConfiguration(),
        "unittest", new Abortable() {

          @Override
          public void abort(String why, Throwable e) {
            LOG.error("Fatal ZK Error: " + why, e);
            org.junit.Assert.assertFalse("Fatal ZK error", true);
          }

          @Override
          public boolean isAborted() {
            return false;
          }

    });

    // get all the master threads
    List<MasterThread> masterThreads = cluster.getMasterThreads();
    assertEquals(1, masterThreads.size());

    // only one master thread, let's wait for it to be initialized
    assertTrue(cluster.waitForActiveAndReadyMaster());
    HMaster master = masterThreads.get(0).getMaster();
    assertTrue(master.isActiveMaster());
    assertTrue(master.isInitialized());

    // disable load balancing on this master
    master.balanceSwitch(false);

    // create two tables in META, each with 30 regions
    byte [] FAMILY = Bytes.toBytes("family");
    byte[][] SPLIT_KEYS =
        TEST_UTIL.getRegionSplitStartKeys(Bytes.toBytes("aaa"), Bytes.toBytes("zzz"), 30);

    byte [] enabledTable = Bytes.toBytes("enabledTable");
    HTableDescriptor htdEnabled = new HTableDescriptor(TableName.valueOf(enabledTable));
    htdEnabled.addFamily(new HColumnDescriptor(FAMILY));
    FileSystem filesystem = FileSystem.get(conf);
    Path rootdir = FSUtils.getRootDir(conf);
    FSTableDescriptors fstd = new FSTableDescriptors(conf, filesystem, rootdir);
    // Write the .tableinfo
    fstd.createTableDescriptor(htdEnabled);
    HRegionInfo hriEnabled = new HRegionInfo(htdEnabled.getTableName(),
        null, null);
    createRegion(hriEnabled, rootdir, conf, htdEnabled);

    List<HRegionInfo> enabledRegions = TEST_UTIL.createMultiRegionsInMeta(
        TEST_UTIL.getConfiguration(), htdEnabled, SPLIT_KEYS);

    TableName disabledTable =
        TableName.valueOf("disabledTable");
    HTableDescriptor htdDisabled = new HTableDescriptor(disabledTable);
    htdDisabled.addFamily(new HColumnDescriptor(FAMILY));
    // Write the .tableinfo
    fstd.createTableDescriptor(htdDisabled);
    HRegionInfo hriDisabled = new HRegionInfo(htdDisabled.getTableName(), null, null);
    createRegion(hriDisabled, rootdir, conf, htdDisabled);

    List<HRegionInfo> disabledRegions = TEST_UTIL.createMultiRegionsInMeta(
        TEST_UTIL.getConfiguration(), htdDisabled, SPLIT_KEYS);

    log("Regions in hbase:meta and Namespace have been created");

    // at this point we only expect 2 regions to be assigned out (catalogs and namespace  )
    assertEquals(2, cluster.countServedRegions());

    // The first RS will stay online
    List<RegionServerThread> regionservers =
      cluster.getRegionServerThreads();
    HRegionServer hrs = regionservers.get(0).getRegionServer();

    // The second RS is going to be hard-killed
    RegionServerThread hrsDeadThread = regionservers.get(1);
    HRegionServer hrsDead = hrsDeadThread.getRegionServer();
    ServerName deadServerName = hrsDead.getServerName();

    // we'll need some regions to already be assigned out properly on live RS
    List<HRegionInfo> enabledAndAssignedRegions = new ArrayList<HRegionInfo>();
    enabledAndAssignedRegions.addAll(enabledRegions.subList(0, 6));
    enabledRegions.removeAll(enabledAndAssignedRegions);
    List<HRegionInfo> disabledAndAssignedRegions = new ArrayList<HRegionInfo>();
    disabledAndAssignedRegions.addAll(disabledRegions.subList(0, 6));
    disabledRegions.removeAll(disabledAndAssignedRegions);

    // now actually assign them
    for (HRegionInfo hri : enabledAndAssignedRegions) {
      master.assignmentManager.addPlan(hri.getEncodedName(),
          new RegionPlan(hri, null, hrs.getServerName()));
      master.assignmentManager.assign(hri, true);
    }
    for (HRegionInfo hri : disabledAndAssignedRegions) {
      master.assignmentManager.addPlan(hri.getEncodedName(),
          new RegionPlan(hri, null, hrs.getServerName()));
      master.assignmentManager.assign(hri, true);
    }

    log("Waiting for assignment to finish");
    ZKAssign.blockUntilNoRIT(zkw);
    master.assignmentManager.waitUntilNoRegionsInTransition(60000);
    log("Assignment completed");

    assertTrue(" Table must be enabled.", master.getAssignmentManager()
        .getTableStateManager().isTableState(TableName.valueOf("enabledTable"),
        TableState.State.ENABLED));
    // we also need regions assigned out on the dead server
    List<HRegionInfo> enabledAndOnDeadRegions = new ArrayList<HRegionInfo>();
    enabledAndOnDeadRegions.addAll(enabledRegions.subList(0, 6));
    enabledRegions.removeAll(enabledAndOnDeadRegions);
    List<HRegionInfo> disabledAndOnDeadRegions = new ArrayList<HRegionInfo>();
    disabledAndOnDeadRegions.addAll(disabledRegions.subList(0, 6));
    disabledRegions.removeAll(disabledAndOnDeadRegions);

    // set region plan to server to be killed and trigger assign
    for (HRegionInfo hri : enabledAndOnDeadRegions) {
      master.assignmentManager.addPlan(hri.getEncodedName(),
          new RegionPlan(hri, null, deadServerName));
      master.assignmentManager.assign(hri, true);
    }
    for (HRegionInfo hri : disabledAndOnDeadRegions) {
      master.assignmentManager.addPlan(hri.getEncodedName(),
          new RegionPlan(hri, null, deadServerName));
      master.assignmentManager.assign(hri, true);
    }

    // wait for no more RIT
    log("Waiting for assignment to finish");
    ZKAssign.blockUntilNoRIT(zkw);
    master.assignmentManager.waitUntilNoRegionsInTransition(60000);
    log("Assignment completed");

    // Due to master.assignment.assign(hri) could fail to assign a region to a specified RS
    // therefore, we need make sure that regions are in the expected RS
    verifyRegionLocation(hrs, enabledAndAssignedRegions);
    verifyRegionLocation(hrs, disabledAndAssignedRegions);
    verifyRegionLocation(hrsDead, enabledAndOnDeadRegions);
    verifyRegionLocation(hrsDead, disabledAndOnDeadRegions);

    assertTrue(" Didn't get enough regions of enabledTalbe on live rs.",
      enabledAndAssignedRegions.size() >= 2);
    assertTrue(" Didn't get enough regions of disalbedTable on live rs.",
      disabledAndAssignedRegions.size() >= 2);
    assertTrue(" Didn't get enough regions of enabledTalbe on dead rs.",
      enabledAndOnDeadRegions.size() >= 2);
    assertTrue(" Didn't get enough regions of disalbedTable on dead rs.",
      disabledAndOnDeadRegions.size() >= 2);

    // Stop the master
    log("Aborting master");
    cluster.abortMaster(0);
    cluster.waitOnMaster(0);
    log("Master has aborted");

    /*
     * Now, let's start mocking up some weird states as described in the method
     * javadoc.
     */

    List<HRegionInfo> regionsThatShouldBeOnline = new ArrayList<HRegionInfo>();
    List<HRegionInfo> regionsThatShouldBeOffline = new ArrayList<HRegionInfo>();

    log("Beginning to mock scenarios");

    // Disable the disabledTable in ZK
    TableStateManager tsm = master.getTableStateManager();
    tsm.setTableState(disabledTable, TableState.State.DISABLED);

    assertTrue(" The enabled table should be identified on master fail over.",
        tsm.isTableState(TableName.valueOf("enabledTable"), TableState.State.ENABLED));
    /*
     * ZK = CLOSING
     */

    // Region of enabled table being closed on dead RS but not finished
    HRegionInfo region = enabledAndOnDeadRegions.remove(0);
    regionsThatShouldBeOnline.add(region);
    ZKAssign.createNodeClosing(zkw, region, deadServerName);
    LOG.debug("\n\nRegion of enabled table was CLOSING on dead RS\n" +
        region + "\n\n");

    // Region of disabled table being closed on dead RS but not finished
    region = disabledAndOnDeadRegions.remove(0);
    regionsThatShouldBeOffline.add(region);
    ZKAssign.createNodeClosing(zkw, region, deadServerName);
    LOG.debug("\n\nRegion of disabled table was CLOSING on dead RS\n" +
        region + "\n\n");

    /*
     * ZK = CLOSED
     */

    // Region of enabled on dead server gets closed but not ack'd by master
    region = enabledAndOnDeadRegions.remove(0);
    regionsThatShouldBeOnline.add(region);
    int version = ZKAssign.createNodeClosing(zkw, region, deadServerName);
    ZKAssign.transitionNodeClosed(zkw, region, deadServerName, version);
    LOG.debug("\n\nRegion of enabled table was CLOSED on dead RS\n" +
        region + "\n\n");

    // Region of disabled on dead server gets closed but not ack'd by master
    region = disabledAndOnDeadRegions.remove(0);
    regionsThatShouldBeOffline.add(region);
    version = ZKAssign.createNodeClosing(zkw, region, deadServerName);
    ZKAssign.transitionNodeClosed(zkw, region, deadServerName, version);
    LOG.debug("\n\nRegion of disabled table was CLOSED on dead RS\n" +
        region + "\n\n");

    /*
     * ZK = OPENING
     */

    // RS was opening a region of enabled table then died
    region = enabledRegions.remove(0);
    regionsThatShouldBeOnline.add(region);
    ZKAssign.createNodeOffline(zkw, region, deadServerName);
    ZKAssign.transitionNodeOpening(zkw, region, deadServerName);
    LOG.debug("\n\nRegion of enabled table was OPENING on dead RS\n" +
        region + "\n\n");

    // RS was opening a region of disabled table then died
    region = disabledRegions.remove(0);
    regionsThatShouldBeOffline.add(region);
    ZKAssign.createNodeOffline(zkw, region, deadServerName);
    ZKAssign.transitionNodeOpening(zkw, region, deadServerName);
    LOG.debug("\n\nRegion of disabled table was OPENING on dead RS\n" +
        region + "\n\n");

    /*
     * ZK = OPENED
     */

    // Region of enabled table was opened on dead RS
    region = enabledRegions.remove(0);
    regionsThatShouldBeOnline.add(region);
    ZKAssign.createNodeOffline(zkw, region, deadServerName);
    ProtobufUtil.openRegion(null, hrsDead.getRSRpcServices(),
      hrsDead.getServerName(), region);
    while (true) {
      byte [] bytes = ZKAssign.getData(zkw, region.getEncodedName());
      RegionTransition rt = RegionTransition.parseFrom(bytes);
      if (rt != null && rt.getEventType().equals(EventType.RS_ZK_REGION_OPENED)) {
        break;
      }
      Thread.sleep(100);
    }
    LOG.debug("\n\nRegion of enabled table was OPENED on dead RS\n" + region + "\n\n");

    // Region of disabled table was opened on dead RS
    region = disabledRegions.remove(0);
    regionsThatShouldBeOffline.add(region);
    ZKAssign.createNodeOffline(zkw, region, deadServerName);
    ProtobufUtil.openRegion(null, hrsDead.getRSRpcServices(),
      hrsDead.getServerName(), region);
    while (true) {
      byte [] bytes = ZKAssign.getData(zkw, region.getEncodedName());
      RegionTransition rt = RegionTransition.parseFrom(bytes);
      if (rt != null && rt.getEventType().equals(EventType.RS_ZK_REGION_OPENED)) {
        break;
      }
      Thread.sleep(100);
    }
    LOG.debug("\n\nRegion of disabled table was OPENED on dead RS\n" + region + "\n\n");

    /*
     * ZK = NONE
     */

    // Region of enabled table was open at steady-state on dead RS
    region = enabledRegions.remove(0);
    regionsThatShouldBeOnline.add(region);
    ZKAssign.createNodeOffline(zkw, region, deadServerName);
    ProtobufUtil.openRegion(null, hrsDead.getRSRpcServices(),
      hrsDead.getServerName(), region);
    while (true) {
      byte [] bytes = ZKAssign.getData(zkw, region.getEncodedName());
      RegionTransition rt = RegionTransition.parseFrom(bytes);
      if (rt != null && rt.getEventType().equals(EventType.RS_ZK_REGION_OPENED)) {
        ZKAssign.deleteOpenedNode(zkw, region.getEncodedName(), rt.getServerName());
        LOG.debug("DELETED " + rt);
        break;
      }
      Thread.sleep(100);
    }
    LOG.debug("\n\nRegion of enabled table was open at steady-state on dead RS"
        + "\n" + region + "\n\n");

    // Region of disabled table was open at steady-state on dead RS
    region = disabledRegions.remove(0);
    regionsThatShouldBeOffline.add(region);
    ZKAssign.createNodeOffline(zkw, region, deadServerName);
    ProtobufUtil.openRegion(null, hrsDead.getRSRpcServices(),
      hrsDead.getServerName(), region);
    while (true) {
      byte [] bytes = ZKAssign.getData(zkw, region.getEncodedName());
      RegionTransition rt = RegionTransition.parseFrom(bytes);
      if (rt != null && rt.getEventType().equals(EventType.RS_ZK_REGION_OPENED)) {
        ZKAssign.deleteOpenedNode(zkw, region.getEncodedName(), rt.getServerName());
        break;
      }
      Thread.sleep(100);
    }
    LOG.debug("\n\nRegion of disabled table was open at steady-state on dead RS"
      + "\n" + region + "\n\n");

    /*
     * DONE MOCKING
     */

    log("Done mocking data up in ZK");

    // Kill the RS that had a hard death
    log("Killing RS " + deadServerName);
    hrsDead.abort("Killing for unit test");
    log("RS " + deadServerName + " killed");

    // Start up a new master.  Wait until regionserver is completely down
    // before starting new master because of hbase-4511.
    while (hrsDeadThread.isAlive()) {
      Threads.sleep(10);
    }
    log("Starting up a new master");
    master = cluster.startMaster().getMaster();
    log("Waiting for master to be ready");
    assertTrue(cluster.waitForActiveAndReadyMaster());
    log("Master is ready");

    // Wait until SSH processing completed for dead server.
    while (master.getServerManager().areDeadServersInProgress()) {
      Thread.sleep(10);
    }

    // Failover should be completed, now wait for no RIT
    log("Waiting for no more RIT");
    ZKAssign.blockUntilNoRIT(zkw);
    log("No more RIT in ZK");
    long now = System.currentTimeMillis();
    long maxTime = 120000;
    boolean done = master.assignmentManager.waitUntilNoRegionsInTransition(maxTime);
    if (!done) {
      RegionStates regionStates = master.getAssignmentManager().getRegionStates();
      LOG.info("rit=" + regionStates.getRegionsInTransition());
    }
    long elapsed = System.currentTimeMillis() - now;
    assertTrue("Elapsed=" + elapsed + ", maxTime=" + maxTime + ", done=" + done,
      elapsed < maxTime);
    log("No more RIT in RIT map, doing final test verification");

    // Grab all the regions that are online across RSs
    Set<HRegionInfo> onlineRegions = new TreeSet<HRegionInfo>();
    now = System.currentTimeMillis();
    maxTime = 30000;
    for (JVMClusterUtil.RegionServerThread rst :
        cluster.getRegionServerThreads()) {
      try {
        HRegionServer rs = rst.getRegionServer();
        while (!rs.getRegionsInTransitionInRS().isEmpty()) {
          elapsed = System.currentTimeMillis() - now;
          assertTrue("Test timed out in getting online regions", elapsed < maxTime);
          if (rs.isAborted() || rs.isStopped()) {
            // This region server is stopped, skip it.
            break;
          }
          Thread.sleep(100);
        }
        onlineRegions.addAll(ProtobufUtil.getOnlineRegions(rs.getRSRpcServices()));
      } catch (RegionServerStoppedException e) {
        LOG.info("Got RegionServerStoppedException", e);
      }
    }

    // Now, everything that should be online should be online
    for (HRegionInfo hri : regionsThatShouldBeOnline) {
      assertTrue("region=" + hri.getRegionNameAsString() + ", " + onlineRegions.toString(),
        onlineRegions.contains(hri));
    }

    // Everything that should be offline should not be online
    for (HRegionInfo hri : regionsThatShouldBeOffline) {
      assertFalse(onlineRegions.contains(hri));
    }

    log("Done with verification, all passed, shutting down cluster");

    // Done, shutdown the cluster
    TEST_UTIL.shutdownMiniCluster();
  }
```
```
Cyclomatic Complexity	 5
Assertions		 2
Lines of Code		42
-   (timeout=180000)
  public void testShouldCheckMasterFailOverWhenMETAIsInOpenedState()
      throws Exception {
    LOG.info("Starting testShouldCheckMasterFailOverWhenMETAIsInOpenedState");
    final int NUM_MASTERS = 1;
    final int NUM_RS = 2;

    // Start the cluster
    HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setInt("hbase.master.info.port", -1);
    conf.setBoolean("hbase.assignment.usezk", true);

    TEST_UTIL.startMiniCluster(NUM_MASTERS, NUM_RS);
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();

    // Find regionserver carrying meta.
    List<RegionServerThread> regionServerThreads =
      cluster.getRegionServerThreads();
    Region metaRegion = null;
    HRegionServer metaRegionServer = null;
    for (RegionServerThread regionServerThread : regionServerThreads) {
      HRegionServer regionServer = regionServerThread.getRegionServer();
      metaRegion = regionServer.getOnlineRegion(HRegionInfo.FIRST_META_REGIONINFO.getRegionName());
      regionServer.abort("");
      if (null != metaRegion) {
        metaRegionServer = regionServer;
        break;
      }
    }

    assertNotNull(metaRegion);
    assertNotNull(metaRegionServer);

    TEST_UTIL.shutdownMiniHBaseCluster();

    // Create a ZKW to use in the test
    ZooKeeperWatcher zkw =
      HBaseTestingUtility.createAndForceNodeToOpenedState(TEST_UTIL,
          metaRegion, metaRegionServer.getServerName());

    LOG.info("Staring cluster for second time");
    TEST_UTIL.startMiniHBaseCluster(NUM_MASTERS, NUM_RS);

    HMaster master = TEST_UTIL.getHBaseCluster().getMaster();
    while (!master.isInitialized()) {
      Thread.sleep(100);
    }
    // Failover should be completed, now wait for no RIT
    log("Waiting for no more RIT");
    ZKAssign.blockUntilNoRIT(zkw);

    zkw.close();
    // Stop the cluster
    TEST_UTIL.shutdownMiniCluster();
  }
```
```
Cyclomatic Complexity	 7
Assertions		 6
Lines of Code		52
-  (timeout=240000)
  public void testOfflineRegionReAssginedAfterMasterRestart() throws Exception {
    final TableName table = TableName.valueOf("testOfflineRegionReAssginedAfterMasterRestart");
    final int NUM_MASTERS = 1;
    final int NUM_RS = 2;

    // Create config to use for this cluster
    Configuration conf = HBaseConfiguration.create();
    conf.setBoolean("hbase.assignment.usezk", true);

    // Start the cluster
    final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility(conf);
    TEST_UTIL.startMiniCluster(NUM_MASTERS, NUM_RS);
    log("Cluster started");

    TEST_UTIL.createTable(table, Bytes.toBytes("family"));
    HMaster master = TEST_UTIL.getHBaseCluster().getMaster();
    RegionStates regionStates = master.getAssignmentManager().getRegionStates();
    HRegionInfo hri = regionStates.getRegionsOfTable(table).get(0);
    ServerName serverName = regionStates.getRegionServerOfRegion(hri);
    TEST_UTIL.assertRegionOnServer(hri, serverName, 200);

    ServerName dstName = null;
    for (ServerName tmpServer : master.serverManager.getOnlineServers().keySet()) {
      if (!tmpServer.equals(serverName)) {
        dstName = tmpServer;
        break;
      }
    }
    // find a different server
    assertTrue(dstName != null);
    // shutdown HBase cluster
    TEST_UTIL.shutdownMiniHBaseCluster();
    // create a RIT node in offline state
    ZooKeeperWatcher zkw = TEST_UTIL.getZooKeeperWatcher();
    ZKAssign.createNodeOffline(zkw, hri, dstName);
    Stat stat = new Stat();
    byte[] data =
        ZKAssign.getDataNoWatch(zkw, hri.getEncodedName(), stat);
    assertTrue(data != null);
    RegionTransition rt = RegionTransition.parseFrom(data);
    assertTrue(rt.getEventType() == EventType.M_ZK_REGION_OFFLINE);

    LOG.info(hri.getEncodedName() + " region is in offline state with source server=" + serverName
        + " and dst server=" + dstName);

    // start HBase cluster
    TEST_UTIL.startMiniHBaseCluster(NUM_MASTERS, NUM_RS);

    while (true) {
      master = TEST_UTIL.getHBaseCluster().getMaster();
      if (master != null && master.isInitialized()) {
        ServerManager serverManager = master.getServerManager();
        if (!serverManager.areDeadServersInProgress()) {
          break;
        }
      }
      Thread.sleep(200);
    }

    // verify the region is assigned
    master = TEST_UTIL.getHBaseCluster().getMaster();
    master.getAssignmentManager().waitForAssignment(hri);
    regionStates = master.getAssignmentManager().getRegionStates();
    RegionState newState = regionStates.getRegionState(hri);
    assertTrue(newState.isOpened());
  }
```
```
Cyclomatic Complexity	 2
Assertions		 2
Lines of Code		13
-  
  public void testReplicaCost() {
    Configuration conf = HBaseConfiguration.create();
    StochasticLoadBalancer.CostFunction
        costFunction = new StochasticLoadBalancer.RegionReplicaHostCostFunction(conf);
    for (int[] mockCluster : clusterStateMocks) {
      BaseLoadBalancer.Cluster cluster = mockCluster(mockCluster);
      costFunction.init(cluster);
      double cost = costFunction.cost();
      assertTrue(cost >= 0);
      assertTrue(cost <= 1.01);
    }
  }
```
```
Cyclomatic Complexity	 2
Assertions		 4
Lines of Code		54
-  
  public void testReplicaCostForReplicas() {
    Configuration conf = HBaseConfiguration.create();
    StochasticLoadBalancer.CostFunction
        costFunction = new StochasticLoadBalancer.RegionReplicaHostCostFunction(conf);

    int [] servers = new int[] {3,3,3,3,3};
    TreeMap<ServerName, List<HRegionInfo>> clusterState = mockClusterServers(servers);

    BaseLoadBalancer.Cluster cluster;

    cluster = new BaseLoadBalancer.Cluster(clusterState, null, null, null);
    costFunction.init(cluster);
    double costWithoutReplicas = costFunction.cost();
    assertEquals(0, costWithoutReplicas, 0);

    // replicate the region from first server to the last server
    HRegionInfo replica1 = RegionReplicaUtil.getRegionInfoForReplica(
      clusterState.firstEntry().getValue().get(0),1);
    clusterState.lastEntry().getValue().add(replica1);

    cluster = new BaseLoadBalancer.Cluster(clusterState, null, null, null);
    costFunction.init(cluster);
    double costWith1ReplicaDifferentServer = costFunction.cost();

    assertEquals(0, costWith1ReplicaDifferentServer, 0);

    // add a third replica to the last server
    HRegionInfo replica2 = RegionReplicaUtil.getRegionInfoForReplica(replica1, 2);
    clusterState.lastEntry().getValue().add(replica2);

    cluster = new BaseLoadBalancer.Cluster(clusterState, null, null, null);
    costFunction.init(cluster);
    double costWith1ReplicaSameServer = costFunction.cost();

    assertTrue(costWith1ReplicaDifferentServer < costWith1ReplicaSameServer);

    // test with replication = 4 for following:

    HRegionInfo replica3;
    Iterator<Entry<ServerName, List<HRegionInfo>>> it;
    Entry<ServerName, List<HRegionInfo>> entry;

    clusterState = mockClusterServers(servers);
    it = clusterState.entrySet().iterator();
    entry = it.next(); //first server
    HRegionInfo hri = entry.getValue().get(0);
    replica1 = RegionReplicaUtil.getRegionInfoForReplica(hri, 1);
    replica2 = RegionReplicaUtil.getRegionInfoForReplica(hri, 2);
    replica3 = RegionReplicaUtil.getRegionInfoForReplica(hri, 3);
    entry.getValue().add(replica1);
    entry.getValue().add(replica2);
    it.next().getValue().add(replica3); //2nd server

    cluster = new BaseLoadBalancer.Cluster(clusterState, null, null, null);
    costFunction.init(cluster);
    double costWith3ReplicasSameServer = costFunction.cost();

    clusterState = mockClusterServers(servers);
    hri = clusterState.firstEntry().getValue().get(0);
    replica1 = RegionReplicaUtil.getRegionInfoForReplica(hri, 1);
    replica2 = RegionReplicaUtil.getRegionInfoForReplica(hri, 2);
    replica3 = RegionReplicaUtil.getRegionInfoForReplica(hri, 3);

    clusterState.firstEntry().getValue().add(replica1);
    clusterState.lastEntry().getValue().add(replica2);
    clusterState.lastEntry().getValue().add(replica3);

    cluster = new BaseLoadBalancer.Cluster(clusterState, null, null, null);
    costFunction.init(cluster);
    double costWith2ReplicasOnTwoServers = costFunction.cost();

    assertTrue(costWith2ReplicasOnTwoServers < costWith3ReplicasSameServer);
  }
```
```
Cyclomatic Complexity	 5
Assertions		 2
Lines of Code		21
-  
  public void testNeedsBalanceForColocatedReplicas() {
    // check for the case where there are two hosts and with one rack, and where
    // both the replicas are hosted on the same server
    List<HRegionInfo> regions = randomRegions(1);
    ServerName s1 = ServerName.valueOf("host1", 1000, 11111);
    ServerName s2 = ServerName.valueOf("host11", 1000, 11111);
    Map<ServerName, List<HRegionInfo>> map = new HashMap<ServerName, List<HRegionInfo>>();
    map.put(s1, regions);
    regions.add(RegionReplicaUtil.getRegionInfoForReplica(regions.get(0), 1));
    // until the step above s1 holds two replicas of a region
    regions = randomRegions(1);
    map.put(s2, regions);
    assertTrue(loadBalancer.needsBalance(new Cluster(map, null, null, null)));
    // check for the case where there are two hosts on the same rack and there are two racks
    // and both the replicas are on the same rack
    map.clear();
    regions = randomRegions(1);
    List<HRegionInfo> regionsOnS2 = new ArrayList<HRegionInfo>(1);
    regionsOnS2.add(RegionReplicaUtil.getRegionInfoForReplica(regions.get(0), 1));
    map.put(s1, regions);
    map.put(s2, regionsOnS2);
    // add another server so that the cluster has some host on another rack
    map.put(ServerName.valueOf("host2", 1000, 11111), randomRegions(1));
    assertTrue(loadBalancer.needsBalance(new Cluster(map, null, null,
        new ForTestRackManagerOne())));
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		9
-   (timeout = 60000)
  public void testSmallCluster() {
    int numNodes = 10;
    int numRegions = 1000;
    int numRegionsPerServer = 40; //all servers except one
    int replication = 1;
    int numTables = 10;
    testWithCluster(numNodes, numRegions, numRegionsPerServer, replication, numTables, true, true);
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		9
-   (timeout = 60000)
  public void testSmallCluster2() {
    int numNodes = 20;
    int numRegions = 2000;
    int numRegionsPerServer = 40; //all servers except one
    int replication = 1;
    int numTables = 10;
    testWithCluster(numNodes, numRegions, numRegionsPerServer, replication, numTables, true, true);
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		9
-   (timeout = 60000)
  public void testSmallCluster3() {
    int numNodes = 20;
    int numRegions = 2000;
    int numRegionsPerServer = 1; // all servers except one
    int replication = 1;
    int numTables = 10;
    /* fails because of max moves */
    testWithCluster(numNodes, numRegions, numRegionsPerServer, replication, numTables, false, false);
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		9
-   (timeout = 800000)
  public void testMidCluster() {
    int numNodes = 100;
    int numRegions = 10000;
    int numRegionsPerServer = 60; // all servers except one
    int replication = 1;
    int numTables = 40;
    testWithCluster(numNodes, numRegions, numRegionsPerServer, replication, numTables, true, true);
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		15
-   (timeout = 800000)
  public void testMidCluster2() {
    int numNodes = 200;
    int numRegions = 100000;
    int numRegionsPerServer = 40; // all servers except one
    int replication = 1;
    int numTables = 400;
    testWithCluster(numNodes,
        numRegions,
        numRegionsPerServer,
        replication,
        numTables,
        false, /* num large num regions means may not always get to best balance with one run */
        false);
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		9
-   (timeout = 800000)
  public void testMidCluster3() {
    int numNodes = 100;
    int numRegions = 2000;
    int numRegionsPerServer = 9; // all servers except one
    int replication = 1;
    int numTables = 110;
    testWithCluster(numNodes, numRegions, numRegionsPerServer, replication, numTables, true, true);
    // TODO(eclark): Make sure that the tables are well distributed.
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		9
-  
  public void testLargeCluster() {
    int numNodes = 1000;
    int numRegions = 100000; //100 regions per RS
    int numRegionsPerServer = 80; //all servers except one
    int numTables = 100;
    int replication = 1;
    testWithCluster(numNodes, numRegions, numRegionsPerServer, replication, numTables, true, true);
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		9
-   (timeout = 800000)
  public void testRegionReplicasOnSmallCluster() {
    int numNodes = 10;
    int numRegions = 1000;
    int replication = 3; // 3 replicas per region
    int numRegionsPerServer = 80; //all regions are mostly balanced
    int numTables = 10;
    testWithCluster(numNodes, numRegions, numRegionsPerServer, replication, numTables, true, true);
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		17
-   (timeout = 800000)
  public void testRegionReplicationOnMidClusterWithRacks() {
    conf.setLong(StochasticLoadBalancer.MAX_STEPS_KEY, 10000000L);
    conf.setFloat("hbase.master.balancer.stochastic.maxMovePercent", 1.0f);
    conf.setLong("hbase.master.balancer.stochastic.maxRunningTime", 120 * 1000); // 120 sec
    loadBalancer.setConf(conf);
    int numNodes = 30;
    int numRegions = numNodes * 30;
    int replication = 3; // 3 replicas per region
    int numRegionsPerServer = 28;
    int numTables = 10;
    int numRacks = 4; // all replicas should be on a different rack
    Map<ServerName, List<HRegionInfo>> serverMap =
        createServerMap(numNodes, numRegions, numRegionsPerServer, replication, numTables);
    RackManager rm = new ForTestRackManager(numRacks);

    testWithCluster(serverMap, rm, false, true);@@@   }
```
```
Cyclomatic Complexity	 2
Assertions		 1
Lines of Code		15
-  (timeout = 15000)
  public void testForDifferntHFileRefsZnodeVersion() throws Exception {
    // 1. Create a file
    Path file = new Path(root, "testForDifferntHFileRefsZnodeVersion");
    fs.createNewFile(file);
    // 2. Assert file is successfully created
    assertTrue("Test file not created!", fs.exists(file));
    ReplicationHFileCleaner cleaner = new ReplicationHFileCleaner();
    cleaner.setConf(conf);

    ReplicationQueuesClient replicationQueuesClient = Mockito.mock(ReplicationQueuesClient.class);
    //Return different znode version for each call
    Mockito.when(replicationQueuesClient.getHFileRefsNodeChangeVersion()).thenReturn(1, 2);

    Class<? extends ReplicationHFileCleaner> cleanerClass = cleaner.getClass();
    Field rqc = cleanerClass.getDeclaredField("rqc");
    rqc.setAccessible(true);
    rqc.set(cleaner, replicationQueuesClient);

    cleaner.isFileDeletable(fs.getFileStatus(file));@@@   }
```
```
Cyclomatic Complexity	 4
Assertions		 10
Lines of Code		49
-  
  public void testMasterOpsWhileSplitting() throws Exception {
    TableName tableName = TableName.valueOf("TestSplit");
    byte[] familyName = Bytes.toBytes("fam");
    try (HTable ht = TEST_UTIL.createTable(tableName, familyName)) {
      TEST_UTIL.loadTable(ht, familyName, false);
    }
    HRegionServer server = TEST_UTIL.getHBaseCluster().getRegionServer(0);
    byte[] firstRow = Bytes.toBytes("aaa");
    byte[] splitRow = Bytes.toBytes("lll");
    byte[] lastRow = Bytes.toBytes("zzz");
    try (Connection conn = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration())) {
      // this will also cache the region
      byte[] regionName = conn.getRegionLocator(tableName).getRegionLocation(splitRow)
          .getRegionInfo().getRegionName();
      Region region = server.getRegion(regionName);
      SplitTransactionImpl split = new SplitTransactionImpl((HRegion) region, splitRow);
      split.prepare();

      // 1. phase I
      PairOfSameType<Region> regions = split.createDaughters(server, server, null);
      assertFalse(test(conn, tableName, firstRow, server));
      assertFalse(test(conn, tableName, lastRow, server));

      // passing null as services prevents final step
      // 2, most of phase II
      split.openDaughters(server, null, regions.getFirst(), regions.getSecond());
      assertFalse(test(conn, tableName, firstRow, server));
      assertFalse(test(conn, tableName, lastRow, server));

      // 3. finish phase II
      // note that this replicates some code from SplitTransaction
      // 2nd daughter first
      if (split.useZKForAssignment) {
        server.postOpenDeployTasks(regions.getSecond());
      } else {
        server.reportRegionStateTransition(
          RegionServerStatusProtos.RegionStateTransition.TransitionCode.SPLIT,
          region.getRegionInfo(), regions.getFirst().getRegionInfo(),
          regions.getSecond().getRegionInfo());
      }

      // first daughter second
      if (split.useZKForAssignment) {
        server.postOpenDeployTasks(regions.getFirst());
      }

      // Add to online regions
      server.addToOnlineRegions(regions.getSecond());
      // THIS is the crucial point:
      // the 2nd daughter was added, so querying before the split key should fail.
      assertFalse(test(conn, tableName, firstRow, server));
      // past splitkey is ok.
      assertTrue(test(conn, tableName, lastRow, server));

      // Add to online regions
      server.addToOnlineRegions(regions.getFirst());
      assertTrue(test(conn, tableName, firstRow, server));
      assertTrue(test(conn, tableName, lastRow, server));

      if (split.useZKForAssignment) {
        // 4. phase III
        ((BaseCoordinatedStateManager) server.getCoordinatedStateManager())
            .getSplitTransactionCoordination().completeSplitTransaction(server, regions.getFirst(),
              regions.getSecond(), split.std, region);
      }

      assertTrue(test(conn, tableName, firstRow, server));
      assertTrue(test(conn, tableName, lastRow, server));
    }
  }
```
```
Cyclomatic Complexity	 4
Assertions		 5
Lines of Code		42
-  
  public void testTableAvailableWhileSplitting() throws Exception {
    TableName tableName = TableName.valueOf("TestTableAvailableWhileSplitting");
    byte[] familyName = Bytes.toBytes("fam");
    try (HTable ht = TEST_UTIL.createTable(tableName, familyName)) {
      TEST_UTIL.loadTable(ht, familyName, false);
    }
    Admin admin = TEST_UTIL.getHBaseAdmin();
    HRegionServer server = TEST_UTIL.getHBaseCluster().getRegionServer(0);
    byte[] splitRow = Bytes.toBytes("lll");
    try (Connection conn = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration())) {
      byte[] regionName = conn.getRegionLocator(tableName).getRegionLocation(splitRow)
          .getRegionInfo().getRegionName();
      Region region = server.getRegion(regionName);
      SplitTransactionImpl split = new SplitTransactionImpl((HRegion) region, splitRow);
      split.prepare();
      assertTrue(admin.isTableAvailable(tableName));

      // 1. phase I
      PairOfSameType<Region> regions = split.createDaughters(server, server, null);
      // Parent should be offline at this stage and daughters not yet open
      assertFalse(admin.isTableAvailable(tableName));

      // passing null as services prevents final step of postOpenDeployTasks
      // 2, most of phase II
      split.openDaughters(server, null, regions.getFirst(), regions.getSecond());
      assertFalse(admin.isTableAvailable(tableName));

      // Finish openeing daughters
      // 2nd daughter first
      if (split.useZKForAssignment) {
        server.postOpenDeployTasks(regions.getSecond());
      } else {
        server.reportRegionStateTransition(
          RegionServerStatusProtos.RegionStateTransition.TransitionCode.SPLIT,
          region.getRegionInfo(), regions.getFirst().getRegionInfo(),
          regions.getSecond().getRegionInfo());
      }

      // first daughter second
      if (split.useZKForAssignment) {
        server.postOpenDeployTasks(regions.getFirst());
      }

      // After postOpenDeploy daughters should have location in meta
      assertTrue(admin.isTableAvailable(tableName));

      server.addToOnlineRegions(regions.getSecond());
      server.addToOnlineRegions(regions.getFirst());
      assertTrue(admin.isTableAvailable(tableName));
    } finally {
      if (admin != null) {
        admin.close();
      }
    }
  }
```
```
Cyclomatic Complexity	 3
Assertions		 7
Lines of Code		45
-  (timeout = 60000)
  public void testSplitFailedCompactionAndSplit() throws Exception {
    final TableName tableName = TableName.valueOf("testSplitFailedCompactionAndSplit");
    Configuration conf = TESTING_UTIL.getConfiguration();
    try {
      HBaseAdmin admin = new HBaseAdmin(conf);
      // Create table then get the single region for our new table.
      HTableDescriptor htd = new HTableDescriptor(tableName);
      byte[] cf = Bytes.toBytes("cf");
      htd.addFamily(new HColumnDescriptor(cf));
      admin.createTable(htd);

      for (int i = 0; cluster.getRegions(tableName).size() == 0 && i < 100; i++) {
        Thread.sleep(100);
      }
      assertEquals(1, cluster.getRegions(tableName).size());

      HRegion region = cluster.getRegions(tableName).get(0);
      Store store = region.getStore(cf);
      int regionServerIndex = cluster.getServerWith(region.getRegionInfo().getRegionName());
      HRegionServer regionServer = cluster.getRegionServer(regionServerIndex);

      Table t = new HTable(conf, tableName);
      // insert data
      insertData(tableName, admin, t);
      insertData(tableName, admin, t);

      int fileNum = store.getStorefiles().size();
      // 0, Compaction Request
      store.triggerMajorCompaction();
      CompactionContext cc = store.requestCompaction();
      assertNotNull(cc);
      // 1, A timeout split
      // 1.1 close region
      assertEquals(2, region.close(false).get(cf).size());
      // 1.2 rollback and Region initialize again
      region.initialize();

      // 2, Run Compaction cc
      assertFalse(region.compact(cc, store, NoLimitThroughputController.INSTANCE));
      assertTrue(fileNum > store.getStorefiles().size());

      // 3, Split
      SplitTransaction st = new SplitTransactionImpl(region, Bytes.toBytes("row3"));
      assertTrue(st.prepare());
      st.execute(regionServer, regionServer);
      LOG.info("Waiting for region to come out of RIT");
      TESTING_UTIL.waitFor(60000, 1000, new Waiter.Predicate<Exception>() {
        @Override
        public boolean evaluate() throws Exception {
          RegionStates regionStates = cluster.getMaster().getAssignmentManager().getRegionStates();
          return !regionStates.isRegionsInTransition();
        }
      });
      assertEquals(2, cluster.getRegions(tableName).size());
    } finally {
      TESTING_UTIL.deleteTable(tableName);
    }
  }
```
```
Cyclomatic Complexity	 1
Assertions		 4
Lines of Code		39
-  (timeout = 120000)
  public void testFailedSplit() throws Exception {
    TableName tableName = TableName.valueOf("testFailedSplit");
    byte[] colFamily = Bytes.toBytes("info");
    TESTING_UTIL.createTable(tableName, colFamily);
    Connection connection = ConnectionFactory.createConnection(TESTING_UTIL.getConfiguration());
    HTable table = (HTable) connection.getTable(tableName);
    try {
      TESTING_UTIL.loadTable(table, colFamily);
      List<HRegionInfo> regions = TESTING_UTIL.getHBaseAdmin().getTableRegions(tableName);
      assertTrue(regions.size() == 1);
      final HRegion actualRegion = cluster.getRegions(tableName).get(0);
      actualRegion.getCoprocessorHost().load(FailingSplitRegionObserver.class,
        Coprocessor.PRIORITY_USER, actualRegion.getBaseConf());

      // The following split would fail.
      admin.split(tableName);
      FailingSplitRegionObserver observer = (FailingSplitRegionObserver) actualRegion
          .getCoprocessorHost().findCoprocessor(FailingSplitRegionObserver.class.getName());
      assertNotNull(observer);
      observer.latch.await();
      observer.postSplit.await();
      LOG.info("Waiting for region to come out of RIT: " + actualRegion);
      TESTING_UTIL.waitFor(60000, 1000, new Waiter.Predicate<Exception>() {
        @Override
        public boolean evaluate() throws Exception {
          RegionStates regionStates = cluster.getMaster().getAssignmentManager().getRegionStates();
          return !regionStates.isRegionsInTransition();
        }
      });
      regions = TESTING_UTIL.getHBaseAdmin().getTableRegions(tableName);
      assertTrue(regions.size() == 1);
      RegionStates regionStates = cluster.getMaster().getAssignmentManager().getRegionStates();
      Set<RegionState> rit = regionStates.getRegionsInTransition();
      assertTrue(rit.size() == 0);
    } finally {
      table.close();
      connection.close();
      TESTING_UTIL.deleteTable(tableName);
    }
  }
```
```
Cyclomatic Complexity	 2
Assertions		 2
Lines of Code		51
-   (timeout=300000)
  public void testSSHCleanupDaugtherRegionsOfAbortedSplit() throws Exception {
    TableName table = TableName.valueOf("testSSHCleanupDaugtherRegionsOfAbortedSplit");
    try {
      HTableDescriptor desc = new HTableDescriptor(table);
      desc.addFamily(new HColumnDescriptor(Bytes.toBytes("f")));
      admin.createTable(desc);
      HTable hTable = new HTable(cluster.getConfiguration(), desc.getTableName());
      for(int i = 1; i < 5; i++) {
        Put p1 = new Put(("r"+i).getBytes());
        p1.add(Bytes.toBytes("f"), "q1".getBytes(), "v".getBytes());
        hTable.put(p1);
      }
      admin.flush(desc.getTableName());
      List<HRegion> regions = cluster.getRegions(desc.getTableName());
      int serverWith = cluster.getServerWith(regions.get(0).getRegionInfo().getRegionName());
      HRegionServer regionServer = cluster.getRegionServer(serverWith);
      cluster.getServerWith(regions.get(0).getRegionInfo().getRegionName());
      SplitTransactionImpl st = new SplitTransactionImpl(regions.get(0), Bytes.toBytes("r3"));
      st.prepare();
      st.stepsBeforePONR(regionServer, regionServer, false);
      Path tableDir =
          FSUtils.getTableDir(cluster.getMaster().getMasterFileSystem().getRootDir(),
            desc.getTableName());
      tableDir.getFileSystem(cluster.getConfiguration());
      List<Path> regionDirs =
          FSUtils.getRegionDirs(tableDir.getFileSystem(cluster.getConfiguration()), tableDir);
      assertEquals(3,regionDirs.size());
      cluster.startRegionServer();
      regionServer.kill();
      cluster.getRegionServerThreads().get(serverWith).join();
      // Wait until finish processing of shutdown
      TESTING_UTIL.waitFor(60000, 1000, new Waiter.Predicate<Exception>() {
        @Override
        public boolean evaluate() throws Exception {
          return !cluster.getMaster().getServerManager().areDeadServersInProgress();
        }
      });
      // Wait until there are no more regions in transition
      TESTING_UTIL.waitFor(60000, 1000, new Waiter.Predicate<Exception>() {
        @Override
        public boolean evaluate() throws Exception {
          return !cluster.getMaster().getAssignmentManager().
              getRegionStates().isRegionsInTransition();
        }
      });
      regionDirs =
          FSUtils.getRegionDirs(tableDir.getFileSystem(cluster.getConfiguration()), tableDir);
      assertEquals(1,regionDirs.size());
    } finally {
      TESTING_UTIL.deleteTable(table);
    }
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		17
-  
  public void testNoEdits() throws Exception {
    TableName tableName = TableName.valueOf("TestLogRollPeriodNoEdits");
    TEST_UTIL.createTable(tableName, "cf");
    try {
      Table table = new HTable(TEST_UTIL.getConfiguration(), tableName);
      try {
        HRegionServer server = TEST_UTIL.getRSForFirstRegionInTable(tableName);
        WAL log = server.getWAL(null);
        checkMinLogRolls(log, 5);
      } finally {
        table.close();
      }
    } finally {
      TEST_UTIL.deleteTable(tableName);
    }
  }
```
```
Cyclomatic Complexity	 2
Assertions		 0
Lines of Code		38
-  (timeout=60000)
  public void testWithEdits() throws Exception {
    final TableName tableName = TableName.valueOf("TestLogRollPeriodWithEdits");
    final String family = "cf";

    TEST_UTIL.createTable(tableName, family);
    try {
      HRegionServer server = TEST_UTIL.getRSForFirstRegionInTable(tableName);
      WAL log = server.getWAL(null);
      final Table table = new HTable(TEST_UTIL.getConfiguration(), tableName);

      Thread writerThread = new Thread("writer") {
        @Override
        public void run() {
          try {
            long row = 0;
            while (!interrupted()) {
              Put p = new Put(Bytes.toBytes(String.format("row%d", row)));
              p.add(Bytes.toBytes(family), Bytes.toBytes("col"), Bytes.toBytes(row));
              table.put(p);
              row++;

              Thread.sleep(LOG_ROLL_PERIOD / 16);
            }
          } catch (Exception e) {
            LOG.warn(e);
          } 
        }
      };

      try {
        writerThread.start();
        checkMinLogRolls(log, 5);
      } finally {
        writerThread.interrupt();
        writerThread.join();
        table.close();
      }  
    } finally {
      TEST_UTIL.deleteTable(tableName);
    }
  }
```
```
Cyclomatic Complexity	 2
Assertions		 1
Lines of Code		49
-  (timeout=300000)
  public void testMultiSlaveReplication() throws Exception {
    LOG.info("Start the testMultiSlaveReplication Test");
    MiniHBaseCluster master = utility1.startMiniCluster();
    utility2.startMiniCluster();
    utility3.startMiniCluster();
    ReplicationAdmin admin1 = new ReplicationAdmin(conf1);

    new HBaseAdmin(conf1).createTable(table);
    new HBaseAdmin(conf2).createTable(table);
    new HBaseAdmin(conf3).createTable(table);
    Table htable1 = new HTable(conf1, tableName);
    htable1.setWriteBufferSize(1024);
    Table htable2 = new HTable(conf2, tableName);
    htable2.setWriteBufferSize(1024);
    Table htable3 = new HTable(conf3, tableName);
    htable3.setWriteBufferSize(1024);

    ReplicationPeerConfig rpc = new ReplicationPeerConfig();
    rpc.setClusterKey(utility2.getClusterKey());
    admin1.addPeer("1", rpc);

    // put "row" and wait 'til it got around, then delete
    putAndWait(row, famName, htable1, htable2);
    deleteAndWait(row, htable1, htable2);
    // check it wasn't replication to cluster 3
    checkRow(row,0,htable3);

    putAndWait(row2, famName, htable1, htable2);

    // now roll the region server's logs
    rollWALAndWait(utility1, htable1.getName(), row2);

    // after the log was rolled put a new row
    putAndWait(row3, famName, htable1, htable2);

    rpc = new ReplicationPeerConfig();
    rpc.setClusterKey(utility3.getClusterKey());
    admin1.addPeer("2", rpc);

    // put a row, check it was replicated to all clusters
    putAndWait(row1, famName, htable1, htable2, htable3);
    // delete and verify
    deleteAndWait(row1, htable1, htable2, htable3);

    // make sure row2 did not get replicated after
    // cluster 3 was added
    checkRow(row2,0,htable3);

    // row3 will get replicated, because it was in the
    // latest log
    checkRow(row3,1,htable3);

    Put p = new Put(row);
    p.add(famName, row, row);
    htable1.put(p);
    // now roll the logs again
    rollWALAndWait(utility1, htable1.getName(), row);

    // cleanup "row2", also conveniently use this to wait replication
    // to finish
    deleteAndWait(row2, htable1, htable2, htable3);
    // Even if the log was rolled in the middle of the replication
    // "row" is still replication.
    checkRow(row, 1, htable2);
    // Replication thread of cluster 2 may be sleeping, and since row2 is not there in it,
    // we should wait before checking.
    checkWithWait(row, 1, htable3);

    // cleanup the rest
    deleteAndWait(row, htable1, htable2, htable3);
    deleteAndWait(row3, htable1, htable2, htable3);

    utility3.shutdownMiniCluster();
    utility2.shutdownMiniCluster();
    utility1.shutdownMiniCluster();@@@+    table = TableDescriptorBuilder.newBuilder(tableName)@@@+      .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(famName)@@@+        .setScope(HConstants.REPLICATION_SCOPE_GLOBAL).build())@@@+      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(noRepfamName)).build();@@@   }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		4
-  (timeout=300000)
  public void killOneMasterRS() throws Exception {
    loadTableAndKillRS(utility1);
  }
```
```
Cyclomatic Complexity	 2
Assertions		 4
Lines of Code		19
-  
  public void testWALKeySerialization() throws Exception {
    Map<String, byte[]> attributes = new HashMap<String, byte[]>();
    attributes.put("foo", Bytes.toBytes("foo-value"));
    attributes.put("bar", Bytes.toBytes("bar-value"));
    WALKey key = new WALKey(info.getEncodedNameAsBytes(), tableName,
      System.currentTimeMillis(), 0L, 0L, mvcc, attributes);
    assertEquals(attributes, key.getExtendedAttributes());

    WALProtos.WALKey.Builder builder = key.getBuilder(null);
    WALProtos.WALKey serializedKey = builder.build();

    WALKey deserializedKey = new WALKey();
    deserializedKey.readFieldsFromPb(serializedKey, null);

    //equals() only checks region name, sequence id and write time
    assertEquals(key, deserializedKey);
    //can't use Map.equals() because byte arrays use reference equality
    assertEquals(key.getExtendedAttributes().keySet(),
      deserializedKey.getExtendedAttributes().keySet());
    for (Map.Entry<String, byte[]> entry : deserializedKey.getExtendedAttributes().entrySet()) {
      assertArrayEquals(key.getExtendedAttribute(entry.getKey()), entry.getValue());
    }
  }
```
```
Cyclomatic Complexity	 2
Assertions		 5
Lines of Code		31
-  
  public void testReplicationSourceWALReaderThreadWithFilter() throws Exception {
    final byte[] notReplicatedCf = Bytes.toBytes("notReplicated");
    final Map<TableName, List<String>> tableCfs = new HashMap<>();
    tableCfs.put(tableName, Collections.singletonList(Bytes.toString(family)));
    ReplicationPeer peer = mock(ReplicationPeer.class);
    when(peer.getTableCFs()).thenReturn(tableCfs);
    WALEntryFilter filter = new ChainWALEntryFilter(new TableCfWALEntryFilter(peer));

    // add filterable entries
    appendToLogPlus(3, notReplicatedCf);
    appendToLogPlus(3, notReplicatedCf);
    appendToLogPlus(3, notReplicatedCf);

    // add non filterable entries
    appendEntriesToLog(2);

    ReplicationSourceManager mockSourceManager = mock(ReplicationSourceManager.class);
    ReplicationSource source = Mockito.mock(ReplicationSource.class);
    when(source.isPeerEnabled()).thenReturn(true);
    when(mockSourceManager.getTotalBufferUsed()).thenReturn(new AtomicLong(0));
    final ReplicationSourceWALReaderThread reader =
            new ReplicationSourceWALReaderThread(mockSourceManager, getQueueInfo(), walQueue,
                    0, fs, conf, filter, new MetricsSource("1"), source);
    reader.start();

    WALEntryBatch entryBatch = reader.take();

    assertNotNull(entryBatch);
    assertFalse(entryBatch.isEmpty());
    List<Entry> walEntries = entryBatch.getWalEntries();
    assertEquals(2, walEntries.size());
    for (Entry entry : walEntries) {
      ArrayList<Cell> cells = entry.getEdit().getCells();
      assertTrue(cells.size() == 1);
      assertTrue(CellUtil.matchingFamily(cells.get(0), family));
    }
  }
```
```
Cyclomatic Complexity	 -1
Assertions		 7
Lines of Code		36
-  
  public void testReplicationSourceWALReaderThreadWithFilterWhenLogRolled() throws Exception {
    final byte[] notReplicatedCf = Bytes.toBytes("notReplicated");
    final Map<TableName, List<String>> tableCfs = new HashMap<>();
    tableCfs.put(tableName, Collections.singletonList(Bytes.toString(family)));
    ReplicationPeer peer = mock(ReplicationPeer.class);
    when(peer.getTableCFs()).thenReturn(tableCfs);
    WALEntryFilter filter = new ChainWALEntryFilter(new TableCfWALEntryFilter(peer));

    appendToLogPlus(3, notReplicatedCf);

    Path firstWAL = walQueue.peek();
    final long eof = getPosition(firstWAL);

    ReplicationSourceManager mockSourceManager = mock(ReplicationSourceManager.class);
    ReplicationSource source = Mockito.mock(ReplicationSource.class);
    when(source.isPeerEnabled()).thenReturn(true);
    when(mockSourceManager.getTotalBufferUsed()).thenReturn(new AtomicLong(0));
    final ReplicationSourceWALReaderThread reader =
            new ReplicationSourceWALReaderThread(mockSourceManager, getQueueInfo(), walQueue,
                    0, fs, conf, filter, new MetricsSource("1"), source);
    reader.start();

    // reader won't put any batch, even if EOF reached.
    Waiter.waitFor(conf, 20000, new Waiter.Predicate<Exception>() {
      @Override public boolean evaluate() {
        return reader.getLastReadPosition() >= eof;
      }
    });
    assertNull(reader.poll(0));

    log.rollWriter();

    // should get empty batch with current wal position, after wal rolled
    WALEntryBatch entryBatch = reader.take();

    Path lastWAL= walQueue.peek();
    long positionToBeLogged = getPosition(lastWAL);

    assertNotNull(entryBatch);
    assertTrue(entryBatch.isEmpty());
    assertEquals(1, walQueue.size());
    assertNotEquals(firstWAL, entryBatch.getLastWalPath());
    assertEquals(lastWAL, entryBatch.getLastWalPath());
    assertEquals(positionToBeLogged, entryBatch.getLastWalPosition());
  }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		13
-   (timeout=180000)
  public void testSplit() throws Exception {
    AccessTestAction action = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preSplit(ObserverContext.createAndPrepare(RCP_ENV, null));
        return null;
      }
    };

    verifyAllowed(action, SUPERUSER, USER_ADMIN, USER_OWNER, USER_GROUP_ADMIN);
    verifyDenied(action, USER_CREATE, USER_RW, USER_RO, USER_NONE, USER_GROUP_READ,
      USER_GROUP_WRITE, USER_GROUP_CREATE);
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		7
-  
  public void testMigration() throws DeserializationException {
    Configuration conf = UTIL.getConfiguration();
    ListMultimap<String,TablePermission> permissions = createPermissions();
    byte [] bytes = writePermissionsAsBytes(permissions, conf);
    AccessControlLists.readPermissions(bytes, conf);
  }
```
```
Cyclomatic Complexity	 -1
Assertions		 3
Lines of Code		37
-  
  public void testTokenAuthentication() throws Exception {
    UserGroupInformation testuser =
        UserGroupInformation.createUserForTesting("testuser", new String[]{"testgroup"});

    testuser.setAuthenticationMethod(
        UserGroupInformation.AuthenticationMethod.TOKEN);
    final Configuration conf = TEST_UTIL.getConfiguration();
    UserGroupInformation.setConfiguration(conf);
    Token<AuthenticationTokenIdentifier> token =
        secretManager.generateToken("testuser");
    LOG.debug("Got token: " + token.toString());
    testuser.addToken(token);

    // verify the server authenticates us as this token user
    testuser.doAs(new PrivilegedExceptionAction<Object>() {
      public Object run() throws Exception {
        Configuration c = server.getConfiguration();
        RpcClient rpcClient = RpcClientFactory.createClient(c, clusterId.toString());
        ServerName sn =
            ServerName.valueOf(server.getAddress().getHostName(), server.getAddress().getPort(),
                System.currentTimeMillis());
        try {
          BlockingRpcChannel channel = rpcClient.createBlockingRpcChannel(sn,
              User.getCurrent(), HConstants.DEFAULT_HBASE_RPC_TIMEOUT);
          AuthenticationProtos.AuthenticationService.BlockingInterface stub =
              AuthenticationProtos.AuthenticationService.newBlockingStub(channel);
          AuthenticationProtos.WhoAmIResponse response =
              stub.whoAmI(null, AuthenticationProtos.WhoAmIRequest.getDefaultInstance());
          String myname = response.getUsername();
          assertEquals("testuser", myname);
          String authMethod = response.getAuthMethod();
          assertEquals("TOKEN", authMethod);
        } finally {
          rpcClient.close();
        }
        return null;
      }
    });
  }
```
```
Cyclomatic Complexity	 2
Assertions		 0
Lines of Code		4
-  
  public void testThriftServerHttpTraceForbiddenWhenOptionsDisabled() throws Exception {
    // HTTP TRACE method should be disabled for security
    // See https://www.owasp.org/index.php/Cross_Site_Tracing
    checkHttpMethods("TRACE", HttpURLConnection.HTTP_FORBIDDEN);
  }
```
```
Cyclomatic Complexity	 2
Assertions		 0
Lines of Code		6
-  
  public void testThriftServerHttpTraceForbiddenWhenOptionsEnabled() throws Exception {
    // HTTP TRACE method should be disabled for security
    // See https://www.owasp.org/index.php/Cross_Site_Tracing
    TEST_UTIL.getConfiguration().setBoolean(ThriftServerRunner.THRIFT_HTTP_ALLOW_OPTIONS_METHOD,
        true);
    checkHttpMethods("TRACE", HttpURLConnection.HTTP_FORBIDDEN);
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		5
-  
  public void testThriftServerHttpOptionsForbiddenWhenOptionsDisabled() throws Exception {
    // HTTP OPTIONS method should be disabled by default, so we make sure
    // hbase.thrift.http.allow.options.method is not set anywhere in the config
    TEST_UTIL.getConfiguration().unset(ThriftServerRunner.THRIFT_HTTP_ALLOW_OPTIONS_METHOD);
    checkHttpMethods("OPTIONS", HttpURLConnection.HTTP_FORBIDDEN);
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		6
-  
  public void testThriftServerHttpOptionsOkWhenOptionsEnabled() throws Exception {
    TEST_UTIL.getConfiguration().setBoolean(ThriftServerRunner.THRIFT_HTTP_ALLOW_OPTIONS_METHOD,
        true);
    checkHttpMethods("OPTIONS", HttpURLConnection.HTTP_OK);
  }
```
## 7092b814bd5c5e243987853771dc2ec339ae0b0c ##
```
Cyclomatic Complexity	1
Assertions		1
Lines of Code		9
-  
  public void testSubmit() throws Exception {
    ClusterConnection hc = createHConnection();
    AsyncProcess ap = new MyAsyncProcess(hc, conf);

    List<Put> puts = new ArrayList<Put>();
    puts.add(createPut(1, true));

    ap.submit(DUMMY_TABLE, puts, false, null, false);
    Assert.assertTrue(puts.isEmpty());@@@   }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		13
-  
  public void testIllegalArgument() throws IOException {
    ClusterConnection conn = createHConnection();
    final long maxHeapSizePerRequest = conn.getConfiguration().getLong(AsyncProcess.HBASE_CLIENT_MAX_PERREQUEST_HEAPSIZE,
      AsyncProcess.DEFAULT_HBASE_CLIENT_MAX_PERREQUEST_HEAPSIZE);
    conn.getConfiguration().setLong(AsyncProcess.HBASE_CLIENT_MAX_PERREQUEST_HEAPSIZE, -1);
    try {
      MyAsyncProcess ap = new MyAsyncProcess(conn, conf, true);
      fail("The maxHeapSizePerRequest must be bigger than zero");
    } catch (IllegalArgumentException e) {
    }
    conn.getConfiguration().setLong(AsyncProcess.HBASE_CLIENT_MAX_PERREQUEST_HEAPSIZE, maxHeapSizePerRequest);
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		4
-  
  public void testHTableFailedPutWithBuffer() throws Exception {
    doHTableFailedPut(true);
  }
```
```
Cyclomatic Complexity	 5
Assertions		 8
Lines of Code		32
-  
  public void testRequestSizeCheckerr() throws IOException {
    final long maxHeapSizePerRequest = 2 * 1024 * 1024;
    final ClusterConnection conn = createHConnection();
    RequestSizeChecker checker = new RequestSizeChecker(maxHeapSizePerRequest);

    // inner state is unchanged.
    for (int i = 0; i != 10; ++i) {
      ReturnCode code = checker.canTakeOperation(loc1, maxHeapSizePerRequest);
      assertEquals(RowChecker.ReturnCode.INCLUDE, code);
      code = checker.canTakeOperation(loc2, maxHeapSizePerRequest);
      assertEquals(RowChecker.ReturnCode.INCLUDE, code);
    }

    // accept the data located on loc1 region.
    ReturnCode acceptCode = checker.canTakeOperation(loc1, maxHeapSizePerRequest);
    assertEquals(RowChecker.ReturnCode.INCLUDE, acceptCode);
    checker.notifyFinal(acceptCode, loc1, maxHeapSizePerRequest);

    // the sn server reachs the limit.
    for (int i = 0; i != 10; ++i) {
      ReturnCode code = checker.canTakeOperation(loc1, maxHeapSizePerRequest);
      assertNotEquals(RowChecker.ReturnCode.INCLUDE, code);
      code = checker.canTakeOperation(loc2, maxHeapSizePerRequest);
      assertNotEquals(RowChecker.ReturnCode.INCLUDE, code);
    }

    // the request to sn2 server should be accepted.
    for (int i = 0; i != 10; ++i) {
      ReturnCode code = checker.canTakeOperation(loc3, maxHeapSizePerRequest);
      assertEquals(RowChecker.ReturnCode.INCLUDE, code);
    }

    checker.reset();
    for (int i = 0; i != 10; ++i) {
      ReturnCode code = checker.canTakeOperation(loc1, maxHeapSizePerRequest);
      assertEquals(RowChecker.ReturnCode.INCLUDE, code);
      code = checker.canTakeOperation(loc2, maxHeapSizePerRequest);
      assertEquals(RowChecker.ReturnCode.INCLUDE, code);
    }
  }
```
```
Cyclomatic Complexity	 6
Assertions		 4
Lines of Code		25
-  
  public void testSubmittedSizeChecker() {
    final long maxHeapSizeSubmit = 2 * 1024 * 1024;
    SubmittedSizeChecker checker = new SubmittedSizeChecker(maxHeapSizeSubmit);

    for (int i = 0; i != 10; ++i) {
      ReturnCode include = checker.canTakeOperation(loc1, 100000);
      assertEquals(ReturnCode.INCLUDE, include);
    }

    for (int i = 0; i != 10; ++i) {
      checker.notifyFinal(ReturnCode.INCLUDE, loc1, maxHeapSizeSubmit);
    }

    for (int i = 0; i != 10; ++i) {
      ReturnCode include = checker.canTakeOperation(loc1, 100000);
      assertEquals(ReturnCode.END, include);
    }
    for (int i = 0; i != 10; ++i) {
      ReturnCode include = checker.canTakeOperation(loc2, 100000);
      assertEquals(ReturnCode.END, include);
    }
    checker.reset();
    for (int i = 0; i != 10; ++i) {
      ReturnCode include = checker.canTakeOperation(loc1, 100000);
      assertEquals(ReturnCode.INCLUDE, include);
    }
  }
```
```
Cyclomatic Complexity	 1
Assertions		 8
Lines of Code		25
-   (timeout=20000)
  public void testTriggerNowFailsWhenNotScheduled() throws InterruptedException {
    final int period = 100;
    // Small sleep time buffer to allow CountingChore to complete
    final int sleep = 5;
    ChoreService service = new ChoreService("testTriggerNowFailsWhenNotScheduled");
    CountingChore chore = new CountingChore("dn", period);

    try {
      assertFalse(chore.triggerNow());
      assertTrue(chore.getCountOfChoreCalls() == 0);

      service.scheduleChore(chore);
      Thread.sleep(sleep);
      assertEquals(1, chore.getCountOfChoreCalls());
      Thread.sleep(period);
      assertEquals(2, chore.getCountOfChoreCalls());
      assertTrue(chore.triggerNow());
      Thread.sleep(sleep);
      assertTrue(chore.triggerNow());
      Thread.sleep(sleep);
      assertTrue(chore.triggerNow());
      Thread.sleep(sleep);
      assertEquals(5, chore.getCountOfChoreCalls());
    } finally {
      shutdownService(service);@@@     }@@@   }
```
```
Cyclomatic Complexity	 3
Assertions		 5
Lines of Code		32
-   (timeout=300000)
  public void testShouldFailOnlineSchemaUpdateIfOnlineSchemaIsNotEnabled()
      throws Exception {
    final TableName tableName = TableName.valueOf("changeTableSchemaOnlineFailure");
    TEST_UTIL.getMiniHBaseCluster().getMaster().getConfiguration().setBoolean(
        "hbase.online.schema.update.enable", false);
    HTableDescriptor[] tables = admin.listTables();
    int numTables = tables.length;
    TEST_UTIL.createTable(tableName, HConstants.CATALOG_FAMILY).close();
    tables = this.admin.listTables();
    assertEquals(numTables + 1, tables.length);

    // FIRST, do htabledescriptor changes.
    HTableDescriptor htd = this.admin.getTableDescriptor(tableName);
    // Make a copy and assert copy is good.
    HTableDescriptor copy = new HTableDescriptor(htd);
    assertTrue(htd.equals(copy));
    // Now amend the copy. Introduce differences.
    long newFlushSize = htd.getMemStoreFlushSize() / 2;
    if (newFlushSize <=0) {
      newFlushSize = HTableDescriptor.DEFAULT_MEMSTORE_FLUSH_SIZE / 2;
    }
    copy.setMemStoreFlushSize(newFlushSize);
    final String key = "anyoldkey";
    assertTrue(htd.getValue(key) == null);
    copy.setValue(key, key);
    boolean expectedException = false;
    try {
      admin.modifyTable(tableName, copy);
    } catch (TableNotDisabledException re) {
      expectedException = true;
    }
    assertTrue("Online schema update should not happen.", expectedException);

    // Reset the value for the other tests
    TEST_UTIL.getMiniHBaseCluster().getMaster().getConfiguration().setBoolean(
        "hbase.online.schema.update.enable", true);
  }
```
```
Cyclomatic Complexity	 21
Assertions		 12
Lines of Code		165
-   (timeout=240000)
  public void testMasterFailoverWithMockedRIT() throws Exception {

    final int NUM_MASTERS = 1;
    final int NUM_RS = 3;

    // Create config to use for this cluster
    Configuration conf = HBaseConfiguration.create();
    conf.setBoolean("hbase.assignment.usezk", true);

    // Start the cluster
    HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility(conf);
    TEST_UTIL.startMiniCluster(NUM_MASTERS, NUM_RS);
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    log("Cluster started");

    // Create a ZKW to use in the test
    ZooKeeperWatcher zkw = HBaseTestingUtility.getZooKeeperWatcher(TEST_UTIL);

    // get all the master threads
    List<MasterThread> masterThreads = cluster.getMasterThreads();
    assertEquals(1, masterThreads.size());

    // only one master thread, let's wait for it to be initialized
    assertTrue(cluster.waitForActiveAndReadyMaster());
    HMaster master = masterThreads.get(0).getMaster();
    assertTrue(master.isActiveMaster());
    assertTrue(master.isInitialized());

    // disable load balancing on this master
    master.balanceSwitch(false);

    // create two tables in META, each with 10 regions
    byte [] FAMILY = Bytes.toBytes("family");
    byte [][] SPLIT_KEYS = new byte [][] {
        new byte[0], Bytes.toBytes("aaa"), Bytes.toBytes("bbb"),
        Bytes.toBytes("ccc"), Bytes.toBytes("ddd"), Bytes.toBytes("eee"),
        Bytes.toBytes("fff"), Bytes.toBytes("ggg"), Bytes.toBytes("hhh"),
        Bytes.toBytes("iii"), Bytes.toBytes("jjj")
    };

    byte [] enabledTable = Bytes.toBytes("enabledTable");
    HTableDescriptor htdEnabled = new HTableDescriptor(TableName.valueOf(enabledTable));
    htdEnabled.addFamily(new HColumnDescriptor(FAMILY));

    FileSystem filesystem = FileSystem.get(conf);
    Path rootdir = FSUtils.getRootDir(conf);
    FSTableDescriptors fstd = new FSTableDescriptors(conf, filesystem, rootdir);
    // Write the .tableinfo
    fstd.createTableDescriptor(htdEnabled);

    HRegionInfo hriEnabled = new HRegionInfo(htdEnabled.getTableName(), null, null);
    createRegion(hriEnabled, rootdir, conf, htdEnabled);

    List<HRegionInfo> enabledRegions = TEST_UTIL.createMultiRegionsInMeta(
        TEST_UTIL.getConfiguration(), htdEnabled, SPLIT_KEYS);

    TableName disabledTable = TableName.valueOf("disabledTable");
    HTableDescriptor htdDisabled = new HTableDescriptor(disabledTable);
    htdDisabled.addFamily(new HColumnDescriptor(FAMILY));
    // Write the .tableinfo
    fstd.createTableDescriptor(htdDisabled);
    HRegionInfo hriDisabled = new HRegionInfo(htdDisabled.getTableName(), null, null);
    createRegion(hriDisabled, rootdir, conf, htdDisabled);
    List<HRegionInfo> disabledRegions = TEST_UTIL.createMultiRegionsInMeta(
        TEST_UTIL.getConfiguration(), htdDisabled, SPLIT_KEYS);

    TableName tableWithMergingRegions = TableName.valueOf("tableWithMergingRegions");
    TEST_UTIL.createTable(tableWithMergingRegions, FAMILY, new byte [][] {Bytes.toBytes("m")});

    log("Regions in hbase:meta and namespace have been created");

    // at this point we only expect 4 regions to be assigned out
    // (catalogs and namespace, + 2 merging regions)
    assertEquals(4, cluster.countServedRegions());

    // Move merging regions to the same region server
    AssignmentManager am = master.getAssignmentManager();
    RegionStates regionStates = am.getRegionStates();
    List<HRegionInfo> mergingRegions = regionStates.getRegionsOfTable(tableWithMergingRegions);
    assertEquals(2, mergingRegions.size());
    HRegionInfo a = mergingRegions.get(0);
    HRegionInfo b = mergingRegions.get(1);
    HRegionInfo newRegion = RegionMergeTransactionImpl.getMergedRegionInfo(a, b);
    ServerName mergingServer = regionStates.getRegionServerOfRegion(a);
    ServerName serverB = regionStates.getRegionServerOfRegion(b);
    if (!serverB.equals(mergingServer)) {
      RegionPlan plan = new RegionPlan(b, serverB, mergingServer);
      am.balance(plan);
      assertTrue(am.waitForAssignment(b));
    }

    // Let's just assign everything to first RS
    HRegionServer hrs = cluster.getRegionServer(0);
    ServerName serverName = hrs.getServerName();
    HRegionInfo closingRegion = enabledRegions.remove(0);
    // we'll need some regions to already be assigned out properly on live RS
    List<HRegionInfo> enabledAndAssignedRegions = new ArrayList<HRegionInfo>();
    enabledAndAssignedRegions.add(enabledRegions.remove(0));
    enabledAndAssignedRegions.add(enabledRegions.remove(0));
    enabledAndAssignedRegions.add(closingRegion);

    List<HRegionInfo> disabledAndAssignedRegions = new ArrayList<HRegionInfo>();
    disabledAndAssignedRegions.add(disabledRegions.remove(0));
    disabledAndAssignedRegions.add(disabledRegions.remove(0));

    // now actually assign them
    for (HRegionInfo hri : enabledAndAssignedRegions) {
      master.assignmentManager.addPlan(hri.getEncodedName(),
          new RegionPlan(hri, null, serverName));
      master.assignmentManager.assign(hri, true);
    }

    for (HRegionInfo hri : disabledAndAssignedRegions) {
      master.assignmentManager.addPlan(hri.getEncodedName(),
          new RegionPlan(hri, null, serverName));
      master.assignmentManager.assign(hri, true);
    }

    // wait for no more RIT
    log("Waiting for assignment to finish");
    ZKAssign.blockUntilNoRIT(zkw);
    log("Assignment completed");

    // Stop the master
    log("Aborting master");
    cluster.abortMaster(0);
    cluster.waitOnMaster(0);
    log("Master has aborted");

    /*
     * Now, let's start mocking up some weird states as described in the method
     * javadoc.
     */

    List<HRegionInfo> regionsThatShouldBeOnline = new ArrayList<HRegionInfo>();
    List<HRegionInfo> regionsThatShouldBeOffline = new ArrayList<HRegionInfo>();

    log("Beginning to mock scenarios");

    // Disable the disabledTable in ZK
    TableStateManager zktable = new ZKTableStateManager(zkw);
    zktable.setTableState(disabledTable, ZooKeeperProtos.Table.State.DISABLED);

    /*
     *  ZK = OFFLINE
     */

    // Region that should be assigned but is not and is in ZK as OFFLINE
    // Cause: This can happen if the master crashed after creating the znode but before sending the
    //  request to the region server
    HRegionInfo region = enabledRegions.remove(0);
    regionsThatShouldBeOnline.add(region);
    ZKAssign.createNodeOffline(zkw, region, serverName);

    /*
     * ZK = CLOSING
     */
    // Cause: Same as offline.
    regionsThatShouldBeOnline.add(closingRegion);
    ZKAssign.createNodeClosing(zkw, closingRegion, serverName);

    /*
     * ZK = CLOSED
     */

    // Region of enabled table closed but not ack
    //Cause: Master was down while the region server updated the ZK status.
    region = enabledRegions.remove(0);
    regionsThatShouldBeOnline.add(region);
    int version = ZKAssign.createNodeClosing(zkw, region, serverName);
    ZKAssign.transitionNodeClosed(zkw, region, serverName, version);

    // Region of disabled table closed but not ack
    region = disabledRegions.remove(0);
    regionsThatShouldBeOffline.add(region);
    version = ZKAssign.createNodeClosing(zkw, region, serverName);
    ZKAssign.transitionNodeClosed(zkw, region, serverName, version);

    /*
     * ZK = OPENED
     */

    // Region of enabled table was opened on RS
    // Cause: as offline
    region = enabledRegions.remove(0);
    regionsThatShouldBeOnline.add(region);
    ZKAssign.createNodeOffline(zkw, region, serverName);
    ProtobufUtil.openRegion(null, hrs.getRSRpcServices(), hrs.getServerName(), region);
    while (true) {
      byte [] bytes = ZKAssign.getData(zkw, region.getEncodedName());
      RegionTransition rt = RegionTransition.parseFrom(bytes);
      if (rt != null && rt.getEventType().equals(EventType.RS_ZK_REGION_OPENED)) {
        break;
      }
      Thread.sleep(100);
    }

    // Region of disable table was opened on RS
    // Cause: Master failed while updating the status for this region server.
    region = disabledRegions.remove(0);
    regionsThatShouldBeOffline.add(region);
    ZKAssign.createNodeOffline(zkw, region, serverName);
    ProtobufUtil.openRegion(null, hrs.getRSRpcServices(), hrs.getServerName(), region);
    while (true) {
      byte [] bytes = ZKAssign.getData(zkw, region.getEncodedName());
      RegionTransition rt = RegionTransition.parseFrom(bytes);
      if (rt != null && rt.getEventType().equals(EventType.RS_ZK_REGION_OPENED)) {
        break;
      }
      Thread.sleep(100);
    }

    /*
     * ZK = MERGING
     */

    // Regions of table of merging regions
    // Cause: Master was down while merging was going on
    hrs.getCoordinatedStateManager().
      getRegionMergeCoordination().startRegionMergeTransaction(newRegion, mergingServer, a, b);

    /*
     * ZK = NONE
     */

    /*
     * DONE MOCKING
     */

    log("Done mocking data up in ZK");

    // Start up a new master
    log("Starting up a new master");
    master = cluster.startMaster().getMaster();
    log("Waiting for master to be ready");
    cluster.waitForActiveAndReadyMaster();
    log("Master is ready");

    // Get new region states since master restarted
    regionStates = master.getAssignmentManager().getRegionStates();
    // Merging region should remain merging
    assertTrue(regionStates.isRegionInState(a, State.MERGING));
    assertTrue(regionStates.isRegionInState(b, State.MERGING));
    assertTrue(regionStates.isRegionInState(newRegion, State.MERGING_NEW));
    // Now remove the faked merging znode, merging regions should be
    // offlined automatically, otherwise it is a bug in AM.
    ZKAssign.deleteNodeFailSilent(zkw, newRegion);

    // Failover should be completed, now wait for no RIT
    log("Waiting for no more RIT");
    ZKAssign.blockUntilNoRIT(zkw);
    log("No more RIT in ZK, now doing final test verification");

    // Grab all the regions that are online across RSs
    Set<HRegionInfo> onlineRegions = new TreeSet<HRegionInfo>();
    for (JVMClusterUtil.RegionServerThread rst :
      cluster.getRegionServerThreads()) {
      onlineRegions.addAll(ProtobufUtil.getOnlineRegions(
        rst.getRegionServer().getRSRpcServices()));
    }

    // Now, everything that should be online should be online
    for (HRegionInfo hri : regionsThatShouldBeOnline) {
      assertTrue(onlineRegions.contains(hri));
    }

    // Everything that should be offline should not be online
    for (HRegionInfo hri : regionsThatShouldBeOffline) {
      if (onlineRegions.contains(hri)) {
       LOG.debug(hri);
      }
      assertFalse(onlineRegions.contains(hri));
    }

    log("Done with verification, all passed, shutting down cluster");

    // Done, shutdown the cluster
    TEST_UTIL.shutdownMiniCluster();
  }
```
```
Cyclomatic Complexity	 25
Assertions		 21
Lines of Code		284
-   (timeout=180000)
  public void testMasterFailoverWithMockedRITOnDeadRS() throws Exception {

    final int NUM_MASTERS = 1;
    final int NUM_RS = 2;

    // Create and start the cluster
    HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setBoolean("hbase.assignment.usezk", true);

    conf.setInt(ServerManager.WAIT_ON_REGIONSERVERS_MINTOSTART, 1);
    conf.setInt(ServerManager.WAIT_ON_REGIONSERVERS_MAXTOSTART, 2);
    TEST_UTIL.startMiniCluster(NUM_MASTERS, NUM_RS);
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    log("Cluster started");

    // Create a ZKW to use in the test
    ZooKeeperWatcher zkw = new ZooKeeperWatcher(TEST_UTIL.getConfiguration(),
        "unittest", new Abortable() {

          @Override
          public void abort(String why, Throwable e) {
            LOG.error("Fatal ZK Error: " + why, e);
            org.junit.Assert.assertFalse("Fatal ZK error", true);
          }

          @Override
          public boolean isAborted() {
            return false;
          }

    });

    // get all the master threads
    List<MasterThread> masterThreads = cluster.getMasterThreads();
    assertEquals(1, masterThreads.size());

    // only one master thread, let's wait for it to be initialized
    assertTrue(cluster.waitForActiveAndReadyMaster());
    HMaster master = masterThreads.get(0).getMaster();
    assertTrue(master.isActiveMaster());
    assertTrue(master.isInitialized());

    // disable load balancing on this master
    master.balanceSwitch(false);

    // create two tables in META, each with 30 regions
    byte [] FAMILY = Bytes.toBytes("family");
    byte[][] SPLIT_KEYS =
        TEST_UTIL.getRegionSplitStartKeys(Bytes.toBytes("aaa"), Bytes.toBytes("zzz"), 30);

    byte [] enabledTable = Bytes.toBytes("enabledTable");
    HTableDescriptor htdEnabled = new HTableDescriptor(TableName.valueOf(enabledTable));
    htdEnabled.addFamily(new HColumnDescriptor(FAMILY));
    FileSystem filesystem = FileSystem.get(conf);
    Path rootdir = FSUtils.getRootDir(conf);
    FSTableDescriptors fstd = new FSTableDescriptors(conf, filesystem, rootdir);
    // Write the .tableinfo
    fstd.createTableDescriptor(htdEnabled);
    HRegionInfo hriEnabled = new HRegionInfo(htdEnabled.getTableName(),
        null, null);
    createRegion(hriEnabled, rootdir, conf, htdEnabled);

    List<HRegionInfo> enabledRegions = TEST_UTIL.createMultiRegionsInMeta(
        TEST_UTIL.getConfiguration(), htdEnabled, SPLIT_KEYS);

    TableName disabledTable =
        TableName.valueOf("disabledTable");
    HTableDescriptor htdDisabled = new HTableDescriptor(disabledTable);
    htdDisabled.addFamily(new HColumnDescriptor(FAMILY));
    // Write the .tableinfo
    fstd.createTableDescriptor(htdDisabled);
    HRegionInfo hriDisabled = new HRegionInfo(htdDisabled.getTableName(), null, null);
    createRegion(hriDisabled, rootdir, conf, htdDisabled);

    List<HRegionInfo> disabledRegions = TEST_UTIL.createMultiRegionsInMeta(
        TEST_UTIL.getConfiguration(), htdDisabled, SPLIT_KEYS);

    log("Regions in hbase:meta and Namespace have been created");

    // at this point we only expect 2 regions to be assigned out (catalogs and namespace  )
    assertEquals(2, cluster.countServedRegions());

    // The first RS will stay online
    List<RegionServerThread> regionservers =
      cluster.getRegionServerThreads();
    HRegionServer hrs = regionservers.get(0).getRegionServer();

    // The second RS is going to be hard-killed
    RegionServerThread hrsDeadThread = regionservers.get(1);
    HRegionServer hrsDead = hrsDeadThread.getRegionServer();
    ServerName deadServerName = hrsDead.getServerName();

    // we'll need some regions to already be assigned out properly on live RS
    List<HRegionInfo> enabledAndAssignedRegions = new ArrayList<HRegionInfo>();
    enabledAndAssignedRegions.addAll(enabledRegions.subList(0, 6));
    enabledRegions.removeAll(enabledAndAssignedRegions);
    List<HRegionInfo> disabledAndAssignedRegions = new ArrayList<HRegionInfo>();
    disabledAndAssignedRegions.addAll(disabledRegions.subList(0, 6));
    disabledRegions.removeAll(disabledAndAssignedRegions);

    // now actually assign them
    for (HRegionInfo hri : enabledAndAssignedRegions) {
      master.assignmentManager.addPlan(hri.getEncodedName(),
          new RegionPlan(hri, null, hrs.getServerName()));
      master.assignmentManager.assign(hri, true);
    }
    for (HRegionInfo hri : disabledAndAssignedRegions) {
      master.assignmentManager.addPlan(hri.getEncodedName(),
          new RegionPlan(hri, null, hrs.getServerName()));
      master.assignmentManager.assign(hri, true);
    }

    log("Waiting for assignment to finish");
    ZKAssign.blockUntilNoRIT(zkw);
    master.assignmentManager.waitUntilNoRegionsInTransition(60000);
    log("Assignment completed");

    assertTrue(" Table must be enabled.", master.getAssignmentManager()
        .getTableStateManager().isTableState(TableName.valueOf("enabledTable"),
        ZooKeeperProtos.Table.State.ENABLED));
    // we also need regions assigned out on the dead server
    List<HRegionInfo> enabledAndOnDeadRegions = new ArrayList<HRegionInfo>();
    enabledAndOnDeadRegions.addAll(enabledRegions.subList(0, 6));
    enabledRegions.removeAll(enabledAndOnDeadRegions);
    List<HRegionInfo> disabledAndOnDeadRegions = new ArrayList<HRegionInfo>();
    disabledAndOnDeadRegions.addAll(disabledRegions.subList(0, 6));
    disabledRegions.removeAll(disabledAndOnDeadRegions);

    // set region plan to server to be killed and trigger assign
    for (HRegionInfo hri : enabledAndOnDeadRegions) {
      master.assignmentManager.addPlan(hri.getEncodedName(),
          new RegionPlan(hri, null, deadServerName));
      master.assignmentManager.assign(hri, true);
    }
    for (HRegionInfo hri : disabledAndOnDeadRegions) {
      master.assignmentManager.addPlan(hri.getEncodedName(),
          new RegionPlan(hri, null, deadServerName));
      master.assignmentManager.assign(hri, true);
    }

    // wait for no more RIT
    log("Waiting for assignment to finish");
    ZKAssign.blockUntilNoRIT(zkw);
    master.assignmentManager.waitUntilNoRegionsInTransition(60000);
    log("Assignment completed");

    // Due to master.assignment.assign(hri) could fail to assign a region to a specified RS
    // therefore, we need make sure that regions are in the expected RS
    verifyRegionLocation(hrs, enabledAndAssignedRegions);
    verifyRegionLocation(hrs, disabledAndAssignedRegions);
    verifyRegionLocation(hrsDead, enabledAndOnDeadRegions);
    verifyRegionLocation(hrsDead, disabledAndOnDeadRegions);

    assertTrue(" Didn't get enough regions of enabledTalbe on live rs.",
      enabledAndAssignedRegions.size() >= 2);
    assertTrue(" Didn't get enough regions of disalbedTable on live rs.",
      disabledAndAssignedRegions.size() >= 2);
    assertTrue(" Didn't get enough regions of enabledTalbe on dead rs.",
      enabledAndOnDeadRegions.size() >= 2);
    assertTrue(" Didn't get enough regions of disalbedTable on dead rs.",
      disabledAndOnDeadRegions.size() >= 2);

    // Stop the master
    log("Aborting master");
    cluster.abortMaster(0);
    cluster.waitOnMaster(0);
    log("Master has aborted");

    /*
     * Now, let's start mocking up some weird states as described in the method
     * javadoc.
     */

    List<HRegionInfo> regionsThatShouldBeOnline = new ArrayList<HRegionInfo>();
    List<HRegionInfo> regionsThatShouldBeOffline = new ArrayList<HRegionInfo>();

    log("Beginning to mock scenarios");

    // Disable the disabledTable in ZK
    TableStateManager zktable = new ZKTableStateManager(zkw);
    zktable.setTableState(disabledTable, ZooKeeperProtos.Table.State.DISABLED);

    assertTrue(" The enabled table should be identified on master fail over.",
        zktable.isTableState(TableName.valueOf("enabledTable"),
          ZooKeeperProtos.Table.State.ENABLED));

    /*
     * ZK = CLOSING
     */

    // Region of enabled table being closed on dead RS but not finished
    HRegionInfo region = enabledAndOnDeadRegions.remove(0);
    regionsThatShouldBeOnline.add(region);
    ZKAssign.createNodeClosing(zkw, region, deadServerName);
    LOG.debug("\n\nRegion of enabled table was CLOSING on dead RS\n" +
        region + "\n\n");

    // Region of disabled table being closed on dead RS but not finished
    region = disabledAndOnDeadRegions.remove(0);
    regionsThatShouldBeOffline.add(region);
    ZKAssign.createNodeClosing(zkw, region, deadServerName);
    LOG.debug("\n\nRegion of disabled table was CLOSING on dead RS\n" +
        region + "\n\n");

    /*
     * ZK = CLOSED
     */

    // Region of enabled on dead server gets closed but not ack'd by master
    region = enabledAndOnDeadRegions.remove(0);
    regionsThatShouldBeOnline.add(region);
    int version = ZKAssign.createNodeClosing(zkw, region, deadServerName);
    ZKAssign.transitionNodeClosed(zkw, region, deadServerName, version);
    LOG.debug("\n\nRegion of enabled table was CLOSED on dead RS\n" +
        region + "\n\n");

    // Region of disabled on dead server gets closed but not ack'd by master
    region = disabledAndOnDeadRegions.remove(0);
    regionsThatShouldBeOffline.add(region);
    version = ZKAssign.createNodeClosing(zkw, region, deadServerName);
    ZKAssign.transitionNodeClosed(zkw, region, deadServerName, version);
    LOG.debug("\n\nRegion of disabled table was CLOSED on dead RS\n" +
        region + "\n\n");

    /*
     * ZK = OPENING
     */

    // RS was opening a region of enabled table then died
    region = enabledRegions.remove(0);
    regionsThatShouldBeOnline.add(region);
    ZKAssign.createNodeOffline(zkw, region, deadServerName);
    ZKAssign.transitionNodeOpening(zkw, region, deadServerName);
    LOG.debug("\n\nRegion of enabled table was OPENING on dead RS\n" +
        region + "\n\n");

    // RS was opening a region of disabled table then died
    region = disabledRegions.remove(0);
    regionsThatShouldBeOffline.add(region);
    ZKAssign.createNodeOffline(zkw, region, deadServerName);
    ZKAssign.transitionNodeOpening(zkw, region, deadServerName);
    LOG.debug("\n\nRegion of disabled table was OPENING on dead RS\n" +
        region + "\n\n");

    /*
     * ZK = OPENED
     */

    // Region of enabled table was opened on dead RS
    region = enabledRegions.remove(0);
    regionsThatShouldBeOnline.add(region);
    ZKAssign.createNodeOffline(zkw, region, deadServerName);
    ProtobufUtil.openRegion(null, hrsDead.getRSRpcServices(),
      hrsDead.getServerName(), region);
    while (true) {
      byte [] bytes = ZKAssign.getData(zkw, region.getEncodedName());
      RegionTransition rt = RegionTransition.parseFrom(bytes);
      if (rt != null && rt.getEventType().equals(EventType.RS_ZK_REGION_OPENED)) {
        break;
      }
      Thread.sleep(100);
    }
    LOG.debug("\n\nRegion of enabled table was OPENED on dead RS\n" + region + "\n\n");

    // Region of disabled table was opened on dead RS
    region = disabledRegions.remove(0);
    regionsThatShouldBeOffline.add(region);
    ZKAssign.createNodeOffline(zkw, region, deadServerName);
    ProtobufUtil.openRegion(null, hrsDead.getRSRpcServices(),
      hrsDead.getServerName(), region);
    while (true) {
      byte [] bytes = ZKAssign.getData(zkw, region.getEncodedName());
      RegionTransition rt = RegionTransition.parseFrom(bytes);
      if (rt != null && rt.getEventType().equals(EventType.RS_ZK_REGION_OPENED)) {
        break;
      }
      Thread.sleep(100);
    }
    LOG.debug("\n\nRegion of disabled table was OPENED on dead RS\n" + region + "\n\n");

    /*
     * ZK = NONE
     */

    // Region of enabled table was open at steady-state on dead RS
    region = enabledRegions.remove(0);
    regionsThatShouldBeOnline.add(region);
    ZKAssign.createNodeOffline(zkw, region, deadServerName);
    ProtobufUtil.openRegion(null, hrsDead.getRSRpcServices(),
      hrsDead.getServerName(), region);
    while (true) {
      byte [] bytes = ZKAssign.getData(zkw, region.getEncodedName());
      RegionTransition rt = RegionTransition.parseFrom(bytes);
      if (rt != null && rt.getEventType().equals(EventType.RS_ZK_REGION_OPENED)) {
        ZKAssign.deleteOpenedNode(zkw, region.getEncodedName(), rt.getServerName());
        LOG.debug("DELETED " + rt);
        break;
      }
      Thread.sleep(100);
    }
    LOG.debug("\n\nRegion of enabled table was open at steady-state on dead RS"
        + "\n" + region + "\n\n");

    // Region of disabled table was open at steady-state on dead RS
    region = disabledRegions.remove(0);
    regionsThatShouldBeOffline.add(region);
    ZKAssign.createNodeOffline(zkw, region, deadServerName);
    ProtobufUtil.openRegion(null, hrsDead.getRSRpcServices(),
      hrsDead.getServerName(), region);
    while (true) {
      byte [] bytes = ZKAssign.getData(zkw, region.getEncodedName());
      RegionTransition rt = RegionTransition.parseFrom(bytes);
      if (rt != null && rt.getEventType().equals(EventType.RS_ZK_REGION_OPENED)) {
        ZKAssign.deleteOpenedNode(zkw, region.getEncodedName(), rt.getServerName());
        break;
      }
      Thread.sleep(100);
    }
    LOG.debug("\n\nRegion of disabled table was open at steady-state on dead RS"
      + "\n" + region + "\n\n");

    /*
     * DONE MOCKING
     */

    log("Done mocking data up in ZK");

    // Kill the RS that had a hard death
    log("Killing RS " + deadServerName);
    hrsDead.abort("Killing for unit test");
    log("RS " + deadServerName + " killed");

    // Start up a new master.  Wait until regionserver is completely down
    // before starting new master because of hbase-4511.
    while (hrsDeadThread.isAlive()) {
      Threads.sleep(10);
    }
    log("Starting up a new master");
    master = cluster.startMaster().getMaster();
    log("Waiting for master to be ready");
    assertTrue(cluster.waitForActiveAndReadyMaster());
    log("Master is ready");

    // Wait until SSH processing completed for dead server.
    while (master.getServerManager().areDeadServersInProgress()) {
      Thread.sleep(10);
    }

    // Failover should be completed, now wait for no RIT
    log("Waiting for no more RIT");
    ZKAssign.blockUntilNoRIT(zkw);
    log("No more RIT in ZK");
    long now = System.currentTimeMillis();
    long maxTime = 120000;
    boolean done = master.assignmentManager.waitUntilNoRegionsInTransition(maxTime);
    if (!done) {
      RegionStates regionStates = master.getAssignmentManager().getRegionStates();
      LOG.info("rit=" + regionStates.getRegionsInTransition());
    }
    long elapsed = System.currentTimeMillis() - now;
    assertTrue("Elapsed=" + elapsed + ", maxTime=" + maxTime + ", done=" + done,
      elapsed < maxTime);
    log("No more RIT in RIT map, doing final test verification");

    // Grab all the regions that are online across RSs
    Set<HRegionInfo> onlineRegions = new TreeSet<HRegionInfo>();
    now = System.currentTimeMillis();
    maxTime = 30000;
    for (JVMClusterUtil.RegionServerThread rst :
        cluster.getRegionServerThreads()) {
      try {
        HRegionServer rs = rst.getRegionServer();
        while (!rs.getRegionsInTransitionInRS().isEmpty()) {
          elapsed = System.currentTimeMillis() - now;
          assertTrue("Test timed out in getting online regions", elapsed < maxTime);
          if (rs.isAborted() || rs.isStopped()) {
            // This region server is stopped, skip it.
            break;
          }
          Thread.sleep(100);
        }
        onlineRegions.addAll(ProtobufUtil.getOnlineRegions(rs.getRSRpcServices()));
      } catch (RegionServerStoppedException e) {
        LOG.info("Got RegionServerStoppedException", e);
      }
    }

    // Now, everything that should be online should be online
    for (HRegionInfo hri : regionsThatShouldBeOnline) {
      assertTrue("region=" + hri.getRegionNameAsString() + ", " + onlineRegions.toString(),
        onlineRegions.contains(hri));
    }

    // Everything that should be offline should not be online
    for (HRegionInfo hri : regionsThatShouldBeOffline) {
      assertFalse(onlineRegions.contains(hri));
    }

    log("Done with verification, all passed, shutting down cluster");

    // Done, shutdown the cluster
    TEST_UTIL.shutdownMiniCluster();
  }
```
```
Cyclomatic Complexity	 5
Assertions		 2
Lines of Code		42
-   (timeout=180000)
  public void testShouldCheckMasterFailOverWhenMETAIsInOpenedState()
      throws Exception {
    LOG.info("Starting testShouldCheckMasterFailOverWhenMETAIsInOpenedState");
    final int NUM_MASTERS = 1;
    final int NUM_RS = 2;

    // Start the cluster
    HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setInt("hbase.master.info.port", -1);
    conf.setBoolean("hbase.assignment.usezk", true);

    TEST_UTIL.startMiniCluster(NUM_MASTERS, NUM_RS);
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();

    // Find regionserver carrying meta.
    List<RegionServerThread> regionServerThreads =
      cluster.getRegionServerThreads();
    Region metaRegion = null;
    HRegionServer metaRegionServer = null;
    for (RegionServerThread regionServerThread : regionServerThreads) {
      HRegionServer regionServer = regionServerThread.getRegionServer();
      metaRegion = regionServer.getOnlineRegion(HRegionInfo.FIRST_META_REGIONINFO.getRegionName());
      regionServer.abort("");
      if (null != metaRegion) {
        metaRegionServer = regionServer;
        break;
      }
    }

    assertNotNull(metaRegion);
    assertNotNull(metaRegionServer);

    TEST_UTIL.shutdownMiniHBaseCluster();

    // Create a ZKW to use in the test
    ZooKeeperWatcher zkw =
      HBaseTestingUtility.createAndForceNodeToOpenedState(TEST_UTIL,
          metaRegion, metaRegionServer.getServerName());

    LOG.info("Staring cluster for second time");
    TEST_UTIL.startMiniHBaseCluster(NUM_MASTERS, NUM_RS);

    HMaster master = TEST_UTIL.getHBaseCluster().getMaster();
    while (!master.isInitialized()) {
      Thread.sleep(100);
    }
    // Failover should be completed, now wait for no RIT
    log("Waiting for no more RIT");
    ZKAssign.blockUntilNoRIT(zkw);

    zkw.close();
    // Stop the cluster
    TEST_UTIL.shutdownMiniCluster();
  }
```
```
Cyclomatic Complexity	 7
Assertions		 6
Lines of Code		52
-  (timeout=240000)
  public void testOfflineRegionReAssginedAfterMasterRestart() throws Exception {
    final TableName table = TableName.valueOf("testOfflineRegionReAssginedAfterMasterRestart");
    final int NUM_MASTERS = 1;
    final int NUM_RS = 2;

    // Create config to use for this cluster
    Configuration conf = HBaseConfiguration.create();
    conf.setBoolean("hbase.assignment.usezk", true);

    // Start the cluster
    final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility(conf);
    TEST_UTIL.startMiniCluster(NUM_MASTERS, NUM_RS);
    log("Cluster started");

    TEST_UTIL.createTable(table, Bytes.toBytes("family"));
    HMaster master = TEST_UTIL.getHBaseCluster().getMaster();
    RegionStates regionStates = master.getAssignmentManager().getRegionStates();
    HRegionInfo hri = regionStates.getRegionsOfTable(table).get(0);
    ServerName serverName = regionStates.getRegionServerOfRegion(hri);
    TEST_UTIL.assertRegionOnServer(hri, serverName, 200);

    ServerName dstName = null;
    for (ServerName tmpServer : master.serverManager.getOnlineServers().keySet()) {
      if (!tmpServer.equals(serverName)) {
        dstName = tmpServer;
        break;
      }
    }
    // find a different server
    assertTrue(dstName != null);
    // shutdown HBase cluster
    TEST_UTIL.shutdownMiniHBaseCluster();
    // create a RIT node in offline state
    ZooKeeperWatcher zkw = TEST_UTIL.getZooKeeperWatcher();
    ZKAssign.createNodeOffline(zkw, hri, dstName);
    Stat stat = new Stat();
    byte[] data =
        ZKAssign.getDataNoWatch(zkw, hri.getEncodedName(), stat);
    assertTrue(data != null);
    RegionTransition rt = RegionTransition.parseFrom(data);
    assertTrue(rt.getEventType() == EventType.M_ZK_REGION_OFFLINE);

    LOG.info(hri.getEncodedName() + " region is in offline state with source server=" + serverName
        + " and dst server=" + dstName);

    // start HBase cluster
    TEST_UTIL.startMiniHBaseCluster(NUM_MASTERS, NUM_RS);

    while (true) {
      master = TEST_UTIL.getHBaseCluster().getMaster();
      if (master != null && master.isInitialized()) {
        ServerManager serverManager = master.getServerManager();
        if (!serverManager.areDeadServersInProgress()) {
          break;
        }
      }
      Thread.sleep(200);
    }

    // verify the region is assigned
    master = TEST_UTIL.getHBaseCluster().getMaster();
    master.getAssignmentManager().waitForAssignment(hri);
    regionStates = master.getAssignmentManager().getRegionStates();
    RegionState newState = regionStates.getRegionState(hri);
    assertTrue(newState.isOpened());
  }
```
```
Cyclomatic Complexity	 4
Assertions		 10
Lines of Code		82
-   (timeout=180000)
  @SuppressWarnings("deprecation")
  public void testPendingOpenOrCloseWhenMasterFailover() throws Exception {
    final int NUM_MASTERS = 1;
    final int NUM_RS = 1;

    // Create config to use for this cluster
    Configuration conf = HBaseConfiguration.create();
    conf.setBoolean("hbase.assignment.usezk", false);

    // Start the cluster
    HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility(conf);
    TEST_UTIL.startMiniCluster(NUM_MASTERS, NUM_RS);
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    log("Cluster started");

    // get all the master threads
    List<MasterThread> masterThreads = cluster.getMasterThreads();
    assertEquals(1, masterThreads.size());

    // only one master thread, let's wait for it to be initialized
    assertTrue(cluster.waitForActiveAndReadyMaster());
    HMaster master = masterThreads.get(0).getMaster();
    assertTrue(master.isActiveMaster());
    assertTrue(master.isInitialized());

    // Create a table with a region online
    Table onlineTable = TEST_UTIL.createTable(TableName.valueOf("onlineTable"), "family");
    onlineTable.close();
    // Create a table in META, so it has a region offline
    HTableDescriptor offlineTable = new HTableDescriptor(
      TableName.valueOf(Bytes.toBytes("offlineTable")));
    offlineTable.addFamily(new HColumnDescriptor(Bytes.toBytes("family")));

    FileSystem filesystem = FileSystem.get(conf);
    Path rootdir = FSUtils.getRootDir(conf);
    FSTableDescriptors fstd = new FSTableDescriptors(conf, filesystem, rootdir);
    fstd.createTableDescriptor(offlineTable);

    HRegionInfo hriOffline = new HRegionInfo(offlineTable.getTableName(), null, null);
    createRegion(hriOffline, rootdir, conf, offlineTable);
    MetaTableAccessor.addRegionToMeta(master.getConnection(), hriOffline);

    log("Regions in hbase:meta and namespace have been created");

    // at this point we only expect 3 regions to be assigned out
    // (catalogs and namespace, + 1 online region)
    assertEquals(3, cluster.countServedRegions());
    HRegionInfo hriOnline = null;
    try (RegionLocator locator =
        TEST_UTIL.getConnection().getRegionLocator(TableName.valueOf("onlineTable"))) {
      hriOnline = locator.getRegionLocation(HConstants.EMPTY_START_ROW).getRegionInfo();
    }
    RegionStates regionStates = master.getAssignmentManager().getRegionStates();
    RegionStateStore stateStore = master.getAssignmentManager().getRegionStateStore();

    // Put the online region in pending_close. It is actually already opened.
    // This is to simulate that the region close RPC is not sent out before failover
    RegionState oldState = regionStates.getRegionState(hriOnline);
    RegionState newState = new RegionState(
      hriOnline, State.PENDING_CLOSE, oldState.getServerName());
    stateStore.updateRegionState(HConstants.NO_SEQNUM, newState, oldState);

    // Put the offline region in pending_open. It is actually not opened yet.
    // This is to simulate that the region open RPC is not sent out before failover
    oldState = new RegionState(hriOffline, State.OFFLINE);
    newState = new RegionState(hriOffline, State.PENDING_OPEN, newState.getServerName());
    stateStore.updateRegionState(HConstants.NO_SEQNUM, newState, oldState);

    HRegionInfo failedClose = new HRegionInfo(offlineTable.getTableName(), null, null);
    createRegion(failedClose, rootdir, conf, offlineTable);
    MetaTableAccessor.addRegionToMeta(master.getConnection(), failedClose);

    oldState = new RegionState(failedClose, State.PENDING_CLOSE);
    newState = new RegionState(failedClose, State.FAILED_CLOSE, newState.getServerName());
    stateStore.updateRegionState(HConstants.NO_SEQNUM, newState, oldState);


    HRegionInfo failedOpen = new HRegionInfo(offlineTable.getTableName(), null, null);
    createRegion(failedOpen, rootdir, conf, offlineTable);
    MetaTableAccessor.addRegionToMeta(master.getConnection(), failedOpen);

    // Simulate a region transitioning to failed open when the region server reports the
    // transition as FAILED_OPEN
    oldState = new RegionState(failedOpen, State.PENDING_OPEN);
    newState = new RegionState(failedOpen, State.FAILED_OPEN, newState.getServerName());
    stateStore.updateRegionState(HConstants.NO_SEQNUM, newState, oldState);

    HRegionInfo failedOpenNullServer = new HRegionInfo(offlineTable.getTableName(), null, null);
    createRegion(failedOpenNullServer, rootdir, conf, offlineTable);
    MetaTableAccessor.addRegionToMeta(master.getConnection(), failedOpenNullServer);

    // Simulate a region transitioning to failed open when the master couldn't find a plan for
    // the region
    oldState = new RegionState(failedOpenNullServer, State.OFFLINE);
    newState = new RegionState(failedOpenNullServer, State.FAILED_OPEN, null);
    stateStore.updateRegionState(HConstants.NO_SEQNUM, newState, oldState);



    // Stop the master
    log("Aborting master");
    cluster.abortMaster(0);
    cluster.waitOnMaster(0);
    log("Master has aborted");

    // Start up a new master
    log("Starting up a new master");
    master = cluster.startMaster().getMaster();
    log("Waiting for master to be ready");
    cluster.waitForActiveAndReadyMaster();
    log("Master is ready");

    // Wait till no region in transition any more
    master.getAssignmentManager().waitUntilNoRegionsInTransition(60000);

    // Get new region states since master restarted
    regionStates = master.getAssignmentManager().getRegionStates();

    // Both pending_open (RPC sent/not yet) regions should be online
    assertTrue(regionStates.isRegionOnline(hriOffline));
    assertTrue(regionStates.isRegionOnline(hriOnline));
    assertTrue(regionStates.isRegionOnline(failedClose));
    assertTrue(regionStates.isRegionOnline(failedOpenNullServer));
    assertTrue(regionStates.isRegionOnline(failedOpen));

    log("Done with verification, shutting down cluster");

    // Done, shutdown the cluster
    TEST_UTIL.shutdownMiniCluster();
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		27
-  (timeout = 60000)
  public void testRollbackAndDoubleExecutionAfterPONR() throws Exception {
    final TableName tableName = TableName.valueOf("testRollbackAndDoubleExecutionAfterPONR");
    final String familyToAddName = "cf2";
    final String familyToRemove = "cf1";
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    // create the table
    HRegionInfo[] regions = MasterProcedureTestingUtility.createTable(
      procExec, tableName, null, familyToRemove);
    UTIL.getHBaseAdmin().disableTable(tableName);

    ProcedureTestingUtility.waitNoProcedureRunning(procExec);
    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, true);

    HTableDescriptor htd = new HTableDescriptor(UTIL.getHBaseAdmin().getTableDescriptor(tableName));
    htd.setCompactionEnabled(!htd.isCompactionEnabled());
    htd.addFamily(new HColumnDescriptor(familyToAddName));
    htd.removeFamily(familyToRemove.getBytes());
    htd.setRegionReplication(3);

    // Start the Modify procedure && kill the executor
    long procId = procExec.submitProcedure(
      new ModifyTableProcedure(procExec.getEnvironment(), htd));

    // Failing after MODIFY_TABLE_DELETE_FS_LAYOUT we should not trigger the rollback.
    // NOTE: the 5 (number of MODIFY_TABLE_DELETE_FS_LAYOUT + 1 step) is hardcoded,
    //       so you have to look at this test at least once when you add a new step.
    int numberOfSteps = 5;
    MasterProcedureTestingUtility.testRollbackAndDoubleExecutionAfterPONR(
      procExec,
      procId,
      numberOfSteps,
      ModifyTableState.values());

    // "cf2" should be added and "cf1" should be removed
    MasterProcedureTestingUtility.validateTableCreation(UTIL.getHBaseCluster().getMaster(),
      tableName, regions, false, familyToAddName);
  }
```
```
Cyclomatic Complexity	 10
Assertions		 6
Lines of Code		66
-  
  public void testBasicSplit() throws Exception {
    byte[][] families = { fam1, fam2, fam3 };

    Configuration hc = initSplit();
    // Setting up region
    String method = this.getName();
    this.region = initHRegion(tableName, method, hc, families);

    try {
      LOG.info("" + HBaseTestCase.addContent(region, fam3));
      region.flush(true);
      region.compactStores();
      byte[] splitRow = region.checkSplit();
      assertNotNull(splitRow);
      LOG.info("SplitRow: " + Bytes.toString(splitRow));
      HRegion[] regions = splitRegion(region, splitRow);
      try {
        // Need to open the regions.
        // TODO: Add an 'open' to HRegion... don't do open by constructing
        // instance.
        for (int i = 0; i < regions.length; i++) {
          regions[i] = HRegion.openHRegion(regions[i], null);
        }
        // Assert can get rows out of new regions. Should be able to get first
        // row from first region and the midkey from second region.
        assertGet(regions[0], fam3, Bytes.toBytes(START_KEY));
        assertGet(regions[1], fam3, splitRow);
        // Test I can get scanner and that it starts at right place.
        assertScan(regions[0], fam3, Bytes.toBytes(START_KEY));
        assertScan(regions[1], fam3, splitRow);
        // Now prove can't split regions that have references.
        for (int i = 0; i < regions.length; i++) {
          // Add so much data to this region, we create a store file that is >
          // than one of our unsplitable references. it will.
          for (int j = 0; j < 2; j++) {
            HBaseTestCase.addContent(regions[i], fam3);
          }
          HBaseTestCase.addContent(regions[i], fam2);
          HBaseTestCase.addContent(regions[i], fam1);
          regions[i].flush(true);
        }

        byte[][] midkeys = new byte[regions.length][];
        // To make regions splitable force compaction.
        for (int i = 0; i < regions.length; i++) {
          regions[i].compactStores();
          midkeys[i] = regions[i].checkSplit();
        }

        TreeMap<String, HRegion> sortedMap = new TreeMap<String, HRegion>();
        // Split these two daughter regions so then I'll have 4 regions. Will
        // split because added data above.
        for (int i = 0; i < regions.length; i++) {
          HRegion[] rs = null;
          if (midkeys[i] != null) {
            rs = splitRegion(regions[i], midkeys[i]);
            for (int j = 0; j < rs.length; j++) {
              sortedMap.put(Bytes.toString(rs[j].getRegionInfo().getRegionName()),
                HRegion.openHRegion(rs[j], null));
            }
          }
        }
        LOG.info("Made 4 regions");
        // The splits should have been even. Test I can get some arbitrary row
        // out of each.
        int interval = (LAST_CHAR - FIRST_CHAR) / 3;
        byte[] b = Bytes.toBytes(START_KEY);
        for (HRegion r : sortedMap.values()) {
          assertGet(r, fam3, b);
          b[0] += interval;
        }
      } finally {
        for (int i = 0; i < regions.length; i++) {
          try {
            regions[i].close();
          } catch (IOException e) {
            // Ignore.
          }
        }
      }
    } finally {
      HRegion.closeHRegion(this.region);
      this.region = null;
    }
  }
```
```
Cyclomatic Complexity	 2
Assertions		 3
Lines of Code		27
-  
  public void testSplitRegion() throws IOException {
    byte[] qualifier = Bytes.toBytes("qualifier");
    Configuration hc = initSplit();
    int numRows = 10;
    byte[][] families = { fam1, fam3 };

    // Setting up region
    String method = this.getName();
    this.region = initHRegion(tableName, method, hc, families);

    // Put data in region
    int startRow = 100;
    putData(startRow, numRows, qualifier, families);
    int splitRow = startRow + numRows;
    putData(splitRow, numRows, qualifier, families);
    region.flush(true);

    HRegion[] regions = null;
    try {
      regions = splitRegion(region, Bytes.toBytes("" + splitRow));
      // Opening the regions returned.
      for (int i = 0; i < regions.length; i++) {
        regions[i] = HRegion.openHRegion(regions[i], null);
      }
      // Verifying that the region has been split
      assertEquals(2, regions.length);

      // Verifying that all data is still there and that data is in the right
      // place
      verifyData(regions[0], startRow, numRows, qualifier, families);
      verifyData(regions[1], splitRow, numRows, qualifier, families);

    } finally {
      HRegion.closeHRegion(this.region);
      this.region = null;
    }
  }
```
```
Cyclomatic Complexity	 2
Assertions		 9
Lines of Code		43
-  
  public void testClearForceSplit() throws IOException {
    byte[] qualifier = Bytes.toBytes("qualifier");
    Configuration hc = initSplit();
    int numRows = 10;
    byte[][] families = { fam1, fam3 };

    // Setting up region
    String method = this.getName();
    this.region = initHRegion(tableName, method, hc, families);

    // Put data in region
    int startRow = 100;
    putData(startRow, numRows, qualifier, families);
    int splitRow = startRow + numRows;
    byte[] splitRowBytes = Bytes.toBytes("" + splitRow);
    putData(splitRow, numRows, qualifier, families);
    region.flush(true);

    HRegion[] regions = null;
    try {
      // Set force split
      region.forceSplit(splitRowBytes);
      assertTrue(region.shouldForceSplit());
      // Split point should be the force split row
      assertTrue(Bytes.equals(splitRowBytes, region.checkSplit()));

      // Add a store that has references.
      HStore storeMock = Mockito.mock(HStore.class);
      when(storeMock.hasReferences()).thenReturn(true);
      when(storeMock.getFamily()).thenReturn(new HColumnDescriptor("cf"));
      when(storeMock.close()).thenReturn(ImmutableList.<StoreFile>of());
      when(storeMock.getColumnFamilyName()).thenReturn("cf");
      region.stores.put(Bytes.toBytes(storeMock.getColumnFamilyName()), storeMock);
      assertTrue(region.hasReferences());

      // Will not split since the store has references.
      regions = splitRegion(region, splitRowBytes);
      assertNull(regions);

      // Region force split should be cleared after the split try.
      assertFalse(region.shouldForceSplit());

      // Remove the store that has references.
      region.stores.remove(Bytes.toBytes(storeMock.getColumnFamilyName()));
      assertFalse(region.hasReferences());

      // Now we can split.
      regions = splitRegion(region, splitRowBytes);

      // Opening the regions returned.
      for (int i = 0; i < regions.length; i++) {
        regions[i] = HRegion.openHRegion(regions[i], null);
      }
      // Verifying that the region has been split
      assertEquals(2, regions.length);

      // Verifying that all data is still there and that data is in the right
      // place
      verifyData(regions[0], startRow, numRows, qualifier, families);
      verifyData(regions[1], splitRow, numRows, qualifier, families);

    } finally {
      HRegion.closeHRegion(this.region);
      this.region = null;
    }@@@   }
```
```
Cyclomatic Complexity	 2
Assertions		 21
Lines of Code		61
-  
  public void testOpenRegionWrittenToWALForLogReplay() throws Exception {
    // similar to the above test but with distributed log replay
    final ServerName serverName = ServerName.valueOf("testOpenRegionWrittenToWALForLogReplay",
      100, 42);
    final RegionServerServices rss = spy(TEST_UTIL.createMockRegionServerService(serverName));

    HTableDescriptor htd
        = new HTableDescriptor(TableName.valueOf("testOpenRegionWrittenToWALForLogReplay"));
    htd.addFamily(new HColumnDescriptor(fam1));
    htd.addFamily(new HColumnDescriptor(fam2));

    HRegionInfo hri = new HRegionInfo(htd.getTableName(),
      HConstants.EMPTY_BYTE_ARRAY, HConstants.EMPTY_BYTE_ARRAY);

    // open the region w/o rss and wal and flush some files
    HRegion region = HRegion.createHRegion(hri, TEST_UTIL.getDataTestDir(), TEST_UTIL
             .getConfiguration(), htd);
    assertNotNull(region);

    // create a file in fam1 for the region before opening in OpenRegionHandler
    region.put(new Put(Bytes.toBytes("a")).add(fam1, fam1, fam1));
    region.flush(true);
    HRegion.closeHRegion(region);

    ArgumentCaptor<WALEdit> editCaptor = ArgumentCaptor.forClass(WALEdit.class);

    // capture append() calls
    WAL wal = mockWAL();
    when(rss.getWAL((HRegionInfo) any())).thenReturn(wal);

    // add the region to recovering regions
    HashMap<String, Region> recoveringRegions = Maps.newHashMap();
    recoveringRegions.put(region.getRegionInfo().getEncodedName(), null);
    when(rss.getRecoveringRegions()).thenReturn(recoveringRegions);

    try {
      Configuration conf = new Configuration(TEST_UTIL.getConfiguration());
      conf.set(HConstants.REGION_IMPL, HRegionWithSeqId.class.getName());
      region = HRegion.openHRegion(hri, htd, rss.getWAL(hri),
        conf, rss, null);

      // verify that we have not appended region open event to WAL because this region is still
      // recovering
      verify(wal, times(0)).append((HTableDescriptor)any(), (HRegionInfo)any(), (WALKey)any()
        , editCaptor.capture(), anyBoolean());

      // not put the region out of recovering state
      new FinishRegionRecoveringHandler(rss, region.getRegionInfo().getEncodedName(), "/foo")
        .prepare().process();

      // now we should have put the entry
      verify(wal, times(1)).append((HTableDescriptor)any(), (HRegionInfo)any(), (WALKey)any()
        , editCaptor.capture(), anyBoolean());

      WALEdit edit = editCaptor.getValue();
      assertNotNull(edit);
      assertNotNull(edit.getCells());
      assertEquals(1, edit.getCells().size());
      RegionEventDescriptor desc = WALEdit.getRegionEventDescriptor(edit.getCells().get(0));
      assertNotNull(desc);

      LOG.info("RegionEventDescriptor from WAL: " + desc);

      assertEquals(RegionEventDescriptor.EventType.REGION_OPEN, desc.getEventType());
      assertTrue(Bytes.equals(desc.getTableName().toByteArray(), htd.getName()));
      assertTrue(Bytes.equals(desc.getEncodedRegionName().toByteArray(),
        hri.getEncodedNameAsBytes()));
      assertTrue(desc.getLogSequenceNumber() > 0);
      assertEquals(serverName, ProtobufUtil.toServerName(desc.getServer()));
      assertEquals(2, desc.getStoresCount());

      StoreDescriptor store = desc.getStores(0);
      assertTrue(Bytes.equals(store.getFamilyName().toByteArray(), fam1));
      assertEquals(store.getStoreHomeDir(), Bytes.toString(fam1));
      assertEquals(1, store.getStoreFileCount()); // 1store file
      assertFalse(store.getStoreFile(0).contains("/")); // ensure path is relative

      store = desc.getStores(1);
      assertTrue(Bytes.equals(store.getFamilyName().toByteArray(), fam2));
      assertEquals(store.getStoreHomeDir(), Bytes.toString(fam2));
      assertEquals(0, store.getStoreFileCount()); // no store files

    } finally {
      HRegion.closeHRegion(region);
    }
  }
```
```
Cyclomatic Complexity	 2
Assertions		 4
Lines of Code		43
-  
  public void testWhenL2BlockCacheIsOnHeap() throws Exception {
    HeapMemoryManager heapMemoryManager = null;
    BlockCacheStub blockCache = new BlockCacheStub((long) (maxHeapSize * 0.4));
    MemstoreFlusherStub memStoreFlusher = new MemstoreFlusherStub((long) (maxHeapSize * 0.3));
    Configuration conf = HBaseConfiguration.create();
    conf.setFloat(HeapMemoryManager.MEMSTORE_SIZE_MAX_RANGE_KEY, 0.7f);
    conf.setFloat(HeapMemoryManager.MEMSTORE_SIZE_MIN_RANGE_KEY, 0.1f);
    conf.setFloat(HeapMemoryManager.BLOCK_CACHE_SIZE_MAX_RANGE_KEY, 0.7f);
    conf.setFloat(HeapMemoryManager.BLOCK_CACHE_SIZE_MIN_RANGE_KEY, 0.1f);
    conf.setInt(DefaultHeapMemoryTuner.NUM_PERIODS_TO_IGNORE, 0);
    conf.setFloat(HeapMemorySizeUtil.MEMSTORE_SIZE_KEY, 0.4F);
    conf.setFloat(HConstants.HFILE_BLOCK_CACHE_SIZE_KEY, 0.3F);
    conf.setFloat(HConstants.BUCKET_CACHE_SIZE_KEY, 0.1F);
    conf.set(HConstants.BUCKET_CACHE_IOENGINE_KEY, "heap");

    conf.setLong(HeapMemoryManager.HBASE_RS_HEAP_MEMORY_TUNER_PERIOD, 1000);
    conf.setClass(HeapMemoryManager.HBASE_RS_HEAP_MEMORY_TUNER_CLASS, CustomHeapMemoryTuner.class,
        HeapMemoryTuner.class);

    try {
      heapMemoryManager = new HeapMemoryManager(blockCache, memStoreFlusher,
          new RegionServerStub(conf), new RegionServerAccountingStub());
      fail("Should have failed as the collective heap memory need is above 80%");
    } catch (Exception e) {
    }

    // Change the max/min ranges for memstore and bock cache so as to pass the criteria check
    conf.setFloat(HeapMemoryManager.MEMSTORE_SIZE_MAX_RANGE_KEY, 0.6f);
    conf.setFloat(HeapMemoryManager.BLOCK_CACHE_SIZE_MAX_RANGE_KEY, 0.6f);
    heapMemoryManager = new HeapMemoryManager(blockCache, memStoreFlusher,
        new RegionServerStub(conf), new RegionServerAccountingStub());
    long oldMemstoreSize = memStoreFlusher.memstoreSize;
    long oldBlockCacheSize = blockCache.maxSize;
    final ChoreService choreService = new ChoreService("TEST_SERVER_NAME");
    heapMemoryManager.start(choreService);
    CustomHeapMemoryTuner.memstoreSize = 0.4f;
    CustomHeapMemoryTuner.blockCacheSize = 0.4f;
    // Allow the tuner to run once and do necessary memory up
   Thread.sleep(1500);
    // The size should not get changes as the collection of memstore size and L1 and L2 block cache
    // size will cross the ax allowed 80% mark
    assertEquals(oldMemstoreSize, memStoreFlusher.memstoreSize);
    assertEquals(oldBlockCacheSize, blockCache.maxSize);
    CustomHeapMemoryTuner.memstoreSize = 0.1f;
    CustomHeapMemoryTuner.blockCacheSize = 0.5f;
    // Allow the tuner to run once and do necessary memory up
    waitForTune(memStoreFlusher, memStoreFlusher.memstoreSize);
    assertHeapSpace(0.1f, memStoreFlusher.memstoreSize);
    assertHeapSpace(0.5f, blockCache.maxSize);
  }
```
```
Cyclomatic Complexity	 -1
Assertions		 0
Lines of Code		5
-12751") (timeout = 180000)
  public void testLogReplayWithDistributedReplay() throws Exception {
    TEST_UTIL.getConfiguration().setBoolean(HConstants.DISTRIBUTED_LOG_REPLAY_KEY, true);
    doTestLogReplay();
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		17
-  
  public void testNoEdits() throws Exception {
    TableName tableName = TableName.valueOf("TestLogRollPeriodNoEdits");
    TEST_UTIL.createTable(tableName, "cf");
    try {
      Table table = new HTable(TEST_UTIL.getConfiguration(), tableName);
      try {
        HRegionServer server = TEST_UTIL.getRSForFirstRegionInTable(tableName);
        WAL log = server.getWAL(null);
        checkMinLogRolls(log, 5);
      } finally {
        table.close();
      }
    } finally {
      TEST_UTIL.deleteTable(tableName);
    }
  }
```
```
Cyclomatic Complexity	 2
Assertions		 0
Lines of Code		38
-  (timeout=60000)
  public void testWithEdits() throws Exception {
    final TableName tableName = TableName.valueOf("TestLogRollPeriodWithEdits");
    final String family = "cf";

    TEST_UTIL.createTable(tableName, family);
    try {
      HRegionServer server = TEST_UTIL.getRSForFirstRegionInTable(tableName);
      WAL log = server.getWAL(null);
      final Table table = new HTable(TEST_UTIL.getConfiguration(), tableName);

      Thread writerThread = new Thread("writer") {
        @Override
        public void run() {
          try {
            long row = 0;
            while (!interrupted()) {
              Put p = new Put(Bytes.toBytes(String.format("row%d", row)));
              p.add(Bytes.toBytes(family), Bytes.toBytes("col"), Bytes.toBytes(row));
              table.put(p);
              row++;

              Thread.sleep(LOG_ROLL_PERIOD / 16);
            }
          } catch (Exception e) {
            LOG.warn(e);
          } 
        }
      };

      try {
        writerThread.start();
        checkMinLogRolls(log, 5);
      } finally {
        writerThread.interrupt();
        writerThread.join();
        table.close();
      }  
    } finally {
      TEST_UTIL.deleteTable(tableName);
    }
  }
```
```
Cyclomatic Complexity	 6
Assertions		 3
Lines of Code		36
-   (timeout = 240000)
  public void testReplayedEditsAreSkipped() throws Exception {
    openRegion(HTU, rs0, hriSecondary);
    ClusterConnection connection =
        (ClusterConnection) ConnectionFactory.createConnection(HTU.getConfiguration());
    RegionReplicaReplicationEndpoint replicator = new RegionReplicaReplicationEndpoint();

    ReplicationEndpoint.Context context = mock(ReplicationEndpoint.Context.class);
    when(context.getConfiguration()).thenReturn(HTU.getConfiguration());
    when(context.getMetrics()).thenReturn(mock(MetricsSource.class));

    ReplicationPeer mockPeer = mock(ReplicationPeer.class);
    when(mockPeer.getTableCFs()).thenReturn(null);
    when(context.getReplicationPeer()).thenReturn(mockPeer);

    replicator.init(context);
    replicator.start();

    // test the filter for the RE, not actual replication
    WALEntryFilter filter = replicator.getWALEntryfilter();

    //load some data to primary
    HTU.loadNumericRows(table, f, 0, 1000);

    Assert.assertEquals(1000, entries.size());
    for (Entry e: entries) {
      if (Integer.parseInt(Bytes.toString(e.getEdit().getCells().get(0).getValue())) % 2 == 0) {
        e.getKey().setOrigLogSeqNum(1); // simulate dist log replay by setting orig seq id
      }
    }

    long skipped = 0, replayed = 0;
    for (Entry e : entries) {
      if (filter.filter(e) == null) {
        skipped++;
      } else {
        replayed++;
      }
    }

    assertEquals(500, skipped);
    assertEquals(500, replayed);

    HTU.deleteNumericRows(table, f, 0, 1000);
    closeRegion(HTU, rs0, hriSecondary);
    connection.close();
  }
```
```
Cyclomatic Complexity	 8
Assertions		 8
Lines of Code		60
-  
  public void testReadLegacyLog() throws IOException {
    final int columnCount = 5;
    final int recordCount = 5;
    final TableName tableName =
        TableName.valueOf("tablename");
    final byte[] row = Bytes.toBytes("row");
    long timestamp = System.currentTimeMillis();
    Path path = new Path(dir, "tempwal");
    SequenceFileLogWriter sflw = null;
    WAL.Reader reader = null;
    try {
      HRegionInfo hri = new HRegionInfo(tableName,
          HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW);
      HTableDescriptor htd = new HTableDescriptor(tableName);
      fs.mkdirs(dir);
      // Write log in pre-PB format.
      sflw = new SequenceFileLogWriter();
      sflw.init(fs, path, conf, false);
      for (int i = 0; i < recordCount; ++i) {
        WALKey key = new HLogKey(
            hri.getEncodedNameAsBytes(), tableName, i, timestamp, HConstants.DEFAULT_CLUSTER_ID);
        WALEdit edit = new WALEdit();
        for (int j = 0; j < columnCount; ++j) {
          if (i == 0) {
            htd.addFamily(new HColumnDescriptor("column" + j));
          }
          String value = i + "" + j;
          edit.add(new KeyValue(row, row, row, timestamp, Bytes.toBytes(value)));
        }
        sflw.append(new WAL.Entry(key, edit));
      }
      sflw.sync();
      sflw.close();

      // Now read the log using standard means.
      reader = wals.createReader(fs, path);
      assertTrue(reader instanceof SequenceFileLogReader);
      for (int i = 0; i < recordCount; ++i) {
        WAL.Entry entry = reader.next();
        assertNotNull(entry);
        assertEquals(columnCount, entry.getEdit().size());
        assertArrayEquals(hri.getEncodedNameAsBytes(), entry.getKey().getEncodedRegionName());
        assertEquals(tableName, entry.getKey().getTablename());
        int idx = 0;
        for (Cell val : entry.getEdit().getCells()) {
          assertTrue(Bytes.equals(row, val.getRow()));
          String value = i + "" + idx;
          assertArrayEquals(Bytes.toBytes(value), val.getValue());
          idx++;
        }
      }
      WAL.Entry entry = reader.next();
      assertNull(entry);
    } finally {
      if (sflw != null) {
        sflw.close();
      }
      if (reader != null) {
        reader.close();
      }
    }
  }
```
## 16116fa35e2e4a8e1728b3f31dd4dbb06cbd3ba5 ##
```
Cyclomatic Complexity	1
Assertions		0
Lines of Code		4
-    
    public void run1() throws InterruptedException {
        Thread.sleep(100);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 4
Lines of Code		14
-  
  public void testErrorNotGzipped() throws Exception {
    Header[] headers = new Header[2];
    headers[0] = new Header("Accept", Constants.MIMETYPE_BINARY);
    headers[1] = new Header("Accept-Encoding", "gzip");
    Response response = client.get("/" + TABLE + "/" + ROW_1 + "/" + COLUMN_2, headers);
    assertEquals(404, response.getCode());
    String contentEncoding = response.getHeader("Content-Encoding");
    assertTrue(contentEncoding == null || !contentEncoding.contains("gzip"));
    response = client.get("/" + TABLE, headers);
    assertEquals(405, response.getCode());
    contentEncoding = response.getHeader("Content-Encoding");
    assertTrue(contentEncoding == null || !contentEncoding.contains("gzip"));
  }
```
```
Cyclomatic Complexity	 1
Assertions		 6
Lines of Code		11
-  
  public void testValueOfNamespaceAndQualifier() {
    TableName name0 = TableName.valueOf("table");
    TableName name1 = TableName.valueOf("table", "table");
    assertEquals(NamespaceDescriptor.DEFAULT_NAMESPACE_NAME_STR, name0.getNamespaceAsString());
    assertEquals("table", name0.getQualifierAsString());
    assertEquals("table", name0.getNameAsString());
    assertEquals("table", name1.getNamespaceAsString());
    assertEquals("table", name1.getQualifierAsString());
    assertEquals("table:table", name1.getNameAsString());
  }
```
```
Cyclomatic Complexity	 1
Assertions		 12
Lines of Code		186
-  
  public void testVersions() throws Exception {
    byte [] TABLE = Bytes.toBytes("testVersions");@@@+      // Verify we can get each one properly@@@+      getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);@@@+      getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);@@@+      getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);@@@+      getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[7], VALUES[7]);@@@+      scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);@@@+      scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);@@@+      scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);@@@+      scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[7], VALUES[7]);@@@ 
    long [] STAMPS = makeStamps(20);
    byte [][] VALUES = makeNAscii(VALUE, 20);@@@+      // Verify we don't accidentally get others@@@+      getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[0]);@@@+      getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[9]);@@@+      scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[0]);@@@+      scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[9]);@@@ 
    Table ht = TEST_UTIL.createTable(TABLE, FAMILY, 10);
    TEST_UTIL.waitTableAvailable(TABLE, 10000);
    // Insert 4 versions of same column
    Put put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    put.add(FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    put.add(FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    put.add(FAMILY, QUALIFIER, STAMPS[5], VALUES[5]);
    ht.put(put);@@@+      // Ensure maxVersions of table is respected@@@ 
    // Verify we can get each one properly
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[5], VALUES[5]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[5], VALUES[5]);@@@+      TEST_UTIL.flush();@@@ 
    // Verify we don't accidentally get others
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[0]);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[3]);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[6]);
    scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[0]);
    scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[3]);
    scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[6]);

    // Ensure maxVersions in query is respected
    Get get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIER);
    get.setMaxVersions(2);
    Result result = ht.get(get);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[4], STAMPS[5]},
        new byte[][] {VALUES[4], VALUES[5]},
        0, 1);

    Scan scan = new Scan(ROW);
    scan.addColumn(FAMILY, QUALIFIER);
    scan.setMaxVersions(2);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[4], STAMPS[5]},
        new byte[][] {VALUES[4], VALUES[5]},
        0, 1);

    // Flush and redo

    TEST_UTIL.flush();

    // Verify we can get each one properly
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[5], VALUES[5]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[5], VALUES[5]);

    // Verify we don't accidentally get others
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[0]);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[3]);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[6]);
    scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[0]);
    scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[3]);
    scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[6]);

    // Ensure maxVersions in query is respected
    get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIER);
    get.setMaxVersions(2);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[4], STAMPS[5]},
        new byte[][] {VALUES[4], VALUES[5]},
        0, 1);

    scan = new Scan(ROW);
    scan.addColumn(FAMILY, QUALIFIER);
    scan.setMaxVersions(2);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[4], STAMPS[5]},
        new byte[][] {VALUES[4], VALUES[5]},
        0, 1);


    // Add some memstore and retest

    // Insert 4 more versions of same column and a dupe
    put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, STAMPS[3], VALUES[3]);
    put.add(FAMILY, QUALIFIER, STAMPS[6], VALUES[6]);
    put.add(FAMILY, QUALIFIER, STAMPS[7], VALUES[7]);
    put.add(FAMILY, QUALIFIER, STAMPS[8], VALUES[8]);
    ht.put(put);

    // Ensure maxVersions in query is respected
    get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIER);
    get.setMaxVersions();
    result = ht.get(get);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[1], STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7], STAMPS[8]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3], VALUES[4], VALUES[5], VALUES[6], VALUES[7], VALUES[8]},
        0, 7);

    scan = new Scan(ROW);
    scan.addColumn(FAMILY, QUALIFIER);
    scan.setMaxVersions();
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[1], STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7], STAMPS[8]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3], VALUES[4], VALUES[5], VALUES[6], VALUES[7], VALUES[8]},
        0, 7);

    get = new Get(ROW);
    get.setMaxVersions();
    result = ht.get(get);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[1], STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7], STAMPS[8]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3], VALUES[4], VALUES[5], VALUES[6], VALUES[7], VALUES[8]},
        0, 7);

    scan = new Scan(ROW);
    scan.setMaxVersions();
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[1], STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7], STAMPS[8]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3], VALUES[4], VALUES[5], VALUES[6], VALUES[7], VALUES[8]},
        0, 7);

    // Verify we can get each one properly
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[7], VALUES[7]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[7], VALUES[7]);

    // Verify we don't accidentally get others
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[0]);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[9]);
    scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[0]);
    scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[9]);

    // Ensure maxVersions of table is respected

    TEST_UTIL.flush();

    // Insert 4 more versions of same column and a dupe
    put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, STAMPS[9], VALUES[9]);
    put.add(FAMILY, QUALIFIER, STAMPS[11], VALUES[11]);
    put.add(FAMILY, QUALIFIER, STAMPS[13], VALUES[13]);
    put.add(FAMILY, QUALIFIER, STAMPS[15], VALUES[15]);
    ht.put(put);

    get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIER);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7], STAMPS[8], STAMPS[9], STAMPS[11], STAMPS[13], STAMPS[15]},
        new byte[][] {VALUES[3], VALUES[4], VALUES[5], VALUES[6], VALUES[7], VALUES[8], VALUES[9], VALUES[11], VALUES[13], VALUES[15]},
        0, 9);

    scan = new Scan(ROW);
    scan.addColumn(FAMILY, QUALIFIER);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7], STAMPS[8], STAMPS[9], STAMPS[11], STAMPS[13], STAMPS[15]},
        new byte[][] {VALUES[3], VALUES[4], VALUES[5], VALUES[6], VALUES[7], VALUES[8], VALUES[9], VALUES[11], VALUES[13], VALUES[15]},
        0, 9);

    // Delete a version in the memstore and a version in a storefile
    Delete delete = new Delete(ROW);
    delete.deleteColumn(FAMILY, QUALIFIER, STAMPS[11]);
    delete.deleteColumn(FAMILY, QUALIFIER, STAMPS[7]);
    ht.delete(delete);

    // Test that it's gone
    get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIER);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[1], STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[8], STAMPS[9], STAMPS[13], STAMPS[15]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3], VALUES[4], VALUES[5], VALUES[6], VALUES[8], VALUES[9], VALUES[13], VALUES[15]},
        0, 9);

    scan = new Scan(ROW);
    scan.addColumn(FAMILY, QUALIFIER);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[1], STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[8], STAMPS[9], STAMPS[13], STAMPS[15]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3], VALUES[4], VALUES[5], VALUES[6], VALUES[8], VALUES[9], VALUES[13], VALUES[15]},
        0, 9);

  }
```
```
Cyclomatic Complexity	 1
Assertions		 18
Lines of Code		163
-  
  public void testVersionLimits() throws Exception {
    byte [] TABLE = Bytes.toBytes("testVersionLimits");
    byte [][] FAMILIES = makeNAscii(FAMILY, 3);
    int [] LIMITS = {1,3,5};
    long [] STAMPS = makeStamps(10);
    byte [][] VALUES = makeNAscii(VALUE, 10);
    Table ht = TEST_UTIL.createTable(TABLE, FAMILIES, LIMITS);
    TEST_UTIL.waitTableAvailable(TABLE, 10000);
    // Insert limit + 1 on each family
    Put put = new Put(ROW);
    put.add(FAMILIES[0], QUALIFIER, STAMPS[0], VALUES[0]);
    put.add(FAMILIES[0], QUALIFIER, STAMPS[1], VALUES[1]);
    put.add(FAMILIES[1], QUALIFIER, STAMPS[0], VALUES[0]);
    put.add(FAMILIES[1], QUALIFIER, STAMPS[1], VALUES[1]);
    put.add(FAMILIES[1], QUALIFIER, STAMPS[2], VALUES[2]);
    put.add(FAMILIES[1], QUALIFIER, STAMPS[3], VALUES[3]);
    put.add(FAMILIES[2], QUALIFIER, STAMPS[0], VALUES[0]);
    put.add(FAMILIES[2], QUALIFIER, STAMPS[1], VALUES[1]);
    put.add(FAMILIES[2], QUALIFIER, STAMPS[2], VALUES[2]);
    put.add(FAMILIES[2], QUALIFIER, STAMPS[3], VALUES[3]);
    put.add(FAMILIES[2], QUALIFIER, STAMPS[4], VALUES[4]);
    put.add(FAMILIES[2], QUALIFIER, STAMPS[5], VALUES[5]);
    put.add(FAMILIES[2], QUALIFIER, STAMPS[6], VALUES[6]);
    ht.put(put);

    // Verify we only get the right number out of each

    // Family0

    Get get = new Get(ROW);
    get.addColumn(FAMILIES[0], QUALIFIER);
    get.setMaxVersions(Integer.MAX_VALUE);
    Result result = ht.get(get);
    assertNResult(result, ROW, FAMILIES[0], QUALIFIER,
        new long [] {STAMPS[1]},
        new byte[][] {VALUES[1]},
        0, 0);

    get = new Get(ROW);
    get.addFamily(FAMILIES[0]);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILIES[0], QUALIFIER,
        new long [] {STAMPS[1]},
        new byte[][] {VALUES[1]},
        0, 0);

    Scan scan = new Scan(ROW);
    scan.addColumn(FAMILIES[0], QUALIFIER);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILIES[0], QUALIFIER,
        new long [] {STAMPS[1]},
        new byte[][] {VALUES[1]},
        0, 0);

    scan = new Scan(ROW);
    scan.addFamily(FAMILIES[0]);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILIES[0], QUALIFIER,
        new long [] {STAMPS[1]},
        new byte[][] {VALUES[1]},
        0, 0);

    // Family1

    get = new Get(ROW);
    get.addColumn(FAMILIES[1], QUALIFIER);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILIES[1], QUALIFIER,
        new long [] {STAMPS[1], STAMPS[2], STAMPS[3]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3]},
        0, 2);

    get = new Get(ROW);
    get.addFamily(FAMILIES[1]);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILIES[1], QUALIFIER,
        new long [] {STAMPS[1], STAMPS[2], STAMPS[3]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3]},
        0, 2);

    scan = new Scan(ROW);
    scan.addColumn(FAMILIES[1], QUALIFIER);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILIES[1], QUALIFIER,
        new long [] {STAMPS[1], STAMPS[2], STAMPS[3]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3]},
        0, 2);

    scan = new Scan(ROW);
    scan.addFamily(FAMILIES[1]);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILIES[1], QUALIFIER,
        new long [] {STAMPS[1], STAMPS[2], STAMPS[3]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3]},
        0, 2);

    // Family2

    get = new Get(ROW);
    get.addColumn(FAMILIES[2], QUALIFIER);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILIES[2], QUALIFIER,
        new long [] {STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6]},
        new byte[][] {VALUES[2], VALUES[3], VALUES[4], VALUES[5], VALUES[6]},
        0, 4);

    get = new Get(ROW);
    get.addFamily(FAMILIES[2]);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILIES[2], QUALIFIER,
        new long [] {STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6]},
        new byte[][] {VALUES[2], VALUES[3], VALUES[4], VALUES[5], VALUES[6]},
        0, 4);

    scan = new Scan(ROW);
    scan.addColumn(FAMILIES[2], QUALIFIER);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILIES[2], QUALIFIER,
        new long [] {STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6]},
        new byte[][] {VALUES[2], VALUES[3], VALUES[4], VALUES[5], VALUES[6]},
        0, 4);

    scan = new Scan(ROW);
    scan.addFamily(FAMILIES[2]);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILIES[2], QUALIFIER,
        new long [] {STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6]},
        new byte[][] {VALUES[2], VALUES[3], VALUES[4], VALUES[5], VALUES[6]},
        0, 4);

    // Try all families

    get = new Get(ROW);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertTrue("Expected 9 keys but received " + result.size(),
        result.size() == 9);

    get = new Get(ROW);
    get.addFamily(FAMILIES[0]);
    get.addFamily(FAMILIES[1]);
    get.addFamily(FAMILIES[2]);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertTrue("Expected 9 keys but received " + result.size(),
        result.size() == 9);

    get = new Get(ROW);
    get.addColumn(FAMILIES[0], QUALIFIER);
    get.addColumn(FAMILIES[1], QUALIFIER);
    get.addColumn(FAMILIES[2], QUALIFIER);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertTrue("Expected 9 keys but received " + result.size(),
        result.size() == 9);

    scan = new Scan(ROW);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertTrue("Expected 9 keys but received " + result.size(),
        result.size() == 9);

    scan = new Scan(ROW);
    scan.setMaxVersions(Integer.MAX_VALUE);
    scan.addFamily(FAMILIES[0]);
    scan.addFamily(FAMILIES[1]);
    scan.addFamily(FAMILIES[2]);
    result = getSingleScanResult(ht, scan);
    assertTrue("Expected 9 keys but received " + result.size(),
        result.size() == 9);

    scan = new Scan(ROW);
    scan.setMaxVersions(Integer.MAX_VALUE);
    scan.addColumn(FAMILIES[0], QUALIFIER);
    scan.addColumn(FAMILIES[1], QUALIFIER);
    scan.addColumn(FAMILIES[2], QUALIFIER);
    result = getSingleScanResult(ht, scan);
    assertTrue("Expected 9 keys but received " + result.size(),
        result.size() == 9);

  }
```
```
Cyclomatic Complexity	 5
Assertions		 2
Lines of Code		33
-  
  public void testDeleteFamilyVersion() throws Exception {
    HBaseAdmin admin = new HBaseAdmin(TEST_UTIL.getConfiguration());
    byte [] TABLE = Bytes.toBytes("testDeleteFamilyVersion");

    byte [][] QUALIFIERS = makeNAscii(QUALIFIER, 1);
    byte [][] VALUES = makeN(VALUE, 5);
    long [] ts = {1000, 2000, 3000, 4000, 5000};

    Table ht = TEST_UTIL.createTable(TABLE, FAMILY, 5);
    TEST_UTIL.waitTableAvailable(TABLE, 10000);

    Put put = new Put(ROW);
    for (int q = 0; q < 1; q++)
      for (int t = 0; t < 5; t++)
        put.add(FAMILY, QUALIFIERS[q], ts[t], VALUES[t]);
    ht.put(put);
    admin.flush(TABLE);

    Delete delete = new Delete(ROW);
    delete.deleteFamilyVersion(FAMILY, ts[1]);  // delete version '2000'
    delete.deleteFamilyVersion(FAMILY, ts[3]);  // delete version '4000'
    ht.delete(delete);
    admin.flush(TABLE);

    for (int i = 0; i < 1; i++) {
      Get get = new Get(ROW);
      get.addColumn(FAMILY, QUALIFIERS[i]);
      get.setMaxVersions(Integer.MAX_VALUE);
      Result result = ht.get(get);
      // verify version '1000'/'3000'/'5000' remains for all columns
      assertNResult(result, ROW, FAMILY, QUALIFIERS[i],
          new long [] {ts[0], ts[2], ts[4]},
          new byte[][] {VALUES[0], VALUES[2], VALUES[4]},
          0, 2);
    }
    ht.close();
    admin.close();
  }
```
```
Cyclomatic Complexity	 7
Assertions		 7
Lines of Code		89
-  
  public void testDeleteFamilyVersionWithOtherDeletes() throws Exception {
    byte [] TABLE = Bytes.toBytes("testDeleteFamilyVersionWithOtherDeletes");

    byte [][] QUALIFIERS = makeNAscii(QUALIFIER, 5);
    byte [][] VALUES = makeN(VALUE, 5);
    long [] ts = {1000, 2000, 3000, 4000, 5000};

    HBaseAdmin admin = new HBaseAdmin(TEST_UTIL.getConfiguration());
    Table ht = TEST_UTIL.createTable(TABLE, FAMILY, 5);
    TEST_UTIL.waitTableAvailable(TABLE, 10000);

    Put put = null;
    Result result = null;
    Get get = null;
    Delete delete = null;

    // 1. put on ROW
    put = new Put(ROW);
    for (int q = 0; q < 5; q++)
      for (int t = 0; t < 5; t++)
        put.add(FAMILY, QUALIFIERS[q], ts[t], VALUES[t]);
    ht.put(put);
    admin.flush(TABLE);

    // 2. put on ROWS[0]
    byte [] ROW2 = Bytes.toBytes("myRowForTest");
    put = new Put(ROW2);
    for (int q = 0; q < 5; q++)
      for (int t = 0; t < 5; t++)
        put.add(FAMILY, QUALIFIERS[q], ts[t], VALUES[t]);
    ht.put(put);
    admin.flush(TABLE);

    // 3. delete on ROW
    delete = new Delete(ROW);
    // delete version <= 2000 of all columns
    // note: deleteFamily must be the first since it will mask
    // the subsequent other type deletes!
    delete.deleteFamily(FAMILY, ts[1]);
    // delete version '4000' of all columns
    delete.deleteFamilyVersion(FAMILY, ts[3]);
   // delete version <= 3000 of column 0
    delete.deleteColumns(FAMILY, QUALIFIERS[0], ts[2]);
    // delete version <= 5000 of column 2
    delete.deleteColumns(FAMILY, QUALIFIERS[2], ts[4]);
    // delete version 5000 of column 4
    delete.deleteColumn(FAMILY, QUALIFIERS[4], ts[4]);
    ht.delete(delete);
    admin.flush(TABLE);

     // 4. delete on ROWS[0]
    delete = new Delete(ROW2);
    delete.deleteFamilyVersion(FAMILY, ts[1]);  // delete version '2000'
    delete.deleteFamilyVersion(FAMILY, ts[3]);  // delete version '4000'
    ht.delete(delete);
    admin.flush(TABLE);

    // 5. check ROW
    get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIERS[0]);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILY, QUALIFIERS[0],
        new long [] {ts[4]},
        new byte[][] {VALUES[4]},
        0, 0);

    get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIERS[1]);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILY, QUALIFIERS[1],
        new long [] {ts[2], ts[4]},
        new byte[][] {VALUES[2], VALUES[4]},
        0, 1);

    get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIERS[2]);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertEquals(0, result.size());

    get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIERS[3]);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILY, QUALIFIERS[3],
        new long [] {ts[2], ts[4]},
        new byte[][] {VALUES[2], VALUES[4]},
        0, 1);

    get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIERS[4]);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILY, QUALIFIERS[4],
        new long [] {ts[2]},
        new byte[][] {VALUES[2]},
        0, 0);

    // 6. check ROWS[0]
    for (int i = 0; i < 5; i++) {
      get = new Get(ROW2);
      get.addColumn(FAMILY, QUALIFIERS[i]);
      get.setMaxVersions(Integer.MAX_VALUE);
      result = ht.get(get);
      // verify version '1000'/'3000'/'5000' remains for all columns
      assertNResult(result, ROW2, FAMILY, QUALIFIERS[i],
          new long [] {ts[0], ts[2], ts[4]},
          new byte[][] {VALUES[0], VALUES[2], VALUES[4]},
          0, 2);
    }
    ht.close();
    admin.close();
  }
```
```
Cyclomatic Complexity	 10
Assertions		 29
Lines of Code		259
-  
  public void testDeletes() throws Exception {
    byte [] TABLE = Bytes.toBytes("testDeletes");

    byte [][] ROWS = makeNAscii(ROW, 6);
    byte [][] FAMILIES = makeNAscii(FAMILY, 3);
    byte [][] VALUES = makeN(VALUE, 5);
    long [] ts = {1000, 2000, 3000, 4000, 5000};

    Table ht = TEST_UTIL.createTable(TABLE, FAMILIES, 3);
    TEST_UTIL.waitTableAvailable(TABLE, 10000);

    Put put = new Put(ROW);
    put.add(FAMILIES[0], QUALIFIER, ts[0], VALUES[0]);
    put.add(FAMILIES[0], QUALIFIER, ts[1], VALUES[1]);
    ht.put(put);

    Delete delete = new Delete(ROW);
    delete.deleteFamily(FAMILIES[0], ts[0]);
    ht.delete(delete);

    Get get = new Get(ROW);
    get.addFamily(FAMILIES[0]);
    get.setMaxVersions(Integer.MAX_VALUE);
    Result result = ht.get(get);
    assertNResult(result, ROW, FAMILIES[0], QUALIFIER,
        new long [] {ts[1]},
        new byte[][] {VALUES[1]},
        0, 0);

    Scan scan = new Scan(ROW);
    scan.addFamily(FAMILIES[0]);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILIES[0], QUALIFIER,
        new long [] {ts[1]},
        new byte[][] {VALUES[1]},
        0, 0);

    // Test delete latest version
    put = new Put(ROW);
    put.add(FAMILIES[0], QUALIFIER, ts[4], VALUES[4]);
    put.add(FAMILIES[0], QUALIFIER, ts[2], VALUES[2]);
    put.add(FAMILIES[0], QUALIFIER, ts[3], VALUES[3]);
    put.add(FAMILIES[0], null, ts[4], VALUES[4]);
    put.add(FAMILIES[0], null, ts[2], VALUES[2]);
    put.add(FAMILIES[0], null, ts[3], VALUES[3]);
    ht.put(put);

    delete = new Delete(ROW);
    delete.deleteColumn(FAMILIES[0], QUALIFIER); // ts[4]
    ht.delete(delete);

    get = new Get(ROW);
    get.addColumn(FAMILIES[0], QUALIFIER);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILIES[0], QUALIFIER,
        new long [] {ts[1], ts[2], ts[3]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3]},
        0, 2);

    scan = new Scan(ROW);
    scan.addColumn(FAMILIES[0], QUALIFIER);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILIES[0], QUALIFIER,
        new long [] {ts[1], ts[2], ts[3]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3]},
        0, 2);

    // Test for HBASE-1847
    delete = new Delete(ROW);
    delete.deleteColumn(FAMILIES[0], null);
    ht.delete(delete);

    // Cleanup null qualifier
    delete = new Delete(ROW);
    delete.deleteColumns(FAMILIES[0], null);
    ht.delete(delete);

    // Expected client behavior might be that you can re-put deleted values
    // But alas, this is not to be.  We can't put them back in either case.

    put = new Put(ROW);
    put.add(FAMILIES[0], QUALIFIER, ts[0], VALUES[0]); // 1000
    put.add(FAMILIES[0], QUALIFIER, ts[4], VALUES[4]); // 5000
    ht.put(put);


    // It used to be due to the internal implementation of Get, that
    // the Get() call would return ts[4] UNLIKE the Scan below. With
    // the switch to using Scan for Get this is no longer the case.
    get = new Get(ROW);
    get.addFamily(FAMILIES[0]);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILIES[0], QUALIFIER,
        new long [] {ts[1], ts[2], ts[3]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3]},
        0, 2);

    // The Scanner returns the previous values, the expected-naive-unexpected behavior

    scan = new Scan(ROW);
    scan.addFamily(FAMILIES[0]);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILIES[0], QUALIFIER,
        new long [] {ts[1], ts[2], ts[3]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3]},
        0, 2);

    // Test deleting an entire family from one row but not the other various ways

    put = new Put(ROWS[0]);
    put.add(FAMILIES[1], QUALIFIER, ts[0], VALUES[0]);
    put.add(FAMILIES[1], QUALIFIER, ts[1], VALUES[1]);
    put.add(FAMILIES[2], QUALIFIER, ts[2], VALUES[2]);
    put.add(FAMILIES[2], QUALIFIER, ts[3], VALUES[3]);
    ht.put(put);

    put = new Put(ROWS[1]);
    put.add(FAMILIES[1], QUALIFIER, ts[0], VALUES[0]);
    put.add(FAMILIES[1], QUALIFIER, ts[1], VALUES[1]);
    put.add(FAMILIES[2], QUALIFIER, ts[2], VALUES[2]);
    put.add(FAMILIES[2], QUALIFIER, ts[3], VALUES[3]);
    ht.put(put);

    put = new Put(ROWS[2]);
    put.add(FAMILIES[1], QUALIFIER, ts[0], VALUES[0]);
    put.add(FAMILIES[1], QUALIFIER, ts[1], VALUES[1]);
    put.add(FAMILIES[2], QUALIFIER, ts[2], VALUES[2]);
    put.add(FAMILIES[2], QUALIFIER, ts[3], VALUES[3]);
    ht.put(put);

    // Assert that above went in.
    get = new Get(ROWS[2]);
    get.addFamily(FAMILIES[1]);
    get.addFamily(FAMILIES[2]);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertTrue("Expected 4 key but received " + result.size() + ": " + result,
        result.size() == 4);

    delete = new Delete(ROWS[0]);
    delete.deleteFamily(FAMILIES[2]);
    ht.delete(delete);

    delete = new Delete(ROWS[1]);
    delete.deleteColumns(FAMILIES[1], QUALIFIER);
    ht.delete(delete);

    delete = new Delete(ROWS[2]);
    delete.deleteColumn(FAMILIES[1], QUALIFIER);
    delete.deleteColumn(FAMILIES[1], QUALIFIER);
    delete.deleteColumn(FAMILIES[2], QUALIFIER);
    ht.delete(delete);

    get = new Get(ROWS[0]);
    get.addFamily(FAMILIES[1]);
    get.addFamily(FAMILIES[2]);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertTrue("Expected 2 keys but received " + result.size(),
        result.size() == 2);
    assertNResult(result, ROWS[0], FAMILIES[1], QUALIFIER,
        new long [] {ts[0], ts[1]},
        new byte[][] {VALUES[0], VALUES[1]},
        0, 1);

    scan = new Scan(ROWS[0]);
    scan.addFamily(FAMILIES[1]);
    scan.addFamily(FAMILIES[2]);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertTrue("Expected 2 keys but received " + result.size(),
        result.size() == 2);
    assertNResult(result, ROWS[0], FAMILIES[1], QUALIFIER,
        new long [] {ts[0], ts[1]},
        new byte[][] {VALUES[0], VALUES[1]},
        0, 1);

    get = new Get(ROWS[1]);
    get.addFamily(FAMILIES[1]);
    get.addFamily(FAMILIES[2]);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertTrue("Expected 2 keys but received " + result.size(),
        result.size() == 2);

    scan = new Scan(ROWS[1]);
    scan.addFamily(FAMILIES[1]);
    scan.addFamily(FAMILIES[2]);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertTrue("Expected 2 keys but received " + result.size(),
        result.size() == 2);

    get = new Get(ROWS[2]);
    get.addFamily(FAMILIES[1]);
    get.addFamily(FAMILIES[2]);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertEquals(1, result.size());
    assertNResult(result, ROWS[2], FAMILIES[2], QUALIFIER,
        new long [] {ts[2]},
        new byte[][] {VALUES[2]},
        0, 0);

    scan = new Scan(ROWS[2]);
    scan.addFamily(FAMILIES[1]);
    scan.addFamily(FAMILIES[2]);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertEquals(1, result.size());
    assertNResult(result, ROWS[2], FAMILIES[2], QUALIFIER,
        new long [] {ts[2]},
        new byte[][] {VALUES[2]},
        0, 0);

    // Test if we delete the family first in one row (HBASE-1541)

    delete = new Delete(ROWS[3]);
    delete.deleteFamily(FAMILIES[1]);
    ht.delete(delete);

    put = new Put(ROWS[3]);
    put.add(FAMILIES[2], QUALIFIER, VALUES[0]);
    ht.put(put);

    put = new Put(ROWS[4]);
    put.add(FAMILIES[1], QUALIFIER, VALUES[1]);
    put.add(FAMILIES[2], QUALIFIER, VALUES[2]);
    ht.put(put);

    get = new Get(ROWS[3]);
    get.addFamily(FAMILIES[1]);
    get.addFamily(FAMILIES[2]);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertTrue("Expected 1 key but received " + result.size(),
        result.size() == 1);

    get = new Get(ROWS[4]);
    get.addFamily(FAMILIES[1]);
    get.addFamily(FAMILIES[2]);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertTrue("Expected 2 keys but received " + result.size(),
        result.size() == 2);

    scan = new Scan(ROWS[3]);
    scan.addFamily(FAMILIES[1]);
    scan.addFamily(FAMILIES[2]);
    scan.setMaxVersions(Integer.MAX_VALUE);
    ResultScanner scanner = ht.getScanner(scan);
    result = scanner.next();
    assertTrue("Expected 1 key but received " + result.size(),
        result.size() == 1);
    assertTrue(Bytes.equals(CellUtil.cloneRow(result.rawCells()[0]), ROWS[3]));
    assertTrue(Bytes.equals(CellUtil.cloneValue(result.rawCells()[0]), VALUES[0]));
    result = scanner.next();
    assertTrue("Expected 2 keys but received " + result.size(),
        result.size() == 2);
    assertTrue(Bytes.equals(CellUtil.cloneRow(result.rawCells()[0]), ROWS[4]));
    assertTrue(Bytes.equals(CellUtil.cloneRow(result.rawCells()[1]), ROWS[4]));
    assertTrue(Bytes.equals(CellUtil.cloneValue(result.rawCells()[0]), VALUES[1]));
    assertTrue(Bytes.equals(CellUtil.cloneValue(result.rawCells()[1]), VALUES[2]));
    scanner.close();

    // Add test of bulk deleting.
    for (int i = 0; i < 10; i++) {
      byte [] bytes = Bytes.toBytes(i);
      put = new Put(bytes);
      put.setDurability(Durability.SKIP_WAL);
      put.add(FAMILIES[0], QUALIFIER, bytes);@@@+      // Insert 4 more versions of same column and a dupe@@@+      put = new Put(ROW);@@@+      put.addColumn(FAMILY, QUALIFIER, STAMPS[9], VALUES[9]);@@@+      put.addColumn(FAMILY, QUALIFIER, STAMPS[11], VALUES[11]);@@@+      put.addColumn(FAMILY, QUALIFIER, STAMPS[13], VALUES[13]);@@@+      put.addColumn(FAMILY, QUALIFIER, STAMPS[15], VALUES[15]);@@@       ht.put(put);
    }
    for (int i = 0; i < 10; i++) {
      byte [] bytes = Bytes.toBytes(i);
      get = new Get(bytes);
      get.addFamily(FAMILIES[0]);@@@+@@@+      get = new Get(ROW);@@@+      get.addColumn(FAMILY, QUALIFIER);@@@+      get.readVersions(Integer.MAX_VALUE);@@@       result = ht.get(get);
      assertTrue(result.size() == 1);
    }
    ArrayList<Delete> deletes = new ArrayList<Delete>();
    for (int i = 0; i < 10; i++) {
      byte [] bytes = Bytes.toBytes(i);
      delete = new Delete(bytes);
      delete.deleteFamily(FAMILIES[0]);
      deletes.add(delete);
    }
    ht.delete(deletes);
    for (int i = 0; i < 10; i++) {
      byte [] bytes = Bytes.toBytes(i);
      get = new Get(bytes);
      get.addFamily(FAMILIES[0]);
      result = ht.get(get);
      assertTrue(result.size() == 0);
    }
  }
```
```
Cyclomatic Complexity	 9
Assertions		 11
Lines of Code		63
-  
  public void testJiraTest867() throws Exception {
    int numRows = 10;
    int numColsPerRow = 2000;

    byte [] TABLE = Bytes.toBytes("testJiraTest867");

    byte [][] ROWS = makeN(ROW, numRows);
    byte [][] QUALIFIERS = makeN(QUALIFIER, numColsPerRow);

    Table ht = TEST_UTIL.createTable(TABLE, FAMILY);
    TEST_UTIL.waitTableAvailable(TABLE, 10000);

    // Insert rows

    for(int i=0;i<numRows;i++) {
      Put put = new Put(ROWS[i]);
      put.setDurability(Durability.SKIP_WAL);
      for(int j=0;j<numColsPerRow;j++) {
        put.add(FAMILY, QUALIFIERS[j], QUALIFIERS[j]);
      }
      assertTrue("Put expected to contain " + numColsPerRow + " columns but " +
          "only contains " + put.size(), put.size() == numColsPerRow);
      ht.put(put);
    }

    // Get a row
    Get get = new Get(ROWS[numRows-1]);
    Result result = ht.get(get);
    assertNumKeys(result, numColsPerRow);
    Cell [] keys = result.rawCells();
    for(int i=0;i<result.size();i++) {
      assertKey(keys[i], ROWS[numRows-1], FAMILY, QUALIFIERS[i], QUALIFIERS[i]);
    }

    // Scan the rows
    Scan scan = new Scan();
    ResultScanner scanner = ht.getScanner(scan);
    int rowCount = 0;
    while((result = scanner.next()) != null) {
      assertNumKeys(result, numColsPerRow);
      Cell [] kvs = result.rawCells();
      for(int i=0;i<numColsPerRow;i++) {
        assertKey(kvs[i], ROWS[rowCount], FAMILY, QUALIFIERS[i], QUALIFIERS[i]);
      }
      rowCount++;
    }
    scanner.close();
    assertTrue("Expected to scan " + numRows + " rows but actually scanned "
        + rowCount + " rows", rowCount == numRows);

    // flush and try again

    TEST_UTIL.flush();

    // Get a row
    get = new Get(ROWS[numRows-1]);
    result = ht.get(get);
    assertNumKeys(result, numColsPerRow);
    keys = result.rawCells();
    for(int i=0;i<result.size();i++) {
      assertKey(keys[i], ROWS[numRows-1], FAMILY, QUALIFIERS[i], QUALIFIERS[i]);
    }

    // Scan the rows
    scan = new Scan();
    scanner = ht.getScanner(scan);
    rowCount = 0;
    while((result = scanner.next()) != null) {
      assertNumKeys(result, numColsPerRow);
      Cell [] kvs = result.rawCells();
      for(int i=0;i<numColsPerRow;i++) {
        assertKey(kvs[i], ROWS[rowCount], FAMILY, QUALIFIERS[i], QUALIFIERS[i]);
      }
      rowCount++;
    }
    scanner.close();
    assertTrue("Expected to scan " + numRows + " rows but actually scanned "
        + rowCount + " rows", rowCount == numRows);

  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		39
-  
  public void testJiraTest861() throws Exception {

    byte [] TABLE = Bytes.toBytes("testJiraTest861");
    byte [][] VALUES = makeNAscii(VALUE, 7);
    long [] STAMPS = makeStamps(7);

    Table ht = TEST_UTIL.createTable(TABLE, FAMILY, 10);
    TEST_UTIL.waitTableAvailable(TABLE, 10000);

    // Insert three versions

    Put put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, STAMPS[3], VALUES[3]);
    put.add(FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    put.add(FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    ht.put(put);

    // Get the middle value
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);

    // Try to get one version before (expect fail)
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[1]);

    // Try to get one version after (expect fail)
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[5]);

    // Try same from storefile
    TEST_UTIL.flush();
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[1]);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[5]);

    // Insert two more versions surrounding others, into memstore
    put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, STAMPS[0], VALUES[0]);
    put.add(FAMILY, QUALIFIER, STAMPS[6], VALUES[6]);
    ht.put(put);

    // Check we can get everything we should and can't get what we shouldn't
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[0], VALUES[0]);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[1]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[3], VALUES[3]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[5]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[6], VALUES[6]);

    // Try same from two storefiles
    TEST_UTIL.flush();
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[0], VALUES[0]);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[1]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[3], VALUES[3]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[5]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[6], VALUES[6]);

  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		33
-  
  public void testJiraTest33() throws Exception {

    byte [] TABLE = Bytes.toBytes("testJiraTest33");
    byte [][] VALUES = makeNAscii(VALUE, 7);
    long [] STAMPS = makeStamps(7);

    Table ht = TEST_UTIL.createTable(TABLE, FAMILY, 10);
    TEST_UTIL.waitTableAvailable(TABLE, 10000);

    // Insert lots versions

    Put put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, STAMPS[0], VALUES[0]);
    put.add(FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    put.add(FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    put.add(FAMILY, QUALIFIER, STAMPS[3], VALUES[3]);
    put.add(FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    put.add(FAMILY, QUALIFIER, STAMPS[5], VALUES[5]);
    ht.put(put);

    getVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 5);
    getVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 2);
    getVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 4, 5);
    getVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 2, 3);

    scanVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 5);
    scanVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 2);
    scanVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 4, 5);
    scanVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 2, 3);

    // Try same from storefile
    TEST_UTIL.flush();

    getVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 5);
    getVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 2);
    getVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 4, 5);
    getVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 2, 3);

    scanVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 5);
    scanVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 2);
    scanVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 4, 5);
    scanVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 2, 3);

  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		13
-  
  public void testJiraTest1014() throws Exception {

    byte [] TABLE = Bytes.toBytes("testJiraTest1014");

    Table ht = TEST_UTIL.createTable(TABLE, FAMILY, 10);
    TEST_UTIL.waitTableAvailable(TABLE, 10000);

    long manualStamp = 12345;

    // Insert lots versions

    Put put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, manualStamp, VALUE);
    ht.put(put);

    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, manualStamp, VALUE);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, manualStamp-1);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, manualStamp+1);

  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		29
-  
  public void testJiraTest1182() throws Exception {

    byte [] TABLE = Bytes.toBytes("testJiraTest1182");
    byte [][] VALUES = makeNAscii(VALUE, 7);
    long [] STAMPS = makeStamps(7);

    Table ht = TEST_UTIL.createTable(TABLE, FAMILY, 10);
    TEST_UTIL.waitTableAvailable(TABLE, 10000);

    // Insert lots versions

    Put put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, STAMPS[0], VALUES[0]);
    put.add(FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    put.add(FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    put.add(FAMILY, QUALIFIER, STAMPS[3], VALUES[3]);
    put.add(FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    put.add(FAMILY, QUALIFIER, STAMPS[5], VALUES[5]);
    ht.put(put);

    getVersionRangeAndVerifyGreaterThan(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 5);
    getVersionRangeAndVerifyGreaterThan(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 2, 5);
    getVersionRangeAndVerifyGreaterThan(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 4, 5);

    scanVersionRangeAndVerifyGreaterThan(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 5);
    scanVersionRangeAndVerifyGreaterThan(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 2, 5);
    scanVersionRangeAndVerifyGreaterThan(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 4, 5);

    // Try same from storefile
    TEST_UTIL.flush();

    getVersionRangeAndVerifyGreaterThan(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 5);
    getVersionRangeAndVerifyGreaterThan(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 2, 5);
    getVersionRangeAndVerifyGreaterThan(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 4, 5);

    scanVersionRangeAndVerifyGreaterThan(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 5);
    scanVersionRangeAndVerifyGreaterThan(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 2, 5);
    scanVersionRangeAndVerifyGreaterThan(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 4, 5);
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		21
-  
  public void testJiraTest52() throws Exception {
    byte [] TABLE = Bytes.toBytes("testJiraTest52");
    byte [][] VALUES = makeNAscii(VALUE, 7);
    long [] STAMPS = makeStamps(7);

    Table ht = TEST_UTIL.createTable(TABLE, FAMILY, 10);
    TEST_UTIL.waitTableAvailable(TABLE, 10000);

    // Insert lots versions

    Put put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, STAMPS[0], VALUES[0]);
    put.add(FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    put.add(FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    put.add(FAMILY, QUALIFIER, STAMPS[3], VALUES[3]);
    put.add(FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    put.add(FAMILY, QUALIFIER, STAMPS[5], VALUES[5]);
    ht.put(put);

    getAllVersionsAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 5);

    scanAllVersionsAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 5);

    // Try same from storefile
    TEST_UTIL.flush();

    getAllVersionsAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 5);

    scanAllVersionsAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 5);
  }
```
```
Cyclomatic Complexity	 1
Assertions		 12
Lines of Code		167
-  
  public void testDuplicateVersions() throws Exception {
    byte [] TABLE = Bytes.toBytes("testDuplicateVersions");

    long [] STAMPS = makeStamps(20);
    byte [][] VALUES = makeNAscii(VALUE, 20);

    Table ht = TEST_UTIL.createTable(TABLE, FAMILY, 10);
    TEST_UTIL.waitTableAvailable(TABLE, 10000);

    // Insert 4 versions of same column
    Put put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    put.add(FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    put.add(FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    put.add(FAMILY, QUALIFIER, STAMPS[5], VALUES[5]);
    ht.put(put);

    // Verify we can get each one properly
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[5], VALUES[5]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[5], VALUES[5]);

    // Verify we don't accidentally get others
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[0]);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[3]);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[6]);
    scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[0]);
    scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[3]);
    scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[6]);

    // Ensure maxVersions in query is respected
    Get get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIER);
    get.setMaxVersions(2);
    Result result = ht.get(get);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[4], STAMPS[5]},
        new byte[][] {VALUES[4], VALUES[5]},
        0, 1);

    Scan scan = new Scan(ROW);
    scan.addColumn(FAMILY, QUALIFIER);
    scan.setMaxVersions(2);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[4], STAMPS[5]},
        new byte[][] {VALUES[4], VALUES[5]},
        0, 1);

    // Flush and redo

    TEST_UTIL.flush();

    // Verify we can get each one properly
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[5], VALUES[5]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[5], VALUES[5]);

    // Verify we don't accidentally get others
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[0]);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[3]);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[6]);
    scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[0]);
    scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[3]);
    scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[6]);

    // Ensure maxVersions in query is respected
    get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIER);
    get.setMaxVersions(2);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[4], STAMPS[5]},
        new byte[][] {VALUES[4], VALUES[5]},
        0, 1);

    scan = new Scan(ROW);
    scan.addColumn(FAMILY, QUALIFIER);
    scan.setMaxVersions(2);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[4], STAMPS[5]},
        new byte[][] {VALUES[4], VALUES[5]},
        0, 1);


    // Add some memstore and retest

    // Insert 4 more versions of same column and a dupe
    put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, STAMPS[3], VALUES[3]);
    put.add(FAMILY, QUALIFIER, STAMPS[4], VALUES[14]);
    put.add(FAMILY, QUALIFIER, STAMPS[6], VALUES[6]);
    put.add(FAMILY, QUALIFIER, STAMPS[7], VALUES[7]);
    put.add(FAMILY, QUALIFIER, STAMPS[8], VALUES[8]);
    ht.put(put);

    // Ensure maxVersions in query is respected
    get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIER);
    get.setMaxVersions(7);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7], STAMPS[8]},
        new byte[][] {VALUES[2], VALUES[3], VALUES[14], VALUES[5], VALUES[6], VALUES[7], VALUES[8]},
        0, 6);

    scan = new Scan(ROW);
    scan.addColumn(FAMILY, QUALIFIER);
    scan.setMaxVersions(7);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7], STAMPS[8]},
        new byte[][] {VALUES[2], VALUES[3], VALUES[14], VALUES[5], VALUES[6], VALUES[7], VALUES[8]},
        0, 6);

    get = new Get(ROW);
    get.setMaxVersions(7);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7], STAMPS[8]},
        new byte[][] {VALUES[2], VALUES[3], VALUES[14], VALUES[5], VALUES[6], VALUES[7], VALUES[8]},
        0, 6);

    scan = new Scan(ROW);
    scan.setMaxVersions(7);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7], STAMPS[8]},
        new byte[][] {VALUES[2], VALUES[3], VALUES[14], VALUES[5], VALUES[6], VALUES[7], VALUES[8]},
        0, 6);

    // Verify we can get each one properly
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[14]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[7], VALUES[7]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[14]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[7], VALUES[7]);

    // Verify we don't accidentally get others
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[0]);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[9]);
    scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[0]);
    scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[9]);

    // Ensure maxVersions of table is respected

    TEST_UTIL.flush();

    // Insert 4 more versions of same column and a dupe
    put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, STAMPS[9], VALUES[9]);
    put.add(FAMILY, QUALIFIER, STAMPS[11], VALUES[11]);
    put.add(FAMILY, QUALIFIER, STAMPS[13], VALUES[13]);
    put.add(FAMILY, QUALIFIER, STAMPS[15], VALUES[15]);
    ht.put(put);

    get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIER);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7], STAMPS[8], STAMPS[9], STAMPS[11], STAMPS[13], STAMPS[15]},
        new byte[][] {VALUES[3], VALUES[14], VALUES[5], VALUES[6], VALUES[7], VALUES[8], VALUES[9], VALUES[11], VALUES[13], VALUES[15]},
        0, 9);

    scan = new Scan(ROW);
    scan.addColumn(FAMILY, QUALIFIER);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7], STAMPS[8], STAMPS[9], STAMPS[11], STAMPS[13], STAMPS[15]},
        new byte[][] {VALUES[3], VALUES[14], VALUES[5], VALUES[6], VALUES[7], VALUES[8], VALUES[9], VALUES[11], VALUES[13], VALUES[15]},
        0, 9);

    // Delete a version in the memstore and a version in a storefile
    Delete delete = new Delete(ROW);
    delete.deleteColumn(FAMILY, QUALIFIER, STAMPS[11]);
    delete.deleteColumn(FAMILY, QUALIFIER, STAMPS[7]);
    ht.delete(delete);

    // Test that it's gone
    get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIER);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[1], STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[8], STAMPS[9], STAMPS[13], STAMPS[15]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3], VALUES[14], VALUES[5], VALUES[6], VALUES[8], VALUES[9], VALUES[13], VALUES[15]},
        0, 9);

    scan = new Scan(ROW);
    scan.addColumn(FAMILY, QUALIFIER);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[1], STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[8], STAMPS[9], STAMPS[13], STAMPS[15]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3], VALUES[14], VALUES[5], VALUES[6], VALUES[8], VALUES[9], VALUES[13], VALUES[15]},
        0, 9);
  }
```
```
Cyclomatic Complexity	 1
Assertions		 4
Lines of Code		35
-  
  public void testUpdates() throws Exception {

    byte [] TABLE = Bytes.toBytes("testUpdates");
    Table hTable = TEST_UTIL.createTable(TABLE, FAMILY, 10);
    TEST_UTIL.waitTableAvailable(TABLE, 10000);

    // Write a column with values at timestamp 1, 2 and 3
    byte[] row = Bytes.toBytes("row1");
    byte[] qualifier = Bytes.toBytes("myCol");
    Put put = new Put(row);
    put.add(FAMILY, qualifier, 1L, Bytes.toBytes("AAA"));
    hTable.put(put);

    put = new Put(row);
    put.add(FAMILY, qualifier, 2L, Bytes.toBytes("BBB"));
    hTable.put(put);

    put = new Put(row);
    put.add(FAMILY, qualifier, 3L, Bytes.toBytes("EEE"));
    hTable.put(put);

    Get get = new Get(row);
    get.addColumn(FAMILY, qualifier);
    get.setMaxVersions();

    // Check that the column indeed has the right values at timestamps 1 and
    // 2
    Result result = hTable.get(get);
    NavigableMap<Long, byte[]> navigableMap =
        result.getMap().get(FAMILY).get(qualifier);
    assertEquals("AAA", Bytes.toString(navigableMap.get(1L)));
    assertEquals("BBB", Bytes.toString(navigableMap.get(2L)));

    // Update the value at timestamp 1
    put = new Put(row);
    put.add(FAMILY, qualifier, 1L, Bytes.toBytes("CCC"));
    hTable.put(put);

    // Update the value at timestamp 2
    put = new Put(row);
    put.add(FAMILY, qualifier, 2L, Bytes.toBytes("DDD"));
    hTable.put(put);

    // Check that the values at timestamp 2 and 1 got updated
    result = hTable.get(get);
    navigableMap = result.getMap().get(FAMILY).get(qualifier);
    assertEquals("CCC", Bytes.toString(navigableMap.get(1L)));
    assertEquals("DDD", Bytes.toString(navigableMap.get(2L)));
  }
```
```
Cyclomatic Complexity	 1
Assertions		 4
Lines of Code		42
-  
  public void testUpdatesWithMajorCompaction() throws Exception {

    TableName TABLE = TableName.valueOf("testUpdatesWithMajorCompaction");
    Table hTable = TEST_UTIL.createTable(TABLE, FAMILY, 10);
    TEST_UTIL.waitTableAvailable(TABLE, 10000);

    HBaseAdmin admin = new HBaseAdmin(TEST_UTIL.getConfiguration());

    // Write a column with values at timestamp 1, 2 and 3
    byte[] row = Bytes.toBytes("row2");
    byte[] qualifier = Bytes.toBytes("myCol");
    Put put = new Put(row);
    put.add(FAMILY, qualifier, 1L, Bytes.toBytes("AAA"));
    hTable.put(put);

    put = new Put(row);
    put.add(FAMILY, qualifier, 2L, Bytes.toBytes("BBB"));
    hTable.put(put);

    put = new Put(row);
    put.add(FAMILY, qualifier, 3L, Bytes.toBytes("EEE"));
    hTable.put(put);

    Get get = new Get(row);
    get.addColumn(FAMILY, qualifier);
    get.setMaxVersions();

    // Check that the column indeed has the right values at timestamps 1 and
    // 2
    Result result = hTable.get(get);
    NavigableMap<Long, byte[]> navigableMap =
        result.getMap().get(FAMILY).get(qualifier);
    assertEquals("AAA", Bytes.toString(navigableMap.get(1L)));
    assertEquals("BBB", Bytes.toString(navigableMap.get(2L)));

    // Trigger a major compaction
    admin.flush(TABLE);
    admin.majorCompact(TABLE);
    Thread.sleep(6000);

    // Update the value at timestamp 1
    put = new Put(row);
    put.add(FAMILY, qualifier, 1L, Bytes.toBytes("CCC"));
    hTable.put(put);

    // Update the value at timestamp 2
    put = new Put(row);
    put.add(FAMILY, qualifier, 2L, Bytes.toBytes("DDD"));
    hTable.put(put);

    // Trigger a major compaction
    admin.flush(TABLE);
    admin.majorCompact(TABLE);
    Thread.sleep(6000);

    // Check that the values at timestamp 2 and 1 got updated
    result = hTable.get(get);
    navigableMap = result.getMap().get(FAMILY).get(qualifier);
    assertEquals("CCC", Bytes.toString(navigableMap.get(1L)));
    assertEquals("DDD", Bytes.toString(navigableMap.get(2L)));
  }
```
```
Cyclomatic Complexity	 1
Assertions		 4
Lines of Code		46
-  
  public void testMajorCompactionBetweenTwoUpdates() throws Exception {

    String tableName = "testMajorCompactionBetweenTwoUpdates";
    byte [] TABLE = Bytes.toBytes(tableName);
    Table hTable = TEST_UTIL.createTable(TABLE, FAMILY, 10);
    TEST_UTIL.waitTableAvailable(TABLE, 10000);
    HBaseAdmin admin = new HBaseAdmin(TEST_UTIL.getConfiguration());

    // Write a column with values at timestamp 1, 2 and 3
    byte[] row = Bytes.toBytes("row3");
    byte[] qualifier = Bytes.toBytes("myCol");
    Put put = new Put(row);
    put.add(FAMILY, qualifier, 1L, Bytes.toBytes("AAA"));
    hTable.put(put);

    put = new Put(row);
    put.add(FAMILY, qualifier, 2L, Bytes.toBytes("BBB"));
    hTable.put(put);

    put = new Put(row);
    put.add(FAMILY, qualifier, 3L, Bytes.toBytes("EEE"));
    hTable.put(put);

    Get get = new Get(row);
    get.addColumn(FAMILY, qualifier);
    get.setMaxVersions();

    // Check that the column indeed has the right values at timestamps 1 and
    // 2
    Result result = hTable.get(get);
    NavigableMap<Long, byte[]> navigableMap =
        result.getMap().get(FAMILY).get(qualifier);
    assertEquals("AAA", Bytes.toString(navigableMap.get(1L)));
    assertEquals("BBB", Bytes.toString(navigableMap.get(2L)));

    // Trigger a major compaction
    admin.flush(tableName);
    admin.majorCompact(tableName);
    Thread.sleep(6000);

    // Update the value at timestamp 1
    put = new Put(row);
    put.add(FAMILY, qualifier, 1L, Bytes.toBytes("CCC"));
    hTable.put(put);

    // Trigger a major compaction
    admin.flush(tableName);
    admin.majorCompact(tableName);
    Thread.sleep(6000);

    // Update the value at timestamp 2
    put = new Put(row);
    put.add(FAMILY, qualifier, 2L, Bytes.toBytes("DDD"));
    hTable.put(put);

    // Trigger a major compaction
    admin.flush(tableName);
    admin.majorCompact(tableName);
    Thread.sleep(6000);

    // Check that the values at timestamp 2 and 1 got updated
    result = hTable.get(get);
    navigableMap = result.getMap().get(FAMILY).get(qualifier);

    assertEquals("CCC", Bytes.toString(navigableMap.get(1L)));
    assertEquals("DDD", Bytes.toString(navigableMap.get(2L)));
  }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		9
-  
  public void testGet_EmptyTable() throws IOException, InterruptedException {
    Table table = TEST_UTIL.createTable(TableName.valueOf("testGet_EmptyTable"), FAMILY);
    TEST_UTIL.waitTableAvailable(TableName.valueOf("testGet_EmptyTable"), 10000);
    Get get = new Get(ROW);
    get.addFamily(FAMILY);
    Result r = table.get(get);
    assertTrue(r.isEmpty());
  }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		20
-  
  public void testGet_NullQualifier() throws IOException, InterruptedException {
    Table table = TEST_UTIL.createTable(TableName.valueOf("testGet_NullQualifier"), FAMILY);
    TEST_UTIL.waitTableAvailable(TableName.valueOf("testGet_NullQualifier"), 10000);
    Put put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, VALUE);
    table.put(put);

    put = new Put(ROW);
    put.add(FAMILY, null, VALUE);
    table.put(put);
    LOG.info("Row put");

    Get get = new Get(ROW);
    get.addColumn(FAMILY, null);
    Result r = table.get(get);
    assertEquals(1, r.size());

    get = new Get(ROW);
    get.addFamily(FAMILY);
    r = table.get(get);
    assertEquals(2, r.size());
  }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		20
-  
  public void testGet_NonExistentRow() throws IOException, InterruptedException {
    Table table = TEST_UTIL.createTable(TableName.valueOf("testGet_NonExistentRow"), FAMILY);
    TEST_UTIL.waitTableAvailable(TableName.valueOf("testGet_NonExistentRow"), 10000);
    Put put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, VALUE);
    table.put(put);
    LOG.info("Row put");

    Get get = new Get(ROW);
    get.addFamily(FAMILY);
    Result r = table.get(get);
    assertFalse(r.isEmpty());
    System.out.println("Row retrieved successfully");

    byte [] missingrow = Bytes.toBytes("missingrow");
    get = new Get(missingrow);
    get.addFamily(FAMILY);
    r = table.get(get);
    assertTrue(r.isEmpty());
    LOG.info("Row missing as it should be");
  }
```
```
Cyclomatic Complexity	 4
Assertions		 5
Lines of Code		31
-  
  public void testPut() throws IOException, InterruptedException {
    final byte [] CONTENTS_FAMILY = Bytes.toBytes("contents");
    final byte [] SMALL_FAMILY = Bytes.toBytes("smallfam");
    final byte [] row1 = Bytes.toBytes("row1");
    final byte [] row2 = Bytes.toBytes("row2");
    final byte [] value = Bytes.toBytes("abcd");
    Table table = TEST_UTIL.createTable(Bytes.toBytes("testPut"),
      new byte [][] {CONTENTS_FAMILY, SMALL_FAMILY});
    TEST_UTIL.waitTableAvailable(Bytes.toBytes("testPut"), 10000);
    Put put = new Put(row1);
    put.add(CONTENTS_FAMILY, null, value);
    table.put(put);

    put = new Put(row2);
    put.add(CONTENTS_FAMILY, null, value);

    assertEquals(put.size(), 1);
    assertEquals(put.getFamilyCellMap().get(CONTENTS_FAMILY).size(), 1);

    // KeyValue v1 expectation.  Cast for now until we go all Cell all the time. TODO
    KeyValue kv = (KeyValue)put.getFamilyCellMap().get(CONTENTS_FAMILY).get(0);

    assertTrue(Bytes.equals(kv.getFamily(), CONTENTS_FAMILY));
    // will it return null or an empty byte array?
    assertTrue(Bytes.equals(kv.getQualifier(), new byte[0]));

    assertTrue(Bytes.equals(kv.getValue(), value));

    table.put(put);

    Scan scan = new Scan();
    scan.addColumn(CONTENTS_FAMILY, null);
    ResultScanner scanner = table.getScanner(scan);
    for (Result r : scanner) {
      for(Cell key : r.rawCells()) {
        System.out.println(Bytes.toString(r.getRow()) + ": " + key.toString());
      }
    }
  }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		16
-  
  public void testPutNoCF() throws IOException, InterruptedException {
    final byte[] BAD_FAM = Bytes.toBytes("BAD_CF");
    final byte[] VAL = Bytes.toBytes(100);
    Table table = TEST_UTIL.createTable(Bytes.toBytes("testPutNoCF"), FAMILY);
    TEST_UTIL.waitTableAvailable(Bytes.toBytes("testPutNoCF"), 10000);

    boolean caughtNSCFE = false;

    try {
      Put p = new Put(ROW);
      p.add(BAD_FAM, QUALIFIER, VAL);
      table.put(p);
    } catch (RetriesExhaustedWithDetailsException e) {
      caughtNSCFE = e.getCause(0) instanceof NoSuchColumnFamilyException;
    }
    assertTrue("Should throw NoSuchColumnFamilyException", caughtNSCFE);

  }
```
```
Cyclomatic Complexity	 3
Assertions		 1
Lines of Code		27
-  
  public void testRowsPut() throws IOException, InterruptedException {
    final byte[] CONTENTS_FAMILY = Bytes.toBytes("contents");
    final byte[] SMALL_FAMILY = Bytes.toBytes("smallfam");
    final int NB_BATCH_ROWS = 10;
    final byte[] value = Bytes.toBytes("abcd");
    Table table = TEST_UTIL.createTable(Bytes.toBytes("testRowsPut"),
      new byte[][] {CONTENTS_FAMILY, SMALL_FAMILY });
    TEST_UTIL.waitTableAvailable(Bytes.toBytes("testRowsPut"), 10000);
    ArrayList<Put> rowsUpdate = new ArrayList<Put>();
    for (int i = 0; i < NB_BATCH_ROWS; i++) {
      byte[] row = Bytes.toBytes("row" + i);
      Put put = new Put(row);
      put.setDurability(Durability.SKIP_WAL);
      put.add(CONTENTS_FAMILY, null, value);
      rowsUpdate.add(put);
    }
    table.put(rowsUpdate);
    Scan scan = new Scan();
    scan.addFamily(CONTENTS_FAMILY);
    ResultScanner scanner = table.getScanner(scan);
    int nbRows = 0;
    for (@SuppressWarnings("unused")
    Result row : scanner)
      nbRows++;
    assertEquals(NB_BATCH_ROWS, nbRows);
  }
```
```
Cyclomatic Complexity	 4
Assertions		 2
Lines of Code		40
-  
  public void testRowsPutBufferedOneFlush() throws IOException, InterruptedException {
    final byte [] CONTENTS_FAMILY = Bytes.toBytes("contents");
    final byte [] SMALL_FAMILY = Bytes.toBytes("smallfam");
    final byte [] value = Bytes.toBytes("abcd");
    final int NB_BATCH_ROWS = 10;
    HTable table = TEST_UTIL.createTable(Bytes.toBytes("testRowsPutBufferedOneFlush"),
      new byte [][] {CONTENTS_FAMILY, SMALL_FAMILY});
    TEST_UTIL.waitTableAvailable(Bytes.toBytes("testRowsPutBufferedOneFlush"),
        10000);
    table.setAutoFlush(false);
    ArrayList<Put> rowsUpdate = new ArrayList<Put>();
    for (int i = 0; i < NB_BATCH_ROWS * 10; i++) {
      byte[] row = Bytes.toBytes("row" + i);
      Put put = new Put(row);
      put.setDurability(Durability.SKIP_WAL);
      put.add(CONTENTS_FAMILY, null, value);
      rowsUpdate.add(put);
    }
    table.put(rowsUpdate);

    Scan scan = new Scan();
    scan.addFamily(CONTENTS_FAMILY);
    ResultScanner scanner = table.getScanner(scan);
    int nbRows = 0;
    for (@SuppressWarnings("unused")
    Result row : scanner)
      nbRows++;
    assertEquals(0, nbRows);
    scanner.close();

    table.flushCommits();

    scan = new Scan();
    scan.addFamily(CONTENTS_FAMILY);
    scanner = table.getScanner(scan);
    nbRows = 0;
    for (@SuppressWarnings("unused")
    Result row : scanner)
      nbRows++;
    assertEquals(NB_BATCH_ROWS * 10, nbRows);
    table.close();
  }
```
```
Cyclomatic Complexity	 3
Assertions		 1
Lines of Code		29
-  
  public void testRowsPutBufferedManyManyFlushes() throws IOException, InterruptedException {
    final byte[] CONTENTS_FAMILY = Bytes.toBytes("contents");
    final byte[] SMALL_FAMILY = Bytes.toBytes("smallfam");
    final byte[] value = Bytes.toBytes("abcd");
    final int NB_BATCH_ROWS = 10;
    HTable table = TEST_UTIL.createTable(Bytes.toBytes("testRowsPutBufferedManyManyFlushes"),
      new byte[][] {CONTENTS_FAMILY, SMALL_FAMILY });
    TEST_UTIL.waitTableAvailable(Bytes.toBytes("testRowsPutBufferedManyManyFlushes"),
        10000);
    table.setWriteBufferSize(10);
    ArrayList<Put> rowsUpdate = new ArrayList<Put>();
    for (int i = 0; i < NB_BATCH_ROWS * 10; i++) {
      byte[] row = Bytes.toBytes("row" + i);
      Put put = new Put(row);
      put.setDurability(Durability.SKIP_WAL);
      put.add(CONTENTS_FAMILY, null, value);
      rowsUpdate.add(put);
    }
    table.put(rowsUpdate);

    Scan scan = new Scan();
    scan.addFamily(CONTENTS_FAMILY);
    ResultScanner scanner = table.getScanner(scan);
    int nbRows = 0;
    for (@SuppressWarnings("unused")
    Result row : scanner)
      nbRows++;
    assertEquals(NB_BATCH_ROWS * 10, nbRows);
  }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		25
-  
  public void testAddKeyValue() throws IOException {
    final byte[] CONTENTS_FAMILY = Bytes.toBytes("contents");
    final byte[] value = Bytes.toBytes("abcd");
    final byte[] row1 = Bytes.toBytes("row1");
    final byte[] row2 = Bytes.toBytes("row2");
    byte[] qualifier = Bytes.toBytes("qf1");
    Put put = new Put(row1);

    // Adding KeyValue with the same row
    KeyValue kv = new KeyValue(row1, CONTENTS_FAMILY, qualifier, value);
    boolean ok = true;
    try {
      put.add(kv);
    } catch (IOException e) {
      ok = false;
    }
    assertEquals(true, ok);

    // Adding KeyValue with the different row
    kv = new KeyValue(row2, CONTENTS_FAMILY, qualifier, value);
    ok = false;
    try {
      put.add(kv);
    } catch (IOException e) {
      ok = true;
    }
    assertEquals(true, ok);
  }
```
```
Cyclomatic Complexity	 3
Assertions		 6
Lines of Code		19
-  
  public void testAppendWithoutWAL() throws Exception {
    List<Result> resultsWithWal = doAppend(true);
    List<Result> resultsWithoutWal = doAppend(false);
    assertEquals(resultsWithWal.size(), resultsWithoutWal.size());
    for (int i = 0; i != resultsWithWal.size(); ++i) {
      Result resultWithWal = resultsWithWal.get(i);
      Result resultWithoutWal = resultsWithoutWal.get(i);
      assertEquals(resultWithWal.rawCells().length, resultWithoutWal.rawCells().length);
      for (int j = 0; j != resultWithWal.rawCells().length; ++j) {
        Cell cellWithWal = resultWithWal.rawCells()[j];
        Cell cellWithoutWal = resultWithoutWal.rawCells()[j];
        assertTrue(Bytes.equals(CellUtil.cloneRow(cellWithWal), CellUtil.cloneRow(cellWithoutWal)));
        assertTrue(Bytes.equals(CellUtil.cloneFamily(cellWithWal), CellUtil.cloneFamily(cellWithoutWal)));
        assertTrue(Bytes.equals(CellUtil.cloneQualifier(cellWithWal), CellUtil.cloneQualifier(cellWithoutWal)));
        assertTrue(Bytes.equals(CellUtil.cloneValue(cellWithWal), CellUtil.cloneValue(cellWithoutWal)));
      }
    }
  }
```
```
Cyclomatic Complexity	 10
Assertions		 2
Lines of Code		74
-  
  public void testHBase737() throws IOException, InterruptedException {
    final byte [] FAM1 = Bytes.toBytes("fam1");
    final byte [] FAM2 = Bytes.toBytes("fam2");
    // Open table
    Table table = TEST_UTIL.createTable(Bytes.toBytes("testHBase737"),
      new byte [][] {FAM1, FAM2});
    TEST_UTIL.waitTableAvailable(Bytes.toBytes("testHBase737"), 10000);
    // Insert some values
    Put put = new Put(ROW);
    put.add(FAM1, Bytes.toBytes("letters"), Bytes.toBytes("abcdefg"));
    table.put(put);
    try {
      Thread.sleep(1000);
    } catch (InterruptedException i) {
      //ignore
    }

    put = new Put(ROW);
    put.add(FAM1, Bytes.toBytes("numbers"), Bytes.toBytes("123456"));
    table.put(put);

    try {
      Thread.sleep(1000);
    } catch (InterruptedException i) {
      //ignore
    }

    put = new Put(ROW);
    put.add(FAM2, Bytes.toBytes("letters"), Bytes.toBytes("hijklmnop"));
    table.put(put);

    long times[] = new long[3];

    // First scan the memstore

    Scan scan = new Scan();
    scan.addFamily(FAM1);
    scan.addFamily(FAM2);
    ResultScanner s = table.getScanner(scan);
    try {
      int index = 0;
      Result r = null;
      while ((r = s.next()) != null) {
        for(Cell key : r.rawCells()) {
          times[index++] = key.getTimestamp();
        }
      }
    } finally {
      s.close();
    }
    for (int i = 0; i < times.length - 1; i++) {
      for (int j = i + 1; j < times.length; j++) {
        assertTrue(times[j] > times[i]);
      }
    }

    // Flush data to disk and try again
    TEST_UTIL.flush();

    // Reset times
    for(int i=0;i<times.length;i++) {
      times[i] = 0;
    }

    try {
      Thread.sleep(1000);
    } catch (InterruptedException i) {
      //ignore
    }
    scan = new Scan();
    scan.addFamily(FAM1);
    scan.addFamily(FAM2);
    s = table.getScanner(scan);
    try {
      int index = 0;
      Result r = null;
      while ((r = s.next()) != null) {
        for(Cell key : r.rawCells()) {
          times[index++] = key.getTimestamp();
        }
      }
    } finally {
      s.close();
    }
    for (int i = 0; i < times.length - 1; i++) {
      for (int j = i + 1; j < times.length; j++) {
        assertTrue(times[j] > times[i]);
      }
    }
  }
```
```
Cyclomatic Complexity	 5
Assertions		 2
Lines of Code		27
-  
  public void testListTables() throws IOException, InterruptedException {
    TableName t1 = TableName.valueOf("testListTables1");
    TableName t2 = TableName.valueOf("testListTables2");
    TableName t3 = TableName.valueOf("testListTables3");
    TableName [] tables = new TableName[] { t1, t2, t3 };
    for (int i = 0; i < tables.length; i++) {
      TEST_UTIL.createTable(tables[i], FAMILY);
      TEST_UTIL.waitTableAvailable(tables[i], 10000);
    }
    Admin admin = new HBaseAdmin(TEST_UTIL.getConfiguration());
    HTableDescriptor[] ts = admin.listTables();
    HashSet<HTableDescriptor> result = new HashSet<HTableDescriptor>(ts.length);
    Collections.addAll(result, ts);
    int size = result.size();
    assertTrue(size >= tables.length);
    for (int i = 0; i < tables.length && i < size; i++) {
      boolean found = false;
      for (int j = 0; j < ts.length; j++) {
        if (ts[j].getTableName().equals(tables[i])) {
          found = true;
          break;
        }
      }
      assertTrue("Not found: " + tables[i], found);
    }
  }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		9
-  
  public void testUnmanagedHConnection() throws IOException, InterruptedException {
    final TableName tableName = TableName.valueOf("testUnmanagedHConnection");
    HTable t = createUnmangedHConnectionHTable(tableName);
    HBaseAdmin ha = new HBaseAdmin(t.getConnection());
    assertTrue(ha.tableExists(tableName));
    assertTrue(t.get(new Get(ROW)).isEmpty());
    ha.close();
  }
```
```
Cyclomatic Complexity	 1
Assertions		 5
Lines of Code		19
-  
  public void testUnmanagedHConnectionReconnect() throws Exception {
    final TableName tableName = TableName.valueOf("testUnmanagedHConnectionReconnect");
    HTable t = createUnmangedHConnectionHTable(tableName);
    Connection conn = t.getConnection();
    try (HBaseAdmin ha = new HBaseAdmin(conn)) {
      assertTrue(ha.tableExists(tableName));
      assertTrue(t.get(new Get(ROW)).isEmpty());
    }

    // stop the master
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    cluster.stopMaster(0, false);
    cluster.waitOnMaster(0);

    // start up a new master
    cluster.startMaster();
    assertTrue(cluster.waitForActiveAndReadyMaster());

    // test that the same unmanaged connection works with a new
    // HBaseAdmin and can connect to the new master;
    try (HBaseAdmin newAdmin = new HBaseAdmin(conn)) {
      assertTrue(newAdmin.tableExists(tableName));
      assertTrue(newAdmin.getClusterStatus().getServersSize() == SLAVES);
    }
  }
```
```
Cyclomatic Complexity	 7
Assertions		 5
Lines of Code		55
-  
  public void testMiscHTableStuff() throws IOException, InterruptedException {
    final TableName tableAname = TableName.valueOf("testMiscHTableStuffA");
    final TableName tableBname = TableName.valueOf("testMiscHTableStuffB");
    final byte[] attrName = Bytes.toBytes("TESTATTR");
    final byte[] attrValue = Bytes.toBytes("somevalue");
    byte[] value = Bytes.toBytes("value");

    Table a = TEST_UTIL.createTable(tableAname, HConstants.CATALOG_FAMILY);
    TEST_UTIL.waitTableAvailable(tableAname, 10000);
    Table b = TEST_UTIL.createTable(tableBname, HConstants.CATALOG_FAMILY);
    TEST_UTIL.waitTableAvailable(tableBname, 10000);
    Put put = new Put(ROW);
    put.add(HConstants.CATALOG_FAMILY, null, value);
    a.put(put);

    // open a new connection to A and a connection to b
    Table newA = new HTable(TEST_UTIL.getConfiguration(), tableAname);

    // copy data from A to B
    Scan scan = new Scan();
    scan.addFamily(HConstants.CATALOG_FAMILY);
    ResultScanner s = newA.getScanner(scan);
    try {
      for (Result r : s) {
        put = new Put(r.getRow());
        put.setDurability(Durability.SKIP_WAL);
        for (Cell kv : r.rawCells()) {
          put.add(kv);
        }
        b.put(put);
      }
    } finally {
      s.close();
    }

    // Opening a new connection to A will cause the tables to be reloaded
    Table anotherA = new HTable(TEST_UTIL.getConfiguration(), tableAname);
    Get get = new Get(ROW);
    get.addFamily(HConstants.CATALOG_FAMILY);
    anotherA.get(get);

    // We can still access A through newA because it has the table information
    // cached. And if it needs to recalibrate, that will cause the information
    // to be reloaded.

    // Test user metadata
    Admin admin = new HBaseAdmin(TEST_UTIL.getConfiguration());
    // make a modifiable descriptor
    HTableDescriptor desc = new HTableDescriptor(a.getTableDescriptor());
    // offline the table
    admin.disableTable(tableAname);
    // add a user attribute to HTD
    desc.setValue(attrName, attrValue);
    // add a user attribute to HCD
    for (HColumnDescriptor c : desc.getFamilies())
      c.setValue(attrName, attrValue);
    // update metadata for all regions of this table
    admin.modifyTable(tableAname, desc);
    // enable the table
    admin.enableTable(tableAname);

    // Test that attribute changes were applied
    desc = a.getTableDescriptor();
    assertEquals("wrong table descriptor returned", desc.getTableName(), tableAname);
    // check HTD attribute
    value = desc.getValue(attrName);
    assertFalse("missing HTD attribute value", value == null);
    assertFalse("HTD attribute value is incorrect",
      Bytes.compareTo(value, attrValue) != 0);
    // check HCD attribute
    for (HColumnDescriptor c : desc.getFamilies()) {
      value = c.getValue(attrName);
      assertFalse("missing HCD attribute value", value == null);
      assertFalse("HCD attribute value is incorrect",
        Bytes.compareTo(value, attrValue) != 0);
    }
  }
```
```
Cyclomatic Complexity	 1
Assertions		 25
Lines of Code		73
-  
  public void testGetClosestRowBefore() throws IOException, InterruptedException {
    final TableName tableAname = TableName.valueOf("testGetClosestRowBefore");
    final byte[] firstRow = Bytes.toBytes("row111");
    final byte[] secondRow = Bytes.toBytes("row222");
    final byte[] thirdRow = Bytes.toBytes("row333");
    final byte[] forthRow = Bytes.toBytes("row444");
    final byte[] beforeFirstRow = Bytes.toBytes("row");
    final byte[] beforeSecondRow = Bytes.toBytes("row22");
    final byte[] beforeThirdRow = Bytes.toBytes("row33");
    final byte[] beforeForthRow = Bytes.toBytes("row44");

    HTable table =
        TEST_UTIL.createTable(tableAname,
          new byte[][] { HConstants.CATALOG_FAMILY, Bytes.toBytes("info2") }, 1,
            1024);
    TEST_UTIL.waitTableAvailable(tableAname, 10000);
    // set block size to 64 to making 2 kvs into one block, bypassing the walkForwardInSingleRow
    // in Store.rowAtOrBeforeFromStoreFile
    String regionName = table.getRegionLocations().firstKey().getEncodedName();
    Region region =
        TEST_UTIL.getRSForFirstRegionInTable(tableAname).getFromOnlineRegions(regionName);
    Put put1 = new Put(firstRow);
    Put put2 = new Put(secondRow);
    Put put3 = new Put(thirdRow);
    Put put4 = new Put(forthRow);
    byte[] one = new byte[] { 1 };
    byte[] two = new byte[] { 2 };
    byte[] three = new byte[] { 3 };
    byte[] four = new byte[] { 4 };

    put1.add(HConstants.CATALOG_FAMILY, null, one);
    put2.add(HConstants.CATALOG_FAMILY, null, two);
    put3.add(HConstants.CATALOG_FAMILY, null, three);
    put4.add(HConstants.CATALOG_FAMILY, null, four);
    table.put(put1);
    table.put(put2);
    table.put(put3);
    table.put(put4);
    region.flush(true);
    Result result = null;

    // Test before first that null is returned
    result = table.getRowOrBefore(beforeFirstRow, HConstants.CATALOG_FAMILY);
    assertTrue(result == null);

    // Test at first that first is returned
    result = table.getRowOrBefore(firstRow, HConstants.CATALOG_FAMILY);
    assertTrue(result.containsColumn(HConstants.CATALOG_FAMILY, null));
    assertTrue(Bytes.equals(result.getRow(), firstRow));
    assertTrue(Bytes.equals(result.getValue(HConstants.CATALOG_FAMILY, null), one));

    // Test in between first and second that first is returned
    result = table.getRowOrBefore(beforeSecondRow, HConstants.CATALOG_FAMILY);
    assertTrue(result.containsColumn(HConstants.CATALOG_FAMILY, null));
    assertTrue(Bytes.equals(result.getRow(), firstRow));
    assertTrue(Bytes.equals(result.getValue(HConstants.CATALOG_FAMILY, null), one));

    // Test at second make sure second is returned
    result = table.getRowOrBefore(secondRow, HConstants.CATALOG_FAMILY);
    assertTrue(result.containsColumn(HConstants.CATALOG_FAMILY, null));
    assertTrue(Bytes.equals(result.getRow(), secondRow));
    assertTrue(Bytes.equals(result.getValue(HConstants.CATALOG_FAMILY, null), two));

    // Test in second and third, make sure second is returned
    result = table.getRowOrBefore(beforeThirdRow, HConstants.CATALOG_FAMILY);
    assertTrue(result.containsColumn(HConstants.CATALOG_FAMILY, null));
    assertTrue(Bytes.equals(result.getRow(), secondRow));
    assertTrue(Bytes.equals(result.getValue(HConstants.CATALOG_FAMILY, null), two));

    // Test at third make sure third is returned
    result = table.getRowOrBefore(thirdRow, HConstants.CATALOG_FAMILY);
    assertTrue(result.containsColumn(HConstants.CATALOG_FAMILY, null));
    assertTrue(Bytes.equals(result.getRow(), thirdRow));
    assertTrue(Bytes.equals(result.getValue(HConstants.CATALOG_FAMILY, null), three));

    // Test in third and forth, make sure third is returned
    result = table.getRowOrBefore(beforeForthRow, HConstants.CATALOG_FAMILY);
    assertTrue(result.containsColumn(HConstants.CATALOG_FAMILY, null));
    assertTrue(Bytes.equals(result.getRow(), thirdRow));
    assertTrue(Bytes.equals(result.getValue(HConstants.CATALOG_FAMILY, null), three));

    // Test at forth make sure forth is returned
    result = table.getRowOrBefore(forthRow, HConstants.CATALOG_FAMILY);
    assertTrue(result.containsColumn(HConstants.CATALOG_FAMILY, null));
    assertTrue(Bytes.equals(result.getRow(), forthRow));
    assertTrue(Bytes.equals(result.getValue(HConstants.CATALOG_FAMILY, null), four));

    // Test after forth make sure forth is returned
    result = table.getRowOrBefore(Bytes.add(forthRow, one), HConstants.CATALOG_FAMILY);
    assertTrue(result.containsColumn(HConstants.CATALOG_FAMILY, null));
    assertTrue(Bytes.equals(result.getRow(), forthRow));
    assertTrue(Bytes.equals(result.getValue(HConstants.CATALOG_FAMILY, null), four));

    table.close();
  }
```
```
Cyclomatic Complexity	 1
Assertions		 3
Lines of Code		11
-  
  public void testScanVariableReuse() throws Exception {
    Scan scan = new Scan();
    scan.addFamily(FAMILY);
    scan.addColumn(FAMILY, ROW);

    assertTrue(scan.getFamilyMap().get(FAMILY).size() == 1);

    scan = new Scan();
    scan.addFamily(FAMILY);

    assertTrue(scan.getFamilyMap().get(FAMILY) == null);
    assertTrue(scan.getFamilyMap().containsKey(FAMILY));
  }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		28
-  
  public void testMultiRowMutation() throws Exception {
    LOG.info("Starting testMultiRowMutation");
    final TableName TABLENAME = TableName.valueOf("testMultiRowMutation");
    final byte [] ROW1 = Bytes.toBytes("testRow1");

    Table t = TEST_UTIL.createTable(TABLENAME, FAMILY);
    TEST_UTIL.waitTableAvailable(TABLENAME, 10000);
    Put p = new Put(ROW);
    p.add(FAMILY, QUALIFIER, VALUE);
    MutationProto m1 = ProtobufUtil.toMutation(MutationType.PUT, p);

    p = new Put(ROW1);
    p.add(FAMILY, QUALIFIER, VALUE);
    MutationProto m2 = ProtobufUtil.toMutation(MutationType.PUT, p);

    MutateRowsRequest.Builder mrmBuilder = MutateRowsRequest.newBuilder();
    mrmBuilder.addMutationRequest(m1);
    mrmBuilder.addMutationRequest(m2);
    MutateRowsRequest mrm = mrmBuilder.build();
    CoprocessorRpcChannel channel = t.coprocessorService(ROW);
    MultiRowMutationService.BlockingInterface service =
       MultiRowMutationService.newBlockingStub(channel);
    service.mutateRows(null, mrm);
    Get g = new Get(ROW);
    Result r = t.get(g);
    assertEquals(0, Bytes.compareTo(VALUE, r.getValue(FAMILY, QUALIFIER)));
    g = new Get(ROW1);
    r = t.get(g);
    assertEquals(0, Bytes.compareTo(VALUE, r.getValue(FAMILY, QUALIFIER)));
  }
```
```
Cyclomatic Complexity	 3
Assertions		 3
Lines of Code		44
-  
  public void testRowMutation() throws Exception {
    LOG.info("Starting testRowMutation");
    final TableName TABLENAME = TableName.valueOf("testRowMutation");
    Table t = TEST_UTIL.createTable(TABLENAME, FAMILY);
    TEST_UTIL.waitTableAvailable(TABLENAME, 10000);
    byte [][] QUALIFIERS = new byte [][] {
        Bytes.toBytes("a"), Bytes.toBytes("b")
    };
    RowMutations arm = new RowMutations(ROW);
    Put p = new Put(ROW);
    p.add(FAMILY, QUALIFIERS[0], VALUE);
    arm.add(p);
    t.mutateRow(arm);

    Get g = new Get(ROW);
    Result r = t.get(g);
    assertEquals(0, Bytes.compareTo(VALUE, r.getValue(FAMILY, QUALIFIERS[0])));

    arm = new RowMutations(ROW);
    p = new Put(ROW);
    p.add(FAMILY, QUALIFIERS[1], VALUE);
    arm.add(p);
    Delete d = new Delete(ROW);
    d.deleteColumns(FAMILY, QUALIFIERS[0]);
    arm.add(d);
    // TODO: Trying mutateRow again.  The batch was failing with a one try only.
    t.mutateRow(arm);
    r = t.get(g);
    assertEquals(0, Bytes.compareTo(VALUE, r.getValue(FAMILY, QUALIFIERS[1])));
    assertNull(r.getValue(FAMILY, QUALIFIERS[0]));

    //Test that we get a region level exception
    try {
      arm = new RowMutations(ROW);
      p = new Put(ROW);
      p.add(new byte[]{'b', 'o', 'g', 'u', 's'}, QUALIFIERS[0], VALUE);
      arm.add(p);
      t.mutateRow(arm);
      fail("Expected NoSuchColumnFamilyException");
    } catch(RetriesExhaustedWithDetailsException e) {
      for(Throwable rootCause: e.getCauses()){
        if(rootCause instanceof NoSuchColumnFamilyException){
          return;
        }
      }
      throw e;
    }
  }
```
```
Cyclomatic Complexity	 1
Assertions		 6
Lines of Code		27
-  
  public void testAppend() throws Exception {
    LOG.info("Starting testAppend");
    final TableName TABLENAME = TableName.valueOf("testAppend");
    Table t = TEST_UTIL.createTable(TABLENAME, FAMILY);
    TEST_UTIL.waitTableAvailable(TABLENAME, 10000);
    byte[] v1 = Bytes.toBytes("42");
    byte[] v2 = Bytes.toBytes("23");
    byte [][] QUALIFIERS = new byte [][] {
        Bytes.toBytes("b"), Bytes.toBytes("a"), Bytes.toBytes("c")
    };
    Append a = new Append(ROW);
    a.add(FAMILY, QUALIFIERS[0], v1);
    a.add(FAMILY, QUALIFIERS[1], v2);
    a.setReturnResults(false);
    assertNullResult(t.append(a));

    a = new Append(ROW);
    a.add(FAMILY, QUALIFIERS[0], v2);
    a.add(FAMILY, QUALIFIERS[1], v1);
    a.add(FAMILY, QUALIFIERS[2], v2);
    Result r = t.append(a);
    assertEquals(0, Bytes.compareTo(Bytes.add(v1,v2), r.getValue(FAMILY, QUALIFIERS[0])));
    assertEquals(0, Bytes.compareTo(Bytes.add(v2,v1), r.getValue(FAMILY, QUALIFIERS[1])));
    // QUALIFIERS[2] previously not exist, verify both value and timestamp are correct
    assertEquals(0, Bytes.compareTo(v2, r.getValue(FAMILY, QUALIFIERS[2])));
    assertEquals(r.getColumnLatest(FAMILY, QUALIFIERS[0]).getTimestamp(),
        r.getColumnLatest(FAMILY, QUALIFIERS[2]).getTimestamp());
  }
```
```
Cyclomatic Complexity	 3
Assertions		 2
Lines of Code		30
-  
  public void testClientPoolRoundRobin() throws IOException, InterruptedException {
    final TableName tableName = TableName.valueOf("testClientPoolRoundRobin");

    int poolSize = 3;
    int numVersions = poolSize * 2;
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.set(HConstants.HBASE_CLIENT_IPC_POOL_TYPE, "round-robin");
    conf.setInt(HConstants.HBASE_CLIENT_IPC_POOL_SIZE, poolSize);

    Table table = TEST_UTIL.createTable(tableName, new byte[][] { FAMILY }, conf, Integer.MAX_VALUE);
    TEST_UTIL.waitTableAvailable(tableName, 10000);

    final long ts = EnvironmentEdgeManager.currentTime();
    Get get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIER);
    get.setMaxVersions();

    for (int versions = 1; versions <= numVersions; versions++) {
      Put put = new Put(ROW);
      put.add(FAMILY, QUALIFIER, ts + versions, VALUE);
      table.put(put);

      Result result = table.get(get);
      NavigableMap<Long, byte[]> navigableMap = result.getMap().get(FAMILY)
          .get(QUALIFIER);

      assertEquals("The number of versions of '" + Bytes.toString(FAMILY) + ":"
          + Bytes.toString(QUALIFIER) + " did not match", versions, navigableMap.size());
      for (Map.Entry<Long, byte[]> entry : navigableMap.entrySet()) {
        assertTrue("The value at time " + entry.getKey()
            + " did not match what was put",
            Bytes.equals(VALUE, entry.getValue()));
      }
    }
  }
```
```
Cyclomatic Complexity	 -1
Assertions		 5
Lines of Code		71
-8989") 
  public void testClientPoolThreadLocal() throws IOException, InterruptedException {
    final TableName tableName = TableName.valueOf("testClientPoolThreadLocal");

    int poolSize = Integer.MAX_VALUE;
    int numVersions = 3;
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.set(HConstants.HBASE_CLIENT_IPC_POOL_TYPE, "thread-local");
    conf.setInt(HConstants.HBASE_CLIENT_IPC_POOL_SIZE, poolSize);

    final Table table = TEST_UTIL.createTable(tableName,
        new byte[][] { FAMILY }, conf, 3);
    TEST_UTIL.waitTableAvailable(tableName, 10000);
    final long ts = EnvironmentEdgeManager.currentTime();
    final Get get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIER);
    get.setMaxVersions();

    for (int versions = 1; versions <= numVersions; versions++) {
      Put put = new Put(ROW);
      put.add(FAMILY, QUALIFIER, ts + versions, VALUE);
      table.put(put);

      Result result = table.get(get);
      NavigableMap<Long, byte[]> navigableMap = result.getMap().get(FAMILY)
          .get(QUALIFIER);

      assertEquals("The number of versions of '" + Bytes.toString(FAMILY) + ":"
          + Bytes.toString(QUALIFIER) + " did not match", versions, navigableMap.size());
      for (Map.Entry<Long, byte[]> entry : navigableMap.entrySet()) {
        assertTrue("The value at time " + entry.getKey()
            + " did not match what was put",
            Bytes.equals(VALUE, entry.getValue()));
      }
    }

    final Object waitLock = new Object();
    ExecutorService executorService = Executors.newFixedThreadPool(numVersions);
    final AtomicReference<AssertionError> error = new AtomicReference<AssertionError>(null);
    for (int versions = numVersions; versions < numVersions * 2; versions++) {
      final int versionsCopy = versions;
      executorService.submit(new Callable<Void>() {
        @Override
        public Void call() {
          try {
            Put put = new Put(ROW);
            put.add(FAMILY, QUALIFIER, ts + versionsCopy, VALUE);
            table.put(put);

            Result result = table.get(get);
            NavigableMap<Long, byte[]> navigableMap = result.getMap()
                .get(FAMILY).get(QUALIFIER);

            assertEquals("The number of versions of '" + Bytes.toString(FAMILY) + ":"
                + Bytes.toString(QUALIFIER) + " did not match " + versionsCopy, versionsCopy,
                navigableMap.size());
            for (Map.Entry<Long, byte[]> entry : navigableMap.entrySet()) {
              assertTrue("The value at time " + entry.getKey()
                  + " did not match what was put",
                  Bytes.equals(VALUE, entry.getValue()));
            }
            synchronized (waitLock) {
              waitLock.wait();
            }
          } catch (Exception e) {
          } catch (AssertionError e) {
            // the error happens in a thread, it won't fail the test,
            // need to pass it to the caller for proper handling.
            error.set(e);
            LOG.error(e);
          }

          return null;
        }
      });
    }
    synchronized (waitLock) {
      waitLock.notifyAll();
    }
    executorService.shutdownNow();
    assertNull(error.get());
  }
```
```
Cyclomatic Complexity	 3
Assertions		 4
Lines of Code		25
-  
  public void testCheckAndPut() throws IOException, InterruptedException {
    final byte [] anotherrow = Bytes.toBytes("anotherrow");
    final byte [] value2 = Bytes.toBytes("abcd");

    Table table = TEST_UTIL.createTable(TableName.valueOf("testCheckAndPut"), FAMILY);
    TEST_UTIL.waitTableAvailable(TableName.valueOf("testCheckAndPut"), 10000);
    Put put1 = new Put(ROW);
    put1.add(FAMILY, QUALIFIER, VALUE);

    // row doesn't exist, so using non-null value should be considered "not match".
    boolean ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, VALUE, put1);
    assertEquals(ok, false);

    // row doesn't exist, so using "null" to check for existence should be considered "match".
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, null, put1);
    assertEquals(ok, true);

    // row now exists, so using "null" to check for existence should be considered "not match".
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, null, put1);
    assertEquals(ok, false);

    Put put2 = new Put(ROW);
    put2.add(FAMILY, QUALIFIER, value2);

    // row now exists, use the matching value to check
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, VALUE, put2);
    assertEquals(ok, true);

    Put put3 = new Put(anotherrow);
    put3.add(FAMILY, QUALIFIER, VALUE);

    // try to do CheckAndPut on different rows
    try {
        ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, value2, put3);
        fail("trying to check and modify different rows should have failed.");
    } catch(Exception e) {}
  }
```
```
Cyclomatic Complexity	 2
Assertions		 19
Lines of Code		51
-  
  public void testCheckAndPutWithCompareOp() throws IOException, InterruptedException {
    final byte [] value1 = Bytes.toBytes("aaaa");
    final byte [] value2 = Bytes.toBytes("bbbb");
    final byte [] value3 = Bytes.toBytes("cccc");
    final byte [] value4 = Bytes.toBytes("dddd");

    Table table = TEST_UTIL.createTable(TableName.valueOf("testCheckAndPutWithCompareOp"), FAMILY);
    TEST_UTIL.waitTableAvailable(TableName.valueOf("testCheckAndPutWithCompareOp"), 10000);

    Put put2 = new Put(ROW);
    put2.add(FAMILY, QUALIFIER, value2);

    Put put3 = new Put(ROW);
    put3.add(FAMILY, QUALIFIER, value3);

    // row doesn't exist, so using "null" to check for existence should be considered "match".
    boolean ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, null, put2);
    assertEquals(ok, true);

    // cell = "bbbb", using "aaaa" to compare only LESS/LESS_OR_EQUAL/NOT_EQUAL
    // turns out "match"
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, CompareOp.GREATER, value1, put2);
    assertEquals(ok, false);
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, CompareOp.EQUAL, value1, put2);
    assertEquals(ok, false);
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, CompareOp.GREATER_OR_EQUAL, value1, put2);
    assertEquals(ok, false);
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, CompareOp.LESS, value1, put2);
    assertEquals(ok, true);
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, CompareOp.LESS_OR_EQUAL, value1, put2);
    assertEquals(ok, true);
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, CompareOp.NOT_EQUAL, value1, put3);
    assertEquals(ok, true);

    // cell = "cccc", using "dddd" to compare only LARGER/LARGER_OR_EQUAL/NOT_EQUAL
    // turns out "match"
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, CompareOp.LESS, value4, put3);
    assertEquals(ok, false);
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, CompareOp.LESS_OR_EQUAL, value4, put3);
    assertEquals(ok, false);
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, CompareOp.EQUAL, value4, put3);
    assertEquals(ok, false);
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, CompareOp.GREATER, value4, put3);
    assertEquals(ok, true);
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, CompareOp.GREATER_OR_EQUAL, value4, put3);
    assertEquals(ok, true);
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, CompareOp.NOT_EQUAL, value4, put2);
    assertEquals(ok, true);

    // cell = "bbbb", using "bbbb" to compare only GREATER_OR_EQUAL/LESS_OR_EQUAL/EQUAL
    // turns out "match"
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, CompareOp.GREATER, value2, put2);
    assertEquals(ok, false);
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, CompareOp.NOT_EQUAL, value2, put2);
    assertEquals(ok, false);
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, CompareOp.LESS, value2, put2);
    assertEquals(ok, false);
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, CompareOp.GREATER_OR_EQUAL, value2, put2);
    assertEquals(ok, true);
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, CompareOp.LESS_OR_EQUAL, value2, put2);
    assertEquals(ok, true);
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, CompareOp.EQUAL, value2, put3);
    assertEquals(ok, true);
  }
```
```
Cyclomatic Complexity	 1
Assertions		 18
Lines of Code		60
-  
  public void testCheckAndDeleteWithCompareOp() throws IOException, InterruptedException {
    final byte [] value1 = Bytes.toBytes("aaaa");
    final byte [] value2 = Bytes.toBytes("bbbb");
    final byte [] value3 = Bytes.toBytes("cccc");
    final byte [] value4 = Bytes.toBytes("dddd");
    Table table = TEST_UTIL.createTable(TableName.valueOf("testCheckAndDeleteWithCompareOp"),
        FAMILY);
    TEST_UTIL.waitTableAvailable(TableName.valueOf("testCheckAndDeleteWithCompareOp"), 10000);

    Put put2 = new Put(ROW);
    put2.add(FAMILY, QUALIFIER, value2);
    table.put(put2);

    Put put3 = new Put(ROW);
    put3.add(FAMILY, QUALIFIER, value3);

    Delete delete = new Delete(ROW);
    delete.deleteColumns(FAMILY, QUALIFIER);

    // cell = "bbbb", using "aaaa" to compare only LESS/LESS_OR_EQUAL/NOT_EQUAL
    // turns out "match"
    boolean ok = table.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareOp.GREATER, value1, delete);
    assertEquals(ok, false);
    ok = table.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareOp.EQUAL, value1, delete);
    assertEquals(ok, false);
    ok = table.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareOp.GREATER_OR_EQUAL, value1, delete);
    assertEquals(ok, false);
    ok = table.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareOp.LESS, value1, delete);
    assertEquals(ok, true);
    table.put(put2);
    ok = table.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareOp.LESS_OR_EQUAL, value1, delete);
    assertEquals(ok, true);
    table.put(put2);

    assertEquals(ok, true);

    // cell = "cccc", using "dddd" to compare only LARGER/LARGER_OR_EQUAL/NOT_EQUAL
    // turns out "match"
    table.put(put3);
    ok = table.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareOp.LESS, value4, delete);

    assertEquals(ok, false);
    ok = table.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareOp.LESS_OR_EQUAL, value4, delete);

    assertEquals(ok, false);
    ok = table.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareOp.EQUAL, value4, delete);

    assertEquals(ok, false);
    ok = table.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareOp.GREATER, value4, delete);

    assertEquals(ok, true);
    table.put(put3);
    ok = table.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareOp.GREATER_OR_EQUAL, value4, delete);
    assertEquals(ok, true);
    table.put(put3);
    ok = table.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareOp.NOT_EQUAL, value4, delete);

    assertEquals(ok, true);

    // cell = "bbbb", using "bbbb" to compare only GREATER_OR_EQUAL/LESS_OR_EQUAL/EQUAL
    // turns out "match"
    table.put(put2);
    ok = table.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareOp.GREATER, value2, delete);
    assertEquals(ok, false);
    ok = table.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareOp.NOT_EQUAL, value2, delete);
    assertEquals(ok, false);
    ok = table.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareOp.LESS, value2, delete);
    assertEquals(ok, false);
    ok = table.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareOp.GREATER_OR_EQUAL, value2, delete);
    assertEquals(ok, true);
    table.put(put2);
    ok = table.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareOp.LESS_OR_EQUAL, value2, delete);
    assertEquals(ok, true);
    table.put(put2);
    ok = table.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareOp.EQUAL, value2, delete);
    assertEquals(ok, true);
  }
```
```
Cyclomatic Complexity	 3
Assertions		 29
Lines of Code		82
-  
  public void testCacheOnWriteEvictOnClose() throws Exception {
    TableName tableName = TableName.valueOf("testCOWEOCfromClient");
    byte [] data = Bytes.toBytes("data");
    HTable table = TEST_UTIL.createTable(tableName, FAMILY);
    TEST_UTIL.waitTableAvailable(tableName, 10000);
    // get the block cache and region
    String regionName = table.getRegionLocations().firstKey().getEncodedName();
    Region region = TEST_UTIL.getRSForFirstRegionInTable(tableName)
      .getFromOnlineRegions(regionName);
    Store store = region.getStores().iterator().next();
    CacheConfig cacheConf = store.getCacheConfig();
    cacheConf.setCacheDataOnWrite(true);
    cacheConf.setEvictOnClose(true);
    BlockCache cache = cacheConf.getBlockCache();

    // establish baseline stats
    long startBlockCount = cache.getBlockCount();
    long startBlockHits = cache.getStats().getHitCount();
    long startBlockMiss = cache.getStats().getMissCount();

    // wait till baseline is stable, (minimal 500 ms)
    for (int i = 0; i < 5; i++) {
      Thread.sleep(100);
      if (startBlockCount != cache.getBlockCount()
          || startBlockHits != cache.getStats().getHitCount()
          || startBlockMiss != cache.getStats().getMissCount()) {
        startBlockCount = cache.getBlockCount();
        startBlockHits = cache.getStats().getHitCount();
        startBlockMiss = cache.getStats().getMissCount();
        i = -1;
      }
    }

    // insert data
    Put put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, data);
    table.put(put);
    assertTrue(Bytes.equals(table.get(new Get(ROW)).value(), data));
    // data was in memstore so don't expect any changes
    assertEquals(startBlockCount, cache.getBlockCount());
    assertEquals(startBlockHits, cache.getStats().getHitCount());
    assertEquals(startBlockMiss, cache.getStats().getMissCount());
    // flush the data
    System.out.println("Flushing cache");
    region.flush(true);
    // expect one more block in cache, no change in hits/misses
    long expectedBlockCount = startBlockCount + 1;
    long expectedBlockHits = startBlockHits;
    long expectedBlockMiss = startBlockMiss;
    assertEquals(expectedBlockCount, cache.getBlockCount());
    assertEquals(expectedBlockHits, cache.getStats().getHitCount());
    assertEquals(expectedBlockMiss, cache.getStats().getMissCount());
    // read the data and expect same blocks, one new hit, no misses
    assertTrue(Bytes.equals(table.get(new Get(ROW)).value(), data));
    assertEquals(expectedBlockCount, cache.getBlockCount());
    assertEquals(++expectedBlockHits, cache.getStats().getHitCount());
    assertEquals(expectedBlockMiss, cache.getStats().getMissCount());
    // insert a second column, read the row, no new blocks, one new hit
    byte [] QUALIFIER2 = Bytes.add(QUALIFIER, QUALIFIER);
    byte [] data2 = Bytes.add(data, data);
    put = new Put(ROW);
    put.add(FAMILY, QUALIFIER2, data2);
    table.put(put);
    Result r = table.get(new Get(ROW));
    assertTrue(Bytes.equals(r.getValue(FAMILY, QUALIFIER), data));
    assertTrue(Bytes.equals(r.getValue(FAMILY, QUALIFIER2), data2));
    assertEquals(expectedBlockCount, cache.getBlockCount());
    assertEquals(++expectedBlockHits, cache.getStats().getHitCount());
    assertEquals(expectedBlockMiss, cache.getStats().getMissCount());
    // flush, one new block
    System.out.println("Flushing cache");
    region.flush(true);
    assertEquals(++expectedBlockCount, cache.getBlockCount());
    assertEquals(expectedBlockHits, cache.getStats().getHitCount());
    assertEquals(expectedBlockMiss, cache.getStats().getMissCount());
    // compact, net minus two blocks, two hits, no misses
    System.out.println("Compacting");
    assertEquals(2, store.getStorefilesCount());
    store.triggerMajorCompaction();
    region.compact(true);
    waitForStoreFileCount(store, 1, 10000); // wait 10 seconds max
    assertEquals(1, store.getStorefilesCount());
    expectedBlockCount -= 2; // evicted two blocks, cached none
    assertEquals(expectedBlockCount, cache.getBlockCount());
    expectedBlockHits += 2;
    assertEquals(expectedBlockMiss, cache.getStats().getMissCount());
    assertEquals(expectedBlockHits, cache.getStats().getHitCount());
    // read the row, this should be a cache miss because we don't cache data
    // blocks on compaction
    r = table.get(new Get(ROW));
    assertTrue(Bytes.equals(r.getValue(FAMILY, QUALIFIER), data));
    assertTrue(Bytes.equals(r.getValue(FAMILY, QUALIFIER2), data2));
    expectedBlockCount += 1; // cached one data block
    assertEquals(expectedBlockCount, cache.getBlockCount());
    assertEquals(expectedBlockHits, cache.getStats().getHitCount());
    assertEquals(++expectedBlockMiss, cache.getStats().getMissCount());
  }
```
```
Cyclomatic Complexity	 4
Assertions		 6
Lines of Code		35
-  
  /**
   * Tests the non cached version of getRegionLocator by moving a region.
   */
  public void testNonCachedGetRegionLocation() throws Exception {
    // Test Initialization.
    TableName TABLE = TableName.valueOf("testNonCachedGetRegionLocation");
    byte [] family1 = Bytes.toBytes("f1");
    byte [] family2 = Bytes.toBytes("f2");
    try (HTable table = TEST_UTIL.createTable(TABLE, new byte[][] {family1, family2}, 10);
        Admin admin = new HBaseAdmin(TEST_UTIL.getConfiguration())) {
      TEST_UTIL.waitTableAvailable(TABLE, 10000);
      Map <HRegionInfo, ServerName> regionsMap = table.getRegionLocations();
      assertEquals(1, regionsMap.size());
      HRegionInfo regionInfo = regionsMap.keySet().iterator().next();
      ServerName addrBefore = regionsMap.get(regionInfo);
      // Verify region location before move.
      HRegionLocation addrCache = table.getRegionLocation(regionInfo.getStartKey(), false);
      HRegionLocation addrNoCache = table.getRegionLocation(regionInfo.getStartKey(),  true);

      assertEquals(addrBefore.getPort(), addrCache.getPort());
      assertEquals(addrBefore.getPort(), addrNoCache.getPort());

      ServerName addrAfter = null;
      // Now move the region to a different server.
      for (int i = 0; i < SLAVES; i++) {
        HRegionServer regionServer = TEST_UTIL.getHBaseCluster().getRegionServer(i);
        ServerName addr = regionServer.getServerName();
        if (addr.getPort() != addrBefore.getPort()) {
          admin.move(regionInfo.getEncodedNameAsBytes(),
              Bytes.toBytes(addr.toString()));
          // Wait for the region to move.
          Thread.sleep(5000);
          addrAfter = addr;
          break;
        }
      }

      // Verify the region was moved.
      addrCache = table.getRegionLocation(regionInfo.getStartKey(), false);
      addrNoCache = table.getRegionLocation(regionInfo.getStartKey(), true);
      assertNotNull(addrAfter);
      assertTrue(addrAfter.getPort() != addrCache.getPort());
      assertEquals(addrAfter.getPort(), addrNoCache.getPort());
    }
  }
```
```
Cyclomatic Complexity	 1
Assertions		 10
Lines of Code		37
-  
  /**
   * Tests getRegionsInRange by creating some regions over which a range of
   * keys spans; then changing the key range.
   */
  public void testGetRegionsInRange() throws Exception {
    // Test Initialization.
    byte [] startKey = Bytes.toBytes("ddc");
    byte [] endKey = Bytes.toBytes("mmm");
    TableName TABLE = TableName.valueOf("testGetRegionsInRange");
    HTable table = TEST_UTIL.createMultiRegionTable(TABLE, new byte[][] { FAMILY }, 10);
    int numOfRegions = -1;
    try (RegionLocator r = table.getRegionLocator()) {
      numOfRegions = r.getStartKeys().length;
    }
    assertEquals(26, numOfRegions);

    // Get the regions in this range
    List<HRegionLocation> regionsList = table.getRegionsInRange(startKey,
      endKey);
    assertEquals(10, regionsList.size());

    // Change the start key
    startKey = Bytes.toBytes("fff");
    regionsList = table.getRegionsInRange(startKey, endKey);
    assertEquals(7, regionsList.size());

    // Change the end key
    endKey = Bytes.toBytes("nnn");
    regionsList = table.getRegionsInRange(startKey, endKey);
    assertEquals(8, regionsList.size());

    // Empty start key
    regionsList = table.getRegionsInRange(HConstants.EMPTY_START_ROW, endKey);
    assertEquals(13, regionsList.size());

    // Empty end key
    regionsList = table.getRegionsInRange(startKey, HConstants.EMPTY_END_ROW);
    assertEquals(21, regionsList.size());

    // Both start and end keys empty
    regionsList = table.getRegionsInRange(HConstants.EMPTY_START_ROW,
      HConstants.EMPTY_END_ROW);
    assertEquals(26, regionsList.size());

    // Change the end key to somewhere in the last block
    endKey = Bytes.toBytes("zzz1");
    regionsList = table.getRegionsInRange(startKey, endKey);
    assertEquals(21, regionsList.size());

    // Change the start key to somewhere in the first block
    startKey = Bytes.toBytes("aac");
    regionsList = table.getRegionsInRange(startKey, endKey);
    assertEquals(26, regionsList.size());

    // Make start and end key the same
    startKey = endKey = Bytes.toBytes("ccc");
    regionsList = table.getRegionsInRange(startKey, endKey);
    assertEquals(1, regionsList.size());
  }
```
```
Cyclomatic Complexity	 2
Assertions		 1
Lines of Code		22
-  
  public void testJira6912() throws Exception {
    TableName TABLE = TableName.valueOf("testJira6912");
    Table foo = TEST_UTIL.createTable(TABLE, new byte[][] {FAMILY}, 10);
    TEST_UTIL.waitTableAvailable(TABLE, 10000);
    List<Put> puts = new ArrayList<Put>();
    for (int i=0;i !=100; i++){
      Put put = new Put(Bytes.toBytes(i));
      put.add(FAMILY, FAMILY, Bytes.toBytes(i));
      puts.add(put);
    }
    foo.put(puts);
    // If i comment this out it works
    TEST_UTIL.flush();

    Scan scan = new Scan();
    scan.setStartRow(Bytes.toBytes(1));
    scan.setStopRow(Bytes.toBytes(3));
    scan.addColumn(FAMILY, FAMILY);
    scan.setFilter(new RowFilter(CompareFilter.CompareOp.NOT_EQUAL, new BinaryComparator(Bytes.toBytes(1))));

    ResultScanner scanner = foo.getScanner(scan);
    Result[] bar = scanner.next(100);
    assertEquals(1, bar.length);
  }
```
```
Cyclomatic Complexity	 1
Assertions		 4
Lines of Code		24
-  
  public void testScan_NullQualifier() throws IOException, InterruptedException {
    Table table = TEST_UTIL.createTable(TableName.valueOf("testScan_NullQualifier"), FAMILY);
    TEST_UTIL.waitTableAvailable(TableName.valueOf("testScan_NullQualifier"), 10000);

    Put put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, VALUE);
    table.put(put);

    put = new Put(ROW);
    put.add(FAMILY, null, VALUE);
    table.put(put);
    LOG.info("Row put");

    Scan scan = new Scan();
    scan.addColumn(FAMILY, null);

    ResultScanner scanner = table.getScanner(scan);
    Result[] bar = scanner.next(100);
    assertEquals(1, bar.length);
    assertEquals(1, bar[0].size());

    scan = new Scan();
    scan.addFamily(FAMILY);

    scanner = table.getScanner(scan);
    bar = scanner.next(100);
    assertEquals(1, bar.length);
    assertEquals(2, bar[0].size());
  }
```
```
Cyclomatic Complexity	 2
Assertions		 2
Lines of Code		81
-  
  public void testIllegalTableDescriptor() throws Exception {
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf("testIllegalTableDescriptor"));
    HColumnDescriptor hcd = new HColumnDescriptor(FAMILY);

    // create table with 0 families
    checkTableIsIllegal(htd);
    htd.addFamily(hcd);
    checkTableIsLegal(htd);

    htd.setMaxFileSize(1024); // 1K
    checkTableIsIllegal(htd);
    htd.setMaxFileSize(0);
    checkTableIsIllegal(htd);
    htd.setMaxFileSize(1024 * 1024 * 1024); // 1G
    checkTableIsLegal(htd);

    htd.setMemStoreFlushSize(1024);
    checkTableIsIllegal(htd);
    htd.setMemStoreFlushSize(0);
    checkTableIsIllegal(htd);
    htd.setMemStoreFlushSize(128 * 1024 * 1024); // 128M
    checkTableIsLegal(htd);

    htd.setRegionSplitPolicyClassName("nonexisting.foo.class");
    checkTableIsIllegal(htd);
    htd.setRegionSplitPolicyClassName(null);
    checkTableIsLegal(htd);

    hcd.setBlocksize(0);
    checkTableIsIllegal(htd);
    hcd.setBlocksize(1024 * 1024 * 128); // 128M
    checkTableIsIllegal(htd);
    hcd.setBlocksize(1024);
    checkTableIsLegal(htd);

    hcd.setTimeToLive(0);
    checkTableIsIllegal(htd);
    hcd.setTimeToLive(-1);
    checkTableIsIllegal(htd);
    hcd.setTimeToLive(1);
    checkTableIsLegal(htd);

    hcd.setMinVersions(-1);
    checkTableIsIllegal(htd);
    hcd.setMinVersions(3);
    try {
      hcd.setMaxVersions(2);
      fail();
    } catch (IllegalArgumentException ex) {
      // expected
      hcd.setMaxVersions(10);
    }
    checkTableIsLegal(htd);

    // HBASE-13776 Setting illegal versions for HColumnDescriptor
    //  does not throw IllegalArgumentException
    // finally, minVersions must be less than or equal to maxVersions
    hcd.setMaxVersions(4);
    hcd.setMinVersions(5);
    checkTableIsIllegal(htd);
    hcd.setMinVersions(3);

    try {
      hcd.setScope(-1);
      fail("Illegal value for setScope did not throw");
    } catch (IllegalArgumentException e) {
      // expected
      hcd.setScope(0);
    }
    checkTableIsLegal(htd);

    try {
      hcd.setDFSReplication((short) -1);
      fail("Illegal value for setDFSReplication did not throw");
    } catch (IllegalArgumentException e) {
      // pass
    }
    // set an illegal DFS replication value by hand
    hcd.setValue(HColumnDescriptor.DFS_REPLICATION, "-1");
    checkTableIsIllegal(htd);
    try {
      hcd.setDFSReplication((short) -1);
      fail("Should throw exception if an illegal value is explicitly being set");
    } catch (IllegalArgumentException e) {
      // pass
    }

    // check the conf settings to disable sanity checks
    htd.setMemStoreFlushSize(0);

    // Check that logs warn on invalid table but allow it.
    ListAppender listAppender = new ListAppender();
    Logger log = Logger.getLogger(HMaster.class);
    log.addAppender(listAppender);
    log.setLevel(Level.WARN);

    htd.setConfiguration("hbase.table.sanity.checks", Boolean.FALSE.toString());
    checkTableIsLegal(htd);

    assertFalse(listAppender.getMessages().isEmpty());
    assertTrue(listAppender.getMessages().get(0).startsWith("MEMSTORE_FLUSHSIZE for table "
        + "descriptor or \"hbase.hregion.memstore.flush.size\" (0) is too small, which might "
        + "cause very frequent flushing."));

    log.removeAppender(listAppender);
  }
```
```
Cyclomatic Complexity	 2
Assertions		 0
Lines of Code		21
-  
  public void testGetRegionLocationFromPrimaryMetaRegion() throws IOException, InterruptedException {
    HTU.getHBaseAdmin().setBalancerRunning(false, true);

    ((ConnectionManager.HConnectionImplementation) HTU.getHBaseAdmin().getConnection()).
        setUseMetaReplicas(true);

    // Create table then get the single region for our new table.
    HTableDescriptor hdt = HTU.createTableDescriptor("testGetRegionLocationFromPrimaryMetaRegion");
    hdt.setRegionReplication(2);
    try {

      HTU.createTable(hdt, new byte[][] { f }, null);

      RegionServerHostingPrimayMetaRegionSlowOrStopCopro.slowDownPrimaryMetaScan = true;

      // Get user table location, always get it from the primary meta replica
      RegionLocations url = ((ClusterConnection) HTU.getConnection())
          .locateRegion(hdt.getTableName(), row, false, false);

    } finally {
      RegionServerHostingPrimayMetaRegionSlowOrStopCopro.slowDownPrimaryMetaScan = false;
      ((ConnectionManager.HConnectionImplementation) HTU.getHBaseAdmin().getConnection()).
          setUseMetaReplicas(false);
      HTU.getHBaseAdmin().setBalancerRunning(true, true);
      HTU.getHBaseAdmin().disableTable(hdt.getTableName());
      HTU.deleteTable(hdt.getTableName());
    }
  }
```
```
Cyclomatic Complexity	 7
Assertions		 2
Lines of Code		66
-  
  public void testReplicaGetWithPrimaryAndMetaDown() throws IOException, InterruptedException {
    HTU.getHBaseAdmin().setBalancerRunning(false, true);

    ((ConnectionManager.HConnectionImplementation)HTU.getHBaseAdmin().getConnection()).
        setUseMetaReplicas(true);

    // Create table then get the single region for our new table.
    HTableDescriptor hdt = HTU.createTableDescriptor("testReplicaGetWithPrimaryAndMetaDown");
    hdt.setRegionReplication(2);
    try {

      Table table = HTU.createTable(hdt, new byte[][] { f }, null);

      // Get Meta location
      RegionLocations mrl = ((ClusterConnection) HTU.getConnection())
          .locateRegion(TableName.META_TABLE_NAME,
              HConstants.EMPTY_START_ROW, false, false);

      // Get user table location
      RegionLocations url = ((ClusterConnection) HTU.getConnection())
          .locateRegion(hdt.getTableName(), row, false, false);

      // Make sure that user primary region is co-hosted with the meta region
      if (!url.getDefaultRegionLocation().getServerName().equals(
          mrl.getDefaultRegionLocation().getServerName())) {
        HTU.moveRegionAndWait(url.getDefaultRegionLocation().getRegionInfo(),
            mrl.getDefaultRegionLocation().getServerName());
      }

      // Make sure that the user replica region is not hosted by the same region server with
      // primary
      if (url.getRegionLocation(1).getServerName().equals(mrl.getDefaultRegionLocation()
          .getServerName())) {
        HTU.moveRegionAndWait(url.getRegionLocation(1).getRegionInfo(),
            url.getDefaultRegionLocation().getServerName());
      }

      // Wait until the meta table is updated with new location info
      while (true) {
        mrl = ((ClusterConnection) HTU.getConnection())
            .locateRegion(TableName.META_TABLE_NAME, HConstants.EMPTY_START_ROW, false, false);

        // Get user table location
        url = ((ClusterConnection) HTU.getConnection())
            .locateRegion(hdt.getTableName(), row, false, true);

        LOG.info("meta locations " + mrl);
        LOG.info("table locations " + url);
        ServerName a = url.getDefaultRegionLocation().getServerName();
        ServerName b = mrl.getDefaultRegionLocation().getServerName();
        if(a.equals(b)) {
          break;
        } else {
          LOG.info("Waiting for new region info to be updated in meta table");
          Thread.sleep(100);
        }
      }

      Put p = new Put(row);
      p.addColumn(f, row, row);
      table.put(p);

      // Flush so it can be picked by the replica refresher thread
      HTU.flush(table.getName());

      // Sleep for some time until data is picked up by replicas
      try {
        Thread.sleep(2 * REFRESH_PERIOD);
      } catch (InterruptedException e1) {
        LOG.error(e1);
      }

      // Simulating the RS down
      RegionServerHostingPrimayMetaRegionSlowOrStopCopro.throwException = true;

      // The first Get is supposed to succeed
      Get g = new Get(row);
      g.setConsistency(Consistency.TIMELINE);
      Result r = table.get(g);
      Assert.assertTrue(r.isStale());

      // The second Get will succeed as well
      r = table.get(g);
      Assert.assertTrue(r.isStale());

    } finally {
      ((ConnectionManager.HConnectionImplementation)HTU.getHBaseAdmin().getConnection()).
          setUseMetaReplicas(false);
      RegionServerHostingPrimayMetaRegionSlowOrStopCopro.throwException = false;
      HTU.getHBaseAdmin().setBalancerRunning(true, true);
      HTU.getHBaseAdmin().disableTable(hdt.getTableName());@@@+      HTU.getAdmin().disableTable(hdt.getTableName());@@@       HTU.deleteTable(hdt.getTableName());@@@     }@@@   }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		5
-  
  public void testLegacyWALObserverWriteToWAL() throws Exception {
    final WAL log = wals.getWAL(UNSPECIFIED_REGION, null);
    verifyWritesSeen(log, getCoprocessor(log, SampleRegionWALObserver.Legacy.class), true);
  }
```
```
Cyclomatic Complexity	 2
Assertions		 24
Lines of Code		64
-  
  public void testNonLegacyWALKeysDoNotExplode() throws Exception {
    TableName tableName = TableName.valueOf(TEST_TABLE);
    final HTableDescriptor htd = createBasic3FamilyHTD(Bytes
        .toString(TEST_TABLE));
    final HRegionInfo hri = new HRegionInfo(tableName, null, null);
    MultiVersionConcurrencyControl mvcc = new MultiVersionConcurrencyControl();

    fs.mkdirs(new Path(FSUtils.getTableDir(hbaseRootDir, tableName), hri.getEncodedName()));

    final Configuration newConf = HBaseConfiguration.create(this.conf);

    final WAL wal = wals.getWAL(UNSPECIFIED_REGION, null);
    final SampleRegionWALObserver newApi = getCoprocessor(wal, SampleRegionWALObserver.class);
    newApi.setTestValues(TEST_TABLE, TEST_ROW, null, null, null, null, null, null);
    final SampleRegionWALObserver oldApi = getCoprocessor(wal,
        SampleRegionWALObserver.Legacy.class);
    oldApi.setTestValues(TEST_TABLE, TEST_ROW, null, null, null, null, null, null);

    LOG.debug("ensuring wal entries haven't happened before we start");
    assertFalse(newApi.isPreWALWriteCalled());
    assertFalse(newApi.isPostWALWriteCalled());
    assertFalse(newApi.isPreWALWriteDeprecatedCalled());
    assertFalse(newApi.isPostWALWriteDeprecatedCalled());
    assertFalse(oldApi.isPreWALWriteCalled());
    assertFalse(oldApi.isPostWALWriteCalled());
    assertFalse(oldApi.isPreWALWriteDeprecatedCalled());
    assertFalse(oldApi.isPostWALWriteDeprecatedCalled());

    LOG.debug("writing to WAL with non-legacy keys.");
    final int countPerFamily = 5;
    for (HColumnDescriptor hcd : htd.getFamilies()) {
      addWALEdits(tableName, hri, TEST_ROW, hcd.getName(), countPerFamily,
          EnvironmentEdgeManager.getDelegate(), wal, htd, mvcc);
    }

    LOG.debug("Verify that only the non-legacy CP saw edits.");
    assertTrue(newApi.isPreWALWriteCalled());
    assertTrue(newApi.isPostWALWriteCalled());
    assertFalse(newApi.isPreWALWriteDeprecatedCalled());
    assertFalse(newApi.isPostWALWriteDeprecatedCalled());
    // wish we could test that the log message happened :/
    assertFalse(oldApi.isPreWALWriteCalled());
    assertFalse(oldApi.isPostWALWriteCalled());
    assertFalse(oldApi.isPreWALWriteDeprecatedCalled());
    assertFalse(oldApi.isPostWALWriteDeprecatedCalled());

    LOG.debug("reseting cp state.");
    newApi.setTestValues(TEST_TABLE, TEST_ROW, null, null, null, null, null, null);
    oldApi.setTestValues(TEST_TABLE, TEST_ROW, null, null, null, null, null, null);

    LOG.debug("write a log edit that supports legacy cps.");
    final long now = EnvironmentEdgeManager.currentTime();
    final WALKey legacyKey = new HLogKey(hri.getEncodedNameAsBytes(), hri.getTable(), now, mvcc);
    final WALEdit edit = new WALEdit();
    final byte[] nonce = Bytes.toBytes("1772");
    edit.add(new KeyValue(TEST_ROW, TEST_FAMILY[0], nonce, now, nonce));
    final long txid = wal.append(htd, hri, legacyKey, edit, true);
    wal.sync(txid);

    LOG.debug("Make sure legacy cps can see supported edits after having been skipped.");
    assertTrue("non-legacy WALObserver didn't see pre-write.", newApi.isPreWALWriteCalled());
    assertTrue("non-legacy WALObserver didn't see post-write.", newApi.isPostWALWriteCalled());
    assertFalse("non-legacy WALObserver shouldn't have seen legacy pre-write.",
        newApi.isPreWALWriteDeprecatedCalled());
    assertFalse("non-legacy WALObserver shouldn't have seen legacy post-write.",
        newApi.isPostWALWriteDeprecatedCalled());
    assertTrue("legacy WALObserver didn't see pre-write.", oldApi.isPreWALWriteCalled());
    assertTrue("legacy WALObserver didn't see post-write.", oldApi.isPostWALWriteCalled());
    assertTrue("legacy WALObserver didn't see legacy pre-write.",
        oldApi.isPreWALWriteDeprecatedCalled());
    assertTrue("legacy WALObserver didn't see legacy post-write.",
        oldApi.isPostWALWriteDeprecatedCalled());@@@   }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		22
-  
  public void testRowIndexWithTagsButNoTagsInCell() throws IOException {
    List<KeyValue> kvList = new ArrayList<>();
    byte[] row = new byte[0];
    byte[] family = new byte[0];
    byte[] qualifier = new byte[0];
    byte[] value = new byte[0];
    KeyValue expectedKV = new KeyValue(row, family, qualifier, -1L, Type.Put, value);
    kvList.add(expectedKV);
    DataBlockEncoding encoding = DataBlockEncoding.ROW_INDEX_V1;
    DataBlockEncoder encoder = encoding.getEncoder();
    ByteBuffer encodedBuffer =
        encodeKeyValues(encoding, kvList, getEncodingContext(Algorithm.NONE, encoding));
    HFileContext meta =
        new HFileContextBuilder().withHBaseCheckSum(false).withIncludesMvcc(includesMemstoreTS)
            .withIncludesTags(includesTags).withCompression(Compression.Algorithm.NONE).build();
    DataBlockEncoder.EncodedSeeker seeker =
        encoder.createSeeker(KeyValue.COMPARATOR, encoder.newDataBlockDecodingContext(meta));
    seeker.setCurrentBuffer(encodedBuffer);
    Cell cell = seeker.getKeyValue();
    Assert.assertEquals(expectedKV.getLength(), ((KeyValue) cell).getLength());
  }
```
```
Cyclomatic Complexity	 2
Assertions		 4
Lines of Code		27
-  
  public void compatibilityTest() throws Exception {
    HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
    Path testDir = TEST_UTIL.getDataTestDir();
    TEST_UTIL.getTestFileSystem().mkdirs(testDir);
    String persistencePath = testDir + "/bucket.persistence";
    BucketCache bucketCache =
      new BucketCache("file:" + testDir + "/bucket.cache", capacitySize, constructedBlockSize,
        null, writeThreads, writerQLen, persistencePath);
    long usedSize = bucketCache.getAllocator().getUsedSize();
    assertTrue(usedSize == 0);

    CacheTestUtils.HFileBlockPair[] blocks =
      CacheTestUtils.generateHFileBlocks(constructedBlockSize, 1);
    // Add blocks
    for (CacheTestUtils.HFileBlockPair block : blocks) {
      cacheAndWaitUntilFlushedToBucket(bucketCache, block.getBlockName(), block.getBlock());
    }
    usedSize = bucketCache.getAllocator().getUsedSize();
    assertTrue(usedSize != 0);
    // persistence backingMap using old way
    persistToFileInOldWay(persistencePath + ".old", bucketCache.getMaxSize(),
      bucketCache.backingMap, bucketCache.getDeserialiserMap());
    bucketCache.shutdown();

    // restore cache from file which skip check checksum
    bucketCache =
      new BucketCache("file:" + testDir + "/bucket.cache", capacitySize, constructedBlockSize,
        null, writeThreads, writerQLen, persistencePath + ".old");
    assertEquals(usedSize, bucketCache.getAllocator().getUsedSize());
    assertEquals(blocks.length, bucketCache.backingMap.size());
  }
```
```
Cyclomatic Complexity	 2
Assertions		 4
Lines of Code		54
-  
  public void testReplicaCostForReplicas() {
    Configuration conf = HBaseConfiguration.create();
    StochasticLoadBalancer.CostFunction
        costFunction = new StochasticLoadBalancer.RegionReplicaHostCostFunction(conf);

    int [] servers = new int[] {3,3,3,3,3};
    TreeMap<ServerName, List<HRegionInfo>> clusterState = mockClusterServers(servers);

    BaseLoadBalancer.Cluster cluster;

    cluster = new BaseLoadBalancer.Cluster(clusterState, null, null, null);
    costFunction.init(cluster);
    double costWithoutReplicas = costFunction.cost();
    assertEquals(0, costWithoutReplicas, 0);

    // replicate the region from first server to the last server
    HRegionInfo replica1 = RegionReplicaUtil.getRegionInfoForReplica(
      clusterState.firstEntry().getValue().get(0),1);
    clusterState.lastEntry().getValue().add(replica1);

    cluster = new BaseLoadBalancer.Cluster(clusterState, null, null, null);
    costFunction.init(cluster);
    double costWith1ReplicaDifferentServer = costFunction.cost();

    assertEquals(0, costWith1ReplicaDifferentServer, 0);

    // add a third replica to the last server
    HRegionInfo replica2 = RegionReplicaUtil.getRegionInfoForReplica(replica1, 2);
    clusterState.lastEntry().getValue().add(replica2);

    cluster = new BaseLoadBalancer.Cluster(clusterState, null, null, null);
    costFunction.init(cluster);
    double costWith1ReplicaSameServer = costFunction.cost();

    assertTrue(costWith1ReplicaDifferentServer < costWith1ReplicaSameServer);

    // test with replication = 4 for following:

    HRegionInfo replica3;
    Iterator<Entry<ServerName, List<HRegionInfo>>> it;
    Entry<ServerName, List<HRegionInfo>> entry;

    clusterState = mockClusterServers(servers);
    it = clusterState.entrySet().iterator();
    entry = it.next(); //first server
    HRegionInfo hri = entry.getValue().get(0);
    replica1 = RegionReplicaUtil.getRegionInfoForReplica(hri, 1);
    replica2 = RegionReplicaUtil.getRegionInfoForReplica(hri, 2);
    replica3 = RegionReplicaUtil.getRegionInfoForReplica(hri, 3);
    entry.getValue().add(replica1);
    entry.getValue().add(replica2);
    it.next().getValue().add(replica3); //2nd server

    cluster = new BaseLoadBalancer.Cluster(clusterState, null, null, null);
    costFunction.init(cluster);
    double costWith3ReplicasSameServer = costFunction.cost();

    clusterState = mockClusterServers(servers);
    hri = clusterState.firstEntry().getValue().get(0);
    replica1 = RegionReplicaUtil.getRegionInfoForReplica(hri, 1);
    replica2 = RegionReplicaUtil.getRegionInfoForReplica(hri, 2);
    replica3 = RegionReplicaUtil.getRegionInfoForReplica(hri, 3);

    clusterState.firstEntry().getValue().add(replica1);
    clusterState.lastEntry().getValue().add(replica2);
    clusterState.lastEntry().getValue().add(replica3);

    cluster = new BaseLoadBalancer.Cluster(clusterState, null, null, null);
    costFunction.init(cluster);
    double costWith2ReplicasOnTwoServers = costFunction.cost();

    assertTrue(costWith2ReplicasOnTwoServers < costWith3ReplicasSameServer);
  }
```
```
Cyclomatic Complexity	 5
Assertions		 2
Lines of Code		21
-  
  public void testNeedsBalanceForColocatedReplicas() {
    // check for the case where there are two hosts and with one rack, and where
    // both the replicas are hosted on the same server
    List<HRegionInfo> regions = randomRegions(1);
    ServerName s1 = ServerName.valueOf("host1", 1000, 11111);
    ServerName s2 = ServerName.valueOf("host11", 1000, 11111);
    Map<ServerName, List<HRegionInfo>> map = new HashMap<ServerName, List<HRegionInfo>>();
    map.put(s1, regions);
    regions.add(RegionReplicaUtil.getRegionInfoForReplica(regions.get(0), 1));
    // until the step above s1 holds two replicas of a region
    regions = randomRegions(1);
    map.put(s2, regions);
    assertTrue(loadBalancer.needsBalance(new Cluster(map, null, null, null)));
    // check for the case where there are two hosts on the same rack and there are two racks
    // and both the replicas are on the same rack
    map.clear();
    regions = randomRegions(1);
    List<HRegionInfo> regionsOnS2 = new ArrayList<HRegionInfo>(1);
    regionsOnS2.add(RegionReplicaUtil.getRegionInfoForReplica(regions.get(0), 1));
    map.put(s1, regions);
    map.put(s2, regionsOnS2);
    // add another server so that the cluster has some host on another rack
    map.put(ServerName.valueOf("host2", 1000, 11111), randomRegions(1));
    assertTrue(loadBalancer.needsBalance(new Cluster(map, null, null,
        new ForTestRackManagerOne())));
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		9
-   (timeout = 60000)
  public void testSmallCluster() {
    int numNodes = 10;
    int numRegions = 1000;
    int numRegionsPerServer = 40; //all servers except one
    int replication = 1;
    int numTables = 10;
    testWithCluster(numNodes, numRegions, numRegionsPerServer, replication, numTables, true, true);
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		9
-   (timeout = 60000)
  public void testSmallCluster2() {
    int numNodes = 20;
    int numRegions = 2000;
    int numRegionsPerServer = 40; //all servers except one
    int replication = 1;
    int numTables = 10;
    testWithCluster(numNodes, numRegions, numRegionsPerServer, replication, numTables, true, true);
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		9
-   (timeout = 60000)
  public void testSmallCluster3() {
    int numNodes = 20;
    int numRegions = 2000;
    int numRegionsPerServer = 1; // all servers except one
    int replication = 1;
    int numTables = 10;
    /* fails because of max moves */
    testWithCluster(numNodes, numRegions, numRegionsPerServer, replication, numTables, false, false);
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		9
-   (timeout = 800000)
  public void testMidCluster() {
    int numNodes = 100;
    int numRegions = 10000;
    int numRegionsPerServer = 60; // all servers except one
    int replication = 1;
    int numTables = 40;
    testWithCluster(numNodes, numRegions, numRegionsPerServer, replication, numTables, true, true);
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		15
-   (timeout = 800000)
  public void testMidCluster2() {
    int numNodes = 200;
    int numRegions = 100000;
    int numRegionsPerServer = 40; // all servers except one
    int replication = 1;
    int numTables = 400;
    testWithCluster(numNodes,
        numRegions,
        numRegionsPerServer,
        replication,
        numTables,
        false, /* num large num regions means may not always get to best balance with one run */
        false);
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		9
-   (timeout = 800000)
  public void testMidCluster3() {
    int numNodes = 100;
    int numRegions = 2000;
    int numRegionsPerServer = 9; // all servers except one
    int replication = 1;
    int numTables = 110;
    testWithCluster(numNodes, numRegions, numRegionsPerServer, replication, numTables, true, true);
    // TODO(eclark): Make sure that the tables are well distributed.
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		9
-  
  public void testLargeCluster() {
    int numNodes = 1000;
    int numRegions = 100000; //100 regions per RS
    int numRegionsPerServer = 80; //all servers except one
    int numTables = 100;
    int replication = 1;
    testWithCluster(numNodes, numRegions, numRegionsPerServer, replication, numTables, true, true);
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		9
-   (timeout = 800000)
  public void testRegionReplicasOnSmallCluster() {
    int numNodes = 10;
    int numRegions = 1000;
    int replication = 3; // 3 replicas per region
    int numRegionsPerServer = 80; //all regions are mostly balanced
    int numTables = 10;
    testWithCluster(numNodes, numRegions, numRegionsPerServer, replication, numTables, true, true);
  }
```
```
Cyclomatic Complexity	 2
Assertions		 1
Lines of Code		15
-  (timeout = 15000)
  public void testForDifferntHFileRefsZnodeVersion() throws Exception {
    // 1. Create a file
    Path file = new Path(root, "testForDifferntHFileRefsZnodeVersion");
    fs.createNewFile(file);
    // 2. Assert file is successfully created
    assertTrue("Test file not created!", fs.exists(file));
    ReplicationHFileCleaner cleaner = new ReplicationHFileCleaner();
    cleaner.setConf(conf);

    ReplicationQueuesClient replicationQueuesClient = Mockito.mock(ReplicationQueuesClient.class);
    //Return different znode version for each call
    Mockito.when(replicationQueuesClient.getHFileRefsNodeChangeVersion()).thenReturn(1, 2);

    Class<? extends ReplicationHFileCleaner> cleanerClass = cleaner.getClass();
    Field rqc = cleanerClass.getDeclaredField("rqc");
    rqc.setAccessible(true);
    rqc.set(cleaner, replicationQueuesClient);

    cleaner.isFileDeletable(fs.getFileStatus(file));@@@   }
```
```
Cyclomatic Complexity	 4
Assertions		 10
Lines of Code		49
-  
  public void testMasterOpsWhileSplitting() throws Exception {
    TableName tableName = TableName.valueOf("TestSplit");
    byte[] familyName = Bytes.toBytes("fam");
    try (HTable ht = TEST_UTIL.createTable(tableName, familyName)) {
      TEST_UTIL.loadTable(ht, familyName, false);
    }
    HRegionServer server = TEST_UTIL.getHBaseCluster().getRegionServer(0);
    byte[] firstRow = Bytes.toBytes("aaa");
    byte[] splitRow = Bytes.toBytes("lll");
    byte[] lastRow = Bytes.toBytes("zzz");
    try (Connection conn = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration())) {
      // this will also cache the region
      byte[] regionName = conn.getRegionLocator(tableName).getRegionLocation(splitRow)
          .getRegionInfo().getRegionName();
      Region region = server.getRegion(regionName);
      SplitTransactionImpl split = new SplitTransactionImpl((HRegion) region, splitRow);
      split.prepare();

      // 1. phase I
      PairOfSameType<Region> regions = split.createDaughters(server, server, null);
      assertFalse(test(conn, tableName, firstRow, server));
      assertFalse(test(conn, tableName, lastRow, server));

      // passing null as services prevents final step
      // 2, most of phase II
      split.openDaughters(server, null, regions.getFirst(), regions.getSecond());
      assertFalse(test(conn, tableName, firstRow, server));
      assertFalse(test(conn, tableName, lastRow, server));

      // 3. finish phase II
      // note that this replicates some code from SplitTransaction
      // 2nd daughter first
      if (split.useZKForAssignment) {
        server.postOpenDeployTasks(regions.getSecond());
      } else {
        server.reportRegionStateTransition(
          RegionServerStatusProtos.RegionStateTransition.TransitionCode.SPLIT,
          region.getRegionInfo(), regions.getFirst().getRegionInfo(),
          regions.getSecond().getRegionInfo());
      }

      // first daughter second
      if (split.useZKForAssignment) {
        server.postOpenDeployTasks(regions.getFirst());
      }

      // Add to online regions
      server.addToOnlineRegions(regions.getSecond());
      // THIS is the crucial point:
      // the 2nd daughter was added, so querying before the split key should fail.
      assertFalse(test(conn, tableName, firstRow, server));
      // past splitkey is ok.
      assertTrue(test(conn, tableName, lastRow, server));

      // Add to online regions
      server.addToOnlineRegions(regions.getFirst());
      assertTrue(test(conn, tableName, firstRow, server));
      assertTrue(test(conn, tableName, lastRow, server));

      if (split.useZKForAssignment) {
        // 4. phase III
        ((BaseCoordinatedStateManager) server.getCoordinatedStateManager())
            .getSplitTransactionCoordination().completeSplitTransaction(server, regions.getFirst(),
              regions.getSecond(), split.std, region);
      }

      assertTrue(test(conn, tableName, firstRow, server));
      assertTrue(test(conn, tableName, lastRow, server));
    }
  }
```
```
Cyclomatic Complexity	 4
Assertions		 5
Lines of Code		42
-  
  public void testTableAvailableWhileSplitting() throws Exception {
    TableName tableName = TableName.valueOf("TestTableAvailableWhileSplitting");
    byte[] familyName = Bytes.toBytes("fam");
    try (HTable ht = TEST_UTIL.createTable(tableName, familyName)) {
      TEST_UTIL.loadTable(ht, familyName, false);
    }
    Admin admin = TEST_UTIL.getHBaseAdmin();
    HRegionServer server = TEST_UTIL.getHBaseCluster().getRegionServer(0);
    byte[] splitRow = Bytes.toBytes("lll");
    try (Connection conn = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration())) {
      byte[] regionName = conn.getRegionLocator(tableName).getRegionLocation(splitRow)
          .getRegionInfo().getRegionName();
      Region region = server.getRegion(regionName);
      SplitTransactionImpl split = new SplitTransactionImpl((HRegion) region, splitRow);
      split.prepare();
      assertTrue(admin.isTableAvailable(tableName));

      // 1. phase I
      PairOfSameType<Region> regions = split.createDaughters(server, server, null);
      // Parent should be offline at this stage and daughters not yet open
      assertFalse(admin.isTableAvailable(tableName));

      // passing null as services prevents final step of postOpenDeployTasks
      // 2, most of phase II
      split.openDaughters(server, null, regions.getFirst(), regions.getSecond());
      assertFalse(admin.isTableAvailable(tableName));

      // Finish openeing daughters
      // 2nd daughter first
      if (split.useZKForAssignment) {
        server.postOpenDeployTasks(regions.getSecond());
      } else {
        server.reportRegionStateTransition(
          RegionServerStatusProtos.RegionStateTransition.TransitionCode.SPLIT,
          region.getRegionInfo(), regions.getFirst().getRegionInfo(),
          regions.getSecond().getRegionInfo());
      }

      // first daughter second
      if (split.useZKForAssignment) {
        server.postOpenDeployTasks(regions.getFirst());
      }

      // After postOpenDeploy daughters should have location in meta
      assertTrue(admin.isTableAvailable(tableName));

      server.addToOnlineRegions(regions.getSecond());
      server.addToOnlineRegions(regions.getFirst());
      assertTrue(admin.isTableAvailable(tableName));
    } finally {
      if (admin != null) {
        admin.close();
      }
    }
  }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		5
-  
  public void testMultiLogThresholdActions() throws ServiceException, IOException {
    sendMultiRequest(THRESHOLD + 1, ActionType.ACTIONS);
    verify(LD, Mockito.times(1)).logBatchWarning(Mockito.anyString(), Mockito.anyInt(), Mockito.anyInt());
  }
```
```
Cyclomatic Complexity	 1
Assertions		 4
Lines of Code		39
-  (timeout = 120000)
  public void testFailedSplit() throws Exception {
    TableName tableName = TableName.valueOf("testFailedSplit");
    byte[] colFamily = Bytes.toBytes("info");
    TESTING_UTIL.createTable(tableName, colFamily);
    Connection connection = ConnectionFactory.createConnection(TESTING_UTIL.getConfiguration());
    HTable table = (HTable) connection.getTable(tableName);
    try {
      TESTING_UTIL.loadTable(table, colFamily);
      List<HRegionInfo> regions = TESTING_UTIL.getHBaseAdmin().getTableRegions(tableName);
      assertTrue(regions.size() == 1);
      final HRegion actualRegion = cluster.getRegions(tableName).get(0);
      actualRegion.getCoprocessorHost().load(FailingSplitRegionObserver.class,
        Coprocessor.PRIORITY_USER, actualRegion.getBaseConf());

      // The following split would fail.
      admin.split(tableName);
      FailingSplitRegionObserver observer = (FailingSplitRegionObserver) actualRegion
          .getCoprocessorHost().findCoprocessor(FailingSplitRegionObserver.class.getName());
      assertNotNull(observer);
      observer.latch.await();
      observer.postSplit.await();
      LOG.info("Waiting for region to come out of RIT: " + actualRegion);
      TESTING_UTIL.waitFor(60000, 1000, new Waiter.Predicate<Exception>() {
        @Override
        public boolean evaluate() throws Exception {
          RegionStates regionStates = cluster.getMaster().getAssignmentManager().getRegionStates();
          return !regionStates.isRegionsInTransition();
        }
      });
      regions = TESTING_UTIL.getHBaseAdmin().getTableRegions(tableName);
      assertTrue(regions.size() == 1);
      RegionStates regionStates = cluster.getMaster().getAssignmentManager().getRegionStates();
      Set<RegionState> rit = regionStates.getRegionsInTransition();
      assertTrue(rit.size() == 0);
    } finally {
      table.close();
      connection.close();
      TESTING_UTIL.deleteTable(tableName);
    }
  }
```
```
Cyclomatic Complexity	 2
Assertions		 2
Lines of Code		51
-   (timeout=300000)
  public void testSSHCleanupDaugtherRegionsOfAbortedSplit() throws Exception {
    TableName table = TableName.valueOf("testSSHCleanupDaugtherRegionsOfAbortedSplit");
    try {
      HTableDescriptor desc = new HTableDescriptor(table);
      desc.addFamily(new HColumnDescriptor(Bytes.toBytes("f")));
      admin.createTable(desc);
      HTable hTable = new HTable(cluster.getConfiguration(), desc.getTableName());
      for(int i = 1; i < 5; i++) {
        Put p1 = new Put(("r"+i).getBytes());
        p1.add(Bytes.toBytes("f"), "q1".getBytes(), "v".getBytes());
        hTable.put(p1);
      }
      admin.flush(desc.getTableName());
      List<HRegion> regions = cluster.getRegions(desc.getTableName());
      int serverWith = cluster.getServerWith(regions.get(0).getRegionInfo().getRegionName());
      HRegionServer regionServer = cluster.getRegionServer(serverWith);
      cluster.getServerWith(regions.get(0).getRegionInfo().getRegionName());
      SplitTransactionImpl st = new SplitTransactionImpl(regions.get(0), Bytes.toBytes("r3"));
      st.prepare();
      st.stepsBeforePONR(regionServer, regionServer, false);
      Path tableDir =
          FSUtils.getTableDir(cluster.getMaster().getMasterFileSystem().getRootDir(),
            desc.getTableName());
      tableDir.getFileSystem(cluster.getConfiguration());
      List<Path> regionDirs =
          FSUtils.getRegionDirs(tableDir.getFileSystem(cluster.getConfiguration()), tableDir);
      assertEquals(3,regionDirs.size());
      cluster.startRegionServer();
      regionServer.kill();
      cluster.getRegionServerThreads().get(serverWith).join();
      // Wait until finish processing of shutdown
      TESTING_UTIL.waitFor(60000, 1000, new Waiter.Predicate<Exception>() {
        @Override
        public boolean evaluate() throws Exception {
          return !cluster.getMaster().getServerManager().areDeadServersInProgress();
        }
      });
      // Wait until there are no more regions in transition
      TESTING_UTIL.waitFor(60000, 1000, new Waiter.Predicate<Exception>() {
        @Override
        public boolean evaluate() throws Exception {
          return !cluster.getMaster().getAssignmentManager().
              getRegionStates().isRegionsInTransition();
        }
      });
      regionDirs =
          FSUtils.getRegionDirs(tableDir.getFileSystem(cluster.getConfiguration()), tableDir);
      assertEquals(1,regionDirs.size());
    } finally {
      TESTING_UTIL.deleteTable(table);
    }
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		17
-  
  public void testNoEdits() throws Exception {
    TableName tableName = TableName.valueOf("TestLogRollPeriodNoEdits");
    TEST_UTIL.createTable(tableName, "cf");
    try {
      Table table = new HTable(TEST_UTIL.getConfiguration(), tableName);
      try {
        HRegionServer server = TEST_UTIL.getRSForFirstRegionInTable(tableName);
        WAL log = server.getWAL(null);
        checkMinLogRolls(log, 5);
      } finally {
        table.close();
      }
    } finally {
      TEST_UTIL.deleteTable(tableName);
    }
  }
```
```
Cyclomatic Complexity	 2
Assertions		 0
Lines of Code		38
-  (timeout=60000)
  public void testWithEdits() throws Exception {
    final TableName tableName = TableName.valueOf("TestLogRollPeriodWithEdits");
    final String family = "cf";

    TEST_UTIL.createTable(tableName, family);
    try {
      HRegionServer server = TEST_UTIL.getRSForFirstRegionInTable(tableName);
      WAL log = server.getWAL(null);
      final Table table = new HTable(TEST_UTIL.getConfiguration(), tableName);

      Thread writerThread = new Thread("writer") {
        @Override
        public void run() {
          try {
            long row = 0;
            while (!interrupted()) {
              Put p = new Put(Bytes.toBytes(String.format("row%d", row)));
              p.add(Bytes.toBytes(family), Bytes.toBytes("col"), Bytes.toBytes(row));
              table.put(p);
              row++;

              Thread.sleep(LOG_ROLL_PERIOD / 16);
            }
          } catch (Exception e) {
            LOG.warn(e);
          } 
        }
      };

      try {
        writerThread.start();
        checkMinLogRolls(log, 5);
      } finally {
        writerThread.interrupt();
        writerThread.join();
        table.close();
      }  
    } finally {
      TEST_UTIL.deleteTable(tableName);
    }
  }
```
```
Cyclomatic Complexity	 1
Assertions		 5
Lines of Code		30
-  
  public void testZKLockCleaner() throws Exception {
    MiniHBaseCluster cluster = utility1.startMiniCluster(1, 2);
    HTableDescriptor table = new HTableDescriptor(TableName.valueOf(Bytes.toBytes("zk")));
    HColumnDescriptor fam = new HColumnDescriptor(famName);
    fam.setScope(HConstants.REPLICATION_SCOPE_GLOBAL);
    table.addFamily(fam);
    new HBaseAdmin(conf1).createTable(table);
    ReplicationAdmin replicationAdmin = new ReplicationAdmin(conf1);
    ReplicationPeerConfig rpc = new ReplicationPeerConfig();
    rpc.setClusterKey(utility2.getClusterKey());
    replicationAdmin.addPeer("cluster2", rpc, null);
    HRegionServer rs = cluster.getRegionServer(0);
    ReplicationQueuesZKImpl zk = new ReplicationQueuesZKImpl(rs.getZooKeeper(), conf1, rs);
    zk.init(rs.getServerName().toString());
    List<String> replicators = zk.getListOfReplicators();
    assertEquals(2, replicators.size());
    String zNode = cluster.getRegionServer(1).getServerName().toString();

    assertTrue(zk.lockOtherRS(zNode));
    assertTrue(zk.checkLockExists(zNode));
    Thread.sleep(10000);
    assertTrue(zk.checkLockExists(zNode));
    cluster.abortRegionServer(0);
    Thread.sleep(10000);
    HRegionServer rs1 = cluster.getRegionServer(1);
    zk = new ReplicationQueuesZKImpl(rs1.getZooKeeper(), conf1, rs1);
    zk.init(rs1.getServerName().toString());
    assertFalse(zk.checkLockExists(zNode));

    utility1.shutdownMiniCluster();
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		4
-  (timeout=300000)
  public void killOneMasterRS() throws Exception {
    loadTableAndKillRS(utility1);
  }
```
```
Cyclomatic Complexity	 2
Assertions		 5
Lines of Code		28
-  
  public void testReplicationSourceWALReaderThreadWithFilter() throws Exception {
    final byte[] notReplicatedCf = Bytes.toBytes("notReplicated");
    final Map<TableName, List<String>> tableCfs = new HashMap<>();
    tableCfs.put(tableName, Collections.singletonList(Bytes.toString(family)));
    ReplicationPeer peer = mock(ReplicationPeer.class);
    when(peer.getTableCFs()).thenReturn(tableCfs);
    WALEntryFilter filter = new ChainWALEntryFilter(new TableCfWALEntryFilter(peer));

    // add filterable entries
    appendToLogPlus(3, notReplicatedCf);
    appendToLogPlus(3, notReplicatedCf);
    appendToLogPlus(3, notReplicatedCf);

    // add non filterable entries
    appendEntriesToLog(2);

    ReplicationSourceManager mockSourceManager = mock(ReplicationSourceManager.class);
    final ReplicationSourceWALReaderThread reader =
            new ReplicationSourceWALReaderThread(mockSourceManager, getQueueInfo(), walQueue,
                    0, fs, conf, filter, new MetricsSource("1"));
    reader.start();

    WALEntryBatch entryBatch = reader.take();

    assertNotNull(entryBatch);
    assertFalse(entryBatch.isEmpty());
    List<Entry> walEntries = entryBatch.getWalEntries();
    assertEquals(2, walEntries.size());
    for (Entry entry : walEntries) {
      ArrayList<Cell> cells = entry.getEdit().getCells();
      assertTrue(cells.size() == 1);
      assertTrue(CellUtil.matchingFamily(cells.get(0), family));
    }
  }
```
```
Cyclomatic Complexity	 -1
Assertions		 7
Lines of Code		33
-  
  public void testReplicationSourceWALReaderThreadWithFilterWhenLogRolled() throws Exception {
    final byte[] notReplicatedCf = Bytes.toBytes("notReplicated");
    final Map<TableName, List<String>> tableCfs = new HashMap<>();
    tableCfs.put(tableName, Collections.singletonList(Bytes.toString(family)));
    ReplicationPeer peer = mock(ReplicationPeer.class);
    when(peer.getTableCFs()).thenReturn(tableCfs);
    WALEntryFilter filter = new ChainWALEntryFilter(new TableCfWALEntryFilter(peer));

    appendToLogPlus(3, notReplicatedCf);

    Path firstWAL = walQueue.peek();
    final long eof = getPosition(firstWAL);

    ReplicationSourceManager mockSourceManager = mock(ReplicationSourceManager.class);
    final ReplicationSourceWALReaderThread reader =
            new ReplicationSourceWALReaderThread(mockSourceManager, getQueueInfo(), walQueue,
                    0, fs, conf, filter, new MetricsSource("1"));
    reader.start();

    // reader won't put any batch, even if EOF reached.
    Waiter.waitFor(conf, 20000, new Waiter.Predicate<Exception>() {
      @Override public boolean evaluate() {
        return reader.getLastReadPosition() >= eof;
      }
    });
    assertNull(reader.poll(0));

    log.rollWriter();

    // should get empty batch with current wal position, after wal rolled
    WALEntryBatch entryBatch = reader.take();

    Path lastWAL= walQueue.peek();
    long positionToBeLogged = getPosition(lastWAL);

    assertNotNull(entryBatch);
    assertTrue(entryBatch.isEmpty());
    assertEquals(1, walQueue.size());
    assertNotEquals(firstWAL, entryBatch.getLastWalPath());
    assertEquals(lastWAL, entryBatch.getLastWalPath());
    assertEquals(positionToBeLogged, entryBatch.getLastWalPosition());
  }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		13
-   (timeout=180000)
  public void testSplit() throws Exception {
    AccessTestAction action = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preSplit(ObserverContext.createAndPrepare(RCP_ENV, null));
        return null;
      }
    };

    verifyAllowed(action, SUPERUSER, USER_ADMIN, USER_OWNER, USER_GROUP_ADMIN);
    verifyDenied(action, USER_CREATE, USER_RW, USER_RO, USER_NONE, USER_GROUP_READ,
      USER_GROUP_WRITE, USER_GROUP_CREATE);
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		7
-  
  public void testMigration() throws DeserializationException {
    Configuration conf = UTIL.getConfiguration();
    ListMultimap<String,TablePermission> permissions = createPermissions();
    byte [] bytes = writePermissionsAsBytes(permissions, conf);
    AccessControlLists.readPermissions(bytes, conf);
  }
```
```
Cyclomatic Complexity	 -1
Assertions		 3
Lines of Code		37
-  
  public void testTokenAuthentication() throws Exception {
    UserGroupInformation testuser =
        UserGroupInformation.createUserForTesting("testuser", new String[]{"testgroup"});

    testuser.setAuthenticationMethod(
        UserGroupInformation.AuthenticationMethod.TOKEN);
    final Configuration conf = TEST_UTIL.getConfiguration();
    UserGroupInformation.setConfiguration(conf);
    Token<AuthenticationTokenIdentifier> token =
        secretManager.generateToken("testuser");
    LOG.debug("Got token: " + token.toString());
    testuser.addToken(token);

    // verify the server authenticates us as this token user
    testuser.doAs(new PrivilegedExceptionAction<Object>() {
      public Object run() throws Exception {
        Configuration c = server.getConfiguration();
        RpcClient rpcClient = RpcClientFactory.createClient(c, clusterId.toString());
        ServerName sn =
            ServerName.valueOf(server.getAddress().getHostName(), server.getAddress().getPort(),
                System.currentTimeMillis());
        try {
          BlockingRpcChannel channel = rpcClient.createBlockingRpcChannel(sn,
              User.getCurrent(), HConstants.DEFAULT_HBASE_RPC_TIMEOUT);
          AuthenticationProtos.AuthenticationService.BlockingInterface stub =
              AuthenticationProtos.AuthenticationService.newBlockingStub(channel);
          AuthenticationProtos.WhoAmIResponse response =
              stub.whoAmI(null, AuthenticationProtos.WhoAmIRequest.getDefaultInstance());
          String myname = response.getUsername();
          assertEquals("testuser", myname);
          String authMethod = response.getAuthMethod();
          assertEquals("TOKEN", authMethod);
        } finally {
          rpcClient.close();
        }
        return null;
      }
    });
  }
```
```
Cyclomatic Complexity	 5
Assertions		 8
Lines of Code		40
-  
  @Ignore("Intermittent argument matching failures, see HBASE-18813")
  public void testReadTableTimeouts() throws Exception {
    final TableName [] tableNames = new TableName[2];
    tableNames[0] = TableName.valueOf("testReadTableTimeouts1");
    tableNames[1] = TableName.valueOf("testReadTableTimeouts2");
    // Create 2 test tables.
    for (int j = 0; j<2; j++) {
      Table table = testingUtility.createTable(tableNames[j], new byte[][] { FAMILY });
      // insert some test rows
      for (int i=0; i<1000; i++) {
        byte[] iBytes = Bytes.toBytes(i + j);
        Put p = new Put(iBytes);
        p.addColumn(FAMILY, COLUMN, iBytes);
        table.put(p);
      }
    }
    ExecutorService executor = new ScheduledThreadPoolExecutor(1);
    CanaryTool.RegionStdOutSink sink = spy(new CanaryTool.RegionStdOutSink());
    CanaryTool canary = new CanaryTool(executor, sink);
    String configuredTimeoutStr = tableNames[0].getNameAsString() + "=" + Long.MAX_VALUE + "," +
      tableNames[1].getNameAsString() + "=0";
    String[] args = { "-readTableTimeouts", configuredTimeoutStr, tableNames[0].getNameAsString(), tableNames[1].getNameAsString()};
    assertEquals(0, ToolRunner.run(testingUtility.getConfiguration(), canary, args));
    verify(sink, times(tableNames.length)).initializeAndGetReadLatencyForTable(isA(String.class));
    for (int i=0; i<2; i++) {
      assertNotEquals("verify non-null read latency", null, sink.getReadLatencyMap().get(tableNames[i].getNameAsString()));
      assertNotEquals("verify non-zero read latency", 0L, sink.getReadLatencyMap().get(tableNames[i].getNameAsString()));
    }
    // One table's timeout is set for 0 ms and thus, should lead to an error.
    verify(mockAppender, times(1)).doAppend(argThat(new ArgumentMatcher<LoggingEvent>() {
      @Override
      public boolean matches(Object argument) {
        return ((LoggingEvent) argument).getRenderedMessage().contains("exceeded the configured read timeout.");
      }
    }));
    verify(mockAppender, times(2)).doAppend(argThat(new ArgumentMatcher<LoggingEvent>() {
      @Override
      public boolean matches(Object argument) {
        return ((LoggingEvent) argument).getRenderedMessage().contains("Configured read timeout");
      }
    }));
  }
```
```
Cyclomatic Complexity	 1
Assertions		 6
Lines of Code		18
-  
  @Ignore("Intermittent argument matching failures, see HBASE-18813")
  public void testWriteTableTimeout() throws Exception {
    ExecutorService executor = new ScheduledThreadPoolExecutor(1);
    CanaryTool.RegionStdOutSink sink = spy(new CanaryTool.RegionStdOutSink());
    CanaryTool canary = new CanaryTool(executor, sink);
    String[] args = { "-writeSniffing", "-writeTableTimeout", String.valueOf(Long.MAX_VALUE)};
    assertEquals(0, ToolRunner.run(testingUtility.getConfiguration(), canary, args));
    assertNotEquals("verify non-null write latency", null, sink.getWriteLatency());
    assertNotEquals("verify non-zero write latency", 0L, sink.getWriteLatency());
    verify(mockAppender, times(1)).doAppend(argThat(
        new ArgumentMatcher<LoggingEvent>() {
          @Override
          public boolean matches(Object argument) {
            return ((LoggingEvent) argument).getRenderedMessage().contains("Configured write timeout");
          }
        }));
  }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		10
-  
  public void testRegionserverNoRegions() throws Exception {
    runRegionserverCanary();
    verify(mockAppender).doAppend(argThat(new ArgumentMatcher<LoggingEvent>() {
      @Override
      public boolean matches(Object argument) {
        return ((LoggingEvent) argument).getRenderedMessage().contains("Regionserver not serving any regions");
      }
    }));
  }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		12
-  
  public void testRegionserverWithRegions() throws Exception {
    TableName tableName = TableName.valueOf("testTable");
    testingUtility.createTable(tableName, new byte[][] { FAMILY });
    runRegionserverCanary();
    verify(mockAppender, never()).doAppend(argThat(new ArgumentMatcher<LoggingEvent>() {
      @Override
      public boolean matches(Object argument) {
        return ((LoggingEvent) argument).getRenderedMessage().contains("Regionserver not serving any regions");
      }
    }));
  }
```
```
Cyclomatic Complexity	 2
Assertions		 0
Lines of Code		4
-  
  public void testThriftServerHttpTraceForbiddenWhenOptionsDisabled() throws Exception {
    // HTTP TRACE method should be disabled for security
    // See https://www.owasp.org/index.php/Cross_Site_Tracing
    checkHttpMethods("TRACE", HttpURLConnection.HTTP_FORBIDDEN);
  }
```
```
Cyclomatic Complexity	 2
Assertions		 0
Lines of Code		6
-  
  public void testThriftServerHttpTraceForbiddenWhenOptionsEnabled() throws Exception {
    // HTTP TRACE method should be disabled for security
    // See https://www.owasp.org/index.php/Cross_Site_Tracing
    TEST_UTIL.getConfiguration().setBoolean(ThriftServerRunner.THRIFT_HTTP_ALLOW_OPTIONS_METHOD,
        true);
    checkHttpMethods("TRACE", HttpURLConnection.HTTP_FORBIDDEN);
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		5
-  
  public void testThriftServerHttpOptionsForbiddenWhenOptionsDisabled() throws Exception {
    // HTTP OPTIONS method should be disabled by default, so we make sure
    // hbase.thrift.http.allow.options.method is not set anywhere in the config
    TEST_UTIL.getConfiguration().unset(ThriftServerRunner.THRIFT_HTTP_ALLOW_OPTIONS_METHOD);
    checkHttpMethods("OPTIONS", HttpURLConnection.HTTP_FORBIDDEN);
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		6
-  
  public void testThriftServerHttpOptionsOkWhenOptionsEnabled() throws Exception {
    TEST_UTIL.getConfiguration().setBoolean(ThriftServerRunner.THRIFT_HTTP_ALLOW_OPTIONS_METHOD,
        true);
    checkHttpMethods("OPTIONS", HttpURLConnection.HTTP_OK);
  }
```
## 9cc3b79d5fef69d12bdb7d298d558895dd92f38f ##
```
Cyclomatic Complexity	1
Assertions		4
Lines of Code		14
-  
  public void testErrorNotGzipped() throws Exception {
    Header[] headers = new Header[2];
    headers[0] = new Header("Accept", Constants.MIMETYPE_BINARY);
    headers[1] = new Header("Accept-Encoding", "gzip");
    Response response = client.get("/" + TABLE + "/" + ROW_1 + "/" + COLUMN_2, headers);
    assertEquals(404, response.getCode());
    String contentEncoding = response.getHeader("Content-Encoding");
    assertTrue(contentEncoding == null || !contentEncoding.contains("gzip"));
    response = client.get("/" + TABLE, headers);
    assertEquals(405, response.getCode());
    contentEncoding = response.getHeader("Content-Encoding");
    assertTrue(contentEncoding == null || !contentEncoding.contains("gzip"));
  }
```
```
Cyclomatic Complexity	 1
Assertions		 6
Lines of Code		11
-  
  public void testValueOfNamespaceAndQualifier() {
    TableName name0 = TableName.valueOf("table");
    TableName name1 = TableName.valueOf("table", "table");
    assertEquals(NamespaceDescriptor.DEFAULT_NAMESPACE_NAME_STR, name0.getNamespaceAsString());
    assertEquals("table", name0.getQualifierAsString());
    assertEquals("table", name0.getNameAsString());
    assertEquals("table", name1.getNamespaceAsString());
    assertEquals("table", name1.getQualifierAsString());
    assertEquals("table:table", name1.getNameAsString());
  }
```
```
Cyclomatic Complexity	 1
Assertions		 12
Lines of Code		186
-  
  public void testVersions() throws Exception {
    byte [] TABLE = Bytes.toBytes("testVersions");@@@+      // Verify we can get each one properly@@@+      getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);@@@+      getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);@@@+      getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);@@@+      getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[7], VALUES[7]);@@@+      scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);@@@+      scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);@@@+      scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);@@@+      scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[7], VALUES[7]);@@@ 
    long [] STAMPS = makeStamps(20);
    byte [][] VALUES = makeNAscii(VALUE, 20);@@@+      // Verify we don't accidentally get others@@@+      getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[0]);@@@+      getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[9]);@@@+      scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[0]);@@@+      scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[9]);@@@ 
    Table ht = TEST_UTIL.createTable(TABLE, FAMILY, 10);
    TEST_UTIL.waitTableAvailable(TABLE, 10000);
    // Insert 4 versions of same column
    Put put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    put.add(FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    put.add(FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    put.add(FAMILY, QUALIFIER, STAMPS[5], VALUES[5]);
    ht.put(put);@@@+      // Ensure maxVersions of table is respected@@@ 
    // Verify we can get each one properly
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[5], VALUES[5]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[5], VALUES[5]);@@@+      TEST_UTIL.flush();@@@ 
    // Verify we don't accidentally get others
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[0]);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[3]);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[6]);
    scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[0]);
    scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[3]);
    scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[6]);

    // Ensure maxVersions in query is respected
    Get get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIER);
    get.setMaxVersions(2);
    Result result = ht.get(get);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[4], STAMPS[5]},
        new byte[][] {VALUES[4], VALUES[5]},
        0, 1);

    Scan scan = new Scan(ROW);
    scan.addColumn(FAMILY, QUALIFIER);
    scan.setMaxVersions(2);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[4], STAMPS[5]},
        new byte[][] {VALUES[4], VALUES[5]},
        0, 1);

    // Flush and redo

    TEST_UTIL.flush();

    // Verify we can get each one properly
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[5], VALUES[5]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[5], VALUES[5]);

    // Verify we don't accidentally get others
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[0]);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[3]);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[6]);
    scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[0]);
    scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[3]);
    scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[6]);

    // Ensure maxVersions in query is respected
    get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIER);
    get.setMaxVersions(2);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[4], STAMPS[5]},
        new byte[][] {VALUES[4], VALUES[5]},
        0, 1);

    scan = new Scan(ROW);
    scan.addColumn(FAMILY, QUALIFIER);
    scan.setMaxVersions(2);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[4], STAMPS[5]},
        new byte[][] {VALUES[4], VALUES[5]},
        0, 1);


    // Add some memstore and retest

    // Insert 4 more versions of same column and a dupe
    put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, STAMPS[3], VALUES[3]);
    put.add(FAMILY, QUALIFIER, STAMPS[6], VALUES[6]);
    put.add(FAMILY, QUALIFIER, STAMPS[7], VALUES[7]);
    put.add(FAMILY, QUALIFIER, STAMPS[8], VALUES[8]);
    ht.put(put);

    // Ensure maxVersions in query is respected
    get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIER);
    get.setMaxVersions();
    result = ht.get(get);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[1], STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7], STAMPS[8]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3], VALUES[4], VALUES[5], VALUES[6], VALUES[7], VALUES[8]},
        0, 7);

    scan = new Scan(ROW);
    scan.addColumn(FAMILY, QUALIFIER);
    scan.setMaxVersions();
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[1], STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7], STAMPS[8]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3], VALUES[4], VALUES[5], VALUES[6], VALUES[7], VALUES[8]},
        0, 7);

    get = new Get(ROW);
    get.setMaxVersions();
    result = ht.get(get);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[1], STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7], STAMPS[8]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3], VALUES[4], VALUES[5], VALUES[6], VALUES[7], VALUES[8]},
        0, 7);

    scan = new Scan(ROW);
    scan.setMaxVersions();
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[1], STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7], STAMPS[8]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3], VALUES[4], VALUES[5], VALUES[6], VALUES[7], VALUES[8]},
        0, 7);

    // Verify we can get each one properly
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[7], VALUES[7]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[7], VALUES[7]);

    // Verify we don't accidentally get others
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[0]);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[9]);
    scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[0]);
    scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[9]);

    // Ensure maxVersions of table is respected

    TEST_UTIL.flush();

    // Insert 4 more versions of same column and a dupe
    put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, STAMPS[9], VALUES[9]);
    put.add(FAMILY, QUALIFIER, STAMPS[11], VALUES[11]);
    put.add(FAMILY, QUALIFIER, STAMPS[13], VALUES[13]);
    put.add(FAMILY, QUALIFIER, STAMPS[15], VALUES[15]);
    ht.put(put);

    get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIER);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7], STAMPS[8], STAMPS[9], STAMPS[11], STAMPS[13], STAMPS[15]},
        new byte[][] {VALUES[3], VALUES[4], VALUES[5], VALUES[6], VALUES[7], VALUES[8], VALUES[9], VALUES[11], VALUES[13], VALUES[15]},
        0, 9);

    scan = new Scan(ROW);
    scan.addColumn(FAMILY, QUALIFIER);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7], STAMPS[8], STAMPS[9], STAMPS[11], STAMPS[13], STAMPS[15]},
        new byte[][] {VALUES[3], VALUES[4], VALUES[5], VALUES[6], VALUES[7], VALUES[8], VALUES[9], VALUES[11], VALUES[13], VALUES[15]},
        0, 9);

    // Delete a version in the memstore and a version in a storefile
    Delete delete = new Delete(ROW);
    delete.deleteColumn(FAMILY, QUALIFIER, STAMPS[11]);
    delete.deleteColumn(FAMILY, QUALIFIER, STAMPS[7]);
    ht.delete(delete);

    // Test that it's gone
    get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIER);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[1], STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[8], STAMPS[9], STAMPS[13], STAMPS[15]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3], VALUES[4], VALUES[5], VALUES[6], VALUES[8], VALUES[9], VALUES[13], VALUES[15]},
        0, 9);

    scan = new Scan(ROW);
    scan.addColumn(FAMILY, QUALIFIER);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[1], STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[8], STAMPS[9], STAMPS[13], STAMPS[15]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3], VALUES[4], VALUES[5], VALUES[6], VALUES[8], VALUES[9], VALUES[13], VALUES[15]},
        0, 9);

  }
```
```
Cyclomatic Complexity	 1
Assertions		 18
Lines of Code		163
-  
  public void testVersionLimits() throws Exception {
    byte [] TABLE = Bytes.toBytes("testVersionLimits");
    byte [][] FAMILIES = makeNAscii(FAMILY, 3);
    int [] LIMITS = {1,3,5};
    long [] STAMPS = makeStamps(10);
    byte [][] VALUES = makeNAscii(VALUE, 10);
    Table ht = TEST_UTIL.createTable(TABLE, FAMILIES, LIMITS);
    TEST_UTIL.waitTableAvailable(TABLE, 10000);
    // Insert limit + 1 on each family
    Put put = new Put(ROW);
    put.add(FAMILIES[0], QUALIFIER, STAMPS[0], VALUES[0]);
    put.add(FAMILIES[0], QUALIFIER, STAMPS[1], VALUES[1]);
    put.add(FAMILIES[1], QUALIFIER, STAMPS[0], VALUES[0]);
    put.add(FAMILIES[1], QUALIFIER, STAMPS[1], VALUES[1]);
    put.add(FAMILIES[1], QUALIFIER, STAMPS[2], VALUES[2]);
    put.add(FAMILIES[1], QUALIFIER, STAMPS[3], VALUES[3]);
    put.add(FAMILIES[2], QUALIFIER, STAMPS[0], VALUES[0]);
    put.add(FAMILIES[2], QUALIFIER, STAMPS[1], VALUES[1]);
    put.add(FAMILIES[2], QUALIFIER, STAMPS[2], VALUES[2]);
    put.add(FAMILIES[2], QUALIFIER, STAMPS[3], VALUES[3]);
    put.add(FAMILIES[2], QUALIFIER, STAMPS[4], VALUES[4]);
    put.add(FAMILIES[2], QUALIFIER, STAMPS[5], VALUES[5]);
    put.add(FAMILIES[2], QUALIFIER, STAMPS[6], VALUES[6]);
    ht.put(put);

    // Verify we only get the right number out of each

    // Family0

    Get get = new Get(ROW);
    get.addColumn(FAMILIES[0], QUALIFIER);
    get.setMaxVersions(Integer.MAX_VALUE);
    Result result = ht.get(get);
    assertNResult(result, ROW, FAMILIES[0], QUALIFIER,
        new long [] {STAMPS[1]},
        new byte[][] {VALUES[1]},
        0, 0);

    get = new Get(ROW);
    get.addFamily(FAMILIES[0]);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILIES[0], QUALIFIER,
        new long [] {STAMPS[1]},
        new byte[][] {VALUES[1]},
        0, 0);

    Scan scan = new Scan(ROW);
    scan.addColumn(FAMILIES[0], QUALIFIER);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILIES[0], QUALIFIER,
        new long [] {STAMPS[1]},
        new byte[][] {VALUES[1]},
        0, 0);

    scan = new Scan(ROW);
    scan.addFamily(FAMILIES[0]);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILIES[0], QUALIFIER,
        new long [] {STAMPS[1]},
        new byte[][] {VALUES[1]},
        0, 0);

    // Family1

    get = new Get(ROW);
    get.addColumn(FAMILIES[1], QUALIFIER);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILIES[1], QUALIFIER,
        new long [] {STAMPS[1], STAMPS[2], STAMPS[3]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3]},
        0, 2);

    get = new Get(ROW);
    get.addFamily(FAMILIES[1]);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILIES[1], QUALIFIER,
        new long [] {STAMPS[1], STAMPS[2], STAMPS[3]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3]},
        0, 2);

    scan = new Scan(ROW);
    scan.addColumn(FAMILIES[1], QUALIFIER);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILIES[1], QUALIFIER,
        new long [] {STAMPS[1], STAMPS[2], STAMPS[3]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3]},
        0, 2);

    scan = new Scan(ROW);
    scan.addFamily(FAMILIES[1]);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILIES[1], QUALIFIER,
        new long [] {STAMPS[1], STAMPS[2], STAMPS[3]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3]},
        0, 2);

    // Family2

    get = new Get(ROW);
    get.addColumn(FAMILIES[2], QUALIFIER);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILIES[2], QUALIFIER,
        new long [] {STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6]},
        new byte[][] {VALUES[2], VALUES[3], VALUES[4], VALUES[5], VALUES[6]},
        0, 4);

    get = new Get(ROW);
    get.addFamily(FAMILIES[2]);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILIES[2], QUALIFIER,
        new long [] {STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6]},
        new byte[][] {VALUES[2], VALUES[3], VALUES[4], VALUES[5], VALUES[6]},
        0, 4);

    scan = new Scan(ROW);
    scan.addColumn(FAMILIES[2], QUALIFIER);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILIES[2], QUALIFIER,
        new long [] {STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6]},
        new byte[][] {VALUES[2], VALUES[3], VALUES[4], VALUES[5], VALUES[6]},
        0, 4);

    scan = new Scan(ROW);
    scan.addFamily(FAMILIES[2]);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILIES[2], QUALIFIER,
        new long [] {STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6]},
        new byte[][] {VALUES[2], VALUES[3], VALUES[4], VALUES[5], VALUES[6]},
        0, 4);

    // Try all families

    get = new Get(ROW);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertTrue("Expected 9 keys but received " + result.size(),
        result.size() == 9);

    get = new Get(ROW);
    get.addFamily(FAMILIES[0]);
    get.addFamily(FAMILIES[1]);
    get.addFamily(FAMILIES[2]);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertTrue("Expected 9 keys but received " + result.size(),
        result.size() == 9);

    get = new Get(ROW);
    get.addColumn(FAMILIES[0], QUALIFIER);
    get.addColumn(FAMILIES[1], QUALIFIER);
    get.addColumn(FAMILIES[2], QUALIFIER);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertTrue("Expected 9 keys but received " + result.size(),
        result.size() == 9);

    scan = new Scan(ROW);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertTrue("Expected 9 keys but received " + result.size(),
        result.size() == 9);

    scan = new Scan(ROW);
    scan.setMaxVersions(Integer.MAX_VALUE);
    scan.addFamily(FAMILIES[0]);
    scan.addFamily(FAMILIES[1]);
    scan.addFamily(FAMILIES[2]);
    result = getSingleScanResult(ht, scan);
    assertTrue("Expected 9 keys but received " + result.size(),
        result.size() == 9);

    scan = new Scan(ROW);
    scan.setMaxVersions(Integer.MAX_VALUE);
    scan.addColumn(FAMILIES[0], QUALIFIER);
    scan.addColumn(FAMILIES[1], QUALIFIER);
    scan.addColumn(FAMILIES[2], QUALIFIER);
    result = getSingleScanResult(ht, scan);
    assertTrue("Expected 9 keys but received " + result.size(),
        result.size() == 9);

  }
```
```
Cyclomatic Complexity	 5
Assertions		 2
Lines of Code		33
-  
  public void testDeleteFamilyVersion() throws Exception {
    HBaseAdmin admin = new HBaseAdmin(TEST_UTIL.getConfiguration());
    byte [] TABLE = Bytes.toBytes("testDeleteFamilyVersion");

    byte [][] QUALIFIERS = makeNAscii(QUALIFIER, 1);
    byte [][] VALUES = makeN(VALUE, 5);
    long [] ts = {1000, 2000, 3000, 4000, 5000};

    Table ht = TEST_UTIL.createTable(TABLE, FAMILY, 5);
    TEST_UTIL.waitTableAvailable(TABLE, 10000);

    Put put = new Put(ROW);
    for (int q = 0; q < 1; q++)
      for (int t = 0; t < 5; t++)
        put.add(FAMILY, QUALIFIERS[q], ts[t], VALUES[t]);
    ht.put(put);
    admin.flush(TABLE);

    Delete delete = new Delete(ROW);
    delete.deleteFamilyVersion(FAMILY, ts[1]);  // delete version '2000'
    delete.deleteFamilyVersion(FAMILY, ts[3]);  // delete version '4000'
    ht.delete(delete);
    admin.flush(TABLE);

    for (int i = 0; i < 1; i++) {
      Get get = new Get(ROW);
      get.addColumn(FAMILY, QUALIFIERS[i]);
      get.setMaxVersions(Integer.MAX_VALUE);
      Result result = ht.get(get);
      // verify version '1000'/'3000'/'5000' remains for all columns
      assertNResult(result, ROW, FAMILY, QUALIFIERS[i],
          new long [] {ts[0], ts[2], ts[4]},
          new byte[][] {VALUES[0], VALUES[2], VALUES[4]},
          0, 2);
    }
    ht.close();
    admin.close();
  }
```
```
Cyclomatic Complexity	 7
Assertions		 7
Lines of Code		89
-  
  public void testDeleteFamilyVersionWithOtherDeletes() throws Exception {
    byte [] TABLE = Bytes.toBytes("testDeleteFamilyVersionWithOtherDeletes");

    byte [][] QUALIFIERS = makeNAscii(QUALIFIER, 5);
    byte [][] VALUES = makeN(VALUE, 5);
    long [] ts = {1000, 2000, 3000, 4000, 5000};

    HBaseAdmin admin = new HBaseAdmin(TEST_UTIL.getConfiguration());
    Table ht = TEST_UTIL.createTable(TABLE, FAMILY, 5);
    TEST_UTIL.waitTableAvailable(TABLE, 10000);

    Put put = null;
    Result result = null;
    Get get = null;
    Delete delete = null;

    // 1. put on ROW
    put = new Put(ROW);
    for (int q = 0; q < 5; q++)
      for (int t = 0; t < 5; t++)
        put.add(FAMILY, QUALIFIERS[q], ts[t], VALUES[t]);
    ht.put(put);
    admin.flush(TABLE);

    // 2. put on ROWS[0]
    byte [] ROW2 = Bytes.toBytes("myRowForTest");
    put = new Put(ROW2);
    for (int q = 0; q < 5; q++)
      for (int t = 0; t < 5; t++)
        put.add(FAMILY, QUALIFIERS[q], ts[t], VALUES[t]);
    ht.put(put);
    admin.flush(TABLE);

    // 3. delete on ROW
    delete = new Delete(ROW);
    // delete version <= 2000 of all columns
    // note: deleteFamily must be the first since it will mask
    // the subsequent other type deletes!
    delete.deleteFamily(FAMILY, ts[1]);
    // delete version '4000' of all columns
    delete.deleteFamilyVersion(FAMILY, ts[3]);
   // delete version <= 3000 of column 0
    delete.deleteColumns(FAMILY, QUALIFIERS[0], ts[2]);
    // delete version <= 5000 of column 2
    delete.deleteColumns(FAMILY, QUALIFIERS[2], ts[4]);
    // delete version 5000 of column 4
    delete.deleteColumn(FAMILY, QUALIFIERS[4], ts[4]);
    ht.delete(delete);
    admin.flush(TABLE);

     // 4. delete on ROWS[0]
    delete = new Delete(ROW2);
    delete.deleteFamilyVersion(FAMILY, ts[1]);  // delete version '2000'
    delete.deleteFamilyVersion(FAMILY, ts[3]);  // delete version '4000'
    ht.delete(delete);
    admin.flush(TABLE);

    // 5. check ROW
    get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIERS[0]);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILY, QUALIFIERS[0],
        new long [] {ts[4]},
        new byte[][] {VALUES[4]},
        0, 0);

    get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIERS[1]);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILY, QUALIFIERS[1],
        new long [] {ts[2], ts[4]},
        new byte[][] {VALUES[2], VALUES[4]},
        0, 1);

    get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIERS[2]);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertEquals(0, result.size());

    get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIERS[3]);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILY, QUALIFIERS[3],
        new long [] {ts[2], ts[4]},
        new byte[][] {VALUES[2], VALUES[4]},
        0, 1);

    get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIERS[4]);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILY, QUALIFIERS[4],
        new long [] {ts[2]},
        new byte[][] {VALUES[2]},
        0, 0);

    // 6. check ROWS[0]
    for (int i = 0; i < 5; i++) {
      get = new Get(ROW2);
      get.addColumn(FAMILY, QUALIFIERS[i]);
      get.setMaxVersions(Integer.MAX_VALUE);
      result = ht.get(get);
      // verify version '1000'/'3000'/'5000' remains for all columns
      assertNResult(result, ROW2, FAMILY, QUALIFIERS[i],
          new long [] {ts[0], ts[2], ts[4]},
          new byte[][] {VALUES[0], VALUES[2], VALUES[4]},
          0, 2);
    }
    ht.close();
    admin.close();
  }
```
```
Cyclomatic Complexity	 10
Assertions		 29
Lines of Code		259
-  
  public void testDeletes() throws Exception {
    byte [] TABLE = Bytes.toBytes("testDeletes");

    byte [][] ROWS = makeNAscii(ROW, 6);
    byte [][] FAMILIES = makeNAscii(FAMILY, 3);
    byte [][] VALUES = makeN(VALUE, 5);
    long [] ts = {1000, 2000, 3000, 4000, 5000};

    Table ht = TEST_UTIL.createTable(TABLE, FAMILIES, 3);
    TEST_UTIL.waitTableAvailable(TABLE, 10000);

    Put put = new Put(ROW);
    put.add(FAMILIES[0], QUALIFIER, ts[0], VALUES[0]);
    put.add(FAMILIES[0], QUALIFIER, ts[1], VALUES[1]);
    ht.put(put);

    Delete delete = new Delete(ROW);
    delete.deleteFamily(FAMILIES[0], ts[0]);
    ht.delete(delete);

    Get get = new Get(ROW);
    get.addFamily(FAMILIES[0]);
    get.setMaxVersions(Integer.MAX_VALUE);
    Result result = ht.get(get);
    assertNResult(result, ROW, FAMILIES[0], QUALIFIER,
        new long [] {ts[1]},
        new byte[][] {VALUES[1]},
        0, 0);

    Scan scan = new Scan(ROW);
    scan.addFamily(FAMILIES[0]);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILIES[0], QUALIFIER,
        new long [] {ts[1]},
        new byte[][] {VALUES[1]},
        0, 0);

    // Test delete latest version
    put = new Put(ROW);
    put.add(FAMILIES[0], QUALIFIER, ts[4], VALUES[4]);
    put.add(FAMILIES[0], QUALIFIER, ts[2], VALUES[2]);
    put.add(FAMILIES[0], QUALIFIER, ts[3], VALUES[3]);
    put.add(FAMILIES[0], null, ts[4], VALUES[4]);
    put.add(FAMILIES[0], null, ts[2], VALUES[2]);
    put.add(FAMILIES[0], null, ts[3], VALUES[3]);
    ht.put(put);

    delete = new Delete(ROW);
    delete.deleteColumn(FAMILIES[0], QUALIFIER); // ts[4]
    ht.delete(delete);

    get = new Get(ROW);
    get.addColumn(FAMILIES[0], QUALIFIER);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILIES[0], QUALIFIER,
        new long [] {ts[1], ts[2], ts[3]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3]},
        0, 2);

    scan = new Scan(ROW);
    scan.addColumn(FAMILIES[0], QUALIFIER);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILIES[0], QUALIFIER,
        new long [] {ts[1], ts[2], ts[3]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3]},
        0, 2);

    // Test for HBASE-1847
    delete = new Delete(ROW);
    delete.deleteColumn(FAMILIES[0], null);
    ht.delete(delete);

    // Cleanup null qualifier
    delete = new Delete(ROW);
    delete.deleteColumns(FAMILIES[0], null);
    ht.delete(delete);

    // Expected client behavior might be that you can re-put deleted values
    // But alas, this is not to be.  We can't put them back in either case.

    put = new Put(ROW);
    put.add(FAMILIES[0], QUALIFIER, ts[0], VALUES[0]); // 1000
    put.add(FAMILIES[0], QUALIFIER, ts[4], VALUES[4]); // 5000
    ht.put(put);


    // It used to be due to the internal implementation of Get, that
    // the Get() call would return ts[4] UNLIKE the Scan below. With
    // the switch to using Scan for Get this is no longer the case.
    get = new Get(ROW);
    get.addFamily(FAMILIES[0]);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILIES[0], QUALIFIER,
        new long [] {ts[1], ts[2], ts[3]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3]},
        0, 2);

    // The Scanner returns the previous values, the expected-naive-unexpected behavior

    scan = new Scan(ROW);
    scan.addFamily(FAMILIES[0]);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILIES[0], QUALIFIER,
        new long [] {ts[1], ts[2], ts[3]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3]},
        0, 2);

    // Test deleting an entire family from one row but not the other various ways

    put = new Put(ROWS[0]);
    put.add(FAMILIES[1], QUALIFIER, ts[0], VALUES[0]);
    put.add(FAMILIES[1], QUALIFIER, ts[1], VALUES[1]);
    put.add(FAMILIES[2], QUALIFIER, ts[2], VALUES[2]);
    put.add(FAMILIES[2], QUALIFIER, ts[3], VALUES[3]);
    ht.put(put);

    put = new Put(ROWS[1]);
    put.add(FAMILIES[1], QUALIFIER, ts[0], VALUES[0]);
    put.add(FAMILIES[1], QUALIFIER, ts[1], VALUES[1]);
    put.add(FAMILIES[2], QUALIFIER, ts[2], VALUES[2]);
    put.add(FAMILIES[2], QUALIFIER, ts[3], VALUES[3]);
    ht.put(put);

    put = new Put(ROWS[2]);
    put.add(FAMILIES[1], QUALIFIER, ts[0], VALUES[0]);
    put.add(FAMILIES[1], QUALIFIER, ts[1], VALUES[1]);
    put.add(FAMILIES[2], QUALIFIER, ts[2], VALUES[2]);
    put.add(FAMILIES[2], QUALIFIER, ts[3], VALUES[3]);
    ht.put(put);

    // Assert that above went in.
    get = new Get(ROWS[2]);
    get.addFamily(FAMILIES[1]);
    get.addFamily(FAMILIES[2]);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertTrue("Expected 4 key but received " + result.size() + ": " + result,
        result.size() == 4);

    delete = new Delete(ROWS[0]);
    delete.deleteFamily(FAMILIES[2]);
    ht.delete(delete);

    delete = new Delete(ROWS[1]);
    delete.deleteColumns(FAMILIES[1], QUALIFIER);
    ht.delete(delete);

    delete = new Delete(ROWS[2]);
    delete.deleteColumn(FAMILIES[1], QUALIFIER);
    delete.deleteColumn(FAMILIES[1], QUALIFIER);
    delete.deleteColumn(FAMILIES[2], QUALIFIER);
    ht.delete(delete);

    get = new Get(ROWS[0]);
    get.addFamily(FAMILIES[1]);
    get.addFamily(FAMILIES[2]);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertTrue("Expected 2 keys but received " + result.size(),
        result.size() == 2);
    assertNResult(result, ROWS[0], FAMILIES[1], QUALIFIER,
        new long [] {ts[0], ts[1]},
        new byte[][] {VALUES[0], VALUES[1]},
        0, 1);

    scan = new Scan(ROWS[0]);
    scan.addFamily(FAMILIES[1]);
    scan.addFamily(FAMILIES[2]);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertTrue("Expected 2 keys but received " + result.size(),
        result.size() == 2);
    assertNResult(result, ROWS[0], FAMILIES[1], QUALIFIER,
        new long [] {ts[0], ts[1]},
        new byte[][] {VALUES[0], VALUES[1]},
        0, 1);

    get = new Get(ROWS[1]);
    get.addFamily(FAMILIES[1]);
    get.addFamily(FAMILIES[2]);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertTrue("Expected 2 keys but received " + result.size(),
        result.size() == 2);

    scan = new Scan(ROWS[1]);
    scan.addFamily(FAMILIES[1]);
    scan.addFamily(FAMILIES[2]);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertTrue("Expected 2 keys but received " + result.size(),
        result.size() == 2);

    get = new Get(ROWS[2]);
    get.addFamily(FAMILIES[1]);
    get.addFamily(FAMILIES[2]);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertEquals(1, result.size());
    assertNResult(result, ROWS[2], FAMILIES[2], QUALIFIER,
        new long [] {ts[2]},
        new byte[][] {VALUES[2]},
        0, 0);

    scan = new Scan(ROWS[2]);
    scan.addFamily(FAMILIES[1]);
    scan.addFamily(FAMILIES[2]);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertEquals(1, result.size());
    assertNResult(result, ROWS[2], FAMILIES[2], QUALIFIER,
        new long [] {ts[2]},
        new byte[][] {VALUES[2]},
        0, 0);

    // Test if we delete the family first in one row (HBASE-1541)

    delete = new Delete(ROWS[3]);
    delete.deleteFamily(FAMILIES[1]);
    ht.delete(delete);

    put = new Put(ROWS[3]);
    put.add(FAMILIES[2], QUALIFIER, VALUES[0]);
    ht.put(put);

    put = new Put(ROWS[4]);
    put.add(FAMILIES[1], QUALIFIER, VALUES[1]);
    put.add(FAMILIES[2], QUALIFIER, VALUES[2]);
    ht.put(put);

    get = new Get(ROWS[3]);
    get.addFamily(FAMILIES[1]);
    get.addFamily(FAMILIES[2]);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertTrue("Expected 1 key but received " + result.size(),
        result.size() == 1);

    get = new Get(ROWS[4]);
    get.addFamily(FAMILIES[1]);
    get.addFamily(FAMILIES[2]);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertTrue("Expected 2 keys but received " + result.size(),
        result.size() == 2);

    scan = new Scan(ROWS[3]);
    scan.addFamily(FAMILIES[1]);
    scan.addFamily(FAMILIES[2]);
    scan.setMaxVersions(Integer.MAX_VALUE);
    ResultScanner scanner = ht.getScanner(scan);
    result = scanner.next();
    assertTrue("Expected 1 key but received " + result.size(),
        result.size() == 1);
    assertTrue(Bytes.equals(CellUtil.cloneRow(result.rawCells()[0]), ROWS[3]));
    assertTrue(Bytes.equals(CellUtil.cloneValue(result.rawCells()[0]), VALUES[0]));
    result = scanner.next();
    assertTrue("Expected 2 keys but received " + result.size(),
        result.size() == 2);
    assertTrue(Bytes.equals(CellUtil.cloneRow(result.rawCells()[0]), ROWS[4]));
    assertTrue(Bytes.equals(CellUtil.cloneRow(result.rawCells()[1]), ROWS[4]));
    assertTrue(Bytes.equals(CellUtil.cloneValue(result.rawCells()[0]), VALUES[1]));
    assertTrue(Bytes.equals(CellUtil.cloneValue(result.rawCells()[1]), VALUES[2]));
    scanner.close();

    // Add test of bulk deleting.
    for (int i = 0; i < 10; i++) {
      byte [] bytes = Bytes.toBytes(i);
      put = new Put(bytes);
      put.setDurability(Durability.SKIP_WAL);
      put.add(FAMILIES[0], QUALIFIER, bytes);@@@+      // Insert 4 more versions of same column and a dupe@@@+      put = new Put(ROW);@@@+      put.addColumn(FAMILY, QUALIFIER, STAMPS[9], VALUES[9]);@@@+      put.addColumn(FAMILY, QUALIFIER, STAMPS[11], VALUES[11]);@@@+      put.addColumn(FAMILY, QUALIFIER, STAMPS[13], VALUES[13]);@@@+      put.addColumn(FAMILY, QUALIFIER, STAMPS[15], VALUES[15]);@@@       ht.put(put);
    }
    for (int i = 0; i < 10; i++) {
      byte [] bytes = Bytes.toBytes(i);
      get = new Get(bytes);
      get.addFamily(FAMILIES[0]);@@@+@@@+      get = new Get(ROW);@@@+      get.addColumn(FAMILY, QUALIFIER);@@@+      get.readVersions(Integer.MAX_VALUE);@@@       result = ht.get(get);
      assertTrue(result.size() == 1);
    }
    ArrayList<Delete> deletes = new ArrayList<Delete>();
    for (int i = 0; i < 10; i++) {
      byte [] bytes = Bytes.toBytes(i);
      delete = new Delete(bytes);
      delete.deleteFamily(FAMILIES[0]);
      deletes.add(delete);
    }
    ht.delete(deletes);
    for (int i = 0; i < 10; i++) {
      byte [] bytes = Bytes.toBytes(i);
      get = new Get(bytes);
      get.addFamily(FAMILIES[0]);
      result = ht.get(get);
      assertTrue(result.size() == 0);
    }
  }
```
```
Cyclomatic Complexity	 9
Assertions		 11
Lines of Code		63
-  
  public void testJiraTest867() throws Exception {
    int numRows = 10;
    int numColsPerRow = 2000;

    byte [] TABLE = Bytes.toBytes("testJiraTest867");

    byte [][] ROWS = makeN(ROW, numRows);
    byte [][] QUALIFIERS = makeN(QUALIFIER, numColsPerRow);

    Table ht = TEST_UTIL.createTable(TABLE, FAMILY);
    TEST_UTIL.waitTableAvailable(TABLE, 10000);

    // Insert rows

    for(int i=0;i<numRows;i++) {
      Put put = new Put(ROWS[i]);
      put.setDurability(Durability.SKIP_WAL);
      for(int j=0;j<numColsPerRow;j++) {
        put.add(FAMILY, QUALIFIERS[j], QUALIFIERS[j]);
      }
      assertTrue("Put expected to contain " + numColsPerRow + " columns but " +
          "only contains " + put.size(), put.size() == numColsPerRow);
      ht.put(put);
    }

    // Get a row
    Get get = new Get(ROWS[numRows-1]);
    Result result = ht.get(get);
    assertNumKeys(result, numColsPerRow);
    Cell [] keys = result.rawCells();
    for(int i=0;i<result.size();i++) {
      assertKey(keys[i], ROWS[numRows-1], FAMILY, QUALIFIERS[i], QUALIFIERS[i]);
    }

    // Scan the rows
    Scan scan = new Scan();
    ResultScanner scanner = ht.getScanner(scan);
    int rowCount = 0;
    while((result = scanner.next()) != null) {
      assertNumKeys(result, numColsPerRow);
      Cell [] kvs = result.rawCells();
      for(int i=0;i<numColsPerRow;i++) {
        assertKey(kvs[i], ROWS[rowCount], FAMILY, QUALIFIERS[i], QUALIFIERS[i]);
      }
      rowCount++;
    }
    scanner.close();
    assertTrue("Expected to scan " + numRows + " rows but actually scanned "
        + rowCount + " rows", rowCount == numRows);

    // flush and try again

    TEST_UTIL.flush();

    // Get a row
    get = new Get(ROWS[numRows-1]);
    result = ht.get(get);
    assertNumKeys(result, numColsPerRow);
    keys = result.rawCells();
    for(int i=0;i<result.size();i++) {
      assertKey(keys[i], ROWS[numRows-1], FAMILY, QUALIFIERS[i], QUALIFIERS[i]);
    }

    // Scan the rows
    scan = new Scan();
    scanner = ht.getScanner(scan);
    rowCount = 0;
    while((result = scanner.next()) != null) {
      assertNumKeys(result, numColsPerRow);
      Cell [] kvs = result.rawCells();
      for(int i=0;i<numColsPerRow;i++) {
        assertKey(kvs[i], ROWS[rowCount], FAMILY, QUALIFIERS[i], QUALIFIERS[i]);
      }
      rowCount++;
    }
    scanner.close();
    assertTrue("Expected to scan " + numRows + " rows but actually scanned "
        + rowCount + " rows", rowCount == numRows);

  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		39
-  
  public void testJiraTest861() throws Exception {

    byte [] TABLE = Bytes.toBytes("testJiraTest861");
    byte [][] VALUES = makeNAscii(VALUE, 7);
    long [] STAMPS = makeStamps(7);

    Table ht = TEST_UTIL.createTable(TABLE, FAMILY, 10);
    TEST_UTIL.waitTableAvailable(TABLE, 10000);

    // Insert three versions

    Put put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, STAMPS[3], VALUES[3]);
    put.add(FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    put.add(FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    ht.put(put);

    // Get the middle value
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);

    // Try to get one version before (expect fail)
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[1]);

    // Try to get one version after (expect fail)
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[5]);

    // Try same from storefile
    TEST_UTIL.flush();
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[1]);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[5]);

    // Insert two more versions surrounding others, into memstore
    put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, STAMPS[0], VALUES[0]);
    put.add(FAMILY, QUALIFIER, STAMPS[6], VALUES[6]);
    ht.put(put);

    // Check we can get everything we should and can't get what we shouldn't
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[0], VALUES[0]);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[1]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[3], VALUES[3]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[5]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[6], VALUES[6]);

    // Try same from two storefiles
    TEST_UTIL.flush();
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[0], VALUES[0]);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[1]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[3], VALUES[3]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[5]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[6], VALUES[6]);

  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		33
-  
  public void testJiraTest33() throws Exception {

    byte [] TABLE = Bytes.toBytes("testJiraTest33");
    byte [][] VALUES = makeNAscii(VALUE, 7);
    long [] STAMPS = makeStamps(7);

    Table ht = TEST_UTIL.createTable(TABLE, FAMILY, 10);
    TEST_UTIL.waitTableAvailable(TABLE, 10000);

    // Insert lots versions

    Put put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, STAMPS[0], VALUES[0]);
    put.add(FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    put.add(FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    put.add(FAMILY, QUALIFIER, STAMPS[3], VALUES[3]);
    put.add(FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    put.add(FAMILY, QUALIFIER, STAMPS[5], VALUES[5]);
    ht.put(put);

    getVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 5);
    getVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 2);
    getVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 4, 5);
    getVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 2, 3);

    scanVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 5);
    scanVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 2);
    scanVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 4, 5);
    scanVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 2, 3);

    // Try same from storefile
    TEST_UTIL.flush();

    getVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 5);
    getVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 2);
    getVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 4, 5);
    getVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 2, 3);

    scanVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 5);
    scanVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 2);
    scanVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 4, 5);
    scanVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 2, 3);

  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		13
-  
  public void testJiraTest1014() throws Exception {

    byte [] TABLE = Bytes.toBytes("testJiraTest1014");

    Table ht = TEST_UTIL.createTable(TABLE, FAMILY, 10);
    TEST_UTIL.waitTableAvailable(TABLE, 10000);

    long manualStamp = 12345;

    // Insert lots versions

    Put put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, manualStamp, VALUE);
    ht.put(put);

    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, manualStamp, VALUE);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, manualStamp-1);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, manualStamp+1);

  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		29
-  
  public void testJiraTest1182() throws Exception {

    byte [] TABLE = Bytes.toBytes("testJiraTest1182");
    byte [][] VALUES = makeNAscii(VALUE, 7);
    long [] STAMPS = makeStamps(7);

    Table ht = TEST_UTIL.createTable(TABLE, FAMILY, 10);
    TEST_UTIL.waitTableAvailable(TABLE, 10000);

    // Insert lots versions

    Put put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, STAMPS[0], VALUES[0]);
    put.add(FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    put.add(FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    put.add(FAMILY, QUALIFIER, STAMPS[3], VALUES[3]);
    put.add(FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    put.add(FAMILY, QUALIFIER, STAMPS[5], VALUES[5]);
    ht.put(put);

    getVersionRangeAndVerifyGreaterThan(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 5);
    getVersionRangeAndVerifyGreaterThan(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 2, 5);
    getVersionRangeAndVerifyGreaterThan(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 4, 5);

    scanVersionRangeAndVerifyGreaterThan(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 5);
    scanVersionRangeAndVerifyGreaterThan(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 2, 5);
    scanVersionRangeAndVerifyGreaterThan(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 4, 5);

    // Try same from storefile
    TEST_UTIL.flush();

    getVersionRangeAndVerifyGreaterThan(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 5);
    getVersionRangeAndVerifyGreaterThan(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 2, 5);
    getVersionRangeAndVerifyGreaterThan(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 4, 5);

    scanVersionRangeAndVerifyGreaterThan(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 5);
    scanVersionRangeAndVerifyGreaterThan(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 2, 5);
    scanVersionRangeAndVerifyGreaterThan(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 4, 5);
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		21
-  
  public void testJiraTest52() throws Exception {
    byte [] TABLE = Bytes.toBytes("testJiraTest52");
    byte [][] VALUES = makeNAscii(VALUE, 7);
    long [] STAMPS = makeStamps(7);

    Table ht = TEST_UTIL.createTable(TABLE, FAMILY, 10);
    TEST_UTIL.waitTableAvailable(TABLE, 10000);

    // Insert lots versions

    Put put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, STAMPS[0], VALUES[0]);
    put.add(FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    put.add(FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    put.add(FAMILY, QUALIFIER, STAMPS[3], VALUES[3]);
    put.add(FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    put.add(FAMILY, QUALIFIER, STAMPS[5], VALUES[5]);
    ht.put(put);

    getAllVersionsAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 5);

    scanAllVersionsAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 5);

    // Try same from storefile
    TEST_UTIL.flush();

    getAllVersionsAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 5);

    scanAllVersionsAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 5);
  }
```
```
Cyclomatic Complexity	 1
Assertions		 12
Lines of Code		167
-  
  public void testDuplicateVersions() throws Exception {
    byte [] TABLE = Bytes.toBytes("testDuplicateVersions");

    long [] STAMPS = makeStamps(20);
    byte [][] VALUES = makeNAscii(VALUE, 20);

    Table ht = TEST_UTIL.createTable(TABLE, FAMILY, 10);
    TEST_UTIL.waitTableAvailable(TABLE, 10000);

    // Insert 4 versions of same column
    Put put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    put.add(FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    put.add(FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    put.add(FAMILY, QUALIFIER, STAMPS[5], VALUES[5]);
    ht.put(put);

    // Verify we can get each one properly
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[5], VALUES[5]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[5], VALUES[5]);

    // Verify we don't accidentally get others
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[0]);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[3]);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[6]);
    scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[0]);
    scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[3]);
    scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[6]);

    // Ensure maxVersions in query is respected
    Get get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIER);
    get.setMaxVersions(2);
    Result result = ht.get(get);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[4], STAMPS[5]},
        new byte[][] {VALUES[4], VALUES[5]},
        0, 1);

    Scan scan = new Scan(ROW);
    scan.addColumn(FAMILY, QUALIFIER);
    scan.setMaxVersions(2);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[4], STAMPS[5]},
        new byte[][] {VALUES[4], VALUES[5]},
        0, 1);

    // Flush and redo

    TEST_UTIL.flush();

    // Verify we can get each one properly
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[5], VALUES[5]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[5], VALUES[5]);

    // Verify we don't accidentally get others
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[0]);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[3]);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[6]);
    scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[0]);
    scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[3]);
    scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[6]);

    // Ensure maxVersions in query is respected
    get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIER);
    get.setMaxVersions(2);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[4], STAMPS[5]},
        new byte[][] {VALUES[4], VALUES[5]},
        0, 1);

    scan = new Scan(ROW);
    scan.addColumn(FAMILY, QUALIFIER);
    scan.setMaxVersions(2);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[4], STAMPS[5]},
        new byte[][] {VALUES[4], VALUES[5]},
        0, 1);


    // Add some memstore and retest

    // Insert 4 more versions of same column and a dupe
    put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, STAMPS[3], VALUES[3]);
    put.add(FAMILY, QUALIFIER, STAMPS[4], VALUES[14]);
    put.add(FAMILY, QUALIFIER, STAMPS[6], VALUES[6]);
    put.add(FAMILY, QUALIFIER, STAMPS[7], VALUES[7]);
    put.add(FAMILY, QUALIFIER, STAMPS[8], VALUES[8]);
    ht.put(put);

    // Ensure maxVersions in query is respected
    get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIER);
    get.setMaxVersions(7);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7], STAMPS[8]},
        new byte[][] {VALUES[2], VALUES[3], VALUES[14], VALUES[5], VALUES[6], VALUES[7], VALUES[8]},
        0, 6);

    scan = new Scan(ROW);
    scan.addColumn(FAMILY, QUALIFIER);
    scan.setMaxVersions(7);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7], STAMPS[8]},
        new byte[][] {VALUES[2], VALUES[3], VALUES[14], VALUES[5], VALUES[6], VALUES[7], VALUES[8]},
        0, 6);

    get = new Get(ROW);
    get.setMaxVersions(7);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7], STAMPS[8]},
        new byte[][] {VALUES[2], VALUES[3], VALUES[14], VALUES[5], VALUES[6], VALUES[7], VALUES[8]},
        0, 6);

    scan = new Scan(ROW);
    scan.setMaxVersions(7);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7], STAMPS[8]},
        new byte[][] {VALUES[2], VALUES[3], VALUES[14], VALUES[5], VALUES[6], VALUES[7], VALUES[8]},
        0, 6);

    // Verify we can get each one properly
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[14]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[7], VALUES[7]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[14]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[7], VALUES[7]);

    // Verify we don't accidentally get others
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[0]);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[9]);
    scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[0]);
    scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[9]);

    // Ensure maxVersions of table is respected

    TEST_UTIL.flush();

    // Insert 4 more versions of same column and a dupe
    put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, STAMPS[9], VALUES[9]);
    put.add(FAMILY, QUALIFIER, STAMPS[11], VALUES[11]);
    put.add(FAMILY, QUALIFIER, STAMPS[13], VALUES[13]);
    put.add(FAMILY, QUALIFIER, STAMPS[15], VALUES[15]);
    ht.put(put);

    get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIER);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7], STAMPS[8], STAMPS[9], STAMPS[11], STAMPS[13], STAMPS[15]},
        new byte[][] {VALUES[3], VALUES[14], VALUES[5], VALUES[6], VALUES[7], VALUES[8], VALUES[9], VALUES[11], VALUES[13], VALUES[15]},
        0, 9);

    scan = new Scan(ROW);
    scan.addColumn(FAMILY, QUALIFIER);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7], STAMPS[8], STAMPS[9], STAMPS[11], STAMPS[13], STAMPS[15]},
        new byte[][] {VALUES[3], VALUES[14], VALUES[5], VALUES[6], VALUES[7], VALUES[8], VALUES[9], VALUES[11], VALUES[13], VALUES[15]},
        0, 9);

    // Delete a version in the memstore and a version in a storefile
    Delete delete = new Delete(ROW);
    delete.deleteColumn(FAMILY, QUALIFIER, STAMPS[11]);
    delete.deleteColumn(FAMILY, QUALIFIER, STAMPS[7]);
    ht.delete(delete);

    // Test that it's gone
    get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIER);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[1], STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[8], STAMPS[9], STAMPS[13], STAMPS[15]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3], VALUES[14], VALUES[5], VALUES[6], VALUES[8], VALUES[9], VALUES[13], VALUES[15]},
        0, 9);

    scan = new Scan(ROW);
    scan.addColumn(FAMILY, QUALIFIER);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[1], STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[8], STAMPS[9], STAMPS[13], STAMPS[15]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3], VALUES[14], VALUES[5], VALUES[6], VALUES[8], VALUES[9], VALUES[13], VALUES[15]},
        0, 9);
  }
```
```
Cyclomatic Complexity	 1
Assertions		 4
Lines of Code		35
-  
  public void testUpdates() throws Exception {

    byte [] TABLE = Bytes.toBytes("testUpdates");
    Table hTable = TEST_UTIL.createTable(TABLE, FAMILY, 10);
    TEST_UTIL.waitTableAvailable(TABLE, 10000);

    // Write a column with values at timestamp 1, 2 and 3
    byte[] row = Bytes.toBytes("row1");
    byte[] qualifier = Bytes.toBytes("myCol");
    Put put = new Put(row);
    put.add(FAMILY, qualifier, 1L, Bytes.toBytes("AAA"));
    hTable.put(put);

    put = new Put(row);
    put.add(FAMILY, qualifier, 2L, Bytes.toBytes("BBB"));
    hTable.put(put);

    put = new Put(row);
    put.add(FAMILY, qualifier, 3L, Bytes.toBytes("EEE"));
    hTable.put(put);

    Get get = new Get(row);
    get.addColumn(FAMILY, qualifier);
    get.setMaxVersions();

    // Check that the column indeed has the right values at timestamps 1 and
    // 2
    Result result = hTable.get(get);
    NavigableMap<Long, byte[]> navigableMap =
        result.getMap().get(FAMILY).get(qualifier);
    assertEquals("AAA", Bytes.toString(navigableMap.get(1L)));
    assertEquals("BBB", Bytes.toString(navigableMap.get(2L)));

    // Update the value at timestamp 1
    put = new Put(row);
    put.add(FAMILY, qualifier, 1L, Bytes.toBytes("CCC"));
    hTable.put(put);

    // Update the value at timestamp 2
    put = new Put(row);
    put.add(FAMILY, qualifier, 2L, Bytes.toBytes("DDD"));
    hTable.put(put);

    // Check that the values at timestamp 2 and 1 got updated
    result = hTable.get(get);
    navigableMap = result.getMap().get(FAMILY).get(qualifier);
    assertEquals("CCC", Bytes.toString(navigableMap.get(1L)));
    assertEquals("DDD", Bytes.toString(navigableMap.get(2L)));
  }
```
```
Cyclomatic Complexity	 1
Assertions		 4
Lines of Code		42
-  
  public void testUpdatesWithMajorCompaction() throws Exception {

    TableName TABLE = TableName.valueOf("testUpdatesWithMajorCompaction");
    Table hTable = TEST_UTIL.createTable(TABLE, FAMILY, 10);
    TEST_UTIL.waitTableAvailable(TABLE, 10000);

    HBaseAdmin admin = new HBaseAdmin(TEST_UTIL.getConfiguration());

    // Write a column with values at timestamp 1, 2 and 3
    byte[] row = Bytes.toBytes("row2");
    byte[] qualifier = Bytes.toBytes("myCol");
    Put put = new Put(row);
    put.add(FAMILY, qualifier, 1L, Bytes.toBytes("AAA"));
    hTable.put(put);

    put = new Put(row);
    put.add(FAMILY, qualifier, 2L, Bytes.toBytes("BBB"));
    hTable.put(put);

    put = new Put(row);
    put.add(FAMILY, qualifier, 3L, Bytes.toBytes("EEE"));
    hTable.put(put);

    Get get = new Get(row);
    get.addColumn(FAMILY, qualifier);
    get.setMaxVersions();

    // Check that the column indeed has the right values at timestamps 1 and
    // 2
    Result result = hTable.get(get);
    NavigableMap<Long, byte[]> navigableMap =
        result.getMap().get(FAMILY).get(qualifier);
    assertEquals("AAA", Bytes.toString(navigableMap.get(1L)));
    assertEquals("BBB", Bytes.toString(navigableMap.get(2L)));

    // Trigger a major compaction
    admin.flush(TABLE);
    admin.majorCompact(TABLE);
    Thread.sleep(6000);

    // Update the value at timestamp 1
    put = new Put(row);
    put.add(FAMILY, qualifier, 1L, Bytes.toBytes("CCC"));
    hTable.put(put);

    // Update the value at timestamp 2
    put = new Put(row);
    put.add(FAMILY, qualifier, 2L, Bytes.toBytes("DDD"));
    hTable.put(put);

    // Trigger a major compaction
    admin.flush(TABLE);
    admin.majorCompact(TABLE);
    Thread.sleep(6000);

    // Check that the values at timestamp 2 and 1 got updated
    result = hTable.get(get);
    navigableMap = result.getMap().get(FAMILY).get(qualifier);
    assertEquals("CCC", Bytes.toString(navigableMap.get(1L)));
    assertEquals("DDD", Bytes.toString(navigableMap.get(2L)));
  }
```
```
Cyclomatic Complexity	 1
Assertions		 4
Lines of Code		46
-  
  public void testMajorCompactionBetweenTwoUpdates() throws Exception {

    String tableName = "testMajorCompactionBetweenTwoUpdates";
    byte [] TABLE = Bytes.toBytes(tableName);
    Table hTable = TEST_UTIL.createTable(TABLE, FAMILY, 10);
    TEST_UTIL.waitTableAvailable(TABLE, 10000);
    HBaseAdmin admin = new HBaseAdmin(TEST_UTIL.getConfiguration());

    // Write a column with values at timestamp 1, 2 and 3
    byte[] row = Bytes.toBytes("row3");
    byte[] qualifier = Bytes.toBytes("myCol");
    Put put = new Put(row);
    put.add(FAMILY, qualifier, 1L, Bytes.toBytes("AAA"));
    hTable.put(put);

    put = new Put(row);
    put.add(FAMILY, qualifier, 2L, Bytes.toBytes("BBB"));
    hTable.put(put);

    put = new Put(row);
    put.add(FAMILY, qualifier, 3L, Bytes.toBytes("EEE"));
    hTable.put(put);

    Get get = new Get(row);
    get.addColumn(FAMILY, qualifier);
    get.setMaxVersions();

    // Check that the column indeed has the right values at timestamps 1 and
    // 2
    Result result = hTable.get(get);
    NavigableMap<Long, byte[]> navigableMap =
        result.getMap().get(FAMILY).get(qualifier);
    assertEquals("AAA", Bytes.toString(navigableMap.get(1L)));
    assertEquals("BBB", Bytes.toString(navigableMap.get(2L)));

    // Trigger a major compaction
    admin.flush(tableName);
    admin.majorCompact(tableName);
    Thread.sleep(6000);

    // Update the value at timestamp 1
    put = new Put(row);
    put.add(FAMILY, qualifier, 1L, Bytes.toBytes("CCC"));
    hTable.put(put);

    // Trigger a major compaction
    admin.flush(tableName);
    admin.majorCompact(tableName);
    Thread.sleep(6000);

    // Update the value at timestamp 2
    put = new Put(row);
    put.add(FAMILY, qualifier, 2L, Bytes.toBytes("DDD"));
    hTable.put(put);

    // Trigger a major compaction
    admin.flush(tableName);
    admin.majorCompact(tableName);
    Thread.sleep(6000);

    // Check that the values at timestamp 2 and 1 got updated
    result = hTable.get(get);
    navigableMap = result.getMap().get(FAMILY).get(qualifier);

    assertEquals("CCC", Bytes.toString(navigableMap.get(1L)));
    assertEquals("DDD", Bytes.toString(navigableMap.get(2L)));
  }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		9
-  
  public void testGet_EmptyTable() throws IOException, InterruptedException {
    Table table = TEST_UTIL.createTable(TableName.valueOf("testGet_EmptyTable"), FAMILY);
    TEST_UTIL.waitTableAvailable(TableName.valueOf("testGet_EmptyTable"), 10000);
    Get get = new Get(ROW);
    get.addFamily(FAMILY);
    Result r = table.get(get);
    assertTrue(r.isEmpty());
  }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		20
-  
  public void testGet_NullQualifier() throws IOException, InterruptedException {
    Table table = TEST_UTIL.createTable(TableName.valueOf("testGet_NullQualifier"), FAMILY);
    TEST_UTIL.waitTableAvailable(TableName.valueOf("testGet_NullQualifier"), 10000);
    Put put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, VALUE);
    table.put(put);

    put = new Put(ROW);
    put.add(FAMILY, null, VALUE);
    table.put(put);
    LOG.info("Row put");

    Get get = new Get(ROW);
    get.addColumn(FAMILY, null);
    Result r = table.get(get);
    assertEquals(1, r.size());

    get = new Get(ROW);
    get.addFamily(FAMILY);
    r = table.get(get);
    assertEquals(2, r.size());
  }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		20
-  
  public void testGet_NonExistentRow() throws IOException, InterruptedException {
    Table table = TEST_UTIL.createTable(TableName.valueOf("testGet_NonExistentRow"), FAMILY);
    TEST_UTIL.waitTableAvailable(TableName.valueOf("testGet_NonExistentRow"), 10000);
    Put put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, VALUE);
    table.put(put);
    LOG.info("Row put");

    Get get = new Get(ROW);
    get.addFamily(FAMILY);
    Result r = table.get(get);
    assertFalse(r.isEmpty());
    System.out.println("Row retrieved successfully");

    byte [] missingrow = Bytes.toBytes("missingrow");
    get = new Get(missingrow);
    get.addFamily(FAMILY);
    r = table.get(get);
    assertTrue(r.isEmpty());
    LOG.info("Row missing as it should be");
  }
```
```
Cyclomatic Complexity	 4
Assertions		 5
Lines of Code		31
-  
  public void testPut() throws IOException, InterruptedException {
    final byte [] CONTENTS_FAMILY = Bytes.toBytes("contents");
    final byte [] SMALL_FAMILY = Bytes.toBytes("smallfam");
    final byte [] row1 = Bytes.toBytes("row1");
    final byte [] row2 = Bytes.toBytes("row2");
    final byte [] value = Bytes.toBytes("abcd");
    Table table = TEST_UTIL.createTable(Bytes.toBytes("testPut"),
      new byte [][] {CONTENTS_FAMILY, SMALL_FAMILY});
    TEST_UTIL.waitTableAvailable(Bytes.toBytes("testPut"), 10000);
    Put put = new Put(row1);
    put.add(CONTENTS_FAMILY, null, value);
    table.put(put);

    put = new Put(row2);
    put.add(CONTENTS_FAMILY, null, value);

    assertEquals(put.size(), 1);
    assertEquals(put.getFamilyCellMap().get(CONTENTS_FAMILY).size(), 1);

    // KeyValue v1 expectation.  Cast for now until we go all Cell all the time. TODO
    KeyValue kv = (KeyValue)put.getFamilyCellMap().get(CONTENTS_FAMILY).get(0);

    assertTrue(Bytes.equals(kv.getFamily(), CONTENTS_FAMILY));
    // will it return null or an empty byte array?
    assertTrue(Bytes.equals(kv.getQualifier(), new byte[0]));

    assertTrue(Bytes.equals(kv.getValue(), value));

    table.put(put);

    Scan scan = new Scan();
    scan.addColumn(CONTENTS_FAMILY, null);
    ResultScanner scanner = table.getScanner(scan);
    for (Result r : scanner) {
      for(Cell key : r.rawCells()) {
        System.out.println(Bytes.toString(r.getRow()) + ": " + key.toString());
      }
    }
  }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		16
-  
  public void testPutNoCF() throws IOException, InterruptedException {
    final byte[] BAD_FAM = Bytes.toBytes("BAD_CF");
    final byte[] VAL = Bytes.toBytes(100);
    Table table = TEST_UTIL.createTable(Bytes.toBytes("testPutNoCF"), FAMILY);
    TEST_UTIL.waitTableAvailable(Bytes.toBytes("testPutNoCF"), 10000);

    boolean caughtNSCFE = false;

    try {
      Put p = new Put(ROW);
      p.add(BAD_FAM, QUALIFIER, VAL);
      table.put(p);
    } catch (RetriesExhaustedWithDetailsException e) {
      caughtNSCFE = e.getCause(0) instanceof NoSuchColumnFamilyException;
    }
    assertTrue("Should throw NoSuchColumnFamilyException", caughtNSCFE);

  }
```
```
Cyclomatic Complexity	 3
Assertions		 1
Lines of Code		27
-  
  public void testRowsPut() throws IOException, InterruptedException {
    final byte[] CONTENTS_FAMILY = Bytes.toBytes("contents");
    final byte[] SMALL_FAMILY = Bytes.toBytes("smallfam");
    final int NB_BATCH_ROWS = 10;
    final byte[] value = Bytes.toBytes("abcd");
    Table table = TEST_UTIL.createTable(Bytes.toBytes("testRowsPut"),
      new byte[][] {CONTENTS_FAMILY, SMALL_FAMILY });
    TEST_UTIL.waitTableAvailable(Bytes.toBytes("testRowsPut"), 10000);
    ArrayList<Put> rowsUpdate = new ArrayList<Put>();
    for (int i = 0; i < NB_BATCH_ROWS; i++) {
      byte[] row = Bytes.toBytes("row" + i);
      Put put = new Put(row);
      put.setDurability(Durability.SKIP_WAL);
      put.add(CONTENTS_FAMILY, null, value);
      rowsUpdate.add(put);
    }
    table.put(rowsUpdate);
    Scan scan = new Scan();
    scan.addFamily(CONTENTS_FAMILY);
    ResultScanner scanner = table.getScanner(scan);
    int nbRows = 0;
    for (@SuppressWarnings("unused")
    Result row : scanner)
      nbRows++;
    assertEquals(NB_BATCH_ROWS, nbRows);
  }
```
```
Cyclomatic Complexity	 4
Assertions		 2
Lines of Code		40
-  
  public void testRowsPutBufferedOneFlush() throws IOException, InterruptedException {
    final byte [] CONTENTS_FAMILY = Bytes.toBytes("contents");
    final byte [] SMALL_FAMILY = Bytes.toBytes("smallfam");
    final byte [] value = Bytes.toBytes("abcd");
    final int NB_BATCH_ROWS = 10;
    HTable table = TEST_UTIL.createTable(Bytes.toBytes("testRowsPutBufferedOneFlush"),
      new byte [][] {CONTENTS_FAMILY, SMALL_FAMILY});
    TEST_UTIL.waitTableAvailable(Bytes.toBytes("testRowsPutBufferedOneFlush"),
        10000);
    table.setAutoFlush(false);
    ArrayList<Put> rowsUpdate = new ArrayList<Put>();
    for (int i = 0; i < NB_BATCH_ROWS * 10; i++) {
      byte[] row = Bytes.toBytes("row" + i);
      Put put = new Put(row);
      put.setDurability(Durability.SKIP_WAL);
      put.add(CONTENTS_FAMILY, null, value);
      rowsUpdate.add(put);
    }
    table.put(rowsUpdate);

    Scan scan = new Scan();
    scan.addFamily(CONTENTS_FAMILY);
    ResultScanner scanner = table.getScanner(scan);
    int nbRows = 0;
    for (@SuppressWarnings("unused")
    Result row : scanner)
      nbRows++;
    assertEquals(0, nbRows);
    scanner.close();

    table.flushCommits();

    scan = new Scan();
    scan.addFamily(CONTENTS_FAMILY);
    scanner = table.getScanner(scan);
    nbRows = 0;
    for (@SuppressWarnings("unused")
    Result row : scanner)
      nbRows++;
    assertEquals(NB_BATCH_ROWS * 10, nbRows);
    table.close();
  }
```
```
Cyclomatic Complexity	 3
Assertions		 1
Lines of Code		29
-  
  public void testRowsPutBufferedManyManyFlushes() throws IOException, InterruptedException {
    final byte[] CONTENTS_FAMILY = Bytes.toBytes("contents");
    final byte[] SMALL_FAMILY = Bytes.toBytes("smallfam");
    final byte[] value = Bytes.toBytes("abcd");
    final int NB_BATCH_ROWS = 10;
    HTable table = TEST_UTIL.createTable(Bytes.toBytes("testRowsPutBufferedManyManyFlushes"),
      new byte[][] {CONTENTS_FAMILY, SMALL_FAMILY });
    TEST_UTIL.waitTableAvailable(Bytes.toBytes("testRowsPutBufferedManyManyFlushes"),
        10000);
    table.setWriteBufferSize(10);
    ArrayList<Put> rowsUpdate = new ArrayList<Put>();
    for (int i = 0; i < NB_BATCH_ROWS * 10; i++) {
      byte[] row = Bytes.toBytes("row" + i);
      Put put = new Put(row);
      put.setDurability(Durability.SKIP_WAL);
      put.add(CONTENTS_FAMILY, null, value);
      rowsUpdate.add(put);
    }
    table.put(rowsUpdate);

    Scan scan = new Scan();
    scan.addFamily(CONTENTS_FAMILY);
    ResultScanner scanner = table.getScanner(scan);
    int nbRows = 0;
    for (@SuppressWarnings("unused")
    Result row : scanner)
      nbRows++;
    assertEquals(NB_BATCH_ROWS * 10, nbRows);
  }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		25
-  
  public void testAddKeyValue() throws IOException {
    final byte[] CONTENTS_FAMILY = Bytes.toBytes("contents");
    final byte[] value = Bytes.toBytes("abcd");
    final byte[] row1 = Bytes.toBytes("row1");
    final byte[] row2 = Bytes.toBytes("row2");
    byte[] qualifier = Bytes.toBytes("qf1");
    Put put = new Put(row1);

    // Adding KeyValue with the same row
    KeyValue kv = new KeyValue(row1, CONTENTS_FAMILY, qualifier, value);
    boolean ok = true;
    try {
      put.add(kv);
    } catch (IOException e) {
      ok = false;
    }
    assertEquals(true, ok);

    // Adding KeyValue with the different row
    kv = new KeyValue(row2, CONTENTS_FAMILY, qualifier, value);
    ok = false;
    try {
      put.add(kv);
    } catch (IOException e) {
      ok = true;
    }
    assertEquals(true, ok);
  }
```
```
Cyclomatic Complexity	 3
Assertions		 6
Lines of Code		19
-  
  public void testAppendWithoutWAL() throws Exception {
    List<Result> resultsWithWal = doAppend(true);
    List<Result> resultsWithoutWal = doAppend(false);
    assertEquals(resultsWithWal.size(), resultsWithoutWal.size());
    for (int i = 0; i != resultsWithWal.size(); ++i) {
      Result resultWithWal = resultsWithWal.get(i);
      Result resultWithoutWal = resultsWithoutWal.get(i);
      assertEquals(resultWithWal.rawCells().length, resultWithoutWal.rawCells().length);
      for (int j = 0; j != resultWithWal.rawCells().length; ++j) {
        Cell cellWithWal = resultWithWal.rawCells()[j];
        Cell cellWithoutWal = resultWithoutWal.rawCells()[j];
        assertTrue(Bytes.equals(CellUtil.cloneRow(cellWithWal), CellUtil.cloneRow(cellWithoutWal)));
        assertTrue(Bytes.equals(CellUtil.cloneFamily(cellWithWal), CellUtil.cloneFamily(cellWithoutWal)));
        assertTrue(Bytes.equals(CellUtil.cloneQualifier(cellWithWal), CellUtil.cloneQualifier(cellWithoutWal)));
        assertTrue(Bytes.equals(CellUtil.cloneValue(cellWithWal), CellUtil.cloneValue(cellWithoutWal)));
      }
    }
  }
```
```
Cyclomatic Complexity	 10
Assertions		 2
Lines of Code		74
-  
  public void testHBase737() throws IOException, InterruptedException {
    final byte [] FAM1 = Bytes.toBytes("fam1");
    final byte [] FAM2 = Bytes.toBytes("fam2");
    // Open table
    Table table = TEST_UTIL.createTable(Bytes.toBytes("testHBase737"),
      new byte [][] {FAM1, FAM2});
    TEST_UTIL.waitTableAvailable(Bytes.toBytes("testHBase737"), 10000);
    // Insert some values
    Put put = new Put(ROW);
    put.add(FAM1, Bytes.toBytes("letters"), Bytes.toBytes("abcdefg"));
    table.put(put);
    try {
      Thread.sleep(1000);
    } catch (InterruptedException i) {
      //ignore
    }

    put = new Put(ROW);
    put.add(FAM1, Bytes.toBytes("numbers"), Bytes.toBytes("123456"));
    table.put(put);

    try {
      Thread.sleep(1000);
    } catch (InterruptedException i) {
      //ignore
    }

    put = new Put(ROW);
    put.add(FAM2, Bytes.toBytes("letters"), Bytes.toBytes("hijklmnop"));
    table.put(put);

    long times[] = new long[3];

    // First scan the memstore

    Scan scan = new Scan();
    scan.addFamily(FAM1);
    scan.addFamily(FAM2);
    ResultScanner s = table.getScanner(scan);
    try {
      int index = 0;
      Result r = null;
      while ((r = s.next()) != null) {
        for(Cell key : r.rawCells()) {
          times[index++] = key.getTimestamp();
        }
      }
    } finally {
      s.close();
    }
    for (int i = 0; i < times.length - 1; i++) {
      for (int j = i + 1; j < times.length; j++) {
        assertTrue(times[j] > times[i]);
      }
    }

    // Flush data to disk and try again
    TEST_UTIL.flush();

    // Reset times
    for(int i=0;i<times.length;i++) {
      times[i] = 0;
    }

    try {
      Thread.sleep(1000);
    } catch (InterruptedException i) {
      //ignore
    }
    scan = new Scan();
    scan.addFamily(FAM1);
    scan.addFamily(FAM2);
    s = table.getScanner(scan);
    try {
      int index = 0;
      Result r = null;
      while ((r = s.next()) != null) {
        for(Cell key : r.rawCells()) {
          times[index++] = key.getTimestamp();
        }
      }
    } finally {
      s.close();
    }
    for (int i = 0; i < times.length - 1; i++) {
      for (int j = i + 1; j < times.length; j++) {
        assertTrue(times[j] > times[i]);
      }
    }
  }
```
```
Cyclomatic Complexity	 5
Assertions		 2
Lines of Code		27
-  
  public void testListTables() throws IOException, InterruptedException {
    TableName t1 = TableName.valueOf("testListTables1");
    TableName t2 = TableName.valueOf("testListTables2");
    TableName t3 = TableName.valueOf("testListTables3");
    TableName [] tables = new TableName[] { t1, t2, t3 };
    for (int i = 0; i < tables.length; i++) {
      TEST_UTIL.createTable(tables[i], FAMILY);
      TEST_UTIL.waitTableAvailable(tables[i], 10000);
    }
    Admin admin = new HBaseAdmin(TEST_UTIL.getConfiguration());
    HTableDescriptor[] ts = admin.listTables();
    HashSet<HTableDescriptor> result = new HashSet<HTableDescriptor>(ts.length);
    Collections.addAll(result, ts);
    int size = result.size();
    assertTrue(size >= tables.length);
    for (int i = 0; i < tables.length && i < size; i++) {
      boolean found = false;
      for (int j = 0; j < ts.length; j++) {
        if (ts[j].getTableName().equals(tables[i])) {
          found = true;
          break;
        }
      }
      assertTrue("Not found: " + tables[i], found);
    }
  }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		9
-  
  public void testUnmanagedHConnection() throws IOException, InterruptedException {
    final TableName tableName = TableName.valueOf("testUnmanagedHConnection");
    HTable t = createUnmangedHConnectionHTable(tableName);
    HBaseAdmin ha = new HBaseAdmin(t.getConnection());
    assertTrue(ha.tableExists(tableName));
    assertTrue(t.get(new Get(ROW)).isEmpty());
    ha.close();
  }
```
```
Cyclomatic Complexity	 1
Assertions		 5
Lines of Code		19
-  
  public void testUnmanagedHConnectionReconnect() throws Exception {
    final TableName tableName = TableName.valueOf("testUnmanagedHConnectionReconnect");
    HTable t = createUnmangedHConnectionHTable(tableName);
    Connection conn = t.getConnection();
    try (HBaseAdmin ha = new HBaseAdmin(conn)) {
      assertTrue(ha.tableExists(tableName));
      assertTrue(t.get(new Get(ROW)).isEmpty());
    }

    // stop the master
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    cluster.stopMaster(0, false);
    cluster.waitOnMaster(0);

    // start up a new master
    cluster.startMaster();
    assertTrue(cluster.waitForActiveAndReadyMaster());

    // test that the same unmanaged connection works with a new
    // HBaseAdmin and can connect to the new master;
    try (HBaseAdmin newAdmin = new HBaseAdmin(conn)) {
      assertTrue(newAdmin.tableExists(tableName));
      assertTrue(newAdmin.getClusterStatus().getServersSize() == SLAVES);
    }
  }
```
```
Cyclomatic Complexity	 7
Assertions		 5
Lines of Code		55
-  
  public void testMiscHTableStuff() throws IOException, InterruptedException {
    final TableName tableAname = TableName.valueOf("testMiscHTableStuffA");
    final TableName tableBname = TableName.valueOf("testMiscHTableStuffB");
    final byte[] attrName = Bytes.toBytes("TESTATTR");
    final byte[] attrValue = Bytes.toBytes("somevalue");
    byte[] value = Bytes.toBytes("value");

    Table a = TEST_UTIL.createTable(tableAname, HConstants.CATALOG_FAMILY);
    TEST_UTIL.waitTableAvailable(tableAname, 10000);
    Table b = TEST_UTIL.createTable(tableBname, HConstants.CATALOG_FAMILY);
    TEST_UTIL.waitTableAvailable(tableBname, 10000);
    Put put = new Put(ROW);
    put.add(HConstants.CATALOG_FAMILY, null, value);
    a.put(put);

    // open a new connection to A and a connection to b
    Table newA = new HTable(TEST_UTIL.getConfiguration(), tableAname);

    // copy data from A to B
    Scan scan = new Scan();
    scan.addFamily(HConstants.CATALOG_FAMILY);
    ResultScanner s = newA.getScanner(scan);
    try {
      for (Result r : s) {
        put = new Put(r.getRow());
        put.setDurability(Durability.SKIP_WAL);
        for (Cell kv : r.rawCells()) {
          put.add(kv);
        }
        b.put(put);
      }
    } finally {
      s.close();
    }

    // Opening a new connection to A will cause the tables to be reloaded
    Table anotherA = new HTable(TEST_UTIL.getConfiguration(), tableAname);
    Get get = new Get(ROW);
    get.addFamily(HConstants.CATALOG_FAMILY);
    anotherA.get(get);

    // We can still access A through newA because it has the table information
    // cached. And if it needs to recalibrate, that will cause the information
    // to be reloaded.

    // Test user metadata
    Admin admin = new HBaseAdmin(TEST_UTIL.getConfiguration());
    // make a modifiable descriptor
    HTableDescriptor desc = new HTableDescriptor(a.getTableDescriptor());
    // offline the table
    admin.disableTable(tableAname);
    // add a user attribute to HTD
    desc.setValue(attrName, attrValue);
    // add a user attribute to HCD
    for (HColumnDescriptor c : desc.getFamilies())
      c.setValue(attrName, attrValue);
    // update metadata for all regions of this table
    admin.modifyTable(tableAname, desc);
    // enable the table
    admin.enableTable(tableAname);

    // Test that attribute changes were applied
    desc = a.getTableDescriptor();
    assertEquals("wrong table descriptor returned", desc.getTableName(), tableAname);
    // check HTD attribute
    value = desc.getValue(attrName);
    assertFalse("missing HTD attribute value", value == null);
    assertFalse("HTD attribute value is incorrect",
      Bytes.compareTo(value, attrValue) != 0);
    // check HCD attribute
    for (HColumnDescriptor c : desc.getFamilies()) {
      value = c.getValue(attrName);
      assertFalse("missing HCD attribute value", value == null);
      assertFalse("HCD attribute value is incorrect",
        Bytes.compareTo(value, attrValue) != 0);
    }
  }
```
```
Cyclomatic Complexity	 1
Assertions		 25
Lines of Code		73
-  
  public void testGetClosestRowBefore() throws IOException, InterruptedException {
    final TableName tableAname = TableName.valueOf("testGetClosestRowBefore");
    final byte[] firstRow = Bytes.toBytes("row111");
    final byte[] secondRow = Bytes.toBytes("row222");
    final byte[] thirdRow = Bytes.toBytes("row333");
    final byte[] forthRow = Bytes.toBytes("row444");
    final byte[] beforeFirstRow = Bytes.toBytes("row");
    final byte[] beforeSecondRow = Bytes.toBytes("row22");
    final byte[] beforeThirdRow = Bytes.toBytes("row33");
    final byte[] beforeForthRow = Bytes.toBytes("row44");

    HTable table =
        TEST_UTIL.createTable(tableAname,
          new byte[][] { HConstants.CATALOG_FAMILY, Bytes.toBytes("info2") }, 1,
            1024);
    TEST_UTIL.waitTableAvailable(tableAname, 10000);
    // set block size to 64 to making 2 kvs into one block, bypassing the walkForwardInSingleRow
    // in Store.rowAtOrBeforeFromStoreFile
    String regionName = table.getRegionLocations().firstKey().getEncodedName();
    Region region =
        TEST_UTIL.getRSForFirstRegionInTable(tableAname).getFromOnlineRegions(regionName);
    Put put1 = new Put(firstRow);
    Put put2 = new Put(secondRow);
    Put put3 = new Put(thirdRow);
    Put put4 = new Put(forthRow);
    byte[] one = new byte[] { 1 };
    byte[] two = new byte[] { 2 };
    byte[] three = new byte[] { 3 };
    byte[] four = new byte[] { 4 };

    put1.add(HConstants.CATALOG_FAMILY, null, one);
    put2.add(HConstants.CATALOG_FAMILY, null, two);
    put3.add(HConstants.CATALOG_FAMILY, null, three);
    put4.add(HConstants.CATALOG_FAMILY, null, four);
    table.put(put1);
    table.put(put2);
    table.put(put3);
    table.put(put4);
    region.flush(true);
    Result result = null;

    // Test before first that null is returned
    result = table.getRowOrBefore(beforeFirstRow, HConstants.CATALOG_FAMILY);
    assertTrue(result == null);

    // Test at first that first is returned
    result = table.getRowOrBefore(firstRow, HConstants.CATALOG_FAMILY);
    assertTrue(result.containsColumn(HConstants.CATALOG_FAMILY, null));
    assertTrue(Bytes.equals(result.getRow(), firstRow));
    assertTrue(Bytes.equals(result.getValue(HConstants.CATALOG_FAMILY, null), one));

    // Test in between first and second that first is returned
    result = table.getRowOrBefore(beforeSecondRow, HConstants.CATALOG_FAMILY);
    assertTrue(result.containsColumn(HConstants.CATALOG_FAMILY, null));
    assertTrue(Bytes.equals(result.getRow(), firstRow));
    assertTrue(Bytes.equals(result.getValue(HConstants.CATALOG_FAMILY, null), one));

    // Test at second make sure second is returned
    result = table.getRowOrBefore(secondRow, HConstants.CATALOG_FAMILY);
    assertTrue(result.containsColumn(HConstants.CATALOG_FAMILY, null));
    assertTrue(Bytes.equals(result.getRow(), secondRow));
    assertTrue(Bytes.equals(result.getValue(HConstants.CATALOG_FAMILY, null), two));

    // Test in second and third, make sure second is returned
    result = table.getRowOrBefore(beforeThirdRow, HConstants.CATALOG_FAMILY);
    assertTrue(result.containsColumn(HConstants.CATALOG_FAMILY, null));
    assertTrue(Bytes.equals(result.getRow(), secondRow));
    assertTrue(Bytes.equals(result.getValue(HConstants.CATALOG_FAMILY, null), two));

    // Test at third make sure third is returned
    result = table.getRowOrBefore(thirdRow, HConstants.CATALOG_FAMILY);
    assertTrue(result.containsColumn(HConstants.CATALOG_FAMILY, null));
    assertTrue(Bytes.equals(result.getRow(), thirdRow));
    assertTrue(Bytes.equals(result.getValue(HConstants.CATALOG_FAMILY, null), three));

    // Test in third and forth, make sure third is returned
    result = table.getRowOrBefore(beforeForthRow, HConstants.CATALOG_FAMILY);
    assertTrue(result.containsColumn(HConstants.CATALOG_FAMILY, null));
    assertTrue(Bytes.equals(result.getRow(), thirdRow));
    assertTrue(Bytes.equals(result.getValue(HConstants.CATALOG_FAMILY, null), three));

    // Test at forth make sure forth is returned
    result = table.getRowOrBefore(forthRow, HConstants.CATALOG_FAMILY);
    assertTrue(result.containsColumn(HConstants.CATALOG_FAMILY, null));
    assertTrue(Bytes.equals(result.getRow(), forthRow));
    assertTrue(Bytes.equals(result.getValue(HConstants.CATALOG_FAMILY, null), four));

    // Test after forth make sure forth is returned
    result = table.getRowOrBefore(Bytes.add(forthRow, one), HConstants.CATALOG_FAMILY);
    assertTrue(result.containsColumn(HConstants.CATALOG_FAMILY, null));
    assertTrue(Bytes.equals(result.getRow(), forthRow));
    assertTrue(Bytes.equals(result.getValue(HConstants.CATALOG_FAMILY, null), four));

    table.close();
  }
```
```
Cyclomatic Complexity	 1
Assertions		 3
Lines of Code		11
-  
  public void testScanVariableReuse() throws Exception {
    Scan scan = new Scan();
    scan.addFamily(FAMILY);
    scan.addColumn(FAMILY, ROW);

    assertTrue(scan.getFamilyMap().get(FAMILY).size() == 1);

    scan = new Scan();
    scan.addFamily(FAMILY);

    assertTrue(scan.getFamilyMap().get(FAMILY) == null);
    assertTrue(scan.getFamilyMap().containsKey(FAMILY));
  }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		28
-  
  public void testMultiRowMutation() throws Exception {
    LOG.info("Starting testMultiRowMutation");
    final TableName TABLENAME = TableName.valueOf("testMultiRowMutation");
    final byte [] ROW1 = Bytes.toBytes("testRow1");

    Table t = TEST_UTIL.createTable(TABLENAME, FAMILY);
    TEST_UTIL.waitTableAvailable(TABLENAME, 10000);
    Put p = new Put(ROW);
    p.add(FAMILY, QUALIFIER, VALUE);
    MutationProto m1 = ProtobufUtil.toMutation(MutationType.PUT, p);

    p = new Put(ROW1);
    p.add(FAMILY, QUALIFIER, VALUE);
    MutationProto m2 = ProtobufUtil.toMutation(MutationType.PUT, p);

    MutateRowsRequest.Builder mrmBuilder = MutateRowsRequest.newBuilder();
    mrmBuilder.addMutationRequest(m1);
    mrmBuilder.addMutationRequest(m2);
    MutateRowsRequest mrm = mrmBuilder.build();
    CoprocessorRpcChannel channel = t.coprocessorService(ROW);
    MultiRowMutationService.BlockingInterface service =
       MultiRowMutationService.newBlockingStub(channel);
    service.mutateRows(null, mrm);
    Get g = new Get(ROW);
    Result r = t.get(g);
    assertEquals(0, Bytes.compareTo(VALUE, r.getValue(FAMILY, QUALIFIER)));
    g = new Get(ROW1);
    r = t.get(g);
    assertEquals(0, Bytes.compareTo(VALUE, r.getValue(FAMILY, QUALIFIER)));
  }
```
```
Cyclomatic Complexity	 3
Assertions		 3
Lines of Code		44
-  
  public void testRowMutation() throws Exception {
    LOG.info("Starting testRowMutation");
    final TableName TABLENAME = TableName.valueOf("testRowMutation");
    Table t = TEST_UTIL.createTable(TABLENAME, FAMILY);
    TEST_UTIL.waitTableAvailable(TABLENAME, 10000);
    byte [][] QUALIFIERS = new byte [][] {
        Bytes.toBytes("a"), Bytes.toBytes("b")
    };
    RowMutations arm = new RowMutations(ROW);
    Put p = new Put(ROW);
    p.add(FAMILY, QUALIFIERS[0], VALUE);
    arm.add(p);
    t.mutateRow(arm);

    Get g = new Get(ROW);
    Result r = t.get(g);
    assertEquals(0, Bytes.compareTo(VALUE, r.getValue(FAMILY, QUALIFIERS[0])));

    arm = new RowMutations(ROW);
    p = new Put(ROW);
    p.add(FAMILY, QUALIFIERS[1], VALUE);
    arm.add(p);
    Delete d = new Delete(ROW);
    d.deleteColumns(FAMILY, QUALIFIERS[0]);
    arm.add(d);
    // TODO: Trying mutateRow again.  The batch was failing with a one try only.
    t.mutateRow(arm);
    r = t.get(g);
    assertEquals(0, Bytes.compareTo(VALUE, r.getValue(FAMILY, QUALIFIERS[1])));
    assertNull(r.getValue(FAMILY, QUALIFIERS[0]));

    //Test that we get a region level exception
    try {
      arm = new RowMutations(ROW);
      p = new Put(ROW);
      p.add(new byte[]{'b', 'o', 'g', 'u', 's'}, QUALIFIERS[0], VALUE);
      arm.add(p);
      t.mutateRow(arm);
      fail("Expected NoSuchColumnFamilyException");
    } catch(RetriesExhaustedWithDetailsException e) {
      for(Throwable rootCause: e.getCauses()){
        if(rootCause instanceof NoSuchColumnFamilyException){
          return;
        }
      }
      throw e;
    }
  }
```
```
Cyclomatic Complexity	 1
Assertions		 6
Lines of Code		27
-  
  public void testAppend() throws Exception {
    LOG.info("Starting testAppend");
    final TableName TABLENAME = TableName.valueOf("testAppend");
    Table t = TEST_UTIL.createTable(TABLENAME, FAMILY);
    TEST_UTIL.waitTableAvailable(TABLENAME, 10000);
    byte[] v1 = Bytes.toBytes("42");
    byte[] v2 = Bytes.toBytes("23");
    byte [][] QUALIFIERS = new byte [][] {
        Bytes.toBytes("b"), Bytes.toBytes("a"), Bytes.toBytes("c")
    };
    Append a = new Append(ROW);
    a.add(FAMILY, QUALIFIERS[0], v1);
    a.add(FAMILY, QUALIFIERS[1], v2);
    a.setReturnResults(false);
    assertNullResult(t.append(a));

    a = new Append(ROW);
    a.add(FAMILY, QUALIFIERS[0], v2);
    a.add(FAMILY, QUALIFIERS[1], v1);
    a.add(FAMILY, QUALIFIERS[2], v2);
    Result r = t.append(a);
    assertEquals(0, Bytes.compareTo(Bytes.add(v1,v2), r.getValue(FAMILY, QUALIFIERS[0])));
    assertEquals(0, Bytes.compareTo(Bytes.add(v2,v1), r.getValue(FAMILY, QUALIFIERS[1])));
    // QUALIFIERS[2] previously not exist, verify both value and timestamp are correct
    assertEquals(0, Bytes.compareTo(v2, r.getValue(FAMILY, QUALIFIERS[2])));
    assertEquals(r.getColumnLatest(FAMILY, QUALIFIERS[0]).getTimestamp(),
        r.getColumnLatest(FAMILY, QUALIFIERS[2]).getTimestamp());
  }
```
```
Cyclomatic Complexity	 3
Assertions		 2
Lines of Code		30
-  
  public void testClientPoolRoundRobin() throws IOException, InterruptedException {
    final TableName tableName = TableName.valueOf("testClientPoolRoundRobin");

    int poolSize = 3;
    int numVersions = poolSize * 2;
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.set(HConstants.HBASE_CLIENT_IPC_POOL_TYPE, "round-robin");
    conf.setInt(HConstants.HBASE_CLIENT_IPC_POOL_SIZE, poolSize);

    Table table = TEST_UTIL.createTable(tableName, new byte[][] { FAMILY }, conf, Integer.MAX_VALUE);
    TEST_UTIL.waitTableAvailable(tableName, 10000);

    final long ts = EnvironmentEdgeManager.currentTime();
    Get get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIER);
    get.setMaxVersions();

    for (int versions = 1; versions <= numVersions; versions++) {
      Put put = new Put(ROW);
      put.add(FAMILY, QUALIFIER, ts + versions, VALUE);
      table.put(put);

      Result result = table.get(get);
      NavigableMap<Long, byte[]> navigableMap = result.getMap().get(FAMILY)
          .get(QUALIFIER);

      assertEquals("The number of versions of '" + Bytes.toString(FAMILY) + ":"
          + Bytes.toString(QUALIFIER) + " did not match", versions, navigableMap.size());
      for (Map.Entry<Long, byte[]> entry : navigableMap.entrySet()) {
        assertTrue("The value at time " + entry.getKey()
            + " did not match what was put",
            Bytes.equals(VALUE, entry.getValue()));
      }
    }
  }
```
```
Cyclomatic Complexity	 -1
Assertions		 5
Lines of Code		71
-8989") 
  public void testClientPoolThreadLocal() throws IOException, InterruptedException {
    final TableName tableName = TableName.valueOf("testClientPoolThreadLocal");

    int poolSize = Integer.MAX_VALUE;
    int numVersions = 3;
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.set(HConstants.HBASE_CLIENT_IPC_POOL_TYPE, "thread-local");
    conf.setInt(HConstants.HBASE_CLIENT_IPC_POOL_SIZE, poolSize);

    final Table table = TEST_UTIL.createTable(tableName,
        new byte[][] { FAMILY }, conf, 3);
    TEST_UTIL.waitTableAvailable(tableName, 10000);
    final long ts = EnvironmentEdgeManager.currentTime();
    final Get get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIER);
    get.setMaxVersions();

    for (int versions = 1; versions <= numVersions; versions++) {
      Put put = new Put(ROW);
      put.add(FAMILY, QUALIFIER, ts + versions, VALUE);
      table.put(put);

      Result result = table.get(get);
      NavigableMap<Long, byte[]> navigableMap = result.getMap().get(FAMILY)
          .get(QUALIFIER);

      assertEquals("The number of versions of '" + Bytes.toString(FAMILY) + ":"
          + Bytes.toString(QUALIFIER) + " did not match", versions, navigableMap.size());
      for (Map.Entry<Long, byte[]> entry : navigableMap.entrySet()) {
        assertTrue("The value at time " + entry.getKey()
            + " did not match what was put",
            Bytes.equals(VALUE, entry.getValue()));
      }
    }

    final Object waitLock = new Object();
    ExecutorService executorService = Executors.newFixedThreadPool(numVersions);
    final AtomicReference<AssertionError> error = new AtomicReference<AssertionError>(null);
    for (int versions = numVersions; versions < numVersions * 2; versions++) {
      final int versionsCopy = versions;
      executorService.submit(new Callable<Void>() {
        @Override
        public Void call() {
          try {
            Put put = new Put(ROW);
            put.add(FAMILY, QUALIFIER, ts + versionsCopy, VALUE);
            table.put(put);

            Result result = table.get(get);
            NavigableMap<Long, byte[]> navigableMap = result.getMap()
                .get(FAMILY).get(QUALIFIER);

            assertEquals("The number of versions of '" + Bytes.toString(FAMILY) + ":"
                + Bytes.toString(QUALIFIER) + " did not match " + versionsCopy, versionsCopy,
                navigableMap.size());
            for (Map.Entry<Long, byte[]> entry : navigableMap.entrySet()) {
              assertTrue("The value at time " + entry.getKey()
                  + " did not match what was put",
                  Bytes.equals(VALUE, entry.getValue()));
            }
            synchronized (waitLock) {
              waitLock.wait();
            }
          } catch (Exception e) {
          } catch (AssertionError e) {
            // the error happens in a thread, it won't fail the test,
            // need to pass it to the caller for proper handling.
            error.set(e);
            LOG.error(e);
          }

          return null;
        }
      });
    }
    synchronized (waitLock) {
      waitLock.notifyAll();
    }
    executorService.shutdownNow();
    assertNull(error.get());
  }
```
```
Cyclomatic Complexity	 3
Assertions		 4
Lines of Code		25
-  
  public void testCheckAndPut() throws IOException, InterruptedException {
    final byte [] anotherrow = Bytes.toBytes("anotherrow");
    final byte [] value2 = Bytes.toBytes("abcd");

    Table table = TEST_UTIL.createTable(TableName.valueOf("testCheckAndPut"), FAMILY);
    TEST_UTIL.waitTableAvailable(TableName.valueOf("testCheckAndPut"), 10000);
    Put put1 = new Put(ROW);
    put1.add(FAMILY, QUALIFIER, VALUE);

    // row doesn't exist, so using non-null value should be considered "not match".
    boolean ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, VALUE, put1);
    assertEquals(ok, false);

    // row doesn't exist, so using "null" to check for existence should be considered "match".
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, null, put1);
    assertEquals(ok, true);

    // row now exists, so using "null" to check for existence should be considered "not match".
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, null, put1);
    assertEquals(ok, false);

    Put put2 = new Put(ROW);
    put2.add(FAMILY, QUALIFIER, value2);

    // row now exists, use the matching value to check
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, VALUE, put2);
    assertEquals(ok, true);

    Put put3 = new Put(anotherrow);
    put3.add(FAMILY, QUALIFIER, VALUE);

    // try to do CheckAndPut on different rows
    try {
        ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, value2, put3);
        fail("trying to check and modify different rows should have failed.");
    } catch(Exception e) {}
  }
```
```
Cyclomatic Complexity	 2
Assertions		 19
Lines of Code		51
-  
  public void testCheckAndPutWithCompareOp() throws IOException, InterruptedException {
    final byte [] value1 = Bytes.toBytes("aaaa");
    final byte [] value2 = Bytes.toBytes("bbbb");
    final byte [] value3 = Bytes.toBytes("cccc");
    final byte [] value4 = Bytes.toBytes("dddd");

    Table table = TEST_UTIL.createTable(TableName.valueOf("testCheckAndPutWithCompareOp"), FAMILY);
    TEST_UTIL.waitTableAvailable(TableName.valueOf("testCheckAndPutWithCompareOp"), 10000);

    Put put2 = new Put(ROW);
    put2.add(FAMILY, QUALIFIER, value2);

    Put put3 = new Put(ROW);
    put3.add(FAMILY, QUALIFIER, value3);

    // row doesn't exist, so using "null" to check for existence should be considered "match".
    boolean ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, null, put2);
    assertEquals(ok, true);

    // cell = "bbbb", using "aaaa" to compare only LESS/LESS_OR_EQUAL/NOT_EQUAL
    // turns out "match"
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, CompareOp.GREATER, value1, put2);
    assertEquals(ok, false);
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, CompareOp.EQUAL, value1, put2);
    assertEquals(ok, false);
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, CompareOp.GREATER_OR_EQUAL, value1, put2);
    assertEquals(ok, false);
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, CompareOp.LESS, value1, put2);
    assertEquals(ok, true);
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, CompareOp.LESS_OR_EQUAL, value1, put2);
    assertEquals(ok, true);
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, CompareOp.NOT_EQUAL, value1, put3);
    assertEquals(ok, true);

    // cell = "cccc", using "dddd" to compare only LARGER/LARGER_OR_EQUAL/NOT_EQUAL
    // turns out "match"
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, CompareOp.LESS, value4, put3);
    assertEquals(ok, false);
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, CompareOp.LESS_OR_EQUAL, value4, put3);
    assertEquals(ok, false);
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, CompareOp.EQUAL, value4, put3);
    assertEquals(ok, false);
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, CompareOp.GREATER, value4, put3);
    assertEquals(ok, true);
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, CompareOp.GREATER_OR_EQUAL, value4, put3);
    assertEquals(ok, true);
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, CompareOp.NOT_EQUAL, value4, put2);
    assertEquals(ok, true);

    // cell = "bbbb", using "bbbb" to compare only GREATER_OR_EQUAL/LESS_OR_EQUAL/EQUAL
    // turns out "match"
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, CompareOp.GREATER, value2, put2);
    assertEquals(ok, false);
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, CompareOp.NOT_EQUAL, value2, put2);
    assertEquals(ok, false);
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, CompareOp.LESS, value2, put2);
    assertEquals(ok, false);
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, CompareOp.GREATER_OR_EQUAL, value2, put2);
    assertEquals(ok, true);
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, CompareOp.LESS_OR_EQUAL, value2, put2);
    assertEquals(ok, true);
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, CompareOp.EQUAL, value2, put3);
    assertEquals(ok, true);
  }
```
```
Cyclomatic Complexity	 1
Assertions		 18
Lines of Code		60
-  
  public void testCheckAndDeleteWithCompareOp() throws IOException, InterruptedException {
    final byte [] value1 = Bytes.toBytes("aaaa");
    final byte [] value2 = Bytes.toBytes("bbbb");
    final byte [] value3 = Bytes.toBytes("cccc");
    final byte [] value4 = Bytes.toBytes("dddd");
    Table table = TEST_UTIL.createTable(TableName.valueOf("testCheckAndDeleteWithCompareOp"),
        FAMILY);
    TEST_UTIL.waitTableAvailable(TableName.valueOf("testCheckAndDeleteWithCompareOp"), 10000);

    Put put2 = new Put(ROW);
    put2.add(FAMILY, QUALIFIER, value2);
    table.put(put2);

    Put put3 = new Put(ROW);
    put3.add(FAMILY, QUALIFIER, value3);

    Delete delete = new Delete(ROW);
    delete.deleteColumns(FAMILY, QUALIFIER);

    // cell = "bbbb", using "aaaa" to compare only LESS/LESS_OR_EQUAL/NOT_EQUAL
    // turns out "match"
    boolean ok = table.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareOp.GREATER, value1, delete);
    assertEquals(ok, false);
    ok = table.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareOp.EQUAL, value1, delete);
    assertEquals(ok, false);
    ok = table.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareOp.GREATER_OR_EQUAL, value1, delete);
    assertEquals(ok, false);
    ok = table.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareOp.LESS, value1, delete);
    assertEquals(ok, true);
    table.put(put2);
    ok = table.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareOp.LESS_OR_EQUAL, value1, delete);
    assertEquals(ok, true);
    table.put(put2);

    assertEquals(ok, true);

    // cell = "cccc", using "dddd" to compare only LARGER/LARGER_OR_EQUAL/NOT_EQUAL
    // turns out "match"
    table.put(put3);
    ok = table.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareOp.LESS, value4, delete);

    assertEquals(ok, false);
    ok = table.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareOp.LESS_OR_EQUAL, value4, delete);

    assertEquals(ok, false);
    ok = table.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareOp.EQUAL, value4, delete);

    assertEquals(ok, false);
    ok = table.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareOp.GREATER, value4, delete);

    assertEquals(ok, true);
    table.put(put3);
    ok = table.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareOp.GREATER_OR_EQUAL, value4, delete);
    assertEquals(ok, true);
    table.put(put3);
    ok = table.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareOp.NOT_EQUAL, value4, delete);

    assertEquals(ok, true);

    // cell = "bbbb", using "bbbb" to compare only GREATER_OR_EQUAL/LESS_OR_EQUAL/EQUAL
    // turns out "match"
    table.put(put2);
    ok = table.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareOp.GREATER, value2, delete);
    assertEquals(ok, false);
    ok = table.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareOp.NOT_EQUAL, value2, delete);
    assertEquals(ok, false);
    ok = table.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareOp.LESS, value2, delete);
    assertEquals(ok, false);
    ok = table.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareOp.GREATER_OR_EQUAL, value2, delete);
    assertEquals(ok, true);
    table.put(put2);
    ok = table.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareOp.LESS_OR_EQUAL, value2, delete);
    assertEquals(ok, true);
    table.put(put2);
    ok = table.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareOp.EQUAL, value2, delete);
    assertEquals(ok, true);
  }
```
```
Cyclomatic Complexity	 5
Assertions		 29
Lines of Code		83
-  
  public void testCacheOnWriteEvictOnClose() throws Exception {
    TableName tableName = TableName.valueOf("testCOWEOCfromClient");
    byte [] data = Bytes.toBytes("data");
    HTable table = TEST_UTIL.createTable(tableName, FAMILY);
    TEST_UTIL.waitTableAvailable(tableName, 10000);
    // get the block cache and region
    String regionName = table.getRegionLocations().firstKey().getEncodedName();
    Region region = TEST_UTIL.getRSForFirstRegionInTable(tableName)
      .getFromOnlineRegions(regionName);
    Store store = region.getStores().iterator().next();
    CacheConfig cacheConf = store.getCacheConfig();
    cacheConf.setCacheDataOnWrite(true);
    cacheConf.setEvictOnClose(true);
    BlockCache cache = cacheConf.getBlockCache();

    // establish baseline stats
    long startBlockCount = cache.getBlockCount();
    long startBlockHits = cache.getStats().getHitCount();
    long startBlockMiss = cache.getStats().getMissCount();

    // wait till baseline is stable, (minimal 500 ms)
    for (int i = 0; i < 5; i++) {
      Thread.sleep(100);
      if (startBlockCount != cache.getBlockCount()
          || startBlockHits != cache.getStats().getHitCount()
          || startBlockMiss != cache.getStats().getMissCount()) {
        startBlockCount = cache.getBlockCount();
        startBlockHits = cache.getStats().getHitCount();
        startBlockMiss = cache.getStats().getMissCount();
        i = -1;
      }
    }

    // insert data
    Put put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, data);
    table.put(put);
    assertTrue(Bytes.equals(table.get(new Get(ROW)).value(), data));
    // data was in memstore so don't expect any changes
    assertEquals(startBlockCount, cache.getBlockCount());
    assertEquals(startBlockHits, cache.getStats().getHitCount());
    assertEquals(startBlockMiss, cache.getStats().getMissCount());
    // flush the data
    LOG.debug("Flushing cache");
    region.flush(true);
    // expect two more blocks in cache - DATA and ROOT_INDEX
    // , no change in hits/misses
    long expectedBlockCount = startBlockCount + 2;
    long expectedBlockHits = startBlockHits;
    long expectedBlockMiss = startBlockMiss;
    assertEquals(expectedBlockCount, cache.getBlockCount());
    assertEquals(expectedBlockHits, cache.getStats().getHitCount());
    assertEquals(expectedBlockMiss, cache.getStats().getMissCount());
    // read the data and expect same blocks, one new hit, no misses
    assertTrue(Bytes.equals(table.get(new Get(ROW)).value(), data));
    assertEquals(expectedBlockCount, cache.getBlockCount());
    assertEquals(++expectedBlockHits, cache.getStats().getHitCount());
    assertEquals(expectedBlockMiss, cache.getStats().getMissCount());
    // insert a second column, read the row, no new blocks, one new hit
    byte [] QUALIFIER2 = Bytes.add(QUALIFIER, QUALIFIER);
    byte [] data2 = Bytes.add(data, data);
    put = new Put(ROW);
    put.add(FAMILY, QUALIFIER2, data2);
    table.put(put);
    Result r = table.get(new Get(ROW));
    assertTrue(Bytes.equals(r.getValue(FAMILY, QUALIFIER), data));
    assertTrue(Bytes.equals(r.getValue(FAMILY, QUALIFIER2), data2));
    assertEquals(expectedBlockCount, cache.getBlockCount());
    assertEquals(++expectedBlockHits, cache.getStats().getHitCount());
    assertEquals(expectedBlockMiss, cache.getStats().getMissCount());
    // flush, one new block
    System.out.println("Flushing cache");
    region.flush(true);
    // + 1 for Index Block, +1 for data block
    expectedBlockCount += 2;
    assertEquals(expectedBlockCount, cache.getBlockCount());
    assertEquals(expectedBlockHits, cache.getStats().getHitCount());
    assertEquals(expectedBlockMiss, cache.getStats().getMissCount());
    // compact, net minus two blocks, two hits, no misses
    System.out.println("Compacting");
    assertEquals(2, store.getStorefilesCount());
    store.triggerMajorCompaction();
    region.compact(true);
    waitForStoreFileCount(store, 1, 10000); // wait 10 seconds max
    assertEquals(1, store.getStorefilesCount());
    // evicted two data blocks and two index blocks and compaction does not cache new blocks
    expectedBlockCount = 0;
    assertEquals(expectedBlockCount, cache.getBlockCount());
    expectedBlockHits += 2;
    assertEquals(expectedBlockMiss, cache.getStats().getMissCount());
    assertEquals(expectedBlockHits, cache.getStats().getHitCount());
    // read the row, this should be a cache miss because we don't cache data
    // blocks on compaction
    r = table.get(new Get(ROW));
    assertTrue(Bytes.equals(r.getValue(FAMILY, QUALIFIER), data));
    assertTrue(Bytes.equals(r.getValue(FAMILY, QUALIFIER2), data2));
    expectedBlockCount += 1; // cached one data block
    assertEquals(expectedBlockCount, cache.getBlockCount());
    assertEquals(expectedBlockHits, cache.getStats().getHitCount());
    assertEquals(++expectedBlockMiss, cache.getStats().getMissCount());
  }
```
```
Cyclomatic Complexity	 4
Assertions		 6
Lines of Code		35
-  
  /**
   * Tests the non cached version of getRegionLocator by moving a region.
   */
  public void testNonCachedGetRegionLocation() throws Exception {
    // Test Initialization.
    TableName TABLE = TableName.valueOf("testNonCachedGetRegionLocation");
    byte [] family1 = Bytes.toBytes("f1");
    byte [] family2 = Bytes.toBytes("f2");
    try (HTable table = TEST_UTIL.createTable(TABLE, new byte[][] {family1, family2}, 10);
        Admin admin = new HBaseAdmin(TEST_UTIL.getConfiguration())) {
      TEST_UTIL.waitTableAvailable(TABLE, 10000);
      Map <HRegionInfo, ServerName> regionsMap = table.getRegionLocations();
      assertEquals(1, regionsMap.size());
      HRegionInfo regionInfo = regionsMap.keySet().iterator().next();
      ServerName addrBefore = regionsMap.get(regionInfo);
      // Verify region location before move.
      HRegionLocation addrCache = table.getRegionLocation(regionInfo.getStartKey(), false);
      HRegionLocation addrNoCache = table.getRegionLocation(regionInfo.getStartKey(),  true);

      assertEquals(addrBefore.getPort(), addrCache.getPort());
      assertEquals(addrBefore.getPort(), addrNoCache.getPort());

      ServerName addrAfter = null;
      // Now move the region to a different server.
      for (int i = 0; i < SLAVES; i++) {
        HRegionServer regionServer = TEST_UTIL.getHBaseCluster().getRegionServer(i);
        ServerName addr = regionServer.getServerName();
        if (addr.getPort() != addrBefore.getPort()) {
          admin.move(regionInfo.getEncodedNameAsBytes(),
              Bytes.toBytes(addr.toString()));
          // Wait for the region to move.
          Thread.sleep(5000);
          addrAfter = addr;
          break;
        }
      }

      // Verify the region was moved.
      addrCache = table.getRegionLocation(regionInfo.getStartKey(), false);
      addrNoCache = table.getRegionLocation(regionInfo.getStartKey(), true);
      assertNotNull(addrAfter);
      assertTrue(addrAfter.getPort() != addrCache.getPort());
      assertEquals(addrAfter.getPort(), addrNoCache.getPort());
    }
  }
```
```
Cyclomatic Complexity	 1
Assertions		 10
Lines of Code		37
-  
  /**
   * Tests getRegionsInRange by creating some regions over which a range of
   * keys spans; then changing the key range.
   */
  public void testGetRegionsInRange() throws Exception {
    // Test Initialization.
    byte [] startKey = Bytes.toBytes("ddc");
    byte [] endKey = Bytes.toBytes("mmm");
    TableName TABLE = TableName.valueOf("testGetRegionsInRange");
    HTable table = TEST_UTIL.createMultiRegionTable(TABLE, new byte[][] { FAMILY }, 10);
    int numOfRegions = -1;
    try (RegionLocator r = table.getRegionLocator()) {
      numOfRegions = r.getStartKeys().length;
    }
    assertEquals(26, numOfRegions);

    // Get the regions in this range
    List<HRegionLocation> regionsList = table.getRegionsInRange(startKey,
      endKey);
    assertEquals(10, regionsList.size());

    // Change the start key
    startKey = Bytes.toBytes("fff");
    regionsList = table.getRegionsInRange(startKey, endKey);
    assertEquals(7, regionsList.size());

    // Change the end key
    endKey = Bytes.toBytes("nnn");
    regionsList = table.getRegionsInRange(startKey, endKey);
    assertEquals(8, regionsList.size());

    // Empty start key
    regionsList = table.getRegionsInRange(HConstants.EMPTY_START_ROW, endKey);
    assertEquals(13, regionsList.size());

    // Empty end key
    regionsList = table.getRegionsInRange(startKey, HConstants.EMPTY_END_ROW);
    assertEquals(21, regionsList.size());

    // Both start and end keys empty
    regionsList = table.getRegionsInRange(HConstants.EMPTY_START_ROW,
      HConstants.EMPTY_END_ROW);
    assertEquals(26, regionsList.size());

    // Change the end key to somewhere in the last block
    endKey = Bytes.toBytes("zzz1");
    regionsList = table.getRegionsInRange(startKey, endKey);
    assertEquals(21, regionsList.size());

    // Change the start key to somewhere in the first block
    startKey = Bytes.toBytes("aac");
    regionsList = table.getRegionsInRange(startKey, endKey);
    assertEquals(26, regionsList.size());

    // Make start and end key the same
    startKey = endKey = Bytes.toBytes("ccc");
    regionsList = table.getRegionsInRange(startKey, endKey);
    assertEquals(1, regionsList.size());
  }
```
```
Cyclomatic Complexity	 2
Assertions		 1
Lines of Code		22
-  
  public void testJira6912() throws Exception {
    TableName TABLE = TableName.valueOf("testJira6912");
    Table foo = TEST_UTIL.createTable(TABLE, new byte[][] {FAMILY}, 10);
    TEST_UTIL.waitTableAvailable(TABLE, 10000);
    List<Put> puts = new ArrayList<Put>();
    for (int i=0;i !=100; i++){
      Put put = new Put(Bytes.toBytes(i));
      put.add(FAMILY, FAMILY, Bytes.toBytes(i));
      puts.add(put);
    }
    foo.put(puts);
    // If i comment this out it works
    TEST_UTIL.flush();

    Scan scan = new Scan();
    scan.setStartRow(Bytes.toBytes(1));
    scan.setStopRow(Bytes.toBytes(3));
    scan.addColumn(FAMILY, FAMILY);
    scan.setFilter(new RowFilter(CompareFilter.CompareOp.NOT_EQUAL, new BinaryComparator(Bytes.toBytes(1))));

    ResultScanner scanner = foo.getScanner(scan);
    Result[] bar = scanner.next(100);
    assertEquals(1, bar.length);
  }
```
```
Cyclomatic Complexity	 1
Assertions		 4
Lines of Code		24
-  
  public void testScan_NullQualifier() throws IOException, InterruptedException {
    Table table = TEST_UTIL.createTable(TableName.valueOf("testScan_NullQualifier"), FAMILY);
    TEST_UTIL.waitTableAvailable(TableName.valueOf("testScan_NullQualifier"), 10000);

    Put put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, VALUE);
    table.put(put);

    put = new Put(ROW);
    put.add(FAMILY, null, VALUE);
    table.put(put);
    LOG.info("Row put");

    Scan scan = new Scan();
    scan.addColumn(FAMILY, null);

    ResultScanner scanner = table.getScanner(scan);
    Result[] bar = scanner.next(100);
    assertEquals(1, bar.length);
    assertEquals(1, bar[0].size());

    scan = new Scan();
    scan.addFamily(FAMILY);

    scanner = table.getScanner(scan);
    bar = scanner.next(100);
    assertEquals(1, bar.length);
    assertEquals(2, bar[0].size());
  }
```
```
Cyclomatic Complexity	 2
Assertions		 2
Lines of Code		81
-  
  public void testIllegalTableDescriptor() throws Exception {
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf("testIllegalTableDescriptor"));
    HColumnDescriptor hcd = new HColumnDescriptor(FAMILY);

    // create table with 0 families
    checkTableIsIllegal(htd);
    htd.addFamily(hcd);
    checkTableIsLegal(htd);

    htd.setMaxFileSize(1024); // 1K
    checkTableIsIllegal(htd);
    htd.setMaxFileSize(0);
    checkTableIsIllegal(htd);
    htd.setMaxFileSize(1024 * 1024 * 1024); // 1G
    checkTableIsLegal(htd);

    htd.setMemStoreFlushSize(1024);
    checkTableIsIllegal(htd);
    htd.setMemStoreFlushSize(0);
    checkTableIsIllegal(htd);
    htd.setMemStoreFlushSize(128 * 1024 * 1024); // 128M
    checkTableIsLegal(htd);

    htd.setRegionSplitPolicyClassName("nonexisting.foo.class");
    checkTableIsIllegal(htd);
    htd.setRegionSplitPolicyClassName(null);
    checkTableIsLegal(htd);

    hcd.setBlocksize(0);
    checkTableIsIllegal(htd);
    hcd.setBlocksize(1024 * 1024 * 128); // 128M
    checkTableIsIllegal(htd);
    hcd.setBlocksize(1024);
    checkTableIsLegal(htd);

    hcd.setTimeToLive(0);
    checkTableIsIllegal(htd);
    hcd.setTimeToLive(-1);
    checkTableIsIllegal(htd);
    hcd.setTimeToLive(1);
    checkTableIsLegal(htd);

    hcd.setMinVersions(-1);
    checkTableIsIllegal(htd);
    hcd.setMinVersions(3);
    try {
      hcd.setMaxVersions(2);
      fail();
    } catch (IllegalArgumentException ex) {
      // expected
      hcd.setMaxVersions(10);
    }
    checkTableIsLegal(htd);

    // HBASE-13776 Setting illegal versions for HColumnDescriptor
    //  does not throw IllegalArgumentException
    // finally, minVersions must be less than or equal to maxVersions
    hcd.setMaxVersions(4);
    hcd.setMinVersions(5);
    checkTableIsIllegal(htd);
    hcd.setMinVersions(3);

    try {
      hcd.setScope(-1);
      fail("Illegal value for setScope did not throw");
    } catch (IllegalArgumentException e) {
      // expected
      hcd.setScope(0);
    }
    checkTableIsLegal(htd);

    try {
      hcd.setDFSReplication((short) -1);
      fail("Illegal value for setDFSReplication did not throw");
    } catch (IllegalArgumentException e) {
      // pass
    }
    // set an illegal DFS replication value by hand
    hcd.setValue(HColumnDescriptor.DFS_REPLICATION, "-1");
    checkTableIsIllegal(htd);
    try {
      hcd.setDFSReplication((short) -1);
      fail("Should throw exception if an illegal value is explicitly being set");
    } catch (IllegalArgumentException e) {
      // pass
    }

    // check the conf settings to disable sanity checks
    htd.setMemStoreFlushSize(0);

    // Check that logs warn on invalid table but allow it.
    ListAppender listAppender = new ListAppender();
    Logger log = Logger.getLogger(HMaster.class);
    log.addAppender(listAppender);
    log.setLevel(Level.WARN);

    htd.setConfiguration("hbase.table.sanity.checks", Boolean.FALSE.toString());
    checkTableIsLegal(htd);

    assertFalse(listAppender.getMessages().isEmpty());
    assertTrue(listAppender.getMessages().get(0).startsWith("MEMSTORE_FLUSHSIZE for table "
        + "descriptor or \"hbase.hregion.memstore.flush.size\" (0) is too small, which might "
        + "cause very frequent flushing."));

    log.removeAppender(listAppender);
  }
```
```
Cyclomatic Complexity	 2
Assertions		 0
Lines of Code		21
-  
  public void testGetRegionLocationFromPrimaryMetaRegion() throws IOException, InterruptedException {
    HTU.getHBaseAdmin().setBalancerRunning(false, true);

    ((ConnectionManager.HConnectionImplementation) HTU.getHBaseAdmin().getConnection()).
        setUseMetaReplicas(true);

    // Create table then get the single region for our new table.
    HTableDescriptor hdt = HTU.createTableDescriptor("testGetRegionLocationFromPrimaryMetaRegion");
    hdt.setRegionReplication(2);
    try {

      HTU.createTable(hdt, new byte[][] { f }, null);

      RegionServerHostingPrimayMetaRegionSlowOrStopCopro.slowDownPrimaryMetaScan = true;

      // Get user table location, always get it from the primary meta replica
      RegionLocations url = ((ClusterConnection) HTU.getConnection())
          .locateRegion(hdt.getTableName(), row, false, false);

    } finally {
      RegionServerHostingPrimayMetaRegionSlowOrStopCopro.slowDownPrimaryMetaScan = false;
      ((ConnectionManager.HConnectionImplementation) HTU.getHBaseAdmin().getConnection()).
          setUseMetaReplicas(false);
      HTU.getHBaseAdmin().setBalancerRunning(true, true);
      HTU.getHBaseAdmin().disableTable(hdt.getTableName());
      HTU.deleteTable(hdt.getTableName());
    }
  }
```
```
Cyclomatic Complexity	 7
Assertions		 2
Lines of Code		66
-  
  public void testReplicaGetWithPrimaryAndMetaDown() throws IOException, InterruptedException {
    HTU.getHBaseAdmin().setBalancerRunning(false, true);

    ((ConnectionManager.HConnectionImplementation)HTU.getHBaseAdmin().getConnection()).
        setUseMetaReplicas(true);

    // Create table then get the single region for our new table.
    HTableDescriptor hdt = HTU.createTableDescriptor("testReplicaGetWithPrimaryAndMetaDown");
    hdt.setRegionReplication(2);
    try {

      Table table = HTU.createTable(hdt, new byte[][] { f }, null);

      // Get Meta location
      RegionLocations mrl = ((ClusterConnection) HTU.getConnection())
          .locateRegion(TableName.META_TABLE_NAME,
              HConstants.EMPTY_START_ROW, false, false);

      // Get user table location
      RegionLocations url = ((ClusterConnection) HTU.getConnection())
          .locateRegion(hdt.getTableName(), row, false, false);

      // Make sure that user primary region is co-hosted with the meta region
      if (!url.getDefaultRegionLocation().getServerName().equals(
          mrl.getDefaultRegionLocation().getServerName())) {
        HTU.moveRegionAndWait(url.getDefaultRegionLocation().getRegionInfo(),
            mrl.getDefaultRegionLocation().getServerName());
      }

      // Make sure that the user replica region is not hosted by the same region server with
      // primary
      if (url.getRegionLocation(1).getServerName().equals(mrl.getDefaultRegionLocation()
          .getServerName())) {
        HTU.moveRegionAndWait(url.getRegionLocation(1).getRegionInfo(),
            url.getDefaultRegionLocation().getServerName());
      }

      // Wait until the meta table is updated with new location info
      while (true) {
        mrl = ((ClusterConnection) HTU.getConnection())
            .locateRegion(TableName.META_TABLE_NAME, HConstants.EMPTY_START_ROW, false, false);

        // Get user table location
        url = ((ClusterConnection) HTU.getConnection())
            .locateRegion(hdt.getTableName(), row, false, true);

        LOG.info("meta locations " + mrl);
        LOG.info("table locations " + url);
        ServerName a = url.getDefaultRegionLocation().getServerName();
        ServerName b = mrl.getDefaultRegionLocation().getServerName();
        if(a.equals(b)) {
          break;
        } else {
          LOG.info("Waiting for new region info to be updated in meta table");
          Thread.sleep(100);
        }
      }

      Put p = new Put(row);
      p.addColumn(f, row, row);
      table.put(p);

      // Flush so it can be picked by the replica refresher thread
      HTU.flush(table.getName());

      // Sleep for some time until data is picked up by replicas
      try {
        Thread.sleep(2 * REFRESH_PERIOD);
      } catch (InterruptedException e1) {
        LOG.error(e1);
      }

      // Simulating the RS down
      RegionServerHostingPrimayMetaRegionSlowOrStopCopro.throwException = true;

      // The first Get is supposed to succeed
      Get g = new Get(row);
      g.setConsistency(Consistency.TIMELINE);
      Result r = table.get(g);
      Assert.assertTrue(r.isStale());

      // The second Get will succeed as well
      r = table.get(g);
      Assert.assertTrue(r.isStale());

    } finally {
      ((ConnectionManager.HConnectionImplementation)HTU.getHBaseAdmin().getConnection()).
          setUseMetaReplicas(false);
      RegionServerHostingPrimayMetaRegionSlowOrStopCopro.throwException = false;
      HTU.getHBaseAdmin().setBalancerRunning(true, true);
      HTU.getHBaseAdmin().disableTable(hdt.getTableName());@@@+      HTU.getAdmin().disableTable(hdt.getTableName());@@@       HTU.deleteTable(hdt.getTableName());@@@     }@@@   }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		5
-  
  public void testLegacyWALObserverWriteToWAL() throws Exception {
    final WAL log = wals.getWAL(UNSPECIFIED_REGION, null);
    verifyWritesSeen(log, getCoprocessor(log, SampleRegionWALObserver.Legacy.class), true);
  }
```
```
Cyclomatic Complexity	 2
Assertions		 24
Lines of Code		64
-  
  public void testNonLegacyWALKeysDoNotExplode() throws Exception {
    TableName tableName = TableName.valueOf(TEST_TABLE);
    final HTableDescriptor htd = createBasic3FamilyHTD(Bytes
        .toString(TEST_TABLE));
    final HRegionInfo hri = new HRegionInfo(tableName, null, null);
    MultiVersionConcurrencyControl mvcc = new MultiVersionConcurrencyControl();

    fs.mkdirs(new Path(FSUtils.getTableDir(hbaseRootDir, tableName), hri.getEncodedName()));

    final Configuration newConf = HBaseConfiguration.create(this.conf);

    final WAL wal = wals.getWAL(UNSPECIFIED_REGION, null);
    final SampleRegionWALObserver newApi = getCoprocessor(wal, SampleRegionWALObserver.class);
    newApi.setTestValues(TEST_TABLE, TEST_ROW, null, null, null, null, null, null);
    final SampleRegionWALObserver oldApi = getCoprocessor(wal,
        SampleRegionWALObserver.Legacy.class);
    oldApi.setTestValues(TEST_TABLE, TEST_ROW, null, null, null, null, null, null);

    LOG.debug("ensuring wal entries haven't happened before we start");
    assertFalse(newApi.isPreWALWriteCalled());
    assertFalse(newApi.isPostWALWriteCalled());
    assertFalse(newApi.isPreWALWriteDeprecatedCalled());
    assertFalse(newApi.isPostWALWriteDeprecatedCalled());
    assertFalse(oldApi.isPreWALWriteCalled());
    assertFalse(oldApi.isPostWALWriteCalled());
    assertFalse(oldApi.isPreWALWriteDeprecatedCalled());
    assertFalse(oldApi.isPostWALWriteDeprecatedCalled());

    LOG.debug("writing to WAL with non-legacy keys.");
    final int countPerFamily = 5;
    for (HColumnDescriptor hcd : htd.getFamilies()) {
      addWALEdits(tableName, hri, TEST_ROW, hcd.getName(), countPerFamily,
          EnvironmentEdgeManager.getDelegate(), wal, htd, mvcc);
    }

    LOG.debug("Verify that only the non-legacy CP saw edits.");
    assertTrue(newApi.isPreWALWriteCalled());
    assertTrue(newApi.isPostWALWriteCalled());
    assertFalse(newApi.isPreWALWriteDeprecatedCalled());
    assertFalse(newApi.isPostWALWriteDeprecatedCalled());
    // wish we could test that the log message happened :/
    assertFalse(oldApi.isPreWALWriteCalled());
    assertFalse(oldApi.isPostWALWriteCalled());
    assertFalse(oldApi.isPreWALWriteDeprecatedCalled());
    assertFalse(oldApi.isPostWALWriteDeprecatedCalled());

    LOG.debug("reseting cp state.");
    newApi.setTestValues(TEST_TABLE, TEST_ROW, null, null, null, null, null, null);
    oldApi.setTestValues(TEST_TABLE, TEST_ROW, null, null, null, null, null, null);

    LOG.debug("write a log edit that supports legacy cps.");
    final long now = EnvironmentEdgeManager.currentTime();
    final WALKey legacyKey = new HLogKey(hri.getEncodedNameAsBytes(), hri.getTable(), now, mvcc);
    final WALEdit edit = new WALEdit();
    final byte[] nonce = Bytes.toBytes("1772");
    edit.add(new KeyValue(TEST_ROW, TEST_FAMILY[0], nonce, now, nonce));
    final long txid = wal.append(htd, hri, legacyKey, edit, true);
    wal.sync(txid);

    LOG.debug("Make sure legacy cps can see supported edits after having been skipped.");
    assertTrue("non-legacy WALObserver didn't see pre-write.", newApi.isPreWALWriteCalled());
    assertTrue("non-legacy WALObserver didn't see post-write.", newApi.isPostWALWriteCalled());
    assertFalse("non-legacy WALObserver shouldn't have seen legacy pre-write.",
        newApi.isPreWALWriteDeprecatedCalled());
    assertFalse("non-legacy WALObserver shouldn't have seen legacy post-write.",
        newApi.isPostWALWriteDeprecatedCalled());
    assertTrue("legacy WALObserver didn't see pre-write.", oldApi.isPreWALWriteCalled());
    assertTrue("legacy WALObserver didn't see post-write.", oldApi.isPostWALWriteCalled());
    assertTrue("legacy WALObserver didn't see legacy pre-write.",
        oldApi.isPreWALWriteDeprecatedCalled());
    assertTrue("legacy WALObserver didn't see legacy post-write.",
        oldApi.isPostWALWriteDeprecatedCalled());@@@   }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		22
-  
  public void testRowIndexWithTagsButNoTagsInCell() throws IOException {
    List<KeyValue> kvList = new ArrayList<>();
    byte[] row = new byte[0];
    byte[] family = new byte[0];
    byte[] qualifier = new byte[0];
    byte[] value = new byte[0];
    KeyValue expectedKV = new KeyValue(row, family, qualifier, 1L, Type.Put, value);
    kvList.add(expectedKV);
    DataBlockEncoding encoding = DataBlockEncoding.ROW_INDEX_V1;
    DataBlockEncoder encoder = encoding.getEncoder();
    ByteBuffer encodedBuffer =
        encodeKeyValues(encoding, kvList, getEncodingContext(Algorithm.NONE, encoding));
    HFileContext meta =
        new HFileContextBuilder().withHBaseCheckSum(false).withIncludesMvcc(includesMemstoreTS)
            .withIncludesTags(includesTags).withCompression(Compression.Algorithm.NONE).build();
    DataBlockEncoder.EncodedSeeker seeker =
        encoder.createSeeker(KeyValue.COMPARATOR, encoder.newDataBlockDecodingContext(meta));
    seeker.setCurrentBuffer(encodedBuffer);
    Cell cell = seeker.getKeyValue();
    Assert.assertEquals(expectedKV.getLength(), ((KeyValue) cell).getLength());
  }
```
```
Cyclomatic Complexity	 2
Assertions		 4
Lines of Code		27
-  
  public void compatibilityTest() throws Exception {
    HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
    Path testDir = TEST_UTIL.getDataTestDir();
    TEST_UTIL.getTestFileSystem().mkdirs(testDir);
    String persistencePath = testDir + "/bucket.persistence";
    BucketCache bucketCache =
      new BucketCache("file:" + testDir + "/bucket.cache", capacitySize, constructedBlockSize,
        null, writeThreads, writerQLen, persistencePath);
    long usedSize = bucketCache.getAllocator().getUsedSize();
    assertTrue(usedSize == 0);

    CacheTestUtils.HFileBlockPair[] blocks =
      CacheTestUtils.generateHFileBlocks(constructedBlockSize, 1);
    // Add blocks
    for (CacheTestUtils.HFileBlockPair block : blocks) {
      cacheAndWaitUntilFlushedToBucket(bucketCache, block.getBlockName(), block.getBlock());
    }
    usedSize = bucketCache.getAllocator().getUsedSize();
    assertTrue(usedSize != 0);
    // persistence backingMap using old way
    persistToFileInOldWay(persistencePath + ".old", bucketCache.getMaxSize(),
      bucketCache.backingMap, bucketCache.getDeserialiserMap());
    bucketCache.shutdown();

    // restore cache from file which skip check checksum
    bucketCache =
      new BucketCache("file:" + testDir + "/bucket.cache", capacitySize, constructedBlockSize,
        null, writeThreads, writerQLen, persistencePath + ".old");
    assertEquals(usedSize, bucketCache.getAllocator().getUsedSize());
    assertEquals(blocks.length, bucketCache.backingMap.size());
  }
```
```
Cyclomatic Complexity	 2
Assertions		 2
Lines of Code		13
-  
  public void testReplicaCost() {
    Configuration conf = HBaseConfiguration.create();
    StochasticLoadBalancer.CostFunction
        costFunction = new StochasticLoadBalancer.RegionReplicaHostCostFunction(conf);
    for (int[] mockCluster : clusterStateMocks) {
      BaseLoadBalancer.Cluster cluster = mockCluster(mockCluster);
      costFunction.init(cluster);
      double cost = costFunction.cost();
      assertTrue(cost >= 0);
      assertTrue(cost <= 1.01);
    }
  }
```
```
Cyclomatic Complexity	 2
Assertions		 4
Lines of Code		54
-  
  public void testReplicaCostForReplicas() {
    Configuration conf = HBaseConfiguration.create();
    StochasticLoadBalancer.CostFunction
        costFunction = new StochasticLoadBalancer.RegionReplicaHostCostFunction(conf);

    int [] servers = new int[] {3,3,3,3,3};
    TreeMap<ServerName, List<HRegionInfo>> clusterState = mockClusterServers(servers);

    BaseLoadBalancer.Cluster cluster;

    cluster = new BaseLoadBalancer.Cluster(clusterState, null, null, null);
    costFunction.init(cluster);
    double costWithoutReplicas = costFunction.cost();
    assertEquals(0, costWithoutReplicas, 0);

    // replicate the region from first server to the last server
    HRegionInfo replica1 = RegionReplicaUtil.getRegionInfoForReplica(
      clusterState.firstEntry().getValue().get(0),1);
    clusterState.lastEntry().getValue().add(replica1);

    cluster = new BaseLoadBalancer.Cluster(clusterState, null, null, null);
    costFunction.init(cluster);
    double costWith1ReplicaDifferentServer = costFunction.cost();

    assertEquals(0, costWith1ReplicaDifferentServer, 0);

    // add a third replica to the last server
    HRegionInfo replica2 = RegionReplicaUtil.getRegionInfoForReplica(replica1, 2);
    clusterState.lastEntry().getValue().add(replica2);

    cluster = new BaseLoadBalancer.Cluster(clusterState, null, null, null);
    costFunction.init(cluster);
    double costWith1ReplicaSameServer = costFunction.cost();

    assertTrue(costWith1ReplicaDifferentServer < costWith1ReplicaSameServer);

    // test with replication = 4 for following:

    HRegionInfo replica3;
    Iterator<Entry<ServerName, List<HRegionInfo>>> it;
    Entry<ServerName, List<HRegionInfo>> entry;

    clusterState = mockClusterServers(servers);
    it = clusterState.entrySet().iterator();
    entry = it.next(); //first server
    HRegionInfo hri = entry.getValue().get(0);
    replica1 = RegionReplicaUtil.getRegionInfoForReplica(hri, 1);
    replica2 = RegionReplicaUtil.getRegionInfoForReplica(hri, 2);
    replica3 = RegionReplicaUtil.getRegionInfoForReplica(hri, 3);
    entry.getValue().add(replica1);
    entry.getValue().add(replica2);
    it.next().getValue().add(replica3); //2nd server

    cluster = new BaseLoadBalancer.Cluster(clusterState, null, null, null);
    costFunction.init(cluster);
    double costWith3ReplicasSameServer = costFunction.cost();

    clusterState = mockClusterServers(servers);
    hri = clusterState.firstEntry().getValue().get(0);
    replica1 = RegionReplicaUtil.getRegionInfoForReplica(hri, 1);
    replica2 = RegionReplicaUtil.getRegionInfoForReplica(hri, 2);
    replica3 = RegionReplicaUtil.getRegionInfoForReplica(hri, 3);

    clusterState.firstEntry().getValue().add(replica1);
    clusterState.lastEntry().getValue().add(replica2);
    clusterState.lastEntry().getValue().add(replica3);

    cluster = new BaseLoadBalancer.Cluster(clusterState, null, null, null);
    costFunction.init(cluster);
    double costWith2ReplicasOnTwoServers = costFunction.cost();

    assertTrue(costWith2ReplicasOnTwoServers < costWith3ReplicasSameServer);
  }
```
```
Cyclomatic Complexity	 5
Assertions		 2
Lines of Code		21
-  
  public void testNeedsBalanceForColocatedReplicas() {
    // check for the case where there are two hosts and with one rack, and where
    // both the replicas are hosted on the same server
    List<HRegionInfo> regions = randomRegions(1);
    ServerName s1 = ServerName.valueOf("host1", 1000, 11111);
    ServerName s2 = ServerName.valueOf("host11", 1000, 11111);
    Map<ServerName, List<HRegionInfo>> map = new HashMap<ServerName, List<HRegionInfo>>();
    map.put(s1, regions);
    regions.add(RegionReplicaUtil.getRegionInfoForReplica(regions.get(0), 1));
    // until the step above s1 holds two replicas of a region
    regions = randomRegions(1);
    map.put(s2, regions);
    assertTrue(loadBalancer.needsBalance(new Cluster(map, null, null, null)));
    // check for the case where there are two hosts on the same rack and there are two racks
    // and both the replicas are on the same rack
    map.clear();
    regions = randomRegions(1);
    List<HRegionInfo> regionsOnS2 = new ArrayList<HRegionInfo>(1);
    regionsOnS2.add(RegionReplicaUtil.getRegionInfoForReplica(regions.get(0), 1));
    map.put(s1, regions);
    map.put(s2, regionsOnS2);
    // add another server so that the cluster has some host on another rack
    map.put(ServerName.valueOf("host2", 1000, 11111), randomRegions(1));
    assertTrue(loadBalancer.needsBalance(new Cluster(map, null, null,
        new ForTestRackManagerOne())));
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		9
-   (timeout = 60000)
  public void testSmallCluster() {
    int numNodes = 10;
    int numRegions = 1000;
    int numRegionsPerServer = 40; //all servers except one
    int replication = 1;
    int numTables = 10;
    testWithCluster(numNodes, numRegions, numRegionsPerServer, replication, numTables, true, true);
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		9
-   (timeout = 60000)
  public void testSmallCluster2() {
    int numNodes = 20;
    int numRegions = 2000;
    int numRegionsPerServer = 40; //all servers except one
    int replication = 1;
    int numTables = 10;
    testWithCluster(numNodes, numRegions, numRegionsPerServer, replication, numTables, true, true);
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		9
-   (timeout = 60000)
  public void testSmallCluster3() {
    int numNodes = 20;
    int numRegions = 2000;
    int numRegionsPerServer = 1; // all servers except one
    int replication = 1;
    int numTables = 10;
    /* fails because of max moves */
    testWithCluster(numNodes, numRegions, numRegionsPerServer, replication, numTables, false, false);
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		9
-   (timeout = 800000)
  public void testMidCluster() {
    int numNodes = 100;
    int numRegions = 10000;
    int numRegionsPerServer = 60; // all servers except one
    int replication = 1;
    int numTables = 40;
    testWithCluster(numNodes, numRegions, numRegionsPerServer, replication, numTables, true, true);
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		15
-   (timeout = 800000)
  public void testMidCluster2() {
    int numNodes = 200;
    int numRegions = 100000;
    int numRegionsPerServer = 40; // all servers except one
    int replication = 1;
    int numTables = 400;
    testWithCluster(numNodes,
        numRegions,
        numRegionsPerServer,
        replication,
        numTables,
        false, /* num large num regions means may not always get to best balance with one run */
        false);
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		9
-   (timeout = 800000)
  public void testMidCluster3() {
    int numNodes = 100;
    int numRegions = 2000;
    int numRegionsPerServer = 9; // all servers except one
    int replication = 1;
    int numTables = 110;
    testWithCluster(numNodes, numRegions, numRegionsPerServer, replication, numTables, true, true);
    // TODO(eclark): Make sure that the tables are well distributed.
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		9
-  
  public void testLargeCluster() {
    int numNodes = 1000;
    int numRegions = 100000; //100 regions per RS
    int numRegionsPerServer = 80; //all servers except one
    int numTables = 100;
    int replication = 1;
    testWithCluster(numNodes, numRegions, numRegionsPerServer, replication, numTables, true, true);
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		9
-   (timeout = 800000)
  public void testRegionReplicasOnSmallCluster() {
    int numNodes = 10;
    int numRegions = 1000;
    int replication = 3; // 3 replicas per region
    int numRegionsPerServer = 80; //all regions are mostly balanced
    int numTables = 10;
    testWithCluster(numNodes, numRegions, numRegionsPerServer, replication, numTables, true, true);
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		17
-   (timeout = 800000)
  public void testRegionReplicationOnMidClusterWithRacks() {
    conf.setLong(StochasticLoadBalancer.MAX_STEPS_KEY, 10000000L);
    conf.setFloat("hbase.master.balancer.stochastic.maxMovePercent", 1.0f);
    conf.setLong("hbase.master.balancer.stochastic.maxRunningTime", 120 * 1000); // 120 sec
    loadBalancer.setConf(conf);
    int numNodes = 30;
    int numRegions = numNodes * 30;
    int replication = 3; // 3 replicas per region
    int numRegionsPerServer = 28;
    int numTables = 10;
    int numRacks = 4; // all replicas should be on a different rack
    Map<ServerName, List<HRegionInfo>> serverMap =
        createServerMap(numNodes, numRegions, numRegionsPerServer, replication, numTables);
    RackManager rm = new ForTestRackManager(numRacks);

    testWithCluster(serverMap, rm, false, true);@@@   }
```
```
Cyclomatic Complexity	 2
Assertions		 1
Lines of Code		15
-  (timeout = 15000)
  public void testForDifferntHFileRefsZnodeVersion() throws Exception {
    // 1. Create a file
    Path file = new Path(root, "testForDifferntHFileRefsZnodeVersion");
    fs.createNewFile(file);
    // 2. Assert file is successfully created
    assertTrue("Test file not created!", fs.exists(file));
    ReplicationHFileCleaner cleaner = new ReplicationHFileCleaner();
    cleaner.setConf(conf);

    ReplicationQueuesClient replicationQueuesClient = Mockito.mock(ReplicationQueuesClient.class);
    //Return different znode version for each call
    Mockito.when(replicationQueuesClient.getHFileRefsNodeChangeVersion()).thenReturn(1, 2);

    Class<? extends ReplicationHFileCleaner> cleanerClass = cleaner.getClass();
    Field rqc = cleanerClass.getDeclaredField("rqc");
    rqc.setAccessible(true);
    rqc.set(cleaner, replicationQueuesClient);

    cleaner.isFileDeletable(fs.getFileStatus(file));@@@   }
```
```
Cyclomatic Complexity	 4
Assertions		 10
Lines of Code		49
-  
  public void testMasterOpsWhileSplitting() throws Exception {
    TableName tableName = TableName.valueOf("TestSplit");
    byte[] familyName = Bytes.toBytes("fam");
    try (HTable ht = TEST_UTIL.createTable(tableName, familyName)) {
      TEST_UTIL.loadTable(ht, familyName, false);
    }
    HRegionServer server = TEST_UTIL.getHBaseCluster().getRegionServer(0);
    byte[] firstRow = Bytes.toBytes("aaa");
    byte[] splitRow = Bytes.toBytes("lll");
    byte[] lastRow = Bytes.toBytes("zzz");
    try (Connection conn = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration())) {
      // this will also cache the region
      byte[] regionName = conn.getRegionLocator(tableName).getRegionLocation(splitRow)
          .getRegionInfo().getRegionName();
      Region region = server.getRegion(regionName);
      SplitTransactionImpl split = new SplitTransactionImpl((HRegion) region, splitRow);
      split.prepare();

      // 1. phase I
      PairOfSameType<Region> regions = split.createDaughters(server, server, null);
      assertFalse(test(conn, tableName, firstRow, server));
      assertFalse(test(conn, tableName, lastRow, server));

      // passing null as services prevents final step
      // 2, most of phase II
      split.openDaughters(server, null, regions.getFirst(), regions.getSecond());
      assertFalse(test(conn, tableName, firstRow, server));
      assertFalse(test(conn, tableName, lastRow, server));

      // 3. finish phase II
      // note that this replicates some code from SplitTransaction
      // 2nd daughter first
      if (split.useZKForAssignment) {
        server.postOpenDeployTasks(regions.getSecond());
      } else {
        server.reportRegionStateTransition(
          RegionServerStatusProtos.RegionStateTransition.TransitionCode.SPLIT,
          region.getRegionInfo(), regions.getFirst().getRegionInfo(),
          regions.getSecond().getRegionInfo());
      }

      // first daughter second
      if (split.useZKForAssignment) {
        server.postOpenDeployTasks(regions.getFirst());
      }

      // Add to online regions
      server.addToOnlineRegions(regions.getSecond());
      // THIS is the crucial point:
      // the 2nd daughter was added, so querying before the split key should fail.
      assertFalse(test(conn, tableName, firstRow, server));
      // past splitkey is ok.
      assertTrue(test(conn, tableName, lastRow, server));

      // Add to online regions
      server.addToOnlineRegions(regions.getFirst());
      assertTrue(test(conn, tableName, firstRow, server));
      assertTrue(test(conn, tableName, lastRow, server));

      if (split.useZKForAssignment) {
        // 4. phase III
        ((BaseCoordinatedStateManager) server.getCoordinatedStateManager())
            .getSplitTransactionCoordination().completeSplitTransaction(server, regions.getFirst(),
              regions.getSecond(), split.std, region);
      }

      assertTrue(test(conn, tableName, firstRow, server));
      assertTrue(test(conn, tableName, lastRow, server));
    }
  }
```
```
Cyclomatic Complexity	 4
Assertions		 5
Lines of Code		42
-  
  public void testTableAvailableWhileSplitting() throws Exception {
    TableName tableName = TableName.valueOf("TestTableAvailableWhileSplitting");
    byte[] familyName = Bytes.toBytes("fam");
    try (HTable ht = TEST_UTIL.createTable(tableName, familyName)) {
      TEST_UTIL.loadTable(ht, familyName, false);
    }
    Admin admin = TEST_UTIL.getHBaseAdmin();
    HRegionServer server = TEST_UTIL.getHBaseCluster().getRegionServer(0);
    byte[] splitRow = Bytes.toBytes("lll");
    try (Connection conn = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration())) {
      byte[] regionName = conn.getRegionLocator(tableName).getRegionLocation(splitRow)
          .getRegionInfo().getRegionName();
      Region region = server.getRegion(regionName);
      SplitTransactionImpl split = new SplitTransactionImpl((HRegion) region, splitRow);
      split.prepare();
      assertTrue(admin.isTableAvailable(tableName));

      // 1. phase I
      PairOfSameType<Region> regions = split.createDaughters(server, server, null);
      // Parent should be offline at this stage and daughters not yet open
      assertFalse(admin.isTableAvailable(tableName));

      // passing null as services prevents final step of postOpenDeployTasks
      // 2, most of phase II
      split.openDaughters(server, null, regions.getFirst(), regions.getSecond());
      assertFalse(admin.isTableAvailable(tableName));

      // Finish openeing daughters
      // 2nd daughter first
      if (split.useZKForAssignment) {
        server.postOpenDeployTasks(regions.getSecond());
      } else {
        server.reportRegionStateTransition(
          RegionServerStatusProtos.RegionStateTransition.TransitionCode.SPLIT,
          region.getRegionInfo(), regions.getFirst().getRegionInfo(),
          regions.getSecond().getRegionInfo());
      }

      // first daughter second
      if (split.useZKForAssignment) {
        server.postOpenDeployTasks(regions.getFirst());
      }

      // After postOpenDeploy daughters should have location in meta
      assertTrue(admin.isTableAvailable(tableName));

      server.addToOnlineRegions(regions.getSecond());
      server.addToOnlineRegions(regions.getFirst());
      assertTrue(admin.isTableAvailable(tableName));
    } finally {
      if (admin != null) {
        admin.close();
      }
    }
  }
```
```
Cyclomatic Complexity	 3
Assertions		 7
Lines of Code		45
-  (timeout = 60000)
  public void testSplitFailedCompactionAndSplit() throws Exception {
    final TableName tableName = TableName.valueOf("testSplitFailedCompactionAndSplit");
    Configuration conf = TESTING_UTIL.getConfiguration();
    try {
      HBaseAdmin admin = new HBaseAdmin(conf);
      // Create table then get the single region for our new table.
      HTableDescriptor htd = new HTableDescriptor(tableName);
      byte[] cf = Bytes.toBytes("cf");
      htd.addFamily(new HColumnDescriptor(cf));
      admin.createTable(htd);

      for (int i = 0; cluster.getRegions(tableName).size() == 0 && i < 100; i++) {
        Thread.sleep(100);
      }
      assertEquals(1, cluster.getRegions(tableName).size());

      HRegion region = cluster.getRegions(tableName).get(0);
      Store store = region.getStore(cf);
      int regionServerIndex = cluster.getServerWith(region.getRegionInfo().getRegionName());
      HRegionServer regionServer = cluster.getRegionServer(regionServerIndex);

      Table t = new HTable(conf, tableName);
      // insert data
      insertData(tableName, admin, t);
      insertData(tableName, admin, t);

      int fileNum = store.getStorefiles().size();
      // 0, Compaction Request
      store.triggerMajorCompaction();
      CompactionContext cc = store.requestCompaction();
      assertNotNull(cc);
      // 1, A timeout split
      // 1.1 close region
      assertEquals(2, region.close(false).get(cf).size());
      // 1.2 rollback and Region initialize again
      region.initialize();

      // 2, Run Compaction cc
      assertFalse(region.compact(cc, store, NoLimitThroughputController.INSTANCE));
      assertTrue(fileNum > store.getStorefiles().size());

      // 3, Split
      SplitTransaction st = new SplitTransactionImpl(region, Bytes.toBytes("row3"));
      assertTrue(st.prepare());
      st.execute(regionServer, regionServer);
      LOG.info("Waiting for region to come out of RIT");
      TESTING_UTIL.waitFor(60000, 1000, new Waiter.Predicate<Exception>() {
        @Override
        public boolean evaluate() throws Exception {
          RegionStates regionStates = cluster.getMaster().getAssignmentManager().getRegionStates();
          return !regionStates.isRegionsInTransition();
        }
      });
      assertEquals(2, cluster.getRegions(tableName).size());
    } finally {
      TESTING_UTIL.deleteTable(tableName);
    }
  }
```
```
Cyclomatic Complexity	 7
Assertions		 5
Lines of Code		60
-  (timeout = 400000)
  public void testMasterRestartWhenSplittingIsPartial()
      throws IOException, InterruptedException, NodeExistsException,
      KeeperException, DeserializationException, ServiceException {
    final TableName tableName = TableName.valueOf("testMasterRestartWhenSplittingIsPartial");

    if (!useZKForAssignment) {
      // This test doesn't apply if not using ZK for assignment
      return;
    }

    // Create table then get the single region for our new table.
    HTable t = createTableAndWait(tableName, HConstants.CATALOG_FAMILY);
    List<HRegion> regions = cluster.getRegions(tableName);
    HRegionInfo hri = getAndCheckSingleTableRegion(regions);

    int tableRegionIndex = ensureTableRegionNotOnSameServerAsMeta(admin, hri);

    // Turn off balancer so it doesn't cut in and mess up our placements.
    this.admin.setBalancerRunning(false, true);
    // Turn off the meta scanner so it don't remove parent on us.
    cluster.getMaster().setCatalogJanitorEnabled(false);
    ZooKeeperWatcher zkw = new ZooKeeperWatcher(t.getConfiguration(),
      "testMasterRestartWhenSplittingIsPartial", new UselessTestAbortable());
    try {
      // Add a bit of load up into the table so splittable.
      TESTING_UTIL.loadTable(t, HConstants.CATALOG_FAMILY, false);
      // Get region pre-split.
      HRegionServer server = cluster.getRegionServer(tableRegionIndex);
      printOutRegions(server, "Initial regions: ");
      // Now, before we split, set special flag in master, a flag that has
      // it FAIL the processing of split.
      AssignmentManager.setTestSkipSplitHandling(true);
      // Now try splitting and it should work.

      this.admin.split(hri.getRegionNameAsString());
      checkAndGetDaughters(tableName);
      // Assert the ephemeral node is up in zk.
      String path = ZKAssign.getNodeName(zkw, hri.getEncodedName());
      Stat stats = zkw.getRecoverableZooKeeper().exists(path, false);
      LOG.info("EPHEMERAL NODE BEFORE SERVER ABORT, path=" + path + ", stats="
          + stats);
      byte[] bytes = ZKAssign.getData(zkw, hri.getEncodedName());
      RegionTransition rtd = RegionTransition.parseFrom(bytes);
      // State could be SPLIT or SPLITTING.
      assertTrue(rtd.getEventType().equals(EventType.RS_ZK_REGION_SPLIT)
          || rtd.getEventType().equals(EventType.RS_ZK_REGION_SPLITTING));

      // abort and wait for new master.
      MockMasterWithoutCatalogJanitor master = abortAndWaitForMaster();

      this.admin = new HBaseAdmin(TESTING_UTIL.getConfiguration());

      // Update the region to be offline and split, so that HRegionInfo#equals
      // returns true in checking rebuilt region states map.
      hri.setOffline(true);
      hri.setSplit(true);
      ServerName regionServerOfRegion = master.getAssignmentManager()
        .getRegionStates().getRegionServerOfRegion(hri);
      assertTrue(regionServerOfRegion != null);

      // Remove the block so that split can move ahead.
      AssignmentManager.setTestSkipSplitHandling(false);
      String node = ZKAssign.getNodeName(zkw, hri.getEncodedName());
      Stat stat = new Stat();
      byte[] data = ZKUtil.getDataNoWatch(zkw, node, stat);
      // ZKUtil.create
      for (int i=0; data != null && i<60; i++) {
        Thread.sleep(1000);
        data = ZKUtil.getDataNoWatch(zkw, node, stat);
      }
      assertNull("Waited too long for ZK node to be removed: "+node, data);
      RegionStates regionStates = master.getAssignmentManager().getRegionStates();
      assertTrue("Split parent should be in SPLIT state",
        regionStates.isRegionInState(hri, State.SPLIT));
      regionServerOfRegion = regionStates.getRegionServerOfRegion(hri);
      assertTrue(regionServerOfRegion == null);
    } finally {
      // Set this flag back.
      AssignmentManager.setTestSkipSplitHandling(false);
      admin.setBalancerRunning(true, false);
      cluster.getMaster().setCatalogJanitorEnabled(true);
      t.close();
      zkw.close();
    }
  }
```
```
Cyclomatic Complexity	 1
Assertions		 4
Lines of Code		39
-  (timeout = 120000)
  public void testFailedSplit() throws Exception {
    TableName tableName = TableName.valueOf("testFailedSplit");
    byte[] colFamily = Bytes.toBytes("info");
    TESTING_UTIL.createTable(tableName, colFamily);
    Connection connection = ConnectionFactory.createConnection(TESTING_UTIL.getConfiguration());
    HTable table = (HTable) connection.getTable(tableName);
    try {
      TESTING_UTIL.loadTable(table, colFamily);
      List<HRegionInfo> regions = TESTING_UTIL.getHBaseAdmin().getTableRegions(tableName);
      assertTrue(regions.size() == 1);
      final HRegion actualRegion = cluster.getRegions(tableName).get(0);
      actualRegion.getCoprocessorHost().load(FailingSplitRegionObserver.class,
        Coprocessor.PRIORITY_USER, actualRegion.getBaseConf());

      // The following split would fail.
      admin.split(tableName);
      FailingSplitRegionObserver observer = (FailingSplitRegionObserver) actualRegion
          .getCoprocessorHost().findCoprocessor(FailingSplitRegionObserver.class.getName());
      assertNotNull(observer);
      observer.latch.await();
      observer.postSplit.await();
      LOG.info("Waiting for region to come out of RIT: " + actualRegion);
      TESTING_UTIL.waitFor(60000, 1000, new Waiter.Predicate<Exception>() {
        @Override
        public boolean evaluate() throws Exception {
          RegionStates regionStates = cluster.getMaster().getAssignmentManager().getRegionStates();
          return !regionStates.isRegionsInTransition();
        }
      });
      regions = TESTING_UTIL.getHBaseAdmin().getTableRegions(tableName);
      assertTrue(regions.size() == 1);
      RegionStates regionStates = cluster.getMaster().getAssignmentManager().getRegionStates();
      Set<RegionState> rit = regionStates.getRegionsInTransition();
      assertTrue(rit.size() == 0);
    } finally {
      table.close();
      connection.close();
      TESTING_UTIL.deleteTable(tableName);
    }
  }
```
```
Cyclomatic Complexity	 2
Assertions		 2
Lines of Code		51
-   (timeout=300000)
  public void testSSHCleanupDaugtherRegionsOfAbortedSplit() throws Exception {
    TableName table = TableName.valueOf("testSSHCleanupDaugtherRegionsOfAbortedSplit");
    try {
      HTableDescriptor desc = new HTableDescriptor(table);
      desc.addFamily(new HColumnDescriptor(Bytes.toBytes("f")));
      admin.createTable(desc);
      HTable hTable = new HTable(cluster.getConfiguration(), desc.getTableName());
      for(int i = 1; i < 5; i++) {
        Put p1 = new Put(("r"+i).getBytes());
        p1.add(Bytes.toBytes("f"), "q1".getBytes(), "v".getBytes());
        hTable.put(p1);
      }
      admin.flush(desc.getTableName());
      List<HRegion> regions = cluster.getRegions(desc.getTableName());
      int serverWith = cluster.getServerWith(regions.get(0).getRegionInfo().getRegionName());
      HRegionServer regionServer = cluster.getRegionServer(serverWith);
      cluster.getServerWith(regions.get(0).getRegionInfo().getRegionName());
      SplitTransactionImpl st = new SplitTransactionImpl(regions.get(0), Bytes.toBytes("r3"));
      st.prepare();
      st.stepsBeforePONR(regionServer, regionServer, false);
      Path tableDir =
          FSUtils.getTableDir(cluster.getMaster().getMasterFileSystem().getRootDir(),
            desc.getTableName());
      tableDir.getFileSystem(cluster.getConfiguration());
      List<Path> regionDirs =
          FSUtils.getRegionDirs(tableDir.getFileSystem(cluster.getConfiguration()), tableDir);
      assertEquals(3,regionDirs.size());
      cluster.startRegionServer();
      regionServer.kill();
      cluster.getRegionServerThreads().get(serverWith).join();
      // Wait until finish processing of shutdown
      TESTING_UTIL.waitFor(60000, 1000, new Waiter.Predicate<Exception>() {
        @Override
        public boolean evaluate() throws Exception {
          return !cluster.getMaster().getServerManager().areDeadServersInProgress();
        }
      });
      // Wait until there are no more regions in transition
      TESTING_UTIL.waitFor(60000, 1000, new Waiter.Predicate<Exception>() {
        @Override
        public boolean evaluate() throws Exception {
          return !cluster.getMaster().getAssignmentManager().
              getRegionStates().isRegionsInTransition();
        }
      });
      regionDirs =
          FSUtils.getRegionDirs(tableDir.getFileSystem(cluster.getConfiguration()), tableDir);
      assertEquals(1,regionDirs.size());
    } finally {
      TESTING_UTIL.deleteTable(table);
    }
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		17
-  
  public void testNoEdits() throws Exception {
    TableName tableName = TableName.valueOf("TestLogRollPeriodNoEdits");
    TEST_UTIL.createTable(tableName, "cf");
    try {
      Table table = new HTable(TEST_UTIL.getConfiguration(), tableName);
      try {
        HRegionServer server = TEST_UTIL.getRSForFirstRegionInTable(tableName);
        WAL log = server.getWAL(null);
        checkMinLogRolls(log, 5);
      } finally {
        table.close();
      }
    } finally {
      TEST_UTIL.deleteTable(tableName);
    }
  }
```
```
Cyclomatic Complexity	 2
Assertions		 0
Lines of Code		38
-  (timeout=60000)
  public void testWithEdits() throws Exception {
    final TableName tableName = TableName.valueOf("TestLogRollPeriodWithEdits");
    final String family = "cf";

    TEST_UTIL.createTable(tableName, family);
    try {
      HRegionServer server = TEST_UTIL.getRSForFirstRegionInTable(tableName);
      WAL log = server.getWAL(null);
      final Table table = new HTable(TEST_UTIL.getConfiguration(), tableName);

      Thread writerThread = new Thread("writer") {
        @Override
        public void run() {
          try {
            long row = 0;
            while (!interrupted()) {
              Put p = new Put(Bytes.toBytes(String.format("row%d", row)));
              p.add(Bytes.toBytes(family), Bytes.toBytes("col"), Bytes.toBytes(row));
              table.put(p);
              row++;

              Thread.sleep(LOG_ROLL_PERIOD / 16);
            }
          } catch (Exception e) {
            LOG.warn(e);
          } 
        }
      };

      try {
        writerThread.start();
        checkMinLogRolls(log, 5);
      } finally {
        writerThread.interrupt();
        writerThread.join();
        table.close();
      }  
    } finally {
      TEST_UTIL.deleteTable(tableName);
    }
  }
```
```
Cyclomatic Complexity	 1
Assertions		 5
Lines of Code		30
-  
  public void testZKLockCleaner() throws Exception {
    MiniHBaseCluster cluster = utility1.startMiniCluster(1, 2);
    HTableDescriptor table = new HTableDescriptor(TableName.valueOf(Bytes.toBytes("zk")));
    HColumnDescriptor fam = new HColumnDescriptor(famName);
    fam.setScope(HConstants.REPLICATION_SCOPE_GLOBAL);
    table.addFamily(fam);
    new HBaseAdmin(conf1).createTable(table);
    ReplicationAdmin replicationAdmin = new ReplicationAdmin(conf1);
    ReplicationPeerConfig rpc = new ReplicationPeerConfig();
    rpc.setClusterKey(utility2.getClusterKey());
    replicationAdmin.addPeer("cluster2", rpc, null);
    HRegionServer rs = cluster.getRegionServer(0);
    ReplicationQueuesZKImpl zk = new ReplicationQueuesZKImpl(rs.getZooKeeper(), conf1, rs);
    zk.init(rs.getServerName().toString());
    List<String> replicators = zk.getListOfReplicators();
    assertEquals(2, replicators.size());
    String zNode = cluster.getRegionServer(1).getServerName().toString();

    assertTrue(zk.lockOtherRS(zNode));
    assertTrue(zk.checkLockExists(zNode));
    Thread.sleep(10000);
    assertTrue(zk.checkLockExists(zNode));
    cluster.abortRegionServer(0);
    Thread.sleep(10000);
    HRegionServer rs1 = cluster.getRegionServer(1);
    zk = new ReplicationQueuesZKImpl(rs1.getZooKeeper(), conf1, rs1);
    zk.init(rs1.getServerName().toString());
    assertFalse(zk.checkLockExists(zNode));

    utility1.shutdownMiniCluster();
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		4
-  (timeout=300000)
  public void killOneMasterRS() throws Exception {
    loadTableAndKillRS(utility1);
  }
```
```
Cyclomatic Complexity	 2
Assertions		 4
Lines of Code		19
-  
  public void testWALKeySerialization() throws Exception {
    Map<String, byte[]> attributes = new HashMap<String, byte[]>();
    attributes.put("foo", Bytes.toBytes("foo-value"));
    attributes.put("bar", Bytes.toBytes("bar-value"));
    WALKey key = new WALKey(info.getEncodedNameAsBytes(), tableName,
      System.currentTimeMillis(), 0L, 0L, mvcc, attributes);
    assertEquals(attributes, key.getExtendedAttributes());

    WALProtos.WALKey.Builder builder = key.getBuilder(null);
    WALProtos.WALKey serializedKey = builder.build();

    WALKey deserializedKey = new WALKey();
    deserializedKey.readFieldsFromPb(serializedKey, null);

    //equals() only checks region name, sequence id and write time
    assertEquals(key, deserializedKey);
    //can't use Map.equals() because byte arrays use reference equality
    assertEquals(key.getExtendedAttributes().keySet(),
      deserializedKey.getExtendedAttributes().keySet());
    for (Map.Entry<String, byte[]> entry : deserializedKey.getExtendedAttributes().entrySet()) {
      assertArrayEquals(key.getExtendedAttribute(entry.getKey()), entry.getValue());
    }
  }
```
```
Cyclomatic Complexity	 2
Assertions		 5
Lines of Code		29
-  
  public void testReplicationSourceWALReaderThreadWithFilter() throws Exception {
    final byte[] notReplicatedCf = Bytes.toBytes("notReplicated");
    final Map<TableName, List<String>> tableCfs = new HashMap<>();
    tableCfs.put(tableName, Collections.singletonList(Bytes.toString(family)));
    ReplicationPeer peer = mock(ReplicationPeer.class);
    when(peer.getTableCFs()).thenReturn(tableCfs);
    WALEntryFilter filter = new ChainWALEntryFilter(new TableCfWALEntryFilter(peer));

    // add filterable entries
    appendToLogPlus(3, notReplicatedCf);
    appendToLogPlus(3, notReplicatedCf);
    appendToLogPlus(3, notReplicatedCf);

    // add non filterable entries
    appendEntriesToLog(2);

    ReplicationSourceManager mockSourceManager = mock(ReplicationSourceManager.class);
    when(mockSourceManager.getTotalBufferUsed()).thenReturn(new AtomicLong(0));
    final ReplicationSourceWALReaderThread reader =
            new ReplicationSourceWALReaderThread(mockSourceManager, getQueueInfo(), walQueue,
                    0, fs, conf, filter, new MetricsSource("1"));
    reader.start();

    WALEntryBatch entryBatch = reader.take();

    assertNotNull(entryBatch);
    assertFalse(entryBatch.isEmpty());
    List<Entry> walEntries = entryBatch.getWalEntries();
    assertEquals(2, walEntries.size());
    for (Entry entry : walEntries) {
      ArrayList<Cell> cells = entry.getEdit().getCells();
      assertTrue(cells.size() == 1);
      assertTrue(CellUtil.matchingFamily(cells.get(0), family));
    }
  }
```
```
Cyclomatic Complexity	 -1
Assertions		 7
Lines of Code		34
-  
  public void testReplicationSourceWALReaderThreadWithFilterWhenLogRolled() throws Exception {
    final byte[] notReplicatedCf = Bytes.toBytes("notReplicated");
    final Map<TableName, List<String>> tableCfs = new HashMap<>();
    tableCfs.put(tableName, Collections.singletonList(Bytes.toString(family)));
    ReplicationPeer peer = mock(ReplicationPeer.class);
    when(peer.getTableCFs()).thenReturn(tableCfs);
    WALEntryFilter filter = new ChainWALEntryFilter(new TableCfWALEntryFilter(peer));

    appendToLogPlus(3, notReplicatedCf);

    Path firstWAL = walQueue.peek();
    final long eof = getPosition(firstWAL);

    ReplicationSourceManager mockSourceManager = mock(ReplicationSourceManager.class);
    when(mockSourceManager.getTotalBufferUsed()).thenReturn(new AtomicLong(0));
    final ReplicationSourceWALReaderThread reader =
            new ReplicationSourceWALReaderThread(mockSourceManager, getQueueInfo(), walQueue,
                    0, fs, conf, filter, new MetricsSource("1"));
    reader.start();

    // reader won't put any batch, even if EOF reached.
    Waiter.waitFor(conf, 20000, new Waiter.Predicate<Exception>() {
      @Override public boolean evaluate() {
        return reader.getLastReadPosition() >= eof;
      }
    });
    assertNull(reader.poll(0));

    log.rollWriter();

    // should get empty batch with current wal position, after wal rolled
    WALEntryBatch entryBatch = reader.take();

    Path lastWAL= walQueue.peek();
    long positionToBeLogged = getPosition(lastWAL);

    assertNotNull(entryBatch);
    assertTrue(entryBatch.isEmpty());
    assertEquals(1, walQueue.size());
    assertNotEquals(firstWAL, entryBatch.getLastWalPath());
    assertEquals(lastWAL, entryBatch.getLastWalPath());
    assertEquals(positionToBeLogged, entryBatch.getLastWalPosition());
  }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		13
-   (timeout=180000)
  public void testSplit() throws Exception {
    AccessTestAction action = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preSplit(ObserverContext.createAndPrepare(RCP_ENV, null));
        return null;
      }
    };

    verifyAllowed(action, SUPERUSER, USER_ADMIN, USER_OWNER, USER_GROUP_ADMIN);
    verifyDenied(action, USER_CREATE, USER_RW, USER_RO, USER_NONE, USER_GROUP_READ,
      USER_GROUP_WRITE, USER_GROUP_CREATE);
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		7
-  
  public void testMigration() throws DeserializationException {
    Configuration conf = UTIL.getConfiguration();
    ListMultimap<String,TablePermission> permissions = createPermissions();
    byte [] bytes = writePermissionsAsBytes(permissions, conf);
    AccessControlLists.readPermissions(bytes, conf);
  }
```
```
Cyclomatic Complexity	 -1
Assertions		 3
Lines of Code		37
-  
  public void testTokenAuthentication() throws Exception {
    UserGroupInformation testuser =
        UserGroupInformation.createUserForTesting("testuser", new String[]{"testgroup"});

    testuser.setAuthenticationMethod(
        UserGroupInformation.AuthenticationMethod.TOKEN);
    final Configuration conf = TEST_UTIL.getConfiguration();
    UserGroupInformation.setConfiguration(conf);
    Token<AuthenticationTokenIdentifier> token =
        secretManager.generateToken("testuser");
    LOG.debug("Got token: " + token.toString());
    testuser.addToken(token);

    // verify the server authenticates us as this token user
    testuser.doAs(new PrivilegedExceptionAction<Object>() {
      public Object run() throws Exception {
        Configuration c = server.getConfiguration();
        RpcClient rpcClient = RpcClientFactory.createClient(c, clusterId.toString());
        ServerName sn =
            ServerName.valueOf(server.getAddress().getHostName(), server.getAddress().getPort(),
                System.currentTimeMillis());
        try {
          BlockingRpcChannel channel = rpcClient.createBlockingRpcChannel(sn,
              User.getCurrent(), HConstants.DEFAULT_HBASE_RPC_TIMEOUT);
          AuthenticationProtos.AuthenticationService.BlockingInterface stub =
              AuthenticationProtos.AuthenticationService.newBlockingStub(channel);
          AuthenticationProtos.WhoAmIResponse response =
              stub.whoAmI(null, AuthenticationProtos.WhoAmIRequest.getDefaultInstance());
          String myname = response.getUsername();
          assertEquals("testuser", myname);
          String authMethod = response.getAuthMethod();
          assertEquals("TOKEN", authMethod);
        } finally {
          rpcClient.close();
        }
        return null;
      }
    });
  }
```
```
Cyclomatic Complexity	 2
Assertions		 0
Lines of Code		4
-  
  public void testThriftServerHttpTraceForbiddenWhenOptionsDisabled() throws Exception {
    // HTTP TRACE method should be disabled for security
    // See https://www.owasp.org/index.php/Cross_Site_Tracing
    checkHttpMethods("TRACE", HttpURLConnection.HTTP_FORBIDDEN);
  }
```
```
Cyclomatic Complexity	 2
Assertions		 0
Lines of Code		6
-  
  public void testThriftServerHttpTraceForbiddenWhenOptionsEnabled() throws Exception {
    // HTTP TRACE method should be disabled for security
    // See https://www.owasp.org/index.php/Cross_Site_Tracing
    TEST_UTIL.getConfiguration().setBoolean(ThriftServerRunner.THRIFT_HTTP_ALLOW_OPTIONS_METHOD,
        true);
    checkHttpMethods("TRACE", HttpURLConnection.HTTP_FORBIDDEN);
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		5
-  
  public void testThriftServerHttpOptionsForbiddenWhenOptionsDisabled() throws Exception {
    // HTTP OPTIONS method should be disabled by default, so we make sure
    // hbase.thrift.http.allow.options.method is not set anywhere in the config
    TEST_UTIL.getConfiguration().unset(ThriftServerRunner.THRIFT_HTTP_ALLOW_OPTIONS_METHOD);
    checkHttpMethods("OPTIONS", HttpURLConnection.HTTP_FORBIDDEN);
  }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		6
-  
  public void testThriftServerHttpOptionsOkWhenOptionsEnabled() throws Exception {
    TEST_UTIL.getConfiguration().setBoolean(ThriftServerRunner.THRIFT_HTTP_ALLOW_OPTIONS_METHOD,
        true);
    checkHttpMethods("OPTIONS", HttpURLConnection.HTTP_OK);
  }
```
