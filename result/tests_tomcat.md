## db63171a8e713ad9bec2d2ead47dce39f7b3179b ##
```
Cyclomatic Complexity	6
Assertions		12
Lines of Code		92
-    
    public void testAliases() throws Exception {
        // Some sample text
        String foxText = "The quick brown fox jumps over the lazy dog";
        String loremIpsum = "Lorem ipsum dolor sit amet, consectetur adipisicing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.";

        // Set up a temporary docBase and some alternates that we can
        // set up as aliases.
        File tmpDir = new File(getTemporaryDirectory(),
                               "tomcat-unit-test." + TestNamingContext.class.getName());

        // Make sure we've got a clean slate
        ExpandWar.delete(tmpDir);

        File docBase = new File(tmpDir, "docBase");
        File alternate1 = new File(tmpDir, "alternate1");
        File alternate2 = new File(tmpDir, "alternate2");

        // Register for clean-up
        addDeleteOnTearDown(tmpDir);

        if(!tmpDir.mkdirs())
            throw new IOException("Could not create temp directory " + tmpDir);
        if(!docBase.mkdir())
            throw new IOException("Could not create temp directory " + docBase);
        if(!alternate1.mkdir())
            throw new IOException("Could not create temp directory " + alternate1);
        if(!alternate2.mkdir())
            throw new IOException("Could not create temp directory " + alternate2);

        // Create a file in each alternate directory that we can attempt to access
        FileOutputStream fos = new FileOutputStream(new File(alternate1, "test1.txt"));
        try {
            fos.write(foxText.getBytes("UTF-8"));
            fos.flush();
        } finally {
            fos.close();
        }

        fos = new FileOutputStream(new File(alternate2, "test2.txt"));
        try {
            fos.write(loremIpsum.getBytes("UTF-8"));
            fos.flush();
        } finally {
            fos.close();
        }

        // Finally, create the Context
        FileDirContext ctx = new FileDirContext();
        ctx.setDocBase(docBase.getCanonicalPath());
        ctx.setAliases("/a1=" + alternate1.getCanonicalPath()
                       +",/a2=" + alternate2.getCanonicalPath());

        // Check first alias
        Object file = ctx.lookup("/a1/test1.txt");

        Assert.assertNotNull(file);
        Assert.assertTrue(file instanceof Resource);

        byte[] buffer = new byte[4096];
        Resource res = (Resource)file;

        InputStream is = res.streamContent();
        int len;
        try {
            len = is.read(buffer);
        } finally {
            is.close();
        }
        String contents = new String(buffer, 0, len, "UTF-8");

        assertEquals(foxText, contents);

        // Check second alias
        file = ctx.lookup("/a2/test2.txt");

        Assert.assertNotNull(file);
        Assert.assertTrue(file instanceof Resource);

        res = (Resource)file;
        is = res.streamContent();
        try {
            len = is.read(buffer);
        } finally {
            is.close();
        }
        contents = new String(buffer, 0, len, "UTF-8");

        assertEquals(loremIpsum, contents);

        // Test aliases with spaces around the separators
        ctx.setAliases("   /a1= " + alternate1.getCanonicalPath()
                       + "\n\n"
                       +", /a2 =\n" + alternate2.getCanonicalPath()
                       + ",");

        // Check first alias
        file = ctx.lookup("/a1/test1.txt");

        Assert.assertNotNull(file);
        Assert.assertTrue(file instanceof Resource);

        res = (Resource)file;
        is = res.streamContent();
        try {
            len = is.read(buffer);
        } finally {
            is.close();
        }
        contents = new String(buffer, 0, len, "UTF-8");

        assertEquals(foxText, contents);

        // Check second alias
        file = ctx.lookup("/a2/test2.txt");

        Assert.assertNotNull(file);
        Assert.assertTrue(file instanceof Resource);

        res = (Resource)file;
        is = res.streamContent();
        try {
            len = is.read(buffer);
        } finally {
            is.close();
        }
        contents = new String(buffer, 0, len, "UTF-8");

        assertEquals(loremIpsum, contents);
    }
```
```
Cyclomatic Complexity	 -1
Assertions		 3
Lines of Code		11
-    
    @Deprecated
    public void testFindNotBytes() throws UnsupportedEncodingException {
        byte[] bytes = "Hello\u00a0world".getBytes("ISO-8859-1");
        final int len = bytes.length;

        assertEquals(4, ByteChunk.findNotBytes(bytes, 0, len, new byte[] { 'l',
                'e', 'H' }));
        assertEquals(-1, ByteChunk.findNotBytes(bytes, 0, len, bytes));
        assertEquals(-1, ByteChunk.findNotBytes(bytes, 2, 3, new byte[] { 'l',
                'e', 'H' }));@@@     }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		6
-    
    public void testHeaderLimits3() throws Exception {
        // Cannot process 101 header
        Tomcat tomcat = getTomcatInstance();
        setupHeadersTest(tomcat);
        runHeadersTest(false, tomcat, 101, 100);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		7
-    
    public void testHeaderLimits4() throws Exception {
        // Can change maxHeaderCount
        Tomcat tomcat = getTomcatInstance();
        setupHeadersTest(tomcat);
        tomcat.getConnector().setMaxHeaderCount(-1);
        runHeadersTest(true, tomcat, 300, -1);
    }
```
## ede3616678956c1b50602deb2e352dbd5f92b62a ##
```
Cyclomatic Complexity	1
Assertions		0
Lines of Code		6
-    
    public void v1NullValue() {
        Cookie cookie = new Cookie("foo", null);
        cookie.setVersion(1);
        doTest(cookie, "foo=\"\"; Version=1", "foo=");
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		6
-    
    public void v1QuotedValue() {
        Cookie cookie = new Cookie("foo", "\"bar\"");
        cookie.setVersion(1);
        doTest(cookie, "foo=\"bar\"; Version=1", "foo=\"bar\"");
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		6
-    
    public void v1ValueContainsSemicolon() {
        Cookie cookie = new Cookie("foo", "a;b");
        cookie.setVersion(1);
        doTest(cookie, "foo=\"a;b\"; Version=1", null);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		6
-    
    public void v1ValueContainsComma() {
        Cookie cookie = new Cookie("foo", "a,b");
        cookie.setVersion(1);
        doTest(cookie, "foo=\"a,b\"; Version=1", null);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		6
-    
    public void v1ValueContainsSpace() {
        Cookie cookie = new Cookie("foo", "a b");
        cookie.setVersion(1);
        doTest(cookie, "foo=\"a b\"; Version=1", null);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		6
-    
    public void v1ValueContainsEquals() {
        Cookie cookie = new Cookie("foo", "a=b");
        cookie.setVersion(1);
        doTest(cookie, "foo=\"a=b\"; Version=1", "foo=a=b");
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		6
-    
    public void v1ValueContainsQuote() {
        Cookie cookie = new Cookie("foo", "a\"b");
        cookie.setVersion(1);
        doTest(cookie, "foo=\"a\\\"b\"; Version=1", null);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		6
-    
    public void v1ValueContainsNonV0Separator() {
        Cookie cookie = new Cookie("foo", "a()<>@,;:\\\"/[]?={}b");
        cookie.setVersion(1);
        doTest(cookie, "foo=\"a()<>@,;:\\\\\\\"/[]?={}b\"; Version=1", null);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		6
-    
    public void v1ValueContainsBackslash() {
        Cookie cookie = new Cookie("foo", "a\\b");
        cookie.setVersion(1);
        doTest(cookie, "foo=\"a\\\\b\"; Version=1", null);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		6
-    
    public void v1ValueContainsBackslashAndQuote() {
        Cookie cookie = new Cookie("foo", "a\"b\\c");
        cookie.setVersion(1);
        doTest(cookie, "foo=\"a\\\"b\\\\c\"; Version=1", null);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		4
-    
    public void testBasicCookieOld() {
        doTestBasicCookie(false);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		4
-    
    public void v1TokenValueOld() {
        doV1TokenValue(false);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		4
-    
    public void v1NameOnlyIsDroppedRfc6265() {
        doV1NameOnlyIsDropped(true);
    }
```
## 179358a24ae2fe035579c2e81b4924742329157f ##
```
Cyclomatic Complexity	6
Assertions		12
Lines of Code		92
-    
    public void testAliases() throws Exception {
        // Some sample text
        String foxText = "The quick brown fox jumps over the lazy dog";
        String loremIpsum = "Lorem ipsum dolor sit amet, consectetur adipisicing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.";

        // Set up a temporary docBase and some alternates that we can
        // set up as aliases.
        File tmpDir = new File(getTemporaryDirectory(),
                               "tomcat-unit-test." + TestNamingContext.class.getName());

        // Make sure we've got a clean slate
        ExpandWar.delete(tmpDir);

        File docBase = new File(tmpDir, "docBase");
        File alternate1 = new File(tmpDir, "alternate1");
        File alternate2 = new File(tmpDir, "alternate2");

        // Register for clean-up
        addDeleteOnTearDown(tmpDir);

        if(!tmpDir.mkdirs())
            throw new IOException("Could not create temp directory " + tmpDir);
        if(!docBase.mkdir())
            throw new IOException("Could not create temp directory " + docBase);
        if(!alternate1.mkdir())
            throw new IOException("Could not create temp directory " + alternate1);
        if(!alternate2.mkdir())
            throw new IOException("Could not create temp directory " + alternate2);

        // Create a file in each alternate directory that we can attempt to access
        FileOutputStream fos = new FileOutputStream(new File(alternate1, "test1.txt"));
        try {
            fos.write(foxText.getBytes("UTF-8"));
            fos.flush();
        } finally {
            fos.close();
        }

        fos = new FileOutputStream(new File(alternate2, "test2.txt"));
        try {
            fos.write(loremIpsum.getBytes("UTF-8"));
            fos.flush();
        } finally {
            fos.close();
        }

        // Finally, create the Context
        FileDirContext ctx = new FileDirContext();
        ctx.setDocBase(docBase.getCanonicalPath());
        ctx.setAliases("/a1=" + alternate1.getCanonicalPath()
                       +",/a2=" + alternate2.getCanonicalPath());

        // Check first alias
        Object file = ctx.lookup("/a1/test1.txt");

        Assert.assertNotNull(file);
        Assert.assertTrue(file instanceof Resource);

        byte[] buffer = new byte[4096];
        Resource res = (Resource)file;

        InputStream is = res.streamContent();
        int len;
        try {
            len = is.read(buffer);
        } finally {
            is.close();
        }
        String contents = new String(buffer, 0, len, "UTF-8");

        assertEquals(foxText, contents);

        // Check second alias
        file = ctx.lookup("/a2/test2.txt");

        Assert.assertNotNull(file);
        Assert.assertTrue(file instanceof Resource);

        res = (Resource)file;
        is = res.streamContent();
        try {
            len = is.read(buffer);
        } finally {
            is.close();
        }
        contents = new String(buffer, 0, len, "UTF-8");

        assertEquals(loremIpsum, contents);

        // Test aliases with spaces around the separators
        ctx.setAliases("   /a1= " + alternate1.getCanonicalPath()
                       + "\n\n"
                       +", /a2 =\n" + alternate2.getCanonicalPath()
                       + ",");

        // Check first alias
        file = ctx.lookup("/a1/test1.txt");

        Assert.assertNotNull(file);
        Assert.assertTrue(file instanceof Resource);

        res = (Resource)file;
        is = res.streamContent();
        try {
            len = is.read(buffer);
        } finally {
            is.close();
        }
        contents = new String(buffer, 0, len, "UTF-8");

        assertEquals(foxText, contents);

        // Check second alias
        file = ctx.lookup("/a2/test2.txt");

        Assert.assertNotNull(file);
        Assert.assertTrue(file instanceof Resource);

        res = (Resource)file;
        is = res.streamContent();
        try {
            len = is.read(buffer);
        } finally {
            is.close();
        }
        contents = new String(buffer, 0, len, "UTF-8");

        assertEquals(loremIpsum, contents);
    }
```
```
Cyclomatic Complexity	 -1
Assertions		 3
Lines of Code		11
-    
    @Deprecated
    public void testFindNotBytes() throws UnsupportedEncodingException {
        byte[] bytes = "Hello\u00a0world".getBytes("ISO-8859-1");
        final int len = bytes.length;

        assertEquals(4, ByteChunk.findNotBytes(bytes, 0, len, new byte[] { 'l',
                'e', 'H' }));
        assertEquals(-1, ByteChunk.findNotBytes(bytes, 0, len, bytes));
        assertEquals(-1, ByteChunk.findNotBytes(bytes, 2, 3, new byte[] { 'l',
                'e', 'H' }));@@@     }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		6
-    
    public void testHeaderLimits3() throws Exception {
        // Cannot process 101 header
        Tomcat tomcat = getTomcatInstance();
        setupHeadersTest(tomcat);
        runHeadersTest(false, tomcat, 101, 100);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		7
-    
    public void testHeaderLimits4() throws Exception {
        // Can change maxHeaderCount
        Tomcat tomcat = getTomcatInstance();
        setupHeadersTest(tomcat);
        tomcat.getConnector().setMaxHeaderCount(-1);
        runHeadersTest(true, tomcat, 300, -1);
    }
```
## 28f5e2967824d67a85b0fab4370e3dc25bf9b07e ##
```
Cyclomatic Complexity	1
Assertions		2
Lines of Code		9
-    
    public void testBug48627() throws Exception {
        getTomcatInstanceTestWebapp(false, true);

        ByteChunk res = getUrl("http://localhost:" + getPort() +
                "/test/bug48nnn/bug48627.jsp");

        String result = res.toString();
        // Beware of the differences between escaping in JSP attributes and
        // in Java Strings
        assertEcho(result, "00-\\");
        assertEcho(result, "01-\\");
    }
```
```
Cyclomatic Complexity	 1
Assertions		 30
Lines of Code		37
-    
    public void testBug48668a() throws Exception {
        getTomcatInstanceTestWebapp(false, true);

        ByteChunk res = getUrl("http://localhost:" + getPort() +
                "/test/bug48nnn/bug48668a.jsp");
        String result = res.toString();
        assertEcho(result, "00-Hello world</p>#{foo.bar}");
        assertEcho(result, "01-Hello world</p>${foo.bar}");
        assertEcho(result, "10-Hello ${'foo.bar}");
        assertEcho(result, "11-Hello ${'foo.bar}");
        assertEcho(result, "12-Hello #{'foo.bar}");
        assertEcho(result, "13-Hello #{'foo.bar}");
        assertEcho(result, "14-Hello ${'foo}");
        assertEcho(result, "15-Hello ${'foo}");
        assertEcho(result, "16-Hello #{'foo}");
        assertEcho(result, "17-Hello #{'foo}");
        assertEcho(result, "18-Hello ${'foo.bar}");
        assertEcho(result, "19-Hello ${'foo.bar}");
        assertEcho(result, "20-Hello #{'foo.bar}");
        assertEcho(result, "21-Hello #{'foo.bar}");
        assertEcho(result, "30-Hello ${'foo}");
        assertEcho(result, "31-Hello ${'foo}");
        assertEcho(result, "32-Hello #{'foo}");
        assertEcho(result, "33-Hello #{'foo}");
        assertEcho(result, "34-Hello ${'foo}");
        assertEcho(result, "35-Hello ${'foo}");
        assertEcho(result, "36-Hello #{'foo}");
        assertEcho(result, "37-Hello #{'foo}");
        assertEcho(result, "40-Hello ${'foo}");
        assertEcho(result, "41-Hello ${'foo}");
        assertEcho(result, "42-Hello #{'foo}");
        assertEcho(result, "43-Hello #{'foo}");
        assertEcho(result, "50-Hello ${'foo}");
        assertEcho(result, "51-Hello ${'foo}");
        assertEcho(result, "52-Hello #{'foo}");
        assertEcho(result, "53-Hello #{'foo}");
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		4
-    
    public void testLegacyWithEquals() throws Exception {
        doTestLegacyEquals(true);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		4
-    
    public void testLegacyWithoutEquals() throws Exception {
        doTestLegacyEquals(false);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		4
-    
    public void testLegacyWithNameOnly() throws Exception {
        doTestLegacyNameOnly(true);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		4
-    
    public void testLegacyWithoutNameOnly() throws Exception {
        doTestLegacyNameOnly(false);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		4
-    
    public void testLegacyWithoutSeps() throws Exception {
        doTestLegacySeps(false, true);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		4
-    
    public void testLegacyWithFwdSlash() throws Exception {
        doTestLegacySeps(true, false);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		4
-    
    public void testLegacyWithoutFwdSlash() throws Exception {
        doTestLegacySeps(false, false);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		6
-    
    public void v1NullValue() {
        Cookie cookie = new Cookie("foo", null);
        cookie.setVersion(1);
        doTest(cookie, "foo=\"\"; Version=1", "foo=");
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		6
-    
    public void v1QuotedValue() {
        Cookie cookie = new Cookie("foo", "\"bar\"");
        cookie.setVersion(1);
        doTest(cookie, "foo=\"bar\"; Version=1", "foo=\"bar\"");
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		6
-    
    public void v1ValueContainsSemicolon() {
        Cookie cookie = new Cookie("foo", "a;b");
        cookie.setVersion(1);
        doTest(cookie, "foo=\"a;b\"; Version=1", null);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		6
-    
    public void v1ValueContainsComma() {
        Cookie cookie = new Cookie("foo", "a,b");
        cookie.setVersion(1);
        doTest(cookie, "foo=\"a,b\"; Version=1", null);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		6
-    
    public void v1ValueContainsSpace() {
        Cookie cookie = new Cookie("foo", "a b");
        cookie.setVersion(1);
        doTest(cookie, "foo=\"a b\"; Version=1", null);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		6
-    
    public void v1ValueContainsEquals() {
        Cookie cookie = new Cookie("foo", "a=b");
        cookie.setVersion(1);
        doTest(cookie, "foo=\"a=b\"; Version=1", "foo=a=b");
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		6
-    
    public void v1ValueContainsQuote() {
        Cookie cookie = new Cookie("foo", "a\"b");
        cookie.setVersion(1);
        doTest(cookie, "foo=\"a\\\"b\"; Version=1", null);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		6
-    
    public void v1ValueContainsNonV0Separator() {
        Cookie cookie = new Cookie("foo", "a()<>@,;:\\\"/[]?={}b");
        cookie.setVersion(1);
        doTest(cookie, "foo=\"a()<>@,;:\\\\\\\"/[]?={}b\"; Version=1", null);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		6
-    
    public void v1ValueContainsBackslash() {
        Cookie cookie = new Cookie("foo", "a\\b");
        cookie.setVersion(1);
        doTest(cookie, "foo=\"a\\\\b\"; Version=1", null);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		6
-    
    public void v1ValueContainsBackslashAndQuote() {
        Cookie cookie = new Cookie("foo", "a\"b\\c");
        cookie.setVersion(1);
        doTest(cookie, "foo=\"a\\\"b\\\\c\"; Version=1", null);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		4
-    
    public void testBasicCookieOld() {
        doTestBasicCookie(false);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		4
-    
    public void v1TokenValueOld() {
        doV1TokenValue(false);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		4
-    
    public void v1NameOnlyIsDroppedRfc6265() {
        doV1NameOnlyIsDropped(true);
    }
```
## b93fa63eade1deee9594d7ad3e372ea835ff99c1 ##
```
Cyclomatic Complexity	6
Assertions		12
Lines of Code		92
-    
    public void testAliases() throws Exception {
        // Some sample text
        String foxText = "The quick brown fox jumps over the lazy dog";
        String loremIpsum = "Lorem ipsum dolor sit amet, consectetur adipisicing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.";

        // Set up a temporary docBase and some alternates that we can
        // set up as aliases.
        File tmpDir = new File(getTemporaryDirectory(),
                               "tomcat-unit-test." + TestNamingContext.class.getName());

        // Make sure we've got a clean slate
        ExpandWar.delete(tmpDir);

        File docBase = new File(tmpDir, "docBase");
        File alternate1 = new File(tmpDir, "alternate1");
        File alternate2 = new File(tmpDir, "alternate2");

        // Register for clean-up
        addDeleteOnTearDown(tmpDir);

        if(!tmpDir.mkdirs())
            throw new IOException("Could not create temp directory " + tmpDir);
        if(!docBase.mkdir())
            throw new IOException("Could not create temp directory " + docBase);
        if(!alternate1.mkdir())
            throw new IOException("Could not create temp directory " + alternate1);
        if(!alternate2.mkdir())
            throw new IOException("Could not create temp directory " + alternate2);

        // Create a file in each alternate directory that we can attempt to access
        FileOutputStream fos = new FileOutputStream(new File(alternate1, "test1.txt"));
        try {
            fos.write(foxText.getBytes("UTF-8"));
            fos.flush();
        } finally {
            fos.close();
        }

        fos = new FileOutputStream(new File(alternate2, "test2.txt"));
        try {
            fos.write(loremIpsum.getBytes("UTF-8"));
            fos.flush();
        } finally {
            fos.close();
        }

        // Finally, create the Context
        FileDirContext ctx = new FileDirContext();
        ctx.setDocBase(docBase.getCanonicalPath());
        ctx.setAliases("/a1=" + alternate1.getCanonicalPath()
                       +",/a2=" + alternate2.getCanonicalPath());

        // Check first alias
        Object file = ctx.lookup("/a1/test1.txt");

        Assert.assertNotNull(file);
        Assert.assertTrue(file instanceof Resource);

        byte[] buffer = new byte[4096];
        Resource res = (Resource)file;

        InputStream is = res.streamContent();
        int len;
        try {
            len = is.read(buffer);
        } finally {
            is.close();
        }
        String contents = new String(buffer, 0, len, "UTF-8");

        assertEquals(foxText, contents);

        // Check second alias
        file = ctx.lookup("/a2/test2.txt");

        Assert.assertNotNull(file);
        Assert.assertTrue(file instanceof Resource);

        res = (Resource)file;
        is = res.streamContent();
        try {
            len = is.read(buffer);
        } finally {
            is.close();
        }
        contents = new String(buffer, 0, len, "UTF-8");

        assertEquals(loremIpsum, contents);

        // Test aliases with spaces around the separators
        ctx.setAliases("   /a1= " + alternate1.getCanonicalPath()
                       + "\n\n"
                       +", /a2 =\n" + alternate2.getCanonicalPath()
                       + ",");

        // Check first alias
        file = ctx.lookup("/a1/test1.txt");

        Assert.assertNotNull(file);
        Assert.assertTrue(file instanceof Resource);

        res = (Resource)file;
        is = res.streamContent();
        try {
            len = is.read(buffer);
        } finally {
            is.close();
        }
        contents = new String(buffer, 0, len, "UTF-8");

        assertEquals(foxText, contents);

        // Check second alias
        file = ctx.lookup("/a2/test2.txt");

        Assert.assertNotNull(file);
        Assert.assertTrue(file instanceof Resource);

        res = (Resource)file;
        is = res.streamContent();
        try {
            len = is.read(buffer);
        } finally {
            is.close();
        }
        contents = new String(buffer, 0, len, "UTF-8");

        assertEquals(loremIpsum, contents);
    }
```
```
Cyclomatic Complexity	 -1
Assertions		 3
Lines of Code		11
-    
    @Deprecated
    public void testFindNotBytes() throws UnsupportedEncodingException {
        byte[] bytes = "Hello\u00a0world".getBytes("ISO-8859-1");
        final int len = bytes.length;

        assertEquals(4, ByteChunk.findNotBytes(bytes, 0, len, new byte[] { 'l',
                'e', 'H' }));
        assertEquals(-1, ByteChunk.findNotBytes(bytes, 0, len, bytes));
        assertEquals(-1, ByteChunk.findNotBytes(bytes, 2, 3, new byte[] { 'l',
                'e', 'H' }));@@@     }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		6
-    
    public void testHeaderLimits3() throws Exception {
        // Cannot process 101 header
        Tomcat tomcat = getTomcatInstance();
        setupHeadersTest(tomcat);
        runHeadersTest(false, tomcat, 101, 100);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		7
-    
    public void testHeaderLimits4() throws Exception {
        // Can change maxHeaderCount
        Tomcat tomcat = getTomcatInstance();
        setupHeadersTest(tomcat);
        tomcat.getConnector().setMaxHeaderCount(-1);
        runHeadersTest(true, tomcat, 300, -1);
    }
```
