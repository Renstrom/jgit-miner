## 7ed68d068133cc8ce54d68cda4f8ce8cf27f3827 ##
```
Cyclomatic Complexity	1
Assertions		5
Lines of Code		17
-    
    public void testMutable()
    {
        HttpFields headers = HttpFields.build()
            .add(HttpHeader.ETAG, "tag")
            .add("name0", "value0")
            .add("name1", "value1").asImmutable();

        headers = HttpFields.build(headers, EnumSet.of(HttpHeader.ETAG, HttpHeader.CONTENT_RANGE))
            .add(new PreEncodedHttpField(HttpHeader.CONNECTION, HttpHeaderValue.CLOSE.asString()))
            .addDateField("name2", System.currentTimeMillis()).asImmutable();

        headers = HttpFields.build(headers, new HttpField(HttpHeader.CONNECTION, "open"));

        assertThat(headers.size(), is(4));
        assertThat(headers.getField(0).getValue(), is("value0"));
        assertThat(headers.getField(1).getValue(), is("value1"));
        assertThat(headers.getField(2).getValue(), is("open"));
        assertThat(headers.getField(3).getName(), is("name2"));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 4
Lines of Code		11
-    
    public void testMap()
    {
        Map<HttpFields.Immutable, String> map = new HashMap<>();
        map.put(HttpFields.build().add("X", "1").add(HttpHeader.ETAG, "tag").asImmutable(), "1");
        map.put(HttpFields.build().add("X", "2").add(HttpHeader.ETAG, "other").asImmutable(), "2");

        assertThat(map.get(HttpFields.build().add("X", "1").add(HttpHeader.ETAG, "tag").asImmutable()), is("1"));
        assertThat(map.get(HttpFields.build().add("X", "2").add(HttpHeader.ETAG, "other").asImmutable()), is("2"));
        assertThat(map.get(HttpFields.build().add("X", "2").asImmutable()), nullValue());
        assertThat(map.get(HttpFields.build().add("X", "2").add(HttpHeader.ETAG, "tag").asImmutable()), nullValue());
    }
```
```
Cyclomatic Complexity	 3
Assertions		 35
Lines of Code		63
-    
    public void testHttpHeaderValueParseCsv()
    {
        final List<HttpHeaderValue> list = new ArrayList<>();
        final List<String> unknowns = new ArrayList<>();

        assertTrue(HttpHeaderValue.parseCsvIndex("", list::add, unknowns::add));
        assertThat(list, empty());
        assertThat(unknowns, empty());

        assertTrue(HttpHeaderValue.parseCsvIndex(" ", list::add, unknowns::add));
        assertThat(list, empty());
        assertThat(unknowns, empty());

        assertTrue(HttpHeaderValue.parseCsvIndex(",", list::add, unknowns::add));
        assertThat(list, empty());
        assertThat(unknowns, empty());

        assertTrue(HttpHeaderValue.parseCsvIndex(",,", list::add, unknowns::add));
        assertThat(list, empty());
        assertThat(unknowns, empty());

        assertTrue(HttpHeaderValue.parseCsvIndex(" , , ", list::add, unknowns::add));
        assertThat(list, empty());
        assertThat(unknowns, empty());

        list.clear();
        assertTrue(HttpHeaderValue.parseCsvIndex("close", list::add));
        assertThat(list, contains(HttpHeaderValue.CLOSE));

        list.clear();
        assertTrue(HttpHeaderValue.parseCsvIndex(" close ", list::add));
        assertThat(list, contains(HttpHeaderValue.CLOSE));

        list.clear();
        assertTrue(HttpHeaderValue.parseCsvIndex(",close,", list::add));
        assertThat(list, contains(HttpHeaderValue.CLOSE));

        list.clear();
        assertTrue(HttpHeaderValue.parseCsvIndex(" , close , ", list::add));
        assertThat(list, contains(HttpHeaderValue.CLOSE));

        list.clear();
        assertTrue(HttpHeaderValue.parseCsvIndex(" close,GZIP, chunked    , Keep-Alive   ", list::add));
        assertThat(list, contains(HttpHeaderValue.CLOSE, HttpHeaderValue.GZIP, HttpHeaderValue.CHUNKED, HttpHeaderValue.KEEP_ALIVE));

        list.clear();
        assertTrue(HttpHeaderValue.parseCsvIndex(" close,GZIP, chunked    , Keep-Alive   ", t ->
        {
            if (t.toString().startsWith("c"))
                list.add(t);
            return true;
        }));
        assertThat(list, contains(HttpHeaderValue.CLOSE, HttpHeaderValue.CHUNKED));

        list.clear();
        assertFalse(HttpHeaderValue.parseCsvIndex(" close,GZIP, chunked    , Keep-Alive   ", t ->
        {
            if (HttpHeaderValue.CHUNKED == t)
                return false;
            list.add(t);
            return true;
        }));
        assertThat(list, contains(HttpHeaderValue.CLOSE, HttpHeaderValue.GZIP));

        list.clear();
        unknowns.clear();
        assertTrue(HttpHeaderValue.parseCsvIndex("closed,close, unknown , bytes", list::add, unknowns::add));
        assertThat(list, contains(HttpHeaderValue.CLOSE, HttpHeaderValue.BYTES));
        assertThat(unknowns, contains("closed", "unknown"));

        list.clear();
        unknowns.clear();
        assertFalse(HttpHeaderValue.parseCsvIndex("close, unknown , bytes", list::add, s -> false));
        assertThat(list, contains(HttpHeaderValue.CLOSE));
        assertThat(unknowns, empty());@@@     }
```
```
Cyclomatic Complexity	 1
Assertions		 10
Lines of Code		15
-    
    public void testExample() throws Exception
    {
        HttpURI uri = HttpURI.from("http://user:password@host:8888/ignored/../p%61th;ignored/info;param?query=value#fragment");

        assertThat(uri.getScheme(), is("http"));
        assertThat(uri.getUser(), is("user:password"));
        assertThat(uri.getHost(), is("host"));
        assertThat(uri.getPort(), is(8888));
        assertThat(uri.getPath(), is("/ignored/../p%61th;ignored/info;param"));
        assertThat(uri.getDecodedPath(), is("/path/info"));
        assertThat(uri.getParam(), is("param"));
        assertThat(uri.getQuery(), is("query=value"));
        assertThat(uri.getFragment(), is("fragment"));
        assertThat(uri.getAuthority(), is("host:8888"));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 6
Lines of Code		10
-    
    public void testJoin()
    {
        assertThat(QuotedCSV.join((String)null), nullValue());
        assertThat(QuotedCSV.join(Collections.emptyList()), is(emptyString()));
        assertThat(QuotedCSV.join(Collections.singletonList("hi")), is("hi"));
        assertThat(QuotedCSV.join("hi", "ho"), is("hi, ho"));
        assertThat(QuotedCSV.join("h i", "h,o"), is("\"h i\", \"h,o\""));
        assertThat(QuotedCSV.join("h\"i", "h\to"), is("\"h\\\"i\", \"h\\to\""));@@@     }
```
```
Cyclomatic Complexity	 4
Assertions		 13
Lines of Code		102
-    
    public void testGracefulServerGoAway() throws Exception
    {
        AtomicReference<Session> serverSessionRef = new AtomicReference<>();
        CountDownLatch serverSessionLatch = new CountDownLatch(1);
        CountDownLatch dataLatch = new CountDownLatch(2);
        start(new ServerSessionListener.Adapter()
        {
            @Override
            public void onAccept(Session session)
            {
                serverSessionRef.set(session);
                serverSessionLatch.countDown();
            }

            @Override
            public Stream.Listener onNewStream(Stream stream, HeadersFrame frame)
            {
                return new Stream.Listener.Adapter()
                {
                    @Override
                    public void onData(Stream stream, DataFrame frame, Callback callback)
                    {
                        callback.succeeded();
                        dataLatch.countDown();
                        if (frame.isEndStream())
                        {
                            MetaData.Response response = new MetaData.Response(HttpVersion.HTTP_2, HttpStatus.OK_200, HttpFields.EMPTY);
                            stream.headers(new HeadersFrame(stream.getId(), response, null, true), Callback.NOOP);
                        }
                    }
                };
            }
        });
        // Avoid aggressive idle timeout to allow the test verifications.
        connector.setShutdownIdleTimeout(connector.getIdleTimeout());

        CountDownLatch clientGracefulGoAwayLatch = new CountDownLatch(1);
        CountDownLatch clientGoAwayLatch = new CountDownLatch(1);
        CountDownLatch clientCloseLatch = new CountDownLatch(1);
        Session clientSession = newClient(new Session.Listener.Adapter()
        {
            @Override
            public void onGoAway(Session session, GoAwayFrame frame)
            {
                if (frame.isGraceful())
                    clientGracefulGoAwayLatch.countDown();
                else
                    clientGoAwayLatch.countDown();
            }

            @Override
            public void onClose(Session session, GoAwayFrame frame)
            {
                clientCloseLatch.countDown();
            }
        });
        assertTrue(serverSessionLatch.await(5, TimeUnit.SECONDS));
        Session serverSession = serverSessionRef.get();

        // Start 2 requests without completing them yet.
        CountDownLatch responseLatch = new CountDownLatch(2);
        MetaData.Request metaData1 = newRequest("GET", HttpFields.EMPTY);
        HeadersFrame request1 = new HeadersFrame(metaData1, null, false);
        FuturePromise<Stream> promise1 = new FuturePromise<>();
        Stream.Listener.Adapter listener = new Stream.Listener.Adapter()
        {
            @Override
            public void onHeaders(Stream stream, HeadersFrame frame)
            {
                if (frame.isEndStream())
                {
                    MetaData.Response response = (MetaData.Response)frame.getMetaData();
                    assertEquals(HttpStatus.OK_200, response.getStatus());
                    responseLatch.countDown();
                }
            }
        };
        clientSession.newStream(request1, promise1, listener);
        Stream stream1 = promise1.get(5, TimeUnit.SECONDS);
        stream1.data(new DataFrame(stream1.getId(), ByteBuffer.allocate(1), false), Callback.NOOP);

        MetaData.Request metaData2 = newRequest("GET", HttpFields.EMPTY);
        HeadersFrame request2 = new HeadersFrame(metaData2, null, false);
        FuturePromise<Stream> promise2 = new FuturePromise<>();
        clientSession.newStream(request2, promise2, listener);
        Stream stream2 = promise2.get(5, TimeUnit.SECONDS);
        stream2.data(new DataFrame(stream2.getId(), ByteBuffer.allocate(1), false), Callback.NOOP);

        assertTrue(dataLatch.await(5, TimeUnit.SECONDS));

        // Both requests are now on the server, shutdown gracefully the server session.
        int port = connector.getLocalPort();
        CompletableFuture<Void> shutdown = Graceful.shutdown(server);

        // Client should receive the graceful GOAWAY.
        assertTrue(clientGracefulGoAwayLatch.await(5, TimeUnit.SECONDS));
        // Client should not receive the non-graceful GOAWAY.
        assertFalse(clientGoAwayLatch.await(500, TimeUnit.MILLISECONDS));
        // Client should not be closed yet.
        assertFalse(clientCloseLatch.await(500, TimeUnit.MILLISECONDS));

        // Client cannot create new requests after receiving a GOAWAY.
        HostPortHttpField authority3 = new HostPortHttpField("localhost" + ":" + port);
        MetaData.Request metaData3 = new MetaData.Request("GET", HttpScheme.HTTP.asString(), authority3, servletPath, HttpVersion.HTTP_2, HttpFields.EMPTY, -1);
        HeadersFrame request3 = new HeadersFrame(metaData3, null, true);
        FuturePromise<Stream> promise3 = new FuturePromise<>();
        clientSession.newStream(request3, promise3, new Stream.Listener.Adapter());
        assertThrows(ExecutionException.class, () -> promise3.get(5, TimeUnit.SECONDS));

        // Finish the previous requests and expect the responses.
        stream1.data(new DataFrame(stream1.getId(), BufferUtil.EMPTY_BUFFER, true), Callback.NOOP);
        stream2.data(new DataFrame(stream2.getId(), BufferUtil.EMPTY_BUFFER, true), Callback.NOOP);
        assertTrue(responseLatch.await(5, TimeUnit.SECONDS));
        assertNull(shutdown.get(5, TimeUnit.SECONDS));

        // Now GOAWAY should arrive to the client.
        assertTrue(clientGoAwayLatch.await(5, TimeUnit.SECONDS));
        assertTrue(clientCloseLatch.await(5, TimeUnit.SECONDS));

        assertFalse(((HTTP2Session)clientSession).getEndPoint().isOpen());
        assertFalse(((HTTP2Session)serverSession).getEndPoint().isOpen());@@@     }
```
```
Cyclomatic Complexity	 1
Assertions		 7
Lines of Code		33
-    
    public void testDefaultContextPath() throws Exception
    {
        File testDir = MavenTestingUtils.getTargetTestingDir("dfltcp");
        FS.ensureEmpty(testDir);
        File webInf = new File(testDir, "WEB-INF");
        FS.ensureDirExists(webInf);
        
        File quickstartXml = new File(webInf, "quickstart-web.xml");
        assertFalse(quickstartXml.exists());

        // generate a quickstart-web.xml
        WebAppContext quickstart = new WebAppContext();
        quickstart.setResourceBase(testDir.getAbsolutePath());
        quickstart.addConfiguration(new QuickStartConfiguration());
        quickstart.setAttribute(QuickStartConfiguration.MODE, QuickStartConfiguration.Mode.GENERATE);
        quickstart.setAttribute(QuickStartConfiguration.ORIGIN_ATTRIBUTE, "origin");
        quickstart.setDescriptor(MavenTestingUtils.getTestResourceFile("web.xml").getAbsolutePath());
        quickstart.setContextPath("/foo");
        server.setHandler(quickstart);
        server.setDryRun(true);
        server.start();
        assertEquals("/foo", quickstart.getContextPath());
        assertFalse(quickstart.isContextPathDefault());

        assertTrue(quickstartXml.exists());

        // quick start
        WebAppContext webapp = new WebAppContext();
        webapp.addConfiguration(new QuickStartConfiguration());
        quickstart.setAttribute(QuickStartConfiguration.MODE, QuickStartConfiguration.Mode.QUICKSTART);
        webapp.setResourceBase(testDir.getAbsolutePath());
        webapp.getServerClassMatcher().exclude("org.eclipse.jetty.quickstart.");
        server.setHandler(webapp);

        server.setDryRun(false);
        server.start();

        // verify the context path is the default-context-path
        assertEquals("/thisIsTheDefault", webapp.getContextPath());
        assertTrue(webapp.isContextPathDefault());
    }
```
```
Cyclomatic Complexity	 1
Assertions		 5
Lines of Code		33
-    
    public void testDefaultRequestAndResponseEncodings() throws Exception
    {
        File testDir = MavenTestingUtils.getTargetTestingDir("dfltenc");
        FS.ensureEmpty(testDir);
        File webInf = new File(testDir, "WEB-INF");
        FS.ensureDirExists(webInf);
        
        File quickstartXml = new File(webInf, "quickstart-web.xml");
        assertFalse(quickstartXml.exists());

        // generate a quickstart-web.xml
        WebAppContext quickstart = new WebAppContext();
        quickstart.setResourceBase(testDir.getAbsolutePath());
        quickstart.addConfiguration(new QuickStartConfiguration());
        quickstart.setAttribute(QuickStartConfiguration.MODE, QuickStartConfiguration.Mode.GENERATE);
        quickstart.setAttribute(QuickStartConfiguration.ORIGIN_ATTRIBUTE, "origin");
        quickstart.setDescriptor(MavenTestingUtils.getTestResourceFile("web.xml").getAbsolutePath());
        quickstart.setContextPath("/foo");
        server.setHandler(quickstart);
        server.setDryRun(true);
        server.start();
        
        assertTrue(quickstartXml.exists());
        
        // quick start
        WebAppContext webapp = new WebAppContext();
        webapp.addConfiguration(new QuickStartConfiguration());
        quickstart.setAttribute(QuickStartConfiguration.MODE, QuickStartConfiguration.Mode.QUICKSTART);
        webapp.setResourceBase(testDir.getAbsolutePath());
        webapp.getServerClassMatcher().exclude("org.eclipse.jetty.quickstart.");
        server.setHandler(webapp);

        server.setDryRun(false);
        server.start();
        
        assertEquals("ascii", webapp.getDefaultRequestCharacterEncoding());
        assertEquals("utf-16", webapp.getDefaultResponseCharacterEncoding());@@@+        assertEquals("foo", sh.getName());@@@+        server.stop();@@@     }
```
```
Cyclomatic Complexity	 2
Assertions		 6
Lines of Code		27
-    
    public void testUrlQuery() throws Exception
    {
        CookiePatternRule rule = new CookiePatternRule();
        rule.setPattern("*");
        rule.setName("fruit");
        rule.setValue("banana");

        startServer(rule);

        StringBuilder rawRequest = new StringBuilder();
        rawRequest.append("GET /other?fruit=apple HTTP/1.1\r\n");
        rawRequest.append("Host: local\r\n");
        rawRequest.append("Connection: close\r\n");
        rawRequest.append("\r\n");

        String rawResponse = localConnector.getResponse(rawRequest.toString());
        HttpTester.Response response = HttpTester.parseResponse(rawResponse);

        String responseContent = response.getContent();
        assertResponseContentLine(responseContent, "baseRequest.requestUri=", "/other");
        assertResponseContentLine(responseContent, "request.queryString=", "fruit=apple");

        // verify
        HttpField setCookieField = response.getField(HttpHeader.SET_COOKIE);
        assertThat("response should have Set-Cookie", setCookieField, notNullValue());
        for (String value : setCookieField.getValues())
        {
            String[] result = value.split("=");
            assertThat(result[0], is("fruit"));
            assertThat(result[1], is("banana"));
        }@@@     }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		9
-    
    public void testWin2kSP1WithIE6() throws Exception
    {
        _request.setHttpFields(HttpFields.build(_request.getHttpFields())
            .add("User-Agent", "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.01)"));

        String result = _rule.matchAndApply(_request.getRequestURI(), _request, _response);

        assertEquals(_request.getRequestURI(), result);
        assertEquals(HttpHeaderValue.CLOSE.asString(), _response.getHeader(HttpHeader.CONNECTION.asString()));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		9
-    
    public void testWin2kSP1WithIE7() throws Exception
    {
        _request.setHttpFields(HttpFields.build(_request.getHttpFields())
            .add("User-Agent", "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.01)"));

        String result = _rule.matchAndApply(_request.getRequestURI(), _request, _response);

        assertNull(result);
        assertNull(_response.getHeader(HttpHeader.CONNECTION.asString()));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 8
Lines of Code		18
-    
    public void testGracefulWithContext() throws Exception
    {
        Socket client0 = newClientBusy(POST_A_12345, handlerA);
        Socket client1 = newClientBusy(POST_A_12345_C, handlerA);
        Socket client2 = newClientIdle(POST_A_12345, handlerA);

        backgroundComplete(client0, handlerA);
        backgroundComplete(client1, handlerA);
        Future<Integer> status2 = backgroundUnavailable(client2, POST_A_12345, contextA, handlerA);

        assertGracefulStop(server);

        assertResponse(client0, true);
        assertResponse(client1, false);
        assertThat(status2.get(), is(503));

        assertQuickClose(client0);
        assertQuickClose(client1);
        assertQuickClose(client2);
        assertHandled(handlerA, false);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 8
Lines of Code		19
-    
    public void testGracefulContext() throws Exception
    {
        Socket client0 = newClientBusy(POST_B_12345, handlerB);
        Socket client1 = newClientBusy(POST_B_12345_C, handlerB);
        Socket client2 = newClientIdle(POST_B_12345, handlerB);

        backgroundComplete(client0, handlerB);
        backgroundComplete(client1, handlerB);
        Future<Integer> status2 = backgroundUnavailable(client2, POST_B_12345, contextB, handlerB);

        Graceful.shutdown(contextB).orTimeout(10, TimeUnit.SECONDS).get();

        assertResponse(client0, false);
        assertResponse(client1, false);
        assertThat(status2.get(), is(503));

        assertAvailable(client0, POST_A_12345, handlerA);
        assertAvailable(client1, POST_A_12345_C, handlerA);
        assertAvailable(client2, POST_A_12345, handlerA);

        assertHandled(handlerA, false);
        assertHandled(handlerB, false);
    }
```
```
Cyclomatic Complexity	 2
Assertions		 6
Lines of Code		50
-    
    public void testRequestCharacterEncoding() throws Exception
    {
        AtomicReference<String> result = new AtomicReference<>(null);
        AtomicReference<String> overrideCharEncoding = new AtomicReference<>(null);

        _server.stop();
        ContextHandler handler = new CharEncodingContextHandler();
        _server.setHandler(handler);
        handler.setHandler(_handler);
        _handler._checker = new RequestTester()
        {
            @Override
            public boolean check(HttpServletRequest request, HttpServletResponse response)
            {
                try
                {
                    String s = overrideCharEncoding.get();
                    if (s != null)
                        request.setCharacterEncoding(s);

                    result.set(request.getCharacterEncoding());
                    return true;
                }
                catch (UnsupportedEncodingException e)
                {
                    return false;
                }
            }
        };
        _server.start();

        String request = "GET / HTTP/1.1\n" +
            "Host: whatever\r\n" +
            "Content-Type: text/html;charset=utf8\n" +
            "Connection: close\n" +
            "\n";

        //test setting the default char encoding
        handler.setDefaultRequestCharacterEncoding("ascii");
        String response = _connector.getResponse(request);
        assertTrue(response.startsWith("HTTP/1.1 200"));
        assertEquals("ascii", result.get());

        //test overriding the default char encoding with explicit encoding
        result.set(null);
        overrideCharEncoding.set("utf-16");
        response = _connector.getResponse(request);
        assertTrue(response.startsWith("HTTP/1.1 200"));
        assertEquals("utf-16", result.get());

        //test fallback to content-type encoding
        result.set(null);
        overrideCharEncoding.set(null);
        handler.setDefaultRequestCharacterEncoding(null);
        response = _connector.getResponse(request);
        assertTrue(response.startsWith("HTTP/1.1 200"));
        assertEquals("utf-8", result.get());@@@     }
```
```
Cyclomatic Complexity	 1
Assertions		 19
Lines of Code		60
-    
    public void testPushBuilder() throws Exception
    {
        String uri = "/foo/something";
        Request request = new TestRequest(null, null);
        request.getResponse().getHttpFields().add(new HttpCookie.SetCookieHttpField(new HttpCookie("good", "thumbsup", 100), CookieCompliance.RFC6265));
        request.getResponse().getHttpFields().add(new HttpCookie.SetCookieHttpField(new HttpCookie("bonza", "bewdy", 1), CookieCompliance.RFC6265));
        request.getResponse().getHttpFields().add(new HttpCookie.SetCookieHttpField(new HttpCookie("bad", "thumbsdown", 0), CookieCompliance.RFC6265));
        HttpFields.Mutable fields = HttpFields.build();
        fields.add(HttpHeader.AUTHORIZATION, "Basic foo");
        request.setMetaData(new MetaData.Request("GET", HttpURI.from(uri), HttpVersion.HTTP_1_0, fields));
        assertTrue(request.isPushSupported());
        PushBuilder builder = request.newPushBuilder();
        assertNotNull(builder);
        assertEquals("GET", builder.getMethod());
        assertThrows(NullPointerException.class, () ->
        {
            builder.method(null);
        });
        assertThrows(IllegalArgumentException.class, () ->
        {
            builder.method("");
        });
        assertThrows(IllegalArgumentException.class, () ->
        {
            builder.method("   ");
        });
        assertThrows(IllegalArgumentException.class, () ->
        {
            builder.method("POST");
        });
        assertThrows(IllegalArgumentException.class, () ->
        {
            builder.method("PUT");
        });
        assertThrows(IllegalArgumentException.class, () ->
        {
            builder.method("DELETE");
        });
        assertThrows(IllegalArgumentException.class, () ->
        {
            builder.method("CONNECT");
        });
        assertThrows(IllegalArgumentException.class, () ->
        {
            builder.method("OPTIONS");
        });
        assertThrows(IllegalArgumentException.class, () ->
        {
            builder.method("TRACE");
        });
        assertEquals(TestRequest.TEST_SESSION_ID, builder.getSessionId());
        builder.path("/foo/something-else.txt");
        assertEquals("/foo/something-else.txt", builder.getPath());
        assertEquals("Basic foo", builder.getHeader("Authorization"));
        assertThat(builder.getHeader("Cookie"), containsString("bonza"));
        assertThat(builder.getHeader("Cookie"), containsString("good"));
        assertThat(builder.getHeader("Cookie"), containsString("maxpos"));
        assertThat(builder.getHeader("Cookie"), not(containsString("bad")));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		18
-    
    public void testPushBuilderWithIdNoAuth() throws Exception
    {
        String uri = "/foo/something";
        Request request = new TestRequest(null, null)
        {
            @Override
            public Principal getUserPrincipal()
            {
                return () -> "test";
            }
        };
        HttpFields.Mutable fields = HttpFields.build();
        request.setMetaData(new MetaData.Request("GET", HttpURI.from(uri), HttpVersion.HTTP_1_0, fields));
        assertTrue(request.isPushSupported());
        PushBuilder builder = request.newPushBuilder();
        assertNotNull(builder);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 48
Lines of Code		77
-    
    public void testServletPathMapping() throws Exception
    {
        ServletPathSpec spec;
        String uri;
        ServletPathMapping m;

        spec = null;
        uri = null;
        m = new ServletPathMapping(spec, null, uri);
        assertThat(m.getMappingMatch(), nullValue());
        assertThat(m.getMatchValue(), is(""));
        assertThat(m.getPattern(), nullValue());
        assertThat(m.getServletName(), is(""));
        assertThat(m.getServletPath(), nullValue());
        assertThat(m.getPathInfo(), nullValue());

        spec = new ServletPathSpec("");
        uri = "/";
        m = new ServletPathMapping(spec, "Something", uri);
        assertThat(m.getMappingMatch(), is(MappingMatch.CONTEXT_ROOT));
        assertThat(m.getMatchValue(), is(""));
        assertThat(m.getPattern(), is(""));
        assertThat(m.getServletName(), is("Something"));
        assertThat(m.getServletPath(), is(spec.getPathMatch(uri)));
        assertThat(m.getPathInfo(), is(spec.getPathInfo(uri)));

        spec = new ServletPathSpec("/");
        uri = "/some/path";
        m = new ServletPathMapping(spec, "Default", uri);
        assertThat(m.getMappingMatch(), is(MappingMatch.DEFAULT));
        assertThat(m.getMatchValue(), is(""));
        assertThat(m.getPattern(), is("/"));
        assertThat(m.getServletName(), is("Default"));
        assertThat(m.getServletPath(), is(spec.getPathMatch(uri)));
        assertThat(m.getPathInfo(), is(spec.getPathInfo(uri)));

        spec = new ServletPathSpec("/foo/*");
        uri = "/foo/bar";
        m = new ServletPathMapping(spec, "FooServlet", uri);
        assertThat(m.getMappingMatch(), is(MappingMatch.PATH));
        assertThat(m.getMatchValue(), is("foo"));
        assertThat(m.getPattern(), is("/foo/*"));
        assertThat(m.getServletName(), is("FooServlet"));
        assertThat(m.getServletPath(), is(spec.getPathMatch(uri)));
        assertThat(m.getPathInfo(), is(spec.getPathInfo(uri)));

        uri = "/foo/";
        m = new ServletPathMapping(spec, "FooServlet", uri);
        assertThat(m.getMappingMatch(), is(MappingMatch.PATH));
        assertThat(m.getMatchValue(), is("foo"));
        assertThat(m.getPattern(), is("/foo/*"));
        assertThat(m.getServletName(), is("FooServlet"));
        assertThat(m.getServletPath(), is(spec.getPathMatch(uri)));
        assertThat(m.getPathInfo(), is(spec.getPathInfo(uri)));

        uri = "/foo";
        m = new ServletPathMapping(spec, "FooServlet", uri);
        assertThat(m.getMappingMatch(), is(MappingMatch.PATH));
        assertThat(m.getMatchValue(), is("foo"));
        assertThat(m.getPattern(), is("/foo/*"));
        assertThat(m.getServletName(), is("FooServlet"));
        assertThat(m.getServletPath(), is(spec.getPathMatch(uri)));
        assertThat(m.getPathInfo(), is(spec.getPathInfo(uri)));

        spec = new ServletPathSpec("*.jsp");
        uri = "/foo/bar.jsp";
        m = new ServletPathMapping(spec, "JspServlet", uri);
        assertThat(m.getMappingMatch(), is(MappingMatch.EXTENSION));
        assertThat(m.getMatchValue(), is("foo/bar"));
        assertThat(m.getPattern(), is("*.jsp"));
        assertThat(m.getServletName(), is("JspServlet"));
        assertThat(m.getServletPath(), is(spec.getPathMatch(uri)));
        assertThat(m.getPathInfo(), is(spec.getPathInfo(uri)));

        spec = new ServletPathSpec("/catalog");
        uri = "/catalog";
        m = new ServletPathMapping(spec, "CatalogServlet", uri);
        assertThat(m.getMappingMatch(), is(MappingMatch.EXACT));
        assertThat(m.getMatchValue(), is("catalog"));
        assertThat(m.getPattern(), is("/catalog"));
        assertThat(m.getServletName(), is("CatalogServlet"));
        assertThat(m.getServletPath(), is(spec.getPathMatch(uri)));
        assertThat(m.getPathInfo(), is(spec.getPathInfo(uri)));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 11
Lines of Code		36
-    
    public void testLocaleAndContentTypeEncoding() throws Exception
    {
        _server.stop();
        MimeTypes.getInferredEncodings().put("text/html", "iso-8859-1");
        ContextHandler handler = new ContextHandler();
        handler.addLocaleEncoding("ja", "euc-jp");
        handler.addLocaleEncoding("zh_CN", "gb18030");
        _server.setHandler(handler);
        handler.setHandler(new DumpHandler());
        _server.start();

        Response response = getResponse();
        response.getHttpChannel().getRequest().setContext(handler.getServletContext(), "/");

        response.setContentType("text/html");
        assertEquals("iso-8859-1", response.getCharacterEncoding());

        // setLocale should change character encoding based on
        // locale-encoding-mapping-list
        response.setLocale(Locale.JAPAN);
        assertEquals("euc-jp", response.getCharacterEncoding());

        // setLocale should change character encoding based on
        // locale-encoding-mapping-list
        response.setLocale(Locale.CHINA);
        assertEquals("gb18030", response.getCharacterEncoding());

        // setContentType here doesn't define character encoding
        response.setContentType("text/html");
        assertEquals("gb18030", response.getCharacterEncoding());

        // setCharacterEncoding should still be able to change encoding
        response.setCharacterEncoding("utf-8");
        assertEquals("utf-8", response.getCharacterEncoding());

        // setLocale should not override explicit character encoding request
        response.setLocale(Locale.JAPAN);
        assertEquals("utf-8", response.getCharacterEncoding());

        // setContentType should still be able to change encoding
        response.setContentType("text/html;charset=gb18030");
        assertEquals("gb18030", response.getCharacterEncoding());

        // setCharacterEncoding should still be able to change encoding
        response.setCharacterEncoding("utf-8");
        assertEquals("utf-8", response.getCharacterEncoding());

        // getWriter should freeze the character encoding
        PrintWriter pw = response.getWriter();
        assertEquals("utf-8", response.getCharacterEncoding());

        // setCharacterEncoding should no longer be able to change the encoding
        response.setCharacterEncoding("iso-8859-1");
        assertEquals("utf-8", response.getCharacterEncoding());

        // setLocale should not override explicit character encoding request
        response.setLocale(Locale.JAPAN);
        assertEquals("utf-8", response.getCharacterEncoding());
    }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		9
-    
    public void testContentEncodingViaContentTypeChange() throws Exception
    {
        Response response = getResponse();
        response.setContentType("text/html;charset=Shift_Jis");
        assertEquals("Shift_Jis", response.getCharacterEncoding());

        response.setContentType("text/xml");
        assertEquals("Shift_Jis", response.getCharacterEncoding());@@@     }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		51
-    
    public void testSessionListenerOrdering()
        throws Exception
    {
        final StringBuffer result = new StringBuffer();

        class Listener1 implements HttpSessionListener
        {

            @Override
            public void sessionCreated(HttpSessionEvent se)
            {
                result.append("Listener1 create;");
            }

            @Override
            public void sessionDestroyed(HttpSessionEvent se)
            {
                result.append("Listener1 destroy;");
            }
        }

        class Listener2 implements HttpSessionListener
        {

            @Override
            public void sessionCreated(HttpSessionEvent se)
            {
                result.append("Listener2 create;");
            }

            @Override
            public void sessionDestroyed(HttpSessionEvent se)
            {
                result.append("Listener2 destroy;");
            }

        }

        Server server = new Server();
        SessionHandler sessionHandler = new SessionHandler();
        try
        {
            sessionHandler.addEventListener(new Listener1());
            sessionHandler.addEventListener(new Listener2());
            sessionHandler.setServer(server);
            sessionHandler.start();
            Session session = new Session(sessionHandler, new SessionData("aa", "_", "0.0", 0, 0, 0, 0));
            sessionHandler.callSessionCreatedListeners(session);
            sessionHandler.callSessionDestroyedListeners(session);
            assertEquals("Listener1 create;Listener2 create;Listener2 destroy;Listener1 destroy;", result.toString());
        }
        finally
        {
            sessionHandler.stop();
        }@@@+        assertThrows(IllegalArgumentException.class, () ->@@@+            sessionHandler.setSessionTrackingModes(new HashSet<>(Arrays.asList(SessionTrackingMode.SSL, SessionTrackingMode.URL))));@@@     }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		16
-    
    public void testNamedInclude() throws Exception
    {
        _contextHandler.addServlet(NamedIncludeServlet.class, "/include/*");
        String echo = _contextHandler.addServlet(EchoURIServlet.class, "/echo/*").getName();

        String expected =
            "HTTP/1.1 200 OK\r\n" +
                "Content-Length: 62\r\n" +
                "\r\n" +
                "/context\r\n" +
                "/include\r\n" +
                "/info\r\n" +
                "/context/include/info;param=value\r\n";
        String responses = _connector.getResponse("GET /context/include/info;param=value?name=" + echo + " HTTP/1.0\n\n");
        assertEquals(expected, responses);
    }
```
```
Cyclomatic Complexity	 3
Assertions		 2
Lines of Code		11
-    
    public void testDispatchMapping() throws Exception
    {
        _contextHandler.addServlet(new ServletHolder("TestServlet", MappingServlet.class), "/TestServlet");
        _contextHandler.addServlet(new ServletHolder("DispatchServlet", AsyncDispatch2TestServlet.class), "/DispatchServlet");
        _contextHandler.addServlet(new ServletHolder("DispatchServlet2", AsyncDispatch2TestServlet.class), "/DispatchServlet2");

        // TODO Test TCK hack for https://github.com/eclipse-ee4j/jakartaee-tck/issues/585
        String response = _connector.getResponse("GET /context/DispatchServlet HTTP/1.0\n\n");
        assertThat(response, containsString("matchValue=DispatchServlet, pattern=/DispatchServlet, servletName=DispatchServlet, mappingMatch=EXACT"));

        // TODO Test how it should work after fix for https://github.com/eclipse-ee4j/jakartaee-tck/issues/585
        String response2 = _connector.getResponse("GET /context/DispatchServlet2 HTTP/1.0\n\n");
        assertThat(response2, containsString("matchValue=TestServlet, pattern=/TestServlet, servletName=TestServlet, mappingMatch=EXACT"));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 9
Lines of Code		25
-    
    public void testErrorOverridesMimeTypeAndCharset() throws Exception
    {
        StringBuilder rawRequest = new StringBuilder();
        rawRequest.append("GET /error-mime-charset-writer/ HTTP/1.1\r\n");
        rawRequest.append("Host: test\r\n");
        rawRequest.append("Connection: close\r\n");
        rawRequest.append("Accept: */*\r\n");
        rawRequest.append("Accept-Charset: *\r\n");
        rawRequest.append("\r\n");

        String rawResponse = _connector.getResponse(rawRequest.toString());
        System.out.println(rawResponse);
        HttpTester.Response response = HttpTester.parseResponse(rawResponse);

        assertThat(response.getStatus(), is(595));
        String actualContentType = response.get(HttpHeader.CONTENT_TYPE);
        // should not expect to see charset line from servlet
        assertThat(actualContentType, not(containsString("charset=US-ASCII")));
        String body = response.getContent();

        assertThat(body, containsString("ERROR_PAGE: /595"));
        assertThat(body, containsString("ERROR_MESSAGE: 595"));
        assertThat(body, containsString("ERROR_CODE: 595"));
        assertThat(body, containsString("ERROR_EXCEPTION: null"));
        assertThat(body, containsString("ERROR_EXCEPTION_TYPE: null"));
        assertThat(body, containsString("ERROR_SERVLET: org.eclipse.jetty.servlet.ErrorPageTest$ErrorContentTypeCharsetWriterInitializedServlet-"));
        assertThat(body, containsString("ERROR_REQUEST_URI: /error-mime-charset-writer/"));@@@     }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		20
-    
    public void testCreateInstance() throws Exception
    {
        try (StacklessLogging ignore = new StacklessLogging(ServletHandler.class, ServletContextHandler.class))
        {
            //test without a ServletContextHandler or current ContextHandler
            FilterHolder holder = new FilterHolder();
            holder.setName("foo");
            holder.setHeldClass(DummyFilter.class);
            Filter filter = holder.createInstance();
            assertNotNull(filter);

            //test with a ServletContextHandler
            Server server = new Server();
            ServletContextHandler context = new ServletContextHandler();
            server.setHandler(context);
            ServletHandler handler = context.getServletHandler();
            handler.addFilter(holder);
            holder.setServletHandler(handler);
            context.start();
            assertNotNull(holder.getFilter());
        }@@@     }
```
```
Cyclomatic Complexity	 2
Assertions		 2
Lines of Code		14
-    
    public void testCharsetResetToJsonMimeType() throws Exception
    {
        HttpTester.Request request = new HttpTester.Request();
        request.setMethod("GET");
        request.setURI("/charset/json-reset/");
        request.setVersion(HttpVersion.HTTP_1_1);
        request.setHeader("Connection", "close");
        request.setHeader("Host", "test");

        ByteBuffer responseBuffer = connector.getResponse(request.generate());
        // System.err.println(BufferUtil.toUTF8String(responseBuffer));
        HttpTester.Response response = HttpTester.parseResponse(responseBuffer);

        // Now test for properly formatted HTTP Response Headers.
        assertThat("Response Code", response.getStatus(), is(200));
        // The Content-Type should not have a charset= portion
        assertThat("Response Header Content-Type", response.get("Content-Type"), is("application/json"));
    }
```
```
Cyclomatic Complexity	 2
Assertions		 2
Lines of Code		14
-    
    public void testCharsetChangeToJsonMimeType() throws Exception
    {
        HttpTester.Request request = new HttpTester.Request();
        request.setMethod("GET");
        request.setURI("/charset/json-change/");
        request.setVersion(HttpVersion.HTTP_1_1);
        request.setHeader("Connection", "close");
        request.setHeader("Host", "test");

        ByteBuffer responseBuffer = connector.getResponse(request.generate());
        // System.err.println(BufferUtil.toUTF8String(responseBuffer));
        HttpTester.Response response = HttpTester.parseResponse(responseBuffer);

        // Now test for properly formatted HTTP Response Headers.
        assertThat("Response Code", response.getStatus(), is(200));
        // The Content-Type should not have a charset= portion
        assertThat("Response Header Content-Type", response.get("Content-Type"), is("application/json"));
    }
```
```
Cyclomatic Complexity	 2
Assertions		 2
Lines of Code		14
-    
    public void testCharsetChangeToJsonMimeTypeSetCharsetToNull() throws Exception
    {
        HttpTester.Request request = new HttpTester.Request();
        request.setMethod("GET");
        request.setURI("/charset/json-change-null/");
        request.setVersion(HttpVersion.HTTP_1_1);
        request.setHeader("Connection", "close");
        request.setHeader("Host", "test");

        ByteBuffer responseBuffer = connector.getResponse(request.generate());
        // System.err.println(BufferUtil.toUTF8String(responseBuffer));
        HttpTester.Response response = HttpTester.parseResponse(responseBuffer);

        // Now test for properly formatted HTTP Response Headers.
        assertThat("Response Code", response.getStatus(), is(200));
        // The Content-Type should not have a charset= portion
        assertThat("Response Header Content-Type", response.get("Content-Type"), is("application/json"));@@@     }
```
```
Cyclomatic Complexity	 1
Assertions		 6
Lines of Code		18
-    
    public void testGetSetSessionTimeout() throws Exception
    {
        ContextHandlerCollection contexts = new ContextHandlerCollection();
        _server.setHandler(contexts);

        int startMin = 7;
        Integer timeout = Integer.valueOf(100);
        ServletContextHandler root = new ServletContextHandler(contexts, "/", ServletContextHandler.SESSIONS);
        root.getSessionHandler().setMaxInactiveInterval((int)TimeUnit.MINUTES.toSeconds(startMin));
        root.addBean(new MySCIStarter(root.getServletContext(), new MySCI(true, timeout.intValue())), true);
        _server.start();

        //test starting value of setSessionTimeout
        assertEquals(startMin, (Integer)root.getServletContext().getAttribute("MYSCI.startSessionTimeout"));
        //test can set session timeout from ServletContainerInitializer
        assertTrue((Boolean)root.getServletContext().getAttribute("MYSCI.setSessionTimeout"));
        //test can get session timeout from ServletContainerInitializer
        assertEquals(timeout, (Integer)root.getServletContext().getAttribute("MYSCI.getSessionTimeout"));
        assertNull(root.getAttribute("MYSCI.sessionTimeoutFailure"));
        //test can't get session timeout from ContextListener that is not from annotation or web.xml
        assertTrue((Boolean)root.getServletContext().getAttribute("MyContextListener.getSessionTimeout"));
        //test can't set session timeout from ContextListener that is not from annotation or web.xml
        assertTrue((Boolean)root.getServletContext().getAttribute("MyContextListener.setSessionTimeout"));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 3
Lines of Code		20
-    
    public void testCreateMethodsFromSCL() throws Exception
    {
      //A filter can be created by an SCI
        ContextHandlerCollection contexts = new ContextHandlerCollection();
        _server.setHandler(contexts);

        ServletContextHandler root = new ServletContextHandler(contexts, "/");
        class ListenerCreatingSCI implements ServletContainerInitializer
        {
            @Override
            public void onStartup(Set<Class<?>> c, ServletContext ctx) throws ServletException
            {
                ctx.addListener(new CreatingSCL());
            }
        }

        root.addBean(new MySCIStarter(root.getServletContext(), new ListenerCreatingSCI()), true);
        _server.start();
        assertTrue((Boolean)root.getServletContext().getAttribute("CreatingSCL.filter"));
        assertTrue((Boolean)root.getServletContext().getAttribute("CreatingSCL.servlet"));
        assertTrue((Boolean)root.getServletContext().getAttribute("CreatingSCL.listener"));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 4
Lines of Code		15
-    
    public void testProvidersUsingDefault() throws Exception
    {
        Path homePath = MavenTestingUtils.getTestResourceDir("providers-home").toPath().toRealPath();

        List<String> cmdLineArgs = new ArrayList<>();
        cmdLineArgs.add("user.dir=" + homePath);
        cmdLineArgs.add("jetty.home=" + homePath);
        cmdLineArgs.add("--module=server");

        Main main = new Main();
        StartArgs args = main.processCommandLine(cmdLineArgs.toArray(new String[cmdLineArgs.size()]));
        BaseHome baseHome = main.getBaseHome();

        assertThat("jetty.home", baseHome.getHome(), is(homePath.toString()));
        assertThat("jetty.base", baseHome.getBase(), is(homePath.toString()));

        ConfigurationAssert.assertConfiguration(baseHome, args, "assert-providers-default.txt");
    }
```
```
Cyclomatic Complexity	 1
Assertions		 4
Lines of Code		16
-    
    public void testProvidersUsingSpecific() throws Exception
    {
        Path homePath = MavenTestingUtils.getTestResourceDir("providers-home").toPath().toRealPath();

        List<String> cmdLineArgs = new ArrayList<>();
        cmdLineArgs.add("user.dir=" + homePath);
        cmdLineArgs.add("jetty.home=" + homePath);
        cmdLineArgs.add("--module=server");
        cmdLineArgs.add("--module=logging-b");

        Main main = new Main();
        StartArgs args = main.processCommandLine(cmdLineArgs.toArray(new String[cmdLineArgs.size()]));
        BaseHome baseHome = main.getBaseHome();

        assertThat("jetty.home", baseHome.getHome(), is(homePath.toString()));
        assertThat("jetty.base", baseHome.getBase(), is(homePath.toString()));

        ConfigurationAssert.assertConfiguration(baseHome, args, "assert-providers-specific.txt");@@@     }
```
```
Cyclomatic Complexity	 1
Assertions		 9
Lines of Code		23
-    
    public void testDownloadSnapshotRepo()
        throws Exception
    {
        Path snapshotLocalRepoDir = testdir.getPath().resolve("snapshot-repo");
        FS.ensureEmpty(snapshotLocalRepoDir);

        MavenLocalRepoFileInitializer repo =
            new MavenLocalRepoFileInitializer(baseHome, snapshotLocalRepoDir, false, "https://oss.sonatype.org/content/repositories/jetty-snapshots/");
        String ref = "maven://org.eclipse.jetty/jetty-rewrite/11.0.0-SNAPSHOT/jar";
        Coordinates coords = repo.getCoordinates(URI.create(ref));
        assertThat("Coordinates", coords, notNullValue());

        assertThat("coords.groupId", coords.groupId, is("org.eclipse.jetty"));
        assertThat("coords.artifactId", coords.artifactId, is("jetty-rewrite"));
        assertThat("coords.version", coords.version, is("11.0.0-SNAPSHOT"));
        assertThat("coords.type", coords.type, is("jar"));
        assertThat("coords.classifier", coords.classifier, is(nullValue()));

        assertThat("coords.toCentralURI", coords.toCentralURI().toASCIIString(),
            is("https://oss.sonatype.org/content/repositories/jetty-snapshots/org/eclipse/jetty/jetty-rewrite/11.0.0-SNAPSHOT/jetty-rewrite-11.0.0-SNAPSHOT.jar"));

        Path destination = baseHome.getBasePath().resolve("jetty-rewrite-11.0.0-SNAPSHOT.jar");
        repo.download(coords, destination);
        assertThat(Files.exists(destination), is(true));
        assertThat("Snapshot File size", destination.toFile().length(), greaterThan(10_000L));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		14
-    
    public void testDownloadSnapshotRepoWithExtractDeep()
        throws Exception
    {
        Path snapshotLocalRepoDir = testdir.getPath().resolve("snapshot-repo");
        FS.ensureEmpty(snapshotLocalRepoDir);

        MavenLocalRepoFileInitializer repo =
            new MavenLocalRepoFileInitializer(baseHome, snapshotLocalRepoDir, false,
                "https://oss.sonatype.org/content/repositories/jetty-snapshots/");
        String ref = "maven://org.eclipse.jetty/test-jetty-webapp/11.0.0-SNAPSHOT/jar/config";
        Path baseDir = baseHome.getBasePath();
        repo.create(URI.create(ref), "extract:company/");

        assertThat(Files.exists(baseDir.resolve("company/webapps/test.d/override-web.xml")), is(true));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 4
Lines of Code		10
-    
    public void testCopyIndirect()
    {
        ByteBuffer b = BufferUtil.toBuffer("Hello World");
        ByteBuffer c = BufferUtil.copy(b);
        assertEquals("Hello World", BufferUtil.toString(c));
        assertFalse(c.isDirect());
        assertThat(b, not(sameInstance(c)));
        assertThat(b.array(), not(sameInstance(c.array())));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 3
Lines of Code		10
-    
    public void testCopyDirect()
    {
        ByteBuffer b = BufferUtil.allocateDirect(11);
        BufferUtil.append(b, "Hello World");
        ByteBuffer c = BufferUtil.copy(b);
        assertEquals("Hello World", BufferUtil.toString(c));
        assertTrue(c.isDirect());
        assertThat(b, not(sameInstance(c)));@@@     }
```
```
Cyclomatic Complexity	 1
Assertions		 24
Lines of Code		28
-    
    public void testRequiredCapacity()
    {
        assertThat(requiredCapacity(Set.of("ABC", "abc"), true), is(1 + 6));
        assertThat(requiredCapacity(Set.of("ABC", "abc"), false), is(1 + 3));
        assertThat(requiredCapacity(Set.of(""), false), is(1 + 0));
        assertThat(requiredCapacity(Set.of("ABC", ""), false), is(1 + 3));
        assertThat(requiredCapacity(Set.of("ABC"), false), is(1 + 3));
        assertThat(requiredCapacity(Set.of("ABC", "XYZ"), false), is(1 + 6));
        assertThat(requiredCapacity(Set.of("A00", "A11"), false), is(1 + 5));
        assertThat(requiredCapacity(Set.of("A00", "A01", "A10", "A11"), false), is(1 + 7));
        assertThat(requiredCapacity(Set.of("A", "AB"), false), is(1 + 2));
        assertThat(requiredCapacity(Set.of("A", "ABC"), false), is(1 + 3));
        assertThat(requiredCapacity(Set.of("A", "ABCD"), false), is(1 + 4));
        assertThat(requiredCapacity(Set.of("AB", "ABC"), false), is(1 + 3));
        assertThat(requiredCapacity(Set.of("ABC", "ABCD"), false), is(1 + 4));
        assertThat(requiredCapacity(Set.of("ABC", "ABCDEF"), false), is(1 + 6));
        assertThat(requiredCapacity(Set.of("AB", "A"), false), is(1 + 2));
        assertThat(requiredCapacity(Set.of("ABC", "ABCDEF"), false), is(1 + 6));
        assertThat(requiredCapacity(Set.of("ABCDEF", "ABC"), false), is(1 + 6));
        assertThat(requiredCapacity(Set.of("ABC", "ABCDEF", "ABX"), false), is(1 + 7));
        assertThat(requiredCapacity(Set.of("ABCDEF", "ABC", "ABX"), false), is(1 + 7));
        assertThat(requiredCapacity(Set.of("ADEF", "AQPR4", "AQZ"), false), is(1 + 9));
        assertThat(requiredCapacity(Set.of("111", "ADEF", "AQPR4", "AQZ", "999"), false), is(1 + 15));
        assertThat(requiredCapacity(Set.of("utf-16", "utf-8"), false), is(1 + 7));
        assertThat(requiredCapacity(Set.of("utf-16", "utf-8", "utf16", "utf8"), false), is(1 + 10));
        assertThat(requiredCapacity(Set.of("utf-8", "utf8", "utf-16", "utf16", "iso-8859-1", "iso_8859_1"), false), is(1 + 27));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		8
-    
    public void testLargeRequiredCapacity()
    {
        String x = "x".repeat(Character.MAX_VALUE / 2);
        String y = "y".repeat(Character.MAX_VALUE / 2);
        String z = "z".repeat(Character.MAX_VALUE / 2);
        assertThat(requiredCapacity(Set.of(x, y, z), true), is(1 + 3 * (Character.MAX_VALUE / 2)));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 3
Lines of Code		9
-    
    public void testArrayTrieCapacity()
    {
        ArrayTrie<String> trie = new ArrayTrie<>(Character.MAX_VALUE);
        String huge = "x".repeat(Character.MAX_VALUE - 1);
        assertTrue(trie.put(huge, "wow"));
        assertThat(trie.get(huge), is("wow"));

        assertThrows(IllegalArgumentException.class, () -> new ArrayTrie<String>(Character.MAX_VALUE + 1));@@@     }
```
```
Cyclomatic Complexity	 1
Assertions		 3
Lines of Code		13
-    
    public void testJoinWithStopTimeout() throws Exception
    {
        final long stopTimeout = 100;
        QueuedThreadPool threadPool = new QueuedThreadPool();
        threadPool.setStopTimeout(100);
        threadPool.start();

        // Verify that join does not timeout after waiting twice the stopTimeout.
        assertThrows(Throwable.class, () ->
            assertTimeoutPreemptively(Duration.ofMillis(stopTimeout * 2), threadPool::join)
        );

        // After stopping the ThreadPool join should unblock.
        LifeCycle.stop(threadPool);
        assertTimeoutPreemptively(Duration.ofMillis(stopTimeout), threadPool::join);@@@     }
```
```
Cyclomatic Complexity	 2
Assertions		 2
Lines of Code		17
-    
    public void testFindAndFilterContainerPathsJDK9() throws Exception
    {
        MetaInfConfiguration config = new MetaInfConfiguration();
        WebAppContext context = new WebAppContext();
        context.setAttribute(MetaInfConfiguration.CONTAINER_JAR_PATTERN, ".*/jetty-util-[^/]*\\.jar$|.*/jetty-util/target/classes/$|.*/foo-bar-janb.jar");
        WebAppClassLoader loader = new WebAppClassLoader(context);
        context.setClassLoader(loader);
        config.findAndFilterContainerPaths(context);
        List<Resource> containerResources = context.getMetaData().getContainerResources();
        assertEquals(2, containerResources.size());
        for (Resource r : containerResources)
        {
            String s = r.toString();
            assertTrue(s.endsWith("foo-bar-janb.jar") || s.contains("jetty-util"));
        }@@@     }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		11
-    
    public void testSetWithNullProperty() throws Exception
    {
        XmlConfiguration configuration = asXmlConfiguration("<Configure class=\"org.eclipse.jetty.xml.TestConfiguration\"><Set name=\"TestString\" property=\"prop\" id=\"test\"/></Configure>");
        configuration.getProperties().remove("prop");
        TestConfiguration tc = new TestConfiguration();
        tc.setTestString("default");
        configuration.configure(tc);
        assertEquals("default", tc.getTestString());
        assertNull(configuration.getIdMap().get("test"));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		11
-    
    public void testSetWithPropertyAndValue() throws Exception
    {
        XmlConfiguration configuration = asXmlConfiguration("<Configure class=\"org.eclipse.jetty.xml.TestConfiguration\"><Set name=\"TestString\" property=\"prop\" id=\"test\">Value</Set></Configure>");
        configuration.getProperties().put("prop", "This is a property value");
        TestConfiguration tc = new TestConfiguration();
        tc.setTestString("default");
        configuration.configure(tc);
        assertEquals("Value", tc.getTestString());
        assertEquals(configuration.getIdMap().get("test"), "Value");
    }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		11
-    
    public void testSetWithNullPropertyAndValue() throws Exception
    {
        XmlConfiguration configuration = asXmlConfiguration("<Configure class=\"org.eclipse.jetty.xml.TestConfiguration\"><Set name=\"TestString\" property=\"prop\" id=\"test\">Value</Set></Configure>");
        configuration.getProperties().remove("prop");
        TestConfiguration tc = new TestConfiguration();
        tc.setTestString("default");
        configuration.configure(tc);
        assertEquals("default", tc.getTestString());
        assertNull(configuration.getIdMap().get("test"));
    }
```
```
Cyclomatic Complexity	 10
Assertions		 17
Lines of Code		67
-    
    public void testCleanOrphans() throws Exception
    {
        //create the SessionDataStore
        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/test");
        SessionDataStoreFactory factory = createSessionDataStoreFactory();
        ((AbstractSessionDataStoreFactory)factory).setGracePeriodSec(GRACE_PERIOD_SEC);
        SessionDataStore store = factory.getSessionDataStore(context.getSessionHandler());
        SessionContext sessionContext = new SessionContext("foo", context.getServletContext());
        store.initialize(sessionContext);

        long now = System.currentTimeMillis();
        
        //persist a long ago expired session for our context
        SessionData oldSession = store.newSessionData("001", 100, 101, 100, TimeUnit.MINUTES.toMillis(60));
        oldSession.setExpiry(200);
        oldSession.setLastNode("me");
        persistSession(oldSession);
        assertTrue(checkSessionExists(oldSession));
        
        //persist a recently expired session for our context
        SessionData expiredSession = store.newSessionData("002", 100, 101, 100, TimeUnit.MINUTES.toMillis(60));
        expiredSession.setExpiry(RECENT_TIMESTAMP);
        expiredSession.setLastNode("me");
        persistSession(expiredSession);
        assertTrue(checkSessionExists(expiredSession));

        //persist a non expired session for our context
        SessionData unexpiredSession = store.newSessionData("003", 100, now + 10, now + 5, TimeUnit.MINUTES.toMillis(60));
        unexpiredSession.setExpiry(now + TimeUnit.MINUTES.toMillis(10));
        unexpiredSession.setLastNode("me");
        persistSession(unexpiredSession);
        assertTrue(checkSessionExists(unexpiredSession));

        //persist an immortal session for our context
        SessionData immortalSession = store.newSessionData("004", 100, now + 10, now + 5, TimeUnit.MINUTES.toMillis(60));
        immortalSession.setExpiry(0);
        immortalSession.setLastNode("me");
        persistSession(immortalSession);
        assertTrue(checkSessionExists(immortalSession));
        
        //create sessions for a different context
        //persist a long ago expired session for a different context
        SessionData oldForeignSession = store.newSessionData("005", 100, 101, 100, TimeUnit.MINUTES.toMillis(60));
        oldForeignSession.setContextPath("_other");
        oldForeignSession.setExpiry(200);
        oldForeignSession.setLastNode("me");
        persistSession(oldForeignSession);
        assertTrue(checkSessionExists(oldForeignSession));
        
        //persist a recently expired session for our context
        SessionData expiredForeignSession = store.newSessionData("006", 100, 101, 100, TimeUnit.MINUTES.toMillis(60));
        expiredForeignSession.setContextPath("_other");
        expiredForeignSession.setExpiry(RECENT_TIMESTAMP);
        expiredForeignSession.setLastNode("me");
        persistSession(expiredForeignSession);
        assertTrue(checkSessionExists(expiredForeignSession));
        
        //persist a non expired session for our context
        SessionData unexpiredForeignSession = store.newSessionData("007", 100, now + 10, now + 5, TimeUnit.MINUTES.toMillis(60));
        unexpiredForeignSession.setContextPath("_other");
        unexpiredForeignSession.setExpiry(now + TimeUnit.MINUTES.toMillis(10));
        unexpiredForeignSession.setLastNode("me");
        persistSession(unexpiredForeignSession);
        assertTrue(checkSessionExists(unexpiredForeignSession));
        
        //persist an immortal session for our context
        SessionData immortalForeignSession = store.newSessionData("008", 100, now + 10, now + 5, TimeUnit.MINUTES.toMillis(60));
        immortalForeignSession.setContextPath("_other");
        immortalForeignSession.setExpiry(0);
        immortalForeignSession.setLastNode("me");
        persistSession(immortalForeignSession);
        assertTrue(checkSessionExists(immortalForeignSession));
        
        store.start();
        
        ((AbstractSessionDataStore)store).cleanOrphans(now - TimeUnit.SECONDS.toMillis(10 * GRACE_PERIOD_SEC));

        //old session should be gone
        assertFalse(checkSessionExists(oldSession));
        //recently expired session should still be there
        assertTrue(checkSessionExists(expiredSession));
        //unexpired session should still be there
        assertTrue(checkSessionExists(unexpiredSession));
        //immortal session should still exist
        assertTrue(checkSessionExists(immortalSession));
        //old foreign session should be gone
        assertFalse(checkSessionExists(oldSession));
        //recently expired foreign session should still be there
        assertTrue(checkSessionExists(expiredSession));
        //unexpired foreign session should still be there
        assertTrue(checkSessionExists(unexpiredSession));
        //immortal foreign session should still exist
        assertTrue(checkSessionExists(immortalSession));@@@+        assertThat(expiredIds, containsInAnyOrder("1234"));@@@     }
```
## e1e16ae54bf33e95b41caf5a05e65f4e872e9dfa ##
```
Cyclomatic Complexity	1
Assertions		5
Lines of Code		17
-    
    public void testMutable()
    {
        HttpFields headers = HttpFields.build()
            .add(HttpHeader.ETAG, "tag")
            .add("name0", "value0")
            .add("name1", "value1").asImmutable();

        headers = HttpFields.build(headers, EnumSet.of(HttpHeader.ETAG, HttpHeader.CONTENT_RANGE))
            .add(new PreEncodedHttpField(HttpHeader.CONNECTION, HttpHeaderValue.CLOSE.asString()))
            .addDateField("name2", System.currentTimeMillis()).asImmutable();

        headers = HttpFields.build(headers, new HttpField(HttpHeader.CONNECTION, "open"));

        assertThat(headers.size(), is(4));
        assertThat(headers.getField(0).getValue(), is("value0"));
        assertThat(headers.getField(1).getValue(), is("value1"));
        assertThat(headers.getField(2).getValue(), is("open"));
        assertThat(headers.getField(3).getName(), is("name2"));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 4
Lines of Code		11
-    
    public void testMap()
    {
        Map<HttpFields.Immutable, String> map = new HashMap<>();
        map.put(HttpFields.build().add("X", "1").add(HttpHeader.ETAG, "tag").asImmutable(), "1");
        map.put(HttpFields.build().add("X", "2").add(HttpHeader.ETAG, "other").asImmutable(), "2");

        assertThat(map.get(HttpFields.build().add("X", "1").add(HttpHeader.ETAG, "tag").asImmutable()), is("1"));
        assertThat(map.get(HttpFields.build().add("X", "2").add(HttpHeader.ETAG, "other").asImmutable()), is("2"));
        assertThat(map.get(HttpFields.build().add("X", "2").asImmutable()), nullValue());
        assertThat(map.get(HttpFields.build().add("X", "2").add(HttpHeader.ETAG, "tag").asImmutable()), nullValue());
    }
```
```
Cyclomatic Complexity	 3
Assertions		 35
Lines of Code		63
-    
    public void testHttpHeaderValueParseCsv()
    {
        final List<HttpHeaderValue> list = new ArrayList<>();
        final List<String> unknowns = new ArrayList<>();

        assertTrue(HttpHeaderValue.parseCsvIndex("", list::add, unknowns::add));
        assertThat(list, empty());
        assertThat(unknowns, empty());

        assertTrue(HttpHeaderValue.parseCsvIndex(" ", list::add, unknowns::add));
        assertThat(list, empty());
        assertThat(unknowns, empty());

        assertTrue(HttpHeaderValue.parseCsvIndex(",", list::add, unknowns::add));
        assertThat(list, empty());
        assertThat(unknowns, empty());

        assertTrue(HttpHeaderValue.parseCsvIndex(",,", list::add, unknowns::add));
        assertThat(list, empty());
        assertThat(unknowns, empty());

        assertTrue(HttpHeaderValue.parseCsvIndex(" , , ", list::add, unknowns::add));
        assertThat(list, empty());
        assertThat(unknowns, empty());

        list.clear();
        assertTrue(HttpHeaderValue.parseCsvIndex("close", list::add));
        assertThat(list, contains(HttpHeaderValue.CLOSE));

        list.clear();
        assertTrue(HttpHeaderValue.parseCsvIndex(" close ", list::add));
        assertThat(list, contains(HttpHeaderValue.CLOSE));

        list.clear();
        assertTrue(HttpHeaderValue.parseCsvIndex(",close,", list::add));
        assertThat(list, contains(HttpHeaderValue.CLOSE));

        list.clear();
        assertTrue(HttpHeaderValue.parseCsvIndex(" , close , ", list::add));
        assertThat(list, contains(HttpHeaderValue.CLOSE));

        list.clear();
        assertTrue(HttpHeaderValue.parseCsvIndex(" close,GZIP, chunked    , Keep-Alive   ", list::add));
        assertThat(list, contains(HttpHeaderValue.CLOSE, HttpHeaderValue.GZIP, HttpHeaderValue.CHUNKED, HttpHeaderValue.KEEP_ALIVE));

        list.clear();
        assertTrue(HttpHeaderValue.parseCsvIndex(" close,GZIP, chunked    , Keep-Alive   ", t ->
        {
            if (t.toString().startsWith("c"))
                list.add(t);
            return true;
        }));
        assertThat(list, contains(HttpHeaderValue.CLOSE, HttpHeaderValue.CHUNKED));

        list.clear();
        assertFalse(HttpHeaderValue.parseCsvIndex(" close,GZIP, chunked    , Keep-Alive   ", t ->
        {
            if (HttpHeaderValue.CHUNKED == t)
                return false;
            list.add(t);
            return true;
        }));
        assertThat(list, contains(HttpHeaderValue.CLOSE, HttpHeaderValue.GZIP));

        list.clear();
        unknowns.clear();
        assertTrue(HttpHeaderValue.parseCsvIndex("closed,close, unknown , bytes", list::add, unknowns::add));
        assertThat(list, contains(HttpHeaderValue.CLOSE, HttpHeaderValue.BYTES));
        assertThat(unknowns, contains("closed", "unknown"));

        list.clear();
        unknowns.clear();
        assertFalse(HttpHeaderValue.parseCsvIndex("close, unknown , bytes", list::add, s -> false));
        assertThat(list, contains(HttpHeaderValue.CLOSE));
        assertThat(unknowns, empty());@@@     }
```
```
Cyclomatic Complexity	 1
Assertions		 10
Lines of Code		15
-    
    public void testExample() throws Exception
    {
        HttpURI uri = HttpURI.from("http://user:password@host:8888/ignored/../p%61th;ignored/info;param?query=value#fragment");

        assertThat(uri.getScheme(), is("http"));
        assertThat(uri.getUser(), is("user:password"));
        assertThat(uri.getHost(), is("host"));
        assertThat(uri.getPort(), is(8888));
        assertThat(uri.getPath(), is("/ignored/../p%61th;ignored/info;param"));
        assertThat(uri.getDecodedPath(), is("/path/info"));
        assertThat(uri.getParam(), is("param"));
        assertThat(uri.getQuery(), is("query=value"));
        assertThat(uri.getFragment(), is("fragment"));
        assertThat(uri.getAuthority(), is("host:8888"));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 6
Lines of Code		10
-    
    public void testJoin()
    {
        assertThat(QuotedCSV.join((String)null), nullValue());
        assertThat(QuotedCSV.join(Collections.emptyList()), is(emptyString()));
        assertThat(QuotedCSV.join(Collections.singletonList("hi")), is("hi"));
        assertThat(QuotedCSV.join("hi", "ho"), is("hi, ho"));
        assertThat(QuotedCSV.join("h i", "h,o"), is("\"h i\", \"h,o\""));
        assertThat(QuotedCSV.join("h\"i", "h\to"), is("\"h\\\"i\", \"h\\to\""));@@@     }
```
```
Cyclomatic Complexity	 4
Assertions		 13
Lines of Code		102
-    
    public void testGracefulServerGoAway() throws Exception
    {
        AtomicReference<Session> serverSessionRef = new AtomicReference<>();
        CountDownLatch serverSessionLatch = new CountDownLatch(1);
        CountDownLatch dataLatch = new CountDownLatch(2);
        start(new ServerSessionListener.Adapter()
        {
            @Override
            public void onAccept(Session session)
            {
                serverSessionRef.set(session);
                serverSessionLatch.countDown();
            }

            @Override
            public Stream.Listener onNewStream(Stream stream, HeadersFrame frame)
            {
                return new Stream.Listener.Adapter()
                {
                    @Override
                    public void onData(Stream stream, DataFrame frame, Callback callback)
                    {
                        callback.succeeded();
                        dataLatch.countDown();
                        if (frame.isEndStream())
                        {
                            MetaData.Response response = new MetaData.Response(HttpVersion.HTTP_2, HttpStatus.OK_200, HttpFields.EMPTY);
                            stream.headers(new HeadersFrame(stream.getId(), response, null, true), Callback.NOOP);
                        }
                    }
                };
            }
        });
        // Avoid aggressive idle timeout to allow the test verifications.
        connector.setShutdownIdleTimeout(connector.getIdleTimeout());

        CountDownLatch clientGracefulGoAwayLatch = new CountDownLatch(1);
        CountDownLatch clientGoAwayLatch = new CountDownLatch(1);
        CountDownLatch clientCloseLatch = new CountDownLatch(1);
        Session clientSession = newClient(new Session.Listener.Adapter()
        {
            @Override
            public void onGoAway(Session session, GoAwayFrame frame)
            {
                if (frame.isGraceful())
                    clientGracefulGoAwayLatch.countDown();
                else
                    clientGoAwayLatch.countDown();
            }

            @Override
            public void onClose(Session session, GoAwayFrame frame)
            {
                clientCloseLatch.countDown();
            }
        });
        assertTrue(serverSessionLatch.await(5, TimeUnit.SECONDS));
        Session serverSession = serverSessionRef.get();

        // Start 2 requests without completing them yet.
        CountDownLatch responseLatch = new CountDownLatch(2);
        MetaData.Request metaData1 = newRequest("GET", HttpFields.EMPTY);
        HeadersFrame request1 = new HeadersFrame(metaData1, null, false);
        FuturePromise<Stream> promise1 = new FuturePromise<>();
        Stream.Listener.Adapter listener = new Stream.Listener.Adapter()
        {
            @Override
            public void onHeaders(Stream stream, HeadersFrame frame)
            {
                if (frame.isEndStream())
                {
                    MetaData.Response response = (MetaData.Response)frame.getMetaData();
                    assertEquals(HttpStatus.OK_200, response.getStatus());
                    responseLatch.countDown();
                }
            }
        };
        clientSession.newStream(request1, promise1, listener);
        Stream stream1 = promise1.get(5, TimeUnit.SECONDS);
        stream1.data(new DataFrame(stream1.getId(), ByteBuffer.allocate(1), false), Callback.NOOP);

        MetaData.Request metaData2 = newRequest("GET", HttpFields.EMPTY);
        HeadersFrame request2 = new HeadersFrame(metaData2, null, false);
        FuturePromise<Stream> promise2 = new FuturePromise<>();
        clientSession.newStream(request2, promise2, listener);
        Stream stream2 = promise2.get(5, TimeUnit.SECONDS);
        stream2.data(new DataFrame(stream2.getId(), ByteBuffer.allocate(1), false), Callback.NOOP);

        assertTrue(dataLatch.await(5, TimeUnit.SECONDS));

        // Both requests are now on the server, shutdown gracefully the server session.
        int port = connector.getLocalPort();
        CompletableFuture<Void> shutdown = Graceful.shutdown(server);

        // Client should receive the graceful GOAWAY.
        assertTrue(clientGracefulGoAwayLatch.await(5, TimeUnit.SECONDS));
        // Client should not receive the non-graceful GOAWAY.
        assertFalse(clientGoAwayLatch.await(500, TimeUnit.MILLISECONDS));
        // Client should not be closed yet.
        assertFalse(clientCloseLatch.await(500, TimeUnit.MILLISECONDS));

        // Client cannot create new requests after receiving a GOAWAY.
        HostPortHttpField authority3 = new HostPortHttpField("localhost" + ":" + port);
        MetaData.Request metaData3 = new MetaData.Request("GET", HttpScheme.HTTP.asString(), authority3, servletPath, HttpVersion.HTTP_2, HttpFields.EMPTY, -1);
        HeadersFrame request3 = new HeadersFrame(metaData3, null, true);
        FuturePromise<Stream> promise3 = new FuturePromise<>();
        clientSession.newStream(request3, promise3, new Stream.Listener.Adapter());
        assertThrows(ExecutionException.class, () -> promise3.get(5, TimeUnit.SECONDS));

        // Finish the previous requests and expect the responses.
        stream1.data(new DataFrame(stream1.getId(), BufferUtil.EMPTY_BUFFER, true), Callback.NOOP);
        stream2.data(new DataFrame(stream2.getId(), BufferUtil.EMPTY_BUFFER, true), Callback.NOOP);
        assertTrue(responseLatch.await(5, TimeUnit.SECONDS));
        assertNull(shutdown.get(5, TimeUnit.SECONDS));

        // Now GOAWAY should arrive to the client.
        assertTrue(clientGoAwayLatch.await(5, TimeUnit.SECONDS));
        assertTrue(clientCloseLatch.await(5, TimeUnit.SECONDS));

        assertFalse(((HTTP2Session)clientSession).getEndPoint().isOpen());
        assertFalse(((HTTP2Session)serverSession).getEndPoint().isOpen());@@@     }
```
```
Cyclomatic Complexity	 1
Assertions		 7
Lines of Code		33
-    
    public void testDefaultContextPath() throws Exception
    {
        File testDir = MavenTestingUtils.getTargetTestingDir("dfltcp");
        FS.ensureEmpty(testDir);
        File webInf = new File(testDir, "WEB-INF");
        FS.ensureDirExists(webInf);
        
        File quickstartXml = new File(webInf, "quickstart-web.xml");
        assertFalse(quickstartXml.exists());

        // generate a quickstart-web.xml
        WebAppContext quickstart = new WebAppContext();
        quickstart.setResourceBase(testDir.getAbsolutePath());
        quickstart.addConfiguration(new QuickStartConfiguration());
        quickstart.setAttribute(QuickStartConfiguration.MODE, QuickStartConfiguration.Mode.GENERATE);
        quickstart.setAttribute(QuickStartConfiguration.ORIGIN_ATTRIBUTE, "origin");
        quickstart.setDescriptor(MavenTestingUtils.getTestResourceFile("web.xml").getAbsolutePath());
        quickstart.setContextPath("/foo");
        server.setHandler(quickstart);
        server.setDryRun(true);
        server.start();
        assertEquals("/foo", quickstart.getContextPath());
        assertFalse(quickstart.isContextPathDefault());

        assertTrue(quickstartXml.exists());

        // quick start
        WebAppContext webapp = new WebAppContext();
        webapp.addConfiguration(new QuickStartConfiguration());
        quickstart.setAttribute(QuickStartConfiguration.MODE, QuickStartConfiguration.Mode.QUICKSTART);
        webapp.setResourceBase(testDir.getAbsolutePath());
        webapp.getServerClassMatcher().exclude("org.eclipse.jetty.quickstart.");
        server.setHandler(webapp);

        server.setDryRun(false);
        server.start();

        // verify the context path is the default-context-path
        assertEquals("/thisIsTheDefault", webapp.getContextPath());
        assertTrue(webapp.isContextPathDefault());
    }
```
```
Cyclomatic Complexity	 1
Assertions		 5
Lines of Code		33
-    
    public void testDefaultRequestAndResponseEncodings() throws Exception
    {
        File testDir = MavenTestingUtils.getTargetTestingDir("dfltenc");
        FS.ensureEmpty(testDir);
        File webInf = new File(testDir, "WEB-INF");
        FS.ensureDirExists(webInf);
        
        File quickstartXml = new File(webInf, "quickstart-web.xml");
        assertFalse(quickstartXml.exists());

        // generate a quickstart-web.xml
        WebAppContext quickstart = new WebAppContext();
        quickstart.setResourceBase(testDir.getAbsolutePath());
        quickstart.addConfiguration(new QuickStartConfiguration());
        quickstart.setAttribute(QuickStartConfiguration.MODE, QuickStartConfiguration.Mode.GENERATE);
        quickstart.setAttribute(QuickStartConfiguration.ORIGIN_ATTRIBUTE, "origin");
        quickstart.setDescriptor(MavenTestingUtils.getTestResourceFile("web.xml").getAbsolutePath());
        quickstart.setContextPath("/foo");
        server.setHandler(quickstart);
        server.setDryRun(true);
        server.start();
        
        assertTrue(quickstartXml.exists());
        
        // quick start
        WebAppContext webapp = new WebAppContext();
        webapp.addConfiguration(new QuickStartConfiguration());
        quickstart.setAttribute(QuickStartConfiguration.MODE, QuickStartConfiguration.Mode.QUICKSTART);
        webapp.setResourceBase(testDir.getAbsolutePath());
        webapp.getServerClassMatcher().exclude("org.eclipse.jetty.quickstart.");
        server.setHandler(webapp);

        server.setDryRun(false);
        server.start();
        
        assertEquals("ascii", webapp.getDefaultRequestCharacterEncoding());
        assertEquals("utf-16", webapp.getDefaultResponseCharacterEncoding());@@@+        assertEquals("foo", sh.getName());@@@+        server.stop();@@@     }
```
```
Cyclomatic Complexity	 2
Assertions		 6
Lines of Code		27
-    
    public void testUrlQuery() throws Exception
    {
        CookiePatternRule rule = new CookiePatternRule();
        rule.setPattern("*");
        rule.setName("fruit");
        rule.setValue("banana");

        startServer(rule);

        StringBuilder rawRequest = new StringBuilder();
        rawRequest.append("GET /other?fruit=apple HTTP/1.1\r\n");
        rawRequest.append("Host: local\r\n");
        rawRequest.append("Connection: close\r\n");
        rawRequest.append("\r\n");

        String rawResponse = localConnector.getResponse(rawRequest.toString());
        HttpTester.Response response = HttpTester.parseResponse(rawResponse);

        String responseContent = response.getContent();
        assertResponseContentLine(responseContent, "baseRequest.requestUri=", "/other");
        assertResponseContentLine(responseContent, "request.queryString=", "fruit=apple");

        // verify
        HttpField setCookieField = response.getField(HttpHeader.SET_COOKIE);
        assertThat("response should have Set-Cookie", setCookieField, notNullValue());
        for (String value : setCookieField.getValues())
        {
            String[] result = value.split("=");
            assertThat(result[0], is("fruit"));
            assertThat(result[1], is("banana"));
        }@@@     }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		9
-    
    public void testWin2kSP1WithIE6() throws Exception
    {
        _request.setHttpFields(HttpFields.build(_request.getHttpFields())
            .add("User-Agent", "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.01)"));

        String result = _rule.matchAndApply(_request.getRequestURI(), _request, _response);

        assertEquals(_request.getRequestURI(), result);
        assertEquals(HttpHeaderValue.CLOSE.asString(), _response.getHeader(HttpHeader.CONNECTION.asString()));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		9
-    
    public void testWin2kSP1WithIE7() throws Exception
    {
        _request.setHttpFields(HttpFields.build(_request.getHttpFields())
            .add("User-Agent", "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.01)"));

        String result = _rule.matchAndApply(_request.getRequestURI(), _request, _response);

        assertNull(result);
        assertNull(_response.getHeader(HttpHeader.CONNECTION.asString()));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 8
Lines of Code		18
-    
    public void testGracefulWithContext() throws Exception
    {
        Socket client0 = newClientBusy(POST_A_12345, handlerA);
        Socket client1 = newClientBusy(POST_A_12345_C, handlerA);
        Socket client2 = newClientIdle(POST_A_12345, handlerA);

        backgroundComplete(client0, handlerA);
        backgroundComplete(client1, handlerA);
        Future<Integer> status2 = backgroundUnavailable(client2, POST_A_12345, contextA, handlerA);

        assertGracefulStop(server);

        assertResponse(client0, true);
        assertResponse(client1, false);
        assertThat(status2.get(), is(503));

        assertQuickClose(client0);
        assertQuickClose(client1);
        assertQuickClose(client2);
        assertHandled(handlerA, false);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 8
Lines of Code		19
-    
    public void testGracefulContext() throws Exception
    {
        Socket client0 = newClientBusy(POST_B_12345, handlerB);
        Socket client1 = newClientBusy(POST_B_12345_C, handlerB);
        Socket client2 = newClientIdle(POST_B_12345, handlerB);

        backgroundComplete(client0, handlerB);
        backgroundComplete(client1, handlerB);
        Future<Integer> status2 = backgroundUnavailable(client2, POST_B_12345, contextB, handlerB);

        Graceful.shutdown(contextB).orTimeout(10, TimeUnit.SECONDS).get();

        assertResponse(client0, false);
        assertResponse(client1, false);
        assertThat(status2.get(), is(503));

        assertAvailable(client0, POST_A_12345, handlerA);
        assertAvailable(client1, POST_A_12345_C, handlerA);
        assertAvailable(client2, POST_A_12345, handlerA);

        assertHandled(handlerA, false);
        assertHandled(handlerB, false);
    }
```
```
Cyclomatic Complexity	 2
Assertions		 6
Lines of Code		50
-    
    public void testRequestCharacterEncoding() throws Exception
    {
        AtomicReference<String> result = new AtomicReference<>(null);
        AtomicReference<String> overrideCharEncoding = new AtomicReference<>(null);

        _server.stop();
        ContextHandler handler = new CharEncodingContextHandler();
        _server.setHandler(handler);
        handler.setHandler(_handler);
        _handler._checker = new RequestTester()
        {
            @Override
            public boolean check(HttpServletRequest request, HttpServletResponse response)
            {
                try
                {
                    String s = overrideCharEncoding.get();
                    if (s != null)
                        request.setCharacterEncoding(s);

                    result.set(request.getCharacterEncoding());
                    return true;
                }
                catch (UnsupportedEncodingException e)
                {
                    return false;
                }
            }
        };
        _server.start();

        String request = "GET / HTTP/1.1\n" +
            "Host: whatever\r\n" +
            "Content-Type: text/html;charset=utf8\n" +
            "Connection: close\n" +
            "\n";

        //test setting the default char encoding
        handler.setDefaultRequestCharacterEncoding("ascii");
        String response = _connector.getResponse(request);
        assertTrue(response.startsWith("HTTP/1.1 200"));
        assertEquals("ascii", result.get());

        //test overriding the default char encoding with explicit encoding
        result.set(null);
        overrideCharEncoding.set("utf-16");
        response = _connector.getResponse(request);
        assertTrue(response.startsWith("HTTP/1.1 200"));
        assertEquals("utf-16", result.get());

        //test fallback to content-type encoding
        result.set(null);
        overrideCharEncoding.set(null);
        handler.setDefaultRequestCharacterEncoding(null);
        response = _connector.getResponse(request);
        assertTrue(response.startsWith("HTTP/1.1 200"));
        assertEquals("utf-8", result.get());@@@     }
```
```
Cyclomatic Complexity	 1
Assertions		 19
Lines of Code		60
-    
    public void testPushBuilder() throws Exception
    {
        String uri = "/foo/something";
        Request request = new TestRequest(null, null);
        request.getResponse().getHttpFields().add(new HttpCookie.SetCookieHttpField(new HttpCookie("good","thumbsup", 100), CookieCompliance.RFC6265));
        request.getResponse().getHttpFields().add(new HttpCookie.SetCookieHttpField(new HttpCookie("bonza","bewdy", 1), CookieCompliance.RFC6265));
        request.getResponse().getHttpFields().add(new HttpCookie.SetCookieHttpField(new HttpCookie("bad", "thumbsdown", 0), CookieCompliance.RFC6265));
        HttpFields.Mutable fields = HttpFields.build();
        fields.add(HttpHeader.AUTHORIZATION, "Basic foo");
        request.setMetaData(new MetaData.Request("GET", HttpURI.from(uri), HttpVersion.HTTP_1_0, fields));
        assertTrue(request.isPushSupported());
        PushBuilder builder = request.newPushBuilder();
        assertNotNull(builder);
        assertEquals("GET", builder.getMethod());
        assertThrows(NullPointerException.class, () ->
        {
            builder.method(null);
        });
        assertThrows(IllegalArgumentException.class, () ->
        {
            builder.method("");
        });
        assertThrows(IllegalArgumentException.class, () ->
        {
            builder.method("   ");
        });
        assertThrows(IllegalArgumentException.class, () ->
        {
            builder.method("POST");
        });
        assertThrows(IllegalArgumentException.class, () ->
        {
            builder.method("PUT");
        });
        assertThrows(IllegalArgumentException.class, () ->
        {
            builder.method("DELETE");
        });
        assertThrows(IllegalArgumentException.class, () ->
        {
            builder.method("CONNECT");
        });
        assertThrows(IllegalArgumentException.class, () ->
        {
            builder.method("OPTIONS");
        });
        assertThrows(IllegalArgumentException.class, () ->
        {
            builder.method("TRACE");
        });
        assertEquals(TestRequest.TEST_SESSION_ID, builder.getSessionId());
        builder.path("/foo/something-else.txt");
        assertEquals("/foo/something-else.txt", builder.getPath());
        assertEquals("Basic foo", builder.getHeader("Authorization"));
        assertThat(builder.getHeader("Cookie"), containsString("bonza"));
        assertThat(builder.getHeader("Cookie"), containsString("good"));
        assertThat(builder.getHeader("Cookie"), containsString("maxpos"));
        assertThat(builder.getHeader("Cookie"), not(containsString("bad")));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		18
-    
    public void testPushBuilderWithIdNoAuth() throws Exception
    {
        String uri = "/foo/something";
        Request request = new TestRequest(null, null)
        {
            @Override
            public Principal getUserPrincipal()
            {
                return () -> "test";
            }
        };
        HttpFields.Mutable fields = HttpFields.build();
        request.setMetaData(new MetaData.Request("GET", HttpURI.from(uri), HttpVersion.HTTP_1_0, fields));
        assertTrue(request.isPushSupported());
        PushBuilder builder = request.newPushBuilder();
        assertNotNull(builder);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 48
Lines of Code		77
-    
    public void testServletPathMapping() throws Exception
    {
        ServletPathSpec spec;
        String uri;
        ServletPathMapping m;

        spec = null;
        uri = null;
        m = new ServletPathMapping(spec, null, uri);
        assertThat(m.getMappingMatch(), nullValue());
        assertThat(m.getMatchValue(), is(""));
        assertThat(m.getPattern(), nullValue());
        assertThat(m.getServletName(), is(""));
        assertThat(m.getServletPath(), nullValue());
        assertThat(m.getPathInfo(), nullValue());

        spec = new ServletPathSpec("");
        uri = "/";
        m = new ServletPathMapping(spec, "Something", uri);
        assertThat(m.getMappingMatch(), is(MappingMatch.CONTEXT_ROOT));
        assertThat(m.getMatchValue(), is(""));
        assertThat(m.getPattern(), is(""));
        assertThat(m.getServletName(), is("Something"));
        assertThat(m.getServletPath(), is(spec.getPathMatch(uri)));
        assertThat(m.getPathInfo(), is(spec.getPathInfo(uri)));

        spec = new ServletPathSpec("/");
        uri = "/some/path";
        m = new ServletPathMapping(spec, "Default", uri);
        assertThat(m.getMappingMatch(), is(MappingMatch.DEFAULT));
        assertThat(m.getMatchValue(), is(""));
        assertThat(m.getPattern(), is("/"));
        assertThat(m.getServletName(), is("Default"));
        assertThat(m.getServletPath(), is(spec.getPathMatch(uri)));
        assertThat(m.getPathInfo(), is(spec.getPathInfo(uri)));

        spec = new ServletPathSpec("/foo/*");
        uri = "/foo/bar";
        m = new ServletPathMapping(spec, "FooServlet", uri);
        assertThat(m.getMappingMatch(), is(MappingMatch.PATH));
        assertThat(m.getMatchValue(), is("foo"));
        assertThat(m.getPattern(), is("/foo/*"));
        assertThat(m.getServletName(), is("FooServlet"));
        assertThat(m.getServletPath(), is(spec.getPathMatch(uri)));
        assertThat(m.getPathInfo(), is(spec.getPathInfo(uri)));

        uri = "/foo/";
        m = new ServletPathMapping(spec, "FooServlet", uri);
        assertThat(m.getMappingMatch(), is(MappingMatch.PATH));
        assertThat(m.getMatchValue(), is("foo"));
        assertThat(m.getPattern(), is("/foo/*"));
        assertThat(m.getServletName(), is("FooServlet"));
        assertThat(m.getServletPath(), is(spec.getPathMatch(uri)));
        assertThat(m.getPathInfo(), is(spec.getPathInfo(uri)));

        uri = "/foo";
        m = new ServletPathMapping(spec, "FooServlet", uri);
        assertThat(m.getMappingMatch(), is(MappingMatch.PATH));
        assertThat(m.getMatchValue(), is("foo"));
        assertThat(m.getPattern(), is("/foo/*"));
        assertThat(m.getServletName(), is("FooServlet"));
        assertThat(m.getServletPath(), is(spec.getPathMatch(uri)));
        assertThat(m.getPathInfo(), is(spec.getPathInfo(uri)));

        spec = new ServletPathSpec("*.jsp");
        uri = "/foo/bar.jsp";
        m = new ServletPathMapping(spec, "JspServlet", uri);
        assertThat(m.getMappingMatch(), is(MappingMatch.EXTENSION));
        assertThat(m.getMatchValue(), is("foo/bar"));
        assertThat(m.getPattern(), is("*.jsp"));
        assertThat(m.getServletName(), is("JspServlet"));
        assertThat(m.getServletPath(), is(spec.getPathMatch(uri)));
        assertThat(m.getPathInfo(), is(spec.getPathInfo(uri)));

        spec = new ServletPathSpec("/catalog");
        uri = "/catalog";
        m = new ServletPathMapping(spec, "CatalogServlet", uri);
        assertThat(m.getMappingMatch(), is(MappingMatch.EXACT));
        assertThat(m.getMatchValue(), is("catalog"));
        assertThat(m.getPattern(), is("/catalog"));
        assertThat(m.getServletName(), is("CatalogServlet"));
        assertThat(m.getServletPath(), is(spec.getPathMatch(uri)));
        assertThat(m.getPathInfo(), is(spec.getPathInfo(uri)));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 11
Lines of Code		36
-    
    public void testLocaleAndContentTypeEncoding() throws Exception
    {
        _server.stop();
        MimeTypes.getInferredEncodings().put("text/html", "iso-8859-1");
        ContextHandler handler = new ContextHandler();
        handler.addLocaleEncoding("ja", "euc-jp");
        handler.addLocaleEncoding("zh_CN", "gb18030");
        _server.setHandler(handler);
        handler.setHandler(new DumpHandler());
        _server.start();

        Response response = getResponse();
        response.getHttpChannel().getRequest().setContext(handler.getServletContext(), "/");

        response.setContentType("text/html");
        assertEquals("iso-8859-1", response.getCharacterEncoding());

        // setLocale should change character encoding based on
        // locale-encoding-mapping-list
        response.setLocale(Locale.JAPAN);
        assertEquals("euc-jp", response.getCharacterEncoding());

        // setLocale should change character encoding based on
        // locale-encoding-mapping-list
        response.setLocale(Locale.CHINA);
        assertEquals("gb18030", response.getCharacterEncoding());

        // setContentType here doesn't define character encoding
        response.setContentType("text/html");
        assertEquals("gb18030", response.getCharacterEncoding());

        // setCharacterEncoding should still be able to change encoding
        response.setCharacterEncoding("utf-8");
        assertEquals("utf-8", response.getCharacterEncoding());

        // setLocale should not override explicit character encoding request
        response.setLocale(Locale.JAPAN);
        assertEquals("utf-8", response.getCharacterEncoding());

        // setContentType should still be able to change encoding
        response.setContentType("text/html;charset=gb18030");
        assertEquals("gb18030", response.getCharacterEncoding());

        // setCharacterEncoding should still be able to change encoding
        response.setCharacterEncoding("utf-8");
        assertEquals("utf-8", response.getCharacterEncoding());

        // getWriter should freeze the character encoding
        PrintWriter pw = response.getWriter();
        assertEquals("utf-8", response.getCharacterEncoding());

        // setCharacterEncoding should no longer be able to change the encoding
        response.setCharacterEncoding("iso-8859-1");
        assertEquals("utf-8", response.getCharacterEncoding());

        // setLocale should not override explicit character encoding request
        response.setLocale(Locale.JAPAN);
        assertEquals("utf-8", response.getCharacterEncoding());
    }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		9
-    
    public void testContentEncodingViaContentTypeChange() throws Exception
    {
        Response response = getResponse();
        response.setContentType("text/html;charset=Shift_Jis");
        assertEquals("Shift_Jis", response.getCharacterEncoding());

        response.setContentType("text/xml");
        assertEquals("Shift_Jis", response.getCharacterEncoding());@@@     }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		51
-    
    public void testSessionListenerOrdering()
        throws Exception
    {
        final StringBuffer result = new StringBuffer();

        class Listener1 implements HttpSessionListener
        {

            @Override
            public void sessionCreated(HttpSessionEvent se)
            {
                result.append("Listener1 create;");
            }

            @Override
            public void sessionDestroyed(HttpSessionEvent se)
            {
                result.append("Listener1 destroy;");
            }
        }

        class Listener2 implements HttpSessionListener
        {

            @Override
            public void sessionCreated(HttpSessionEvent se)
            {
                result.append("Listener2 create;");
            }

            @Override
            public void sessionDestroyed(HttpSessionEvent se)
            {
                result.append("Listener2 destroy;");
            }

        }

        Server server = new Server();
        SessionHandler sessionHandler = new SessionHandler();
        try
        {
            sessionHandler.addEventListener(new Listener1());
            sessionHandler.addEventListener(new Listener2());
            sessionHandler.setServer(server);
            sessionHandler.start();
            Session session = new Session(sessionHandler, new SessionData("aa", "_", "0.0", 0, 0, 0, 0));
            sessionHandler.callSessionCreatedListeners(session);
            sessionHandler.callSessionDestroyedListeners(session);
            assertEquals("Listener1 create;Listener2 create;Listener2 destroy;Listener1 destroy;", result.toString());
        }
        finally
        {
            sessionHandler.stop();
        }@@@+        assertThrows(IllegalArgumentException.class,() ->@@@+            sessionHandler.setSessionTrackingModes(new HashSet<>(Arrays.asList(SessionTrackingMode.SSL, SessionTrackingMode.URL))));@@@     }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		16
-    
    public void testNamedInclude() throws Exception
    {
        _contextHandler.addServlet(NamedIncludeServlet.class, "/include/*");
        String echo = _contextHandler.addServlet(EchoURIServlet.class, "/echo/*").getName();

        String expected =
            "HTTP/1.1 200 OK\r\n" +
                "Content-Length: 62\r\n" +
                "\r\n" +
                "/context\r\n" +
                "/include\r\n" +
                "/info\r\n" +
                "/context/include/info;param=value\r\n";
        String responses = _connector.getResponse("GET /context/include/info;param=value?name=" + echo + " HTTP/1.0\n\n");
        assertEquals(expected, responses);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		8
-    
    public void testDispatchMapping() throws Exception
    {
        _contextHandler.addServlet(new ServletHolder("TestServlet", MappingServlet.class), "/TestServlet");
        _contextHandler.addServlet(new ServletHolder("DispatchServlet", AsyncDispatch2TestServlet.class), "/DispatchServlet");

        String response = _connector.getResponse("GET /context/DispatchServlet HTTP/1.0\n\n");
        assertThat(response, containsString("matchValue=TestServlet, pattern=/TestServlet, servletName=TestServlet, mappingMatch=EXACT"));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 9
Lines of Code		25
-    
    public void testErrorOverridesMimeTypeAndCharset() throws Exception
    {
        StringBuilder rawRequest = new StringBuilder();
        rawRequest.append("GET /error-mime-charset-writer/ HTTP/1.1\r\n");
        rawRequest.append("Host: test\r\n");
        rawRequest.append("Connection: close\r\n");
        rawRequest.append("Accept: */*\r\n");
        rawRequest.append("Accept-Charset: *\r\n");
        rawRequest.append("\r\n");

        String rawResponse = _connector.getResponse(rawRequest.toString());
        System.out.println(rawResponse);
        HttpTester.Response response = HttpTester.parseResponse(rawResponse);

        assertThat(response.getStatus(), is(595));
        String actualContentType = response.get(HttpHeader.CONTENT_TYPE);
        // should not expect to see charset line from servlet
        assertThat(actualContentType, not(containsString("charset=US-ASCII")));
        String body = response.getContent();

        assertThat(body, containsString("ERROR_PAGE: /595"));
        assertThat(body, containsString("ERROR_MESSAGE: 595"));
        assertThat(body, containsString("ERROR_CODE: 595"));
        assertThat(body, containsString("ERROR_EXCEPTION: null"));
        assertThat(body, containsString("ERROR_EXCEPTION_TYPE: null"));
        assertThat(body, containsString("ERROR_SERVLET: org.eclipse.jetty.servlet.ErrorPageTest$ErrorContentTypeCharsetWriterInitializedServlet-"));
        assertThat(body, containsString("ERROR_REQUEST_URI: /error-mime-charset-writer/"));@@@     }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		20
-    
    public void testCreateInstance() throws Exception
    {
        try (StacklessLogging ignore = new StacklessLogging(ServletHandler.class, ServletContextHandler.class))
        {
            //test without a ServletContextHandler or current ContextHandler
            FilterHolder holder = new FilterHolder();
            holder.setName("foo");
            holder.setHeldClass(DummyFilter.class);
            Filter filter = holder.createInstance();
            assertNotNull(filter);

            //test with a ServletContextHandler
            Server server = new Server();
            ServletContextHandler context = new ServletContextHandler();
            server.setHandler(context);
            ServletHandler handler = context.getServletHandler();
            handler.addFilter(holder);
            holder.setServletHandler(handler);
            context.start();
            assertNotNull(holder.getFilter());
        }@@@     }
```
```
Cyclomatic Complexity	 2
Assertions		 2
Lines of Code		14
-    
    public void testCharsetResetToJsonMimeType() throws Exception
    {
        HttpTester.Request request = new HttpTester.Request();
        request.setMethod("GET");
        request.setURI("/charset/json-reset/");
        request.setVersion(HttpVersion.HTTP_1_1);
        request.setHeader("Connection", "close");
        request.setHeader("Host", "test");

        ByteBuffer responseBuffer = connector.getResponse(request.generate());
        // System.err.println(BufferUtil.toUTF8String(responseBuffer));
        HttpTester.Response response = HttpTester.parseResponse(responseBuffer);

        // Now test for properly formatted HTTP Response Headers.
        assertThat("Response Code", response.getStatus(), is(200));
        // The Content-Type should not have a charset= portion
        assertThat("Response Header Content-Type", response.get("Content-Type"), is("application/json"));
    }
```
```
Cyclomatic Complexity	 2
Assertions		 2
Lines of Code		14
-    
    public void testCharsetChangeToJsonMimeType() throws Exception
    {
        HttpTester.Request request = new HttpTester.Request();
        request.setMethod("GET");
        request.setURI("/charset/json-change/");
        request.setVersion(HttpVersion.HTTP_1_1);
        request.setHeader("Connection", "close");
        request.setHeader("Host", "test");

        ByteBuffer responseBuffer = connector.getResponse(request.generate());
        // System.err.println(BufferUtil.toUTF8String(responseBuffer));
        HttpTester.Response response = HttpTester.parseResponse(responseBuffer);

        // Now test for properly formatted HTTP Response Headers.
        assertThat("Response Code", response.getStatus(), is(200));
        // The Content-Type should not have a charset= portion
        assertThat("Response Header Content-Type", response.get("Content-Type"), is("application/json"));
    }
```
```
Cyclomatic Complexity	 2
Assertions		 2
Lines of Code		14
-    
    public void testCharsetChangeToJsonMimeTypeSetCharsetToNull() throws Exception
    {
        HttpTester.Request request = new HttpTester.Request();
        request.setMethod("GET");
        request.setURI("/charset/json-change-null/");
        request.setVersion(HttpVersion.HTTP_1_1);
        request.setHeader("Connection", "close");
        request.setHeader("Host", "test");

        ByteBuffer responseBuffer = connector.getResponse(request.generate());
        // System.err.println(BufferUtil.toUTF8String(responseBuffer));
        HttpTester.Response response = HttpTester.parseResponse(responseBuffer);

        // Now test for properly formatted HTTP Response Headers.
        assertThat("Response Code", response.getStatus(), is(200));
        // The Content-Type should not have a charset= portion
        assertThat("Response Header Content-Type", response.get("Content-Type"), is("application/json"));@@@     }
```
```
Cyclomatic Complexity	 1
Assertions		 6
Lines of Code		18
-    
    public void testGetSetSessionTimeout() throws Exception
    {
        ContextHandlerCollection contexts = new ContextHandlerCollection();
        _server.setHandler(contexts);

        int startMin = 7;
        Integer timeout = Integer.valueOf(100);
        ServletContextHandler root = new ServletContextHandler(contexts, "/", ServletContextHandler.SESSIONS);
        root.getSessionHandler().setMaxInactiveInterval((int)TimeUnit.MINUTES.toSeconds(startMin));
        root.addBean(new MySCIStarter(root.getServletContext(), new MySCI(true, timeout.intValue())), true);
        _server.start();

        //test starting value of setSessionTimeout
        assertEquals(startMin, (Integer)root.getServletContext().getAttribute("MYSCI.startSessionTimeout"));
        //test can set session timeout from ServletContainerInitializer
        assertTrue((Boolean)root.getServletContext().getAttribute("MYSCI.setSessionTimeout"));
        //test can get session timeout from ServletContainerInitializer
        assertEquals(timeout, (Integer)root.getServletContext().getAttribute("MYSCI.getSessionTimeout"));
        assertNull(root.getAttribute("MYSCI.sessionTimeoutFailure"));
        //test can't get session timeout from ContextListener that is not from annotation or web.xml
        assertTrue((Boolean)root.getServletContext().getAttribute("MyContextListener.getSessionTimeout"));
        //test can't set session timeout from ContextListener that is not from annotation or web.xml
        assertTrue((Boolean)root.getServletContext().getAttribute("MyContextListener.setSessionTimeout"));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 3
Lines of Code		20
-    
    public void testCreateMethodsFromSCL() throws Exception
    {
      //A filter can be created by an SCI
        ContextHandlerCollection contexts = new ContextHandlerCollection();
        _server.setHandler(contexts);

        ServletContextHandler root = new ServletContextHandler(contexts, "/");
        class ListenerCreatingSCI implements ServletContainerInitializer
        {
            @Override
            public void onStartup(Set<Class<?>> c, ServletContext ctx) throws ServletException
            {
                ctx.addListener(new CreatingSCL());
            }
        }

        root.addBean(new MySCIStarter(root.getServletContext(), new ListenerCreatingSCI()), true);
        _server.start();
        assertTrue((Boolean)root.getServletContext().getAttribute("CreatingSCL.filter"));
        assertTrue((Boolean)root.getServletContext().getAttribute("CreatingSCL.servlet"));
        assertTrue((Boolean)root.getServletContext().getAttribute("CreatingSCL.listener"));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 4
Lines of Code		15
-    
    public void testProvidersUsingDefault() throws Exception
    {
        Path homePath = MavenTestingUtils.getTestResourceDir("providers-home").toPath().toRealPath();

        List<String> cmdLineArgs = new ArrayList<>();
        cmdLineArgs.add("user.dir=" + homePath);
        cmdLineArgs.add("jetty.home=" + homePath);
        cmdLineArgs.add("--module=server");

        Main main = new Main();
        StartArgs args = main.processCommandLine(cmdLineArgs.toArray(new String[cmdLineArgs.size()]));
        BaseHome baseHome = main.getBaseHome();

        assertThat("jetty.home", baseHome.getHome(), is(homePath.toString()));
        assertThat("jetty.base", baseHome.getBase(), is(homePath.toString()));

        ConfigurationAssert.assertConfiguration(baseHome, args, "assert-providers-default.txt");
    }
```
```
Cyclomatic Complexity	 1
Assertions		 4
Lines of Code		16
-    
    public void testProvidersUsingSpecific() throws Exception
    {
        Path homePath = MavenTestingUtils.getTestResourceDir("providers-home").toPath().toRealPath();

        List<String> cmdLineArgs = new ArrayList<>();
        cmdLineArgs.add("user.dir=" + homePath);
        cmdLineArgs.add("jetty.home=" + homePath);
        cmdLineArgs.add("--module=server");
        cmdLineArgs.add("--module=logging-b");

        Main main = new Main();
        StartArgs args = main.processCommandLine(cmdLineArgs.toArray(new String[cmdLineArgs.size()]));
        BaseHome baseHome = main.getBaseHome();

        assertThat("jetty.home", baseHome.getHome(), is(homePath.toString()));
        assertThat("jetty.base", baseHome.getBase(), is(homePath.toString()));

        ConfigurationAssert.assertConfiguration(baseHome, args, "assert-providers-specific.txt");@@@     }
```
```
Cyclomatic Complexity	 1
Assertions		 9
Lines of Code		23
-    
    public void testDownloadSnapshotRepo()
        throws Exception
    {
        Path snapshotLocalRepoDir = testdir.getPath().resolve("snapshot-repo");
        FS.ensureEmpty(snapshotLocalRepoDir);

        MavenLocalRepoFileInitializer repo =
            new MavenLocalRepoFileInitializer(baseHome, snapshotLocalRepoDir, false, "https://oss.sonatype.org/content/repositories/jetty-snapshots/");
        String ref = "maven://org.eclipse.jetty/jetty-rewrite/11.0.0-SNAPSHOT/jar";
        Coordinates coords = repo.getCoordinates(URI.create(ref));
        assertThat("Coordinates", coords, notNullValue());

        assertThat("coords.groupId", coords.groupId, is("org.eclipse.jetty"));
        assertThat("coords.artifactId", coords.artifactId, is("jetty-rewrite"));
        assertThat("coords.version", coords.version, is("11.0.0-SNAPSHOT"));
        assertThat("coords.type", coords.type, is("jar"));
        assertThat("coords.classifier", coords.classifier, is(nullValue()));

        assertThat("coords.toCentralURI", coords.toCentralURI().toASCIIString(),
            is("https://oss.sonatype.org/content/repositories/jetty-snapshots/org/eclipse/jetty/jetty-rewrite/11.0.0-SNAPSHOT/jetty-rewrite-11.0.0-SNAPSHOT.jar"));

        Path destination = baseHome.getBasePath().resolve("jetty-rewrite-11.0.0-SNAPSHOT.jar");
        repo.download(coords, destination);
        assertThat(Files.exists(destination), is(true));
        assertThat("Snapshot File size", destination.toFile().length(), greaterThan(10_000L));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		14
-    
    public void testDownloadSnapshotRepoWithExtractDeep()
        throws Exception
    {
        Path snapshotLocalRepoDir = testdir.getPath().resolve("snapshot-repo");
        FS.ensureEmpty(snapshotLocalRepoDir);

        MavenLocalRepoFileInitializer repo =
            new MavenLocalRepoFileInitializer(baseHome, snapshotLocalRepoDir, false,
                "https://oss.sonatype.org/content/repositories/jetty-snapshots/");
        String ref = "maven://org.eclipse.jetty/test-jetty-webapp/11.0.0-SNAPSHOT/jar/config";
        Path baseDir = baseHome.getBasePath();
        repo.create(URI.create(ref), "extract:company/");

        assertThat(Files.exists(baseDir.resolve("company/webapps/test.d/override-web.xml")), is(true));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 4
Lines of Code		10
-    
    public void testCopyIndirect()
    {
        ByteBuffer b = BufferUtil.toBuffer("Hello World");
        ByteBuffer c = BufferUtil.copy(b);
        assertEquals("Hello World", BufferUtil.toString(c));
        assertFalse(c.isDirect());
        assertThat(b, not(sameInstance(c)));
        assertThat(b.array(), not(sameInstance(c.array())));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 3
Lines of Code		10
-    
    public void testCopyDirect()
    {
        ByteBuffer b = BufferUtil.allocateDirect(11);
        BufferUtil.append(b, "Hello World");
        ByteBuffer c = BufferUtil.copy(b);
        assertEquals("Hello World", BufferUtil.toString(c));
        assertTrue(c.isDirect());
        assertThat(b, not(sameInstance(c)));@@@     }
```
```
Cyclomatic Complexity	 1
Assertions		 24
Lines of Code		28
-    
    public void testRequiredCapacity()
    {
        assertThat(requiredCapacity(Set.of("ABC", "abc"), true), is(1 + 6));
        assertThat(requiredCapacity(Set.of("ABC", "abc"), false), is(1 + 3));
        assertThat(requiredCapacity(Set.of(""), false), is(1 + 0));
        assertThat(requiredCapacity(Set.of("ABC", ""), false), is(1 + 3));
        assertThat(requiredCapacity(Set.of("ABC"), false), is(1 + 3));
        assertThat(requiredCapacity(Set.of("ABC", "XYZ"), false), is(1 + 6));
        assertThat(requiredCapacity(Set.of("A00", "A11"), false), is(1 + 5));
        assertThat(requiredCapacity(Set.of("A00", "A01", "A10", "A11"), false), is(1 + 7));
        assertThat(requiredCapacity(Set.of("A", "AB"), false), is(1 + 2));
        assertThat(requiredCapacity(Set.of("A", "ABC"), false), is(1 + 3));
        assertThat(requiredCapacity(Set.of("A", "ABCD"), false), is(1 + 4));
        assertThat(requiredCapacity(Set.of("AB", "ABC"), false), is(1 + 3));
        assertThat(requiredCapacity(Set.of("ABC", "ABCD"), false), is(1 + 4));
        assertThat(requiredCapacity(Set.of("ABC", "ABCDEF"), false), is(1 + 6));
        assertThat(requiredCapacity(Set.of("AB", "A"), false), is(1 + 2));
        assertThat(requiredCapacity(Set.of("ABC", "ABCDEF"), false), is(1 + 6));
        assertThat(requiredCapacity(Set.of("ABCDEF", "ABC"), false), is(1 + 6));
        assertThat(requiredCapacity(Set.of("ABC", "ABCDEF", "ABX"), false), is(1 + 7));
        assertThat(requiredCapacity(Set.of("ABCDEF", "ABC", "ABX"), false), is(1 + 7));
        assertThat(requiredCapacity(Set.of("ADEF", "AQPR4", "AQZ"), false), is(1 + 9));
        assertThat(requiredCapacity(Set.of("111", "ADEF", "AQPR4", "AQZ", "999"), false), is(1 + 15));
        assertThat(requiredCapacity(Set.of("utf-16", "utf-8"), false), is(1 + 7));
        assertThat(requiredCapacity(Set.of("utf-16", "utf-8", "utf16", "utf8"), false), is(1 + 10));
        assertThat(requiredCapacity(Set.of("utf-8", "utf8", "utf-16", "utf16", "iso-8859-1", "iso_8859_1"), false), is(1 + 27));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		8
-    
    public void testLargeRequiredCapacity()
    {
        String x = "x".repeat(Character.MAX_VALUE / 2);
        String y = "y".repeat(Character.MAX_VALUE / 2);
        String z = "z".repeat(Character.MAX_VALUE / 2);
        assertThat(requiredCapacity(Set.of(x, y, z), true), is(1 + 3 * (Character.MAX_VALUE / 2)));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 3
Lines of Code		9
-    
    public void testArrayTrieCapacity()
    {
        ArrayTrie<String> trie = new ArrayTrie<>(Character.MAX_VALUE);
        String huge = "x".repeat(Character.MAX_VALUE - 1);
        assertTrue(trie.put(huge, "wow"));
        assertThat(trie.get(huge), is("wow"));

        assertThrows(IllegalArgumentException.class, () -> new ArrayTrie<String>(Character.MAX_VALUE + 1));@@@     }
```
```
Cyclomatic Complexity	 1
Assertions		 3
Lines of Code		13
-    
    public void testJoinWithStopTimeout() throws Exception
    {
        final long stopTimeout = 100;
        QueuedThreadPool threadPool = new QueuedThreadPool();
        threadPool.setStopTimeout(100);
        threadPool.start();

        // Verify that join does not timeout after waiting twice the stopTimeout.
        assertThrows(Throwable.class, () ->
            assertTimeoutPreemptively(Duration.ofMillis(stopTimeout * 2), threadPool::join)
        );

        // After stopping the ThreadPool join should unblock.
        LifeCycle.stop(threadPool);
        assertTimeoutPreemptively(Duration.ofMillis(stopTimeout), threadPool::join);@@@     }
```
```
Cyclomatic Complexity	 2
Assertions		 2
Lines of Code		17
-    
    public void testFindAndFilterContainerPathsJDK9() throws Exception
    {
        MetaInfConfiguration config = new MetaInfConfiguration();
        WebAppContext context = new WebAppContext();
        context.setAttribute(MetaInfConfiguration.CONTAINER_JAR_PATTERN, ".*/jetty-util-[^/]*\\.jar$|.*/jetty-util/target/classes/$|.*/foo-bar-janb.jar");
        WebAppClassLoader loader = new WebAppClassLoader(context);
        context.setClassLoader(loader);
        config.findAndFilterContainerPaths(context);
        List<Resource> containerResources = context.getMetaData().getContainerResources();
        assertEquals(2, containerResources.size());
        for (Resource r : containerResources)
        {
            String s = r.toString();
            assertTrue(s.endsWith("foo-bar-janb.jar") || s.contains("jetty-util"));
        }@@@     }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		11
-    
    public void testSetWithNullProperty() throws Exception
    {
        XmlConfiguration configuration = asXmlConfiguration("<Configure class=\"org.eclipse.jetty.xml.TestConfiguration\"><Set name=\"TestString\" property=\"prop\" id=\"test\"/></Configure>");
        configuration.getProperties().remove("prop");
        TestConfiguration tc = new TestConfiguration();
        tc.setTestString("default");
        configuration.configure(tc);
        assertEquals("default", tc.getTestString());
        assertNull(configuration.getIdMap().get("test"));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		11
-    
    public void testSetWithPropertyAndValue() throws Exception
    {
        XmlConfiguration configuration = asXmlConfiguration("<Configure class=\"org.eclipse.jetty.xml.TestConfiguration\"><Set name=\"TestString\" property=\"prop\" id=\"test\">Value</Set></Configure>");
        configuration.getProperties().put("prop", "This is a property value");
        TestConfiguration tc = new TestConfiguration();
        tc.setTestString("default");
        configuration.configure(tc);
        assertEquals("Value", tc.getTestString());
        assertEquals(configuration.getIdMap().get("test"), "Value");
    }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		11
-    
    public void testSetWithNullPropertyAndValue() throws Exception
    {
        XmlConfiguration configuration = asXmlConfiguration("<Configure class=\"org.eclipse.jetty.xml.TestConfiguration\"><Set name=\"TestString\" property=\"prop\" id=\"test\">Value</Set></Configure>");
        configuration.getProperties().remove("prop");
        TestConfiguration tc = new TestConfiguration();
        tc.setTestString("default");
        configuration.configure(tc);
        assertEquals("default", tc.getTestString());
        assertNull(configuration.getIdMap().get("test"));
    }
```
```
Cyclomatic Complexity	 10
Assertions		 17
Lines of Code		67
-    
    public void testCleanOrphans() throws Exception
    {
        //create the SessionDataStore
        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/test");
        SessionDataStoreFactory factory = createSessionDataStoreFactory();
        ((AbstractSessionDataStoreFactory)factory).setGracePeriodSec(GRACE_PERIOD_SEC);
        SessionDataStore store = factory.getSessionDataStore(context.getSessionHandler());
        SessionContext sessionContext = new SessionContext("foo", context.getServletContext());
        store.initialize(sessionContext);

        long now = System.currentTimeMillis();
        
        //persist a long ago expired session for our context
        SessionData oldSession = store.newSessionData("001", 100, 101, 100, TimeUnit.MINUTES.toMillis(60));
        oldSession.setExpiry(200);
        oldSession.setLastNode("me");
        persistSession(oldSession);
        assertTrue(checkSessionExists(oldSession));
        
        //persist a recently expired session for our context
        SessionData expiredSession = store.newSessionData("002", 100, 101, 100, TimeUnit.MINUTES.toMillis(60));
        expiredSession.setExpiry(RECENT_TIMESTAMP);
        expiredSession.setLastNode("me");
        persistSession(expiredSession);
        assertTrue(checkSessionExists(expiredSession));

        //persist a non expired session for our context
        SessionData unexpiredSession = store.newSessionData("003", 100, now + 10, now + 5, TimeUnit.MINUTES.toMillis(60));
        unexpiredSession.setExpiry(now + TimeUnit.MINUTES.toMillis(10));
        unexpiredSession.setLastNode("me");
        persistSession(unexpiredSession);
        assertTrue(checkSessionExists(unexpiredSession));

        //persist an immortal session for our context
        SessionData immortalSession = store.newSessionData("004", 100, now + 10, now + 5, TimeUnit.MINUTES.toMillis(60));
        immortalSession.setExpiry(0);
        immortalSession.setLastNode("me");
        persistSession(immortalSession);
        assertTrue(checkSessionExists(immortalSession));
        
        //create sessions for a different context
        //persist a long ago expired session for a different context
        SessionData oldForeignSession = store.newSessionData("005", 100, 101, 100, TimeUnit.MINUTES.toMillis(60));
        oldForeignSession.setContextPath("_other");
        oldForeignSession.setExpiry(200);
        oldForeignSession.setLastNode("me");
        persistSession(oldForeignSession);
        assertTrue(checkSessionExists(oldForeignSession));
        
        //persist a recently expired session for our context
        SessionData expiredForeignSession = store.newSessionData("006", 100, 101, 100, TimeUnit.MINUTES.toMillis(60));
        expiredForeignSession.setContextPath("_other");
        expiredForeignSession.setExpiry(RECENT_TIMESTAMP);
        expiredForeignSession.setLastNode("me");
        persistSession(expiredForeignSession);
        assertTrue(checkSessionExists(expiredForeignSession));
        
        //persist a non expired session for our context
        SessionData unexpiredForeignSession = store.newSessionData("007", 100, now + 10, now + 5, TimeUnit.MINUTES.toMillis(60));
        unexpiredForeignSession.setContextPath("_other");
        unexpiredForeignSession.setExpiry(now + TimeUnit.MINUTES.toMillis(10));
        unexpiredForeignSession.setLastNode("me");
        persistSession(unexpiredForeignSession);
        assertTrue(checkSessionExists(unexpiredForeignSession));
        
        //persist an immortal session for our context
        SessionData immortalForeignSession = store.newSessionData("008", 100, now + 10, now + 5, TimeUnit.MINUTES.toMillis(60));
        immortalForeignSession.setContextPath("_other");
        immortalForeignSession.setExpiry(0);
        immortalForeignSession.setLastNode("me");
        persistSession(immortalForeignSession);
        assertTrue(checkSessionExists(immortalForeignSession));
        
        store.start();
        
        ((AbstractSessionDataStore)store).cleanOrphans(now - TimeUnit.SECONDS.toMillis(10 * GRACE_PERIOD_SEC));

        //old session should be gone
        assertFalse(checkSessionExists(oldSession));
        //recently expired session should still be there
        assertTrue(checkSessionExists(expiredSession));
        //unexpired session should still be there
        assertTrue(checkSessionExists(unexpiredSession));
        //immortal session should still exist
        assertTrue(checkSessionExists(immortalSession));
        //old foreign session should be gone
        assertFalse(checkSessionExists(oldSession));
        //recently expired foreign session should still be there
        assertTrue(checkSessionExists(expiredSession));
        //unexpired foreign session should still be there
        assertTrue(checkSessionExists(unexpiredSession));
        //immortal foreign session should still exist
        assertTrue(checkSessionExists(immortalSession));@@@+        assertThat(expiredIds, containsInAnyOrder("1234"));@@@     }
```
## 2adea58037b6393bebd4fb8e68d51f63be4b00b7 ##
```
Cyclomatic Complexity	1
Assertions		5
Lines of Code		17
-    
    public void testMutable()
    {
        HttpFields headers = HttpFields.build()
            .add(HttpHeader.ETAG, "tag")
            .add("name0", "value0")
            .add("name1", "value1").asImmutable();

        headers = HttpFields.build(headers, EnumSet.of(HttpHeader.ETAG, HttpHeader.CONTENT_RANGE))
            .add(new PreEncodedHttpField(HttpHeader.CONNECTION, HttpHeaderValue.CLOSE.asString()))
            .addDateField("name2", System.currentTimeMillis()).asImmutable();

        headers = HttpFields.build(headers, new HttpField(HttpHeader.CONNECTION, "open"));

        assertThat(headers.size(), is(4));
        assertThat(headers.getField(0).getValue(), is("value0"));
        assertThat(headers.getField(1).getValue(), is("value1"));
        assertThat(headers.getField(2).getValue(), is("open"));
        assertThat(headers.getField(3).getName(), is("name2"));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 4
Lines of Code		11
-    
    public void testMap()
    {
        Map<HttpFields.Immutable, String> map = new HashMap<>();
        map.put(HttpFields.build().add("X", "1").add(HttpHeader.ETAG, "tag").asImmutable(), "1");
        map.put(HttpFields.build().add("X", "2").add(HttpHeader.ETAG, "other").asImmutable(), "2");

        assertThat(map.get(HttpFields.build().add("X", "1").add(HttpHeader.ETAG, "tag").asImmutable()), is("1"));
        assertThat(map.get(HttpFields.build().add("X", "2").add(HttpHeader.ETAG, "other").asImmutable()), is("2"));
        assertThat(map.get(HttpFields.build().add("X", "2").asImmutable()), nullValue());
        assertThat(map.get(HttpFields.build().add("X", "2").add(HttpHeader.ETAG, "tag").asImmutable()), nullValue());
    }
```
```
Cyclomatic Complexity	 3
Assertions		 35
Lines of Code		63
-    
    public void testHttpHeaderValueParseCsv()
    {
        final List<HttpHeaderValue> list = new ArrayList<>();
        final List<String> unknowns = new ArrayList<>();

        assertTrue(HttpHeaderValue.parseCsvIndex("", list::add, unknowns::add));
        assertThat(list, empty());
        assertThat(unknowns, empty());

        assertTrue(HttpHeaderValue.parseCsvIndex(" ", list::add, unknowns::add));
        assertThat(list, empty());
        assertThat(unknowns, empty());

        assertTrue(HttpHeaderValue.parseCsvIndex(",", list::add, unknowns::add));
        assertThat(list, empty());
        assertThat(unknowns, empty());

        assertTrue(HttpHeaderValue.parseCsvIndex(",,", list::add, unknowns::add));
        assertThat(list, empty());
        assertThat(unknowns, empty());

        assertTrue(HttpHeaderValue.parseCsvIndex(" , , ", list::add, unknowns::add));
        assertThat(list, empty());
        assertThat(unknowns, empty());

        list.clear();
        assertTrue(HttpHeaderValue.parseCsvIndex("close", list::add));
        assertThat(list, contains(HttpHeaderValue.CLOSE));

        list.clear();
        assertTrue(HttpHeaderValue.parseCsvIndex(" close ", list::add));
        assertThat(list, contains(HttpHeaderValue.CLOSE));

        list.clear();
        assertTrue(HttpHeaderValue.parseCsvIndex(",close,", list::add));
        assertThat(list, contains(HttpHeaderValue.CLOSE));

        list.clear();
        assertTrue(HttpHeaderValue.parseCsvIndex(" , close , ", list::add));
        assertThat(list, contains(HttpHeaderValue.CLOSE));

        list.clear();
        assertTrue(HttpHeaderValue.parseCsvIndex(" close,GZIP, chunked    , Keep-Alive   ", list::add));
        assertThat(list, contains(HttpHeaderValue.CLOSE, HttpHeaderValue.GZIP, HttpHeaderValue.CHUNKED, HttpHeaderValue.KEEP_ALIVE));

        list.clear();
        assertTrue(HttpHeaderValue.parseCsvIndex(" close,GZIP, chunked    , Keep-Alive   ", t ->
        {
            if (t.toString().startsWith("c"))
                list.add(t);
            return true;
        }));
        assertThat(list, contains(HttpHeaderValue.CLOSE, HttpHeaderValue.CHUNKED));

        list.clear();
        assertFalse(HttpHeaderValue.parseCsvIndex(" close,GZIP, chunked    , Keep-Alive   ", t ->
        {
            if (HttpHeaderValue.CHUNKED == t)
                return false;
            list.add(t);
            return true;
        }));
        assertThat(list, contains(HttpHeaderValue.CLOSE, HttpHeaderValue.GZIP));

        list.clear();
        unknowns.clear();
        assertTrue(HttpHeaderValue.parseCsvIndex("closed,close, unknown , bytes", list::add, unknowns::add));
        assertThat(list, contains(HttpHeaderValue.CLOSE, HttpHeaderValue.BYTES));
        assertThat(unknowns, contains("closed", "unknown"));

        list.clear();
        unknowns.clear();
        assertFalse(HttpHeaderValue.parseCsvIndex("close, unknown , bytes", list::add, s -> false));
        assertThat(list, contains(HttpHeaderValue.CLOSE));
        assertThat(unknowns, empty());@@@     }
```
```
Cyclomatic Complexity	 1
Assertions		 10
Lines of Code		15
-    
    public void testExample() throws Exception
    {
        HttpURI uri = HttpURI.from("http://user:password@host:8888/ignored/../p%61th;ignored/info;param?query=value#fragment");

        assertThat(uri.getScheme(), is("http"));
        assertThat(uri.getUser(), is("user:password"));
        assertThat(uri.getHost(), is("host"));
        assertThat(uri.getPort(), is(8888));
        assertThat(uri.getPath(), is("/ignored/../p%61th;ignored/info;param"));
        assertThat(uri.getDecodedPath(), is("/path/info"));
        assertThat(uri.getParam(), is("param"));
        assertThat(uri.getQuery(), is("query=value"));
        assertThat(uri.getFragment(), is("fragment"));
        assertThat(uri.getAuthority(), is("host:8888"));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 6
Lines of Code		10
-    
    public void testJoin()
    {
        assertThat(QuotedCSV.join((String)null), nullValue());
        assertThat(QuotedCSV.join(Collections.emptyList()), is(emptyString()));
        assertThat(QuotedCSV.join(Collections.singletonList("hi")), is("hi"));
        assertThat(QuotedCSV.join("hi", "ho"), is("hi, ho"));
        assertThat(QuotedCSV.join("h i", "h,o"), is("\"h i\", \"h,o\""));
        assertThat(QuotedCSV.join("h\"i", "h\to"), is("\"h\\\"i\", \"h\\to\""));@@@     }
```
```
Cyclomatic Complexity	 4
Assertions		 13
Lines of Code		102
-    
    public void testGracefulServerGoAway() throws Exception
    {
        AtomicReference<Session> serverSessionRef = new AtomicReference<>();
        CountDownLatch serverSessionLatch = new CountDownLatch(1);
        CountDownLatch dataLatch = new CountDownLatch(2);
        start(new ServerSessionListener.Adapter()
        {
            @Override
            public void onAccept(Session session)
            {
                serverSessionRef.set(session);
                serverSessionLatch.countDown();
            }

            @Override
            public Stream.Listener onNewStream(Stream stream, HeadersFrame frame)
            {
                return new Stream.Listener.Adapter()
                {
                    @Override
                    public void onData(Stream stream, DataFrame frame, Callback callback)
                    {
                        callback.succeeded();
                        dataLatch.countDown();
                        if (frame.isEndStream())
                        {
                            MetaData.Response response = new MetaData.Response(HttpVersion.HTTP_2, HttpStatus.OK_200, HttpFields.EMPTY);
                            stream.headers(new HeadersFrame(stream.getId(), response, null, true), Callback.NOOP);
                        }
                    }
                };
            }
        });
        // Avoid aggressive idle timeout to allow the test verifications.
        connector.setShutdownIdleTimeout(connector.getIdleTimeout());

        CountDownLatch clientGracefulGoAwayLatch = new CountDownLatch(1);
        CountDownLatch clientGoAwayLatch = new CountDownLatch(1);
        CountDownLatch clientCloseLatch = new CountDownLatch(1);
        Session clientSession = newClient(new Session.Listener.Adapter()
        {
            @Override
            public void onGoAway(Session session, GoAwayFrame frame)
            {
                if (frame.isGraceful())
                    clientGracefulGoAwayLatch.countDown();
                else
                    clientGoAwayLatch.countDown();
            }

            @Override
            public void onClose(Session session, GoAwayFrame frame)
            {
                clientCloseLatch.countDown();
            }
        });
        assertTrue(serverSessionLatch.await(5, TimeUnit.SECONDS));
        Session serverSession = serverSessionRef.get();

        // Start 2 requests without completing them yet.
        CountDownLatch responseLatch = new CountDownLatch(2);
        MetaData.Request metaData1 = newRequest("GET", HttpFields.EMPTY);
        HeadersFrame request1 = new HeadersFrame(metaData1, null, false);
        FuturePromise<Stream> promise1 = new FuturePromise<>();
        Stream.Listener.Adapter listener = new Stream.Listener.Adapter()
        {
            @Override
            public void onHeaders(Stream stream, HeadersFrame frame)
            {
                if (frame.isEndStream())
                {
                    MetaData.Response response = (MetaData.Response)frame.getMetaData();
                    assertEquals(HttpStatus.OK_200, response.getStatus());
                    responseLatch.countDown();
                }
            }
        };
        clientSession.newStream(request1, promise1, listener);
        Stream stream1 = promise1.get(5, TimeUnit.SECONDS);
        stream1.data(new DataFrame(stream1.getId(), ByteBuffer.allocate(1), false), Callback.NOOP);

        MetaData.Request metaData2 = newRequest("GET", HttpFields.EMPTY);
        HeadersFrame request2 = new HeadersFrame(metaData2, null, false);
        FuturePromise<Stream> promise2 = new FuturePromise<>();
        clientSession.newStream(request2, promise2, listener);
        Stream stream2 = promise2.get(5, TimeUnit.SECONDS);
        stream2.data(new DataFrame(stream2.getId(), ByteBuffer.allocate(1), false), Callback.NOOP);

        assertTrue(dataLatch.await(5, TimeUnit.SECONDS));

        // Both requests are now on the server, shutdown gracefully the server session.
        int port = connector.getLocalPort();
        CompletableFuture<Void> shutdown = Graceful.shutdown(server);

        // Client should receive the graceful GOAWAY.
        assertTrue(clientGracefulGoAwayLatch.await(5, TimeUnit.SECONDS));
        // Client should not receive the non-graceful GOAWAY.
        assertFalse(clientGoAwayLatch.await(500, TimeUnit.MILLISECONDS));
        // Client should not be closed yet.
        assertFalse(clientCloseLatch.await(500, TimeUnit.MILLISECONDS));

        // Client cannot create new requests after receiving a GOAWAY.
        HostPortHttpField authority3 = new HostPortHttpField("localhost" + ":" + port);
        MetaData.Request metaData3 = new MetaData.Request("GET", HttpScheme.HTTP.asString(), authority3, servletPath, HttpVersion.HTTP_2, HttpFields.EMPTY, -1);
        HeadersFrame request3 = new HeadersFrame(metaData3, null, true);
        FuturePromise<Stream> promise3 = new FuturePromise<>();
        clientSession.newStream(request3, promise3, new Stream.Listener.Adapter());
        assertThrows(ExecutionException.class, () -> promise3.get(5, TimeUnit.SECONDS));

        // Finish the previous requests and expect the responses.
        stream1.data(new DataFrame(stream1.getId(), BufferUtil.EMPTY_BUFFER, true), Callback.NOOP);
        stream2.data(new DataFrame(stream2.getId(), BufferUtil.EMPTY_BUFFER, true), Callback.NOOP);
        assertTrue(responseLatch.await(5, TimeUnit.SECONDS));
        assertNull(shutdown.get(5, TimeUnit.SECONDS));

        // Now GOAWAY should arrive to the client.
        assertTrue(clientGoAwayLatch.await(5, TimeUnit.SECONDS));
        assertTrue(clientCloseLatch.await(5, TimeUnit.SECONDS));

        assertFalse(((HTTP2Session)clientSession).getEndPoint().isOpen());
        assertFalse(((HTTP2Session)serverSession).getEndPoint().isOpen());@@@     }
```
```
Cyclomatic Complexity	 1
Assertions		 7
Lines of Code		33
-    
    public void testDefaultContextPath() throws Exception
    {
        File testDir = MavenTestingUtils.getTargetTestingDir("dfltcp");
        FS.ensureEmpty(testDir);
        File webInf = new File(testDir, "WEB-INF");
        FS.ensureDirExists(webInf);
        
        File quickstartXml = new File(webInf, "quickstart-web.xml");
        assertFalse(quickstartXml.exists());

        // generate a quickstart-web.xml
        WebAppContext quickstart = new WebAppContext();
        quickstart.setResourceBase(testDir.getAbsolutePath());
        quickstart.addConfiguration(new QuickStartConfiguration());
        quickstart.setAttribute(QuickStartConfiguration.MODE, QuickStartConfiguration.Mode.GENERATE);
        quickstart.setAttribute(QuickStartConfiguration.ORIGIN_ATTRIBUTE, "origin");
        quickstart.setDescriptor(MavenTestingUtils.getTestResourceFile("web.xml").getAbsolutePath());
        quickstart.setContextPath("/foo");
        server.setHandler(quickstart);
        server.setDryRun(true);
        server.start();
        assertEquals("/foo", quickstart.getContextPath());
        assertFalse(quickstart.isContextPathDefault());

        assertTrue(quickstartXml.exists());

        // quick start
        WebAppContext webapp = new WebAppContext();
        webapp.addConfiguration(new QuickStartConfiguration());
        quickstart.setAttribute(QuickStartConfiguration.MODE, QuickStartConfiguration.Mode.QUICKSTART);
        webapp.setResourceBase(testDir.getAbsolutePath());
        webapp.getServerClassMatcher().exclude("org.eclipse.jetty.quickstart.");
        server.setHandler(webapp);

        server.setDryRun(false);
        server.start();

        // verify the context path is the default-context-path
        assertEquals("/thisIsTheDefault", webapp.getContextPath());
        assertTrue(webapp.isContextPathDefault());
    }
```
```
Cyclomatic Complexity	 1
Assertions		 5
Lines of Code		33
-    
    public void testDefaultRequestAndResponseEncodings() throws Exception
    {
        File testDir = MavenTestingUtils.getTargetTestingDir("dfltenc");
        FS.ensureEmpty(testDir);
        File webInf = new File(testDir, "WEB-INF");
        FS.ensureDirExists(webInf);
        
        File quickstartXml = new File(webInf, "quickstart-web.xml");
        assertFalse(quickstartXml.exists());

        // generate a quickstart-web.xml
        WebAppContext quickstart = new WebAppContext();
        quickstart.setResourceBase(testDir.getAbsolutePath());
        quickstart.addConfiguration(new QuickStartConfiguration());
        quickstart.setAttribute(QuickStartConfiguration.MODE, QuickStartConfiguration.Mode.GENERATE);
        quickstart.setAttribute(QuickStartConfiguration.ORIGIN_ATTRIBUTE, "origin");
        quickstart.setDescriptor(MavenTestingUtils.getTestResourceFile("web.xml").getAbsolutePath());
        quickstart.setContextPath("/foo");
        server.setHandler(quickstart);
        server.setDryRun(true);
        server.start();
        
        assertTrue(quickstartXml.exists());
        
        // quick start
        WebAppContext webapp = new WebAppContext();
        webapp.addConfiguration(new QuickStartConfiguration());
        quickstart.setAttribute(QuickStartConfiguration.MODE, QuickStartConfiguration.Mode.QUICKSTART);
        webapp.setResourceBase(testDir.getAbsolutePath());
        webapp.getServerClassMatcher().exclude("org.eclipse.jetty.quickstart.");
        server.setHandler(webapp);

        server.setDryRun(false);
        server.start();
        
        assertEquals("ascii", webapp.getDefaultRequestCharacterEncoding());
        assertEquals("utf-16", webapp.getDefaultResponseCharacterEncoding());@@@+        assertEquals("foo", sh.getName());@@@+        server.stop();@@@     }
```
```
Cyclomatic Complexity	 2
Assertions		 6
Lines of Code		27
-    
    public void testUrlQuery() throws Exception
    {
        CookiePatternRule rule = new CookiePatternRule();
        rule.setPattern("*");
        rule.setName("fruit");
        rule.setValue("banana");

        startServer(rule);

        StringBuilder rawRequest = new StringBuilder();
        rawRequest.append("GET /other?fruit=apple HTTP/1.1\r\n");
        rawRequest.append("Host: local\r\n");
        rawRequest.append("Connection: close\r\n");
        rawRequest.append("\r\n");

        String rawResponse = localConnector.getResponse(rawRequest.toString());
        HttpTester.Response response = HttpTester.parseResponse(rawResponse);

        String responseContent = response.getContent();
        assertResponseContentLine(responseContent, "baseRequest.requestUri=", "/other");
        assertResponseContentLine(responseContent, "request.queryString=", "fruit=apple");

        // verify
        HttpField setCookieField = response.getField(HttpHeader.SET_COOKIE);
        assertThat("response should have Set-Cookie", setCookieField, notNullValue());
        for (String value : setCookieField.getValues())
        {
            String[] result = value.split("=");
            assertThat(result[0], is("fruit"));
            assertThat(result[1], is("banana"));
        }@@@     }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		9
-    
    public void testWin2kSP1WithIE6() throws Exception
    {
        _request.setHttpFields(HttpFields.build(_request.getHttpFields())
            .add("User-Agent", "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.01)"));

        String result = _rule.matchAndApply(_request.getRequestURI(), _request, _response);

        assertEquals(_request.getRequestURI(), result);
        assertEquals(HttpHeaderValue.CLOSE.asString(), _response.getHeader(HttpHeader.CONNECTION.asString()));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		9
-    
    public void testWin2kSP1WithIE7() throws Exception
    {
        _request.setHttpFields(HttpFields.build(_request.getHttpFields())
            .add("User-Agent", "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.01)"));

        String result = _rule.matchAndApply(_request.getRequestURI(), _request, _response);

        assertNull(result);
        assertNull(_response.getHeader(HttpHeader.CONNECTION.asString()));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 8
Lines of Code		18
-    
    public void testGracefulWithContext() throws Exception
    {
        Socket client0 = newClientBusy(POST_A_12345, handlerA);
        Socket client1 = newClientBusy(POST_A_12345_C, handlerA);
        Socket client2 = newClientIdle(POST_A_12345, handlerA);

        backgroundComplete(client0, handlerA);
        backgroundComplete(client1, handlerA);
        Future<Integer> status2 = backgroundUnavailable(client2, POST_A_12345, contextA, handlerA);

        assertGracefulStop(server);

        assertResponse(client0, true);
        assertResponse(client1, false);
        assertThat(status2.get(), is(503));

        assertQuickClose(client0);
        assertQuickClose(client1);
        assertQuickClose(client2);
        assertHandled(handlerA, false);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 8
Lines of Code		19
-    
    public void testGracefulContext() throws Exception
    {
        Socket client0 = newClientBusy(POST_B_12345, handlerB);
        Socket client1 = newClientBusy(POST_B_12345_C, handlerB);
        Socket client2 = newClientIdle(POST_B_12345, handlerB);

        backgroundComplete(client0, handlerB);
        backgroundComplete(client1, handlerB);
        Future<Integer> status2 = backgroundUnavailable(client2, POST_B_12345, contextB, handlerB);

        Graceful.shutdown(contextB).orTimeout(10, TimeUnit.SECONDS).get();

        assertResponse(client0, false);
        assertResponse(client1, false);
        assertThat(status2.get(), is(503));

        assertAvailable(client0, POST_A_12345, handlerA);
        assertAvailable(client1, POST_A_12345_C, handlerA);
        assertAvailable(client2, POST_A_12345, handlerA);

        assertHandled(handlerA, false);
        assertHandled(handlerB, false);
    }
```
```
Cyclomatic Complexity	 2
Assertions		 6
Lines of Code		50
-    
    public void testRequestCharacterEncoding() throws Exception
    {
        AtomicReference<String> result = new AtomicReference<>(null);
        AtomicReference<String> overrideCharEncoding = new AtomicReference<>(null);

        _server.stop();
        ContextHandler handler = new CharEncodingContextHandler();
        _server.setHandler(handler);
        handler.setHandler(_handler);
        _handler._checker = new RequestTester()
        {
            @Override
            public boolean check(HttpServletRequest request, HttpServletResponse response)
            {
                try
                {
                    String s = overrideCharEncoding.get();
                    if (s != null)
                        request.setCharacterEncoding(s);

                    result.set(request.getCharacterEncoding());
                    return true;
                }
                catch (UnsupportedEncodingException e)
                {
                    return false;
                }
            }
        };
        _server.start();

        String request = "GET / HTTP/1.1\n" +
            "Host: whatever\r\n" +
            "Content-Type: text/html;charset=utf8\n" +
            "Connection: close\n" +
            "\n";

        //test setting the default char encoding
        handler.setDefaultRequestCharacterEncoding("ascii");
        String response = _connector.getResponse(request);
        assertTrue(response.startsWith("HTTP/1.1 200"));
        assertEquals("ascii", result.get());

        //test overriding the default char encoding with explicit encoding
        result.set(null);
        overrideCharEncoding.set("utf-16");
        response = _connector.getResponse(request);
        assertTrue(response.startsWith("HTTP/1.1 200"));
        assertEquals("utf-16", result.get());

        //test fallback to content-type encoding
        result.set(null);
        overrideCharEncoding.set(null);
        handler.setDefaultRequestCharacterEncoding(null);
        response = _connector.getResponse(request);
        assertTrue(response.startsWith("HTTP/1.1 200"));
        assertEquals("utf-8", result.get());@@@     }
```
```
Cyclomatic Complexity	 1
Assertions		 19
Lines of Code		60
-    
    public void testPushBuilder() throws Exception
    {
        String uri = "/foo/something";
        Request request = new TestRequest(null, null);
        request.getResponse().getHttpFields().add(new HttpCookie.SetCookieHttpField(new HttpCookie("good","thumbsup", 100), CookieCompliance.RFC6265));
        request.getResponse().getHttpFields().add(new HttpCookie.SetCookieHttpField(new HttpCookie("bonza","bewdy", 1), CookieCompliance.RFC6265));
        request.getResponse().getHttpFields().add(new HttpCookie.SetCookieHttpField(new HttpCookie("bad", "thumbsdown", 0), CookieCompliance.RFC6265));
        HttpFields.Mutable fields = HttpFields.build();
        fields.add(HttpHeader.AUTHORIZATION, "Basic foo");
        request.setMetaData(new MetaData.Request("GET", HttpURI.from(uri), HttpVersion.HTTP_1_0, fields));
        assertTrue(request.isPushSupported());
        PushBuilder builder = request.newPushBuilder();
        assertNotNull(builder);
        assertEquals("GET", builder.getMethod());
        assertThrows(NullPointerException.class, () ->
        {
            builder.method(null);
        });
        assertThrows(IllegalArgumentException.class, () ->
        {
            builder.method("");
        });
        assertThrows(IllegalArgumentException.class, () ->
        {
            builder.method("   ");
        });
        assertThrows(IllegalArgumentException.class, () ->
        {
            builder.method("POST");
        });
        assertThrows(IllegalArgumentException.class, () ->
        {
            builder.method("PUT");
        });
        assertThrows(IllegalArgumentException.class, () ->
        {
            builder.method("DELETE");
        });
        assertThrows(IllegalArgumentException.class, () ->
        {
            builder.method("CONNECT");
        });
        assertThrows(IllegalArgumentException.class, () ->
        {
            builder.method("OPTIONS");
        });
        assertThrows(IllegalArgumentException.class, () ->
        {
            builder.method("TRACE");
        });
        assertEquals(TestRequest.TEST_SESSION_ID, builder.getSessionId());
        builder.path("/foo/something-else.txt");
        assertEquals("/foo/something-else.txt", builder.getPath());
        assertEquals("Basic foo", builder.getHeader("Authorization"));
        assertThat(builder.getHeader("Cookie"), containsString("bonza"));
        assertThat(builder.getHeader("Cookie"), containsString("good"));
        assertThat(builder.getHeader("Cookie"), containsString("maxpos"));
        assertThat(builder.getHeader("Cookie"), not(containsString("bad")));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		18
-    
    public void testPushBuilderWithIdNoAuth() throws Exception
    {
        String uri = "/foo/something";
        Request request = new TestRequest(null, null)
        {
            @Override
            public Principal getUserPrincipal()
            {
                return () -> "test";
            }
        };
        HttpFields.Mutable fields = HttpFields.build();
        request.setMetaData(new MetaData.Request("GET", HttpURI.from(uri), HttpVersion.HTTP_1_0, fields));
        assertTrue(request.isPushSupported());
        PushBuilder builder = request.newPushBuilder();
        assertNotNull(builder);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 48
Lines of Code		77
-    
    public void testServletPathMapping() throws Exception
    {
        ServletPathSpec spec;
        String uri;
        ServletPathMapping m;

        spec = null;
        uri = null;
        m = new ServletPathMapping(spec, null, uri);
        assertThat(m.getMappingMatch(), nullValue());
        assertThat(m.getMatchValue(), is(""));
        assertThat(m.getPattern(), nullValue());
        assertThat(m.getServletName(), is(""));
        assertThat(m.getServletPath(), nullValue());
        assertThat(m.getPathInfo(), nullValue());

        spec = new ServletPathSpec("");
        uri = "/";
        m = new ServletPathMapping(spec, "Something", uri);
        assertThat(m.getMappingMatch(), is(MappingMatch.CONTEXT_ROOT));
        assertThat(m.getMatchValue(), is(""));
        assertThat(m.getPattern(), is(""));
        assertThat(m.getServletName(), is("Something"));
        assertThat(m.getServletPath(), is(spec.getPathMatch(uri)));
        assertThat(m.getPathInfo(), is(spec.getPathInfo(uri)));

        spec = new ServletPathSpec("/");
        uri = "/some/path";
        m = new ServletPathMapping(spec, "Default", uri);
        assertThat(m.getMappingMatch(), is(MappingMatch.DEFAULT));
        assertThat(m.getMatchValue(), is(""));
        assertThat(m.getPattern(), is("/"));
        assertThat(m.getServletName(), is("Default"));
        assertThat(m.getServletPath(), is(spec.getPathMatch(uri)));
        assertThat(m.getPathInfo(), is(spec.getPathInfo(uri)));

        spec = new ServletPathSpec("/foo/*");
        uri = "/foo/bar";
        m = new ServletPathMapping(spec, "FooServlet", uri);
        assertThat(m.getMappingMatch(), is(MappingMatch.PATH));
        assertThat(m.getMatchValue(), is("foo"));
        assertThat(m.getPattern(), is("/foo/*"));
        assertThat(m.getServletName(), is("FooServlet"));
        assertThat(m.getServletPath(), is(spec.getPathMatch(uri)));
        assertThat(m.getPathInfo(), is(spec.getPathInfo(uri)));

        uri = "/foo/";
        m = new ServletPathMapping(spec, "FooServlet", uri);
        assertThat(m.getMappingMatch(), is(MappingMatch.PATH));
        assertThat(m.getMatchValue(), is("foo"));
        assertThat(m.getPattern(), is("/foo/*"));
        assertThat(m.getServletName(), is("FooServlet"));
        assertThat(m.getServletPath(), is(spec.getPathMatch(uri)));
        assertThat(m.getPathInfo(), is(spec.getPathInfo(uri)));

        uri = "/foo";
        m = new ServletPathMapping(spec, "FooServlet", uri);
        assertThat(m.getMappingMatch(), is(MappingMatch.PATH));
        assertThat(m.getMatchValue(), is("foo"));
        assertThat(m.getPattern(), is("/foo/*"));
        assertThat(m.getServletName(), is("FooServlet"));
        assertThat(m.getServletPath(), is(spec.getPathMatch(uri)));
        assertThat(m.getPathInfo(), is(spec.getPathInfo(uri)));

        spec = new ServletPathSpec("*.jsp");
        uri = "/foo/bar.jsp";
        m = new ServletPathMapping(spec, "JspServlet", uri);
        assertThat(m.getMappingMatch(), is(MappingMatch.EXTENSION));
        assertThat(m.getMatchValue(), is("foo/bar"));
        assertThat(m.getPattern(), is("*.jsp"));
        assertThat(m.getServletName(), is("JspServlet"));
        assertThat(m.getServletPath(), is(spec.getPathMatch(uri)));
        assertThat(m.getPathInfo(), is(spec.getPathInfo(uri)));

        spec = new ServletPathSpec("/catalog");
        uri = "/catalog";
        m = new ServletPathMapping(spec, "CatalogServlet", uri);
        assertThat(m.getMappingMatch(), is(MappingMatch.EXACT));
        assertThat(m.getMatchValue(), is("catalog"));
        assertThat(m.getPattern(), is("/catalog"));
        assertThat(m.getServletName(), is("CatalogServlet"));
        assertThat(m.getServletPath(), is(spec.getPathMatch(uri)));
        assertThat(m.getPathInfo(), is(spec.getPathInfo(uri)));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 11
Lines of Code		36
-    
    public void testLocaleAndContentTypeEncoding() throws Exception
    {
        _server.stop();
        MimeTypes.getInferredEncodings().put("text/html", "iso-8859-1");
        ContextHandler handler = new ContextHandler();
        handler.addLocaleEncoding("ja", "euc-jp");
        handler.addLocaleEncoding("zh_CN", "gb18030");
        _server.setHandler(handler);
        handler.setHandler(new DumpHandler());
        _server.start();

        Response response = getResponse();
        response.getHttpChannel().getRequest().setContext(handler.getServletContext(), "/");

        response.setContentType("text/html");
        assertEquals("iso-8859-1", response.getCharacterEncoding());

        // setLocale should change character encoding based on
        // locale-encoding-mapping-list
        response.setLocale(Locale.JAPAN);
        assertEquals("euc-jp", response.getCharacterEncoding());

        // setLocale should change character encoding based on
        // locale-encoding-mapping-list
        response.setLocale(Locale.CHINA);
        assertEquals("gb18030", response.getCharacterEncoding());

        // setContentType here doesn't define character encoding
        response.setContentType("text/html");
        assertEquals("gb18030", response.getCharacterEncoding());

        // setCharacterEncoding should still be able to change encoding
        response.setCharacterEncoding("utf-8");
        assertEquals("utf-8", response.getCharacterEncoding());

        // setLocale should not override explicit character encoding request
        response.setLocale(Locale.JAPAN);
        assertEquals("utf-8", response.getCharacterEncoding());

        // setContentType should still be able to change encoding
        response.setContentType("text/html;charset=gb18030");
        assertEquals("gb18030", response.getCharacterEncoding());

        // setCharacterEncoding should still be able to change encoding
        response.setCharacterEncoding("utf-8");
        assertEquals("utf-8", response.getCharacterEncoding());

        // getWriter should freeze the character encoding
        PrintWriter pw = response.getWriter();
        assertEquals("utf-8", response.getCharacterEncoding());

        // setCharacterEncoding should no longer be able to change the encoding
        response.setCharacterEncoding("iso-8859-1");
        assertEquals("utf-8", response.getCharacterEncoding());

        // setLocale should not override explicit character encoding request
        response.setLocale(Locale.JAPAN);
        assertEquals("utf-8", response.getCharacterEncoding());
    }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		9
-    
    public void testContentEncodingViaContentTypeChange() throws Exception
    {
        Response response = getResponse();
        response.setContentType("text/html;charset=Shift_Jis");
        assertEquals("Shift_Jis", response.getCharacterEncoding());

        response.setContentType("text/xml");
        assertEquals("Shift_Jis", response.getCharacterEncoding());@@@     }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		51
-    
    public void testSessionListenerOrdering()
        throws Exception
    {
        final StringBuffer result = new StringBuffer();

        class Listener1 implements HttpSessionListener
        {

            @Override
            public void sessionCreated(HttpSessionEvent se)
            {
                result.append("Listener1 create;");
            }

            @Override
            public void sessionDestroyed(HttpSessionEvent se)
            {
                result.append("Listener1 destroy;");
            }
        }

        class Listener2 implements HttpSessionListener
        {

            @Override
            public void sessionCreated(HttpSessionEvent se)
            {
                result.append("Listener2 create;");
            }

            @Override
            public void sessionDestroyed(HttpSessionEvent se)
            {
                result.append("Listener2 destroy;");
            }

        }

        Server server = new Server();
        SessionHandler sessionHandler = new SessionHandler();
        try
        {
            sessionHandler.addEventListener(new Listener1());
            sessionHandler.addEventListener(new Listener2());
            sessionHandler.setServer(server);
            sessionHandler.start();
            Session session = new Session(sessionHandler, new SessionData("aa", "_", "0.0", 0, 0, 0, 0));
            sessionHandler.callSessionCreatedListeners(session);
            sessionHandler.callSessionDestroyedListeners(session);
            assertEquals("Listener1 create;Listener2 create;Listener2 destroy;Listener1 destroy;", result.toString());
        }
        finally
        {
            sessionHandler.stop();
        }@@@+        assertThrows(IllegalArgumentException.class,() ->@@@+            sessionHandler.setSessionTrackingModes(new HashSet<>(Arrays.asList(SessionTrackingMode.SSL, SessionTrackingMode.URL))));@@@     }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		16
-    
    public void testNamedInclude() throws Exception
    {
        _contextHandler.addServlet(NamedIncludeServlet.class, "/include/*");
        String echo = _contextHandler.addServlet(EchoURIServlet.class, "/echo/*").getName();

        String expected =
            "HTTP/1.1 200 OK\r\n" +
                "Content-Length: 62\r\n" +
                "\r\n" +
                "/context\r\n" +
                "/include\r\n" +
                "/info\r\n" +
                "/context/include/info;param=value\r\n";
        String responses = _connector.getResponse("GET /context/include/info;param=value?name=" + echo + " HTTP/1.0\n\n");
        assertEquals(expected, responses);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		8
-    
    public void testDispatchMapping() throws Exception
    {
        _contextHandler.addServlet(new ServletHolder("TestServlet", MappingServlet.class), "/TestServlet");
        _contextHandler.addServlet(new ServletHolder("DispatchServlet", AsyncDispatch2TestServlet.class), "/DispatchServlet");

        String response = _connector.getResponse("GET /context/DispatchServlet HTTP/1.0\n\n");
        assertThat(response, containsString("matchValue=TestServlet, pattern=/TestServlet, servletName=TestServlet, mappingMatch=EXACT"));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 9
Lines of Code		25
-    
    public void testErrorOverridesMimeTypeAndCharset() throws Exception
    {
        StringBuilder rawRequest = new StringBuilder();
        rawRequest.append("GET /error-mime-charset-writer/ HTTP/1.1\r\n");
        rawRequest.append("Host: test\r\n");
        rawRequest.append("Connection: close\r\n");
        rawRequest.append("Accept: */*\r\n");
        rawRequest.append("Accept-Charset: *\r\n");
        rawRequest.append("\r\n");

        String rawResponse = _connector.getResponse(rawRequest.toString());
        System.out.println(rawResponse);
        HttpTester.Response response = HttpTester.parseResponse(rawResponse);

        assertThat(response.getStatus(), is(595));
        String actualContentType = response.get(HttpHeader.CONTENT_TYPE);
        // should not expect to see charset line from servlet
        assertThat(actualContentType, not(containsString("charset=US-ASCII")));
        String body = response.getContent();

        assertThat(body, containsString("ERROR_PAGE: /595"));
        assertThat(body, containsString("ERROR_MESSAGE: 595"));
        assertThat(body, containsString("ERROR_CODE: 595"));
        assertThat(body, containsString("ERROR_EXCEPTION: null"));
        assertThat(body, containsString("ERROR_EXCEPTION_TYPE: null"));
        assertThat(body, containsString("ERROR_SERVLET: org.eclipse.jetty.servlet.ErrorPageTest$ErrorContentTypeCharsetWriterInitializedServlet-"));
        assertThat(body, containsString("ERROR_REQUEST_URI: /error-mime-charset-writer/"));@@@     }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		20
-    
    public void testCreateInstance() throws Exception
    {
        try (StacklessLogging ignore = new StacklessLogging(ServletHandler.class, ServletContextHandler.class))
        {
            //test without a ServletContextHandler or current ContextHandler
            FilterHolder holder = new FilterHolder();
            holder.setName("foo");
            holder.setHeldClass(DummyFilter.class);
            Filter filter = holder.createInstance();
            assertNotNull(filter);

            //test with a ServletContextHandler
            Server server = new Server();
            ServletContextHandler context = new ServletContextHandler();
            server.setHandler(context);
            ServletHandler handler = context.getServletHandler();
            handler.addFilter(holder);
            holder.setServletHandler(handler);
            context.start();
            assertNotNull(holder.getFilter());
        }@@@     }
```
```
Cyclomatic Complexity	 2
Assertions		 2
Lines of Code		14
-    
    public void testCharsetResetToJsonMimeType() throws Exception
    {
        HttpTester.Request request = new HttpTester.Request();
        request.setMethod("GET");
        request.setURI("/charset/json-reset/");
        request.setVersion(HttpVersion.HTTP_1_1);
        request.setHeader("Connection", "close");
        request.setHeader("Host", "test");

        ByteBuffer responseBuffer = connector.getResponse(request.generate());
        // System.err.println(BufferUtil.toUTF8String(responseBuffer));
        HttpTester.Response response = HttpTester.parseResponse(responseBuffer);

        // Now test for properly formatted HTTP Response Headers.
        assertThat("Response Code", response.getStatus(), is(200));
        // The Content-Type should not have a charset= portion
        assertThat("Response Header Content-Type", response.get("Content-Type"), is("application/json"));
    }
```
```
Cyclomatic Complexity	 2
Assertions		 2
Lines of Code		14
-    
    public void testCharsetChangeToJsonMimeType() throws Exception
    {
        HttpTester.Request request = new HttpTester.Request();
        request.setMethod("GET");
        request.setURI("/charset/json-change/");
        request.setVersion(HttpVersion.HTTP_1_1);
        request.setHeader("Connection", "close");
        request.setHeader("Host", "test");

        ByteBuffer responseBuffer = connector.getResponse(request.generate());
        // System.err.println(BufferUtil.toUTF8String(responseBuffer));
        HttpTester.Response response = HttpTester.parseResponse(responseBuffer);

        // Now test for properly formatted HTTP Response Headers.
        assertThat("Response Code", response.getStatus(), is(200));
        // The Content-Type should not have a charset= portion
        assertThat("Response Header Content-Type", response.get("Content-Type"), is("application/json"));
    }
```
```
Cyclomatic Complexity	 2
Assertions		 2
Lines of Code		14
-    
    public void testCharsetChangeToJsonMimeTypeSetCharsetToNull() throws Exception
    {
        HttpTester.Request request = new HttpTester.Request();
        request.setMethod("GET");
        request.setURI("/charset/json-change-null/");
        request.setVersion(HttpVersion.HTTP_1_1);
        request.setHeader("Connection", "close");
        request.setHeader("Host", "test");

        ByteBuffer responseBuffer = connector.getResponse(request.generate());
        // System.err.println(BufferUtil.toUTF8String(responseBuffer));
        HttpTester.Response response = HttpTester.parseResponse(responseBuffer);

        // Now test for properly formatted HTTP Response Headers.
        assertThat("Response Code", response.getStatus(), is(200));
        // The Content-Type should not have a charset= portion
        assertThat("Response Header Content-Type", response.get("Content-Type"), is("application/json"));@@@     }
```
```
Cyclomatic Complexity	 1
Assertions		 6
Lines of Code		18
-    
    public void testGetSetSessionTimeout() throws Exception
    {
        ContextHandlerCollection contexts = new ContextHandlerCollection();
        _server.setHandler(contexts);

        int startMin = 7;
        Integer timeout = Integer.valueOf(100);
        ServletContextHandler root = new ServletContextHandler(contexts, "/", ServletContextHandler.SESSIONS);
        root.getSessionHandler().setMaxInactiveInterval((int)TimeUnit.MINUTES.toSeconds(startMin));
        root.addBean(new MySCIStarter(root.getServletContext(), new MySCI(true, timeout.intValue())), true);
        _server.start();

        //test starting value of setSessionTimeout
        assertEquals(startMin, (Integer)root.getServletContext().getAttribute("MYSCI.startSessionTimeout"));
        //test can set session timeout from ServletContainerInitializer
        assertTrue((Boolean)root.getServletContext().getAttribute("MYSCI.setSessionTimeout"));
        //test can get session timeout from ServletContainerInitializer
        assertEquals(timeout, (Integer)root.getServletContext().getAttribute("MYSCI.getSessionTimeout"));
        assertNull(root.getAttribute("MYSCI.sessionTimeoutFailure"));
        //test can't get session timeout from ContextListener that is not from annotation or web.xml
        assertTrue((Boolean)root.getServletContext().getAttribute("MyContextListener.getSessionTimeout"));
        //test can't set session timeout from ContextListener that is not from annotation or web.xml
        assertTrue((Boolean)root.getServletContext().getAttribute("MyContextListener.setSessionTimeout"));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 3
Lines of Code		20
-    
    public void testCreateMethodsFromSCL() throws Exception
    {
      //A filter can be created by an SCI
        ContextHandlerCollection contexts = new ContextHandlerCollection();
        _server.setHandler(contexts);

        ServletContextHandler root = new ServletContextHandler(contexts, "/");
        class ListenerCreatingSCI implements ServletContainerInitializer
        {
            @Override
            public void onStartup(Set<Class<?>> c, ServletContext ctx) throws ServletException
            {
                ctx.addListener(new CreatingSCL());
            }
        }

        root.addBean(new MySCIStarter(root.getServletContext(), new ListenerCreatingSCI()), true);
        _server.start();
        assertTrue((Boolean)root.getServletContext().getAttribute("CreatingSCL.filter"));
        assertTrue((Boolean)root.getServletContext().getAttribute("CreatingSCL.servlet"));
        assertTrue((Boolean)root.getServletContext().getAttribute("CreatingSCL.listener"));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 4
Lines of Code		15
-    
    public void testProvidersUsingDefault() throws Exception
    {
        Path homePath = MavenTestingUtils.getTestResourceDir("providers-home").toPath().toRealPath();

        List<String> cmdLineArgs = new ArrayList<>();
        cmdLineArgs.add("user.dir=" + homePath);
        cmdLineArgs.add("jetty.home=" + homePath);
        cmdLineArgs.add("--module=server");

        Main main = new Main();
        StartArgs args = main.processCommandLine(cmdLineArgs.toArray(new String[cmdLineArgs.size()]));
        BaseHome baseHome = main.getBaseHome();

        assertThat("jetty.home", baseHome.getHome(), is(homePath.toString()));
        assertThat("jetty.base", baseHome.getBase(), is(homePath.toString()));

        ConfigurationAssert.assertConfiguration(baseHome, args, "assert-providers-default.txt");
    }
```
```
Cyclomatic Complexity	 1
Assertions		 4
Lines of Code		16
-    
    public void testProvidersUsingSpecific() throws Exception
    {
        Path homePath = MavenTestingUtils.getTestResourceDir("providers-home").toPath().toRealPath();

        List<String> cmdLineArgs = new ArrayList<>();
        cmdLineArgs.add("user.dir=" + homePath);
        cmdLineArgs.add("jetty.home=" + homePath);
        cmdLineArgs.add("--module=server");
        cmdLineArgs.add("--module=logging-b");

        Main main = new Main();
        StartArgs args = main.processCommandLine(cmdLineArgs.toArray(new String[cmdLineArgs.size()]));
        BaseHome baseHome = main.getBaseHome();

        assertThat("jetty.home", baseHome.getHome(), is(homePath.toString()));
        assertThat("jetty.base", baseHome.getBase(), is(homePath.toString()));

        ConfigurationAssert.assertConfiguration(baseHome, args, "assert-providers-specific.txt");@@@     }
```
```
Cyclomatic Complexity	 1
Assertions		 9
Lines of Code		23
-    
    public void testDownloadSnapshotRepo()
        throws Exception
    {
        Path snapshotLocalRepoDir = testdir.getPath().resolve("snapshot-repo");
        FS.ensureEmpty(snapshotLocalRepoDir);

        MavenLocalRepoFileInitializer repo =
            new MavenLocalRepoFileInitializer(baseHome, snapshotLocalRepoDir, false, "https://oss.sonatype.org/content/repositories/jetty-snapshots/");
        String ref = "maven://org.eclipse.jetty/jetty-rewrite/11.0.0-SNAPSHOT/jar";
        Coordinates coords = repo.getCoordinates(URI.create(ref));
        assertThat("Coordinates", coords, notNullValue());

        assertThat("coords.groupId", coords.groupId, is("org.eclipse.jetty"));
        assertThat("coords.artifactId", coords.artifactId, is("jetty-rewrite"));
        assertThat("coords.version", coords.version, is("11.0.0-SNAPSHOT"));
        assertThat("coords.type", coords.type, is("jar"));
        assertThat("coords.classifier", coords.classifier, is(nullValue()));

        assertThat("coords.toCentralURI", coords.toCentralURI().toASCIIString(),
            is("https://oss.sonatype.org/content/repositories/jetty-snapshots/org/eclipse/jetty/jetty-rewrite/11.0.0-SNAPSHOT/jetty-rewrite-11.0.0-SNAPSHOT.jar"));

        Path destination = baseHome.getBasePath().resolve("jetty-rewrite-11.0.0-SNAPSHOT.jar");
        repo.download(coords, destination);
        assertThat(Files.exists(destination), is(true));
        assertThat("Snapshot File size", destination.toFile().length(), greaterThan(10_000L));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		14
-    
    public void testDownloadSnapshotRepoWithExtractDeep()
        throws Exception
    {
        Path snapshotLocalRepoDir = testdir.getPath().resolve("snapshot-repo");
        FS.ensureEmpty(snapshotLocalRepoDir);

        MavenLocalRepoFileInitializer repo =
            new MavenLocalRepoFileInitializer(baseHome, snapshotLocalRepoDir, false,
                "https://oss.sonatype.org/content/repositories/jetty-snapshots/");
        String ref = "maven://org.eclipse.jetty/test-jetty-webapp/11.0.0-SNAPSHOT/jar/config";
        Path baseDir = baseHome.getBasePath();
        repo.create(URI.create(ref), "extract:company/");

        assertThat(Files.exists(baseDir.resolve("company/webapps/test.d/override-web.xml")), is(true));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 4
Lines of Code		10
-    
    public void testCopyIndirect()
    {
        ByteBuffer b = BufferUtil.toBuffer("Hello World");
        ByteBuffer c = BufferUtil.copy(b);
        assertEquals("Hello World", BufferUtil.toString(c));
        assertFalse(c.isDirect());
        assertThat(b, not(sameInstance(c)));
        assertThat(b.array(), not(sameInstance(c.array())));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 3
Lines of Code		10
-    
    public void testCopyDirect()
    {
        ByteBuffer b = BufferUtil.allocateDirect(11);
        BufferUtil.append(b, "Hello World");
        ByteBuffer c = BufferUtil.copy(b);
        assertEquals("Hello World", BufferUtil.toString(c));
        assertTrue(c.isDirect());
        assertThat(b, not(sameInstance(c)));@@@     }
```
```
Cyclomatic Complexity	 1
Assertions		 24
Lines of Code		28
-    
    public void testRequiredCapacity()
    {
        assertThat(requiredCapacity(Set.of("ABC", "abc"), true), is(1 + 6));
        assertThat(requiredCapacity(Set.of("ABC", "abc"), false), is(1 + 3));
        assertThat(requiredCapacity(Set.of(""), false), is(1 + 0));
        assertThat(requiredCapacity(Set.of("ABC", ""), false), is(1 + 3));
        assertThat(requiredCapacity(Set.of("ABC"), false), is(1 + 3));
        assertThat(requiredCapacity(Set.of("ABC", "XYZ"), false), is(1 + 6));
        assertThat(requiredCapacity(Set.of("A00", "A11"), false), is(1 + 5));
        assertThat(requiredCapacity(Set.of("A00", "A01", "A10", "A11"), false), is(1 + 7));
        assertThat(requiredCapacity(Set.of("A", "AB"), false), is(1 + 2));
        assertThat(requiredCapacity(Set.of("A", "ABC"), false), is(1 + 3));
        assertThat(requiredCapacity(Set.of("A", "ABCD"), false), is(1 + 4));
        assertThat(requiredCapacity(Set.of("AB", "ABC"), false), is(1 + 3));
        assertThat(requiredCapacity(Set.of("ABC", "ABCD"), false), is(1 + 4));
        assertThat(requiredCapacity(Set.of("ABC", "ABCDEF"), false), is(1 + 6));
        assertThat(requiredCapacity(Set.of("AB", "A"), false), is(1 + 2));
        assertThat(requiredCapacity(Set.of("ABC", "ABCDEF"), false), is(1 + 6));
        assertThat(requiredCapacity(Set.of("ABCDEF", "ABC"), false), is(1 + 6));
        assertThat(requiredCapacity(Set.of("ABC", "ABCDEF", "ABX"), false), is(1 + 7));
        assertThat(requiredCapacity(Set.of("ABCDEF", "ABC", "ABX"), false), is(1 + 7));
        assertThat(requiredCapacity(Set.of("ADEF", "AQPR4", "AQZ"), false), is(1 + 9));
        assertThat(requiredCapacity(Set.of("111", "ADEF", "AQPR4", "AQZ", "999"), false), is(1 + 15));
        assertThat(requiredCapacity(Set.of("utf-16", "utf-8"), false), is(1 + 7));
        assertThat(requiredCapacity(Set.of("utf-16", "utf-8", "utf16", "utf8"), false), is(1 + 10));
        assertThat(requiredCapacity(Set.of("utf-8", "utf8", "utf-16", "utf16", "iso-8859-1", "iso_8859_1"), false), is(1 + 27));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		8
-    
    public void testLargeRequiredCapacity()
    {
        String x = "x".repeat(Character.MAX_VALUE / 2);
        String y = "y".repeat(Character.MAX_VALUE / 2);
        String z = "z".repeat(Character.MAX_VALUE / 2);
        assertThat(requiredCapacity(Set.of(x, y, z), true), is(1 + 3 * (Character.MAX_VALUE / 2)));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 3
Lines of Code		9
-    
    public void testArrayTrieCapacity()
    {
        ArrayTrie<String> trie = new ArrayTrie<>(Character.MAX_VALUE);
        String huge = "x".repeat(Character.MAX_VALUE - 1);
        assertTrue(trie.put(huge, "wow"));
        assertThat(trie.get(huge), is("wow"));

        assertThrows(IllegalArgumentException.class, () -> new ArrayTrie<String>(Character.MAX_VALUE + 1));@@@     }
```
```
Cyclomatic Complexity	 1
Assertions		 3
Lines of Code		13
-    
    public void testJoinWithStopTimeout() throws Exception
    {
        final long stopTimeout = 100;
        QueuedThreadPool threadPool = new QueuedThreadPool();
        threadPool.setStopTimeout(100);
        threadPool.start();

        // Verify that join does not timeout after waiting twice the stopTimeout.
        assertThrows(Throwable.class, () ->
            assertTimeoutPreemptively(Duration.ofMillis(stopTimeout * 2), threadPool::join)
        );

        // After stopping the ThreadPool join should unblock.
        LifeCycle.stop(threadPool);
        assertTimeoutPreemptively(Duration.ofMillis(stopTimeout), threadPool::join);@@@     }
```
```
Cyclomatic Complexity	 2
Assertions		 2
Lines of Code		17
-    
    public void testFindAndFilterContainerPathsJDK9() throws Exception
    {
        MetaInfConfiguration config = new MetaInfConfiguration();
        WebAppContext context = new WebAppContext();
        context.setAttribute(MetaInfConfiguration.CONTAINER_JAR_PATTERN, ".*/jetty-util-[^/]*\\.jar$|.*/jetty-util/target/classes/$|.*/foo-bar-janb.jar");
        WebAppClassLoader loader = new WebAppClassLoader(context);
        context.setClassLoader(loader);
        config.findAndFilterContainerPaths(context);
        List<Resource> containerResources = context.getMetaData().getContainerResources();
        assertEquals(2, containerResources.size());
        for (Resource r : containerResources)
        {
            String s = r.toString();
            assertTrue(s.endsWith("foo-bar-janb.jar") || s.contains("jetty-util"));
        }@@@     }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		11
-    
    public void testSetWithNullProperty() throws Exception
    {
        XmlConfiguration configuration = asXmlConfiguration("<Configure class=\"org.eclipse.jetty.xml.TestConfiguration\"><Set name=\"TestString\" property=\"prop\" id=\"test\"/></Configure>");
        configuration.getProperties().remove("prop");
        TestConfiguration tc = new TestConfiguration();
        tc.setTestString("default");
        configuration.configure(tc);
        assertEquals("default", tc.getTestString());
        assertNull(configuration.getIdMap().get("test"));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		11
-    
    public void testSetWithPropertyAndValue() throws Exception
    {
        XmlConfiguration configuration = asXmlConfiguration("<Configure class=\"org.eclipse.jetty.xml.TestConfiguration\"><Set name=\"TestString\" property=\"prop\" id=\"test\">Value</Set></Configure>");
        configuration.getProperties().put("prop", "This is a property value");
        TestConfiguration tc = new TestConfiguration();
        tc.setTestString("default");
        configuration.configure(tc);
        assertEquals("Value", tc.getTestString());
        assertEquals(configuration.getIdMap().get("test"), "Value");
    }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		11
-    
    public void testSetWithNullPropertyAndValue() throws Exception
    {
        XmlConfiguration configuration = asXmlConfiguration("<Configure class=\"org.eclipse.jetty.xml.TestConfiguration\"><Set name=\"TestString\" property=\"prop\" id=\"test\">Value</Set></Configure>");
        configuration.getProperties().remove("prop");
        TestConfiguration tc = new TestConfiguration();
        tc.setTestString("default");
        configuration.configure(tc);
        assertEquals("default", tc.getTestString());
        assertNull(configuration.getIdMap().get("test"));
    }
```
```
Cyclomatic Complexity	 10
Assertions		 17
Lines of Code		67
-    
    public void testCleanOrphans() throws Exception
    {
        //create the SessionDataStore
        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/test");
        SessionDataStoreFactory factory = createSessionDataStoreFactory();
        ((AbstractSessionDataStoreFactory)factory).setGracePeriodSec(GRACE_PERIOD_SEC);
        SessionDataStore store = factory.getSessionDataStore(context.getSessionHandler());
        SessionContext sessionContext = new SessionContext("foo", context.getServletContext());
        store.initialize(sessionContext);

        long now = System.currentTimeMillis();
        
        //persist a long ago expired session for our context
        SessionData oldSession = store.newSessionData("001", 100, 101, 100, TimeUnit.MINUTES.toMillis(60));
        oldSession.setExpiry(200);
        oldSession.setLastNode("me");
        persistSession(oldSession);
        assertTrue(checkSessionExists(oldSession));
        
        //persist a recently expired session for our context
        SessionData expiredSession = store.newSessionData("002", 100, 101, 100, TimeUnit.MINUTES.toMillis(60));
        expiredSession.setExpiry(RECENT_TIMESTAMP);
        expiredSession.setLastNode("me");
        persistSession(expiredSession);
        assertTrue(checkSessionExists(expiredSession));

        //persist a non expired session for our context
        SessionData unexpiredSession = store.newSessionData("003", 100, now + 10, now + 5, TimeUnit.MINUTES.toMillis(60));
        unexpiredSession.setExpiry(now + TimeUnit.MINUTES.toMillis(10));
        unexpiredSession.setLastNode("me");
        persistSession(unexpiredSession);
        assertTrue(checkSessionExists(unexpiredSession));

        //persist an immortal session for our context
        SessionData immortalSession = store.newSessionData("004", 100, now + 10, now + 5, TimeUnit.MINUTES.toMillis(60));
        immortalSession.setExpiry(0);
        immortalSession.setLastNode("me");
        persistSession(immortalSession);
        assertTrue(checkSessionExists(immortalSession));
        
        //create sessions for a different context
        //persist a long ago expired session for a different context
        SessionData oldForeignSession = store.newSessionData("005", 100, 101, 100, TimeUnit.MINUTES.toMillis(60));
        oldForeignSession.setContextPath("_other");
        oldForeignSession.setExpiry(200);
        oldForeignSession.setLastNode("me");
        persistSession(oldForeignSession);
        assertTrue(checkSessionExists(oldForeignSession));
        
        //persist a recently expired session for our context
        SessionData expiredForeignSession = store.newSessionData("006", 100, 101, 100, TimeUnit.MINUTES.toMillis(60));
        expiredForeignSession.setContextPath("_other");
        expiredForeignSession.setExpiry(RECENT_TIMESTAMP);
        expiredForeignSession.setLastNode("me");
        persistSession(expiredForeignSession);
        assertTrue(checkSessionExists(expiredForeignSession));
        
        //persist a non expired session for our context
        SessionData unexpiredForeignSession = store.newSessionData("007", 100, now + 10, now + 5, TimeUnit.MINUTES.toMillis(60));
        unexpiredForeignSession.setContextPath("_other");
        unexpiredForeignSession.setExpiry(now + TimeUnit.MINUTES.toMillis(10));
        unexpiredForeignSession.setLastNode("me");
        persistSession(unexpiredForeignSession);
        assertTrue(checkSessionExists(unexpiredForeignSession));
        
        //persist an immortal session for our context
        SessionData immortalForeignSession = store.newSessionData("008", 100, now + 10, now + 5, TimeUnit.MINUTES.toMillis(60));
        immortalForeignSession.setContextPath("_other");
        immortalForeignSession.setExpiry(0);
        immortalForeignSession.setLastNode("me");
        persistSession(immortalForeignSession);
        assertTrue(checkSessionExists(immortalForeignSession));
        
        store.start();
        
        ((AbstractSessionDataStore)store).cleanOrphans(now - TimeUnit.SECONDS.toMillis(10 * GRACE_PERIOD_SEC));

        //old session should be gone
        assertFalse(checkSessionExists(oldSession));
        //recently expired session should still be there
        assertTrue(checkSessionExists(expiredSession));
        //unexpired session should still be there
        assertTrue(checkSessionExists(unexpiredSession));
        //immortal session should still exist
        assertTrue(checkSessionExists(immortalSession));
        //old foreign session should be gone
        assertFalse(checkSessionExists(oldSession));
        //recently expired foreign session should still be there
        assertTrue(checkSessionExists(expiredSession));
        //unexpired foreign session should still be there
        assertTrue(checkSessionExists(unexpiredSession));
        //immortal foreign session should still exist
        assertTrue(checkSessionExists(immortalSession));@@@+        assertThat(expiredIds, containsInAnyOrder("1234"));@@@     }
```
## c8654c81dae95f69cc663685998880951356afae ##
```
Cyclomatic Complexity	1
Assertions		1
Lines of Code		54
-    
    public void test_ExchangeIsComplete_WhenRequestFailsMidway_WithResponse() throws Exception
    {
        start(new AbstractHandler()
        {
            @Override
            public void handle(String target, org.eclipse.jetty.server.Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException
            {
                // Echo back
                IO.copy(request.getInputStream(), response.getOutputStream());
            }
        });

        final CountDownLatch latch = new CountDownLatch(1);
        client.newRequest("localhost", connector.getLocalPort())
                .scheme(scheme)
                        // The second ByteBuffer set to null will throw an exception
                .content(new ContentProvider()
                {
                    @Override
                    public long getLength()
                    {
                        return -1;
                    }

                    @Override
                    public Iterator<ByteBuffer> iterator()
                    {
                        return new Iterator<ByteBuffer>()
                        {
                            @Override
                            public boolean hasNext()
                            {
                                return true;
                            }

                            @Override
                            public ByteBuffer next()
                            {
                                throw new NoSuchElementException("explicitly_thrown_by_test");
                            }

                            @Override
                            public void remove()
                            {
                                throw new UnsupportedOperationException();
                            }
                        };
                    }
                })
                .send(new Response.Listener.Adapter()
                {
                    @Override
                    public void onComplete(Result result)
                    {
                        latch.countDown();
                    }
                });

        Assert.assertTrue(latch.await(5, TimeUnit.SECONDS));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		28
-    
    public void test_ExchangeIsComplete_WhenRequestFails_WithNoResponse() throws Exception
    {
        start(new EmptyServerHandler());

        final CountDownLatch latch = new CountDownLatch(1);
        final String host = "localhost";
        final int port = connector.getLocalPort();
        client.newRequest(host, port)
                .scheme(scheme)
                .onRequestBegin(new Request.BeginListener()
                {
                    @Override
                    public void onBegin(Request request)
                    {
                        HttpDestinationOverHTTP destination = (HttpDestinationOverHTTP)client.getDestination(scheme, host, port);
                        destination.getConnectionPool().getActiveConnections().peek().close();
                    }
                })
                .send(new Response.Listener.Adapter()
                {
                    @Override
                    public void onComplete(Result result)
                    {
                        latch.countDown();
                    }
                });

        Assert.assertTrue(latch.await(5, TimeUnit.SECONDS));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		24
-    
    public void test_ExchangeIsComplete_WhenRequestFails_WithNoResponse() throws Exception
    {
        start(new EmptyServerHandler());

        final CountDownLatch latch = new CountDownLatch(1);
        final String host = "localhost";
        final int port = connector.getLocalPort();
        client.newRequest(host, port)
                .scheme(scheme)
                .onRequestBegin(request ->
                {
                    HttpDestinationOverHTTP destination = (HttpDestinationOverHTTP)client.getDestination(scheme, host, port);
                    destination.getConnectionPool().getActiveConnections().peek().close();
                })
                .send(new Response.Listener.Adapter()
                {
                    @Override
                    public void onComplete(Result result)
                    {
                        latch.countDown();
                    }
                });

        Assert.assertTrue(latch.await(5, TimeUnit.SECONDS));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 5
Lines of Code		44
-    
    public void testRequestIdleTimeout() throws Exception
    {
        final long idleTimeout = 1000;
        start(new AbstractHandler()
        {
            @Override
            public void handle(String target, org.eclipse.jetty.server.Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException
            {
                try
                {
                    baseRequest.setHandled(true);
                    TimeUnit.MILLISECONDS.sleep(2 * idleTimeout);
                }
                catch (InterruptedException x)
                {
                    throw new ServletException(x);
                }
            }
        });

        final String host = "localhost";
        final int port = connector.getLocalPort();
        try
        {
            client.newRequest(host, port)
                    .scheme(scheme)
                    .idleTimeout(idleTimeout, TimeUnit.MILLISECONDS)
                    .timeout(3 * idleTimeout, TimeUnit.MILLISECONDS)
                    .send();
            Assert.fail();
        }
        catch (ExecutionException expected)
        {
            Assert.assertTrue(expected.getCause() instanceof TimeoutException);
        }

        // Make another request without specifying the idle timeout, should not fail
        ContentResponse response = client.newRequest(host, port)
                .scheme(scheme)
                .timeout(3 * idleTimeout, TimeUnit.MILLISECONDS)
                .send();

        Assert.assertNotNull(response);
        Assert.assertEquals(200, response.getStatus());@@@+        assertEquals(200, response.getStatus());@@@+        assertArrayEquals(data, response.getContent());@@@     }
```
```
Cyclomatic Complexity	 1
Assertions		 3
Lines of Code		16
-    
    public void testFoldedField7230() throws Exception
    {
        ByteBuffer buffer = BufferUtil.toBuffer(
                "GET / HTTP/1.0\r\n" +
                        "Host: localhost\r\n" +
                        "Name: value\r\n" +
                        " extra\r\n" +
                        "\r\n");

        HttpParser.RequestHandler handler = new Handler();
        HttpParser parser = new HttpParser(handler, 4096, HttpCompliance.RFC7230);
        parseAll(parser, buffer);

        Assert.assertThat(_bad, Matchers.notNullValue());
        Assert.assertThat(_bad, Matchers.containsString("Header Folding"));
        Assert.assertNull(_complianceViolation);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		14
-    
    public void testWhiteSpaceInName() throws Exception
    {
        ByteBuffer buffer = BufferUtil.toBuffer(
                "GET / HTTP/1.0\r\n" +
                        "Host: localhost\r\n" +
                        "N ame: value\r\n" +
                        "\r\n");

        HttpParser.RequestHandler handler = new Handler();
        HttpParser parser = new HttpParser(handler, 4096, HttpCompliance.RFC7230);
        parseAll(parser, buffer);

        Assert.assertThat(_bad, Matchers.notNullValue());
        Assert.assertThat(_bad, Matchers.containsString("Illegal character"));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		14
-    
    public void testWhiteSpaceAfterName() throws Exception
    {
        ByteBuffer buffer = BufferUtil.toBuffer(
                "GET / HTTP/1.0\r\n" +
                        "Host: localhost\r\n" +
                        "Name : value\r\n" +
                        "\r\n");

        HttpParser.RequestHandler handler = new Handler();
        HttpParser parser = new HttpParser(handler, 4096, HttpCompliance.RFC7230);
        parseAll(parser, buffer);

        Assert.assertThat(_bad, Matchers.notNullValue());
        Assert.assertThat(_bad, Matchers.containsString("Illegal character"));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		14
-    
    public void testNoColon7230() throws Exception
    {
        ByteBuffer buffer = BufferUtil.toBuffer(
                "GET / HTTP/1.0\r\n" +
                        "Host: localhost\r\n" +
                        "Name\r\n" +
                        "\r\n");

        HttpParser.RequestHandler handler = new Handler();
        HttpParser parser = new HttpParser(handler,HttpCompliance.RFC7230);
        parseAll(parser, buffer);
        Assert.assertThat(_bad, Matchers.containsString("Illegal character"));
        Assert.assertNull(_complianceViolation);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		5
-    
    public void testGetMimeByExtension_Png_MultiDot()
    {
        assertMimeTypeByExtension("image/png","org.eclipse.jetty.Logo.png");
    }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		5
-    
    public void testGetMimeByExtension_Png_DeepPath()
    {
        assertMimeTypeByExtension("image/png","/org/eclipse/jetty/Logo.png");
    }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		6
-    
    public void testGetMimeByExtension_Text()
    {
        assertMimeTypeByExtension("text/plain","test.txt");
        assertMimeTypeByExtension("text/plain","TEST.TXT");
    }
```
```
Cyclomatic Complexity	 2
Assertions		 8
Lines of Code		61
-    
    public void testServerTwoDataFramesWithStalledStream() throws Exception
    {
        // Frames in queue = DATA1, DATA2.
        // Server writes part of DATA1, then stalls.
        // A window update unstalls the session, verify that the data is correctly sent.

        Random random = new Random();
        final byte[] chunk1 = new byte[1024];
        random.nextBytes(chunk1);
        final byte[] chunk2 = new byte[2048];
        random.nextBytes(chunk2);

        // Two SETTINGS frames: the initial after the preface,
        // and the explicit where we set the stream window size to zero.
        final AtomicReference<CountDownLatch> settingsLatch = new AtomicReference<>(new CountDownLatch(2));
        final CountDownLatch dataLatch = new CountDownLatch(1);
        start(new ServerSessionListener.Adapter()
        {
            @Override
            public void onSettings(Session session, SettingsFrame frame)
            {
                settingsLatch.get().countDown();
            }

            @Override
            public Stream.Listener onNewStream(Stream stream, HeadersFrame frame)
            {
                stream.data(new DataFrame(stream.getId(), ByteBuffer.wrap(chunk1), false), Callback.NOOP);
                stream.data(new DataFrame(stream.getId(), ByteBuffer.wrap(chunk2), true), Callback.NOOP);
                dataLatch.countDown();
                return null;
            }
        });

        Session session = newClient(new Session.Listener.Adapter());
        Map<Integer, Integer> settings = new HashMap<>();
        settings.put(SettingsFrame.INITIAL_WINDOW_SIZE, 0);
        session.settings(new SettingsFrame(settings, false), Callback.NOOP);
        Assert.assertTrue(settingsLatch.get().await(5, TimeUnit.SECONDS));

        byte[] content = new byte[chunk1.length + chunk2.length];
        final ByteBuffer buffer = ByteBuffer.wrap(content);
        MetaData.Request metaData = newRequest("GET", new HttpFields());
        HeadersFrame requestFrame = new HeadersFrame(metaData, null, true);
        final CountDownLatch responseLatch = new CountDownLatch(1);
        session.newStream(requestFrame, new Promise.Adapter<>(), new Stream.Listener.Adapter()
        {
            @Override
            public void onData(Stream stream, DataFrame frame, Callback callback)
            {
                buffer.put(frame.getData());
                callback.succeeded();
                if (frame.isEndStream())
                    responseLatch.countDown();
            }
        });
        Assert.assertTrue(dataLatch.await(5, TimeUnit.SECONDS));

        // Now we have the 2 DATA frames queued in the server.

        // Unstall the stream window.
        settingsLatch.set(new CountDownLatch(1));
        settings.clear();
        settings.put(SettingsFrame.INITIAL_WINDOW_SIZE, chunk1.length / 2);
        session.settings(new SettingsFrame(settings, false), Callback.NOOP);
        Assert.assertTrue(settingsLatch.get().await(5, TimeUnit.SECONDS));

        Assert.assertTrue(responseLatch.await(5, TimeUnit.SECONDS));

        // Check that the data is sent correctly.
        byte[] expected = new byte[content.length];
        System.arraycopy(chunk1, 0, expected, 0, chunk1.length);
        System.arraycopy(chunk2, 0, expected, chunk1.length, chunk2.length);
        Assert.assertArrayEquals(expected, content);@@@+        assertTrue(latch.await(15, TimeUnit.SECONDS));@@@+        assertArrayEquals(data, bytes);@@@     }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		19
-    
    @Ignore("ignore, used in testing jconsole atm")
    public void testThreadPool() throws Exception
    {

        Derived derived = new Derived();
        ObjectMBean mbean = (ObjectMBean)ObjectMBean.mbeanFor(derived);

        ObjectMBean managed = (ObjectMBean)ObjectMBean.mbeanFor(derived.getManagedInstance());
        mbean.setMBeanContainer(container);
        managed.setMBeanContainer(container);

        QueuedThreadPool qtp = new QueuedThreadPool();

        ObjectMBean bqtp = (ObjectMBean)ObjectMBean.mbeanFor(qtp);

        bqtp.getMBeanInfo();

        container.beanAdded(null,derived);
        container.beanAdded(null,derived.getManagedInstance());
        container.beanAdded(null,mbean);
        container.beanAdded(null,managed);
        container.beanAdded(null,qtp);


        Thread.sleep(10000000);

    }
```
```
Cyclomatic Complexity	 1
Assertions		 8
Lines of Code		13
-    
    public void testMethodNameMining() throws Exception
    {
        ObjectMBean mbean = new ObjectMBean(new Derived());

        Assert.assertEquals("fullName",mbean.toVariableName("getFullName"));
        Assert.assertEquals("fullName",mbean.toVariableName("getfullName"));
        Assert.assertEquals("fullName",mbean.toVariableName("isFullName"));
        Assert.assertEquals("fullName",mbean.toVariableName("isfullName"));
        Assert.assertEquals("fullName",mbean.toVariableName("setFullName"));
        Assert.assertEquals("fullName",mbean.toVariableName("setfullName"));
        Assert.assertEquals("fullName",mbean.toVariableName("FullName"));
        Assert.assertEquals("fullName",mbean.toVariableName("fullName"));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		5
-    
    public void assertAllBundlesActiveOrResolved()
    {
        TestOSGiUtil.assertAllBundlesActiveOrResolved(bundleContext);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		5
-    
    public void assertAllBundlesActiveOrResolved()
    {
        TestOSGiUtil.assertAllBundlesActiveOrResolved(bundleContext);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		6
-    
    public void assertAllBundlesActiveOrResolved()
    {
        TestOSGiUtil.assertAllBundlesActiveOrResolved(bundleContext);
        TestOSGiUtil.debugBundles(bundleContext);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		5
-    
    public void testHttpService() throws Exception
    {
        TestOSGiUtil.testHttpServiceGreetings(bundleContext, "http", TestJettyOSGiBootCore.DEFAULT_HTTP_PORT);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		6
-    
    public void assertAllBundlesActiveOrResolved()
    {
        TestOSGiUtil.debugBundles(bundleContext);
        TestOSGiUtil.assertAllBundlesActiveOrResolved(bundleContext);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		5
-    
    public void testHttpService() throws Exception
    {
        TestOSGiUtil.testHttpServiceGreetings(bundleContext, "http", TestJettyOSGiBootCore.DEFAULT_HTTP_PORT);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		9
-    
    public void testWin2kWithIE6() throws Exception
    {
        HttpFields fields = _request.getHttpFields();
        fields.add("User-Agent", "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.0)");

        String result = _rule.matchAndApply(_request.getRequestURI(), _request, _response);

        assertEquals(_request.getRequestURI(), result);
        assertEquals(HttpHeaderValue.CLOSE.asString(), _response.getHeader(HttpHeader.CONNECTION.asString()));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		9
-    
    public void testWin2kWithIE7() throws Exception
    {
        HttpFields fields = _request.getHttpFields();
        fields.add("User-Agent", "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.0)");

        String result = _rule.matchAndApply(_request.getRequestURI(), _request, _response);

        assertEquals(null, result);
        assertEquals(null, _response.getHeader(HttpHeader.CONNECTION.asString()));
    }
```
```
Cyclomatic Complexity	 2
Assertions		 2
Lines of Code		12
-    
    public void testErrorStatusNoReason() throws IOException
    {
        for (int i = 400; i < 600; i++)
        {
            _rule.setCode("" + i);
            _rule.apply(null, _request, _response);

            assertEquals(i, _response.getStatus());
            assertEquals("", _response.getReason());
            super.reset();
        }
    }
```
```
Cyclomatic Complexity	 -1
Assertions		 7
Lines of Code		64
-    (timeout=60000)
    public void testMaxIdleWithRequest10ClientIgnoresClose() throws Exception
    {
        final Exchanger<EndPoint> exchanger = new Exchanger<>();
        configureServer(new HelloWorldHandler()
        {
            @Override
            public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException,
                    ServletException
            {
                try
                {
                    exchanger.exchange(baseRequest.getHttpChannel().getEndPoint());
                }
                catch (Exception e)
                {
                    e.printStackTrace();
                }
                super.handle(target, baseRequest, request, response);
            }

        });
        Socket client=newSocket(_serverURI.getHost(),_serverURI.getPort());
        client.setSoTimeout(10000);

        Assert.assertFalse(client.isClosed());

        OutputStream os=client.getOutputStream();
        InputStream is=client.getInputStream();

        os.write((
                "GET / HTTP/1.0\r\n"+
                "host: "+_serverURI.getHost()+":"+_serverURI.getPort()+"\r\n"+
                "connection: close\r\n"+
        "\r\n").getBytes("utf-8"));
        os.flush();

        // Get the server side endpoint
        EndPoint endPoint = exchanger.exchange(null,10,TimeUnit.SECONDS);
        if (endPoint instanceof SslConnection.DecryptedEndPoint)
            endPoint=endPoint.getConnection().getEndPoint();

        // read the response
        String result=IO.toString(is);
        Assert.assertThat("OK",result, Matchers.containsString("200 OK"));

        // check client reads EOF
        Assert.assertEquals(-1, is.read());
        Assert.assertTrue(endPoint.isOutputShutdown());

        Thread.sleep(2 * MAX_IDLE_TIME);

        // further writes will get broken pipe or similar
        try
        {
            long end=System.currentTimeMillis()+MAX_IDLE_TIME+3000;
            while (System.currentTimeMillis()<end)
            {
                os.write("THIS DATA SHOULD NOT BE PARSED!\n\n".getBytes("utf-8"));
                os.flush();
                Thread.sleep(100);
            }
            Assert.fail("half close should have timed out");
        }
        catch(SocketException e)
        {
            // expected

            // Give the SSL onClose time to act
            Thread.sleep(100);
        }@@@+        TimeUnit.MILLISECONDS.sleep(2 * MAX_IDLE_TIME);@@@ @@@         // check the server side is closed
        Assert.assertFalse(endPoint.isOpen());@@@+        assertFalse(endPoint.isOpen());@@@+        Object transport = endPoint.getTransport();@@@+        if (transport instanceof Channel)@@@+            assertFalse(((Channel)transport).isOpen());@@@     }
```
```
Cyclomatic Complexity	 1
Assertions		 16
Lines of Code		32
-    
    public void testSslSession() throws Exception
    {
        _customizer.setSslIsSecure(false);
        String response=_connector.getResponse(
             "GET / HTTP/1.1\n"+
             "Host: myhost\n"+
             "Proxy-Ssl-Id: Wibble\n"+
             "\n");
        
        assertThat(response, Matchers.containsString("200 OK"));
        assertEquals("http",_results.poll());
        assertEquals("myhost",_results.poll());
        assertEquals("80",_results.poll());
        assertEquals("0.0.0.0",_results.poll());
        assertEquals("0",_results.poll());
        assertFalse(_wasSecure.get());
        assertEquals("Wibble",_sslSession.get());
      
        _customizer.setSslIsSecure(true);  
        response=_connector.getResponse(
             "GET / HTTP/1.1\n"+
             "Host: myhost\n"+
             "Proxy-Ssl-Id: 0123456789abcdef\n"+
             "\n");
        
        assertThat(response, Matchers.containsString("200 OK"));
        assertEquals("https",_results.poll());
        assertEquals("myhost",_results.poll());
        assertEquals("443",_results.poll());
        assertEquals("0.0.0.0",_results.poll());
        assertEquals("0",_results.poll());
        assertTrue(_wasSecure.get());
        assertEquals("0123456789abcdef",_sslSession.get());
    }
```
```
Cyclomatic Complexity	 1
Assertions		 16
Lines of Code		32
-    
    public void testSslCertificate() throws Exception
    {
        _customizer.setSslIsSecure(false);
        String response=_connector.getResponse(
             "GET / HTTP/1.1\n"+
             "Host: myhost\n"+
             "Proxy-auth-cert: Wibble\n"+
             "\n");
        
        assertThat(response, Matchers.containsString("200 OK"));
        assertEquals("http",_results.poll());
        assertEquals("myhost",_results.poll());
        assertEquals("80",_results.poll());
        assertEquals("0.0.0.0",_results.poll());
        assertEquals("0",_results.poll());
        assertFalse(_wasSecure.get());
        assertEquals("Wibble",_sslCertificate.get());
        
      
        _customizer.setSslIsSecure(true);  
        response=_connector.getResponse(
             "GET / HTTP/1.1\n"+
             "Host: myhost\n"+
             "Proxy-auth-cert: 0123456789abcdef\n"+
             "\n");
        
        assertThat(response, Matchers.containsString("200 OK"));
        assertEquals("https",_results.poll());
        assertEquals("myhost",_results.poll());
        assertEquals("443",_results.poll());
        assertEquals("0.0.0.0",_results.poll());
        assertEquals("0",_results.poll());
        assertTrue(_wasSecure.get());
        assertEquals("0123456789abcdef",_sslCertificate.get());
    }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		7
-    
    public void testHttp09_NoVersion() throws Exception
    {
        String request = "GET / HTTP/0.9\r\n\r\n";
        String response = connector.getResponses(request);
        assertThat(response, containsString("400 Bad Version"));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		9
-    
    public void testHttp09_NoHeaders() throws Exception
    {
        connector.getConnectionFactory(HttpConnectionFactory.class).setHttpCompliance(HttpCompliance.RFC2616);
        // header looking like another request is ignored 
        String request = "GET /one\r\nGET :/two\r\n\r\n";
        String response = connector.getResponses(request);
        assertThat(response, containsString("pathInfo=/"));
        assertThat(response, not(containsString("two")));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 5
Lines of Code		14
-    
    public void testHttp09_MultipleRequests() throws Exception
    {
        connector.getConnectionFactory(HttpConnectionFactory.class).setHttpCompliance(HttpCompliance.RFC2616);
        // Verify that LocalConnector supports pipelining with HTTP/1.1.
        String requests = "GET /?id=123 HTTP/1.1\r\nHost: localhost\r\n\r\nGET /?id=456 HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
        String responses = connector.getResponses(requests);
        assertThat(responses, containsString("id=123"));
        assertThat(responses, containsString("id=456"));

        // Verify that pipelining does not work with HTTP/0.9.
        requests = "GET /?id=123\r\n\r\nGET /?id=456\r\n\r\nGET /?id=789\r\n\r\n";
        responses = connector.getResponses(requests);
        assertThat(responses, containsString("id=123"));
        assertThat(responses, not(containsString("id=456")));
        assertThat(responses, not(containsString("id=789")));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		26
-    
    public void testHttp11_ChunkedBodyTruncation() throws Exception
    {
        String request = "POST /?id=123 HTTP/1.1\r\n" +
                "Host: local\r\n" +
                "Transfer-Encoding: chunked\r\n" +
                "Content-Type: text/plain\r\n" +
                "Connection: close\r\n" +
                "\r\n" +
                "1ff00000008\r\n" +
                "abcdefgh\r\n" +
                "\r\n" +
                "0\r\n" +
                "\r\n" +
                "POST /?id=bogus HTTP/1.1\r\n" +
                "Content-Length: 5\r\n" +
                "Host: dummy-host.example.com\r\n" +
                "\r\n" +
                "12345";
        String responses = connector.getResponses(request);
        
        assertThat(responses,anyOf(
                isEmptyOrNullString(),
                containsString(" 413 "),
                containsString(" 500 ")
                ));        
    }
```
```
Cyclomatic Complexity	 3
Assertions		 1
Lines of Code		38
-    
    public void testHttp11_MultipleContentLength() throws Exception
    {
        HttpParser.LOG.info("badMessage: 400 Bad messages EXPECTED...");
        int contentLengths[][]= {
                {0,8},
                {8,0},
                {8,8},
                {0,8,0},
                {1,2,3,4,5,6,7,8},
                {8,2,1},
                {0,0},
                {8,0,8},
                {-1,8},
                {8,-1},
                {-1,8,-1},
                {-1,-1},
                {8,-1,8},
        };

        for(int x = 0; x < contentLengths.length; x++)
        {
            StringBuilder request = new StringBuilder();
            request.append("POST /?id=").append(Integer.toString(x)).append(" HTTP/1.1\r\n");
            request.append("Host: local\r\n");
            int clen[] = contentLengths[x];
            for(int n = 0; n<clen.length; n++)
            {
                request.append("Content-Length: ").append(Integer.toString(clen[n])).append("\r\n");
            }
            request.append("Content-Type: text/plain\r\n");
            request.append("Connection: close\r\n");
            request.append("\r\n");
            request.append("abcdefgh"); // actual content of 8 bytes
    
            String rawResponses = connector.getResponses(request.toString());
            HttpTester.Response response = HttpTester.parseResponse(rawResponses);
            assertThat("Response.status", response.getStatus(), is(HttpServletResponse.SC_BAD_REQUEST));
        }
    }
```
```
Cyclomatic Complexity	 4
Assertions		 1
Lines of Code		33
-    
    public void testHttp11_ContentLengthAndChunk() throws Exception
    {
        HttpParser.LOG.info("badMessage: 400 Bad messages EXPECTED...");
        int contentLengths[][]= {
                {-1,8},
                {8,-1},
                {8,-1,8},
        };

        for(int x = 0; x < contentLengths.length; x++)
        {
            StringBuilder request = new StringBuilder();
            request.append("POST /?id=").append(Integer.toString(x)).append(" HTTP/1.1\r\n");
            request.append("Host: local\r\n");
            int clen[] = contentLengths[x];
            for(int n = 0; n<clen.length; n++)
            {
                if (clen[n]==-1)
                    request.append("Transfer-Encoding: chunked\r\n");
                else
                    request.append("Content-Length: ").append(Integer.toString(clen[n])).append("\r\n");
            }
            request.append("Content-Type: text/plain\r\n");
            request.append("Connection: close\r\n");
            request.append("\r\n");
            request.append("8;\r\n"); // chunk header
            request.append("abcdefgh"); // actual content of 8 bytes
            request.append("\r\n0;\r\n"); // last chunk
    
            String rawResponses = connector.getResponses(request.toString());
            HttpTester.Response response = HttpTester.parseResponse(rawResponses);
            assertThat("Response.status", response.getStatus(), is(HttpServletResponse.SC_BAD_REQUEST));
        }
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		11
-    
    public void testNoPath() throws Exception
    {
        String response=connector.getResponses("GET http://localhost:80 HTTP/1.1\r\n"+
                "Host: localhost:80\r\n"+
                "Connection: close\r\n"+
                "\r\n");

        int offset=0;
        offset = checkContains(response,offset,"HTTP/1.1 200");
        checkContains(response,offset,"pathInfo=/");
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		12
-    
    public void testDate() throws Exception
    {
        String response=connector.getResponse("GET / HTTP/1.1\r\n"+
                "Host: localhost:80\r\n"+
                "Connection: close\r\n"+
                "\r\n");

        int offset=0;
        offset = checkContains(response,offset,"HTTP/1.1 200");
        offset = checkContains(response,offset,"Date: ");
        checkContains(response,offset,"pathInfo=/");
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		12
-    
    public void testSetDate() throws Exception
    {
        String response=connector.getResponses("GET /?date=1+Jan+1970 HTTP/1.1\r\n"+
                "Host: localhost:80\r\n"+
                "Connection: close\r\n"+
                "\r\n");

        int offset=0;
        offset = checkContains(response,offset,"HTTP/1.1 200");
        offset = checkContains(response,offset,"Date: 1 Jan 1970");
        checkContains(response,offset,"pathInfo=/");
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		8
-    
    public void testBadNoPath() throws Exception
    {
        String response=connector.getResponses("GET http://localhost:80/../cheat HTTP/1.1\r\n"+
                "Host: localhost:80\r\n"+
                "\r\n");
        checkContains(response,0,"HTTP/1.1 400");
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		7
-    
    public void testOKPathDotDotPath() throws Exception
    {
        String response=connector.getResponses("GET /ooops/../path HTTP/1.0\r\nHost: localhost:80\r\n\n");
        checkContains(response,0,"HTTP/1.1 200 OK");
        checkContains(response,0,"pathInfo=/path");
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		6
-    
    public void testBadPathDotDotPath() throws Exception
    {
        String response=connector.getResponses("GET /ooops/../../path HTTP/1.0\r\nHost: localhost:80\r\n\n");
        checkContains(response,0,"HTTP/1.1 400 Bad URI");
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		7
-    
    public void testOKPathEncodedDotDotPath() throws Exception
    {
        String response=connector.getResponses("GET /ooops/%2e%2e/path HTTP/1.0\r\nHost: localhost:80\r\n\n");
        checkContains(response,0,"HTTP/1.1 200 OK");
        checkContains(response,0,"pathInfo=/path");
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		6
-    
    public void testBadPathEncodedDotDotPath() throws Exception
    {
        String response=connector.getResponses("GET /ooops/%2e%2e/%2e%2e/path HTTP/1.0\r\nHost: localhost:80\r\n\n");
        checkContains(response,0,"HTTP/1.1 400 Bad URI");
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		6
-    
    public void testBadDotDotPath() throws Exception
    {
        String response=connector.getResponses("GET ../path HTTP/1.0\r\nHost: localhost:80\r\n\n");
        checkContains(response,0,"HTTP/1.1 400 Bad URI");
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		6
-    
    public void testBadSlashDotDotPath() throws Exception
    {
        String response=connector.getResponses("GET /../path HTTP/1.0\r\nHost: localhost:80\r\n\n");
        checkContains(response,0,"HTTP/1.1 400 Bad URI");
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		6
-    
    public void testEncodedBadDotDotPath() throws Exception
    {
        String response=connector.getResponses("GET %2e%2e/path HTTP/1.0\r\nHost: localhost:80\r\n\n");
        checkContains(response,0,"HTTP/1.1 400 Bad URI");
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		10
-    
    public void test_0_9() throws Exception
    {
        connector.getConnectionFactory(HttpConnectionFactory.class).setHttpCompliance(HttpCompliance.RFC2616);
        String response=connector.getResponses("GET /R1\n");

        int offset=0;
        checkNotContained(response,offset,"HTTP/1.1");
        checkNotContained(response,offset,"200");
        checkContains(response,offset,"pathInfo=/R1");
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		11
-    
    public void testSimple() throws Exception
    {
        String response=connector.getResponses("GET /R1 HTTP/1.1\r\n"+
                "Host: localhost\r\n"+
                "Connection: close\r\n"+
                "\r\n");

        int offset=0;
        offset = checkContains(response,offset,"HTTP/1.1 200");
        checkContains(response,offset,"/R1");
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		15
-    
    public void testEmptyChunk() throws Exception
    {
        String response=connector.getResponse("GET /R1 HTTP/1.1\r\n"+
                "Host: localhost\r\n"+
                "Transfer-Encoding: chunked\r\n"+
                "Content-Type: text/plain\r\n"+
                "Connection: close\r\n"+
                "\r\n"+
                "0\r\n" +
                "\r\n");

        int offset=0;
        offset = checkContains(response,offset,"HTTP/1.1 200");
        checkContains(response,offset,"/R1");
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		18
-    
    public void testChunk() throws Exception
    {
        String response=connector.getResponse("GET /R1 HTTP/1.1\r\n"+
                "Host: localhost\r\n"+
                "Transfer-Encoding: chunked\r\n"+
                "Content-Type: text/plain\r\n"+
                "Connection: close\r\n"+
                "\r\n"+
                "A\r\n" +
                "0123456789\r\n"+
                "0\r\n" +
                "\r\n");

        int offset=0;
        offset = checkContains(response,offset,"HTTP/1.1 200");
        offset = checkContains(response,offset,"/R1");
        checkContains(response,offset,"0123456789");
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		19
-    
    public void testChunkTrailer() throws Exception
    {
        String response=connector.getResponse("GET /R1 HTTP/1.1\r\n"+
                "Host: localhost\r\n"+
                "Transfer-Encoding: chunked\r\n"+
                "Content-Type: text/plain\r\n"+
                "Connection: close\r\n"+
                "\r\n"+
                "A\r\n" +
                "0123456789\r\n"+
                "0\r\n" +
                "Trailer: ignored\r\n" +
                "\r\n");

        int offset=0;
        offset = checkContains(response,offset,"HTTP/1.1 200");
        offset = checkContains(response,offset,"/R1");
        checkContains(response,offset,"0123456789");
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		17
-    
    public void testChunkNoTrailer() throws Exception
    {
        String response=connector.getResponse("GET /R1 HTTP/1.1\r\n"+
                "Host: localhost\r\n"+
                "Transfer-Encoding: chunked\r\n"+
                "Content-Type: text/plain\r\n"+
                "Connection: close\r\n"+
                "\r\n"+
                "A\r\n" +
                "0123456789\r\n"+
                "0\r\n");

        int offset=0;
        offset = checkContains(response,offset,"HTTP/1.1 200");
        offset = checkContains(response,offset,"/R1");
        checkContains(response,offset,"0123456789");
    }
```
```
Cyclomatic Complexity	 5
Assertions		 3
Lines of Code		47
-    
    public void testHead() throws Exception
    {
        String responsePOST=connector.getResponses("POST /R1 HTTP/1.1\r\n"+
            "Host: localhost\r\n"+
            "Connection: close\r\n"+
            "\r\n");

        String responseHEAD=connector.getResponses("HEAD /R1 HTTP/1.1\r\n"+
            "Host: localhost\r\n"+
            "Connection: close\r\n"+
            "\r\n");

        String postLine;
        boolean postDate=false;
        Set<String> postHeaders = new HashSet<>();
        try(BufferedReader in = new BufferedReader(new StringReader(responsePOST)))
        {
            postLine = in.readLine();
            String line=in.readLine();
            while (line!=null && line.length()>0)
            {
                if (line.startsWith("Date:"))
                    postDate=true;
                else
                    postHeaders.add(line);
                line=in.readLine();
            }
        }
        String headLine;
        boolean headDate=false;
        Set<String> headHeaders = new HashSet<>();
        try(BufferedReader in = new BufferedReader(new StringReader(responseHEAD)))
        {
            headLine = in.readLine();
            String line=in.readLine();
            while (line!=null && line.length()>0)
            {
                if (line.startsWith("Date:"))
                    headDate=true;
                else
                    headHeaders.add(line);
                line=in.readLine();
            }
        }

        assertThat(postLine,equalTo(headLine));
        assertThat(postDate,equalTo(headDate));
        assertTrue(postHeaders.equals(headHeaders));
    }
```
```
Cyclomatic Complexity	 5
Assertions		 3
Lines of Code		45
-    
    public void testHeadChunked() throws Exception
    {
        String responsePOST=connector.getResponse("POST /R1?no-content-length=true HTTP/1.1\r\n"+
                "Host: localhost\r\n"+
                "\r\n",false,1,TimeUnit.SECONDS);

        String responseHEAD=connector.getResponse("HEAD /R1?no-content-length=true HTTP/1.1\r\n"+
            "Host: localhost\r\n"+
            "\r\n",true,1,TimeUnit.SECONDS);

        String postLine;
        boolean postDate=false;
        Set<String> postHeaders = new HashSet<>();
        try(BufferedReader in = new BufferedReader(new StringReader(responsePOST)))
        {
            postLine = in.readLine();
            String line=in.readLine();
            while (line!=null && line.length()>0)
            {
                if (line.startsWith("Date:"))
                    postDate=true;
                else
                    postHeaders.add(line);
                line=in.readLine();
            }
        }
        String headLine;
        boolean headDate=false;
        Set<String> headHeaders = new HashSet<>();
        try(BufferedReader in = new BufferedReader(new StringReader(responseHEAD)))
        {
            headLine = in.readLine();
            String line=in.readLine();
            while (line!=null && line.length()>0)
            {
                if (line.startsWith("Date:"))
                    headDate=true;
                else
                    headHeaders.add(line);
                line=in.readLine();
            }
        }

        assertThat(postLine,equalTo(headLine));
        assertThat(postDate,equalTo(headDate));
        assertTrue(postHeaders.equals(headHeaders));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		11
-    
    public void testBadHostPort() throws Exception
    {
        Log.getLogger(HttpParser.class).info("badMessage: Number formate exception expected ...");
        String response;

        response=connector.getResponses("GET http://localhost:EXPECTED_NUMBER_FORMAT_EXCEPTION/ HTTP/1.1\r\n"+
            "Host: localhost\r\n"+
            "Connection: close\r\n"+
            "\r\n");
        checkContains(response,0,"HTTP/1.1 400");
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		8
-    
    public void testNoHost() throws Exception
    {
        String response;

        response=connector.getResponse("GET / HTTP/1.1\r\n"+
            "\r\n");
        checkContains(response,0,"HTTP/1.1 400");
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		9
-    
    public void testEmptyHost() throws Exception
    {
        String response;

        response=connector.getResponse("GET / HTTP/1.1\r\n"+
            "Host:\r\n"+
            "\r\n");
        checkContains(response,0,"HTTP/1.1 200");
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		14
-    
    public void testBadURIencoding() throws Exception
    {
        Log.getLogger(HttpParser.class).info("badMessage: bad encoding expected ...");
        String response;

        try(StacklessLogging stackless = new StacklessLogging(HttpParser.class))
        {
            response=connector.getResponse("GET /bad/encoding%1 HTTP/1.1\r\n"+
                    "Host: localhost\r\n"+
                    "Connection: close\r\n"+
                    "\r\n");
            checkContains(response,0,"HTTP/1.1 400");
        }
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		16
-    
    public void testBadUTF8FallsbackTo8859() throws Exception
    {
        Log.getLogger(HttpParser.class).info("badMessage: bad encoding expected ...");
        String response;

        response=connector.getResponses("GET /foo/bar%c0%00 HTTP/1.1\r\n"+
            "Host: localhost\r\n"+
            "Connection: close\r\n"+
            "\r\n");
        checkContains(response,0,"HTTP/1.1 200"); //now fallback to iso-8859-1

        response=connector.getResponses("GET /bad/utf8%c1 HTTP/1.1\r\n"+
            "Host: localhost\r\n"+
            "Connection: close\r\n"+
            "\r\n");
        checkContains(response,0,"HTTP/1.1 200"); //now fallback to iso-8859-1
    }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		10
-    
    public void testHandlerDoesNotSetHandled() throws Exception
    {
        DoesNotSetHandledHandler handler = new DoesNotSetHandledHandler(false);
        server.setHandler(handler);
        server.start();

        HttpTester.Response response = executeRequest();

        assertThat("response code", response.getStatus(), is(404));
        assertThat("no exceptions", handler.failure(), is(nullValue()));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 3
Lines of Code		11
-    
    public void testHandlerSetsHandledAndWritesSomeContent() throws Exception
    {
        SetHandledWriteSomeDataHandler handler = new SetHandledWriteSomeDataHandler(false);
        server.setHandler(handler);
        server.start();

        HttpTester.Response response = executeRequest();

        assertThat("response code", response.getStatus(), is(200));
        assertHeader(response, "content-length", "6");
        assertThat("no exceptions", handler.failure(), is(nullValue()));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		10
-    
    public void testHandlerSetsHandledAndWritesSomeContentAndThrow() throws Exception
    {
        SetHandledWriteSomeDataHandler handler = new SetHandledWriteSomeDataHandler(true);
        server.setHandler(handler);
        server.start();

        HttpTester.Response response = executeRequest();

        assertThat("response code", response.getStatus(), is(500));
        assertThat("no exceptions", handler.failure(), is(nullValue()));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		35
-    
    public void test3_6_b() throws Exception
    {
        int offset=0;
        // Chunked
        String response = connector.getResponses(
                "GET /R1 HTTP/1.1\n" +
                        "Host: localhost\n" +
                        "Transfer-Encoding: chunked\n" +
                        "Content-Type: text/plain\n" +
                        "\n" +
                        "2;\n" +
                        "12\n" +
                        "3;\n" +
                        "345\n" +
                        "0;\n\n" +

                        "GET /R2 HTTP/1.1\n" +
                        "Host: localhost\n" +
                        "Transfer-Encoding: chunked\n" +
                        "Content-Type: text/plain\n" +
                        "\n" +
                        "4;\n" +
                        "6789\n" +
                        "5;\n" +
                        "abcde\n" +
                        "0;\n\n" +

                        "GET /R3 HTTP/1.1\n" +
                        "Host: localhost\n" +
                        "Connection: close\n" +
                        "\n");
        offset=checkContains(response,offset,"HTTP/1.1 200","3.6.1 Chunking");
        offset=checkContains(response,offset,"12345","3.6.1 Chunking");
        offset=checkContains(response,offset,"HTTP/1.1 200","3.6.1 Chunking");
        offset=checkContains(response,offset,"6789abcde","3.6.1 Chunking");
        offset=checkContains(response,offset,"/R3","3.6.1 Chunking");
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		36
-    
    public void test3_6_c() throws Exception
    {
        int offset=0;
        String response = connector.getResponses(
                "POST /R1 HTTP/1.1\n" +
                        "Host: localhost\n" +
                        "Transfer-Encoding: chunked\n" +
                        "Content-Type: text/plain\n" +
                        "\n" +
                        "3;\n" +
                        "fgh\n" +
                        "3;\n" +
                        "Ijk\n" +
                        "0;\n\n" +

                        "POST /R2 HTTP/1.1\n" +
                        "Host: localhost\n" +
                        "Transfer-Encoding: chunked\n" +
                        "Content-Type: text/plain\n" +
                        "\n" +
                        "4;\n" +
                        "lmno\n" +
                        "5;\n" +
                        "Pqrst\n" +
                        "0;\n\n" +

                        "GET /R3 HTTP/1.1\n" +
                        "Host: localhost\n" +
                        "Connection: close\n" +
                        "\n");
        checkNotContained(response,"HTTP/1.1 100","3.6.1 Chunking");
        offset=checkContains(response,offset,"HTTP/1.1 200","3.6.1 Chunking");
        offset=checkContains(response,offset,"fghIjk","3.6.1 Chunking");
        offset=checkContains(response,offset,"HTTP/1.1 200","3.6.1 Chunking");
        offset=checkContains(response,offset,"lmnoPqrst","3.6.1 Chunking");
        offset=checkContains(response,offset,"/R3","3.6.1 Chunking");
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		24
-    
    public void test3_6_d() throws Exception
    {
        int offset=0;
        // Chunked and keep alive
        String response = connector.getResponses(
                "GET /R1 HTTP/1.1\n" +
                        "Host: localhost\n" +
                        "Transfer-Encoding: chunked\n" +
                        "Content-Type: text/plain\n" +
                        "Connection: keep-alive\n" +
                        "\n" +
                        "3;\n" +
                        "123\n" +
                        "3;\n" +
                        "456\n" +
                        "0;\n\n" +

                        "GET /R2 HTTP/1.1\n" +
                        "Host: localhost\n" +
                        "Connection: close\n" +
                        "\n");
        offset=checkContains(response,offset,"HTTP/1.1 200","3.6.1 Chunking")+10;
        offset=checkContains(response,offset,"123456","3.6.1 Chunking");
        offset=checkContains(response,offset,"/R2","3.6.1 Chunking")+10;
    }
```
```
Cyclomatic Complexity	 1
Assertions		 6
Lines of Code		14
-    
    public void test3_9() throws Exception
    {
        HttpFields fields=new HttpFields();

        fields.put("Q","bbb;q=0.5,aaa,ccc;q=0.002,d;q=0,e;q=0.0001,ddd;q=0.001,aa2,abb;q=0.7");
        Enumeration<String> qualities=fields.getValues("Q",", \t");
        List<String> list=HttpFields.qualityList(qualities);
        assertEquals("Quality parameters","aaa",HttpFields.valueParameters(list.get(0),null));
        assertEquals("Quality parameters","aa2",HttpFields.valueParameters(list.get(1),null));
        assertEquals("Quality parameters","abb",HttpFields.valueParameters(list.get(2),null));
        assertEquals("Quality parameters","bbb",HttpFields.valueParameters(list.get(3),null));
        assertEquals("Quality parameters","ccc",HttpFields.valueParameters(list.get(4),null));
        assertEquals("Quality parameters","ddd",HttpFields.valueParameters(list.get(5),null));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		22
-    
    public void test8_2_3() throws Exception
    {
        int offset=0;
        // Expect 100
        LocalConnector.LocalEndPoint endp =connector.executeRequest("GET /R1 HTTP/1.1\n"+
                "Host: localhost\n"+
                "Connection: close\n"+
                "Expect: 100-continue\n"+
                "Content-Type: text/plain\n"+
                "Content-Length: 8\n"+
                "\n");
        Thread.sleep(200);
        String infomational= endp.takeOutputString();
        offset=checkContains(infomational,offset,"HTTP/1.1 100 ","8.2.3 expect 100")+1;
        checkNotContained(infomational,offset,"HTTP/1.1 200","8.2.3 expect 100");

        endp.addInput("654321\015\012");

        Thread.sleep(200);
        String response= endp.takeOutputString();
        offset=0;
        offset=checkContains(response,offset,"HTTP/1.1 200","8.2.3 expect 100")+1;
        offset=checkContains(response,offset,"654321","8.2.3 expect 100")+1;
    }
```
```
Cyclomatic Complexity	 1
Assertions		 4
Lines of Code		13
-    
    public void testIPv6() throws Exception
    {
        String response=_connector.getResponses("PROXY UNKNOWN eeee:eeee:eeee:eeee:eeee:eeee:eeee:eeee ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff 65535 65535\r\n"+
                "GET /path HTTP/1.1\n"+
                "Host: server:80\n"+
                "Connection: close\n"+
                "\n");
        
        Assert.assertThat(response,Matchers.containsString("HTTP/1.1 200"));
        Assert.assertThat(response,Matchers.containsString("pathInfo=/path"));
        Assert.assertThat(response,Matchers.containsString("remote=eeee:eeee:eeee:eeee:eeee:eeee:eeee:eeee:65535"));
        Assert.assertThat(response,Matchers.containsString("local=ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff:65535"));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		10
-    
    public void testTooLong() throws Exception
    {
        String response=_connector.getResponses("PROXY TOOLONG!!! eeee:eeee:eeee:eeee:0000:0000:0000:0000 ffff:ffff:ffff:ffff:0000:0000:0000:0000 65535 65535\r\n"+
                "GET /path HTTP/1.1\n"+
                "Host: server:80\n"+
                "Connection: close\n"+
                "\n");
        
        Assert.assertThat(response,Matchers.equalTo(""));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		7
-    
    public void testNotComplete() throws Exception
    {
        _connector.setIdleTimeout(100);
        String response=_connector.getResponses("PROXY TIMEOUT");
        Assert.assertThat(response,Matchers.equalTo(""));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		10
-    
    public void testBadChar() throws Exception
    {
        String response=_connector.getResponses("PROXY\tTCP 1.2.3.4 5.6.7.8 111 222\r\n"+
                "GET /path HTTP/1.1\n"+
                "Host: server:80\n"+
                "Connection: close\n"+
                "\n");
        Assert.assertThat(response,Matchers.equalTo(""));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		10
-    
    public void testBadCRLF() throws Exception
    {
        String response=_connector.getResponses("PROXY TCP 1.2.3.4 5.6.7.8 111 222\r \n"+
                "GET /path HTTP/1.1\n"+
                "Host: server:80\n"+
                "Connection: close\n"+
                "\n");
        Assert.assertThat(response,Matchers.equalTo(""));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		5
-    
    public void testStartStopSamePortDifferentKey() throws Exception
    {
        testStartStop(true);
    }
```
```
Cyclomatic Complexity	 2
Assertions		 1
Lines of Code		31
-    
    public void testOnErrorCalledForExceptionAfterStartAsync() throws Exception
    {
        RuntimeException exception = new RuntimeException();
        start(new HttpServlet()
        {
            @Override
            protected void service(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException
            {
                AsyncContext asyncContext = request.startAsync();
                asyncContext.addListener(new AsyncListenerAdapter()
                {
                    @Override
                    public void onError(AsyncEvent event) throws IOException
                    {
                        if (exception == event.getThrowable())
                            response.setStatus(HttpStatus.OK_200);
                        else
                            response.setStatus(HttpStatus.INTERNAL_SERVER_ERROR_500);
                        asyncContext.complete();
                    }
                });
                throw exception;
            }
        });

        try (StacklessLogging suppressor = new StacklessLogging(HttpChannel.class))
        {
            HttpTester.Response response = HttpTester.parseResponse(connector.getResponse(newRequest("")));
            Assert.assertEquals(HttpStatus.OK_200, response.getStatus());
        }
    }
```
```
Cyclomatic Complexity	 2
Assertions		 1
Lines of Code		33
-    
    public void testOnErrorCalledForExceptionThrownByOnTimeout() throws Exception
    {
        RuntimeException exception = new RuntimeException();
        start(new HttpServlet()
        {
            @Override
            protected void service(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException
            {
                AsyncContext asyncContext = request.startAsync();
                asyncContext.setTimeout(1000);
                asyncContext.addListener(new AsyncListenerAdapter()
                {
                    @Override
                    public void onTimeout(AsyncEvent event) throws IOException
                    {
                        throw exception;
                    }

                    @Override
                    public void onError(AsyncEvent event) throws IOException
                    {
                        if (exception == event.getThrowable())
                            response.setStatus(HttpStatus.OK_200);
                        else
                            response.setStatus(HttpStatus.INTERNAL_SERVER_ERROR_500);
                        asyncContext.complete();
                    }
                });
            }
        });

        HttpTester.Response response = HttpTester.parseResponse(connector.getResponse(newRequest("")));
        Assert.assertEquals(HttpStatus.OK_200, response.getStatus());
    }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		44
-    
    public void testOnErrorNotCalledForExceptionThrownByOnComplete() throws Exception
    {
        CountDownLatch errorLatch = new CountDownLatch(1);
        start(new HttpServlet()
        {
            @Override
            protected void service(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException
            {
                AsyncContext asyncContext = request.startAsync();
                asyncContext.addListener(new AsyncListenerAdapter()
                {
                    @Override
                    public void onComplete(AsyncEvent event) throws IOException
                    {
                        // Way too late to handle this exception, should only be logged.
                        throw new Error();
                    }

                    @Override
                    public void onError(AsyncEvent event) throws IOException
                    {
                        errorLatch.countDown();
                    }
                });
                new Thread(() ->
                {
                    try
                    {
                        Thread.sleep(1000);
                        response.setStatus(HttpStatus.OK_200);
                        asyncContext.complete();
                    }
                    catch (InterruptedException ignored)
                    {
                    }
                }).start();
            }
        });

        try (StacklessLogging suppressor = new StacklessLogging(HttpChannelState.class))
        {
            HttpTester.Response response = HttpTester.parseResponse(connector.getResponse(newRequest("")));
            Assert.assertEquals(HttpStatus.OK_200, response.getStatus());
            Assert.assertFalse(errorLatch.await(1, TimeUnit.SECONDS));
        }
    }
```
```
Cyclomatic Complexity	 1
Assertions		 3
Lines of Code		16
-    
    public void testStartOnTimeoutErrorComplete() throws Exception
    {
        String response=process("start=200&timeout=error&error=complete",null);
        assertThat(response,startsWith("HTTP/1.1 200 OK"));
        assertThat(__history,contains(
            "REQUEST /ctx/path/info",
            "initial",
            "start",
            "onTimeout",
            "error",
            "onError",
            "complete",
            "onComplete"));

        assertContains("COMPLETED",response);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 4
Lines of Code		19
-    
    public void testStartOnTimeoutErrorDispatch() throws Exception
    {
        String response=process("start=200&timeout=error&error=dispatch",null);
        assertThat(response,startsWith("HTTP/1.1 200 OK"));
        assertThat(__history,contains(
            "REQUEST /ctx/path/info",
            "initial",
            "start",
            "onTimeout",
            "error",
            "onError",
            "dispatch",
            "ASYNC /ctx/path/info",
            "!initial",
            "onComplete"));

        assertContains("DISPATCHED",response);@@@+        assertContains("ERROR DISPATCH", response);@@@     }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		5
-    
    public void testIfModifiedSmall() throws Exception
    {
        testIfModified("Hello World");
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		5
-    
    public void testIfModifiedLarge() throws Exception
    {
        testIfModified("Now is the time for all good men to come to the aid of the party");
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		5
-    
    public void testIfETagSmall() throws Exception
    {
        testIfETag("Hello World");
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		5
-    
    public void testIfETagLarge() throws Exception
    {
        testIfETag("Now is the time for all good men to come to the aid of the party");
    }
```
```
Cyclomatic Complexity	 1
Assertions		 4
Lines of Code		12
-    
    public void testConfig_ShouldRecurse_2() throws IOException
    {
        Path dir = testdir.getEmptyPathDir();

        // Create a few directories
        Files.createDirectories(dir.resolve("a/b/c/d"));

        PathWatcher.Config config = new PathWatcher.Config(dir);

        config.setRecurseDepth(2);
        assertThat("Config.recurse[1].shouldRecurse[./a/b/c]",config.shouldRecurseDirectory(dir.resolve("a/b/c")),is(false));
        assertThat("Config.recurse[1].shouldRecurse[./a/b]",config.shouldRecurseDirectory(dir.resolve("a/b")),is(true));
        assertThat("Config.recurse[1].shouldRecurse[./a]",config.shouldRecurseDirectory(dir.resolve("a")),is(true));
        assertThat("Config.recurse[1].shouldRecurse[./]",config.shouldRecurseDirectory(dir),is(true));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 8
Lines of Code		16
-    
    public void testConfig_ShouldRecurse_3() throws IOException
    {
        Path dir = testdir.getEmptyPathDir();
        
        //Create some deep dirs
        Files.createDirectories(dir.resolve("a/b/c/d/e/f/g"));
        
        PathWatcher.Config config = new PathWatcher.Config(dir);
        config.setRecurseDepth(PathWatcher.Config.UNLIMITED_DEPTH);
        assertThat("Config.recurse[1].shouldRecurse[./a/b/c/d/g]",config.shouldRecurseDirectory(dir.resolve("a/b/c/d/g")),is(true));
        assertThat("Config.recurse[1].shouldRecurse[./a/b/c/d/f]",config.shouldRecurseDirectory(dir.resolve("a/b/c/d/f")),is(true));
        assertThat("Config.recurse[1].shouldRecurse[./a/b/c/d/e]",config.shouldRecurseDirectory(dir.resolve("a/b/c/d/e")),is(true));
        assertThat("Config.recurse[1].shouldRecurse[./a/b/c/d]",config.shouldRecurseDirectory(dir.resolve("a/b/c/d")),is(true));
        assertThat("Config.recurse[1].shouldRecurse[./a/b/c]",config.shouldRecurseDirectory(dir.resolve("a/b/c")),is(true));
        assertThat("Config.recurse[1].shouldRecurse[./a/b]",config.shouldRecurseDirectory(dir.resolve("a/b")),is(true));
        assertThat("Config.recurse[1].shouldRecurse[./a]",config.shouldRecurseDirectory(dir.resolve("a")),is(true));
        assertThat("Config.recurse[1].shouldRecurse[./]",config.shouldRecurseDirectory(dir),is(true));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 4
Lines of Code		16
-    
    public void testSidConversion() throws Exception
    {
        String sid4 = "S-1-4-21-3623811015-3361044348-30300820";
        String sid5 = "S-1-5-21-3623811015-3361044348-30300820-1013";
        String sid6 = "S-1-6-21-3623811015-3361044348-30300820-1013-23445";
        String sid12 = "S-1-12-21-3623811015-3361044348-30300820-1013-23445-21-3623811015-3361044348-30300820-1013-23445";

        byte[] sid4Bytes = StringUtil.sidStringToBytes(sid4);
        byte[] sid5Bytes = StringUtil.sidStringToBytes(sid5);
        byte[] sid6Bytes = StringUtil.sidStringToBytes(sid6);
        byte[] sid12Bytes = StringUtil.sidStringToBytes(sid12);

        Assert.assertEquals(sid4, StringUtil.sidBytesToString(sid4Bytes));
        Assert.assertEquals(sid5, StringUtil.sidBytesToString(sid5Bytes));
        Assert.assertEquals(sid6, StringUtil.sidBytesToString(sid6Bytes));
        Assert.assertEquals(sid12, StringUtil.sidBytesToString(sid12Bytes));

    }
```
```
Cyclomatic Complexity	 1
Assertions		 17
Lines of Code		21
-    
    public void testGetDirectBuffer() throws Exception
    {
        Assert.assertEquals(1,trie.get(BufferUtil.toDirectBuffer("xhellox"),1,5).intValue());
        Assert.assertEquals(2,trie.get(BufferUtil.toDirectBuffer("xhellox"),1,2).intValue());
        Assert.assertEquals(3,trie.get(BufferUtil.toDirectBuffer("xhellox"),1,4).intValue());
        Assert.assertEquals(4,trie.get(BufferUtil.toDirectBuffer("wibble"),0,6).intValue());
        Assert.assertEquals(5,trie.get(BufferUtil.toDirectBuffer("xWobble"),1,6).intValue());
        Assert.assertEquals(6,trie.get(BufferUtil.toDirectBuffer("xfoo-barx"),1,7).intValue());
        Assert.assertEquals(7,trie.get(BufferUtil.toDirectBuffer("xfoo+barx"),1,7).intValue());
        
        Assert.assertEquals(1,trie.get(BufferUtil.toDirectBuffer("xhellox"),1,5).intValue());
        Assert.assertEquals(2,trie.get(BufferUtil.toDirectBuffer("xHELLox"),1,2).intValue());
        Assert.assertEquals(3,trie.get(BufferUtil.toDirectBuffer("xhellox"),1,4).intValue());
        Assert.assertEquals(4,trie.get(BufferUtil.toDirectBuffer("Wibble"),0,6).intValue());
        Assert.assertEquals(5,trie.get(BufferUtil.toDirectBuffer("xwobble"),1,6).intValue());
        Assert.assertEquals(6,trie.get(BufferUtil.toDirectBuffer("xFOO-barx"),1,7).intValue());
        Assert.assertEquals(7,trie.get(BufferUtil.toDirectBuffer("xFOO+barx"),1,7).intValue());

        Assert.assertEquals(null,trie.get(BufferUtil.toDirectBuffer("xHelloworldx"),1,10));
        Assert.assertEquals(null,trie.get(BufferUtil.toDirectBuffer("xHelpx"),1,4));
        Assert.assertEquals(null,trie.get(BufferUtil.toDirectBuffer("xBlahx"),1,4));
    }
```
```
Cyclomatic Complexity	 2
Assertions		 1
Lines of Code		10
-     
    public void testFull() throws Exception
    {
       if (!(trie instanceof ArrayTrie<?> || trie instanceof ArrayTernaryTrie<?>))
           return;
       
       Assert.assertFalse(trie.put("Large: This is a really large key and should blow the maximum size of the array trie as lots of nodes should already be used.",99));
       testGetString();
       testGetBestArray();
       testGetBestBuffer();
    }
```
```
Cyclomatic Complexity	 1
Assertions		 56
Lines of Code		61
-    
    public void testAddDecodedPaths()
    {
        assertEquals("null+null", URIUtil.addPaths(null,null),null);
        assertEquals("null+", URIUtil.addPaths(null,""),"");
        assertEquals("null+bbb", URIUtil.addPaths(null,"bbb"),"bbb");
        assertEquals("null+/", URIUtil.addPaths(null,"/"),"/");
        assertEquals("null+/bbb", URIUtil.addPaths(null,"/bbb"),"/bbb");

        assertEquals("+null", URIUtil.addPaths("",null),"");
        assertEquals("+", URIUtil.addPaths("",""),"");
        assertEquals("+bbb", URIUtil.addPaths("","bbb"),"bbb");
        assertEquals("+/", URIUtil.addPaths("","/"),"/");
        assertEquals("+/bbb", URIUtil.addPaths("","/bbb"),"/bbb");

        assertEquals("aaa+null", URIUtil.addPaths("aaa",null),"aaa");
        assertEquals("aaa+", URIUtil.addPaths("aaa",""),"aaa");
        assertEquals("aaa+bbb", URIUtil.addPaths("aaa","bbb"),"aaa/bbb");
        assertEquals("aaa+/", URIUtil.addPaths("aaa","/"),"aaa/");
        assertEquals("aaa+/bbb", URIUtil.addPaths("aaa","/bbb"),"aaa/bbb");

        assertEquals("/+null", URIUtil.addPaths("/",null),"/");
        assertEquals("/+", URIUtil.addPaths("/",""),"/");
        assertEquals("/+bbb", URIUtil.addPaths("/","bbb"),"/bbb");
        assertEquals("/+/", URIUtil.addPaths("/","/"),"/");
        assertEquals("/+/bbb", URIUtil.addPaths("/","/bbb"),"/bbb");

        assertEquals("aaa/+null", URIUtil.addPaths("aaa/",null),"aaa/");
        assertEquals("aaa/+", URIUtil.addPaths("aaa/",""),"aaa/");
        assertEquals("aaa/+bbb", URIUtil.addPaths("aaa/","bbb"),"aaa/bbb");
        assertEquals("aaa/+/", URIUtil.addPaths("aaa/","/"),"aaa/");
        assertEquals("aaa/+/bbb", URIUtil.addPaths("aaa/","/bbb"),"aaa/bbb");

        assertEquals(";JS+null", URIUtil.addPaths(";JS",null),";JS");
        assertEquals(";JS+", URIUtil.addPaths(";JS",""),";JS");
        assertEquals(";JS+bbb", URIUtil.addPaths(";JS","bbb"),";JS/bbb");
        assertEquals(";JS+/", URIUtil.addPaths(";JS","/"),";JS/");
        assertEquals(";JS+/bbb", URIUtil.addPaths(";JS","/bbb"),";JS/bbb");

        assertEquals("aaa;JS+null", URIUtil.addPaths("aaa;JS",null),"aaa;JS");
        assertEquals("aaa;JS+", URIUtil.addPaths("aaa;JS",""),"aaa;JS");
        assertEquals("aaa;JS+bbb", URIUtil.addPaths("aaa;JS","bbb"),"aaa;JS/bbb");
        assertEquals("aaa;JS+/", URIUtil.addPaths("aaa;JS","/"),"aaa;JS/");
        assertEquals("aaa;JS+/bbb", URIUtil.addPaths("aaa;JS","/bbb"),"aaa;JS/bbb");

        assertEquals("aaa;JS+null", URIUtil.addPaths("aaa/;JS",null),"aaa/;JS");
        assertEquals("aaa;JS+", URIUtil.addPaths("aaa/;JS",""),"aaa/;JS");
        assertEquals("aaa;JS+bbb", URIUtil.addPaths("aaa/;JS","bbb"),"aaa/;JS/bbb");
        assertEquals("aaa;JS+/", URIUtil.addPaths("aaa/;JS","/"),"aaa/;JS/");
        assertEquals("aaa;JS+/bbb", URIUtil.addPaths("aaa/;JS","/bbb"),"aaa/;JS/bbb");

        assertEquals("?A=1+null", URIUtil.addPaths("?A=1",null),"?A=1");
        assertEquals("?A=1+", URIUtil.addPaths("?A=1",""),"?A=1");
        assertEquals("?A=1+bbb", URIUtil.addPaths("?A=1","bbb"),"?A=1/bbb");
        assertEquals("?A=1+/", URIUtil.addPaths("?A=1","/"),"?A=1/");
        assertEquals("?A=1+/bbb", URIUtil.addPaths("?A=1","/bbb"),"?A=1/bbb");

        assertEquals("aaa?A=1+null", URIUtil.addPaths("aaa?A=1",null),"aaa?A=1");
        assertEquals("aaa?A=1+", URIUtil.addPaths("aaa?A=1",""),"aaa?A=1");
        assertEquals("aaa?A=1+bbb", URIUtil.addPaths("aaa?A=1","bbb"),"aaa?A=1/bbb");
        assertEquals("aaa?A=1+/", URIUtil.addPaths("aaa?A=1","/"),"aaa?A=1/");
        assertEquals("aaa?A=1+/bbb", URIUtil.addPaths("aaa?A=1","/bbb"),"aaa?A=1/bbb");

        assertEquals("aaa?A=1+null", URIUtil.addPaths("aaa/?A=1",null),"aaa/?A=1");
        assertEquals("aaa?A=1+", URIUtil.addPaths("aaa/?A=1",""),"aaa/?A=1");
        assertEquals("aaa?A=1+bbb", URIUtil.addPaths("aaa/?A=1","bbb"),"aaa/?A=1/bbb");
        assertEquals("aaa?A=1+/", URIUtil.addPaths("aaa/?A=1","/"),"aaa/?A=1/");
        assertEquals("aaa?A=1+/bbb", URIUtil.addPaths("aaa/?A=1","/bbb"),"aaa/?A=1/bbb");
@@@+        String path = URIUtil.decodePath(encodedPath);@@@+        assertEquals(expectedPath, path);@@@     }
```
```
Cyclomatic Complexity	 1
Assertions		 14
Lines of Code		18
-    
    public void testEqualsIgnoreEncoding()
    {
        assertTrue(URIUtil.equalsIgnoreEncodings("http://example.com/foo/bar","http://example.com/foo/bar" ));
        assertTrue(URIUtil.equalsIgnoreEncodings("/barry's","/barry%27s"));
        assertTrue(URIUtil.equalsIgnoreEncodings("/barry%27s","/barry's"));
        assertTrue(URIUtil.equalsIgnoreEncodings("/barry%27s","/barry%27s"));
        assertTrue(URIUtil.equalsIgnoreEncodings("/b rry's","/b%20rry%27s"));
        assertTrue(URIUtil.equalsIgnoreEncodings("/b rry%27s","/b%20rry's"));
        assertTrue(URIUtil.equalsIgnoreEncodings("/b rry%27s","/b%20rry%27s"));
        
        assertTrue(URIUtil.equalsIgnoreEncodings("/foo%2fbar","/foo%2fbar"));
        assertTrue(URIUtil.equalsIgnoreEncodings("/foo%2fbar","/foo%2Fbar"));
        
        assertFalse(URIUtil.equalsIgnoreEncodings("ABC", "abc"));
        assertFalse(URIUtil.equalsIgnoreEncodings("/barry's","/barry%26s"));
        
        assertFalse(URIUtil.equalsIgnoreEncodings("/foo/bar","/foo%2fbar"));
        assertFalse(URIUtil.equalsIgnoreEncodings("/foo2fbar","/foo/bar"));@@@+        assertThrows(IllegalArgumentException.class, () -> URIUtil.decodePath(encodedPath));@@@     }
```
```
Cyclomatic Complexity	 -1
Assertions		 32
Lines of Code		60
-    
    public void testUrlEncoded()
    {

        UrlEncoded url_encoded = new UrlEncoded();
        assertEquals("Initially not empty",0, url_encoded.size());

        url_encoded.clear();
        url_encoded.decode("");
        assertEquals("Not empty after decode(\"\")",0, url_encoded.size());

        url_encoded.clear();
        url_encoded.decode("Name1=Value1");
        assertEquals("simple param size",1, url_encoded.size());
        assertEquals("simple encode","Name1=Value1", url_encoded.encode());
        assertEquals("simple get","Value1", url_encoded.getString("Name1"));

        url_encoded.clear();
        url_encoded.decode("Name2=");
        assertEquals("dangling param size",1, url_encoded.size());
        assertEquals("dangling encode","Name2", url_encoded.encode());
        assertEquals("dangling get","", url_encoded.getString("Name2"));

        url_encoded.clear();
        url_encoded.decode("Name3");
        assertEquals("noValue param size",1, url_encoded.size());
        assertEquals("noValue encode","Name3", url_encoded.encode());
        assertEquals("noValue get","", url_encoded.getString("Name3"));

        url_encoded.clear();
        url_encoded.decode("Name4=V\u0629lue+4%21");
        assertEquals("encoded param size",1, url_encoded.size());
        assertEquals("encoded encode","Name4=V%D8%A9lue+4%21", url_encoded.encode());
        assertEquals("encoded get","V\u0629lue 4!", url_encoded.getString("Name4"));

        url_encoded.clear();
        url_encoded.decode("Name4=Value%2B4%21");
        assertEquals("encoded param size",1, url_encoded.size());
        assertEquals("encoded encode","Name4=Value%2B4%21", url_encoded.encode());
        assertEquals("encoded get","Value+4!", url_encoded.getString("Name4"));

        url_encoded.clear();
        url_encoded.decode("Name4=Value+4%21%20%214");
        assertEquals("encoded param size",1, url_encoded.size());
        assertEquals("encoded encode","Name4=Value+4%21+%214", url_encoded.encode());
        assertEquals("encoded get","Value 4! !4", url_encoded.getString("Name4"));


        url_encoded.clear();
        url_encoded.decode("Name5=aaa&Name6=bbb");
        assertEquals("multi param size",2, url_encoded.size());
        assertTrue("multi encode "+url_encoded.encode(),
                   url_encoded.encode().equals("Name5=aaa&Name6=bbb") ||
                   url_encoded.encode().equals("Name6=bbb&Name5=aaa")
                   );
        assertEquals("multi get","aaa", url_encoded.getString("Name5"));
        assertEquals("multi get","bbb", url_encoded.getString("Name6"));

        url_encoded.clear();
        url_encoded.decode("Name7=aaa&Name7=b%2Cb&Name7=ccc");
        assertEquals("multi encode","Name7=aaa&Name7=b%2Cb&Name7=ccc",url_encoded.encode());
        assertEquals("list get all", url_encoded.getString("Name7"),"aaa,b,b,ccc");
        assertEquals("list get","aaa", url_encoded.getValues("Name7").get(0));
        assertEquals("list get", url_encoded.getValues("Name7").get(1),"b,b");
        assertEquals("list get","ccc", url_encoded.getValues("Name7").get(2));

        url_encoded.clear();
        url_encoded.decode("Name8=xx%2C++yy++%2Czz");
        assertEquals("encoded param size",1, url_encoded.size());
        assertEquals("encoded encode","Name8=xx%2C++yy++%2Czz", url_encoded.encode());
        assertEquals("encoded get", url_encoded.getString("Name8"),"xx,  yy  ,zz");
    }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		21
-    
    public void testResourceTsResourceKsWrongPW() throws Exception
    {
        Resource keystoreResource = Resource.newSystemResource("keystore");
        Resource truststoreResource = Resource.newSystemResource("keystore");

        cf.setKeyStoreResource(keystoreResource);
        cf.setTrustStoreResource(truststoreResource);
        cf.setKeyStorePassword("storepwd");
        cf.setKeyManagerPassword("wrong_keypwd");
        cf.setTrustStorePassword("storepwd");

        try (StacklessLogging stackless = new StacklessLogging(AbstractLifeCycle.class))
        {
            cf.start();
            Assert.fail();
        }
        catch(java.security.UnrecoverableKeyException e)
        {
            Assert.assertThat(e.toString(),Matchers.containsString("UnrecoverableKeyException"));
        }@@@+        assertNotNull(cf.getSslContext());@@@     }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		7
-    
    @Slow
    @Ignore
    public void testManySchedulesAndCancels() throws Exception
    {
        schedule(100,5000,3800,200);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		5
-    
    public void testFewSchedulesAndCancels() throws Exception
    {
        schedule(10,500,380,20);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		14
-    
    @Slow
    @Ignore
    public void testBenchmark() throws Exception
    {
        schedule(2000,10000,2000,50);
        PlatformMonitor benchmark = new PlatformMonitor();
        PlatformMonitor.Start start = benchmark.start();
        System.err.println(start);
        System.err.println(_scheduler);
        schedule(2000,30000,2000,50);
        PlatformMonitor.Stop stop = benchmark.stop();
        System.err.println(stop);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		17
-    
    public void testFindAndFilterContainerPathsTarget8()
    throws Exception
    {
        Assume.assumeTrue(JavaVersion.VERSION.getMajor() >= 9);
        Assume.assumeTrue(System.getProperty("jdk.module.path") != null);
        WebInfConfiguration config = new WebInfConfiguration();
        WebAppContext context = new WebAppContext();
        context.setAttribute(JavaVersion.JAVA_TARGET_PLATFORM, "8");
        context.setAttribute(WebInfConfiguration.CONTAINER_JAR_PATTERN, ".*/jetty-util-[^/]*\\.jar$|.*/jetty-util/target/classes/$|.*/foo-bar-janb.jar");
        WebAppClassLoader loader = new WebAppClassLoader(context);
        context.setClassLoader(loader);
        config.findAndFilterContainerPaths(context);
        List<Resource> containerResources = context.getMetaData().getContainerResources();
        assertEquals(1, containerResources.size());
        assertTrue(containerResources.get(0).toString().contains("jetty-util"));
    }
```
```
Cyclomatic Complexity	 2
Assertions		 43
Lines of Code		61
-    
    public void testPassedObject() throws Exception@@@+    public static String[] xmlConfigs()@@@     {
        for (String configure : _configure)
        {
            Map<String,String> properties = new HashMap<>();
            properties.put("whatever", "xxx");
            TestConfiguration.VALUE=77;
            URL url = XmlConfigurationTest.class.getClassLoader().getResource(configure);
            XmlConfiguration configuration = new XmlConfiguration(url);
            TestConfiguration tc = new TestConfiguration("tc");
            configuration.getProperties().putAll(properties);
            configuration.configure(tc);

            assertEquals("Set String","SetValue",tc.testObject);
            assertEquals("Set Type",2,tc.testInt);

            assertEquals(18080, tc.propValue);

            assertEquals("Put","PutValue",tc.get("Test"));
            assertEquals("Put dft","2",tc.get("TestDft"));
            assertEquals("Put type",2,tc.get("TestInt"));

            assertEquals("Trim","PutValue",tc.get("Trim"));
            assertEquals("Null",null,tc.get("Null"));
            assertEquals("NullTrim",null,tc.get("NullTrim"));

            assertEquals("ObjectTrim",1.2345,tc.get("ObjectTrim"));
            assertEquals("Objects","-1String",tc.get("Objects"));
            assertEquals( "ObjectsTrim", "-1String",tc.get("ObjectsTrim"));
            assertEquals( "String", "\n    PutValue\n  ",tc.get("String"));
            assertEquals( "NullString", "",tc.get("NullString"));
            assertEquals( "WhiteSpace", "\n  ",tc.get("WhiteSpace"));
            assertEquals( "ObjectString", "\n    1.2345\n  ",tc.get("ObjectString"));
            assertEquals( "ObjectsString", "-1String",tc.get("ObjectsString"));
            assertEquals( "ObjectsWhiteString", "-1\n  String",tc.get("ObjectsWhiteString"));

            assertEquals( "SystemProperty", System.getProperty("user.dir")+"/stuff",tc.get("SystemProperty"));
            assertEquals( "Env", System.getenv("HOME"),tc.get("Env"));

            assertEquals( "Property", "xxx", tc.get("Property"));


            assertEquals( "Called", "Yes",tc.get("Called"));

            assertTrue(TestConfiguration.called);

            assertEquals("oa[0]","Blah",tc.oa[0]);
            assertEquals("oa[1]","1.2.3.4:5678",tc.oa[1]);
            assertEquals("oa[2]",1.2345,tc.oa[2]);
            assertEquals("oa[3]",null,tc.oa[3]);

            assertEquals("ia[0]",1,tc.ia[0]);
            assertEquals("ia[1]",2,tc.ia[1]);
            assertEquals("ia[2]",3,tc.ia[2]);
            assertEquals("ia[3]",0,tc.ia[3]);

            TestConfiguration tc2=tc.nested;
            assertTrue(tc2!=null);
            assertEquals( "Called(bool)",true,tc2.get("Arg"));

            assertEquals("nested config",null,tc.get("Arg"));
            assertEquals("nested config",true,tc2.get("Arg"));

            assertEquals("nested config","Call1",tc2.testObject);
            assertEquals("nested config",4,tc2.testInt);
            assertEquals( "nested call", "http://www.eclipse.com/",tc2.url.toString());

            assertEquals("static to field",tc.testField1,77);
            assertEquals("field to field",tc.testField2,2);
            assertEquals("literal to static",TestConfiguration.VALUE,42);
            
            assertEquals("value0",((Map<String,String>)configuration.getIdMap().get("map")).get("key0"));
            assertEquals("value1",((Map<String,String>)configuration.getIdMap().get("map")).get("key1"));
        }@@@+        return new String[]{"org/eclipse/jetty/xml/configureWithAttr.xml", "org/eclipse/jetty/xml/configureWithElements.xml"};@@@     }
```
```
Cyclomatic Complexity	 2
Assertions		 2
Lines of Code		24
-    
    public void testNoBlockingTimeoutBlockingReadIdleTimeoutFires() throws Exception
    {
        httpConfig.setBlockingTimeout(-1);
        CountDownLatch handlerLatch = new CountDownLatch(1);
        start(new BlockingReadHandler(handlerLatch));
        long idleTimeout = 2500;
        setServerIdleTimeout(idleTimeout);

        try (StacklessLogging stackless = new StacklessLogging(HttpChannel.class))
        {
            DeferredContentProvider contentProvider = new DeferredContentProvider(ByteBuffer.allocate(1));
            CountDownLatch resultLatch = new CountDownLatch(1);
            client.POST(newURI())
                    .content(contentProvider)
                    .send(result ->
                    {
                        if (result.getResponse().getStatus() == HttpStatus.INTERNAL_SERVER_ERROR_500)
                            resultLatch.countDown();
                    });

            // Blocking read should timeout.
            Assert.assertTrue(handlerLatch.await(2 * idleTimeout, TimeUnit.MILLISECONDS));
            // Complete the request.
            contentProvider.close();
            Assert.assertTrue(resultLatch.await(5, TimeUnit.SECONDS));
        }
    }
```
```
Cyclomatic Complexity	 2
Assertions		 2
Lines of Code		25
-    
    public void testBlockingTimeoutSmallerThanIdleTimeoutBlockingReadBlockingTimeoutFires() throws Exception
    {
        long blockingTimeout = 2500;
        httpConfig.setBlockingTimeout(blockingTimeout);
        CountDownLatch handlerLatch = new CountDownLatch(1);
        start(new BlockingReadHandler(handlerLatch));
        long idleTimeout = 3 * blockingTimeout;
        setServerIdleTimeout(idleTimeout);

        try (StacklessLogging stackless = new StacklessLogging(HttpChannel.class))
        {
            DeferredContentProvider contentProvider = new DeferredContentProvider(ByteBuffer.allocate(1));
            CountDownLatch resultLatch = new CountDownLatch(1);
            client.POST(newURI())
                    .content(contentProvider)
                    .send(result ->
                    {
                        if (result.getResponse().getStatus() == HttpStatus.INTERNAL_SERVER_ERROR_500)
                            resultLatch.countDown();
                    });

            // Blocking read should timeout.
            Assert.assertTrue(handlerLatch.await(2 * blockingTimeout, TimeUnit.MILLISECONDS));
            // Complete the request.
            contentProvider.close();
            Assert.assertTrue(resultLatch.await(5, TimeUnit.SECONDS));
        }
    }
```
```
Cyclomatic Complexity	 2
Assertions		 2
Lines of Code		25
-    
    public void testBlockingTimeoutLargerThanIdleTimeoutBlockingReadIdleTimeoutFires() throws Exception
    {
        long idleTimeout = 2500;
        long blockingTimeout = 3 * idleTimeout;
        httpConfig.setBlockingTimeout(blockingTimeout);
        CountDownLatch handlerLatch = new CountDownLatch(1);
        start(new BlockingReadHandler(handlerLatch));
        setServerIdleTimeout(idleTimeout);

        try (StacklessLogging stackless = new StacklessLogging(HttpChannel.class))
        {
            DeferredContentProvider contentProvider = new DeferredContentProvider(ByteBuffer.allocate(1));
            CountDownLatch resultLatch = new CountDownLatch(1);
            client.POST(newURI())
                    .content(contentProvider)
                    .send(result ->
                    {
                        if (result.getResponse().getStatus() == HttpStatus.INTERNAL_SERVER_ERROR_500)
                            resultLatch.countDown();
                    });

            // Blocking read should timeout.
            Assert.assertTrue(handlerLatch.await(2 * idleTimeout, TimeUnit.MILLISECONDS));
            // Complete the request.
            contentProvider.close();
            Assert.assertTrue(resultLatch.await(5, TimeUnit.SECONDS));
        }
    }
```
```
Cyclomatic Complexity	 -1
Assertions		 2
Lines of Code		33
-    
    public void testNoBlockingTimeoutBlockingWriteIdleTimeoutFires() throws Exception
    {
        httpConfig.setBlockingTimeout(-1);
        CountDownLatch handlerLatch = new CountDownLatch(1);
        start(new BlockingWriteHandler(handlerLatch));
        long idleTimeout = 2500;
        setServerIdleTimeout(idleTimeout);

        try (StacklessLogging stackless = new StacklessLogging(HttpChannel.class))
        {
            BlockingQueue<Callback> callbacks = new LinkedBlockingQueue<>();
            CountDownLatch resultLatch = new CountDownLatch(1);
            client.newRequest(newURI())
                    .onResponseContentAsync((response, content, callback) ->
                    {
                        // Do not succeed the callback so the server will block writing.
                        callbacks.offer(callback);
                    })
                    .send(result ->
                    {
                        if (result.isFailed())
                            resultLatch.countDown();
                    });

            // Blocking write should timeout.
            Assert.assertTrue(handlerLatch.await(2 * idleTimeout, TimeUnit.MILLISECONDS));
            // After the server stopped sending, consume on the client to read the early EOF.
            while (true)
            {
                Callback callback = callbacks.poll(1, TimeUnit.SECONDS);
                if (callback == null)
                    break;
                callback.succeeded();@@@             }
            Assert.assertTrue(resultLatch.await(5, TimeUnit.SECONDS));
        }@@@+        }
```
```
Cyclomatic Complexity	 -1
Assertions		 2
Lines of Code		36
-    
    public void testBlockingTimeoutSmallerThanIdleTimeoutBlockingWriteBlockingTimeoutFires() throws Exception@@@+    private void testReadWithDelayedFirstContentIdleTimeoutFires(TransportScenario scenario, Handler handler, boolean delayDispatch) throws Exception@@@     {
        long blockingTimeout = 2500;
        httpConfig.setBlockingTimeout(blockingTimeout);@@@+        scenario.httpConfig.setDelayDispatchUntilContent(delayDispatch);@@@         CountDownLatch handlerLatch = new CountDownLatch(1);
        start(new BlockingWriteHandler(handlerLatch));
        long idleTimeout = 3 * blockingTimeout;
        setServerIdleTimeout(idleTimeout);

        try (StacklessLogging stackless = new StacklessLogging(HttpChannel.class))
        {
            BlockingQueue<Callback> callbacks = new LinkedBlockingQueue<>();
            CountDownLatch resultLatch = new CountDownLatch(1);
            client.newRequest(newURI())
                    .onResponseContentAsync((response, content, callback) ->
                    {
                        // Do not succeed the callback so the server will block writing.
                        callbacks.offer(callback);
                    })
                    .send(result ->
                    {
                        if (result.isFailed())
                            resultLatch.countDown();
                    });

            // Blocking write should timeout.
            Assert.assertTrue(handlerLatch.await(2 * blockingTimeout, TimeUnit.MILLISECONDS));
            // After the server stopped sending, consume on the client to read the early EOF.
            while (true)
            {
                Callback callback = callbacks.poll(1, TimeUnit.SECONDS);
                if (callback == null)
                    break;
                callback.succeeded();
            }
            Assert.assertTrue(resultLatch.await(5, TimeUnit.SECONDS));
        }
    }
```
```
Cyclomatic Complexity	 -1
Assertions		 2
Lines of Code		34
-    
    public void testBlockingTimeoutLargerThanIdleTimeoutBlockingWriteIdleTimeoutFires() throws Exception
    {
        long idleTimeout = 2500;
        long blockingTimeout = 3 * idleTimeout;
        httpConfig.setBlockingTimeout(blockingTimeout);
        CountDownLatch handlerLatch = new CountDownLatch(1);
        start(new BlockingWriteHandler(handlerLatch));
        setServerIdleTimeout(idleTimeout);

        try (StacklessLogging stackless = new StacklessLogging(HttpChannel.class))
        {
            BlockingQueue<Callback> callbacks = new LinkedBlockingQueue<>();
            CountDownLatch resultLatch = new CountDownLatch(1);
            client.newRequest(newURI())
                    .onResponseContentAsync((response, content, callback) ->
                    {
                        // Do not succeed the callback so the server will block writing.
                        callbacks.offer(callback);
                    })
                    .send(result ->
                    {
                        if (result.isFailed())
                            resultLatch.countDown();
                    });

            // Blocking read should timeout.
            Assert.assertTrue(handlerLatch.await(2 * idleTimeout, TimeUnit.MILLISECONDS));
            // After the server stopped sending, consume on the client to read the early EOF.
            while (true)
            {
                Callback callback = callbacks.poll(1, TimeUnit.SECONDS);
                if (callback == null)
                    break;
                callback.succeeded();
            }
            Assert.assertTrue(resultLatch.await(5, TimeUnit.SECONDS));
        }
    }
```
```
Cyclomatic Complexity	 3
Assertions		 4
Lines of Code		15
-    
    public void testOne() throws Exception
    {
        System.err.printf("[%d] TEST   c=%s, m=%s, delayDispatch=%b delayInFrame=%s content-length:%d expect=%d read=%d content:%s%n",_id,_client.getSimpleName(),_mode,__config.isDelayDispatchUntilContent(),_delay,_length,_status,_read,_send);

        TestClient client=_client.newInstance();
        String response = client.send("/ctx/test?mode="+_mode,50,_delay,_length,_send);
        
        int sum=0;
        for (String s:_send)
            for (char c : s.toCharArray())
                sum+=c;
        
        assertThat(response,startsWith("HTTP"));
        assertThat(response,Matchers.containsString(" "+_status+" "));
        assertThat(response,Matchers.containsString("read="+_read));
        assertThat(response,Matchers.containsString("sum="+sum));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		14
-     
     public void testGetNonExistantUser () throws Exception
     {
         try
         {
             startClient("foo", "bar");
             ContentResponse response = _client.GET(_baseUri.resolve("input.txt"));
             assertEquals(HttpServletResponse.SC_UNAUTHORIZED,response.getStatus());
         }
         finally
         {
             stopClient();
         }
     }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		18
-     
     public void testPost() throws Exception
     {
         try
         {
             startClient();

             Request request = _client.newRequest(_baseUri.resolve("test"));
             request.method(HttpMethod.POST);
             request.content(new BytesContentProvider(_content.getBytes()));
             ContentResponse response = request.send();
             assertEquals(HttpStatus.OK_200,response.getStatus());
             assertEquals(_content,_testServer.getTestHandler().getRequestContent());
         }
         finally
         {
             stopClient();
         }
     }
```
## f745d5d5df07746cc89c73cdcf5a29b421cfd5fe ##
```
Cyclomatic Complexity	1
Assertions		5
Lines of Code		17
-    
    public void testMutable()
    {
        HttpFields headers = HttpFields.build()
            .add(HttpHeader.ETAG, "tag")
            .add("name0", "value0")
            .add("name1", "value1").asImmutable();

        headers = HttpFields.build(headers, EnumSet.of(HttpHeader.ETAG, HttpHeader.CONTENT_RANGE))
            .add(new PreEncodedHttpField(HttpHeader.CONNECTION, HttpHeaderValue.CLOSE.asString()))
            .addDateField("name2", System.currentTimeMillis()).asImmutable();

        headers = HttpFields.build(headers, new HttpField(HttpHeader.CONNECTION, "open"));

        assertThat(headers.size(), is(4));
        assertThat(headers.getField(0).getValue(), is("value0"));
        assertThat(headers.getField(1).getValue(), is("value1"));
        assertThat(headers.getField(2).getValue(), is("open"));
        assertThat(headers.getField(3).getName(), is("name2"));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 4
Lines of Code		11
-    
    public void testMap()
    {
        Map<HttpFields.Immutable, String> map = new HashMap<>();
        map.put(HttpFields.build().add("X", "1").add(HttpHeader.ETAG, "tag").asImmutable(), "1");
        map.put(HttpFields.build().add("X", "2").add(HttpHeader.ETAG, "other").asImmutable(), "2");

        assertThat(map.get(HttpFields.build().add("X", "1").add(HttpHeader.ETAG, "tag").asImmutable()), is("1"));
        assertThat(map.get(HttpFields.build().add("X", "2").add(HttpHeader.ETAG, "other").asImmutable()), is("2"));
        assertThat(map.get(HttpFields.build().add("X", "2").asImmutable()), nullValue());
        assertThat(map.get(HttpFields.build().add("X", "2").add(HttpHeader.ETAG, "tag").asImmutable()), nullValue());
    }
```
```
Cyclomatic Complexity	 1
Assertions		 10
Lines of Code		15
-    
    public void testExample() throws Exception
    {
        HttpURI uri = HttpURI.from("http://user:password@host:8888/ignored/../p%61th;ignored/info;param?query=value#fragment");

        assertThat(uri.getScheme(), is("http"));
        assertThat(uri.getUser(), is("user:password"));
        assertThat(uri.getHost(), is("host"));
        assertThat(uri.getPort(), is(8888));
        assertThat(uri.getPath(), is("/ignored/../p%61th;ignored/info;param"));
        assertThat(uri.getDecodedPath(), is("/path/info"));
        assertThat(uri.getParam(), is("param"));
        assertThat(uri.getQuery(), is("query=value"));
        assertThat(uri.getFragment(), is("fragment"));
        assertThat(uri.getAuthority(), is("host:8888"));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 6
Lines of Code		10
-    
    public void testJoin()
    {
        assertThat(QuotedCSV.join((String)null), nullValue());
        assertThat(QuotedCSV.join(Collections.emptyList()), is(emptyString()));
        assertThat(QuotedCSV.join(Collections.singletonList("hi")), is("hi"));
        assertThat(QuotedCSV.join("hi", "ho"), is("hi, ho"));
        assertThat(QuotedCSV.join("h i", "h,o"), is("\"h i\", \"h,o\""));
        assertThat(QuotedCSV.join("h\"i", "h\to"), is("\"h\\\"i\", \"h\\to\""));@@@     }
```
```
Cyclomatic Complexity	 3
Assertions		 10
Lines of Code		100
-    
    public void testGracefulServerGoAway() throws Exception
    {
        AtomicReference<Session> serverSessionRef = new AtomicReference<>();
        CountDownLatch serverSessionLatch = new CountDownLatch(1);
        CountDownLatch dataLatch = new CountDownLatch(2);
        start(new ServerSessionListener.Adapter()
        {
            @Override
            public void onAccept(Session session)
            {
                serverSessionRef.set(session);
                serverSessionLatch.countDown();
            }

            @Override
            public Stream.Listener onNewStream(Stream stream, HeadersFrame frame)
            {
                return new Stream.Listener.Adapter()
                {
                    @Override
                    public void onData(Stream stream, DataFrame frame, Callback callback)
                    {
                        callback.succeeded();
                        dataLatch.countDown();
                        if (frame.isEndStream())
                        {
                            MetaData.Response response = new MetaData.Response(HttpVersion.HTTP_2, HttpStatus.OK_200, HttpFields.EMPTY);
                            stream.headers(new HeadersFrame(stream.getId(), response, null, true), Callback.NOOP);
                        }
                    }
                };
            }
        });
        // Avoid aggressive idle timeout to allow the test verifications.
        connector.setShutdownIdleTimeout(connector.getIdleTimeout());

        CountDownLatch clientCloseLatch = new CountDownLatch(1);
        Session clientSession = newClient(new Session.Listener.Adapter()
        {
            @Override
            public void onClose(Session session, GoAwayFrame frame)
            {
                clientCloseLatch.countDown();
            }
        });
        assertTrue(serverSessionLatch.await(5, TimeUnit.SECONDS));
        Session serverSession = serverSessionRef.get();

        // Start 2 requests without completing them yet.
        CountDownLatch responseLatch = new CountDownLatch(2);
        MetaData.Request metaData1 = newRequest("GET", HttpFields.EMPTY);
        HeadersFrame request1 = new HeadersFrame(metaData1, null, false);
        FuturePromise<Stream> promise1 = new FuturePromise<>();
        Stream.Listener.Adapter listener = new Stream.Listener.Adapter()
        {
            @Override
            public void onHeaders(Stream stream, HeadersFrame frame)
            {
                if (frame.isEndStream())
                {
                    MetaData.Response response = (MetaData.Response)frame.getMetaData();
                    assertEquals(HttpStatus.OK_200, response.getStatus());
                    responseLatch.countDown();
                }
            }
        };
        clientSession.newStream(request1, promise1, listener);
        Stream stream1 = promise1.get(5, TimeUnit.SECONDS);
        stream1.data(new DataFrame(stream1.getId(), ByteBuffer.allocate(1), false), Callback.NOOP);

        MetaData.Request metaData2 = newRequest("GET", HttpFields.EMPTY);
        HeadersFrame request2 = new HeadersFrame(metaData2, null, false);
        FuturePromise<Stream> promise2 = new FuturePromise<>();
        clientSession.newStream(request2, promise2, listener);
        Stream stream2 = promise2.get(5, TimeUnit.SECONDS);
        stream2.data(new DataFrame(stream2.getId(), ByteBuffer.allocate(1), false), Callback.NOOP);

        assertTrue(dataLatch.await(5, TimeUnit.SECONDS));

        // Both requests are now on the server, shutdown gracefully the server session.
        int port = connector.getLocalPort();
        CompletableFuture<Void> shutdown = Graceful.shutdown(server);

        // GOAWAY should not arrive to the client yet.
        assertFalse(clientCloseLatch.await(1, TimeUnit.SECONDS));

        // New requests should be immediately rejected.
        HostPortHttpField authority3 = new HostPortHttpField("localhost" + ":" + port);
        MetaData.Request metaData3 = new MetaData.Request("GET", HttpScheme.HTTP.asString(), authority3, servletPath, HttpVersion.HTTP_2, HttpFields.EMPTY, -1);
        HeadersFrame request3 = new HeadersFrame(metaData3, null, false);
        FuturePromise<Stream> promise3 = new FuturePromise<>();
        CountDownLatch resetLatch = new CountDownLatch(1);
        clientSession.newStream(request3, promise3, new Stream.Listener.Adapter()
        {
            @Override
            public void onReset(Stream stream, ResetFrame frame)
            {
                resetLatch.countDown();
            }
        });
        Stream stream3 = promise3.get(5, TimeUnit.SECONDS);
        stream3.data(new DataFrame(stream3.getId(), ByteBuffer.allocate(1), true), Callback.NOOP);
        assertTrue(resetLatch.await(5, TimeUnit.SECONDS));

        // Finish the previous requests and expect the responses.
        stream1.data(new DataFrame(stream1.getId(), BufferUtil.EMPTY_BUFFER, true), Callback.NOOP);
        stream2.data(new DataFrame(stream2.getId(), BufferUtil.EMPTY_BUFFER, true), Callback.NOOP);
        assertTrue(responseLatch.await(5, TimeUnit.SECONDS));
        assertNull(shutdown.get(5, TimeUnit.SECONDS));

        // Now GOAWAY should arrive to the client.
        assertTrue(clientCloseLatch.await(5, TimeUnit.SECONDS));
        // Wait to process the GOAWAY frames and close the EndPoints.
        Thread.sleep(1000);
        assertFalse(((HTTP2Session)clientSession).getEndPoint().isOpen());
        assertFalse(((HTTP2Session)serverSession).getEndPoint().isOpen());@@@     }
```
```
Cyclomatic Complexity	 1
Assertions		 7
Lines of Code		29
-    
    public void testDefaultContextPath() throws Exception
    {
        File quickstartXml = new File(webInf, "quickstart-web.xml");
        assertFalse(quickstartXml.exists());

        // generate a quickstart-web.xml
        WebAppContext quickstart = new WebAppContext();
        quickstart.setResourceBase(testDir.getAbsolutePath());
        quickstart.addConfiguration(new QuickStartConfiguration());
        quickstart.setAttribute(QuickStartConfiguration.MODE, QuickStartConfiguration.Mode.GENERATE);
        quickstart.setAttribute(QuickStartConfiguration.ORIGIN_ATTRIBUTE, "origin");
        quickstart.setDescriptor(MavenTestingUtils.getTestResourceFile("web.xml").getAbsolutePath());
        quickstart.setContextPath("/foo");
        server.setHandler(quickstart);
        server.setDryRun(true);
        server.start();
        assertEquals("/foo", quickstart.getContextPath());
        assertFalse(quickstart.isContextPathDefault());

        assertTrue(quickstartXml.exists());

        // quick start
        WebAppContext webapp = new WebAppContext();
        webapp.addConfiguration(new QuickStartConfiguration());
        quickstart.setAttribute(QuickStartConfiguration.MODE, QuickStartConfiguration.Mode.QUICKSTART);
        webapp.setResourceBase(testDir.getAbsolutePath());
        webapp.setClassLoader(new URLClassLoader(new URL[0], Thread.currentThread().getContextClassLoader()));
        server.setHandler(webapp);

        server.setDryRun(false);
        server.start();

        // verify the context path is the default-context-path
        assertEquals("/thisIsTheDefault", webapp.getContextPath());
        assertTrue(webapp.isContextPathDefault());
    }
```
```
Cyclomatic Complexity	 1
Assertions		 4
Lines of Code		28
-    
    public void testDefaultRequestAndResponseEncodings() throws Exception
    {
        File quickstartXml = new File(webInf, "quickstart-web.xml");
        assertFalse(quickstartXml.exists());

        // generate a quickstart-web.xml
        WebAppContext quickstart = new WebAppContext();
        quickstart.setResourceBase(testDir.getAbsolutePath());
        quickstart.addConfiguration(new QuickStartConfiguration());
        quickstart.setAttribute(QuickStartConfiguration.MODE, QuickStartConfiguration.Mode.GENERATE);
        quickstart.setAttribute(QuickStartConfiguration.ORIGIN_ATTRIBUTE, "origin");
        quickstart.setDescriptor(MavenTestingUtils.getTestResourceFile("web.xml").getAbsolutePath());
        quickstart.setContextPath("/foo");
        server.setHandler(quickstart);
        server.setDryRun(true);
        server.start();
        
        assertTrue(quickstartXml.exists());
        
        // quick start
        WebAppContext webapp = new WebAppContext();
        webapp.addConfiguration(new QuickStartConfiguration());
        quickstart.setAttribute(QuickStartConfiguration.MODE, QuickStartConfiguration.Mode.QUICKSTART);
        webapp.setResourceBase(testDir.getAbsolutePath());
        webapp.setClassLoader(new URLClassLoader(new URL[0], Thread.currentThread().getContextClassLoader()));
        server.setHandler(webapp);

        server.setDryRun(false);
        server.start();
        
        assertEquals("ascii", webapp.getDefaultRequestCharacterEncoding());
        assertEquals("utf-16", webapp.getDefaultResponseCharacterEncoding());@@@+        server.stop();@@@     }
```
```
Cyclomatic Complexity	 2
Assertions		 6
Lines of Code		27
-    
    public void testUrlQuery() throws Exception
    {
        CookiePatternRule rule = new CookiePatternRule();
        rule.setPattern("*");
        rule.setName("fruit");
        rule.setValue("banana");

        startServer(rule);

        StringBuilder rawRequest = new StringBuilder();
        rawRequest.append("GET /other?fruit=apple HTTP/1.1\r\n");
        rawRequest.append("Host: local\r\n");
        rawRequest.append("Connection: close\r\n");
        rawRequest.append("\r\n");

        String rawResponse = localConnector.getResponse(rawRequest.toString());
        HttpTester.Response response = HttpTester.parseResponse(rawResponse);

        String responseContent = response.getContent();
        assertResponseContentLine(responseContent, "baseRequest.requestUri=", "/other");
        assertResponseContentLine(responseContent, "request.queryString=", "fruit=apple");

        // verify
        HttpField setCookieField = response.getField(HttpHeader.SET_COOKIE);
        assertThat("response should have Set-Cookie", setCookieField, notNullValue());
        for (String value : setCookieField.getValues())
        {
            String[] result = value.split("=");
            assertThat(result[0], is("fruit"));
            assertThat(result[1], is("banana"));
        }@@@     }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		9
-    
    public void testWin2kSP1WithIE6() throws Exception
    {
        _request.setHttpFields(HttpFields.build(_request.getHttpFields())
            .add("User-Agent", "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.01)"));

        String result = _rule.matchAndApply(_request.getRequestURI(), _request, _response);

        assertEquals(_request.getRequestURI(), result);
        assertEquals(HttpHeaderValue.CLOSE.asString(), _response.getHeader(HttpHeader.CONNECTION.asString()));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		9
-    
    public void testWin2kSP1WithIE7() throws Exception
    {
        _request.setHttpFields(HttpFields.build(_request.getHttpFields())
            .add("User-Agent", "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.01)"));

        String result = _rule.matchAndApply(_request.getRequestURI(), _request, _response);

        assertNull(result);
        assertNull(_response.getHeader(HttpHeader.CONNECTION.asString()));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 8
Lines of Code		18
-    
    public void testGracefulWithContext() throws Exception
    {
        Socket client0 = newClientBusy(POST_A_12345, handlerA);
        Socket client1 = newClientBusy(POST_A_12345_C, handlerA);
        Socket client2 = newClientIdle(POST_A_12345, handlerA);

        backgroundComplete(client0, handlerA);
        backgroundComplete(client1, handlerA);
        Future<Integer> status2 = backgroundUnavailable(client2, POST_A_12345, contextA, handlerA);

        assertGracefulStop(server);

        assertResponse(client0, true);
        assertResponse(client1, false);
        assertThat(status2.get(), is(503));

        assertQuickClose(client0);
        assertQuickClose(client1);
        assertQuickClose(client2);
        assertHandled(handlerA, false);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 8
Lines of Code		19
-    
    public void testGracefulContext() throws Exception
    {
        Socket client0 = newClientBusy(POST_B_12345, handlerB);
        Socket client1 = newClientBusy(POST_B_12345_C, handlerB);
        Socket client2 = newClientIdle(POST_B_12345, handlerB);

        backgroundComplete(client0, handlerB);
        backgroundComplete(client1, handlerB);
        Future<Integer> status2 = backgroundUnavailable(client2, POST_B_12345, contextB, handlerB);

        Graceful.shutdown(contextB).orTimeout(10, TimeUnit.SECONDS).get();

        assertResponse(client0, false);
        assertResponse(client1, false);
        assertThat(status2.get(), is(503));

        assertAvailable(client0, POST_A_12345, handlerA);
        assertAvailable(client1, POST_A_12345_C, handlerA);
        assertAvailable(client2, POST_A_12345, handlerA);

        assertHandled(handlerA, false);
        assertHandled(handlerB, false);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		8
-    
    public void testSetListenerWithNull() throws Exception
    {
        //test can't be null
        assertThrows(NullPointerException.class, () ->
        {
            _in.setReadListener(null);
        });
    }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		8
-    
    public void testSetListenerNotAsync() throws Exception
    {
        //test not async
        assertThrows(IllegalStateException.class, () ->
        {
            _in.setReadListener(_listener);
        });
    }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		12
-    
    public void testSetListenerAlreadySet() throws Exception
    {
        //set up a listener
        ((TestHttpChannelState)_in.getHttpChannelState()).setFakeAsyncState(true);
        _in.setReadListener(_listener);
        //throw away any events generated by setting the listener
        _history.clear();
        ((TestHttpChannelState)_in.getHttpChannelState()).setFakeAsyncState(false);
        //now test that you can't set another listener
        assertThrows(IllegalStateException.class, () ->
        {
            _in.setReadListener(_listener);
        });@@@     }
```
```
Cyclomatic Complexity	 2
Assertions		 6
Lines of Code		50
-    
    public void testRequestCharacterEncoding() throws Exception
    {
        AtomicReference<String> result = new AtomicReference<>(null);
        AtomicReference<String> overrideCharEncoding = new AtomicReference<>(null);

        _server.stop();
        ContextHandler handler = new CharEncodingContextHandler();
        _server.setHandler(handler);
        handler.setHandler(_handler);
        _handler._checker = new RequestTester()
        {
            @Override
            public boolean check(HttpServletRequest request, HttpServletResponse response)
            {
                try
                {
                    String s = overrideCharEncoding.get();
                    if (s != null)
                        request.setCharacterEncoding(s);

                    result.set(request.getCharacterEncoding());
                    return true;
                }
                catch (UnsupportedEncodingException e)
                {
                    return false;
                }
            }
        };
        _server.start();

        String request = "GET / HTTP/1.1\n" +
            "Host: whatever\r\n" +
            "Content-Type: text/html;charset=utf8\n" +
            "Connection: close\n" +
            "\n";

        //test setting the default char encoding
        handler.setDefaultRequestCharacterEncoding("ascii");
        String response = _connector.getResponse(request);
        assertTrue(response.startsWith("HTTP/1.1 200"));
        assertEquals("ascii", result.get());

        //test overriding the default char encoding with explicit encoding
        result.set(null);
        overrideCharEncoding.set("utf-16");
        response = _connector.getResponse(request);
        assertTrue(response.startsWith("HTTP/1.1 200"));
        assertEquals("utf-16", result.get());

        //test fallback to content-type encoding
        result.set(null);
        overrideCharEncoding.set(null);
        handler.setDefaultRequestCharacterEncoding(null);
        response = _connector.getResponse(request);
        assertTrue(response.startsWith("HTTP/1.1 200"));
        assertEquals("utf-8", result.get());@@@     }
```
```
Cyclomatic Complexity	 1
Assertions		 19
Lines of Code		60
-    
    public void testPushBuilder() throws Exception
    {
        String uri = "/foo/something";
        Request request = new TestRequest(null, null);
        request.getResponse().getHttpFields().add(new HttpCookie.SetCookieHttpField(new HttpCookie("good","thumbsup", 100), CookieCompliance.RFC6265));
        request.getResponse().getHttpFields().add(new HttpCookie.SetCookieHttpField(new HttpCookie("bonza","bewdy", 1), CookieCompliance.RFC6265));
        request.getResponse().getHttpFields().add(new HttpCookie.SetCookieHttpField(new HttpCookie("bad", "thumbsdown", 0), CookieCompliance.RFC6265));
        HttpFields.Mutable fields = HttpFields.build();
        fields.add(HttpHeader.AUTHORIZATION, "Basic foo");
        request.setMetaData(new MetaData.Request("GET", HttpURI.from(uri), HttpVersion.HTTP_1_0, fields));
        assertTrue(request.isPushSupported());
        PushBuilder builder = request.newPushBuilder();
        assertNotNull(builder);
        assertEquals("GET", builder.getMethod());
        assertThrows(NullPointerException.class, () ->
        {
            builder.method(null);
        });
        assertThrows(IllegalArgumentException.class, () ->
        {
            builder.method("");
        });
        assertThrows(IllegalArgumentException.class, () ->
        {
            builder.method("   ");
        });
        assertThrows(IllegalArgumentException.class, () ->
        {
            builder.method("POST");
        });
        assertThrows(IllegalArgumentException.class, () ->
        {
            builder.method("PUT");
        });
        assertThrows(IllegalArgumentException.class, () ->
        {
            builder.method("DELETE");
        });
        assertThrows(IllegalArgumentException.class, () ->
        {
            builder.method("CONNECT");
        });
        assertThrows(IllegalArgumentException.class, () ->
        {
            builder.method("OPTIONS");
        });
        assertThrows(IllegalArgumentException.class, () ->
        {
            builder.method("TRACE");
        });
        assertEquals(TestRequest.TEST_SESSION_ID, builder.getSessionId());
        builder.path("/foo/something-else.txt");
        assertEquals("/foo/something-else.txt", builder.getPath());
        assertEquals("Basic foo", builder.getHeader("Authorization"));
        assertThat(builder.getHeader("Cookie"), containsString("bonza"));
        assertThat(builder.getHeader("Cookie"), containsString("good"));
        assertThat(builder.getHeader("Cookie"), containsString("maxpos"));
        assertThat(builder.getHeader("Cookie"), not(containsString("bad")));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 48
Lines of Code		77
-    
    public void testServletPathMapping() throws Exception
    {
        ServletPathSpec spec;
        String uri;
        ServletPathMapping m;

        spec = null;
        uri = null;
        m = new ServletPathMapping(spec, null, uri);
        assertThat(m.getMappingMatch(), nullValue());
        assertThat(m.getMatchValue(), is(""));
        assertThat(m.getPattern(), nullValue());
        assertThat(m.getServletName(), is(""));
        assertThat(m.getServletPath(), nullValue());
        assertThat(m.getPathInfo(), nullValue());

        spec = new ServletPathSpec("");
        uri = "/";
        m = new ServletPathMapping(spec, "Something", uri);
        assertThat(m.getMappingMatch(), is(MappingMatch.CONTEXT_ROOT));
        assertThat(m.getMatchValue(), is(""));
        assertThat(m.getPattern(), is(""));
        assertThat(m.getServletName(), is("Something"));
        assertThat(m.getServletPath(), is(spec.getPathMatch(uri)));
        assertThat(m.getPathInfo(), is(spec.getPathInfo(uri)));

        spec = new ServletPathSpec("/");
        uri = "/some/path";
        m = new ServletPathMapping(spec, "Default", uri);
        assertThat(m.getMappingMatch(), is(MappingMatch.DEFAULT));
        assertThat(m.getMatchValue(), is(""));
        assertThat(m.getPattern(), is("/"));
        assertThat(m.getServletName(), is("Default"));
        assertThat(m.getServletPath(), is(spec.getPathMatch(uri)));
        assertThat(m.getPathInfo(), is(spec.getPathInfo(uri)));

        spec = new ServletPathSpec("/foo/*");
        uri = "/foo/bar";
        m = new ServletPathMapping(spec, "FooServlet", uri);
        assertThat(m.getMappingMatch(), is(MappingMatch.PATH));
        assertThat(m.getMatchValue(), is("foo"));
        assertThat(m.getPattern(), is("/foo/*"));
        assertThat(m.getServletName(), is("FooServlet"));
        assertThat(m.getServletPath(), is(spec.getPathMatch(uri)));
        assertThat(m.getPathInfo(), is(spec.getPathInfo(uri)));

        uri = "/foo/";
        m = new ServletPathMapping(spec, "FooServlet", uri);
        assertThat(m.getMappingMatch(), is(MappingMatch.PATH));
        assertThat(m.getMatchValue(), is("foo"));
        assertThat(m.getPattern(), is("/foo/*"));
        assertThat(m.getServletName(), is("FooServlet"));
        assertThat(m.getServletPath(), is(spec.getPathMatch(uri)));
        assertThat(m.getPathInfo(), is(spec.getPathInfo(uri)));

        uri = "/foo";
        m = new ServletPathMapping(spec, "FooServlet", uri);
        assertThat(m.getMappingMatch(), is(MappingMatch.PATH));
        assertThat(m.getMatchValue(), is("foo"));
        assertThat(m.getPattern(), is("/foo/*"));
        assertThat(m.getServletName(), is("FooServlet"));
        assertThat(m.getServletPath(), is(spec.getPathMatch(uri)));
        assertThat(m.getPathInfo(), is(spec.getPathInfo(uri)));

        spec = new ServletPathSpec("*.jsp");
        uri = "/foo/bar.jsp";
        m = new ServletPathMapping(spec, "JspServlet", uri);
        assertThat(m.getMappingMatch(), is(MappingMatch.EXTENSION));
        assertThat(m.getMatchValue(), is("foo/bar"));
        assertThat(m.getPattern(), is("*.jsp"));
        assertThat(m.getServletName(), is("JspServlet"));
        assertThat(m.getServletPath(), is(spec.getPathMatch(uri)));
        assertThat(m.getPathInfo(), is(spec.getPathInfo(uri)));

        spec = new ServletPathSpec("/catalog");
        uri = "/catalog";
        m = new ServletPathMapping(spec, "CatalogServlet", uri);
        assertThat(m.getMappingMatch(), is(MappingMatch.EXACT));
        assertThat(m.getMatchValue(), is("catalog"));
        assertThat(m.getPattern(), is("/catalog"));
        assertThat(m.getServletName(), is("CatalogServlet"));
        assertThat(m.getServletPath(), is(spec.getPathMatch(uri)));
        assertThat(m.getPathInfo(), is(spec.getPathInfo(uri)));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 11
Lines of Code		36
-    
    public void testLocaleAndContentTypeEncoding() throws Exception
    {
        _server.stop();
        MimeTypes.getInferredEncodings().put("text/html", "iso-8859-1");
        ContextHandler handler = new ContextHandler();
        handler.addLocaleEncoding("ja", "euc-jp");
        handler.addLocaleEncoding("zh_CN", "gb18030");
        _server.setHandler(handler);
        handler.setHandler(new DumpHandler());
        _server.start();

        Response response = getResponse();
        response.getHttpChannel().getRequest().setContext(handler.getServletContext(), "/");
        
        response.setContentType("text/html");
        assertEquals("iso-8859-1", response.getCharacterEncoding());

        // setLocale should change character encoding based on
        // locale-encoding-mapping-list
        response.setLocale(Locale.JAPAN);
        assertEquals("euc-jp", response.getCharacterEncoding());

        // setLocale should change character encoding based on
        // locale-encoding-mapping-list
        response.setLocale(Locale.CHINA);
        assertEquals("gb18030", response.getCharacterEncoding());

        // setContentType here doesn't define character encoding
        response.setContentType("text/html");
        assertEquals("gb18030", response.getCharacterEncoding());

        // setCharacterEncoding should still be able to change encoding
        response.setCharacterEncoding("utf-8");
        assertEquals("utf-8", response.getCharacterEncoding());

        // setLocale should not override explicit character encoding request
        response.setLocale(Locale.JAPAN);
        assertEquals("utf-8", response.getCharacterEncoding());

        // setContentType should still be able to change encoding
        response.setContentType("text/html;charset=gb18030");
        assertEquals("gb18030", response.getCharacterEncoding());

        // setCharacterEncoding should still be able to change encoding
        response.setCharacterEncoding("utf-8");
        assertEquals("utf-8", response.getCharacterEncoding());

        // getWriter should freeze the character encoding
        PrintWriter pw = response.getWriter();
        assertEquals("utf-8", response.getCharacterEncoding());

        // setCharacterEncoding should no longer be able to change the encoding
        response.setCharacterEncoding("iso-8859-1");
        assertEquals("utf-8", response.getCharacterEncoding());

        // setLocale should not override explicit character encoding request
        response.setLocale(Locale.JAPAN);
        assertEquals("utf-8", response.getCharacterEncoding());
    }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		9
-    
    public void testContentEncodingViaContentTypeChange() throws Exception
    {
        Response response = getResponse();
        response.setContentType("text/html;charset=Shift_Jis");
        assertEquals("Shift_Jis", response.getCharacterEncoding());
        
        response.setContentType("text/xml");
        assertEquals("Shift_Jis", response.getCharacterEncoding());@@@     }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		51
-    
    public void testSessionListenerOrdering()
        throws Exception
    {
        final StringBuffer result = new StringBuffer();

        class Listener1 implements HttpSessionListener
        {

            @Override
            public void sessionCreated(HttpSessionEvent se)
            {
                result.append("Listener1 create;");
            }

            @Override
            public void sessionDestroyed(HttpSessionEvent se)
            {
                result.append("Listener1 destroy;");
            }
        }

        class Listener2 implements HttpSessionListener
        {

            @Override
            public void sessionCreated(HttpSessionEvent se)
            {
                result.append("Listener2 create;");
            }

            @Override
            public void sessionDestroyed(HttpSessionEvent se)
            {
                result.append("Listener2 destroy;");
            }

        }

        Server server = new Server();
        SessionHandler sessionHandler = new SessionHandler();
        try
        {
            sessionHandler.addEventListener(new Listener1());
            sessionHandler.addEventListener(new Listener2());
            sessionHandler.setServer(server);
            sessionHandler.start();
            Session session = new Session(sessionHandler, new SessionData("aa", "_", "0.0", 0, 0, 0, 0));
            sessionHandler.callSessionCreatedListeners(session);
            sessionHandler.callSessionDestroyedListeners(session);
            assertEquals("Listener1 create;Listener2 create;Listener2 destroy;Listener1 destroy;", result.toString());
        }
        finally
        {
            sessionHandler.stop();
        }@@@+        assertThrows(IllegalArgumentException.class,() ->@@@+            sessionHandler.setSessionTrackingModes(new HashSet<>(Arrays.asList(SessionTrackingMode.SSL, SessionTrackingMode.URL))));@@@     }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		16
-    
    public void testNamedInclude() throws Exception
    {
        _contextHandler.addServlet(NamedIncludeServlet.class, "/include/*");
        String echo = _contextHandler.addServlet(EchoURIServlet.class, "/echo/*").getName();

        String expected =
            "HTTP/1.1 200 OK\r\n" +
                "Content-Length: 62\r\n" +
                "\r\n" +
                "/context\r\n" +
                "/include\r\n" +
                "/info\r\n" +
                "/context/include/info;param=value\r\n";
        String responses = _connector.getResponse("GET /context/include/info;param=value?name=" + echo + " HTTP/1.0\n\n");
        assertEquals(expected, responses);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 9
Lines of Code		25
-    
    public void testErrorOverridesMimeTypeAndCharset() throws Exception
    {
        StringBuilder rawRequest = new StringBuilder();
        rawRequest.append("GET /error-mime-charset-writer/ HTTP/1.1\r\n");
        rawRequest.append("Host: test\r\n");
        rawRequest.append("Connection: close\r\n");
        rawRequest.append("Accept: */*\r\n");
        rawRequest.append("Accept-Charset: *\r\n");
        rawRequest.append("\r\n");

        String rawResponse = _connector.getResponse(rawRequest.toString());
        System.out.println(rawResponse);
        HttpTester.Response response = HttpTester.parseResponse(rawResponse);

        assertThat(response.getStatus(), is(595));
        String actualContentType = response.get(HttpHeader.CONTENT_TYPE);
        // should not expect to see charset line from servlet
        assertThat(actualContentType, not(containsString("charset=US-ASCII")));
        String body = response.getContent();

        assertThat(body, containsString("ERROR_PAGE: /595"));
        assertThat(body, containsString("ERROR_MESSAGE: 595"));
        assertThat(body, containsString("ERROR_CODE: 595"));
        assertThat(body, containsString("ERROR_EXCEPTION: null"));
        assertThat(body, containsString("ERROR_EXCEPTION_TYPE: null"));
        assertThat(body, containsString("ERROR_SERVLET: org.eclipse.jetty.servlet.ErrorPageTest$ErrorContentTypeCharsetWriterInitializedServlet-"));
        assertThat(body, containsString("ERROR_REQUEST_URI: /error-mime-charset-writer/"));@@@     }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		20
-    
    public void testCreateInstance() throws Exception
    {
        try (StacklessLogging ignore = new StacklessLogging(ServletHandler.class, ServletContextHandler.class))
        {
            //test without a ServletContextHandler or current ContextHandler
            FilterHolder holder = new FilterHolder();
            holder.setName("foo");
            holder.setHeldClass(DummyFilter.class);
            Filter filter = holder.createInstance();
            assertNotNull(filter);

            //test with a ServletContextHandler
            Server server = new Server();
            ServletContextHandler context = new ServletContextHandler();
            server.setHandler(context);
            ServletHandler handler = context.getServletHandler();
            handler.addFilter(holder);
            holder.setServletHandler(handler);
            context.start();
            assertNotNull(holder.getFilter());
        }@@@     }
```
```
Cyclomatic Complexity	 2
Assertions		 2
Lines of Code		14
-    
    public void testCharsetResetToJsonMimeType() throws Exception
    {
        HttpTester.Request request = new HttpTester.Request();
        request.setMethod("GET");
        request.setURI("/charset/json-reset/");
        request.setVersion(HttpVersion.HTTP_1_1);
        request.setHeader("Connection", "close");
        request.setHeader("Host", "test");

        ByteBuffer responseBuffer = connector.getResponse(request.generate());
        // System.err.println(BufferUtil.toUTF8String(responseBuffer));
        HttpTester.Response response = HttpTester.parseResponse(responseBuffer);

        // Now test for properly formatted HTTP Response Headers.
        assertThat("Response Code", response.getStatus(), is(200));
        // The Content-Type should not have a charset= portion
        assertThat("Response Header Content-Type", response.get("Content-Type"), is("application/json"));
    }
```
```
Cyclomatic Complexity	 2
Assertions		 2
Lines of Code		14
-    
    public void testCharsetChangeToJsonMimeType() throws Exception
    {
        HttpTester.Request request = new HttpTester.Request();
        request.setMethod("GET");
        request.setURI("/charset/json-change/");
        request.setVersion(HttpVersion.HTTP_1_1);
        request.setHeader("Connection", "close");
        request.setHeader("Host", "test");

        ByteBuffer responseBuffer = connector.getResponse(request.generate());
        // System.err.println(BufferUtil.toUTF8String(responseBuffer));
        HttpTester.Response response = HttpTester.parseResponse(responseBuffer);

        // Now test for properly formatted HTTP Response Headers.
        assertThat("Response Code", response.getStatus(), is(200));
        // The Content-Type should not have a charset= portion
        assertThat("Response Header Content-Type", response.get("Content-Type"), is("application/json"));
    }
```
```
Cyclomatic Complexity	 2
Assertions		 2
Lines of Code		14
-    
    public void testCharsetChangeToJsonMimeTypeSetCharsetToNull() throws Exception
    {
        HttpTester.Request request = new HttpTester.Request();
        request.setMethod("GET");
        request.setURI("/charset/json-change-null/");
        request.setVersion(HttpVersion.HTTP_1_1);
        request.setHeader("Connection", "close");
        request.setHeader("Host", "test");

        ByteBuffer responseBuffer = connector.getResponse(request.generate());
        // System.err.println(BufferUtil.toUTF8String(responseBuffer));
        HttpTester.Response response = HttpTester.parseResponse(responseBuffer);

        // Now test for properly formatted HTTP Response Headers.
        assertThat("Response Code", response.getStatus(), is(200));
        // The Content-Type should not have a charset= portion
        assertThat("Response Header Content-Type", response.get("Content-Type"), is("application/json"));@@@     }
```
```
Cyclomatic Complexity	 1
Assertions		 3
Lines of Code		23
-    
    public void testInitParams() throws Exception
    {
        //Test get/setInitParam with null throws NPE
        ServletContextHandler root = new ServletContextHandler(_server, "/", ServletContextHandler.SESSIONS);
        _server.setHandler(root);
        ListenerHolder initialListener = new ListenerHolder();
        initialListener.setListener(new ServletContextListener()
        {
            public void contextInitialized(ServletContextEvent sce)
            {
                sce.getServletContext().setInitParameter("foo", "bar");
                assertEquals("bar", sce.getServletContext().getInitParameter("foo"));
                assertThrows(NullPointerException.class, 
                    () ->  sce.getServletContext().setInitParameter(null, "bad")
                );
                assertThrows(NullPointerException.class,
                    () -> sce.getServletContext().getInitParameter(null)
                );
            }
        });
        
        root.getServletHandler().addListener(initialListener);
        _server.start();
    }
```
```
Cyclomatic Complexity	 1
Assertions		 6
Lines of Code		18
-    
    public void testGetSetSessionTimeout() throws Exception
    {
        ContextHandlerCollection contexts = new ContextHandlerCollection();
        _server.setHandler(contexts);

        int startMin = 7;
        Integer timeout = Integer.valueOf(100);
        ServletContextHandler root = new ServletContextHandler(contexts, "/", ServletContextHandler.SESSIONS);
        root.getSessionHandler().setMaxInactiveInterval((int)TimeUnit.MINUTES.toSeconds(startMin));
        root.addBean(new MySCIStarter(root.getServletContext(), new MySCI(true, timeout.intValue())), true);
        _server.start();
        
        //test starting value of setSessionTimeout
        assertEquals(startMin, (Integer)root.getServletContext().getAttribute("MYSCI.startSessionTimeout"));
        //test can set session timeout from ServletContainerInitializer
        assertTrue((Boolean)root.getServletContext().getAttribute("MYSCI.setSessionTimeout"));
        //test can get session timeout from ServletContainerInitializer
        assertEquals(timeout, (Integer)root.getServletContext().getAttribute("MYSCI.getSessionTimeout"));
        assertNull(root.getAttribute("MYSCI.sessionTimeoutFailure"));
        //test can't get session timeout from ContextListener that is not from annotation or web.xml
        assertTrue((Boolean)root.getServletContext().getAttribute("MyContextListener.getSessionTimeout"));
        //test can't set session timeout from ContextListener that is not from annotation or web.xml
        assertTrue((Boolean)root.getServletContext().getAttribute("MyContextListener.setSessionTimeout"));@@@     }
```
```
Cyclomatic Complexity	 1
Assertions		 0
Lines of Code		40
-    
    public void testCreateMethodsFromSCI() throws Exception
    {
        //A filter can be created by an SCI
        ContextHandlerCollection contexts = new ContextHandlerCollection();
        _server.setHandler(contexts);

        ServletContextHandler root = new ServletContextHandler(contexts, "/");
        class FilterCreatingSCI implements ServletContainerInitializer
        {
            @Override
            public void onStartup(Set<Class<?>> c, ServletContext ctx) throws ServletException
            {
                try
                {
                    ctx.createFilter(MyFilter.class);
                }
                catch (Exception e)
                {
                    fail(e);
                }

                try
                {
                    ctx.createServlet(HelloServlet.class);
                }
                catch (Exception e)
                {
                    fail(e);
                }

                try
                {
                    ctx.createListener(MyContextListener.class);
                }
                catch (Exception e)
                {
                    fail(e);
                }
            }
        }
        
        root.addBean(new MySCIStarter(root.getServletContext(), new FilterCreatingSCI()), true);
        _server.start();    
    }
```
```
Cyclomatic Complexity	 1
Assertions		 3
Lines of Code		20
-    
    public void testCreateMethodsFromSCL() throws Exception
    {
      //A filter can be created by an SCI
        ContextHandlerCollection contexts = new ContextHandlerCollection();
        _server.setHandler(contexts);

        ServletContextHandler root = new ServletContextHandler(contexts, "/");
        class ListenerCreatingSCI implements ServletContainerInitializer
        {
            @Override
            public void onStartup(Set<Class<?>> c, ServletContext ctx) throws ServletException
            {
                ctx.addListener(new CreatingSCL());
            }
        }
        
        root.addBean(new MySCIStarter(root.getServletContext(), new ListenerCreatingSCI()), true);
        _server.start();
        assertTrue((Boolean)root.getServletContext().getAttribute("CreatingSCL.filter"));
        assertTrue((Boolean)root.getServletContext().getAttribute("CreatingSCL.servlet"));
        assertTrue((Boolean)root.getServletContext().getAttribute("CreatingSCL.listener"));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 4
Lines of Code		34
-    
    public void testAddJspFile() throws Exception
    {
        ContextHandlerCollection contexts = new ContextHandlerCollection();
        _server.setHandler(contexts);

        ServletContextHandler root = new ServletContextHandler(contexts, "/");
        ServletHolder jspServlet = new ServletHolder();
        jspServlet.setName("jsp");
        jspServlet.setHeldClass(FakeJspServlet.class);
        root.addServlet(jspServlet, "*.jsp");
        class JSPAddingSCI implements ServletContainerInitializer
        {
            @Override
            public void onStartup(Set<Class<?>> c, ServletContext ctx) throws ServletException
            {
                try
                {
                    ServletRegistration rego = ctx.addJspFile("some.jsp", "/path/to/some.jsp");
                    rego.addMapping("/somejsp/*");
                }
                catch (Exception e)
                {
                    fail(e);
                }
            }
        }

        assertThrows(IllegalArgumentException.class, () ->  root.getServletContext().addJspFile(null, "/path/to/some.jsp"));
        assertThrows(IllegalArgumentException.class, () ->  root.getServletContext().addJspFile("", "/path/to/some.jsp"));

        root.addBean(new MySCIStarter(root.getServletContext(), new JSPAddingSCI()), true);
        _server.start();
        ServletHandler.MappedServlet mappedServlet = root.getServletHandler().getMappedServlet("/somejsp/xxx");
        assertNotNull(mappedServlet.getServletHolder());
        assertEquals("some.jsp", mappedServlet.getServletHolder().getName());
    }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		33
-    
    public void testAddJspFileWithExistingRegistration() throws Exception
    {
        ContextHandlerCollection contexts = new ContextHandlerCollection();
        _server.setHandler(contexts);

        ServletContextHandler root = new ServletContextHandler(contexts, "/");
        ServletHolder jspServlet = new ServletHolder();
        jspServlet.setName("jsp");
        jspServlet.setHeldClass(FakeJspServlet.class);
        root.addServlet(jspServlet, "*.jsp");
        //add a full registration so that the addJspFile will fail
        ServletHolder barServlet = new ServletHolder();
        barServlet.setName("some.jsp");
        barServlet.setHeldClass(HelloServlet.class);
        root.addServlet(barServlet, "/bar/*");
        class JSPAddingSCI implements ServletContainerInitializer
        {
            @Override
            public void onStartup(Set<Class<?>> c, ServletContext ctx) throws ServletException
            {
                try
                {
                    ServletRegistration rego = ctx.addJspFile("some.jsp", "/path/to/some.jsp");
                    assertNull(rego);
                }
                catch (Exception e)
                {
                    fail(e);
                }
            }
        }

        root.addBean(new MySCIStarter(root.getServletContext(), new JSPAddingSCI()), true);
        _server.start();
    }
```
```
Cyclomatic Complexity	 1
Assertions		 3
Lines of Code		35
-    
    public void testAddJspFileWithPartialRegistration() throws Exception
    {
        ContextHandlerCollection contexts = new ContextHandlerCollection();
        _server.setHandler(contexts);

        ServletContextHandler root = new ServletContextHandler(contexts, "/");
        ServletHolder jspServlet = new ServletHolder();
        jspServlet.setName("jsp");
        jspServlet.setHeldClass(FakeJspServlet.class);
        root.addServlet(jspServlet, "*.jsp");
        //add a preliminary registration so that the addJspFile will complete it
        ServletHolder barServlet = new ServletHolder();
        barServlet.setName("some.jsp");
        root.addServlet(barServlet, "/bar/*");
        class JSPAddingSCI implements ServletContainerInitializer
        {
            @Override
            public void onStartup(Set<Class<?>> c, ServletContext ctx) throws ServletException
            {
                try
                {
                    ServletRegistration rego = ctx.addJspFile("some.jsp", "/path/to/some.jsp");
                    assertNotNull(rego);
                }
                catch (Exception e)
                {
                    fail(e);
                }
            }
        }

        root.addBean(new MySCIStarter(root.getServletContext(), new JSPAddingSCI()), true);
        _server.start();
        ServletHandler.MappedServlet mappedServlet = root.getServletHandler().getMappedServlet("/bar/xxx");
        assertNotNull(mappedServlet.getServletHolder());
        assertEquals("some.jsp", mappedServlet.getServletHolder().getName());
    }
```
```
Cyclomatic Complexity	 1
Assertions		 4
Lines of Code		15
-    
    public void testProvidersUsingDefault() throws Exception
    {
        Path homePath = MavenTestingUtils.getTestResourceDir("providers-home").toPath().toRealPath();

        List<String> cmdLineArgs = new ArrayList<>();
        cmdLineArgs.add("user.dir=" + homePath);
        cmdLineArgs.add("jetty.home=" + homePath);
        cmdLineArgs.add("--module=server");

        Main main = new Main();
        StartArgs args = main.processCommandLine(cmdLineArgs.toArray(new String[cmdLineArgs.size()]));
        BaseHome baseHome = main.getBaseHome();

        assertThat("jetty.home", baseHome.getHome(), is(homePath.toString()));
        assertThat("jetty.base", baseHome.getBase(), is(homePath.toString()));

        ConfigurationAssert.assertConfiguration(baseHome, args, "assert-providers-default.txt");
    }
```
```
Cyclomatic Complexity	 1
Assertions		 4
Lines of Code		16
-    
    public void testProvidersUsingSpecific() throws Exception
    {
        Path homePath = MavenTestingUtils.getTestResourceDir("providers-home").toPath().toRealPath();

        List<String> cmdLineArgs = new ArrayList<>();
        cmdLineArgs.add("user.dir=" + homePath);
        cmdLineArgs.add("jetty.home=" + homePath);
        cmdLineArgs.add("--module=server");
        cmdLineArgs.add("--module=logging-b");

        Main main = new Main();
        StartArgs args = main.processCommandLine(cmdLineArgs.toArray(new String[cmdLineArgs.size()]));
        BaseHome baseHome = main.getBaseHome();

        assertThat("jetty.home", baseHome.getHome(), is(homePath.toString()));
        assertThat("jetty.base", baseHome.getBase(), is(homePath.toString()));

        ConfigurationAssert.assertConfiguration(baseHome, args, "assert-providers-specific.txt");@@@     }
```
```
Cyclomatic Complexity	 1
Assertions		 4
Lines of Code		10
-    
    public void testCopyIndirect()
    {
        ByteBuffer b = BufferUtil.toBuffer("Hello World");
        ByteBuffer c = BufferUtil.copy(b);
        assertEquals("Hello World", BufferUtil.toString(c));
        assertFalse(c.isDirect());
        assertThat(b, not(sameInstance(c)));
        assertThat(b.array(), not(sameInstance(c.array())));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 3
Lines of Code		10
-    
    public void testCopyDirect()
    {
        ByteBuffer b = BufferUtil.allocateDirect(11);
        BufferUtil.append(b, "Hello World");
        ByteBuffer c = BufferUtil.copy(b);
        assertEquals("Hello World", BufferUtil.toString(c));
        assertTrue(c.isDirect());
        assertThat(b, not(sameInstance(c)));@@@     }
```
```
Cyclomatic Complexity	 1
Assertions		 3
Lines of Code		13
-    
    public void testJoinWithStopTimeout() throws Exception
    {
        final long stopTimeout = 100;
        QueuedThreadPool threadPool = new QueuedThreadPool();
        threadPool.setStopTimeout(100);
        threadPool.start();

        // Verify that join does not timeout after waiting twice the stopTimeout.
        assertThrows(Throwable.class, () ->
            assertTimeoutPreemptively(Duration.ofMillis(stopTimeout * 2), threadPool::join)
        );

        // After stopping the ThreadPool join should unblock.
        LifeCycle.stop(threadPool);
        assertTimeoutPreemptively(Duration.ofMillis(stopTimeout), threadPool::join);@@@     }
```
```
Cyclomatic Complexity	 2
Assertions		 2
Lines of Code		17
-    
    public void testFindAndFilterContainerPathsJDK9() throws Exception
    {
        MetaInfConfiguration config = new MetaInfConfiguration();
        WebAppContext context = new WebAppContext();
        context.setAttribute(MetaInfConfiguration.CONTAINER_JAR_PATTERN, ".*/jetty-util-[^/]*\\.jar$|.*/jetty-util/target/classes/$|.*/foo-bar-janb.jar");
        WebAppClassLoader loader = new WebAppClassLoader(context);
        context.setClassLoader(loader);
        config.findAndFilterContainerPaths(context);
        List<Resource> containerResources = context.getMetaData().getContainerResources();
        assertEquals(2, containerResources.size());
        for (Resource r : containerResources)
        {
            String s = r.toString();
            assertTrue(s.endsWith("foo-bar-janb.jar") || s.contains("jetty-util"));
        }@@@     }
```
```
Cyclomatic Complexity	 1
Assertions		 9
Lines of Code		43
-    
    public void testDefaultContextPath() throws Exception
    {
        Server server = new Server();
        File webXml = MavenTestingUtils.getTestResourceFile("web-with-default-context-path.xml");
        File webXmlEmptyPath = MavenTestingUtils.getTestResourceFile("web-with-empty-default-context-path.xml");
        File webDefaultXml = MavenTestingUtils.getTestResourceFile("web-default-with-default-context-path.xml");
        File overrideWebXml = MavenTestingUtils.getTestResourceFile("override-web-with-default-context-path.xml");
        assertNotNull(webXml);
        assertNotNull(webDefaultXml);
        assertNotNull(overrideWebXml);
        assertNotNull(webXmlEmptyPath);
        
        try
        {
            WebAppContext wac = new WebAppContext();
            wac.setResourceBase(MavenTestingUtils.getTargetTestingDir().getAbsolutePath());
            server.setHandler(wac);
            
            //test that an empty default-context-path defaults to root
            wac.setDescriptor(webXmlEmptyPath.getAbsolutePath());
            server.start();
            assertEquals("/", wac.getContextPath());
            
            server.stop();
            
            //test web-default.xml value is used
            wac.setDescriptor(null);
            wac.setDefaultsDescriptor(webDefaultXml.getAbsolutePath());
            server.start();
            assertEquals("/one", wac.getContextPath());
            
            server.stop();
            
            //test web.xml value is used
            wac.setDescriptor(webXml.getAbsolutePath());
            server.start();
            assertEquals("/two", wac.getContextPath());
            
            server.stop();
            
            //test override-web.xml value is used
            wac.setOverrideDescriptor(overrideWebXml.getAbsolutePath());
            server.start();
            assertEquals("/three", wac.getContextPath());

            server.stop();
            
            //test that explicitly set context path is used instead
            wac.setContextPath("/foo");
            server.start();
            assertEquals("/foo", wac.getContextPath());
        }
        finally
        {
            server.stop();@@@         }@@@     }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		11
-    
    public void testSetWithNullProperty() throws Exception
    {
        XmlConfiguration configuration = asXmlConfiguration("<Configure class=\"org.eclipse.jetty.xml.TestConfiguration\"><Set name=\"TestString\" property=\"prop\" id=\"test\"/></Configure>");
        configuration.getProperties().remove("prop");
        TestConfiguration tc = new TestConfiguration();
        tc.setTestString("default");
        configuration.configure(tc);
        assertEquals("default", tc.getTestString());
        assertNull(configuration.getIdMap().get("test"));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		11
-    
    public void testSetWithPropertyAndValue() throws Exception
    {
        XmlConfiguration configuration = asXmlConfiguration("<Configure class=\"org.eclipse.jetty.xml.TestConfiguration\"><Set name=\"TestString\" property=\"prop\" id=\"test\">Value</Set></Configure>");
        configuration.getProperties().put("prop", "This is a property value");
        TestConfiguration tc = new TestConfiguration();
        tc.setTestString("default");
        configuration.configure(tc);
        assertEquals("Value", tc.getTestString());
        assertEquals(configuration.getIdMap().get("test"), "Value");
    }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		11
-    
    public void testSetWithNullPropertyAndValue() throws Exception
    {
        XmlConfiguration configuration = asXmlConfiguration("<Configure class=\"org.eclipse.jetty.xml.TestConfiguration\"><Set name=\"TestString\" property=\"prop\" id=\"test\">Value</Set></Configure>");
        configuration.getProperties().remove("prop");
        TestConfiguration tc = new TestConfiguration();
        tc.setTestString("default");
        configuration.configure(tc);
        assertEquals("default", tc.getTestString());
        assertNull(configuration.getIdMap().get("test"));
    }
```
```
Cyclomatic Complexity	 1
Assertions		 11
Lines of Code		41
-    
    public void testQuickStartGenerationAndRun() throws Exception
    {
        String jettyVersion = System.getProperty("jettyVersion");
        DistributionTester distribution = DistributionTester.Builder.newInstance()
            .jettyVersion(jettyVersion)
            .mavenLocalRepository(System.getProperty("mavenRepoPath"))
            .build();

        String[] args1 = {
            "--create-startd",
            "--approve-all-licenses",
            "--add-to-start=resources,server,http,webapp,deploy,jsp,servlet,servlets,quickstart" 
        };
        
        try (DistributionTester.Run run1 = distribution.start(args1))
        {
            assertTrue(run1.awaitFor(5, TimeUnit.SECONDS));
            assertEquals(0, run1.getExitValue());

            File war = distribution.resolveArtifact("org.eclipse.jetty.tests:test-simple-webapp:war:" + jettyVersion);
            distribution.installWarFile(war, "test");

     
            try (DistributionTester.Run run2 = distribution.start("jetty.quickstart.mode=GENERATE"))
            {
                assertTrue(run2.awaitConsoleLogsFor("QuickStartGeneratorConfiguration:main: Generated", 10, TimeUnit.SECONDS));
                Path unpackedWebapp = distribution.getJettyBase().resolve("webapps").resolve("test");
                assertTrue(Files.exists(unpackedWebapp));
                Path webInf = unpackedWebapp.resolve("WEB-INF");
                assertTrue(Files.exists(webInf));
                Path quickstartWebXml = webInf.resolve("quickstart-web.xml");
                assertTrue(Files.exists(quickstartWebXml));
                assertNotEquals(0, Files.size(quickstartWebXml));
                
                int port = distribution.freePort();
                
                try (DistributionTester.Run run3 = distribution.start("jetty.http.port=" + port, "jetty.quickstart.mode=QUICKSTART"))
                {
                    assertTrue(run3.awaitConsoleLogsFor("Started Server@", 10, TimeUnit.SECONDS));

                    startHttpClient();
                    ContentResponse response = client.GET("http://localhost:" + port + "/test/index.jsp");
                    assertEquals(HttpStatus.OK_200, response.getStatus());
                    assertThat(response.getContentAsString(), containsString("Hello"));
                    assertThat(response.getContentAsString(), not(containsString("<%")));
                }
            }
        }
    }
```
```
Cyclomatic Complexity	 1
Assertions		 4
Lines of Code		32
-    
    public void testWebAppWithProxyAndJPMS() throws Exception
    {
        String jettyVersion = System.getProperty("jettyVersion");
        DistributionTester distribution = DistributionTester.Builder.newInstance()
            .jettyVersion(jettyVersion)
            .mavenLocalRepository(System.getProperty("mavenRepoPath"))
            .build();

        String[] args1 = {
            "--create-startd",
            "--add-to-start=http,webapp,deploy,resources"
        };
        try (DistributionTester.Run run1 = distribution.start(args1))
        {
            assertTrue(run1.awaitFor(5, TimeUnit.SECONDS));
            assertEquals(0, run1.getExitValue());

            Path logFile = distribution.getJettyBase().resolve("resources").resolve("jetty-logging.properties");
            try (BufferedWriter writer = Files.newBufferedWriter(logFile, StandardCharsets.UTF_8, StandardOpenOption.CREATE))
            {
                writer.write("org.eclipse.jetty.LEVEL=INFO");
            }

            File war = distribution.resolveArtifact("org.eclipse.jetty.tests:test-proxy-webapp:war:" + jettyVersion);
            distribution.installWarFile(war, "proxy");

            int port = distribution.freePort();
            try (DistributionTester.Run run2 = distribution.start("--jpms", "jetty.http.port=" + port, "jetty.server.dumpAfterStart=true"))
            {
                assertTrue(run2.awaitConsoleLogsFor("Started Server@", 10, TimeUnit.SECONDS));

                startHttpClient(() -> new HttpClient(new HttpClientTransportOverHTTP(1)));
                ContentResponse response = client.GET("http://localhost:" + port + "/proxy/current/");
                assertEquals(HttpStatus.OK_200, response.getStatus());
            }
        }
    }
```
```
Cyclomatic Complexity	 1
Assertions		 5
Lines of Code		35
-    
    public void testStartStopLog4j2Modules() throws Exception
    {
        Path jettyBase = Files.createTempDirectory("jetty_base");

        String jettyVersion = System.getProperty("jettyVersion");
        DistributionTester distribution = DistributionTester.Builder.newInstance() //
            .jettyVersion(jettyVersion) //
            .jettyBase(jettyBase) //
            .mavenLocalRepository(System.getProperty("mavenRepoPath")) //
            .build();

        String[] args = {
            "--create-startd",
            "--approve-all-licenses",
            "--add-to-start=http,logging-log4j2"
        };

        try (DistributionTester.Run run1 = distribution.start(args))
        {
            assertTrue(run1.awaitFor(5, TimeUnit.SECONDS));
            assertEquals(0, run1.getExitValue());

            Files.copy(Paths.get("src/test/resources/log4j2.xml"), //
                       Paths.get(jettyBase.toString(),"resources").resolve("log4j2.xml"), //
                       StandardCopyOption.REPLACE_EXISTING);

            int port = distribution.freePort();
            try (DistributionTester.Run run2 = distribution.start("jetty.http.port=" + port))
            {
                assertTrue(run2.awaitLogsFileFor(
                    jettyBase.resolve("logs").resolve("jetty.log"), //
                    "Started Server@", 10, TimeUnit.SECONDS));

                startHttpClient();
                ContentResponse response = client.GET("http://localhost:" + port);
                assertEquals(HttpStatus.NOT_FOUND_404, response.getStatus());

                run2.stop();
                assertTrue(run2.awaitFor(5, TimeUnit.SECONDS));
            }
        }
    }
```
