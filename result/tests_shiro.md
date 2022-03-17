## eb8662d91fb5bfe49605d9815f5cd113c830fcf4 ##
```
Cyclomatic Complexity	1
Assertions		1
Lines of Code		6
-    
    void testEncryptPasswordWithInvalidMatch() {
        def service = new DefaultPasswordService()
        def encrypted = service.encryptPassword("ABCDEF")
        assertFalse service.passwordsMatch("ABC", encrypted)
    }
```
```
Cyclomatic Complexity	 1
Assertions		 3
Lines of Code		12
-    
    void testBackwardsCompatibility() {
        def service = new DefaultPasswordService()
        def encrypted = service.encryptPassword("12345")
        def submitted = "12345"
        assertTrue service.passwordsMatch(submitted, encrypted);

        //change some settings:
        service.hashService.hashAlgorithmName = "MD5"
        service.hashService.hashIterations = 250000

        def encrypted2 = service.encryptPassword(submitted)

        assertFalse encrypted == encrypted2

        assertTrue service.passwordsMatch(submitted, encrypted2)
    }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		8
-    
    void testStringComparisonWhenNotUsingAParsableHashFormat() {

        def service = new DefaultPasswordService()
        service.hashFormat = new HexFormat()
        //can't use random salts when using HexFormat:
        service.hashService.generatePublicSalt = false

        def formatted = service.encryptPassword("12345")

        assertTrue service.passwordsMatch("12345", formatted)
    }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		6
-    
    void testDifferentIterations() {
        def service = new DefaultHashService(hashIterations: 2)
        def hash = hash(service, "test")
        assertEquals 2, hash.iterations
    }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		10
-    
    void testDifferentRandomNumberGenerator() {

        def ByteSource randomBytes = new SecureRandomNumberGenerator().nextBytes()
        def rng = createMock(RandomNumberGenerator)
        expect(rng.nextBytes()).andReturn randomBytes

        replay rng

        def service = new DefaultHashService(randomNumberGenerator: rng, generatePublicSalt: true)
        hash(service, "test")

        verify rng
    }
```
## 264136ef9b2b392239e1d36f5f78a0bef17b7ecb ##
```
Cyclomatic Complexity	1
Assertions		1
Lines of Code		6
-    
    void testEncryptPasswordWithInvalidMatch() {
        def service = new DefaultPasswordService()
        def encrypted = service.encryptPassword("ABCDEF")
        assertFalse service.passwordsMatch("ABC", encrypted)
    }
```
```
Cyclomatic Complexity	 1
Assertions		 3
Lines of Code		12
-    
    void testBackwardsCompatibility() {
        def service = new DefaultPasswordService()
        def encrypted = service.encryptPassword("12345")
        def submitted = "12345"
        assertTrue service.passwordsMatch(submitted, encrypted);

        //change some settings:
        service.hashService.hashAlgorithmName = "MD5"
        service.hashService.hashIterations = 250000

        def encrypted2 = service.encryptPassword(submitted)

        assertFalse encrypted == encrypted2

        assertTrue service.passwordsMatch(submitted, encrypted2)
    }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		8
-    
    void testStringComparisonWhenNotUsingAParsableHashFormat() {

        def service = new DefaultPasswordService()
        service.hashFormat = new HexFormat()
        //can't use random salts when using HexFormat:
        service.hashService.generatePublicSalt = false

        def formatted = service.encryptPassword("12345")

        assertTrue service.passwordsMatch("12345", formatted)
    }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		6
-    
    void testDifferentIterations() {
        def service = new DefaultHashService(hashIterations: 2)
        def hash = hash(service, "test")
        assertEquals 2, hash.iterations
    }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		10
-    
    void testDifferentRandomNumberGenerator() {

        def ByteSource randomBytes = new SecureRandomNumberGenerator().nextBytes()
        def rng = createMock(RandomNumberGenerator)
        expect(rng.nextBytes()).andReturn randomBytes

        replay rng

        def service = new DefaultHashService(randomNumberGenerator: rng, generatePublicSalt: true)
        hash(service, "test")

        verify rng
    }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		13
-    
    public void testGetChainWhenPathEndsWithSlash() {
        HttpServletRequest request = createNiceMock(HttpServletRequest.class);
        HttpServletResponse response = createNiceMock(HttpServletResponse.class);
        FilterChain chain = createNiceMock(FilterChain.class);

        //ensure at least one chain is defined:
        resolver.getFilterChainManager().addToChain("/resource/*/book", "authcBasic");

        expect(request.getServletPath()).andReturn("");
        expect(request.getPathInfo()).andReturn("/resource/123/book/");
        replay(request);

        FilterChain resolved = resolver.getChain(request, response, chain);
        assertNotNull(resolved);
        verify(request);
    }
```
```
Cyclomatic Complexity	 1
Assertions		 2
Lines of Code		13
-    
    public void testGetChainWhenPathDoesNotEndWithSlash() {
        HttpServletRequest request = createNiceMock(HttpServletRequest.class);
        HttpServletResponse response = createNiceMock(HttpServletResponse.class);
        FilterChain chain = createNiceMock(FilterChain.class);

        //ensure at least one chain is defined:
        resolver.getFilterChainManager().addToChain("/resource/*/book", "authcBasic");

        expect(request.getServletPath()).andReturn("");
        expect(request.getPathInfo()).andReturn("/resource/123/book");
        replay(request);

        FilterChain resolved = resolver.getChain(request, response, chain);
        assertNotNull(resolved);
        verify(request);@@@     }
```
