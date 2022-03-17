## b6fc9b4724a7c63d499c87ac0c4f4e17893fcbbf ##
```
Cyclomatic Complexity	1
Assertions		1
Lines of Code		6
-    
    public void checkEmptyTokenDoesNotPresentInHeader() throws Exception {
        String influxdbUrl = getInfluxDbUrl();
        setupSenderAndSendMetric(influxdbUrl, "");

        assertNoAuthHeader(resultQueue.take());
    }
```
```
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		6
-    
    public void checkEmptyOnlyWhitespaceTokenDoesNotPresentInHeader() throws Exception {
        String influxdbUrl = getInfluxDbUrl();
        setupSenderAndSendMetric(influxdbUrl, "  ");

        assertNoAuthHeader(resultQueue.take());
    }
```
## 4618f816c45d69d3b19c0c83f19cf7f5752eafc0 ##
```
Cyclomatic Complexity	1
Assertions		0
Lines of Code		5
-    (expected=InvalidVariableException.class)
	public void testChangeCaseError() throws Exception {
		changeCase.setParameters(params);
		changeCase.execute(result, null);
	}
```
