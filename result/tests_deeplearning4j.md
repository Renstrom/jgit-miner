## e746bcf7c9a21a8287fbb2bcf7ef9f5ff9cfaf88 ##
```
Cyclomatic Complexity	1
Assertions		0
Lines of Code		4
-    
    public void testOther() {
        SameDiff importAssertion = SameDiff.importFrozenTF(new File("C:\\Users\\agibs\\AppData\\Local\\Temp\\frozen_model.pb1052249717728562635tmp"));
    }
```
## 394cf869eee1f773ca0947dc0f67805e46a8eb30 ##
```
Cyclomatic Complexity	1
Assertions		0
Lines of Code		11
-    
    fun testMobileNet() {
        Nd4j.getExecutioner().enableDebugMode(true)
        Nd4j.getExecutioner().enableVerboseMode(true)
        val importer = OnnxFrameworkImporter()
        val resource = ClassPathResource("mobilenet.onnx").file
        val inputs = Nd4j.ones(1,3,224,224)
        val inputDict = mapOf("input.1" to inputs)
        val runImport = importer.runImport(resource.absolutePath,inputDict)
        runImport.outputAll(inputDict)
    }
```
## 92dd8acd88ecb60eee714c8221dad84f2e8727e7 ##
```
Cyclomatic Complexity	1
Assertions		0
Lines of Code		12
-    
    fun testMobileNet() {
        System.setProperty("org.bytedeco.javacpp.nopointergc","true")
        Nd4j.getExecutioner().enableDebugMode(true)
        Nd4j.getExecutioner().enableVerboseMode(true)
        val importer = OnnxFrameworkImporter()
        val resource = ClassPathResource("mobilenet.onnx").file
        val inputs = Nd4j.ones(1,3,224,224)
        val inputDict = mapOf("input.1" to inputs)
        val runImport = importer.runImport(resource.absolutePath,inputDict)
        runImport.outputAll(inputDict)
    }
```
