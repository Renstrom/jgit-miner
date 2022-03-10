## JGIT Miner ##


Simple JGIT dataminer to log certain commit histories of open-source repositories parsed. 

Author : renstr


##How to Run from repo ##

### Requirements ###

Java version 11+

Make sure you have the following program installed:

[JAVACC](javacc.github.io/javacc)

How to run:

```
1. mvn clean install 
2. jjjtree src/main/java/javacc/sample.jjt
3. javacc src/main/java/javacc/sample.jj
4. javac src/main/java/javacc/*.java
5. javac src/main/java/Miner.java
6. java src/main/java/Miner
```



