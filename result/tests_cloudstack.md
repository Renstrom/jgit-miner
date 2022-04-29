## eb60ac976b99fd8c33a09288e32dc3696ad27a53 ##
```
Commit	eb60ac976b99fd8c33a09288e32dc3696ad27a53
Directory name		utils
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		10
    
    public void testResolution() {
        long nanoTime1 = 0l;
        long nanoTime2 = 0l;
        nanoTime1 = System.nanoTime();
        nanoTime2 = System.nanoTime();

        // Using sysout here because is faster than the logger and we don't want to
        // waste time.
        System.out.println("Nano time 1: " + nanoTime1);
        System.out.println("Nano time 2: " + nanoTime2);

        // We are measuring the elapsed time in 2 consecutive calls of System.nanoTime()
        // That's the same as 0.002 milliseconds or 2000 nanoseconds.
        Assert.assertTrue("Expected exactly 2 but it took more than 3 microseconds between 2 consecutive calls to System.nanoTime().", nanoTime2 - nanoTime1 <= 3000);
    }
```
## d74d134698ce381b5d18d6c71815f5042ffd1816 ##
```
Commit	d74d134698ce381b5d18d6c71815f5042ffd1816
Directory name		utils
Cyclomatic Complexity	 1
Assertions		 1
Lines of Code		10
    
    public void testResolution() {
        long nanoTime1 = 0l;
        long nanoTime2 = 0l;
        nanoTime1 = System.nanoTime();
        nanoTime2 = System.nanoTime();

        // Using sysout here because is faster than the logger and we don't want to
        // waste time.
        System.out.println("Nano time 1: " + nanoTime1);
        System.out.println("Nano time 2: " + nanoTime2);

        // We are measuring the elapsed time in 2 consecutive calls of System.nanoTime()
        // That's the same as 0.002 milliseconds or 2000 nanoseconds.
        Assert.assertTrue("Expected exactly 2 but it took more than 3 microseconds between 2 consecutive calls to System.nanoTime().", nanoTime2 - nanoTime1 <= 3000);
    }
```
