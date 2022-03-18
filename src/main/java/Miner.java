import java.io.*;
import java.nio.charset.StandardCharsets;

import java.text.SimpleDateFormat;
import java.util.*;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.api.errors.JGitInternalException;

import org.eclipse.jgit.diff.*;
import org.eclipse.jgit.internal.storage.file.FileRepository;
import org.eclipse.jgit.lib.*;
import org.eclipse.jgit.patch.FileHeader;
import org.eclipse.jgit.revwalk.RevCommit;

import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.revwalk.filter.MessageRevFilter;
import org.eclipse.jgit.revwalk.filter.RevFilter;


import org.eclipse.jgit.treewalk.TreeWalk;
import org.eclipse.jgit.treewalk.filter.PathSuffixFilter;
import org.eclipse.jgit.treewalk.filter.TreeFilter;



class Pair {
    public static <T, U> Map.Entry<T,U> of(T first, U second){
        return new AbstractMap.SimpleEntry<>(first,second);
    }
}


/**
 * Algorithm to mine GIT Commit histories using JGit
 * src/main/java/Miner.java match()
 * @author Anders Renström
 */
public class Miner {


    static ArrayList<String> fullData                    = new ArrayList<>();
    static ArrayList<String> hashValues                  = new ArrayList<>();
    static HashMap<String, ArrayList<String>> tests      = new HashMap<>();

    static ByteArrayOutputStream out    = new ByteArrayOutputStream();
    static String REPODIRECTORY         = "gitRepos/repos/"; // Directory in which all repos are saved
    static String PATHGITDIRECTORY      = REPODIRECTORY + "/.git"; // Local directory to git repo
    static String URLTOREPO             = "https://github.com/iluwatar/java-design-patterns.git"; // URL to repo
    static String OUTPUTPATH            = "java-design-patterns";
    static int nrOfTests                = 0;
    static int[] numberOfAsserts        = new int[200];


    //  {commitid,asserts loc cyclomaticComplexity asserts/loc cyclomaticComplexity/loc};
    static HashMap<String, String>              detailedCommitInformation     = new HashMap<>(200);


    static HashMap<Integer,Integer>             numberOfLoc                   = new HashMap<>(200);
    static HashMap<Integer,Integer>             numberOfCyclomaticComplexity  = new HashMap<>(200);
    static HashMap<String, Integer>             numberOfRemovedTestsPerCommit = new HashMap<>(200);
   // static HashMap<String, ArrayList<String>> methodTest           = new HashMap<>(200);

    static FileOutputStream f;


    // Pattern to find the correct commits
    final static Pattern COMMITPATTERN = Pattern.compile(
            "((remov(e|ed|ing))|(refact(or|ing|ored)\\s)|(\\supdat(e|ing|es)\\s)).*(test|\\stest(s|er|ing|ed)\\s)", Pattern.CASE_INSENSITIVE);

    /**
     * Checks if test is almost completely removed
     * @param input Test function
     * @return false if the test is removed
     */
    private static boolean filterOutput(String input) {
        double x = 0;
        double y = 0;
        for (String line : input.split("@@@")) {
            x++;
            if (line.startsWith("-")) {
                y++;

            }
        }
        return y/x < 0.9;
    }

    /**
     * Register results
     */
    private static void registerResults(String content, String commitID, String date, String path, RevCommit commitInformation) throws IOException{
        byte[] message = (
                "name:\t"       + commitID
                        + "\nauthor:\t" + commitInformation.getAuthorIdent().getName()
                        + "\ndate\t"    + date
                        + "\nmessage\t" + commitInformation.getFullMessage() + "\n").getBytes(StandardCharsets.UTF_8);

        byte[] hashID = commitInformation.getName().getBytes(StandardCharsets.UTF_8);

        long asserts = getOccurences("(assert|verify)", content);
        int loc = (int) getOccurrencesLOC("@@@", content);
        content = content.replaceAll("@@@-", "\n");//
        content = content.replaceAll("@Test", "");
        String getFunctionName = getFunctionName(content);
        int cyclomaticComplexity = CCSolver.getCyclomaticComplexity(content);
        //   String fullFunctionPath = path + " "+ getFunctionName;
        numberOfLoc.put(loc,
                numberOfLoc.containsKey(loc) ?
                        numberOfLoc.get(loc)+1
                        :1);
        numberOfCyclomaticComplexity.put(
                cyclomaticComplexity,
                numberOfCyclomaticComplexity.containsKey(cyclomaticComplexity) ?
                        numberOfCyclomaticComplexity.get(cyclomaticComplexity)+1
                        :1);
        numberOfRemovedTestsPerCommit.put(date +","+commitID,
                numberOfRemovedTestsPerCommit.containsKey(date +","+commitID) ?
                        numberOfRemovedTestsPerCommit.get(date+","+commitID)+1
                        :1);

        String output = asserts + " " + loc + " " + cyclomaticComplexity +" " + (float) asserts/loc +  " " +  (float) cyclomaticComplexity/loc;
        if (tests.containsKey(commitID)){
            String add = "Cyclomatic Complexity\t "+ cyclomaticComplexity+ "\nAssertions\t\t "+ asserts + "\nLines of Code\t\t"+ loc + "\n" + content;
            //        methodTest.get(commitID).add(fullFunctionPath);
            tests.get(commitID).add(add);
            detailedCommitInformation.put(commitID,  detailedCommitInformation.get(commitID) +" " +output);

        }else {
            //  ArrayList<String> temp = new ArrayList<>();
            //   temp.add(fullFunctionPath);
            //    methodTest.put(commitID,temp);
            ArrayList<String> temp2 = new ArrayList<>();
            String add = "Cyclomatic Complexity\t"+ cyclomaticComplexity+ "\nAssertions\t\t"+ asserts + "\nLines of Code\t\t"+ loc + "\n" + content;
            temp2.add(add);
            tests.put(commitID,temp2);
            detailedCommitInformation.put(commitID,output);
        }
        numberOfAsserts[(int) asserts]++;


        if (asserts > 0) { //

            f.write(message);
            f.write((getFunctionName + " " + loc + " " + asserts + "\n").getBytes(StandardCharsets.UTF_8));
            f.write("hashid \t: ".getBytes(StandardCharsets.UTF_8));
            f.write(hashID);
            f.write(("\n" + content + "\n").getBytes(StandardCharsets.UTF_8));
        }
    }


    /**
     * Simple writer to save data
     * @param x                 Function that was being used
     * @param p                 Pattern to parse out data
     * @param date              Date when commit occurred
     * @param path              Path to file
     * @param commitInformation commit information of the current commit analysed
     */
    private static void parseBody(String x, Pattern p, String date, String commitID, String path, RevCommit commitInformation) throws IOException {
        Matcher m = p.matcher(x);
        while (m.find()) {
            String content = m.group(1);
            boolean b = filterOutput(content);
            if (!b) {
                nrOfTests++;
                registerResults(content,commitID,date, path, commitInformation);
            }
        }
    }


    /**
     *
     * @param commitInformation Commit body
     * @param x                 body of the commitMessage
     * @param delimiter         pattern
     * @param entry             Used to get the path of the current test
     * @param id                id of the body
     */
    private static void parseResult(RevCommit commitInformation, String x, Pattern delimiter, DiffEntry entry, String id) throws IOException, GitAPIException {
        Date time = (new Date(commitInformation.getCommitTime() * 1000L));
        SimpleDateFormat format = new SimpleDateFormat("dd/MM/yyyy");
        String date = format.format(time);
        x = x.replaceAll("\n", "@@@");
        if (100000 > x.length()) {
            parseBody(x, delimiter, date,id, entry.getOldPath(), commitInformation);
        } else { // Avoids stackoverflow issues
            parseBody(x.substring(0, x.length() >> 2)                    , delimiter, date, id, entry.getOldPath(), commitInformation);
            parseBody(x.substring(x.length() >> 2, x.length() >> 1)      , delimiter, date, id, entry.getOldPath(), commitInformation);
            parseBody(x.substring(x.length() >> 1, (x.length() * 3) >> 2), delimiter, date, id, entry.getOldPath(), commitInformation);
            parseBody(x.substring((x.length() * 3) >> 2)                 , delimiter, date, id, entry.getOldPath(),  commitInformation);
        }
    }


    /**
     * Generates the git diffs for each commit.
     * @param git jgit git repository
     * @param oldCommit Old commit
     * @param newCommit New Commit
     * @param id Hash id for the current commit
     */
    private static void commitDiff(Git git, RevCommit oldCommit, RevCommit newCommit, String id) throws GitAPIException, IOException {
        DiffFormatter formatter = new DiffFormatter(out);
        formatter.setRepository(git.getRepository());
        Pattern delimiter = Pattern.compile("(?=\\-[^@]+@Test[^{]+[{])((?:(?=.*?[{](?!.*?\\2)(.*}(?!.*\\3).*))(?=.*?}(?!.*?\\3)(.*)).)+?.*?(?=\\2)[^{]*(?=\\3$))");
        formatter.setPathFilter(TreeFilter.ANY_DIFF);
        formatter.setPathFilter(PathSuffixFilter.create(".java"));
        TreeWalk walk = new TreeWalk(git.getRepository());
        walk.addTree(newCommit.getTree());
        walk.addTree(oldCommit.getTree());
        List<DiffEntry> diff2 = DiffEntry.scan(walk,true);
        for(DiffEntry entry : diff2){
            formatter.format(entry);
            FileHeader fh = formatter.toFileHeader(entry);
            for (Edit f: fh.toEditList()) {
                if(f.getType() == Edit.Type.REPLACE ){
                    parseResult(newCommit,out.toString(StandardCharsets.UTF_8),delimiter,entry,id);
                }
                out.reset();
            }
        }
    }


    /**
     * Iterate over each line of the git message to see that it follows the correct messages
     * @param message Commit messages
     * @return true if pattern sought after was matched, false otherwise
     */
    private static boolean match(String message) {
        for (String s : message.split("\n")) {
            if (COMMITPATTERN.matcher(s).find())
                return true;
        }
        return false;
    }


    /**
     * Walk function that generates all the commits that exists, output the ones that fulfills the filter options
     */
    public static void walk() throws IOException, GitAPIException {
        long nrOfCommits = 0;
        RevCommit oldTargetCommit = null;
        boolean matched = false; // Used to print the correct git diff
        boolean foundInThisBranch ;
        Repository repo = new FileRepository(PATHGITDIRECTORY);
        Git git = new Git(repo);
        RevWalk walk = new RevWalk(repo);
        RevFilter revFilter = MessageRevFilter.create("RuntimeException");
        walk.setRevFilter(revFilter);
        List<Ref> branches = git.branchList().call();
        String id = "";
        for (Ref branch : branches) { // Iterating over all branches
            String branchName = branch.getName();


            System.out.println("Commits of branch: " + branch.getName());
            System.out.println("-------------------------------------");

            Iterable<RevCommit> commits = git.log().all().call();

            for (RevCommit commit : commits) {

                nrOfCommits++;
                if (nrOfCommits % 1000 == 0) {
                    System.out.println(nrOfCommits + " commits checked so far");
                }
                RevCommit targetCommit =  walk.parseCommit(repo.resolve(
                        commit.getName()));
                foundInThisBranch = false;
                if (matched){
                    commitDiff(git,oldTargetCommit,targetCommit,id);
                    matched = false;
                }
                for (Map.Entry<String, Ref> e : repo.getAllRefs().entrySet()) {
                    if (e.getKey().startsWith(Constants.R_HEADS)) {
                        if (walk.isMergedInto(targetCommit, walk.parseCommit(
                                e.getValue().getObjectId()))) {
                            String foundInBranch = e.getValue().getName();
                            if (branchName.equals(foundInBranch)) {
                                foundInThisBranch = true;
                                break;
                            }
                        }
                    }
                }
                if (foundInThisBranch) {
                    if (match(commit.getFullMessage())) {
                        String name = commit.getName();
                        String author = commit.getAuthorIdent().getName();
                        String date = (new Date(commit.getCommitTime() * 1000L)).toString();
                        String fullMessage = commit.getShortMessage();
                        fullData.add("Name         : " + name);
                        fullData.add("Author       : " + author);
                        fullData.add("Date         : " + date);
                        fullData.add("Full message : " + fullMessage);
                        hashValues.add(name);
                        oldTargetCommit = targetCommit;
                        id = commit.getName();
                       // if(commit.getParentCount() != 0)
                      //      commitDiff(git,commit,commit.getParent(0));
                        matched = true;
                    }
                }
            }
        }
    }



    /**
     * Clones down a repo given URL
     *
     * @param url Sample url https://github.com/Renstrom/act.git
     * @throws JGitInternalException Makes sure that the repo isn't already cloned
     */
    private static void getRepo(String url) throws GitAPIException, JGitInternalException {
        System.out.println("Cloning repo " + url);
        Git.cloneRepository()
                .setURI(url)
                .setDirectory(new File(REPODIRECTORY))
                .call();

    }

    /**
     * Creates a directory for the corresponding repository, if folder gets created the output will be
     */
    private static void makeDirectory() {
        System.out.println("output/" + OUTPUTPATH);
        File f = new File("output/" + OUTPUTPATH);
        if (f.mkdir()) {
            System.out.println(f.getName() + " folder was created");
        } else {
            System.out.println(f.getName() + " folder was already created");
        }
    }




    /**
     * Calculates the number of occurences of a specific pattern in a function
     *
     * @param pattern pattern used
     * @param test    test function analyzed
     * @return amount of occurences
     */
    private static long getOccurences(String pattern, String test) {

        Pattern locPat  = Pattern.compile(pattern);
        Matcher loc     = locPat.matcher(test);

        return loc.results().count();
    }


    /**
     * Gets the number of lines in a file
     * @param pattern Currently not used
     * @param test test function used
     * @return number of lines
     */
    private static long getOccurrencesLOC(String pattern, String test){
        test = test.replaceAll("@@@", "\n");
        test = test.replaceAll("((['\"])(?:(?!\\2|\\\\).|\\\\.)*\\2)|//[^\\n]*|/\\*(?:[^*]|\\*(?!/))*\\*/","");
        String[] input = test.split("\n");
        long count = 0;
        for (String i: input) {
            if(!i.matches("\\- *")){
                count++;
            }
        }
        return count;
    }

    /**
     * Gets the function name of the test
     * @param test String of the test
     * @return name of the test
     */
    private static String getFunctionName(String test) {
        for (String name : test.split("\n")) {
            String[] name2 = name.split("\\(");
            if (name2.length > 0) {
                if (!name2[0].contains("@")) {
                    String[] name3 = name2[0].split("\\s");
                    if (name3.length > 0) {
                        return name3[name3.length - 1];
                    }
                }
            }
        }
        return "foo"; // generic function name if the parsing goes wrong
    }


    /**
     * Simple write to file function, where each file is currently written down as strings
     * @param pathAndName Complete path to file
     * @param data        Data recorded
     */
    private static void writeToFile(String pathAndName, ArrayList<String> data) {
        new File(pathAndName);
        try {
            FileWriter writer = new FileWriter(pathAndName);
            for (String line : data) {
                writer.write(line + "\n");
            }
            writer.close();
        } catch (IOException e) {
            System.out.println("An error occured");
        }
    }


    /**
     * Generated the distribution of the int array where the index showcases which value gets used
     * @param intArray number of occurences of a specific value (example assertions)
     * @return distribution
     */
    private static String getDistribution(int[] intArray) {
        StringBuilder output = new StringBuilder();
        for (int i = 0; i < intArray.length; i++) {
            if (intArray[i] != 0) // Removes number of
                output.append("{").append(i).append(",").append(intArray[i]).append("} ");
        }
        return "[" + output + "]";
    }

    /**
     * Generates a string for the output
     * @param hMap Hashmap to iterate through (Should not be used for generic single datatypes)
     * @param format format in how it should be structured
     * @return Distribution
     */
    private static String getDistribution(HashMap<?, ?> hMap, String format) {
        StringBuilder output = new StringBuilder();
        TreeMap<?, ?> sorted = new TreeMap<>(hMap);
        for (Map.Entry<?, ?> entry : sorted.entrySet())
            output.append(String.format(format, entry.getKey(), entry.getValue()));
        return "[" + output + "]";
    }


    /**
     * Iterates over hashmap with an arraylist, works similar to get distribution function (see above)
     * @param hMap
     * @param format
     * @return
     */
    private static String getDistribution2(HashMap<String, ArrayList<String>> hMap, String format) {
        StringBuilder output = new StringBuilder();
        TreeMap<String, ArrayList<String>> sorted = new TreeMap<>(hMap);
        output.append(OUTPUTPATH);
        for (Map.Entry<String, ArrayList<String>> entry : sorted.entrySet()) {
            output.append("\n[").append(entry.getKey()).append(" ");
            for (String path : entry.getValue()) {
                output.append(path).append(" ");
            }
            output.append("]");
        }
        return "[" + output + "]";
    }


    /**
     * Gets the details of an array
     * @param array
     * @return
     */
    private static String getDetails(int[] array) {

        int med = nrOfTests / 2;
        int max = -1;
        int min = 99999;
        int nonZero = 0;
        for (int val = 0; val < array.length; val++) {
            if (array[val] > max) max = array[val];
            if (array[val] < min) min = array[val];
            if (val != 0) {
                nonZero += val * array[val];
                if (med < nonZero && med == nrOfTests / 2) {
                    med = val;
                }
            }
        }
        double avg = (double) nonZero / (double) nrOfTests;

        return String.format("[%s %s %s %s %s]", nrOfTests, avg, max, min, med);
    }

    /**
     * Gets the details of a hashmap (median, max, min, total lines and avg)
     * @param hMap h
     * @return String of details formatted [x,x,x,x,x]
     */
    private static String getDetails(HashMap<Integer, Integer> hMap) {
        TreeMap<Integer, Integer> sorted = new TreeMap<>(hMap);
        double totLines = 0;
        int median = nrOfTests / 2;
        int testsIteratedOver = 0;
        int max = -1;
        int min = 9999999;
        for (Map.Entry<Integer, Integer> entry : sorted.entrySet()) {
            testsIteratedOver += entry.getValue();
            if (entry.getKey() < min) {
                min = entry.getKey();
            } else if (entry.getKey() > max) {
                max = entry.getValue();
            }
            if (testsIteratedOver > median && median == nrOfTests / 2) {
                median = entry.getKey();
            }
            totLines += entry.getKey() * entry.getValue();
        }
        double avgLOC = totLines / nrOfTests;

        return String.format("[%s %s %s %s %s]", nrOfTests, avgLOC, max, min, median);
    }




    private static boolean saveResults(String fileName, String distribution, String details, String format) {
        try {
            System.out.println("Saving " + fileName + " results");
            System.out.println(details);
            FileWriter writer = new FileWriter("result/" + fileName + ".txt", true);
            writer.write(String.format(format, OUTPUTPATH, distribution, details));
            System.out.println(fileName + " results saved in result/" + fileName + ".txt");
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    /**
     * Values saved
     * @param fileName name of file
     * @param topPoints The toppoints generated
     * @return boolean result, returns true if it gets saved correctly
     */
    private static boolean saveResultsTest(String fileName, HashMap<String, Integer> topPoints){
        try {
            System.out.println("Saving " + fileName + " results");

            FileWriter writer = new FileWriter("result/" + fileName + ".md", false);
            StringBuilder points = new StringBuilder();
            for (String key: topPoints.keySet()) {
                points.append("## ").append(key).append(" ##\n");
                System.out.println(topPoints.get(key));
                for (String test: tests.get(key)) {
                    test = test.replaceAll("\n\\+","\n");
                    test = test.replaceAll("^\\+","");
                    points.append("```\n").append(test).append("\n```\n");
                }
            }
            writer.write(points.toString());
            System.out.println(fileName + " results saved in result/" + fileName + ".txt");
            writer.close();

        }catch (IOException e){
            e.printStackTrace();
            return false;
        }


        return true;
    }

    /**
     * Helper function for generate top array
     * @param unSortedMap
     * @param ascending
     * @return
     */
    private static HashMap<String, Integer> sortByValue(HashMap<String,Integer> unSortedMap, boolean ascending){
        List<Map.Entry<String,Integer>> list = new LinkedList<>(unSortedMap.entrySet());
        list.sort((v1,v2) -> ascending
                ? v1.getValue().compareTo(v2.getValue()) == 0
                ? v1.getKey().compareTo(v2.getKey())
                : v1.getValue().compareTo(v2.getValue()) : v2.getValue().compareTo(v1.getValue()) == 0 ? v2.getKey().compareTo(v1.getKey()) : v2.getValue().compareTo(v1.getValue()));
        return list.stream().collect(Collectors.toMap(Map.Entry::getKey,Map.Entry::getValue, (a,b) -> b, LinkedHashMap::new ));
    }

    /**
     * Sorts a map to get the top values
     * @return
     */
    private static HashMap<String, Integer> generateTopArray(){
        HashMap<String, Integer> tempMap = new HashMap<>();
        for (Map.Entry<String,ArrayList<String>> entry : tests.entrySet()) {
            tempMap.put(entry.getKey(),entry.getValue().size());
        }
        HashMap<String,Integer> sortedMapAsc = sortByValue(tempMap,false);
        HashMap<String, Integer> returnMap = new HashMap<>();
        int x = 0;
        for (Map.Entry<String, Integer> entry: sortedMapAsc.entrySet()) {
            if (x ==5){
                return returnMap;
            }
            x++;
            returnMap.put(entry.getKey(), entry.getValue());
        }
        return returnMap;
    }







    public static void saveResults() {
        boolean a= false,b = false, c =false,d=false,e=false, g = false, h = false; // initializations used to remove tests if needed
      //  a = saveResults("assert", getDistribution(numberOfAsserts), getDetails(numberOfAsserts), "\n[%s]%s%s");
      //  b = saveResults("loc", getDistribution(numberOfLoc, "{%s,%s}"), getDetails(numberOfLoc), "\n[%s]%s%s");
       // c = saveResults("commit", getDistribution(numberOfRemovedTestsPerCommit, "{%s,%s}"), "", "\n[%s]%s%s");
       // d = saveResults("fullPathMethodsAndCommits", getDistribution2(methodTest, "\n{%s,%s}"), "", "\n[%s]\n%s%s");
       // e = saveResults("cyclomatic", getDistribution(numberOfCyclomaticComplexity, "{%s,%s}"),getDetails(numberOfCyclomaticComplexity), "\n[%s]%s%s");
       // g = saveResultsTest("tests_"+OUTPUTPATH, generateTopArray());
        h = saveResults("detailedInformation",getDistribution(detailedCommitInformation,"{%s,%s}"),"","\n[%s]%s%s");
        if (a && b && c && d && e && g && h) {
            System.out.println("Data saved");
        } else {
            System.out.println("Error occured");
        }
    }



    /**
     * Function that gets called by setup function
     * @throws IOException
     * @throws GitAPIException
     */
    private static void run() throws IOException, GitAPIException {
        try {
            makeDirectory();
            getRepo(URLTOREPO);

        } catch (JGitInternalException | GitAPIException e) {
            System.out.println("Repo is already stored");
        } finally {
            walk();
            System.out.println(nrOfTests + " tests accumulated");
            writeToFile("output/" + OUTPUTPATH + "/full.txt", fullData);
            System.out.printf("Saving hashID information output/%s/full.txt%n", OUTPUTPATH);
            writeToFile("output/" + OUTPUTPATH + "/hash.txt", hashValues);
            System.out.println("Repo data done");
            saveResults();
        }
    }


    /**
     * Setups variables for the given repo
     * @param name Name of the project repo
     * @param url  URL to the project repo (MUST BE A GITHUB REPOSITORY)
     */
    private static void setupVariables(String name, String url) {
        REPODIRECTORY                   = "gitRepos/repos/" + name;
        URLTOREPO                       = url;
        PATHGITDIRECTORY                = REPODIRECTORY + "/.git";
        OUTPUTPATH                      = name;
        nrOfTests                       = 0;
        numberOfAsserts                 = new int[200];
        numberOfLoc                     = new HashMap<>(200);
        numberOfRemovedTestsPerCommit   = new HashMap<>(200);
        numberOfCyclomaticComplexity    = new HashMap<>(200);
        tests                           = new HashMap<>();
        detailedCommitInformation       = new HashMap<>(200);


        try {
            f = new FileOutputStream("output/" + OUTPUTPATH + "/diff.txt");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    /**
     * Showcase which repository are planed to run
     *
     * @param names      All names of the repos
     * @param checkMarks Array of boolean, which indicates which functions gets to run
     */
    private static void printRepos(ArrayList<String> names, boolean[] checkMarks) {
        for (int i = 0; i < names.size(); i++) {
            String output;
            if (checkMarks[i]) {
                output = "[✓]";
            } else {
                output = "[]";
            }
            System.out.printf("%s %s %s%n", i, output, names.get(i));
        }
    }

    /**
     * Setup function to get all the necessary information to create the program.
     */
    private static void setup() {
        System.out.println("Hello and Welcome to the JGIT Miner\n Please select which repos you would like to mine for test files: ");
        try {
            String line;
            ArrayList<String> names = new ArrayList<>();
            ArrayList<String> urls = new ArrayList<>();
            BufferedReader reader =
                    new BufferedReader(new FileReader("input.txt"));
            System.out.println("Hello and Welcome to the JGIT Miner\n Please select which repos you would like to mine for test files: ");
            while ((line = reader.readLine()) != null) {
                // row[0] = name, row[1] = http link
                String[] row = line.split("\\s");
                names.add(row[0]);
                urls.add(row[1]);
            }
            reader.close();
            boolean[] checkMarks = new boolean[names.size()];
            boolean start = false;

            System.out.println("Please choose which repos you would like to mine");
            BufferedReader inp = new BufferedReader(new InputStreamReader(System.in));
            while (!start) {
                printRepos(names, checkMarks);
                System.out.println("Please choose which repos you would like to mine, write \"-1\" to start");
                String str = inp.readLine();
                if (str.equals("-1")) {
                    start = true;
                } else {
                    int check;
                    try {
                        check = Integer.parseInt(str);
                    } catch (NumberFormatException e) {
                        System.out.println("Please enter a valid number");
                        continue;
                    }
                    if (check >= 0 && check < checkMarks.length) {
                        checkMarks[check] = !checkMarks[check];
                    } else {
                        System.out.println("Out of range");
                    }
                }
            }
            inp.close();
            System.out.println("Setup done");
            for (int i = 0; i < names.size(); i++) {
                if (checkMarks[i]) {
                    System.out.println("Running repo : " + names.get(i));
                    setupVariables(names.get(i), urls.get(i));
                    run();
                }
            }
        } catch (FileNotFoundException e) {
            System.err.println("File does not exist ");
            System.err.println("In order to run the program an input.txt in the top repo folder must be added consisting of\"repo name\" \"http link \" \n example : jgit-miner https://github.com/Renstrom/jgit-miner.git ");
            System.exit(1);
        } catch (IOException e) {
            System.err.println("Structure of the code is not valid");
            System.err.println("In order to run the program an input.txt in the top repo folder must be added consisting of\"repo name\" \"http link\" \n example :\njgit-miner https://github.com/Renstrom/jgit-miner.git");
            System.exit(1);

        } catch (GitAPIException e) {
            e.printStackTrace();
        }
    }


    /**
     * Generic main function
     * @param args
     * @throws GitAPIException
     * @throws IOException
     */
    public static void main(String[] args) throws GitAPIException, IOException {
        setup();

    }
}