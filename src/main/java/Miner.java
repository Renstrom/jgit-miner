


import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.api.errors.JGitInternalException;
import org.eclipse.jgit.diff.DiffEntry;
import org.eclipse.jgit.diff.DiffFormatter;
import org.eclipse.jgit.internal.storage.file.FileRepository;
import org.eclipse.jgit.lib.Constants;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.ObjectReader;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevTree;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.revwalk.filter.MessageRevFilter;
import org.eclipse.jgit.revwalk.filter.RevFilter;
import org.eclipse.jgit.treewalk.AbstractTreeIterator;
import org.eclipse.jgit.treewalk.CanonicalTreeParser;
import org.eclipse.jgit.treewalk.filter.PathSuffixFilter;
import org.eclipse.jgit.treewalk.filter.TreeFilter;


/**
 * Algorithm to mine GIT Commit histories using JGit
 * @author Anders Renström
 */
public class Miner {


    static ArrayList<String> fullData       = new ArrayList<>();
    static ArrayList<String> hashValues     = new ArrayList<>();
    static ArrayList<String> refactoredData = new ArrayList<>(); // NOTE May be temporary




    static String REPODIRECTORY       = "gitRepos/repos/"; // Directory in which all repos are saved
    static String PATHGITDIRECTORY    = REPODIRECTORY+"/.git"; // Local directory to git repo
    static String URLTOREPO           = "https://github.com/iluwatar/java-design-patterns.git"; // URL to repo
    static String OUTPUTPATH          =  "java-design-patterns";

    static FileOutputStream f;




    // Pattern to find the correct commits
    final static Pattern COMMITPATTERN = Pattern.compile(
            "((refact(or|ing|ored)\\s)|(\\supdat(e|ing|es)\\s)).*(test|\\stest(s|er|ing|ed)\\s)", Pattern.CASE_INSENSITIVE);

    final static Pattern COMMITPATTERN2 = Pattern.compile(
            "((\\srefact(or|ing|red)\\s)|(\\supdat(e|ing|es)\\s*))(test|\\stest(s|er|ing|ed)\\s)", Pattern.CASE_INSENSITIVE);


    /**
     * Walk function that generates all the commits that exists, output the ones that fulfills the filter options
     */
    public static void walk() throws IOException, GitAPIException {
        long nrOfCommits = 0;
        String oldHashID;
        String newHashID    = "";
        boolean matched     = false; // Used to print the correct git diff

        Repository repo     = new FileRepository(PATHGITDIRECTORY);
        Git git             = new Git(repo);
        RevWalk walk        = new RevWalk(repo);
        RevFilter revFilter = MessageRevFilter.create("RuntimeException");
        walk.setRevFilter(revFilter);

        List<Ref> branches  = git.branchList().call();

        for (Ref branch : branches) { // Iterating over all branches
            String branchName = branch.getName();

            System.out.println("Commits of branch: " + branch.getName());
            System.out.println("-------------------------------------");

            Iterable<RevCommit> commits = git.log().all().call();

            for (RevCommit commit : commits) {
                nrOfCommits++;
                if(nrOfCommits%1000==0){
                    System.out.println(nrOfCommits+" commits checked so far");
                }
                if(!COMMITPATTERN.matcher(commit.getFullMessage()).find() ){
                    continue;
                }
                boolean foundInThisBranch = false;


                RevCommit targetCommit = walk.parseCommit(repo.resolve(
                        commit.getName()));
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
                    if(!newHashID.equals("") && matched){
                        oldHashID = commit.getName();
                        AbstractTreeIterator oldCommit = prepareTreeParser(repo, oldHashID);
                        AbstractTreeIterator newCommit = prepareTreeParser(repo, newHashID);
                        gitDiff(repo,oldCommit,newCommit);
                        matched = false;
                    }
                    newHashID = commit.getName();
                    if(COMMITPATTERN.matcher(commit.getFullMessage()).find()){
                        String name         = commit.getName();
                        String author       = commit.getAuthorIdent().getName();
                        String date         = (new Date(commit.getCommitTime() * 1000L)).toString();
                        String fullMessage  = commit.getShortMessage();
                        fullData.add("Name         : "+ name);
                        fullData.add("Author       : " +author);
                        fullData.add("Date         : " +date);
                        fullData.add("Full message : " +fullMessage);
                        hashValues.add(name);
                        matched = true;
                    }
                }
            }
    }
    }


    private static boolean filterOutput(String input){
        int y = 0;

        for (String line: input.split("@@@")) {
            if(y == 0){
                y++;
                continue;
            }
            if(line.startsWith("+")){
                return true;
            }
        }
        return false;
    }



    private static void parseBody(String x, Pattern p ) throws IOException {
        Matcher m = p.matcher(x);
        while (m.find()) {

            String content = m.group(1);
            boolean b = filterOutput(content);
            if (!b)
                f.write((content.replaceAll("@@@", "\n") + "\n").getBytes(StandardCharsets.UTF_8));

        }
    }
    static ByteArrayOutputStream out = new ByteArrayOutputStream();
    /**
     * Generates the git diffs for each commit.
     * @param repo Repository analysing
     * @param oldCommit Old commit tree iterator
     * @param newCommit New Commit tree iterator
     */
    private static void gitDiff(Repository repo, AbstractTreeIterator oldCommit, AbstractTreeIterator newCommit) throws GitAPIException, IOException {
        Pattern delimiter = Pattern.compile("(?=@Test[^{]+[{])((?:(?=.*?[{](?!.*?\\2)(.*}(?!.*\\3).*))(?=.*?}(?!.*?\\3)(.*)).)+?.*?(?=\\2)[^{]*(?=\\3$))");
        DiffFormatter formatter = new DiffFormatter(out) ;
        TreeFilter treeFilter = PathSuffixFilter.create(".java");

        Git git = new Git(repo) ;
        List<DiffEntry> diff = git.diff().
                    setOldTree(oldCommit).
                    setNewTree(newCommit).
                    setPathFilter(treeFilter).
                    setShowNameAndStatusOnly(true).
                    call();
        for (DiffEntry entry : diff) {


            formatter.setRepository(repo);
            formatter.format(entry);
            formatter.setPathFilter(TreeFilter.ANY_DIFF);
            formatter.toFileHeader(entry);
            String x = out.toString(StandardCharsets.UTF_8);


            x = x.replaceAll("\n", "@@@");

            if( 100000>x.length()){
                parseBody(x,delimiter);
            } else{
                System.out.println(x.length());
                parseBody(x.substring(0,x.length()>>2),delimiter);
                parseBody(x.substring(x.length()>>2,x.length()>>1),delimiter);
                parseBody(x.substring(x.length()>>1,(x.length()*3)>>2),delimiter);

                parseBody(x.substring((x.length()*3)>>2),delimiter);

            }

            out.reset();
        }
        //out.close();

    }





    /**
     * Gathers the Abstracttreeiterator to get the git diffs from each commit.
     * @param repository  Repository in which the repo is contained
     * @param objectId Commit hash id
     * @return AbstractTreeIterator to loop over
     * @throws IOException If reader can't be resetted
     */
    private static AbstractTreeIterator prepareTreeParser(Repository repository, String objectId) throws IOException {
        RevWalk walk        = new RevWalk(repository);
        RevCommit commit    = walk.parseCommit(ObjectId.fromString(objectId));
        RevTree tree        = walk.parseTree(commit.getTree().getId());
        CanonicalTreeParser treeParser  = new CanonicalTreeParser();
        ObjectReader        reader      = repository.newObjectReader();
        treeParser.reset(reader, tree.getId());
        walk.dispose();



        return treeParser;
    }

    /**
     * Clones down a repo given URL
     * @param url Sample url https://github.com/Renstrom/act.git
     * @throws JGitInternalException Makes sure that the repo isn't already cloned
     */
    private static void getRepo(String url) throws GitAPIException, JGitInternalException {
        System.out.println("Cloning repo " +  url);
        Git.cloneRepository()
                .setURI(url)
                .setDirectory(new File(REPODIRECTORY))
                .call();

    }

    /**
     * Creates a directory for the corresponding repository, if folder gets created the output will be
     */
    private static void makeDirectory(){
        System.out.println("output/"+OUTPUTPATH);
        File f = new File("output/"+OUTPUTPATH);
        if(f.mkdir()){
            System.out.println(f.getName() + " folder was created");
        } else{
            System.out.println(f.getName() + " folder was already created");
        }
    }


    /**
     * Simple write to file function, where each file is currently written down as strings
     * @param pathAndName Complete path to file
     * @param data Data recorded
     */
    private static void writeToFile(String pathAndName, ArrayList<String> data){
        new File(pathAndName);
        try{
            FileWriter writer = new FileWriter(pathAndName);
            for (String line: data) {
                writer.write(line+"\n");

            }
            writer.close();
        } catch (IOException e){
            System.out.println("An error occured");
        }
    }





    /**
     *
     * TODO refactor data to fit SVM planned data point structure (Number, #LOCDiff, Cyclomatic Complexity, #Asserts)
     * NOTE MAY BE MOVED TO SEPARATE FILE
     */
    private static void refactorData(){
    }


    private static long getOccurences(String pattern, String test){

        Pattern locPat = Pattern.compile(pattern);
        Matcher loc = locPat.matcher(test);

        return  loc.results().count();
    }

    private static String getName(String pattern, String test){
        Pattern namePat = Pattern.compile(pattern);
        Matcher name = namePat.matcher(test);

        return name.group(1);
    }

    private static String getFunctionName(String test){
        Pattern namePat = Pattern.compile("\\(");
        String name = namePat.split(test)[0];
        String[] newName = name.split("\\s");

        return newName[newName.length-1];
    }



    /**
     * Regex to get all test files (?=@Test[^{]+\{)((?:(?=.*?\{(?!.*?\2)(.*}(?!.*\3).*))(?=.*?}(?!.*?\3)(.*)).)+?.*?(?=\2)[^{]*(?=\3$)) using backtracking
     *
     */
    private static void parseOutTests() throws IOException {
        try{
            f.close();
          //  String content = Files.readString(Paths.get("output/" + OUTPUTPATH + "/diff.txt"));
            File file = new File("output/" + OUTPUTPATH + "/diff.txt");
            Scanner scan = new Scanner(file);
            new File("output/" + OUTPUTPATH + "/tests.txt");
            new File("output/" + OUTPUTPATH + "/tests.txt");
            FileWriter writer = new FileWriter("output/" + OUTPUTPATH + "/tests.txt");

            writer.write("Function names | Lines of code | #assertions\n");
            Pattern testMethodPattern = Pattern.compile("(?=@Test[^{]+[{])((?:(?=.*?[{](?!.*?\\2)(.*}(?!.*\\3).*))(?=.*?}(?!.*?\\3)(.*)).)+?.*?(?=\\2)[^{]*(?=\\3$))", Pattern.MULTILINE);
            Pattern delimiter = Pattern.compile("(?=(?=@Test[^{]+[{])((?:(?=.*?[{](?!.*?\\2)(.*}(?!.*\\3).*))(?=.*?}(?!.*?\\3)(.*)).)+?.*?(?=\\2)[^{]*(?=\\3$)))");
            scan.useDelimiter(delimiter);
       //     Pattern testMethodPattern2 = Pattern.compile("@Test\\s*@@@@-");
            while(scan.hasNext()) {
                String content = scan.next();

                Matcher m = testMethodPattern.matcher(content);

                while (m.find()) {

                    String test = m.group(1);
                    System.out.println(test);


                    long asserts = getOccurences("(assert|verify)", test);
                    long loc = getOccurences("\n", test);
                    String getFunctionName = getFunctionName(test);
                    writer.write(getFunctionName + " " + loc + " " + asserts + "\n");
                }

            }
            writer.close();

        } catch(FileNotFoundException e){
            System.out.println("error occured");
        }

    }





    private static void setupVariables(String name, String url){
        REPODIRECTORY =  "gitRepos/repos/"+name;
        URLTOREPO = url;
        PATHGITDIRECTORY = REPODIRECTORY+"/.git";
        OUTPUTPATH = name;

        try {
            f = new FileOutputStream(new File("output/"+OUTPUTPATH+"/diff.txt"));
        } catch (FileNotFoundException e) {
                e.printStackTrace();
        }


    }

    private static void printRepos(ArrayList<String> names, boolean[] checkMarks){
        for (int i = 0; i < names.size(); i++) {
            String output;
            if(checkMarks[i]){
                output = "[✓]";
            } else{
                output = "[]";
            }
            System.out.printf("%s %s %s%n", i, output, names.get(i));
        }
    }

    private static void setup(){
        System.out.println("Hello and Welcome to the JGIT Miner\n Please select which repos you would like to mine for test files: ");
        File f = new File("input.txt");
        try{
            String line;
            ArrayList<String> names = new ArrayList<>();
            ArrayList<String> urls = new ArrayList<>();
            BufferedReader reader =
                    new BufferedReader(new FileReader("input.txt"));
            System.out.println("Hello and Welcome to the JGIT Miner\n Please select which repos you would like to mine for test files: ");
            while (( line = reader.readLine()) != null) {

                // row[0] = name, row[1] = http link
                String[] row = line.split("\\s");
                names.add(row[0]);
                urls.add(row[1]);
            }
            reader.close();
            boolean[] checkMarks = new boolean[names.size()];
            boolean start = false;

            System.out.println("Please choose which repos you would like to mine");
            BufferedReader inp = new BufferedReader (new InputStreamReader(System.in));
            while(!start){

                printRepos(names,checkMarks);
                System.out.println("Please choose which repos you would like to mine, write \"-1\" to start");
                String str = inp.readLine();
                if(str.equals("-1")){
                    start = true;
                } else {
                    int check = 0;
                    try{
                        check = Integer.parseInt(str);
                    } catch(NumberFormatException e){
                        System.out.println("Please enter a valid number");
                        continue;
                    }
                    if (check >= 0 && check< checkMarks.length ){
                        checkMarks[check] = !checkMarks[check];
                    } else{
                        System.out.println("Out of range");
                    }

                }

            }
            inp.close();
            System.out.println("Setup done");
            for (int i = 0; i < names.size(); i++) {
                if(checkMarks[i]) {
                    System.out.println("Running repo : " + names.get(i));
                    setupVariables(names.get(i), urls.get(i));
                    run();
                }
            }


        } catch(FileNotFoundException e ) {
            System.err.println("File does not exist ");
            System.err.println("In order to run the program an input.txt in the top repo folder must be added consisting of\"repo name\" \"http link \" \n example : jgit-miner https://github.com/Renstrom/jgit-miner.git ");
            System.exit(1);
        } catch (IOException e){
            System.err.println("Structure of the code is not valid");
            System.err.println("In order to run the program an input.txt in the top repo folder must be added consisting of\"repo name\" \"http link \" \n example : jgit-miner https://github.com/Renstrom/jgit-miner.git ");
            System.exit(1);

        } catch (GitAPIException e) {
            e.printStackTrace();
        }


    }

    private static void run() throws IOException, GitAPIException {
        makeDirectory();
        try{
            makeDirectory();
            getRepo(URLTOREPO);

        } catch (JGitInternalException | GitAPIException e){
            System.out.println("Repo is already stored");
        } finally {
            walk();

            System.out.println("Trying to parse out tests");
            parseOutTests();
            writeToFile("output/"+OUTPUTPATH+"/full.txt",fullData);
            System.out.printf("Saving hashID information output/%s/full.txt%n", OUTPUTPATH);
            writeToFile("output/"+OUTPUTPATH+"/hash.txt",hashValues);
            System.out.println("Repo data done");

        }
    }
    public static void main(String[] args) throws GitAPIException, IOException {
            setup();
    }
}