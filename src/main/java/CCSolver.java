import java.util.ArrayList;
import java.util.HashMap;


/**
 * Temporary imports that will be removed once completed
 */
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.api.errors.JGitInternalException;
import org.eclipse.jgit.diff.DiffEntry;
import org.eclipse.jgit.diff.DiffFormatter;
import org.eclipse.jgit.internal.storage.file.FileRepository;
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
 * Cyclomatic complexity solver
 * @author Anders Renström
 */
public class CCSolver {
    // Hashmap where the structure will be <path+methodName, #cyclomatic complexity>
    HashMap<String, Integer> cycloMaticComplexity = new HashMap<>(500);

    private static String REPONAME;
    private static String URL;


    private static void setURLName(String url) {
        URL = url;
    }

    private static void setRepoName(String repo) {
        REPONAME = repo;
    }


    /**
     * @param methodName name of the method.
     * @param filePath   path to file in which the tests exists in.
     * @return CyclomaticComplexity
     */
    private static int getCyclomaticComplexity(String methodName, String filePath) {
        return 0;
    }

    /**
     * Calculates cyclomatic complexity using [xxx] program
     */
    private static void calculateCyclomaticComplexity(String commitID) {

    }


    /**
     * Should be in output/repo/testmethods
     * Structure commitID testMethod
     *
     * @param commitId ID of commit in which code exist in
     * @return all test methods in
     */
    private static ArrayList<String> getTestMethods(String commitId) {

        return new ArrayList<>();
    }

    /**
     * Does a git checkout to the given commitID
     * @param commitID ID to change git repo to IDs version
     */
    private static void jGitCheckout(String commitID) {

    }

    /**
     * Gets the URL from input.txt
     * @param repo name of repo
     * @return URL to repo
     */
    private static String getPath(String repo) {
        return "";
    }


    /**
     * Return an arraylist of all the commitIds from the xxx.txt file
     * @return Arraylist of commitIds
     */
    private static ArrayList<String> getCommitIds() {
        return new ArrayList<>();
    }




    /**
     * Saves the results, iterates over result in hashmap given commitID
     * @param commitID name of repo
     * @param format format of the output result
     */
    private static void saveResults(String commitID,String format) {

    }


    /**
     * Calling function for Miner
     * @param repo Repo in which to calculate cyclomatic Complexity
     */
    public static void generateCylomaticComplexity(String repo) {
        setRepoName(repo);
        setURLName(getPath(repo));
        ArrayList<String> commitIDS = getCommitIds();
        for (String ids : commitIDS) {
            calculateCyclomaticComplexity(ids);
        }
    }

    /**
     * function to easy try out code.
     * @param args
     */
    public static void main(String[] args) {
        generateCylomaticComplexity("deeplearning4j");
    }
}
