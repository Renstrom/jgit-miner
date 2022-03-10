import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;

import javacc.Example1;

import java.util.Scanner;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;

import org.eclipse.jgit.internal.storage.file.FileRepository;
import org.eclipse.jgit.lib.Repository;



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
     * @param function Full string of the function.
     * @param filePath   path to file in which the tests exists in.
     * @return CyclomaticComplexity
     */
    public static int getCyclomaticComplexity(String function) {
        try{
            InputStream targetStream = new ByteArrayInputStream(function.getBytes(StandardCharsets.UTF_8));
            int x = Example1.CyclomaticComplexity(targetStream);
            targetStream.close();
            return x;
        }catch(Exception e){
            e.printStackTrace();
        }

        return 0;
    }

    private static Repository getRepo(String path) {
        try {
            return new FileRepository(path);
        } catch (IOException e ){
            return null;
        }
    }

    /**
     * Calculates cyclomatic complexity using [xxx] program
     */
    private static void calculateCyclomaticComplexity(String commitID) throws GitAPIException {
        Repository repo = getRepo("gitRepos/repos/"+REPONAME);
        jGitCheckout(repo,commitID);
    }


    /**
     * Should be in output/repo/testMethods.txt
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
     * @repo
     */
    private static void jGitCheckout(Repository repo,String commitID) throws GitAPIException {
        Git git = new Git(repo);
        git.checkout().setStartPoint(commitID).call();
    }

    /**
     * Gets the URL from input.txt
     * @return URL to repo
     */
    private static String getPath() {
        File pathFile = new File("input.txt");
        try{
            Scanner sc = new Scanner(pathFile);     //file to be scanned

            while (sc.hasNextLine()) {
                String[] repo = sc.nextLine().split(" ");
                if(repo[0].equals(REPONAME)){
                    return repo[1];
                }
            }
        } catch(IOException e){
            e.printStackTrace();
        }
        return "NOT FOUND";

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
     * [commitID {{totalPath,cyclomatic complexity},{totalPath,cyclomatic complexity},{totalPath,cyclomatic complexity}}
     *  commitID {{totalPath,cyclomatic complexity},{totalPath,cyclomatic complexity},{totalPath,cyclomatic complexity}}] New line is to show readability will not be included in format
     * @param outPut format of the output result
     * @param fileName name of file which to saved the result in
     */
    private static void saveResults(String outPut, String fileName) {
        try {
            FileWriter writer = new FileWriter("result/" + fileName + ".txt", true);
            writer.write(outPut);
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * Calling function for Miner
     * @param repo Repo in which to calculate cyclomatic Complexity
     */
    public static void generateCylomaticComplexity(String repo) throws GitAPIException {
        setRepoName(repo);
        setURLName(getPath()); // Need to set reponame before
        ArrayList<String> commitIDS = getCommitIds();
        for (String ids : commitIDS) {
            calculateCyclomaticComplexity(ids);
        }
    }

    /**
     * function to easy try out code.
     */
    public static void main(String[] args) throws GitAPIException {
        generateCylomaticComplexity("deeplearning4j");
    }
}
