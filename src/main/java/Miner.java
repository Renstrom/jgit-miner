


import java.io.*;

import java.util.ArrayList;
import java.util.Date;

import java.util.List;
import java.util.Map;
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
import org.eclipse.jgit.treewalk.AbstractTreeIterator;
import org.eclipse.jgit.treewalk.CanonicalTreeParser;
import org.eclipse.jgit.treewalk.filter.PathSuffixFilter;
import org.eclipse.jgit.treewalk.filter.TreeFilter;


/**
 * Algorithm to mine GIT Commit histories using JGit
 *
 * @author Anders Renström
 */
public class Miner {


    static ArrayList<String> fullData       = new ArrayList<>();
    static ArrayList<String> hashValues     = new ArrayList<>();
    static ArrayList<String> refactoredData = new ArrayList<>(); // NOTE May be temporary




    final static String REPODIRECTORY       = "gitRepos/repos/java-design-patterns"; // Directory in which all repos are saved
    final static String PATHGITDIRECTORY    = REPODIRECTORY+"/.git"; // Local directory to git repo
    final static String URLTOREPO           = "https://github.com/iluwatar/java-design-patterns.git"; // URL to repo
    final static String OUTPUTPATH          =  "java-design-patterns";
    static FileOutputStream f;

    static {
        try {
             f = new FileOutputStream(new File("output/"+OUTPUTPATH+"/diff.txt"));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }


    // Pattern to find the correct commits
    final static Pattern COMMITPATTERN = Pattern.compile(
            "(refact(or|ing|red))|(updat(e|ing|es))", Pattern.CASE_INSENSITIVE);

    final static Pattern COMMITPATTERN2 = Pattern.compile(
            "test|test(s|er|ing|ed)", Pattern.CASE_INSENSITIVE);

    public Miner() throws FileNotFoundException {
    }

    /**
     * Walk function that generates all the commits that exists, output the ones that fulfills the filter options
     */
    public static void walk() throws IOException, GitAPIException {
        String oldHashID;
        String newHashID    = "";
        boolean matched     = false; // Used to print the correct git diff

        Repository repo     = new FileRepository(PATHGITDIRECTORY);
        Git git             = new Git(repo);
        RevWalk walk        = new RevWalk(repo);
        List<Ref> branches  = git.branchList().call();

        for (Ref branch : branches) { // Iterating over all branches
            String branchName = branch.getName();

            System.out.println("Commits of branch: " + branch.getName());
            System.out.println("-------------------------------------");

            Iterable<RevCommit> commits = git.log().all().call();

            for (RevCommit commit : commits) {
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
                    if(COMMITPATTERN.matcher(commit.getFullMessage()).find() && COMMITPATTERN2.matcher(commit.getFullMessage()).find()){
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

    /**
     * Generates the git diffs for each commit.
     * @param repo Repository analysing
     * @param oldCommit Old commit tree iterator
     * @param newCommit New Commit tree iterator
     */
    private static void gitDiff(Repository repo, AbstractTreeIterator oldCommit, AbstractTreeIterator newCommit) throws GitAPIException, IOException {
        TreeFilter treeFilter = PathSuffixFilter.create(".java");
        Git git = new Git(repo) ;
        List<DiffEntry> diff = git.diff().
                    setOldTree(oldCommit).
                    setNewTree(newCommit).
                    setPathFilter(treeFilter).
                    setShowNameAndStatusOnly(true).
                    call();
        for (DiffEntry entry : diff) {


            DiffFormatter formatter = new DiffFormatter(f) ;
            formatter.setRepository(repo);
            formatter.format(entry);
            formatter.toFileHeader(entry).toEditList();
        }

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
        new File("output").mkdir();
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



    public static void main(String[] args) throws GitAPIException, IOException {
        try{
            makeDirectory();
            getRepo(URLTOREPO);
        } catch (JGitInternalException e){
            System.out.println("Repo is already stored");
        } finally {
            walk();
            writeToFile("output/"+OUTPUTPATH+"/full.txt",fullData);
            writeToFile("output/"+OUTPUTPATH+"/hash.txt",hashValues);
        }



    }
}