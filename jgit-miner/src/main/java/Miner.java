


import java.io.File;
import java.io.IOException;

import java.util.Date;

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.api.errors.InvalidRemoteException;
import org.eclipse.jgit.api.errors.JGitInternalException;
import org.eclipse.jgit.api.errors.TransportException;
import org.eclipse.jgit.diff.DiffEntry;
import org.eclipse.jgit.diff.DiffFormatter;
import org.eclipse.jgit.internal.storage.file.FileRepository;
import org.eclipse.jgit.lib.*;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevTree;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.treewalk.AbstractTreeIterator;
import org.eclipse.jgit.treewalk.CanonicalTreeParser;


public class Miner {
    final static String REPODIRECTORY = "gitRepos/repos"; // Directory in which all repos are saved
    final static String PATHGITDIRECTORY = REPODIRECTORY+"/.git"; // Local directory to git repo
    final static String URLTOREPO = "https://github.com/Renstrom/act.git"; // URL to repo


    // Pattern to find the correct commits
    final static Pattern COMMITPATTERN = Pattern.compile(
            "((refact(or|ing|red))test?(s|ing))|(updat(e|ing|es)test?(s|ing))|test", Pattern.CASE_INSENSITIVE);
    /**
     * Walk function that generates all the commits that exists, output the ones that fulfills the filter options
     */
    public static void walk() throws IOException, GitAPIException {
        String oldHashID;
        String newHashID = "";
        boolean matched = false; // Used to print the correct git diff
        Repository repo = new FileRepository(PATHGITDIRECTORY);
        Git git = new Git(repo);
        RevWalk walk = new RevWalk(repo);

        List<Ref> branches = git.branchList().call();

        for (Ref branch : branches) {
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
                       // gitDiff(repo,oldCommit,newCommit);


                        matched = false;
                    }


                    newHashID = commit.getName();
                    if(COMMITPATTERN.matcher(commit.getFullMessage()).find()){
                        System.out.println(commit.getName());
                        System.out.println(commit.getAuthorIdent().getName());
                        System.out.println(new Date(commit.getCommitTime() * 1000L));
                        System.out.println(commit.getFullMessage());
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
        Git git = new Git(repo) ;
            List<DiffEntry> diff = git.diff().
                    setOldTree(oldCommit).
                    setNewTree(newCommit).
                    call();
            for (DiffEntry entry : diff) {
                DiffFormatter formatter = new DiffFormatter(System.out) ;
                formatter.setRepository(repo);
                formatter.format(entry);
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
        RevWalk walk = new RevWalk(repository);
        RevCommit commit = walk.parseCommit(ObjectId.fromString(objectId));
        RevTree tree = walk.parseTree(commit.getTree().getId());

        CanonicalTreeParser treeParser = new CanonicalTreeParser();
        ObjectReader reader = repository.newObjectReader();
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
        Git.cloneRepository()
                .setURI(url)
                .setDirectory(new File(REPODIRECTORY))
                .call();

    }


    public static void main(String[] args) throws GitAPIException, IOException {
        try{
            getRepo(URLTOREPO);
        } catch (JGitInternalException e){
            System.out.println("Repo is already stored");
        }
        walk();


    }
}