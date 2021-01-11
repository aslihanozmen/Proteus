package util.fd.tane;

import java.util.*;

import model.Table;

/**
 * The class TANEjava provides key features to extract functional dependencies of any table of a given
 * database. The following code is sufficient to obtain functional dependencies for a
 * given table. Note that the DatabaseConnection object must be associated with a table by using the <i>setTable</i>
 * method.<p>
 * <code>
 * DatabaseConnection con = new DatabaseConnection();<br>
 * try{<br>
 * con.connect(DatabaseConnection.DBvendors.Oracle10gSeries,"localhost","1521","XE", USER, PASSWORD);<br>
 * con.setTable("employees");<br>
 * TANEjava tane = new TANEjava(con);<br>
 * tane.getFD();<br>
 * </code>
 *
 * @author Tobias
 */

public class TANEjava {

    private int stoplevel;

    //table information
    private Table table = null;            //the current table to be investigated
    private String[] table_columns;
    private int table_rows;
    int foundFDs = 0;

    // --------------Information the algorithm uses------------------------>
    private HashMap<BitSet, CandidateInfo> last_level = null;    //level l-1
    private HashMap<BitSet, CandidateInfo> current_level = null;    //level l
    private HashMap<BitSet, ArrayList<BitSet>> prefix_blocks = null;


    /* Variables for calculating with partitions */
    private int tuple_tbl[];
    private int set_tbl[];
    private int set_visit[];
    private int newtuple_tbl[];
    private int nooftuples = 0;
    private int noofsets = 0;

    private double errorThreshold = 0.05;

    public ArrayList<FunctionalDependency> fds = new ArrayList<FunctionalDependency>();

    //----------------DEBUG MODE------------------------
    boolean debug = false;

    String tempFolder = System.getProperty("java.io.tmpdir");


    public TANEjava(Table table, double errorThreshold) throws Exception {

        this.table = table;
        table_columns = table.getColumnMapping().getColumnNames();
        table_rows = table.getNumRows();


        this.errorThreshold = errorThreshold;

        //default stoplevel
        stoplevel = table_columns.length;

        //inialize tables to calculate partitions effiently
        nooftuples = table_rows + 1;
        noofsets = nooftuples;

        tuple_tbl = new int[nooftuples];
        set_tbl = new int[noofsets];
        set_visit = new int[noofsets];
        newtuple_tbl = new int[noofsets];
    }


    /**
     * This is the TANE main algorithm and returns all minimal non-trivial
     * functional dependencies. By default the output is written in TanejavaOutput.xml. You can change the filename by using
     * calling setTaneResultFile().<p>
     * Before any use of this method, a table must
     * be specified by using the <i>setTable</i>  method.<p>
     * <code>
     * DatabaseConnection con = new DatabaseConnection();<br>
     * try{<br>
     * con.connect(DatabaseConnection.DBvendors.Oracle10gSeries,"localhost","1521","XE", USER, PASSWORD);<br>
     * con.setTable("employees");<br>
     * TANEjava tane = new TANEjava(con);<br>
     * tane.getFD();<br>
     * </code>
     *
     * @throws Exception
     * @throws OutOfMemoryError
     * @throws ArrayStoreException
     */
    public ArrayList<FunctionalDependency> getFD() throws Exception, OutOfMemoryError, ArrayStoreException {

        fds = new ArrayList<FunctionalDependency>();
        try {
            long start = System.currentTimeMillis(); // start timing

            /*the levels are hold into a hashtable for constant access time O(1)
             * Only two level are needed
             */
            last_level = new HashMap<BitSet, CandidateInfo>();
            current_level = new HashMap<BitSet, CandidateInfo>();

            // ----------Create Level 0 ----------------------------
            BitSet empty = new BitSet();
            CandidateInfo c = new CandidateInfo();
            c.setRHS_BitRange(1, table_columns.length + 1);    // C(empty)=R
            last_level.put(empty, c);

            // ------------create Level 1--------------------->
            for (int i = 1; i <= table_columns.length; i++) {
                BitSet candidate = new BitSet();
                candidate.set(i); // initalize BitSet

                CandidateInfo candidateInfo = new CandidateInfo(); // Create CandiateInfo
                candidateInfo.setRHS_BitRange(1, table_columns.length + 1);

                StrippedPartition sp = new StrippedPartition(table.projectColumn(i - 1), table_rows);

                candidateInfo.setStrippedPartition(sp);
                current_level.put(candidate, candidateInfo);


            }


            // -----------------------------MAIN_LOOP------------------>
            int l = 1;
            while (!current_level.isEmpty() && l <= stoplevel) {
                computeDependencies(l);        //find all minimal dependencies in previous level

                prune();                    //prunning the search space


                //delete last_level
                for (BitSet bs : last_level.keySet()) {
                    CandidateInfo info = last_level.get(bs);
                    info.clear();
                }
                last_level.clear();    //last level is no longer of interest
                last_level = null;
                System.gc();    //run garbage collector

                //calculate the next level from the current level
                generateNextLevel(l);
                l++;
            }

            long time = System.currentTimeMillis() - start; // stop timing
            //Convert milliseconds to hh:mm:ss
            int sec = (int) time / 1000;
            int min = sec / 60;
            sec = sec % 60;    //neue sekunden
            int hours = min / 60;
            min = min % 60;

            if (debug) {
                System.out.println("Es wurden " + foundFDs + " minimale FDs gefunden");
                System.out.println("Zeit f�r Berechnung: " + hours + "h:" + min + "min:" + sec + "s");
            }


            foundFDs = 0;  //reset FDs to 0


            return fds;
        } finally {

            System.gc();    //run GC
        }


    }


    /**
     * This method finds the minimal dependencies with the left-hand side in L<sub>l-1</sub>
     */
    /**
     * @param level
     * @throws Exception
     * @throws OutOfMemoryError
     */
    private void computeDependencies(int level) throws Exception, OutOfMemoryError {
        //------------COMPUTE THE INITALIZE RHS CANDIDATES------>
        //System.out.println("Entering generate COMPUTE in Level " +level);

        for (BitSet X : current_level.keySet()) {
            BitSet rhs = current_level.get(X).getRHS();
            BitSet Xclone = (BitSet) X.clone();
            //iterate through all bits in the attribute set
            for (int l = X.nextSetBit(0); l >= 0; l = X.nextSetBit(l + 1)) {
                Xclone.clear(l);
                BitSet CxwithouA = last_level.get(Xclone).getRHS();
                rhs.and(CxwithouA);
                Xclone.set(l);
            }
            CandidateInfo c = current_level.get(X);
            c.setRHS(rhs);
            current_level.put(X, c);
        }
        //--------------------COMPUTE DEPENDENCIES--------------------------->
        //for each AttributeSet do
        if (level > 1) {
            //BitSet that represents the Relationscheme


            for (BitSet attribute : current_level.keySet()) {
                //For each A in X intersec C(X) do
                BitSet C = current_level.get(attribute).getRHS();
                BitSet intersection = (BitSet) attribute.clone();


                intersection.and(C);    //intersection X n C(X)

                //create a copy the attributeSet to avoid a ConcurrentModificationError
                BitSet X = (BitSet) attribute.clone();


                //go through all A in XnC(X)
                for (int l = intersection.nextSetBit(0); l >= 0; l = intersection.nextSetBit(l + 1)) {
                    X.clear(l);
                    boolean fd_holds = false;
                    //if the memory version of Tane-java ist used, Stripped Partitions are used

                    StrippedPartition spXwithoutA = last_level.get(X).getStrippedPartition();
                    StrippedPartition spX = current_level.get(attribute).getStrippedPartition();
                    //check if FD holds or not
                    if (spXwithoutA.error() - spX.error() > errorThreshold * table_rows) {
                        fd_holds = false;
                    } else if (spXwithoutA.error() <= errorThreshold * table_rows) {
                        fd_holds = true;
                    } else {
                        int error = 0;

                        int[] marker = new int[table_rows];
                        for (int i = 0; i < marker.length; i++) {
                            marker[i] = 0;
                        }

                        int[] xElements = spX.getElements();

                        int curSize = 0;
                        for (int i = 0; i < xElements.length; i++) {
                            curSize++;
                            if (Bits.testBit(xElements[i], 31)) {
                                marker[Bits.clearBit(xElements[i], 31) - 1] = curSize;
                                curSize = 0;
                            }

                        }

                        int[] xwaElements = spXwithoutA.getElements();

                        curSize = 0;
                        int m = 1;
                        for (int i = 0; i < xwaElements.length; i++) {
                            curSize++;
                            if (Bits.testBit(xwaElements[i], 31)) {
                                int t = Bits.clearBit(xwaElements[i], 31);
                                m = Math.max(m, marker[t - 1]);

                                error += curSize - m;

                                m = 1;
                                curSize = 0;
                            } else {
                                m = Math.max(m, marker[xwaElements[i] - 1]);
                            }

                        }


                        if (error <= errorThreshold * table_rows) {
                            fd_holds = true;
                        }

                    }
//					if(spX.error() == spXwithoutA.error()){
//						fd_holds = true;
//					}

                    if (fd_holds) {
                        foundFDs++;
                        //console output
                        if (debug)
                            System.out.println(X + " --> " + l);


                        FunctionalDependency fd = new FunctionalDependency();

                        for (int j = X.nextSetBit(0); j >= 0; j = X.nextSetBit(j + 1)) {
                            //fd.addX(table_columns[j-1]);
                            fd.addXId((j - 1));
                        }

                        //fd.addY(table_columns[l-1]);
                        fd.addYId((l - 1));

                        fds.add(fd);

                        //Remove A from C(X)

                        C.clear(l);

                        //Remove all B in R\X from C(X)
                        BitSet R = new BitSet();
                        R.set(1, table_columns.length + 1);

                        //R equals R\X
                        R.xor(attribute);    //xor --> symmetische Differenz entspricht R\X, da X Teilmenge aus R
                        C.xor(R);            //delete R\X from C(X)

                        //Remove all A in X, if there's a B in X such that X\{A,B}--> B holds
                        for (int k = R.nextSetBit(0); k >= 0; k = R.nextSetBit(k + 1)) {
                            BitSet XB = (BitSet) attribute.clone();
                            XB.set(k); //set bit k in XB
                            XB.clear(l);    //remove bit l from XB
                            if (current_level.containsKey(XB)) {
                                current_level.get(XB).getRHS().clear(l);
                            }

                        }

                        //add l to attributset again
                        X.set(l);
                    }
                    X.set(l);
                }
            }
        }
    }

    /**
     * This method prunes the search space. Goal is to reduce the computation on each
     * level by using results from the previous levels.
     */
    private void prune() throws Exception, OutOfMemoryError {
        prefix_blocks = new HashMap<BitSet, ArrayList<BitSet>>();
        ArrayList<BitSet> BitsToRemove = new ArrayList<BitSet>();
        boolean RHS_is_empty = false;
        boolean X_is_superkey = false;


        // check if C(X) is empty
        for (BitSet attr : current_level.keySet()) {
            RHS_is_empty = false;
            X_is_superkey = false;

            if (current_level.get(attr).getRHS().isEmpty()) {

                RHS_is_empty = true;
                BitsToRemove.add(attr);
                continue; // go to loop header
            }
            // check if X is a (super)key Code adapted from original TANE source

            if (current_level.get(attr).getStrippedPartition().error() <= errorThreshold * table_rows)
                X_is_superkey = true;

            // do pruning
            if (X_is_superkey) {
                BitSet R = new BitSet();
                R.set(1, table_columns.length + 1); // R
                R.xor(attr); // R\X
                R.and(current_level.get(attr).getRHS()); // R\X intersection
                // C(X)

                for (int l = R.nextSetBit(0); l >= 0; l = R.nextSetBit(l + 1)) {
                    if (debug)
                        System.out.println(attr + " --> " + l + " (key)");


                    FunctionalDependency fd = new FunctionalDependency();

                    for (int j = attr.nextSetBit(0); j >= 0; j = attr.nextSetBit(j + 1)) {

                        //fd.addX(table_columns[j-1]);
                        fd.addXId((j - 1));
                    }


                    //fd.addY(table_columns[l-1]);
                    fd.addYId((l - 1));

                    fds.add(fd);


                    foundFDs++;
                    BitsToRemove.add(attr);

                }
            }
            if (!RHS_is_empty && !X_is_superkey) {    //both must be false, otherise design error
                // generate prefix block if the attribute set has not
                // removed from level
                if (prefix_blocks.containsKey(getPrefix(attr))) {
                    ArrayList<BitSet> set = prefix_blocks.get(getPrefix(attr));
                    set.add(attr);
                    prefix_blocks.put(getPrefix(attr), set);
                } else {
                    ArrayList<BitSet> set = new ArrayList<BitSet>();
                    set.add(attr);
                    prefix_blocks.put(getPrefix(attr), set);
                }

            }

        }
        for (BitSet X : BitsToRemove)
            current_level.remove(X);
        BitsToRemove.clear();
        BitsToRemove = null;

    }

    /**
     * Generates the next level.
     *
     * @return
     * @throws Exception
     */
    private void generateNextLevel(int level) throws Exception, OutOfMemoryError, ArrayStoreException {
        //Calculate size of number of candiates in next Level
        int numberOfNextLevelCandidates = (int) (current_level.size() * (table_columns.length - level)) / (level + 1);
        StrippedPartition candidateI = null;
        StrippedPartition candidateK = null;

        last_level = current_level;
        //current_level.clear();

        current_level = new HashMap<BitSet, CandidateInfo>(numberOfNextLevelCandidates);

        //System.out.println("Entering generate Nextgeneration");

        //go through each PrefixBlock
        for (BitSet prefix : prefix_blocks.keySet()) {
            ArrayList<BitSet> prefix_block = prefix_blocks.get(prefix);

            for (int i = 0; i < prefix_block.size(); i++) {
                //load partition
                candidateI = last_level.get(prefix_block.get(i)).getStrippedPartition();
                loadPartitionTable(candidateI);

                for (int k = i + 1; k < prefix_block.size(); k++) {

                    //Create LHS candidate for next level
                    BitSet nextCandidate = new BitSet();    //empty bitset
                    nextCandidate.or(prefix_block.get(i));            //or --> equals the union of two bitsets
                    nextCandidate.or(prefix_block.get(k));
                    //System.out.println("ATTSET I: " +prefix_block.get(i));
                    //System.out.println("ATTSET K: " +prefix_block.get(k));

                    //Create Partition
                    candidateK = last_level.get(prefix_block.get(k)).getStrippedPartition();
                    //load partition
                    //check if all subsets of length l of the new candidate are in the current level
                    //iterate over each bit, remove it, check if subset, add bit again
                    boolean candidateIsValid = true;
                    for (int l = nextCandidate.nextSetBit(0); l >= 0; l = nextCandidate.nextSetBit(l + 1)) {
                        //delete Attribute and check if candidate is in currentLevel
                        nextCandidate.clear(l);
                        if (!last_level.containsKey(nextCandidate)) {
                            candidateIsValid = false;
                            nextCandidate.set(l);
                            break;
                        }
                        //add delted attribute to the nextCandidate again
                        nextCandidate.set(l);
                    }
                    //if candiate is valid then calaculate stripped partitions
                    if (candidateIsValid) {
                        //create new CandidateInfo
                        CandidateInfo info = new CandidateInfo();
                        info.setRHS_BitRange(1, table_columns.length + 1);

                        //calculate StrippedPartition
                        StrippedPartition sp_new = strippedProduct(candidateK);
                        info.setStrippedPartition(sp_new);

                        current_level.put(nextCandidate, info);
                    }
                }
                unloadPartitionTable(candidateI);


            }
        }
        prefix_blocks.clear();        //delete prefix_blocks
        prefix_blocks = null;


    }

    /**
     * Return the prefix of an attribute set.
     * Removes the highes bit of the BitSet and return a new BitSet
     *
     * @param bitset -A BitSet object
     * @return BitSet -the new coded BitSet with the highest bit removed.
     */
    private BitSet getPrefix(BitSet bitset) {
        BitSet prefix = (BitSet) bitset.clone();
        //delete highest bit
        prefix.clear(prefix.length() - 1);

        return prefix;
    }


    /**
     * Calling this method results in functional dependencies having at most <i>stoplevel</i>
     * attributes on its left-hand-side.
     *
     * @param level - max attributes on the LHS.
     */
    public void setStoplevel(int level) {
        this.stoplevel = level + 1;
    }

    /**
     * Computes the minimal cover for a set of functional dependencies.
     *
     * @param FD_SET -a set of functional dependencies
     * @return
     */
    public static ComparableSet<FunctionalDependency> minimalCover(ComparableSet<FunctionalDependency> FD_SET) {
        ComparableSet<FunctionalDependency> cover = new ComparableSet<FunctionalDependency>();

        // Applying decomposition rule
        for (FunctionalDependency fd : FD_SET) {
            for (String Y : fd.getY()) {
                FunctionalDependency fd_new = new FunctionalDependency();
                fd_new.addX(fd.getX());
                fd_new.addY(Y);
                cover.add(fd_new);
            }
        }
        // make the LHS of cover minimal
        ComparableSet<FunctionalDependency> newFDs = new ComparableSet<FunctionalDependency>();

        Iterator<FunctionalDependency> it = cover.iterator();
        while (it.hasNext()) {
            FunctionalDependency fd = it.next();
            if (fd.getX().size() > 1) {
                for (String B : fd.getX()) {
                    if (fd.getY()
                            .isSubset(closure(cover, fd.getX().without(B)))) {
                        FunctionalDependency replacedFD = new FunctionalDependency();
                        replacedFD.addX(fd.getX().without(B));
                        replacedFD.addY(fd.getY());
                        newFDs.add(replacedFD);
                        it.remove();
                    }
                }
            }
        }
        cover.addAll(newFDs);
        newFDs = null; // clean up for Garbage Collector
        // System.out.println("Fc nach Linksreduktion: "+cover);
        // remove redundant FDs
        it = cover.iterator(); // inializes the iterator new
        while (it.hasNext()) {
            FunctionalDependency fd = it.next();
            if (fd.getY().isSubset(closure(cover.without(fd), fd.getX())))
                it.remove();
        }

        return cover;
    }

    /**
     * Computes the closure of an attribute set X under F where F is a set of
     * functional dependencies. For all subsets A in the closure we have
     * <p>
     * <center>X --> A</center> Therefore X --> 'closure' also holds due to the
     * decomposition rule
     *
     * @param FD_SET
     * @param ATTR_SET
     * @return ComparableSet<String> - the closure of an attribute according to
     * F
     */
    public static ComparableSet<String> closure(ComparableSet<FunctionalDependency> FD_SET, ComparableSet<String> ATTR_SET) {
        ComparableSet<String> closure = new ComparableSet<String>();
        ComparableSet<String> oldClosure = new ComparableSet<String>();

        closure.addAll(ATTR_SET);
        do {
            // oldClosure.clear();
            oldClosure.addAll(closure);
            for (FunctionalDependency fd : FD_SET) {
                if (fd.getX().isSubset(closure))
                    closure.addAll(fd.getY());

            }
        } while (!closure.equals(oldClosure));
        return closure;
    }

    /**
     * This class is needed before the product of two partitions are build.
     * More precisely, it inializes the table T[] of Algorithm 'StrippedPartition'
     * in "TANE - An efficient Algorithm for discovering functional dependencies".
     *
     * @param p - the stripped partition whose table T[] has to be inializes.
     */
    private void loadPartitionTable(StrippedPartition p) {
        int i = 0, setno = 1;

        int[] elements = p.getElements();
        while (i < elements.length) {
            while (true) {
                if (Bits.testBit(elements[i], 31)) break;
                tuple_tbl[elements[i++] - 1] = setno;
            }
            tuple_tbl[Bits.clearBit(elements[i++], 31) - 1] = setno;
            setno++;
        }
        noofsets = setno - 1;
        set_tbl[noofsets + 1] = 1;
    }

    /**
     * Sets the table T[] to all 0;
     *
     * @param p
     */
    private void unloadPartitionTable(StrippedPartition p) {
        int i = 0;
        int elements[] = p.getElements();

        for (i = 0; i < elements.length; i++) {
            tuple_tbl[Bits.clearBit(elements[i], 31) - 1] = 0;
        }
        set_tbl[noofsets + 1] = 0;
    }

    /**
     * Computes a new partition P<sub>z</sub> = P<sub>x</sub> * P<sub>y</sub>.<p>
     * The partition P<sub>x</sub> has to be loaded by the method loadPartitionTable()
     * <code>
     * loadPartitionTable(X)<br>
     * StrippedPartition Z = strippedProduct(Y)<br>
     * unloadPartitionTable(X)
     * </code>
     *
     * @param p
     * @return
     */
    private StrippedPartition strippedProduct(StrippedPartition p) {
        int i = 0, j = 0, k = 0, base_index = 1, setindex = 0;
        int element = 0;
        int newElements = 0;
        int newNoofSets = 0;

        int elements[] = p.getElements();

        while (i < elements.length) {
            setindex = 0;
            while (!Bits.testBit(elements[i], 31)) {
                if (set_tbl[tuple_tbl[elements[i++] - 1]]++ == 0) {
                    set_visit[setindex++] = tuple_tbl[elements[i - 1] - 1];
                }
            }
            elements[i] = Bits.clearBit(elements[i], 31);

            if (set_tbl[tuple_tbl[elements[i++] - 1]]++ == 0) {
                set_visit[setindex++] = tuple_tbl[elements[i - 1] - 1];
            }

            set_tbl[0] = 0;
            for (k = 0; k < setindex; k++) {
                if (set_visit[k] == 0) continue;
                if (set_tbl[set_visit[k]] == 1) {
                    set_tbl[set_visit[k]] = 0;
                    set_visit[k] = 0;
                    continue;
                }
                base_index += set_tbl[set_visit[k]];
                set_tbl[set_visit[k]] = base_index - set_tbl[set_visit[k]];
            }

            for (; j < i; j++) {
                if (set_tbl[tuple_tbl[elements[j] - 1]] == 0) {
                    continue;
                }
                element = set_tbl[tuple_tbl[elements[j] - 1]]++;
                newtuple_tbl[element - 1] = elements[j];
                newElements++;    //ArraySize of the new partition
            }
            elements[i - 1] = Bits.setBit(elements[i - 1], 31);

            for (k = 0; k < setindex; k++) {
                if (set_visit[k] == 0) continue;
                newtuple_tbl[set_tbl[set_visit[k]] - 2] = Bits.setBit(newtuple_tbl[set_tbl[set_visit[k]] - 2], 31);
                newNoofSets++;
                set_tbl[set_visit[k]] = 0;
            }
        }
        int newPartition[] = new int[newElements];
        for (i = 0; i < newElements; i++) {
            newPartition[i] = newtuple_tbl[i];
        }
        StrippedPartition sp = new StrippedPartition(newPartition, newElements, newNoofSets);

        return sp;
    }

    /**
     * Shows discovered functional dependencies on the console, if
     * debug <code>debug</code> is true.
     *
     * @param debug
     */
    public void setConsoleOutput(boolean debug) {
        this.debug = debug;
    }

    protected void finalize() throws Throwable, Exception {

    }

    /**
     * Sets the temporary folder for swapping stripped partitions. By defualt, the
     * temp folder is your operating system's defualt temporary folder.
     *
     * @param tempFolder - the new temporary folder
     */
    public void setTempFolder(String tempFolder) {
        this.tempFolder = tempFolder;
    }

}
