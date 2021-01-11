package util.fd.tane;

import gnu.trove.list.array.TIntArrayList;


/**
 * Objects of this class hold are functionale dependency. The class is used by
 * algorithms in TANEjava.java to calculate the cover and closure for a set F
 * of functional dependencies.
 *
 * @author Tobias
 */
public class FunctionalDependency implements Comparable<FunctionalDependency> {
    protected ComparableSet<String> X = new ComparableSet<String>();
    protected ComparableSet<String> Y = new ComparableSet<String>();
    private TIntArrayList XIds = new TIntArrayList();
    private TIntArrayList YIds = new TIntArrayList();

    public TIntArrayList getXIds() {
        return XIds;
    }

    public TIntArrayList getYIds() {
        return YIds;
    }

    /**
     * Empty constructor
     */
    public FunctionalDependency() {
    }

    /**
     * Returns the left-hand-side of a functional dependency
     *
     * @return ComparableSet<String> - the the left-hand-side attributes
     */
    public ComparableSet<String> getX() {
        return X;
    }

    /**
     * Returns the right-hand-side of a functional dependency
     *
     * @return ComparableSet<String> - the the right-hand-side attributes
     */
    public ComparableSet<String> getY() {
        return Y;
    }

    /**
     * Adds a new attribue to the left-hand-side
     *
     * @param attribute - the left-hand-side atttibute
     */
    public void addX(String attribute) {
        X.add(attribute);
    }

    /**
     * Adds a new attribue to the left-hand-side
     *
     * @param attributeId - the left-hand-side atttibute
     */
    public void addXId(int attributeId) {
        XIds.add(attributeId);
        addX("" + attributeId);
        ;
    }

    /**
     * Adds an attribue set to the left-hand-side
     *
     * @param attribute - the left-hand-side atttibutes
     */
    public void addX(ComparableSet<String> attribute) {
        X.addAll(attribute);
    }

    /**
     * Adds an attribue to the right-hand-side
     *
     * @param attributeId - the right-hand-side atttibute
     */
    public void addYId(int attributeId) {
        YIds.add(attributeId);
        Y.add(attributeId + "");
    }

    /**
     * Adds an attribue to the right-hand-side
     *
     * @param attribute - the right-hand-side atttibute
     */
    public void addY(String attribute) {
        Y.add(attribute);
    }

    /**
     * Adds an attribue set to the right-hand-side
     *
     * @param attribute - the right-hand-side atttibutes
     */
    public void addY(ComparableSet<String> attribute) {
        Y.addAll(attribute);
    }

    public int compareTo(FunctionalDependency o) {
        int cmp = X.compareTo(o.getX());

        //Wenn erster Paar gleich ist, dann entscheide anhand vom Zweiten
        if (cmp == 0) {
            cmp = Y.compareTo(o.getY());
        }
        return cmp;
    }

    /**
     * Prints a functional dependency.
     */
    public String toString() {
        return X + "->" + Y;
    }

    /**
     * Clears the attribues of the RHS and LHS candidates
     */
    public void clear() {
        X.clear();
        Y.clear();
    }
}
