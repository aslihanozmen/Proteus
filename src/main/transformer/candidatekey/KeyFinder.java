package main.transformer.candidatekey;

import org.magicwerk.brownies.collections.GapList;
import util.fd.tane.FunctionalDependency;

import java.util.*;

public class KeyFinder {

    private Map<Set<String>, Set<String>> fdMap;
    private List<String> attributeList;

    public KeyFinder() {
        this.attributeList = new ArrayList<>();
        this.fdMap = new HashMap<>();
    }

    public void setAttributeList(List<String> attributeList) {
        this.attributeList = attributeList;
    }

    public Set<String> getCandidateKeys() {

        HashSet<String> keys = new HashSet<>();
        ArrayList<String> ignoreList = new ArrayList<>();
        List<String> combinationList = Main.getCombinations(attributeList);
        combinationList.sort(Comparator.comparingInt(String::length));
        for (String combinationItem : combinationList) {
            boolean stop = isStop(ignoreList, combinationItem);
            if (stop) {
                continue;
            }

            HashSet<String> atts = new HashSet<>();
            for (Character C : combinationItem.toCharArray()) {
                atts.add(C.toString());
            }

            Set<String> theClosure = new HashSet<>(atts);
            atts.clear();
            do {
                theClosure = Main.closure(theClosure, fdMap);
                Set<String> closure2 = Main.closure(theClosure, fdMap);
                if (theClosure.size() == closure2.size()) {
                    break;
                } else {
                    theClosure = closure2;
                }
            } while (true);

            if (theClosure.containsAll(attributeList)) {
                keys.add(combinationItem);
                ignoreList.add(combinationItem);

            }
            theClosure.clear();
        }
        combinationList.clear();
        ignoreList.clear();
        return keys;
    }

    private boolean isStop(ArrayList<String> ignoreList, String combinationItem) {
        boolean stop = false;
        for (String ignore : ignoreList) {
            int found = 0;
            for (Character c : combinationItem.toCharArray()) {
                if (ignore.contains(c.toString())) {
                    found++;
                }
            }
            if (found == ignore.length()) {
                stop = true;
                break;
            }
        }
        ignoreList.clear();
        return stop;
    }

    public void addFD(List<FunctionalDependency> fds) {

        List<String> lhs;
        HashSet<String> lhsHashSet;
        HashSet<String> rhsHashSet;
        for (FunctionalDependency s : fds) {
            lhs = new GapList<>(s.getX());
            Collections.sort(lhs);
            // Sort left hand side alphabetically (due to how the HashMap works, AB!=BA)
            lhsHashSet = new HashSet<>(lhs);
            // Sort left hand side alphabetically (due to how the HashMap works, AB!=BA)
             rhsHashSet = new HashSet<>(s.getY());
            fdMap.put(lhsHashSet, rhsHashSet);
        }
        fds.clear();
    }

    public void reset() {
        attributeList.clear();
        fdMap.clear();
    }
}
