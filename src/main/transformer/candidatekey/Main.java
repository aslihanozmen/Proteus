package main.transformer.candidatekey;

import java.util.*;
import java.util.Map.Entry;

public class Main {

    public static void f(String prefix, String chars, List<String> result) {
        StringBuilder sb =  new StringBuilder(prefix);
        for (int i = 0; i < chars.length(); i++) {
            String prefixWithChar = sb.append(chars.charAt(i)).toString();
            result.add(prefixWithChar);
            f(prefixWithChar, chars.substring(i + 1), result);
            sb.setLength(0);
        }
    }

    static List<String> getCombinations(List<String> chars) {
        ArrayList<String> result = new ArrayList<>();
        StringBuilder str = new StringBuilder();
        for (String c : chars) {
            str.append(c);
        }
        chars.clear();
        Main.f("", str.toString(), result);
        return result;
    }

    public static Set<String> closure(Set<String> attributes, Map<Set<String>, Set<String>> dependencies) {
        HashSet<String> closureSet = new HashSet<>(attributes);

        for (Entry<Set<String>, Set<String>> dependency : dependencies.entrySet()) {
            if (closureSet.containsAll(dependency.getKey()) && !closureSet.containsAll(dependency.getValue())) {
                closureSet.addAll(dependency.getValue());
            }
        }
        return closureSet;
    }
}
