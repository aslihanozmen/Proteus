package util;

import java.util.List;

/**
 * Check whether two strings are similar with each other
 *
 * @author xchu
 */
public class Similarity {


    private static int threadshold = 3;

    public static int getThreadshold() {
        return threadshold;
    }

    public static void setThreadshold(int threadshold) {
        Similarity.threadshold = threadshold;
    }

    //This is where you set the threadshold
    public static boolean levenshteinDistance(String s1, String s2) {
        int dis = computeLevenshteinDistance(s1, s2);
        if (dis <= threadshold)
            return true;
        else
            return false;
    }

    public static boolean cosineDistance(String s1, String s2) {
        throw new UnsupportedOperationException();
    }


    private static int minimum(int a, int b, int c) {
        return Math.min(Math.min(a, b), c);
    }

    public static int computeLevenshteinDistance(CharSequence str1,
                                                 CharSequence str2) {
        int[][] distance = new int[str1.length() + 1][str2.length() + 1];

        for (int i = 0; i <= str1.length(); i++)
            distance[i][0] = i;
        for (int j = 0; j <= str2.length(); j++)
            distance[0][j] = j;

        for (int i = 1; i <= str1.length(); i++)
            for (int j = 1; j <= str2.length(); j++)
                distance[i][j] = minimum(
                        distance[i - 1][j] + 1,
                        distance[i][j - 1] + 1,
                        distance[i - 1][j - 1]
                                + ((str1.charAt(i - 1) == str2.charAt(j - 1)) ? 0
                                : 1));

        return distance[str1.length()][str2.length()];
    }

    public static double similarity(String s1, String s2) {
        if (s1 == null && s2 == null) {
            return 1;
        } else if (s1 == null && s2 != null) {
            return 0;
        } else if (s1 != null && s2 == null) {
            return 0;
        } else {
            int len = Math.max(s1.length(), s2.length());

            return 1 - computeLevenshteinDistance(s1, s2) * 1.0 / len;
        }
    }

    public static double similarity(String[] s1, String[] s2) {
        double totalSim = 0;
        for (int i = 0; i < s1.length; i++) {
            totalSim += similarity(s1[i], s2[i]);
        }

        return totalSim /= s1.length;

    }


    public static double similarity(List<String> s1, List<String> s2) {
        double totalSim = 0;
        for (int i = 0; i < s1.size(); i++) {
            totalSim += similarity(s1.get(i), s2.get(i));
        }

        return totalSim /= s1.size();

    }
}
