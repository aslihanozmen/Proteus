package util;

public class EditDistance {

    public static double editDistance(String a, String b) {
        if (a == null && b == null) {
            return 0;
        } else if (a != null && b == null) {
            return 1;
        } else if (a == null && b != null) {
            return 1;
        } else {
            a = a.toLowerCase();
            b = b.toLowerCase();
            // i == 0
            int[] costs = new int[b.length() + 1];
            for (int j = 0; j < costs.length; j++)
                costs[j] = j;
            for (int i = 1; i <= a.length(); i++) {
                // j == 0; nw = lev(i - 1, j)
                costs[0] = i;
                int nw = i - 1;
                for (int j = 1; j <= b.length(); j++) {
                    int cj = Math.min(1 + Math.min(costs[j], costs[j - 1]),
                            a.charAt(i - 1) == b.charAt(j - 1) ? nw : nw + 1);
                    nw = costs[j];
                    costs[j] = cj;
                }
            }
            double bigLen = Math.max(a.length(), b.length());

            return costs[b.length()] / bigLen;
        }
    }

    public static double similarity(String a, String b) {
        return 1 - editDistance(a, b);
    }

}