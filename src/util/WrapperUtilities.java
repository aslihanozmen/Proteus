package util;

import gnu.trove.map.TObjectIntMap;

public class WrapperUtilities {

    public static int indexOfmaxValue(int[] list) {
        int max = 0;
        for (int i = 0; i < list.length; i++) {
            if (list[max] < list[i]) {
                max = i;
            }
        }
        return max;
    }

    public static String getPathWithMaxValue(TObjectIntMap<String> outputFields, TObjectIntMap<String> outputFieldLength) {
        int max = 0;
        String maxKey = null;
        for (String path : outputFields.keySet()) {
            int count = outputFields.get(path);
            if (count > max) {
                maxKey = path;
                max = count;
            } else if (count == max) {
                if (outputFieldLength.get(maxKey) > outputFieldLength.get(path)) {
                    maxKey = path;
                    max = count;
                }
            }
        }
        return maxKey;
    }

}
