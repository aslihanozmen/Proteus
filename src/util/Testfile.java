package util;

public class Testfile {


    public static void main(String[] args) {
        long time = System.currentTimeMillis();
        int i = 0;
        while (i < 2000000000) {
            i++;
            if (i % 1000000000 == 0) {
                System.out.println();
            }

        }
        System.out.println(System.currentTimeMillis() - time);
        time = System.currentTimeMillis();

        i = 0;
        while (i < 2000000000) {
            ++i;
            if (i % 1000000000 == 0) {
                System.out.println();
            }

        }
        System.out.println(System.currentTimeMillis() - time);
    }
}
