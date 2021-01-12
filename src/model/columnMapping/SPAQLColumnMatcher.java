//package model.columnMapping;
//
//import java.util.Set;
//
//import qa.qcri.katara.kbcommon.KBReader;
//
///**
// * Does not match texts longer than maxLength (in the constructor)...Numerals are matched textually only
// *
// * @author John Morcos
// */
//public class SPAQLColumnMatcher extends AbstractColumnMatcher {
//    private String dir;
//
//    private KBReader kbr = null;
//    private int maxLength = 1000;
//
//
//    public SPAQLColumnMatcher(String dir, int maxLength) throws Exception {
//        this.dir = dir;
//        this.maxLength = maxLength;
//        kbr = new KBReader(dir);
//    }
//
//
//    @Override
//    protected void finalize() throws Throwable {
//        kbr.close();
//        super.finalize();
//
//    }
//
//
//    @Override
//    public boolean computeDoMatch(String s1, String s2) {
//        s1 = s1.toLowerCase();
//        s1 = s1.replaceAll("&nbsp;", " ");
//        s1 = s1.replaceAll("\\s+", " ");
//        s1 = s1.trim();
//
//
//        s2 = s2.toLowerCase();
//        s2 = s2.replaceAll("&nbsp;", " ");
//        s2 = s2.replaceAll("\\s+", " ");
//        s2 = s2.trim();
//
//        if (s1.equalsIgnoreCase(s2)) {
//            return true;
//        } else if (s1 != null && !s1.matches("(\\d|\\W)+")
//                && s2 != null && !s2.matches("(\\d|\\W)+")) {
//            Set<String> entities = kbr.getEntitiesWithLabelsMatching(s1, s2, 10);
//            if (entities != null && entities.size() > 0) {
//                return true;
//            }
//        }
//
//        return false;
//    }
//}
