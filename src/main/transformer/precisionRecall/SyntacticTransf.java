package main.transformer.precisionRecall;


import ch.epfl.lara.synthesis.stringsolver.StringSolver;

public class SyntacticTransf {

    public static void main(String[] arg) {
//        StringSolver ss3 = new StringSolver();
//        ss3.setLoopLevel(0);
//        ss3.setTimeout(60);
//        ss3.add("IT | Employee: Aslihan Ozmen |  Department: IT", 2);
//        ss3.add("Marketing | Employee: Gamze Yatmaz |  Department: Marketing", 2);
//        String output3 = ss3.solve("Sales | Employee: Alex Voit", false);
//        System.out.println(output3);
//
//        StringSolver ss1 = new StringSolver();
//        ss1.setLoopLevel(0);
//        ss1.add("1:00 PM | 1300", 1);
//        ss1.add("6:00 PM | 1800", 1);
//        String output1 = ss1.solve("11:00 PM", false);
//        System.out.println(output1);
//
//        StringSolver ss2 = new StringSolver();
//        ss2.setLoopLevel(0);
//        ss2.add("4 | April", 1);
//        ss2.add("7 | July", 1);
//        String output2 = ss2.solve("12", false);
//        System.out.println(output2);
//
//
//        StringSolver ss4 = new StringSolver();
//        ss4.setLoopLevel(0);
//        ss4.add("1 mb | 0.001 gb", 1);
//        ss4.add("790 mb | 0.790 gb", 1);
//        String output4 = ss4.solve("49 mb", false);
//        System.out.println(output4);
//
//        StringSolver ss5 = new StringSolver();
//        ss5.setLoopLevel(0);
//        ss5.add("username=John Doe; expires=Thu 18 Dec 2013 12:00:00 UTC; domain=.google.com; path=/ | .google.com", 1);
//        ss5.add("skin=noskin; path=/; domain=.amazon.com; expires=Mon 22-Mar-2010 18:40:34 GMT; | .amazon.com", 1);
//        String output5= ss5.solve("path=/; domain=.microsoft.com; expires=Mon, 1-Mar-2010 18:40:34 GMT;", false);
//        System.out.println(output5);
//
//        StringSolver ss7 = new StringSolver();
//        ss7.setLoopLevel(0);
//        ss7.add("19851231 | Dec 31, 1985", 1);
//        ss7.add("19811120 | Nov 20, 1981", 1);
//        String output7 =  ss7.solve("20110301", false);
//        System.out.println(output7);
//
//
//        StringSolver ss8 = new StringSolver();
//        ss8.setLoopLevel(0);
//        ss8.add("00:06:32.4458750 | 0 hrs 6 mins 32 secs", 1);
//        ss8.add("11:12:13.3458750 | 11 hrs 12 mins 13 secs", 1);
//        ss8.add("23:01:09.0988712 | 23 hrs 1 mins 9 secs", 1);
//        String output8 = ss8.solve("34:12:10.0353485", false);
//        System.out.println(output8);
//
//
//        StringSolver ss9 = new StringSolver();
//        ss9.setLoopLevel(0);
//        ss9.add("Stephan | Hawkings | Mr. Stephan Hawkings", 2);
//        ss9.add("Elon | Musk | Mr. Elon Musk", 2);
//        ss9.add("Bill | Gates | Mr. Bill Gates", 2);
//        String output9 = ss9.solve("George | Micheal", false);
//        System.out.println(output9);
//
//        StringSolver ss10 = new StringSolver();
//        ss10.setLoopLevel(0);
//        ss10.add("Thursday, 1st January, 1970 | January", 1);
//        ss10.add("Wednesday, 2nd March, 1980 | March", 1);
//        ss10.add("Monday, 5th May, 1999| May", 1);
//        String output10 = ss10.solve("Friday, 3rd April, 1989", false);
//        System.out.println(output10);


//        StringSolver ss11 = new StringSolver();
//        ss11.setLoopLevel(0);
//        ss11.add("1 Microsoft Way Redmond | Citibank-Redmond", 1);
//        ss11.add("1201 Super Commerce Blvd Ste H Richmond | Citibank-Richmond", 1);
//        ss11.add("100 Sunset Blvd Room Sacramento|  Citibank-Sacramento", 1);
//        String output11 = ss11.solve("1 Infinite Loop Perry", false);
//        System.out.println(output11);
//        System.out.println((ss11.solve().get()));

//        StringSolver ss14 = new StringSolver();
//        ss14.setLoopLevel(0);
//        ss14.add("WA, Washington | WA", 1);
//        ss14.add("VA, Virginia | VA", 1);
//        ss14.add("CA, California | CA", 1);
//        String output11 = ss14.solve("TX, Texas", false);
//        System.out.println(output11);
//        System.out.println((ss14.solve().get()));

        StringSolver ss15 = new StringSolver();
        ss15.setLoopLevel(0);
        ss15.add("What is the matter | WHAT IS THE MATTER", 1);
        ss15.add("Something is wrong | Something is wrong", 1);
        ss15.add("Background color | BACKGROUND COLOR", 1);
        String output15 = ss15.solve("Everyday activity", false);
        System.out.println(output15);

//        StringSolver ss12 = new StringSolver();
//        ss12.setLoopLevel(0);
//        ss12.add("10023 | US-10020", 1);
//        ss12.add("21134 | US-2113-U", 1);
//        ss12.add("32245 |  US-3224-U", 1);
//        String output12 = ss12.solve("43356", false);
//        System.out.println(output12);

//        StringSolver ss13 = new StringSolver();
//        ss13.setLoopLevel(0);
//        ss13.setAdvancedStats(true);
//        ss13.add("1 Microsoft Way Redmond | Redmond", 1);
//        ss13.add("1201 Super Commerce Blvd Ste H Richmond | Richmond", 1);
//        ss13.add("410 Terry Ave Beaumont | Beaumont", 1);
//        System.out.println(ss13.getStatistics());
//        String output13 = ss13.solve("100 Sunset Blvd Room Sacramento", false);
//        System.out.println(output13);


    }
}