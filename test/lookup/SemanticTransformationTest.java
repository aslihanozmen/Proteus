package lookup;

import main.transformer.lookup.LookupTransformation;
import main.transformer.model.csv.CsvLookupSource;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

public class SemanticTransformationTest {


    @Test
    @Disabled
    void concatenatedCountryNameToCountryCode() throws Exception {
        LookupTransformation lookupTransformation = new LookupTransformation(new CsvLookupSource("test/resources/CountryNameToCode/(225500, [0], [1], 0).csv"));
        lookupTransformation.provideExample("Afghanistan Albania Algeria", "+ 93 + 355 + 213");
        lookupTransformation.provideExample("Andorra Angola Ascensione", "+ 376 + 244 + 247");
        lookupTransformation.provideExample("Argentina Armenia Aruba", "+ 54 + 374 + 297");
        Assertions.assertThat(lookupTransformation.findMatch("Australia Austria Bahrein", false)).isEqualTo("+ 61 + 43 + 973");
    }


    @Test
    void testLikePresentationCsv() throws Exception {
        LookupTransformation lookupTransformation = new LookupTransformation(new CsvLookupSource("test/resources/semanticLikePresentation.csv"));
        lookupTransformation.provideExample("044-58-3429", "Mr. Steve Russell");
        lookupTransformation.provideExample("027-36-4557", "Mr. John Henry");
        Assertions.assertThat(lookupTransformation.findMatch("034-83-7683", false)).isEqualTo("Mr. William Johnson");
    }

    @Test
    void testLikePresentationCsvForPaper() throws Exception {
        CsvLookupSource csvLookupSource = new CsvLookupSource("test/resources/testLikePresentationCsvForPaper/286.csv");
        csvLookupSource.setEditDistance(0.5);
        LookupTransformation lookupTransformation = new LookupTransformation(csvLookupSource);
        lookupTransformation.provideExample("Land: Algeria", "City: Algiers");
        lookupTransformation.provideExample("Land: Angola", "City: Luanda");
        Assertions.assertThat(lookupTransformation.findMatch("Land: Egypt", false)).isEqualTo("City: Cairo");
        Assertions.assertThat(lookupTransformation.findMatch("Land: Tunisia", false)).isEqualTo("City: Tunis");
    }

    @Test
    void testForBenchmark2() throws Exception {
        CsvLookupSource csvLookupSource = new CsvLookupSource("test/resources/semanticForBenchmark2.csv");
        csvLookupSource.setEditDistance(0.5);
        LookupTransformation lookupTransformation = new LookupTransformation(csvLookupSource);
        lookupTransformation.provideExample("Employee: Aslihan Ozmen", "Department: IT");
        lookupTransformation.provideExample("Employee: Gamze Yatmaz", "Department: Marketing");
        Assertions.assertThat(lookupTransformation.findMatch("Employee: Maria Sierra", false)).isEqualTo("Department: Sales");
    }

    @Test
    void testForBenchmark3() throws Exception {
        LookupTransformation lookupTransformation = new LookupTransformation(new CsvLookupSource("test/resources/semanticForBenchmark2.csv"));
        lookupTransformation.provideExample("Aslihan Ozmen", "Ozmen (IT)");
        lookupTransformation.provideExample("Gamze Yatmaz", "Yatmaz (Marketing)");
        Assertions.assertThat(lookupTransformation.findMatch("Maria Sierra", false)).isEqualTo("Sierra (Sales)");
    }


    @Test
    @Disabled
    void testForBenchmark() throws Exception {
        LookupTransformation lookupTransformation = new LookupTransformation(new CsvLookupSource(
                "test/resources/benchmark.csv"));
        lookupTransformation.provideExample("Germany", "Berlin");
        lookupTransformation.provideExample("England", "London");
        lookupTransformation.provideExample("Turkey", "Ankara");
        Assertions.assertThat(lookupTransformation.findMatch("France", false)).isEqualTo("Paris");
    }

    @Test
    void testNestedSyntacticLookupTransformationCsv() throws Exception {
        CsvLookupSource csvLookupSource = new CsvLookupSource(
                "test/resources/nestedSyntacticLookup_table1.csv");
        csvLookupSource.setEditDistance(0.2);
        LookupTransformation lookupTransformation = new LookupTransformation(csvLookupSource);
        lookupTransformation.provideExample("c4 c3 c1", "Facebook Apple Microsoft");
        Assertions.assertThat(lookupTransformation.findMatch("c2 c5 c6", false)).isEqualTo("Google IBM Xerox");
        Assertions.assertThat(lookupTransformation.findMatch("c1 c5 c4", false)).isEqualTo("Microsoft IBM Facebook");
        Assertions.assertThat(lookupTransformation.findMatch("c2 c3 c4", false)).isEqualTo("Google Apple Facebook");
    }

    @Test
    void testLookupTransformationWithConcatenationCsv() throws Exception {
        CsvLookupSource csvLookupSource = new CsvLookupSource(
                "test/resources/lookupWithConcatenation.csv");
        csvLookupSource.setEditDistance(0.3);
        csvLookupSource.setMultiCol(true);
        LookupTransformation lookupTransformation = new LookupTransformation(csvLookupSource);
        List<String> inputsExample = new ArrayList<>();
        inputsExample.add("Honda");
        inputsExample.add("125");
        lookupTransformation.provideMultipleInputsExample(inputsExample, "11.500");
        List<String> remainingInput1 = new ArrayList<>();
        remainingInput1.add("Ducati");
        remainingInput1.add("100");
        Assertions.assertThat(lookupTransformation.findMultipleInputMatch(remainingInput1, false)).isEqualTo("10.000");
        List<String> remainingInput2 = new ArrayList<>();
        remainingInput2.add("Honda");
        remainingInput2.add("250");
        Assertions.assertThat(lookupTransformation.findMultipleInputMatch(remainingInput2, false)).isEqualTo("19.000");
        List<String> remainingInput3 = new ArrayList<>();
        remainingInput3.add("Ducati");
        remainingInput3.add("250");
        Assertions.assertThat(lookupTransformation.findMultipleInputMatch(remainingInput3, false)).isEqualTo("18.000");
    }

    @Test
    void testSyntacticWithMultipleLookupTransformationCsv() throws Exception {
        CsvLookupSource csvLookupSource = new CsvLookupSource("" +
                "test/resources/syntactictWithMultipleLookups_table100.csv",
                "test/resources/syntactictWithMultipleLookups_table98.csv");
        csvLookupSource.setMultiCol(true);
        LookupTransformation lookupTransformation = new LookupTransformation(csvLookupSource);
        List<String> inputsExample = new ArrayList<>();
        inputsExample.add("Stroller");
        inputsExample.add("10/12/2010");
        lookupTransformation.provideMultipleInputsExample(inputsExample, "$145.67+0.30*145.67");

        List<String> inputsExample2 = new ArrayList<>();
        inputsExample2.add("Bib");
        inputsExample2.add("23/12/2010");
        lookupTransformation.provideMultipleInputsExample(inputsExample2, "$3.56+0.45*3.56");

        List<String> remainingInput1 = new ArrayList<>();
        remainingInput1.add("Diapers");
        remainingInput1.add("21/1/2011");
        Assertions.assertThat(lookupTransformation.findMultipleInputMatch(remainingInput1, false)).isEqualTo("$21.45+0.35*21.45");

        List<String> remainingInput2 = new ArrayList<>();
        remainingInput2.add("Wipes");
        remainingInput2.add("2/4/2009");
        Assertions.assertThat(lookupTransformation.findMultipleInputMatch(remainingInput2, false)).isEqualTo("$5.12+0.40*5.12");

        List<String> remainingInput3 = new ArrayList<>();
        remainingInput3.add("Aspirator");
        remainingInput3.add("23/2/2010");
        Assertions.assertThat(lookupTransformation.findMultipleInputMatch(remainingInput3, false)).isEqualTo("$2.56+0.30*2.56");
    }

    @Test
    void testSyntacticWithMultipleLookupTransformationCsvForPaper() throws Exception {
        LookupTransformation lookupTransformation = new LookupTransformation(new CsvLookupSource(
                "test/resources/tablesForComplexSemanticTransInPaper/syntactictWithMultipleLookups_table100.csv",
                "test/resources/tablesForComplexSemanticTransInPaper/syntactictWithMultipleLookups_table98.csv"));
        List<String> inputsExample = new ArrayList<>();
        inputsExample.add("Beverages");
        inputsExample.add("11/1/2011");
        lookupTransformation.provideMultipleInputsExample(inputsExample, "18.12+10.00*118.12€");

        List<String> inputsExample2 = new ArrayList<>();
        inputsExample2.add("Dairy");
        inputsExample2.add("24/1/2011");
        lookupTransformation.provideMultipleInputsExample(inputsExample2, "10.14+24.00*10.14€");

        List<String> remainingInput1 = new ArrayList<>();
        remainingInput1.add("Cereals");
        remainingInput1.add("22/2/2012");
        Assertions.assertThat(lookupTransformation.findMultipleInputMatch(remainingInput1, false)).isEqualTo("22.15+13.00*22.15€");

        List<String> remainingInput2 = new ArrayList<>();
        remainingInput2.add("Meat");
        remainingInput2.add("3/5/2010");
        Assertions.assertThat(lookupTransformation.findMultipleInputMatch(remainingInput2, false)).isEqualTo("21.35+49.00*21.35€");

        List<String> remainingInput3 = new ArrayList<>();
        remainingInput3.add("Seafood");
        remainingInput3.add("24/3/2011");
        Assertions.assertThat(lookupTransformation.findMultipleInputMatch(remainingInput3, false)).isEqualTo("25.36+37.00*25.36€");
    }
}