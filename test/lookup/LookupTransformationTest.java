package lookup;

import main.transformer.lookup.LookupTransformation;
import main.transformer.lookup.bean.CandidateKey;
import main.transformer.model.csv.CsvLookupSource;
import main.transformer.precisionRecall.Indirect;
import main.transformer.util.LevenshteinDistanceStrategy;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.*;

class LookupTransformationTest {

    @Test
    void checkFDs() throws Exception {
       CsvLookupSource csv = new  CsvLookupSource("test/resources/dataWithOneIntermediateTable.csv");
       System.out.println(csv.findFDs(csv.getTableList().get(0)));
        Assertions.assertThat(csv.findFDs(csv.getTableList().get(0)).toString()).isEqualToIgnoringCase("[[0]->[1], [0]->[2], [1]->[0], [1]->[2], [2]->[0], [2]->[1]]");
    }

    @Test
    void findCandidateKeysTest() throws Exception {
        CsvLookupSource csv = new  CsvLookupSource("test/resources/candidateKey/candidateKeyTest.csv");
        System.out.println(csv.findCandidateKeys());
        //Assertions.assertThat(csv.findFDs(csv.getTableList().get(0)).toString()).isEqualToIgnoringCase("[[0]->[1], [0]->[2], [1]->[0], [1]->[2], [2]->[0], [2]->[1]]");
    }


    @Test
    void checkEditDistance() throws Exception {
        LevenshteinDistanceStrategy strategy = new LevenshteinDistanceStrategy();
        System.out.println(strategy.score("book", "back"));
    }

    @Test
    void testLikeInTheArticleWithCsv() throws Exception {

        LookupTransformation lookupTransformation = new LookupTransformation(new CsvLookupSource("test/resources/table1.csv", "test/resources/table2.csv"));

        lookupTransformation.provideExample("Peter Shaw", "110");
        Assertions.assertThat(lookupTransformation.findMatch("Gary Limb", false)).isEqualTo("225");
        Assertions.assertThat(lookupTransformation.findMatch("Mike Henry", false)).isEqualTo("2015");
        Assertions.assertThat(lookupTransformation.findMatch("Sean Riley", false)).isEqualTo("495");

    }

    @Test
    void testWithOneIntermediateTableCsv() throws Exception {

        LookupTransformation lookupTransformation = new LookupTransformation(new CsvLookupSource("test/resources/dataWithOneIntermediateTable.csv"));

        lookupTransformation.provideExample("044-58-3429", "Steve Russell");
        Assertions.assertThat(lookupTransformation.findMatch("018-45-8949", false)).isEqualTo("Ian Jordan");
        Assertions.assertThat(lookupTransformation.findMatch("023-34-3254", false)).isEqualTo("Mary Dina");
    }

    @Test
    @Disabled
    void testWithOneTableCsvForDataXFormer() throws Exception {

        LookupTransformation lookupTransformation = new LookupTransformation(new CsvLookupSource("test/resources/countryCapitalToLanguage_table1.csv"));

        List<String> inputsExample = new ArrayList<>();
        inputsExample.add("London");
        inputsExample.add("England");
        lookupTransformation.provideMultipleInputsExample(inputsExample, "English");
        Assertions.assertThat(lookupTransformation.findMatch("Amsterdam Netherlands", false)).isEqualTo("Dutch");
        Assertions.assertThat(lookupTransformation.findMatch("Copenhagen Denmark", false)).isEqualTo("Danish");
    }

    @Test
    @Disabled
    void testWithTwoIntermediateTableCsvThesisReport() throws Exception {

        LookupTransformation lookupTransformation = new LookupTransformation(new CsvLookupSource("test/resources/dataWithTwoIntermediateTable_table_1.csv", "test/resources/dataWithTwoIntermediateTable_table_2.csv"));

        lookupTransformation.provideExample("California Institute of Technology", "California");
        lookupTransformation.provideExample("Technical University of Berlin", "Berlin");
        Assertions.assertThat(lookupTransformation.findMatch("Harvard University", false)).isEqualTo("Massachusetts");
    }

    @Test
    @Disabled
    void testWithTwoIntermediateTableCsv() throws Exception {

        LookupTransformation lookupTransformation = new LookupTransformation(new CsvLookupSource("test/resources/dataWithTwoIntermediateTable_table_1.csv", "test/resources/dataWithTwoIntermediateTable_table_2.csv"));

        lookupTransformation.provideExample("Stroller", "$145.67");
        Assertions.assertThat(lookupTransformation.findMatch("Aspirator", false)).isEqualTo("$2.56");
        Assertions.assertThat(lookupTransformation.findMatch("Wipes", false)).isEqualTo("$5.12");
        Assertions.assertThat(lookupTransformation.findMatch("Bib", false)).isEqualTo("$3.56");
    }

    @Test
    void testScalingLookupTransfCsv() throws Exception {

        Long startTime = System.currentTimeMillis();
        LookupTransformation lookupTransformation = new LookupTransformation(
                new CsvLookupSource("test/resources/testLookupWithManyCsvFiles/4.csv",
                        "test/resources/testLookupWithManyCsvFiles/1.csv",
                        "test/resources/testLookupWithManyCsvFiles/2.csv",
                        "test/resources/testLookupWithManyCsvFiles/3.csv"
                )
        );

        lookupTransformation.provideExample("23135106", "AMZN");
        lookupTransformation.provideExample("594918104", "MSFT");
        lookupTransformation.provideExample("37045V100", "GM");

        Assertions.assertThat(lookupTransformation.findMatch("459200101", false)).isEqualTo("IBM");
        Assertions.assertThat(lookupTransformation.findMatch("60505104", false)).isEqualTo("BAC");
        Assertions.assertThat(lookupTransformation.findMatch("369604103", false)).isEqualTo("GE");
        Assertions.assertThat(lookupTransformation.findMatch("20030N101", false)).isEqualTo("CMCSA");

        Assertions.assertThat(lookupTransformation.findMatch("803054204", false)).isEqualTo("SAP");
        Long endTime = System.currentTimeMillis();
        System.out.println("Runtime: " + (endTime - startTime));

    }

    @Test
    void testScalingLookupTransfCsvShouldPassSinceWeProvideExactSameDataInDifferentOrder() throws Exception {

        Long startTime = System.currentTimeMillis();
        LookupTransformation lookupTransformation = new LookupTransformation(
                new CsvLookupSource(
                        "test/resources/testLookupWithManyCsvFiles/94869.csv",
                        "test/resources/testLookupWithManyCsvFiles/45968.csv",
                        "test/resources/testLookupWithManyCsvFiles/45689.csv",
                        "test/resources/testLookupWithManyCsvFiles/68.csv"
                )
        );

        lookupTransformation.provideExample("37045V100", "GM");
        lookupTransformation.provideExample("594918104", "MSFT");
        lookupTransformation.provideExample("23135106", "AMZN");

        Assertions.assertThat(lookupTransformation.findMatch("459200101", false)).isEqualTo("IBM");
        Assertions.assertThat(lookupTransformation.findMatch("60505104", false)).isEqualTo("BAC");
        Assertions.assertThat(lookupTransformation.findMatch("369604103", false)).isEqualTo("GE");
        Assertions.assertThat(lookupTransformation.findMatch("20030N101", false)).isEqualTo("CMCSA");

        Assertions.assertThat(lookupTransformation.findMatch("803054204", false)).isEqualTo("SAP");
        Long endTime = System.currentTimeMillis();
        System.out.println("Runtime: " + (endTime - startTime));

    }

    @Test
        // @Disabled
    void testScalingLookupTransfProvidedExampleOrderChangedCsv() throws Exception {

        Long startTime = System.currentTimeMillis();
        LookupTransformation lookupTransformation = new LookupTransformation(
                new CsvLookupSource(
                        "test/resources/testLookupWithManyCsvFiles/1.csv",
                        "test/resources/testLookupWithManyCsvFiles/2.csv",
                        "test/resources/testLookupWithManyCsvFiles/3.csv",
                        "test/resources/testLookupWithManyCsvFiles/4.csv"
                )
        );

        lookupTransformation.provideExample("37045V100", "GM");
        lookupTransformation.provideExample("594918104", "MSFT");
        lookupTransformation.provideExample("23135106", "AMZN");
        Assertions.assertThat(lookupTransformation.findMatch("459200101", false)).isEqualTo("IBM");
        Assertions.assertThat(lookupTransformation.findMatch("60505104",false)).isEqualTo("BAC");
        Assertions.assertThat(lookupTransformation.findMatch("369604103", false)).isEqualTo("GE");
        Assertions.assertThat(lookupTransformation.findMatch("20030N101", false)).isEqualTo("CMCSA");
        Assertions.assertThat(lookupTransformation.findMatch("803054204", false)).isEqualTo("SAP");
        Long endTime = System.currentTimeMillis();
        System.out.println("Runtime: " + (endTime - startTime));

    }

    @Test
        // @Disabled
    void testWithGivenCandidateKeysFunctionalTransf() throws Exception {

        Long startTime = System.currentTimeMillis();

        LookupTransformation lookupTransformation = new LookupTransformation(
                new CsvLookupSource(
                        "test/resources/countryToCapital/28300040.csv"
                )
        );
        CandidateKey ck = new CandidateKey("28300040.csv", "28300040.csv-0");
        CandidateKey ck1 = new CandidateKey("28300040.csv", "28300040.csv-4");
        Set<CandidateKey> candidateKeys = new HashSet<>();
        candidateKeys.add(ck);
        candidateKeys.add(ck1);

        lookupTransformation.provideMultipleInputsExampleWithCandidateKeys(Collections.singletonList("Afghanistan"),
                "Kabul", candidateKeys);
        lookupTransformation.provideMultipleInputsExampleWithCandidateKeys(Collections.singletonList("Albania"),
                "Tirane", candidateKeys);
        lookupTransformation.provideMultipleInputsExampleWithCandidateKeys(Collections.singletonList("Algeria"),
                "Algiers", candidateKeys);

        Assertions.assertThat(lookupTransformation.findMatch("Germany", false)).isEqualTo("Berlin");
        Assertions.assertThat(lookupTransformation.findMatch("Hungary",false)).isEqualTo("Budapest");
        Assertions.assertThat(lookupTransformation.findMatch("Italy", false)).isEqualTo("Rome");
        Assertions.assertThat(lookupTransformation.findMatch("Turkey", false)).isEqualTo("Ankara");
        Assertions.assertThat(lookupTransformation.findMatch("Vietnam", false)).isEqualTo("Hanoi");

        Long endTime = System.currentTimeMillis();
        System.out.println("Runtime: " + (endTime - startTime));

    }

    @Test
    void createCandidateKeys() {
        Assertions.assertThat(Indirect.findCandidateKeys("test/resources/candidateKey").size()).isEqualTo(6);
    }
}