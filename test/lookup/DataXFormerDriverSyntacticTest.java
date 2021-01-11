package lookup;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static main.dataXFormerDriver.getSyntacticalMapping;
import static main.dataXFormerDriver.prepareInputForDataXFormer;

class DataXFormerDriverSyntacticTest {

    @Test
    void getSyntacticalMappingWithSingleOutputTest() throws Exception {

        Map<String,String> syntacticalMappings =  getSyntacticalMapping("test/resources/inputSyntacticMapping/Benchmark_funct_comp_address_state.csv");
        Assertions.assertThat(syntacticalMappings.size()).isEqualTo(13);

        Map<String,String> syntacticalMappingsDenonym =  getSyntacticalMapping("test/resources/inputSyntacticMapping/Benchmark_prettycleaned_denonyms.csv");
        Assertions.assertThat(syntacticalMappingsDenonym.size()).isEqualTo(4);
    }

    @Test
    void getSyntacticalMappingWithMultipleOutputTest() throws Exception {

        Map<String,String> syntacticalMappingsHeader =  getSyntacticalMapping("test/resources/inputSyntacticMapping/CountryToCapitalWithDifferentHeader.csv");
        Assertions.assertThat(syntacticalMappingsHeader.size()).isEqualTo(2);

        Map<String,String> syntacticalMappingsConcatenated =  getSyntacticalMapping("test/resources/inputSyntacticMapping/CountryToCapitalWithMyApproach.csv");
        Assertions.assertThat(syntacticalMappingsConcatenated.size()).isEqualTo(2);
    }

    @Test
    void getSyntacticalMappingWithSingleInputOutputTest() throws Exception {

        Map<String,String> syntacticalMappingsHeader =  getSyntacticalMapping("test/resources/inputSyntacticMapping/BenchmarkCUSIPToTicker.csv");
        Assertions.assertThat(syntacticalMappingsHeader.size()).isEqualTo(2);

    }

    @Test
    void prepareInputForDataXFormerTest() throws Exception {

        String syntacticalMappingsHeader =  prepareInputForDataXFormer("test/resources/inputSyntacticMapping/CountryToCapitalWithDifferentHeader.csv");
        Assertions.assertThat(syntacticalMappingsHeader.isEmpty()).isEqualTo(false);
    }


}
