package lookup;

import main.dataXFormerDriver;
import org.junit.jupiter.api.Test;

public class BenchmarkTest {

    @Test
    void testColorNumberToColorCode() throws Exception {

        // prepare everything here like create a temp file
        String[] arguments = new String[4];
        arguments[0] = "./benchmarkForReport/experiment/Benchmark_color_number_to_color_code.csv";
        arguments[1] = "0";
        arguments[2] = "1";
        arguments[3] = "NSF";
        dataXFormerDriver.main(arguments);
        System.gc();
    }

    @Test
    void testMbToGb() throws Exception {

        String[] arguments = new String[4];
        arguments[0] = "./benchmarkForReport/experiment/Benchmark_convert_mb_to_gb.csv";
        arguments[1] = "0";
        arguments[2] = "1";
        arguments[3] = "S";
        dataXFormerDriver.main(arguments);
        System.gc();
    }

    @Test
    void testMonthNumberToMonthName() throws Exception {

        String[] arguments = new String[4];
        arguments[0] = "./benchmarkForReport/experiment/Benchmark_convert_month_number_to_month_name.csv";
        arguments[1] = "0";
        arguments[2] = "1";
        arguments[3] = "S";
        dataXFormerDriver.main(arguments);
        System.gc();
    }

    @Test
    void testRegularTimeToMilitaryTime() throws Exception {

        String[] arguments = new String[4];
        arguments[0] = "./benchmarkForReport/experiment/benchmark_convert_regular_time_to_military_time.csv";
        arguments[1] = "0";
        arguments[2] = "1";
        arguments[3] = "NSF";
        dataXFormerDriver.main(arguments);
        System.gc();
    }

    @Test
    void testyyyymmddToDatetime() throws Exception {

        String[] arguments = new String[4];
        arguments[0] = "./benchmarkForReport/experiment/Benchmark_convert_yyyymmdd_to_datetime.csv";
        arguments[1] = "0";
        arguments[2] = "1";
        arguments[3] = "S";
        dataXFormerDriver.main(arguments);
        System.gc();
    }

    @Test
    void testCookiesToDomainName() throws Exception {

        String[] arguments = new String[4];
        arguments[0] = "./benchmarkForReport/experiment/Benchmark_cookies_to_domain_name.csv";
        arguments[1] = "0";
        arguments[2] = "1";
        arguments[3] = "S";
        dataXFormerDriver.main(arguments);
        System.gc();
    }

    @Test
    void testElement2BP() throws Exception {

        String[] arguments = new String[4];
        arguments[0] = "./benchmarkForReport/experiment/Benchmark_element2BP.csv";
        arguments[1] = "0";
        arguments[2] = "1";
        arguments[3] = "SFU";
        dataXFormerDriver.main(arguments);
        System.gc();
    }

    @Test
    void testExtractMonthFromDatetime() throws Exception {

        String[] arguments = new String[4];
        arguments[0] = "./benchmarkForReport/experiment/Benchmark_Extract_month_from_datetime.csv";
        arguments[1] = "0";
        arguments[2] = "1";
        arguments[3] = "S";
        dataXFormerDriver.main(arguments);
        System.gc();
    }

    @Test
    void testCompAddressToState() throws Exception {

        String[] arguments = new String[4];
        arguments[0] = "./benchmarkForReport/experiment/Benchmark_funct_comp_address_state.csv";
        arguments[1] = "0";
        arguments[2] = "1";
        arguments[3] = "SF";
        dataXFormerDriver.main(arguments);
        System.gc();
    }

    @Test
    void testISBNNumberSplitter() throws Exception {

        String[] arguments = new String[4];
        arguments[0] = "./benchmarkForReport/experiment/Benchmark_ISBN_Number_Siplitter.csv";
        arguments[1] = "0";
        arguments[2] = "1";
        arguments[3] = "S";
        dataXFormerDriver.main(arguments);
        System.gc();
    }

    @Test
    void testMEMEToFilename() throws Exception {
        // FIX ME
        String[] arguments = new String[4];
        arguments[0] = "./benchmarkForReport/experiment/Benchmark_MEME_to_Filename.csv";
        arguments[1] = "0";
        arguments[2] = "1";
        arguments[3] = "NSF";
        dataXFormerDriver.main(arguments);
        System.gc();
    }

    @Test
    void testNumericPadding() throws Exception {

        String[] arguments = new String[4];
        arguments[0] = "./benchmarkForReport/experiment/Benchmark_numeric_padding.csv";
        arguments[1] = "0";
        arguments[2] = "1";
        arguments[3] = "S";
        dataXFormerDriver.main(arguments);
        System.gc();
    }

    @Test
    void testConcatenatedDenonyms() throws Exception {

        String[] arguments = new String[4];
        arguments[0] = "./benchmarkForReport/experiment/Benchmark_prettycleaned_denonyms.csv";
        arguments[1] = "0";
        arguments[2] = "1";
        arguments[3] = "SFU";
        dataXFormerDriver.main(arguments);
        System.gc();
    }


    @Test
    void testProductToCompany() throws Exception {

        String[] arguments = new String[4];
        arguments[0] = "./benchmarkForReport/experiment/Benchmark_product2Company.csv";
        arguments[1] = "0";
        arguments[2] = "1";
        arguments[3] = "SF";
        dataXFormerDriver.main(arguments);
        System.gc();
    }

    @Test
    void testStringUpperCasing() throws Exception {

        String[] arguments = new String[4];
        arguments[0] = "./benchmarkForReport/experiment/Benchmark_string_uppercaseing.csv";
        arguments[1] = "0";
        arguments[2] = "1";
        arguments[3] = "S";
        dataXFormerDriver.main(arguments);
        System.gc();
    }

    @Test
    void testTimeSpanToHrsMinsSecs() throws Exception {

        String[] arguments = new String[4];
        arguments[0] = "./benchmarkForReport/experiment/Benchmark_time_span_to_hrs_mins_secs.csv";
        arguments[1] = "0";
        arguments[2] = "1";
        arguments[3] = "S";
        dataXFormerDriver.main(arguments);
        System.gc();
    }

    @Test
    void testCUSIPToTicker() throws Exception {

        String[] arguments = new String[4];
        arguments[0] = "./benchmarkForReport/experiment/BenchmarkCUSIPToTicker.csv";
        arguments[1] = "0";
        arguments[2] = "1";
        arguments[3] = "NSF";
        dataXFormerDriver.main(arguments);
        System.gc();
    }

    @Test
    void testCUSIPToTickerIndirect() throws Exception {

        String[] arguments = new String[4];
        arguments[0] = "./benchmarkForReport/experiment/BenchmarkCUSIPToTicker.csv";
        arguments[1] = "0";
        arguments[2] = "1";
        arguments[3] = "SI";
        dataXFormerDriver.main(arguments);
        System.gc();
    }

    @Test
    void testConcatenatedCountryNamesToCapitals() throws Exception {

        String[] arguments = new String[4];
        arguments[0] = "./benchmarkForReport/experiment/concatenated_country_names_to_capitals.csv";
        arguments[1] = "0";
        arguments[2] = "1";
        arguments[3] = "SFU";
        dataXFormerDriver.main(arguments);
        System.gc();
    }

    @Test
    void testConcatenatedCountryCapitalsWithDiffHeaders() throws Exception {

        // prepare everything here like create a temp file
        String[] arguments = new String[4];
        arguments[0] = "./benchmarkForReport/experiment/CountryToCapitalWithDifferentHeader.csv";
        arguments[1] = "0";
        arguments[2] = "1";
        arguments[3] = "SF";
        dataXFormerDriver.main(arguments);
        System.gc();
    }
}
