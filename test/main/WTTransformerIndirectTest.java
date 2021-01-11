package main;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.Set;


class WTTransformerIndirectTest {

    @Test
    void getColumnStringsTest() throws IOException {

        WTTransformerIndirect wtTransformerIndirect = new WTTransformerIndirect();

        File dir = new File("./test/resources/indirectColumnCheck/tablesToCheck");
        String tableId = "139899189";
        String tableFull = "(139899189, [8], [9], 0)";

        Set<String> columns =  wtTransformerIndirect.getColumnStrings(dir,tableId,tableFull);
        Assertions.assertThat(columns.size()).isEqualTo(6);

        File expected = wtTransformerIndirect.getIndirectNames(dir,columns,tableFull,tableId);
        Assertions.assertThat(expected.toString().contains(dir.toString())).isEqualTo(true);

    }
}
