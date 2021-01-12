package model.sql;

import main.transformer.model.sql.Database;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

class DatabaseTest {
    @Test
    void getTables() throws Exception {
        try (final Database database = new Database("src/test/resources/data.sql")) {
            Assertions.assertThat(database.getTables()).containsExactlyInAnyOrder("CUSTDATA", "SALE");
        }
    }

    @Test
    void getTableColumns() throws Exception {
        try (final Database database = new Database("src/test/resources/data.sql")) {
            Assertions.assertThat(database.getColumnNames("CUSTDATA")).containsExactlyInAnyOrder("NAME", "ADDR", "ST");
            Assertions.assertThat(database.getColumnNames("SALE")).containsExactlyInAnyOrder("ADDR", "ST", "SOMEDATE", "PRICE");
        }
    }
}