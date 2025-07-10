package com.boarhat;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;

import javax.sql.DataSource;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class SelectTest {
    private static JdbcTemplate jdbcTemplate;

    @BeforeAll
    static void beforeAll() {
        DataSource ds = new EmbeddedDatabaseBuilder()
                .setType(EmbeddedDatabaseType.H2)
                .addScript("classpath:select/schema.sql")
                .addScript("classpath:select/data.sql")
                .build();
        jdbcTemplate = new JdbcTemplate(ds);
    }

    @Test
    void select_test() {
        String sql = """
                SELECT * FROM client
                """;
        List<Map<String, Object>> clients = jdbcTemplate.queryForList(sql);
        assertThat(clients).hasSize(10)
                .containsExactlyInAnyOrder(
                        Map.of("ID", 1, "FIRST_NAME", "John", "LAST_NAME", "Doe", "CITY", "Los Angeles"),
                        Map.of("ID", 2, "FIRST_NAME", "Jane", "LAST_NAME", "Smith", "CITY", "Los Angeles"),
                        Map.of("ID", 3, "FIRST_NAME", "Alice", "LAST_NAME", "Johnson", "CITY", "Chicago"),
                        Map.of("ID", 4, "FIRST_NAME", "Bob", "LAST_NAME", "Brown", "CITY", "Houston"),
                        Map.of("ID", 5, "FIRST_NAME", "Charlie", "LAST_NAME", "Davis", "CITY", "Phoenix"),
                        Map.of("ID", 6, "FIRST_NAME", "Eve", "LAST_NAME", "Wilson", "CITY", "Philadelphia"),
                        Map.of("ID", 7, "FIRST_NAME", "Frank", "LAST_NAME", "Garcia", "CITY", "San Antonio"),
                        Map.of("ID", 8, "FIRST_NAME", "Grace", "LAST_NAME", "Martinez", "CITY", "San Diego"),
                        Map.of("ID", 9, "FIRST_NAME", "Hank", "LAST_NAME", "Lopez", "CITY", "Dallas"),
                        Map.of("ID", 10, "FIRST_NAME", "Ivy", "LAST_NAME", "Gonzalez", "CITY", "Philadelphia"));
    }

    @Test
    void select_specific_columns_test() {
        String sql = """
                SELECT FIRST_NAME, LAST_NAME FROM client
                """;
        List<Map<String, Object>> clients = jdbcTemplate.queryForList(sql);
        assertThat(clients).hasSize(10)
                .containsExactlyInAnyOrder(
                        Map.of("FIRST_NAME", "John", "LAST_NAME", "Doe"),
                        Map.of("FIRST_NAME", "Jane", "LAST_NAME", "Smith"),
                        Map.of("FIRST_NAME", "Alice", "LAST_NAME", "Johnson"),
                        Map.of("FIRST_NAME", "Bob", "LAST_NAME", "Brown"),
                        Map.of("FIRST_NAME", "Charlie", "LAST_NAME", "Davis"),
                        Map.of("FIRST_NAME", "Eve", "LAST_NAME", "Wilson"),
                        Map.of("FIRST_NAME", "Frank", "LAST_NAME", "Garcia"),
                        Map.of("FIRST_NAME", "Grace", "LAST_NAME", "Martinez"),
                        Map.of("FIRST_NAME", "Hank", "LAST_NAME", "Lopez"),
                        Map.of("FIRST_NAME", "Ivy", "LAST_NAME", "Gonzalez"));
    }

    @Test
    void select_distinct_test() {
        String sql = """
                SELECT DISTINCT CITY FROM client
                """;
        List<Map<String, Object>> cities = jdbcTemplate.queryForList(sql);
        assertThat(cities).hasSize(8)
                .containsExactlyInAnyOrder(
                        Map.of("CITY", "Los Angeles"),
                        Map.of("CITY", "Chicago"),
                        Map.of("CITY", "Houston"),
                        Map.of("CITY", "Phoenix"),
                        Map.of("CITY", "Philadelphia"),
                        Map.of("CITY", "San Antonio"),
                        Map.of("CITY", "San Diego"),
                        Map.of("CITY", "Dallas"));
    }
}
