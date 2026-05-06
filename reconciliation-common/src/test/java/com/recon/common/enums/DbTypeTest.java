package com.recon.common.enums;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class DbTypeTest {

    @Test
    void shouldHaveTwoValues() {
        assertThat(DbType.values()).containsExactly(DbType.MONGODB, DbType.POSTGRESQL);
    }

    @Test
    void shouldParseFromString() {
        assertThat(DbType.valueOf("MONGODB")).isEqualTo(DbType.MONGODB);
        assertThat(DbType.valueOf("POSTGRESQL")).isEqualTo(DbType.POSTGRESQL);
    }
}
