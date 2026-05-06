package com.recon.common.enums;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class OperationTypeTest {

    @Test
    void shouldHaveFourValues() {
        assertThat(OperationType.values()).containsExactly(
                OperationType.INSERT,
                OperationType.UPDATE,
                OperationType.DELETE,
                OperationType.REPLACE
        );
    }
}
