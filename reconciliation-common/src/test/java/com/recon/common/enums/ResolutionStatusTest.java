package com.recon.common.enums;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ResolutionStatusTest {

    @Test
    void shouldHaveFourValues() {
        assertThat(ResolutionStatus.values()).containsExactly(
                ResolutionStatus.AUTO_RESOLVED,
                ResolutionStatus.MANUAL_REQUIRED,
                ResolutionStatus.SKIPPED,
                ResolutionStatus.ERROR
        );
    }
}
