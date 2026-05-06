package com.recon.engine.flink.process;

import com.recon.common.model.ChangeEvent;
import org.bson.Document;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("SourceClusterInjector")
class SourceClusterInjectorTest {

    private final SourceClusterInjector injector = new SourceClusterInjector("gcp");

    @Test
    void shouldInjectSourceClusterIntoDocument() {
        var event = ChangeEvent.builder().documentKey("doc1").build();
        var doc = new Document("name", "test");

        var result = injector.inject(doc, event);

        assertThat(result.getString("sourceCluster")).isEqualTo("gcp");
        assertThat(result.getString("name")).isEqualTo("test");
    }

    @Test
    void shouldOverwriteExistingSourceCluster() {
        var event = ChangeEvent.builder().build();
        var doc = new Document("sourceCluster", "hic");

        var result = injector.inject(doc, event);

        assertThat(result.getString("sourceCluster")).isEqualTo("gcp");
    }

    @Test
    void shouldHandleNullDocument() {
        var result = injector.inject(null, null);
        assertThat(result).isNull();
    }

    @Test
    void shouldInjectIntoMapDocument() {
        Map<String, Object> doc = new HashMap<>();
        doc.put("name", "test");

        var result = injector.injectMap(doc);

        assertThat(result).containsEntry("sourceCluster", "gcp");
        assertThat(result).containsEntry("name", "test");
    }

    @Test
    void shouldHandleNullMap() {
        assertThat(injector.injectMap(null)).isNull();
    }

    @Test
    void shouldReturnSourceClusterLabel() {
        assertThat(injector.getSourceClusterLabel()).isEqualTo("gcp");
    }

    @Test
    void shouldUseProvidedLabel() {
        var hicInjector = new SourceClusterInjector("hic");
        var doc = new Document();
        var result = hicInjector.inject(doc, null);
        assertThat(result.getString("sourceCluster")).isEqualTo("hic");
    }
}
