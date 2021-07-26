package io.github.nejckorasa.kafka.connect.smt.transforms;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptyMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static io.github.nejckorasa.kafka.connect.smt.transforms.TestRecordFactory.*;

class MapHeaderTest {

    private final MapHeader<SourceRecord> mapHeader = new MapHeader<>();

    @AfterEach
    public void tearDown() {
        mapHeader.close();
    }

    @Test
    void transformsStringHeader() throws JsonProcessingException {
        mapHeader.configure(Map.of("header.name", "myHeaders"));

        final SourceRecord record = someRecordWithStringJsonHeader("myHeaders", Map.of("header1", "value1", "header2", "value2"));

        final SourceRecord transformedRecord = mapHeader.apply(record);
        assertEquals(record.value(), transformedRecord.value());
        assertEquals("value1", transformedRecord.headers().lastWithName("header1").value());
        assertEquals("value2", transformedRecord.headers().lastWithName("header2").value());
        assertNull(transformedRecord.headers().lastWithName("myHeaders"));
    }

    @Test
    void transformsBytesHeader() throws JsonProcessingException {
        mapHeader.configure(Map.of("header.name", "headers_json_map"));

        final SourceRecord record = someRecordWithBytesJsonHeader("headers_json_map", Map.of("header1", "value1", "header2", "value2"));

        final SourceRecord transformedRecord = mapHeader.apply(record);
        assertEquals(record.value(), transformedRecord.value());
        assertEquals("value1", transformedRecord.headers().lastWithName("header1").value());
        assertEquals("value2", transformedRecord.headers().lastWithName("header2").value());
        assertNull(transformedRecord.headers().lastWithName("headers_json_map"));
    }

    @Test
    void transformsHeadersWithDefaultFieldName() throws JsonProcessingException {
        mapHeader.configure(emptyMap());

        final SourceRecord record = someRecordWithStringJsonHeader("headers", Map.of("header1", "value1", "header2", "value2"));

        final SourceRecord transformedRecord = mapHeader.apply(record);
        assertEquals(record.value(), transformedRecord.value());
        assertEquals("value1", transformedRecord.headers().lastWithName("header1").value());
        assertEquals("value2", transformedRecord.headers().lastWithName("header2").value());
        assertNull(transformedRecord.headers().lastWithName("headers"));
    }

    @Test
    void skipsRecordTransformationOnMissingHeaderField() {
        mapHeader.configure(emptyMap());

        final SourceRecord record = someRecord();

        final SourceRecord transformedRecord = mapHeader.apply(record);
        assertEquals(record.value(), transformedRecord.value());
        assertNull(transformedRecord.headers().lastWithName("headers"));
    }

    @Test
    void skipsRecordTransformationOnUnexpectedStringHeaderValue() throws JsonProcessingException {
        mapHeader.configure(emptyMap());

        final SourceRecord record = someRecordWithStringJsonHeader("headers", Set.of("1", "2", "3"));

        final SourceRecord transformedRecord = mapHeader.apply(record);
        assertEquals(record.value(), transformedRecord.value());
        assertNull(transformedRecord.headers().lastWithName("headers"));
    }

    @Test
    void skipsRecordTransformationOnUnexpectedBytesHeaderValue() throws JsonProcessingException {
        mapHeader.configure(Map.of());

        final SourceRecord record = someRecordWithBytesJsonHeader("headers", Set.of("1", "2", "3"));

        final SourceRecord transformedRecord = mapHeader.apply(record);
        assertEquals(record.value(), transformedRecord.value());
        assertNull(transformedRecord.headers().lastWithName("headers"));
    }
}
