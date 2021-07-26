package io.github.nejckorasa.kafka.connect.smt.transforms;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class FilterOnMatchingHeaderTest {

    private final FilterOnMatchingHeader<SourceRecord> filter = new FilterOnMatchingHeader<>();

    @AfterEach
    public void tearDown() {
        filter.close();
    }

    @Test
    void excludeFilterRetainsRecordWhenNoHeaders() {
        filter.configure(Map.of("name", "myFilterHeader", "value", "requiredHeaderValue"));

        final SourceRecord record = TestRecordFactory.someRecord();

        SourceRecord filteredRecord = filter.apply(record);
        assertNotNull(filteredRecord);
        assertEquals(record, filteredRecord);
    }

    @Test
    void includeFilterDropsRecordWhenNoHeaders() {
        filter.configure(Map.of("name", "myFilterHeader", "value", "requiredHeaderValue", "filter.type", "include"));

        final SourceRecord record = TestRecordFactory.someRecord();

        assertNull(filter.apply(record));
    }

    @Test
    void excludeFilterRetainsRecordOnMissingHeader() {
        filter.configure(Map.of("name", "myFilterHeader", "value", "requiredHeaderValue"));

        final SourceRecord record = TestRecordFactory.someRecord();

        SourceRecord filteredRecord = filter.apply(record);
        assertNotNull(filteredRecord);
        assertEquals(record, filteredRecord);
    }

    @Test
    void includeFilterDropsRecordOnMissingHeader() {
        filter.configure(Map.of("name", "myFilterHeader", "value", "requiredHeaderValue", "filter.type", "include"));

        final SourceRecord record = TestRecordFactory.someRecord();

        assertNull(filter.apply(record));
    }

    @Test
    void excludeFilterRetainsRecordOnNotMatchingHeader() {
        filter.configure(Map.of("name", "myFilterHeader", "value", "requiredHeaderValue"));

        final SourceRecord record = TestRecordFactory.someRecordWithStringHeader("myFilterHeader", "otherValue");

        SourceRecord filteredRecord = filter.apply(record);
        assertNotNull(filteredRecord);
        assertEquals(record, filteredRecord);
    }

    @Test
    void includeFilterDropsRecordOnNotMatchingHeader() {
        filter.configure(Map.of("name", "myFilterHeader", "value", "requiredHeaderValue", "filter.type", "include"));

        final SourceRecord record = TestRecordFactory.someRecordWithStringHeader("myFilterHeader", "otherValue");

        assertNull(filter.apply(record));
    }

    @Test
    void excludeFilterDropsRecordOnMatchingHeader() {
        filter.configure(Map.of("name", "myFilterHeader", "value", "requiredHeaderValue"));

        final SourceRecord record = TestRecordFactory.someRecordWithStringHeader("myFilterHeader", "requiredHeaderValue");

        SourceRecord filteredRecord = filter.apply(record);
        assertNull(filteredRecord);
    }

    @Test
    void includeFilterRetainsRecordOnMatchingHeader() {
        filter.configure(Map.of("name", "myFilterHeader", "value", "requiredHeaderValue", "filter.type", "include"));

        final SourceRecord record = TestRecordFactory.someRecordWithStringHeader("myFilterHeader", "requiredHeaderValue");

        SourceRecord filteredRecord = filter.apply(record);
        assertNotNull(filteredRecord);
        assertEquals(record, filteredRecord);
    }
}