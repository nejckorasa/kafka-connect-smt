package io.github.nejckorasa.kafka.connect.smt.transforms;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.Map;
import java.util.UUID;

public class TestRecordFactory {

    private static final ObjectMapper json = new ObjectMapper();

    public static SourceRecord someRecord() {
        return new SourceRecord(null, null, "test", 0, null, Map.of("someKey", 42L, "anotherKey", UUID.randomUUID()));
    }

    public static SourceRecord someRecordWithStringHeader(String headerKey, String headerValue) {
        SourceRecord record = someRecord();
        record.headers().addString(headerKey, headerValue);
        return record;
    }

    public static SourceRecord someRecordWithStringJsonHeader(String headerKey, Object headerValue) throws JsonProcessingException {
        SourceRecord record = someRecord();
        record.headers().addString(headerKey, json.writeValueAsString(headerValue));
        return record;
    }

    public static SourceRecord someRecordWithBytesJsonHeader(String headerKey, Object headerValue) throws JsonProcessingException {
        SourceRecord record = someRecord();
        record.headers().addBytes(headerKey, json.writeValueAsBytes(headerValue));
        return record;
    }
}
