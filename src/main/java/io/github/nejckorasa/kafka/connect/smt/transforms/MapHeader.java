package io.github.nejckorasa.kafka.connect.smt.transforms;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

import static java.util.Collections.emptyMap;

public class MapHeader<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String OVERVIEW_DOC = "Split a composite header into separate headers."
            + "<p/> Composite Header must be a map and stored in json format, every map entry will add a new header with key as header name and value as header value."
            + "<p/> Use the transformation type (<code>" + MapHeader.class.getName() + "</code>).";

    private interface ConfigName {
        String COMPOSITE_HEADER_NAME = "header.name";
    }

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ConfigName.COMPOSITE_HEADER_NAME, ConfigDef.Type.STRING, "headers",
                    ConfigDef.Importance.HIGH,
                    "Composite header name");

    private static final Logger log = LoggerFactory.getLogger(MapHeader.class);
    public static final ObjectMapper json = new ObjectMapper();

    private String compositeHeaderName;

    @Override
    public R apply(R record) {
        Headers headers = record.headers();

        Optional.ofNullable(headers.lastWithName(compositeHeaderName))
                .map(this::deserializeCompositeHeader)
                .ifPresent(newHeaders -> newHeaders.forEach((k, v) -> headers.addString(k, v.toString())));

        headers.remove(compositeHeaderName);

        return record.newRecord(
                record.topic(), null,
                record.keySchema(), record.key(),
                record.valueSchema(), record.value(),
                record.timestamp(), headers
        );
    }

    private Map<String, Object> deserializeCompositeHeader(Header header) {
        Map<String, Object> newHeaders = emptyMap();
        if (Schema.Type.STRING == header.schema().type()) {
            try {
                //noinspection unchecked
                newHeaders = (Map<String, Object>) json.readValue((String) header.value(), Map.class);
            } catch (JsonProcessingException e) {
                log.error("Failed to deserialize STRING header", e);
            }
        } else if (Schema.Type.BYTES == header.schema().type()) {
            try {
                //noinspection unchecked
                newHeaders = (Map<String, Object>) json.readValue((byte[]) header.value(), Map.class);
            } catch (IOException e) {
                log.error("Failed to deserialize BYTES header", e);
            }
        }
        return newHeaders;
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
        compositeHeaderName = config.getString(ConfigName.COMPOSITE_HEADER_NAME);
        log.info("MapHeader configured: {}", this);
    }

    @Override
    public void close() {

    }

    @Override
    public String toString() {
        return "MapHeader{" +
                "compositeHeaderName='" + compositeHeaderName + '\'' +
                '}';
    }
}
