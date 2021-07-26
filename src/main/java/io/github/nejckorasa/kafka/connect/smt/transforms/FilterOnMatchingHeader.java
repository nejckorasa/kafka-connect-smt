package io.github.nejckorasa.kafka.connect.smt.transforms;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.NonNullValidator;
import org.apache.kafka.common.config.ConfigDef.ValidString;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;

public class FilterOnMatchingHeader<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String OVERVIEW_DOC = "Includes or drops records with at least one header with the configured name that matches the configured value.";

    private interface ConfigName {
        String TYPE_CONFIG = "filter.type";
        String NAME_CONFIG = "name";
        String VALUE_CONFIG = "value";
    }

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ConfigName.TYPE_CONFIG, ConfigDef.Type.STRING, "exclude",
                    ConfigDef.CompositeValidator.of(new NonNullValidator(), ValidString.in("include", "exclude")),
                    ConfigDef.Importance.MEDIUM,
                    "The header name.")
            .define(ConfigName.NAME_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
                    new ConfigDef.NonEmptyString(),
                    ConfigDef.Importance.MEDIUM,
                    "The header name.")
            .define(ConfigName.VALUE_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
                    new ConfigDef.NonEmptyString(),
                    ConfigDef.Importance.MEDIUM,
                    "The header value.");

    private static final Logger log = LoggerFactory.getLogger(FilterOnMatchingHeader.class);

    private String type;
    private String name;
    private String value;

    @Override
    public R apply(R record) {
        boolean matchesHeader = matchesHeader(record);
        if (type.equals("exclude")) return matchesHeader ? null : record;
        if (type.equals("include")) return matchesHeader ? record : null;
        return record;
    }

    private boolean matchesHeader(R record) {
        String headerValueToMatch = Optional.ofNullable(record.headers().lastWithName(name))
                .map(Header::value)
                .map(Object::toString)
                .orElse(null);

        boolean result = headerValueToMatch != null && headerValueToMatch.equals(value);
        log.debug("Testing {} == {} returned {}", value, headerValueToMatch, result);
        return result;
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        SimpleConfig config = new SimpleConfig(config(), configs);
        this.type = config.getString(ConfigName.TYPE_CONFIG);
        this.name = config.getString(ConfigName.NAME_CONFIG);
        this.value = config.getString(ConfigName.VALUE_CONFIG);
        log.info("HeaderMatches configured: {}", this);
    }

    @Override
    public void close() {

    }

    @Override
    public String toString() {
        return "FilterOnMatchingHeader{" +
                "type='" + type + '\'' +
                ", name='" + name + '\'' +
                ", value='" + value + '\'' +
                '}';
    }
}

