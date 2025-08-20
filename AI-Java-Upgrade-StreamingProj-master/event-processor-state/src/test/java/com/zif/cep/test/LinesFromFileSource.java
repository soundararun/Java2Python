package com.zif.cep.test;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.json.simple.JSONObject;

import com.zif.cep.core.JSONObjectDeserializationSchema;
import com.zif.cep.core.Rule;
import com.zif.cep.core.RuleDeserializationSchema;

/**
 * @author Vijay
 *
 */
public abstract class LinesFromFileSource<T> implements SourceFunction<T> {

    private final String filepath;
    private long timestamp;

    public LinesFromFileSource(String filepath, long timestamp) {
        this.filepath = filepath;
        this.timestamp = timestamp;
    }

    protected abstract T convert(String line);

    @Override
    public void run(SourceContext<T> ctx) throws Exception {

        List<String> lines;
        try (Stream<String> stream = Files.lines(Paths.get(filepath))) {
            lines = stream.collect(Collectors.toList());
        }

        for (String line : lines) {
            synchronized (ctx.getCheckpointLock()) {
                ctx.collectWithTimestamp(convert(line), timestamp++);
            }
        }
    }

    @Override
    public void cancel() {
        // do nothing
    }

    public static class EventFromFileSource extends LinesFromFileSource<JSONObject> {
        public EventFromFileSource(String filepath, long timestamp) {
            super(filepath,timestamp);
        }

        @Override
        protected JSONObject convert(String eventStr) {
        	return new JSONObjectDeserializationSchema().deserialize(eventStr.getBytes());             
        }
    }
    
    public static class RuleFromFileSource extends LinesFromFileSource<Rule> {
        public RuleFromFileSource(String filepath, long timestamp) {
            super(filepath,timestamp);
        }

        @Override
        protected Rule convert(String ruleStr) {
        	return new RuleDeserializationSchema().deserialize(ruleStr.getBytes());             
        }
    }
}