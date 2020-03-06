package com.ververica.flinktraining.examples.datastream_java;

import com.ververica.flinktraining.exercises.datastream_java.datatypes.ConnectedCarEvent;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;
import org.apache.flink.util.Collector;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.Locale;
import java.util.PriorityQueue;

public class MyCustomOrder {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> inputData = env.socketTextStream("localhost", 10, "\n");

        DataStream<Cards> cardsDataStream =
                inputData
                .map((String line) -> Cards.fromString(line))
                .assignTimestampsAndWatermarks(new CardAssigner());

        cardsDataStream
                .keyBy((card) -> card.cardHash )
                .process(new SortFunction())
                .print();



        env.execute("My Custom Order");
    }


    @AllArgsConstructor
    @NoArgsConstructor
    @Data
    private static class Cards implements Comparable<Cards> {

        private long timestamp;
        private String cardHash;

        private static transient DateTimeFormatter timeFormatter =
                DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ssZ").withLocale(Locale.US).withZoneUTC();

        /**
         * Parses the socket into a Card Model
         * @param line line that enter
         * @return Cards Model
         */
        public static Cards fromString(String line) {
            String[] tokens = line.split("(,|;)\\s*");
            if (tokens.length != 2) {
                throw new RuntimeException("Invalid record: " + line);
            }
            Cards event = new Cards();
            try {
                event.timestamp = DateTime.parse(tokens[0], timeFormatter).getMillis();
                event.cardHash = tokens[1];
            } catch (NumberFormatException nfe) {
                throw new RuntimeException("Invalid field: " + line, nfe);
            }
            return event;
        }

        @Override
        public int compareTo(Cards other) {
            return Long.compare(this.timestamp, other.timestamp);
        }

    }

    private static class CardAssigner implements AssignerWithPunctuatedWatermarks<Cards> {

        @Override
        public long extractTimestamp(Cards cards, long l) {
            return cards.timestamp;
        }

        @Nullable
        @Override
        public Watermark checkAndGetNextWatermark(Cards cards, long extractedTimeStamp) {
            return new Watermark(extractedTimeStamp);
        }

    }

    private static class SortFunction extends KeyedProcessFunction<String, Cards, Cards> {
        private ValueState<PriorityQueue<Cards>> queueState = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<PriorityQueue<Cards>> descriptor = new ValueStateDescriptor<>(
                    // state name
                    "sorted-events",
                    // type information of state
                    TypeInformation.of(new TypeHint<PriorityQueue<Cards>>() {
                    }));
            queueState = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement(Cards cards, Context context, Collector<Cards> collector) throws Exception {

            TimerService timerService = context.timerService();

            if (context.timestamp() > timerService.currentWatermark()) {
                PriorityQueue<Cards> queue = queueState.value();
                if (queue == null) {
                    queue = new PriorityQueue<>(100);
                }
                queue.add(cards);
                queueState.update(queue);
                timerService.registerEventTimeTimer(cards.timestamp);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext context, Collector<Cards> out) throws Exception {

            PriorityQueue<Cards> queue = queueState.value();

            Long watermark = context.timerService().currentWatermark();

            Cards head = queue.peek(); //Regresa el primer elemento de la lista

            while (head != null && head.timestamp <= watermark) {
                out.collect(head);
                queue.remove(head);
                head = queue.peek();
            }

        }
    }
}
