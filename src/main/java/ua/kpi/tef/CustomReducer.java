package ua.kpi.tef;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Таня on 30.09.2018.
 */
public class CustomReducer extends
        Reducer<Text, AnswersCountMaxRatingResult, Text, AnswersCountMaxRatingResult> {
    private AnswersCountMaxRatingResult result = new AnswersCountMaxRatingResult();

    @Override
    protected void reduce(Text key, Iterable<AnswersCountMaxRatingResult> values, Context context) throws IOException, InterruptedException {
        result.setMax(-10000);
        int sum = 0;

        for (AnswersCountMaxRatingResult value : values) {
            if (value.getMax() > result.getMax()) {
                result.setMax(value.getMax());
            }
            sum += value.getCount();
        }
        result.setCount(sum);
        context.write(key, result);
    }
}
