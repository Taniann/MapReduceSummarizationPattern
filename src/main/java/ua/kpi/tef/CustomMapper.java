package ua.kpi.tef;

import com.opencsv.CSVParser;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Таня on 30.09.2018.
 */
public class CustomMapper extends Mapper<Object, Text, Text, AnswersCountMaxRatingResult> {
    private Text outUserId = new Text();
    private AnswersCountMaxRatingResult outResult = new AnswersCountMaxRatingResult();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
       CSVParser parser = new CSVParser();
       String[] lines = parser.parseLineMulti(value.toString());

       String userId = lines[1];

      //  String line = value.toString();
      //  String[] fields = line.split(" ");
        long scoreStr = 0L;
        try{
           scoreStr = Long.parseLong(lines[4]);
        }
          catch(NumberFormatException nfe){
         }

        outResult.setMax(scoreStr);
        outResult.setCount(1);
        outUserId.set(userId);

        context.write(outUserId, outResult);

     }
}
