package ua.kpi.tef;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by Таня on 30.09.2018.
 */
public class Driver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if(args.length < 2) {
            System.exit(-1);
        }

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "Answer cout + Max Rating value");

        job.setJarByClass(Driver.class);
        job.setMapperClass(CustomMapper.class);
        job.setCombinerClass(CustomReducer.class);
        job.setReducerClass(CustomReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(AnswersCountMaxRatingResult.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean success = job.waitForCompletion(true);
        System.exit(success ? 0 : 1);
    }
}
