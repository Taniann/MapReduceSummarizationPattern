package ua.kpi.tef;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Таня on 30.09.2018.
 */

@ToString
@Getter
@Setter
public class AnswersCountMaxRatingResult implements Writable {
    private long max = 0;
    private long count = 0;

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(max);
        dataOutput.writeLong(count);
    }

    public void readFields(DataInput dataInput) throws IOException {
        max = dataInput.readLong();
        count = dataInput.readLong();
    }

}
