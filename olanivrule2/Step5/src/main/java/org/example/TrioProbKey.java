package org.example;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TrioProbKey implements WritableComparable<TrioProbKey> {
    private String w1;
    private String w2;
    private String w3; // This field exists but won't be used for comparison
    private double number;

    public TrioProbKey() {}

    public TrioProbKey(String w1, String w2, String w3, double number) {
        this.w1 = w1;
        this.w2 = w2;
        this.w3 = w3;
        this.number = number;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeString(out, w1);
        WritableUtils.writeString(out, w2);
        WritableUtils.writeString(out, w3);
        out.writeDouble(number);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        w1 = WritableUtils.readString(in);
        w2 = WritableUtils.readString(in);
        w3 = WritableUtils.readString(in);
        number = in.readDouble();
    }

    @Override
    public int compareTo(TrioProbKey o) {
        // Compare w1
        int cmp = w1.compareTo(o.w1);
        if (cmp != 0) {
            return cmp;
        }

        // Compare w2
        cmp = w2.compareTo(o.w2);
        if (cmp != 0) {
            return cmp;
        }

        // Compare by number
        return Double.compare(o.number, this.number);
    }

    @Override
    public String toString() {
        return w1 + " " + w2 + " " + w3 + " " + number;
    }

    public String getTrio() {
        return w1 + " " + w2 + " " + w3;
    }

    // Getters and setters
    public String getW1() {
        return w1;
    }

    public void setW1(String w1) {
        this.w1 = w1;
    }

    public String getW2() {
        return w2;
    }

    public void setW2(String w2) {
        this.w2 = w2;
    }

    public String getW3() {
        return w3;
    }

    public void setW3(String w3) {
        this.w3 = w3;
    }

    public Double getNumber() {
        return number;
    }

    public void setNumber(Double number) {
        this.number = number;
    }


}

