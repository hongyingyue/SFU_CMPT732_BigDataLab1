import java.io.IOException;
import java.io.DataOutput;
import java.io.DataInput;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.LongWritable;

/**
 * A Writable that represents a pair of long values.
 */
public class LongPairWritable implements Writable {
	private long a;
	private long b;
	private LongWritable writ = new LongWritable();

	public LongPairWritable(long a, long b) {
		this.a = a;
		this.b = b;
	}

	public LongPairWritable() {
		this(0, 0);
	}
	
	public long get_0() {
		return a;
	}
	public long get_1() {
		return b;
	}
	public void set(long a, long b) {
		this.a = a;
		this.b = b;		
	}

	public void write(DataOutput out) throws IOException {
		writ.set(a);
		writ.write(out);
		writ.set(b);
		writ.write(out);
	}
	public void readFields(DataInput in) throws IOException {
		writ.readFields(in);
		a = writ.get();
		writ.readFields(in);
		b = writ.get();
	}
	
	public String toString() {
		return "(" + Long.toString(a) + "," + Long.toString(b) + ")";
	}

}
