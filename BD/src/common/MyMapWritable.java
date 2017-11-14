package common;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

public class MyMapWritable extends MapWritable {

	public MyMapWritable() {
		super();
		// TODO Auto-generated constructor stub
	}

	public MyMapWritable(MapWritable other) {
		super(other);
		// TODO Auto-generated constructor stub
	}

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("{");
		for( Entry<Writable, Writable> e: this.entrySet()){
        	sb.append(e.getKey().toString() + ":" + e.getValue().toString());     
        	sb.append(" ");
        }		
		sb.append("}");
		return sb.toString();
	}

	
}
