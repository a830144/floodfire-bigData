package home3;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UserJoinReducer extends Reducer<Text, Text, Text, Text> {

	String joinType = "leftouter";
	private ArrayList<Text> listA = new ArrayList<Text>();
	private ArrayList<Text> listB = new ArrayList<Text>();
	Text key;

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		// Clear our lists 
		listA.clear(); 
		listB.clear();
		for (Text t : values) {
			if (t.charAt(0) == 'A') {
				listA.add(new Text(t.toString().substring(1)));
			} else if (t.charAt(0) == 'B') {
				listB.add(new Text(t.toString().substring(1)));
			}
		}
		this.key = key;
		// Execute our join logic now that the lists are filled
		executeJoinLogic(context);
	}

	private void executeJoinLogic(Context context) throws IOException,
			InterruptedException {

		// For each entry in A,
		for (Text A : listA) {
			System.out.println("A.toString() ::"+A.toString() );
			// If list B is not empty, join A and B
			if (!listB.isEmpty()) {
				for (Text B : listB) {
					context.write(new Text(""),
							new Text(A.toString() + "\t" + B.toString()));
					System.out.println("B.toString() ::"+B.toString() );
				}
			} else { // Else, output A by itself
				context.write(new Text(""), new Text(A.toString() + ""));
			}
		}
		System.out.println("logic end::" );
	}

}
