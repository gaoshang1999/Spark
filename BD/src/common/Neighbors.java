package common;

import java.util.ArrayList;
import java.util.List;

public class Neighbors {
	public static String[]  neighbors(String[] items, int i){
		List<String> list = new ArrayList<String>();
		String w = items[i];
		for(int j = i+1; j< items.length; j++){
			String u = items[j];
			if(u.equals(w)){
				break;
			}
			list.add(u);
		}
		
		return list.toArray(new String[list.size()]);
	}
}
