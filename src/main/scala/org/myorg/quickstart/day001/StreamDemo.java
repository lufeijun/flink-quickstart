package org.myorg.quickstart.day001;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class StreamDemo {

	public static void main(String[] args) {

		Stream<Integer> data = Stream.of(1, 2, 3, 4, 5, 6, 8, 5, 4, 4);


		List<Integer> collected = data.filter(t -> t >= 5).collect(Collectors.toList());

		for (Integer i : collected) {
			System.out.println( i );
		}


	}


}
