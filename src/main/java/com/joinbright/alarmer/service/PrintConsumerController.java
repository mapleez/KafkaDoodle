package com.joinbright.alarmer.service;

import java.util.List;
import java.util.Properties;

public class PrintConsumerController extends ConsumerController {

	public PrintConsumerController (Properties props) {
		super (props);
	}

	@Override
	protected void dealWithMessage (List <String> messages) {
//		messages.forEach ((String msg) -> System.out.println (msg));
		for (String m : messages) {
			System.out.println (m);
		}
	}

}
