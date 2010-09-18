//UpdateFunctionMap.java
package com.mongodb.jdbc.function.update;

import java.util.HashMap;
import java.util.Map;

public class UpdateFunctionMap {
	public static Map<String, AbstractUpdateFunctionHandler> handlerMap;
	static {
		handlerMap = new HashMap<String, AbstractUpdateFunctionHandler>();
		AbstractUpdateFunctionHandler addToSet = new AddToSetFunctionHandler();
		AbstractUpdateFunctionHandler inc = new IncFunctionHandler();
		AbstractUpdateFunctionHandler pop = new PopFunctionHandler();
		AbstractUpdateFunctionHandler pullAll = new PullAllFunctionHandler();
		AbstractUpdateFunctionHandler pull = new PullFunctionHandler();
		AbstractUpdateFunctionHandler pushAll = new PushAllFunctionHandler();
		AbstractUpdateFunctionHandler push = new PushFunctionHandler();
		AbstractUpdateFunctionHandler set = new SetFunctionHandler();
		AbstractUpdateFunctionHandler unset = new UnsetFunctionHandler();

		handlerMap.put("addtoset", addToSet);
		handlerMap.put("inc", inc);
		handlerMap.put("pop", pop);
		handlerMap.put("pullall", pullAll);
		handlerMap.put("pull", pull);
		handlerMap.put("pushall", pushAll);
		handlerMap.put("push", push);
		handlerMap.put("set", set);
		handlerMap.put("unset", unset);
	}
}
