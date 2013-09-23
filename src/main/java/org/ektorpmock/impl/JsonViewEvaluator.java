package org.ektorpmock.impl;

import org.ektorpmock.ViewEvaluator;
import org.ektorp.support.DesignDocument;
import org.mozilla.javascript.Context;
import org.mozilla.javascript.Scriptable;
import org.mozilla.javascript.Script;
import org.mozilla.javascript.NativeObject;
import org.ektorp.ViewQuery;
import org.ektorp.impl.StreamingJsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JsonViewEvaluator implements ViewEvaluator {

    private static String MAP_SOURCE;
    private static String REDUCE_SOURCE;

    static {
        MAP_SOURCE = "" +
                "       var map = %s;\n" +
                "       function emit(key, value) {\n" +
                "           var emitted = new java.util.HashMap();\n" +
                "           emitted.put('id', jsonData._id);\n" +
                "           emitted.put('key', key);\n" +
                "           emitted.put('value', value);\n" +
                "           if (queryKey == null || key.equals(queryKey)) {\n" +
                "               viewList.add(emitted);\n" +
                "           }\n" +
                "       }\n" +
                "       map(jsonData);";
        REDUCE_SOURCE = "\n" +
                "       var _count = function(key, values, rereduce) {\n" +
                "           return count(values);\n" +
                "       };\n" +
                "       var _sum = function(key, values, rereduce) {\n" +
                "           return sum(values);\n" +
                "       };\n" +
                "       var _stats = function(key, values, rereduce) {\n" +
                "           return {\n" +
                "               sum: sum(values),\n" +
                "               count: count(values),\n" +
                "               min: min(values),\n" +
                "               max: max(values),\n" +
                "               sumsqr: sumsqr(values)\n" +
                "            }\n" +
                "       }\n" +
                "       function min(list) {\n" +
                "               var min;\n" +
                "       for (i = 0; i < list.length; i++) {\n" +
                "           if (!min || (list[i] < min)) {\n" +
                "               min = list[i];\n" +
                "           }\n" +
                "       }\n" +
                "       return min;\n" +
                "       }\n" +
                "       function max(list) {\n" +
                "               var max;\n" +
                "       for (i = 0; i < list.length; i++) {\n" +
                "           if (!max || (list[i] > max)) {\n" +
                "               max = list[i];\n" +
                "           }\n" +
                "       }\n" +
                "       return max;\n" +
                "       }\n" +
                "       function sumsqr(list) {\n" +
                "               var sqrs = [];\n" +
                "       for(i = 0; i < list.length; i++) {\n" +
                "           sqrs.push(Math.pow(list[i], 2));\n" +
                "       }\n" +
                "       return sum(sqrs);\n" +
                "       }\n" +
                "\n" +                "        var reduce = %s;\n" +
                "       function sum(list) {\n" +
                "               var sum = 0;\n" +
                "       for(i = 0; i < list.length; i++) {\n" +
                "           sum += list[i]\n" +
                "       }\n" +
                "       return sum;\n" +
                "       };\n" +
                "       function count(list) {\n" +
                "           return list.length;\n" +
                "       }\n" +
                "       var runReduceGrouped = function() {\n" +
                "           var map = {};\n" +
                "           for (i=0;i<jsonData.length;i++) {\n" +
                "               var obj = jsonData[i];\n" +
                "               if (!map[obj.key]) {\n" +
                "                   map[obj.key] = [obj.value];\n" +
                "               } else {\n" +
                "                   map[obj.key].push(obj.value);\n" +
                "               }\n" +
                "           }\n" +
                "           var reduced = {};\n" +
                "           for (key in map) {\n" +
                "               reduced[key] = reduce(key, map[key]);\n" +
                "           }\n" +
                "           return reduced;\n" +
                "       };\n" +
                "\n" +
                "       var runReduceUngrouped = function() {\n" +
                "           var map = {};\n" +
                "           for (i=0;i<jsonData.length;i++) {\n" +
                "               var obj = jsonData[i];\n" +
                "               if (!map[null]) {\n" +
                "                   map[null] = [obj.value];\n" +
                "               } else {\n" +
                "                   map[null].push(obj.value);\n" +
                "               }\n" +
                "           }\n" +
                "           var reduced = {};\n" +
                "           for (key in map) {\n" +
                "               reduced[key] = reduce(key, map[key]);\n" +
                "           }\n" +
                "           return reduced;\n" +
                "       };\n" +
                "\n";

    }


    @Override
    public List evaluateView(DesignDocument.View view, ViewQuery query, List data) {
        List mapList = map(view, data, query.getKey());

        if (view.getReduce() != null) {
            List reduceList = reduce(view, mapList, query.isGroup());
            return reduceList;
        } else {
            return mapList;
        }
    }

    private List reduce(DesignDocument.View view, List mapList, boolean group) {
        String reduceJson = view.getReduce();
        List reduceList = new ArrayList();

        Context cx = Context.enter();
        try {
            // Initialize the standard objects (Object, Function, etc.)
            // This must be done before scripts can be executed.
            Scriptable scope = cx.initStandardObjects();

            scope.put("reduceList", scope, reduceList);
            Script script = cx.compileString( String.format(REDUCE_SOURCE, reduceJson), "sharedScript", 1, null);
            script.exec(cx, scope);

            String jsonData = new StreamingJsonSerializer(new ObjectMapper()).toJson(mapList);

            cx.evaluateString(scope, "jsonData = " + jsonData,
                    "SetJsonData", 1, null);
            NativeObject obj;
            if (group) {
                obj = (NativeObject) cx.evaluateString(scope, "runReduceGrouped()", "SetJsonData", 1, null);
            } else {
                obj = (NativeObject) cx.evaluateString(scope, "runReduceUngrouped()", "SetJsonData", 1, null);
            }
            for (Object key: obj.keySet()) {
                Map reducedRow = new HashMap();
                reducedRow.put("key", key);
                reducedRow.put("value", obj.get(key));
                reduceList.add(reducedRow);
            }
        } finally {
            Context.exit();
        }
        return reduceList;
    }

    private List map(DesignDocument.View view, List data, Object key) {
        String mapJson = view.getMap();
        List viewList = new ArrayList();

        Context cx = Context.enter();
        try {
            // Initialize the standard objects (Object, Function, etc.)
            // This must be done before scripts can be executed.
            Scriptable scope = cx.initStandardObjects();

            scope.put("viewList", scope, viewList);
            scope.put("queryKey", scope, key);
            Script script = cx.compileString(String.format(MAP_SOURCE, mapJson), "sharedScript", 1, null);

            for (Object o: data) {
                cx.evaluateString(scope, "jsonData = " + o,
                        "SetJsonData", 1, null);
                script.exec(cx, scope);
            }
        } finally {
            Context.exit();
        }
        return viewList;
    }
}
