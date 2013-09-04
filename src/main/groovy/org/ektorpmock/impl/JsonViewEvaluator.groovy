package org.ektorpmock.impl

import org.ektorp.ViewEvaluator
import org.ektorp.support.DesignDocument
import org.mozilla.javascript.Context
import org.mozilla.javascript.Scriptable
import org.mozilla.javascript.Script
import org.codehaus.jackson.map.ObjectMapper
import org.mozilla.javascript.NativeObject
import org.ektorp.ViewQuery
import org.ektorp.impl.StreamingJsonSerializer

class JsonViewEvaluator implements ViewEvaluator{

    @Override
    List evaluateView(DesignDocument.View view, ViewQuery query, List data) {
        def mapList = map(view, data, query.key)

        if (view.reduce) {
            def reduceList = reduce(view, mapList, query.group)
            return reduceList
        } else {
            return mapList
        }
    }

    private List reduce(DesignDocument.View view, List mapList, boolean group = true) {
        def reduceJson = view.reduce
        List reduceList = []

        Context cx = Context.enter();
        try {
            // Initialize the standard objects (Object, Function, etc.)
            // This must be done before scripts can be executed.
            Scriptable scope = cx.initStandardObjects();

            // Precompile source only once
            String source = """
                    var _count = function(key, values, rereduce) {
                        return count(values);
                    };
                    var _sum = function(key, values, rereduce) {
                        return sum(values);
                    };
                    var _stats = function(key, values, rereduce) {
                        return {
                            sum: sum(values),
                            count: count(values),
                            min: min(values),
                            max: max(values),
                            sumsqr: sumsqr(values)
                        }
                    }
                    function min(list) {
                        var min;
                        for (i = 0; i < list.length; i++) {
                            if (!min || (list[i] < min)) {
                                min = list[i];
                            }
                        }
                        return min;
                    }
                    function max(list) {
                        var max;
                        for (i = 0; i < list.length; i++) {
                            if (!max || (list[i] > max)) {
                                max = list[i];
                            }
                        }
                        return max;
                    }
                    function sumsqr(list) {
                        var sqrs = [];
                        for(i = 0; i < list.length; i++) {
                            sqrs.push(Math.pow(list[i], 2));
                        }
                        return sum(sqrs);
                    }

                    var reduce = ${reduceJson};
                    function sum(list) {
                        var sum = 0;
                        for(i = 0; i < list.length; i++) {
                            sum += list[i]
                        }
                        return sum;
                    };
                    function count(list) {
                        return list.length;
                    }
                    var runReduceGrouped = function() {
                        var map = {};
                        for (i=0;i<jsonData.length;i++) {
                          var obj = jsonData[i];
                          if (!map[obj.key]) {
                            map[obj.key] = [obj.value];
                          } else {
                            map[obj.key].push(obj.value);
                          }
                        }
                        var reduced = {};
                        for (key in map) {
                            reduced[key] = reduce(key, map[key]);
                        }
                        return reduced;
                    };

                    var runReduceUngrouped = function() {
                        var map = {};
                        for (i=0;i<jsonData.length;i++) {
                          var obj = jsonData[i];
                          if (!map[null]) {
                            map[null] = [obj.value];
                          } else {
                            map[null].push(obj.value);
                          }
                        }
                        var reduced = {};
                        for (key in map) {
                            reduced[key] = reduce(key, map[key]);
                        }
                        return reduced;
                    };


                """

            scope.put("reduceList", scope, reduceList)
            Script script = cx.compileString(source, "sharedScript", 1, null);
            script.exec(cx, scope)

            def jsonData = new StreamingJsonSerializer(new ObjectMapper()).toJson(mapList)

            cx.evaluateString(scope, "jsonData = $jsonData",
                    "SetJsonData", 1, null);
            NativeObject obj
            if (group) {
                obj = cx.evaluateString(scope, "runReduceGrouped()",
                        "SetJsonData", 1, null);
            } else {
                obj = cx.evaluateString(scope, "runReduceUngrouped()",
                        "SetJsonData", 1, null);
            }
            obj.keySet().each {
                reduceList.add([key: it, value: obj.get(it)])
            }

        } finally {
            Context.exit();
        }
        return reduceList
    }

    private List map(DesignDocument.View view, List data, Object key) {
        def mapJson = view.map
        List viewList = []

        Context cx = Context.enter();
        try {
            // Initialize the standard objects (Object, Function, etc.)
            // This must be done before scripts can be executed.
            Scriptable scope = cx.initStandardObjects();

            // Precompile source only once
            String source = """
                    var map = ${mapJson};
                    function emit(key, value) {
                        var emitted = new java.util.HashMap();
                        emitted.put('id', jsonData._id);
                        emitted.put('key', key);
                        emitted.put('value', value);
                        if (queryKey == null || key.equals(queryKey)) {
                            viewList.add(emitted);
                        }
                    }
                    map(jsonData);
                """

            scope.put("viewList", scope, viewList)
            scope.put("queryKey", scope, key)
            Script script = cx.compileString(source, "sharedScript", 1, null);

            data.each {
                cx.evaluateString(scope, "jsonData = $it",
                        "SetJsonData", 1, null);
                script.exec(cx, scope)
            }

        } finally {
            Context.exit();
        }
        return viewList
    }
}
