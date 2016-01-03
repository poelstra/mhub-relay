"use strict";
var fs = require("fs");
var util_1 = require("./util");
;
function parseNodeSpec(s) {
    if (typeof s !== "string") {
        return s;
    }
    var match = s.match(/([^\/]+)\/(.*)/);
    if (!match) {
        throw new Error("invalid NodeSpec '" + s + "', expected e.g. 'server/node'");
    }
    return {
        server: match[1],
        node: match[2]
    };
}
// Read and parse config file
function parseConfig(filename) {
    var config = JSON.parse(fs.readFileSync(filename, "utf8"));
    // Parse connection settings
    if (typeof config.connections !== "object") {
        throw new Error("invalid configuration: missing 'connections' object");
    }
    var connections = config.connections;
    // Parse bindings
    var bindings = {};
    if (typeof config.bindings !== "object") {
        throw new Error("invalid configuration: missing 'bindings' object");
    }
    for (var id in config.bindings) {
        if (!config.bindings.hasOwnProperty(id)) {
            continue;
        }
        var bindConf = config.bindings[id];
        var input = util_1.ensureArray(bindConf.input).map(function (i) {
            if (typeof i === "string") {
                return {
                    node: parseNodeSpec(i)
                };
            }
            else {
                return {
                    node: parseNodeSpec(i.node),
                    pattern: i.pattern
                };
            }
        });
        var output = util_1.ensureArray(bindConf.output).map(parseNodeSpec);
        var transform = void 0;
        if (bindConf.transform) {
            try {
                var t = require((bindConf.transform));
                if (typeof t === "object" && t.default) {
                    t = t.default;
                }
                if (typeof t !== "function") {
                    throw new Error("transform must (default) export a single function");
                }
                transform = t;
            }
            catch (e) {
                throw new Error("error loading transform '" + bindConf.transform + "': " + e);
            }
        }
        bindings[id] = {
            input: input,
            output: output,
            transform: transform
        };
    }
    return {
        connections: connections,
        bindings: bindings
    };
}
exports.parseConfig = parseConfig;
