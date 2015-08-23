/**
 * Relay between two or more MHub servers.
 */
/// <reference path="./typings/tsd.d.ts" />
"use strict";
var uuid = require("node-uuid");
var fs = require("fs");
var path = require("path");
var mhub_1 = require("mhub");
var currentInstance = "relay-" + uuid.v1();
var configFile = path.resolve(__dirname, "relay.conf.json");
console.log("Using config file " + configFile);
var config = JSON.parse(fs.readFileSync(configFile, "utf8"));
;
var servers = {};
var Server = (function () {
    function Server(name) {
        var _this = this;
        this.subscriptions = Object.create(null);
        this.name = name;
        for (var id in config.bindings) {
            if (!config.bindings.hasOwnProperty(id)) {
                continue;
            }
            var binding = config.bindings[id];
            var filters = binding.filters.filter(function (f) { return f.from.server === _this.name; });
            if (filters.length === 0) {
                continue;
            }
            this.subscriptions[id] = {
                filters: filters,
                target: binding.target
            };
        }
    }
    Server.prototype._log = function () {
        var args = [];
        for (var _i = 0; _i < arguments.length; _i++) {
            args[_i - 0] = arguments[_i];
        }
        args.unshift("[" + this.name + "]");
        console.log.apply(null, args);
    };
    Server.prototype._connect = function () {
        var _this = this;
        var c = new mhub_1.MClient(config.servers[this.name]);
        this._log("connecting to " + config.servers[this.name]);
        c.on("open", function () {
            _this._log("connected");
            _this.client = c;
            _this._subscribe();
        });
        c.on("close", function () {
            _this._log("closed");
            _this.client = null;
            setTimeout(function () {
                _this._connect();
            }, 1000);
        });
        c.on("error", function (e) {
            _this._log("error:", e);
            _this.client = null;
            setTimeout(function () {
                _this._connect();
            }, 1000);
        });
        c.on("message", function (m, subscription) {
            _this._handleMessage(m, subscription);
        });
    };
    Server.prototype._handleMessage = function (message, subscription) {
        if (message.headers["x-via-" + currentInstance]) {
            return; // Skip the messages we posted ourselves
        }
        if (!this.client) {
            return;
        }
        var sub = this.subscriptions[subscription];
        if (!sub) {
            return;
        }
        var destServer = servers[sub.target.server];
        console.log("#" + subscription + " [" + this.name + "] -> [" + sub.target.server + "/" + sub.target.node + "]: " + message.topic);
        destServer.client.publish(sub.target.node, message);
    };
    Server.prototype._subscribe = function () {
        var _this = this;
        for (var id in this.subscriptions) {
            this.subscriptions[id].filters.forEach(function (f) {
                _this.client.subscribe(f.from.node, f.pattern, id);
            });
        }
    };
    Server.prototype.start = function () {
        this._connect();
    };
    return Server;
})();
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
for (var id in config.bindings) {
    if (!config.bindings.hasOwnProperty(id)) {
        continue;
    }
    var binding = config.bindings[id];
    binding.target = parseNodeSpec(binding.target);
    binding.filters.forEach(function (f) {
        f.from = parseNodeSpec(f.from);
    });
}
Object.keys(config.servers).forEach(function (name) {
    servers[name] = new Server(name);
});
Object.keys(servers).forEach(function (name) {
    servers[name].start();
});
