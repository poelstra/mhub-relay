/**
 * Relay between two or more MHub servers.
 */

/// <reference path="./typings/tsd.d.ts" />

"use strict";

import * as uuid from "node-uuid";
import * as fs from "fs";
import * as path from "path";
import { MClient, Message } from "mhub";

var currentInstance = "relay-" + uuid.v1();

var configFile = path.resolve(__dirname, "relay.conf.json");
console.log("Using config file " + configFile);
var config: RelayConfig = JSON.parse(fs.readFileSync(configFile, "utf8"));

interface NodeSpec {
	server: string;
	node: string;
};

interface Filter {
	from: NodeSpec; // Can be passed as "server/node" in config file
	pattern?: string;
}

interface Binding {
	filters: Filter[];
	target: NodeSpec; // Can be passed as "server/node" in config file
}

interface RelayConfig {
	/**
	 * name -> URL, e.g. ws://localhost:13900
	 */
	servers: { [ name: string ]: string; };

	bindings: { [name: string ]: Binding; };
}

var servers: { [name: string]: Server } = {};

class Server {
	client: MClient;
	name: string;
	subscriptions: { [id: string]: Binding; } = Object.create(null);

	constructor(name: string) {
		this.name = name;
		for (var id in config.bindings) {
			if (!config.bindings.hasOwnProperty(id)) {
				continue;
			}
			var binding = config.bindings[id];
			let filters = binding.filters.filter(
				(f) => f.from.server === this.name
			);
			if (filters.length === 0) {
				continue;
			}
			this.subscriptions[id] = {
				filters: filters,
				target: binding.target
			};
		}
	}

	private _log(...args: any[]): void {
		args.unshift("[" + this.name + "]");
		console.log.apply(null, args);
	}

	private _connect(): void {
		var c = new MClient(config.servers[this.name]);
		this._log("connecting to " + config.servers[this.name]);
		c.on("open", (): void => {
			this._log("connected");
			this.client = c;
			this._subscribe();
		});
		c.on("close", (): void => {
			this._log("closed");
			this.client = null;
			setTimeout((): void => {
				this._connect();
			}, 1000);
		});
		c.on("error", (e: Error): void => {
			this._log("error:", e);
			this.client = null;
			setTimeout((): void => {
				this._connect();
			}, 1000);
		});
		c.on("message", (m: Message, subscription: string): void => {
			this._handleMessage(m, subscription);
		});
	}

	private _handleMessage(message: Message, subscription: string): void {
		if (message.headers["x-via-" + currentInstance]) {
			return; // Skip the messages we posted ourselves
		}
		if (!this.client) {
			return;
		}
		let sub = this.subscriptions[subscription];
		if (!sub) {
			return;
		}
		var destServer = servers[sub.target.server];
		console.log(`#${subscription} [${this.name}] -> [${sub.target.server}/${sub.target.node}]: ${message.topic}`);
		destServer.client.publish(sub.target.node, message);
	}

	private _subscribe(): void {
		for (var id in this.subscriptions) {
			this.subscriptions[id].filters.forEach((f) => {
				this.client.subscribe(f.from.node, f.pattern, id);
			});
		}
	}

	start(): void {
		this._connect();
	}
}

function parseNodeSpec(s: string|NodeSpec): NodeSpec {
	if (typeof s !== "string") {
		return s;
	}
	let match = (<string>s).match(/([^\/]+)\/(.*)/);
	if (!match) {
		throw new Error(`invalid NodeSpec '${s}', expected e.g. 'server/node'`);
	}
	return {
		server: match[1],
		node: match[2]
	};
}

for (let id in config.bindings) {
	if (!config.bindings.hasOwnProperty(id)) {
		continue;
	}
	let binding = config.bindings[id];
	binding.target = parseNodeSpec(binding.target);
	binding.filters.forEach((f: Filter): void => {
		f.from = parseNodeSpec(f.from);
	});
}

Object.keys(config.servers).forEach((name: string): void => {
	servers[name] = new Server(name);
});

Object.keys(servers).forEach((name: string): void => {
	servers[name].start();
});
