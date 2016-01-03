#!/usr/bin/env node

/**
 * Relay between two or more MHub servers.
 */

"use strict";

import * as uuid from "node-uuid";
import * as path from "path";
import { MClient, Message } from "mhub";

import { MaybeArray, ensureArray } from "./util";
import { RelayConfig, Binding, Input, NodeSpec, parseConfig } from "./config";

var currentInstance = "relay-" + uuid.v1();

var config: RelayConfig;
var connections: { [name: string]: Connection } = {};

class Connection {
	client: MClient;
	name: string;
	bindings: { [id: string]: Binding; } = Object.create(null);

	private _reconnectTimer: any = null;
	private _connecting: MClient = null;

	constructor(name: string) {
		this.name = name;
		// TODO: move this step to config parsing
		for (var id in config.bindings) {
			if (!config.bindings.hasOwnProperty(id)) {
				continue;
			}
			var binding = config.bindings[id];
			let input = binding.input.filter(
				(f) => f.node.server === this.name
			);
			if (input.length === 0) {
				continue;
			}

			this.bindings[id] = {
				input: input,
				output: binding.output,
				transform: binding.transform
			};
		}
	}

	private _log(...args: any[]): void {
		args.unshift("[" + this.name + "]");
		console.log.apply(null, args);
	}

	private _connect(): void {
		if (this._connecting) {
			return;
		}
		var c = new MClient(config.connections[this.name]);
		this._connecting = c;
		this._log("connecting to " + config.connections[this.name]);
		c.on("open", (): void => {
			this._log("connected");
			this.client = c;
			this._connecting = null; // TODO maybe wait until subscribe is done
			this._subscribe();
		});
		c.on("close", (): void => {
			this._log("closed");
			this._reconnect();
		});
		c.on("error", (e: Error): void => {
			this._log("error:", e);
			this._reconnect();
		});
		c.on("message", (m: Message, subscription: string): void => {
			this._handleMessage(m, subscription);
		});
	}

	private _reconnect(): void {
		if (this._connecting) {
			this._connecting.close();
			this._connecting = null;
		}
		if (this.client) {
			this.client.close();
			this.client = null;
		}
		if (!this._reconnectTimer) {
			this._reconnectTimer = setTimeout((): void => {
				this._reconnectTimer = null;
				this._connect();
			}, 3000);
		}
	}

	private _handleMessage(message: Message, subscription: string): void {
		/* TODO: think this through some more: relay can also be used to process
		   messages that come back again on e.g. different nodes, which would
		   not necessarily be loops.
		   Additionally, need to actually set this header on outgoing messages.
		if (message.headers["x-via-" + currentInstance]) {
			return; // Skip the messages we posted ourselves
		}
		*/
		if (!this.client) {
			return;
		}
		let binding = this.bindings[subscription];
		if (!binding) {
			return;
		}
		console.log(`#${subscription} [${this.name}] ${message.topic}`);

		if (!binding.transform) {
			this._publish(message, binding);
		} else {
			Promise.resolve(message)
				.then(binding.transform)
				.then((transformed: void|MaybeArray<void|Message>): void => {
					if (transformed === undefined) {
						return;
					}
					ensureArray(transformed).forEach((msg: Message) => {
						if (typeof msg !== "object") {
							throw new Error("invalid transformed message: object or array of objects expected");
						}
						if (typeof msg.topic !== "string") {
							throw new Error("invalid transformed message: topic string expected");
						}
						this._publish(msg, binding);
					});
				});
		}
	}

	private _publish(message: Message, binding: Binding): void {
		binding.output.forEach((out: NodeSpec) => {
			var outConn = connections[out.server];
			// TODO prefix every line with a unique message ID?
			// Because transformed message may be out-of-order/delayed etc
			console.log(` -> [${out.server}/${out.node}]: ${message.topic}`);
			if (outConn.client) {
				outConn.client.publish(out.node, message);
			} else {
				console.warn(`  -> error: ${out.server}: not connected`);
			}
		});
	}

	private _subscribe(): void {
		for (var id in this.bindings) {
			this.bindings[id].input.forEach((i) => {
				this.client.subscribe(i.node.node, i.pattern, id);
			});
		}
	}

	start(): void {
		this._connect();
	}
}

const configFile = path.resolve(__dirname, "relay.conf.json");
console.log("Using config file " + configFile);
config = parseConfig(configFile);

// Create connection objects (also validates config semantically)
Object.keys(config.connections).forEach((name: string): void => {
	connections[name] = new Connection(name);
});

// Start the connections
Object.keys(connections).forEach((name: string): void => {
	connections[name].start();
});
