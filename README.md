# fluentd-node 
[![Build Status](https://github.com/jamiees2/fluentd-node/actions/workflows/main.yml/badge.svg)](https://github.com/jamiees2/fluentd-node/actions)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Docs](https://img.shields.io/badge/Docs-latest-green)](https://jamiees2.github.io/fluentd-node/)

fluent-logger implementation for Node.js. 
Built upon [fluent-logger-node](https://github.com/fluent/fluent-logger-node).

[![NPM](https://nodei.co/npm/fluentd-node.png?downloads=true&downloadRank=true)](https://nodei.co/npm/fluentd-node/)


## Install

    $ npm install fluentd-node

## Prerequisites

The fluent daemon should be listening on a TCP port.

Simple configuration is following:

```aconf
<source>
  @type forward
  port 24224
</source>

<match **.*>
  @type stdout
</match>
```

## Usage

### Send an event record to Fluentd

```js
const FluentClient = require("fluentd-node").FluentClient;
const logger = new FluentClient("tag_prefix", {
  socket: {
    host: "localhost",
    port: 24224,
    timeout: 3000, // 3 seconds
  }
});
```

The emit method has following signature

```js
emit(data: Record<string, any>): Promise<void>;
emit(data: Record<string, any>, timestamp: number | Date | EventTime): Promise<void>;
emit(label: string, data: Record<string, any>): Promise<void>;
emit(label: string, data: Record<string, any>, timestamp: number | Date | EventTime): Promise<void>;
```

The returned Promise is resolved once the event is written to the socket, or rejected if an error occurs.

### Fluentd acknowledgements
Fluentd provides explicit support for acknowledgements, which allow the client to be sure that the event reached its destination. 

Enabling acknowledgements means that the promise returned by `emit` will be resolved once the client receives an explicit acknowledgement from the server.
```js
const FluentClient = require("fluentd-node").FluentClient;
const logger = new FluentClient("tag_prefix", {
  ack: {}
});
```

### Event modes
Fluentd provides multiple message modes, `Message`, `Forward`, `PackedForward`(default), `CompressedPackedForward`. The Fluent client supports all of them.


```js
const FluentClient = require("fluentd-node").FluentClient;
const logger = new FluentClient("tag_prefix", {
  eventMode: "Message" | "Forward" | "PackedForward" | "CompressedPackedForward"
});
```


### Disable automatic reconnect 
```js
const logger = new FluentClient("tag_prefix", {
  socket: {
    host: "localhost",
    port: 24224,
    timeout: 3000, // 3 seconds
    disableReconnect: true
  }
});
```

### Shared key authentication

Logger configuration:

```js
const logger = new FluentClient("tag_prefix", {
  socket: {
    host: "localhost",
    port: 24224,
    timeout: 3000, // 3 seconds
  }
  security: {
    clientHostname: "client.localdomain",
    sharedKey: "secure_communication_is_awesome"
  }
});
```

Server configuration:

```aconf
<source>
  @type forward
  port 24224
  <security>
    self_hostname input.testing.local
    shared_key secure_communication_is_awesome
  </security>
</source>

<match dummy.*>
  @type stdout
</match>
```

See also the [Fluentd](https://github.com/fluent/fluentd) examples.

### TLS/SSL encryption

Logger configuration:

```js
const logger = new FluentClient("tag_prefix", {
  socket: {
    host: "localhost",
    port: 24224,
    timeout: 3000, // 3 seconds
  }
  security: {
    clientHostname: "client.localdomain",
    sharedKey: "secure_communication_is_awesome"
  }
  tls: {
    ca: fs.readFileSync("/path/to/ca_cert.pem")
  }
});
```

Server configuration:

```aconf
<source>
  @type forward
  port 24224
  <transport tls>
    ca_cert_path /path/to/ca_cert.pem
    ca_private_key_path /path/to/ca_key.pem
    ca_private_key_passphrase very_secret_passphrase
  </transport>
  <security>
    self_hostname input.testing.local
    shared_key secure_communication_is_awesome
  </security>
</source>

<match dummy.*>
  @type stdout
</match>
```

FYI: You can generate certificates using fluent-ca-generate command since Fluentd 1.1.0.

See also [How to enable TLS/SSL encryption](https://docs.fluentd.org/input/forward#how-to-enable-tls-encryption).

### Mutual TLS Authentication

Logger configuration:

```js
const logger = new FluentClient("tag_prefix", {
  socket: {
    host: "localhost",
    port: 24224,
    timeout: 3000, // 3 seconds
  }
  security: {
    clientHostname: "client.localdomain",
    sharedKey: "secure_communication_is_awesome"
  }
  tls: {
    ca: fs.readFileSync("/path/to/ca_cert.pem"),
    cert: fs.readFileSync("/path/to/client-cert.pem"),
    key: fs.readFileSync("/path/to/client-key.pem"),
    passphrase: "very-secret"
  }
});
```

Server configuration:

```aconf
<source>
  @type forward
  port 24224
  <transport tls>
    ca_path /path/to/ca-cert.pem
    cert_path /path/to/server-cert.pem
    private_key_path /path/to/server-key.pem
    private_key_passphrase very_secret_passphrase
    client_cert_auth true
  </transport>
  <security>
    self_hostname input.testing.local
    shared_key secure_communication_is_awesome
  </security>
</source>

<match dummy.*>
  @type stdout
</match>
```

### EventTime support

We can also specify [EventTime](https://github.com/fluent/fluentd/wiki/Forward-Protocol-Specification-v1#eventtime-ext-format) as timestamp.

```js
const FluentClient = require("fluentd-node").FluentClient;
const EventTime = require("fluentd-node").EventTime;
const eventTime = new EventTime(1489547207, 745003500); // 2017-03-15 12:06:47 +0900
const logger = new FluentClient("tag_prefix", {
  socket: {
    host: "localhost",
    port: 24224,
    timeout: 3000, // 3 seconds
  }
});
logger.emit("tag", { message: "This is a message" }, eventTime);
```

### Handling errors
The Fluent client will manage errors internally, and reject promises on errors. If you"d like to access the non-user facing internal errors, you can do so by passing `errorHandler`

```js
const FluentClient = require("fluentd-node").FluentClient;
const logger = new FluentClient("tag_prefix", {
  onSocketError: (err: Error) => {
    console.log("error!", err)
  }
});
```

### Retrying events
Sometimes it makes sense to resubmit events if their initial submission failed. You can do this by specifying `eventRetry`.
```js
const FluentClient = require("fluentd-node").FluentClient;
const logger = new FluentClient("tag_prefix", {
  eventRetry: {}
});
```

### Client Options
For a full list of the client options and methods, see the [FluentClient docs](https://jamiees2.github.io/fluentd-node/classes/fluentclient.html)

### Server
`fluentd-node` includes a fully functional forward server which can be used as a downstream Fluent sink. 

```js
const FluentServer = require("fluentd-node").FluentServer;

const server = new FluentServer({ listenOptions: { port: 24224 }});

await server.listen();
```

Fluentd config:
```aconf
<match pattern>
  @type forward
  send_timeout 60s
  recover_wait 10s
  hard_timeout 60s

  <server>
    name fluent_node 
    host 127.0.0.1
    port 24224
    weight 60
  </server>

  <secondary>
    @type file
    path /var/log/fluent/forward-failed
  </secondary>
</match>
```

For a full list of the server options and methods, see the [FluentServer docs](https://jamiees2.github.io/fluentd-node/classes/fluentserver.html)


## License

Apache License, Version 2.0.

[fluent-logger-python]: https://github.com/fluent/fluent-logger-python
[fluent-logger-node]: https://github.com/fluent/fluent-logger-node


## About NodeJS versions

This package is compatible with NodeJS versions >= 12.
