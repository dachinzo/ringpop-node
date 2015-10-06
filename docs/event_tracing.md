# A simple primer for testing event tracing
At present there is only one traceable event: `membership.checksum.update`

In order to use/test it, you will need to use tcurl and tcat from the tcat branch in tcurl. (Perhaps this will be merged, but for now that's simplest.)

In a window, spawn a ring, via tick cluster, or by hand -- whatever makes sense. In our case, we'll assume that a ring of nodes that has a member on localhost:8200.

The listener (tcat) looks like this:
```
    /path/to/tcurl/tcat.js -p localhost:4444 ringpop foobar
```
Where `foobar` is an arbitrary endpoint name (like a URL path). `tcat` will listen on localhost port 4444, service `ringpop`, and accept and log all messages to the `foobar` endpoint.

In another window, register the event tracer at a single member, in this case localhost:8200:
```
tcurl -p 127.0.0.1:8200 ringpop /trace/add -3 '{"event": "membership.checksum.update", "expiresIn": 1000000, "sink": { "type": "tchannel", "serviceName": "ringpop", "endpoint": "foobar", "hostPort":"127.0.0.1:4444" } }'
```
The command should return a result of `OK`. (The ringpop member @ 8200 may emit a socket closed error -- this is due to an assumption in tcurl, but is not a problem.)

Now you should have all membership checksum updates log to the tcat window. To test it, kill a ringpop member NOT at port 8200. That should generate a message. Restart it. That should also generate a message.

