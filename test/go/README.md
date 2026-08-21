# Official Go SDK integration

This module provisions a temporary `us`/`wnam` application through the control API, then uses the
official [`pusher-http-go/v5`](https://github.com/pusher/pusher-http-go) SDK to trigger an event,
query channel state, and trigger a batch against the running service.

With the local stack running:

```sh
PUSHER_GO_E2E=1 \
PUSHER_GO_E2E_HOST=127.0.0.1:1337 \
PUSHER_CONTROL_TOKEN=control-token \
mise exec go@1.25 -- go test -v ./...
```

Run the command from `test/go`. The test skips unless `PUSHER_GO_E2E=1` is set and deletes its
temporary application on completion. Workerd does not implement jurisdiction enforcement; that
part of placement must be verified on Cloudflare.
