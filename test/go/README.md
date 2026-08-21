# Official Go SDK integration

This module creates a temporary `us`/`wnam` application through the control API. It then uses the
official [`pusher-http-go/v5`](https://github.com/pusher/pusher-http-go) SDK. The test publishes one
event, gets the channel state, and publishes one event batch.

Start the local service. Then, run this command from `test/go`:

```sh
PUSHER_GO_E2E=1 \
PUSHER_GO_E2E_HOST=127.0.0.1:1337 \
PUSHER_CONTROL_TOKEN=control-token \
mise exec go@1.25 -- go test -v ./...
```

The test runs only if `PUSHER_GO_E2E` is `1`. The test deletes its temporary application when it is
complete. Workerd does not enforce jurisdictions. Run the test on Cloudflare to verify jurisdiction
placement.
