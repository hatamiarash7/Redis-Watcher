// Package integration contains end-to-end tests against a real Redis instance.
//
// The tests are gated behind the `integration` build tag so the default
// `go test ./...` invocation does not require a running Redis. Run with:
//
//	go test -tags=integration -count=1 ./test/integration/...
//
// The tests honour two environment variables:
//
//	REDIS_WATCHER_TEST_ADDR     - tcp host:port (default 127.0.0.1:6379)
//	REDIS_WATCHER_TEST_PASSWORD - optional AUTH password
//
// docker-compose.yml ships a Redis service usable for these tests.
package integration
