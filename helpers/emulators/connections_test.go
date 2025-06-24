package emulators

import (
	"testing"
)

func Test_getEmulatorOptions(t *testing.T) {
	endpoint := "localhost:8080"
	opts := getEmulatorOptions(endpoint)

	// We cannot reliably use reflect.DeepEqual on option.ClientOption
	// instances because they wrap unexported types and pointer addresses
	// which will differ even if the options are functionally identical.
	//
	// The primary test for getEmulatorOptions is ensuring that clients
	// configured with these options can successfully connect to the emulators.
	// This is covered by the integration tests for each specific emulator
	// (e.g., TestSetupBigQueryEmulator, TestSetupFirestoreEmulator, etc.).

	// We can, however, verify the number of options returned if that's a
	// specific contract we want to assert.
	expectedNumOptions := 3 // WithEndpoint, WithoutAuthentication, WithGRPCDialOption
	if len(opts) != expectedNumOptions {
		t.Errorf("Expected %d client options, got %d", expectedNumOptions, len(opts))
	}

	t.Log("Direct comparison of google.golang.org/api/option.ClientOption types is not practical due to unexported internals.")
	t.Log("Functionality of getEmulatorOptions is implicitly tested via successful client connections in other emulator setup tests.")
}
