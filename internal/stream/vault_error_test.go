package stream

import (
	"fmt"
	"testing"

	gogram "github.com/amarnathcjd/gogram"
	"github.com/amarnathcjd/gogram/telegram"
)

func TestIsPermanentVaultError(t *testing.T) {
	t.Parallel()
	permanent := []error{
		ErrVaultMessageMissing,
		ErrVaultMessageNoMedia,
		ErrVaultRecordMismatch,
		fmt.Errorf("wrapped: %w", &gogram.ErrResponseCode{Message: "MESSAGE_ID_INVALID"}),
		fmt.Errorf("wrapped: %w", &gogram.ErrResponseCode{Message: "MESSAGE_DELETED"}),
		&telegram.RpcError{Message: "MESSAGE_ID_INVALID"},
	}
	for _, err := range permanent {
		if !isPermanentVaultError(err) {
			t.Errorf("isPermanentVaultError(%v) = false, want true", err)
		}
	}

	transient := []error{
		&gogram.ErrResponseCode{Message: "FLOOD_WAIT_10"},
		&gogram.ErrResponseCode{Message: "CHAT_ADMIN_REQUIRED"},
		fmt.Errorf("network timeout"),
	}
	for _, err := range transient {
		if isPermanentVaultError(err) {
			t.Errorf("isPermanentVaultError(%v) = true, want false", err)
		}
	}
}

func TestMaxNativeIntPositive(t *testing.T) {
	t.Parallel()
	if got := maxNativeInt(); got <= 0 {
		t.Errorf("maxNativeInt() = %d, want positive", got)
	}
}
