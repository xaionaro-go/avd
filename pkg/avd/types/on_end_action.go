package types

import (
	"encoding/json"
	"fmt"
	"strings"
)

type OnEndAction int

const (
	OnEndActionCloseConsumers = OnEndAction(iota)
	OnEndActionWaitForNewPublisher
	EndOfOnEndAction
)

func (a OnEndAction) String() string {
	switch a {
	case OnEndActionCloseConsumers:
		return "close_consumers"
	case OnEndActionWaitForNewPublisher:
		return "wait_for_new_publisher"
	default:
		return fmt.Sprintf("unknown_action_%d", int(a))
	}
}

func (a *OnEndAction) UnmarshalYAML(b []byte) error {
	var modeString string
	if err := json.Unmarshal(b, &modeString); err != nil {
		return err
	}

	modeString = strings.Trim(strings.ToLower(modeString), " ")
	for candidate := OnEndAction(0); candidate < EndOfOnEndAction; candidate++ {
		if candidate.String() == modeString {
			*a = candidate
			return nil
		}
	}

	return fmt.Errorf("unknown OnEndAction: '%s'", modeString)
}

func (a OnEndAction) MarshalYAML() ([]byte, error) {
	return json.Marshal(a.String())
}
