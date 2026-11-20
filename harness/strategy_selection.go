package harness

import "encoding/json"

// StrategySelection selects one registered context strategy by name with its
// opaque settings; an empty ID selects the included strategy.
type StrategySelection struct {
	ID       string          `json:"id"`
	Settings json.RawMessage `json:"-"`
}

func (v StrategySelection) MarshalJSON() ([]byte, error) {
	var settings json.RawMessage
	if len(v.Settings) != 0 {
		settings = frame(v.Settings)
	}
	return json.Marshal(struct {
		ID       string          `json:"id"`
		Settings json.RawMessage `json:"settings_base64,omitempty"`
	}{ID: v.ID, Settings: settings})
}

func (v *StrategySelection) UnmarshalJSON(data []byte) error {
	members, err := decodePayloadObject(data)
	if err != nil {
		return err
	}
	if err := rejectUnknownMembers(members, "id", "settings_base64"); err != nil {
		return err
	}
	id, err := stringMember(members, "id", true)
	if err != nil {
		return err
	}
	var settings []byte
	if _, present := members["settings_base64"]; present {
		settings, err = framedMember(members, "settings_base64", true)
		if err != nil {
			return err
		}
	}
	v.ID, v.Settings = id, json.RawMessage(settings)
	return nil
}
