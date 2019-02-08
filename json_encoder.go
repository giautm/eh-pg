package ehpg

import (
	"encoding/json"

	eh "github.com/looplab/eventhorizon"
)

type Encoder interface {
	Marshal(eh.EventData) ([]byte, error)
	Unmarshal(eh.EventType, []byte) (eh.EventData, error)
	String() string
}

type jsonEncoder struct{}

func (jsonEncoder) Marshal(data eh.EventData) ([]byte, error) {
	if data != nil {
		return json.Marshal(data)
	}
	return nil, nil
}

func (jsonEncoder) Unmarshal(eventType eh.EventType, raw []byte) (data eh.EventData, err error) {
	if len(raw) == 0 {
		return nil, nil
	}
	if data, err = eh.CreateEventData(eventType); err == nil {
		if err = json.Unmarshal(raw, data); err == nil {
			return data, nil
		}
	}
	return nil, err
}

func (jsonEncoder) String() string {
	return "json"
}
