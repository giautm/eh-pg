package ehpg

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/go-pg/pg"
	"github.com/go-pg/pg/orm"
	"github.com/google/uuid"
	eh "github.com/looplab/eventhorizon"
)

// ErrCouldNotClearDB is when the database could not be cleared.
var ErrCouldNotClearDB = errors.New("could not clear database")

// ErrConflictVersion is when a version conflict occurs when saving an aggregate.
var ErrVersionConflict = errors.New("Can not create/update aggregate")

// ErrCouldNotMarshalEvent is when an event could not be marshaled into JSON.
var ErrCouldNotMarshalEvent = errors.New("could not marshal event")

// ErrCouldNotUnmarshalEvent is when an event could not be unmarshaled into a concrete type.
var ErrCouldNotUnmarshalEvent = errors.New("could not unmarshal event")

// ErrCouldNotSaveAggregate is when an aggregate could not be saved.
var ErrCouldNotSaveAggregate = errors.New("could not save aggregate")

// EventStore implements an eh.EventStore for PostgreSQL.
type EventStore struct {
	db      *pg.DB
	encoder Encoder
}

var _ = eh.EventStore(&EventStore{})

type AggregateRecord struct {
	tableName struct{} `sql:"aggregates"`

	Namespace   string    `sql:"namespace,type:varchar(250),pk"`
	AggregateID uuid.UUID `sql:"aggregate_id,type:uuid,pk"`
	Version     int       `sql:"version"`
}

type AggregateEvent struct {
	tableName struct{} `sql:"event_store"`

	EventID       uuid.UUID              `sql:"event_id,type:uuid,pk"`
	Namespace     string                 `sql:"namespace,type:varchar(250)"`
	AggregateID   uuid.UUID              `sql:"aggregate_id,type:uuid"`
	AggregateType eh.AggregateType       `sql:"aggregate_type,type:varchar(250)"`
	EventType     eh.EventType           `sql:"event_type,type:varchar(250)"`
	RawData       json.RawMessage        `sql:"data,type:jsonb"`
	Timestamp     time.Time              `sql:"timestamp"`
	Version       int                    `sql:"version"`
	Context       map[string]interface{} `sql:"context"`
	data          eh.EventData           `sql:"-"`
}

// NewUUID for mocking in tests
var NewUUID = uuid.New

// newDBEvent returns a new dbEvent for an event.
func (s *EventStore) newDBEvent(ctx context.Context, event eh.Event) (*AggregateEvent, error) {
	ns := eh.NamespaceFromContext(ctx)

	// Marshal event data if there is any.
	raw, err := s.encoder.Marshal(event.Data())
	if err != nil {
		return nil, eh.EventStoreError{
			BaseErr:   err,
			Err:       ErrCouldNotMarshalEvent,
			Namespace: ns,
		}
	}

	return &AggregateEvent{
		EventID:       NewUUID(),
		AggregateID:   event.AggregateID(),
		AggregateType: event.AggregateType(),
		EventType:     event.EventType(),
		RawData:       raw,
		Timestamp:     event.Timestamp(),
		Version:       event.Version(),
		Context:       eh.MarshalContext(ctx),
		Namespace:     ns,
	}, nil
}

// NewEventStore creates a new EventStore.
func NewEventStore(db *pg.DB) (*EventStore, error) {
	s := &EventStore{
		db:      db,
		encoder: &jsonEncoder{},
	}
	err := s.CreateTables(&orm.CreateTableOptions{
		IfNotExists: true,
	})
	if err != nil {
		return nil, err
	}
	return s, nil
}

var tables = []interface{}{
	(*AggregateEvent)(nil),
	(*AggregateRecord)(nil),
}

func (s *EventStore) CreateTables(opts *orm.CreateTableOptions) error {
	return s.db.RunInTransaction(func(tx *pg.Tx) error {
		for _, table := range tables {
			if err := s.db.CreateTable(table, opts); err != nil {
				return err
			}
		}

		return nil
	})
}

// Save implements the Save method of the eventhorizon.EventStore interface.
func (s *EventStore) Save(ctx context.Context, events []eh.Event, originalVersion int) error {
	ns := eh.NamespaceFromContext(ctx)

	if len(events) == 0 {
		return eh.EventStoreError{
			Err:       eh.ErrNoEventsToAppend,
			Namespace: ns,
		}
	}

	// Build all event records, with incrementing versions starting from the
	// original aggregate version.
	dbEvents := make([]AggregateEvent, len(events))
	aggregateID := events[0].AggregateID()
	version := originalVersion
	for i, event := range events {
		// Only accept events belonging to the same aggregate.
		if event.AggregateID() != aggregateID {
			return eh.EventStoreError{
				Err:       eh.ErrInvalidEvent,
				Namespace: ns,
			}
		}

		// Only accept events that apply to the correct aggregate version.
		if event.Version() != version+1 {
			return eh.EventStoreError{
				Err:       eh.ErrIncorrectEventVersion,
				Namespace: ns,
			}
		}

		// Create the event record for the DB.
		e, err := s.newDBEvent(ctx, event)
		if err != nil {
			return err
		}
		dbEvents[i] = *e
		version++
	}

	// Either insert a new aggregate or append to an existing.
	err := s.db.WithParam("namespace", ns).
		WithContext(ctx).
		RunInTransaction(func(tx *pg.Tx) (err error) {
			var result orm.Result
			aggregate := AggregateRecord{
				Namespace:   ns,
				AggregateID: aggregateID,
			}
			if originalVersion == 0 {
				aggregate.Version = len(dbEvents)
				result, err = tx.Model(&aggregate).
					Insert()
			} else {
				result, err = tx.Model(&aggregate).
					Where("namespace = ?namespace AND version = ? AND aggregate_id = ?", originalVersion, aggregateID).
					Set("version = version + ?", len(dbEvents)).
					Update()
			}
			if err != nil {
				return err
			}
			if result.RowsAffected() != 1 {
				return ErrVersionConflict
			}
			if err = tx.Insert(&dbEvents); err != nil {
				return err
			}
			return nil
		})
	if err != nil {
		return eh.EventStoreError{
			BaseErr:   err,
			Err:       ErrCouldNotSaveAggregate,
			Namespace: ns,
		}
	}

	return nil
}

// Load implements the Load method of the eventhorizon.EventStore interface.
func (s *EventStore) Load(ctx context.Context, id uuid.UUID) ([]eh.Event, error) {
	ns := eh.NamespaceFromContext(ctx)

	var events []eh.Event
	err := s.db.WithParam("namespace", ns).
		WithContext(ctx).
		Model((*AggregateEvent)(nil)).
		Where("namespace = ?namespace AND aggregate_id = ?", id).
		Order("version ASC").
		ForEach(func(e *AggregateEvent) (err error) {
			if e.data, err = s.encoder.Unmarshal(e.EventType, e.RawData); err != nil {
				return eh.EventStoreError{
					BaseErr:   err,
					Err:       ErrCouldNotUnmarshalEvent,
					Namespace: ns,
				}
			}
			e.RawData = nil
			events = append(events, event{
				AggregateEvent: *e,
			})
			return nil
		})
	if err != nil {
		return nil, eh.EventStoreError{
			BaseErr:   err,
			Err:       err,
			Namespace: ns,
		}
	}

	return events, nil
}

// Replace an event, the version must match. Useful for maintenance actions.
// Returns ErrAggregateNotFound if there is no aggregate.
func (s *EventStore) Replace(ctx context.Context, event eh.Event) error {
	ns := eh.NamespaceFromContext(ctx)

	exist, err := s.db.WithParam("namespace", ns).
		WithContext(ctx).
		Model((*AggregateRecord)(nil)).
		Where("namespace = ?namespace").
		Where("aggregate_id = ?", event.AggregateID()).
		Exists()
	if err != nil {
		return eh.EventStoreError{
			BaseErr:   err,
			Err:       err,
			Namespace: ns,
		}
	} else if !exist {
		return eh.ErrAggregateNotFound
	}

	var eventID uuid.UUID
	err = s.db.WithParam("namespace", ns).
		WithContext(ctx).
		Model((*AggregateEvent)(nil)).
		Where("namespace = ?namespace").
		Where("aggregate_id = ? AND version = ?", event.AggregateID(), event.Version()).
		Column("event_id").
		Select(&eventID)
	if err == nil {
		// Create the event record for the DB.
		e, err := s.newDBEvent(ctx, event)
		if err != nil {
			return eh.EventStoreError{
				BaseErr:   err,
				Err:       err,
				Namespace: ns,
			}
		}
		e.EventID = eventID
		err = s.db.Update(e)
	}
	if err == pg.ErrNoRows {
		return eh.ErrInvalidEvent
	} else if err != nil {
		return eh.EventStoreError{
			BaseErr:   err,
			Err:       err,
			Namespace: ns,
		}
	}

	return nil
}

// RenameEvent renames all instances of the event type.
func (s *EventStore) RenameEvent(ctx context.Context, from, to eh.EventType) error {
	ns := eh.NamespaceFromContext(ctx)

	_, err := s.db.WithParam("namespace", ns).
		WithContext(ctx).
		Model((*AggregateEvent)(nil)).
		Where("namespace = ?namespace AND event_type = ?", from).
		Set("event_type = ?", to).
		Update()
	if err != nil {
		return eh.EventStoreError{
			BaseErr:   err,
			Err:       ErrCouldNotSaveAggregate,
			Namespace: ns,
		}
	}

	return nil
}

func (s *EventStore) Close() error {
	return s.db.Close()
}

// Clear clears the event storage.
func (s *EventStore) Clear(ctx context.Context) error {
	ns := eh.NamespaceFromContext(ctx)

	err := s.db.WithParam("namespace", ns).
		WithContext(ctx).
		RunInTransaction(func(tx *pg.Tx) (err error) {
			_, err = tx.Model((*AggregateRecord)(nil)).
				Where("namespace = ?namespace").
				Delete()
			if err == nil {
				_, err = tx.Model((*AggregateEvent)(nil)).
					Where("namespace = ?namespace").
					Delete()
			}
			return err
		})
	if err != nil {
		return eh.EventStoreError{
			BaseErr:   err,
			Err:       ErrCouldNotClearDB,
			Namespace: ns,
		}
	}
	return nil
}

// event is the private implementation of the eventhorizon.Event interface
// for a MongoDB event store.
type event struct {
	AggregateEvent
}

// AggrgateID implements the AggrgateID method of the eventhorizon.Event interface.
func (e event) AggregateID() uuid.UUID {
	return e.AggregateEvent.AggregateID
}

// AggregateType implements the AggregateType method of the eventhorizon.Event interface.
func (e event) AggregateType() eh.AggregateType {
	return e.AggregateEvent.AggregateType
}

// EventType implements the EventType method of the eventhorizon.Event interface.
func (e event) EventType() eh.EventType {
	return e.AggregateEvent.EventType
}

// Data implements the Data method of the eventhorizon.Event interface.
func (e event) Data() eh.EventData {
	return e.AggregateEvent.data
}

// Version implements the Version method of the eventhorizon.Event interface.
func (e event) Version() int {
	return e.AggregateEvent.Version
}

// Timestamp implements the Timestamp method of the eventhorizon.Event interface.
func (e event) Timestamp() time.Time {
	return e.AggregateEvent.Timestamp
}

// String implements the String method of the eventhorizon.Event interface.
func (e event) String() string {
	return fmt.Sprintf("%s@%d", e.AggregateEvent.EventType, e.AggregateEvent.Version)
}
