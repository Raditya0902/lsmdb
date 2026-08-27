// Package kvstate adapts committed Raft commands to the LSM engine.
package kvstate

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	lsmdbv1 "lsmdb/api/lsmdb/v1"
	"lsmdb/db"

	"google.golang.org/protobuf/proto"
)

var snapshotMagic = [8]byte{'L', 'S', 'M', 'S', 'N', 'A', 'P', 1}

const (
	userPrefix    = "\x01"
	sessionPrefix = "\x00session/"
	MaxKeyBytes   = 16 << 10
	MaxValueBytes = 4 << 20
)

// Machine stores user data and retry-deduplication metadata in separate key namespaces.
type Machine struct{ db *db.DB }

// Open opens a persistent replica-mode state machine.
func Open(path string, options *db.Options) (*Machine, error) {
	if options == nil {
		options = db.DefaultOptions()
	}
	copy := *options
	copy.DurabilityMode = db.DurabilityReplica
	database, err := db.Open(path, &copy)
	if err != nil {
		return nil, err
	}
	return &Machine{db: database}, nil
}

// EncodeCommand validates and serializes a mutation for the replicated log.
func EncodeCommand(operation lsmdbv1.Command_Operation, key, value []byte, clientID string, requestSeq uint64) ([]byte, error) {
	if operation != lsmdbv1.Command_OPERATION_PUT && operation != lsmdbv1.Command_OPERATION_DELETE {
		return nil, errors.New("command operation must be put or delete")
	}
	if len(key) == 0 || len(key) > MaxKeyBytes {
		return nil, fmt.Errorf("key length must be between 1 and %d bytes", MaxKeyBytes)
	}
	if len(value) > MaxValueBytes {
		return nil, fmt.Errorf("value exceeds %d bytes", MaxValueBytes)
	}
	if clientID == "" || requestSeq == 0 {
		return nil, errors.New("client ID and positive request sequence are required")
	}
	return proto.Marshal(&lsmdbv1.Command{
		Operation: operation, Key: append([]byte(nil), key...), Value: append([]byte(nil), value...),
		ClientId: clientID, RequestSeq: requestSeq,
	})
}

// Apply deterministically applies one committed log entry.
func (m *Machine) Apply(index uint64, data []byte) error {
	if len(data) == 0 {
		return m.db.ApplyBatch(index, nil)
	}
	var command lsmdbv1.Command
	if err := proto.Unmarshal(data, &command); err != nil {
		return fmt.Errorf("decode state-machine command: %w", err)
	}
	if command.Operation != lsmdbv1.Command_OPERATION_PUT && command.Operation != lsmdbv1.Command_OPERATION_DELETE {
		return fmt.Errorf("unsupported state-machine operation %s", command.Operation)
	}
	if command.ClientId == "" || command.RequestSeq == 0 {
		return errors.New("committed command has invalid request identity")
	}

	sessionKey := sessionPrefix + command.ClientId
	if raw, ok := m.db.Get(sessionKey); ok && len(raw) == 8 {
		if command.RequestSeq <= binary.BigEndian.Uint64(raw) {
			return m.db.ApplyBatch(index, nil)
		}
	}

	mutation := db.Mutation{Type: db.MutationPut, Key: userPrefix + string(command.Key), Value: command.Value}
	if command.Operation == lsmdbv1.Command_OPERATION_DELETE {
		mutation.Type = db.MutationDelete
		mutation.Value = nil
	}
	sequence := make([]byte, 8)
	binary.BigEndian.PutUint64(sequence, command.RequestSeq)
	return m.db.ApplyBatch(index, []db.Mutation{
		mutation,
		{Type: db.MutationPut, Key: sessionKey, Value: sequence},
	})
}

// Get returns a user value without exposing internal metadata keys.
func (m *Machine) Get(key []byte) ([]byte, bool, error) {
	if len(key) == 0 || len(key) > MaxKeyBytes {
		return nil, false, fmt.Errorf("key length must be between 1 and %d bytes", MaxKeyBytes)
	}
	value, ok := m.db.Get(userPrefix + string(key))
	return value, ok, nil
}

// Snapshot serializes all user and client-session state at the applied index.
func (m *Machine) Snapshot() (uint64, []byte, error) {
	index := m.db.AppliedIndex()
	if index == 0 {
		return 0, nil, errors.New("cannot snapshot an unapplied state machine")
	}
	pairs, err := m.db.ExportReplicaState()
	if err != nil {
		return 0, nil, err
	}
	var out bytes.Buffer
	out.Write(snapshotMagic[:])
	if err := binary.Write(&out, binary.BigEndian, uint64(len(pairs))); err != nil {
		return 0, nil, err
	}
	for _, pair := range pairs {
		if uint64(len(pair.Key)) > uint64(^uint32(0)) || uint64(len(pair.Value)) > uint64(^uint32(0)) {
			return 0, nil, errors.New("snapshot record is too large")
		}
		_ = binary.Write(&out, binary.BigEndian, uint32(len(pair.Key)))
		_ = binary.Write(&out, binary.BigEndian, uint32(len(pair.Value)))
		out.WriteString(pair.Key)
		out.Write(pair.Value)
	}
	return index, out.Bytes(), nil
}

// Restore atomically replaces the local LSM image with a received snapshot.
func (m *Machine) Restore(index uint64, data []byte) error {
	reader := bytes.NewReader(data)
	var magic [8]byte
	if _, err := io.ReadFull(reader, magic[:]); err != nil || magic != snapshotMagic {
		return errors.New("invalid state snapshot magic")
	}
	var count uint64
	if err := binary.Read(reader, binary.BigEndian, &count); err != nil {
		return fmt.Errorf("decode snapshot count: %w", err)
	}
	if count > uint64(len(data))/8 {
		return errors.New("invalid state snapshot record count")
	}
	pairs := make([]db.KVPair, 0, count)
	for i := uint64(0); i < count; i++ {
		var keyLen, valueLen uint32
		if err := binary.Read(reader, binary.BigEndian, &keyLen); err != nil {
			return fmt.Errorf("decode snapshot key length: %w", err)
		}
		if err := binary.Read(reader, binary.BigEndian, &valueLen); err != nil {
			return fmt.Errorf("decode snapshot value length: %w", err)
		}
		if uint64(keyLen)+uint64(valueLen) > uint64(reader.Len()) {
			return errors.New("truncated state snapshot record")
		}
		key := make([]byte, keyLen)
		value := make([]byte, valueLen)
		if _, err := io.ReadFull(reader, key); err != nil {
			return err
		}
		if _, err := io.ReadFull(reader, value); err != nil {
			return err
		}
		pairs = append(pairs, db.KVPair{Key: string(key), Value: value})
	}
	if reader.Len() != 0 {
		return errors.New("state snapshot has trailing data")
	}
	return m.db.ReplaceReplicaState(index, pairs)
}

func (m *Machine) AppliedIndex() uint64 { return m.db.AppliedIndex() }
func (m *Machine) DurableIndex() uint64 { return m.db.DurableIndex() }
func (m *Machine) Close() error         { return m.db.Close() }
