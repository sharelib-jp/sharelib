package lib

import (
	"testing"
	"time"
)

// デフォルトの設定で Snowflake ID が正しく生成されることを確認するテスト
func TestSnowflakeIDGenerationWithDefaultConfig(t *testing.T) {
	cfg := DefaultSnowflakeConfig()
	timestamp := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	sequence := uint16(5)

	id, err := GenerateSnowflakeID(cfg, timestamp, sequence)
	if err != nil {
		t.Fatalf("Failed to generate Snowflake ID: %v", err)
	}

	if id.Int64() != 1456074443981303813 {
		t.Fatalf("Generated Snowflake ID does not match expected value, got %d", id.Int64())
	}
}

// デフォルトの設定で Snowflake ID が正しく復元できることのテスト
func TestSnowflakeIDDecompositionWithDefaultConfig(t *testing.T) {
	cfg := DefaultSnowflakeConfig()
	timestamp := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	sequence := uint16(10)

	id, err := GenerateSnowflakeID(cfg, timestamp, sequence)
	if err != nil {
		t.Fatalf("Failed to generate Snowflake ID: %v", err)
	}

	prefix := id.GetPrefix(cfg)
	if prefix != cfg.Prefix {
		t.Fatalf("Prefix does not match, got %d, want %d", prefix, cfg.Prefix)
	}

	ts := id.GetTimestamp(cfg)
	if !ts.Equal(timestamp) {
		t.Fatalf("Timestamp does not match, got %v, want %v", ts, timestamp)
	}

	machineID := id.GetMachineID(cfg)
	if machineID != cfg.MachineID {
		t.Fatalf("MachineID does not match, got %d, want %d", machineID, cfg.MachineID)
	}

	seq := id.GetSequence(cfg)
	if seq != sequence {
		t.Fatalf("Sequence does not match, got %d, want %d", seq, sequence)
	}
}
