package lib

import (
	"fmt"
	"time"
)

type SnowflakeId int64

type SnowflakeBitConfig struct {
	PrefixBits    uint8
	TimeStampBits uint8
	MachineIDBits uint8
	SequenceBits  uint8
	Prefix        uint8     // プレフィックス
	MeasureTime   time.Time // 基準時刻
	MachineID     int64     // マシンID
}

func CheckSnowflakeConfig(cfg SnowflakeBitConfig) error {
	totalBits := cfg.PrefixBits + cfg.TimeStampBits + cfg.MachineIDBits + cfg.SequenceBits
	if totalBits != 64 {
		return fmt.Errorf("invalid SnowflakeConfig: total bits must be 64, got %d", totalBits)
	}
	return nil
}

func DefaultSnowflakeConfig() SnowflakeBitConfig {
	return SnowflakeBitConfig{
		PrefixBits:    1,
		TimeStampBits: 41,
		MachineIDBits: 10,
		SequenceBits:  12,
		Prefix:        0,
		MeasureTime:   time.Date(2015, 1, 1, 0, 0, 0, 0, time.UTC),
		MachineID:     123,
	}
}

func GenerateSnowflakeID(cfg SnowflakeBitConfig, timestamp time.Time, sequence uint8) (SnowflakeId, error) {
	if err := CheckSnowflakeConfig(cfg); err != nil {
		return 0, err
	}
	timeDiff := timestamp.Sub(cfg.MeasureTime).Milliseconds()
	if timeDiff < 0 {
		return 0, fmt.Errorf("timestamp is before measure time")
	}

	var id int64
	id |= (int64(cfg.Prefix) & ((1 << cfg.PrefixBits) - 1)) << (cfg.TimeStampBits + cfg.MachineIDBits + cfg.SequenceBits)
	id |= (timeDiff & ((1 << cfg.TimeStampBits) - 1)) << (cfg.MachineIDBits + cfg.SequenceBits)
	id |= (cfg.MachineID & ((1 << cfg.MachineIDBits) - 1)) << cfg.SequenceBits
	id |= int64(sequence & ((1 << cfg.SequenceBits) - 1))
	return SnowflakeId(id), nil
}

func (id SnowflakeId) String() string {
	return fmt.Sprintf("%d", int64(id))
}

func (id SnowflakeId) Int64() int64 {
	return int64(id)
}

func ParseSnowflakeID(id int64) SnowflakeId {
	return SnowflakeId(id)
}

func (id SnowflakeId) Decompose(cfg SnowflakeBitConfig) (prefix uint8, timestamp time.Time, machineID int64, sequence uint8) {
	sequence = uint8(int64(id) & ((1 << cfg.SequenceBits) - 1))
	machineID = (int64(id) >> cfg.SequenceBits) & ((1 << cfg.MachineIDBits) - 1)
	timeDiff := (int64(id) >> (cfg.MachineIDBits + cfg.SequenceBits)) & ((1 << cfg.TimeStampBits) - 1)
	prefix = uint8((int64(id) >> (cfg.TimeStampBits + cfg.MachineIDBits + cfg.SequenceBits)) & ((1 << cfg.PrefixBits) - 1))
	timestamp = cfg.MeasureTime.Add(time.Duration(timeDiff) * time.Millisecond)
	return
}

func (id SnowflakeId) GetPrefix(cfg SnowflakeBitConfig) uint8 {
	prefix, _, _, _ := id.Decompose(cfg)
	return prefix
}

func (id SnowflakeId) GetTimestamp(cfg SnowflakeBitConfig) time.Time {
	_, timestamp, _, _ := id.Decompose(cfg)
	return timestamp
}

func (id SnowflakeId) GetMachineID(cfg SnowflakeBitConfig) int64 {
	_, _, machineID, _ := id.Decompose(cfg)
	return machineID
}

func (id SnowflakeId) GetSequence(cfg SnowflakeBitConfig) uint8 {
	_, _, _, sequence := id.Decompose(cfg)
	return sequence
}
