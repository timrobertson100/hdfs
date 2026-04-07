package transfer

import (
	"testing"

	hadoop_common "github.com/timrobertson100/hdfs/v2/internal/protocol/hadoop_common"
	hdfs "github.com/timrobertson100/hdfs/v2/internal/protocol/hadoop_hdfs"
	"google.golang.org/protobuf/proto"
)

// stubBlock returns a minimal LocatedBlockProto with the given numBytes, for
// use in internalBlockLength tests.
func stubBlock(numBytes uint64) *hdfs.LocatedBlockProto {
	return &hdfs.LocatedBlockProto{
		B: &hdfs.ExtendedBlockProto{
			PoolId:          proto.String("test-pool"),
			BlockId:         proto.Uint64(1),
			GenerationStamp: proto.Uint64(1),
			NumBytes:        proto.Uint64(numBytes),
		},
		Offset:     proto.Uint64(0),
		Corrupt:    proto.Bool(false),
		BlockToken: &hadoop_common.TokenProto{},
	}
}

// sbr is a helper that builds a StripedBlockReader with the given parameters
// without connecting to any datanodes.
func sbr(numBytes uint64, cellSize, numDataUnits int) *StripedBlockReader {
	return &StripedBlockReader{
		Block:        stubBlock(numBytes),
		cellSize:     cellSize,
		numDataUnits: numDataUnits,
	}
}

// TestInternalBlockLength mirrors the examples from Hadoop's
// StripedBlockUtil.getInternalBlockLength / lastCellSize.
func TestInternalBlockLength(t *testing.T) {
	const MB = 1 << 20

	tests := []struct {
		name         string
		numBytes     uint64 // logical block size
		cellSize     int
		numDataUnits int
		unitIndex    int
		want         int64
	}{
		// ── Exact stripe boundary ────────────────────────────────────────────
		// RS(3,2), cell=1MB, data=3MB → 1 full stripe, each unit holds 1MB.
		{
			name: "exact stripe boundary RS3 unit0",
			numBytes: 3 * MB, cellSize: MB, numDataUnits: 3, unitIndex: 0,
			want: MB,
		},
		{
			name: "exact stripe boundary RS3 unit2",
			numBytes: 3 * MB, cellSize: MB, numDataUnits: 3, unitIndex: 2,
			want: MB,
		},
		// RS(6,3), cell=1MB, data=6MB → 1 full stripe.
		{
			name: "exact stripe RS6 unit5",
			numBytes: 6 * MB, cellSize: MB, numDataUnits: 6, unitIndex: 5,
			want: MB,
		},

		// ── Partial last stripe ──────────────────────────────────────────────
		// RS(3,2), cell=1MB, data=23193677B (from the issue).
		// stripeSize=3MB, lastStripe=23193677%3145728=1173581B
		//   unit0: (7)*1MB + min(1MB, 1173581)       = 7*1MB+1MB   = 8388608
		//   unit1: (7)*1MB + min(1MB, 1173581-1MB)   = 7*1MB+125005 = 7465037
		//   unit2: (7)*1MB + min(1MB, max(0,…-2MB))  = 7*1MB+0      = 7340032
		{
			name: "issue repro RS3 unit0",
			numBytes: 23193677, cellSize: MB, numDataUnits: 3, unitIndex: 0,
			want: 8388608,
		},
		{
			name: "issue repro RS3 unit1",
			numBytes: 23193677, cellSize: MB, numDataUnits: 3, unitIndex: 1,
			want: 7465037,
		},
		{
			name: "issue repro RS3 unit2",
			numBytes: 23193677, cellSize: MB, numDataUnits: 3, unitIndex: 2,
			want: 7340032,
		},

		// ── Small file, less than one cell ───────────────────────────────────
		// RS(3,2), cell=1MB, data=500B → only unit0 has data, others empty.
		{
			name: "sub-cell file unit0",
			numBytes: 500, cellSize: MB, numDataUnits: 3, unitIndex: 0,
			want: 500,
		},
		{
			name: "sub-cell file unit1",
			numBytes: 500, cellSize: MB, numDataUnits: 3, unitIndex: 1,
			want: 0,
		},

		// ── Multi-stripe, partial last stripe ───────────────────────────────
		// RS(3,2), cell=1MB, data=5MB.
		// stripeSize=3MB, numStripes=2, lastStripe=2MB
		//   unit0: 1*1MB + min(1MB, 2MB)     = 2MB
		//   unit1: 1*1MB + min(1MB, 1MB)     = 2MB
		//   unit2: 1*1MB + min(1MB, 0)       = 1MB
		{
			name: "5MB RS3 unit0",
			numBytes: 5 * MB, cellSize: MB, numDataUnits: 3, unitIndex: 0,
			want: 2 * MB,
		},
		{
			name: "5MB RS3 unit1",
			numBytes: 5 * MB, cellSize: MB, numDataUnits: 3, unitIndex: 1,
			want: 2 * MB,
		},
		{
			name: "5MB RS3 unit2",
			numBytes: 5 * MB, cellSize: MB, numDataUnits: 3, unitIndex: 2,
			want: MB,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			r := sbr(tc.numBytes, tc.cellSize, tc.numDataUnits)
			got := r.internalBlockLength(tc.unitIndex)
			if got != tc.want {
				t.Errorf("internalBlockLength(%d B, cell=%d, k=%d, unit=%d) = %d, want %d",
					tc.numBytes, tc.cellSize, tc.numDataUnits, tc.unitIndex, got, tc.want)
			}
		})
	}
}
