package transfer

import (
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"net"
	"time"

	hadoop_common "github.com/timrobertson100/hdfs/v2/internal/protocol/hadoop_common"
	hdfs "github.com/timrobertson100/hdfs/v2/internal/protocol/hadoop_hdfs"
	"google.golang.org/protobuf/proto"
)

// BlockReadCloser is the common interface for block readers used by FileReader.
// Both BlockReader and StripedBlockReader satisfy this interface.
type BlockReadCloser interface {
	io.ReadCloser
	// Skip attempts to discard n bytes in the current stream without
	// reconnecting. It returns an error if the skip cannot be performed
	// in-stream (the caller should then close and reconnect at the new offset).
	Skip(n int64) error
	// SetDeadline sets the deadline on the underlying network connection(s).
	SetDeadline(t time.Time) error
}

// StripedBlockReader reads from an erasure-coded striped block group.
//
// HDFS stripes data across k data-unit datanodes in cell-sized pieces. To
// reconstruct the logical byte stream, we open one TCP connection per data
// unit and read cell-by-cell, cycling through datanodes in stripe order:
//
//	cell_0 (unit 0), cell_1 (unit 1), ..., cell_{k-1} (unit k-1),
//	cell_k (unit 0), cell_{k+1} (unit 1), ...
//
// This mirrors the approach used by Hadoop's DFSStripedInputStream /
// StripeReader, simplified to the happy path (all data units available).
//
// Terminology follows StripedBlockUtil.java:
//   - stripeSize = cellSize × numDataUnits
//   - stripe     = one full round of k cells
type StripedBlockReader struct {
	// ClientName is the client identifier sent to datanodes.
	ClientName string
	// Block is the located block group as returned by the namenode.
	Block *hdfs.LocatedBlockProto
	// Offset is the starting logical offset within the block group.
	Offset int64
	// ECPolicy contains the cell size and data/parity unit counts.
	ECPolicy *hdfs.ErasureCodingPolicyProto
	// UseDatanodeHostname controls whether to connect via hostname or IP.
	UseDatanodeHostname bool
	// DialFunc is the base TCP dialer. If nil, (&net.Dialer{}).DialContext
	// is used.
	DialFunc func(ctx context.Context, network, addr string) (net.Conn, error)
	// WrapDialFunc, if non-nil, wraps DialFunc with per-token authentication
	// (e.g. SASL). It is called once per data unit with that unit's block
	// token, so each connection is authenticated with the correct credentials.
	WrapDialFunc func(
		dc func(ctx context.Context, network, addr string) (net.Conn, error),
		token *hadoop_common.TokenProto,
	) (func(ctx context.Context, network, addr string) (net.Conn, error), error)

	cellSize     int
	numDataUnits int

	// conns and streams are indexed by data unit index (0 … numDataUnits-1).
	// A nil entry means no data for that unit at or after the start offset.
	conns   []net.Conn
	streams []*blockReadStream

	logicalOffset int64
	closed        bool
	deadline      time.Time
}

// internalBlockLength returns the byte length of the internal block held by
// data unit unitIndex. The formula is identical to Hadoop's
// StripedBlockUtil.getInternalBlockLength / lastCellSize.
func (br *StripedBlockReader) internalBlockLength(unitIndex int) int64 {
	numBytes := int64(br.Block.GetB().GetNumBytes())
	cellSize := int64(br.cellSize)
	stripeSize := cellSize * int64(br.numDataUnits)

	lastStripeDataLen := numBytes % stripeSize
	if lastStripeDataLen == 0 {
		return numBytes / int64(br.numDataUnits)
	}

	numStripes := (numBytes-1)/stripeSize + 1

	// lastCellSize: how much of the last stripe belongs to unit unitIndex.
	lastSize := lastStripeDataLen - int64(unitIndex)*cellSize
	if lastSize < 0 {
		lastSize = 0
	}
	if lastSize > cellSize {
		lastSize = cellSize
	}

	return (numStripes-1)*cellSize + lastSize
}

// init is called lazily on the first Read or Skip. It connects to every data
// unit and sets each stream to the correct internal starting offset.
func (br *StripedBlockReader) init() error {
	br.cellSize = int(br.ECPolicy.GetCellSize())
	br.numDataUnits = int(br.ECPolicy.GetSchema().GetDataUnits())

	if br.cellSize <= 0 {
		return fmt.Errorf("invalid EC cell size: %d", br.cellSize)
	}
	if br.numDataUnits <= 0 {
		return fmt.Errorf("invalid number of EC data units: %d", br.numDataUnits)
	}

	locs := br.Block.GetLocs()
	blockIndices := br.Block.GetBlockIndices()
	blockTokens := br.Block.GetBlockTokens()

	// Build a map from data-unit index to (DatanodeID, token).
	type unitInfo struct {
		id    *hdfs.DatanodeIDProto
		token *hadoop_common.TokenProto
	}
	units := make(map[int]unitInfo, br.numDataUnits)
	for i, loc := range locs {
		if i >= len(blockIndices) {
			break
		}
		idx := int(blockIndices[i])
		if _, exists := units[idx]; exists {
			continue // use first occurrence of each index
		}
		token := br.Block.GetBlockToken()
		if i < len(blockTokens) {
			token = blockTokens[i]
		}
		units[idx] = unitInfo{id: loc.GetId(), token: token}
	}

	// Determine each unit's starting internal offset given br.Offset.
	//
	// At logical offset X:
	//   stripeNum    = X / stripeSize
	//   posInStripe  = X % stripeSize
	//   cellInStripe = posInStripe / cellSize   (which unit owns the current cell)
	//   posInCell    = X % cellSize
	//
	// For unit i:
	//   i < cellInStripe  → its cell in stripe stripeNum was already consumed;
	//                        start at the beginning of the NEXT stripe.
	//   i == cellInStripe → we are mid-cell; start at stripe*cell + posInCell.
	//   i > cellInStripe  → its cell hasn't been read yet; start at stripe*cell.
	stripeSize := int64(br.cellSize * br.numDataUnits)
	stripeNum := br.Offset / stripeSize
	posInStripe := br.Offset % stripeSize
	cellInStripe := int(posInStripe / int64(br.cellSize))
	posInCell := br.Offset % int64(br.cellSize)

	br.conns = make([]net.Conn, br.numDataUnits)
	br.streams = make([]*blockReadStream, br.numDataUnits)

	for i := 0; i < br.numDataUnits; i++ {
		var internalStart int64
		switch {
		case i < cellInStripe:
			internalStart = (stripeNum + 1) * int64(br.cellSize)
		case i == cellInStripe:
			internalStart = stripeNum*int64(br.cellSize) + posInCell
		default:
			internalStart = stripeNum * int64(br.cellSize)
		}

		internalLen := br.internalBlockLength(i)
		if internalStart >= internalLen {
			// Nothing to read from this unit at or after the start offset.
			continue
		}

		info, ok := units[i]
		if !ok {
			br.closeAll()
			return fmt.Errorf("EC data unit %d not found in block locations", i)
		}

		conn, stream, err := br.connectToUnit(info.id, info.token, uint64(i), internalStart, internalLen)
		if err != nil {
			br.closeAll()
			return fmt.Errorf("connecting to EC data unit %d: %w", i, err)
		}
		br.conns[i] = conn
		br.streams[i] = stream
	}

	br.logicalOffset = br.Offset
	return nil
}

// connectToUnit opens a TCP connection to a single data-unit datanode, sends
// an OpReadBlockProto for the internal block, and returns the ready connection
// and its associated blockReadStream.
func (br *StripedBlockReader) connectToUnit(
	id *hdfs.DatanodeIDProto,
	token *hadoop_common.TokenProto,
	unitIndex uint64,
	internalOffset, internalLen int64,
) (net.Conn, *blockReadStream, error) {
	address := getDatanodeAddress(id, br.UseDatanodeHostname)

	dialFunc := br.DialFunc
	if dialFunc == nil {
		dialFunc = (&net.Dialer{}).DialContext
	}
	if br.WrapDialFunc != nil {
		var err error
		dialFunc, err = br.WrapDialFunc(dialFunc, token)
		if err != nil {
			return nil, nil, err
		}
	}

	conn, err := dialFunc(context.Background(), "tcp", address)
	if err != nil {
		return nil, nil, err
	}

	// Construct the internal block descriptor.
	// Internal block ID = group block ID + unit index (Hadoop convention).
	groupBlock := br.Block.GetB()
	internalBlock := proto.Clone(groupBlock).(*hdfs.ExtendedBlockProto)
	internalBlock.BlockId = proto.Uint64(groupBlock.GetBlockId() + unitIndex)
	internalBlock.NumBytes = proto.Uint64(uint64(internalLen))

	op := &hdfs.OpReadBlockProto{
		Header: &hdfs.ClientOperationHeaderProto{
			BaseHeader: &hdfs.BaseHeaderProto{
				Block: internalBlock,
				Token: token,
			},
			ClientName: proto.String(br.ClientName),
		},
		Offset:        proto.Uint64(uint64(internalOffset)),
		Len:           proto.Uint64(uint64(internalLen - internalOffset)),
		SendChecksums: proto.Bool(true),
	}

	if err := writeBlockOpRequest(conn, readBlockOp, op); err != nil {
		conn.Close()
		return nil, nil, err
	}

	resp, err := readBlockOpResponse(conn)
	if err != nil {
		conn.Close()
		return nil, nil, err
	}
	if resp.GetStatus() != hdfs.Status_SUCCESS {
		conn.Close()
		return nil, nil, fmt.Errorf("read failed: %s (%s)",
			resp.GetStatus().String(), resp.GetMessage())
	}

	readInfo := resp.GetReadOpChecksumInfo()
	checksumInfo := readInfo.GetChecksum()

	var checksumTab *crc32.Table
	var checksumSize int
	switch checksumInfo.GetType() {
	case hdfs.ChecksumTypeProto_CHECKSUM_CRC32:
		checksumTab = crc32.IEEETable
		checksumSize = 4
	case hdfs.ChecksumTypeProto_CHECKSUM_CRC32C:
		checksumTab = crc32.MakeTable(crc32.Castagnoli)
		checksumSize = 4
	case hdfs.ChecksumTypeProto_CHECKSUM_NULL:
		checksumTab = nil
		checksumSize = 0
	default:
		conn.Close()
		return nil, nil, fmt.Errorf("unsupported checksum type: %d", checksumInfo.GetType())
	}

	chunkOffset := int64(readInfo.GetChunkOffset())
	chunkSize := int(checksumInfo.GetBytesPerChecksum())
	stream := newBlockReadStream(conn, chunkSize, checksumTab, checksumSize)

	// The datanode starts the stream at a chunk boundary. Discard the bytes
	// before the requested internalOffset.
	if amountToDiscard := internalOffset - chunkOffset; amountToDiscard > 0 {
		if _, err := io.CopyN(io.Discard, stream, amountToDiscard); err != nil {
			if err == io.EOF {
				err = io.ErrUnexpectedEOF
			}
			conn.Close()
			return nil, nil, err
		}
	}

	if err := conn.SetDeadline(br.deadline); err != nil {
		conn.Close()
		return nil, nil, err
	}

	return conn, stream, nil
}

// SetDeadline sets the deadline on all open datanode connections.
func (br *StripedBlockReader) SetDeadline(t time.Time) error {
	br.deadline = t
	for _, conn := range br.conns {
		if conn != nil {
			if err := conn.SetDeadline(t); err != nil {
				return err
			}
		}
	}
	return nil
}

// Read implements io.Reader.
//
// It routes each read to the correct data-unit stream based on the current
// logical offset and limits the read to the remaining bytes in the current
// cell, so that streams are always advanced in stripe order.
func (br *StripedBlockReader) Read(b []byte) (int, error) {
	if br.closed {
		return 0, io.ErrClosedPipe
	}
	if len(b) == 0 {
		return 0, nil
	}

	if br.streams == nil {
		if err := br.init(); err != nil {
			return 0, err
		}
	}

	blockLen := int64(br.Block.GetB().GetNumBytes())
	if br.logicalOffset >= blockLen {
		return 0, io.EOF
	}

	stripeSize := int64(br.cellSize * br.numDataUnits)
	stripeNum := br.logicalOffset / stripeSize
	posInStripe := br.logicalOffset % stripeSize
	cellIdx := int(posInStripe / int64(br.cellSize))
	posInCell := br.logicalOffset % int64(br.cellSize)

	// Bytes remaining in this cell for the current data unit.
	internalLen := br.internalBlockLength(cellIdx)
	currentInternalOff := stripeNum*int64(br.cellSize) + posInCell
	cellRemaining := internalLen - currentInternalOff

	if cellRemaining <= 0 {
		// Should not happen for a well-formed block, but guard anyway.
		return 0, io.EOF
	}

	// Cap the read to avoid crossing a cell boundary (which would require
	// switching to a different stream mid-call).
	maxRead := int64(len(b))
	if maxRead > cellRemaining {
		maxRead = cellRemaining
	}

	if br.streams[cellIdx] == nil {
		return 0, fmt.Errorf("EC data unit %d stream not available", cellIdx)
	}

	n, err := br.streams[cellIdx].Read(b[:maxRead])
	br.logicalOffset += int64(n)

	if err == io.EOF {
		if br.logicalOffset >= blockLen {
			return n, io.EOF
		}
		// EOF before the end of the logical block is unexpected.
		return n, io.ErrUnexpectedEOF
	}

	return n, err
}

// Skip attempts to discard n bytes without reconnecting. It only handles
// skips that stay within the current cell; larger skips (or skips across cell
// boundaries) return an error so the caller can close and reconnect at the
// desired offset.
func (br *StripedBlockReader) Skip(n int64) error {
	if n == 0 {
		return nil
	}
	if br.streams == nil || n < 0 || n > maxSkip {
		return errors.New("unable to skip")
	}

	stripeSize := int64(br.cellSize * br.numDataUnits)
	stripeNum := br.logicalOffset / stripeSize
	posInStripe := br.logicalOffset % stripeSize
	cellIdx := int(posInStripe / int64(br.cellSize))
	posInCell := br.logicalOffset % int64(br.cellSize)

	// How many bytes actually remain in this cell (respects the internal
	// block length for the last, possibly-partial cell).
	internalLen := br.internalBlockLength(cellIdx)
	currentInternalOff := stripeNum*int64(br.cellSize) + posInCell
	cellRemaining := internalLen - currentInternalOff

	// Only skip within the current cell to keep stream positions consistent.
	if n > cellRemaining {
		return errors.New("skip crosses cell boundary in striped block")
	}

	if br.streams[cellIdx] == nil {
		return errors.New("no stream for current EC cell")
	}

	_, err := io.CopyN(io.Discard, br.streams[cellIdx], n)
	if err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		br.streams[cellIdx] = nil
		if br.conns[cellIdx] != nil {
			br.conns[cellIdx].Close()
			br.conns[cellIdx] = nil
		}
		return err
	}

	br.logicalOffset += n
	return nil
}

// Close implements io.Closer.
func (br *StripedBlockReader) Close() error {
	br.closed = true
	br.closeAll()
	return nil
}

func (br *StripedBlockReader) closeAll() {
	for i, conn := range br.conns {
		if conn != nil {
			conn.Close()
			br.conns[i] = nil
		}
	}
}
