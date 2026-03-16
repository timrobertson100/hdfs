package transfer

import (
	"context"
	"errors"
	"io"
	"net"
	"time"

	hadoop_common "github.com/colinmarc/hdfs/v2/internal/protocol/hadoop_common"
	hdfs "github.com/colinmarc/hdfs/v2/internal/protocol/hadoop_hdfs"
)

// ChecksumReader provides an interface for reading the "MD5CRC32" checksums of
// individual blocks. It abstracts over reading from multiple datanodes, in
// order to be robust to failures.
type ChecksumReader struct {
	// Block is the block location provided by the namenode.
	Block *hdfs.LocatedBlockProto
	// UseDatanodeHostname specifies whether the datanodes should be connected to
	// via their hostnames (if true) or IP addresses (if false).
	UseDatanodeHostname bool
	// DialFunc is used to connect to the datanodes. If nil, then (&net.Dialer{}).DialContext is used
	DialFunc func(ctx context.Context, network, addr string) (net.Conn, error)

	deadline       time.Time
	datanodes      *datanodeFailover
	datanodeTokens map[string]*hadoop_common.TokenProto
}

// SetDeadline sets the deadline for future ReadChecksum calls. A zero value
// for t means Read will not time out.
func (cr *ChecksumReader) SetDeadline(t time.Time) error {
	cr.deadline = t
	// Return the error at connection time.
	return nil
}

// ReadChecksum returns the checksum of the block.
func (cr *ChecksumReader) ReadChecksum() ([]byte, error) {
	if cr.datanodes == nil {
		locs := cr.Block.GetLocs()
		datanodes := make([]string, len(locs))
		for i, loc := range locs {
			dn := loc.GetId()
			datanodes[i] = getDatanodeAddress(dn, cr.UseDatanodeHostname)
		}

		cr.datanodes = newDatanodeFailover(datanodes)

		// For EC (striped) blocks, each datanode holds an internal block with its
		// own token stored in blockTokens. Build a map from address to token so
		// that readChecksum can send the correct token for each datanode.
		if blockTokens := cr.Block.GetBlockTokens(); len(blockTokens) == len(locs) {
			cr.datanodeTokens = make(map[string]*hadoop_common.TokenProto, len(locs))
			for i, token := range blockTokens {
				cr.datanodeTokens[datanodes[i]] = token
			}
		}
	}

	for cr.datanodes.numRemaining() > 0 {
		address := cr.datanodes.next()
		checksum, err := cr.readChecksum(address)
		if err != nil {
			cr.datanodes.recordFailure(err)
			continue
		}

		return checksum, nil
	}

	err := cr.datanodes.lastError()
	if err != nil {
		err = errors.New("No available datanodes for block.")
	}

	return nil, err
}

func (cr *ChecksumReader) readChecksum(address string) ([]byte, error) {
	if cr.DialFunc == nil {
		cr.DialFunc = (&net.Dialer{}).DialContext
	}

	conn, err := cr.DialFunc(context.Background(), "tcp", address)
	if err != nil {
		return nil, err
	}

	err = conn.SetDeadline(cr.deadline)
	if err != nil {
		return nil, err
	}

	err = cr.writeBlockChecksumRequest(conn, address)
	if err != nil {
		return nil, err
	}

	resp, err := cr.readBlockChecksumResponse(conn)
	if err != nil {
		return nil, err
	}

	return resp.GetChecksumResponse().GetBlockChecksum(), nil
}

// A checksum request to a datanode:
// +-----------------------------------------------------------+
// |  Data Transfer Protocol Version, int16                    |
// +-----------------------------------------------------------+
// |  Op code, 1 byte (CHECKSUM_BLOCK = 0x55)                  |
// +-----------------------------------------------------------+
// |  varint length + OpReadBlockProto                         |
// +-----------------------------------------------------------+
func (cr *ChecksumReader) writeBlockChecksumRequest(w io.Writer, address string) error {
	header := []byte{0x00, dataTransferVersion, checksumBlockOp}

	// For EC (striped) blocks, use the per-datanode token if available.
	token := cr.Block.GetBlockToken()
	if t, ok := cr.datanodeTokens[address]; ok {
		token = t
	}

	op := newChecksumBlockOp(cr.Block, token)
	opBytes, err := makePrefixedMessage(op)
	if err != nil {
		return err
	}

	req := append(header, opBytes...)
	_, err = w.Write(req)
	if err != nil {
		return err
	}

	return nil
}

// The response from the datanode:
// +-----------------------------------------------------------+
// |  varint length + BlockOpResponseProto                     |
// +-----------------------------------------------------------+
func (cr *ChecksumReader) readBlockChecksumResponse(r io.Reader) (*hdfs.BlockOpResponseProto, error) {
	resp := &hdfs.BlockOpResponseProto{}
	err := readPrefixedMessage(r, resp)
	return resp, err
}

func newChecksumBlockOp(block *hdfs.LocatedBlockProto, token *hadoop_common.TokenProto) *hdfs.OpBlockChecksumProto {
	return &hdfs.OpBlockChecksumProto{
		Header: &hdfs.BaseHeaderProto{
			Block: block.GetB(),
			Token: token,
		},
	}
}
