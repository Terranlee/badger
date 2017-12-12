package badger

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/dgraph-io/badger/y"

	"github.com/dgraph-io/badger/protos"
)

func writeTo(entry *protos.KVPair, w io.Writer) error {
	if err := binary.Write(w, binary.LittleEndian, uint64(entry.Size())); err != nil {
		return err
	}
	buf, err := entry.Marshal()
	if err != nil {
		return err
	}
	_, err = w.Write(buf)
	return err
}

// Parse meta data. Return <transaction-type> <operation-type>
// If <transaction-type> == -1: transaction ends
// If <transaction-type> == 0: is transaction operation
// If <transaction-type> == 1: is not transaction operation

// If <operation-type> == 0: is set
// If <operation-type> == 1: is delete
func parseMeta(meta byte) (int, int) {
	if (meta & bitFinTxn) != 0 {
		// Transaction end
		return -1, 0
	}
	opType := 0
	if (meta & bitDelete) != 0 {
		opType = 1
	}
	txnType := 1
	if (meta & bitTxn) != 0 {
		txnType = 0
	}
	return txnType, opType
}

// Restore badger from a single VLog file
// This replays all the entries in a vlog file
func (db *DB) LoadSingleVLog(filename string, txn *Txn, end int) (*Txn, error) {
	// Create a buf, load data from vlog chunk by chunk
	// Currently, use 10M as chunk size
	// If a single entity is larger than 10M, this will fail
	bufSize := 10 * 1024 * 1024
	buffer := make([]byte, bufSize)

	// Create an empty header info, for future decode
	h := header{
		klen:      0,
		vlen:      0,
		expiresAt: 0,
		meta:      0,
		userMeta:  0,
	}

	in_flags := os.O_RDONLY
	fin, errin := os.OpenFile(filename, in_flags, 0666)
	if errin != nil {
		fmt.Println("Error open input")
	}

	// How many bytes are left in the last read from vlog
	remain := 0
	// Global offset used for loading snapshot
	globalOffset := 0

	for {
		// Read from vlog to buf, buffer[:remain] contains the data from last read
		n_read, err := fin.Read(buffer[remain:])
		if err != nil {
			fmt.Println("Error loading VLog file")
		}

		// Total length of valid data in this buffer
		bufValidLen := n_read + remain

		localOffset := 0
		for localOffset < bufValidLen {
			// Entry by entry decode and replay
			if localOffset+18 >= bufValidLen {
				break
			}

			h.Decode(buffer[localOffset:])
			// 4 is 32bit crc checksum length
			entryTotalLen := int(headerBufSize + h.klen + h.vlen + 4)
			if (entryTotalLen + localOffset) > bufValidLen {
				// This entry is not complete in current buffer, need data from next read
				break
			}

			txnType, opType := parseMeta(h.meta)

			if txnType == 1 {
				// TODO: How to deal with no transaction write?
				// Currently assume that all writes are transactional
				fmt.Println("Currently does not support no transaction write")
				return nil, errors.New("Found entry not in transaction")
			} else if txnType == -1 {
				// Close transaction
				if txn == nil {
					// Close transaction error
					fmt.Println("Error, try to close a transaction when no transaction is found")
					return nil, errors.New("Try to close a transaction when no transaction is found")
				}
				tsBytes := make([]byte, 8)
				kstart := uint32(localOffset + headerBufSize)
				copy(tsBytes, buffer[kstart+h.klen-8:kstart+h.klen])
				originTs := binary.LittleEndian.Uint64(tsBytes)

				y.Check(txn.Commit(nil, originTs))
				txn = nil
			} else if txnType == 0 {
				// In a transaction
				if txn == nil {
					txn = db.NewTransaction(true)
				}

				// 8 is for uint64 commitTs, the key in vlog is y.KeyWithTs(txnKey, commitTs)
				// See transaction.go line 400 for details
				// Remove this commitTs to get the real key and replay the add/delete
				key := make([]byte, h.klen-8)
				tsBytes := make([]byte, 8)

				kstart := uint32(localOffset + headerBufSize)
				copy(key, buffer[kstart:kstart+h.klen-8])
				copy(tsBytes, buffer[kstart+h.klen-8:kstart+h.klen])
				originTs := binary.LittleEndian.Uint64(tsBytes)
				if opType == 0 {
					value := make([]byte, h.vlen)
					vstart := uint32(kstart + h.klen)
					copy(value, buffer[vstart:vstart+h.vlen])
					y.Check(txn.SetWithTs(key, value, originTs))
				} else {
					y.Check(txn.Delete(key))
				}
			}

			localOffset += entryTotalLen

			// For loading snapshot
			globalOffset += entryTotalLen
			if end != -1 && globalOffset == end {
				break
			}
		}

		// For loading snapshot
		if end != -1 && globalOffset == end {
			break
		}

		// The remaining data in the end of previous buffer,
		// copy them to the start of new buffer, and load data from file in the next loop
		copy(buffer, buffer[localOffset:])
		remain = bufValidLen - localOffset

		// If the total amount of data is less than bufSize, end of file, break
		if bufValidLen != bufSize {
			break
		}
	}

	fin.Close()

	return txn, nil
}

// Get all vlog files from a directory
// Vlogs are sorted by filename
func GetAllVLogFiles(dir string) ([]string, error) {
	var vlogs []string

	files, err := ioutil.ReadDir(dir)
	if err != nil {
		fmt.Println("Err opening vlog directory: " + dir)
		return nil, err
	}

	for _, f := range files {
		if strings.HasSuffix(f.Name(), ".vlog") {
			vlogs = append(vlogs, f.Name())
		}
	}

	sort.Strings(vlogs)

	for _, v := range vlogs {
		fmt.Println(v)
	}

	return vlogs, nil
}

// Find the corresponding filename and offset of a snapshot
// If no snapshot if found, return ("", -1)
func FindSnapshotName(dir, snapshotName string) (string, int) {
	snapshots := dir + string(os.PathSeparator) + "SNAPSHOTS"
	file, err := os.Open(snapshots)
	if err != nil {
		fmt.Println("Error open SNAPSHOTS file")
		return "", -1
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		snapshot := scanner.Text()
		strs := strings.Split(snapshot, ":")
		if strs[0] == snapshotName {
			filename := strs[1]
			offset, err := strconv.Atoi(strs[2])
			if err != nil {
				fmt.Println("Can not parse offset value")
				return "", -1
			}
			return filename, offset
		}
	}
	return "", -1
}

// Restore badger from a series of VLog files, replay all the records in VLog
// Parameter is the directory ofr vlog file
func (db *DB) LoadFromVLog(dir string) error {
	vlogs, err := GetAllVLogFiles(dir)
	if err != nil {
		fmt.Println("Error getting vlog files")
		return err
	}

	var txn *Txn = nil

	for _, v := range vlogs {
		txn, err = db.LoadSingleVLog(dir+string(os.PathSeparator)+v, txn, -1)
		if err != nil {
			fmt.Println("Error restore vlog file: " + v)
			return err
		}
	}

	if txn != nil {
		fmt.Println("Error does not end with a transaction end")
	}
	return nil
}

// Restore badger from a series of VLog files,
// Only restore to a specific snapshot point in vlog
func (db *DB) LoadFromVLogToSnapshot(dir, snapshotName string) error {
	// Get the vlog filename and offset of a snapshot
	vlogFile, vlogOffset := FindSnapshotName(dir, snapshotName)
	if vlogOffset == -1 {
		fmt.Println("Can not find corresponding snapshot" + snapshotName)
		return errors.New("Can not find corresponding snapshot name" + snapshotName)
	}

	vlogs, err := GetAllVLogFiles(dir)
	if err != nil {
		fmt.Println("Error getting vlog files")
		return err
	}
	
	var txn *Txn = nil

	for _, v := range vlogs {
		// If it is the last file, use vlogOffset
		// Else, load the whole file
		offset := -1
		if v == vlogFile {
			offset = vlogOffset
		}

		txn, err = db.LoadSingleVLog(dir + string(os.PathSeparator) + v, txn, offset)

		if err != nil {
			fmt.Println("Error restore vlog file: " + v)
			return err
		}

		if v == vlogFile {
			break;
		}
	}

	if txn != nil {
		fmt.Println("Error does not end with a transaction end")
	}
	return nil
}

func (db *DB) Snapshot(snapshotName string) {
	// Push snapshotName into snapshotCh
	// Other things are in db.doWrites()
	db.snapshotCh <- snapshotName
}

// Backup dumps a protobuf-encoded list of all entries in the database into the
// given writer, that are newer than the specified version. It returns a
// timestamp indicating when the entries were dumped which can be passed into a
// later invocation to generate an incremental dump, of entries that have been
// added/modified since the last invocation of DB.Backup()
//
// This can be used to backup the data in a database at a given point in time.
func (db *DB) Backup(w io.Writer, since uint64) (uint64, error) {
	var tsNew uint64
	err := db.View(func(txn *Txn) error {
		opts := DefaultIteratorOptions
		opts.AllVersions = true
		it := txn.NewIterator(opts)
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			if item.Version() < since {
				// Ignore versions less than given timestamp
				continue
			}
			val, err := item.Value()
			if err != nil {
				return err
			}

			entry := &protos.KVPair{
				Key:       y.Copy(item.Key()),
				Value:     y.Copy(val),
				UserMeta:  []byte{item.UserMeta()},
				Version:   item.Version(),
				ExpiresAt: item.ExpiresAt(),
			}

			// Write entries to disk
			if err := writeTo(entry, w); err != nil {
				return err
			}
		}
		tsNew = txn.readTs
		return nil
	})
	return tsNew, err
}

// Load reads a protobuf-encoded list of all entries from a reader and writes
// them to the database. This can be used to restore the database from a backup
// made by calling DB.Dump().
//
// DB.Load() should be called on a database that is not running any other
// concurrent transactions while it is running.
func (db *DB) Load(r io.Reader) error {
	br := bufio.NewReaderSize(r, 16<<10)
	unmarshalBuf := make([]byte, 1<<10)
	var entries []*entry
	var wg sync.WaitGroup
	errChan := make(chan error, 1)

	// func to check for pending error before sending off a batch for writing
	batchSetAsyncIfNoErr := func(entries []*entry) error {
		select {
		case err := <-errChan:
			return err
		default:
			wg.Add(1)
			return db.batchSetAsync(entries, func(err error) {
				defer wg.Done()
				if err != nil {
					select {
					case errChan <- err:
					default:
					}
				}
			})
		}
	}

	for {
		var sz uint64
		err := binary.Read(br, binary.LittleEndian, &sz)
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		if cap(unmarshalBuf) < int(sz) {
			unmarshalBuf = make([]byte, sz)
		}

		e := &protos.KVPair{}
		if _, err = io.ReadFull(br, unmarshalBuf[:sz]); err != nil {
			return err
		}
		if err = e.Unmarshal(unmarshalBuf[:sz]); err != nil {
			return err
		}
		entries = append(entries, &entry{
			Key:       y.KeyWithTs(e.Key, e.Version),
			Value:     e.Value,
			UserMeta:  e.UserMeta[0],
			ExpiresAt: e.ExpiresAt,
		})

		if len(entries) == 1000 {
			if err := batchSetAsyncIfNoErr(entries); err != nil {
				return err
			}
			entries = entries[:0]
		}
	}

	if len(entries) > 0 {
		if err := batchSetAsyncIfNoErr(entries); err != nil {
			return err
		}
	}

	wg.Wait()

	select {
	case err := <-errChan:
		return err
	default:
		return nil
	}
}
