package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pkg/errors"
)

// SAReader Stand Alone reader
type SAReader struct {
	Fd *badger.Txn
}

func (r *SAReader) GetCF(cf string, key []byte) ([]byte, error) {
	// item, err := r.Fd.Get(engine_util.KeyWithCF(cf, key))
	// if err != nil {
	// 	return nil, errors.Wrap(err, "Get err")
	// }
	// data, err := item.Value()
	// if err != nil {
	// 	return nil, errors.Wrap(err, "Value err")
	// }
	data, err := engine_util.GetCFFromTxn(r.Fd, cf, key)
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return nil, nil
		}
		return nil, errors.Wrap(err, "GetCFFromTxn err")
	}

	return data, nil
}
func (r *SAReader) IterCF(cf string) engine_util.DBIterator {
	iter := engine_util.NewCFIterator(cf, r.Fd)
	// iterator := &SAIterator{
	// 	Iter: iter,
	// }

	return iter
}
func (r *SAReader) Close() {
	r.Fd.Discard()
}
