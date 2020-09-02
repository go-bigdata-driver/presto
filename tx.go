package presto

type tx struct{}

func (tx *tx) Commit() error {
	return nil
}

func (tx *tx) Rollback() error {
	return nil
}
