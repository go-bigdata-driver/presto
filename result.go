package presto

type result struct {
	lastID   int64
	affected int64
}

func (r *result) LastInsertId() (int64, error) {
	return r.lastID, nil
}

func (r *result) RowsAffected() (int64, error) {
	return r.affected, nil
}
