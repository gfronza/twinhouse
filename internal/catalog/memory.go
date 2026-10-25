package catalog

// NewMemory returns a purely in-memory Catalog (no persistence; Refresh is a
// no-op), for unit tests of other packages. It applies the same validation as
// the persisted catalog; physical names are "<db>.<table>".
func NewMemory() Catalog { return New(nil, Options{}) }
