package payload

// Safe is used for protection from multithreaded competition
type Safe interface {
	// Copy performs a deep copy of object
	Copy() Safe

	MarshalBinary() []byte
}
