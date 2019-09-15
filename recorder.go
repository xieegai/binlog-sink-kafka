package ksink

// KSinkRecorder the interface to describe sync event recorder
type KSinkRecorder interface {
	//Create the func to create the sync event record before publish
	Create(id string, payload []byte) error
}
