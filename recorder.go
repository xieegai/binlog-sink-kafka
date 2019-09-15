package ksink

type KSinkRecorder interface {
	Create(id string, payload []byte) error
}
