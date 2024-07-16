module github.com/twmb/franz-go

go 1.21

require (
	github.com/klauspost/compress v1.17.8
	github.com/pierrec/lz4/v4 v4.1.21
	github.com/twmb/franz-go/pkg/kmsg v1.8.0
	golang.org/x/crypto v0.23.0
)

require github.com/twmb/franz-go/pkg/kfake v0.0.0-20240710015325-a5f2b710830e // indirect

retract v1.11.4 // This version is actually a breaking change and requires a major version change.
