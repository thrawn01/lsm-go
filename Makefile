.PHONY: flatbuf
flatbuf:
	flatc -o internal/flatbuf --go --gen-object-api --gen-all --gen-onefile --go-namespace flatbuf internal/flatbuf/schemas/manifest.fbs
	go fmt ./gen/*.go
