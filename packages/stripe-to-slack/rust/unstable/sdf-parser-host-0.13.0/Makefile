BASE_DIR := $(abspath $(dir $(lastword $(MAKEFILE_LIST)))/../../)
include $(BASE_DIR)/common.mk


setup-wasm: build-parser-guest
	echo "copying $(YAML_PARSER_WASM) to $(CURDIR)/wasm"
	mkdir -p $(CURDIR)/wasm
	cp $(YAML_PARSER_WASM) $(CURDIR)/wasm