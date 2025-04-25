BASE_DIR := $(abspath $(dir $(lastword $(MAKEFILE_LIST)))/../../)
include $(BASE_DIR)/common.mk

PROJ_NAME = test-guest-resource-row

build:
	cargo component build -p $(PROJ_NAME) $(WASM_BUILD_SETTING)
