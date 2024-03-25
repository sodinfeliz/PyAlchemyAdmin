.PHONY: clean build

PROJECT_DIR := $(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))

clean:
	rm -rf $(PROJECT_DIR)/build
	rm -rf $(PROJECT_DIR)/dist
	rm -rf $(PROJECT_DIR)/*.egg-info

build: clean
	python3 -m build

