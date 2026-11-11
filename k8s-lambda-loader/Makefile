# SAM custom build (BuildMethod: makefile) for K8sLoaderFunction.
# Same strategy as ../github-lambda-loader/Makefile: trimmed deps + vendored
# cartography/ source tree from the pinned fork tag.
#
# Run `sam build --use-container` on macOS (native wheels).
CARTOGRAPHY_GIT_REF ?= v0.0.1rc18
CARTOGRAPHY_FORK_URL ?= https://github.com/wheel5up/cartography.git
VENDOR_TMP := /tmp/cartography-vendor-src-k8s

build-K8sLoaderFunction:
	mkdir -p "$(ARTIFACTS_DIR)"
	cp src/handler.py "$(ARTIFACTS_DIR)/"
	pip install -r requirements.txt --target "$(ARTIFACTS_DIR)" --no-cache-dir
	rm -rf $(VENDOR_TMP)
	git clone --depth 1 --branch "$(CARTOGRAPHY_GIT_REF)" "$(CARTOGRAPHY_FORK_URL)" $(VENDOR_TMP)
	cp -r $(VENDOR_TMP)/cartography "$(ARTIFACTS_DIR)/cartography"
	rm -rf $(VENDOR_TMP)
	find "$(ARTIFACTS_DIR)/cartography" -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true
	# setuptools-scm normally generates _version.py; a raw clone has none.
	printf '%s\n' \
	  "__version__ = version = '$(CARTOGRAPHY_GIT_REF)-lambda-vendored'" \
	  "__version_tuple__ = version_tuple = (0, 0, 0, 'lambda-vendored')" \
	  "__commit_id__ = commit_id = None" \
	  > "$(ARTIFACTS_DIR)/cartography/_version.py"
