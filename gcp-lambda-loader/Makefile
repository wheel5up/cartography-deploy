# SAM custom build (BuildMethod: makefile) for GcpLoaderFunction.
#
# Same strategy as ../kandji-lambda-loader/Makefile: pip-install a trimmed
# dependency set, then vendor the pinned `cartography/` source tree from the
# wheel5up fork.
#
# The Google SDKs are large (~440MB untrimmed, over Lambda's 250MB unzipped
# limit). Trimmed below to what cartography.intel.gcp actually imports:
# - aiplatform: only google.cloud.aiplatform_v1.* is imported; the high-level
#   SDK, v1beta1, vertexai and agentplatform are dropped.
# - googleapiclient bundles ~600 static discovery docs; build_client() uses
#   static discovery, so only the APIs cartography builds are kept.
CARTOGRAPHY_GIT_REF ?= v0.0.1rc31
CARTOGRAPHY_FORK_URL ?= https://github.com/wheel5up/cartography.git
VENDOR_TMP := /tmp/cartography-vendor-src-gcp

DISCOVERY_KEEP := aiplatform.v1 apikeys.v2 bigquery.v2 bigqueryconnection.v1 \
  bigtableadmin.v2 cloudasset.v1 cloudfunctions.v1 cloudkms.v1 \
  cloudresourcemanager.v1 cloudresourcemanager.v3 compute.v1 container.v1 \
  dns.v1 iam.v1 notebooks.v1 notebooks.v2 run.v1 run.v2 secretmanager.v1 \
  serviceusage.v1 sqladmin.v1beta4 storage.v1

build-GcpLoaderFunction:
	mkdir -p "$(ARTIFACTS_DIR)"
	cp src/handler.py "$(ARTIFACTS_DIR)/"
	pip install -r requirements.txt --target "$(ARTIFACTS_DIR)" --no-cache-dir
	# --- trim Google SDKs ---
	rm -rf "$(ARTIFACTS_DIR)/google/cloud/aiplatform" \
	       "$(ARTIFACTS_DIR)/google/cloud/aiplatform_v1beta1" \
	       "$(ARTIFACTS_DIR)/vertexai" "$(ARTIFACTS_DIR)/agentplatform"
	cd "$(ARTIFACTS_DIR)/googleapiclient/discovery_cache/documents" && \
	  for f in *.json; do \
	    keep=0; for k in $(DISCOVERY_KEEP); do [ "$$f" = "$$k.json" ] && keep=1; done; \
	    [ $$keep = 1 ] || rm -f "$$f"; \
	  done
	find "$(ARTIFACTS_DIR)" -name "*.dist-info" -type d -prune -exec rm -rf {} + 2>/dev/null || true
	find "$(ARTIFACTS_DIR)" -name "__pycache__" -type d -prune -exec rm -rf {} + 2>/dev/null || true
	# --- vendor cartography ---
	rm -rf $(VENDOR_TMP)
	git clone --depth 1 --branch "$(CARTOGRAPHY_GIT_REF)" "$(CARTOGRAPHY_FORK_URL)" $(VENDOR_TMP)
	cp -r $(VENDOR_TMP)/cartography "$(ARTIFACTS_DIR)/cartography"
	rm -rf $(VENDOR_TMP)
	find "$(ARTIFACTS_DIR)/cartography" -name "__pycache__" -type d -prune -exec rm -rf {} + 2>/dev/null || true
	# setuptools-scm normally generates _version.py; a raw clone has none.
	printf '%s\n' \
	  "__version__ = version = '$(CARTOGRAPHY_GIT_REF)-lambda-vendored'" \
	  "__version_tuple__ = version_tuple = (0, 0, 0, 'lambda-vendored')" \
	  "__commit_id__ = commit_id = None" \
	  > "$(ARTIFACTS_DIR)/cartography/_version.py"
	# Fail the build (not the first run) if trimming broke an import.
	cd "$(ARTIFACTS_DIR)" && python -c "import handler, cartography.intel.gcp, cartography.intel.gcp.vertex.models, cartography.intel.gcp.artifact_registry; print('import check ok')"
	# Every kept discovery doc must build offline, and compute must still
	# accept returnPartialSuccess (lost in google-api-python-client 2.201.0).
	cd "$(ARTIFACTS_DIR)" && python -c "\
	from googleapiclient.discovery import build; \
	from google.auth.credentials import AnonymousCredentials as A; \
	[build(s, v, credentials=A(), static_discovery=True) for s, v in (d.split('.', 1) for d in '$(DISCOVERY_KEEP)'.split())]; \
	build('compute', 'v1', credentials=A(), static_discovery=True).instances().aggregatedList(project='x', returnPartialSuccess=True); \
	print('discovery check ok')"
