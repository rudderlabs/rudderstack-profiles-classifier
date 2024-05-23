VERSION?=0.0.0
VERSION_FILE=./src/predictions/version.py

version:
	/bin/rm -f $(VERSION_FILE)
	@echo "version = \"$(VERSION)\"" >> $(VERSION_FILE)
	/bin/cat $(VERSION_FILE)