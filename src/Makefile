SUBDIRS = interface skel tools relay client

all:
	for d in $(SUBDIRS); do \
		make -C $$d all; \
	done

clean:
	for d in $(SUBDIRS); do \
		make -C $$d clean; \
	done

.PHONY: all clean

