SUBDIRS = bht

all:
	for d in $(SUBDIRS); do \
		make -C $$d all; \
	done

clean:
	for d in $(SUBDIRS); do \
		make -C $$d dist_clean; \
	done

.PHONY: all clean

