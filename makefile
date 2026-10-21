#
# FILE            makefile
#
# Copyright 2026 Seamware
# SPDX-License-Identifier: Apache-2.0
#
# corIotaLwm2mBridge is a bridge PLUGIN - iotalwm2m.so, dlopen'd by the broker, linked by nothing. A bridge
# lives in a repo of its own, as corMqttBridge and corModbusBridge do: the broker's tree does not grow
# one directory, or one library, per protocol it can speak.
#
PLUGIN        = iotalwm2m.so
CC            = gcc

#
# Where the cor-lib headers are - corBridge, corArgs, corLog, corAlloc, corJson, corTree - in the
# sibling layout every cor repo builds against: <COR_LIBS>/corBridge/BridgeDriver.h, ...
#
#   a workstation    the parent of this checkout (~/git/...) - the default
#   CI               <workspace>/stack/... (corLibs' bootstrap.sh clones them there)
#   coraine-dev      the source of the installed broker, /usr/share/coraine/src/coraine-src.tar.xz -
#                    unpacked into .coraine-src/ on the first build when there are no siblings
#
COR_LIBS         ?= ..
CORAINE_SRC_TAR  ?= /usr/share/coraine/src/coraine-src.tar.xz
PLUGIN_DIR       ?= /opt/seamware/plugins/bridge

ifeq ($(wildcard $(COR_LIBS)/corBridge/BridgeDriver.h),)
ifneq ($(wildcard $(CORAINE_SRC_TAR)),)
COR_LIBS      := .coraine-src
endif
endif

INCLUDE       = -I$(COR_LIBS)
#
# -fvisibility=hidden: the .so exports bridgeRegister and nothing else (it says so where it is defined)
#
CFLAGS        = -std=gnu11 -O2 -Wall -Werror -fPIC -fvisibility=hidden $(INCLUDE) -MMD -MP

#
# BUILD - debug (the default) or release, as every cor lib; corLibs passes it down.
# Traces (COR_T) are compiled in for a debug build only - see corLog.h.
#
BUILD        ?= debug

ifeq ($(BUILD),debug)
CFLAGS       += -DCOR_T_ON
endif

#
# The broker is linked rdynamic, so corLog, corAlloc, corJson and corTree resolve from the running
# process at dlopen - a plugin is built against the headers of the broker it is loaded into. No transport
# library: CoAP over UDP is the plugin's own (coap.c). libm for the values' numbers.
#
LIBS          = -lpthread -lm

SOURCES       = src/lwm2mRegister.c src/lwm2mServer.c src/coap.c
OBJS          = $(SOURCES:.c=.o)
DEPS          = $(SOURCES:.c=.d)

all: $(PLUGIN)

$(PLUGIN): $(OBJS)
	$(CC) -shared $(OBJS) -o $(PLUGIN) $(LIBS)

#
# The objects sit beside the sources, one flavour at a time: .flags holds the compile line they were
# built with, and a different one (BUILD=release after a debug build) rebuilds them instead of
# installing the other flavour's.
#
.flags: FORCE
	@echo '$(CFLAGS)' | cmp -s - $@ || echo '$(CFLAGS)' > $@

FORCE:

%.o: %.c .flags | $(COR_LIBS)/corBridge/BridgeDriver.h
	$(CC) $(CFLAGS) -c $< -o $@

#
# No sibling headers: the coraine-dev source, or say how to get one
#
$(COR_LIBS)/corBridge/BridgeDriver.h:
	@if [ "$(COR_LIBS)" = ".coraine-src" ]; then \
	  echo "unpacking $(CORAINE_SRC_TAR) into .coraine-src/"; \
	  mkdir -p .coraine-src && tar -xJf $(CORAINE_SRC_TAR) -C .coraine-src --strip-components=1; \
	else \
	  echo "no $(COR_LIBS)/corBridge/BridgeDriver.h - clone the cor libs beside this repo (corLibs: ./bootstrap.sh)," >&2; \
	  echo "install coraine-dev, or name the directory that holds them: make COR_LIBS=<dir>" >&2; \
	  exit 1; \
	fi

install: all
	mkdir -p $(PLUGIN_DIR)
	cp -p $(PLUGIN) $(PLUGIN_DIR)/

di: install
ci: clean install

#
# contract - does this still satisfy corBridge's BridgeDriver.h? lwm2mRegister.c is the file that fills
# in the BridgeDriver struct.
#
contract: | $(COR_LIBS)/corBridge/BridgeDriver.h
	@$(CC) -std=gnu11 -Wall -Werror -fPIC -I$(COR_LIBS) -c src/lwm2mRegister.c -o /tmp/corIotaLwm2mBridge-contract.o
	@rm -f /tmp/corIotaLwm2mBridge-contract.o
	@echo "corIotaLwm2mBridge: contract OK - BridgeDriver.h is still satisfied"

#
# test - the functional tests in test/funcTests/cases: a broker (coraine on PATH) loads this build of
# the plugin, on a port of its own. corTest is the cor stack's test runner (corLibs/bin, or the corTest
# directory of the coraine-dev source).
#
CORTEST      ?= $(shell command -v corTest || echo $(COR_LIBS)/corTest/corTest)

# COR_TEST_NO_REAP=on: corTest's start-up reaping (killall -9 coraine, and coraine's mock clients) is
# for coraine's own suite - here it would take down every broker on the host. These tests stop their
# own broker by pid.
#
test: all
	COR_TEST_NO_REAP=on $(CORTEST) --noColor

clean:
	rm -f src/*.o src/*.d *.so *~ .flags

-include $(DEPS)

.PHONY: all install di ci clean contract test FORCE
