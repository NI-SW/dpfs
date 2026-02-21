CXX ?= c++
CXXFLAGS ?= -std=c++17 -O2 -Wall -Wextra

UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Darwin)
  LIB_EXT := dylib
  RPATH_FLAG := -Wl,-rpath,@loader_path/../build
else
  LIB_EXT := so
  RPATH_FLAG := -Wl,-rpath,'$$ORIGIN/../build'
endif

.PHONY: example clean

example: example.cpp
	$(CXX) $(CXXFLAGS) -I.. example.cpp -L../../ -ltiparser $(RPATH_FLAG) -o example

clean:
	rm -f example

