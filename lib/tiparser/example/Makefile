CXX ?= c++
CXXFLAGS ?= -std=c++17 -O2 -Wall -Wextra

UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Darwin)
  RPATH_FLAG := -Wl,-rpath,@loader_path/../build
else
  RPATH_FLAG := -Wl,-rpath,'$$ORIGIN/../build'
endif

.PHONY: select-from-where run clean

select-from-where: select_from_where.cpp
	$(CXX) $(CXXFLAGS) -I.. $< -L../build -ltidbparser $(RPATH_FLAG) -o select-from-where

run: select-from-where
	./select-from-where "select a, b from t where a in (1,2)"

clean:
	rm -f select-from-where
