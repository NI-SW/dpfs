#  DPFS-License-Identifier: Apache-2.0 license
#  Copyright (C) 2025 LBR.
#  All rights reserved.
#

CXXFLAG += -fpie -fPIC -shared

ifeq ($(OSNAME), AIX)
EXTRALIB := $(patsubst %.so,%.a,$(LIB))
EXTRADLL := $(shell echo 'ln -sf $(LIB) $(EXTRALIB)')
# EXTRARMDLL := $(shell echo 'rm -f $(EXTRALIB)')
EXTRACLEAN += $(EXTRALIB)
else ifeq ($(OSNAME), OS400)
EXTRALIB := $(patsubst %.so,%.a,$(LIB))
EXTRADLL := $(shell echo 'ln -sf $(LIB) $(EXTRALIB)')
# EXTRARMDLL := $(shell echo 'rm -f $(EXTRALIB)')
EXTRACLEAN += $(EXTRALIB)
endif

all : $(LIB)
	$(EXTRADLL)

$(LIB) : $(OBJS)
	$(CXX) $(CXXFLAG) $(CXXHEADER) $(CXXLIB) -o $@ $(OBJS)

%.o: %.cpp
	$(CXX) $(CXXFLAG) $(CXXHEADER) $(CXXLIB) -c $< -o $@

clean : 
	rm -f $(LIB) $(OBJS) $(EXTRACLEAN)