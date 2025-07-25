#  DPFS-License-Identifier: Apache-2.0 license
#  Copyright (C) 2025 LBR.
#  All rights reserved.
#

CXXFLAG += -fPIC -shared
# CXXFLAG += 

all : $(LIB)

$(LIB) : $(OBJS)
	$(CXX) $(CXXFLAG) $(CXXHEADER) $(CXXLIB) -o $@ $(OBJS)

$(OBJS) : $(CXXSOURCE)
	$(CXX) $(CXXFLAG) $(CXXHEADER) $(CXXLIB) -c $^

clean : 
	rm -f $(LIB) $(OBJS)