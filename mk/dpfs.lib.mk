#  DPFS-License-Identifier: Apache-2.0 license
#  Copyright (C) 2025 LBR.
#  All rights reserved.
#

CXXFLAG += -fpie -fPIC -shared


all : $(LIB)

$(LIB) : $(OBJS)
	$(CXX) $(CXXFLAG) $(CXXHEADER) $(CXXLIB) -o $@ $(OBJS)

%.o: %.cpp
	$(CXX) $(CXXFLAG) $(CXXHEADER) $(CXXLIB) -c $< -o $@


clean : 
	rm -f $(LIB) $(OBJS)