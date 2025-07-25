#  DPFS-License-Identifier: Apache-2.0 license
#  Copyright (C) 2025 LBR.
#  All rights reserved.
#







all : $(APP)

$(APP) : $(OBJS)
	$(CXX) $(CXXFLAG) $(CXXHEADER) $(CXXLIB) -o $@ $(OBJS)

$(OBJS) : $(CXXSOURCE)
	$(CXX) $(CXXFLAG) $(CXXHEADER) $(CXXLIB) -c $^

clean : 
	rm -f $(APP) $(OBJS)