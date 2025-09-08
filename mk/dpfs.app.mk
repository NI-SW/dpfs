#  DPFS-License-Identifier: Apache-2.0 license
#  Copyright (C) 2025 LBR.
#  All rights reserved.
#


all : $(APP)

$(APP) : $(OBJS)
	$(CXX) $(CXXFLAG) $(CXXHEADER) $(CXXLIB) -o $@ $(OBJS)

%.o: %.cpp
	$(CXX) $(CXXFLAG) $(CXXHEADER) $(CXXLIB) -c $< -o $@

clean : 
	rm -f $(APP) $(OBJS) $(EXTRCLEAN)

# cleanall : clean
# 	rm -f *.log core core.*