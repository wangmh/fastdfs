.SUFFIXES: .c .o

COMPILE = $(CC) -Wall -O2 -DOS_LINUX
#COMPILE = $(CC) -Wall -g -DOS_LINUX -D__DEBUG__
INC_PATH = -I/usr/local/include
LIB_PATH = -L/usr/local/lib -lpthread -lfdfsclient
TARGET_PATH = /usr/local/bin

COMMON_LIB =
ALL_OBJS = 

ALL_PRGS = fdfs_monitor fdfs_test

all: $(ALL_OBJS) $(ALL_PRGS)
.o:
	$(COMPILE) -o $@ $<  $(SHARED_OBJS) $(COMMON_LIB) $(LIB_PATH) $(INC_PATH)
.c:
	$(COMPILE) -o $@ $<  $(ALL_OBJS) $(COMMON_LIB) $(LIB_PATH) $(INC_PATH)
.c.o:
	$(COMPILE) -c -o $@ $<  $(INC_PATH)
install:
	cp -f $(ALL_PRGS) $(TARGET_PATH)
clean:
	rm -f $(ALL_OBJS) $(ALL_PRGS)
