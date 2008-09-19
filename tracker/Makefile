.SUFFIXES: .c .o

COMPILE = $(CC) -O3 -Wall -D_FILE_OFFSET_BITS=64 -DOS_LINUX
INC_PATH = -I../common -I/usr/local/include
LIB_PATH = -L/usr/local/lib  -lpthread
TARGET_PATH = /usr/local/bin

SHARED_OBJS = ../common/hash.o ../common/fdfs_define.o ../common/chain.o \
              ../common/shared_func.o ../common/ini_file_reader.o \
              ../common/logger.o ../common/sockopt.o ../common/fdfs_global.o \
              tracker_proto.o tracker_mem.o tracker_service.o \
              tracker_global.o tracker_func.o

ALL_OBJS = $(SHARED_OBJS)

ALL_PRGS = fdfs_trackerd 

all: $(ALL_OBJS) $(ALL_PRGS)
.o:
	$(COMPILE) -o $@ $<  $(SHARED_OBJS) $(LIB_PATH) $(INC_PATH)
.c:
	$(COMPILE) -o $@ $<  $(ALL_OBJS) $(LIB_PATH) $(INC_PATH)
.c.o:
	$(COMPILE) -c -o $@ $<  $(INC_PATH)
install:
	cp -f $(ALL_PRGS) /usr/local/bin
clean:
	rm -f $(ALL_OBJS) $(ALL_PRGS)
