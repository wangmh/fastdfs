.SUFFIXES: .c .o

COMPILE = $(CC) -Wall -O2 -DOS_LINUX
#COMPILE = $(CC) -Wall -g -DOS_LINUX -D__DEBUG__
INC_PATH = -I../common -I/usr/local/include
LIB_PATH = -L/usr/local/lib -lpthread
TARGET_PATH = /usr/local/bin

COMMON_LIB =
SHARED_OBJS = ../common/hash.o ../common/fdfs_define.o ../common/chain.o \
              ../common/shared_func.o ../common/ini_file_reader.o \
              ../common/logger.o ../common/sockopt.o ../common/fdfs_global.o \
              tracker_proto.o tracker_mem.o tracker_service.o \
              tracker_global.o tracker_func.o

ALL_OBJS = $(SHARED_OBJS)

ALL_PRGS = fdfs_trackerd 

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
