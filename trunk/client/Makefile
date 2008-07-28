.SUFFIXES: .c .o

COMPILE = $(CC) -Wall -O2 -DOS_LINUX
#COMPILE = $(CC) -Wall -g -DOS_LINUX -D__DEBUG__
INC_PATH = -I../common -I../tracker -I/usr/local/include
LIB_PATH = -L/usr/local/lib -lpthread
TARGET_PATH = /usr/local/bin

COMMON_LIB =
SHARED_OBJS = ../common/hash.o ../common/fdfs_define.o ../common/chain.o \
              ../common/shared_func.o ../common/ini_file_reader.o \
              ../common/logger.o ../common/sockopt.o ../common/fdfs_global.o \
              ../common/fdfs_base64.o \
              ../tracker/tracker_proto.o tracker_client.o client_func.o \
              client_global.o storage_client.o

HEADER_FILES = ../common/fdfs_define.h ../common/fdfs_global.h \
               ../common/fdfs_base64.h ../common/shared_func.h \
               ../tracker/tracker_types.h ../tracker/tracker_proto.h \
               tracker_client.h storage_client.h client_func.h \
               client_global.h fdfs_client.h

ALL_OBJS = $(SHARED_OBJS)

ALL_PRGS = fdfs_monitor fdfs_test
ALL_LIBS = libfdfsclient.so.1

all: $(ALL_OBJS) $(ALL_PRGS) $(ALL_LIBS)
libfdfsclient.so.1:
	$(COMPILE) -o $@ $< -shared -fPIC $(SHARED_OBJS) $(COMMON_LIB) $(LIB_PATH)
.o:
	$(COMPILE) -o $@ $<  $(SHARED_OBJS) $(COMMON_LIB) $(LIB_PATH) $(INC_PATH)
.c:
	$(COMPILE) -o $@ $<  $(ALL_OBJS) $(COMMON_LIB) $(LIB_PATH) $(INC_PATH)
.c.o:
	$(COMPILE) -c -o $@ $<  $(INC_PATH)
install:
	cp -f $(ALL_PRGS) $(TARGET_PATH)
	cp -f $(ALL_LIBS) /usr/local/lib/
	cp -f $(HEADER_FILES) /usr/local/include/
	ln -s /usr/local/lib/libfdfsclient.so.1 /usr/local/lib/libfdfsclient.so
clean:
	rm -f $(ALL_OBJS) $(ALL_PRGS) $(ALL_LIBS)
