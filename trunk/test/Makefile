.SUFFIXES: .c .o .lo

COMPILE = $(CC) -g -Wall -D_FILE_OFFSET_BITS=64 -DDEBUG
INC_PATH = -I/usr/local/include
LIB_PATH = -L/usr/local/lib -lfdfsclient
TARGET_PATH = /usr/local/bin

SHARED_OBJS = common_func.o dfs_func.o

ALL_OBJS = $(SHARED_OBJS)

ALL_PRGS = gen_files test_upload test_download test_delete combine_result

all: $(ALL_OBJS) $(ALL_PRGS)
.o:
	$(COMPILE) -o $@ $<  $(SHARED_OBJS) $(LIB_PATH) $(INC_PATH)
.c:
	$(COMPILE) -o $@ $<  $(SHARED_OBJS) $(LIB_PATH) $(INC_PATH)
.c.o:
	$(COMPILE) -c -o $@ $<  $(INC_PATH)
.c.lo:
	$(COMPILE) -c -fPIC -o $@ $<  $(INC_PATH)
install:
	cp -f $(ALL_PRGS) /usr/local/bin
clean:
	rm -f $(ALL_OBJS) $(ALL_PRGS)
