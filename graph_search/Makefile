VERSION_ID = "__SVN__:"`svnversion -n`

WORK_ROOT = ../..
LIB64 = $(WORK_ROOT)/lib64
THIRD64 = $(WORK_ROOT)/third-64

CC = g++
AR = ar

SRC:= $(wildcard *.cpp)
OBJS := $(SRC:.cpp=.o)
DEBUG_OBJS := $(SRC:.cpp=_d.o)
LIBSO = libsocialrealtimesearch.so

INCLUDE = -I./ \
		  -I../pub/ \
          -I$(WORK_ROOT)/lib64/ \
		  -I$(LIB64)/api_caller/include \
		  -I$(WORK_ROOT)/da/algorithms/socialclient/ \
		  -I$(WORK_ROOT)/da/dict/include/ \
		  -I$(WORK_ROOT)/da/dautility/ \
		  -I$(WORK_ROOT)/da/frame/ \
		  -I$(WORK_ROOT)/da/dainterface/ \
		  -I$(WORK_ROOT)/sutils_cxx/include/ \
		  -I$(WORK_ROOT)/include/ \
		  -I$(THIRD64)/jsoncpp/include/ \
          -I$(THIRD64)/tinyxml/include/ \
		  -I$(THIRD64)/libjson/include/ \
		  -I$(THIRD64)/libxml2/include/ \
		  -I$(LIB64)/ependingpool/include/ \
		  -I$(LIB64)/clib/include/ \
		  -I$(LIB64)/odict/include/ \
		  -I$(LIB64)/common/include/ \
		  -I$(LIB64)/xslib/include/ \
		  -I$(THIRD64)/libmemcached/include \

LDFLAGS  = -L./	\
		   -L$(LIB64)/api_caller/lib -lapi_caller \
		   -L$(WORK_ROOT)/da/frame/ -ldaframe \
		   -L$(WORK_ROOT)/da/dautility/ -ldautility \
		   -L$(WORK_ROOT)/da/dict/lib/ -ldadict \
		   -L$(WORK_ROOT)/sutils_cxx/src -lmemq \
		   -L$(LIB64)/clib/lib/ -lclib \
		   -L$(LIB64)/odict/lib/ -lodict \
		   -L$(LIB64)/common/lib/ -lcommon \
		   -L$(THIRD64)/jsoncpp/lib/ -ljsoncpp \
		   -L$(THIRD64)/libmemcached/lib -lmemcached \
           -lz -lpthread \

CPPFLAGS = -g -O2 -Wall -fPIC -shared -rdynamic -Winline -DDEBUG_TIME -D__VERSION_ID__="\"$(VERSION_ID)\""  \
		   --param inline-unit-growth=60000 --param max-inline-insns-single=60000 \
		   --param large-function-growth=60000

all : $(LIBSO) $(DEBUG_LIBSO) 
	cp $(LIBSO) ../shell/so

.cpp.o:
	$(CC) $(CPPFLAGS) -c $(INCLUDE) $<

$(LIBSO):$(OBJS)
	$(CC) $(CPPFLAGS) -Wl,-soname,$(LIBSO) -o $@ $^ $(LDFLAGS) 

clean:
	rm -rf *.o $(LIBSO)

rebuild : clean all
