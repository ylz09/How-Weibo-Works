VERSION_ID = "1.0.0.0"

CC = g++

WORK_ROOT = ../../../
LIB64 = $(WORK_ROOT)/lib64
THIRD64 = $(WORK_ROOT)/third-64

SRC := $(wildcard test*.cpp)
OBJS := $(SRC:%.cpp=%.o)
EXES := $(basename $(SRC))

INCLUDE = -I../ \
		  -I$(WORK_ROOT)/include \
		  -I$(WORK_ROOT)/da/frame/ \
		  -I$(WORK_ROOT)/sutils_cxx/include/ \
		  -I$(LIB64)/ependingpool/include/ \
		  -I$(LIB64)/clib/include/ \
		  -I$(LIB64)/common/include/ \
		  -I$(THIRD64)/gtest/include/ \
		  -I$(THIRD64)/libxml2/include/ \
		  -I$(THIRD64)/libmemcached/include \

LDFLAGS  = -L../\
		   -L../ -lsocialrealtimesearch\
		   -L$(WORK_ROOT)/da/dainterface/ -ldainterface \
		   -L$(LIB64)/ependingpool/lib/ -lependingpool \
		   -L$(LIB64)/clib/lib/ -lclib \
		   -L$(LIB64)/common/lib/ -lcommon \
		   -L$(WORK_ROOT)/sutils_cxx/src -lmemq \
		   -L$(WORK_ROOT)third-64/gtest/lib/ -lgtest_main -lgtest \
		   -L$(THIRD64)/libxml2/lib/ -lxml2 \
		   -L$(THIRD64)/libmemcached/lib -lmemcached \
		   -lpthread -lz -lssl

CPPFLAGS = -g -Wall -Winline -Werror -D__VERSION_ID__="\"$(VERSION_ID)\""

all : recompile $(EXES)

recompile :
	$(MAKE) -C ../ || exit 1

$(EXES): % : %.cpp
	$(CC) $^ -o $@ $(CPPFLAGS) $(INCLUDE) $(LDFLAGS)

.cpp.o:
	$(CC) $(CPPFLAGS) -c $(INCLUDE) $<

clean:
	rm -rf *.o
	rm -f $(EXES)

rebuild : clean all

