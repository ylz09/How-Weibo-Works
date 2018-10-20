INCLUDE	= -I./ -I../
	 
LIBFLAG	= -lcrypto -lm -lz -lpthread -lstdc++

CFLAGS	+= -Wall -fPIC -shared

.c.o:
	@echo "              "
	@echo "CXX ..."
	gcc $(CFLAGS) $(INCLUDE) -c -o $@ $<

OBJS = resort.o ./rbtree/rbtree.o 

TAGET	= q_resort.so

all: $(TAGET)
q_resort.so: $(OBJS)
	gcc -g -o $@ $(OBJS) $(CFLAGS) $(INCLUDE) $(LIBFLAG) 

clean:
	-rm -rf $(TAGET) $(OBJS)


