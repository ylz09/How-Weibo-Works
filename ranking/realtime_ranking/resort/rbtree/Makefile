.c.o:
	gcc -c -o $@ $<

TARGET = test
all: $(TARGET)
test : rbtree.o rtree.o test.o
	gcc -o $@ $^	

clean:
	rm *.o test
