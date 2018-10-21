SO_HOME=output/so
SO_WEIBO := $(SO_HOME)/libweibo.so

SO_VIP := $(SO_HOME)/libvip.so

SO_HOT_WEIBO := $(SO_HOME)/libhot.so

SOS	:= $(SO_WEIBO) $(SO_VIP) $(SO_HOT_WEIBO)

KWFILTER=../../../kwfilter_php/src

t=weibo

all: AC_DEP $(SOS) 

AC_SRC	:=	..
include $(AC_SRC)/make/common.mk

SRC := $(wildcard ./*.cpp)

INCS := $(INCS) \
	-I$(KWFILTER)/client \
	-I$(KWFILTER)/public \

OBJ := ac_caller_bc.o \
	ac_caller_da.o \
	ac_caller_man.o \
	ac_caller_social.o \
	ac_cate.o \
	ac_dispatch.o \
	ac_filter.o \
	ac_plugin_library.o \
	ac_query2.o \
	ac_tool.o \
	ac_truncate.o \
	call_monitor.o \
	call_ntp.o \
	doc_fields.o \
	filter_new.o \
	ac_source_id_dict.o \
	ac_content_control.o \
	ac_query.o \
	ac_intervene_dict.o \
	ac_sandbox.o \
	search_dict.o \
	ac_expr.o \
	./resort/rbtree/rtree.o \
	./resort/rbtree/rbtree.o \
	./resort/resort.o \
	$(KWFILTER)/public/kwfilter_public.o \
	$(KWFILTER)/client/user_strategy.o \
	$(KWFILTER)/client/kwfilter.o  \
	$(KWFILTER)/server/trie.o \
	$(KWFILTER)/public/stand.o \
	$(KWFILTER)/client/unicode.o \
	$(KWFILTER)/public/util.o \

WEIBO_OBJ := $(OBJ) \
	ac_sort.o \
	ac_hot_sort_model.o \
	ac_hot_caller_dc.o \
	hot_category_filter.o \
	ac_plugin_t_call_back.o \
	ac_hot_ext_data_parser.o \

VIP_OBJ := $(OBJ) \
	ac_vip_sort.o \
	ac_hot_sort_model.o \
	ac_hot_caller_dc.o \
	ac_hot_ext_data_parser.o \
	hot_category_filter.o \
	ac_plugin_t_call_back.o \
	csort_vip_base.o \

HOT_WEIBO_OBJ := $(OBJ) \
	ac_hot_sort.o \
	ac_hot_sort_model.o \
	ac_intervene_dict.o \
	ac_hot_ext_data_parser.o \
	hot_category_filter.o \
	ac_hot_caller_dc.o \
	ac_plugin_t_call_back.o \
	ac_hot_rank.o \
	csort_vip_base.o \

PLUGINDIR := $(PLUGINDIR_WEIBO)

AC_DEP:
	mkdir -p output/so;


ifeq (ac_r_pprof,$(pprof))
CFLAGS := -g -fstack-protector-all -Wall -DPPROF_AC -D__VERSION_ID__="\"$(VERSION_ID)\"" -fpic -Wno-deprecated
else
CFLAGS := -g -O2 -fstack-protector-all -Wall  -D__VERSION_ID__="\"$(VERSION_ID)\"" -fpic -Wno-deprecated
endif


CPPFLAGS := $(INCS)

.c.o:
	$(CC) -c $(CFLAGS) -D__MAIN__=main -D__STATIC__=static $(INCS) $< -o $@
.cpp.o:
	$(CXX) -c $(CFLAGS) -D__MAIN__=main -D__STATIC__=static $(INCS) $< -o $@


#miniblog
$(SO_WEIBO) : $(WEIBO_OBJ) 
	$(CC) -g -fPIC -shared -o $@ $^

TOP_SRCDIR := ../../../

$(SO_VIP) : $(VIP_OBJ)
	$(CC) -g -fPIC -L$(TOP_SRCDIR)/lib64/common/lib  -lcommon -I$(TOP_SRCDIR)/lib64/acquery/include/ -I$(TOP_SRCDIR)/third-64/boost/include -I$(TOP_SRCDIR)/ac/src/utils/  -shared -o $@ $^

$(SO_HOT_WEIBO) : $(HOT_WEIBO_OBJ) 
	$(CC) -g -fPIC -shared -o $@ $^

PPROFOBJBASE := ac_caller_bc.o \
	ac_caller_da.o \
	ac_caller_man.o \
	ac_caller_social.o \
	ac_cate.o \
	ac_dispatch.o \
	ac_filter.o \
	ac_plugin_library.o \
	ac_query2.o \
	ac_tool.o \
	ac_truncate.o \
	call_monitor.o \
	call_ntp.o \
	doc_fields.o \
	filter_new.o \
	ac_source_id_dict.o \
	ac_query.o \
	ac_intervene_dict.o \
	ac_sandbox.o \
	search_dict.o \
	./resort/rbtree/rtree.o \
	./resort/rbtree/rbtree.o \
	./resort/resort.o \
	ac_hot_sort_model.o \
	ac_hot_caller_dc.o \
	hot_category_filter.o \
	ac_plugin_t_call_back.o \
	ac_hot_ext_data_parser.o \
	ac_content_control.o \
	$(KWFILTER)/public/kwfilter_public.o \
	$(KWFILTER)/client/user_strategy.o \
	$(KWFILTER)/client/kwfilter.o  \
	$(KWFILTER)/server/trie.o \
	$(KWFILTER)/public/stand.o \
	$(KWFILTER)/client/unicode.o \
	$(KWFILTER)/public/util.o \

PPROFWEIBO := $(PPROFOBJBASE) \
	ac_sort.o \

PPROFHOT := $(PPROFOBJBASE) \
	ac_hot_sort.o \
	ac_hot_rank.o \
	csort_vip_base.o \

$(SO_HOME)/libweibopprof.a : $(PPROFWEIBO)
	mkdir -p output/so
	ar -cr -o $@ $^

$(SO_HOME)/libhotpprof.a : $(PPROFHOT)
	mkdir -p output/so
	ar -cr -o $@ $^

clean:
	find . -name "*.o" | xargs -i rm -rf {}
	find . -name "*.d" | xargs -i rm -rf {}
	rm -rf output  
