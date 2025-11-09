CC =	gcc

CFLAGS =  -g -Wall -Werror 

## picky compiler options
#CFLAGS=-Wall -Wextra -Wpointer-arith -pedantic -g -std=gnu99

LDFLAGS = -lpthread

## add the memory sanitizer
# CFLAGS += -fsanitize=address
# LDFLAGS += -fsanitize=address

# defines the set of configuration variables for the Makefile
include Makefile.config

SRC_APPS = $(wildcard tests/*.c)

TARGETS = $(patsubst %.c, %.run, $(SRC_APPS))

SRC_DEPS = $(wildcard *.c)

SRC_DEPS_OBJ= $(patsubst %.c, %.o, $(SRC_DEPS))

DEPS = $(wildcard *.h)

DEPS += Makefile.config


ifdef VERBOSE
ifneq ($(VERBOSE),0)
CONFIG_FLAGS += -DVERBOSE=$(VERBOSE)
endif
endif

ifdef WITH_DEPENDENCIES
ifneq ($(WITH_DEPENDENCIES),0)
CONFIG_FLAGS += -DWITH_DEPENDENCIES
endif
endif

CONFIG_FLAGS += -DQUEUE_CAPACITY=$(QUEUE_CAPACITY)
CONFIG_FLAGS += -DTHREAD_COUNT=$(THREAD_COUNT)



all: $(TARGETS)

tests/%.run: tests/%.o $(SRC_DEPS_OBJ)
	$(CC) -o $@ $^ $(LDFLAGS)

tests/%.o: tests/%.c $(DEPS)
	$(CC) -c $< $(CFLAGS) $(CONFIG_FLAGS) -o $@

%.o: %.c $(DEPS)
	$(CC) -c $< $(CFLAGS) $(CONFIG_FLAGS)


clean:
	rm -rf *.o tests/*.o tests/*.run *~ tests/*~
